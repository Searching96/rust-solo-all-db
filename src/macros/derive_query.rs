// Query builder derive macro implementation

use proc_macro2::TokenStream;
use quote::{quote, format_ident};
use syn::{Data, DeriveInput, Fields, Result, Error, Attribute, Lit, Meta};

// Generate query builder implementation for a struct
pub fn generate_query_builder(input: &DeriveInput) -> Result<TokenStream> {
    let struct_name = &input.ident;
    let struct_name_str = struct_name.to_string().to_lowercase();

    // Parse struct fields;
    let fields = match &input.data {
        Data::Struct(data_struct) => match &data_struct.fields {
            Fields::Named(fields_named) => &fields_named.named,
            _ => return Err(Error::new_spanned(input, "QueryBuilder only supports structs with named fields")),
        },
        _ => return Err(Error::new_spanned(input, "QueryBuilder can only be derived for structs")),
    };

    // Identify primary key and indexed fields
    let mut primary_key_field = None;
    let mut indexed_fields = Vec::new();
    let mut all_fields = Vec::new();

    for field in fields {
        let field_name = field.ident.as_ref().unwrap();
        let field_type = &field.ty;
        all_fields.push((field_name, field_type));

        // Check for primary_key attribute
        if has_attribute(&field.attrs, "primary_key") {
            primary_key_field = Some(field_name);
        }

        // Check for indexed attribute
        if has_attribute(&field.attrs, "indexed") {
            indexed_fields.push(field_name);
        }
    }

    // If no primary key specified, use first field
    let primary_key = primary_key_field.unwrap_or(&all_fields[0].0);

    // Generate methods
    let find_by_methods = generate_find_by_methods(&all_fields, &indexed_fields, &struct_name, &struct_name_str)?;
    let save_method = generate_save_method(&all_fields, struct_name, &struct_name_str, primary_key)?;
    let delete_method = generate_delete_method(struct_name, &struct_name_str, primary_key)?;
    let new_method = generate_new_method(&all_fields, struct_name)?;
    let serialization_methods = generate_serialization_methods(&all_fields, struct_name)?;

    let expanded = quote! {
        impl #struct_name {
            #new_method
            #save_method
            #delete_method
            #sereialization_methods
            #(#find_by_methods)*
        }

        impl #struct_name {
            // Get the table name for this struct
            pub fn table_name() -> &'static str {
                #struct_name_str
            }
        }
    };

    Ok(expanded)
}

// Generate find_by methods for indexed fields
fn generate_find_by_methods (
    all_fields: &[(syn::Ident, &syn::Type)],
    indexed_fields: &[&syn::Ident],
    struct_name: &syn::Ident,
    struct_name: &str,
) -> Result<Vec<TokenStream>> {
    let mut methods = Vec::new();

    // Generate find_by method for each indexed fields
    for field_name in indexed_fields {
        let method_name = format_ident!("find_by_{}", field_name);
        let field_name_str = field_name.to_string();

        let method = quote! {
            pub fn #method_name(db: &crate::engine::LSMTree, value: &str) -> crate::DbResult<Vec<#struct_name>> {
                let key = format!("{}:{}:{}", #table_name, #field_name_str, value);
                match db.get(&key)? {
                    Some(serialize) => {
                        let instance: #struct_name = serde_json::from_str(&serialize)
                            .map_err(|e| crate::DbError::InvalidOperation(format!("Deserialization error: {}", e)))?;
                        Ok(vec![instance])
                    }
                    None => Ok(vec![]),
                }
            }
        };
        methods.push(method);
    }

    // Generate find_by_id method for primary key
    let primary_key_field = all_fields.first().unwrap().0; // Assume first field is primary key
    let find_by_id_method = quote! {
        pub fn find_by_id(db: &crate::engine::LSMTree, id: &str) -> crate::DbResult<Option<#struct_name>> {
            let key = format!("{}:id:{}", #table_name, id);
            match db.get(&key)? {
                Some(serialize) => {
                    let instance: #struct_name = serde_json::from_str(&serialize)
                        .map_err(|e| crate::DbError::InvalidOperation(format!("Deserialization error: {}", e)))?;
                    Ok(Some(instance))
                }
                None => Ok(None),
            }
        }
    };
    methods.push(find_by_id_method);

    // Generate find_all method
    let find_all_method = quote! {
        pub fn find_all(db: &crate::engine::LSMTree) -> crate::DbResult<Vec<#struct_name>> {
            // In a real implementation, this would scan the table
            // For now, return empty vec as this requires table scanning
            Ok(vec![])
        }
    };
    methods.push(find_all_method);

    Ok(methods)
}

// Generate save method
fn generate_save_method(
    all_fields: &[(syn::Ident, &syn::Type)],
    struct_name: &syn::Ident,
    struct_name_str: &str,
    primary_key: &syn::Ident,
) -> Result<TokenStream> {
    let method = quote! {
        pub fn save(&self, db: &mut crate::engine::LSMTree) -> crate::DbResult<()> {
            let serialized = self.to_json()?;
            let key = format!("{}:id:{}", #table_name, self.#primary_key);
            db.insert(&key, serialized)?;
            Ok(())
        }
    };
    Ok(method)
}

// Generate delete method
fn generate_delete_method(
    struct_name: &syn::Ident,
    struct_name_str: &str,
    primary_key: &syn::Ident,
) -> Result<TokenStream> {
    let method = quote! {
        pub fn delete_by_id(db: &mut crate::engine::LSMTree, id: &str) -> crate::DbResult<bool> {
            let key = format!("{}:id:{}", #table_name, id);
            Ok(db.delete(&key)?)
        }

        pub fn delete(&self, db: &mut crate::engine::LSTree) -> crate::DbResult<bool> {
            Self::delete_by_id(db, &self.#primary_key)
        }
    };
    Ok(method)
}

// Generate constructor method
fn generate_new_method(
    all_fields: &[(syn::Ident, &syn::Type)],
    struct_name: &syn::Ident,
) -> Result<TokenStream> {
    let field_params: Vec<_> = all_fields.iter().map(|(name, ty)| {
        quote! { #name: #ty }
    }).collect();

    let field_assignments: Vec<_> = all_fields.iter().map(|(name, _)| {
        quote! { #name }
    }).collect();

    let method = quote! {
        pub fn new(#(#field_params),*) -> Self {
            Self {
                #(#field_assignments),*
            }
        }
    };
    Ok(method)
}

// Generate serialization helper methodss
fn generate_serialization_methods(
    _all_fields: &[(syn::Ident, &syn::Type)],
    struct_name: &syn::Ident,
) -> Result<TokenStream> {
    let methods = quote! {
        pub fn to_json(&self) -> crate::DbResult<String> {
            serde_json::to_string(self)
                .map_err(|e| crate::DbError::InvalidOperation(format!("Serialization error: {}", e))
            )
        }

        pub fn from_json(json: &str) -> crate::DbResult<Self> {
            serde_json::from_str(json)
                .map_err(|e| crate::DbError::InvalidOperation(format!("Deserialization error: {}", e)))
        }
    };
    Ok(methods)
}

// Check if field has specific attribute
fn has_attribute(attrs: &[Attribute], name: &str) -> bool {
    attrs.iter().any(|attr| {
        if let Ok(meta) = attr.parse_meta() {
            if let Ok(meta) = attr.parse_meta() {
                match meta {
                    Meta::Path(path) => path.is_ident(name),
                    _ => false,
                }
            }
        } else {
            false
        }
    })
}