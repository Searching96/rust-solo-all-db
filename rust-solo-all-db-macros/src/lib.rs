use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Data, Fields};

/// Derive macro that generates query builder methods for structs
#[proc_macro_derive(QueryBuilder, attributes(primary_key, indexed))]
pub fn derive_query_builder(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => panic!("QueryBuilder can only be derived for structs with named fields"),
        },
        _ => panic!("QueryBuilder can only be derived for structs"),
    };

    let primary_key_field = fields.iter().find(|field| {
        field.attrs.iter().any(|attr| attr.path().is_ident("primary_key"))
    });

    let primary_key_name = if let Some(field) = primary_key_field {
        &field.ident
    } else {
        panic!("QueryBuilder requires a field marked with #[primary_key]")
    };

    let expanded = quote! {
        impl #name {
            pub fn save(&self, db: &mut rust_solo_all_db::engine::LSMTree) -> rust_solo_all_db::DbResult<()> {
                let value = serde_json::to_string(self)
                    .map_err(|e| rust_solo_all_db::DbError::InvalidOperation(e.to_string()))?;
                db.insert(self.#primary_key_name.clone(), value)
            }

            pub fn find_by_id(db: &rust_solo_all_db::engine::LSMTree, id: &str) -> rust_solo_all_db::DbResult<Option<#name>> {
                match db.get(id)? {
                    Some(value) => {
                        let item: #name = serde_json::from_str(&value)
                            .map_err(|e| rust_solo_all_db::DbError::InvalidOperation(e.to_string()))?;
                        Ok(Some(item))
                    }
                    None => Ok(None),
                }
            }

            pub fn delete_by_id(db: &mut rust_solo_all_db::engine::LSMTree, id: &str) -> rust_solo_all_db::DbResult<bool> {
                match db.delete(id) {
                    Ok(_) => Ok(true),
                    Err(rust_solo_all_db::DbError::KeyNotFound(_)) => Ok(false),
                    Err(e) => Err(e),
                }
            }
        }
    };

    TokenStream::from(expanded)
}

/// Simple table definition macro
#[proc_macro]
pub fn table(input: TokenStream) -> TokenStream {
    // For now, just generate a basic struct
    // In a real implementation, this would parse the table definition syntax
    quote! {
        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        pub struct GeneratedTable {
            pub id: String,
        }
    }.into()
}

/// Simple migration macro
#[proc_macro]
pub fn migration(input: TokenStream) -> TokenStream {
    // For now, just generate a basic migration struct
    quote! {
        pub struct GeneratedMigration;
        
        impl GeneratedMigration {
            pub fn up(_db: &mut rust_solo_all_db::engine::LSMTree) -> rust_solo_all_db::DbResult<()> {
                println!("Running migration");
                Ok(())
            }
            
            pub fn down(_db: &mut rust_solo_all_db::engine::LSMTree) -> rust_solo_all_db::DbResult<()> {
                println!("Rolling back migration");
                Ok(())
            }
        }
    }.into()
}

/// Simple database configuration macro
#[proc_macro]
pub fn database(input: TokenStream) -> TokenStream {
    quote! {
        {
            let config = rust_solo_all_db::engine::LSMConfig {
                memtable_size_limit: 1000,
                data_dir: std::path::PathBuf::from("data/macro_example"),
                background_compaction: false,
                background_compaction_interval: std::time::Duration::from_secs(1),
                enable_wal: true,
            };
            rust_solo_all_db::engine::LSMTree::with_config(config)
        }
    }.into()
}

/// Simple query macro
#[proc_macro]
pub fn query(input: TokenStream) -> TokenStream {
    quote! {
        {
            // Simple query placeholder - in real implementation would parse SQL
            Ok("Query result".to_string())
        }
    }.into()
}
