use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, LitStr};

mod derive_query;
mod query_dsl;

use derive_query::*;
use query_dsl::*;

/// Derive macro that generates query builder methods for structs
/// 
/// # Example
/// 
/// ```rust
/// #[derive(QueryBuilder)]
/// struct User {
///     id: String,
///     name: String,
///     email: String,
/// }
/// 
/// // Generated methods:
/// impl User {
///     fn find_by_id(db: &LSMTree, id: &str) -> DbResult<Option<User>> { ... }
///     fn find_by_name(db: &LSMTree, name: &str) -> DbResult<Vec<User>> { ... }
///     fn save(&self, db: &mut LSMTree) -> DbResult<()> { ... }
///     fn delete_by_id(db: &mut LSMTree, id: &str) -> DbResult<bool> { ... }
/// }
/// ```

#[proc_macro_derive(QueryBuilder, attributes(primary_key, indexed))]
pub fn derive_query_builder(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    match generate_query_builder_impl(&input) {
        Ok(expanded) => expanded.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Procedural macro for compile-time SQL validation and code generation
/// 
/// # Example
/// 
/// ```rust
/// let user_id = "user123";
/// let results = query!(db, "SELECT value FROM users WHERE key = {user_id}");
/// 
/// // Expands to type-safe, validated SQL execution
/// ```
#[proc_macro]
pub fn query(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as QueryMacroInput);

    match generate_query_macro(&input) {
        Ok(expanded) => expanded.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Macro for creating type-safe table definitions
/// 
/// # Example
/// 
/// ```rust
/// table! {
///     users {
///         id: String (primary_key),
///         name: String,
///         email: String (indexed),
///         created_at: u64,
///     }
/// }
/// ```
#[proc_macro]
pub fn table(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as TableDefinition);
    
    match generate_table_definition(&input) {
        Ok(expanded) => expanded.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Macro for creating migrations
/// 
/// # Example
/// 
/// ```rust
/// migration! {
///     name: "create_users_table",
///     up: "CREATE TABLE users (id STRING PRIMARY KEY, name STRING, email STRING)",
///     down: "DROP TABLE users"
/// }
/// ```
#[proc_macro]
pub fn migration(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as MigrationDefinition);
    
    match generate_migration(&input) {
        Ok(expanded) => expanded.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Helper macro for creating database connections with configuration
/// 
/// # Example
/// 
/// ```rust
/// let db = database! {
///     path: "data/mydb",
///     memtable_size: 1000,
///     enable_wal: true
/// };
/// ```
#[proc_macro]
pub fn database(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DatabaseConfig);
    
    match generate_database_config(&input) {
        Ok(expanded) => expanded.into(),
        Err(err) => err.to_compile_error().into(),
    }
}