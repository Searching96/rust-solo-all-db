// Example usage of the procedural macros

use rust_solo_all_db::macros::*;
use rust_solo_all_db::engine::LSMTree;
use serde::{Serialize, Deserialize};

// Example 1: Query Builder Derive Macro
#[derive(Debug, Clone, Serialize, Deserialize, QueryBuilder)]
struct User {
    #[primary_key]
    id: String,
    #[indexed]
    name: String,
    #[indexed]
    email: String,
    created_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, QueryBuilder)]
struct Product {
    #[primary_key]
    sku: String,
    name: String,
    #[indexed]
    category: String,
    price: f64,
}

// Example 2: Table Definition Macro
table! {
    orders {
        id: String (primary_key),
        user_id: String (indexed),
        product_sku: String (indexed),
        quantity: i32,
        total: f64,
    }
}

// Example 3: Migration Macro
migration! {
    name: "create_users_table",
    up: "CREATE TABLE users (id STRING PRIMARY KEY, name STRING, email STRING)",
    down: "DROP TABLE users"
}

migration! {
    name: "add_products_table", 
    up: "CREATE TABLE products (sku STRING PRIMARY KEY, name STRING, category STRING, price FLOAT)",
    down: "DROP TABLE products"
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Demonstrating Procedural Macros");

    // Example 4: Database Configuration Macro
    let mut db = database! {
        path: "data/macro_example",
        memtable_size: 1000,
        enable_wal: true
    }?;

    // Example 5: Using Generated Query Builder Methods
    let user = User::new(
        "user001".to_string(),
        "Alice Smith".to_string(),
        "alice@example.com".to_string(),
        1642838400, // Unix timestamp
    );

    println!("💾 Saving user: {:?}", user);
    user.save(&mut db)?;

    // Find user by ID (generated method)
    if let Some(found_user) = User::find_by_id(&db, "user001")? {
        println!("🔍 Found user by ID: {:?}", found_user);
    }

    // Find users by name (generated method) 
    let users_by_name = User::find_by_name(&db, "Alice Smith")?;
    println!("🔍 Found {} users named 'Alice Smith'", users_by_name.len());

    // Example 6: Using Query DSL Macro
    let user_id = "user001";
    let results = query!(db, "SELECT * FROM users WHERE key = {user_id}")?;
    println!("📊 Query results: {:?}", results);

    // Example 7: Using Product with Generated Methods
    let product = Product::new(
        "LAPTOP001".to_string(),
        "Gaming Laptop".to_string(),
        "Electronics".to_string(),
        1299.99,
    );

    product.save(&mut db)?;
    println!("💾 Saved product: {:?}", product);

    // Example 8: Running Migrations
    println!("🔧 Running migrations...");
    MigrationCreateUsersTable::up(&mut db)?;
    MigrationAddProductsTable::up(&mut db)?;

    println!("✅ All macro examples completed successfully!");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_query_builder_derive() {
        let temp_dir = tempdir().unwrap();
        let config = rust_solo_all_db::engine::LSMConfig {
            memtable_size_limit: 100,
            data_dir: temp_dir.path().join("db"),
            background_compaction: false,
            background_compaction_interval: std::time::Duration::from_secs(1),
            enable_wal: false,
        };
        
        let mut db = LSMTree::with_config(config).unwrap();
        
        // Test generated methods
        let user = User::new(
            "test001".to_string(),
            "Test User".to_string(),
            "test@example.com".to_string(),
            1642838400,
        );

        // Test save
        user.save(&mut db).unwrap();

        // Test find_by_id
        let found = User::find_by_id(&db, "test001").unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().name, "Test User");

        // Test delete
        let deleted = User::delete_by_id(&mut db, "test001").unwrap();
        assert!(deleted);

        // Verify deletion
        let not_found = User::find_by_id(&db, "test001").unwrap();
        assert!(not_found.is_none());
    }

    #[test]
    fn test_database_macro() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().to_str().unwrap();
        
        let db_result = database! {
            path: path,
            memtable_size: 500,
            enable_wal: false
        };
        
        assert!(db_result.is_ok());
    }

    #[test]
    fn test_migration_generation() {
        // Test that migrations can be created and have the right interface
        assert_eq!(MigrationCreateUsersTable::name(), "create_users_table");
        assert_eq!(MigrationAddProductsTable::name(), "add_products_table");
    }

    #[test] 
    fn test_table_definition() {
        // Test that the Orders struct was generated correctly
        let order = Orders {
            id: "order001".to_string(),
            user_id: "user001".to_string(),
            product_sku: "LAPTOP001".to_string(),
            quantity: 2,
            total: 2599.98,
        };

        assert_eq!(order.id, "order001");
        assert_eq!(order.quantity, 2);
    }
}