mod common;

use common::*;
use rust_solo_all_db::{
    engine::{LSMTree, LSMConfig},
    etl::ETLLoader,
    query::{SQLParser, QueryExecutor, QueryResult},
};

#[test]
fn test_csv_load_query_verify_results() {
    let (mut lsm_tree, temp_dir) = create_test_lsm();
    
    // Test data
    let test_data = vec![
        ("user1", "Alice"),
        ("user2", "Bob"), 
        ("user3", "Charlie"),
        ("user4", "Diana"),
    ];
    
    // Create CSV file
    let csv_path = create_test_csv(&temp_dir, "users.csv", &test_data);
    
    // Load CSV data
    let loader = ETLLoader::new();
    let count = loader.load_csv(&csv_path, &mut lsm_tree, 0, 1)
        .expect("Failed to load CSV");
    
    assert_eq!(count, 4);
    
    // Query the data using SQL
    let mut executor = QueryExecutor::new(&mut lsm_tree);
    
    // Test SELECT query
    let mut parser = SQLParser::new("SELECT * FROM table WHERE key = 'user1'");
    let statement = parser.parse().expect("Failed to parse SELECT");
    let result = executor.execute(statement).expect("Failed to execute SELECT");
    
    match result {
        QueryResult::Select(records) => {
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].get("key").unwrap(), "user1");
            assert_eq!(records[0].get("value").unwrap(), "Alice");
        }
        _ => panic!("Expected Select result"),
    }
    
    // Test INSERT query
    let mut parser = SQLParser::new("INSERT INTO table (key, value) VALUES ('user5', 'Eve')");
    let statement = parser.parse().expect("Failed to parse INSERT");
    let result = executor.execute(statement).expect("Failed to execute INSERT");
    
    match result {
        QueryResult::Insert(count) => assert_eq!(count, 1),
        _ => panic!("Expected Insert result"),
    }
    
    // Drop executor to release borrow
    drop(executor);
    
    // Verify INSERT worked
    assert_eq!(lsm_tree.get("user5").unwrap(), Some("Eve".to_string()));
    
    // Create new executor for DELETE
    let mut executor = QueryExecutor::new(&mut lsm_tree);
    let mut parser = SQLParser::new("DELETE FROM table WHERE key = 'user2'");
    let statement = parser.parse().expect("Failed to parse DELETE");
    let result = executor.execute(statement).expect("Failed to execute DELETE");
    
    match result {
        QueryResult::Delete(count) => assert_eq!(count, 1),
        _ => panic!("Expected Delete result"),
    }
    
    // Drop executor to release borrow
    drop(executor);
    
    // Verify DELETE worked
    assert_eq!(lsm_tree.get("user2").unwrap(), None);
}

#[test]
fn test_insert_compact_query_verify() {
    let (mut lsm_tree, _temp_dir) = create_test_lsm();
    
    // Insert enough data to trigger compaction
    for i in 0..1500 {
        let key = format!("key{:04}", i);
        let value = format!("value{:04}", i);
        lsm_tree.insert(key, value).expect("Failed to insert");
    }
    
    // Force compaction
    lsm_tree.compact().expect("Failed to compact");
    
    // Verify data is still accessible after compaction
    for i in 0..1500 {
        let key = format!("key{:04}", i);
        let expected_value = format!("value{:04}", i);
        let actual_value = lsm_tree.get(&key).expect("Failed to get value");
        assert_eq!(actual_value, Some(expected_value));
    }
    
    // Test query after compaction
    let mut executor = QueryExecutor::new(&mut lsm_tree);
    let mut parser = SQLParser::new("SELECT * FROM table WHERE key = 'key0500'");
    let statement = parser.parse().expect("Failed to parse");
    let result = executor.execute(statement).expect("Failed to execute");
    
    match result {
        QueryResult::Select(records) => {
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].get("value").unwrap(), "value0500");
        }
        _ => panic!("Expected Select result"),
    }
}

#[test]
fn test_wal_recovery_query_verify() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");
    let data_dir = temp_dir.path().join("db");
    
    // Create LSM tree with WAL enabled
    let config = LSMConfig {
        memtable_size_limit: 100,
        data_dir: data_dir.clone(),
        background_compaction: false,
        background_compaction_interval: std::time::Duration::from_secs(10),
        enable_wal: true,
    };
    
    // Insert data and close database
    {
        let mut lsm_tree = LSMTree::with_config(config.clone()).expect("Failed to create LSM tree");
        
        for i in 0..50 {
            let key = format!("recovery_key{}", i);
            let value = format!("recovery_value{}", i);
            lsm_tree.insert(key, value).expect("Failed to insert");
        }
        
        // Don't call flush - simulate crash
    } // LSM tree drops here, simulating crash
    
    // Create new LSM tree with same data directory (should recover from WAL)
    let mut recovered_lsm = LSMTree::with_config(config).expect("Failed to recover LSM tree");
    
    // Verify data was recovered
    for i in 0..50 {
        let key = format!("recovery_key{}", i);
        let expected_value = format!("recovery_value{}", i);
        let actual_value = recovered_lsm.get(&key).expect("Failed to get recovered value");
        assert_eq!(actual_value, Some(expected_value));
    }
    
    // Test query on recovered data
    let mut executor = QueryExecutor::new(&mut recovered_lsm);
    let mut parser = SQLParser::new("SELECT * FROM table WHERE key = 'recovery_key25'");
    let statement = parser.parse().expect("Failed to parse");
    let result = executor.execute(statement).expect("Failed to execute");
    
    match result {
        QueryResult::Select(records) => {
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].get("value").unwrap(), "recovery_value25");
        }
        _ => panic!("Expected Select result"),
    }
}

#[test]
fn test_concurrent_operations_verify_consistency() {
    use std::sync::{Arc, Mutex};
    use std::thread;
    
    let (lsm_tree, _temp_dir) = create_test_lsm();
    let lsm_tree = Arc::new(Mutex::new(lsm_tree));
    
    let mut handles = vec![];
    
    // Spawn multiple threads for concurrent operations
    for thread_id in 0..4 {
        let lsm_clone = Arc::clone(&lsm_tree);
        
        let handle = thread::spawn(move || {
            for i in 0..100 {
                let key = format!("thread{}_key{}", thread_id, i);
                let value = format!("thread{}_value{}", thread_id, i);
                
                // Insert
                {
                    let mut lsm = lsm_clone.lock().unwrap();
                    lsm.insert(key.clone(), value.clone()).expect("Failed to insert");
                }
                
                // Read back immediately
                {
                    let lsm = lsm_clone.lock().unwrap();
                    let result = lsm.get(&key).expect("Failed to get");
                    assert_eq!(result, Some(value));
                }
            }
        });
        
        handles.push(handle);
    }
    
    // Wait for all threads to complete
    for handle in handles {
        handle.join().expect("Thread panicked");
    }
    
    // Verify all data is accessible
    let lsm = lsm_tree.lock().unwrap();
    for thread_id in 0..4 {
        for i in 0..100 {
            let key = format!("thread{}_key{}", thread_id, i);
            let expected_value = format!("thread{}_value{}", thread_id, i);
            let actual_value = lsm.get(&key).expect("Failed to get value");
            assert_eq!(actual_value, Some(expected_value));
        }
    }
}

#[test]
fn test_error_recovery_csv_loading() {
    let (mut lsm_tree, temp_dir) = create_test_lsm();
    
    // Create CSV with some malformed rows
    let csv_path = create_test_csv_with_errors(&temp_dir, "errors.csv");
    
    // Load with recovery mode
    let loader = ETLLoader::new().with_recovery_mode(true);
    let result = loader.load_csv_with_recovery(&csv_path, &mut lsm_tree, 0, 1, true)
        .expect("Failed to load CSV with recovery");
    
    // Should have 3 successful inserts and 1 error
    assert_eq!(result.successful_inserts, 3);
    assert_eq!(result.errors.len(), 1);
    assert!(result.success_rate() > 0.7);
    
    // Verify successful records are queryable
    let mut executor = QueryExecutor::new(&mut lsm_tree);
    let mut parser = SQLParser::new("SELECT * FROM table WHERE key = 'key1'");
    let statement = parser.parse().expect("Failed to parse");
    let result = executor.execute(statement).expect("Failed to execute");
    
    match result {
        QueryResult::Select(records) => {
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].get("value").unwrap(), "value1");
        }
        _ => panic!("Expected Select result"),
    }
}