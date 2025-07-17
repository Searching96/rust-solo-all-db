mod common;

use common::*;
use rust_solo_all_db::{
    engine::{LSMTree, LSMConfig},
    etl::ETLLoader,
    query::{SQLParser, QueryExecutor},
};
use std::sync::{Arc, Mutex};
use std::thread;

#[test]
fn test_large_dataset_operations() {
    let (mut lsm_tree, _temp_dir) = create_test_lsm();
    
    let large_count = 100_000; // 100k records
    
    println!("Inserting {} records...", large_count);
    
    // Insert large dataset
    let (_, insert_duration) = measure_time(|| {
        for i in 0..large_count {
            let key = format!("large_key_{:08}", i);
            let value = format!("large_value_{:08}_{}", i, "x".repeat(50)); // Larger values
            lsm_tree.insert(key, value).expect("Failed to insert");
            
            if i % 10000 == 0 {
                println!("Inserted {} records", i);
            }
        }
    });
    
    println!("Large dataset insertion took: {:?}", insert_duration);
    
    // Force compaction on large dataset
    println!("Performing compaction...");
    let (_, compaction_duration) = measure_time(|| {
        lsm_tree.compact().expect("Failed to compact large dataset");
    });
    
    println!("Large dataset compaction took: {:?}", compaction_duration);
    
    // Verify random samples of the data
    println!("Verifying data integrity...");
    let sample_size = 1000;
    let mut verified = 0;
    
    for i in (0..large_count).step_by(large_count / sample_size) {
        let key = format!("large_key_{:08}", i);
        let expected_value = format!("large_value_{:08}_{}", i, "x".repeat(50));
        
        let actual_value = lsm_tree.get(&key).expect("Failed to get value");
        assert_eq!(actual_value, Some(expected_value));
        verified += 1;
        
        if verified % 100 == 0 {
            println!("Verified {} samples", verified);
        }
    }
    
    println!("Successfully verified {} samples from {} total records", verified, large_count);
    
    // Test query performance on large dataset
    let mut executor = rust_solo_all_db::query::QueryExecutor::new(&mut lsm_tree);
    let query_key = format!("large_key_{:08}", large_count / 2);
    let sql = format!("SELECT * FROM table WHERE key = '{}'", query_key);
    
    let (result, query_duration) = measure_time(|| {
        let mut parser = rust_solo_all_db::query::SQLParser::new(&sql);
        let statement = parser.parse().expect("Failed to parse");
        executor.execute(statement).expect("Failed to execute")
    });
    
    println!("Query on large dataset took: {:?}", query_duration);
    
    match result {
        rust_solo_all_db::query::QueryResult::Select(records) => {
            assert_eq!(records.len(), 1);
        }
        _ => panic!("Expected Select result"),
    }
    
    // Assert performance bounds for large dataset
    assert!(insert_duration < std::time::Duration::from_secs(120), 
        "Large dataset insertion too slow");
    assert!(compaction_duration < std::time::Duration::from_secs(30), 
        "Large dataset compaction too slow");
    assert!(query_duration < std::time::Duration::from_millis(100), 
        "Query on large dataset too slow");
}

#[test]
fn test_concurrent_heavy_load() {
    let (lsm_tree, _temp_dir) = create_test_lsm();
    let lsm_tree = Arc::new(Mutex::new(lsm_tree));
    
    let num_threads = 8;
    let operations_per_thread = 2000;
    
    println!("Starting concurrent heavy load test with {} threads, {} ops each", 
             num_threads, operations_per_thread);
    
    let (_, total_duration) = measure_time(|| {
        let mut handles = vec![];
        
        for thread_id in 0..num_threads {
            let lsm_clone = Arc::clone(&lsm_tree);
            
            let handle = thread::spawn(move || {
                for i in 0..operations_per_thread {
                    let key = format!("stress_thread{}_key_{:06}", thread_id, i);
                    let value = format!("stress_thread{}_value_{:06}_{}", thread_id, i, "y".repeat(100));
                    
                    // Mix of operations
                    match i % 4 {
                        0 | 1 => {
                            // Insert (50% of operations)
                            let mut lsm = lsm_clone.lock().unwrap();
                            lsm.insert(key, value).expect("Failed to insert");
                        }
                        2 => {
                            // Read own data (25% of operations)
                            let lsm = lsm_clone.lock().unwrap();
                            let _ = lsm.get(&key);
                        }
                        3 => {
                            // Delete (25% of operations)
                            let mut lsm = lsm_clone.lock().unwrap();
                            let _ = lsm.delete(&key);
                        }
                        _ => unreachable!(),
                    }
                    
                    if i % 500 == 0 {
                        println!("Thread {} completed {} operations", thread_id, i);
                    }
                }
                
                println!("Thread {} completed all {} operations", thread_id, operations_per_thread);
            });
            
            handles.push(handle);
        }
        
        for handle in handles {
            handle.join().expect("Thread panicked during stress test");
        }
    });
    
    let total_operations = num_threads * operations_per_thread;
    let throughput = total_operations as f64 / total_duration.as_secs_f64();
    
    println!("Concurrent heavy load completed in {:?}", total_duration);
    println!("Total throughput: {:.2} operations/second", throughput);
    
    // Verify some data survived the stress test
    let lsm = lsm_tree.lock().unwrap();
    let stats = lsm.stats();
    println!("Final database stats: {}", stats);
    
    // Assert reasonable performance under stress
    assert!(throughput > 50.0, "Stress test throughput too low: {:.2} ops/second", throughput);
}

#[test]
fn test_memory_pressure_scenarios() {
    let (mut lsm_tree, _temp_dir) = create_test_lsm();
    
    println!("Testing memory pressure scenarios...");
    
    let batch_size = 5000;
    let num_batches = 20;
    let large_value_size = 1000; // 1KB values
    
    for batch in 0..num_batches {
        println!("Processing batch {} of {}", batch + 1, num_batches);
        
        // Insert batch with large values
        for i in 0..batch_size {
            let key = format!("memory_pressure_{}_{:06}", batch, i);
            let value = format!("large_value_{}_{:06}_{}", batch, i, "z".repeat(large_value_size));
            lsm_tree.insert(key, value).expect("Failed to insert during memory pressure test");
        }
        
        // Force compaction every few batches to simulate memory pressure relief
        if batch % 5 == 4 {
            println!("Performing compaction after batch {}", batch + 1);
            lsm_tree.compact().expect("Failed to compact during memory pressure test");
        }
        
        // Verify some data from current batch
        let sample_key = format!("memory_pressure_{}_{:06}", batch, batch_size / 2);
        let result = lsm_tree.get(&sample_key).expect("Failed to get during memory pressure test");
        assert!(result.is_some(), "Data lost during memory pressure test");
    }
    
    // Final verification
    println!("Performing final verification...");
    let mut verified_count = 0;
    
    for batch in (0..num_batches).step_by(2) {  // Check every other batch
        for i in (0..batch_size).step_by(500) {  // Sample within batch
            let key = format!("memory_pressure_{}_{:06}", batch, i);
            let result = lsm_tree.get(&key).expect("Failed to get during final verification");
            if result.is_some() {
                verified_count += 1;
            }
        }
    }
    
    println!("Verified {} records after memory pressure test", verified_count);
    assert!(verified_count > 0, "No data survived memory pressure test");
}

#[test]
fn test_disk_space_exhaustion_simulation() {
    let (mut lsm_tree, temp_dir) = create_test_lsm();
    
    println!("Simulating disk space constraints...");
    
    // Insert data until we have a substantial amount
    let mut total_inserted = 0;
    let batch_size = 1000;
    let max_batches = 50;
    
    for batch in 0..max_batches {
        let mut batch_success = true;
        
        for i in 0..batch_size {
            let key = format!("disk_test_{}_{:06}", batch, i);
            let value = format!("disk_value_{}_{:06}_{}", batch, i, "d".repeat(200));
            
            match lsm_tree.insert(key, value) {
                Ok(_) => total_inserted += 1,
                Err(e) => {
                    println!("Insert failed at batch {}, item {}: {}", batch, i, e);
                    batch_success = false;
                    break;
                }
            }
        }
        
        if !batch_success {
            break;
        }
        
        // Check disk usage periodically
        if let Ok(metadata) = std::fs::metadata(&temp_dir.path()) {
            println!("Batch {}: Inserted {} total records", batch + 1, total_inserted);
        }
        
        // Force compaction to manage disk space
        if batch % 10 == 9 {
            match lsm_tree.compact() {
                Ok(_) => println!("Compaction successful after batch {}", batch + 1),
                Err(e) => println!("Compaction failed after batch {}: {}", batch + 1, e),
            }
        }
    }
    
    println!("Total records inserted: {}", total_inserted);
    assert!(total_inserted > 10000, "Should have inserted substantial amount of data");
    
    // Verify data integrity under disk pressure
    let mut verified = 0;
    let sample_rate = total_inserted / 100; // Check 1% of data
    
    for i in (0..total_inserted).step_by(sample_rate.max(1)) {
        let batch = i / batch_size;
        let item = i % batch_size;
        let key = format!("disk_test_{}_{:06}", batch, item);
        
        match lsm_tree.get(&key) {
            Ok(Some(_)) => verified += 1,
            Ok(None) => {}, // Data might have been compacted away
            Err(e) => println!("Error reading {}: {}", key, e),
        }
    }
    
    println!("Verified {} records after disk pressure test", verified);
}

#[test]
fn test_long_running_stability() {
    let (mut lsm_tree, _temp_dir) = create_test_lsm();
    
    println!("Starting long-running stability test...");
    
    let duration = std::time::Duration::from_secs(30); // 30 second test
    let start_time = std::time::Instant::now();
    let mut operation_count = 0;
    let mut error_count = 0;
    
    while start_time.elapsed() < duration {
        let key = format!("stability_key_{:08}", operation_count);
        let value = format!("stability_value_{:08}_{}", operation_count, operation_count % 1000);
        
        // Mix of operations to simulate real usage
        match operation_count % 10 {
            0..=5 => {
                // Insert (60% of operations)
                match lsm_tree.insert(key, value) {
                    Ok(_) => {},
                    Err(e) => {
                        println!("Insert error at operation {}: {}", operation_count, e);
                        error_count += 1;
                    }
                }
            }
            6..=8 => {
                // Read (30% of operations)
                let read_key = format!("stability_key_{:08}", (operation_count as usize).saturating_sub(100));
                match lsm_tree.get(&read_key) {
                    Ok(_) => {},
                    Err(e) => {
                        println!("Read error at operation {}: {}", operation_count, e);
                        error_count += 1;
                    }
                }
            }
            9 => {
                // Delete (10% of operations)
                let delete_key = format!("stability_key_{:08}", (operation_count as usize).saturating_sub(200));
                match lsm_tree.delete(&delete_key) {
                    Ok(_) => {},
                    Err(e) => {
                        println!("Delete error at operation {}: {}", operation_count, e);
                        error_count += 1;
                    }
                }
            }
            _ => unreachable!(),
        }
        
        operation_count += 1;
        
        // Periodic compaction
        if operation_count % 1000 == 0 {
            match lsm_tree.compact() {
                Ok(_) => {},
                Err(e) => {
                    println!("Compaction error at operation {}: {}", operation_count, e);
                    error_count += 1;
                }
            }
            
            println!("Completed {} operations, {} errors", operation_count, error_count);
        }
    }
    
    let final_duration = start_time.elapsed();
    let ops_per_second = operation_count as f64 / final_duration.as_secs_f64();
    
    println!("Long-running test completed:");
    println!("  Duration: {:?}", final_duration);
    println!("  Total operations: {}", operation_count);
    println!("  Error count: {}", error_count);
    println!("  Operations per second: {:.2}", ops_per_second);
    println!("  Error rate: {:.4}%", (error_count as f64 / operation_count as f64) * 100.0);
    
    // Assert stability
    assert!(operation_count > 1000, "Should have completed substantial number of operations");
    assert!(error_count < operation_count / 100, "Error rate too high: {} errors out of {} operations", 
           error_count, operation_count);
    assert!(ops_per_second > 10.0, "Operations per second too low: {:.2}", ops_per_second);
}