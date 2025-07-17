mod common;

use common::*;
use rust_solo_all_db::{
    engine::{LSMTree, LSMConfig},
    etl::ETLLoader,
    query::{SQLParser, QueryExecutor},
};
use std::time::Duration;

#[test]
fn test_insert_throughput() {
    let (mut lsm_tree, _temp_dir) = create_test_lsm();
    
    let test_count = 10000;
    
    let (_, duration) = measure_time(|| {
        for i in 0..test_count {
            let key = format!("perf_key_{:06}", i);
            let value = format!("perf_value_{:06}", i);
            lsm_tree.insert(key, value).expect("Failed to insert");
        }
    });
    
    let throughput = test_count as f64 / duration.as_secs_f64();
    println!("Insert throughput: {:.2} records/second", throughput);
    
    // Assert reasonable performance (should be at least 1000 inserts/second)
    assert!(throughput > 1000.0, "Insert throughput too low: {:.2} records/second", throughput);
    
    // Verify a sample of the data
    for i in (0..test_count).step_by(1000) {
        let key = format!("perf_key_{:06}", i);
        let expected_value = format!("perf_value_{:06}", i);
        let actual_value = lsm_tree.get(&key).expect("Failed to get value");
        assert_eq!(actual_value, Some(expected_value));
    }
}

#[test]
fn test_query_latency() {
    let (mut lsm_tree, _temp_dir) = create_test_lsm();
    
    // Insert test data
    for i in 0..1000 {
        let key = format!("latency_key_{:04}", i);
        let value = format!("latency_value_{:04}", i);
        lsm_tree.insert(key, value).expect("Failed to insert");
    }
    
    let mut executor = QueryExecutor::new(&mut lsm_tree);
    
    // Measure query latency
    let mut total_duration = Duration::new(0, 0);
    let query_count = 100;
    
    for i in 0..query_count {
        let key = format!("latency_key_{:04}", i % 1000);
        let sql = format!("SELECT * FROM table WHERE key = '{}'", key);
        
        let (result, duration) = measure_time(|| {
            let mut parser = SQLParser::new(&sql);
            let statement = parser.parse().expect("Failed to parse");
            executor.execute(statement).expect("Failed to execute")
        });
        
        total_duration += duration;
        
        // Verify result
        match result {
            rust_solo_all_db::query::QueryResult::Select(records) => {
                assert_eq!(records.len(), 1);
            }
            _ => panic!("Expected Select result"),
        }
    }
    
    let avg_latency = total_duration / query_count;
    println!("Average query latency: {:?}", avg_latency);
    
    // Assert reasonable latency (should be under 10ms per query)
    assert!(avg_latency < Duration::from_millis(10), 
        "Query latency too high: {:?}", avg_latency);
}

#[test]
fn test_compaction_performance() {
    let (mut lsm_tree, _temp_dir) = create_test_lsm();
    
    // Insert enough data to require compaction
    let insert_count = 5000;
    for i in 0..insert_count {
        let key = format!("compact_key_{:06}", i);
        let value = format!("compact_value_{:06}", i);
        lsm_tree.insert(key, value).expect("Failed to insert");
    }
    
    // Measure compaction time
    let (_, compaction_duration) = measure_time(|| {
        lsm_tree.compact().expect("Failed to compact");
    });
    
    println!("Compaction duration: {:?}", compaction_duration);
    
    // Assert reasonable compaction time (should be under 5 seconds for 5k records)
    assert!(compaction_duration < Duration::from_secs(5),
        "Compaction too slow: {:?}", compaction_duration);
    
    // Verify data integrity after compaction
    let sample_size = 100;
    for i in (0..insert_count).step_by(insert_count / sample_size) {
        let key = format!("compact_key_{:06}", i);
        let expected_value = format!("compact_value_{:06}", i);
        let actual_value = lsm_tree.get(&key).expect("Failed to get value");
        assert_eq!(actual_value, Some(expected_value));
    }
}

#[test]
fn test_memory_usage_under_load() {
    let (mut lsm_tree, _temp_dir) = create_test_lsm();
    
    let initial_memory = get_memory_usage();
    
    // Insert data in batches and monitor memory
    let batch_size = 1000;
    let num_batches = 10;
    
    for batch in 0..num_batches {
        // Insert batch
        for i in 0..batch_size {
            let key = format!("mem_key_{}_{:04}", batch, i);
            let value = format!("mem_value_{}_{:04}", batch, i);
            lsm_tree.insert(key, value).expect("Failed to insert");
        }
        
        let current_memory = get_memory_usage();
        println!("Batch {}: Memory usage: {} bytes", batch, current_memory);
        
        // Memory should grow reasonably (not exponentially)
        assert!(current_memory < initial_memory + (batch + 1) * batch_size * 1000,
            "Memory usage growing too fast");
    }
    
    // Force compaction and check memory
    lsm_tree.compact().expect("Failed to compact");
    let post_compaction_memory = get_memory_usage();
    println!("Post-compaction memory: {} bytes", post_compaction_memory);
    
    // Verify data integrity
    for batch in 0..num_batches {
        for i in (0..batch_size).step_by(100) {  // Sample every 100th record
            let key = format!("mem_key_{}_{:04}", batch, i);
            let expected_value = format!("mem_value_{}_{:04}", batch, i);
            let actual_value = lsm_tree.get(&key).expect("Failed to get value");
            assert_eq!(actual_value, Some(expected_value));
        }
    }
}

#[test]
fn test_csv_loading_performance() {
    let (mut lsm_tree, temp_dir) = create_test_lsm();
    
    // Generate large CSV test data
    let record_count = 5000;
    let test_data: Vec<(String, String)> = (0..record_count)
        .map(|i| (format!("csv_key_{:06}", i), format!("csv_value_{:06}", i)))
        .collect();
    
    // Convert to &str tuples for create_test_csv
    let test_data_refs: Vec<(&str, &str)> = test_data.iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    
    let csv_path = create_test_csv(&temp_dir, "large_test.csv", &test_data_refs);
    
    // Measure CSV loading performance
    let loader = ETLLoader::new();
    let (count, load_duration) = measure_time(|| {
        loader.load_csv(&csv_path, &mut lsm_tree, 0, 1)
            .expect("Failed to load CSV")
    });
    
    assert_eq!(count, record_count);
    
    let load_throughput = record_count as f64 / load_duration.as_secs_f64();
    println!("CSV loading throughput: {:.2} records/second", load_throughput);
    
    // Assert reasonable CSV loading performance
    assert!(load_throughput > 500.0, 
        "CSV loading too slow: {:.2} records/second", load_throughput);
    
    // Verify data was loaded correctly
    let sample_size = 100;
    for i in (0..record_count).step_by(record_count / sample_size) {
        let key = format!("csv_key_{:06}", i);
        let expected_value = format!("csv_value_{:06}", i);
        let actual_value = lsm_tree.get(&key).expect("Failed to get value");
        assert_eq!(actual_value, Some(expected_value));
    }
}

#[test] 
fn test_concurrent_read_write_performance() {
    use std::sync::{Arc, Mutex};
    use std::thread;
    
    let (lsm_tree, _temp_dir) = create_test_lsm();
    let lsm_tree = Arc::new(Mutex::new(lsm_tree));
    
    // Pre-populate with data
    {
        let mut lsm = lsm_tree.lock().unwrap();
        for i in 0..1000 {
            let key = format!("concurrent_key_{:04}", i);
            let value = format!("concurrent_value_{:04}", i);
            lsm.insert(key, value).expect("Failed to insert");
        }
    }
    
    let num_threads = 4;
    let operations_per_thread = 500;
    
    let (_, total_duration) = measure_time(|| {
        let mut handles = vec![];
        
        for thread_id in 0..num_threads {
            let lsm_clone = Arc::clone(&lsm_tree);
            
            let handle = thread::spawn(move || {
                for i in 0..operations_per_thread {
                    if i % 2 == 0 {
                        // Write operation
                        let key = format!("thread{}_key_{:04}", thread_id, i);
                        let value = format!("thread{}_value_{:04}", thread_id, i);
                        let mut lsm = lsm_clone.lock().unwrap();
                        lsm.insert(key, value).expect("Failed to insert");
                    } else {
                        // Read operation
                        let key = format!("concurrent_key_{:04}", i % 1000);
                        let lsm = lsm_clone.lock().unwrap();
                        let _ = lsm.get(&key).expect("Failed to get");
                    }
                }
            });
            
            handles.push(handle);
        }
        
        for handle in handles {
            handle.join().expect("Thread panicked");
        }
    });
    
    let total_operations = num_threads * operations_per_thread;
    let throughput = total_operations as f64 / total_duration.as_secs_f64();
    
    println!("Concurrent operations throughput: {:.2} ops/second", throughput);
    
    // Assert reasonable concurrent performance
    assert!(throughput > 100.0, 
        "Concurrent throughput too low: {:.2} ops/second", throughput);
}