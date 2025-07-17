// Common test utilities for integration tests

use rust_solo_all_db::{
    engine::{LSMTree, LSMConfig},
    etl::ETLLoader,
    query::{SQLParser, QueryExecutor},
    DbResult,
};
use std::path::PathBuf;
use tempfile::TempDir;

// Helper function to create a temporary LSM tree for testing
pub fn create_test_lsm() -> (LSMTree, TempDir) {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");
    let config = LSMConfig {
        memtable_size_limit: 1000,
        data_dir: temp_dir.path().join("db"),
        background_compaction: false,
        background_compaction_interval: std::time::Duration::from_secs(10),
        enable_wal: true,
    };
    
    let lsm_tree = LSMTree::with_config(config).expect("Failed to create LSM tree");
    (lsm_tree, temp_dir)
}

// Helper function to create a test CSV file
pub fn create_test_csv(temp_dir: &TempDir, filename: &str, data: &[(&str, &str)]) -> PathBuf {
    use std::fs::File;
    use std::io::Write;
    
    let csv_path = temp_dir.path().join(filename);
    let mut file = File::create(&csv_path).expect("Failed to create CSV file");
    
    // Write header
    writeln!(file, "key,value").expect("Failed to write CSV header");
    
    // Write data
    for (key, value) in data {
        writeln!(file, "{},{}", key, value).expect("Failed to write CSV data");
    }
    
    csv_path
}

// Helper function to create a test CSV file with errors
pub fn create_test_csv_with_errors(temp_dir: &TempDir, filename: &str) -> PathBuf {
    use std::fs::File;
    use std::io::Write;
    
    let csv_path = temp_dir.path().join(filename);
    let mut file = File::create(&csv_path).expect("Failed to create CSV file");
    
    writeln!(file, "key,value").expect("Failed to write CSV header");
    writeln!(file, "key1,value1").expect("Failed to write CSV data");
    writeln!(file, "key2").expect("Failed to write malformed CSV data"); // Missing value
    writeln!(file, "key3,value3").expect("Failed to write CSV data");
    writeln!(file, "key4,\"value with, comma\"").expect("Failed to write CSV data");
    
    csv_path
}

// Helper function to measure execution time
pub fn measure_time<F, R>(f: F) -> (R, std::time::Duration)
where
    F: FnOnce() -> R,
{
    let start = std::time::Instant::now();
    let result = f();
    let duration = start.elapsed();
    (result, duration)
}

// Assert that a duration is within a acceptable bounds
pub fn assert_duration_bounds(duration: std::time::Duration, min_ms: u64, max_ms: u64) {
    let duration_ms = duration.as_millis() as u64;
    assert!(
        duration_ms >= min_ms && duration_ms <= max_ms,
        "Duration {}ms is not within bounds [{}, {}]ms",
        duration_ms, min_ms, max_ms
    );
}

// Generate test data for stress testing
pub fn generate_test_data(count: usize, prefix: &str) -> Vec<(String, String)> {
    (0..count)
        .map(|i| {
            (
                format!("{}_{:06}", prefix, i),
                format!("value_{}_{}", prefix, i),
            )
        })
        .collect()
}

// Memory usage monitoring helper
pub fn get_memory_usage() -> usize {
    // Simple approximation - in a real implementation we'd use a proper memory profiler
    // For now, we'll use a placeholder that could be extended with actual memory monitoring
    std::mem::size_of::<LSMTree>()
}