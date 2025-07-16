use crate::{DbResult, DbError, Value};
use crate::engine::LSMTree;
use crate::etl::csv_parser::CSVParser;
use rayon::prelude::*;
use std::path::Path;
use std::fs::File;
use std::collections::BTreeMap;
use std::sync::Arc;
use parking_lot::Mutex;

#[derive(Debug, Clone)]
pub struct ETLError {
    pub row_number: usize,
    pub error: String,
}

#[derive(Debug, Clone)]
pub struct ETLResult {
    pub total_rows: usize,
    pub successful_inserts: usize,
    pub errors: Vec<ETLError>,
}

impl ETLResult {
    pub fn success_rate(&self) -> f64 {
        if self.total_rows == 0 {
            return 1.0;
        }
        self.successful_inserts as f64 / self.total_rows as f64
    }
}

pub struct ETLLoader {
    batch_size: usize,
    parallel_threads: usize,
    recovery_mode: bool,
}

impl ETLLoader {
    pub fn new() -> Self {
        Self {
            batch_size: 1000,
            parallel_threads: 4, // Default to 4 threads
            recovery_mode: false,
        }
    }

    pub fn with_config(batch_size: usize, parallel_threads: usize) -> Self {
        Self {
            batch_size,
            parallel_threads,
            recovery_mode: false,
        }
    }

    pub fn with_recovery_mode(mut self, recovery_mode: bool) -> Self {
        self.recovery_mode = recovery_mode;
        self
    }

    pub fn load_csv<P: AsRef<Path>>(
        &self,
        file_path: P,
        lsm_tree: &mut LSMTree,
        key_column: usize,
        value_column: usize,
    ) -> DbResult<usize> {
        self.load_csv_with_options(file_path, lsm_tree, key_column, value_column, true)
    }

    pub fn load_csv_with_options<P: AsRef<Path>>(
        &self,
        file_path: P,
        lsm_tree: &mut LSMTree,
        key_column: usize,
        value_column: usize,
        has_headers: bool,
    ) -> DbResult<usize> {
        let file = File::open(file_path).map_err(|e| {
            DbError::InvalidOperation(format!("Failed to open CSV file: {}", e))
        })?;

        let parser = CSVParser::new(key_column, value_column)
            .with_headers(has_headers);
        let records = parser.parse_records(file)?;

        println!("Loaded {} records from CSV, starting parallel insertion...", records.len());
        
        if records.is_empty() {
            println!("No records to insert!");
            return Ok(0);
        }

        // Process records in parallel batches
        let total_inserted = Arc::new(Mutex::new(0));
        let lsm_tree = Arc::new(Mutex::new(lsm_tree));

        records
            .chunks(self.batch_size)
            .enumerate()
            .collect::<Vec<_>>()
            .into_par_iter()
            .for_each(|(batch_idx, chunk)| {
                let mut batch_data = BTreeMap::new();

                // Prepare batch
                for (key, value) in chunk {
                    batch_data.insert(key.clone(), value.clone());
                }

                // Insert batch into LSM tree
                let mut lsm = lsm_tree.lock();
                let mut inserted_count = 0;

                for (key, value) in batch_data {
                    if let Value::Data(data) = value {
                        match lsm.insert(key, data) {
                            Ok(_) => inserted_count += 1,
                            Err(e) => eprintln!("Error inserting records: {}", e),
                        }
                    }
                }

                // Update total count
                let mut total = total_inserted.lock();
                *total += inserted_count;

                println!("Batch {} completed: {} records inserted", batch_idx + 1, inserted_count);
            });

        let final_count = *total_inserted.lock();
        println!("ETL load complete: {} records inserted into LSM tree", final_count);

        Ok(final_count)
    }

    pub fn load_csv_with_recovery<P: AsRef<Path>>(
        &self,
        file_path: P,
        lsm_tree: &mut LSMTree,
        key_column: usize,
        value_column: usize,
        has_headers: bool,
    ) -> DbResult<ETLResult> {
        let file = File::open(file_path).map_err(|e| {
            DbError::InvalidOperation(format!("Failed to open CSV file: {}", e))
        })?;

        let _parser = CSVParser::new(key_column, value_column)
            .with_headers(has_headers);
        
        // Use CSV reader directly for error recovery
        let mut csv_reader = csv::ReaderBuilder::new()
            .delimiter(b',')
            .has_headers(has_headers)
            .from_reader(file);

        let mut successful_records = Vec::new();
        let mut errors = Vec::new();
        let mut row_number = if has_headers { 1 } else { 0 };

        // Process records one by one for error recovery
        for result in csv_reader.records() {
            row_number += 1;
            
            match result {
                Ok(record) => {
                    // Try to extract key and value
                    match self.extract_key_value(&record, key_column, value_column) {
                        Ok((key, value)) => {
                            successful_records.push((key, value));
                        }
                        Err(e) => {
                            errors.push(ETLError {
                                row_number,
                                error: format!("Failed to extract key/value: {}", e),
                            });
                        }
                    }
                }
                Err(e) => {
                    errors.push(ETLError {
                        row_number,
                        error: format!("CSV parsing error: {}", e),
                    });
                }
            }
        }

        let total_rows = successful_records.len() + errors.len();
        println!("Parsed {} successful records, {} errors from CSV", successful_records.len(), errors.len());

        if successful_records.is_empty() {
            return Ok(ETLResult {
                total_rows,
                successful_inserts: 0,
                errors,
            });
        }

        // Process successful records in parallel
        let total_inserted = Arc::new(Mutex::new(0));
        let lsm_tree = Arc::new(Mutex::new(lsm_tree));
        let insertion_errors = Arc::new(Mutex::new(Vec::new()));

        successful_records
            .chunks(self.batch_size)
            .enumerate()
            .collect::<Vec<_>>()
            .into_par_iter()
            .for_each(|(batch_idx, chunk)| {
                let mut batch_data = BTreeMap::new();

                for (key, value) in chunk {
                    batch_data.insert(key.clone(), value.clone());
                }

                let mut lsm = lsm_tree.lock();
                let mut inserted_count = 0;

                for (key, value) in batch_data {
                    if let Value::Data(data) = value {
                        match lsm.insert(key.clone(), data) {
                            Ok(_) => inserted_count += 1,
                            Err(e) => {
                                let mut errors = insertion_errors.lock();
                                errors.push(ETLError {
                                    row_number: 0, // We don't track individual row numbers in batches
                                    error: format!("Failed to insert {}: {}", key, e),
                                });
                            }
                        }
                    }
                }

                let mut total = total_inserted.lock();
                *total += inserted_count;

                println!("Batch {} completed: {} records inserted", batch_idx + 1, inserted_count);
            });

        let final_count = *total_inserted.lock();
        let mut final_errors = errors;
        final_errors.extend(insertion_errors.lock().clone());

        println!("ETL load with recovery complete: {} records inserted, {} errors", final_count, final_errors.len());

        Ok(ETLResult {
            total_rows,
            successful_inserts: final_count,
            errors: final_errors,
        })
    }

    fn extract_key_value(&self, record: &csv::StringRecord, key_column: usize, value_column: usize) -> DbResult<(String, Value)> {
        let key = record.get(key_column).ok_or_else(|| {
            DbError::InvalidOperation(format!("Key column {} not found in record", key_column))
        })?;

        let value = record.get(value_column).ok_or_else(|| {
            DbError::InvalidOperation(format!("Value column {} not found in record", value_column))
        })?;

        Ok((key.to_string(), Value::Data(value.to_string())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use std::io::Write;
    use crate::engine::{LSMTree, LSMConfig};

    #[test]
    fn test_csv_loading() {
        let temp_dir = tempdir().unwrap();
        
        // Create test CSV file
        let csv_path = temp_dir.path().join("test.csv");
        let mut file = File::create(&csv_path).unwrap();
        writeln!(file, "key,value").unwrap();
        writeln!(file, "key1,value1").unwrap();
        writeln!(file, "key2,value2").unwrap();
        writeln!(file, "key3,value3").unwrap();
        
        // Setup LSM tree
        let config = LSMConfig {
            memtable_size_limit: 100,
            data_dir: temp_dir.path().join("db"),
            background_compaction: false,
            background_compaction_interval: std::time::Duration::from_secs(1),
            enable_wal: false,
        };
        
        let mut lsm_tree = LSMTree::with_config(config).unwrap();
        
        // Load CSV
        let loader = ETLLoader::new();
        let count = loader.load_csv(&csv_path, &mut lsm_tree, 0, 1).unwrap();
        
        assert_eq!(count, 3);
        
        // Verify data
        assert_eq!(lsm_tree.get("key1").unwrap(), Some("value1".to_string()));
        assert_eq!(lsm_tree.get("key2").unwrap(), Some("value2".to_string()));
        assert_eq!(lsm_tree.get("key3").unwrap(), Some("value3".to_string()));
    }

    #[test]
    fn test_csv_loading_no_headers() {
        let temp_dir = tempdir().unwrap();
        
        // Create test CSV file without headers
        let csv_path = temp_dir.path().join("test_no_headers.csv");
        let mut file = File::create(&csv_path).unwrap();
        writeln!(file, "user1,data1").unwrap();
        writeln!(file, "user2,data2").unwrap();
        writeln!(file, "user3,data3").unwrap();
        
        // Setup LSM tree
        let config = LSMConfig {
            memtable_size_limit: 100,
            data_dir: temp_dir.path().join("db"),
            background_compaction: false,
            background_compaction_interval: std::time::Duration::from_secs(1),
            enable_wal: false,
        };
        
        let mut lsm_tree = LSMTree::with_config(config).unwrap();
        
        // Load CSV
        let loader = ETLLoader::new();
        let count = loader.load_csv_with_options(&csv_path, &mut lsm_tree, 0, 1, false).unwrap();
        
        assert_eq!(count, 3);
        
        // Verify data
        assert_eq!(lsm_tree.get("user1").unwrap(), Some("data1".to_string()));
        assert_eq!(lsm_tree.get("user2").unwrap(), Some("data2".to_string()));
        assert_eq!(lsm_tree.get("user3").unwrap(), Some("data3".to_string()));
    }

    #[test]
    fn test_csv_loading_different_columns() {
        let temp_dir = tempdir().unwrap();
        
        // Create test CSV file with different column order
        let csv_path = temp_dir.path().join("test_different_cols.csv");
        let mut file = File::create(&csv_path).unwrap();
        writeln!(file, "id,name,value,status").unwrap();
        writeln!(file, "1,Alice,apple,active").unwrap();
        writeln!(file, "2,Bob,banana,inactive").unwrap();
        writeln!(file, "3,Charlie,cherry,active").unwrap();
        
        // Setup LSM tree
        let config = LSMConfig {
            memtable_size_limit: 100,
            data_dir: temp_dir.path().join("db"),
            background_compaction: false,
            background_compaction_interval: std::time::Duration::from_secs(1),
            enable_wal: false,
        };
        
        let mut lsm_tree = LSMTree::with_config(config).unwrap();
        
        // Load CSV using columns 1 (name) and 2 (value)
        let loader = ETLLoader::new();
        let count = loader.load_csv(&csv_path, &mut lsm_tree, 1, 2).unwrap();
        
        assert_eq!(count, 3);
        
        // Verify data
        assert_eq!(lsm_tree.get("Alice").unwrap(), Some("apple".to_string()));
        assert_eq!(lsm_tree.get("Bob").unwrap(), Some("banana".to_string()));
        assert_eq!(lsm_tree.get("Charlie").unwrap(), Some("cherry".to_string()));
    }

    #[test]
    fn test_csv_loading_empty_file() {
        let temp_dir = tempdir().unwrap();
        
        // Create empty CSV file
        let csv_path = temp_dir.path().join("empty.csv");
        let mut file = File::create(&csv_path).unwrap();
        writeln!(file, "key,value").unwrap(); // Only headers
        
        // Setup LSM tree
        let config = LSMConfig {
            memtable_size_limit: 100,
            data_dir: temp_dir.path().join("db"),
            background_compaction: false,
            background_compaction_interval: std::time::Duration::from_secs(1),
            enable_wal: false,
        };
        
        let mut lsm_tree = LSMTree::with_config(config).unwrap();
        
        // Load CSV
        let loader = ETLLoader::new();
        let count = loader.load_csv(&csv_path, &mut lsm_tree, 0, 1).unwrap();
        
        assert_eq!(count, 0);
    }

    #[test]
    fn test_csv_loading_with_recovery_mode() {
        let temp_dir = tempdir().unwrap();
        
        // Create CSV file with some malformed rows
        let csv_path = temp_dir.path().join("test_recovery.csv");
        let mut file = File::create(&csv_path).unwrap();
        writeln!(file, "name,age").unwrap();
        writeln!(file, "Alice,25").unwrap();
        writeln!(file, "Bob").unwrap(); // Missing age column
        writeln!(file, "Charlie,30").unwrap();
        writeln!(file, "Diana,invalid_age").unwrap(); // Invalid age but should still load
        
        // Setup LSM tree
        let config = LSMConfig {
            memtable_size_limit: 100,
            data_dir: temp_dir.path().join("db"),
            background_compaction: false,
            background_compaction_interval: std::time::Duration::from_secs(1),
            enable_wal: false,
        };
        
        let mut lsm_tree = LSMTree::with_config(config).unwrap();
        
        // Load CSV with recovery mode
        let loader = ETLLoader::new().with_recovery_mode(true);
        let result = loader.load_csv_with_recovery(&csv_path, &mut lsm_tree, 0, 1, true).unwrap();
        
        // Should have loaded 3 successful records and 1 error
        assert_eq!(result.successful_inserts, 3);
        assert_eq!(result.errors.len(), 1);
        assert!(result.success_rate() > 0.7);
        
        // Verify successful records
        assert_eq!(lsm_tree.get("Alice").unwrap(), Some("25".to_string()));
        assert_eq!(lsm_tree.get("Charlie").unwrap(), Some("30".to_string()));
        assert_eq!(lsm_tree.get("Diana").unwrap(), Some("invalid_age".to_string()));
        
        // Bob should not be in the database due to parsing error
        assert_eq!(lsm_tree.get("Bob").unwrap(), None);
    }

    #[test]
    fn test_delimiter_detection() {
        let temp_dir = tempdir().unwrap();
        
        // Create CSV with semicolon delimiter
        let csv_path = temp_dir.path().join("test_semicolon.csv");
        let mut file = File::create(&csv_path).unwrap();
        writeln!(file, "name;age;city").unwrap();
        writeln!(file, "Alice;25;NYC").unwrap();
        writeln!(file, "Bob;30;London").unwrap();
        
        // Setup LSM tree
        let config = LSMConfig {
            memtable_size_limit: 100,
            data_dir: temp_dir.path().join("db"),
            background_compaction: false,
            background_compaction_interval: std::time::Duration::from_secs(1),
            enable_wal: false,
        };
        
        let _lsm_tree = LSMTree::with_config(config).unwrap();
        
        // Load CSV with custom delimiter
        let _loader = ETLLoader::new();
        let parser = CSVParser::new(0, 1).with_custom_delimiter(';');
        
        let file = File::open(&csv_path).unwrap();
        let records = parser.parse_records(file).unwrap();
        
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].0, "Alice");
        assert_eq!(records[1].0, "Bob");
    }
}