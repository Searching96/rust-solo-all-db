use crate::{DbResult, DbError, Value};
use crate::engine::LSMTree;
use crate::etl::csv_parser::CSVParser;
use rayon::prelude::*;
use std::path::Path;
use std::fs::File;
use std::collections::BTreeMap;
use std::sync::Arc;
use parking_lot::Mutex;

pub struct ETLLoader {
    batch_size: usize,
    parallel_threads: usize,
}

impl ETLLoader {
    pub fn new() -> Self {
        Self {
            batch_size: 1000,
            parallel_threads: 4, // Default to 4 threads
        }
    }

    pub fn with_config(batch_size: usize, parallel_threads: usize) -> Self {
        Self {
            batch_size,
            parallel_threads,
        }
    }

    pub fn load_csv<P: AsRef<Path>>(
        &self,
        file_path: P,
        lsm_tree: &mut LSMTree,
        key_column: usize,
        value_column: usize,
    ) -> DbResult<usize> {
        let file = File::open(file_path).map_err(|e| {
            DbError::InvalidOperation(format!("Failed to open CSV file: {}", e))
        })?;

        let parser = CSVParser::new(key_column, value_column);
        let records = parser.parse_records(file)?;

        println!("Loaded {} records from CSV, starting parallel insertion...", records.len());

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
}