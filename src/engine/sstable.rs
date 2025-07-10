// SSTable (Sorted String Table) implementation
// An immutable, sorted file format for storing key-value pairs

use crate::{DbError, DbResult, Value};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub key: String,
    pub value: Value,
}

#[derive(Debug, Clone)]
pub struct SSTable {
    file_path: PathBuf,
    record_count: usize,
}

impl SSTable {
    // Create a new SSTable by writing data from a BTreeMap to disk
    pub fn create<P:AsRef<Path>>(
        file_path: P,
        data: &BTreeMap<String, Value>,
    ) -> DbResult<Self> {
        let path = file_path.as_ref().to_path_buf();

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                DbError::InvalidOperation(format!("Failed to create directory: {}", e))
            })?;
        }

        let file = File::create(&path).map_err(|e| {
            DbError::InvalidOperation(format!("Failed to create SSTable file: {}", e))
        })?;

        let mut writer = BufWriter::new(file);

        // Convert BTreeMap to sorted records
        let records: Vec<Record> = data
            .iter()
            .map(|(k, v)| Record {
                key: k.clone(),
                value: v.clone(),
            })
            .collect();

        bincode::serialize_into(&mut writer, &records).map_err(|e| {
            DbError::InvalidOperation(format!("Failed to serialize SSTable: {}", e))
        })?;

        Ok(SSTable {
            file_path: path,
            record_count: records.len(),
        })    
    }

    // Open an existing SSTable from disk
    pub fn open<P: AsRef<Path>>(file_path: P) -> DbResult<Self> {
        let path = file_path.as_ref().to_path_buf();

        if !path.exists() {
            return Err(DbError::InvalidOperation(format!(
                "SSTable file does not exist: {}",
                path.display()
            )));
        }

        // Read the file to count records
        // In real implementation, we would store metadata separately
        let records = Self::load_records(&path)?;

        Ok(SSTable {
            file_path: path,
            record_count: records.len(),
        })
    }

    pub fn get(&self, key: &str) -> DbResult<Option<String>> {
        let records = Self::load_records(&self.file_path)?;

        for record in records {
            if record.key == key { // Since PartialEq is derived, we can use == directly
                match &record.value {
                    Value::Data(s) => return Ok(Some(s.clone())),
                    Value::Tombstone => return Ok(None), // Tombstone means key was deleted
                }
            }

            // Rust does not implement PartialOrd between String and &str,
            if record.key.as_str() > key {
                break;
            }
        }

        Ok(None)
    }

    // Get all records from the SSTable (for debugging or testing)
    pub fn scan(&self) -> DbResult<Vec<Record>> {
        Self::load_records(&self.file_path)
    }

    pub fn len(&self) -> usize {
        self.record_count
    }

    pub fn is_empty(&self) -> bool {
        self.record_count == 0
    }

    // Get the file path of the SSTable
    pub fn file_path(&self) -> &Path {
        &self.file_path
    }

    // Help method to load records from disk
    fn load_records(file_path: &Path) -> DbResult<Vec<Record>> {
        let file = File::open(file_path).map_err(|e| {
            DbError::InvalidOperation(format!("Failed to open SSTable file: {}", e))
        })?;

        let reader = BufReader::new(file);

        bincode::deserialize_from(reader).map_err(|e| {
            DbError::InvalidOperation(format!("Failed to deserialize SSTable: {}", e))
        })
    }
}

#[cfg(test)]
mod tests {
    // Tests are currently commented out - uncomment and import as needed
    // use super::*;
    // use std::collections::BTreeMap;
    // use tempfile::tempdir;

//     #[test]
//     fn test_sstable_create_and_read() {
//         // Create temporary directory
//         let temp_dir = tempdir().unwrap();
//         let sstable_path = temp_dir.path().join("test.sst");

//         // Create test data
//         let mut data = BTreeMap::new();
//         data.insert("key1".to_string(), "value1".to_string());
//         data.insert("key2".to_string(), "value2".to_string());
//         data.insert("key3".to_string(), "value3".to_string());

//         // Create SSTable
//         let sstable = SSTable::create(&sstable_path, &data).unwrap();
//         assert_eq!(sstable.len(), 3);

//         // Test reading values
//         assert_eq!(sstable.get("key1").unwrap(), Some("value1".to_string()));
//         assert_eq!(sstable.get("key2").unwrap(), Some("value2".to_string()));
//         assert_eq!(sstable.get("key3").unwrap(), Some("value3".to_string()));
//         assert_eq!(sstable.get("nonexistent").unwrap(), None);

//         // Test scan
//         let records = sstable.scan().unwrap();
//         assert_eq!(records.len(), 3);
//         as
// sert_eq!(records[0].key, "key1");
//         assert_eq!(records[1].key, "key2");
//         assert_eq!(records[2].key, "key3");
//     }
    // #[test]
    // fn test_sstable_reopen() {
    //     let temp_dir = tempdir().unwrap();
    //     let sstable_path = temp_dir.path().join("test_reopen.sst");

    //     // Create and close SSTable
    //     {
    //         let mut data = BTreeMap::new();
    //         data.insert("persistent_key".to_string(), "persistent_value".to_string());
    //         SSTable::create(&sstable_path, &data).unwrap();
    //     }

    //     // Reopen SSTable
    //     let sstable = SSTable::open(&sstable_path).unwrap();
    //     assert_eq!(sstable.len(), 1);
    //     assert_eq!(
    //         sstable.get("persistent_key").unwrap(),
    //         Some("persistent_value".to_string())
    //     );
    // }
    
    // #[test]
    // fn test_create_persistent_sstable() {
    //     use std::path::Path;
        
    //     // Create SSTable in current directory (will persist)
    //     let sstable_path = Path::new("example_output.sst");
        
    //     // Create test data
    //     let mut data = BTreeMap::new();
    //     data.insert("persistent_key1".to_string(), "persistent_value1".to_string());
    //     data.insert("persistent_key2".to_string(), "persistent_value2".to_string());
    //     data.insert("persistent_key3".to_string(), "persistent_value3".to_string());

    //     // Create SSTable
    //     let sstable = SSTable::create(sstable_path, &data).unwrap();
        
    //     println!("Created persistent SSTable: {}", sstable_path.display());
    //     println!("File size: {} bytes", std::fs::metadata(sstable_path).unwrap().len());
        
    //     // Verify it works
    //     assert_eq!(sstable.get("persistent_key2").unwrap(), Some("persistent_value2".to_string()));
        
    //     // Note: This file will remain in your project directory
    //     // You can examine it or delete it manually
    // }

    // #[test]
    // fn test_inspect_sstable_contents() {
    //     use std::path::Path;
        
    //     // Create a test SSTable
    //     let sstable_path = Path::new("inspect_me.sst");
        
    //     let mut data = BTreeMap::new();
    //     data.insert("apple".to_string(), "red fruit".to_string());
    //     data.insert("banana".to_string(), "yellow fruit".to_string());
    //     data.insert("cherry".to_string(), "small red fruit".to_string());

    //     // Create SSTable
    //     let sstable = SSTable::create(sstable_path, &data).unwrap();
        
    //     // Now let's inspect what's inside
    //     println!("\n=== SSTable Inspection ===");
    //     println!("File: {}", sstable_path.display());
    //     println!("File size: {} bytes", std::fs::metadata(sstable_path).unwrap().len());
    //     println!("Record count: {}", sstable.len());
        
    //     // Scan all records to see the structure
    //     let records = sstable.scan().unwrap();
    //     println!("\nContents (sorted order):");
    //     for (i, record) in records.iter().enumerate() {
    //         println!("  {}: '{}' -> '{}'", i, record.key, record.value);
    //     }
        
    //     // Test individual lookups
    //     println!("\nLookup tests:");
    //     println!("apple -> {:?}", sstable.get("apple").unwrap());
    //     println!("banana -> {:?}", sstable.get("banana").unwrap());
    //     println!("nonexistent -> {:?}", sstable.get("nonexistent").unwrap());
        
    //     // Show raw file info
    //     println!("\nFile metadata:");
    //     let metadata = std::fs::metadata(sstable_path).unwrap();
    //     println!("  Modified: {:?}", metadata.modified().unwrap());
    //     println!("  Size: {} bytes", metadata.len());
        
    //     println!("\n=== Inspection Complete ===");
    // }
}
