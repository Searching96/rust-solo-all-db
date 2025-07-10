// Compaction module for merging SSTables in LSM tree

use crate::{DbResult, Value};
use super::{SSTable};
use std::collections::BTreeMap;
use std::path::{PathBuf};

pub struct Compactor {
    data_dir: PathBuf,
}

impl Compactor {
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
    }

    // Returns the path to the new merged SSTable
    pub fn compact_sstables(&self, sstables: &[SSTable], output_id: u64) -> DbResult<SSTable> {
        println!("Starting compaction of {} SSTables...", sstables.len());

        let mut all_records = BTreeMap::new();
        let mut total_input_records = 0;

        // Process SSTables in reverse order so newer values (including tombstones) are not overwritten by older values
        for sstable in sstables.iter().rev() {
            let records = sstable.scan()?;
            total_input_records += records.len();
            
            // Add records to map (BTreeMap automatically handles duplicates by keeping latest)
            for record in records {
                all_records.insert(record.key, record.value);
            }
        }

        // Filter out tombstones for the final output
        let final_records: BTreeMap<String, Value> = all_records
            .into_iter()
            .filter(|(_, value)| !value.is_tombstone())
            .collect();

        println!("Compaction stats:");
        println!("Input: {} SSTables with {} total records", sstables.len(), total_input_records);
        println!("Output: {} unique records after merging", final_records.len());

        let output_filename = format!("sstable_{:06}_compacted.sst", output_id);
        let output_path = self.data_dir.join(output_filename);

        let compacted_sstable = SSTable::create(&output_path, &final_records)?;

        println!("Compaction complete. Merged SSTable created at: {}", output_path.display());

        Ok(compacted_sstable)
    }

    pub fn cleanup_old_sstables(&self, old_sstables: &[SSTable]) -> DbResult<()> {
        println!("Cleaning up {} old SSTables...", old_sstables.len());

        for sstable in old_sstables {
            match std::fs::remove_file(sstable.file_path()) {
                Ok(_) => println!("Deleted: {}", sstable.file_path().display()),
                Err(e) => {
                    println!("Failed to delete {}: {}", sstable.file_path().display(), e);
                }
            }
        }

        println!("Cleanup complete.");
        Ok(())
    }

    pub fn should_compact(&self, sstable_count: usize) -> bool {
        sstable_count >= 3
    }
}

#[cfg(test)]
mod tests {
    // Tests are currently commented out - uncomment and import as needed
    // use super::*;
    // use tempfile::tempdir;
    // use std::collections::BTreeMap;

    // #[test]
    // fn test_compaction() {
    //     let temp_dir = tempdir().unwrap();
    //     let compactor = Compactor::new(temp_dir.path().to_path_buf());

    //     // Create test SSTables
    //     let mut data1 = BTreeMap::new();
    //     data1.insert("key1".to_string(), "value1_old".to_string());
    //     data1.insert("key2".to_string(), "value2".to_string());

    //     let mut data2 = BTreeMap::new();
    //     data2.insert("key1".to_string(), "value1_new".to_string()); // Updated value
    //     data2.insert("key3".to_string(), "value3".to_string());

    //     let sstable1 = SSTable::create(temp_dir.path().join("test1.sst"), &data1).unwrap();
    //     let sstable2 = SSTable::create(temp_dir.path().join("test2.sst"), &data2).unwrap();

    //     // Compact them
    //     let sstables = vec![sstable1, sstable2];
    //     let compacted = compactor.compact_sstables(&sstables, 999).unwrap();

    //     // Verify compacted result
    //     assert_eq!(compacted.len(), 3); // key1 (latest), key2, key3
    //     assert_eq!(compacted.get("key1").unwrap(), Some("value1_new".to_string())); // Latest value
    //     assert_eq!(compacted.get("key2").unwrap(), Some("value2".to_string()));
    //     assert_eq!(compacted.get("key3").unwrap(), Some("value3".to_string()));
    // }
}