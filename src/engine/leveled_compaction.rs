use crate::engine::{SSTable, LevelManager};
use crate::{DbResult, Value};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
pub struct LeveledCompactor {
    data_dir: PathBuf,
    next_sstable_id: AtomicU64,
}

impl LeveledCompactor {
    pub fn new(data_dir: PathBuf, next_sstable_id: u64) -> Self {
        Self 
        { 
            data_dir, 
            next_sstable_id: AtomicU64::new(next_sstable_id), 
        }
    }

    // Main compaction entry point
    pub fn compact_level(&mut self, level_manager: &mut LevelManager, level: usize) -> DbResult<()> {
        match level {
            0 => self.compact_level_0_to_1(level_manager),
            _ => self.compact_level_n_to_n_plus_1(level_manager, level),
        }
    }

    // Level 0 to 1: Handle overlapping SSTables
    pub fn compact_level_0_to_1(&mut self, level_manager: &mut LevelManager) -> DbResult<()> {
        println!("Starting Level 0 to Level 1 compaction...");

        // Collect all Level 0 SSTables (they can overlap)
        let level_0_sstables = level_manager.get_sstables_at_level(0);
        if level_0_sstables.is_empty() {
            return Ok(());
        }

        // Find the key range covered by Level 0 SSTables
        let min_key = level_0_sstables.iter()
            .map(|sstable| sstable.min_key())
            .min()
            .unwrap_or("")
            .to_string();
        let max_key = level_0_sstables.iter()
            .map(|sstable| sstable.max_key())
            .max()
            .unwrap_or("")
            .to_string();

        // Find overlapping SSTables in Level 1
        let level_1_overlapping = level_manager.get_overlapping_sstables(1, &min_key, &max_key);
    
        // Merge all overlapping SSTables from both levels
        let mut all_sstables = level_0_sstables.clone();
        all_sstables.extend(level_1_overlapping.clone());

        // Merge into new Level 1 SSTable
        let new_sstables = self.merge_sstables(all_sstables, 1)?;

        // Remove old SSTables
        let mut old_sstables = level_0_sstables;
        old_sstables.extend(level_1_overlapping);
        level_manager.remove_sstables(&old_sstables);

        // Add new SSTables to Level 1
        for sstable in new_sstables {
            level_manager.add_sstable(sstable, 1);
        }

        println!("Level 0 to Level 1 compaction completed");
        Ok(())
    }


    // Level N to N+1: Standard leveled compaction
    pub fn compact_level_n_to_n_plus_1(&mut self, level_manager: &mut LevelManager, level: usize) -> DbResult<()> {
        println!("Starting Level {} to Level {} compaction...", level, level + 1);

        // Get compaction candidates from source level
        let source_sstables = level_manager.get_compaction_candidates(level);
        if source_sstables.is_empty() {
            return Ok(());
        }

        // Calculate key range of source SSTables
        let min_key = source_sstables.iter()
            .map(|s| s.min_key())
            .min()
            .unwrap_or("")
            .to_string();
        let max_key = source_sstables.iter()
            .map(|s| s.max_key())
            .max()
            .unwrap_or("")
            .to_string();

        // Find overlapping SSTables in target level
        let target_level = level + 1;
        let target_overlapping = level_manager.get_overlapping_sstables(target_level, &min_key, &max_key);

        // Merge source and overlapping target SSTables
        let mut all_sstables = source_sstables.clone();
        all_sstables.extend(target_overlapping.clone());

        // Merge into new target level SSTables
        let new_sstables = self.merge_sstables(all_sstables, target_level)?;

        // Remove old SSTables
        let mut old_sstables = source_sstables;
        old_sstables.extend(target_overlapping);
        level_manager.remove_sstables(&old_sstables);

        // Add new SSTables to target level
        for sstable in new_sstables {
            level_manager.add_sstable(sstable, target_level);
        }

        println!("Level {} → Level {} compaction completed", level, level + 1);
        Ok(())
    }

        // Helper method to merge multiple SSTables
    fn merge_sstables(&mut self, sstables: Vec<SSTable>, target_level: usize) -> DbResult<Vec<SSTable>> {
        if sstables.is_empty() {
            return Ok(Vec::new());
        }

        // Load all records from all SSTables
        let mut all_records = BTreeMap::new();
        
        for sstable in &sstables {
            let records = sstable.load_records()?;
            for record in records {
                // Later records override earlier ones (newer data wins)
                all_records.insert(record.key.clone(), record.value.clone());
            }
        }

        // Remove tombstones (deleted entries)
        all_records.retain(|_, value| !matches!(value, Value::Tombstone));

        if all_records.is_empty() {
            return Ok(Vec::new());
        }

        // Split into multiple SSTables if too large
        const MAX_SSTABLE_SIZE: usize = 64 * 1024 * 1024; // 64MB per SSTable
        let mut new_sstables = Vec::new();
        let mut current_data = BTreeMap::new();
        let mut current_size = 0;

        for (key, value) in all_records {
            let estimated_size = key.len() + 
                if let Value::Data(ref s) = value { s.len() } else { 0 };
            
            if current_size + estimated_size > MAX_SSTABLE_SIZE && !current_data.is_empty() {
                // Create SSTable from current data
                let sstable_id = self.next_sstable_id();
                let filename = format!("sstable_L{:02}_{:06}.sst", target_level, sstable_id);
                let filepath = self.data_dir.join(filename);
                
                let sstable = SSTable::create_with_level(&filepath, &current_data, target_level)?;
                new_sstables.push(sstable);
                
                // Reset for next SSTable
                current_data.clear();
                current_size = 0;
            }
            
            current_data.insert(key, value);
            current_size += estimated_size;
        }

        // Create final SSTable if there's remaining data
        if !current_data.is_empty() {
            let sstable_id = self.next_sstable_id();
            let filename = format!("sstable_L{:02}_{:06}.sst", target_level, sstable_id);
            let filepath = self.data_dir.join(filename);
            
            let sstable = SSTable::create_with_level(&filepath, &current_data, target_level)?;
            new_sstables.push(sstable);
        }

        // Delete old SSTable files
        for sstable in &sstables {
            if let Err(e) = std::fs::remove_file(sstable.file_path()) {
                eprintln!("Warning: Failed to delete old SSTable file: {}", e);
            }
        }

        Ok(new_sstables)
    }

    // Helper method to generate next SSTable ID
    fn next_sstable_id(&self) -> u64 {
        self.next_sstable_id.fetch_add(1, Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use std::collections::BTreeMap;
    use crate::Value;
    use std::sync::atomic::{AtomicU32, Ordering};

    static TEST_COUNTER: AtomicU32 = AtomicU32::new(0);

    fn create_test_sstable_with_data(level: usize, data: BTreeMap<String, Value>) -> SSTable {
        let temp_dir = tempdir().unwrap();
        let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let sstable_path = temp_dir.path().join(format!("test_level_{}_{}.sst", level, counter));
        
        let sstable = SSTable::create_with_level(&sstable_path, &data, level).unwrap();
        
        // Keep the temp_dir alive by leaking it (for test purposes only)
        std::mem::forget(temp_dir);
        
        sstable
    }

    #[test]
    fn test_sstable_merging() {
        let temp_dir = tempdir().unwrap();
        let mut compactor = LeveledCompactor::new(temp_dir.path().to_path_buf(), 1);

        // Create test SSTables with overlapping data
        let mut data1 = BTreeMap::new();
        data1.insert("key1".to_string(), Value::Data("value1".to_string()));
        data1.insert("key2".to_string(), Value::Data("value2".to_string()));
        
        let mut data2 = BTreeMap::new();
        data2.insert("key2".to_string(), Value::Data("value2_updated".to_string()));
        data2.insert("key3".to_string(), Value::Data("value3".to_string()));

        let sstable1 = create_test_sstable_with_data(0, data1);
        let sstable2 = create_test_sstable_with_data(0, data2);

        let sstables = vec![sstable1, sstable2];
        let merged = compactor.merge_sstables(sstables, 1).unwrap();

        assert!(!merged.is_empty());
        
        // Verify merged data contains expected keys
        let mut found_keys = std::collections::HashSet::new();
        for merged_sstable in merged {
            let records = merged_sstable.load_records().unwrap();
            for record in records {
                found_keys.insert(record.key.clone());
                match record.key.as_str() {
                    "key1" => assert_eq!(record.value, Value::Data("value1".to_string())),
                    "key2" => assert_eq!(record.value, Value::Data("value2_updated".to_string())), // Updated value wins
                    "key3" => assert_eq!(record.value, Value::Data("value3".to_string())),
                    _ => panic!("Unexpected key: {}", record.key),
                }
            }
        }
        
        // Verify all expected keys are present
        assert!(found_keys.contains("key1"));
        assert!(found_keys.contains("key2"));
        assert!(found_keys.contains("key3"));
    }

    #[test]
    fn test_tombstone_removal() {
        let temp_dir = tempdir().unwrap();
        let mut compactor = LeveledCompactor::new(temp_dir.path().to_path_buf(), 1);

        // Create SSTable with tombstone
        let mut data = BTreeMap::new();
        data.insert("key1".to_string(), Value::Data("value1".to_string()));
        data.insert("key2".to_string(), Value::Tombstone);

        let sstable = create_test_sstable_with_data(0, data);
        let merged = compactor.merge_sstables(vec![sstable], 1).unwrap();

        // Verify tombstone is removed
        let mut total_records = 0;
        let mut found_key1 = false;
        
        for merged_sstable in merged {
            let records = merged_sstable.load_records().unwrap();
            total_records += records.len();
            
            for record in records {
                if record.key == "key1" {
                    found_key1 = true;
                    assert_eq!(record.value, Value::Data("value1".to_string()));
                } else if record.key == "key2" {
                    panic!("Tombstone key2 should have been removed!");
                }
            }
        }
        
        assert_eq!(total_records, 1, "Should only have 1 record after tombstone removal");
        assert!(found_key1, "key1 should be present");
    }
}