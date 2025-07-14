// LSM Tree implementation - coordinates MemTable and SSTables

use crate::{Value, WALEntry};
use crate::{DbError, DbResult, MemTable};
use super::SSTable;
use super::WAL;
use super::{LevelManager, LeveledCompactor};
use std::path::{Path, PathBuf};
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;
use parking_lot::RwLock;
use crossbeam_channel::{Sender, unbounded};

#[derive(Debug, Clone)]
pub struct LSMConfig {
    pub memtable_size_limit: usize,
    pub data_dir: PathBuf,
    pub background_compaction: bool,
    pub background_compaction_interval: Duration,
    pub enable_wal: bool,
}

impl Default for LSMConfig {
    fn default() -> Self {
        Self {
            memtable_size_limit: 1000, // Flush after 1000 entries
            data_dir: PathBuf::from("data"),
            background_compaction: true, // Enable background compaction by default
            background_compaction_interval: Duration::from_secs(10),
            enable_wal: true,
        }
    }
}

#[derive(Debug, Clone)]
pub enum CompactionMessage {
    CheckCompaction, // Trigger a compaction check
    ShutDown, // Gracefully shutdown the thread
}

#[derive(Debug)]
pub struct CompactionHandle {
    sender: Sender<CompactionMessage>,
    handle: Option<thread::JoinHandle<()>>,
}

impl CompactionHandle {
    pub fn send_check_compaction(&self) {
        let _ = self.sender.send(CompactionMessage::CheckCompaction);
    }

    pub fn shutdown(mut self) {
        let _ = self.sender.send(CompactionMessage::ShutDown);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

// LSM Tree - coordinates MemTable and multiple SSTables
#[derive(Debug)]
pub struct LSMTree {
    memtable: Arc<RwLock<MemTable>>,
    level_manager: Arc<RwLock<LevelManager>>,
    config: LSMConfig,
    next_sstable_id: Arc<AtomicU64>, // A thread-safe counter for generating unique SSTable filenames
    compaction_handle: Option<CompactionHandle>,
    wal: Option<Arc<RwLock<WAL>>>,
    leveled_compactor: Arc<RwLock<LeveledCompactor>>,
}

impl LSMTree {
    // Create a new LSMTree with default configuration
    pub fn new() -> DbResult<Self> {
        Self::with_config(LSMConfig::default())
    }

    // Create a new LSMTree with a custom configuration
    pub fn with_config(config: LSMConfig) -> DbResult<Self> {
        // Ensure data directory exists
        fs::create_dir_all(&config.data_dir).map_err(|e| {
            DbError::InvalidOperation(format!("Failed to create data directory: {}", e))
        })?;

        // Initialize WAL if enabled
        let wal = if config.enable_wal {
            let wal_path = config.data_dir.join("wal.log");
            let wal_instance = WAL::new(wal_path)?;
            Some(Arc::new(RwLock::new(wal_instance)))
        } else {
            None
        };

        // Load existing SSTables and organize them by level
        let existing_sstables = Self::load_existing_sstables(&config.data_dir)?;
        let next_sstable_id = Self::determine_next_id(&existing_sstables);

        let mut level_manager = LevelManager::new();
        for sstable in existing_sstables {
            let level = sstable.level();
            level_manager.add_sstable(sstable, level);
        }

        let memtable = Arc::new(RwLock::new(MemTable::new()));
        let level_manager = Arc::new(RwLock::new(level_manager));
        let next_sstable_id = Arc::new(AtomicU64::new(next_sstable_id));
        let leveled_compactor = Arc::new(RwLock::new(LeveledCompactor::new(
            config.data_dir.clone(),
            next_sstable_id.load(Ordering::SeqCst),
        )));

        // Create the LSMTree instance
        let mut lsm = Self {
            memtable: memtable.clone(),
            level_manager: level_manager.clone(),
            config: config.clone(),
            next_sstable_id: next_sstable_id.clone(),
            compaction_handle: None,
            wal,
            leveled_compactor: leveled_compactor.clone(),
        };

        // Replay WAL to restore state
        if lsm.wal.is_some() {
            lsm.replay_wal()?;
        }

        // Start background compaction thread if enabled
        let compaction_handle = if config.background_compaction {
            Some(Self::start_background_compaction(
                level_manager.clone(),
                leveled_compactor.clone(),
                config.clone(),
            )?)
        } else {
            None
        };

        lsm.compaction_handle = compaction_handle;

        Ok(lsm)
    }

    fn replay_wal(&mut self) -> DbResult<()> {
        if let Some(ref wal) = self.wal {
            let entries = {
                let wal_guard = wal.read();
                wal_guard.read_all()?
            };

            println!("Replaying {} WAL entries...", entries.len());

            for entry in entries {
                match entry {
                    WALEntry::Insert { key, value } => {
                        let mut memtable = self.memtable.write();
                        memtable.insert(key, value)?;
                    }
                    WALEntry::Delete { key } => {
                        let mut memtable = self.memtable.write();
                        memtable.insert_tombstone(key)?;
                    }
                }
            }
        }

        Ok(())
    }

    fn start_background_compaction(
        level_manager: Arc<RwLock<LevelManager>>,
        leveled_compactor: Arc<RwLock<LeveledCompactor>>,
        config: LSMConfig,
    ) -> DbResult<CompactionHandle> {
        let (tx, rx) = unbounded();
        let handle = thread::spawn(move || {
            loop {
                match rx.recv_timeout(config.background_compaction_interval) {
                    Ok(CompactionMessage::CheckCompaction) | Err(_) => {
                        // Check if any level needs compaction
                        let mut level_manager = level_manager.write();
                        let mut leveled_compactor = leveled_compactor.write();

                        // Check levels in priority order (L0 first, then L1, etc.)
                        for level in 0..=level_manager.get_max_level() {
                            if level_manager.should_compact(level) {
                                println!("Triggering compaction for level {}", level);
                                if let Err(e) = leveled_compactor.compact_level(
                                    &mut level_manager, level) {
                                    eprintln!("Compaction failed for level {}: {}", level, e);
                                }
                                break;
                            }
                        }
                    }
                    Ok(CompactionMessage::ShutDown) => break,
                }
            }
        });

        Ok(CompactionHandle {
            sender: tx,
            handle: Some(handle),
        })
    }

    pub fn insert(&mut self, key: String, value: String) -> DbResult<()> {
        // Write to WAL first (if enabled)
        if let Some(ref wal) = self.wal {
            let entry = WALEntry::Insert {
                key: key.clone(),
                value: value.clone(),
            };
            let mut wal_guard = wal.write();
            wal_guard.append(&entry)?;
        }

        // Then write to MemTable 
        {
            let mut memtable = self.memtable.write();
            memtable.insert(key, value)?;
        }

        // Check if we need to flush
        let memtable_len = {
            let memtable = self.memtable.read();
            memtable.len()
        };
        
        if memtable_len >= self.config.memtable_size_limit {
            self.flush_memtable()?;
        }

        Ok(())
    }

    pub fn get(&self, key: &str) -> DbResult<Option<String>> {
        // First check the MemTable (most recent data)
        {
            let memtable = self.memtable.read();
            match memtable.data().get(key) {
                Some(Value::Data(s)) => return Ok(Some(s.clone())),
                Some(Value::Tombstone) => return Ok(None),
                None => {
                    // Key not found in MemTable, check SSTables
                }
            }
        }

        // Check SSTables with bloom filter optimization
        let level_manager = self.level_manager.read();
        let all_sstables = level_manager.get_all_sstables();

        for sstable in all_sstables.iter() {
            // Quick bloom filter check
            if !sstable.might_contain(key) {
                continue;
            }

            // If bloom filter says it might contain the key, do the actual search
            if let Some(value_str) = sstable.get(key)? {
                return Ok(Some(value_str));
            }
        }

        Ok(None)
    }

    pub fn delete(&mut self, key: &str) -> DbResult<bool> {
        // Write to WAL first (if enabled)
        if let Some(ref wal) = self.wal {
            let entry = WALEntry::Delete {
                key: key.to_string(),
            };
            let mut wal_guard = wal.write();
            wal_guard.append(&entry)?;
        }

        // Insert tombstone in MemTable (this handles deletion from both MemTable and SSTables)
        {
            let mut memtable = self.memtable.write();
            memtable.insert_tombstone(key.to_string())?;
        }

        let memtable_len = {
            let memtable = self.memtable.read();
            memtable.len()
        };

        if memtable_len >= self.config.memtable_size_limit {
            self.flush_memtable()?;
        }

        Ok(true)
    }

    pub fn stats(&self) -> LSMStats {
        let memtable = self.memtable.read();
        let level_manager = self.level_manager.read();
        let level_stats = level_manager.stats();
        
        LSMStats {
            memtable_entries: memtable.len(),
            sstable_count: level_stats.level_stats.values().map(|s| s.file_count).sum(),
            total_sstable_entries: level_stats.level_stats.values().map(|s| s.total_size).sum(),
            next_flush_at: self.config.memtable_size_limit,
        }
    }

    // Force flush MemTable to SSTable (for testing or shutdown)
    pub fn flush(&mut self) -> DbResult<()> {
        let is_empty = {
            let memtable = self.memtable.read();
            memtable.is_empty()
        };
        
        if !is_empty {
            self.flush_memtable()?;
        }
        Ok(())
    }

    // Internal: Flush current MemTable to a new SSTable
    fn flush_memtable(&mut self) -> DbResult<()> {
        let is_empty = {
            let memtable = self.memtable.read();
            memtable.is_empty()
        };
        
        if is_empty {
            return Ok(());
        }

        let current_id = self.next_sstable_id.fetch_add(1, Ordering::SeqCst);
        let filename = format!("sstable_{:06}.sst", current_id);
        let filepath = self.config.data_dir.join(filename);

        // Create SSTable from MemTable data
        let memtable_data = {
            let memtable = self.memtable.read();
            memtable.data().clone()
        };

        let memtable_len = memtable_data.len();
        
        println!("Flushing MemTable with {} entries to {}", 
            memtable_len, filepath.display());
        
        // Create new SSTable at Level 0
        let sstable = SSTable::create_with_level(&filepath, &memtable_data, 0)?;

        // Add to Level Manager
        {
            let mut level_manager = self.level_manager.write();
            level_manager.add_sstable(sstable, 0);
        }

        // Clear MemTable
        {
            let mut memtable = self.memtable.write();
            *memtable = MemTable::new();
        }

        // Truncate WAL since data is now persisted in SSTable
        if let Some(ref wal) = self.wal {
            let mut wal_guard = wal.write();
            wal_guard.truncate()?;
            println!("WAL truncated after flush");
        }

        // Trigger compaction if needed
        if self.config.background_compaction {
            if let Some(ref handle) = self.compaction_handle {
                handle.send_check_compaction();
            }
        }

        Ok(())        
    }

    // Load existing SSTable files from the data directory
    fn load_existing_sstables(data_dir: &Path) -> DbResult<Vec<SSTable>> {
        let mut sstables = Vec::new();

        if !data_dir.exists() {
            return Ok(sstables); // No SSTables if directory doesn't exist (why Ok?)
        }

        let entries = fs::read_dir(data_dir).map_err(|e| {
            DbError::InvalidOperation(format!("Failed to read data directory: {}", e))
        })?;

        let mut sstable_files = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| {
                DbError::InvalidOperation(format!("Failed to read directory entry: {}", e))
            })?;
            
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("sst") {
                sstable_files.push(path);
            }
        }

        // Sort files by name
        sstable_files.sort();
        sstable_files.reverse(); // Newest first (due to our naming convention)

        // Load each SSTable
        for file_path in sstable_files {
            match SSTable::open(&file_path) {
                Ok(sstable) => sstables.push(sstable),
                Err(e) => {
                    println!("Warning: Failed to open SSTable {}: {}", file_path.display(), e);
                    // We can choose to skip this file or handle it differently
                }
            }
        }

        Ok(sstables)
    }

    fn determine_next_id(sstables: &[SSTable]) -> u64 {
        sstables
            .iter()
            .filter_map(|sst| {
                sst.file_path()
                    .file_stem()
                    .and_then(|name| name.to_str())
                    .and_then(|name| name.strip_prefix("sstable_"))
                    .and_then(|id_str| id_str.parse::<u64>().ok())
            })
            .max()
            .map(|max_id| max_id + 1)
            .unwrap_or(0)
    }

}

#[derive(Debug)]
pub struct LSMStats {
    pub memtable_entries: usize,
    pub sstable_count: usize,
    pub total_sstable_entries: usize,
    pub next_flush_at: usize,
}

impl std::fmt::Display for LSMStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LSMTree Stats: MemTable: {}, SSTables: {} (total {} entries), flush at {}",
            self.memtable_entries,
            self.sstable_count,
            self.total_sstable_entries,
            self.next_flush_at
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use std::time::Duration;

    #[test]
    fn test_background_compaction() {
        let temp_dir = tempdir().unwrap();
        let config = LSMConfig {
            memtable_size_limit: 2,  // Very small to trigger flushes
            data_dir: temp_dir.path().to_path_buf(),
            background_compaction: true,
            background_compaction_interval: Duration::from_millis(100), // Fast for testing
            enable_wal: true,
        };

        let mut lsm = LSMTree::with_config(config).unwrap();

        println!("=== Testing Background Compaction ===");

        // Insert data to create multiple SSTables
        println!("Inserting data to trigger flushes...");
        for i in 1..=10 {
            lsm.insert(format!("key{}", i), format!("value{}", i)).unwrap();
            let stats = lsm.stats();
            println!("After insert {}: {}", i, stats);
        }

        // Check initial state
        let initial_stats = lsm.stats();
        println!("Initial state: {}", initial_stats);
        
        // Background compaction should trigger when we have >= 3 SSTables
        if initial_stats.sstable_count >= 3 {
            println!("Waiting for background compaction to trigger...");
            
            // Wait a bit for background compaction to happen
            std::thread::sleep(Duration::from_millis(500));
            
            let after_stats = lsm.stats();
            println!("After background compaction: {}", after_stats);
            
            // Background compaction should have reduced the number of SSTables
            println!("SSTables before: {}, after: {}", initial_stats.sstable_count, after_stats.sstable_count);
        } else {
            println!("Not enough SSTables created for background compaction test");
        }

        // Force manual compaction to test it works
        println!("Testing manual compaction...");
        let before_manual = lsm.stats();
        
        // Manually trigger compaction using the level manager
        {
            let mut level_manager = lsm.level_manager.write();
            let mut compactor = lsm.leveled_compactor.write();
            
            // Check if Level 0 needs compaction
            if level_manager.should_compact(0) {
                let _ = compactor.compact_level(&mut level_manager, 0);
            }
        }
        
        let after_manual = lsm.stats();
        println!("Manual compaction - before: {}, after: {}", before_manual.sstable_count, after_manual.sstable_count);

        // Verify data integrity
        println!("Verifying data integrity...");
        for i in 1..=10 {
            let key = format!("key{}", i);
            let expected = format!("value{}", i);
            match lsm.get(&key).unwrap() {
                Some(value) => assert_eq!(value, expected, "Data integrity check failed for {}", key),
                None => panic!("Key {} was lost during compaction!", key),
            }
        }
        println!("All data integrity checks passed!");
    }

    #[test] 
    fn test_background_compaction_disabled() {
        let temp_dir = tempdir().unwrap();
        let config = LSMConfig {
            memtable_size_limit: 2,
            data_dir: temp_dir.path().to_path_buf(),
            background_compaction: false,  // Disabled
            background_compaction_interval: Duration::from_secs(1),
            enable_wal: true,
        };

        let mut lsm = LSMTree::with_config(config).unwrap();
        
        // Insert data to create multiple SSTables
        for i in 1..=6 {
            lsm.insert(format!("key{}", i), format!("value{}", i)).unwrap();
        }

        let stats = lsm.stats();
        println!("With background compaction disabled: {}", stats);
        
        // Since background compaction is disabled, we should have multiple SSTables
        assert!(stats.sstable_count >= 2, "Should have multiple SSTables when background compaction is disabled");
    }
    // #[test]
    // fn test_lsm_basic_operations() {
    //     let temp_dir = tempdir().unwrap();
    //     let config = LSMConfig {
    //         memtable_size_limit: 3,  // Small limit for testing
    //         data_dir: temp_dir.path().to_path_buf(),
    //     };

    //     let mut lsm = LSMTree::with_config(config).unwrap();

    //     // Insert some data
    //     lsm.insert("key1".to_string(), "value1".to_string()).unwrap();
    //     lsm.insert("key2".to_string(), "value2".to_string()).unwrap();

    //     // Should be in MemTable
    //     assert_eq!(lsm.get("key1").unwrap(), Some("value1".to_string()));
    //     assert_eq!(lsm.get("key2").unwrap(), Some("value2".to_string()));

    //     let stats = lsm.stats();
    //     assert_eq!(stats.memtable_entries, 2);
    //     assert_eq!(stats.sstable_count, 0);
    // }

    // #[test]
    // fn test_lsm_flush_on_size() {
    //     let temp_dir = tempdir().unwrap();
    //     let config = LSMConfig {
    //         memtable_size_limit: 2,  // Very small limit
    //         data_dir: temp_dir.path().to_path_buf(),
    //     };

    //     let mut lsm = LSMTree::with_config(config).unwrap();

    //     // Insert data to trigger flush
    //     lsm.insert("key1".to_string(), "value1".to_string()).unwrap();
    //     lsm.insert("key2".to_string(), "value2".to_string()).unwrap();

    //     let stats_before = lsm.stats();
    //     println!("Before flush: {}", stats_before);

    //     // This should trigger a flush
    //     lsm.insert("key3".to_string(), "value3".to_string()).unwrap();

    //     let stats_after = lsm.stats();
    //     println!("After flush: {}", stats_after);

    //     // MemTable should have been flushed and now contains only key3
    //     assert_eq!(stats_after.memtable_entries, 1);  // Only key3
    //     // Note: SSTable creation will be fixed in the next step
    // }

    // #[test]
    // fn test_lsm_flush_and_read_back() {
    //     let temp_dir = tempdir().unwrap();
    //     let config = LSMConfig {
    //         memtable_size_limit: 2,
    //         data_dir: temp_dir.path().to_path_buf(),
    //     };

    //     let mut lsm = LSMTree::with_config(config).unwrap();

    //     // Insert data to trigger flush
    //     lsm.insert("key1".to_string(), "value1".to_string()).unwrap();
    //     lsm.insert("key2".to_string(), "value2".to_string()).unwrap();
        
    //     // This should trigger flush
    //     lsm.insert("key3".to_string(), "value3".to_string()).unwrap();

    //     // Verify we can read all data (from both MemTable and SSTable)
    //     assert_eq!(lsm.get("key1").unwrap(), Some("value1".to_string())); // From SSTable
    //     assert_eq!(lsm.get("key2").unwrap(), Some("value2".to_string())); // From SSTable  
    //     assert_eq!(lsm.get("key3").unwrap(), Some("value3".to_string())); // From MemTable

    //     let stats = lsm.stats();
    //     println!("Final stats: {}", stats);
    //     assert_eq!(stats.memtable_entries, 1);  // key3
    //     assert_eq!(stats.sstable_count, 1);     // one SSTable file
    // }

    // #[test]
    // fn test_tombstone_deletes() {
    //     let temp_dir = tempdir().unwrap();
    //     let config = LSMConfig {
    //         memtable_size_limit: 2,
    //         data_dir: temp_dir.path().to_path_buf(),
    //     };

    //     let mut lsm = LSMTree::with_config(config).unwrap();

    //     // Insert and flush to SSTable
    //     lsm.insert("key1".to_string(), "value1".to_string()).unwrap();
    //     lsm.insert("key2".to_string(), "value2".to_string()).unwrap();
    //     // This triggers flush to SSTable
    //     lsm.insert("key3".to_string(), "value3".to_string()).unwrap();

    //     // Verify key1 is in SSTable
    //     assert_eq!(lsm.get("key1").unwrap(), Some("value1".to_string()));

    //     // Delete key1 (should insert tombstone)
    //     assert!(lsm.delete("key1").unwrap());

    //     // key1 should now be "deleted" (not found)
    //     assert_eq!(lsm.get("key1").unwrap(), None);

    //     println!("=== Before compaction ===");
    //     println!("Stats: {}", lsm.stats());
    //     for (i, sstable) in lsm.sstables.iter().enumerate() {
    //         println!("SSTable {}: {} entries", i, sstable.len());
    //         let records = sstable.scan().unwrap();
    //         for record in records {
    //             println!("  {} -> {:?}", record.key, record.value);
    //         }
    //     }

    //     // Force compaction
    //     lsm.compact().unwrap();

    //     println!("=== After compaction ===");
    //     println!("Stats: {}", lsm.stats());
    //     for (i, sstable) in lsm.sstables.iter().enumerate() {
    //         println!("SSTable {}: {} entries", i, sstable.len());
    //         let records = sstable.scan().unwrap();
    //         for record in records {
    //             println!("  {} -> {:?}", record.key, record.value);
    //         }
    //     }

    //     // After compaction, key1 should still be deleted
    //     println!("=== Testing key1 after compaction ===");
    //     let result = lsm.get("key1").unwrap();
    //     println!("key1 result: {:?}", result);
    //     assert_eq!(result, None);
        
    //     // But key2 should still exist
    //     assert_eq!(lsm.get("key2").unwrap(), Some("value2".to_string()));
    // }

    // #[test]
    // fn test_tombstone_deletes_debug() {
    //     let temp_dir = tempdir().unwrap();
    //     let config = LSMConfig {
    //         memtable_size_limit: 2,
    //         data_dir: temp_dir.path().to_path_buf(),
    //     };

    //     let mut lsm = LSMTree::with_config(config).unwrap();

    //     // Insert and flush to SSTable
    //     println!("=== Inserting key1, key2 ===");
    //     lsm.insert("key1".to_string(), "value1".to_string()).unwrap();
    //     lsm.insert("key2".to_string(), "value2".to_string()).unwrap();
        
    //     println!("=== Before third insert (should trigger flush) ===");
    //     println!("Stats: {}", lsm.stats());
        
    //     // This triggers flush to SSTable
    //     lsm.insert("key3".to_string(), "value3".to_string()).unwrap();
        
    //     println!("=== After flush ===");
    //     println!("Stats: {}", lsm.stats());

    //     // Verify key1 is in SSTable
    //     println!("=== Checking key1 before delete ===");
    //     let value = lsm.get("key1").unwrap();
    //     println!("key1 value: {:?}", value);
    //     assert_eq!(value, Some("value1".to_string()));

    //     // Delete key1 (should insert tombstone)
    //     println!("=== Deleting key1 ===");
    //     assert!(lsm.delete("key1").unwrap());
        
    //     println!("=== After delete, before get ===");
    //     println!("Stats: {}", lsm.stats());
        
    //     // Check what's in MemTable
    //     println!("MemTable contents after delete:");
    //     for (k, v) in lsm.memtable.data() {
    //         println!("  {} -> {:?}", k, v);
    //     }

    //     // Check what's in each SSTable
    //     println!("=== Checking SSTables ===");
    //     for (i, sstable) in lsm.sstables.iter().enumerate() {
    //         println!("SSTable {}: {} entries", i, sstable.len());
    //         let records = sstable.scan().unwrap();
    //         for record in records {
    //             println!("  {} -> {:?}", record.key, record.value);
    //         }
    //     }

    //     // key1 should now be "deleted" (not found)
    //     println!("=== Getting key1 after delete ===");
    //     let value_after_delete = lsm.get("key1").unwrap();
    //     println!("key1 after delete: {:?}", value_after_delete);
        
    //     // This should be None!
    //     assert_eq!(value_after_delete, None);
    // }

    #[test]
    fn test_wal_recovery() {
        let temp_dir = tempdir().unwrap();
        let config = LSMConfig {
            memtable_size_limit: 10,  // Large limit to prevent auto-flush
            data_dir: temp_dir.path().to_path_buf(),
            background_compaction: false,  // Disable compaction for this test
            background_compaction_interval: Duration::from_secs(1),
            enable_wal: true,  // Enable WAL
        };

        // Phase 1: Insert data with WAL enabled
        {
            let mut lsm = LSMTree::with_config(config.clone()).unwrap();
            
            println!("=== Phase 1: Inserting data with WAL enabled ===");
            lsm.insert("key1".to_string(), "value1".to_string()).unwrap();
            lsm.insert("key2".to_string(), "value2".to_string()).unwrap();
            lsm.insert("key3".to_string(), "value3".to_string()).unwrap();
            
            // Delete one key to test tombstone recovery
            lsm.delete("key2").unwrap();
            
            let stats = lsm.stats();
            println!("Before 'crash': {}", stats);
            
            // Verify data is accessible
            assert_eq!(lsm.get("key1").unwrap(), Some("value1".to_string()));
            assert_eq!(lsm.get("key2").unwrap(), None); // Deleted
            assert_eq!(lsm.get("key3").unwrap(), Some("value3".to_string()));
            
            // Don't flush - simulate a crash where data is only in MemTable and WAL
            // LSMTree goes out of scope here, simulating a crash
        }
        
        // Phase 2: Recover from WAL
        {
            println!("=== Phase 2: Recovering from WAL after 'crash' ===");
            let lsm = LSMTree::with_config(config.clone()).unwrap();
            
            let stats_after_recovery = lsm.stats();
            println!("After WAL recovery: {}", stats_after_recovery);
            
            // Data should be recovered from WAL
            assert_eq!(lsm.get("key1").unwrap(), Some("value1".to_string()), "key1 should be recovered from WAL");
            assert_eq!(lsm.get("key2").unwrap(), None, "key2 should remain deleted after recovery");
            assert_eq!(lsm.get("key3").unwrap(), Some("value3".to_string()), "key3 should be recovered from WAL");
            
            // MemTable should contain the recovered data
            assert_eq!(stats_after_recovery.memtable_entries, 3); // key1, key2 (tombstone), key3
            assert_eq!(stats_after_recovery.sstable_count, 0); // No SSTables since we didn't flush
        }
        
        println!("WAL recovery test completed successfully!");
    }

    #[test]
    fn test_wal_disabled() {
        let temp_dir = tempdir().unwrap();
        let config = LSMConfig {
            memtable_size_limit: 10,
            data_dir: temp_dir.path().to_path_buf(),
            background_compaction: false,
            background_compaction_interval: Duration::from_secs(1),
            enable_wal: false,  // Disable WAL
        };

        // Phase 1: Insert data without WAL
        {
            let mut lsm = LSMTree::with_config(config.clone()).unwrap();
            
            println!("=== Testing WAL disabled ===");
            lsm.insert("key1".to_string(), "value1".to_string()).unwrap();
            lsm.insert("key2".to_string(), "value2".to_string()).unwrap();
            
            // Verify data is accessible
            assert_eq!(lsm.get("key1").unwrap(), Some("value1".to_string()));
            assert_eq!(lsm.get("key2").unwrap(), Some("value2".to_string()));
        }
        
        // Phase 2: After restart, data should be lost (no WAL)
        {
            let lsm = LSMTree::with_config(config.clone()).unwrap();
            
            // Data should be lost since WAL was disabled and we didn't flush
            assert_eq!(lsm.get("key1").unwrap(), None, "key1 should be lost without WAL");
            assert_eq!(lsm.get("key2").unwrap(), None, "key2 should be lost without WAL");
            
            let stats = lsm.stats();
            assert_eq!(stats.memtable_entries, 0);
            assert_eq!(stats.sstable_count, 0);
        }
        
        println!("WAL disabled test completed successfully!");
    }

    #[test]
    fn test_wal_with_flush() {
        let temp_dir = tempdir().unwrap();
        let config = LSMConfig {
            memtable_size_limit: 2,  // Small limit to trigger flush
            data_dir: temp_dir.path().to_path_buf(),
            background_compaction: false,
            background_compaction_interval: Duration::from_secs(1),
            enable_wal: true,
        };

        // Test that WAL works correctly with manual flush
        {
            let mut lsm = LSMTree::with_config(config.clone()).unwrap();
            
            println!("=== Testing WAL with manual flush ===");
            
            // Insert data but don't trigger auto-flush
            lsm.insert("key1".to_string(), "value1".to_string()).unwrap();
            lsm.insert("key2".to_string(), "value2".to_string()).unwrap();
            
            // Manually flush
            lsm.flush().unwrap();
            
            let stats = lsm.stats();
            println!("After manual flush: {}", stats);
            
            // Should have data in SSTable
            assert!(stats.sstable_count >= 1);
            assert_eq!(stats.memtable_entries, 0); // MemTable should be empty after flush
        }
        
        // Phase 2: After restart, data should be recovered from SSTables
        {
            let lsm = LSMTree::with_config(config.clone()).unwrap();
            
            // Data should be recovered from SSTables since WAL was truncated after flush
            assert_eq!(lsm.get("key1").unwrap(), Some("value1".to_string()));
            assert_eq!(lsm.get("key2").unwrap(), Some("value2".to_string()));
            
            let stats = lsm.stats();
            println!("After restart: {}", stats);
            assert!(stats.sstable_count >= 1);
            assert_eq!(stats.memtable_entries, 0); // No WAL entries to replay
        }
        
        println!("WAL with flush test completed successfully!");
    }

    #[test]
    fn test_leveled_compaction_integration() {
        let temp_dir = tempdir().unwrap();
        let config = LSMConfig {
            memtable_size_limit: 2,  // Small to trigger flushes
            data_dir: temp_dir.path().to_path_buf(),
            background_compaction: false, // Manual compaction for testing
            background_compaction_interval: Duration::from_secs(1),
            enable_wal: false,
        };

        let mut lsm = LSMTree::with_config(config).unwrap();

        println!("=== Testing Leveled Compaction Integration ===");
        
        // Insert enough data to trigger multiple levels
        for i in 1..=10 {
            lsm.insert(format!("key{:02}", i), format!("value{}", i)).unwrap();
        }

        // Manually trigger compaction
        {
            let mut level_manager = lsm.level_manager.write();
            let mut compactor = lsm.leveled_compactor.write();
            
            // Check if Level 0 needs compaction
            if level_manager.should_compact(0) {
                compactor.compact_level(&mut level_manager, 0).unwrap();
            }
        }

        // Verify data is still accessible
        for i in 1..=10 {
            let key = format!("key{:02}", i);
            let expected = format!("value{}", i);
            assert_eq!(lsm.get(&key).unwrap(), Some(expected));
        }

        let stats = lsm.stats();
        println!("Final stats: {}", stats);
        
        println!("Leveled compaction integration test passed!");
    }
}