// LSM Tree implementation - coordinates MemTable and SSTables

use crate::Value;
use crate::{DbError, DbResult, MemTable};
use super::SSTable;
use super::Compactor;
use std::path::{Path, PathBuf};
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;
use parking_lot::RwLock;
use crossbeam_channel::{Receiver, Sender, unbounded};



#[derive(Debug, Clone)]
pub struct LSMConfig {
    pub memtable_size_limit: usize,
    pub data_dir: PathBuf,
    pub background_compaction: bool,
    pub background_compaction_interval: Duration,
}

impl Default for LSMConfig {
    fn default() -> Self {
        Self {
            memtable_size_limit: 1000, // Flush after 1000 entries
            data_dir: PathBuf::from("data"),
            background_compaction: true, // Enable background compaction by default
            background_compaction_interval: Duration::from_secs(10),
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
    thread_handle: Option<thread::JoinHandle<()>>,
}

impl CompactionHandle {
    pub fn send_check_compaction(&self) {
        let _ = self.sender.send(CompactionMessage::CheckCompaction);
    }

    pub fn shutdown(mut self) {
        let _ = self.sender.send(CompactionMessage::ShutDown);
        if let Some(handle) = self.thread_handle.take() {
            let _ = handle.join();
        }
    }
}

// LSM Tree - coordinates MemTable and multiple SSTables
#[derive(Debug)]
pub struct LSMTree {
    memtable: Arc<RwLock<MemTable>>,
    sstables: Arc<RwLock<Vec<SSTable>>>,
    config: LSMConfig,
    next_sstable_id: Arc<AtomicU64>, // A thread-safe counter for generating unique SSTable filenames
    compaction_handle: Option<CompactionHandle>,
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

        let sstables = Self::load_existing_sstables(&config.data_dir)?;
        let next_sstable_id = Self::determine_next_id(&sstables);

        let memtable = Arc::new(RwLock::new(MemTable::new()));
        let sstables = Arc::new(RwLock::new(sstables));
        let next_sstable_id = Arc::new(AtomicU64::new(next_sstable_id));

        // Start background compaction thread if enabled
        let compaction_handle = if config.background_compaction {
            Some(Self::start_background_compaction(
                sstables.clone(),
                config.clone(),
                next_sstable_id.clone(),
            )?)
        } else {
            None
        };

        Ok(Self {
            memtable,
            sstables,
            config,
            next_sstable_id,
            compaction_handle,
        })
    }

    fn start_background_compaction(
        sstables: Arc<RwLock<Vec<SSTable>>>,
        config: LSMConfig,
        next_sstable_id: Arc<AtomicU64>,
    ) -> DbResult<CompactionHandle> {
        let (sender, receiver) = unbounded();


        let thread_handle = thread::spawn(move || {
            Self::background_compaction_loop(sstables, config, next_sstable_id, receiver);
        });

        Ok(CompactionHandle {
            sender,
            thread_handle: Some(thread_handle),
        })
    }

    fn background_compaction_loop(
        sstables: Arc<RwLock<Vec<SSTable>>>,
        config: LSMConfig,
        next_sstable_id: Arc<AtomicU64>,
        receiver: Receiver<CompactionMessage>,
    ) {
        loop {
            // Wait for a message or timeout
            match receiver.recv_timeout(config.background_compaction_interval) {
                Ok(CompactionMessage::CheckCompaction) => {
                    // Explicit compaction request
                    Self::try_background_compaction(&sstables, &config, &next_sstable_id);
                }
                Ok(CompactionMessage::ShutDown) => {
                    println!("Background compaction thread shutting down...");
                    break;
                }
                Err(_) => {
                    // Timeout occurred, check if compaction is needed
                    Self::try_background_compaction(&sstables, &config, &next_sstable_id);
                }
            }
        }
    }

    fn try_background_compaction(
        sstables: &Arc<RwLock<Vec<SSTable>>>,
        config: &LSMConfig,
        next_sstable_id: &Arc<AtomicU64>,
    ) {
        let compactor = Compactor::new(config.data_dir.clone());

        // Check if compaction is needed
        let should_compact = {
            let sstables_guard = sstables.read();
            compactor.should_compact(sstables_guard.len())
        };

        if should_compact {
            print!("Background compaction starting...");

            let result = {
                let mut sstables_guard = sstables.write();
                if sstables_guard.len() < 2 {
                    println!("Skipping compaction: only {} SSTables", sstables_guard.len());
                    return;
                }

                let old_sstables = sstables_guard.clone();
                let current_id = next_sstable_id.fetch_add(1, Ordering::SeqCst);

                match compactor.compact_sstables(&old_sstables, current_id) {
                    Ok(compacted_sstable) => {
                        // Replace old SSTables with the new compacted one
                        *sstables_guard = vec![compacted_sstable];
                        Ok(old_sstables)
                    }
                    Err(e) => {
                        println!("Background compaction failed: {}", e);
                        Err(e)
                    }
                }
            };

            // Clean up old files if compaction was successful
            if let Ok(old_sstables) = result {
                if let Err(e) = compactor.cleanup_old_sstables(&old_sstables) {
                    println!("failed to cleanup old SSTables: {}", e);
                } else {
                    println!("Background compaction completed successfully!");
                }
            }
        }
    }

    pub fn insert(&mut self, key: String, value: String) -> DbResult<()> {
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

        // Check SSTables (newest first)
        let sstables = self.sstables.read();
        for sstable in sstables.iter() {
            // Check if this SSTable contains the key by scanning its records
            let records = sstable.scan()?;
            for record in records {
                if record.key == key {
                    match &record.value {
                        Value::Data(s) => return Ok(Some(s.clone())),
                        Value::Tombstone => return Ok(None), // Key is deleted
                    }
                }
                // Early termination since records are sorted
                if record.key.as_str() > key {
                    break;
                }
            }
        }

        Ok(None)
    }

    pub fn delete(&mut self, key: &str) -> DbResult<bool> {
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
        let sstables = self.sstables.read();
        
        LSMStats {
            memtable_entries: memtable.len(),
            sstable_count: sstables.len(),
            total_sstable_entries: sstables.iter().map(|sst| sst.len()).sum(),
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
        
        // Create new SSTable
        let sstable = SSTable::create(&filepath, &memtable_data)?;

        // Add to our list of SSTables (newest first)
        {
            let mut sstables = self.sstables.write();
            sstables.insert(0, sstable);
        }

        // Clear MemTable
        {
            let mut memtable = self.memtable.write();
            *memtable = MemTable::new();
        }

        // Handle compaction based on configuration
        if self.config.background_compaction {
            // Background compaction is enabled, let the background thread handle it
            // We could optionally send a signal to the background thread here
            if let Some(ref handle) = self.compaction_handle {
                handle.send_check_compaction();
            }
        } else {
            // Background compaction is disabled - skip all compaction for testing
            // In a real system, you might want synchronous compaction here instead
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

    // Trigger compaction if needed
    pub fn maybe_compact(&mut self) -> DbResult<()> {
        let compactor = Compactor::new(self.config.data_dir.clone());

        let sstable_count = {
            let sstables = self.sstables.read();
            sstables.len()
        };

        if compactor.should_compact(sstable_count) {
            println!("Compaction triggered: {} SSTables", sstable_count);
            self.compact()?;
        }
        
        Ok(())
    }

    // Force compaction of SSTables
    pub fn compact(&mut self) -> DbResult<()> {
        let sstable_count = {
            let sstables = self.sstables.read();
            sstables.len()
        };

        if sstable_count < 2 {
            println!("Skipping compaction: only {} SSTables", sstable_count);
            return Ok(());
        }
    
        let compactor = Compactor::new(self.config.data_dir.clone());

        let old_sstables = {
            let sstables = self.sstables.read();
            sstables.clone()
        };

        let current_id = self.next_sstable_id.fetch_add(1, Ordering::SeqCst);
        let compacted_sstable = compactor.compact_sstables(&old_sstables, current_id)?;

        // Replace old SSTables with the new compacted one
        {
            let mut sstables = self.sstables.write();
            *sstables = vec![compacted_sstable];
        }

        compactor.cleanup_old_sstables(&old_sstables)?;
        println!("Compaction successful!");
        Ok(())
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
        lsm.compact().unwrap();
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
    //     assert_eq!(stats.total_sstable_entries, 2); // key1, key2
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
}