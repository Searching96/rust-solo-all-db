// LSM Tree implementation - coordinates MemTable and SSTables

use crate::{DbError, DbResult, MemTable};
use super::SSTable;
use super::Compactor;
use std::path::{Path, PathBuf};
use std::fs;

#[derive(Debug, Clone)]
pub struct LSMConfig {
    pub memtable_size_limit: usize,
    pub data_dir: PathBuf,
}

impl Default for LSMConfig {
    fn default() -> Self {
        Self {
            memtable_size_limit: 1000, // Flush after 1000 entries
            data_dir: PathBuf::from("data"),
        }
    }
}

// LSM Tree - coordinates MemTable and multiple SSTables
#[derive(Debug)]
pub struct LSMTree {
    memtable: MemTable,
    sstables: Vec<SSTable>,
    config: LSMConfig,
    next_sstable_id: u64, // Counter for generating unique SSTable filenames
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

        Ok(Self {
            memtable: MemTable::new(),
            sstables,
            config,
            next_sstable_id
        })
    }

    pub fn insert(&mut self, key: String, value: String) -> DbResult<()> {
        self.memtable.insert(key, value)?;

        // Check if we need to flush
        if self.memtable.len() >= self.config.memtable_size_limit {
            self.flush_memtable()?;
        }

        Ok(())
    }

    pub fn get(&self, key: &str) -> DbResult<Option<String>> {
        // First check the MemTable (most recent data)
        match self.memtable.get(key) {
            Ok(value) => return Ok(Some(value.clone())),
            Err(DbError::KeyNotFound(_)) => {
                // Key not found in MemTable, check SSTables
            }
            Err(e) => return Err(e),
        }

        for sstable in &self.sstables {
            match sstable.get(key)? {
                Some(value) => return Ok(Some(value)),
                None => continue,
            }
        }

        Ok(None)
    }

    pub fn delete(&mut self, key: &str) -> DbResult<bool> {
        // For now, we'll implement a simple delete by removing from MemTable
        // In a full LSM implementation, we'd use tombstone markers
        match self.memtable.delete(key) {
            Ok(_) => Ok(true),
            Err(DbError::KeyNotFound(_)) => {
                // Key might be in SSTables, but we can't delete from immutable SSTables
                // For now, return false. In a full implementation, we'd add a tombstone.
                Ok(false)
            }
            Err(e) => Err(e),
        }
    }

    pub fn stats(&self) -> LSMStats {
        LSMStats {
            memtable_entries: self.memtable.len(),
            sstable_count: self.sstables.len(),
            total_sstable_entries: self.sstables.iter().map(|sst| sst.len()).sum(),
            next_flush_at: self.config.memtable_size_limit,
        }
    }

    // Force flush MemTable to SSTable (for testing or shutdown)
    pub fn flush(&mut self) -> DbResult<()> {
        if !self.memtable.is_empty() {
            self.flush_memtable()?;
        }
        Ok(())
    }

    // Internal: Flush current MemTable to a new SSTable
    fn flush_memtable(&mut self) -> DbResult<()> {
        if self.memtable.is_empty() {
            return Ok(());
        }

        let filename = format!("sstable_{:06}.sst", self.next_sstable_id);
        let filepath = self.config.data_dir.join(filename);

        // Create SSTable from MemTable data
        // We need to extract the BTreeMap from MemTable
        // For now, we'll create a new BTreeMap from MemTable entries
        let memtable_data = self.memtable.data();

        // We need a way to iterate over MemTable entries
        // Let's add this method to MemTable later, for now we'll work around it
        
        // TODO: This is a temporary solution - we'll need to add an iterator to MemTable
        // For now, let's create the SSTable with placeholder approach

        println!("Flushing MemTable with {} entries to {}", 
            self.memtable.len(), filepath.display());
        
        // Create new SSTable (we'll fix the data extraction in next step)
        let sstable = SSTable::create(&filepath, &memtable_data)?;

        // Add to our list of SSTables (newest first)
        self.sstables.insert(0, sstable);

        // Clear MemTable and increment SSTable ID
        self.memtable = MemTable::new();
        self.next_sstable_id += 1;

        // Check if compaction is needed after flush
        self.maybe_compact()?;

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

        if compactor.should_compact(self.sstables.len()) {
            println!("Compaction triggered: {} SSTables", self.sstables.len());
            self.compact()?;
        }
        
        Ok(())
    }

    // Force compaction of SSTables
    pub fn compact(&mut self) -> DbResult<()> {
        if self.sstables.len() < 2 {
            println!("Skipping compaction: only {} SSTables", self.sstables.len());
            return Ok(());
        }
    
        let compactor = Compactor::new(self.config.data_dir.clone());

        let old_sstables = self.sstables.clone();
        let compacted_sstable = compactor.compact_sstables(&old_sstables, self.next_sstable_id)?;

        // Replace old SSTables with the new compacted one
        self.sstables = vec![compacted_sstable];
        self.next_sstable_id += 1;

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

    #[test]
    fn test_lsm_basic_operations() {
        let temp_dir = tempdir().unwrap();
        let config = LSMConfig {
            memtable_size_limit: 3,  // Small limit for testing
            data_dir: temp_dir.path().to_path_buf(),
        };

        let mut lsm = LSMTree::with_config(config).unwrap();

        // Insert some data
        lsm.insert("key1".to_string(), "value1".to_string()).unwrap();
        lsm.insert("key2".to_string(), "value2".to_string()).unwrap();

        // Should be in MemTable
        assert_eq!(lsm.get("key1").unwrap(), Some("value1".to_string()));
        assert_eq!(lsm.get("key2").unwrap(), Some("value2".to_string()));

        let stats = lsm.stats();
        assert_eq!(stats.memtable_entries, 2);
        assert_eq!(stats.sstable_count, 0);
    }

    #[test]
    fn test_lsm_flush_on_size() {
        let temp_dir = tempdir().unwrap();
        let config = LSMConfig {
            memtable_size_limit: 2,  // Very small limit
            data_dir: temp_dir.path().to_path_buf(),
        };

        let mut lsm = LSMTree::with_config(config).unwrap();

        // Insert data to trigger flush
        lsm.insert("key1".to_string(), "value1".to_string()).unwrap();
        lsm.insert("key2".to_string(), "value2".to_string()).unwrap();

        let stats_before = lsm.stats();
        println!("Before flush: {}", stats_before);

        // This should trigger a flush
        lsm.insert("key3".to_string(), "value3".to_string()).unwrap();

        let stats_after = lsm.stats();
        println!("After flush: {}", stats_after);

        // MemTable should have been flushed and now contains only key3
        assert_eq!(stats_after.memtable_entries, 1);  // Only key3
        // Note: SSTable creation will be fixed in the next step
    }

    #[test]
    fn test_lsm_flush_and_read_back() {
        let temp_dir = tempdir().unwrap();
        let config = LSMConfig {
            memtable_size_limit: 2,
            data_dir: temp_dir.path().to_path_buf(),
        };

        let mut lsm = LSMTree::with_config(config).unwrap();

        // Insert data to trigger flush
        lsm.insert("key1".to_string(), "value1".to_string()).unwrap();
        lsm.insert("key2".to_string(), "value2".to_string()).unwrap();
        
        // This should trigger flush
        lsm.insert("key3".to_string(), "value3".to_string()).unwrap();

        // Verify we can read all data (from both MemTable and SSTable)
        assert_eq!(lsm.get("key1").unwrap(), Some("value1".to_string())); // From SSTable
        assert_eq!(lsm.get("key2").unwrap(), Some("value2".to_string())); // From SSTable  
        assert_eq!(lsm.get("key3").unwrap(), Some("value3".to_string())); // From MemTable

        let stats = lsm.stats();
        println!("Final stats: {}", stats);
        assert_eq!(stats.memtable_entries, 1);  // key3
        assert_eq!(stats.sstable_count, 1);     // one SSTable file
        assert_eq!(stats.total_sstable_entries, 2); // key1, key2
    }
}