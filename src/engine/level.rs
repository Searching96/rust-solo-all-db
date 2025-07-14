use crate::engine::SSTable;
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct LevelManager {
    levels: BTreeMap<usize, Vec<SSTable>>, // level -> SSTables
    max_level: usize,
    level_size_multiplier: usize, // Usually 10
    level_0_file_limit: usize, // Trigger compaction
}

impl LevelManager {
    pub fn new() -> Self {
        Self {
            levels: BTreeMap::new(),
            max_level: 0,
            level_size_multiplier: 10,
            level_0_file_limit: 4,
        }
    }

    pub fn with_config(level_0_file_limit: usize, level_size_multiplier: usize) -> Self {
        Self {
            levels: BTreeMap::new(),
            max_level: 0,
            level_size_multiplier,
            level_0_file_limit,
        }
    }

    pub fn add_sstable(&mut self, sstable:  SSTable, level: usize) {
        // Update max level if necessary
        if level > self.max_level {
            self. max_level = level;
        }

        // Add SSTable to the specified level
        self.levels.entry(level).or_insert_with(Vec::new).push(sstable);

        // Sort Level 1+ by min_key to maintain order (Level 0 can be unsorted)
        if level > 0 {
            if let Some(level_sstables) = self.levels.get_mut(&level) {
                level_sstables.sort_by(|a, b| a.min_key().cmp(b.min_key()));
            }
        }
    }

    pub fn get_sstables_at_level(&self, level: usize) -> Vec<SSTable> {
        self.levels.get(&level).cloned().unwrap_or_default()
    }

    pub fn get_all_sstables(&self) -> Vec<SSTable> {
        let mut all_sstables = Vec::new();

        // Return in level order (Level 0 first, then Level 1, etc.)
        for level in 0..=self.max_level {
            if let Some(level_sstables) = self.levels.get(&level) {
                all_sstables.extend(level_sstables.clone());
            }
        }

        all_sstables
    }

    pub fn should_compact(&self, level: usize) -> bool {
        match level {
            0 => {
                // Level 0: Check file count
                self.levels.get(&0).map_or(false, |files| files.len() >= self.level_0_file_limit)
            }
            _ => {
                // Level 1+: Check total size
                let level_size = self.get_level_size(level);
                let max_size = self.get_max_level_size(level);
                level_size >= max_size
            }
        }
    }

    pub fn get_compaction_candidates(&self, level: usize) -> Vec<SSTable> {
        match level {
            0 => {
                // Level 0: All files (since they can overlap)
                self.get_sstables_at_level(0)
            }
            _ => {
                // Level 1+: All files that exceed size limit
                let level_sstables = self.get_sstables_at_level(level);
                let max_size = self.get_max_level_size(level);
                let current_size = self.get_level_size(level);

                if current_size < max_size {
                    return Vec::new();
                }

                // For now, compact all files at this level
                // In a real implementation, you would select specific files
                level_sstables
            }
        }
    }

    pub fn get_overlapping_sstables(&self, level: usize, min_key: &str, max_key: &str) -> Vec<SSTable> {
        let level_sstables = self.get_sstables_at_level(level);
        let mut overlapping = Vec::new();

        for sstable in level_sstables {
            // Check if key ranges overlap
            if sstable.max_key() >= min_key && sstable.min_key() <= max_key {
                overlapping.push(sstable);
            }
        }

        overlapping
    }

    pub fn remove_sstables(&mut self, sstables_to_remove: &[SSTable]) {
        for sstable in sstables_to_remove {
            let level = sstable.level();
            if let Some(level_sstables) = self.levels.get_mut(&level) {
                level_sstables.retain(|s| s.file_path() != sstable.file_path());
                
                // Clean up empty levels
                if level_sstables.is_empty() {
                    self.levels.remove(&level);
                }
            }
        }
        
        // Update max_level
        self.max_level = self.levels.keys().max().copied().unwrap_or(0);
    }

    pub fn get_level_size(&self, level: usize) -> usize {
        self.levels.get(&level)
            .map(|sstables| sstables.iter().map(|s| s.len()).sum())
            .unwrap_or(0)
    }

    pub fn get_max_level_size(&self, level: usize) -> usize {
        match level {
            0 => self.level_0_file_limit, // Level 0 is measured by file count
            1 => 10 * 1024 * 1024, // 10MB for Level 1
            _ => {
                // Each level is level_size_multiplier times larger than the previous
                let level_1_size = 10 * 1024 * 1024;
                level_1_size * self.level_size_multiplier.pow((level - 1) as u32)
            }
        }
    }

    pub fn get_level_count(&self, level: usize) -> usize {
        self.levels.get(&level).map_or(0, |files| files.len())
    }

    pub fn get_max_level(&self) -> usize {
        self.max_level
    }

    pub fn stats(&self) -> LevelManagerStats {
        let mut level_stats = BTreeMap::new();

        for level in 0..=self.max_level {
            let count = self.get_level_count(level);
            let size = self.get_level_size(level);
            let max_size = self.get_max_level_size(level);

            level_stats.insert(level, LevelStats {
                file_count: count,
                total_size: size,
                max_size,
                should_compact: self.should_compact(level),
            });
        }

        LevelManagerStats {
            max_level: self.max_level,
            level_stats,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.levels.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct LevelStats {
    pub file_count: usize,
    pub total_size: usize,
    pub max_size: usize,
    pub should_compact: bool,
}

#[derive(Debug)]
pub struct LevelManagerStats {
    pub max_level: usize,
    pub level_stats: BTreeMap<usize, LevelStats>,
}

impl std::fmt::Display for LevelManagerStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Level Manager Stats:")?;
        writeln!(f, " Max level: {}", self.max_level)?;

        for (level, stats) in &self.level_stats {
            writeln!(f, "  Level {}: {} files, {} entries, max: {} (compact: {})", 
                level, 
                stats.file_count, 
                stats.total_size, 
                stats.max_size,
                stats.should_compact
            )?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use std::collections::BTreeMap;
    use crate::Value;

    fn create_test_sstable(level: usize, min_key: &str, max_key: &str) -> SSTable {
        let temp_dir = tempdir().unwrap();
        let sstable_path = temp_dir.path().join(format!("test_level_{}.sst", level));
        
        // Create test data
        let mut data = BTreeMap::new();
        data.insert(min_key.to_string(), Value::Data(format!("value_{}", min_key)));
        data.insert(max_key.to_string(), Value::Data(format!("value_{}", max_key)));
        
        SSTable::create_with_level(&sstable_path, &data, level).unwrap()
    }

    #[test]
    fn test_level_manager_basic() {
        let mut manager = LevelManager::new();
        
        // Add some SSTables
        let sstable1 = create_test_sstable(0, "key1", "key2");
        let sstable2 = create_test_sstable(1, "key3", "key4");
        
        manager.add_sstable(sstable1, 0);
        manager.add_sstable(sstable2, 1);
        
        assert_eq!(manager.get_level_count(0), 1);
        assert_eq!(manager.get_level_count(1), 1);
        assert_eq!(manager.get_max_level(), 1);
    }

    #[test]
    fn test_compaction_trigger() {
        let mut manager = LevelManager::with_config(2, 10); // Small limit for testing
        
        // Add files to Level 0
        for i in 0..3 {
            let sstable = create_test_sstable(0, &format!("key{}", i), &format!("key{}", i+1));
            manager.add_sstable(sstable, 0);
        }
        
        // Should trigger compaction after 2 files
        assert!(manager.should_compact(0));
        
        let candidates = manager.get_compaction_candidates(0);
        assert_eq!(candidates.len(), 3); // All Level 0 files
    }

    #[test]
    fn test_overlapping_sstables() {
        let mut manager = LevelManager::new();
        
        // Add SSTables with different key ranges
        let sstable1 = create_test_sstable(1, "a", "f");
        let sstable2 = create_test_sstable(1, "g", "m");
        let sstable3 = create_test_sstable(1, "n", "z");
        
        manager.add_sstable(sstable1, 1);
        manager.add_sstable(sstable2, 1);
        manager.add_sstable(sstable3, 1);
        
        // Test overlapping range
        let overlapping = manager.get_overlapping_sstables(1, "e", "h");
        assert_eq!(overlapping.len(), 2); // Should overlap with first two SSTables
    }

    #[test]
    fn test_stats() {
        let mut manager = LevelManager::new();
        
        let sstable1 = create_test_sstable(0, "key1", "key2");
        let sstable2 = create_test_sstable(1, "key3", "key4");
        
        manager.add_sstable(sstable1, 0);
        manager.add_sstable(sstable2, 1);
        
        let stats = manager.stats();
        println!("{}", stats);
        
        assert_eq!(stats.max_level, 1);
        assert_eq!(stats.level_stats.len(), 2);
    }
}