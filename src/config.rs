use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    pub storage: StorageConfig,
    pub etl: EtlConfig,
    pub query: QueryConfig,
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub data_dir: PathBuf,
    pub memtable_size_limit: usize,
    pub enable_wal: bool,
    pub background_compaction: bool,
    pub compaction_interval_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EtlConfig {
    pub batch_size: usize,
    pub parallel_threads: usize,
    pub delimiter: char,
    pub hash_headers: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryConfig {
    pub enable_query_cache: bool,
    pub max_result_size: usize,
    pub query_timeout_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
    pub enable_performance_metrics: bool,
    pub log_file: Option<PathBuf>,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            storage: StorageConfig {
                data_dir: PathBuf::from("data"),
                memtable_size_limit: 1000,
                enable_wal: true,
                background_compaction: false,
                compaction_interval_secs: 60,
            },
            etl: EtlConfig {
                batch_size: 1000,
                parallel_threads: 4,
                delimiter: ',',
                hash_headers: true,
            },
            query: QueryConfig {
                enable_query_cache: true,
                max_result_size: 10000,
                query_timeout_secs: 30,
            },
            logging: LoggingConfig {
                level: "info".into(),
                enable_performance_metrics: false,
                log_file: None,
            },
        }
    }
}

impl DatabaseConfig {
    pub fn load_from_file(path: &PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let config: DatabaseConfig = serde_yaml::from_str(&content)?;
        Ok(config)
    }

    pub fn save_to_file(&self, path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
        let content = serde_yaml::to_string(self)?;
        std::fs::write(path, content)?;
        Ok(())
    }

    pub fn to_lsm_config(&self) -> crate::engine::LSMConfig {
        crate::engine::LSMConfig {
            memtable_size_limit: self.storage.memtable_size_limit,
            data_dir: self.storage.data_dir.clone(),
            background_compaction: self.storage.background_compaction,
            background_compaction_interval: Duration::from_secs(self.storage.compaction_interval_secs),
            enable_wal: self.storage.enable_wal,
        }
    }
}