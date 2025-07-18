pub mod engine;
pub mod cli;
pub mod etl;
pub mod query;
pub mod config;
pub mod args;
pub mod metrics;

use std::collections::BTreeMap;
use serde::{Serialize, Deserialize};

pub use config::DatabaseConfig;
pub use args::{Cli, Commands};
pub use metrics::PerformanceMetrics;

// A simple in-memory key-value store using a BTreeMa
#[derive(Debug, Default)]
pub struct MemTable {
    data: BTreeMap<String, Value>,
}

#[derive(Debug, PartialEq)]
pub enum DbError {
    KeyNotFound(String),
    InvalidOperation(String),
    InvalidQuery(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Data(String),
    Tombstone,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WALEntry {
    Insert { key: String, value: String },
    Delete { key: String },
}

impl WALEntry {
    pub fn key(&self) -> &str {
        match self {
            WALEntry::Insert {key, ..} => key,
            WALEntry::Delete {key} => key,
        }
    }
}

impl Value {
    pub fn is_tombstone(&self) -> bool {
        matches!(self, Value::Tombstone)
    }

    pub fn as_data(&self) -> Option<&String> {
        match self {
            Value::Data(s) => Some(s),
            Value::Tombstone => None,
        }
    }
}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbError::KeyNotFound(key) => write!(f, "Key not found: {}", key),
            DbError::InvalidOperation(msg) => write!(f, "Invalid operation: {}", msg),
            DbError::InvalidQuery(msg) => write!(f, "Invalid query: {}", msg),
        }
    }
}

impl std::error::Error for DbError {

}

// Result type for db operations
pub type DbResult<T> = Result<T, DbError>;

impl MemTable {
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, key: String, value: String) -> DbResult<()> {
        self.data.insert(key, Value::Data(value));
        Ok(())
    }

    pub fn insert_tombstone(&mut self, key: String) -> DbResult<()> {
        self.data.insert(key, Value::Tombstone);
        Ok(())
    }

    pub fn get(&self, key: &str) -> DbResult<&String> {
        match self.data.get(key) {
            Some(Value::Data(s)) => Ok(s),
            Some(Value::Tombstone) => Err(DbError::KeyNotFound(key.to_string())),
            None => Err(DbError::KeyNotFound(key.to_string())),
        }
    }

    pub fn delete(&mut self, key: &str) -> DbResult<String> {
        match self.data.get(key) {
            Some(Value::Data(s)) => {
                let value = s.clone();
                self.data.insert(key.to_string(), Value::Tombstone);
                Ok(value)
            }
            Some(Value::Tombstone) => Err(DbError::KeyNotFound(key.to_string())),
            None => {
                // Key not in MemTable, insert tombstone anyway (might be in SSTable)
                self.data.insert(key.to_string(), Value::Tombstone);
                Ok("".to_string()) // We dont know the original value
            }
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn data(&self) -> &BTreeMap<String, Value> {
        &self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_multiple_entries() {
        let mut db = MemTable::new();

        for i in 0..5 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            assert!(db.insert(key, value).is_ok());
        }

        assert_eq!(db.len(), 5);
        assert!(!db.is_empty());

        for i in 0..5 {
            let key = format!("key{}", i);
            let expected_value = format!("value{}", i);
            assert_eq!(db.get(&key).unwrap(), &expected_value);
        }
    }
}

