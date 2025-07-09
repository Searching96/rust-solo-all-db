pub mod engine;

use std::collections::BTreeMap;

// A simple in-memory key-value store using a BTreeMa
#[derive(Debug, Default)]
pub struct MemTable {
    data: BTreeMap<String, String>,
}

#[derive(Debug, PartialEq)]
pub enum DbError {
    KeyNotFound(String),
    InvalidOperation(String),
}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbError::KeyNotFound(key) => write!(f, "Key not found: {}", key),
            DbError::InvalidOperation(msg) => write!(f, "Invalid operation: {}", msg),
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
        self.data.insert(key, value);
        Ok(())
    }

    pub fn get(&self, key: &str) -> DbResult<&String> {
        self.data
            .get(key)
            .ok_or_else(|| DbError::KeyNotFound(key.to_string()))
    }

    pub fn delete(&mut self, key: &str) -> DbResult<String> {
        self.data
            .remove(key)
            .ok_or_else(|| DbError::KeyNotFound(key.to_string()))
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
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

