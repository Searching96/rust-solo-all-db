use crate::query::ast::*;
use crate::engine::LSMTree;
use crate::{DbResult, DbError};
use std::collections::HashMap;

pub struct QueryExecutor<'a> {
    lsm_tree: &'a mut LSMTree,
}

impl<'a> QueryExecutor<'a> {
    pub fn new(lsm_tree: &'a mut LSMTree) -> Self {
        Self { lsm_tree }
    }

    pub fn execute(&mut self, statement: Statement) -> DbResult<QueryResult> {
        match statement {
            Statement::Select(select) => self.execute_select(select),
            Statement::Insert(insert) => self.execute_insert(insert),
            Statement::Delete(delete) => self.execute_delete(delete),
        }
    }

    fn execute_select(&mut self, select: SelectStatement) -> DbResult<QueryResult> {
        // For simplicity, we'll implement a basic key-value lookup
        // In a real implementation, we'd have a proper schema system
    
        if select.columns.contains(&"*".to_string()) {
            // Simple key lookup
            if let Some(where_clause) = &select.where_clause {
                if let Some(key) = self.extract_key_from_condition(&where_clause.condition)? {
                    match self.lsm_tree.get(&key)? {
                        Some(value) => {
                            let mut record = HashMap::new();
                            record.insert("key".to_string(), key);
                            record.insert("value".to_string(), value);

                            Ok(QueryResult::Select(vec![record]))
                        }
                        None => Ok(QueryResult::Select(vec![])),
                    }
                } else {
                    Err(DbError::InvalidOperation(
                        "Complex WHERE clauses not supported yet".to_string()
                    ))
                }
            } else {
                // No WHERE clause is not practical for large datasets
                Err(DbError::InvalidOperation(
                    "SELECT without WHERE clause is not supported (would return all data)".to_string()
                ))
            }
        } else {
            Err(DbError::InvalidOperation(
                "Multi-column SELECT not supported in this key-value implementation".to_string()
            ))
        }
    }

    fn execute_insert(&mut self, insert: InsertStatement) -> DbResult<QueryResult> {
        // For key-value store, we expect key and value columns
        if insert.columns.len() != 2 {
            return Err(DbError::InvalidOperation(
                "INSERT requires exactly 2 columns: key and value".to_string(),
            ));
        }

        if insert.values.len() != 2 {
            return Err(DbError::InvalidOperation(
                "INSERT requires exactly 2 values".to_string(),
            ));
        }

        let key = match &insert.values[0] {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            _ => return Err(DbError::InvalidOperation(
                "Key must be a string or number".to_string(),
            )),
        };

        let value = match &insert.values[1] {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            Value::Boolean(b) => b.to_string(),
            Value::Null => "NULL".to_string(),
        };

        self.lsm_tree.insert(key, value)?;
        Ok(QueryResult::Insert(1))
    }

    fn execute_delete(&mut self, delete: DeleteStatement) -> DbResult<QueryResult> {
        if let Some(where_clause) = &delete.where_clause {
            if let Some(key) = self.extract_key_from_condition(&where_clause.condition)? {
                let deleted = self.lsm_tree.delete(&key)?;
                Ok(QueryResult::Delete(if deleted { 1 } else { 0 }))
            } else {
                Err(DbError::InvalidOperation(
                    "Complex WHERE clauses not supported in DELETE".to_string(),
                ))
            }
        } else {
            Err(DbError::InvalidOperation(
                "DELETE without WHERE clause is not supported".to_string(),
            ))
        }
    }

    fn extract_key_from_condition(&self, condition: &Condition) -> DbResult<Option<String>> {
        match condition {
            Condition::Equals(column, value) => {
                if column == "key" {
                    match value {
                        Value::String(s) => Ok(Some(s.clone())),
                        Value::Number(n) => Ok(Some(n.to_string())),
                        _ => Ok(None),
                    }
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None), // Only simple equality conditions supported for now
        }       
    }
}

pub enum QueryResult {
    Select(Vec<HashMap<String, String>>),
    Insert(usize),
    Delete(usize),
}

impl QueryResult {
    pub fn format(&self) -> String {
        match self {
            QueryResult::Select(records) => {
                if records.is_empty() {
                    "No records found".to_string()
                } else {
                    let mut result = String::new();
                    for (i, record) in records.iter().enumerate() {
                        if i > 0 {
                            result.push('\n');
                        }
                        for (key, value) in record {
                            result.push_str(&format!("{}: {}", key, value));
                        }
                    }
                    result
                }
            }
            QueryResult::Insert(count) => format!("Inserted {} record(s)", count),
            QueryResult::Delete(count) => format!("Deleted {} record(s)", count),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{LSMTree, LSMConfig};
    use tempfile::tempdir;

    #[test]
    fn test_execute_insert() {
        let temp_dir = tempdir().unwrap();
        let mut config = LSMConfig::default();
        config.data_dir = temp_dir.path().to_path_buf();
        config.enable_wal = false;
        
        let mut lsm_tree = LSMTree::with_config(config).unwrap();
        let mut executor = QueryExecutor::new(&mut lsm_tree);

        let insert = InsertStatement {
            table: "users".to_string(),
            columns: vec!["key".to_string(), "value".to_string()],
            values: vec![Value::String("user1".to_string()), Value::String("Alice".to_string())],
        };

        let result = executor.execute(Statement::Insert(insert)).unwrap();
        
        if let QueryResult::Insert(count) = result {
            assert_eq!(count, 1);
        } else {
            panic!("Expected Insert result");
        }

        // Verify the data was inserted
        assert_eq!(lsm_tree.get("user1").unwrap(), Some("Alice".to_string()));
    }

    #[test]
    fn test_execute_select() {
        let temp_dir = tempdir().unwrap();
        let mut config = LSMConfig::default();
        config.data_dir = temp_dir.path().to_path_buf();
        config.enable_wal = false;
        
        let mut lsm_tree = LSMTree::with_config(config).unwrap();
        lsm_tree.insert("user1".to_string(), "Alice".to_string()).unwrap();
        
        let mut executor = QueryExecutor::new(&mut lsm_tree);

        let select = SelectStatement {
            columns: vec!["*".to_string()],
            table: "users".to_string(),
            where_clause: Some(WhereClause {
                condition: Condition::Equals("key".to_string(), Value::String("user1".to_string())),
            }),
            limit: None,
        };

        let result = executor.execute(Statement::Select(select)).unwrap();
        
        if let QueryResult::Select(records) = result {
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].get("key"), Some(&"user1".to_string()));
            assert_eq!(records[0].get("value"), Some(&"Alice".to_string()));
        } else {
            panic!("Expected Select result");
        }
    }

    #[test]
    fn test_execute_delete() {
        let temp_dir = tempdir().unwrap();
        let mut config = LSMConfig::default();
        config.data_dir = temp_dir.path().to_path_buf();
        config.enable_wal = false;
        
        let mut lsm_tree = LSMTree::with_config(config).unwrap();
        lsm_tree.insert("user1".to_string(), "Alice".to_string()).unwrap();
        
        let mut executor = QueryExecutor::new(&mut lsm_tree);

        let delete = DeleteStatement {
            table: "users".to_string(),
            where_clause: Some(WhereClause {
                condition: Condition::Equals("key".to_string(), Value::String("user1".to_string())),
            }),
        };

        let result = executor.execute(Statement::Delete(delete)).unwrap();
        
        if let QueryResult::Delete(count) = result {
            assert_eq!(count, 1);
        } else {
            panic!("Expected Delete result");
        }

        // Verify the data was deleted
        assert_eq!(lsm_tree.get("user1").unwrap(), None);
    }
}