use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Delete(DeleteStatement),
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    pub columns: Vec<String>,
    pub table: String,
    pub where_clause: Option<WhereClause>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertStatement {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStatement {
    pub table: String,
    pub where_clause: Option<WhereClause>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WhereClause {
    pub condition: Condition,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Condition {
    Equals(String, Value),
    NotEquals(String, Value),
    GreaterThan(String, Value),
    LessThan(String, Value),
    GreaterThanOrEqual(String, Value),
    LessThanOrEqual(String, Value),
    Like(String, String),
    And(Box<Condition>, Box<Condition>),
    Or(Box<Condition>, Box<Condition>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    String(String),
    Number(f64),
    Boolean(bool),
    Null,
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::String(s) => write!(f, "'{}'", s),
            Value::Number(n) => write!(f, "{}", n),
            Value::Boolean(b) => write!(f, "{}", b),
            Value::Null => write!(f, "NULL"),
        }
    }
}

impl fmt::Display for Statement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Statement::Select(select) => {
                write!(f, "SELECT {} FROM {}",
                    select.columns.join(", "),
                    select.table
                )?;
                if let Some(where_clause) = &select.where_clause {
                    write!(f, " WHERE {}", where_clause.condition)?;
                }
                if let Some(limit) = select.limit {
                    write!(f, " LIMIT {}", limit)?;
                }
                Ok(())
            }
            Statement::Insert(insert) => {
                write!(f, "INSERT INTO {} ({}) VALUES ({})",
                    insert.table,
                    insert.columns.join(", "),
                    insert.values.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(", ")
                )
            }
            Statement::Delete(delete) => {
                write!(f, "DELETE FROM {}", delete.table)?;
                if let Some(where_clause) = &delete.where_clause {
                    write!(f, " WHERE {}", where_clause.condition)?;
                }
                Ok(())
            }
        }
    }
}

impl fmt::Display for Condition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Condition::Equals(col, val) => write!(f, "{} = {}", col, val),
            Condition::NotEquals(col, val) => write!(f, "{} != {}", col, val),
            Condition::GreaterThan(col, val) => write!(f, "{} > {}", col, val),
            Condition::LessThan(col, val) => write!(f, "{} < {}", col, val),
            Condition::GreaterThanOrEqual(col, val) => write!(f, "{} >= {}", col, val),
            Condition::LessThanOrEqual(col, val) => write!(f, "{} <= {}", col, val),
            Condition::Like(col, pattern) => write!(f, "{} LIKE '{}'", col, pattern),
            Condition::And(left, right) => write!(f, "({} AND {})", left, right),
            Condition::Or(left, right) => write!(f, "({} OR {})", left, right),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_statement_display() {
        let select = SelectStatement {
            columns: vec!["name".to_string(), "age".to_string()],
            table: "users".to_string(),
            where_clause: Some(WhereClause {
                condition: Condition::Equals("id".to_string(), Value::Number(1.0)),
            }),
            limit: Some(10),
        };
        
        let stmt = Statement::Select(select);
        assert_eq!(stmt.to_string(), "SELECT name, age FROM users WHERE id = 1 LIMIT 10");
    }

    #[test]
    fn test_insert_statement_display() {
        let insert = InsertStatement {
            table: "users".to_string(),
            columns: vec!["name".to_string(), "age".to_string()],
            values: vec![Value::String("Alice".to_string()), Value::Number(25.0)],
        };
        
        let stmt = Statement::Insert(insert);
        assert_eq!(stmt.to_string(), "INSERT INTO users (name, age) VALUES ('Alice', 25)");
    }

    #[test]
    fn test_complex_where_condition() {
        let condition = Condition::And(
            Box::new(Condition::Equals("status".to_string(), Value::String("active".to_string()))),
            Box::new(Condition::GreaterThan("age".to_string(), Value::Number(18.0))),
        );
        
        assert_eq!(condition.to_string(), "(status = 'active' AND age > 18)");
    }
}