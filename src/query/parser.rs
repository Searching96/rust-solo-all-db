use crate::query::ast::*;
use crate::{DbResult, DbError};

pub struct SQLParser {
    tokens: Vec<String>,
    position: usize,
}

impl SQLParser {
    pub fn new(sql: &str) -> Self {
        let tokens = tokenize(sql);
        Self { tokens, position: 0 }
    }

    pub fn parse(&mut self) -> DbResult<Statement> {
        if self.token.is_empty() {
            return Err(DbError::InvalidQuery("Empty SQL statement".to_string()));
        }

        match self.token[0].to_uppercase().as_str() {
            "SELECT" => self.parse_select(),
            "INSERT" => self.parse_insert(),
            "DELETE" => self.parse_delete(),
            _ => Err(DbError::InvalidQuery(format!("Unsupported statement: {}", self.token[0]))),
        }
    }

    fn parse_select(&mut self) -> DbResult<Statement> {
        self.consume("SELECT")?;

        let columns = self.parse_columns()?;

        self.consume("FROM")?;
        let table = self.consume_identifier()?;

        let where_clause = if self.peek().map(|s| s.to_uppercase()) == Some("WHERE".to_string()) {
            self.consume("WHERE")?;
            Some(WhereClause {
                condition: self.parse_condition()?,
            })
        } else {
            None
        };

        let limit = if self.peek().map(|s| s.to_uppercase()) == Some("LIMIT".to_string()) {
            self.consume("LIMIT")?;
            Some(self.consume_number()?)
        } else {
            None
        };

        Ok(Statement::Select(SelectStatement {
            columns,
            table,
            where_clause,
            limit,
        }))
    }

    fn parse_insert(&mut self) -> DbResult<Statement> {
        self.consume("INSERT")?;
        self.consume("INTO")?;

        let table = self.consume_identifier()?;
        self.consume("(")?;
        let columns = self.parse_columns()?;
        self.consume(")")?;

        self.consume("VALUES")?;
        self.consume("(")?;
        let values = self.parse_values()?;
        self.consume(")")?;

        Ok(Statement::Insert(InsertStatement {
            table,
            columns,
            values,
        }))
    }

    fn parse_delete(&mut self) -> DbResult<Statement> {
        self.consume("DELETE")?;
        self.consume("FROM")?;

        let table = self.consume_identifier()?;

        let where_clause = if self.peek().map(|s| s.to_uppercase()) == Some("WHERE".to_string()) {
            self.consume("WHERE")?;
            Some(WhereClause {
                condition: self.parse_condition()?,
            })
        } else {
            None
        };

        Ok(Statement::Delete(DeleteStatement {
            table,
            where_clause,
        }))
    }

    fn parse_columns(&mut self) -> DbResult<Vec<String>> {
        let mut columns = Vec::new();
        columns.push(self.consume_identifier()?);

        while self.peek() == Some(&",".to_string()) {
            self.consume(",")?;
            columns.push(self.consume_identifier()?);
        }

        Ok(columns)
    }

    fn parse_value_list(&mut self) -> DbResult<Vec<Value>> {
        let mut values = Vec::new();
        values.push(self.parse_value()?);

        while self.peek() == Some(&",".to_string()) {
            self.consume(",")?;
            values.push(self.parse_value()?);
        }

        Ok(values)
    }

    fn parse_condition(&mut self) -> DbResult<Condition> {
        let left = self.parse_comparison()?;

        if let Some(op) = self.peek() {
            match op.to_uppercase().as_str() {
                "AND" => {
                    self.consume("AND")?;
                    let right = self.parse_condition()?;
                    Ok(Condition::And(Box::new(left), Box::new(right)))
                }
                "OR" => {
                    self.consume("OR")?;
                    let right = self.parse_condition()?;
                    Ok(Condition::Or(Box::new(left), Box::new(right)))
                }
                _ => Ok(left),
            }
        } else {
            Ok(left)
        }
    }

    fn parse_comparison(&mut self) -> DbResult<Condition> {
        let column = self.consume_identifier()?;
        let operator = self.consume_identifier()?;
        let value = self.parse_value()?;

        match operator.to_uppercase().as_str() {
            "=" => Ok(Condition::Equals(column, value)),
            "!=" => Ok(Condition::NotEquals(column, value)),
            ">" => Ok(Condition::GreaterThan(column, value)),
            "<" => Ok(Condition::LessThan(column, value)),
            ">=" => Ok(Condition::GreaterThanOrEqual(column, value)),
            "<=" => Ok(Condition::LessThanOrEqual(column, value)),
            "LIKE" => {
                if let Value::String(pattern) = value {
                    Ok(Condition::Like(column, pattern))
                } else {
                    Err(DbError::InvalidQuery("LIKE operator requires string pattern".to_string()))
                }
            },
            _ => Err(DbError::InvalidQuery(format!("Unsupported operator: {}", operator))),
        }
    }

    fn parse_value(&mut self) -> DbResult<Value> {
        let token = self.peek().ok_or_else(|| {
            DbError::InvalidOperation("Expected value".to_string())
        })?;

        if token.starts_with('\'') && token.ends_with('\'') {
            let value = token[1..token.len()-1].to_string();
            self.advance();
            Ok(Value::String(value))
        } else if token.parse::<f64>().is_ok() {
            let value = token.parse::<f64>().unwrap();
            self.advance();
            Ok(Value::Number(value))
        } else if token.to_uppercase() == "TRUE" {
            self.advance();
            Ok(Value::Boolean(true))
        } else if token.to_uppercase() == "FALSE" {
            self.advance();
            Ok(Value::Boolean(false))
        } else if token.to_uppercase() == "NULL" {
            self.advance();
            Ok(Value::Null)
        } else {
            Err(DbError::InvalidOperation(format!("Invalid value: {}", token)))
        }
    }

    fn consume(&mut self, expected: &str) -> DbResult<()> {
        if let Some(token) = self.peek() {
            if token.to_uppercase() == expected.to_uppercase() {
                self.advance();
                Ok(())
            } else {
                Err(DbError::InvalidOperation(format!("Expected '{}', found '{}'", expected, token)))
            }
        } else {
            Err(DbError::InvalidOperation(format!("Expected '{}', found end of input", expected)))
        }
    }

    fn consume_identifier(&mut self) -> DbResult<String> {
        if let Some(token) = self.peek() {
            let identifier = token.clone();
            self.advance();
            Ok(identifier)
        } else {
            Err(DbError::InvalidOperation("Expected identifier".to_string()))
        }
    }

    fn consume_number(&mut self) -> DeResult<f64> {
        if let Some(token) = self.peek() {
            if let Ok(number) = token.parse::<f64>() {
                self.advance();
                Ok(number)
            } else {
                Err(DbError::InvalidOperation(format!("Expected number, found '{}'", token)))
            }
        } else {
            Err(DbError::InvalidOperation("Expected number".to_string()))
        }
    }

    fn peek(&self) -> Option<&String> {
        self.tokens.get(self.position)
    }

    fn advance(&mut self) {
        if self.position < self.tokens.len() {
            self.position += 1;
        }
    }
}

fn tokenize(sql: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current_token = String::new();
    let mut in_string = false;
    let mut chars = sql.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '\'' => {
                if in_string {
                    current_token.push(ch);
                    tokens.push(current_token.clone());
                    current_token.clear();
                    in_string = false;
                } else {
                    if !current_token.is_empty() {
                        tokens.push(current_token.clone());
                        current_token.clear();
                    }
                    current_token.push(ch);
                    in_string = true;
                }
            }
            ' ' | '\t' | '\n' | '\r' => {
                if in_string {
                    current_token.push(ch);
                } else {
                    if !current_token.is_empty() {
                        tokens.push(current_token.clone());
                        current_token.clear();
                    }
                }
            }
            ',' | '(' | ')' | ';' => {
                if in_string {
                    current_token.push(ch);
                } else {
                    if !current_token.is_empty() {
                        tokens.push(current_token.clone());
                        current_token.clear();
                    }
                    tokens.push(ch.to_string());
                }
            }
            '=' | '!' | '>' | '<' => {
                if in_string {
                    current_token.push(ch);
                } else {
                    if !current_token.is_empty() {
                        tokens.push(current_token.clone());
                        current_token.clear();
                    }

                    if let Some(&next_ch) = chars.peek() {
                        if  (ch == '!' && next_ch == '=') ||
                            (ch == '>' && next_ch == '=') ||
                            (ch == '<' && next_ch == '=') || 
                            (ch == '<' && next_ch == '>') {
                             current_token.push(ch);
                             current_token.push(chars.next().unwrap());
                             tokens.push(current_token.clone());
                             current_token.clear();
                        } else {
                            tokens.push(ch.to_string());
                        }
                    }
                }
            }
            _ => {
                current_token.push(ch);
            }
        }
    }

    if !current_token.is_empty() {
        tokens.push(current_token);
    }

    tokens
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize() {
        let sql = "SELECT name, age FROM users WHERE id = 1";
        let tokens = tokenize(sql);
        assert_eq!(tokens, vec![
            "SELECT", "name", ",", "age", "FROM", "users", "WHERE", "id", "=", "1"
        ]);
    }

    #[test]
    fn test_parse_select() {
        let mut parser = SQLParser::new("SELECT name, age FROM users WHERE id = 1 LIMIT 10");
        let stmt = parser.parse().unwrap();
        
        if let Statement::Select(select) = stmt {
            assert_eq!(select.columns, vec!["name", "age"]);
            assert_eq!(select.table, "users");
            assert!(select.where_clause.is_some());
            assert_eq!(select.limit, Some(10));
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_parse_insert() {
        let mut parser = SQLParser::new("INSERT INTO users (name, age) VALUES ('Alice', 25)");
        let stmt = parser.parse().unwrap();
        
        if let Statement::Insert(insert) = stmt {
            assert_eq!(insert.table, "users");
            assert_eq!(insert.columns, vec!["name", "age"]);
            assert_eq!(insert.values, vec![Value::String("Alice".to_string()), Value::Number(25.0)]);
        } else {
            panic!("Expected INSERT statement");
        }
    }

    #[test]
    fn test_parse_delete() {
        let mut parser = SQLParser::new("DELETE FROM users WHERE age > 65");
        let stmt = parser.parse().unwrap();
        
        if let Statement::Delete(delete) = stmt {
            assert_eq!(delete.table, "users");
            assert!(delete.where_clause.is_some());
        } else {
            panic!("Expected DELETE statement");
        }
    }
}