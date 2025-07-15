use csv::{Reader, StringRecord};
use std::io::Read;
use crate::{DbResult, DbError, Value};

pub struct CSVParser {
    delimiter: u8,
    has_headers: bool,
    key_column: usize,
    value_column: usize,
}

impl CSVParser {
    pub fn new(key_column: usize, value_column: usize,) -> Self {
        Self {
            delimiter: b',',
            has_headers: true,
            key_column,
            value_column,
        }
    }

    pub fn parse_records<R: Read>(&self, reader: R) -> DbResult<Vec<(String, Value)>> {
        let mut csv_reader = Reader::from_reader(reader);
        let mut records = Vec::new();

        for result in csv_reader.records() {
            let record = result.map_err(|e| {
                DbError::InvalidOperation(format!("CSV parsing error: {}", e))
            })?;

            let key = self.extract_key(&record)?;
            let value = self.extract_value(&record)?;

            records.push((key, value));
        }

        Ok(records)
    }

    fn extract_key(&self, record: &StringRecord) -> DbResult<String> {
        record.get(self.key_column)
            .ok_or_else(|| DbError::InvalidOperation("Key column not found".to_string()))
            .map(|s| s.to_string())
    }

    fn extract_value(&self, record: &StringRecord) -> DbResult<Value> {
        record.get(self.value_column)
            .ok_or_else(|| DbError::InvalidOperation("Value column not found".to_string()))
            .map(|s| Value::Data(s.to_string()))
    }
}