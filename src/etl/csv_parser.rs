use csv::StringRecord;
use std::io::Read;
use crate::{DbResult, DbError, Value};

pub struct CSVParser {
    delimiter: u8,
    has_headers: bool,
    key_column: usize,
    value_column: usize,
}

impl CSVParser {
    pub fn new(key_column: usize, value_column: usize) -> Self {
        Self {
            delimiter: b',',
            has_headers: true,
            key_column,
            value_column,
        }
    }

    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = delimiter;
        self
    }

    pub fn with_headers(mut self, has_headers: bool) -> Self {
        self.has_headers = has_headers;
        self
    }    pub fn parse_records<R: Read>(&self, reader: R) -> DbResult<Vec<(String, Value)>> {
        let mut csv_reader = csv::ReaderBuilder::new()
            .delimiter(self.delimiter)
            .has_headers(self.has_headers)
            .from_reader(reader);
        
        let mut records = Vec::new();
        
        for result in csv_reader.records() {
            let record = result.map_err(|e| {
                DbError::InvalidOperation(format!("CSV parsing error: {}", e))
            })?;
            
            let key = self.extract_key(&record)?;
            let value = self.extract_value(&record)?;
            
            records.push((key, value));
        }
        
        println!("Parsed {} records from CSV", records.len());
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