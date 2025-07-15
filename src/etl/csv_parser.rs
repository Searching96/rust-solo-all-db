use csv::StringRecord;
use std::io::Read;
use crate::{DbResult, DbError, Value};

#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    String,
    Number,
    Boolean,
    Date,
}

#[derive(Debug, Clone)]
pub struct CSVSchema {
    pub columns: Vec<String>,
    pub types: Vec<DataType>,
}

impl CSVSchema {
    pub fn new(columns: Vec<String>, types: Vec<DataType>) -> Self {
        Self { columns, types }
    }
    
    pub fn get_column_type(&self, column_index: usize) -> Option<&DataType> {
        self.types.get(column_index)
    }
}

pub struct CSVParser {
    delimiter: u8,
    has_headers: bool,
    key_column: usize,
    value_column: usize,
    schema: Option<CSVSchema>,
}

impl CSVParser {
    pub fn new(key_column: usize, value_column: usize) -> Self {
        Self {
            delimiter: b',',
            has_headers: true,
            key_column,
            value_column,
            schema: None,
        }
    }

    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = delimiter;
        self
    }

    pub fn with_custom_delimiter(mut self, delimiter: char) -> Self {
        self.delimiter = delimiter as u8;
        self
    }

    pub fn detect_delimiter<R: Read>(&self, mut reader: R) -> DbResult<u8> {
        let mut sample = String::new();
        reader.read_to_string(&mut sample).map_err(|e| {
            DbError::InvalidOperation(format!("Failed to read sample for delimiter detection: {}", e))
        })?;

        // Take first few lines as sample
        let sample_lines: Vec<&str> = sample.lines().take(5).collect();
        if sample_lines.is_empty() {
            return Ok(b','); // Default to comma
        }

        // Count potential delimiters
        let delimiters = [b',', b';', b'\t', b'|'];
        let mut delimiter_counts = std::collections::HashMap::new();

        for &delimiter in &delimiters {
            let mut total_count = 0;
            let mut consistent = true;
            let mut expected_count = None;

            for line in &sample_lines {
                let count = line.bytes().filter(|&b| b == delimiter).count();
                if count == 0 {
                    continue;
                }

                if let Some(expected) = expected_count {
                    if count != expected {
                        consistent = false;
                        break;
                    }
                } else {
                    expected_count = Some(count);
                }
                total_count += count;
            }

            if consistent && total_count > 0 {
                delimiter_counts.insert(delimiter, total_count);
            }
        }

        // Return the delimiter with the highest consistent count
        let detected_delimiter = delimiter_counts.into_iter()
            .max_by_key(|(_, count)| *count)
            .map(|(delimiter, _)| delimiter)
            .unwrap_or(b',');
        
        Ok(detected_delimiter)
    }

    pub fn with_headers(mut self, has_headers: bool) -> Self {
        self.has_headers = has_headers;
        self
    }

    pub fn with_schema(mut self, schema: CSVSchema) -> Self {
        self.schema = Some(schema);
        self
    }

    pub fn detect_schema<R: Read>(&self, reader: R) -> DbResult<CSVSchema> {
        let mut csv_reader = csv::ReaderBuilder::new()
            .delimiter(self.delimiter)
            .has_headers(self.has_headers)
            .from_reader(reader);

        let mut columns = Vec::new();
        let mut sample_data: Vec<Vec<String>> = Vec::new();

        // Get headers if available
        if self.has_headers {
            if let Ok(headers) = csv_reader.headers() {
                columns = headers.iter().map(|h| h.to_string()).collect();
            }
        }

        // Collect sample data for type inference
        for (i, result) in csv_reader.records().enumerate() {
            if i >= 100 { // Limit sample size
                break;
            }
            
            let record = result.map_err(|e| {
                DbError::InvalidOperation(format!("CSV parsing error during schema detection: {}", e))
            })?;

            let row: Vec<String> = record.iter().map(|field| field.to_string()).collect();
            sample_data.push(row);
        }

        if sample_data.is_empty() {
            let column_count = columns.len();
            return Ok(CSVSchema::new(columns, vec![DataType::String; column_count]));
        }

        // Generate column names if no headers
        if columns.is_empty() {
            let column_count = sample_data.first().map(|row| row.len()).unwrap_or(0);
            columns = (0..column_count).map(|i| format!("column_{}", i)).collect();
        }

        // Infer types from sample data
        let mut types = Vec::new();
        for col_idx in 0..columns.len() {
            let column_type = self.infer_column_type(&sample_data, col_idx);
            types.push(column_type);
        }

        Ok(CSVSchema::new(columns, types))
    }

    fn infer_column_type(&self, sample_data: &[Vec<String>], column_index: usize) -> DataType {
        let mut number_count = 0;
        let mut boolean_count = 0;
        let mut total_count = 0;

        for row in sample_data {
            if let Some(value) = row.get(column_index) {
                if value.is_empty() {
                    continue;
                }
                
                total_count += 1;

                // Check if it's a number
                if value.parse::<f64>().is_ok() {
                    number_count += 1;
                }
                
                // Check if it's a boolean
                if matches!(value.to_lowercase().as_str(), "true" | "false" | "1" | "0" | "yes" | "no") {
                    boolean_count += 1;
                }
            }
        }

        if total_count == 0 {
            return DataType::String;
        }

        // Use majority rule for type inference
        if number_count * 2 > total_count {
            DataType::Number
        } else if boolean_count * 2 > total_count {
            DataType::Boolean
        } else {
            DataType::String
        }
    }

    pub fn parse_records<R: Read>(&self, reader: R) -> DbResult<Vec<(String, Value)>> {
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

    pub fn validate_record(&self, record: &StringRecord) -> DbResult<()> {
        if let Some(schema) = &self.schema {
            for (i, field) in record.iter().enumerate() {
                if let Some(expected_type) = schema.get_column_type(i) {
                    if !self.validate_field_type(field, expected_type) {
                        return Err(DbError::InvalidOperation(format!(
                            "Field '{}' in column {} doesn't match expected type {:?}",
                            field, i, expected_type
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    fn validate_field_type(&self, field: &str, expected_type: &DataType) -> bool {
        if field.is_empty() {
            return true; // Allow empty fields
        }

        match expected_type {
            DataType::String => true, // String can hold anything
            DataType::Number => field.parse::<f64>().is_ok(),
            DataType::Boolean => matches!(field.to_lowercase().as_str(), "true" | "false" | "1" | "0" | "yes" | "no"),
            DataType::Date => {
                // Simple date validation - you could use chrono for more robust parsing
                field.contains('-') || field.contains('/') || field.contains('.')
            }
        }
    }
}