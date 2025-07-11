use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use crate::{DbError, DbResult, WALEntry};

#[derive(Debug)]
pub struct WAL {
    file_path: PathBuf,
    writer: BufWriter<File>,
}

impl WAL {
    pub fn new<P: AsRef<Path>>(file_path: P) -> DbResult<Self> {
        let file_path = file_path.as_ref().to_path_buf();

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
            .map_err(|e| DbError::InvalidOperation(format!("Failed to open WAL file: {}", e)))?;

        let writer = BufWriter::new(file);

        Ok(Self {
            file_path,
            writer,
        })
    }

    pub fn append(&mut self, entry: &WALEntry) -> DbResult<()> {
        let serialized = bincode::serialize(entry)
            .map_err(|e| DbError::InvalidOperation(format!("Failed to serialize WAL entry: {}", e)))?;
        
        // Write the length first then the data
        let len = serialized.len() as u32;
        self.writer.write_all(&len.to_le_bytes())
            .map_err(|e| DbError::InvalidOperation(format!("Failed to write WAL entry length: {}", e)))?;
        
        self.writer.write_all(&serialized)
            .map_err(|e| DbError::InvalidOperation(format!("Failed to write WAL entry: {}", e)))?;

        // Force sync to disk for durability
        self.writer.flush()
            .map_err(|e| DbError::InvalidOperation(format!("Failed to flush WAL: {}", e)))?;

        Ok(())
    }

    pub fn read_all(&self) -> DbResult<Vec<WALEntry>> {
        let file = File::open(&self.file_path)
            .map_err(|e| DbError::InvalidOperation(format!("Failed to open WAL for reading: {}", e)))?;

        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();

        loop {
            let mut len_bytes = [0u8; 4];
            match reader.read_exact(&mut len_bytes) {
                Ok(()) => {
                    let len = u32::from_le_bytes(len_bytes) as usize;
                
                    // Read the data
                    let mut data = vec![0u8; len];
                    reader.read_exact(&mut data)
                        .map_err(|e| DbError::InvalidOperation(format!("Failed to read WAL entry data: {}", e)))?;

                    // Deserialize the entry
                    let entry: WALEntry = bincode::deserialize(&data)
                        .map_err(|e| DbError::InvalidOperation(format!("Failed to deserialize WAL entry: {}", e)))?;

                    entries.push(entry);
                }
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // End of file reached
                    break;
                }
                Err(e) => {
                    return Err(DbError::InvalidOperation(format!("Failed to read WAL entry length: {}", e)));
                }
            }
        }

        Ok(entries)
    }

    pub fn truncate(&mut self) -> DbResult<()> {
        // Close the current writer
        self.writer.flush()
            .map_err(|e| DbError::InvalidOperation(format!("Failed to flush before truncate: {}", e)))?;
    
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.file_path)
            .map_err(|e| DbError::InvalidOperation(format!("Failed to open WAL for truncation: {}", e)))?;
    
        // Recreate the writer
        self.writer = BufWriter::new(file);

        Ok(())
    }
}