// Command-line interface for the database
use crate::engine::lsm::{LSMTree, LSMConfig};
use crate::DbResult;
use crate::engine::ETLLoader;
use std::io::{self, Write};
use std::path::PathBuf;

pub struct DatabaseCLI {
    db: LSMTree,
}

impl DatabaseCLI {
    pub fn new() -> DbResult<Self> {
        let mut config = LSMConfig::default();
        config.memtable_size_limit = 100; // Smaller limit for CLI demo
        config.data_dir = PathBuf::from("cli_data");

        let db = LSMTree::with_config(config)?;
        Ok(Self { db })
    }

    pub fn run(&mut self) -> DbResult<()> {
        println!("Welcome to the RustDB CLI!");
        println!("Commands: insert <key> <value>, get <key>, delete <key>, load <csv_file> [key_col] [value_col], compact, autocompact, stats, flush, quit");
        println!();

        loop {
            print!("> ");
            io::stdout().flush().unwrap();

            let mut input = String::new();
            match io::stdin().read_line(&mut input) {
                Ok(_) => {
                    let trimmed = input.trim();
                    if trimmed.is_empty() {
                        continue;
                    }

                    match self.handle_command(trimmed) {
                        Ok(should_quit) => {
                            if should_quit {
                                break;
                            }
                        }
                        Err(e) => {
                            println!("Error: {}", e);
                        }
                    }
                }
                Err(e) => {
                    println!("Error reading input: {}", e);
                    break;
                }
            }
        }

        // Flush any remaining data before exit
        self.db.flush()?;
        println!("Database flush. Sayonara!");
        Ok(())
    }

    fn handle_command(&mut self, command: &str) -> DbResult<bool> {
        let parts: Vec<&str> = command.split_whitespace().collect();

        if parts.is_empty() {
            return Ok(false);
        }

        match parts[0].to_lowercase().as_str() {
            "insert" | "put" => {
                if parts.len() != 3 {
                    println!("Usage: insert <key> <value>");
                    return Ok(false);
                }
                self.db.insert(parts[1].to_string(), parts[2].to_string())?;
                println!("Inserted: {} -> {}", parts[1], parts[2]);
            }

            "get" => {
                if parts.len() != 2 {
                    println!("Usage: get <key>");
                    return Ok(false);
                }
                match self.db.get(parts[1])? {
                    Some(value) => println!("{}: {}", parts[1], value),
                    None => println!("Key not found: {}", parts[1]),
                }
            }

            "delete" | "del" => {
                if parts.len() != 2 {
                    println!("Usage: delete <key");
                    return Ok(false);
                }
                if self.db.delete(parts[1])? {
                    println!("Deleted: {}", parts[1]);
                } else {
                    println!("Key not found: {}", parts[1]);
                }
            }

            "stats" => {
                let stats = self.db.stats();
                println!("{}", stats);
            }

            "flush" => {
                self.db.flush()?;
                println!("Database flushed to disk");
            }

            "compact" => {
                self.db.compact()?;
                let stats = self.db.stats();
                println!("After compaction: {}", stats);
            }

            "autocompact" => {
                self.db.maybe_compact()?;
                let stats = self.db.stats();
                println!("After auto-compaction: {}", stats);
            }

            "load" => {
                if parts.len() < 2 {
                    println!("Usage: load <csv_file> [key_column] [value_column]");
                    return Ok(false);
                }

                let file_path = parts[1];
                let key_column = parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(0);
                let value_column = parts.get(3).and_then(|s| s.parse().ok()).unwrap_or(1);
                
                let loader = ETLLoader::new();
                match loader.load_csv(file_path, &mut self.db, key_column, value_column) {
                    Ok(count) => println!("Successfully loaded {} records from {}", count, file_path),
                    Err(e) => println!("Error loading CSV: {}", e),
                }
            }
            
            "help" => {
                self.print_help();
            }

            "quit" | "exit" => {
                return Ok(true);
            }

            _ => {
                println!("Unknown command: {}. Type 'help' for available commands.", parts[0]);
            }
        }

        Ok(false)
    }

    fn print_help(&self) {
        println!("Available commands:");
        println!("  insert <key> <value>                    - Insert a key-value pair");
        println!("  get <key>                               - Get value by key");
        println!("  delete <key>                            - Delete a key");
        println!("  load <csv_file> [key_col] [value_col]   - Load data from CSV file with specified columns (default: 0,1)");
        println!("  compact                                 - Force compaction of all levels");
        println!("  autocompact                             - Check and compact levels if needed");
        println!("  stats                                   - Show database statistics");
        println!("  flush                                   - Force flush to disk");
        println!("  help                                    - Show this help");
        println!("  quit                                    - Exit the CLI");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_cli() -> (DatabaseCLI, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let mut config = LSMConfig::default();
        config.memtable_size_limit = 10; // Very small for testing
        config.data_dir = temp_dir.path().to_path_buf();
        config.enable_wal = false; // Disable WAL for simpler testing
        config.background_compaction = false; // Disable background compaction

        let db = LSMTree::with_config(config).unwrap();
        let cli = DatabaseCLI { db };
        (cli, temp_dir)
    }

    #[test]
    fn test_handle_insert_command() {
        let (mut cli, _temp_dir) = create_test_cli();
        
        let result = cli.handle_command("insert key1 value1");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false); // Should not quit
        
        // Verify the value was inserted
        let value = cli.db.get("key1").unwrap();
        assert_eq!(value, Some("value1".to_string()));
    }

    #[test]
    fn test_handle_get_command() {
        let (mut cli, _temp_dir) = create_test_cli();
        
        // Insert a value first
        cli.db.insert("key1".to_string(), "value1".to_string()).unwrap();
        
        let result = cli.handle_command("get key1");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
    }

    #[test]
    fn test_handle_delete_command() {
        let (mut cli, _temp_dir) = create_test_cli();
        
        // Insert a value first
        cli.db.insert("key1".to_string(), "value1".to_string()).unwrap();
        
        let result = cli.handle_command("delete key1");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
        
        // Verify the value was deleted
        let value = cli.db.get("key1").unwrap();
        assert_eq!(value, None);
    }

    #[test]
    fn test_handle_compact_command() {
        let (mut cli, _temp_dir) = create_test_cli();
        
        // Insert some data to create SSTables
        for i in 0..20 {
            cli.db.insert(format!("key{}", i), format!("value{}", i)).unwrap();
        }
        
        // Force flush to create SSTables
        cli.db.flush().unwrap();
        
        let result = cli.handle_command("compact");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
        
        // Verify data is still accessible after compaction
        let value = cli.db.get("key0").unwrap();
        assert_eq!(value, Some("value0".to_string()));
    }

    #[test]
    fn test_handle_autocompact_command() {
        let (mut cli, _temp_dir) = create_test_cli();
        
        // Insert some data
        for i in 0..15 {
            cli.db.insert(format!("key{}", i), format!("value{}", i)).unwrap();
        }
        
        // Force flush to create SSTables
        cli.db.flush().unwrap();
        
        let result = cli.handle_command("autocompact");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
        
        // Verify data is still accessible after auto-compaction
        let value = cli.db.get("key0").unwrap();
        assert_eq!(value, Some("value0".to_string()));
    }

    #[test]
    fn test_handle_stats_command() {
        let (mut cli, _temp_dir) = create_test_cli();
        
        let result = cli.handle_command("stats");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
    }

    #[test]
    fn test_handle_flush_command() {
        let (mut cli, _temp_dir) = create_test_cli();
        
        // Insert some data
        cli.db.insert("key1".to_string(), "value1".to_string()).unwrap();
        
        let result = cli.handle_command("flush");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
        
        // Verify data is still accessible after flush
        let value = cli.db.get("key1").unwrap();
        assert_eq!(value, Some("value1".to_string()));
    }

    #[test]
    fn test_handle_help_command() {
        let (mut cli, _temp_dir) = create_test_cli();
        
        let result = cli.handle_command("help");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
    }

    #[test]
    fn test_handle_quit_command() {
        let (mut cli, _temp_dir) = create_test_cli();
        
        let result = cli.handle_command("quit");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true); // Should quit
        
        let result = cli.handle_command("exit");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true); // Should also quit
    }

    #[test]
    fn test_handle_unknown_command() {
        let (mut cli, _temp_dir) = create_test_cli();
        
        let result = cli.handle_command("unknown_command");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
    }

    #[test]
    fn test_handle_invalid_insert_command() {
        let (mut cli, _temp_dir) = create_test_cli();
        
        // Test with missing arguments
        let result = cli.handle_command("insert key1");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
        
        let result = cli.handle_command("insert");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
    }

    #[test]
    fn test_handle_invalid_get_command() {
        let (mut cli, _temp_dir) = create_test_cli();
        
        let result = cli.handle_command("get");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
    }

    #[test]
    fn test_handle_invalid_delete_command() {
        let (mut cli, _temp_dir) = create_test_cli();
        
        let result = cli.handle_command("delete");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
    }

    #[test]
    fn test_empty_command() {
        let (mut cli, _temp_dir) = create_test_cli();
        
        let result = cli.handle_command("");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
    }

    #[test]
    fn test_whitespace_command() {
        let (mut cli, _temp_dir) = create_test_cli();
        
        let result = cli.handle_command("   ");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
    }

    #[test]
    fn test_command_aliases() {
        let (mut cli, _temp_dir) = create_test_cli();
        
        // Test put alias for insert
        let result = cli.handle_command("put key1 value1");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
        
        let value = cli.db.get("key1").unwrap();
        assert_eq!(value, Some("value1".to_string()));
        
        // Test del alias for delete
        let result = cli.handle_command("del key1");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
        
        let value = cli.db.get("key1").unwrap();
        assert_eq!(value, None);
    }

    #[test]
    fn test_compact_with_data_integrity() {
        let (mut cli, _temp_dir) = create_test_cli();
        
        // Insert data across multiple flushes to create multiple SSTables
        for batch in 0..3 {
            for i in 0..10 {
                let key = format!("key{}_{}", batch, i);
                let value = format!("value{}_{}", batch, i);
                cli.db.insert(key, value).unwrap();
            }
            cli.db.flush().unwrap();
        }
        
        // Delete some keys to create tombstones
        for i in 0..5 {
            let key = format!("key0_{}", i);
            cli.db.delete(&key).unwrap();
        }
        cli.db.flush().unwrap();
        
        // Perform compaction
        let result = cli.handle_command("compact");
        assert!(result.is_ok());
        
        // Verify deleted keys are still deleted
        for i in 0..5 {
            let key = format!("key0_{}", i);
            let value = cli.db.get(&key).unwrap();
            assert_eq!(value, None);
        }
        
        // Verify remaining keys are still accessible
        for i in 5..10 {
            let key = format!("key0_{}", i);
            let value = cli.db.get(&key).unwrap();
            assert_eq!(value, Some(format!("value0_{}", i)));
        }
        
        // Verify other batches are intact
        for batch in 1..3 {
            for i in 0..10 {
                let key = format!("key{}_{}", batch, i);
                let value = cli.db.get(&key).unwrap();
                assert_eq!(value, Some(format!("value{}_{}", batch, i)));
            }
        }
    }

    #[test]
    fn test_autocompact_with_data_integrity() {
        let (mut cli, _temp_dir) = create_test_cli();
        
        // Insert enough data to trigger auto-compaction
        for i in 0..25 {
            cli.db.insert(format!("key{}", i), format!("value{}", i)).unwrap();
        }
        cli.db.flush().unwrap();
        
        // Perform auto-compaction
        let result = cli.handle_command("autocompact");
        assert!(result.is_ok());
        
        // Verify all data is still accessible
        for i in 0..25 {
            let value = cli.db.get(&format!("key{}", i)).unwrap();
            assert_eq!(value, Some(format!("value{}", i)));
        }
    }
}