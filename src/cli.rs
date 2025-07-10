// Command-line interface for the database
use crate::engine::lsm::{LSMTree, LSMConfig};
use crate::DbResult;
use std::io::{self, Write};
use std::path::PathBuf;

pub struct DatabaseCLI {
    db: LSMTree,
}

impl DatabaseCLI {
    pub fn new() -> DbResult<Self> {
        let config = LSMConfig {
            memtable_size_limit: 100, // Smalller limit for CLI demo
            data_dir: PathBuf::from("cli_data"),
        };

        let db = LSM::with_config(config)?;
        Ok(Self { db })
    }

    pub fn run(&mut self) -> DbResult<()> {
        println!("Welcome to the RustDB CLI!");
        println!("Commands: insert <key> <value>, get <key>, delete <key>, stats, quit");
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

                    match self.handle_commnand(trimmed) {
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
        let parts: Vect<&str> = command.split_whitespace().collect();

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
                match self.db.get(parts[1]) {
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
        println!("  insert <key> <value>  - Insert a key-value pair");
        println!("  get <key>             - Get value by key");
        println!("  delete <key>          - Delete a key");
        println!("  stats                 - Show database statistics");
        println!("  flush                 - Force flush to disk");
        println!("  help                  - Show this help");
        println!("  quit                  - Exit the CLI");
    }
}