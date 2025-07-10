// Main try point for the CLI application

use rust_solo_all_db::cli::DatabaseCLI;

fn main() {
    match DatabaseCLI::new() {
        Ok(mut cli) => {
            if let Err(e) = cli.run() {
                eprintln!("CLI error: {}", e);
                std::process::exit(1);
            }
        }
        Err(e) => {
            eprintln!("Failed to initialize CLI: {}", e);
            std::process::exit(1);
        }
    }
}