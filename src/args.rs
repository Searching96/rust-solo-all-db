use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "rustdb")]
#[command(about = "A high-performance LSM-tree database with ETL and query capabilities.")]
#[command(version = "0.6.9")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    // Configuration file path
    #[arg(short, long, default_value = "db.yaml")]
    pub config: PathBuf,

    // Enable verbose output
    #[arg(short, long)]
    pub verbose: bool,
}

#[derive(Subcommand)]
pub enum Commands {
    // Start interactive CLI mode
    Interactive,

    // Load CSV data into the database
    Load {
        // Path to CSV file
        file: PathBuf,

        // Key column name or index
        #[arg(short, long, default_value = "0")]
        key_column: String,


        // Value column name or index
        #[arg(short, long, default_value = "1")]
        value_column: String,

        // Number of parallel threads
        #[arg(short, long)]
        threads: Option<usize>,

        // Batch size for processing
        #[arg(short, long)]
        batch_size: Option<usize>,
    },

    Query {
        // SQL query to execute
        sql: String,

        // Output format (table, json, csv)
        #[arg(short, long, default_value = "table")]
        format: String,

        // Limit number of result
        #[arg(short, long)]
        limit: Option<usize>,
    },

    Benchmark {
        // Benchmark type (insert, query, load)
        #[arg(default_value = "all")]
        bench_type: String,
        
        // Number of operations to perform
        #[arg(short, long, default_value = "10000")]
        operations: usize,

        // Number of parallel threads
        #[arg(short, long, default_value = "4")]
        threads: usize,
    },

    Stats {
        // Show live/real-time stats
        #[arg(short, long)]
        live: bool,

        // Refresh interval in seconds for live mode
        #[arg(short, long, default_value = "1")]
        interval: usize,
    },

    // Database maintenance operations
    Maintenance {
        #[command(subcommand)]
        operation: MaintenanceOps,
    },

    // Generate default configuration file
    InitConfig {
        #[arg(short, long, default_value = "db.yaml")]
        output: PathBuf,
    },
}

#[derive(Subcommand)]
pub enum MaintenanceOps {
    // Force compaction of all levels
    CompactAll,

    // Vacuum deleted entries
    Vacuum,

    // Verify database integrity
    Verify,

    // Show detailed storage information
    Info,
}

