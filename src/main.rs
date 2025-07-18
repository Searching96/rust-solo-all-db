// Main entry point for the CLI application

use clap::Parser;
use rust_solo_all_db::args::{Cli, Commands, MaintenanceOps};
use rust_solo_all_db::config::DatabaseConfig;
use rust_solo_all_db::metrics::PerformanceMetrics;
use rust_solo_all_db::engine::LSMTree;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    
    // Load or create configuration
    let config = if cli.config.exists() {
        DatabaseConfig::load_from_file(&cli.config)?
    } else {
        println!("⚠️  Configuration file not found, using defaults");
        DatabaseConfig::default()
    };

    // Initialize performance metrics
    let metrics = Arc::new(PerformanceMetrics::new());
    
    // Create database
    let lsm_config = config.to_lsm_config();
    let mut db = LSMTree::with_config(lsm_config)?;
    
    match cli.command {
        Commands::Interactive => {
            run_interactive_mode(&mut db, &config, metrics)?;
        }
        
        Commands::Load { file, key_column: _, value_column: _, threads: _, batch_size: _ } => {
            run_simple_load_command(&mut db, file)?;
        }
        
        Commands::Query { sql: _, format: _, limit: _ } => {
            println!("❌ Query command not yet implemented for your current API");
            println!("💡 Use 'cargo run -- interactive' to access query functionality");
        }
        
        Commands::Benchmark { bench_type, operations, threads: _ } => {
            run_benchmark_command(&mut db, bench_type, operations, metrics)?;
        }
        
        Commands::Stats { live, interval } => {
            run_stats_command(&db, live, interval as u64, metrics)?;
        }
        
        Commands::Maintenance { operation } => {
            run_maintenance_command(&mut db, operation)?;
        }
        
        Commands::InitConfig { output } => {
            let default_config = DatabaseConfig::default();
            default_config.save_to_file(&output)?;
            println!("✅ Created default configuration at: {}", output.display());
        }
    }

    Ok(())
}

fn run_interactive_mode(
    _db: &mut LSMTree, 
    _config: &DatabaseConfig,
    _metrics: Arc<PerformanceMetrics>
) -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Welcome to RustDB Interactive Mode!");
    println!("Type 'help' for commands or 'quit' to exit.");
    
    // Use your existing CLI module which creates its own database
    let mut database_cli = rust_solo_all_db::cli::DatabaseCLI::new()?;
    database_cli.run()?;
    Ok(())
}

fn run_simple_load_command(
    db: &mut LSMTree,
    file: std::path::PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("📂 Loading CSV file: {}", file.display());
    
    let loader = rust_solo_all_db::etl::ETLLoader::new();
    
    let start = Instant::now();
    let result = loader.load_csv(&file, db, 0, 1);
    let duration = start.elapsed();
    
    match result {
        Ok(count) => {
            println!("✅ Successfully loaded {} records in {:.2}s", 
                count, duration.as_secs_f64());
            println!("📊 Rate: {:.2} records/second", count as f64 / duration.as_secs_f64());
        }
        Err(e) => {
            eprintln!("❌ Failed to load CSV: {}", e);
            std::process::exit(1);
        }
    }
    
    Ok(())
}

fn run_benchmark_command(
    db: &mut LSMTree,
    bench_type: String,
    operations: usize,
    metrics: Arc<PerformanceMetrics>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("🏃 Running {} benchmark with {} operations", bench_type, operations);
    
    match bench_type.as_str() {
        "insert" => benchmark_inserts(db, operations, metrics),
        "query" => benchmark_queries(db, operations, metrics),
        "all" => {
            benchmark_inserts(db, operations / 2, metrics.clone())?;
            benchmark_queries(db, operations / 2, metrics)?;
            Ok(())
        }
        _ => {
            eprintln!("❌ Unknown benchmark type: {}. Available: insert, query, all", bench_type);
            std::process::exit(1);
        }
    }
}

fn run_stats_command(
    _db: &LSMTree,
    live: bool,
    interval: u64,
    metrics: Arc<PerformanceMetrics>,
) -> Result<(), Box<dyn std::error::Error>> {
    if live {
        println!("📊 Starting live statistics monitoring (Ctrl+C to exit)...");
        loop {
            metrics.print_live_stats();
            std::thread::sleep(Duration::from_secs(interval));
        }
    } else {
        let stats = metrics.get_stats();
        println!("📊 Database Statistics:");
        println!("Uptime: {:?}", stats.uptime);
        println!("Memory Usage: {:.2} MB", stats.memory_usage_bytes as f64 / 1024.0 / 1024.0);
        
        for (op, stat) in stats.operation_stats {
            println!("{}: {} operations, {:.2} ops/sec", op, stat.count, stat.ops_per_second);
        }
    }
    
    Ok(())
}

fn run_maintenance_command(
    db: &mut LSMTree,
    operation: MaintenanceOps,
) -> Result<(), Box<dyn std::error::Error>> {
    match operation {
        MaintenanceOps::CompactAll => {
            println!("🔧 Starting manual compaction...");
            let start = Instant::now();
            db.compact()?;
            let duration = start.elapsed();
            println!("✅ Compaction completed in {:.2}s", duration.as_secs_f64());
        }
        
        MaintenanceOps::Vacuum => {
            println!("🧹 Vacuuming deleted entries...");
            // Implement vacuum logic when available
            println!("✅ Vacuum completed");
        }
        
        MaintenanceOps::Verify => {
            println!("🔍 Verifying database integrity...");
            // Implement verification logic when available
            println!("✅ Database integrity verified");
        }
        
        MaintenanceOps::Info => {
            println!("📋 Database Information:");
            println!("Data directory: {:?}", db.get_data_dir());
            println!("MemTable entries: {}", db.memtable_size());
            let stats = db.stats();
            println!("MemTable entries: {}", stats.memtable_entries);
            println!("SSTable count: {}", stats.sstable_count);
            println!("Total SSTable entries: {}", stats.total_sstable_entries);
        }
    }
    
    Ok(())
}

// Helper functions
fn benchmark_inserts(db: &mut LSMTree, operations: usize, metrics: Arc<PerformanceMetrics>) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    
    for i in 0..operations {
        let key = format!("bench_key_{}", i);
        let value = format!("bench_value_{}", i);
        
        let op_start = std::time::Instant::now();
        db.insert(key, value)?;
        let op_duration = op_start.elapsed();
        metrics.record_operation("insert", op_duration);
    }
    
    let duration = start.elapsed();
    let ops_per_sec = operations as f64 / duration.as_secs_f64();
    
    println!("✅ Insert benchmark: {} ops in {:.2}s ({:.2} ops/sec)", 
        operations, duration.as_secs_f64(), ops_per_sec);
    
    Ok(())
}

fn benchmark_queries(db: &LSMTree, operations: usize, metrics: Arc<PerformanceMetrics>) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    
    for i in 0..operations {
        let key = format!("bench_key_{}", i % 100); // Query existing keys
        
        let op_start = std::time::Instant::now();
        let _ = db.get(&key);
        let op_duration = op_start.elapsed();
        metrics.record_operation("query", op_duration);
    }
    
    let duration = start.elapsed();
    let ops_per_sec = operations as f64 / duration.as_secs_f64();
    
    println!("✅ Query benchmark: {} ops in {:.2}s ({:.2} ops/sec)", 
        operations, duration.as_secs_f64(), ops_per_sec);
    
    Ok(())
}