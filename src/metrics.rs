use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    start_time: Instant,
    operation_counts: Arc<Mutex<HashMap<String, u64>>>,
    operation_durations: Arc<Mutex<HashMap<String, Duration>>>,
    memory_usage: Arc<Mutex<usize>>,
}

impl PerformanceMetrics {
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            operation_counts: Arc::new(Mutex::new(HashMap::new())),
            operation_durations: Arc::new(Mutex::new(HashMap::new())),
            memory_usage: Arc::new(Mutex::new(0)),
        }
    }

    pub fn record_operation(&self, operation: &str, duration: Duration) {
        let mut counts = self.operation_counts.lock().unwrap();
        let mut durations = self.operation_durations.lock().unwrap();

        *counts.entry(operation.to_string()).or_insert(0) += 1;
        let total_duration = durations.entry(operation.to_string()).or_insert(Duration::ZERO);
        *total_duration += duration;
    }

    pub fn update_memory_usage(&self, bytes: usize) {
        let mut memory = self.memory_usage.lock().unwrap();
        *memory = bytes;
    }

    pub fn get_stats(&self) -> MetricsSnapshot {
        let counts = self.operation_counts.lock().unwrap();
        let durations = self.operation_durations.lock().unwrap();
        let memory = self.memory_usage.lock().unwrap();

        let uptime = self.start_time.elapsed();

        let mut operation_stats = HashMap::new();
        for (op, &count) in counts.iter() {
            let total_duration = durations.get(op).unwrap_or(&Duration::ZERO);
            let avg_duration = if count > 0 {
                *total_duration / count as u32
            } else {
                Duration::ZERO
            };

            operation_stats.insert(op.clone(), OperationStats {
                count,
                total_duration: *total_duration,
                average_duration: avg_duration,
                ops_per_second: if uptime.as_secs() > 0 {
                    count as f64 / uptime.as_secs_f64()
                } else {
                    0.0
                },
            });
        }

        MetricsSnapshot {
            uptime,
            memory_usage_bytes: *memory,
            operation_stats,
        }
    }

    pub fn print_live_stats(&self) {
        let stats = self.get_stats();
        
                // Clear screen and move cursor to top
        print!("\x1B[2J\x1B[1;1H");
        
        println!("🚀 RustDB Live Performance Metrics");
        println!("══════════════════════════════════════");
        println!("Uptime: {:?}", stats.uptime);
        println!("Memory Usage: {:.2} MB", stats.memory_usage_bytes as f64 / 1024.0 / 1024.0);
        println!();
        
        println!("📊 Operation Statistics:");
        println!("┌─────────────────┬─────────┬─────────────┬─────────────┬─────────────┐");
        println!("│ Operation       │ Count   │ Total Time  │ Avg Time    │ Ops/sec     │");
        println!("├─────────────────┼─────────┼─────────────┼─────────────┼─────────────┤");
        
        for (op, stats) in &stats.operation_stats {
            println!("│ {:<15} │ {:<7} │ {:<11.2}s │ {:<11.2}ms │ {:<11.2} │",
                op,
                stats.count,
                stats.total_duration.as_secs_f64(),
                stats.average_duration.as_secs_f64() * 1000.0,
                stats.ops_per_second
            );
        }
        
        println!("└─────────────────┴─────────┴─────────────┴─────────────┴─────────────┘");
        println!("\nPress Ctrl+C to exit live monitoring");
    }
}

#[derive(Debug)]
pub struct MetricsSnapshot {
    pub uptime: Duration,
    pub memory_usage_bytes: usize,
    pub operation_stats: HashMap<String, OperationStats>,
}

#[derive(Debug)]
pub struct OperationStats {
    pub count: u64,
    pub total_duration: Duration,
    pub average_duration: Duration,
    pub ops_per_second: f64,
}

// Helper macro for timing operations
#[macro_export]
macro_rules! time_operation {
    ($metrics:expr, $operation:expr, $code:block) => {{
        let start = std::time::Instant::now();
        let result = $code;
        let duration = start.elapsed();
        $metrics.record_operation($operation, duration);
        result
    }};
}