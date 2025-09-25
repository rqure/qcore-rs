use anyhow::{Context, Result};
use clap::Parser;
use qlib_rs::{sfield, sstr, swrite, sread, EntityId, PageOpts, Requests, StoreProxy};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::thread;
use tracing::{error, info, warn};
use tracing_subscriber;

/// QCore benchmarking tool (redis-benchmark equivalent)
#[derive(Parser, Clone)]
#[command(author, version, about = "QCore benchmark tool - redis-benchmark equivalent for QCore", long_about = None)]
struct Config {
    /// Server hostname (default localhost)
    #[arg(long, default_value = "localhost")]
    host: String,

    /// Server port (default 9100)
    #[arg(short = 'p', long, default_value_t = 9100)]
    port: u16,

    /// Username for authentication
    #[arg(long, default_value = "qei")]
    username: String,

    /// Password for authentication  
    #[arg(long, default_value = "qei")]
    password: String,

    /// Number of parallel connections (default 50)
    #[arg(short = 'c', long, default_value_t = 50)]
    clients: usize,

    /// Total number of requests (default 100000)
    #[arg(short = 'n', long, default_value_t = 100000)]
    requests: u64,

    /// Pipeline <numreq> requests. Default 1 (no pipeline)
    #[arg(short = 'P', long, default_value_t = 1)]
    pipeline: usize,

    /// Data size of SET/GET value in bytes (default 3)
    #[arg(short = 'd', long, default_value_t = 3)]
    data_size: usize,

    /// Only run the comma separated list of tests
    #[arg(short = 't', long, value_delimiter = ',')]
    tests: Option<Vec<String>>,

    /// Quiet. Just show query/sec values
    #[arg(short = 'q', long)]
    quiet: bool,

    /// Output in CSV format
    #[arg(long)]
    csv: bool,

    /// Loop. Run the tests forever
    #[arg(short = 'l', long)]
    r#loop: bool,

    /// Number of decimal places to display in latency output (default 0)
    #[arg(long, default_value_t = 3)]
    precision: usize,

    /// Entity type to use for search/read operations
    #[arg(long, default_value = "Service")]
    entity_type: String,

    /// Enable verbose logging
    #[arg(long)]
    verbose: bool,
}

#[derive(Debug, Clone, PartialEq)]
enum BenchmarkTest {
    EntityExists,
    FindEntities,
    WriteEntity,
    SearchEntities,
    BulkWrite,
    ReadField,
}

impl BenchmarkTest {
    fn from_string(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "entityexists" | "exists" => Some(Self::EntityExists),
            "findentities" | "find" => Some(Self::FindEntities),
            "writeentity" | "write" | "set" => Some(Self::WriteEntity),
            "searchentities" | "search" => Some(Self::SearchEntities),
            "bulkwrite" | "bulk" => Some(Self::BulkWrite),
            "readfield" | "read" | "get" => Some(Self::ReadField),
            _ => None,
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Self::EntityExists => "ENTITYEXISTS",
            Self::FindEntities => "FINDENTITIES",
            Self::WriteEntity => "WRITEENTITY",
            Self::SearchEntities => "SEARCHENTITIES",
            Self::BulkWrite => "BULKWRITE",
            Self::ReadField => "READFIELD",
        }
    }

    fn all_tests() -> Vec<Self> {
        vec![
            Self::EntityExists,
            Self::FindEntities,
            Self::WriteEntity,
            Self::SearchEntities,
            Self::BulkWrite,
            Self::ReadField,
        ]
    }
}

#[derive(Debug, Clone)]
struct TestResult {
    test_name: String,
    total_requests: u64,
    successful_requests: u64,
    failed_requests: u64,
    latencies: Vec<Duration>,
    duration: Duration,
}

impl TestResult {
    fn new(test_name: String) -> Self {
        Self {
            test_name,
            total_requests: 0,
            successful_requests: 0,
            failed_requests: 0,
            latencies: Vec::new(),
            duration: Duration::default(),
        }
    }

    fn requests_per_second(&self) -> f64 {
        if self.duration.as_secs_f64() > 0.0 {
            self.successful_requests as f64 / self.duration.as_secs_f64()
        } else {
            0.0
        }
    }

    fn percentile(&self, p: usize) -> Duration {
        if self.latencies.is_empty() {
            return Duration::default();
        }
        let index = (self.latencies.len() * p / 100).saturating_sub(1);
        self.latencies[index]
    }
}

struct BenchmarkContext {
    config: Arc<Config>,
    test_entities: Vec<EntityId>,
    test_entity_id: Option<EntityId>,
}

impl BenchmarkContext {
    fn new(config: Arc<Config>) -> Result<Self> {
        let mut context = Self {
            config,
            test_entities: Vec::new(),
            test_entity_id: None,
        };
        context.initialize()?;
        Ok(context)
    }

    fn initialize(&mut self) -> Result<()> {
        // Connect to load test data
        let url = format!("{}:{}", self.config.host, self.config.port);
        let store = StoreProxy::connect_and_authenticate(
            &url,
            &self.config.username,
            &self.config.password,
        ).context("Failed to connect for initialization")?;

        // Load existing entities for read operations
        if let Ok(entity_type) = store.get_entity_type(&self.config.entity_type) {
            if let Ok(entities) = store.find_entities(entity_type, None) {
                self.test_entities = entities;
                info!("Found {} existing {} entities", self.test_entities.len(), self.config.entity_type);
            }
        }

        // Find or create test entity for write operations
        if let Ok(perf_test_et) = store.get_entity_type("PerfTestEntity") {
            let entities = store.find_entities(perf_test_et, Some("Name == 'TestEntity'"))
                .unwrap_or_default();
            
            if !entities.is_empty() {
                self.test_entity_id = Some(entities[0]);
                info!("Found benchmark test entity");
            } else {
                warn!("TestEntity not found. Write operations will be limited.");
            }
        }

        Ok(())
    }
}

fn run_benchmark_test(
    config: Arc<Config>,
    context: Arc<BenchmarkContext>,
    test: BenchmarkTest,
    requests_per_client: u64,
) -> Result<TestResult> {
    let start_time = Instant::now();
    let mut handles = Vec::new();
    
    for client_id in 0..config.clients {
        let config_clone = config.clone();
        let context_clone = context.clone();
        let test_clone = test.clone();
        
        let handle = thread::spawn(move || {
            run_client_benchmark(config_clone, context_clone, test_clone, client_id, requests_per_client)
        });
        handles.push(handle);
    }

    let mut total_result = TestResult::new(test.name().to_string());
    
    for handle in handles {
        match handle.join() {
            Ok(Ok(client_result)) => {
                total_result.total_requests += client_result.total_requests;
                total_result.successful_requests += client_result.successful_requests;
                total_result.failed_requests += client_result.failed_requests;
                total_result.latencies.extend(client_result.latencies);
            }
            Ok(Err(e)) => {
                error!("Client failed: {}", e);
            }
            Err(_) => {
                error!("Client thread panicked");
            }
        }
    }

    total_result.duration = start_time.elapsed();
    total_result.latencies.sort();
    
    Ok(total_result)
}

fn run_client_benchmark(
    config: Arc<Config>,
    context: Arc<BenchmarkContext>,
    test: BenchmarkTest,
    client_id: usize,
    requests_count: u64,
) -> Result<TestResult> {
    let url = format!("{}:{}", config.host, config.port);
    let mut store = StoreProxy::connect_and_authenticate(
        &url,
        &config.username,
        &config.password,
    ).with_context(|| format!("Client {} failed to connect", client_id))?;

    let mut result = TestResult::new(test.name().to_string());
    let mut requests_processed = 0u64;
    
    while requests_processed < requests_count {
        let pipeline_start = std::time::Instant::now();
        
        // Build a pipeline of requests - multiple requests in a single Requests object
        let pipeline_size = std::cmp::min(config.pipeline, (requests_count - requests_processed) as usize);
        let mut pipeline_requests = Vec::new();
        
        // Prepare multiple requests for pipelining
        for _ in 0..pipeline_size {
            if let Some(request) = prepare_request(&mut store, &context, &test, client_id, requests_processed)? {
                pipeline_requests.push(request);
            }
            requests_processed += 1;
        }
        
        // Execute the pipeline - send all requests in a single Requests object over the wire
        let pipeline_success = if !pipeline_requests.is_empty() {
            let pipelined_requests = Requests::new(pipeline_requests);
            store.perform(pipelined_requests).is_ok()
        } else {
            // For tests that don't generate requests (like EntityExists), execute directly
            execute_non_request_operations(&mut store, &context, &test, client_id, pipeline_size).is_ok()
        };
        
        let pipeline_latency = pipeline_start.elapsed();
        
        // Record results for each request in the pipeline
        for _ in 0..pipeline_size {
            result.total_requests += 1;
            if pipeline_success {
                result.successful_requests += 1;
            } else {
                result.failed_requests += 1;
            }
            
            // Latency calculation:
            // - For non-pipelined requests (pipeline=1): use full pipeline latency (which is single request latency)
            // - For pipelined requests: divide pipeline latency by number of requests in the pipeline
            //   This represents the average server-side processing time per request in the pipeline
            let request_latency = if config.pipeline == 1 {
                // Non-pipelined: pipeline_latency represents single request latency
                pipeline_latency
            } else {
                // Pipelined: divide pipeline latency by number of requests processed in the pipeline
                pipeline_latency / pipeline_size as u32
            };
            
            result.latencies.push(request_latency);
        }
    }

    Ok(result)
}

fn prepare_request(
    store: &mut StoreProxy,
    context: &BenchmarkContext,
    test: &BenchmarkTest,
    client_id: usize,
    request_num: u64,
) -> Result<Option<qlib_rs::Request>> {
    match test {
        BenchmarkTest::WriteEntity => {
            if let Some(test_entity_id) = context.test_entity_id {
                let test_string_ft = store.get_field_type("TestString")?;
                // Generate string data of specified size
                let data = generate_test_data(context.config.data_size, client_id, request_num);
                Ok(Some(swrite!(test_entity_id, sfield![test_string_ft], sstr!(data))))
            } else {
                Ok(None)
            }
        }
        BenchmarkTest::BulkWrite => {
            if let Some(test_entity_id) = context.test_entity_id {
                let test_string_ft = store.get_field_type("TestString")?;
                // Generate string data of specified size for bulk operations
                let data = generate_test_data(context.config.data_size, client_id, request_num);
                Ok(Some(swrite!(test_entity_id, sfield![test_string_ft], sstr!(data))))
            } else {
                Ok(None)
            }
        }
        BenchmarkTest::ReadField => {
            if !context.test_entities.is_empty() {
                let entity_id = &context.test_entities[client_id % context.test_entities.len()];
                if let Ok(name_ft) = store.get_field_type("Name") {
                    Ok(Some(sread!(*entity_id, sfield![name_ft])))
                } else {
                    Ok(None)
                }
            } else {
                Ok(None)
            }
        }
        // These tests don't generate Request objects, they use direct method calls
        BenchmarkTest::EntityExists | BenchmarkTest::FindEntities | BenchmarkTest::SearchEntities => {
            Ok(None)
        }
    }
}

fn execute_non_request_operations(
    store: &mut StoreProxy,
    context: &BenchmarkContext,
    test: &BenchmarkTest,
    client_id: usize,
    pipeline_size: usize,
) -> Result<()> {
    // For non-request operations, we cannot truly pipeline them since they use direct method calls
    // However, we can still execute multiple operations in sequence to simulate pipeline behavior
    for i in 0..pipeline_size {
        match test {
            BenchmarkTest::EntityExists => {
                if !context.test_entities.is_empty() {
                    let entity_id = &context.test_entities[(client_id + i) % context.test_entities.len()];
                    let _ = store.entity_exists(*entity_id);
                }
            }
            BenchmarkTest::FindEntities => {
                let entity_type = store.get_entity_type(&context.config.entity_type)?;
                let _ = store.find_entities(entity_type, None)?;
            }
            BenchmarkTest::SearchEntities => {
                let entity_type = store.get_entity_type(&context.config.entity_type)?;
                let query = format!("Name != 'NonExistent_{}'", client_id + i);
                let _ = store.find_entities_paginated(entity_type, Some(&PageOpts::new(20, None)), Some(&query))?;
            }
            // Request-based operations should not reach here
            BenchmarkTest::WriteEntity | BenchmarkTest::BulkWrite | BenchmarkTest::ReadField => {
                return Err(anyhow::anyhow!("Request-based operation should not be executed here"));
            }
        }
    }
    Ok(())
}

fn generate_test_data(size: usize, client_id: usize, request_num: u64) -> String {
    if size == 0 {
        return String::new();
    }
    
    // Create a base string with client and request info
    let base = format!("c{}_r{}_", client_id, request_num);
    
    if size <= base.len() {
        // If requested size is smaller than base, truncate
        base.chars().take(size).collect()
    } else {
        // Pad with repeating pattern to reach desired size
        let mut result = base;
        let pattern = "abcdefghijklmnopqrstuvwxyz0123456789";
        let mut pattern_chars = pattern.chars().cycle();
        
        while result.len() < size {
            if let Some(c) = pattern_chars.next() {
                result.push(c);
            } else {
                break;
            }
        }
        
        // Ensure exact size
        result.truncate(size);
        result
    }
}

fn print_results(config: &Config, results: &[TestResult]) {
    if config.csv {
        print_csv_results(results);
    } else if config.quiet {
        print_quiet_results(config, results);
    } else {
        print_detailed_results(config, results);
    }
}

fn print_csv_results(results: &[TestResult]) {
    println!("\"test\",\"rps\",\"avg_latency_ms\",\"p50_ms\",\"p95_ms\",\"p99_ms\"");
    for result in results {
        let avg_latency = if !result.latencies.is_empty() {
            result.latencies.iter().sum::<Duration>().as_millis() / result.latencies.len() as u128
        } else {
            0
        };
        
        println!(
            "\"{}\",{:.2},{},{},{},{}",
            result.test_name,
            result.requests_per_second(),
            avg_latency,
            result.percentile(50).as_millis(),
            result.percentile(95).as_millis(),
            result.percentile(99).as_millis()
        );
    }
}

fn print_quiet_results(config: &Config, results: &[TestResult]) {
    for result in results {
        let p50_ms = result.percentile(50).as_millis() as f64 / 1000.0;
        let p50_formatted = format!("{:.precision$}", p50_ms, precision = config.precision);
        println!(
            "{}: {:.2} requests per second, p50={} msec",
            result.test_name,
            result.requests_per_second(),
            p50_formatted
        );
    }
}

fn print_detailed_results(config: &Config, results: &[TestResult]) {
    for result in results {
        println!("====== {} ======", result.test_name);
        println!("  {} requests completed in {:.2} seconds", 
                 result.successful_requests, result.duration.as_secs_f64());
        println!("  {} parallel clients", config.clients);
        println!("  {} bytes payload", config.data_size);
        
        if config.pipeline > 1 {
            println!("  {} requests per pipeline (sent together in single Requests object)", config.pipeline);
        }
        
        println!("  keep alive: 1");
        println!("");

        if !result.latencies.is_empty() {
            // Print latency distribution
            let percentiles = [50, 90, 95, 99, 100];
            for &p in &percentiles {
                let latency_ms = result.percentile(p).as_millis();
                println!("{:.2}% <= {} milliseconds", 
                         if p == 100 { 100.0 } else { p as f64 }, 
                         latency_ms);
            }
            println!("{:.2} requests per second", result.requests_per_second());
        }
        println!("");
    }
}

fn get_tests_to_run(config: &Config) -> Vec<BenchmarkTest> {
    if let Some(ref test_names) = config.tests {
        test_names
            .iter()
            .filter_map(|name| BenchmarkTest::from_string(name))
            .collect()
    } else {
        BenchmarkTest::all_tests()
    }
}

fn main() -> Result<()> {
    let config = Arc::new(Config::parse());

    // Initialize tracing
    let log_level = if config.verbose {
        "benchmark_tool=debug,qlib_rs=info"
    } else {
        "benchmark_tool=info"
    };

    tracing_subscriber::fmt()
        .with_env_filter(log_level)
        .with_target(false)
        .init();

    if !config.quiet && !config.csv {
        info!("QCore benchmark tool starting");
        info!("Target: {}:{}", config.host, config.port);
        info!("Clients: {}, Requests: {}, Data size: {} bytes", config.clients, config.requests, config.data_size);
        if config.pipeline > 1 {
            info!("Pipeline: {}", config.pipeline);
        }
    }

    // Initialize benchmark context
    let context = Arc::new(BenchmarkContext::new(config.clone())?);
    let tests_to_run = get_tests_to_run(&config);
    let requests_per_client = config.requests / config.clients as u64;

    loop {
        let mut results = Vec::new();

        for test in &tests_to_run {
            if !config.quiet && !config.csv {
                info!("Running {} benchmark...", test.name());
            }
            
            match run_benchmark_test(config.clone(), context.clone(), test.clone(), requests_per_client) {
                Ok(result) => results.push(result),
                Err(e) => {
                    error!("Failed to run {} benchmark: {}", test.name(), e);
                }
            }
        }

        print_results(&config, &results);

        if !config.r#loop {
            break;
        }

        if !config.quiet && !config.csv {
            info!("Looping...");
        }
    }

    Ok(())
}