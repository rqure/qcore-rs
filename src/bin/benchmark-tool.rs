use anyhow::{Context, Result};
use clap::Parser;
use qlib_rs::{EntityId, EntityType, FieldType, PageOpts, Value};
use qlib_rs::data::AsyncStoreProxy;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
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

    /// Number of parallel connections (default 50)
    #[arg(short = 'c', long, default_value_t = 50)]
    clients: usize,

    /// Total number of requests (default 100000)
    #[arg(short = 'n', long, default_value_t = 100000)]
    requests: u64,

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

    /// Pipeline <numreq> requests. Default 1 (no pipeline).
    #[arg(short = 'P', long, default_value_t = 1)]
    pipeline: usize,
}

#[derive(Debug, Clone, PartialEq)]
enum BenchmarkTest {
    EntityExists,
    FindEntities,
    WriteEntity,
    SearchEntities,
    ReadField,
}

impl BenchmarkTest {
    fn from_string(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "entityexists" | "exists" => Some(Self::EntityExists),
            "findentities" | "find" => Some(Self::FindEntities),
            "writeentity" | "write" | "set" => Some(Self::WriteEntity),
            "searchentities" | "search" => Some(Self::SearchEntities),
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
            Self::ReadField => "READFIELD",
        }
    }

    fn all_tests() -> Vec<Self> {
        vec![
            Self::EntityExists,
            Self::FindEntities,
            Self::WriteEntity,
            Self::SearchEntities,
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
        let index = ((self.latencies.len() * p / 100).saturating_sub(1)).min(self.latencies.len() - 1);
        self.latencies[index]
    }
}

struct BenchmarkContext {
    config: Arc<Config>,
    test_entities: Vec<EntityId>,
    test_entity_id: Option<EntityId>,
    // Cached field types for performance
    entity_type_cache: Option<EntityType>,
    test_string_ft: Option<FieldType>,
    name_ft: Option<FieldType>,
}

impl BenchmarkContext {
    fn new(config: Arc<Config>) -> Self {
        Self {
            config,
            test_entities: Vec::new(),
            test_entity_id: None,
            entity_type_cache: None,
            test_string_ft: None,
            name_ft: None,
        }
    }

    async fn initialize(&mut self) -> Result<()> {
        // Connect to load test data
        let url = format!("{}:{}", self.config.host, self.config.port);
        let store = AsyncStoreProxy::connect(&url).await
            .context("Failed to connect for initialization")?;

        // Load existing entities for read operations
        if let Ok(entity_type) = store.get_entity_type(&self.config.entity_type).await {
            self.entity_type_cache = Some(entity_type);
            if let Ok(entities) = store.find_entities(entity_type, None).await {
                self.test_entities = entities;
                info!("Found {} existing {} entities", self.test_entities.len(), self.config.entity_type);
            }
        }

        // Find or create test entity for write operations
        if let Ok(perf_test_et) = store.get_entity_type("PerfTestEntity").await {
            let entities = store.find_entities(perf_test_et, Some("Name == 'TestEntity'")).await
                .unwrap_or_default();
            
            if !entities.is_empty() {
                self.test_entity_id = Some(entities[0]);
                info!("Found benchmark test entity");
            } else {
                warn!("TestEntity not found. Write operations will be limited.");
            }
        }

        // Cache commonly used field types
        if let Ok(test_string_ft) = store.get_field_type("TestString").await {
            self.test_string_ft = Some(test_string_ft);
        }
        if let Ok(name_ft) = store.get_field_type("Name").await {
            self.name_ft = Some(name_ft);
        }

        Ok(())
    }
}

async fn run_benchmark_test(
    config: Arc<Config>,
    context: Arc<BenchmarkContext>,
    test: BenchmarkTest,
    requests_per_client: u64,
) -> Result<TestResult> {
    let start_time = Instant::now();
    let result = Arc::new(Mutex::new(TestResult::new(test.name().to_string())));
    let mut handles = Vec::new();

    // Spawn worker tasks
    for client_id in 0..config.clients {
        let config_clone = config.clone();
        let context_clone = context.clone();
        let result_clone = result.clone();
        let test_clone = test.clone();
        
        let handle = tokio::spawn(async move {
            run_client_benchmark(config_clone, context_clone, test_clone, client_id, requests_per_client, result_clone).await
        });
        handles.push(handle);
    }

    // Wait for all workers to complete
    for handle in handles {
        if let Err(e) = handle.await {
            error!("Worker task panicked: {:?}", e);
        }
    }

    let mut final_result = result.lock().await.clone();
    final_result.duration = start_time.elapsed();
    final_result.latencies.sort();
    
    Ok(final_result)
}

async fn run_client_benchmark(
    config: Arc<Config>,
    context: Arc<BenchmarkContext>,
    test: BenchmarkTest,
    client_id: usize,
    requests_count: u64,
    result: Arc<Mutex<TestResult>>,
) -> Result<()> {
    // Connect to server
    let url = format!("{}:{}", config.host, config.port);
    let store = AsyncStoreProxy::connect(&url).await
        .with_context(|| format!("Client {} failed to connect", client_id))?;

    if config.pipeline <= 1 {
        // Execute requests without pipelining
        for request_num in 0..requests_count {
            let start_time = Instant::now();
            let success = match execute_single_request(&store, &context, &test, client_id, request_num).await {
                Ok(_) => true,
                Err(e) => {
                    if config.verbose {
                        error!("Request failed: {}", e);
                    }
                    false
                }
            };
            let latency = start_time.elapsed();

            // Update results
            {
                let mut result_guard = result.lock().await;
                result_guard.total_requests += 1;
                if success {
                    result_guard.successful_requests += 1;
                } else {
                    result_guard.failed_requests += 1;
                }
                result_guard.latencies.push(latency);
            }
        }
    } else {
        // Execute requests with pipelining
        let mut request_num = 0u64;
        while request_num < requests_count {
            let pipeline_size = config.pipeline.min((requests_count - request_num) as usize);
            let start_time = Instant::now();
            
            let (successes, failures) = match execute_pipeline_requests(&store, &context, &test, client_id, request_num, pipeline_size).await {
                Ok(count) => (count, pipeline_size - count),
                Err(e) => {
                    if config.verbose {
                        error!("Pipeline failed: {}", e);
                    }
                    (0, pipeline_size)
                }
            };
            
            let latency = start_time.elapsed();
            let per_request_latency = latency / pipeline_size as u32;

            // Update results
            {
                let mut result_guard = result.lock().await;
                result_guard.total_requests += pipeline_size as u64;
                result_guard.successful_requests += successes as u64;
                result_guard.failed_requests += failures as u64;
                // Record latency for each request in the pipeline
                for _ in 0..pipeline_size {
                    result_guard.latencies.push(per_request_latency);
                }
            }
            
            request_num += pipeline_size as u64;
        }
    }

    Ok(())
}

async fn execute_pipeline_requests(
    store: &AsyncStoreProxy,
    context: &BenchmarkContext,
    test: &BenchmarkTest,
    client_id: usize,
    start_request_num: u64,
    pipeline_size: usize,
) -> Result<usize> {
    let mut pipeline = store.pipeline();
    
    // Queue all requests in the pipeline
    for i in 0..pipeline_size {
        let request_num = start_request_num + i as u64;
        if let Err(e) = queue_pipeline_request(&mut pipeline, store, context, test, client_id, request_num).await {
            if context.config.verbose {
                error!("Failed to queue request: {}", e);
            }
            // If we can't queue, execute what we have so far
            break;
        }
    }
    
    // Execute the pipeline
    let results = pipeline.execute().await?;
    
    // Count successes (all results are considered successful if execute didn't fail)
    Ok(results.len())
}

async fn queue_pipeline_request(
    pipeline: &mut qlib_rs::data::pipeline::AsyncPipeline<'_>,
    store: &AsyncStoreProxy,
    context: &BenchmarkContext,
    test: &BenchmarkTest,
    client_id: usize,
    request_num: u64,
) -> Result<()> {
    match test {
        BenchmarkTest::EntityExists => {
            if !context.test_entities.is_empty() {
                let entity_id = context.test_entities[(client_id + request_num as usize) % context.test_entities.len()];
                pipeline.entity_exists(entity_id)?;
            }
        }
        BenchmarkTest::FindEntities => {
            // Use cached entity type if available, otherwise look it up
            let entity_type = if let Some(et) = context.entity_type_cache {
                et
            } else {
                store.get_entity_type(&context.config.entity_type).await?
            };
            pipeline.find_entities(entity_type, None)?;
        }
        BenchmarkTest::WriteEntity => {
            if let Some(test_entity_id) = context.test_entity_id {
                // Use cached field type if available, otherwise look it up
                let test_string_ft = if let Some(ft) = context.test_string_ft {
                    ft
                } else {
                    store.get_field_type("TestString").await?
                };
                let data = generate_test_data(context.config.data_size, client_id, request_num);
                pipeline.write(test_entity_id, &[test_string_ft], Value::String(data), None, None, None, None)?;
            }
        }
        BenchmarkTest::SearchEntities => {
            // SearchEntities uses find_entities_paginated which is not yet supported in pipeline
            // Fall back to individual request
            return Err(anyhow::anyhow!("SearchEntities not supported in pipeline mode"));
        }
        BenchmarkTest::ReadField => {
            if !context.test_entities.is_empty() {
                let entity_id = context.test_entities[client_id % context.test_entities.len()];
                // Use cached field type if available, otherwise look it up
                let name_ft = if let Some(ft) = context.name_ft {
                    ft
                } else {
                    store.get_field_type("Name").await?
                };
                pipeline.read(entity_id, &[name_ft])?;
            }
        }
    }
    Ok(())
}

async fn execute_single_request(
    store: &AsyncStoreProxy,
    context: &BenchmarkContext,
    test: &BenchmarkTest,
    client_id: usize,
    request_num: u64,
) -> Result<()> {
    match test {
        BenchmarkTest::EntityExists => {
            if !context.test_entities.is_empty() {
                let entity_id = &context.test_entities[(client_id + request_num as usize) % context.test_entities.len()];
                let _ = store.entity_exists(*entity_id).await;
            }
        }
        BenchmarkTest::FindEntities => {
            let entity_type = store.get_entity_type(&context.config.entity_type).await?;
            let _ = store.find_entities(entity_type, None).await?;
        }
        BenchmarkTest::WriteEntity => {
            if let Some(test_entity_id) = context.test_entity_id {
                let test_string_ft = store.get_field_type("TestString").await?;
                let data = generate_test_data(context.config.data_size, client_id, request_num);
                let _ = store.write(test_entity_id, &[test_string_ft], Value::String(data), None, None, None, None).await?;
            }
        }
        BenchmarkTest::SearchEntities => {
            let entity_type = store.get_entity_type(&context.config.entity_type).await?;
            let query = format!("Name != 'NonExistent_{}'", client_id + request_num as usize);
            let _ = store.find_entities_paginated(entity_type, Some(&PageOpts::new(20, None)), Some(&query)).await?;
        }
        BenchmarkTest::ReadField => {
            if !context.test_entities.is_empty() {
                let entity_id = &context.test_entities[client_id % context.test_entities.len()];
                if let Ok(name_ft) = store.get_field_type("Name").await {
                    let _ = store.read(*entity_id, &[name_ft]).await?;
                }
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
        let avg_latency_ms = if !result.latencies.is_empty() {
            result.latencies.iter().map(|d| d.as_nanos() as f64 / 1_000_000.0).sum::<f64>() / result.latencies.len() as f64
        } else {
            0.0
        };
        
        println!(
            "\"{}\",{:.2},{:.3},{:.3},{:.3},{:.3}",
            result.test_name,
            result.requests_per_second(),
            avg_latency_ms,
            result.percentile(50).as_nanos() as f64 / 1_000_000.0,
            result.percentile(95).as_nanos() as f64 / 1_000_000.0,
            result.percentile(99).as_nanos() as f64 / 1_000_000.0
        );
    }
}

fn print_quiet_results(config: &Config, results: &[TestResult]) {
    for result in results {
        let p50_ms = result.percentile(50).as_nanos() as f64 / 1_000_000.0;
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
        println!("  keep alive: 1");
        println!("");

        if !result.latencies.is_empty() {
            // Print latency distribution
            let percentiles = [50, 90, 95, 99, 100];
            for &p in &percentiles {
                let latency_ms = result.percentile(p).as_nanos() as f64 / 1_000_000.0;
                println!("{:.2}% <= {:.3} milliseconds", 
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

#[tokio::main]
async fn main() -> Result<()> {
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
        info!("Clients: {}, Requests: {}, Data size: {} bytes", 
              config.clients, config.requests, config.data_size);
    }

    // Initialize benchmark context
    let mut context_builder = BenchmarkContext::new(config.clone());
    context_builder.initialize().await?;
    let context = Arc::new(context_builder);
    let tests_to_run = get_tests_to_run(&config);
    let requests_per_client = config.requests / config.clients as u64;

    loop {
        let mut results = Vec::new();

        for test in &tests_to_run {
            if !config.quiet && !config.csv {
                info!("Running {} benchmark...", test.name());
            }
            
            match run_benchmark_test(config.clone(), context.clone(), test.clone(), requests_per_client).await {
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