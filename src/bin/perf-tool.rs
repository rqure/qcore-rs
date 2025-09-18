use anyhow::{Context, Result};
use clap::Parser;
use qlib_rs::{StoreProxy, EntityType, EntityId, Value, FieldType, PageOpts, swrite, sstr, sint};
use serde_json;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::thread;
use tracing::{info, warn, error, debug};
use tracing_subscriber;
use chrono;
use fastrand;

/// Performance testing tool for QCore service
#[derive(Parser, Clone)]
#[command(author, version, about, long_about = None)]
struct Config {
    /// QCore WebSocket URLs (comma-separated for multiple targets)
    #[arg(long, default_value = "localhost:9100,localhost:9101", value_delimiter = ',')]
    core_urls: Vec<String>,

    /// Username for authentication
    #[arg(long, default_value = "qei")]
    username: String,

    /// Password for authentication
    #[arg(long, default_value = "qei")]
    password: String,

    /// Number of concurrent clients
    #[arg(long, default_value_t = 10)]
    clients: usize,

    /// Test duration in seconds
    #[arg(long, default_value_t = 30)]
    duration: u64,

    /// Requests per second per client (0 = unlimited)
    #[arg(long, default_value_t = 0)]
    rps: u64,

    /// Test type to run
    #[arg(long, default_value = "mixed")]
    test_type: TestType,

    /// Entity type to search/read from during testing. Write operations only target PerfTestEntity.
    #[arg(long, default_value = "Service")]
    entity_type: String,

    /// Warmup duration in seconds
    #[arg(long, default_value_t = 5)]
    warmup: u64,

    /// Show detailed latency percentiles
    #[arg(long)]
    detailed_stats: bool,

    /// Output results in JSON format
    #[arg(long)]
    json_output: bool,

    /// Enable verbose logging
    #[arg(long)]
    verbose: bool,
}

#[derive(Debug, Clone, clap::ValueEnum)]
enum TestType {
    /// Read-only operations (EntityExists, FindEntities)
    ReadOnly,
    /// Write operations (Update test entity fields)
    WriteOnly,
    /// Mixed read/write operations
    Mixed,
    /// Entity update test (updates the test entity)
    Create,
    /// Entity search test
    Search,
    /// Bulk operations test
    Bulk,
}

#[derive(Debug, Clone)]
struct ClientResult {
    total_requests: u64,
    successful_requests: u64,
    failed_requests: u64,
    latencies: Vec<Duration>,
}

impl ClientResult {
    fn new() -> Self {
        Self {
            total_requests: 0,
            successful_requests: 0,
            failed_requests: 0,
            latencies: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
struct TestResult {
    total_requests: u64,
    successful_requests: u64,
    failed_requests: u64,
    latencies: Vec<Duration>,
}

impl TestResult {
    fn new() -> Self {
        Self {
            total_requests: 0,
            successful_requests: 0,
            failed_requests: 0,
            latencies: Vec::new(),
        }
    }

    fn merge_from_clients(client_results: &[ClientResult]) -> Self {
        let mut result = Self::new();
        
        for client_result in client_results {
            result.total_requests += client_result.total_requests;
            result.successful_requests += client_result.successful_requests;
            result.failed_requests += client_result.failed_requests;
            result.latencies.extend(client_result.latencies.iter().cloned());
        }
        
        result
    }
}

fn load_existing_entities_from_topology(config: &Config) -> Result<Vec<EntityId>> {
    let mut entities = Vec::new();
    
    // Connect to load topology data using the first URL
    let store = StoreProxy::connect_and_authenticate(
        &config.core_urls[0],
        &config.username,
        &config.password,
    ).context("Failed to connect for topology loading")?;
    
    let entity_type_filter = EntityType::from(config.entity_type.as_str());
    
    // Find existing entities of the specified type
    let found_entities = store.find_entities(&entity_type_filter, None)
        .context("Failed to find entities")?;
    entities.extend(found_entities);
    
    info!("Found {} existing {} entities in topology", entities.len(), config.entity_type);
    Ok(entities)
}

fn find_test_entity(config: &Config) -> Result<EntityId> {
    let store = StoreProxy::connect_and_authenticate(
        &config.core_urls[0],
        &config.username,
        &config.password,
    ).context("Failed to connect for finding test entity")?;
    
    let perf_test_entity_type = EntityType::from("PerfTestEntity");
    
    // Find the TestEntity by searching for entities with Name = "TestEntity"
    let entities = store.find_entities(&perf_test_entity_type, Some("Name == 'TestEntity'".to_string()))
        .context("Failed to find test entity")?;
    
    if entities.is_empty() {
        return Err(anyhow::anyhow!("TestEntity not found in topology. Please ensure the PerfTestEntity with Name='TestEntity' exists."));
    }
    
    Ok(entities[0].clone())
}

fn run_test_client(
    config: Arc<Config>,
    client_id: usize,
    core_url: String,
    test_entities: Arc<Vec<EntityId>>,
    test_entity_id: EntityId,
) -> Result<ClientResult> {
    let mut store = StoreProxy::connect_and_authenticate(
        &core_url,
        &config.username,
        &config.password,
    ).with_context(|| format!("Client {} failed to connect to {}", client_id, core_url))?;

    debug!("Client {} connected successfully", client_id);

    let start_time = Instant::now();
    let test_duration = Duration::from_secs(config.duration);
    let rps_limit = if config.rps > 0 {
        Some(Duration::from_nanos(1_000_000_000 / config.rps))
    } else {
        None
    };

    let mut local_stats = ClientResult::new();
    let mut last_request_time = Instant::now();

    while start_time.elapsed() < test_duration {
        let request_start = Instant::now();

        // Rate limiting
        if let Some(interval) = rps_limit {
            let elapsed_since_last = last_request_time.elapsed();
            if elapsed_since_last < interval {
                thread::sleep(interval - elapsed_since_last);
            }
            last_request_time = Instant::now();
        }

        let success = match perform_test_operation(
            &mut store,
            &config,
            client_id,
            &test_entities,
            &test_entity_id,
        ) {
            Ok(_) => true,
            Err(e) => {
                debug!("Client {} request failed: {}", client_id, e);
                false
            }
        };

        let latency = request_start.elapsed();

        // Update local statistics (no locking needed)
        local_stats.total_requests += 1;
        if success {
            local_stats.successful_requests += 1;
        } else {
            local_stats.failed_requests += 1;
        }
        local_stats.latencies.push(latency);
    }

    debug!("Client {} completed {} requests", client_id, local_stats.total_requests);
    Ok(local_stats)
}

fn perform_test_operation(
    store: &mut StoreProxy,
    config: &Config,
    client_id: usize,
    test_entities: &[EntityId],
    test_entity_id: EntityId,
) -> Result<()> {
    match config.test_type {
        TestType::ReadOnly => {
            if !test_entities.is_empty() {
                let entity_id = &test_entities[client_id % test_entities.len()];
                let _exists = store.entity_exists(entity_id);
            }
        }
        TestType::WriteOnly => {
            // Use the existing TestEntity from the topology for all write operations
            store.perform(vec![
                swrite!(test_entity_id, FieldType::from("TestValue"), sint!(client_id as i64 * 1000 + fastrand::i64(1..1000))),
                swrite!(test_entity_id, FieldType::from("TestString"), sstr!(format!("PerfTest_Client_{}_Time_{}", client_id, chrono::Utc::now().timestamp()))),
                swrite!(test_entity_id, FieldType::from("TestFlag"), Some(Value::Bool(fastrand::bool()))),
            ])?;
        }
        TestType::Mixed => {
            if client_id % 3 == 0 {
                // Read operation
                if !test_entities.is_empty() {
                    let entity_id = &test_entities[client_id % test_entities.len()];
                    let _exists = store.entity_exists(entity_id);
                }
            } else if client_id % 3 == 1 {
                // Write operation to test entity only
                store.perform(vec![
                    swrite!(test_entity_id, FieldType::from("TestString"), sstr!(format!("MixedTest_Client_{}", client_id))),
                    swrite!(test_entity_id, FieldType::from("TestValue"), sint!(client_id as i64)),
                ])?;
            } else {
                // Search operation
                let entity_type = EntityType::from(config.entity_type.as_str());
                let _result = store.find_entities_paginated(&entity_type, Some(PageOpts::new(20, None)), Some(format!("Name != 'NonExistent_{}'", client_id)))?;
            }
        }
        TestType::Create => {
            // Update the existing test entity instead of creating new ones
            store.perform(vec![
                swrite!(test_entity_id, FieldType::from("TestString"), sstr!(format!("CreateTest_{}_{}_{}", client_id, chrono::Utc::now().timestamp(), fastrand::u32(..)))),
                swrite!(test_entity_id, FieldType::from("TestValue"), sint!(fastrand::i64(..))),
                swrite!(test_entity_id, FieldType::from("TestFlag"), Some(Value::Bool(true))),
            ])?;
        }
        TestType::Search => {
            let entity_type = EntityType::from(config.entity_type.as_str());
            let _result = store.find_entities_paginated(
                &entity_type,
                Some(PageOpts::new(20, None)),
                Some(format!("Name != 'NonExistent_{}'", client_id))
            )?;
        }
        TestType::Bulk => {
            // Multiple writes to the existing test entity
            let mut requests = Vec::new();
            for i in 0..10 {
                requests.push(swrite!(test_entity_id, FieldType::from("TestString"), sstr!(format!("BulkTest_{}_{}", client_id, i))));
                requests.push(swrite!(test_entity_id, FieldType::from("TestValue"), sint!(client_id as i64 * 10 + i as i64)));
                if i % 2 == 0 {
                    requests.push(swrite!(test_entity_id, FieldType::from("TestFlag"), Some(Value::Bool(i % 4 == 0))));
                }
            }
            store.perform(requests)?;
        }
    }
    Ok(())
}

fn setup_test_data(config: &Config) -> Result<Vec<EntityId>> {
    info!("Loading existing entities from topology...");
    
    // Load existing entities from topology for read operations only
    let entities = load_existing_entities_from_topology(config)?;
    
    info!("Found {} existing entities for read operations", entities.len());
    
    // We don't create any new entities - only use what exists
    Ok(entities)
}

fn main() -> Result<()> {
    let config = Arc::new(Config::parse());

    // Validate that we have at least one URL
    if config.core_urls.is_empty() {
        return Err(anyhow::anyhow!("At least one core URL must be specified"));
    }

    // Initialize tracing
    let log_level = if config.verbose {
        "perf_tool=debug,qlib_rs=info"
    } else {
        "perf_tool=info"
    };

    tracing_subscriber::fmt()
        .with_env_filter(log_level)
        .with_target(false)
        .init();

    info!("Starting QCore Performance Test");
    info!("Configuration: {} clients for {}s, test type: {:?}", 
          config.clients, config.duration, config.test_type);
    info!("Target URLs: {:?}", config.core_urls);
    info!("Clients will be distributed evenly across {} URL(s)", config.core_urls.len());

    // Find the test entity that we'll use for write operations
    let test_entity_id = find_test_entity(&config)
        .context("Failed to find test entity")?;
    info!("Found test entity: {:?}", test_entity_id);

    // Load test data
    let test_entities = Arc::new(setup_test_data(&config)?);

    // Warmup phase
    if config.warmup > 0 {
        info!("Starting warmup phase ({} seconds)...", config.warmup);
        let warmup_config = Arc::new(Config {
            duration: config.warmup,
            clients: std::cmp::min(config.clients, 5), // Use fewer clients for warmup
            ..(*config).clone()
        });
        
        let mut warmup_handles = Vec::new();
        
        for i in 0..warmup_config.clients {
            let warmup_config_clone = warmup_config.clone();
            let test_entities_clone = test_entities.clone();
            let test_entity_id_clone = test_entity_id;
            let core_url = config.core_urls[i % config.core_urls.len()].clone();
            
            let handle = thread::spawn(move || {
                match run_test_client(
                    warmup_config_clone,
                    i,
                    core_url,
                    test_entities_clone,
                    test_entity_id_clone,
                ) {
                    Ok(_) => {},
                    Err(e) => warn!("Warmup client {} failed: {}", i, e),
                }
            });
            warmup_handles.push(handle);
        }
        
        for handle in warmup_handles {
            let _ = handle.join();
        }
        
        info!("Warmup phase completed");
    }

    // Main test phase
    info!("Starting main test phase...");
    let test_start = Instant::now();
    
    let mut handles = Vec::new();
    
    for i in 0..config.clients {
        let config_clone = config.clone();
        let test_entities_clone = test_entities.clone();
        let test_entity_id_clone = test_entity_id;
        let core_url = config.core_urls[i % config.core_urls.len()].clone();
        
        let handle = thread::spawn(move || {
            run_test_client(
                config_clone,
                i,
                core_url,
                test_entities_clone,
                test_entity_id_clone,
            )
        });
        handles.push(handle);
    }

    // Wait for all clients to complete and collect results
    let mut client_results = Vec::new();
    for handle in handles {
        match handle.join() {
            Ok(Ok(result)) => client_results.push(result),
            Ok(Err(e)) => error!("Client failed: {}", e),
            Err(_) => error!("Client thread panicked"),
        }
    }

    let test_duration = test_start.elapsed();
    
    // Merge client results into global summary
    let stats = TestResult::merge_from_clients(&client_results);
    let total_requests = stats.total_requests;
    let successful_requests = stats.successful_requests;
    let failed_requests = stats.failed_requests;
    let mut latencies = stats.latencies;

    latencies.sort();
    let avg_rps = total_requests as f64 / test_duration.as_secs_f64();
    
    if config.json_output {
        let mut percentiles = std::collections::HashMap::new();
        if !latencies.is_empty() {
            percentiles.insert("p50", latencies[latencies.len() * 50 / 100].as_millis());
            percentiles.insert("p90", latencies[latencies.len() * 90 / 100].as_millis());
            percentiles.insert("p95", latencies[latencies.len() * 95 / 100].as_millis());
            percentiles.insert("p99", latencies[latencies.len() * 99 / 100].as_millis());
        }
        
        let results = serde_json::json!({
            "duration_seconds": test_duration.as_secs(),
            "total_requests": total_requests,
            "successful_requests": successful_requests,
            "failed_requests": failed_requests,
            "success_rate": successful_requests as f64 / total_requests as f64 * 100.0,
            "avg_rps": avg_rps,
            "latency_percentiles_ms": percentiles
        });
        
        println!("{}", serde_json::to_string_pretty(&results)?);
    } else {
        info!("Test completed in {:.2}s", test_duration.as_secs_f64());
        info!("Total requests: {}", total_requests);
        info!("Successful: {} ({:.1}%)", successful_requests, 
              successful_requests as f64 / total_requests as f64 * 100.0);
        info!("Failed: {} ({:.1}%)", failed_requests, 
              failed_requests as f64 / total_requests as f64 * 100.0);
        info!("Average RPS: {:.1}", avg_rps);
        
        if !latencies.is_empty() {
            info!("Latency percentiles:");
            info!("  P50: {:.1}ms", latencies[latencies.len() * 50 / 100].as_millis());
            info!("  P90: {:.1}ms", latencies[latencies.len() * 90 / 100].as_millis());
            info!("  P95: {:.1}ms", latencies[latencies.len() * 95 / 100].as_millis());
            info!("  P99: {:.1}ms", latencies[latencies.len() * 99 / 100].as_millis());
            
            if config.detailed_stats {
                info!("  Min: {:.1}ms", latencies[0].as_millis());
                info!("  Max: {:.1}ms", latencies[latencies.len() - 1].as_millis());
                info!("  Avg: {:.1}ms", latencies.iter().sum::<Duration>().as_millis() / latencies.len() as u128);
            }
        }
    }

    Ok(())
}