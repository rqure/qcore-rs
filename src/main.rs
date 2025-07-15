use clap::Parser;
use qlib_rs::{Context, EntitySchema, Single};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

mod app;
mod network;
mod store;
mod config;
mod websocket;
mod discovery;

use openraft::Config;
use crate::{app::App, network::Network, store::{LogStore, StateMachineStore}};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Parser, Clone, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Opt {
    #[clap(long)]
    pub id: u64,

    #[clap(long, help = "WebSocket address to bind to (e.g., 127.0.0.1:8080)")]
    pub ws_addr: String,

    #[clap(long, help = "Path to the YAML schema configuration file", default_value = "schemas.yaml")]
    pub config_file: PathBuf,

    #[clap(long, help = "Data directory for persistent storage", default_value = "./raft_data")]
    pub data_dir: PathBuf,

    #[clap(long, help = "Maximum number of log files to keep", default_value = "1000")]
    pub max_log_files: usize,

    #[clap(long, help = "Maximum total size of log files in MB", default_value = "100")]
    pub max_log_size_mb: u64,

    #[clap(long, help = "How often to check for log cleanup (every N entries)", default_value = "100")]
    pub log_cleanup_interval: u64,

    #[clap(long, help = "Enable automatic node discovery using mDNS")]
    pub enable_discovery: bool,

    #[clap(long, help = "Minimum number of nodes to wait for during discovery", default_value = "0")]
    pub min_nodes: usize,

    #[clap(long, help = "Timeout in seconds to wait for node discovery", default_value = "30")]
    pub discovery_timeout: u64,

    #[clap(long, help = "Auto-initialize cluster when minimum nodes are discovered")]
    pub auto_init: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));
    
    let options = Opt::parse();
    
    let app = create_app(&options).await?;
    
    let schemas_and_tree = load_schemas(&options.config_file)?;
    
    setup_cluster(&app, &options, &schemas_and_tree).await?;
    
    log::info!("Starting WebSocket server on {}...", options.ws_addr);
    websocket::start_websocket_server(options.ws_addr, app).await.map_err(|e| e.into())
}

async fn create_app(options: &Opt) -> Result<Arc<App>> {
    let config = create_raft_config();
    let log_store = create_log_store(options).await?;
    let state_machine_store = create_state_machine_store(options).await?;
    let network = Network::default();
    
    log::info!("Creating raft instance...");
    let raft = openraft::Raft::new(
        options.id,
        config,
        network.clone(),
        log_store.clone(),
        state_machine_store.clone(),
    ).await.unwrap();
    
    log::info!("Raft instance created successfully");
    
    Ok(Arc::new(App {
        id: options.id,
        addr: options.ws_addr.clone(),
        raft,
        state_machine_store,
        network,
        discovery: Mutex::new(None),
    }))
}

fn create_raft_config() -> Arc<Config> {
    let config = Config {
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        ..Default::default()
    };
    Arc::new(config.validate().unwrap())
}

async fn create_log_store(options: &Opt) -> Result<LogStore<crate::app::TypeConfig>> {
    let log_config = store::LogStoreConfig {
        max_log_files: options.max_log_files,
        max_log_size_mb: options.max_log_size_mb,
        cleanup_interval: options.log_cleanup_interval,
    };
    
    let log_store = LogStore::new_for_node_with_config(
        options.data_dir.clone(), 
        options.id, 
        log_config
    ).map_err(|e| {
        log::error!("Failed to create log store: {}", e);
        e
    })?;
    
    // Load existing state if available
    if let Err(e) = log_store.load_existing_state().await {
        log::warn!("Failed to load existing log state: {}", e);
    }
    
    Ok(log_store)
}

async fn create_state_machine_store(options: &Opt) -> Result<Arc<StateMachineStore>> {
    let state_machine_store = Arc::new(
        StateMachineStore::new_for_node(options.data_dir.clone(), options.id)
            .map_err(|e| {
                log::error!("Failed to create state machine store: {}", e);
                e
            })?
    );
    
    // Load existing state if available
    if let Err(e) = state_machine_store.load_existing_state().await {
        log::warn!("Failed to load existing state machine state: {}", e);
    }
    
    Ok(state_machine_store)
}

type SchemasAndTree = (Option<Vec<EntitySchema<Single>>>, Option<Vec<config::YamlEntityTreeNode>>);

fn load_schemas(config_file: &PathBuf) -> Result<SchemasAndTree> {
    let (schemas, tree_nodes) = config::load_schemas_from_yaml(config_file)
        .map_err(|e| {
            log::error!("Failed to load schemas from config file: {}", e);
            format!("Failed to load schemas from config file: {}", e)
        })?;
    
    log::info!("Successfully loaded {} schema definitions from config file", schemas.len());
    Ok((Some(schemas), tree_nodes))
}

async fn setup_cluster(
    app: &Arc<App>, 
    options: &Opt, 
    schemas_and_tree: &SchemasAndTree
) -> Result<()> {
    if options.enable_discovery {
        setup_cluster_with_discovery(app, options, schemas_and_tree).await
    } else {
        setup_single_node_cluster(app, schemas_and_tree).await
    }
}

async fn setup_cluster_with_discovery(
    app: &Arc<App>, 
    options: &Opt, 
    schemas_and_tree: &SchemasAndTree
) -> Result<()> {
    log::info!("Starting mDNS discovery...");
    
    let port = parse_port_from_addr(&options.ws_addr);
    let discovery = Arc::new(discovery::MdnsDiscovery::new(options.id, port)?);
    let discovery_rx = discovery.start_discovery().await?;
    
    let discovery_config = DiscoveryConfig {
        min_nodes: options.min_nodes,
        auto_init: options.auto_init,
        timeout: Duration::from_secs(options.discovery_timeout),
    };
    
    // Store the discovery instance in the App to keep it alive
    {
        let mut discovery_guard = app.discovery.lock().unwrap();
        *discovery_guard = Some(discovery.clone());
    }
    
    spawn_discovery_handler(app.clone(), discovery_rx, discovery_config, schemas_and_tree.clone()).await;
    
    Ok(())
}

struct DiscoveryConfig {
    min_nodes: usize,
    auto_init: bool,
    timeout: Duration,
}

async fn spawn_discovery_handler(
    app: Arc<App>,
    mut discovery_rx: tokio::sync::mpsc::Receiver<discovery::DiscoveredNode>,
    config: DiscoveryConfig,
    schemas_and_tree: SchemasAndTree,
) {
    tokio::spawn(async move {
        let mut discovered_nodes = Vec::new();
        let mut initialized = false;
        
        if config.min_nodes > 0 {
            wait_for_minimum_nodes(&mut discovery_rx, &mut discovered_nodes, &config).await;
        } else {
            // Single node case with optional discovery
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        
        if config.auto_init && !initialized {
            initialized = try_initialize_cluster(&app, &discovered_nodes, &schemas_and_tree).await;
        }
        
        // Continue processing discovery events
        while let Some(discovered_node) = discovery_rx.recv().await {
            log::info!("Discovered new node: {}", discovered_node.node_id);
            discovered_nodes.push(discovered_node);
            
            if config.auto_init && !initialized {
                initialized = try_initialize_cluster(&app, &discovered_nodes, &schemas_and_tree).await;
            }
        }
    });
}

async fn wait_for_minimum_nodes(
    discovery_rx: &mut tokio::sync::mpsc::Receiver<discovery::DiscoveredNode>,
    discovered_nodes: &mut Vec<discovery::DiscoveredNode>,
    config: &DiscoveryConfig,
) {
    log::info!("Waiting for at least {} nodes (timeout: {:?})...", config.min_nodes, config.timeout);
    
    let start_time = std::time::Instant::now();
    
    while discovered_nodes.len() < config.min_nodes && start_time.elapsed() < config.timeout {
        tokio::select! {
            Some(discovered_node) = discovery_rx.recv() => {
                discovered_nodes.push(discovered_node.clone());
                log::info!("Discovered node {} ({}/{})", 
                    discovered_node.node_id, discovered_nodes.len(), config.min_nodes);
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                // Continue checking
            }
        }
    }
    
    if discovered_nodes.len() < config.min_nodes {
        log::warn!("Discovery timeout reached. Found {} nodes, expected at least {}", 
            discovered_nodes.len(), config.min_nodes);
    }
}

async fn try_initialize_cluster(
    app: &Arc<App>,
    discovered_nodes: &[discovery::DiscoveredNode],
    schemas_and_tree: &SchemasAndTree,
) -> bool {
    if is_cluster_already_initialized(app).await {
        log::info!("Cluster is already initialized, skipping initialization");
        // Even if cluster is already initialized, we should check if schemas need to be initialized
        // This handles the case where the cluster was initialized but schemas weren't applied yet
        tokio::spawn({
            let app = app.clone();
            let schemas_and_tree = schemas_and_tree.clone();
            async move {
                if let Err(e) = initialize_schemas_if_fresh(&app, &schemas_and_tree).await {
                    log::error!("Failed to initialize schemas: {}", e);
                }
            }
        });
        return true;
    }
    
    let result = if discovered_nodes.is_empty() {
        initialize_single_node_cluster(app).await
    } else {
        initialize_cluster_with_nodes(app, discovered_nodes).await
    };
    
    match result {
        Ok(()) => {
            log::info!("Cluster initialized successfully");
            // Spawn schema initialization task to run after cluster is established
            tokio::spawn({
                let app = app.clone();
                let schemas_and_tree = schemas_and_tree.clone();
                async move {
                    if let Err(e) = initialize_schemas_if_fresh(&app, &schemas_and_tree).await {
                        log::error!("Failed to initialize schemas: {}", e);
                    }
                }
            });
            true
        }
        Err(e) => {
            log::error!("Failed to initialize cluster: {}", e);
            false
        }
    }
}

async fn setup_single_node_cluster(
    app: &Arc<App>, 
    schemas_and_tree: &SchemasAndTree
) -> Result<()> {
    if !is_cluster_already_initialized(app).await {
        log::info!("Discovery disabled, initializing as single-node cluster...");
        initialize_single_node_cluster(app).await?;
        log::info!("Single-node cluster initialized successfully");
    } else {
        log::info!("Cluster is already initialized, skipping initialization");
    }
    
    // Spawn schema initialization task to run after cluster is established
    tokio::spawn({
        let app = app.clone();
        let schemas_and_tree = schemas_and_tree.clone();
        async move {
            if let Err(e) = initialize_schemas_if_fresh(&app, &schemas_and_tree).await {
                log::error!("Failed to initialize schemas: {}", e);
            }
        }
    });
    
    Ok(())
}

async fn initialize_schemas_if_fresh(
    app: &Arc<App>,
    schemas_and_tree: &SchemasAndTree,
) -> Result<()> {
    // Wait for the cluster to establish leadership before attempting schema initialization
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // Only the leader should initialize schemas to avoid conflicts
    if !is_leader(app).await {
        log::info!("Not the leader, skipping schema initialization");
        return Ok(());
    }
    
    match is_fresh_cluster(app).await {
        Ok(true) => {
            log::info!("Leader detected fresh cluster, initializing with schemas...");
            initialize_store_with_schemas(app, &schemas_and_tree.0, &schemas_and_tree.1).await?;
            log::info!("Leader initialized store successfully with schemas");
        }
        Ok(false) => {
            log::info!("Cluster already has data, skipping schema initialization");
        }
        Err(e) => {
            log::warn!("Could not determine if cluster is fresh: {}, skipping schema initialization", e);
        }
    }
    Ok(())
}

async fn is_leader(app: &Arc<App>) -> bool {
    let metrics = app.raft.metrics().borrow().clone();
    
    // Check if we are the current leader
    if let Some(leader_id) = metrics.current_leader {
        if leader_id == app.id {
            log::debug!("Current node {} is the leader", app.id);
            return true;
        } else {
            log::debug!("Current node {} is not the leader (leader is {})", app.id, leader_id);
            return false;
        }
    }
    
    // If there's no leader yet, we're not the leader
    log::debug!("No leader elected yet");
    false
}

fn parse_port_from_addr(addr: &str) -> u16 {
    addr.split(':')
        .nth(1)
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(8080)
}

async fn initialize_cluster_with_nodes(
    app: &Arc<App>, 
    nodes: &[discovery::DiscoveredNode]
) -> Result<()> {
    use std::collections::BTreeMap;
    use openraft::BasicNode;
    
    let mut node_map = BTreeMap::new();
    
    // Add ourselves - just use the port part since we're using WebSocket addresses
    let our_port = app.addr.split(':').nth(1).unwrap_or("8080");
    let our_local_addr = format!("127.0.0.1:{}", our_port);
    node_map.insert(app.id, BasicNode { addr: our_local_addr });
    
    // Add discovered nodes using localhost addresses for consistency
    for node in nodes {
        // Convert the discovered address to use localhost
        let port = node.address.split(':').nth(1).unwrap_or("8080");
        let local_addr = format!("127.0.0.1:{}", port);
        node_map.insert(node.node_id, BasicNode { addr: local_addr });
    }
    
    log::info!("Initializing cluster with nodes: {:?}", node_map);
    
    match app.raft.initialize(node_map).await {
        Ok(_) => {
            log::info!("Cluster initialized successfully");
            Ok(())
        }
        Err(e) => {
            log::error!("Failed to initialize cluster: {}", e);
            Err(e.into())
        }
    }
}

async fn initialize_single_node_cluster(
    app: &Arc<App>
) -> Result<()> {
    use std::collections::BTreeMap;
    use openraft::BasicNode;
    
    let mut node_map = BTreeMap::new();
    
    // Add only ourselves for a single-node cluster
    let our_port = app.addr.split(':').nth(1).unwrap_or("8080");
    let our_local_addr = format!("127.0.0.1:{}", our_port);
    node_map.insert(app.id, BasicNode { addr: our_local_addr });
    
    log::info!("Initializing single-node cluster with node: {:?}", node_map);
    
    match app.raft.initialize(node_map).await {
        Ok(_) => {
            log::info!("Single-node cluster initialized successfully");
            Ok(())
        }
        Err(e) => {
            log::error!("Failed to initialize single-node cluster: {}", e);
            Err(e.into())
        }
    }
}

async fn is_cluster_already_initialized(app: &Arc<App>) -> bool {
    // Use the built-in Raft method to check if the cluster is already initialized
    match app.raft.is_initialized().await {
        Ok(initialized) => {
            log::debug!("Cluster initialization check: is_initialized={}", initialized);
            initialized
        }
        Err(e) => {
            log::warn!("Failed to check if cluster is initialized: {}, assuming not initialized", e);
            false
        }
    }
}

async fn is_fresh_cluster(app: &Arc<App>) -> Result<bool> {    
    // Check if there are any entities in the store at all
    // This avoids checking specific schemas and just looks for actual data
    let ctx = qlib_rs::Context {};
    
    // Access the state machine directly for read operations
    let state_machine = app.state_machine_store.state_machine.read().await;
    
    // Try to get all entity types - if this fails or returns empty, cluster is fresh
    match state_machine.data.lock().unwrap().get_entity_types(&ctx, None) {
        Ok(entity_types) => {
            if entity_types.items.is_empty() {
                log::debug!("No entity types found, cluster is fresh");
                Ok(true)
            } else {
                log::debug!("Found {} entity types, checking for entities", entity_types.items.len());
                
                // Check if any of these entity types have actual entities
                let mut has_entities = false;
                for entity_type in entity_types.items {
                    match state_machine.data.lock().unwrap().find_entities(&ctx, &entity_type, None) {
                        Ok(entities) => {
                            if !entities.items.is_empty() {
                                log::debug!("Found {} entities of type {}, cluster is not fresh", entities.items.len(), entity_type);
                                has_entities = true;
                                break;
                            }
                        }
                        Err(_) => {
                            // Ignore errors, might be schema issues
                            continue;
                        }
                    }
                }
                
                if has_entities {
                    Ok(false)
                } else {
                    log::debug!("No entities found in any type, cluster is fresh");
                    Ok(true)
                }
            }
        }
        Err(e) => {
            log::debug!("Error getting entity types: {}, assuming cluster is fresh", e);
            Ok(true)
        }
    }
}

async fn initialize_store_with_schemas(
    app: &Arc<App>,
    schemas: &Option<Vec<EntitySchema<Single>>>,
    tree_nodes: &Option<Vec<config::YamlEntityTreeNode>>
) -> Result<()> {
    use crate::store::{CommandRequest, CommandResponse};
    
    // Double-check that we're still the leader before proceeding
    if !is_leader(app).await {
        log::warn!("No longer the leader, aborting schema initialization");
        return Ok(());
    }
    
    let ctx = Context {};
    
    // Apply schemas if provided
    if let Some(schemas) = schemas {
        log::info!("Leader initializing store with {} schemas via Raft...", schemas.len());
        
        for (index, schema) in schemas.iter().enumerate() {
            // Check leadership before each schema to handle leadership changes
            if !is_leader(app).await {
                log::warn!("Leadership lost during schema initialization, stopping");
                return Ok(());
            }
            
            log::info!("Leader setting entity schema via Raft: {} ({}/{})", schema.entity_type, index + 1, schemas.len());
            
            // Create a Raft command to set the schema
            let request = CommandRequest::SetSchema {
                entity_schema: schema.clone(),
            };
            
            match app.raft.client_write(request).await {
                Ok(response) => {
                    match response.response() {
                        CommandResponse::SetSchema { error: None } => {
                            log::info!("Successfully set schema for entity type: {}", schema.entity_type);
                        }
                        CommandResponse::SetSchema { error: Some(e) } => {
                            log::error!("Failed to set schema for entity type {}: {}", schema.entity_type, e);
                            return Err(format!("Failed to set schema for entity type {}: {}", schema.entity_type, e).into());
                        }
                        other => {
                            log::error!("Unexpected response when setting schema for {}: {:?}", schema.entity_type, other);
                            return Err(format!("Unexpected response when setting schema for {}: {:?}", schema.entity_type, other).into());
                        }
                    }
                }
                Err(e) => {
                    log::error!("Failed to set schema for entity type {} via Raft: {}", schema.entity_type, e);
                    return Err(format!("Failed to set schema for entity type {} via Raft: {}", schema.entity_type, e).into());
                }
            }
        }
        
        log::info!("Leader applied all schemas successfully via Raft");
    }
    
    // Create initial entity tree if provided
    if let Some(tree) = tree_nodes {
        // Check leadership before creating entities
        if !is_leader(app).await {
            log::warn!("Leadership lost before entity tree creation, stopping");
            return Ok(());
        }
        
        log::info!("Leader creating entity tree structure via Raft...");
        
        // Create entities using Raft commands
        match create_entity_tree_via_raft(app, &ctx, tree, None).await {
            Ok(entities) => {
                log::info!("Leader successfully created {} entities from tree definition via Raft", entities.len());
            }
            Err(err) => {
                log::error!("Failed to create entity tree via Raft: {}", err);
                return Err(format!("Failed to create entity tree via Raft: {}", err).into());
            }
        }
    }
    
    Ok(())
}

async fn create_entity_tree_via_raft(
    app: &Arc<App>,
    _ctx: &Context,
    tree_nodes: &[config::YamlEntityTreeNode],
    parent_id: Option<qlib_rs::EntityId>
) -> Result<Vec<qlib_rs::EntityId>> {
    use crate::store::{CommandRequest, CommandResponse};
    
    let mut created_entities = Vec::new();
    
    for node in tree_nodes {
        // Check leadership before creating each entity
        if !is_leader(app).await {
            log::warn!("Leadership lost during entity tree creation, stopping");
            return Ok(created_entities);
        }
        
        // Convert string to EntityType
        let entity_type = qlib_rs::EntityType::from(node.entity_type.clone());
        
        // Create entity via Raft command
        let request = CommandRequest::CreateEntity {
            entity_type,
            parent_id: parent_id.clone(),
            name: node.name.clone(),
        };
        
        match app.raft.client_write(request).await {
            Ok(response) => {
                match response.response() {
                    CommandResponse::CreateEntity { response: Some(entity), error: None } => {
                        let entity_id = entity.entity_id.clone();
                        log::info!("Leader successfully created entity {} of type {}", entity_id, node.entity_type);
                        created_entities.push(entity_id.clone());
                        
                        // Set attributes if provided using UpdateEntity
                        if let Some(attributes) = &node.attributes {
                            // Check leadership before setting attributes
                            if !is_leader(app).await {
                                log::warn!("Leadership lost during attribute setting, stopping");
                                return Ok(created_entities);
                            }
                            
                            let mut requests = Vec::new();
                            for (field_name, value) in attributes {
                                let field_type = qlib_rs::FieldType::from(field_name.clone());
                                requests.push(qlib_rs::Request::Write {
                                    entity_id: entity_id.clone(),
                                    field_type,
                                    value: Some(value.clone().into()),
                                    push_condition: qlib_rs::PushCondition::Always,
                                    adjust_behavior: qlib_rs::AdjustBehavior::Set,
                                    write_time: None,
                                    writer_id: None,
                                });
                            }
                            
                            if !requests.is_empty() {
                                let update_request = CommandRequest::UpdateEntity { request: requests };
                                match app.raft.client_write(update_request).await {
                                    Ok(update_response) => {
                                        match update_response.response() {
                                            CommandResponse::UpdateEntity { error: None, .. } => {
                                                log::debug!("Set attributes for entity {}", entity_id);
                                            }
                                            CommandResponse::UpdateEntity { error: Some(e), .. } => {
                                                log::error!("Failed to set attributes for entity {}: {}", entity_id, e);
                                            }
                                            other => {
                                                log::warn!("Unexpected response when setting attributes for entity {}: {:?}", entity_id, other);
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        log::error!("Failed to set attributes for entity {}: {}", entity_id, e);
                                    }
                                }
                            }
                        }
                        
                        // Create children recursively if provided
                        if let Some(children) = &node.children {
                            let child_result = Box::pin(create_entity_tree_via_raft(app, _ctx, children, Some(entity_id.clone()))).await;
                            match child_result {
                                Ok(child_entities) => {
                                    created_entities.extend(child_entities);
                                }
                                Err(e) => {
                                    log::error!("Failed to create children for entity {}: {}", entity_id, e);
                                }
                            }
                        }
                    }
                    CommandResponse::CreateEntity { response: None, error: Some(e) } => {
                        log::error!("Failed to create entity {}: {}", node.name, e);
                        return Err(format!("Failed to create entity {}: {}", node.name, e).into());
                    }
                    CommandResponse::CreateEntity { response: None, error: None } => {
                        log::error!("Unexpected empty response when creating entity {}", node.name);
                        return Err(format!("Unexpected empty response when creating entity {}", node.name).into());
                    }
                    other => {
                        log::error!("Unexpected response when creating entity {}: {:?}", node.name, other);
                        return Err(format!("Unexpected response when creating entity {}: {:?}", node.name, other).into());
                    }
                }
            }
            Err(e) => {
                log::error!("Failed to create entity {} via Raft: {}", node.name, e);
                return Err(format!("Failed to create entity {} via Raft: {}", node.name, e).into());
            }
        }
    }
    
    Ok(created_entities)
}
