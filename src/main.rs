use clap::Parser;
use qlib_rs::{Context, EntitySchema, Single};
use std::path::PathBuf;

mod app;
mod network;
mod store;
mod config;
mod websocket;
mod discovery;

use std::sync::Arc;

use openraft::Config;

use crate::{app::App, network::Network, store::{LogStore, StateMachineStore}};

#[derive(Parser, Clone, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Opt {
    #[clap(long)]
    pub id: u64,

    #[clap(long, help = "WebSocket address to bind to (e.g., 127.0.0.1:8080)")]
    pub ws_addr: String,

    #[clap(long, help = "Path to the YAML schema configuration file", default_value = "schemas.yaml")]
    pub config_file: Option<PathBuf>,

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
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));
    
    let options = Opt::parse();
    let node_id = options.id;
    let ws_addr = options.ws_addr;
    
    // Parse the port from ws_addr for mDNS discovery
    let port = ws_addr.split(':').nth(1)
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(8080);
    
    // Create a configuration for the raft instance.
    let config = Config {
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        ..Default::default()
    };

    let config = Arc::new(config.validate().unwrap());

    // Create log store configuration
    let log_config = store::LogStoreConfig {
        max_log_files: options.max_log_files,
        max_log_size_mb: options.max_log_size_mb,
        cleanup_interval: options.log_cleanup_interval,
    };

    // Create a instance of where the Raft logs will be stored.
    let log_store = LogStore::new_for_node_with_config(options.data_dir.clone(), node_id, log_config).map_err(|e| {
        log::error!("Failed to create log store: {}", e);
        e
    })?;
    
    // Load existing state if available
    if let Err(e) = log_store.load_existing_state().await {
        log::warn!("Failed to load existing log state: {}", e);
    }
    
    // Create a instance of where the Raft data will be stored.
    let state_machine_store = Arc::new(StateMachineStore::new_for_node(options.data_dir.clone(), node_id).map_err(|e| {
        log::error!("Failed to create state machine store: {}", e);
        e
    })?);
    
    // Load existing state if available
    if let Err(e) = state_machine_store.load_existing_state().await {
        log::warn!("Failed to load existing state machine state: {}", e);
    }

    // Load schema definitions from config file but don't apply them yet
    // They will be applied during cluster initialization only for fresh clusters
    let (schemas, tree_nodes) = if let Some(config_path) = options.config_file {
        match config::load_schemas_from_yaml(&config_path) {
            Ok((schemas, tree_nodes)) => {
                log::info!("Successfully loaded {} schema definitions from config file", schemas.len());
                (Some(schemas), tree_nodes)
            },
            Err(err) => {
                log::error!("Failed to load schemas from config file: {}", err);
                return Err(format!("Failed to load schemas from config file: {}", err).into());
            }
        }
    } else {
        log::info!("No config file provided, schemas will need to be set manually");
        (None, None)
    };

    log::info!("Creating network layer...");
    // Create the network layer that will connect and communicate the raft instances and
    // will be used in conjunction with the store created above.
    let network = Network::default();

    log::info!("Creating raft instance...");
    // Create a local raft instance.
    log::debug!("Calling Raft::new with node_id={}, config={:?}", node_id, config);
    let raft = openraft::Raft::new(
        node_id,
        config.clone(),
        network.clone(),
        log_store.clone(),
        state_machine_store.clone(),
    )
    .await
    .unwrap();
    log::info!("Raft instance created successfully");

    log::info!("Creating application...");
    // Create an application that will store all the instances created above, this will
    // later be used on the websocket services.
    let app = Arc::new(App {
        id: node_id,
        addr: ws_addr.clone(),
        raft,
        state_machine_store,
        network,
    });

    log::info!("Application setup complete. Starting services...");

    // Start mDNS discovery if enabled
    if options.enable_discovery {
        log::info!("Starting mDNS discovery...");
        
        match discovery::MdnsDiscovery::new(node_id, port) {
            Ok(discovery) => {
                let discovery = Arc::new(discovery);
                match discovery.start_discovery().await {
                    Ok(mut discovery_rx) => {
                        // Clone app for the discovery task
                        let app_clone = app.clone();
                        let min_nodes = options.min_nodes;
                        let auto_init = options.auto_init;
                        let discovery_timeout = std::time::Duration::from_secs(options.discovery_timeout);
                        let schemas_clone = schemas.clone();
                        let tree_nodes_clone = tree_nodes.clone();
                        
                        // Spawn discovery handling task
                        tokio::spawn(async move {
                            let mut discovered_nodes = Vec::new();
                            let mut initialized = false;
                            
                            // Wait for initial discovery timeout or minimum nodes
                            if min_nodes > 0 {
                                log::info!("Waiting for at least {} nodes (timeout: {}s)...", min_nodes, options.discovery_timeout);
                                
                                let start_time = std::time::Instant::now();
                                while discovered_nodes.len() < min_nodes && start_time.elapsed() < discovery_timeout {
                                    tokio::select! {
                                        Some(discovered_node) = discovery_rx.recv() => {
                                            discovered_nodes.push(discovered_node.clone());
                                            log::info!("Discovered node {} ({}/{})", discovered_node.node_id, discovered_nodes.len(), min_nodes);
                                                         if discovered_nodes.len() >= min_nodes && auto_init && !initialized {
                                    log::info!("Minimum nodes reached, auto-initializing cluster...");
                                    if let Err(e) = initialize_cluster_with_nodes(&app_clone, &discovered_nodes).await {
                                        log::error!("Failed to initialize cluster: {}", e);
                                    } else {
                                        log::info!("Cluster initialized successfully");
                                        initialized = true;
                                        
                                        // Initialize store with schemas only for fresh clusters
                                        if let Ok(is_fresh) = is_fresh_cluster(&app_clone).await {
                                            if is_fresh {
                                                log::info!("Detected fresh cluster, initializing with schemas...");
                                                if let Err(e) = initialize_store_with_schemas(&app_clone, &schemas_clone, &tree_nodes_clone).await {
                                                    log::error!("Failed to initialize store with schemas: {}", e);
                                                } else {
                                                    log::info!("Store initialized successfully with schemas");
                                                }
                                            } else {
                                                log::info!("Cluster already has data, skipping schema initialization");
                                            }
                                        } else {
                                            log::warn!("Could not determine if cluster is fresh, skipping schema initialization");
                                        }
                                    }
                                }
                                        }
                                        _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                                            // Continue checking
                                        }
                                    }
                                }
                                
                                if discovered_nodes.len() < min_nodes {
                                    log::warn!("Discovery timeout reached. Found {} nodes, expected at least {}", discovered_nodes.len(), min_nodes);
                                    if auto_init && !initialized && !discovered_nodes.is_empty() {
                                        log::info!("Auto-initializing cluster with available nodes...");
                                        if let Err(e) = initialize_cluster_with_nodes(&app_clone, &discovered_nodes).await {
                                            log::error!("Failed to initialize cluster: {}", e);
                                        } else {
                                            log::info!("Cluster initialized successfully with available nodes");
                                            initialized = true;
                                            
                                            // Initialize store with schemas only for fresh clusters
                                            if let Ok(is_fresh) = is_fresh_cluster(&app_clone).await {
                                                if is_fresh {
                                                    log::info!("Detected fresh cluster, initializing with schemas...");
                                                    if let Err(e) = initialize_store_with_schemas(&app_clone, &schemas_clone, &tree_nodes_clone).await {
                                                        log::error!("Failed to initialize store with schemas: {}", e);
                                                    } else {
                                                        log::info!("Store initialized successfully with schemas");
                                                    }
                                                } else {
                                                    log::info!("Cluster already has data, skipping schema initialization");
                                                }
                                            } else {
                                                log::warn!("Could not determine if cluster is fresh, skipping schema initialization");
                                            }
                                        }
                                    }
                                }
                            } else {
                                // No minimum nodes required - for single node clusters, initialize immediately
                                log::info!("Discovery enabled with no minimum node requirement");
                                
                                // Wait a short time to see if we discover any other nodes
                                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                                
                                if discovered_nodes.is_empty() && auto_init && !initialized {
                                    // Check if cluster is already initialized before attempting initialization
                                    if is_cluster_already_initialized(&app_clone).await {
                                        log::info!("Cluster is already initialized, skipping initialization");
                                        initialized = true;
                                        
                                        // Initialize store with schemas only for fresh clusters
                                        if let Ok(is_fresh) = is_fresh_cluster(&app_clone).await {
                                            if is_fresh {
                                                log::info!("Detected fresh cluster, initializing with schemas...");
                                                if let Err(e) = initialize_store_with_schemas(&app_clone, &schemas_clone, &tree_nodes_clone).await {
                                                    log::error!("Failed to initialize store with schemas: {}", e);
                                                } else {
                                                    log::info!("Store initialized successfully with schemas");
                                                }
                                            } else {
                                                log::info!("Cluster already has data, skipping schema initialization");
                                            }
                                        } else {
                                            log::warn!("Could not determine if cluster is fresh, skipping schema initialization");
                                        }
                                    } else {
                                        log::info!("No other nodes discovered, initializing as single-node cluster...");
                                        if let Err(e) = initialize_single_node_cluster(&app_clone).await {
                                            log::error!("Failed to initialize single-node cluster: {}", e);
                                        } else {
                                            log::info!("Single-node cluster initialized successfully");
                                            initialized = true;
                                            
                                            // Initialize store with schemas only for fresh clusters
                                            if let Ok(is_fresh) = is_fresh_cluster(&app_clone).await {
                                                if is_fresh {
                                                    log::info!("Detected fresh cluster, initializing with schemas...");
                                                    if let Err(e) = initialize_store_with_schemas(&app_clone, &schemas_clone, &tree_nodes_clone).await {
                                                        log::error!("Failed to initialize store with schemas: {}", e);
                                                    } else {
                                                        log::info!("Store initialized successfully with schemas");
                                                    }
                                                } else {
                                                    log::info!("Cluster already has data, skipping schema initialization");
                                                }
                                            } else {
                                                log::warn!("Could not determine if cluster is fresh, skipping schema initialization");
                                            }
                                        }
                                    }
                                }
                            }
                            
                            // Continue processing discovery events
                            while let Some(discovered_node) = discovery_rx.recv().await {
                                log::info!("Discovered new node: {}", discovered_node.node_id);
                                discovered_nodes.push(discovered_node);
                                
                                // If auto-init is enabled and we haven't initialized yet, try to initialize
                                if auto_init && !initialized && discovered_nodes.len() >= 1 {
                                    log::info!("Auto-initializing cluster with {} discovered nodes...", discovered_nodes.len());
                                    if let Err(e) = initialize_cluster_with_nodes(&app_clone, &discovered_nodes).await {
                                        log::error!("Failed to initialize cluster: {}", e);
                                    } else {
                                        log::info!("Cluster initialized successfully");
                                        initialized = true;
                                        
                                        // Initialize store with schemas only for fresh clusters
                                        if let Ok(is_fresh) = is_fresh_cluster(&app_clone).await {
                                            if is_fresh {
                                                log::info!("Detected fresh cluster, initializing with schemas...");
                                                if let Err(e) = initialize_store_with_schemas(&app_clone, &schemas_clone, &tree_nodes_clone).await {
                                                    log::error!("Failed to initialize store with schemas: {}", e);
                                                } else {
                                                    log::info!("Store initialized successfully with schemas");
                                                }
                                            } else {
                                                log::info!("Cluster already has data, skipping schema initialization");
                                            }
                                        } else {
                                            log::warn!("Could not determine if cluster is fresh, skipping schema initialization");
                                        }
                                    }
                                }
                            }
                        });
                    }
                    Err(e) => {
                        log::warn!("Failed to start mDNS discovery: {}. Continuing without discovery.", e);
                    }
                }
            }
            Err(e) => {
                log::warn!("Failed to create mDNS discovery: {}. Continuing without discovery.", e);
            }
        }
    } else {
        // Discovery is disabled, check if cluster is already initialized
        if is_cluster_already_initialized(&app).await {
            log::info!("Cluster is already initialized, skipping initialization");
        } else {
            log::info!("Discovery disabled, initializing as single-node cluster...");
            if let Err(e) = initialize_single_node_cluster(&app).await {
                log::error!("Failed to initialize single-node cluster: {}", e);
                return Err(format!("Failed to initialize single-node cluster: {}", e).into());
            }
            log::info!("Single-node cluster initialized successfully");
        }
        
        // Initialize store with schemas only for fresh clusters
        if let Ok(is_fresh) = is_fresh_cluster(&app).await {
            if is_fresh {
                log::info!("Detected fresh cluster, initializing with schemas...");
                if let Err(e) = initialize_store_with_schemas(&app, &schemas, &tree_nodes).await {
                    log::error!("Failed to initialize store with schemas: {}", e);
                } else {
                    log::info!("Store initialized successfully with schemas");
                }
            } else {
                log::info!("Cluster already has data, skipping schema initialization");
            }
        } else {
            log::warn!("Could not determine if cluster is fresh, skipping schema initialization");
        }
    }

    log::info!("Starting WebSocket server on {}...", ws_addr);
    // Start the websocket server.
    websocket::start_websocket_server(ws_addr, app).await.map_err(|e| e.into())
}

async fn initialize_cluster_with_nodes(
    app: &Arc<App>, 
    nodes: &[discovery::DiscoveredNode]
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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

async fn is_fresh_cluster(app: &Arc<App>) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {    
    // Check if any schemas exist in the store by directly accessing the state machine
    // This avoids the Raft layer since GetSchema is a read-only operation
    let entity_type = qlib_rs::EntityType::from("Object".to_string());
    let ctx = qlib_rs::Context {};
    
    // Access the state machine directly for read operations
    let state_machine = app.state_machine_store.state_machine.read().await;
    match state_machine.data.lock().unwrap().get_entity_schema(&ctx, &entity_type) {
        Ok(_schema) => {
            log::debug!("Found existing schema for 'Object', cluster is not fresh");
            Ok(false)
        }
        Err(e) => {
            let error_msg = e.to_string();
            if error_msg.contains("not found") || error_msg.contains("does not exist") {
                log::debug!("No schema found for 'Object', cluster appears fresh");
                Ok(true)
            } else {
                log::debug!("Error checking schema (but not 'not found'): {}, assuming not fresh", error_msg);
                Ok(false)
            }
        }
    }
}

async fn initialize_store_with_schemas(
    app: &Arc<App>,
    schemas: &Option<Vec<EntitySchema<Single>>>,
    tree_nodes: &Option<Vec<config::YamlEntityTreeNode>>
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use crate::store::{CommandRequest, CommandResponse};
    
    let ctx = Context {};
    
    // Apply schemas if provided
    if let Some(schemas) = schemas {
        log::info!("Initializing store with {} schemas via Raft...", schemas.len());
        
        for (index, schema) in schemas.iter().enumerate() {
            log::info!("Setting entity schema via Raft: {} ({}/{})", schema.entity_type, index + 1, schemas.len());
            
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
        
        log::info!("All schemas applied successfully via Raft");
    }
    
    // Create initial entity tree if provided
    if let Some(tree) = tree_nodes {
        log::info!("Creating entity tree structure via Raft...");
        
        // Create entities using Raft commands
        match create_entity_tree_via_raft(app, &ctx, tree, None).await {
            Ok(entities) => {
                log::info!("Successfully created {} entities from tree definition via Raft", entities.len());
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
) -> Result<Vec<qlib_rs::EntityId>, Box<dyn std::error::Error + Send + Sync>> {
    use crate::store::{CommandRequest, CommandResponse};
    
    let mut created_entities = Vec::new();
    
    for node in tree_nodes {
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
                        log::info!("Successfully created entity {} of type {}", entity_id, node.entity_type);
                        created_entities.push(entity_id.clone());
                        
                        // Set attributes if provided using UpdateEntity
                        if let Some(attributes) = &node.attributes {
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
