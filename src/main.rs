use clap::Parser;
use qlib_rs::{Context};
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

    #[clap(long, help = "Minimum number of nodes to wait for during discovery", default_value = "1")]
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

    {
        let mut store = state_machine_store.state_machine.write().await;

        let ctx = Context {};
        
        // Load schemas from YAML if config file is provided, otherwise use defaults
        let (schemas, tree_nodes) = if let Some(config_path) = options.config_file {
            match config::load_schemas_from_yaml(&config_path) {
                Ok((schemas, tree_nodes)) => {
                    log::info!("Successfully loaded {} schemas from config file", schemas.len());
                    (schemas, tree_nodes)
                },
                Err(err) => {
                    log::error!("Failed to load schemas from config file: {}", err);
                    return Err(format!("Failed to load schemas from config file: {}", err).into());
                }
            }
        } else {
            log::error!("No config file provided, using default schemas");
            return Err("No config file provided".into());
        };
        
        // Apply the schemas to the store
        for (index, schema) in schemas.iter().enumerate() {
            log::info!("Setting entity schema: {} ({}/{})", schema.entity_type, index + 1, schemas.len());
            log::debug!("About to set schema for entity type: {}", schema.entity_type);
            log::debug!("Schema details: inherit={:?}, fields_count={}", 
                       schema.inherit, schema.fields.len());
            
            // Try to set the schema with error handling and timeout
            let schema_future = async {
                store.data.set_entity_schema(&ctx, &schema)
            };
            
            match tokio::time::timeout(std::time::Duration::from_secs(5), schema_future).await {
                Ok(Ok(_)) => {
                    log::debug!("Successfully set schema for entity type: {}", schema.entity_type);
                }
                Ok(Err(e)) => {
                    log::error!("Failed to set schema for entity type {}: {:?}", schema.entity_type, e);
                    return Err(format!("Failed to set schema for entity type {}: {:?}", schema.entity_type, e).into());
                }
                Err(_) => {
                    log::error!("Timeout setting schema for entity type: {}", schema.entity_type);
                    return Err(format!("Timeout setting schema for entity type: {}", schema.entity_type).into());
                }
            }
        }

        log::info!("Schemas applied successfully");
        
        // Create the initial tree structure if provided
        if let Some(tree) = tree_nodes {
            log::info!("Creating entity tree structure...");
            match config::create_entity_tree(&mut store.data, &ctx, &tree, None).await {
                Ok(entities) => {
                    log::info!("Successfully created {} entities from tree definition", entities.len());
                }
                Err(err) => {
                    log::error!("Failed to create entity tree: {}", err);
                    // Continue even if tree creation fails
                }
            }
        } else {
            log::info!("No tree nodes to create");
        }
    }

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
        network,
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
                                        }
                                    }
                                }
                            } else {
                                // No minimum nodes required, just listen for discoveries
                                log::info!("Discovery enabled with no minimum node requirement");
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
