use clap::Parser;
use qlib_rs::{Context};
use std::path::PathBuf;

mod app;
mod network;
mod store;
mod config;
mod websocket;

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
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Initialize logging
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));
    
    let options = Opt::parse();
    let node_id = options.id;
    let ws_addr = options.ws_addr;
    
    // Create a configuration for the raft instance.
    let config = Config {
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        ..Default::default()
    };

    let config = Arc::new(config.validate().unwrap());

    // Create a instance of where the Raft logs will be stored.
    let log_store = LogStore::default();
    // Create a instance of where the Raft data will be stored.
    let state_machine_store = Arc::new(StateMachineStore::default());

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
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Failed to load schemas from config file",
                    ));
                }
            }
        } else {
            log::error!("No config file provided, using default schemas");
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "No config file provided",
            ));
        };
        
        // Apply the schemas to the store
        for schema in schemas {
            log::info!("Setting entity schema: {}", schema.entity_type);
            store.data.set_entity_schema(&ctx, &schema).expect("Failed to set entity schema");
        }

        // Create the initial tree structure if provided
        if let Some(tree) = tree_nodes {
            match config::create_entity_tree(&mut store.data, &ctx, &tree, None).await {
                Ok(entities) => {
                    log::info!("Successfully created {} entities from tree definition", entities.len());
                }
                Err(err) => {
                    log::error!("Failed to create entity tree: {}", err);
                    // Continue even if tree creation fails
                }
            }
        }
    }

    // Create the network layer that will connect and communicate the raft instances and
    // will be used in conjunction with the store created above.
    let network = Network::default();

    // Create a local raft instance.
    let raft = openraft::Raft::new(
        node_id,
        config.clone(),
        network,
        log_store.clone(),
        state_machine_store.clone(),
    )
    .await
    .unwrap();

    // Create an application that will store all the instances created above, this will
    // later be used on the websocket services.
    let app = Arc::new(App {
        id: node_id,
        addr: ws_addr.clone(),
        raft,
        state_machine_store,
    });

    // Start the websocket server.
    websocket::start_websocket_server(ws_addr, app).await
}
