mod wal;
mod clients;
mod peers;
mod store;
mod snapshot;
mod files;
mod auth;

use anyhow::Result;
use clap::Parser;

use crate::{auth::AuthHandle, clients::ClientHandle, peers::PeerHandle, snapshot::SnapshotHandle, store::StoreHandle, wal::WalHandle};

/// Configuration passed via CLI arguments
#[derive(Parser, Clone, Debug)]
#[command(name = "core-service", about = "QOS Core Service runtime datastore")]
pub struct Config {
    /// Machine ID (unique identifier for this instance)
    #[arg(long)]
    pub machine: String,

    /// Data directory for storing WAL files and other persistent data
    #[arg(long, default_value = "./data")]
    pub data_dir: String,

    /// Maximum WAL file size in MB
    #[arg(long, default_value_t = 1)]
    pub wal_max_file_size: usize,

    /// Maximum number of WAL files to keep
    #[arg(long, default_value_t = 30)]
    pub wal_max_files: usize,

    /// Number of WAL file rollovers before taking a snapshot
    #[arg(long, default_value_t = 3)]
    pub snapshot_wal_interval: u64,

    /// Maximum number of snapshot files to keep
    #[arg(long, default_value_t = 5)]
    pub snapshot_max_files: usize,

    /// Port for peer-to-peer communication
    #[arg(long, default_value_t = 9000)]
    pub peer_port: u16,

    /// Port for client communication (StoreProxy clients)
    #[arg(long, default_value_t = 9100)]
    pub client_port: u16,

    /// List of peer addresses to connect to (format: host:port)
    #[arg(long, value_delimiter = ',')]
    pub peer_addresses: Vec<String>,

    /// Interval in seconds to retry connecting to peers
    #[arg(long, default_value_t = 3)]
    pub peer_reconnect_interval_secs: u64,

    /// Grace period in seconds to wait after becoming unavailable before requesting full sync
    #[arg(long, default_value_t = 5)]
    pub full_sync_grace_period_secs: u64,

    /// Delay in seconds after startup before self-promoting to leader when no peers are available
    #[arg(long, default_value_t = 5)]
    pub self_promotion_delay_secs: u64,
}

#[derive(Debug, Clone)]
pub struct Services {
    pub auth_handle: AuthHandle,
    pub client_handle: ClientHandle,
    pub peer_handle: PeerHandle,
    pub snapshot_handle: SnapshotHandle,
    pub store_handle: StoreHandle,
    pub wal_handle: WalHandle,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::parse();

    // Initialize tracing with better structured logging
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "qcore_rs=debug,tokio=warn,tokio_tungstenite=warn".to_string())
        )
        .with_target(true)
        .with_thread_ids(true)
        .with_file(cfg!(debug_assertions))
        .with_line_number(cfg!(debug_assertions))
        .init();

    // Initialize services in dependency order
    
    // 1. Start authentication service (no dependencies)
    let auth_handle = crate::auth::AuthenticationService::spawn();
    
    // 2. Start store service (no dependencies)
    let store_handle = crate::store::StoreService::spawn();
    
    // 3. Start snapshot service (no dependencies)
    let snapshot_config = crate::snapshot::SnapshotConfig {
        snapshots_dir: std::path::PathBuf::from(&config.data_dir).join("snapshots"),
        max_files: config.snapshot_max_files,
    };
    let snapshot_handle = crate::snapshot::SnapshotService::spawn(snapshot_config);
    
    // 4. Start WAL service (temporarily with placeholder dependencies)
    let wal_config = crate::wal::WalConfig {
        wal_dir: std::path::PathBuf::from(&config.data_dir).join("wal"),
        max_file_size: config.wal_max_file_size * 1024 * 1024,
        max_files: config.wal_max_files,
        snapshot_wal_interval: config.snapshot_wal_interval,
        machine_id: config.machine.clone(),
    };
    let wal_handle = crate::wal::WalService::spawn(wal_config, snapshot_handle.clone(), store_handle.clone());
    
    // 5. Start client service (temporarily with store dependency)
    let client_config = crate::clients::ClientConfig::from(&config);
    let client_handle = crate::clients::ClientService::spawn(client_config, store_handle.clone());
    
    // 6. Start peer service (temporarily with store dependency and connections)
    let peer_config = crate::peers::PeerConfig::from(&config);
    let connections = std::sync::Arc::new(tokio::sync::Mutex::new(crate::peers::ConnectionState {
        connected_outbound_peers: std::collections::HashMap::new(),
        connected_clients: std::collections::HashMap::new(),
        authenticated_clients: std::collections::HashMap::new(),
    }));
    let peer_handle = crate::peers::PeerService::spawn(peer_config, store_handle.clone(), connections);

    // Create services struct
    let services = Services {
        auth_handle: auth_handle.clone(),
        client_handle: client_handle.clone(),
        peer_handle: peer_handle.clone(),
        snapshot_handle: snapshot_handle.clone(),
        store_handle: store_handle.clone(),
        wal_handle: wal_handle.clone(),
    };

    // Set services for all services that need dependencies
    wal_handle.set_services(services.clone()).await;
    client_handle.set_services(services.clone()).await;
    peer_handle.set_services(services.clone()).await;
    auth_handle.set_services(services.clone()).await;
    store_handle.set_services(services.clone()).await;
    snapshot_handle.set_services(services.clone()).await;

    // Keep the main task alive
    tokio::signal::ctrl_c().await?;
    tracing::info!("Received shutdown signal, terminating...");

    Ok(())
}
