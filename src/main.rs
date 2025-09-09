mod wal;
mod clients;
mod peers;
mod store;
mod snapshot;
mod files;
mod misc;

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;

use crate::{clients::{ClientConfig, ClientHandle, ClientService}, misc::{MiscConfig, MiscHandle, MiscService}, peers::{PeerConfig, PeerHandle, PeerService}, snapshot::{SnapshotConfig, SnapshotHandle, SnapshotService}, store::{StoreHandle, StoreService}, wal::{WalConfig, WalHandle, WalService}};

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
    pub client_handle: ClientHandle,
    pub misc_handle: MiscHandle,
    pub peer_handle: PeerHandle,
    pub snapshot_handle: SnapshotHandle,
    pub store_handle: StoreHandle,
    pub wal_handle: WalHandle,
}

impl Services {
    /// Perform graceful shutdown by taking a final snapshot and writing snapshot marker to WAL
    pub async fn shutdown(&self, machine_id: String) -> Result<()> {
        use qlib_rs::now;
        
        tracing::info!("Taking final snapshot before shutdown");
        
        // Take a snapshot from the store
        let snapshot = match self.store_handle.take_snapshot().await {
            Some(snapshot) => snapshot,
            None => {
                tracing::error!("Failed to take snapshot during shutdown");
                return Err(anyhow::anyhow!("Failed to take snapshot during shutdown"));
            }
        };
        
        // Save the snapshot to disk
        match self.snapshot_handle.save(snapshot).await {
            Ok(snapshot_counter) => {
                tracing::info!(
                    snapshot_counter = snapshot_counter,
                    "Final snapshot saved successfully"
                );
                
                // Write a snapshot marker to the WAL to indicate the final snapshot point
                // This helps during replay to know that the state was snapshotted at shutdown
                let snapshot_request = qlib_rs::Request::Snapshot {
                    snapshot_counter,
                    timestamp: Some(now()),
                    originator: Some(machine_id),
                };
                
                if let Err(e) = self.wal_handle.write_request(snapshot_request).await {
                    tracing::error!(error = %e, "Failed to write final snapshot marker to WAL");
                } else {
                    tracing::info!("Final snapshot marker written to WAL");
                }
            }
            Err(e) => {
                tracing::error!(error = %e, "Failed to save final snapshot");
                return Err(e);
            }
        }
        
        Ok(())
    }
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

    let store_handle = StoreService::spawn(store::StoreConfig {
        machine_id: config.machine.clone(),
    });
    let snapshot_handle = SnapshotService::spawn(SnapshotConfig {
        snapshots_dir: PathBuf::from(&config.data_dir).join("snapshots"),
        max_files: config.snapshot_max_files,
    });
    let wal_handle = WalService::spawn(WalConfig {
        wal_dir: PathBuf::from(&config.data_dir).join("wal"),
        max_file_size: config.wal_max_file_size * 1024 * 1024,
        max_files: config.wal_max_files,
        snapshot_wal_interval: config.snapshot_wal_interval,
        machine_id: config.machine.clone(),
    });
    let client_handle = ClientService::spawn(ClientConfig::from(&config));
    let peer_handle = PeerService::spawn(PeerConfig::from(&config));
    let misc_handle = MiscService::spawn(MiscConfig::from(&config));
    let services = Services {
        client_handle: client_handle.clone(),
        misc_handle: misc_handle.clone(),
        peer_handle: peer_handle.clone(),
        snapshot_handle: snapshot_handle.clone(),
        store_handle: store_handle.clone(),
        wal_handle: wal_handle.clone(),
    };

    // Set services for all services that need dependencies
    wal_handle.set_services(services.clone()).await;
    client_handle.set_services(services.clone()).await;
    peer_handle.set_services(services.clone()).await;
    store_handle.set_services(services.clone()).await;
    snapshot_handle.set_services(services.clone()).await;
    misc_handle.set_services(services.clone()).await;

    // Keep the main task alive
    tokio::signal::ctrl_c().await?;
    tracing::info!("Received shutdown signal, initiating graceful shutdown");

    // Perform graceful shutdown with final snapshot
    if let Err(e) = services.shutdown(config.machine.clone()).await {
        tracing::error!(error = %e, "Error during graceful shutdown");
    }

    tracing::info!("QCore service shutdown complete");
    Ok(())
}
