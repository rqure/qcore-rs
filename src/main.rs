mod wal;
mod peer_manager;
mod snapshot;
mod files;
mod core;

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use tracing::error;

use crate::{
    core::{CoreConfig, CoreService},
    peer_manager::PeerConfig,
    snapshot::{SnapshotConfig, SnapshotService},
    wal::{WalConfig, WalService}
};

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

fn main() -> Result<()> {
    let config = Config::parse();

    // Initialize tracing with better structured logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("qcore_rs=debug".parse().unwrap())
                .add_directive("info".parse().unwrap())
        )
        .init();

    // Create service handles (each runs in its own thread)
    let snapshot_handle = SnapshotService::spawn(SnapshotConfig {
        snapshots_dir: PathBuf::from(&config.data_dir).join(&config.machine).join("snapshots"),
        max_files: config.snapshot_max_files,
    });
    
    let wal_handle = WalService::spawn(WalConfig {
        wal_dir: PathBuf::from(&config.data_dir).join(&config.machine).join("wal"),
        max_file_size: config.wal_max_file_size * 1024 * 1024, // Convert MB to bytes
        max_files: config.wal_max_files,
        snapshot_wal_interval: config.snapshot_wal_interval,
    });
    
    // Create the core service (runs in its own thread) with peer configuration
    let core_handle = CoreService::spawn(CoreConfig::from(&config), PeerConfig::from(&config))?;
    
    // Set up dependencies
    wal_handle.set_snapshot_handle(snapshot_handle.clone());
    wal_handle.set_core_handle(core_handle.clone());
    
    // Set handles for the core service
    core_handle.set_handles(snapshot_handle.clone(), wal_handle)?;
    
    // Initialize store from snapshots and WAL replay
    core_handle.initialize_store()?;
    
    // Set up signal handling for graceful shutdown
    let (shutdown_tx, shutdown_rx) = crossbeam::channel::bounded(1);
    std::thread::spawn(move || {
        if let Err(e) = wait_for_signal() {
            error!("Error waiting for signal: {}", e);
        }
        let _ = shutdown_tx.send(());
    });
    
    // Keep the main thread alive and wait for shutdown signal
    if let Ok(_) = shutdown_rx.recv() {
        tracing::info!("Received shutdown signal, initiating graceful shutdown");
        
        // Trigger a final snapshot before shutdown
        if let Ok(snapshot) = core_handle.take_snapshot() {
            if let Err(e) = snapshot_handle.save(snapshot) {
                error!("Failed to save final snapshot: {}", e);
            }
        }
    }
    
    tracing::info!("QCore service shutdown complete");
    Ok(())
}

fn wait_for_signal() -> Result<()> {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    
    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    })?;
    
    while running.load(Ordering::SeqCst) {
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    
    Ok(())
}
