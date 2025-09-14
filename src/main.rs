mod wal;
mod peers;
mod snapshot;
mod files;
mod core;

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;

use crate::{
    core::{CoreConfig, CoreService},
    peers::{PeerConfig, PeerService},
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
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "qcore_rs=debug,qlib_rs=debug".to_string())
        )
        .with_target(true)
        .with_thread_ids(true)
        .with_file(cfg!(debug_assertions))
        .with_line_number(cfg!(debug_assertions))
        .init();

    // Create service handles (each runs in its own thread)
    let snapshot_handle = SnapshotService::spawn(SnapshotConfig {
        snapshots_dir: PathBuf::from(&config.data_dir).join(&config.machine).join("snapshots"),
        max_files: config.snapshot_max_files,
    });
    
    let wal_handle = WalService::spawn(WalConfig {
        wal_dir: PathBuf::from(&config.data_dir).join(&config.machine).join("wal"),
        max_file_size: config.wal_max_file_size * 1024 * 1024,
        max_files: config.wal_max_files,
        snapshot_wal_interval: config.snapshot_wal_interval,
        machine_id: config.machine.clone(),
    });
    
    let peer_handle = PeerService::spawn(PeerConfig::from(&config));
    
    // Set up dependencies
    wal_handle.set_snapshot_handle(snapshot_handle.clone());
    peer_handle.set_snapshot_handle(snapshot_handle.clone());
    
    // Create the core service (runs in main thread)
    let mut core_service = CoreService::new(CoreConfig::from(&config))?;
    core_service.set_handles(peer_handle, snapshot_handle, wal_handle);
    
    // Set up signal handling for graceful shutdown
    let (shutdown_tx, shutdown_rx) = crossbeam::channel::bounded(1);
    std::thread::spawn(move || {
        if let Err(e) = wait_for_signal() {
            tracing::error!(error = %e, "Error waiting for signal");
        }
        let _ = shutdown_tx.send(());
    });
    
    // Run the core service in the main thread (it uses mio for non-blocking I/O)
    if let Err(e) = core_service.run() {
        tracing::error!(error = %e, "Core service failed");
    }
    
    // Check for shutdown signal (non-blocking)
    if let Ok(_) = shutdown_rx.try_recv() {
        tracing::info!("Received shutdown signal, initiating graceful shutdown");
    }
    
    // TODO: Implement graceful shutdown by taking final snapshot
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
