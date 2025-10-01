mod wal;
mod snapshot;
mod files;
mod core;
mod store_service;
mod heartbeat;
mod fault_tolerance;

use std::path::PathBuf;
use std::collections::HashMap;

use anyhow::Result;
use clap::Parser;
use tracing::error;

use crate::{
    core::{CoreConfig, CoreService},
    snapshot::{SnapshotConfig, SnapshotService},
    store_service::StoreService,
    wal::{WalConfig, WalService},
    heartbeat::{HeartbeatConfig, HeartbeatService},
    fault_tolerance::{FaultToleranceConfig, FaultToleranceService},
};

/// Peer configuration mapping machine ID to address
#[derive(Clone, Debug)]
pub struct PeerMapping {
    pub peers: HashMap<String, String>,
}

impl std::str::FromStr for PeerMapping {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let mut peers = HashMap::new();
        
        if s.trim().is_empty() {
            return Ok(PeerMapping { peers });
        }
        
        for pair in s.split(',') {
            let pair = pair.trim();
            if pair.is_empty() {
                continue;
            }
            
            let parts: Vec<&str> = pair.split('=').collect();
            if parts.len() != 2 {
                return Err(anyhow::anyhow!(
                    "Invalid peer format '{}'. Expected format: machine_id=host:port", 
                    pair
                ));
            }
            
            let machine_id = parts[0].trim().to_string();
            let address = parts[1].trim().to_string();
            
            if machine_id.is_empty() || address.is_empty() {
                return Err(anyhow::anyhow!(
                    "Machine ID and address cannot be empty in '{}'", 
                    pair
                ));
            }
            
            peers.insert(machine_id, address);
        }
        
        Ok(PeerMapping { peers })
    }
}

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

    /// Port for unified client and peer communication
    #[arg(long, default_value_t = 9100)]
    pub port: u16,

    /// List of peer addresses to connect to (format: machine_id=host:port)
    /// Example: qos-b=localhost:9101,qos-c=localhost:9102
    #[arg(long)]
    pub peer_addresses: Option<PeerMapping>,
}

fn main() -> Result<()> {
    let config = Config::parse();

    // Initialize tracing with better structured logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("qcore_rs=info".parse().unwrap())
                .add_directive("info".parse().unwrap())
        )
        .init();

    // Create service handles (each runs in its own thread)
    let store_handle = StoreService::spawn();
    
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

    let _heartbeat_handle = HeartbeatService::spawn(
        HeartbeatConfig {
            machine: config.machine.clone(),
            interval_secs: 1,
        },
        store_handle.clone(),
    );

    let fault_tolerance_handle = FaultToleranceService::spawn(
        FaultToleranceConfig {
            machine: config.machine.clone(),
            is_leader: true, // Will be updated by CoreService based on peer evaluation
            interval_millis: 100,
        },
        store_handle.clone(),
    );

    let core_handle = CoreService::spawn(CoreConfig::from(&config), store_handle.clone());
    
    // Wire up service handles
    wal_handle.set_store_handle(store_handle.clone());
    snapshot_handle.set_store_handle(store_handle.clone());
    
    core_handle.set_snapshot_handle(snapshot_handle.clone());
    core_handle.set_wal_handle(wal_handle.clone());
    core_handle.set_fault_tolerance_handle(fault_tolerance_handle.clone());

    // Load snapshot and replay WAL
    snapshot_handle.load_latest();
    wal_handle.replay();

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
