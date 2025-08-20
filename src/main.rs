use tokio::signal;
use tracing::{info, warn};
use clap::Parser;
use anyhow::Result;

/// Configuration passed via CLI arguments
#[derive(Parser, Clone, Debug)]
#[command(name = "core-service", about = "QOS Core Service runtime datastore")]
struct Config {
    /// Maximum WAL file size in bytes
    #[arg(long, default_value_t = 1024 * 1024)]
    wal_max_file_size: usize,

    /// Snapshot interval in seconds
    #[arg(long, default_value_t = 30)]
    snapshot_interval_secs: u64,

    /// Port for peer-to-peer communication
    #[arg(long, default_value_t = 9000)]
    peer_port: u16,

    /// Port for client communication
    #[arg(long, default_value_t = 9100)]
    client_port: u16,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::parse();

    tracing_subscriber::fmt()
        .with_env_filter("core_service=debug,tokio=info")
        .with_target(false)
        .init();

    info!(?config, "Starting Core service with configuration");

    signal::ctrl_c().await?;
    warn!("Received shutdown signal. Stopping Core service...");

    Ok(())
}
