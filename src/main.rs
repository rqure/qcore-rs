use tokio::signal;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use futures_util::{SinkExt, StreamExt};
use tracing::{info, warn, error, debug};
use clap::Parser;
use anyhow::Result;
use std::sync::Arc;

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

/// Handle a single peer WebSocket connection
async fn handle_inbound_peer_connection(stream: TcpStream, peer_addr: std::net::SocketAddr) -> Result<()> {
    info!("New peer connection from: {}", peer_addr);
    
    let ws_stream = accept_async(stream).await?;
    debug!("WebSocket connection established with peer: {}", peer_addr);
    
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    
    // Send welcome message to peer
    let welcome_msg = Message::Text(format!("{{\"type\":\"welcome\",\"message\":\"Connected to QOS Core Service\",\"peer_id\":\"{}\"}}", uuid::Uuid::new_v4()));
    ws_sender.send(welcome_msg).await?;
    
    // Handle incoming messages from peer
    while let Some(msg) = ws_receiver.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                debug!("Received text from peer {}: {}", peer_addr, text);
                
                // Echo back for now - this would be replaced with actual peer protocol handling
                let response = Message::Text(format!("{{\"type\":\"echo\",\"data\":{}}}", text));
                if let Err(e) = ws_sender.send(response).await {
                    error!("Failed to send response to peer {}: {}", peer_addr, e);
                    break;
                }
            }
            Ok(Message::Binary(data)) => {
                debug!("Received binary data from peer {}: {} bytes", peer_addr, data.len());
                // Handle binary messages - could be used for efficient data transfer
            }
            Ok(Message::Ping(payload)) => {
                debug!("Received ping from peer: {}", peer_addr);
                if let Err(e) = ws_sender.send(Message::Pong(payload)).await {
                    error!("Failed to send pong to peer {}: {}", peer_addr, e);
                    break;
                }
            }
            Ok(Message::Pong(_)) => {
                debug!("Received pong from peer: {}", peer_addr);
            }
            Ok(Message::Close(_)) => {
                info!("Peer {} closed connection", peer_addr);
                break;
            }
            Ok(Message::Frame(_)) => {
                // Handle raw frames if needed - typically not used directly
                debug!("Received raw frame from peer: {}", peer_addr);
            }
            Err(e) => {
                error!("WebSocket error with peer {}: {}", peer_addr, e);
                break;
            }
        }
    }
    
    info!("Peer connection closed: {}", peer_addr);
    Ok(())
}

/// Start the peer WebSocket server task
async fn start_inbound_peer_server(config: Arc<Config>) -> Result<()> {
    let addr = format!("0.0.0.0:{}", config.peer_port);
    let listener = TcpListener::bind(&addr).await?;
    info!("Peer WebSocket server listening on {}", addr);
    
    loop {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                tokio::spawn(async move {
                    if let Err(e) = handle_inbound_peer_connection(stream, peer_addr).await {
                        error!("Error handling peer connection from {}: {}", peer_addr, e);
                    }
                });
            }
            Err(e) => {
                error!("Failed to accept peer connection: {}", e);
                // Continue listening despite individual connection errors
            }
        }
    }
}
#[tokio::main]
async fn main() -> Result<()> {
    let config = Arc::new(Config::parse());

    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "qcore_rs=info,tokio=warn".to_string())
        )
        .with_target(false)
        .init();

    info!(?config, "Starting Core service with configuration");

    // Start the peer WebSocket server task
    let config_clone = Arc::clone(&config);
    let peer_server_task = tokio::spawn(async move {
        if let Err(e) = start_inbound_peer_server(config_clone).await {
            error!("Peer server failed: {}", e);
        }
    });

    // Wait for shutdown signal
    signal::ctrl_c().await?;
    warn!("Received shutdown signal. Stopping Core service...");

    // Abort the peer server task
    peer_server_task.abort();

    Ok(())
}
