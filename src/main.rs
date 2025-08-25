use qlib_rs::{Snowflake, Store, StoreMessage};
use tokio::signal;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, connect_async, tungstenite::Message};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tracing::{info, warn, error, debug};
use clap::Parser;
use anyhow::Result;
use std::sync::Arc;
use std::collections::{HashSet, HashMap};
use tokio::sync::RwLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::fs::{File, OpenOptions, create_dir_all, read_dir, remove_file};
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use std::path::PathBuf;
use serde::{Serialize, Deserialize};

/// Messages exchanged between peers for leader election
#[derive(Serialize, Deserialize, Debug, Clone)]
enum PeerMessage {
    /// Startup message announcing startup time and machine ID
    Startup {
        machine_id: String,
        startup_time: u64, // Timestamp in seconds since UNIX_EPOCH
    },
    /// Request for full synchronization from the leader
    FullSyncRequest {
        machine_id: String,
    },
    /// Response containing a complete snapshot for full synchronization
    FullSyncResponse {
        snapshot: qlib_rs::Snapshot,
    },
    /// Data synchronization request (existing functionality)
    SyncRequest {
        requests: Vec<qlib_rs::Request>,
    },
}

/// Configuration passed via CLI arguments
#[derive(Parser, Clone, Debug)]
#[command(name = "core-service", about = "QOS Core Service runtime datastore")]
struct Config {
    /// Machine ID (unique identifier for this instance)
    #[arg(long)]
    machine: String,

    /// Data directory for storing WAL files and other persistent data
    #[arg(long, default_value = "./data")]
    data_dir: String,

    /// Maximum WAL file size in bytes
    #[arg(long, default_value_t = 1024 * 1024)]
    wal_max_file_size: usize,

    /// Maximum number of WAL files to keep
    #[arg(long, default_value_t = 30)]
    wal_max_files: usize,

    /// Number of WAL file rollovers before taking a snapshot
    #[arg(long, default_value_t = 3)]
    snapshot_wal_interval: u64,

    /// Port for peer-to-peer communication
    #[arg(long, default_value_t = 9000)]
    peer_port: u16,

    /// Port for client communication (StoreProxy clients)
    #[arg(long, default_value_t = 9100)]
    client_port: u16,

    /// List of peer addresses to connect to (format: host:port)
    #[arg(long, value_delimiter = ',')]
    peer_addresses: Vec<String>,

    /// Interval in seconds to retry connecting to peers
    #[arg(long, default_value_t = 30)]
    peer_reconnect_interval_secs: u64,

    /// Delay before starting leader election (useful for testing)
    #[arg(long, default_value_t = 2)]
    leader_election_delay_secs: u64,
}

/// Application state that is shared across all tasks
#[derive(Debug)]
struct AppState {
    /// Configuration
    config: Config,
    
    /// Startup time (timestamp in seconds since UNIX_EPOCH)
    startup_time: u64,
    
    /// Whether this instance has been elected as leader
    is_leader: bool,
    
    /// The machine ID of the current leader (if known)
    current_leader: Option<String>,
    
    /// Whether this instance has completed full sync with the leader
    is_fully_synced: bool,
    
    /// Information about known peers and their startup times
    peer_info: HashMap<String, PeerInfo>,
    
    /// Connected outbound peers with message senders
    connected_outbound_peers: HashMap<String, mpsc::UnboundedSender<Message>>,

    /// Set of currently connected inbound peer addresses
    connected_inbound_peers: HashSet<String>,

    /// Connected clients with message senders
    connected_clients: HashMap<String, mpsc::UnboundedSender<Message>>,

    // Data store
    store: Arc<RwLock<Store>>,
    
    /// Current WAL file handle
    current_wal_file: Option<File>,
    
    /// Current WAL file size in bytes
    current_wal_size: usize,
    
    /// WAL file counter for generating unique filenames
    wal_file_counter: u64,
    
    /// Snapshot file counter for generating unique filenames
    snapshot_file_counter: u64,
    
    /// Number of WAL files created since last snapshot
    wal_files_since_snapshot: u64,
}

/// Information about a peer instance
#[derive(Debug, Clone)]
struct PeerInfo {
    machine_id: String,
    startup_time: u64,
    last_seen: u64,
}

/// Check if this instance should be the leader based on startup times
impl AppState {
    /// Create a new AppState with the given configuration
    fn new(config: Config) -> Self {
        let startup_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        Self {
            config,
            startup_time,
            is_leader: false, // Will be determined through leader election
            current_leader: None,
            is_fully_synced: false,
            peer_info: HashMap::new(),
            connected_outbound_peers: HashMap::new(),
            connected_inbound_peers: HashSet::new(),
            connected_clients: HashMap::new(),
            store: Arc::new(RwLock::new(Store::new(Arc::new(Snowflake::new())))),
            current_wal_file: None,
            current_wal_size: 0,
            wal_file_counter: 0,
            snapshot_file_counter: 0,
            wal_files_since_snapshot: 0,
        }
    }

    /// Get the machine-specific data directory
    fn get_machine_data_dir(&self) -> PathBuf {
        PathBuf::from(&self.config.data_dir).join(&self.config.machine)
    }

    /// Get the machine-specific WAL directory
    fn get_wal_dir(&self) -> PathBuf {
        self.get_machine_data_dir().join("wal")
    }

    /// Get the machine-specific snapshots directory
    fn get_snapshots_dir(&self) -> PathBuf {
        self.get_machine_data_dir().join("snapshots")
    }
}

/// Handle a single peer WebSocket connection
async fn handle_inbound_peer_connection(stream: TcpStream, peer_addr: std::net::SocketAddr, app_state: Arc<RwLock<AppState>>) -> Result<()> {
    let machine = app_state.read().await.config.machine.clone();

    info!("New peer connection from: {}", peer_addr);
    
    let ws_stream = accept_async(stream).await?;
    debug!("WebSocket connection established with peer: {}", peer_addr);
    
    // Add peer to connected inbound peers
    {
        let mut state = app_state.write().await;
        state.connected_inbound_peers.insert(peer_addr.to_string());
    }
    
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    
    // Handle incoming messages from peer
    while let Some(msg) = ws_receiver.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                debug!("Received text from peer {}: {}", peer_addr, text);
                
                // Try to parse as a PeerMessage first
                match serde_json::from_str::<PeerMessage>(&text) {
                    Ok(peer_msg) => {
                        debug!("Received peer message from {}: {:?}", peer_addr, peer_msg);
                        handle_peer_message(peer_msg, &peer_addr, &mut ws_sender, app_state.clone()).await;
                    }
                    Err(_) => {
                        // Try to parse as a Request for synchronization (legacy support)
                        match serde_json::from_str::<qlib_rs::Request>(&text) {
                            Ok(request) => {
                                debug!("Received sync request from peer {}: {:?}", peer_addr, request);
                                
                                // Apply the request to our store if it doesn't already have an originator
                                // (to avoid infinite loops) and ensure the current timestamp is preserved
                                if let Some(originator) = request.originator() {
                                    if *originator != machine {
                                        let mut state = app_state.write().await;
                                        let store = &mut state.store;
                                        let mut store_guard = store.write().await;
                                        
                                        let mut requests = vec![request];
                                        if let Err(e) = store_guard.perform(&mut requests).await {
                                            error!("Failed to apply sync request from peer {}: {}", peer_addr, e);
                                        } else {
                                            debug!("Successfully applied sync request from peer {}", peer_addr);
                                        }
                                    }
                                } else {
                                    debug!("Ignoring request without originator from peer {}", peer_addr);
                                }
                            }
                            Err(_) => {
                                // Not a sync request, treat as regular peer message
                                debug!("Received non-sync message from peer {}", peer_addr);
                                
                                // Echo back for now - this would be replaced with actual peer protocol handling
                                let response = Message::Text(format!("{{\"type\":\"echo\",\"data\":{}}}", text));
                                if let Err(e) = ws_sender.send(response).await {
                                    error!("Failed to send response to peer {}: {}", peer_addr, e);
                                    break;
                                }
                            }
                        }
                    }
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
    
    // Remove peer from connected inbound peers when connection ends
    {
        let mut state = app_state.write().await;
        state.connected_inbound_peers.remove(&peer_addr.to_string());
        // Also remove from peer_info if present
        state.peer_info.retain(|_, info| {
            // Remove peers that haven't been seen recently (this connection ending)
            let current_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            current_time - info.last_seen < 60 // Keep peers seen within last 60 seconds
        });
    }
    
    info!("Peer connection closed: {}", peer_addr);
    Ok(())
}

/// Handle a peer message and respond appropriately
async fn handle_peer_message(
    peer_msg: PeerMessage,
    peer_addr: &std::net::SocketAddr,
    ws_sender: &mut futures_util::stream::SplitSink<tokio_tungstenite::WebSocketStream<TcpStream>, Message>,
    app_state: Arc<RwLock<AppState>>,
) {
    let current_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    match peer_msg {
        PeerMessage::Startup { machine_id, startup_time } => {
            // Update peer information
            {
                let mut state = app_state.write().await;
                state.peer_info.insert(machine_id.clone(), PeerInfo {
                    machine_id: machine_id.clone(),
                    startup_time,
                    last_seen: current_time,
                });
                
                // Determine leadership locally based on startup times
                // If our startup time is the largest (earliest), we are the leader
                let our_startup_time = state.startup_time;
                let our_machine_id = state.config.machine.clone();
                
                // Find the earliest (largest) startup time among all known peers including ourselves
                let earliest_startup = state.peer_info.values()
                    .map(|p| p.startup_time)
                    .min()
                    .unwrap_or(our_startup_time)
                    .min(our_startup_time);
                
                let should_be_leader = our_startup_time <= earliest_startup;
                
                // Handle startup time ties
                if our_startup_time == earliest_startup {
                    let peers_with_same_time: Vec<_> = state.peer_info.values()
                        .filter(|p| p.startup_time == our_startup_time)
                        .collect();
                    
                    if !peers_with_same_time.is_empty() {
                        // We have a tie, use machine_id as tiebreaker
                        info!("Startup time tie detected with {} peers. Using machine_id as tiebreaker.", peers_with_same_time.len());
                        
                        let mut all_machine_ids = peers_with_same_time.iter()
                            .map(|p| p.machine_id.as_str())
                            .collect::<Vec<_>>();
                        all_machine_ids.push(our_machine_id.as_str());
                        
                        let min_machine_id = all_machine_ids.iter().min().unwrap();
                        let should_be_leader_after_tiebreak = **min_machine_id == our_machine_id;
                        
                        // Update leadership status
                        if should_be_leader_after_tiebreak {
                            state.is_leader = true;
                            state.current_leader = Some(our_machine_id.clone());
                            state.is_fully_synced = true;
                            info!("We are the leader after machine_id tiebreaker (startup_time: {}, machine_id: {})", our_startup_time, our_machine_id);
                        } else {
                            state.is_leader = false;
                            // Find the actual leader
                            let leader = state.peer_info.values()
                                .filter(|p| p.startup_time <= earliest_startup)
                                .min_by_key(|p| (&p.startup_time, &p.machine_id))
                                .map(|p| p.machine_id.clone());
                            state.current_leader = leader;
                            info!("We are not the leader after machine_id tiebreaker (startup_time: {}, machine_id: {})", our_startup_time, our_machine_id);
                        }
                        return;
                    }
                }
                
                // Update leadership status
                if should_be_leader {
                    state.is_leader = true;
                    state.current_leader = Some(our_machine_id.clone());
                    state.is_fully_synced = true;
                    info!("We are the leader based on startup time comparison (startup_time: {})", our_startup_time);
                } else {
                    state.is_leader = false;
                    // Find the actual leader
                    let leader = state.peer_info.values()
                        .filter(|p| p.startup_time <= earliest_startup)
                        .min_by_key(|p| (&p.startup_time, &p.machine_id))
                        .map(|p| p.machine_id.clone());
                    state.current_leader = leader;
                    info!("Leader determined: {:?} (our startup_time: {})", state.current_leader, our_startup_time);
                }
            }
            debug!("Updated peer info for {}: startup_time={}", machine_id, startup_time);
            
            // Send our startup info in response
            let (our_machine_id, our_startup_time) = {
                let state = app_state.read().await;
                (state.config.machine.clone(), state.startup_time)
            };
            
            let response = PeerMessage::Startup {
                machine_id: our_machine_id,
                startup_time: our_startup_time,
            };
            
            if let Ok(response_json) = serde_json::to_string(&response) {
                if let Err(e) = ws_sender.send(Message::Text(response_json)).await {
                    error!("Failed to send startup response to peer {}: {}", peer_addr, e);
                }
            }
        }
        
        PeerMessage::FullSyncRequest { machine_id } => {
            // Any peer can respond to sync requests
            info!("Received full sync request from {}, sending snapshot", machine_id);
            
            // Take a snapshot and send it
            let snapshot = {
                let state = app_state.read().await;
                let store = &state.store;
                let store_guard = store.read().await;
                store_guard.take_snapshot()
            };
            
            let response = PeerMessage::FullSyncResponse { snapshot };
            
            if let Ok(response_json) = serde_json::to_string(&response) {
                if let Err(e) = ws_sender.send(Message::Text(response_json)).await {
                    error!("Failed to send full sync response to peer {}: {}", peer_addr, e);
                } else {
                    info!("Sent full sync snapshot to {}", machine_id);
                }
            }
        }
        
        PeerMessage::FullSyncResponse { snapshot } => {
            // Apply the snapshot from the leader
            info!("Received full sync response, applying snapshot");
            
            // Apply the snapshot to the store
            {
                let mut state = app_state.write().await;
                let store = &mut state.store;
                let mut store_guard = store.write().await;
                store_guard.restore_snapshot(snapshot.clone());
                drop(store_guard);
                state.is_fully_synced = true;
            }
            
            // Save the snapshot to disk for persistence
            if let Err(e) = save_snapshot(&snapshot, app_state.clone()).await {
                error!("Failed to save snapshot during full sync: {}", e);
            } else {
                info!("Snapshot saved to disk during full sync");
                
                // Write a snapshot marker to the WAL to indicate the sync point
                // This helps during replay to know that the state was synced at this point
                let snapshot_counter = {
                    let state = app_state.read().await;
                    state.snapshot_file_counter
                };
                
                let snapshot_request = qlib_rs::Request::Snapshot {
                    snapshot_counter,
                    originator: Some(app_state.read().await.config.machine.clone()),
                };
                
                if let Err(e) = write_request_to_wal_direct(&snapshot_request, app_state.clone()).await {
                    error!("Failed to write snapshot marker to WAL: {}", e);
                }
            }
            
            info!("Successfully applied full sync snapshot, instance is now fully synchronized");
        }
        
        PeerMessage::SyncRequest { requests } => {
            // Handle data synchronization (existing functionality)
            let our_machine_id = app_state.read().await.config.machine.clone();
            
            // Check if any request in the batch originated from a different machine
            let should_apply = requests.iter().any(|request| {
                if let Some(originator) = request.originator() {
                    *originator != our_machine_id
                } else {
                    false
                }
            });
            
            if should_apply {
                let mut state = app_state.write().await;
                let store = &mut state.store;
                let mut store_guard = store.write().await;
                
                let mut requests_to_apply = requests;
                if let Err(e) = store_guard.perform(&mut requests_to_apply).await {
                    error!("Failed to apply sync requests from peer {}: {}", peer_addr, e);
                } else {
                    debug!("Successfully applied {} sync requests from peer {}", requests_to_apply.len(), peer_addr);
                }
            } else {
                debug!("Ignoring sync requests without originator from peer {}", peer_addr);
            }
        }
    }
}

/// Handle a single outbound peer WebSocket connection
async fn handle_outbound_peer_connection(peer_addr: &str, app_state: Arc<RwLock<AppState>>) -> Result<()> {
    info!("Attempting to connect to peer: {}", peer_addr);
    
    let ws_url = format!("ws://{}", peer_addr);
    let (ws_stream, _response) = connect_async(&ws_url).await?;
    info!("WebSocket connection established with outbound peer: {}", peer_addr);
    
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    
    // Create a channel for sending messages to this peer
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();
    
    // Store the sender in the connected_outbound_peers HashMap
    {
        let mut state = app_state.write().await;
        state.connected_outbound_peers.insert(peer_addr.to_string(), tx);
    }
    
    // Send initial startup message to announce ourselves
    let machine = app_state.read().await.config.machine.clone();
    let startup_time = app_state.read().await.startup_time;
    let startup = PeerMessage::Startup {
        machine_id: machine.clone(),
        startup_time,
    };
    if let Ok(startup_json) = serde_json::to_string(&startup) {
        if let Err(e) = ws_sender.send(Message::Text(startup_json)).await {
            error!("Failed to send initial startup message to peer {}: {}", peer_addr, e);
        }
    }
    
    // Spawn a task to handle outgoing messages
    let peer_addr_clone = peer_addr.to_string();
    let outgoing_task = tokio::spawn(async move {
        while let Some(message) = rx.recv().await {
            if let Err(e) = ws_sender.send(message).await {
                error!("Failed to send message to peer {}: {}", peer_addr_clone, e);
                break;
            }
        }
    });
    
    // Handle incoming messages from peer (ignore all except connection control)
    while let Some(msg) = ws_receiver.next().await {
        match msg {
            Ok(Message::Text(_text)) => {
                // Ignore all text messages - message handling is done in handle_inbound_peer_connection
                debug!("Ignoring received text message from outbound peer {} (handled via inbound connection)", peer_addr);
            }
            Ok(Message::Binary(_data)) => {
                // Ignore binary data - message handling is done in handle_inbound_peer_connection
                debug!("Ignoring received binary data from outbound peer {} (handled via inbound connection)", peer_addr);
            }
            Ok(Message::Ping(_payload)) => {
                // Respond to pings to keep connection alive
                debug!("Received ping from outbound peer: {}", peer_addr);
                // Note: We can't easily send pong here since ws_sender is in the outgoing task
                // The ping/pong will be handled by the WebSocket implementation
            }
            Ok(Message::Pong(_)) => {
                debug!("Received pong from outbound peer: {}", peer_addr);
            }
            Ok(Message::Close(_)) => {
                info!("Outbound peer {} closed connection", peer_addr);
                break;
            }
            Ok(Message::Frame(_)) => {
                // Handle raw frames if needed - typically not used directly
                debug!("Received raw frame from outbound peer: {}", peer_addr);
            }
            Err(e) => {
                error!("WebSocket error with outbound peer {}: {}", peer_addr, e);
                break;
            }
        }
    }
    
    {
        let mut state = app_state.write().await;
        state.connected_outbound_peers.remove(peer_addr);
        // Also remove from peer_info when connection ends
        state.peer_info.retain(|_, info| {
            let current_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            current_time - info.last_seen < 60 // Keep peers seen within last 60 seconds
        });
    }
    outgoing_task.abort();
    
    info!("Outbound peer connection closed: {}", peer_addr);
    Ok(())
}

/// Handle a single client WebSocket connection that uses StoreProxy protocol
async fn handle_client_connection(stream: TcpStream, client_addr: std::net::SocketAddr, app_state: Arc<RwLock<AppState>>) -> Result<()> {
    info!("New client connection from: {}", client_addr);
    
    let ws_stream = accept_async(stream).await?;
    debug!("WebSocket connection established with client: {}", client_addr);
    
    let (ws_sender, mut ws_receiver) = ws_stream.split();
    
    // Create a channel for sending messages to this client
    let (tx, rx) = mpsc::unbounded_channel::<Message>();
    
    // Store the sender in the connected_clients HashMap
    {
        let mut state = app_state.write().await;
        state.connected_clients.insert(client_addr.to_string(), tx.clone());
    }
    
    // Spawn a task to handle outgoing messages to the client
    let client_addr_clone = client_addr.to_string();
    let app_state_clone = Arc::clone(&app_state);
    let outgoing_task = tokio::spawn(async move {
        let mut ws_sender = ws_sender;
        let mut rx = rx;
        
        while let Some(message) = rx.recv().await {
            if let Err(e) = ws_sender.send(message).await {
                error!("Failed to send message to client {}: {}", client_addr_clone, e);
                break;
            }
        }
        
        // Remove client from connected_clients when outgoing task ends
        let mut state = app_state_clone.write().await;
        state.connected_clients.remove(&client_addr_clone);
    });
    
    // Handle incoming messages from client
    while let Some(msg) = ws_receiver.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                debug!("Received text from client {}: {}", client_addr, text);
                
                // Parse the StoreMessage
                match serde_json::from_str::<StoreMessage>(&text) {
                    Ok(store_msg) => {
                        // Process the message and generate response
                        let response_msg = process_store_message(store_msg, &app_state).await;
                        
                        // Send response back to client using the channel
                        let response_text = match serde_json::to_string(&response_msg) {
                            Ok(text) => text,
                            Err(e) => {
                                error!("Failed to serialize response: {}", e);
                                continue;
                            }
                        };
                        
                        if let Err(e) = tx.send(Message::Text(response_text)) {
                            error!("Failed to send response to client {}: {}", client_addr, e);
                            break;
                        }
                    }
                    Err(e) => {
                        error!("Failed to parse StoreMessage from client {}: {}", client_addr, e);
                        // Send error response
                        let error_msg = StoreMessage::Error {
                            id: uuid::Uuid::new_v4().to_string(),
                            error: format!("Failed to parse message: {}", e),
                        };
                        if let Ok(error_text) = serde_json::to_string(&error_msg) {
                            let _ = tx.send(Message::Text(error_text));
                        }
                    }
                }
            }
            Ok(Message::Binary(_data)) => {
                debug!("Received binary data from client {}", client_addr);
                // For now, we only handle text messages for StoreProxy protocol
            }
            Ok(Message::Ping(payload)) => {
                debug!("Received ping from client: {}", client_addr);
                if let Err(e) = tx.send(Message::Pong(payload)) {
                    error!("Failed to send pong to client {}: {}", client_addr, e);
                    break;
                }
            }
            Ok(Message::Pong(_)) => {
                debug!("Received pong from client: {}", client_addr);
            }
            Ok(Message::Close(_)) => {
                info!("Client {} closed connection", client_addr);
                break;
            }
            Ok(Message::Frame(_)) => {
                debug!("Received raw frame from client: {}", client_addr);
            }
            Err(e) => {
                error!("WebSocket error with client {}: {}", client_addr, e);
                break;
            }
        }
    }
    
    // Remove client from connected_clients when connection ends
    {
        let mut state = app_state.write().await;
        state.connected_clients.remove(&client_addr.to_string());
    }
    
    // Abort the outgoing task
    outgoing_task.abort();
    
    info!("Client connection closed: {}", client_addr);
    Ok(())
}

/// Process a StoreMessage and generate the appropriate response
async fn process_store_message(message: StoreMessage, app_state: &Arc<RwLock<AppState>>) -> StoreMessage {
    let mut state = app_state.write().await;
    let machine = state.config.machine.clone();
    let store = &mut state.store;
    let mut store_guard = store.write().await;

    match message {
        StoreMessage::GetEntitySchema { id, entity_type } => {
            match store_guard.get_entity_schema(&entity_type).await {
                Ok(schema) => StoreMessage::GetEntitySchemaResponse {
                    id,
                    response: Ok(Some(schema)),
                },
                Err(e) => StoreMessage::GetEntitySchemaResponse {
                    id,
                    response: Err(format!("{:?}", e)),
                },
            }
        }
        
        StoreMessage::GetCompleteEntitySchema { id, entity_type } => {
            match store_guard.get_complete_entity_schema(&entity_type).await {
                Ok(schema) => StoreMessage::GetCompleteEntitySchemaResponse {
                    id,
                    response: Ok(schema),
                },
                Err(e) => StoreMessage::GetCompleteEntitySchemaResponse {
                    id,
                    response: Err(format!("{:?}", e)),
                },
            }
        }
        
        StoreMessage::GetFieldSchema { id, entity_type, field_type } => {
            match store_guard.get_field_schema(&entity_type, &field_type).await {
                Ok(schema) => StoreMessage::GetFieldSchemaResponse {
                    id,
                    response: Ok(Some(schema)),
                },
                Err(e) => StoreMessage::GetFieldSchemaResponse {
                    id,
                    response: Err(format!("{:?}", e)),
                },
            }
        }
        
        StoreMessage::EntityExists { id, entity_id } => {
            let exists = store_guard.entity_exists(&entity_id).await;
            StoreMessage::EntityExistsResponse {
                id,
                response: exists,
            }
        }
        
        StoreMessage::FieldExists { id, entity_type, field_type } => {
            let exists = store_guard.field_exists(&entity_type, &field_type).await;
            StoreMessage::FieldExistsResponse {
                id,
                response: exists,
            }
        }
        
        StoreMessage::Perform { id, mut requests } => {
            // Allow write operations on any peer
            requests.iter_mut().for_each(|req| {
                req.try_set_originator(machine.clone());
            });

            match store_guard.perform(&mut requests).await {
                Ok(()) => StoreMessage::PerformResponse {
                    id,
                    response: Ok(requests),
                },
                Err(e) => StoreMessage::PerformResponse {
                    id,
                    response: Err(format!("{:?}", e)),
                },
            }
        }
        
        StoreMessage::FindEntities { id, entity_type, page_opts } => {
            match store_guard.find_entities_paginated(&entity_type, page_opts).await {
                Ok(result) => StoreMessage::FindEntitiesResponse {
                    id,
                    response: Ok(result),
                },
                Err(e) => StoreMessage::FindEntitiesResponse {
                    id,
                    response: Err(format!("{:?}", e)),
                },
            }
        }
        
        StoreMessage::FindEntitiesExact { id, entity_type, page_opts } => {
            match store_guard.find_entities_exact(&entity_type, page_opts).await {
                Ok(result) => StoreMessage::FindEntitiesExactResponse {
                    id,
                    response: Ok(result),
                },
                Err(e) => StoreMessage::FindEntitiesExactResponse {
                    id,
                    response: Err(format!("{:?}", e)),
                },
            }
        }
        
        StoreMessage::GetEntityTypes { id, page_opts } => {
            match store_guard.get_entity_types_paginated(page_opts).await {
                Ok(result) => StoreMessage::GetEntityTypesResponse {
                    id,
                    response: Ok(result),
                },
                Err(e) => StoreMessage::GetEntityTypesResponse {
                    id,
                    response: Err(format!("{:?}", e)),
                },
            }
        }
        
        StoreMessage::RegisterNotification { id, config: _ } => {
            // For now, we'll implement a simple notification registration
            // In a full implementation, you'd want to handle the notification sender properly
            StoreMessage::RegisterNotificationResponse {
                id,
                response: Ok(()),
            }
        }
        
        StoreMessage::UnregisterNotification { id, config: _config } => {
            // For now, we'll implement a simple notification unregistration
            StoreMessage::UnregisterNotificationResponse {
                id,
                response: true,
            }
        }
        
        // These message types should not be received by the server
        StoreMessage::GetEntitySchemaResponse { id, .. } |
        StoreMessage::GetCompleteEntitySchemaResponse { id, .. } |
        StoreMessage::GetFieldSchemaResponse { id, .. } |
        StoreMessage::EntityExistsResponse { id, .. } |
        StoreMessage::FieldExistsResponse { id, .. } |
        StoreMessage::PerformResponse { id, .. } |
        StoreMessage::FindEntitiesResponse { id, .. } |
        StoreMessage::FindEntitiesExactResponse { id, .. } |
        StoreMessage::GetEntityTypesResponse { id, .. } |
        StoreMessage::RegisterNotificationResponse { id, .. } |
        StoreMessage::UnregisterNotificationResponse { id, .. } => {
            StoreMessage::Error {
                id,
                error: "Received response message on server - this should not happen".to_string(),
            }
        }
        
        StoreMessage::Notification { .. } => {
            StoreMessage::Error {
                id: uuid::Uuid::new_v4().to_string(),
                error: "Received notification message on server - this should not happen".to_string(),
            }
        }
        
        StoreMessage::Error { id, error } => {
            warn!("Received error message from client: {} - {}", id, error);
            StoreMessage::Error {
                id: uuid::Uuid::new_v4().to_string(),
                error: "Server received error message from client".to_string(),
            }
        }
    }
}

/// Start the client WebSocket server task
async fn start_client_server(app_state: Arc<RwLock<AppState>>) -> Result<()> {
    let addr = {
        let state = app_state.read().await;
        format!("0.0.0.0:{}", state.config.client_port)
    };
    
    let listener = TcpListener::bind(&addr).await?;
    info!("Client WebSocket server listening on {}", addr);
    
    loop {
        match listener.accept().await {
            Ok((stream, client_addr)) => {
                let app_state_clone = Arc::clone(&app_state);
                tokio::spawn(async move {
                    if let Err(e) = handle_client_connection(stream, client_addr, app_state_clone).await {
                        error!("Error handling client connection from {}: {}", client_addr, e);
                    }
                });
            }
            Err(e) => {
                error!("Failed to accept client connection: {}", e);
                // Continue listening despite individual connection errors
            }
        }
    }
}

/// Start the peer WebSocket server task
async fn start_inbound_peer_server(app_state: Arc<RwLock<AppState>>) -> Result<()> {
    let addr = {
        let state = app_state.read().await;
        format!("0.0.0.0:{}", state.config.peer_port)
    };
    
    let listener = TcpListener::bind(&addr).await?;
    info!("Peer WebSocket server listening on {}", addr);
    
    loop {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                let app_state_clone = Arc::clone(&app_state);
                tokio::spawn(async move {
                    if let Err(e) = handle_inbound_peer_connection(stream, peer_addr, app_state_clone).await {
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

/// Manage outbound peer connections - connects to configured peers and maintains connections
async fn manage_outbound_peer_connections(app_state: Arc<RwLock<AppState>>) -> Result<()> {
    info!("Starting outbound peer connection manager");
    
    let reconnect_interval = {
        let state = app_state.read().await;
        Duration::from_secs(state.config.peer_reconnect_interval_secs)
    };
    
    let mut interval = tokio::time::interval(reconnect_interval);
    
    loop {
        interval.tick().await;
        
        let peers_to_connect = {
            let state = app_state.read().await;
            let connected = &state.connected_outbound_peers;
            state.config.peer_addresses.iter()
                .filter(|addr| !connected.contains_key(*addr))
                .cloned()
                .collect::<Vec<_>>()
        };
        
        for peer_addr in peers_to_connect {
            info!("Attempting to connect to unconnected peer: {}", peer_addr);
            
            let peer_addr_clone = peer_addr.clone();
            let app_state_clone = Arc::clone(&app_state);
            
            tokio::spawn(async move {
                // Attempt connection
                if let Err(e) = handle_outbound_peer_connection(&peer_addr_clone, app_state_clone.clone()).await {
                    error!("Failed to connect to peer {}: {}", peer_addr_clone, e);
                    
                    // Remove from connected set on failure
                    let mut state = app_state_clone.write().await;
                    let connected = &mut state.connected_outbound_peers;
                    connected.remove(&peer_addr_clone);
                } else {
                    info!("Connection to peer {} ended", peer_addr_clone);
                    
                    // Remove from connected set when connection ends
                    let mut state = app_state_clone.write().await;
                    let connected = &mut state.connected_outbound_peers;
                    connected.remove(&peer_addr_clone);
                }
            });
        }
    }
}

/// Consume and process requests from the store's write channel
async fn consume_write_channel(app_state: Arc<RwLock<AppState>>) -> Result<()> {
    info!("Starting write channel consumer");
    
    // Get a clone of the write channel receiver
    let receiver = {
        let state = app_state.read().await;
        let store = &state.store;
        let store_guard = store.read().await;
        store_guard.get_write_channel_receiver()
    };
    
    loop {
        // Wait for a batch of requests from the write channel without holding any store locks
        let requests = {
            let mut receiver_guard = receiver.lock().await;
            receiver_guard.recv().await
        };
        
        match requests {
            Some(requests) => {
                debug!("Writing {} requests to WAL: {:?}", requests.len(), requests);
                
                // Write all requests to WAL file - the requests have already been applied to the store
                for request in &requests {
                    if let Err(e) = write_request_to_wal(request, app_state.clone()).await {
                        error!("Failed to write request to WAL: {}", e);
                    }
                }
                
                // Collect all requests that originated from this machine for batch synchronization
                let current_machine = {
                    let state = app_state.read().await;
                    state.config.machine.clone()
                };
                
                let requests_to_sync: Vec<qlib_rs::Request> = requests.iter()
                    .filter(|request| {
                        if let Some(originator) = request.originator() {
                            originator == &current_machine
                        } else {
                            false
                        }
                    })
                    .cloned()
                    .collect();
                
                // Send batch of requests to peers for synchronization if we have any
                if !requests_to_sync.is_empty() {
                    debug!("Sending {} requests to peers for synchronization", requests_to_sync.len());
                    
                    // Send to all connected outbound peers using PeerMessage
                    let peers_to_notify = {
                        let state = app_state.read().await;
                        state.connected_outbound_peers.clone()
                    };
                    
                    // Create a batch sync message
                    let sync_message = PeerMessage::SyncRequest {
                        requests: requests_to_sync.clone(),
                    };
                    
                    // Serialize the sync message to JSON for transmission
                    match serde_json::to_string(&sync_message) {
                        Ok(message_json) => {
                            let message = Message::Text(message_json);
                            
                            for (peer_addr, sender) in &peers_to_notify {
                                if let Err(e) = sender.send(message.clone()) {
                                    warn!("Failed to send sync requests to peer {}: {}", peer_addr, e);
                                } else {
                                    debug!("Sent {} sync requests to peer: {}", requests_to_sync.len(), peer_addr);
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to serialize sync message for peer synchronization: {}", e);
                        }
                    }
                }
            }
            None => {
                warn!("Write channel closed, stopping consumer");
                return Ok(());
            }
        }
    }
}

/// Clean up old WAL files, keeping only the most recent max_files
async fn cleanup_old_wal_files(wal_dir: &PathBuf, max_files: usize) -> Result<()> {
    let mut entries = read_dir(wal_dir).await?;
    let mut wal_files = Vec::new();
    
    // Collect all WAL files
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if let Some(filename) = path.file_name() {
            if let Some(filename_str) = filename.to_str() {
                if filename_str.starts_with("wal_") && filename_str.ends_with(".log") {
                    wal_files.push(path);
                }
            }
        }
    }
    
    // Sort files by name (which corresponds to creation order due to counter)
    wal_files.sort();
    
    // Remove old files if we have more than max_files
    if wal_files.len() > max_files {
        let files_to_remove = wal_files.len() - max_files;
        for i in 0..files_to_remove {
            info!("Removing old WAL file: {}", wal_files[i].display());
            if let Err(e) = remove_file(&wal_files[i]).await {
                error!("Failed to remove old WAL file {}: {}", wal_files[i].display(), e);
            }
        }
    }
    
    Ok(())
}

/// Write a request directly to the WAL file without snapshot logic (to avoid recursion)
async fn write_request_to_wal_direct(request: &qlib_rs::Request, app_state: Arc<RwLock<AppState>>) -> Result<()> {
    let mut state = app_state.write().await;
    
    // Serialize the request to JSON
    let serialized = serde_json::to_vec(request)?;
    let serialized_len = serialized.len();
    
    // Ensure we have a WAL file open
    if state.current_wal_file.is_none() {
        // Create WAL directory if it doesn't exist
        let wal_dir = state.get_wal_dir();
        create_dir_all(&wal_dir).await?;
        
        // Create new WAL file in the wal directory
        let wal_filename = format!("wal_{:010}.log", state.wal_file_counter);
        let wal_path = wal_dir.join(&wal_filename);
        
        info!("Creating new WAL file for direct write: {}", wal_path.display());
        
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)
            .await?;
            
        state.current_wal_file = Some(file);
        state.current_wal_size = 0;
        state.wal_file_counter += 1;
    }
    
    // Write to WAL file
    if let Some(ref mut wal_file) = state.current_wal_file {
        // Write length prefix (4 bytes) followed by the serialized data
        let len_bytes = (serialized_len as u32).to_le_bytes();
        wal_file.write_all(&len_bytes).await?;
        wal_file.write_all(&serialized).await?;
        wal_file.flush().await?;
        
        state.current_wal_size += 4 + serialized_len;
        
        debug!("Wrote {} bytes to WAL file (direct)", serialized_len + 4);
    }
    
    Ok(())
}

/// Write a request to the WAL file
async fn write_request_to_wal(request: &qlib_rs::Request, app_state: Arc<RwLock<AppState>>) -> Result<()> {
    let mut state = app_state.write().await;
    
    // Serialize the request to JSON
    let serialized = serde_json::to_vec(request)?;
    let serialized_len = serialized.len();
    
    // Check if we need to create a new WAL file
    let should_create_new_file = state.current_wal_file.is_none() || 
       state.current_wal_size + serialized_len > state.config.wal_max_file_size;
    
    if should_create_new_file {
        // Create WAL directory if it doesn't exist
        let wal_dir = state.get_wal_dir();
        create_dir_all(&wal_dir).await?;
        
        // Create new WAL file in the wal directory
        let wal_filename = format!("wal_{:010}.log", state.wal_file_counter);
        let wal_path = wal_dir.join(&wal_filename);
        
        info!("Creating new WAL file: {}", wal_path.display());
        
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)
            .await?;
            
        state.current_wal_file = Some(file);
        state.current_wal_size = 0;
        state.wal_file_counter += 1;
        state.wal_files_since_snapshot += 1;
        
        // Check if we should take a snapshot based on WAL rollovers
        let should_snapshot = state.wal_files_since_snapshot >= state.config.snapshot_wal_interval;
        
        if should_snapshot {
            info!("Taking snapshot after {} WAL file rollovers", state.wal_files_since_snapshot);
            
            // Take a snapshot
            let snapshot = {
                let store = &state.store;
                let store_guard = store.read().await;
                store_guard.take_snapshot()
            };
            
            drop(state); // Release the lock before calling save_snapshot
            
            // Save the snapshot to disk
            if let Err(e) = save_snapshot(&snapshot, app_state.clone()).await {
                error!("Failed to save snapshot after WAL rollover: {}", e);
            } else {
                // Reset the WAL files counter
                let mut state = app_state.write().await;
                state.wal_files_since_snapshot = 0;
                info!("Snapshot saved successfully after WAL rollover");
                
                // Write a snapshot marker to the WAL to indicate the snapshot point
                let snapshot_counter = state.snapshot_file_counter;
                let machine_id = state.config.machine.clone();
                drop(state); // Release the lock before writing to WAL
                
                let snapshot_request = qlib_rs::Request::Snapshot {
                    snapshot_counter,
                    originator: Some(machine_id),
                };
                
                if let Err(e) = write_request_to_wal_direct(&snapshot_request, app_state.clone()).await {
                    error!("Failed to write snapshot marker to WAL: {}", e);
                }
            }
            
            // Re-acquire the state lock for the rest of the function
            state = app_state.write().await;
        }
        
        // Clean up old WAL files if we exceed the maximum
        let max_files = state.config.wal_max_files;
        if let Err(e) = cleanup_old_wal_files(&wal_dir, max_files).await {
            error!("Failed to clean up old WAL files: {}", e);
        }
    }
    
    // Write to WAL file
    if let Some(ref mut wal_file) = state.current_wal_file {
        // Write length prefix (4 bytes) followed by the serialized data
        let len_bytes = (serialized_len as u32).to_le_bytes();
        wal_file.write_all(&len_bytes).await?;
        wal_file.write_all(&serialized).await?;
        wal_file.flush().await?;
        
        state.current_wal_size += 4 + serialized_len;
        
        debug!("Wrote {} bytes to WAL file", serialized_len + 4);
    }
    
    Ok(())
}

/// Save a snapshot to disk
async fn save_snapshot(snapshot: &qlib_rs::Snapshot, app_state: Arc<RwLock<AppState>>) -> Result<()> {
    let mut state = app_state.write().await;
    
    // Create snapshots directory if it doesn't exist
    let snapshot_dir = state.get_snapshots_dir();
    create_dir_all(&snapshot_dir).await?;
    
    // Create snapshot filename with counter
    let snapshot_filename = format!("snapshot_{:010}.bin", state.snapshot_file_counter);
    let snapshot_path = snapshot_dir.join(&snapshot_filename);
    
    info!("Saving snapshot to: {}", snapshot_path.display());
    
    // Serialize the snapshot using bincode for efficiency
    let serialized = bincode::serialize(snapshot)?;
    
    // Write to file
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&snapshot_path)
        .await?;
    
    file.write_all(&serialized).await?;
    file.flush().await?;
    
    state.snapshot_file_counter += 1;
    
    info!("Snapshot saved successfully, {} bytes", serialized.len());
    
    // Clean up old snapshots (keep only the most recent 3)
    if let Err(e) = cleanup_old_snapshots(&snapshot_dir, 3).await {
        error!("Failed to clean up old snapshots: {}", e);
    }
    
    Ok(())
}

/// Load the latest snapshot from disk and return it along with the snapshot counter
async fn load_latest_snapshot(app_state: Arc<RwLock<AppState>>) -> Result<Option<(qlib_rs::Snapshot, u64)>> {
    let state = app_state.read().await;
    let snapshot_dir = state.get_snapshots_dir();
    
    if !snapshot_dir.exists() {
        info!("No snapshots directory found, starting with empty store");
        return Ok(None);
    }
    
    // Find the latest snapshot file
    let mut entries = read_dir(&snapshot_dir).await?;
    let mut snapshot_files = Vec::new();
    
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if let Some(filename) = path.file_name() {
            if let Some(filename_str) = filename.to_str() {
                if filename_str.starts_with("snapshot_") && filename_str.ends_with(".bin") {
                    // Extract the counter from the filename
                    if let Some(counter_str) = filename_str.strip_prefix("snapshot_").and_then(|s| s.strip_suffix(".bin")) {
                        if let Ok(counter) = counter_str.parse::<u64>() {
                            snapshot_files.push((path, counter));
                        }
                    }
                }
            }
        }
    }
    
    if snapshot_files.is_empty() {
        info!("No snapshot files found, starting with empty store");
        return Ok(None);
    }
    
    // Sort files by counter (which corresponds to creation order)
    snapshot_files.sort_by_key(|(_, counter)| *counter);
    
    // Load the latest snapshot
    let (latest_snapshot_path, latest_counter) = snapshot_files.last().unwrap();
    info!("Loading snapshot from: {} (counter: {})", latest_snapshot_path.display(), latest_counter);
    
    drop(state); // Release the lock before async operations
    
    let mut file = File::open(latest_snapshot_path).await?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).await?;
    
    match bincode::deserialize(&buffer) {
        Ok(snapshot) => {
            info!("Snapshot loaded successfully");
            Ok(Some((snapshot, *latest_counter)))
        }
        Err(e) => {
            error!("Failed to deserialize snapshot: {}", e);
            Ok(None)
        }
    }
}

/// Clean up old snapshot files, keeping only the most recent max_files
async fn cleanup_old_snapshots(snapshot_dir: &PathBuf, max_files: usize) -> Result<()> {
    let mut entries = read_dir(snapshot_dir).await?;
    let mut snapshot_files = Vec::new();
    
    // Collect all snapshot files
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if let Some(filename) = path.file_name() {
            if let Some(filename_str) = filename.to_str() {
                if filename_str.starts_with("snapshot_") && filename_str.ends_with(".bin") {
                    snapshot_files.push(path);
                }
            }
        }
    }
    
    // Sort files by name (which corresponds to creation order due to counter)
    snapshot_files.sort();
    
    // Remove old files if we have more than max_files
    if snapshot_files.len() > max_files {
        let files_to_remove = snapshot_files.len() - max_files;
        for i in 0..files_to_remove {
            info!("Removing old snapshot file: {}", snapshot_files[i].display());
            if let Err(e) = remove_file(&snapshot_files[i]).await {
                error!("Failed to remove old snapshot file {}: {}", snapshot_files[i].display(), e);
            }
        }
    }
    
    Ok(())
}

/// Replay WAL files from a specific point to restore store state
/// If start_from_wal_counter is provided, only replay WAL files with counter >= start_from_wal_counter
async fn replay_wal_files(app_state: Arc<RwLock<AppState>>, start_from_wal_counter: Option<u64>) -> Result<()> {
    let state = app_state.read().await;
    let wal_dir = state.get_wal_dir();
    
    if !wal_dir.exists() {
        info!("No WAL directory found, no replay needed");
        return Ok(());
    }
    
    // Find all WAL files
    let mut entries = read_dir(&wal_dir).await?;
    let mut wal_files = Vec::new();
    
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if let Some(filename) = path.file_name() {
            if let Some(filename_str) = filename.to_str() {
                if filename_str.starts_with("wal_") && filename_str.ends_with(".log") {
                    // Extract the counter from the filename
                    if let Some(counter_str) = filename_str.strip_prefix("wal_").and_then(|s| s.strip_suffix(".log")) {
                        if let Ok(counter) = counter_str.parse::<u64>() {
                            // Only include files that are >= start_from_wal_counter (if specified)
                            if let Some(start_counter) = start_from_wal_counter {
                                if counter >= start_counter {
                                    wal_files.push((path, counter));
                                }
                            } else {
                                wal_files.push((path, counter));
                            }
                        }
                    }
                }
            }
        }
    }
    
    if wal_files.is_empty() {
        if let Some(start_counter) = start_from_wal_counter {
            info!("No WAL files found starting from counter {}, no replay needed", start_counter);
        } else {
            info!("No WAL files found, no replay needed");
        }
        return Ok(());
    }
    
    // Sort files by counter (which corresponds to creation order)
    wal_files.sort_by_key(|(_, counter)| *counter);
    
    drop(state); // Release the lock before processing
    
    if let Some(start_counter) = start_from_wal_counter {
        info!("Replaying {} WAL files starting from counter {}", wal_files.len(), start_counter);
    } else {
        info!("Replaying {} WAL files", wal_files.len());
    }
    
    for (wal_file, counter) in &wal_files {
        info!("Replaying WAL file: {} (counter: {})", wal_file.display(), counter);
        
        if let Err(e) = replay_single_wal_file(wal_file, app_state.clone()).await {
            error!("Failed to replay WAL file {}: {}", wal_file.display(), e);
            // Continue with other files instead of failing completely
        }
    }
    
    info!("WAL replay completed");
    Ok(())
}

/// Replay a single WAL file
async fn replay_single_wal_file(wal_path: &PathBuf, app_state: Arc<RwLock<AppState>>) -> Result<()> {
    let mut file = File::open(wal_path).await?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).await?;
    
    let mut requests_processed = 0;
    let mut last_snapshot_offset = None;
    let mut snapshot_found = false;
    
    // First pass: find the most recent snapshot marker in this WAL file
    let mut temp_offset = 0;
    while temp_offset < buffer.len() {
        // Read length prefix (4 bytes)
        if temp_offset + 4 > buffer.len() {
            break;
        }
        
        let len_bytes = [buffer[temp_offset], buffer[temp_offset+1], buffer[temp_offset+2], buffer[temp_offset+3]];
        let len = u32::from_le_bytes(len_bytes) as usize;
        temp_offset += 4;
        
        // Read the serialized request
        if temp_offset + len > buffer.len() {
            break;
        }
        
        let request_data = &buffer[temp_offset..temp_offset + len];
        
        // Check if this is a snapshot marker
        if let Ok(request) = serde_json::from_slice::<qlib_rs::Request>(request_data) {
            if matches!(request, qlib_rs::Request::Snapshot { .. }) {
                last_snapshot_offset = Some(temp_offset + len);
                snapshot_found = true;
                debug!("Found snapshot marker at offset {} in {}", temp_offset, wal_path.display());
            }
        }
        
        temp_offset += len;
    }
    
    // Start replay from after the most recent snapshot marker (if any)
    let mut offset = if let Some(start_offset) = last_snapshot_offset {
        info!("Starting replay from offset {} (after snapshot marker) in {}", start_offset, wal_path.display());
        start_offset
    } else if snapshot_found {
        // If we found a snapshot but couldn't determine offset, start from beginning
        info!("Snapshot found but offset unclear, replaying entire file: {}", wal_path.display());
        0
    } else {
        // No snapshot found, replay entire file
        0
    };
    
    // Second pass: replay requests from the determined starting point
    while offset < buffer.len() {
        // Read length prefix (4 bytes)
        if offset + 4 > buffer.len() {
            break; // Not enough data for length prefix
        }
        
        let len_bytes = [buffer[offset], buffer[offset+1], buffer[offset+2], buffer[offset+3]];
        let len = u32::from_le_bytes(len_bytes) as usize;
        offset += 4;
        
        // Read the serialized request
        if offset + len > buffer.len() {
            error!("Incomplete request in WAL file at offset {}", offset);
            break;
        }
        
        let request_data = &buffer[offset..offset + len];
        offset += len;
        
        // Deserialize and apply the request
        match serde_json::from_slice::<qlib_rs::Request>(request_data) {
            Ok(request) => {
                // Skip snapshot markers during replay since they're just markers
                if matches!(request, qlib_rs::Request::Snapshot { .. }) {
                    debug!("Skipping snapshot marker during replay");
                    continue;
                }
                
                // Apply the request to the store
                let mut state = app_state.write().await;
                let store = &mut state.store;
                let mut store_guard = store.write().await;
                
                let mut requests = vec![request];
                if let Err(e) = store_guard.perform(&mut requests).await {
                    error!("Failed to apply request during WAL replay: {}", e);
                } else {
                    requests_processed += 1;
                }
                
                drop(store_guard);
                drop(state);
            }
            Err(e) => {
                error!("Failed to deserialize request from WAL: {}", e);
            }
        }
    }
    
    if snapshot_found {
        info!("Replayed {} requests from {} (started after snapshot marker)", requests_processed, wal_path.display());
    } else {
        info!("Replayed {} requests from {} (no snapshot marker found)", requests_processed, wal_path.display());
    }
    Ok(())
}

/// Determine the next WAL file counter based on existing WAL files
async fn get_next_wal_counter(app_state: Arc<RwLock<AppState>>) -> Result<u64> {
    let state = app_state.read().await;
    let wal_dir = state.get_wal_dir();
    
    if !wal_dir.exists() {
        return Ok(0);
    }
    
    let mut entries = read_dir(&wal_dir).await?;
    let mut max_counter = 0;
    
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if let Some(filename) = path.file_name() {
            if let Some(filename_str) = filename.to_str() {
                if filename_str.starts_with("wal_") && filename_str.ends_with(".log") {
                    if let Some(counter_str) = filename_str.strip_prefix("wal_").and_then(|s| s.strip_suffix(".log")) {
                        if let Ok(counter) = counter_str.parse::<u64>() {
                            max_counter = max_counter.max(counter);
                        }
                    }
                }
            }
        }
    }
    
    Ok(max_counter + 1)
}

/// Determine the next snapshot file counter based on existing snapshot files
async fn get_next_snapshot_counter(app_state: Arc<RwLock<AppState>>) -> Result<u64> {
    let state = app_state.read().await;
    let snapshot_dir = state.get_snapshots_dir();
    
    if !snapshot_dir.exists() {
        return Ok(0);
    }
    
    let mut entries = read_dir(&snapshot_dir).await?;
    let mut max_counter = 0;
    
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if let Some(filename) = path.file_name() {
            if let Some(filename_str) = filename.to_str() {
                if filename_str.starts_with("snapshot_") && filename_str.ends_with(".bin") {
                    if let Some(counter_str) = filename_str.strip_prefix("snapshot_").and_then(|s| s.strip_suffix(".bin")) {
                        if let Ok(counter) = counter_str.parse::<u64>() {
                            max_counter = max_counter.max(counter);
                        }
                    }
                }
            }
        }
    }
    
    Ok(max_counter + 1)
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "qcore_rs=info,tokio=warn".to_string())
        )
        .with_target(false)
        .init();

    info!(?config, "Starting Core service with configuration");

    // Create shared application state
    let app_state = Arc::new(RwLock::new(AppState::new(config)));

    // Initialize the WAL file counter based on existing files
    {
        let next_wal_counter = get_next_wal_counter(app_state.clone()).await?;
        let mut state = app_state.write().await;
        state.wal_file_counter = next_wal_counter;
        info!("Initialized WAL file counter to {}", next_wal_counter);
    }

    // Load the latest snapshot if available
    let snapshot_wal_counter = if let Some((snapshot, snapshot_counter)) = load_latest_snapshot(app_state.clone()).await? {
        info!("Restoring store from snapshot (counter: {})", snapshot_counter);
        
        // Initialize the snapshot file counter to continue from the next number
        let next_snapshot_counter = get_next_snapshot_counter(app_state.clone()).await?;
        
        let mut state = app_state.write().await;
        let store = &mut state.store;
        let mut store_guard = store.write().await;
        store_guard.restore_snapshot(snapshot);
        drop(store_guard);
        
        state.snapshot_file_counter = next_snapshot_counter;
        drop(state);
        
        // Calculate which WAL files to replay
        // We need to replay WAL files that were created after this snapshot
        // Since snapshots are taken every N WAL rollovers, we need to calculate the WAL counter
        // based on the snapshot counter
        let config = {
            let state = app_state.read().await;
            state.config.clone()
        };
        
        // The WAL counter that corresponds to this snapshot
        // Since we take a snapshot every N WAL rollovers, the formula is:
        // wal_counter = snapshot_counter * snapshot_wal_interval
        Some(snapshot_counter * config.snapshot_wal_interval)
    } else {
        info!("No snapshot found, starting with empty store");
        
        // Initialize the snapshot file counter
        let next_snapshot_counter = get_next_snapshot_counter(app_state.clone()).await?;
        let mut state = app_state.write().await;
        state.snapshot_file_counter = next_snapshot_counter;
        
        None
    };

    // Replay WAL files to bring the store up to date
    info!("Replaying WAL files");
    if let Err(e) = replay_wal_files(app_state.clone(), snapshot_wal_counter).await {
        error!("Failed to replay WAL files: {}", e);
        return Err(e);
    }

    // Start the write channel consumer task
    let app_state_clone = Arc::clone(&app_state);
    let write_channel_task = tokio::spawn(async move {
        if let Err(e) = consume_write_channel(app_state_clone).await {
            error!("Write channel consumer failed: {}", e);
        }
    });

    // Start the peer WebSocket server task
    let app_state_clone = Arc::clone(&app_state);
    let peer_server_task = tokio::spawn(async move {
        if let Err(e) = start_inbound_peer_server(app_state_clone).await {
            error!("Peer server failed: {}", e);
        }
    });

    // Start the client WebSocket server task
    let app_state_clone = Arc::clone(&app_state);
    let client_server_task = tokio::spawn(async move {
        if let Err(e) = start_client_server(app_state_clone).await {
            error!("Client server failed: {}", e);
        }
    });

    // Start the outbound peer connection manager task
    let app_state_clone = Arc::clone(&app_state);
    let outbound_peer_task = tokio::spawn(async move {
        if let Err(e) = manage_outbound_peer_connections(app_state_clone).await {
            error!("Outbound peer connection manager failed: {}", e);
        }
    });

    // Wait for shutdown signal
    signal::ctrl_c().await?;
    warn!("Received shutdown signal. Stopping Core service...");

    // Take a final snapshot before shutting down
    info!("Taking final snapshot before shutdown");
    let snapshot = {
        let state = app_state.read().await;
        let store = &state.store;
        let store_guard = store.read().await;
        store_guard.take_snapshot()
    };
    
    if let Err(e) = save_snapshot(&snapshot, app_state.clone()).await {
        error!("Failed to save final snapshot: {}", e);
    } else {
        info!("Final snapshot saved successfully");
    }

    // Abort all tasks
    write_channel_task.abort();
    peer_server_task.abort();
    client_server_task.abort();
    outbound_peer_task.abort();

    Ok(())
}
