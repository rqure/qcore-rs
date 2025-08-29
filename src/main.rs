use qlib_rs::{et, ft, notification_channel, AuthConfig, AuthenticationResult, Cache, NotificationSender, NotifyConfig, Snowflake, AsyncStore, StoreTrait, StoreMessage, CelExecutor};
use qlib_rs::auth::{authenticate_subject, AuthorizationScope, get_scope};
use tokio::signal;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, connect_async, tungstenite::Message};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc};
use tracing::{info, warn, error, debug};
use clap::Parser;
use anyhow::Result;
use std::collections::{HashSet, HashMap};
use std::sync::Arc;
use tokio::sync::RwLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::fs::{File, OpenOptions, create_dir_all, read_dir, remove_file};
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use std::path::PathBuf;
use serde::{Serialize, Deserialize};

/// Application availability state
#[derive(Debug, Clone, PartialEq)]
enum AvailabilityState {
    /// Application is unavailable - attempting to sync with leader, clients are force disconnected
    Unavailable,
    /// Application is available - clients are allowed to connect and perform operations
    Available,
}

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

    /// Maximum number of snapshot files to keep
    #[arg(long, default_value_t = 5)]
    snapshot_max_files: usize,

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

    /// Grace period in seconds to wait after becoming unavailable before requesting full sync
    #[arg(long, default_value_t = 5)]
    full_sync_grace_period_secs: u64,
}

/// Application state that is shared across all tasks
#[derive(Debug)]
struct AppState {
    /// Configuration
    config: Config,
    
    /// Startup time (timestamp in seconds since UNIX_EPOCH)
    startup_time: u64,
    
    /// Current availability state of the application
    availability_state: AvailabilityState,
    
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

    /// Client notification senders - maps client address to their notification sender
    /// Used to send notifications to specific clients
    client_notification_senders: HashMap<String, NotificationSender>,

    /// Track notification configurations per client for cleanup on disconnect
    /// Maps client address to a set of registered notification configurations
    client_notification_configs: HashMap<String, HashSet<NotifyConfig>>,

    /// Track authenticated clients - maps client address to authenticated subject ID
    authenticated_clients: HashMap<String, qlib_rs::EntityId>,

    // Data store
    store: Arc<RwLock<AsyncStore>>,
    
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

    /// Timestamp when we became unavailable (for grace period tracking)
    became_unavailable_at: Option<u64>,

    /// Whether a full sync request is pending (to avoid sending multiple)
    full_sync_request_pending: bool,

    /// Permission Cache
    permission_cache: Cache<AsyncStore>,

    /// CEL Executor for evaluating authorization conditions
    cel_executor: CelExecutor,
}

/// Information about a peer instance
#[derive(Debug, Clone)]
struct PeerInfo {
    machine_id: String,
    startup_time: u64,
    last_seen: u64,
}

impl AppState {
    async fn new(config: Config) -> Result<Self> {
        let startup_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let store = Arc::new(RwLock::new(AsyncStore::new(Arc::new(Snowflake::new()))));
        
        Ok(Self {
            config,
            startup_time,
            availability_state: AvailabilityState::Available,
            is_leader: false,
            current_leader: None,
            is_fully_synced: false,
            peer_info: HashMap::new(),
            connected_outbound_peers: HashMap::new(),
            connected_inbound_peers: HashSet::new(),
            connected_clients: HashMap::new(),
            client_notification_senders: HashMap::new(),
            client_notification_configs: HashMap::new(),
            authenticated_clients: HashMap::new(),
            store: store.clone(),
            current_wal_file: None,
            current_wal_size: 0,
            wal_file_counter: 0,
            snapshot_file_counter: 0,
            wal_files_since_snapshot: 0,
            became_unavailable_at: None,
            full_sync_request_pending: false,
            permission_cache: Cache::new(
                store.clone(),
                et::permission(),
                vec![ft::resource_type(), ft::resource_field()],
                vec![ft::scope(), ft::condition()]
            ).await?,
            cel_executor: CelExecutor::new(),
        })
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

    /// Set the availability state and handle state transitions
    fn set_availability_state(&mut self, new_state: AvailabilityState) {
        if self.availability_state != new_state {
            info!("Availability state transition: {:?} -> {:?}", self.availability_state, new_state);
            
            // Track when we become unavailable for grace period timing
            if matches!(new_state, AvailabilityState::Unavailable) {
                let current_time = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                self.became_unavailable_at = Some(current_time);
                self.full_sync_request_pending = false; // Reset pending flag
                info!("Became unavailable at timestamp: {}, starting grace period", current_time);
            } else if matches!(new_state, AvailabilityState::Available) {
                self.became_unavailable_at = None;
                self.full_sync_request_pending = false;
            }
            
            self.availability_state = new_state;
        }
    }

    /// Check if the application is available for client connections
    fn is_available(&self) -> bool {
        matches!(self.availability_state, AvailabilityState::Available)
    }

    /// Force disconnect all connected clients (used when transitioning to unavailable)
    async fn force_disconnect_all_clients(&mut self) {
        if !self.connected_clients.is_empty() {
            info!("Force disconnecting {} clients due to unavailable state", self.connected_clients.len());
            
            // Send close messages to all connected clients
            let disconnect_message = Message::Close(None);
            for (client_addr, sender) in &self.connected_clients {
                if let Err(e) = sender.send(disconnect_message.clone()) {
                    warn!("Failed to send close message to client {}: {}", client_addr, e);
                }
            }
            
            // Clear all client-related data structures
            self.connected_clients.clear();
            
            // Clean up notification senders and configurations
            for (_client_addr, sender) in self.client_notification_senders.drain() {
                drop(sender); // This will close the notification channel
            }
            
            for (client_addr, configs) in self.client_notification_configs.drain() {
                // Note: Notifications will be cleaned up when the channels close
                debug!("Cleared {} notification configs for disconnected client {}", configs.len(), client_addr);
            }
        }
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
                                        if let Err(e) = store_guard.perform_mut(&mut requests).await {
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
                
                let mut should_be_leader = our_startup_time <= earliest_startup;
                
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
                        should_be_leader = **min_machine_id == our_machine_id;
                    }
                }
                
                // Update leadership status
                if should_be_leader {
                    state.is_leader = true;
                    state.current_leader = Some(our_machine_id.clone());
                    state.is_fully_synced = true;
                    state.set_availability_state(AvailabilityState::Available);
                    info!("We are the leader based on startup time comparison (startup_time: {})", our_startup_time);
                } else {
                    state.is_leader = false;
                    state.set_availability_state(AvailabilityState::Unavailable);
                    state.force_disconnect_all_clients().await;
                    let leader = state.peer_info.values()
                        .filter(|p| p.startup_time <= earliest_startup)
                        .min_by_key(|p| (&p.startup_time, &p.machine_id))
                        .map(|p| p.machine_id.clone());
                    state.current_leader = leader;
                    info!("Leader determined: {:?} (our startup_time: {})", state.current_leader, our_startup_time);
                }
            }
            debug!("Updated peer info for {}: startup_time={}", machine_id, startup_time);
        }
        
        PeerMessage::FullSyncRequest { machine_id } => {
            // Any peer can respond to sync requests
            info!("Received full sync request from {}, sending snapshot", machine_id);
            
            // Take a snapshot and send it
            let snapshot = {
                let state = app_state.read().await;
                let store = &state.store;
                let store_guard = store.read().await;
                store_guard.inner().take_snapshot()
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
                store_guard.inner_mut().disable_notifications();
                store_guard.inner_mut().restore_snapshot(snapshot.clone());
                store_guard.inner_mut().enable_notifications();
                drop(store_guard);
                state.is_fully_synced = true;
                state.full_sync_request_pending = false; // Reset pending flag since we got a response
                state.set_availability_state(AvailabilityState::Available);
            }
            
            // Save the snapshot to disk for persistence
            match save_snapshot(&snapshot, app_state.clone()).await {
                Ok(snapshot_counter) => {
                    info!("Snapshot saved to disk during full sync");
                    
                    // Write a snapshot marker to the WAL to indicate the sync point
                    // This helps during replay to know that the state was synced at this point
                    let snapshot_request = qlib_rs::Request::Snapshot {
                        snapshot_counter,
                        originator: Some(app_state.read().await.config.machine.clone()),
                    };
                    
                    if let Err(e) = write_request_to_wal_direct(&snapshot_request, app_state.clone()).await {
                        error!("Failed to write snapshot marker to WAL: {}", e);
                    }
                }
                Err(e) => {
                    error!("Failed to save snapshot during full sync: {}", e);
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
                if let Err(e) = store_guard.perform_mut(&mut requests_to_apply).await {
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
    
    // Check if the application is available for client connections
    {
        let state = app_state.read().await;
        if !state.is_available() {
            info!("Rejecting client connection from {} - application is unavailable", client_addr);
            // Don't accept the WebSocket connection, just return
            return Ok(());
        }
    }
    
    let ws_stream = accept_async(stream).await?;
    debug!("WebSocket connection established with client: {}", client_addr);
    
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    
    // Wait for authentication message as the first message
    let auth_timeout = tokio::time::timeout(Duration::from_secs(10), ws_receiver.next()).await;
    
    let first_message = match auth_timeout {
        Ok(Some(Ok(Message::Text(text)))) => text,
        Ok(Some(Ok(Message::Close(_)))) => {
            info!("Client {} closed connection before authentication", client_addr);
            return Ok(());
        }
        Ok(Some(Err(e))) => {
            error!("WebSocket error from client {} during authentication: {}", client_addr, e);
            return Ok(());
        }
        Ok(None) => {
            info!("Client {} closed connection before authentication", client_addr);
            return Ok(());
        }
        Ok(Some(Ok(_))) => {
            error!("Client {} sent non-text message during authentication", client_addr);
            let _ = ws_sender.close().await;
            return Ok(());
        }
        Err(_) => {
            info!("Client {} authentication timeout", client_addr);
            let _ = ws_sender.close().await;
            return Ok(());
        }
    };
    
    // Parse and validate authentication message
    let auth_message = match serde_json::from_str::<StoreMessage>(&first_message) {
        Ok(StoreMessage::Authenticate { .. }) => {
            serde_json::from_str::<StoreMessage>(&first_message).unwrap()
        }
        _ => {
            error!("Client {} first message was not authentication", client_addr);
            let _ = ws_sender.close().await;
            return Ok(());
        }
    };
    
    // Process authentication
    let auth_response = process_store_message(auth_message, &app_state, Some(client_addr.to_string())).await;
    
    // Send authentication response
    let auth_response_text = match serde_json::to_string(&auth_response) {
        Ok(text) => text,
        Err(e) => {
            error!("Failed to serialize authentication response: {}", e);
            let _ = ws_sender.close().await;
            return Ok(());
        }
    };
    
    if let Err(e) = ws_sender.send(Message::Text(auth_response_text)).await {
        error!("Failed to send authentication response to client {}: {}", client_addr, e);
        return Ok(());
    }
    
    // Check if authentication was successful
    let is_authenticated = {
        let state = app_state.read().await;
        state.authenticated_clients.contains_key(&client_addr.to_string())
    };
    
    if !is_authenticated {
        info!("Client {} authentication failed, closing connection", client_addr);
        let _ = ws_sender.close().await;
        return Ok(());
    }
    
    info!("Client {} authenticated successfully", client_addr);
    
    // Now proceed with normal client handling
    // Create a channel for sending messages to this client
    let (tx, rx) = mpsc::unbounded_channel::<Message>();
    
    // Create a notification channel for this client
    let (notification_sender, mut notification_receiver) = notification_channel();
    
    // Store the sender and notification sender in the connected_clients HashMap
    {
        let mut state = app_state.write().await;
        state.connected_clients.insert(client_addr.to_string(), tx.clone());
        state.client_notification_senders.insert(client_addr.to_string(), notification_sender);
    }
    
    // Spawn a task to handle notifications for this client
    let client_addr_clone_notif = client_addr.to_string();
    let tx_clone_notif = tx.clone();
    let notification_task = tokio::spawn(async move {
        while let Some(notification) = notification_receiver.recv().await {
            // Convert notification to StoreMessage and send to client
            let notification_msg = StoreMessage::Notification { notification };
            if let Ok(notification_text) = serde_json::to_string(&notification_msg) {
                if let Err(e) = tx_clone_notif.send(Message::Text(notification_text)) {
                    error!("Failed to send notification to client {}: {}", client_addr_clone_notif, e);
                    break;
                }
            } else {
                error!("Failed to serialize notification for client {}", client_addr_clone_notif);
            }
        }
        debug!("Notification task ended for client {}", client_addr_clone_notif);
    });
    
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
        state.authenticated_clients.remove(&client_addr_clone);
        state.client_notification_senders.remove(&client_addr_clone);
    });
    
    // Handle incoming messages from client (after successful authentication)
    while let Some(msg) = ws_receiver.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                debug!("Received text from client {}: {}", client_addr, text);
                
                // Parse the StoreMessage
                match serde_json::from_str::<StoreMessage>(&text) {
                    Ok(store_msg) => {
                        // Process the message and generate response
                        let response_msg = process_store_message(store_msg, &app_state, Some(client_addr.to_string())).await;
                        
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
    
    // Remove client from connected_clients and cleanup notifications when connection ends
    {
        let mut state = app_state.write().await;
        let client_addr_string = client_addr.to_string();
        state.connected_clients.remove(&client_addr_string);
        
        // Get the notification sender and configurations for this client
        let notification_sender = state.client_notification_senders.remove(&client_addr_string);
        let client_configs = state.client_notification_configs.remove(&client_addr_string);
        
        // Remove authentication state for this client
        state.authenticated_clients.remove(&client_addr_string);
        
        // Unregister all notifications for this client from the store
        if let Some(configs) = client_configs {
            if let Some(sender) = notification_sender {
                let store = &mut state.store;
                let mut store_guard = store.write().await;
                
                for config in configs {
                    let removed = store_guard.unregister_notification(&config, &sender).await;
                    if removed {
                        debug!("Cleaned up notification config for disconnected client {}: {:?}", client_addr_string, config);
                    } else {
                        warn!("Failed to clean up notification config for disconnected client {}: {:?}", client_addr_string, config);
                    }
                }
                
                drop(store_guard);
                // The notification sender being dropped will close the channel
                drop(sender);
            }
        } else if let Some(sender) = notification_sender {
            // Just drop the sender if no configs were tracked
            drop(sender);
        }
    }
    
    // Abort the outgoing task and notification task
    outgoing_task.abort();
    notification_task.abort();
    
    info!("Client connection closed: {}", client_addr);
    Ok(())
}

/// Process a StoreMessage and generate the appropriate response
async fn process_store_message(message: StoreMessage, app_state: &Arc<RwLock<AppState>>, client_addr: Option<String>) -> StoreMessage {
    // Extract client notification sender if needed (before accessing store)
    let client_notification_sender = if let Some(ref addr) = client_addr {
        let state = app_state.read().await;
        state.client_notification_senders.get(addr).cloned()
    } else {
        None
    };

    match message {
        StoreMessage::Authenticate { id, subject_name, credential } => {
            // Perform authentication
            let auth_config = AuthConfig::default();
            let store = app_state.write().await.store.clone();
            let mut store_guard = store.write().await;
            
            match authenticate_subject(&mut store_guard, &subject_name, &credential, &auth_config).await {
                Ok(subject_id) => {
                    // Store authentication state
                    if let Some(ref addr) = client_addr {
                        let mut state = app_state.write().await;
                        state.authenticated_clients.insert(addr.clone(), subject_id.clone());
                    }
                    
                    // Create authentication result
                    let auth_result = AuthenticationResult {
                        subject_id: subject_id.clone(),
                        subject_type: if subject_id.to_string().starts_with("user:") { 
                            "User".to_string() 
                        } else { 
                            "Service".to_string() 
                        },
                    };
                    
                    StoreMessage::AuthenticateResponse {
                        id,
                        response: Ok(auth_result),
                    }
                }
                Err(e) => StoreMessage::AuthenticateResponse {
                    id,
                    response: Err(format!("{:?}", e)),
                },
            }
        }
        
        StoreMessage::AuthenticateResponse { .. } => {
            // This should not be sent by clients, only by server
            StoreMessage::Error {
                id: "unknown".to_string(),
                error: "Invalid message type".to_string(),
            }
        }
        
        // All other messages require authentication
        _ => {
            // Check if client is authenticated
            let is_authenticated = if let Some(ref addr) = client_addr {
                let state = app_state.read().await;
                state.authenticated_clients.contains_key(addr)
            } else {
                false // No client address means not authenticated
            };
            
            if !is_authenticated {
                return StoreMessage::Error {
                    id: "unknown".to_string(),
                    error: "Authentication required".to_string(),
                };
            }
            
            match message {
                StoreMessage::Authenticate { .. } |
                StoreMessage::AuthenticateResponse { .. } => {
                    // These are handled in the outer match, should not reach here
                    StoreMessage::Error {
                        id: "unknown".to_string(),
                        error: "Authentication messages should not reach this point".to_string(),
                    }
                }
        
                StoreMessage::GetEntitySchema { id, entity_type } => {
                    match app_state
                        .read().await
                        .store
                        .read().await
                        .get_entity_schema(&entity_type).await {
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
                    match app_state
                        .read().await
                        .store
                        .read().await
                        .get_complete_entity_schema(&entity_type).await {
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
                    match app_state
                        .read().await
                        .store
                        .read().await
                        .get_field_schema(&entity_type, &field_type).await {
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
                    let exists = app_state
                        .read().await
                        .store
                        .read().await
                        .entity_exists(&entity_id)
                        .await;
                    StoreMessage::EntityExistsResponse {
                        id,
                        response: exists,
                    }
                }
                
                StoreMessage::FieldExists { id, entity_type, field_type } => {
                    let exists = app_state
                        .read().await
                        .store
                        .read().await
                        .field_exists(&entity_type, &field_type)
                        .await;
                    StoreMessage::FieldExistsResponse {
                        id,
                        response: exists,
                    }
                }
                
                StoreMessage::Perform { id, mut requests } => {
                    // Check if the client is authorized to perform these requests
                    if let Some(client_addr) = &client_addr {
                        // Get the authenticated subject ID for this client
                        let subject_id = {
                            let state = app_state.read().await;
                            state.authenticated_clients.get(client_addr).cloned()
                        };

                        if let Some(subject_id) = subject_id {
                            let permission_cache = &app_state.read().await.permission_cache;

                            for request in &requests {
                                    if let Some(entity_id) = request.entity_id() {
                                        if let Some(field_type) = request.field_type() {
                                            match get_scope(
                                                app_state.read().await.store.clone(),
                                                &mut app_state.write().await.cel_executor,
                                                permission_cache,
                                                &subject_id,
                                                entity_id,
                                                field_type,
                                            ).await {
                                                Ok(scope) => {
                                                    if scope == AuthorizationScope::None {
                                                        return StoreMessage::PerformResponse {
                                                            id,
                                                            response: Err(format!(
                                                                "Access denied: Subject {} is not authorized to access {} on entity {}",
                                                            subject_id,
                                                            field_type,
                                                            entity_id
                                                        )),
                                                    };
                                                }
                                                // For write operations, check if we have write access
                                                if matches!(request, qlib_rs::Request::Write { .. } | qlib_rs::Request::Create { .. } | qlib_rs::Request::Delete { .. } | qlib_rs::Request::SchemaUpdate { .. }) {
                                                    if scope == AuthorizationScope::ReadOnly {
                                                        return StoreMessage::PerformResponse {
                                                            id,
                                                            response: Err(format!(
                                                                "Access denied: Subject {} only has read access to {} on entity {}",
                                                                subject_id,
                                                                field_type,
                                                                entity_id
                                                            )),
                                                        };
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                return StoreMessage::PerformResponse {
                                                    id,
                                                    response: Err(format!("Authorization check failed: {:?}", e)),
                                                };
                                            }
                                        }
                                    }
                                }
                            }
                        } else {
                            return StoreMessage::PerformResponse {
                                id,
                                response: Err("Client is not authenticated".to_string()),
                            };
                        }
                    } else {
                        return StoreMessage::PerformResponse {
                            id,
                            response: Err("Authorization cache not available".to_string()),
                        };
                    }

                    let machine = app_state.read().await.config.machine.clone();

                    // Allow write operations on any peer
                    requests.iter_mut().for_each(|req| {
                        req.try_set_originator(machine.clone());
                    });

                    match app_state
                        .write().await
                        .store
                        .write().await
                        .perform_mut(&mut requests).await {
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
                
                StoreMessage::FindEntities { id, entity_type, page_opts, filter } => {
                    match app_state
                        .read().await
                        .store
                        .read().await
                        .find_entities_paginated(&entity_type, page_opts, filter).await {
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
                
                StoreMessage::FindEntitiesExact { id, entity_type, page_opts, filter } => {
                    match app_state
                        .read().await
                        .store
                        .read().await
                        .find_entities_exact(&entity_type, page_opts, filter).await {
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
                    match app_state
                        .read().await
                        .store
                        .read().await
                        .get_entity_types_paginated(page_opts).await {
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
                
                StoreMessage::RegisterNotification { id, config } => {
                    // Register notification for this client
                    if let Some(client_addr) = &client_addr {
                        if let Some(ref notification_sender) = client_notification_sender {
                            match app_state
                                .write().await
                                .store
                                .write().await
                                .register_notification(config.clone(), notification_sender.clone()).await {
                                Ok(()) => {
                                    app_state
                                        .write().await
                                        .client_notification_configs
                                        .entry(client_addr.clone())
                                        .or_insert_with(HashSet::new)
                                        .insert(config);
                                    
                                    debug!("Registered notification for client {}", client_addr);
                                    StoreMessage::RegisterNotificationResponse {
                                        id,
                                        response: Ok(()),
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to register notification for client {}: {:?}", client_addr, e);
                                    StoreMessage::RegisterNotificationResponse {
                                        id,
                                        response: Err(format!("Failed to register notification: {:?}", e)),
                                    }
                                }
                            }
                        } else {
                            error!("No notification sender found for client {}", client_addr);
                            StoreMessage::RegisterNotificationResponse {
                                id,
                                response: Err("Client notification sender not found".to_string()),
                            }
                        }
                    } else {
                        StoreMessage::RegisterNotificationResponse {
                            id,
                            response: Err("Client address not provided".to_string()),
                        }
                    }
                }
                
                StoreMessage::UnregisterNotification { id, config } => {
                    // Unregister notification for this client
                    if let Some(client_addr) = &client_addr {
                        if let Some(ref notification_sender) = client_notification_sender {
                            let removed = app_state
                                .write().await
                                .store
                                .write().await
                                .unregister_notification(&config, notification_sender).await;
                            
                            // Remove from client's tracked configs if successfully unregistered
                            if removed {
                                if let Some(client_configs) = app_state
                                    .write().await
                                    .client_notification_configs
                                    .get_mut(client_addr) {
                                    client_configs.remove(&config);
                                }
                            }
                            
                            debug!("Unregistered notification for client {}: removed: {}", client_addr, removed);
                            StoreMessage::UnregisterNotificationResponse {
                                id,
                                response: removed,
                            }
                        } else {
                            error!("No notification sender found for client {}", client_addr);
                            StoreMessage::UnregisterNotificationResponse {
                                id,
                                response: false,
                            }
                        }
                    } else {
                        StoreMessage::UnregisterNotificationResponse {
                            id,
                            response: false,
                        }
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
        store_guard.inner().get_write_channel_receiver()
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
                store_guard.inner().take_snapshot()
            };
            
            drop(state); // Release the lock before calling save_snapshot
            
            // Save the snapshot to disk
            match save_snapshot(&snapshot, app_state.clone()).await {
                Ok(snapshot_counter) => {
                    // Reset the WAL files counter
                    let mut state = app_state.write().await;
                    state.wal_files_since_snapshot = 0;
                    info!("Snapshot saved successfully after WAL rollover");
                    
                    // Write a snapshot marker to the WAL to indicate the snapshot point
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
                Err(e) => {
                    error!("Failed to save snapshot after WAL rollover: {}", e);
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

/// Save a snapshot to disk and return the snapshot counter that was used
async fn save_snapshot(snapshot: &qlib_rs::Snapshot, app_state: Arc<RwLock<AppState>>) -> Result<u64> {
    let mut state = app_state.write().await;
    
    // Create snapshots directory if it doesn't exist
    let snapshot_dir = state.get_snapshots_dir();
    create_dir_all(&snapshot_dir).await?;
    
    // Increment the counter before creating the filename
    let current_snapshot_counter = state.snapshot_file_counter;
    state.snapshot_file_counter += 1;
    
    // Create snapshot filename with the current counter
    let snapshot_filename = format!("snapshot_{:010}.bin", current_snapshot_counter);
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
    
    info!("Snapshot saved successfully, {} bytes", serialized.len());
    
    // Clean up old snapshots using the configured limit
    let max_files = state.config.snapshot_max_files;
    if let Err(e) = cleanup_old_snapshots(&snapshot_dir, max_files).await {
        error!("Failed to clean up old snapshots: {}", e);
    }
    
    Ok(current_snapshot_counter)
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

/// Replay WAL files to restore store state
/// This function scans all WAL files to find the most recent snapshot marker
/// and starts replaying from that point
async fn replay_wal_files(app_state: Arc<RwLock<AppState>>) -> Result<()> {
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
                            wal_files.push((path, counter));
                        }
                    }
                }
            }
        }
    }
    
    if wal_files.is_empty() {
        info!("No WAL files found, no replay needed");
        return Ok(());
    }
    
    // Sort files by counter (which corresponds to creation order)
    wal_files.sort_by_key(|(_, counter)| *counter);
    
    drop(state); // Release the lock before processing
    
    // Find the most recent snapshot marker across all WAL files
    let mut most_recent_snapshot: Option<(PathBuf, u64, usize)> = None; // (file_path, wal_counter, offset_after_snapshot)
    
    info!("Scanning {} WAL files to find the most recent snapshot marker", wal_files.len());
    
    for (wal_file, counter) in &wal_files {
        if let Ok(snapshot_info) = find_most_recent_snapshot_in_wal(wal_file).await {
            if let Some((offset_after_snapshot, _)) = snapshot_info {
                // Update if this is the most recent snapshot found so far
                if most_recent_snapshot.is_none() || counter > &most_recent_snapshot.as_ref().unwrap().1 {
                    most_recent_snapshot = Some((wal_file.clone(), *counter, offset_after_snapshot));
                }
            }
        }
    }
    
    // Disable notifications during WAL replay
    {
        let mut state = app_state.write().await;
        let store = &mut state.store;
        let mut store_guard = store.write().await;
        store_guard.inner_mut().disable_notifications();
        info!("Notifications disabled for WAL replay");
    }
    
    if let Some((snapshot_wal_file, snapshot_counter, snapshot_offset)) = most_recent_snapshot {
        info!("Starting replay from WAL file {} (counter: {}) at offset {}", 
              snapshot_wal_file.display(), snapshot_counter, snapshot_offset);
        
        // First, replay the partial WAL file that contains the snapshot (from the offset)
        if let Err(e) = replay_wal_file_from_offset(&snapshot_wal_file, snapshot_offset, app_state.clone()).await {
            error!("Failed to replay WAL file {} from offset {}: {}", snapshot_wal_file.display(), snapshot_offset, e);
        }
        
        // Then replay all subsequent WAL files completely
        for (wal_file, counter) in &wal_files {
            if counter > &snapshot_counter {
                info!("Replaying complete WAL file: {} (counter: {})", wal_file.display(), counter);
                if let Err(e) = replay_single_wal_file(wal_file, app_state.clone()).await {
                    error!("Failed to replay WAL file {}: {}", wal_file.display(), e);
                }
            }
        }
    } else {
        info!("No snapshot markers found in WAL files, replaying all {} files completely", wal_files.len());
        
        // No snapshot markers found, replay all WAL files completely
        for (wal_file, counter) in &wal_files {
            info!("Replaying WAL file: {} (counter: {})", wal_file.display(), counter);
            if let Err(e) = replay_single_wal_file(wal_file, app_state.clone()).await {
                error!("Failed to replay WAL file {}: {}", wal_file.display(), e);
            }
        }
    }
    
    // Re-enable notifications after WAL replay
    {
        let mut state = app_state.write().await;
        let store = &mut state.store;
        let mut store_guard = store.write().await;
        store_guard.inner_mut().enable_notifications();
        info!("Notifications re-enabled after WAL replay");
    }
    
    info!("WAL replay completed");
    Ok(())
}

/// Apply a single WAL request from request data
async fn apply_wal_request(request_data: &[u8], app_state: &Arc<RwLock<AppState>>, requests_processed: &mut usize) -> Result<()> {
    match serde_json::from_slice::<qlib_rs::Request>(request_data) {
        Ok(request) => {
            // Skip snapshot requests during replay (they are just markers)
            if matches!(request, qlib_rs::Request::Snapshot { .. }) {
                debug!("Skipping snapshot marker during replay");
                return Ok(());
            }
            
            let mut state = app_state.write().await;
            let store = &mut state.store;
            let mut store_guard = store.write().await;
            
            let mut requests = vec![request];
            if let Err(e) = store_guard.perform_mut(&mut requests).await {
                drop(store_guard);
                drop(state);
                return Err(anyhow::anyhow!("Failed to apply request during WAL replay: {}", e));
            } else {
                *requests_processed += 1;
            }
            
            drop(store_guard);
            drop(state);
            Ok(())
        }
        Err(e) => {
            Err(anyhow::anyhow!("Failed to deserialize request: {}", e))
        }
    }
}

/// Replay a single WAL file completely from beginning to end
/// Skips snapshot markers but processes all other requests
async fn replay_single_wal_file(wal_path: &PathBuf, app_state: Arc<RwLock<AppState>>) -> Result<()> {
    let mut file = File::open(wal_path).await?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).await?;
    
    let mut requests_processed = 0;
    let mut offset = 0;
    
    info!("Replaying complete WAL file: {}", wal_path.display());
    
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
        
        // Apply the request using the helper function
        if let Err(e) = apply_wal_request(request_data, &app_state, &mut requests_processed).await {
            error!("Failed to apply request at offset {}: {}", offset - len, e);
        }
    }
    
    info!("Replayed {} requests from {}", requests_processed, wal_path.display());
    Ok(())
}

/// Parse WAL file entries and call a callback for each request
/// The callback receives (offset, request_data_slice, is_last_entry)
async fn parse_wal_file<F>(wal_path: &PathBuf, mut callback: F) -> Result<()>
where
    F: FnMut(usize, &[u8]) -> Result<bool>, // Returns Ok(true) to continue, Ok(false) to stop, Err to error
{
    let mut file = File::open(wal_path).await?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).await?;
    
    let mut offset = 0;
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
        
        // Call the callback with the current data
        match callback(offset, request_data) {
            Ok(true) => {}, // Continue
            Ok(false) => break, // Stop processing
            Err(e) => return Err(e), // Propagate error
        }
        
        offset += len;
    }
    
    Ok(())
}

/// Find the most recent snapshot marker in a WAL file and return its offset
/// Returns Ok(Some((offset_after_snapshot, snapshot_request))) if found, Ok(None) if not found
async fn find_most_recent_snapshot_in_wal(wal_path: &PathBuf) -> Result<Option<(usize, qlib_rs::Request)>> {
    let mut last_snapshot_offset = None;
    let mut last_snapshot_request = None;
    
    parse_wal_file(wal_path, |offset, request_data| {
        // Check if this is a snapshot marker
        if let Ok(request) = serde_json::from_slice::<qlib_rs::Request>(request_data) {
            if matches!(request, qlib_rs::Request::Snapshot { .. }) {
                last_snapshot_offset = Some(offset + request_data.len());
                last_snapshot_request = Some(request);
                debug!("Found snapshot marker at offset {} in {}", offset, wal_path.display());
            }
        }
        Ok(true) // Continue processing
    }).await?;
    
    if let (Some(offset), Some(request)) = (last_snapshot_offset, last_snapshot_request) {
        Ok(Some((offset, request)))
    } else {
        Ok(None)
    }
}

/// Replay a WAL file starting from a specific byte offset
async fn replay_wal_file_from_offset(wal_path: &PathBuf, start_offset: usize, app_state: Arc<RwLock<AppState>>) -> Result<()> {
    let mut file = File::open(wal_path).await?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).await?;
    
    let mut requests_processed = 0;
    let mut offset = start_offset;
    
    info!("Replaying WAL file {} from offset {}", wal_path.display(), start_offset);
    
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
        if let Err(e) = apply_wal_request(request_data, &app_state, &mut requests_processed).await {
            error!("Failed to apply request at offset {}: {}", offset - len, e);
        }
    }
    
    info!("Replayed {} requests from {} (starting from offset {})", requests_processed, wal_path.display(), start_offset);
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

/// Handle miscellaneous periodic tasks that run every 10ms
async fn handle_misc_tasks(app_state: Arc<RwLock<AppState>>) -> Result<()> {
    info!("Starting miscellaneous tasks handler (10ms interval)");
    
    let mut interval = tokio::time::interval(Duration::from_millis(10));
    
    loop {
        interval.tick().await;
        
        // Check if we need to send a full sync request after grace period
        let should_send_full_sync = {
            let state = app_state.read().await;
            
            // Only check if we're unavailable, not the leader, not fully synced, and haven't sent a request yet
            if matches!(state.availability_state, AvailabilityState::Unavailable) &&
               !state.is_leader &&
               !state.is_fully_synced &&
               !state.full_sync_request_pending {
                
                if let Some(became_unavailable_at) = state.became_unavailable_at {
                    let current_time = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    
                    let grace_period_secs = state.config.full_sync_grace_period_secs;
                    let elapsed = current_time.saturating_sub(became_unavailable_at);
                    
                    if elapsed >= grace_period_secs {
                        // Grace period has expired, check if we have a known leader
                        state.current_leader.clone()
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        };
        
        if let Some(leader_machine_id) = should_send_full_sync {
            info!("Grace period expired, sending FullSyncRequest to leader: {}", leader_machine_id);
            
            // Mark that we're sending a request to avoid duplicates
            {
                let mut state = app_state.write().await;
                state.full_sync_request_pending = true;
            }
            
            // Send FullSyncRequest to the leader through any connected outbound peer
            let machine_id = {
                let state = app_state.read().await;
                state.config.machine.clone()
            };
            
            let full_sync_request = PeerMessage::FullSyncRequest {
                machine_id: machine_id.clone(),
            };
            
            if let Ok(request_json) = serde_json::to_string(&full_sync_request) {
                let message = Message::Text(request_json);
                
                // Try to send to any connected outbound peer
                let sent = {
                    let state = app_state.read().await;
                    let mut sent = false;
                    
                    for (peer_addr, sender) in &state.connected_outbound_peers {
                        if let Err(e) = sender.send(message.clone()) {
                            warn!("Failed to send FullSyncRequest to peer {}: {}", peer_addr, e);
                        } else {
                            info!("Sent FullSyncRequest to peer: {}", peer_addr);
                            sent = true;
                            break; // Only need to send to one peer
                        }
                    }
                    sent
                };
                
                if !sent {
                    warn!("No connected outbound peers available to send FullSyncRequest to leader {}", leader_machine_id);
                    // Reset the pending flag so we can try again later
                    let mut state = app_state.write().await;
                    state.full_sync_request_pending = false;
                }
            } else {
                error!("Failed to serialize FullSyncRequest");
                let mut state = app_state.write().await;
                state.full_sync_request_pending = false;
            }
        }
        
        // Keep the task lightweight since it runs every 10ms
        tokio::task::yield_now().await; // Yield to prevent blocking
    }
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
    let app_state = Arc::new(RwLock::new(AppState::new(config).await?));

    // Initialize the WAL file counter based on existing files
    {
        let next_wal_counter = get_next_wal_counter(app_state.clone()).await?;
        let mut state = app_state.write().await;
        state.wal_file_counter = next_wal_counter;
        info!("Initialized WAL file counter to {}", next_wal_counter);
    }

    // Load the latest snapshot if available
    if let Some((snapshot, snapshot_counter)) = load_latest_snapshot(app_state.clone()).await? {
        info!("Restoring store from snapshot (counter: {})", snapshot_counter);
        
        // Initialize the snapshot file counter to continue from the next number
        let next_snapshot_counter = get_next_snapshot_counter(app_state.clone()).await?;
        
        let mut state = app_state.write().await;
        let store = &mut state.store;
        let mut store_guard = store.write().await;
        store_guard.inner_mut().disable_notifications();
        store_guard.inner_mut().restore_snapshot(snapshot);
        store_guard.inner_mut().enable_notifications();
        drop(store_guard);
        
        state.snapshot_file_counter = next_snapshot_counter;
    } else {
        info!("No snapshot found, starting with empty store");
        
        // Initialize the snapshot file counter
        let next_snapshot_counter = get_next_snapshot_counter(app_state.clone()).await?;
        let mut state = app_state.write().await;
        state.snapshot_file_counter = next_snapshot_counter;
    }

    // Replay WAL files to bring the store up to date
    // The replay function will automatically find the most recent snapshot marker
    // in the WAL files and start replaying from that point
    info!("Replaying WAL files");
    if let Err(e) = replay_wal_files(app_state.clone()).await {
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

    // Start the misc tasks handler
    let app_state_clone = Arc::clone(&app_state);
    let misc_task = tokio::spawn(async move {
        if let Err(e) = handle_misc_tasks(app_state_clone).await {
            error!("Misc tasks handler failed: {}", e);
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
        store_guard.inner().take_snapshot()
    };
    
    match save_snapshot(&snapshot, app_state.clone()).await {
        Ok(snapshot_counter) => {
            info!("Final snapshot saved successfully");
            
            // Write a snapshot marker to the WAL to indicate the final snapshot point
            // This helps during replay to know that the state was snapshotted at shutdown
            let snapshot_request = qlib_rs::Request::Snapshot {
                snapshot_counter,
                originator: Some(app_state.read().await.config.machine.clone()),
            };
            
            if let Err(e) = write_request_to_wal_direct(&snapshot_request, app_state.clone()).await {
                error!("Failed to write final snapshot marker to WAL: {}", e);
            } else {
                info!("Final snapshot marker written to WAL");
            }
        }
        Err(e) => {
            error!("Failed to save final snapshot: {}", e);
        }
    }

    // Abort all tasks
    write_channel_task.abort();
    peer_server_task.abort();
    client_server_task.abort();
    outbound_peer_task.abort();
    misc_task.abort();

    Ok(())
}
