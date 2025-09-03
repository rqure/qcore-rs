use qlib_rs::{et, ft, notification_channel, now, schoice, sread, sref, swrite, AsyncStore, AuthConfig, AuthenticationResult, Cache, CelExecutor, EntityId, NotificationSender, NotifyConfig, PushCondition, Snowflake, StoreMessage, StoreTrait};
use qlib_rs::auth::{authenticate_subject, AuthorizationScope, get_scope};
use tokio::signal;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, connect_async, tungstenite::Message};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tracing::{info, warn, error, debug, instrument};
use clap::Parser;
use anyhow::Result;
use std::collections::{HashSet, HashMap};
use std::sync::Arc;
use std::vec;
use tokio::sync::RwLock;
use std::time::Duration;
use tokio::fs::{File, OpenOptions, create_dir_all, read_dir, remove_file};
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use std::path::PathBuf;
use serde::{Serialize, Deserialize};
use time;

/// Application availability state
#[derive(Debug, Clone, PartialEq)]
enum AvailabilityState {
    /// Application is unavailable - attempting to sync with leader, clients are force disconnected
    Unavailable,
    /// Application is available - clients are allowed to connect and perform operations
    Available,
}

/// Helper structure for extracting data without holding locks
#[derive(Debug, Clone)]
struct StateSnapshot {
    config: Config,
    startup_time: u64,
    availability_state: AvailabilityState,
}

impl StateSnapshot {
    fn is_available(&self) -> bool {
        matches!(self.availability_state, AvailabilityState::Available)
    }
}

/// Connection state separated from main state to reduce lock contention
#[derive(Debug)]
struct ConnectionState {
    /// Connected outbound peers with message senders
    connected_outbound_peers: HashMap<String, mpsc::UnboundedSender<Message>>,
    /// Connected clients with message senders
    connected_clients: HashMap<String, mpsc::UnboundedSender<Message>>,
    /// Client notification senders
    client_notification_senders: HashMap<String, NotificationSender>,
    /// Track notification configurations per client for cleanup on disconnect
    client_notification_configs: HashMap<String, HashSet<NotifyConfig>>,
    /// Track authenticated clients
    authenticated_clients: HashMap<String, EntityId>,
}

impl ConnectionState {
    fn new() -> Self {
        Self {
            connected_outbound_peers: HashMap::new(),
            connected_clients: HashMap::new(),
            client_notification_senders: HashMap::new(),
            client_notification_configs: HashMap::new(),
            authenticated_clients: HashMap::new(),
        }
    }
}

/// WAL state separated to avoid lock contention
#[derive(Debug)]
struct WalState {
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

impl WalState {
    fn new() -> Self {
        Self {
            current_wal_file: None,
            current_wal_size: 0,
            wal_file_counter: 0,
            snapshot_file_counter: 0,
            wal_files_since_snapshot: 0,
        }
    }
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

    /// Maximum WAL file size in MB
    #[arg(long, default_value_t = 1)]
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
    #[arg(long, default_value_t = 3)]
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
    /// Core configuration and leadership state - should be accessed minimally
    core_state: RwLock<CoreState>,
    
    /// Connection-related state - separate lock to reduce contention
    connections: RwLock<ConnectionState>,
    
    /// WAL file state - separate lock for file operations
    wal_state: RwLock<WalState>,
    
    /// Peer information tracking
    peer_info: RwLock<HashMap<String, PeerInfo>>,
    
    /// Data store - kept separate as it has its own locking
    store: Arc<RwLock<AsyncStore>>,
    
    /// Permission Cache - separate as it's accessed frequently
    permission_cache: RwLock<Option<Cache<AsyncStore>>>,
    
    /// CEL Executor for evaluating authorization conditions
    cel_executor: RwLock<CelExecutor>,
}

/// Core application state
#[derive(Debug)]
struct CoreState {
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
    
    /// Timestamp when we became unavailable (for grace period tracking)
    became_unavailable_at: Option<u64>,

    /// Whether a full sync request is pending (to avoid sending multiple)
    full_sync_request_pending: bool,
}

/// Information about a peer instance
#[derive(Debug, Clone)]
struct PeerInfo {
    machine_id: String,
    startup_time: u64
}

impl AppState {
    async fn new(config: Config) -> Result<Self> {
        let startup_time = time::OffsetDateTime::now_utc().unix_timestamp() as u64;
        let store = Arc::new(RwLock::new(AsyncStore::new(Arc::new(Snowflake::new()))));
        
        Ok(Self {
            core_state: RwLock::new(CoreState {
                config,
                startup_time,
                availability_state: AvailabilityState::Available,
                is_leader: false,
                current_leader: None,
                is_fully_synced: false,
                became_unavailable_at: None,
                full_sync_request_pending: false,
            }),
            connections: RwLock::new(ConnectionState::new()),
            wal_state: RwLock::new(WalState::new()),
            peer_info: RwLock::new(HashMap::new()),
            store,
            permission_cache: RwLock::new(None),
            cel_executor: RwLock::new(CelExecutor::new()),
        })
    }
    
    /// Get a snapshot of the core state without holding locks
    async fn get_state_snapshot(&self) -> StateSnapshot {
        let core = self.core_state.read().await;
        StateSnapshot {
            config: core.config.clone(),
            startup_time: core.startup_time,
            availability_state: core.availability_state.clone(),
        }
    }
    
    /// Get the machine-specific data directory
    async fn get_machine_data_dir(&self) -> PathBuf {
        let core = self.core_state.read().await;
        PathBuf::from(&core.config.data_dir).join(&core.config.machine)
    }

    /// Get the machine-specific WAL directory
    async fn get_wal_dir(&self) -> PathBuf {
        let machine_data_dir = self.get_machine_data_dir().await;
        machine_data_dir.join("wal")
    }

    /// Get the machine-specific snapshots directory
    async fn get_snapshots_dir(&self) -> PathBuf {
        let machine_data_dir = self.get_machine_data_dir().await;
        machine_data_dir.join("snapshots")
    }

    /// Force disconnect all connected clients (used when transitioning to unavailable)
    async fn force_disconnect_all_clients(&self) {
        let mut connections = self.connections.write().await;
        
        if !connections.connected_clients.is_empty() {
            let client_count = connections.connected_clients.len();
            info!(
                client_count = client_count,
                "Force disconnecting clients due to unavailable state"
            );
            
            // Send close messages to all connected clients
            let disconnect_message = Message::Close(None);
            for (client_addr, sender) in &connections.connected_clients {
                if let Err(e) = sender.send(disconnect_message.clone()) {
                    warn!(
                        client_addr = %client_addr,
                        error = %e,
                        "Failed to send close message to client"
                    );
                }
            }
            
            // Clear all client-related data structures
            connections.connected_clients.clear();
            
            // Clean up notification senders and configurations
            for (_client_addr, sender) in connections.client_notification_senders.drain() {
                drop(sender); // This will close the notification channel
            }
            
            for (client_addr, configs) in connections.client_notification_configs.drain() {
                // Note: Notifications will be cleaned up when the channels close
                debug!(
                    client_addr = %client_addr,
                    config_count = configs.len(),
                    "Cleared notification configs for disconnected client"
                );
            }
        }
    }
}

/// Handle a single peer WebSocket connection
#[instrument(skip(stream, app_state), fields(peer_addr = %peer_addr))]
async fn handle_inbound_peer_connection(stream: TcpStream, peer_addr: std::net::SocketAddr, app_state: Arc<AppState>) -> Result<()> {
    info!("Accepting inbound peer connection");
    
    let ws_stream = accept_async(stream).await?;
    debug!("WebSocket handshake completed");
    
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    
    // Handle incoming messages from peer
    while let Some(msg) = ws_receiver.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                debug!(message_length = text.len(), "Received text message from peer");
                
                // Try to parse as a PeerMessage first
                match serde_json::from_str::<PeerMessage>(&text) {
                    Ok(peer_msg) => {
                        debug!(message_type = ?std::mem::discriminant(&peer_msg), "Processing peer message");
                        handle_peer_message(peer_msg, &peer_addr, &mut ws_sender, app_state.clone()).await;
                    }
                    Err(_) => {
                        debug!("Received non-peer text message from peer, ignoring")
                    }
                }
            }
            Ok(Message::Binary(data)) => {
                debug!(data_length = data.len(), "Received binary data from peer");
                
                // Try to parse as a PeerMessage (likely FullSyncResponse with binary serialization)
                match bincode::deserialize::<PeerMessage>(&data) {
                    Ok(peer_msg) => {
                        debug!(message_type = ?std::mem::discriminant(&peer_msg), "Processing binary peer message");
                        handle_peer_message(peer_msg, &peer_addr, &mut ws_sender, app_state.clone()).await;
                    }
                    Err(e) => {
                        debug!(error = %e, "Failed to deserialize binary peer message, ignoring");
                    }
                }
            }
            Ok(Message::Ping(payload)) => {
                debug!("Received ping from peer");
                if let Err(e) = ws_sender.send(Message::Pong(payload)).await {
                    error!(error = %e, "Failed to send pong to peer");
                    break;
                }
            }
            Ok(Message::Pong(_)) => {
                debug!("Received pong from peer");
            }
            Ok(Message::Close(_)) => {
                info!("Peer closed connection gracefully");
                break;
            }
            Ok(Message::Frame(_)) => {
                // Handle raw frames if needed - typically not used directly
                debug!("Received raw frame from peer");
            }
            Err(e) => {
                error!(error = %e, "WebSocket error with peer");
                break;
            }
        }
    }
    
    // Remove peer from connected inbound peers when connection ends
    {
        let mut peer_info = app_state.peer_info.write().await;
        peer_info.retain(|addr, _| {
            addr != &peer_addr.to_string()
        });
    }
    
    info!("Peer connection terminated");
    Ok(())
}

/// Handle a peer message and respond appropriately
#[instrument(skip(peer_msg, ws_sender, app_state), fields(peer_addr = %peer_addr))]
async fn handle_peer_message(
    peer_msg: PeerMessage,
    peer_addr: &std::net::SocketAddr,
    ws_sender: &mut futures_util::stream::SplitSink<tokio_tungstenite::WebSocketStream<TcpStream>, Message>,
    app_state: Arc<AppState>,
) {
    match peer_msg {
        PeerMessage::Startup { machine_id, startup_time } => {
            info!(
                remote_machine_id = %machine_id, 
                remote_startup_time = startup_time,
                "Processing startup message from peer"
            );
            
            // Update peer information
            {
                let mut peer_info = app_state.peer_info.write().await;
                peer_info.insert(peer_addr.to_string(), PeerInfo {
                    machine_id: machine_id.clone(),
                    startup_time,
                });
            }
            
            // Get current state snapshot for leadership determination
            let state_snapshot = app_state.get_state_snapshot().await;
            let our_startup_time = state_snapshot.startup_time;
            let our_machine_id = state_snapshot.config.machine.clone();
            
            // Find the earliest (largest) startup time among all known peers including ourselves
            let (should_be_leader, earliest_startup) = {
                let peer_info = app_state.peer_info.read().await;
                let earliest_startup = peer_info.values()
                    .map(|p| p.startup_time)
                    .min()
                    .unwrap_or(our_startup_time)
                    .min(our_startup_time);
                
                let mut should_be_leader = our_startup_time <= earliest_startup;
                
                // Handle startup time ties
                if our_startup_time == earliest_startup {
                    let peers_with_same_time: Vec<_> = peer_info.values()
                        .filter(|p| p.startup_time == our_startup_time)
                        .collect();
                    
                    if !peers_with_same_time.is_empty() {
                        // We have a tie, use machine_id as tiebreaker
                        info!(
                            peer_count = peers_with_same_time.len(),
                            startup_time = our_startup_time,
                            "Startup time tie detected, using machine_id as tiebreaker"
                        );
                        
                        let mut all_machine_ids = peers_with_same_time.iter()
                            .map(|p| p.machine_id.as_str())
                            .collect::<Vec<_>>();
                        all_machine_ids.push(our_machine_id.as_str());
                        
                        let min_machine_id = all_machine_ids.iter().min().unwrap();
                        should_be_leader = **min_machine_id == our_machine_id;
                    }
                }

                (should_be_leader, earliest_startup)
            };
            
            // Update leadership status
            if should_be_leader {
                let mut core_state = app_state.core_state.write().await;
                core_state.is_leader = true;
                core_state.current_leader = Some(our_machine_id.clone());
                core_state.is_fully_synced = true;
                let new_state = AvailabilityState::Available;
                let old_state = core_state.availability_state.clone();
                core_state.availability_state = new_state.clone();
                
                // Clear the unavailable timestamp when becoming available
                if old_state != new_state {
                    core_state.became_unavailable_at = None;
                }
                
                if old_state != new_state {
                    info!(
                        old_state = ?old_state,
                        new_state = ?new_state,
                        "Availability state transition"
                    );
                }
                info!(
                    our_startup_time = our_startup_time,
                    earliest_startup = earliest_startup,
                    "Elected as leader"
                );
            } else {
                let leader = {
                    let mut core_state = app_state.core_state.write().await;
                    core_state.is_leader = false;
                    let new_state = AvailabilityState::Unavailable;
                    let old_state = core_state.availability_state.clone();
                    core_state.availability_state = new_state.clone();
                    
                    // Set the timestamp when we became unavailable for grace period tracking
                    if old_state != new_state {
                        core_state.became_unavailable_at = Some(time::OffsetDateTime::now_utc().unix_timestamp() as u64);
                    }
                    
                    let leader = {
                        let peer_info = app_state.peer_info.read().await;
                        peer_info.values()
                            .filter(|p| p.startup_time <= earliest_startup)
                            .min_by_key(|p| (&p.startup_time, &p.machine_id))
                            .map(|p| p.machine_id.clone())
                    };
                    core_state.current_leader = leader.clone();
                                
                    if old_state != new_state {
                        info!(
                            old_state = ?old_state,
                            new_state = ?new_state,
                            "Availability state transition"
                        );
                    }

                    leader
                };

                app_state.force_disconnect_all_clients().await;
                info!(
                    leader = ?leader,
                    our_startup_time = our_startup_time,
                    earliest_startup = earliest_startup,
                    "Leader determined, stepping down"
                );
            }
            
            debug!(
                remote_machine_id = %machine_id,
                startup_time = startup_time,
                "Updated peer information"
            );
        }
        
        PeerMessage::FullSyncRequest { machine_id } => {
            // Any peer can respond to sync requests
            info!(requesting_machine = %machine_id, "Received full sync request, preparing snapshot");
            
            // Take a snapshot and send it
            let snapshot = {
                let store_guard = app_state.store.read().await;
                store_guard.inner().take_snapshot()
            };
            
            info!(
                requesting_machine = %machine_id, 
                snapshot_entities = snapshot.entities.len(),
                "Snapshot prepared, attempting to serialize and send"
            );
            
            let response = PeerMessage::FullSyncResponse { snapshot };
            
            // Use binary serialization for the snapshot since it contains complex key types
            match bincode::serialize(&response) {
                Ok(response_binary) => {
                    info!(
                        requesting_machine = %machine_id,
                        response_size = response_binary.len(),
                        "Snapshot serialized to binary, sending response directly to requesting peer"
                    );
                    
                    // Send the response directly through the inbound connection
                    let message = Message::Binary(response_binary);
                    if let Err(e) = ws_sender.send(message).await {
                        error!(
                            error = %e,
                            requesting_machine = %machine_id,
                            "Failed to send FullSyncResponse to requesting peer"
                        );
                    } else {
                        info!(
                            requesting_machine = %machine_id,
                            "Successfully sent FullSyncResponse to requesting peer"
                        );
                    }
                }
                Err(e) => {
                    error!(
                        error = %e,
                        requesting_machine = %machine_id,
                        "Failed to serialize full sync response"
                    );
                }
            }
        }
        
        PeerMessage::FullSyncResponse { snapshot } => {
            // Apply the snapshot from the leader
            info!("Received full sync response, applying snapshot");
            
            // Apply the snapshot to the store
            {
                let mut store_guard = app_state.store.write().await;
                store_guard.inner_mut().disable_notifications();
                store_guard.inner_mut().restore_snapshot(snapshot.clone());
                store_guard.inner_mut().enable_notifications();
            }
            
            // Update core state
            {
                let mut core_state = app_state.core_state.write().await;
                core_state.is_fully_synced = true;
                core_state.full_sync_request_pending = false; // Reset pending flag since we got a response
                core_state.availability_state = AvailabilityState::Available;
                core_state.became_unavailable_at = None; // Clear timestamp when becoming available
            }
            
            // Save the snapshot to disk for persistence
            match save_snapshot(&snapshot, app_state.clone()).await {
                Ok(snapshot_counter) => {
                    info!(
                        snapshot_counter = snapshot_counter,
                        "Snapshot saved to disk during full sync"
                    );
                    
                    // Write a snapshot marker to the WAL to indicate the sync point
                    // This helps during replay to know that the state was synced at this point
                    let machine_id = {
                        let core = app_state.core_state.read().await;
                        core.config.machine.clone()
                    };
                    
                    let snapshot_request = qlib_rs::Request::Snapshot {
                        snapshot_counter,
                        timestamp: Some(now()),
                        originator: Some(machine_id),
                    };
                    
                    if let Err(e) = write_request_to_wal_direct(&snapshot_request, app_state.clone()).await {
                        error!(
                            error = %e,
                            snapshot_counter = snapshot_counter,
                            "Failed to write snapshot marker to WAL"
                        );
                    }

                    match reinit_caches(app_state.clone()).await {
                        Ok(()) => {
                            info!("Caches reinitialized successfully");
                        }
                        Err(e) => {
                            error!(error = %e, "Failed to reinitialize caches");
                        }
                    }
                }
                Err(e) => {
                    error!(error = %e, "Failed to save snapshot during full sync");
                }
            }
            
            info!("Successfully applied full sync snapshot, instance is now fully synchronized");
        }
        
        PeerMessage::SyncRequest { requests } => {
            // Handle data synchronization (existing functionality)
            let our_machine_id = {
                let core = app_state.core_state.read().await;
                core.config.machine.clone()
            };
            
            // Filter requests to only include those with valid originators (different from our machine)
            let mut requests_to_apply: Vec<_> = requests.into_iter()
                .filter(|request| {
                    if let Some(originator) = request.originator() {
                        *originator != our_machine_id
                    } else {
                        false
                    }
                })
                .filter(|request| {
                    match request {
                        qlib_rs::Request::Snapshot { .. } => false, // Ignore snapshot requests from peers
                        _ => true,
                    }
                })
                .collect();
            
            if !requests_to_apply.is_empty() {
                let mut store_guard = app_state.store.write().await;
                if let Err(e) = store_guard.perform_mut(&mut requests_to_apply).await {
                    error!(
                        error = %e,
                        request_count = requests_to_apply.len(),
                        "Failed to apply sync requests from peer"
                    );
                } else {
                    debug!(
                        request_count = requests_to_apply.len(),
                        "Successfully applied sync requests from peer"
                    );
                }
            } else {
                debug!("No valid sync requests to apply from peer");
            }
        }
    }
}

/// Handle a single outbound peer WebSocket connection
#[instrument(skip(app_state), fields(peer_addr = %peer_addr))]
async fn handle_outbound_peer_connection(peer_addr: &str, app_state: Arc<AppState>) -> Result<()> {
    info!("Attempting to connect to outbound peer");
    
    let ws_url = format!("ws://{}", peer_addr);
    let (ws_stream, _response) = connect_async(&ws_url).await?;
    info!("Successfully connected to outbound peer");
    
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    
    // Create a channel for sending messages to this peer
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();
    
    // Store the sender in the connected_outbound_peers HashMap
    {
        let mut connections = app_state.connections.write().await;
        connections.connected_outbound_peers.insert(peer_addr.to_string(), tx);
    }
    
    // Send initial startup message to announce ourselves
    let (machine, startup_time) = {
        let core = app_state.core_state.read().await;
        (core.config.machine.clone(), core.startup_time)
    };
    let startup = PeerMessage::Startup {
        machine_id: machine.clone(),
        startup_time,
    };
    if let Ok(startup_json) = serde_json::to_string(&startup) {
        if let Err(e) = ws_sender.send(Message::Text(startup_json)).await {
            error!(
                error = %e,
                machine_id = %machine,
                startup_time = startup_time,
                "Failed to send initial startup message"
            );
        } else {
            debug!(
                machine_id = %machine,
                startup_time = startup_time,
                "Sent startup message to peer"
            );
        }
    }
    
    // Spawn a task to handle outgoing messages
    let peer_addr_clone = peer_addr.to_string();
    let outgoing_task = tokio::spawn(async move {
        while let Some(message) = rx.recv().await {
            if let Err(e) = ws_sender.send(message).await {
                error!(
                    error = %e,
                    peer_addr = %peer_addr_clone,
                    "Failed to send message to peer"
                );
                break;
            }
        }
    });
    
    // Handle incoming messages from peer (process FullSyncResponse, ignore others)
    while let Some(msg) = ws_receiver.next().await {
        match msg {
            Ok(Message::Text(_text)) => {
                // Ignore all text messages - message handling is done in handle_inbound_peer_connection
                debug!("Ignoring received text message from outbound peer (handled via inbound connection)");
            }
            Ok(Message::Binary(data)) => {
                // Try to deserialize as PeerMessage to check if it's a FullSyncResponse
                match bincode::deserialize::<PeerMessage>(&data) {
                    Ok(PeerMessage::FullSyncResponse { snapshot }) => {
                        info!("Received FullSyncResponse via outbound connection, applying snapshot");
                        
                        // Apply the snapshot to the store
                        {
                            let mut store_guard = app_state.store.write().await;
                            store_guard.inner_mut().disable_notifications();
                            store_guard.inner_mut().restore_snapshot(snapshot.clone());
                            store_guard.inner_mut().enable_notifications();
                        }
                        
                        // Update core state
                        {
                            let mut core_state = app_state.core_state.write().await;
                            core_state.is_fully_synced = true;
                            core_state.full_sync_request_pending = false;
                            core_state.availability_state = AvailabilityState::Available;
                            core_state.became_unavailable_at = None;
                        }
                        
                        // Save the snapshot to disk for persistence
                        match save_snapshot(&snapshot, app_state.clone()).await {
                            Ok(snapshot_counter) => {
                                info!(
                                    snapshot_counter = snapshot_counter,
                                    "Saved snapshot to disk after full sync"
                                );
                                
                                // Write a snapshot marker to the WAL
                                let machine_id = {
                                    let core = app_state.core_state.read().await;
                                    core.config.machine.clone()
                                };
                                
                                let snapshot_request = qlib_rs::Request::Snapshot {
                                    snapshot_counter,
                                    timestamp: Some(time::OffsetDateTime::now_utc()),
                                    originator: Some(machine_id.clone()),
                                };
                                
                                if let Err(e) = write_request_to_wal_direct(&snapshot_request, app_state.clone()).await {
                                    error!(error = %e, "Failed to write snapshot marker to WAL");
                                }

                                match reinit_caches(app_state.clone()).await {
                                    Ok(_) => info!("Caches reinitialized after full sync"),
                                    Err(e) => error!(error = %e, "Failed to reinitialize caches after full sync"),
                                }
                            }
                            Err(e) => {
                                error!(error = %e, "Failed to save snapshot during full sync");
                            }
                        }
                        
                        info!("Successfully applied full sync snapshot via outbound connection");
                    }
                    Ok(_) => {
                        // Other peer messages - ignore (handled via inbound connection)
                        debug!("Ignoring received binary peer message from outbound peer (handled via inbound connection)");
                    }
                    Err(_) => {
                        // Not a peer message - ignore
                        debug!("Ignoring received binary data from outbound peer (not a peer message)");
                    }
                }
            }
            Ok(Message::Ping(_payload)) => {
                // Respond to pings to keep connection alive
                debug!("Received ping from outbound peer");
                // Note: We can't easily send pong here since ws_sender is in the outgoing task
                // The ping/pong will be handled by the WebSocket implementation
            }
            Ok(Message::Pong(_)) => {
                debug!("Received pong from outbound peer");
            }
            Ok(Message::Close(_)) => {
                info!("Outbound peer closed connection gracefully");
                break;
            }
            Ok(Message::Frame(_)) => {
                // Handle raw frames if needed - typically not used directly
                debug!("Received raw frame from outbound peer");
            }
            Err(e) => {
                error!(error = %e, "WebSocket error with outbound peer");
                break;
            }
        }
    }
    
    outgoing_task.abort();
    
    info!("Outbound peer connection terminated");
    Ok(())
}

/// Handle a single client WebSocket connection that uses StoreProxy protocol
#[instrument(skip(stream, app_state), fields(client_addr = %client_addr))]
async fn handle_client_connection(stream: TcpStream, client_addr: std::net::SocketAddr, app_state: Arc<AppState>) -> Result<()> {
    info!("Accepting client connection");
    
    // Check if the application is available for client connections
    {
        let state_snapshot = app_state.get_state_snapshot().await;
        if !state_snapshot.is_available() {
            info!("Rejecting client connection - application unavailable");
            // Don't accept the WebSocket connection, just return
            return Ok(());
        }
    }
    
    let ws_stream = accept_async(stream).await?;
    debug!("WebSocket handshake completed");
    
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    
    // Wait for authentication message as the first message
    let auth_timeout = tokio::time::timeout(Duration::from_secs(10), ws_receiver.next()).await;
    
    let first_message = match auth_timeout {
        Ok(Some(Ok(Message::Text(text)))) => text,
        Ok(Some(Ok(Message::Close(_)))) => {
            info!("Client closed connection before authentication");
            return Ok(());
        }
        Ok(Some(Err(e))) => {
            error!(error = %e, "WebSocket error from client during authentication");
            return Ok(());
        }
        Ok(None) => {
            info!("Client closed connection before authentication");
            return Ok(());
        }
        Ok(Some(Ok(_))) => {
            error!("Client sent non-text message during authentication");
            let _ = ws_sender.close().await;
            return Ok(());
        }
        Err(_) => {
            info!("Client authentication timeout");
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
            error!("Client first message was not authentication");
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
            error!(error = %e, "Failed to serialize authentication response");
            let _ = ws_sender.close().await;
            return Ok(());
        }
    };
    
    if let Err(e) = ws_sender.send(Message::Text(auth_response_text)).await {
        error!(error = %e, "Failed to send authentication response to client");
        return Ok(());
    }
    
    // Check if authentication was successful
    let is_authenticated = {
        let connections = app_state.connections.read().await;
        connections.authenticated_clients.contains_key(&client_addr.to_string())
    };
    
    if !is_authenticated {
        info!("Client authentication failed, closing connection");
        let _ = ws_sender.close().await;
        return Ok(());
    }
    
    info!("Client authenticated successfully");
    
    // Now proceed with normal client handling
    // Create a channel for sending messages to this client
    let (tx, rx) = mpsc::unbounded_channel::<Message>();
    
    // Create a notification channel for this client
    let (notification_sender, mut notification_receiver) = notification_channel();
    
    // Store the sender and notification sender in the connected_clients HashMap
    {
        let mut connections = app_state.connections.write().await;
        connections.connected_clients.insert(client_addr.to_string(), tx.clone());
        connections.client_notification_senders.insert(client_addr.to_string(), notification_sender);
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
                    error!(
                        client_addr = %client_addr_clone_notif,
                        error = %e,
                        "Failed to send notification to client"
                    );
                    break;
                }
            } else {
                error!(
                    client_addr = %client_addr_clone_notif,
                    "Failed to serialize notification for client"
                );
            }
        }
        debug!(
            client_addr = %client_addr_clone_notif,
            "Notification task ended for client"
        );
    });
    
    // Spawn a task to handle outgoing messages to the client
    let client_addr_clone = client_addr.to_string();
    let app_state_clone = Arc::clone(&app_state);
    let outgoing_task = tokio::spawn(async move {
        let mut ws_sender = ws_sender;
        let mut rx = rx;
        
        while let Some(message) = rx.recv().await {
            if let Err(e) = ws_sender.send(message).await {
                error!(
                    client_addr = %client_addr_clone,
                    error = %e,
                    "Failed to send message to client"
                );
                break;
            }
        }
        
        // Remove client from connected_clients when outgoing task ends
        let mut connections = app_state_clone.connections.write().await;
        connections.connected_clients.remove(&client_addr_clone);
        connections.authenticated_clients.remove(&client_addr_clone);
        connections.client_notification_senders.remove(&client_addr_clone);
    });
    
    // Handle incoming messages from client (after successful authentication)
    while let Some(msg) = ws_receiver.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                debug!(
                    message_length = text.len(),
                    "Received text message from client"
                );
                
                // Parse the StoreMessage
                match serde_json::from_str::<StoreMessage>(&text) {
                    Ok(store_msg) => {
                        // Process the message and generate response
                        let response_msg = process_store_message(store_msg, &app_state, Some(client_addr.to_string())).await;
                        
                        // Send response back to client using the channel
                        let response_text = match serde_json::to_string(&response_msg) {
                            Ok(text) => text,
                            Err(e) => {
                                error!(error = %e, "Failed to serialize response");
                                continue;
                            }
                        };
                        
                        if let Err(e) = tx.send(Message::Text(response_text)) {
                            error!(error = %e, "Failed to send response to client");
                            break;
                        }
                    }
                    Err(e) => {
                        error!(
                            error = %e,
                            message_length = text.len(),
                            "Failed to parse StoreMessage from client"
                        );
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
                debug!("Received binary data from client");
                // For now, we only handle text messages for StoreProxy protocol
            }
            Ok(Message::Ping(payload)) => {
                debug!("Received ping from client");
                if let Err(e) = tx.send(Message::Pong(payload)) {
                    error!(error = %e, "Failed to send pong to client");
                    break;
                }
            }
            Ok(Message::Pong(_)) => {
                debug!("Received pong from client");
            }
            Ok(Message::Close(_)) => {
                info!("Client closed connection gracefully");
                break;
            }
            Ok(Message::Frame(_)) => {
                debug!("Received raw frame from client");
            }
            Err(e) => {
                error!(error = %e, "WebSocket error with client");
                break;
            }
        }
    }
    
    // Remove client from connected_clients and cleanup notifications when connection ends
    {
        let mut connections = app_state.connections.write().await;
        let client_addr_string = client_addr.to_string();
        connections.connected_clients.remove(&client_addr_string);
        
        // Get the notification sender and configurations for this client
        let notification_sender = connections.client_notification_senders.remove(&client_addr_string);
        let client_configs = connections.client_notification_configs.remove(&client_addr_string);
        
        // Remove authentication state for this client
        connections.authenticated_clients.remove(&client_addr_string);
        
        // Unregister all notifications for this client from the store
        if let Some(configs) = client_configs {
            if let Some(sender) = notification_sender {
                {
                    let mut store_guard = app_state.store.write().await;
                    
                    for config in configs {
                        let removed = store_guard.unregister_notification(&config, &sender).await;
                        if removed {
                            debug!(
                                client_addr = %client_addr_string,
                                config = ?config,
                                "Cleaned up notification config for disconnected client"
                            );
                        } else {
                            warn!(
                                client_addr = %client_addr_string,
                                config = ?config,
                                "Failed to clean up notification config for disconnected client"
                            );
                        }
                    }
                }

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
    
    info!("Client connection terminated");
    Ok(())
}

/// Process a StoreMessage and generate the appropriate response
async fn process_store_message(message: StoreMessage, app_state: &Arc<AppState>, client_addr: Option<String>) -> StoreMessage {
    // Extract client notification sender if needed (before accessing store)
    let client_notification_sender = if let Some(ref addr) = client_addr {
        let connections = app_state.connections.read().await;
        connections.client_notification_senders.get(addr).cloned()
    } else {
        None
    };

    match message {
        StoreMessage::Authenticate { id, subject_name, credential } => {
            // Perform authentication
            let auth_config = AuthConfig::default();
            let store = app_state.store.clone();
            let mut store_guard = store.write().await;
            
            match authenticate_subject(&mut store_guard, &subject_name, &credential, &auth_config).await {
                Ok(subject_id) => {
                    // Store authentication state
                    if let Some(ref addr) = client_addr {
                        let mut connections = app_state.connections.write().await;
                        connections.authenticated_clients.insert(addr.clone(), subject_id.clone());
                    }
                    
                    // Create authentication result
                    let auth_result = AuthenticationResult {
                        subject_id: subject_id.clone(),
                        subject_type: subject_id.get_type().to_string(),
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
            let client_id = if let Some(ref addr) = client_addr {
                let connections = app_state.connections.read().await;
                connections.authenticated_clients.get(addr).cloned()
            } else {
                None // No client address means not authenticated
            };
            
            if client_id.is_none() {
                return StoreMessage::Error {
                    id: "unknown".to_string(),
                    error: "Authentication required".to_string(),
                };
            }
            let client_id = client_id.unwrap();
            
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
                    match app_state.store.read().await.get_entity_schema(&entity_type).await {
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
                    match app_state.store.read().await.get_complete_entity_schema(&entity_type).await {
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
                    match app_state.store.read().await.get_field_schema(&entity_type, &field_type).await {
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
                    let exists = app_state.store.read().await.entity_exists(&entity_id).await;
                    StoreMessage::EntityExistsResponse {
                        id,
                        response: exists,
                    }
                }
                
                StoreMessage::FieldExists { id, entity_type, field_type } => {
                    let exists = app_state.store.read().await.field_exists(&entity_type, &field_type).await;
                    StoreMessage::FieldExistsResponse {
                        id,
                        response: exists,
                    }
                }
                
                StoreMessage::Perform { id, mut requests } => {
                    let permission_cache_guard = app_state.permission_cache.read().await;
                    let permission_cache = permission_cache_guard.as_ref();

                    if let Some(permission_cache) = permission_cache {
                        // Check authorization for each request
                        for request in &requests {
                                if let Some(entity_id) = request.entity_id() {
                                    if let Some(field_type) = request.field_type() {
                                        let mut cel_executor = app_state.cel_executor.write().await;
                                        match get_scope(
                                            app_state.store.clone(),
                                            &mut cel_executor,
                                            permission_cache,
                                            &client_id,
                                            entity_id,
                                            field_type,
                                        ).await {
                                            Ok(scope) => {
                                                if scope == AuthorizationScope::None {
                                                    return StoreMessage::PerformResponse {
                                                        id,
                                                        response: Err(format!(
                                                            "Access denied: Subject {} is not authorized to access {} on entity {}",
                                                        client_id,
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
                                                            client_id,
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
                            response: Err("Authorization cache not available".to_string()),
                        };
                    }

                    let machine = {
                        let core = app_state.core_state.read().await;
                        core.config.machine.clone()
                    };

                    requests.iter_mut().for_each(|req| {
                        req.try_set_originator(machine.clone());
                        req.try_set_writer_id(client_id.clone());
                    });

                    match app_state.store.write().await.perform_mut(&mut requests).await {
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
                                .store
                                .write().await
                                .register_notification(config.clone(), notification_sender.clone()).await {
                                Ok(()) => {
                                    let mut connections = app_state
                                        .connections
                                        .write().await;
                                    connections
                                        .client_notification_configs
                                        .entry(client_addr.clone())
                                        .or_insert_with(HashSet::new)
                                        .insert(config.clone());
                                    
                                    debug!(
                                        client_addr = %client_addr,
                                        config = ?config,
                                        "Registered notification for client"
                                    );
                                    StoreMessage::RegisterNotificationResponse {
                                        id,
                                        response: Ok(()),
                                    }
                                }
                                Err(e) => {
                                    error!(
                                        client_addr = %client_addr,
                                        error = ?e,
                                        "Failed to register notification for client"
                                    );
                                    StoreMessage::RegisterNotificationResponse {
                                        id,
                                        response: Err(format!("Failed to register notification: {:?}", e)),
                                    }
                                }
                            }
                        } else {
                            error!(
                                client_addr = %client_addr,
                                "No notification sender found for client"
                            );
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
                                .store
                                .write().await
                                .unregister_notification(&config, notification_sender).await;
                            
                            // Remove from client's tracked configs if successfully unregistered
                            if removed {
                                let mut connections = app_state
                                    .connections
                                    .write().await;
                                if let Some(client_configs) = connections
                                    .client_notification_configs
                                    .get_mut(client_addr) {
                                    client_configs.remove(&config);
                                }
                            }
                            
                            debug!(
                                client_addr = %client_addr,
                                config = ?config,
                                removed = removed,
                                "Unregistered notification for client"
                            );
                            StoreMessage::UnregisterNotificationResponse {
                                id,
                                response: removed,
                            }
                        } else {
                            error!(
                                client_addr = %client_addr,
                                "No notification sender found for client"
                            );
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
                    warn!(
                        message_id = %id,
                        error_message = %error,
                        "Received error message from client"
                    );
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
#[instrument(skip(app_state))]
async fn start_client_server(app_state: Arc<AppState>) -> Result<()> {
    let addr = {
        let core_state = app_state.core_state.read().await;
        format!("0.0.0.0:{}", core_state.config.client_port)
    };
    
    let listener = TcpListener::bind(&addr).await?;
    info!(bind_address = %addr, "Client WebSocket server started");
    
    loop {
        match listener.accept().await {
            Ok((stream, client_addr)) => {
                debug!(client_addr = %client_addr, "Accepted new client connection");
                
                let app_state_clone = Arc::clone(&app_state);
                tokio::spawn(async move {
                    if let Err(e) = handle_client_connection(stream, client_addr, app_state_clone).await {
                        error!(
                            error = %e,
                            client_addr = %client_addr,
                            "Error handling client connection"
                        );
                    }
                });
            }
            Err(e) => {
                error!(error = %e, "Failed to accept client connection");
                // Continue listening despite individual connection errors
            }
        }
    }
}

/// Start the peer WebSocket server task
#[instrument(skip(app_state))]
async fn start_inbound_peer_server(app_state: Arc<AppState>) -> Result<()> {
    let addr = {
        let core_state = app_state.core_state.read().await;
        format!("0.0.0.0:{}", core_state.config.peer_port)
    };
    
    let listener = TcpListener::bind(&addr).await?;
    info!(bind_address = %addr, "Peer WebSocket server started");
    
    loop {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                debug!(peer_addr = %peer_addr, "Accepted new peer connection");
                
                let app_state_clone = Arc::clone(&app_state);
                tokio::spawn(async move {
                    if let Err(e) = handle_inbound_peer_connection(stream, peer_addr, app_state_clone).await {
                        error!(
                            error = %e,
                            peer_addr = %peer_addr,
                            "Error handling peer connection"
                        );
                    }
                });
            }
            Err(e) => {
                error!(error = %e, "Failed to accept peer connection");
                // Continue listening despite individual connection errors
            }
        }
    }
}

/// Manage outbound peer connections - connects to configured peers and maintains connections
async fn manage_outbound_peer_connections(app_state: Arc<AppState>) -> Result<()> {
    info!("Starting outbound peer connection manager");
    
    let reconnect_interval = {
        let core_state = app_state.core_state.read().await;
        Duration::from_secs(core_state.config.peer_reconnect_interval_secs)
    };
    
    let mut interval = tokio::time::interval(reconnect_interval);
    
    loop {
        interval.tick().await;
        
        let peers_to_connect = {
            let connections = app_state.connections.read().await;
            let core_state = app_state.core_state.read().await;
            let connected = &connections.connected_outbound_peers;
            core_state.config.peer_addresses.iter()
                .filter(|addr| !connected.contains_key(*addr))
                .cloned()
                .collect::<Vec<_>>()
        };
        
        for peer_addr in peers_to_connect {
            info!(
                peer_addr = %peer_addr,
                "Attempting to connect to unconnected peer"
            );
            
            let peer_addr_clone = peer_addr.clone();
            let app_state_clone = Arc::clone(&app_state);
            
            tokio::spawn(async move {
                // Attempt connection
                if let Err(e) = handle_outbound_peer_connection(&peer_addr_clone, app_state_clone.clone()).await {
                    error!(
                        peer_addr = %peer_addr_clone,
                        error = %e,
                        "Failed to connect to peer"
                    );
                    
                    // Remove from connected set on failure
                    let mut connections = app_state_clone.connections.write().await;
                    connections.connected_outbound_peers.remove(&peer_addr_clone);
                } else {
                    info!(
                        peer_addr = %peer_addr_clone,
                        "Connection to peer ended"
                    );
                    
                    // Remove from connected set when connection ends
                    let mut connections = app_state_clone.connections.write().await;
                    connections.connected_outbound_peers.remove(&peer_addr_clone);
                }
            });
        }
    }
}

/// Consume and process requests from the store's write channel
async fn consume_write_channel(app_state: Arc<AppState>) -> Result<()> {
    info!("Starting write channel consumer");
    
    // Get a clone of the write channel receiver
    let receiver = {
        let store = &app_state.store;
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
            Some(mut requests) => {                
                // Collect all requests that originated from this machine for batch synchronization
                let current_machine = {
                    let core = app_state.core_state.read().await;
                    core.config.machine.clone()
                };

                requests.iter_mut().for_each(|req| req.try_set_originator(current_machine.clone()));
                
                // Write all requests to WAL file - the requests have already been applied to the store
                for request in &requests {
                    if let Err(e) = write_request_to_wal(request, app_state.clone()).await {
                        error!(
                            error = %e,
                            "Failed to write request to WAL"
                        );
                    }
                }
                
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
                    // Send to all connected outbound peers using PeerMessage
                    let peers_to_notify = {
                        let connections = app_state.connections.read().await;
                        connections.connected_outbound_peers.clone()
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
                                    warn!(
                                        peer_addr = %peer_addr,
                                        error = %e,
                                        "Failed to send sync requests to peer"
                                    );
                                } else {
                                    debug!(
                                        peer_addr = %peer_addr,
                                        count = requests_to_sync.len(),
                                        "Sent sync requests to peer"
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            error!(
                                error = %e,
                                "Failed to serialize sync message for peer synchronization"
                            );
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
            info!(
                wal_file = %wal_files[i].display(),
                "Removing old WAL file"
            );
            if let Err(e) = remove_file(&wal_files[i]).await {
                error!(
                    wal_file = %wal_files[i].display(),
                    error = %e,
                    "Failed to remove old WAL file"
                );
            }
        }
    }
    
    Ok(())
}

/// Write a request directly to the WAL file without snapshot logic (to avoid recursion)
async fn write_request_to_wal_direct(request: &qlib_rs::Request, app_state: Arc<AppState>) -> Result<()> {
    let mut wal_state = app_state.wal_state.write().await;
    
    // Serialize the request to JSON
    let serialized = serde_json::to_vec(request)?;
    let serialized_len = serialized.len();
    
    // Ensure we have a WAL file open
    if wal_state.current_wal_file.is_none() {
        // Create WAL directory if it doesn't exist
        let wal_dir = app_state.get_wal_dir().await;
        create_dir_all(&wal_dir).await?;
        
        // Create new WAL file in the wal directory
        let wal_filename = format!("wal_{:010}.log", wal_state.wal_file_counter);
        let wal_path = wal_dir.join(&wal_filename);
        
        info!(
            wal_file = %wal_path.display(),
            "Creating new WAL file for direct write"
        );
        
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)
            .await?;
            
        wal_state.current_wal_file = Some(file);
        wal_state.current_wal_size = 0;
        wal_state.wal_file_counter += 1;
    }
    
    // Write to WAL file
    if let Some(ref mut wal_file) = wal_state.current_wal_file {
        // Write length prefix (4 bytes) followed by the serialized data
        let len_bytes = (serialized_len as u32).to_le_bytes();
        wal_file.write_all(&len_bytes).await?;
        wal_file.write_all(&serialized).await?;
        wal_file.flush().await?;
        
        wal_state.current_wal_size += 4 + serialized_len;
        
        debug!(
            bytes_written = serialized_len + 4,
            "Wrote request to WAL file (direct)"
        );
    }
    
    Ok(())
}

/// Write a request to the WAL file
#[instrument(skip(request, app_state), fields(request_size = ?request))]
async fn write_request_to_wal(request: &qlib_rs::Request, app_state: Arc<AppState>) -> Result<()> {
    // Serialize the request to JSON
    let serialized = serde_json::to_vec(request)?;
    let serialized_len = serialized.len();
    
    let mut wal_state = app_state.wal_state.write().await;
    let core_state = app_state.core_state.read().await;
    
    // Check if we need to create a new WAL file
    let should_create_new_file = wal_state.current_wal_file.is_none() || 
       wal_state.current_wal_size + serialized_len > core_state.config.wal_max_file_size * 1024 * 1024;
    
    if should_create_new_file {
        // Create WAL directory if it doesn't exist
        let wal_dir = app_state.get_wal_dir().await;
        create_dir_all(&wal_dir).await?;
        
        // Create new WAL file in the wal directory
        let wal_filename = format!("wal_{:010}.log", wal_state.wal_file_counter);
        let wal_path = wal_dir.join(&wal_filename);
        
        info!(
            wal_file = %wal_path.display(),
            wal_counter = wal_state.wal_file_counter,
            current_size = wal_state.current_wal_size,
            max_size = core_state.config.wal_max_file_size * 1024 * 1024,
            "Creating new WAL file"
        );
        
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)
            .await?;
            
        wal_state.current_wal_file = Some(file);
        wal_state.current_wal_size = 0;
        wal_state.wal_file_counter += 1;
        wal_state.wal_files_since_snapshot += 1;
        
        // Check if we should take a snapshot based on WAL rollovers
        let should_snapshot = wal_state.wal_files_since_snapshot >= core_state.config.snapshot_wal_interval;
        
        if should_snapshot {
            info!(
                wal_files_count = wal_state.wal_files_since_snapshot,
                "Taking snapshot after WAL file rollovers"
            );
            
            // Take a snapshot
            let snapshot = {
                let store_guard = app_state.store.read().await;
                store_guard.inner().take_snapshot()
            };
            
            // Save the snapshot to disk
            match save_snapshot(&snapshot, app_state.clone()).await {
                Ok(snapshot_counter) => {
                    // Reset the WAL files counter
                    {
                        let mut wal_state = app_state.wal_state.write().await;
                        wal_state.wal_files_since_snapshot = 0;
                        info!("Snapshot saved successfully after WAL rollover");
                    }

                    // Write a snapshot marker to the WAL to indicate the snapshot point
                    let machine_id = {
                        let core = app_state.core_state.read().await;
                        core.config.machine.clone()
                    };
                    
                    let snapshot_request = qlib_rs::Request::Snapshot {
                        snapshot_counter,
                        timestamp: Some(now()),
                        originator: Some(machine_id),
                    };
                    
                    if let Err(e) = write_request_to_wal_direct(&snapshot_request, app_state.clone()).await {
                        error!(
                            error = %e,
                            "Failed to write snapshot marker to WAL"
                        );
                    }
                }
                Err(e) => {
                    error!(
                        error = %e,
                        "Failed to save snapshot after WAL rollover"
                    );
                }
            }
            
            // Re-acquire the state locks for cleanup
            let _wal_state = app_state.wal_state.read().await;
            let _core_state = app_state.core_state.read().await;
        }
        
        // Clean up old WAL files if we exceed the maximum
        let max_files = core_state.config.wal_max_files;
        let wal_dir = app_state.get_wal_dir().await;
        if let Err(e) = cleanup_old_wal_files(&wal_dir, max_files).await {
            error!(
                error = %e,
                "Failed to clean up old WAL files"
            );
        }
    }
    
    // Write to WAL file
    if let Some(ref mut wal_file) = wal_state.current_wal_file {
        // Write length prefix (4 bytes) followed by the serialized data
        let len_bytes = (serialized_len as u32).to_le_bytes();
        wal_file.write_all(&len_bytes).await?;
        wal_file.write_all(&serialized).await?;
        wal_file.flush().await?;
        
        wal_state.current_wal_size += 4 + serialized_len;
        
        debug!(
            bytes_written = serialized_len + 4,
            "Wrote request to WAL file"
        );
    }
    
    Ok(())
}

/// Save a snapshot to disk and return the snapshot counter that was used
#[instrument(skip(snapshot, app_state))]
async fn save_snapshot(snapshot: &qlib_rs::Snapshot, app_state: Arc<AppState>) -> Result<u64> {
    let mut wal_state = app_state.wal_state.write().await;
    
    // Create snapshots directory if it doesn't exist
    let snapshot_dir = app_state.get_snapshots_dir().await;
    create_dir_all(&snapshot_dir).await?;
    
    // Increment the counter before creating the filename
    let current_snapshot_counter = wal_state.snapshot_file_counter;
    wal_state.snapshot_file_counter += 1;
    
    // Create snapshot filename with the current counter
    let snapshot_filename = format!("snapshot_{:010}.bin", current_snapshot_counter);
    let snapshot_path = snapshot_dir.join(&snapshot_filename);
    
    info!(
        snapshot_file = %snapshot_path.display(),
        snapshot_counter = current_snapshot_counter,
        "Saving snapshot"
    );
    
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
    
    info!(
        snapshot_size_bytes = serialized.len(),
        snapshot_counter = current_snapshot_counter,
        "Snapshot saved successfully"
    );
    
    // Clean up old snapshots using the configured limit
    let max_files = {
        let core = app_state.core_state.read().await;
        core.config.snapshot_max_files
    };
    if let Err(e) = cleanup_old_snapshots(&snapshot_dir, max_files).await {
        error!(
            error = %e,
            "Failed to clean up old snapshots"
        );
    }
    
    Ok(current_snapshot_counter)
}

/// Load the latest snapshot from disk and return it along with the snapshot counter
async fn load_latest_snapshot(app_state: Arc<AppState>) -> Result<Option<(qlib_rs::Snapshot, u64)>> {
    let snapshot_dir = app_state.get_snapshots_dir().await;
    
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
    info!(
        snapshot_file = %latest_snapshot_path.display(),
        snapshot_counter = latest_counter,
        "Loading snapshot"
    );
    
    let mut file = File::open(latest_snapshot_path).await?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).await?;
    
    match bincode::deserialize(&buffer) {
        Ok(snapshot) => {
            info!("Snapshot loaded successfully");
            Ok(Some((snapshot, *latest_counter)))
        }
        Err(e) => {
            error!(
                error = %e,
                "Failed to deserialize snapshot"
            );
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
            info!(
                snapshot_file = %snapshot_files[i].display(),
                "Removing old snapshot file"
            );
            if let Err(e) = remove_file(&snapshot_files[i]).await {
                error!(
                    snapshot_file = %snapshot_files[i].display(),
                    error = %e,
                    "Failed to remove old snapshot file"
                );
            }
        }
    }
    
    Ok(())
}

/// Replay WAL files to restore store state
/// This function scans all WAL files to find the most recent snapshot marker
/// and starts replaying from that point
async fn replay_wal_files(app_state: Arc<AppState>) -> Result<()> {
    let wal_dir = app_state.get_wal_dir().await;
    
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
    
    // Find the most recent snapshot marker across all WAL files
    let mut most_recent_snapshot: Option<(PathBuf, u64, usize)> = None; // (file_path, wal_counter, offset_after_snapshot)
    
    info!(
        wal_files_count = wal_files.len(),
        "Scanning WAL files to find the most recent snapshot marker"
    );
    
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
        let mut store = app_state.store.write().await;
        store.inner_mut().disable_notifications();
        info!("Notifications disabled for WAL replay");
    }
    
    if let Some((snapshot_wal_file, snapshot_counter, snapshot_offset)) = most_recent_snapshot {
        info!(
            wal_file = %snapshot_wal_file.display(),
            wal_counter = snapshot_counter,
            offset = snapshot_offset,
            "Starting replay from WAL file"
        );
        
        // First, replay the partial WAL file that contains the snapshot (from the offset)
        if let Err(e) = replay_wal_file_from_offset(&snapshot_wal_file, snapshot_offset, app_state.clone()).await {
            error!(
                wal_file = %snapshot_wal_file.display(),
                offset = snapshot_offset,
                error = %e,
                "Failed to replay WAL file from offset"
            );
        }
        
        // Then replay all subsequent WAL files completely
        for (wal_file, counter) in &wal_files {
            if counter > &snapshot_counter {
                info!(
                    wal_file = %wal_file.display(),
                    wal_counter = counter,
                    "Replaying complete WAL file"
                );
                if let Err(e) = replay_single_wal_file(wal_file, app_state.clone()).await {
                    error!(
                        wal_file = %wal_file.display(),
                        error = %e,
                        "Failed to replay WAL file"
                    );
                }
            }
        }
    } else {
        info!(
            wal_files_count = wal_files.len(),
            "No snapshot markers found in WAL files, replaying all files completely"
        );
        
        // No snapshot markers found, replay all WAL files completely
        for (wal_file, counter) in &wal_files {
            info!(
                wal_file = %wal_file.display(),
                wal_counter = counter,
                "Replaying WAL file"
            );
            if let Err(e) = replay_single_wal_file(wal_file, app_state.clone()).await {
                error!(
                    wal_file = %wal_file.display(),
                    error = %e,
                    "Failed to replay WAL file"
                );
            }
        }
    }
    
    // Re-enable notifications after WAL replay
    {
        let mut store = app_state.store.write().await;
        store.inner_mut().enable_notifications();
        info!("Notifications re-enabled after WAL replay");
    }
    
    info!("WAL replay completed");
    Ok(())
}

/// Apply a single WAL request from request data
async fn apply_wal_request(request_data: &[u8], app_state: &Arc<AppState>, requests_processed: &mut usize) -> Result<()> {
    match serde_json::from_slice::<qlib_rs::Request>(request_data) {
        Ok(request) => {
            // Skip snapshot requests during replay (they are just markers)
            if matches!(request, qlib_rs::Request::Snapshot { .. }) {
                debug!("Skipping snapshot marker during replay");
                return Ok(());
            }
            
            let mut store = app_state.store.write().await;
            
            let mut requests = vec![request];
            if let Err(e) = store.perform_mut(&mut requests).await {
                return Err(anyhow::anyhow!("Failed to apply request during WAL replay: {}", e));
            } else {
                *requests_processed += 1;
            }
            
            Ok(())
        }
        Err(e) => {
            Err(anyhow::anyhow!("Failed to deserialize request: {}", e))
        }
    }
}

/// Replay a single WAL file completely from beginning to end
/// Skips snapshot markers but processes all other requests
async fn replay_single_wal_file(wal_path: &PathBuf, app_state: Arc<AppState>) -> Result<()> {
    let mut file = File::open(wal_path).await?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).await?;
    
    let mut requests_processed = 0;
    let mut offset = 0;
    
    info!(
        wal_file = %wal_path.display(),
        "Replaying complete WAL file"
    );
    
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
            error!(
                offset = offset,
                "Incomplete request in WAL file"
            );
            break;
        }
        
        let request_data = &buffer[offset..offset + len];
        offset += len;
        
        // Apply the request using the helper function
        if let Err(e) = apply_wal_request(request_data, &app_state, &mut requests_processed).await {
            error!(
                offset = offset - len,
                error = %e,
                "Failed to apply request at offset"
            );
        }
    }
    
    info!(
        requests_processed = requests_processed,
        wal_file = %wal_path.display(),
        "Replayed requests from WAL file"
    );
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
            error!(
                offset = offset,
                wal_file = %wal_path.display(),
                "Incomplete request in WAL file"
            );
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
                debug!(
                    offset = offset,
                    wal_file = %wal_path.display(),
                    "Found snapshot marker"
                );
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
async fn replay_wal_file_from_offset(wal_path: &PathBuf, start_offset: usize, app_state: Arc<AppState>) -> Result<()> {
    let mut file = File::open(wal_path).await?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).await?;
    
    let mut requests_processed = 0;
    let mut offset = start_offset;
    
    info!(
        wal_file = %wal_path.display(),
        start_offset = start_offset,
        "Replaying WAL file from offset"
    );
    
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
            error!(
                offset = offset,
                wal_file = %wal_path.display(),
                "Incomplete request in WAL file"
            );
            break;
        }
        
        let request_data = &buffer[offset..offset + len];
        offset += len;
        
        // Deserialize and apply the request
        if let Err(e) = apply_wal_request(request_data, &app_state, &mut requests_processed).await {
            error!(
                offset = offset - len,
                error = %e,
                "Failed to apply request at offset"
            );
        }
    }
    
    info!(
        requests_processed = requests_processed,
        wal_file = %wal_path.display(),
        start_offset = start_offset,
        "Replayed requests from WAL file starting from offset"
    );
    Ok(())
}

/// Determine the next WAL file counter based on existing WAL files
async fn get_next_wal_counter(app_state: Arc<AppState>) -> Result<u64> {
    let wal_dir = app_state.get_wal_dir().await;
    
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
async fn get_next_snapshot_counter(app_state: Arc<AppState>) -> Result<u64> {
    let snapshot_dir = app_state.get_snapshots_dir().await;
    
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
async fn handle_misc_tasks(app_state: Arc<AppState>) -> Result<()> {
    info!("Starting miscellaneous tasks handler (10ms interval)");
    
    let mut interval = tokio::time::interval(Duration::from_millis(10));
    
    loop {
        interval.tick().await;
        
        // Check if we should self-promote to leader when no peers are connected
        {
            let connections = app_state.connections.read().await;
            let core = app_state.core_state.read().await;
            
            // Self-promote to leader if:
            // 1. We're not already the leader
            // 2. No peer addresses are configured OR no outbound peers are connected
            // 3. No peer info is tracked (no inbound peers)
            let should_self_promote = !core.is_leader && 
                (core.config.peer_addresses.is_empty() || connections.connected_outbound_peers.is_empty());
            
            if should_self_promote {
                let peer_info = app_state.peer_info.read().await;
                if peer_info.is_empty() {
                    drop(peer_info);
                    drop(connections);
                    drop(core);
                    
                    info!("No peers connected and no peer info available, self-promoting to leader");
                    
                    let mut core_state = app_state.core_state.write().await;
                    let our_machine_id = core_state.config.machine.clone();
                    core_state.is_leader = true;
                    core_state.current_leader = Some(our_machine_id.clone());
                    core_state.availability_state = AvailabilityState::Available;
                    core_state.is_fully_synced = true;
                    
                    info!(
                        machine_id = %our_machine_id,
                        "Self-promoted to leader due to no peer connections"
                    );
                }
            }
        }

        // Check if we need to send a full sync request after grace period
        let (should_send_full_sync, is_leader) = {
            let core = app_state.core_state.read().await;
            
            // Only check if we're unavailable, not the leader, not fully synced, and haven't sent a request yet
            if matches!(core.availability_state, AvailabilityState::Unavailable) &&
               !core.is_leader &&
               !core.is_fully_synced &&
               !core.full_sync_request_pending {
                
                if let Some(became_unavailable_at) = core.became_unavailable_at {
                    let current_time = time::OffsetDateTime::now_utc().unix_timestamp() as u64;
                    
                    let grace_period_secs = core.config.full_sync_grace_period_secs;
                    let elapsed = current_time.saturating_sub(became_unavailable_at);
                    
                    if elapsed >= grace_period_secs {
                        // Grace period has expired, check if we have a known leader
                        (core.current_leader.clone(), core.is_leader)
                    } else {
                        (None, core.is_leader)
                    }
                } else {
                    (None, core.is_leader)
                }
            } else {
                (None, core.is_leader)
            }
        };
        
        if let Some(leader_machine_id) = should_send_full_sync {
            info!(
                leader_machine_id = %leader_machine_id,
                "Grace period expired, sending FullSyncRequest to leader"
            );
            
            // Mark that we're sending a request to avoid duplicates
            {
                let mut core = app_state.core_state.write().await;
                core.full_sync_request_pending = true;
            }
            
            // Send FullSyncRequest to the leader through any connected outbound peer
            let machine_id = {
                let core = app_state.core_state.read().await;
                core.config.machine.clone()
            };
            
            let full_sync_request = PeerMessage::FullSyncRequest {
                machine_id: machine_id.clone(),
            };
            
            if let Ok(request_json) = serde_json::to_string(&full_sync_request) {
                let message = Message::Text(request_json);
                
                // Try to send to any connected outbound peer
                let sent = {
                    let connections = app_state.connections.read().await;
                    let mut sent = false;
                    
                    for (peer_addr, sender) in &connections.connected_outbound_peers {
                        if let Err(e) = sender.send(message.clone()) {
                            warn!(
                                peer_addr = %peer_addr,
                                error = %e,
                                "Failed to send FullSyncRequest to peer"
                            );
                        } else {
                            info!(
                                peer_addr = %peer_addr,
                                "Sent FullSyncRequest to peer"
                            );
                            sent = true;
                            break; // Only need to send to one peer
                        }
                    }
                    sent
                };
                
                if !sent {
                    warn!(
                        leader_machine_id = %leader_machine_id,
                        "No connected outbound peers available to send FullSyncRequest to leader"
                    );
                    // Reset the pending flag so we can try again later
                    let mut core = app_state.core_state.write().await;
                    core.full_sync_request_pending = false;
                }
            } else {
                error!("Failed to serialize FullSyncRequest");
                let mut core = app_state.core_state.write().await;
                core.full_sync_request_pending = false;
            }
        }

        // Process cache notifications
        {
            let mut permission_cache = app_state.permission_cache.write().await;
            if let Some(cache) = permission_cache.as_mut() {
                cache.process_notifications();
            }
        }

        if is_leader {
            let mut store = app_state.store.write().await;

            // Find us as a candidate
            let me_as_candidate = {
                let machine = {
                    let core = app_state.core_state.read().await;
                    core.config.machine.clone()
                };

                let mut candidates = store.find_entities(
                    &et::candidate(), 
                    Some(format!("Name == 'qcore' && Parent->Name == '{}'", machine))).await?;

                candidates.pop()
            };

            // Update available list and current leader
            {
                let fault_tolerances = store.find_entities(&et::fault_tolerance(), None).await?;
                for ft_entity_id in fault_tolerances {
                    let ft_fields = store.perform_map(&mut vec![
                        sread!(ft_entity_id.clone(), ft::candidate_list()),
                        sread!(ft_entity_id.clone(), ft::available_list()),
                        sread!(ft_entity_id.clone(), ft::current_leader())
                    ]).await?;

                    let candidates = ft_fields
                        .get(&ft::candidate_list())
                        .unwrap()
                        .value()
                        .unwrap()
                        .expect_entity_list()?;

                    let mut available = Vec::new();
                    for candidate_id in candidates.iter() {
                        let candidate_fields = store.perform_map(&mut vec![
                            sread!(candidate_id.clone(), ft::make_me()),
                            sread!(candidate_id.clone(), ft::heartbeat()),
                            sread!(candidate_id.clone(), ft::death_detection_timeout()),
                        ]).await?;

                        let heartbeat_time = candidate_fields
                            .get(&ft::heartbeat())
                            .unwrap()
                            .write_time()
                            .unwrap();

                        let make_me = candidate_fields
                            .get(&ft::make_me())
                            .unwrap()
                            .value()
                            .unwrap()
                            .expect_choice()?;

                        let death_detection_timeout_millis = candidate_fields
                            .get(&ft::death_detection_timeout())
                            .unwrap()
                            .value()
                            .unwrap()
                            .expect_int()?;
                        
                        let death_detection_timeout_duration = time::Duration::milliseconds(death_detection_timeout_millis);

                        let desired_availability = match make_me {
                            1 => AvailabilityState::Available,
                            _ => AvailabilityState::Unavailable,
                        };

                        if desired_availability == AvailabilityState::Available && 
                           heartbeat_time + death_detection_timeout_duration > now() {
                            available.push(candidate_id.clone());
                        }
                    }

                    store.perform_mut(&mut vec![
                        swrite!(ft_entity_id.clone(), ft::available_list(), Some(qlib_rs::Value::EntityList(available.clone())), PushCondition::Changes),
                    ]).await?;

                    let mut handle_me_as_candidate = false;
                    if let Some(me_as_candidate) = &me_as_candidate {
                        // If we're not in the candidate list, we can't be leader
                        if candidates.contains(me_as_candidate) {
                            handle_me_as_candidate = true;

                            store.perform_mut(&mut vec![
                                swrite!(ft_entity_id.clone(), ft::current_leader(), sref!(Some(me_as_candidate.clone())), PushCondition::Changes)
                            ]).await?;
                        }
                    }

                    if !handle_me_as_candidate {
                        // Now we must promote an available candidate to leader
                        // if the current leader is no longer available.
                        // Note that we want to promote to the next available leader in the candidate list
                        // rather than the first available candidate.
                        let current_leader = ft_fields
                            .get(&ft::current_leader())
                            .unwrap()
                            .value()
                            .unwrap()
                            .expect_entity_reference()?;

                        if current_leader.is_none() {
                            store.perform_mut(&mut vec![
                                swrite!(ft_entity_id.clone(), ft::current_leader(), sref!(available.first().cloned()), PushCondition::Changes),
                            ]).await?;
                        }
                        else if let Some(current_leader) = current_leader {
                            if !available.contains(&current_leader) {
                                // Find the position of the current leader in the candidate list
                                let current_leader_idx = candidates.iter().position(|c| c.clone() == current_leader.clone());
                                
                                if let Some(current_idx) = current_leader_idx {
                                    // Find the next available candidate after the current leader in the candidate list
                                    let mut next_leader = None;
                                    
                                    // Start searching from the position after the current leader
                                    for i in (current_idx + 1)..candidates.len() {
                                        if available.contains(&candidates[i]) {
                                            next_leader = Some(candidates[i].clone());
                                            break;
                                        }
                                    }
                                    
                                    // If no leader found after current position, wrap around to the beginning
                                    if next_leader.is_none() {
                                        for i in 0..=current_idx {
                                            if available.contains(&candidates[i]) {
                                                next_leader = Some(candidates[i].clone());
                                                break;
                                            }
                                        }
                                    }
                                    
                                    store.perform_mut(&mut vec![
                                        swrite!(ft_entity_id.clone(), ft::current_leader(), sref!(next_leader), PushCondition::Changes),
                                    ]).await?;
                                } else {
                                    // Current leader not found in candidates list, just pick the first available
                                    store.perform_mut(&mut vec![
                                        swrite!(ft_entity_id.clone(), ft::current_leader(), sref!(available.first().cloned()), PushCondition::Changes),
                                    ]).await?;
                                }
                            }
                        }
                    }
                }
            }
        }

        tokio::task::yield_now().await;
    }
}

/// Handle heartbeat writing
async fn handle_heartbeat_writing(app_state: Arc<AppState>) -> Result<()> {
    info!("Starting heartbeat writer");
    
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    
    loop {
        interval.tick().await;
        
        {
            let mut store = app_state.store.write().await;
            let machine = {
                let core = app_state.core_state.read().await;
                core.config.machine.clone()
            };

            let candidates = store.find_entities(
                &et::candidate(), 
                Some(format!("Name == 'qcore' && Parent->Name == '{}'", machine))).await?;

            if let Some(candidate) = candidates.first() {
                store.perform_mut(&mut vec![
                    swrite!(candidate.clone(), ft::heartbeat(), schoice!(0)),
                    swrite!(candidate.clone(), ft::make_me(), schoice!(1), PushCondition::Changes)
                ]).await?;
            }
        }

        tokio::task::yield_now().await;
    }
}

async fn reinit_caches(app_state: Arc<AppState>) -> Result<()> {
    {
        let mut permission_cache = app_state.permission_cache.write().await;

        *permission_cache = Some(Cache::new(
            app_state.store.clone(),
            et::permission(),
            vec![ft::resource_type(), ft::resource_field()],
            vec![ft::scope(), ft::condition()]
        ).await?);
    }

    {
        let mut store = app_state.store.write().await;
        
        let me_as_candidate = {
            let machine = {
                let core = app_state.core_state.read().await;
                core.config.machine.clone()
            };

            let mut candidates = store.find_entities(
                &et::candidate(), 
                Some(format!("Name == 'qcore' && Parent->Name == '{}'", machine))).await?;

            candidates.pop()
        };

        if let Some(candidate_id) = &me_as_candidate {
            store.inner_mut().default_writer_id = Some(candidate_id.clone());
        } else {
            store.inner_mut().default_writer_id = None;
        }
    }

    Ok(())
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

    let machine_id = &config.machine;
    let peer_port = config.peer_port;
    let client_port = config.client_port;
    
    info!(
        machine_id = %machine_id,
        peer_port = peer_port,
        client_port = client_port,
        data_dir = %config.data_dir,
        "Starting QCore service"
    );

    // Create shared application state
    let app_state = Arc::new(AppState::new(config).await?);

    // Initialize the WAL file counter based on existing files
    {
        let next_wal_counter = get_next_wal_counter(app_state.clone()).await?;
        let mut wal_state = app_state.wal_state.write().await;
        wal_state.wal_file_counter = next_wal_counter;
        info!(
            wal_counter = next_wal_counter,
            "Initialized WAL file counter"
        );
    }

    // Load the latest snapshot if available
    if let Some((snapshot, snapshot_counter)) = load_latest_snapshot(app_state.clone()).await? {
        info!(
            snapshot_counter = snapshot_counter,
            "Restoring store from snapshot"
        );
        
        // Initialize the snapshot file counter to continue from the next number
        let next_snapshot_counter = get_next_snapshot_counter(app_state.clone()).await?;
        
        {
            let mut store_guard = app_state.store.write().await;
            store_guard.inner_mut().disable_notifications();
            store_guard.inner_mut().restore_snapshot(snapshot);
            store_guard.inner_mut().enable_notifications();
        }
        
        let mut wal_state = app_state.wal_state.write().await;
        wal_state.snapshot_file_counter = next_snapshot_counter;
    } else {
        info!("No snapshot found, starting with empty store");
        
        // Initialize the snapshot file counter
        let next_snapshot_counter = get_next_snapshot_counter(app_state.clone()).await?;
        let mut wal_state = app_state.wal_state.write().await;
        wal_state.snapshot_file_counter = next_snapshot_counter;
    }

    // Replay WAL files to bring the store up to date
    // The replay function will automatically find the most recent snapshot marker
    // in the WAL files and start replaying from that point
    info!("Replaying WAL files");
    if let Err(e) = replay_wal_files(app_state.clone()).await {
        error!(
            error = %e,
            "Failed to replay WAL files"
        );
        return Err(e);
    }

    // Reinitialize caches after WAL replay
    reinit_caches(app_state.clone()).await?;

    // Start the write channel consumer task
    let app_state_clone = Arc::clone(&app_state);
    let mut write_channel_task = tokio::spawn(async move {
        if let Err(e) = consume_write_channel(app_state_clone).await {
            error!(
                error = %e,
                "Write channel consumer failed"
            );
        }
    });

    // Start the peer WebSocket server task
    let app_state_clone = Arc::clone(&app_state);
    let mut peer_server_task = tokio::spawn(async move {
        if let Err(e) = start_inbound_peer_server(app_state_clone).await {
            error!(
                error = %e,
                "Peer server failed"
            );
        }
    });

    // Start the client WebSocket server task
    let app_state_clone = Arc::clone(&app_state);
    let mut client_server_task = tokio::spawn(async move {
        if let Err(e) = start_client_server(app_state_clone).await {
            error!(
                error = %e,
                "Client server failed"
            );
        }
    });

    // Start the outbound peer connection manager task
    let app_state_clone = Arc::clone(&app_state);
    let mut outbound_peer_task = tokio::spawn(async move {
        if let Err(e) = manage_outbound_peer_connections(app_state_clone).await {
            error!(
                error = %e,
                "Outbound peer connection manager failed"
            );
        }
    });

    // Start the misc tasks handler
    let app_state_clone = Arc::clone(&app_state);
    let mut misc_task = tokio::spawn(async move {
        if let Err(e) = handle_misc_tasks(app_state_clone).await {
            error!(
                error = %e,
                "Misc tasks handler failed"
            );
        }
    });

    // Start the heartbeat writer
    let app_state_clone = Arc::clone(&app_state);
    let mut heartbeat_task = tokio::spawn(async move {
        if let Err(e) = handle_heartbeat_writing(app_state_clone).await {
            error!(
                error = %e,
                "Heartbeat writer failed"
            );
        }
    });

    // Wait for either shutdown signal or any critical task to complete/fail
    tokio::select! {
        _ = signal::ctrl_c() => {
            warn!("Received shutdown signal, initiating graceful shutdown");
        }
        result = &mut write_channel_task => {
            match result {
                Ok(_) => error!("Write channel task exited unexpectedly"),
                Err(e) => error!(error = %e, "Write channel task failed"),
            }
            warn!("Critical task failure detected, initiating shutdown");
        }
        result = &mut peer_server_task => {
            match result {
                Ok(_) => error!("Peer server task exited unexpectedly"),
                Err(e) => error!(error = %e, "Peer server task failed"),
            }
            warn!("Critical task failure detected, initiating shutdown");
        }
        result = &mut client_server_task => {
            match result {
                Ok(_) => error!("Client server task exited unexpectedly"),
                Err(e) => error!(error = %e, "Client server task failed"),
            }
            warn!("Critical task failure detected, initiating shutdown");
        }
        result = &mut outbound_peer_task => {
            match result {
                Ok(_) => error!("Outbound peer task exited unexpectedly"),
                Err(e) => error!(error = %e, "Outbound peer task failed"),
            }
            warn!("Critical task failure detected, initiating shutdown");
        }
        result = &mut misc_task => {
            match result {
                Ok(_) => error!("Misc task exited unexpectedly"),
                Err(e) => error!(error = %e, "Misc task failed"),
            }
            warn!("Critical task failure detected, initiating shutdown");
        }
        result = &mut heartbeat_task => {
            match result {
                Ok(_) => error!("Heartbeat task exited unexpectedly"),
                Err(e) => error!(error = %e, "Heartbeat task failed"),
            }
            warn!("Critical task failure detected, initiating shutdown");
        }
    }

    // Take a final snapshot before shutting down
    info!("Taking final snapshot before shutdown");
    let snapshot = {
        let store_guard = app_state.store.read().await;
        store_guard.inner().take_snapshot()
    };
    
    match save_snapshot(&snapshot, app_state.clone()).await {
        Ok(snapshot_counter) => {
            info!(
                snapshot_counter = snapshot_counter,
                "Final snapshot saved successfully"
            );
            
            // Write a snapshot marker to the WAL to indicate the final snapshot point
            // This helps during replay to know that the state was snapshotted at shutdown
            let snapshot_request = qlib_rs::Request::Snapshot {
                snapshot_counter,
                timestamp: Some(now()),
                originator: Some({
                    let core = app_state.core_state.read().await;
                    core.config.machine.clone()
                }),
            };
            
            if let Err(e) = write_request_to_wal_direct(&snapshot_request, app_state.clone()).await {
                error!(error = %e, "Failed to write final snapshot marker to WAL");
            } else {
                info!("Final snapshot marker written to WAL");
            }
        }
        Err(e) => {
            error!(error = %e, "Failed to save final snapshot");
        }
    }

    // Abort all tasks
    info!("Stopping all background tasks");
    write_channel_task.abort();
    peer_server_task.abort();
    client_server_task.abort();
    outbound_peer_task.abort();
    misc_task.abort();
    heartbeat_task.abort();

    info!("QCore service shutdown complete");
    Ok(())
}
