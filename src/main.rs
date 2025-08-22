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
use std::time::Duration;
use tokio::fs::{File, OpenOptions, create_dir_all, read_dir, remove_file};
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use std::path::PathBuf;

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
}

/// Application state that is shared across all tasks
#[derive(Debug)]
struct AppState {
    /// Configuration
    config: Config,
    
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

impl AppState {
    /// Create a new AppState with the given configuration
    fn new(config: Config) -> Self {
        Self {
            config,
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
                
                // Try to parse as a Request for synchronization
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
    }
    
    info!("Peer connection closed: {}", peer_addr);
    Ok(())
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
    
    // Handle incoming messages from peer
    while let Some(msg) = ws_receiver.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                debug!("Received text from outbound peer {}: {}", peer_addr, text);
                
                // Try to parse as a Request for synchronization
                match serde_json::from_str::<qlib_rs::Request>(&text) {
                    Ok(request) => {
                        debug!("Received sync request from outbound peer {}: {:?}", peer_addr, request);
                        
                        // Apply the request to our store if it doesn't already have an originator
                        // (to avoid infinite loops) and ensure the current timestamp is preserved
                        if request.originator().is_some() {
                            let mut state = app_state.write().await;
                            let store = &mut state.store;
                            let mut store_guard = store.write().await;
                            
                            let mut requests = vec![request];
                            if let Err(e) = store_guard.perform(&mut requests).await {
                                error!("Failed to apply sync request from outbound peer {}: {}", peer_addr, e);
                            } else {
                                debug!("Successfully applied sync request from outbound peer {}", peer_addr);
                            }
                        } else {
                            debug!("Ignoring request without originator from outbound peer {}", peer_addr);
                        }
                    }
                    Err(_) => {
                        // Not a sync request, treat as regular peer message
                        debug!("Received non-sync message from outbound peer {}", peer_addr);
                    }
                }
            }
            Ok(Message::Binary(data)) => {
                debug!("Received binary data from outbound peer {}: {} bytes", peer_addr, data.len());
            }
            Ok(Message::Ping(_)) => {
                debug!("Received ping from outbound peer: {}", peer_addr);
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
        
        StoreMessage::TakeSnapshot { id } => {
            let snapshot = store_guard.take_snapshot();
            StoreMessage::TakeSnapshotResponse {
                id,
                response: snapshot,
            }
        }
        
        StoreMessage::RestoreSnapshot { id, snapshot } => {
            store_guard.restore_snapshot(snapshot);
            StoreMessage::RestoreSnapshotResponse {
                id,
                response: Ok(()),
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
        StoreMessage::TakeSnapshotResponse { id, .. } |
        StoreMessage::RestoreSnapshotResponse { id, .. } |
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
        // Wait for a request from the write channel without holding any store locks
        let request = {
            let mut receiver_guard = receiver.lock().await;
            receiver_guard.recv().await
        };
        
        match request {
            Some(request) => {
                debug!("Writing request to WAL: {:?}", request);
                
                // Write request to WAL file - the request has already been applied to the store
                if let Err(e) = write_request_to_wal(&request, app_state.clone()).await {
                    error!("Failed to write request to WAL: {}", e);
                }
                
                // Check if this request originated from the current machine
                let current_machine = {
                    let state = app_state.read().await;
                    state.config.machine.clone()
                };
                
                // If the request originated from this machine, send it to peers for synchronization
                if let Some(originator) = request.originator() {
                    if originator == &current_machine {
                        debug!("Sending request to peers for synchronization: {:?}", request);
                        
                        // Send to all connected outbound peers
                        let peers_to_notify = {
                            let state = app_state.read().await;
                            state.connected_outbound_peers.clone()
                        };
                        
                        // Serialize the request to JSON for transmission
                        match serde_json::to_string(&request) {
                            Ok(request_json) => {
                                let message = Message::Text(request_json);
                                
                                for (peer_addr, sender) in peers_to_notify {
                                    if let Err(e) = sender.send(message.clone()) {
                                        warn!("Failed to send request to peer {}: {}", peer_addr, e);
                                    } else {
                                        debug!("Sent request to peer: {}", peer_addr);
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Failed to serialize request for peer synchronization: {}", e);
                            }
                        }
                    }
                } else {
                    debug!("Request has no originator, not sending to peers");
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
        let wal_dir = PathBuf::from(&state.config.data_dir).join("wal");
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
    let snapshot_dir = PathBuf::from(&state.config.data_dir).join("snapshots");
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
    let snapshot_dir = PathBuf::from(&state.config.data_dir).join("snapshots");
    
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
    let wal_dir = PathBuf::from(&state.config.data_dir).join("wal");
    
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
    
    let mut offset = 0;
    let mut requests_processed = 0;
    
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
    
    info!("Replayed {} requests from {}", requests_processed, wal_path.display());
    Ok(())
}

/// Determine the next WAL file counter based on existing WAL files
async fn get_next_wal_counter(app_state: Arc<RwLock<AppState>>) -> Result<u64> {
    let state = app_state.read().await;
    let wal_dir = PathBuf::from(&state.config.data_dir).join("wal");
    
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
    let snapshot_dir = PathBuf::from(&state.config.data_dir).join("snapshots");
    
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
