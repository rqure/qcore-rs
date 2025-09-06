use crate::states::{AppState, AppStateLocks, AvailabilityState, LockRequest, PeerInfo, PeerMessage};
use crate::persistance::{save_snapshot, write_request_to_wal};
use crate::reinit_caches;
use qlib_rs::{now, StoreTrait};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, connect_async, tungstenite::Message};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tracing::{info, warn, error, debug, instrument};
use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;
use time;

/// Service for managing peer connections, leader election, and data synchronization
#[derive(Clone)]
pub struct PeerService {
    app_state: Arc<AppState>,
}

impl PeerService {
    /// Create a new PeerService instance
    pub fn new(app_state: Arc<AppState>) -> Self {
        Self { app_state }
    }

    /// Start the inbound peer server
    #[instrument(skip(self))]
    pub async fn start_inbound_server(&self) -> Result<()> {
        let addr = {
            let mut locks = self.app_state.acquire_locks(LockRequest {
                core_state: true,
                ..Default::default()
            }).await;
            format!("0.0.0.0:{}", locks.core_state().config.peer_port)
        };
        
        let listener = TcpListener::bind(&addr).await?;
        info!(bind_address = %addr, "Peer WebSocket server started");
        
        while let Ok((stream, peer_addr)) = listener.accept().await {
            info!(peer_addr = %peer_addr, "Accepted inbound peer connection");
            
            let app_state_clone = Arc::clone(&self.app_state);
            tokio::spawn(async move {
                if let Err(e) = Self::handle_inbound_connection(stream, peer_addr, app_state_clone).await {
                    error!(
                        peer_addr = %peer_addr,
                        error = %e,
                        "Error handling inbound peer connection"
                    );
                }
            });
        }
        
        Ok(())
    }

    /// Manage outbound peer connections - connects to configured peers and maintains connections
    pub async fn manage_outbound_connections(&self) -> Result<()> {
        info!("Starting outbound peer connection manager");
        
        let reconnect_interval = {
            let mut locks = self.app_state.acquire_locks(LockRequest {
                core_state: true,
                ..Default::default()
            }).await;
            Duration::from_secs(locks.core_state().config.peer_reconnect_interval_secs)
        };
        
        let mut interval = tokio::time::interval(reconnect_interval);
        
        loop {
            interval.tick().await;
            
            let peers_to_connect = {
                let locks = self.app_state.acquire_locks(LockRequest {
                    connections: true,
                    core_state: true,
                    ..Default::default()
                }).await;

                let (connections, core_state) = locks.connections.as_ref().zip(locks.core_state.as_ref()).unwrap();
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
                let app_state_clone = Arc::clone(&self.app_state);
                
                tokio::spawn(async move {
                    // Attempt connection
                    if let Err(e) = Self::handle_outbound_connection(&peer_addr_clone, app_state_clone.clone()).await {
                        error!(
                            peer_addr = %peer_addr_clone,
                            error = %e,
                            "Failed to connect to peer"
                        );
                    } else {
                        info!(
                            peer_addr = %peer_addr_clone,
                            "Connection to peer ended"
                        );
                    }

                    let mut locks = app_state_clone.acquire_locks(LockRequest {
                        connections: true,
                        ..Default::default()
                    }).await;
                    locks.connections().connected_outbound_peers.remove(&peer_addr_clone);
                });
            }
        }
    }

    /// Handle a single inbound peer WebSocket connection
    #[instrument(skip(stream, app_state), fields(peer_addr = %peer_addr))]
    async fn handle_inbound_connection(stream: TcpStream, peer_addr: std::net::SocketAddr, app_state: Arc<AppState>) -> Result<()> {
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
                            let mut locks = app_state.acquire_locks(LockRequest {
                                core_state: true,
                                peer_info: true,
                                connections: true,
                                store: true,
                                ..Default::default()
                            }).await;
                            Self::handle_peer_message(peer_msg, &peer_addr, &mut ws_sender, &mut locks).await;
                        }
                        Err(e) => {
                            warn!(
                                error = %e,
                                "Failed to parse text message as PeerMessage"
                            );
                        }
                    }
                }
                Ok(Message::Binary(data)) => {
                    debug!(data_length = data.len(), "Received binary message from peer");
                    
                    // Try to parse as a PeerMessage (likely FullSyncResponse with binary serialization)
                    match bincode::deserialize::<PeerMessage>(&data) {
                        Ok(peer_msg) => {
                            debug!(message_type = ?std::mem::discriminant(&peer_msg), "Processing binary peer message");
                            let mut locks = app_state.acquire_locks(LockRequest {
                                core_state: true,
                                peer_info: true,
                                connections: true,
                                store: true,
                                ..Default::default()
                            }).await;
                            Self::handle_peer_message(peer_msg, &peer_addr, &mut ws_sender, &mut locks).await;
                        }
                        Err(e) => {
                            warn!(
                                error = %e,
                                "Failed to parse binary message as PeerMessage"
                            );
                        }
                    }
                }
                Ok(Message::Close(_)) => {
                    info!("Peer closed connection");
                    break;
                }
                Ok(_) => {
                    debug!("Received unsupported message type from peer");
                }
                Err(e) => {
                    warn!(error = %e, "WebSocket error with peer");
                    break;
                }
            }
        }
        
        // Clean up peer info when connection closes
        {
            let mut locks = app_state.acquire_locks(LockRequest {
                peer_info: true,
                core_state: true,
                ..Default::default()
            }).await;
            
            // Get the machine ID that disconnected for leader election check
            let disconnected_machine_id = locks.peer_info().get(&peer_addr.to_string()).map(|p| p.machine_id.clone());
            
            // Remove the peer from our tracking
            locks.peer_info().remove(&peer_addr.to_string());
            
            // Check if the disconnected peer was the current leader
            if let Some(disconnected_machine_id) = disconnected_machine_id {
                let current_leader = locks.core_state().current_leader.clone();
                if current_leader == Some(disconnected_machine_id.clone()) {
                    info!(
                        disconnected_machine = %disconnected_machine_id,
                        "Current leader disconnected, retriggering leader election"
                    );
                    app_state.retrigger_leader_election(&mut locks).await;
                }
            }
        }
        
        info!("Peer connection terminated");
        Ok(())
    }

    /// Handle an outbound peer connection
    #[instrument(skip(app_state))]
    async fn handle_outbound_connection(peer_addr: &str, app_state: Arc<AppState>) -> Result<()> {
        info!(peer_addr = %peer_addr, "Attempting to connect to peer");
        
        let url = format!("ws://{}", peer_addr);
        let (ws_stream, _) = connect_async(&url).await?;
        info!(peer_addr = %peer_addr, "Connected to peer");
        
        let (mut ws_sender, mut ws_receiver) = ws_stream.split();
        
        // Create a channel for sending messages to this peer
        let (sender, mut receiver) = mpsc::unbounded_channel::<Message>();
        
        // Register this outbound peer connection
        {
            let mut locks = app_state.acquire_locks(LockRequest {
                connections: true,
                ..Default::default()
            }).await;
            locks.connections().connected_outbound_peers.insert(peer_addr.to_string(), sender);
        }
        
        // Send startup message immediately after connecting
        let startup = PeerMessage::Startup {
            machine_id: {
                let mut locks = app_state.acquire_locks(LockRequest {
                    core_state: true,
                    ..Default::default()
                }).await;
                locks.core_state().config.machine.clone()
            },
            startup_time: {
                let mut locks = app_state.acquire_locks(LockRequest {
                    core_state: true,
                    ..Default::default()
                }).await;
                locks.core_state().startup_time
            },
        };
        
        let startup_json = serde_json::to_string(&startup)?;
        ws_sender.send(Message::Text(startup_json)).await?;
        info!(peer_addr = %peer_addr, "Sent startup message to peer");
        
        // Handle both sending and receiving in parallel
        tokio::select! {
            // Handle outgoing messages
            _ = async {
                while let Some(message) = receiver.recv().await {
                    if let Err(e) = ws_sender.send(message).await {
                        error!(
                            peer_addr = %peer_addr,
                            error = %e,
                            "Failed to send message to peer"
                        );
                        break;
                    }
                }
            } => {}
            // Handle incoming messages  
            _ = async {
                while let Some(msg) = ws_receiver.next().await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            debug!(message_length = text.len(), "Received text message from outbound peer");
                            
                            match serde_json::from_str::<PeerMessage>(&text) {
                                Ok(peer_msg) => {
                                    debug!(message_type = ?std::mem::discriminant(&peer_msg), "Processing peer message from outbound connection");
                                    let mut locks = app_state.acquire_locks(LockRequest {
                                        core_state: true,
                                        peer_info: true,
                                        connections: true,
                                        store: true,
                                        ..Default::default()
                                    }).await;
                                    
                                    let peer_socket_addr: std::net::SocketAddr = peer_addr.parse().unwrap_or_else(|_| {
                                        std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0)), 0)
                                    });
                                    Self::handle_peer_message(peer_msg, &peer_socket_addr, &mut ws_sender, &mut locks).await;
                                }
                                Err(e) => {
                                    warn!(
                                        error = %e,
                                        "Failed to parse text message as PeerMessage from outbound peer"
                                    );
                                }
                            }
                        }
                        Ok(Message::Binary(data)) => {
                            debug!(data_length = data.len(), "Received binary message from outbound peer");
                            
                            // Try to deserialize as PeerMessage to check if it's a FullSyncResponse
                            match bincode::deserialize::<PeerMessage>(&data) {
                                Ok(PeerMessage::FullSyncResponse { snapshot }) => {
                                    debug!("Processing FullSyncResponse from outbound peer");
                                    let mut locks = app_state.acquire_locks(LockRequest {
                                        core_state: true,
                                        peer_info: true,
                                        connections: true,
                                        store: true,
                                        ..Default::default()
                                    }).await;
                                    
                                    let peer_socket_addr: std::net::SocketAddr = peer_addr.parse().unwrap_or_else(|_| {
                                        std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0)), 0)
                                    });
                                    Self::handle_peer_message(PeerMessage::FullSyncResponse { snapshot }, &peer_socket_addr, &mut ws_sender, &mut locks).await;
                                }
                                Ok(other_msg) => {
                                    debug!(message_type = ?std::mem::discriminant(&other_msg), "Processing binary peer message from outbound connection");
                                    let mut locks = app_state.acquire_locks(LockRequest {
                                        core_state: true,
                                        peer_info: true,
                                        connections: true,
                                        store: true,
                                        ..Default::default()
                                    }).await;
                                    
                                    let peer_socket_addr: std::net::SocketAddr = peer_addr.parse().unwrap_or_else(|_| {
                                        std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0)), 0)
                                    });
                                    Self::handle_peer_message(other_msg, &peer_socket_addr, &mut ws_sender, &mut locks).await;
                                }
                                Err(e) => {
                                    warn!(
                                        error = %e,
                                        "Failed to parse binary message as PeerMessage from outbound peer"
                                    );
                                }
                            }
                        }
                        Ok(Message::Close(_)) => {
                            info!(peer_addr = %peer_addr, "Outbound peer closed connection");
                            break;
                        }
                        Ok(_) => {
                            debug!("Received unsupported message type from outbound peer");
                        }
                        Err(e) => {
                            warn!(
                                peer_addr = %peer_addr,
                                error = %e,
                                "WebSocket error with outbound peer"
                            );
                            break;
                        }
                    }
                }
            } => {}
        }
        
        info!(peer_addr = %peer_addr, "Outbound peer connection ended");
        Ok(())
    }

    /// Handle a peer message and respond appropriately
    #[instrument(skip(peer_msg, ws_sender, locks), fields(peer_addr = %peer_addr))]
    async fn handle_peer_message(
        peer_msg: PeerMessage,
        peer_addr: &std::net::SocketAddr,
        ws_sender: &mut futures_util::stream::SplitSink<tokio_tungstenite::WebSocketStream<TcpStream>, Message>,
        locks: &mut AppStateLocks<'_>,
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
                    let peer_info = locks.peer_info();
                    peer_info.insert(peer_addr.to_string(), PeerInfo {
                        machine_id: machine_id.clone(),
                        startup_time,
                    });
                }
                
                // Get current state snapshot for leadership determination
                let state_snapshot = locks.core_state().get_state_snapshot();
                let our_startup_time = state_snapshot.startup_time;
                let our_machine_id = state_snapshot.config.machine.clone();
                
                // Find the earliest (largest) startup time among all known peers including ourselves
                let (should_be_leader, earliest_startup) = {
                    let peer_info = locks.peer_info();
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
                    let core_state = locks.core_state();
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
                        // First get leader from peer info
                        let leader = {
                            let peer_info = locks.peer_info();
                            peer_info.values()
                                .filter(|p| p.startup_time <= earliest_startup)
                                .min_by_key(|p| (&p.startup_time, &p.machine_id))
                                .map(|p| p.machine_id.clone())
                        };
                        
                        // Then update core state
                        let core_state = locks.core_state();
                        core_state.is_leader = false;
                        let new_state = AvailabilityState::Unavailable;
                        let old_state = core_state.availability_state.clone();
                        core_state.availability_state = new_state.clone();
                        
                        // Reset sync status when stepping down from leader to ensure we request full sync
                        core_state.is_fully_synced = false;
                        core_state.full_sync_request_pending = false;
                        
                        // Set the timestamp when we became unavailable for grace period tracking
                        if old_state != new_state {
                            core_state.became_unavailable_at = Some(time::OffsetDateTime::now_utc().unix_timestamp() as u64);
                        }
                        
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

                    locks.connections().force_disconnect_all_clients();
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
                    let store_guard = locks.store();
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
                    let store_guard = locks.store();
                    store_guard.inner_mut().disable_notifications();
                    store_guard.inner_mut().restore_snapshot(snapshot.clone());
                    store_guard.inner_mut().enable_notifications();
                }
                
                // Update core state
                {
                    let core_state = locks.core_state();
                    core_state.is_fully_synced = true;
                    core_state.full_sync_request_pending = false; // Reset pending flag since we got a response
                    core_state.availability_state = AvailabilityState::Available;
                    core_state.became_unavailable_at = None; // Clear timestamp when becoming available
                }
                
                // Save the snapshot to disk for persistence
                match save_snapshot(&snapshot, locks).await {
                    Ok(snapshot_counter) => {
                        info!(
                            snapshot_counter = snapshot_counter,
                            "Snapshot saved to disk during full sync"
                        );
                        
                        // Write a snapshot marker to the WAL to indicate the sync point
                        // This helps during replay to know that the state was synced at this point
                        let machine_id = {
                            let core = locks.core_state();
                            core.config.machine.clone()
                        };
                        
                        let snapshot_request = qlib_rs::Request::Snapshot {
                            snapshot_counter,
                            timestamp: Some(now()),
                            originator: Some(machine_id),
                        };
                        
                        if let Err(e) = write_request_to_wal(&snapshot_request, locks, true).await {
                            error!(
                                error = %e,
                                snapshot_counter = snapshot_counter,
                                "Failed to write snapshot marker to WAL"
                            );
                        }

                        // Reinitialize caches after full sync
                        if let Err(e) = reinit_caches(locks).await {
                            error!(error = %e, "Failed to reinitialize caches");
                        } else {
                            info!("Caches reinitialized successfully");
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
                    let core = locks.core_state();
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
                    let store_guard = locks.store();
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
}