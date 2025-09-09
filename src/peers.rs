use qlib_rs::now;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, connect_async, tungstenite::Message};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn, error, debug, instrument};
use anyhow::Result;
use std::collections::HashMap;
use std::time::Duration;
use time;
use serde::{Serialize, Deserialize};

use crate::Services;

/// Configuration for the peer service
#[derive(Debug, Clone)]
pub struct PeerConfig {
    /// Machine ID (unique identifier for this instance)
    pub machine: String,
    /// Port for peer-to-peer communication
    pub peer_port: u16,
    /// List of peer addresses to connect to (format: host:port)
    pub peer_addresses: Vec<String>,
    /// Interval in seconds to retry connecting to peers
    pub peer_reconnect_interval_secs: u64,
    /// Grace period in seconds to wait after becoming unavailable before requesting full sync
    pub full_sync_grace_period_secs: u64,
    /// Delay in seconds after startup before self-promoting to leader when no peers are available
    pub self_promotion_delay_secs: u64,
}

impl From<&crate::Config> for PeerConfig {
    fn from(config: &crate::Config) -> Self {
        Self {
            machine: config.machine.clone(),
            peer_port: config.peer_port,
            peer_addresses: config.peer_addresses.clone(),
            peer_reconnect_interval_secs: config.peer_reconnect_interval_secs,
            full_sync_grace_period_secs: config.full_sync_grace_period_secs,
            self_promotion_delay_secs: config.self_promotion_delay_secs,
        }
    }
}

/// Application availability state
#[derive(Debug, Clone, PartialEq)]
pub enum AvailabilityState {
    /// Application is unavailable - attempting to sync with leader, clients are force disconnected
    Unavailable,
    /// Application is available - clients are allowed to connect and perform operations
    Available,
}

/// Messages exchanged between peers for leader election
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum PeerMessage {
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

/// Information about a peer instance
#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub machine_id: String,
    pub startup_time: u64
}

/// Peer service request types
#[derive(Debug)]
pub enum PeerRequest {
    SendSyncMessage {
        requests: Vec<qlib_rs::Request>,
        response: oneshot::Sender<()>,
    },
    GetAvailabilityState {
        response: oneshot::Sender<AvailabilityState>,
    },
    GetLeadershipInfo {
        response: oneshot::Sender<(bool, Option<String>)>, // (is_leader, current_leader)
    },
    IsOutboundPeerConnected {
        peer_addr: String,
        response: oneshot::Sender<bool>,
    },
    PeerConnected {
        peer_addr: String,
        machine_id: String,
        startup_time: u64,
    },
    PeerDisconnected {
        peer_addr: String,
    },
    OutboundPeerConnected {
        peer_addr: String,
        sender: mpsc::UnboundedSender<Message>,
    },
    OutboundPeerDisconnected {
        peer_addr: String,
    },
    FullSyncCompleted,
    ProcessPeerMessage {
        peer_msg: PeerMessage,
        peer_addr: String,
        response: oneshot::Sender<Option<PeerMessage>>,
    },
    SetServices {
        services: Services,
        response: oneshot::Sender<()>,
    },
}

/// Handle for communicating with peer service
#[derive(Debug, Clone)]
pub struct PeerHandle {
    sender: mpsc::UnboundedSender<PeerRequest>,
}

impl PeerHandle {
    pub async fn send_sync_message(&self, requests: Vec<qlib_rs::Request>) {
        let (response_tx, response_rx) = oneshot::channel();
        if self.sender.send(PeerRequest::SendSyncMessage {
            requests,
            response: response_tx,
        }).is_ok() {
            let _ = response_rx.await;
        }
    }

    pub async fn get_availability_state(&self) -> AvailabilityState {
        let (response_tx, response_rx) = oneshot::channel();
        if self.sender.send(PeerRequest::GetAvailabilityState {
            response: response_tx,
        }).is_ok() {
            response_rx.await.unwrap_or(AvailabilityState::Unavailable)
        } else {
            AvailabilityState::Unavailable
        }
    }

    pub async fn get_leadership_info(&self) -> (bool, Option<String>) {
        let (response_tx, response_rx) = oneshot::channel();
        if self.sender.send(PeerRequest::GetLeadershipInfo {
            response: response_tx,
        }).is_ok() {
            response_rx.await.unwrap_or((false, None))
        } else {
            (false, None)
        }
    }

    pub fn peer_connected(&self, peer_addr: String, machine_id: String, startup_time: u64) {
        let _ = self.sender.send(PeerRequest::PeerConnected {
            peer_addr,
            machine_id,
            startup_time,
        });
    }

    pub fn peer_disconnected(&self, peer_addr: String) {
        let _ = self.sender.send(PeerRequest::PeerDisconnected { peer_addr });
    }

    pub fn outbound_peer_connected(&self, peer_addr: String, sender: mpsc::UnboundedSender<Message>) {
        let _ = self.sender.send(PeerRequest::OutboundPeerConnected {
            peer_addr,
            sender,
        });
    }

    pub fn outbound_peer_disconnected(&self, peer_addr: String) {
        let _ = self.sender.send(PeerRequest::OutboundPeerDisconnected { peer_addr });
    }

    pub fn full_sync_completed(&self) {
        let _ = self.sender.send(PeerRequest::FullSyncCompleted);
    }

    pub async fn process_peer_message(&self, peer_msg: PeerMessage, peer_addr: String) -> Option<PeerMessage> {
        let (response_tx, response_rx) = oneshot::channel();
        if self.sender.send(PeerRequest::ProcessPeerMessage {
            peer_msg,
            peer_addr,
            response: response_tx,
        }).is_ok() {
            response_rx.await.unwrap_or(None)
        } else {
            None
        }
    }

    /// Check if we already have an outbound connection to a peer
    pub async fn is_outbound_peer_connected(&self, peer_addr: &str) -> bool {
        let (response_tx, response_rx) = oneshot::channel();
        if self.sender.send(PeerRequest::IsOutboundPeerConnected {
            peer_addr: peer_addr.to_string(),
            response: response_tx,
        }).is_ok() {
            response_rx.await.unwrap_or(false)
        } else {
            false
        }
    }

    /// Set services for dependencies
    pub async fn set_services(&self, services: Services) {
        let (response_tx, response_rx) = oneshot::channel();
        if self.sender.send(PeerRequest::SetServices {
            services,
            response: response_tx,
        }).is_ok() {
            let _ = response_rx.await;
        }
    }
}

pub struct PeerService {
    config: PeerConfig,
    startup_time: u64,
    availability_state: AvailabilityState,
    is_leader: bool,
    current_leader: Option<String>,
    is_fully_synced: bool,
    became_unavailable_at: Option<u64>,
    full_sync_request_pending: bool,
    peer_info: HashMap<String, PeerInfo>,
    connected_outbound_peers: HashMap<String, mpsc::UnboundedSender<Message>>,
    services: Option<Services>,
}

impl PeerService {
    pub fn spawn(
        config: PeerConfig,
    ) -> PeerHandle {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        
        let startup_time = time::OffsetDateTime::now_utc().unix_timestamp() as u64;
        
        let mut service = PeerService {
            config: config.clone(),
            startup_time,
            availability_state: AvailabilityState::Available,
            is_leader: false,
            current_leader: None,
            is_fully_synced: false,
            became_unavailable_at: None,
            full_sync_request_pending: false,
            peer_info: HashMap::new(),
            connected_outbound_peers: HashMap::new(),
            services: None,
        };
        
        let handle = PeerHandle { sender: sender.clone() };
        
        // Start subtasks with the handle
        {
            let handle_clone = handle.clone();
            let config_clone = config.clone();
            tokio::spawn(async move {
                if let Err(e) = start_inbound_peer_server(config_clone, handle_clone).await {
                    error!(error = %e, "Inbound peer server failed");
                }
            });
        }
        
        {
            let handle_clone = handle.clone();
            let config_clone = config.clone();
            tokio::spawn(async move {
                if let Err(e) = manage_outbound_peer_connections(config_clone, handle_clone).await {
                    error!(error = %e, "Outbound peer connection manager failed");
                }
            });
        }
        
        // Main service loop
        tokio::spawn(async move {
            let mut check_interval = tokio::time::interval(Duration::from_millis(100));
            
            loop {
                tokio::select! {
                    _ = check_interval.tick() => {
                        service.check_leader_election_and_sync().await;
                    }
                    request = receiver.recv() => {
                        match request {
                            Some(req) => service.handle_request(req).await,
                            None => break,
                        }
                    }
                }
            }
        });

        handle
    }
    
    async fn handle_request(&mut self, request: PeerRequest) {
        match request {
            PeerRequest::SendSyncMessage { requests, response } => {
                self.send_sync_message_to_peers(requests).await;
                let _ = response.send(());
            }
            PeerRequest::GetAvailabilityState { response } => {
                let _ = response.send(self.availability_state.clone());
            }
            PeerRequest::GetLeadershipInfo { response } => {
                let _ = response.send((self.is_leader, self.current_leader.clone()));
            }
            PeerRequest::IsOutboundPeerConnected { peer_addr, response } => {
                let is_connected = self.connected_outbound_peers.contains_key(&peer_addr);
                let _ = response.send(is_connected);
            }
            PeerRequest::PeerConnected { peer_addr, machine_id, startup_time } => {
                self.handle_peer_connected(peer_addr, machine_id, startup_time).await;
            }
            PeerRequest::PeerDisconnected { peer_addr } => {
                self.handle_peer_disconnected(peer_addr).await;
            }
            PeerRequest::OutboundPeerConnected { peer_addr, sender } => {
                self.connected_outbound_peers.insert(peer_addr.clone(), sender);
                
                // Send startup message
                let startup = PeerMessage::Startup {
                    machine_id: self.config.machine.clone(),
                    startup_time: self.startup_time,
                };
                
                if let Ok(startup_json) = serde_json::to_string(&startup) {
                    if let Some(sender) = self.connected_outbound_peers.get(&peer_addr) {
                        let _ = sender.send(Message::Text(startup_json));
                    }
                }
            }
            PeerRequest::OutboundPeerDisconnected { peer_addr } => {
                self.connected_outbound_peers.remove(&peer_addr);
            }
            PeerRequest::FullSyncCompleted => {
                self.is_fully_synced = true;
                self.full_sync_request_pending = false;
                self.availability_state = AvailabilityState::Available;
                self.became_unavailable_at = None;
            }
            PeerRequest::ProcessPeerMessage { peer_msg, peer_addr, response } => {
                let reply = self.process_peer_message_internal(peer_msg, peer_addr).await;
                let _ = response.send(reply);
            }
            PeerRequest::SetServices { services, response } => {
                self.services = Some(services);
                let _ = response.send(());
            }
        }
    }
    
    async fn check_leader_election_and_sync(&mut self) {
        // Self-promotion logic
        if !self.is_leader && 
           (self.config.peer_addresses.is_empty() || self.connected_outbound_peers.is_empty()) &&
           self.peer_info.is_empty() {
            
            let current_time = time::OffsetDateTime::now_utc().unix_timestamp() as u64;
            let time_since_startup = current_time.saturating_sub(self.startup_time);
            
            if time_since_startup >= self.config.self_promotion_delay_secs {
                info!("Self-promoting to leader due to no peer connections");
                self.is_leader = true;
                self.current_leader = Some(self.config.machine.clone());
                self.availability_state = AvailabilityState::Available;
                self.is_fully_synced = true;
            }
        }
        
        // Full sync request logic
        if matches!(self.availability_state, AvailabilityState::Unavailable) &&
           !self.is_leader &&
           !self.is_fully_synced &&
           !self.full_sync_request_pending {
            
            if let Some(became_unavailable_at) = self.became_unavailable_at {
                let current_time = time::OffsetDateTime::now_utc().unix_timestamp() as u64;
                let elapsed = current_time.saturating_sub(became_unavailable_at);
                
                if elapsed >= self.config.full_sync_grace_period_secs {
                    if let Some(leader_machine_id) = &self.current_leader {
                        self.send_full_sync_request(leader_machine_id.clone()).await;
                    }
                }
            }
        }
    }
    
    async fn handle_peer_connected(&mut self, peer_addr: String, machine_id: String, startup_time: u64) {
        info!(
            peer_addr = %peer_addr,
            machine_id = %machine_id,
            startup_time = startup_time,
            "Peer connected, updating leadership"
        );
        
        self.peer_info.insert(peer_addr, PeerInfo {
            machine_id,
            startup_time,
        });
        
        self.determine_leadership().await;
    }
    
    async fn handle_peer_disconnected(&mut self, peer_addr: String) {
        let disconnected_machine_id = self.peer_info.get(&peer_addr)
            .map(|info| info.machine_id.clone());
        
        self.peer_info.remove(&peer_addr);
        
        // Check if the disconnected peer was the current leader
        if let Some(disconnected_machine_id) = disconnected_machine_id {
            if let Some(ref current_leader) = self.current_leader {
                if *current_leader == disconnected_machine_id {
                    info!(
                        disconnected_machine = %disconnected_machine_id,
                        "Current leader disconnected, retriggering leader election"
                    );
                    self.determine_leadership().await;
                }
            }
        }
    }
    
    async fn determine_leadership(&mut self) {
        let our_startup_time = self.startup_time;
        let our_machine_id = &self.config.machine;
        
        // Find the earliest startup time among all known peers including ourselves
        let earliest_startup = self.peer_info.values()
            .map(|p| p.startup_time)
            .min()
            .unwrap_or(our_startup_time)
            .min(our_startup_time);
        
        let mut should_be_leader = our_startup_time <= earliest_startup;
        
        // Handle startup time ties
        if our_startup_time == earliest_startup {
            let peers_with_same_time: Vec<_> = self.peer_info.values()
                .filter(|p| p.startup_time == our_startup_time)
                .collect();
            
            if !peers_with_same_time.is_empty() {
                let mut all_machine_ids = peers_with_same_time.iter()
                    .map(|p| p.machine_id.as_str())
                    .collect::<Vec<_>>();
                all_machine_ids.push(our_machine_id.as_str());
                
                let min_machine_id = all_machine_ids.iter().min().unwrap();
                should_be_leader = **min_machine_id == *our_machine_id;
            }
        }
        
        let old_state = self.availability_state.clone();
        
        if should_be_leader {
            self.is_leader = true;
            self.current_leader = Some(our_machine_id.clone());
            self.is_fully_synced = true;
            self.availability_state = AvailabilityState::Available;
            self.became_unavailable_at = None;
            
            info!(
                our_startup_time = our_startup_time,
                earliest_startup = earliest_startup,
                "Elected as leader"
            );
        } else {
            let leader = self.peer_info.values()
                .filter(|p| p.startup_time <= earliest_startup)
                .min_by_key(|p| (&p.startup_time, &p.machine_id))
                .map(|p| p.machine_id.clone());
            
            self.is_leader = false;
            self.current_leader = leader;
            self.availability_state = AvailabilityState::Unavailable;
            self.is_fully_synced = false;
            self.full_sync_request_pending = false;
            self.became_unavailable_at = Some(time::OffsetDateTime::now_utc().unix_timestamp() as u64);
            
            info!(
                leader = ?self.current_leader,
                our_startup_time = our_startup_time,
                earliest_startup = earliest_startup,
                "Leader determined, stepping down"
            );
        }
        
        // Force disconnect clients if becoming unavailable
        if old_state != self.availability_state && 
           matches!(self.availability_state, AvailabilityState::Unavailable) {
            if let Some(services) = &self.services {
                let client_handle = &services.client_handle;
                client_handle.force_disconnect_all().await;
                info!("Disconnected all clients due to unavailability");
            }
        }
    }
    
    async fn send_sync_message_to_peers(&self, requests: Vec<qlib_rs::Request>) {
        let current_machine = &self.config.machine;
        
        // Filter requests to only include those from our machine
        let requests_to_sync: Vec<qlib_rs::Request> = requests.iter()
            .filter(|request| {
                if let Some(originator) = request.originator() {
                    originator == current_machine
                } else {
                    false
                }
            })
            .cloned()
            .collect();
        
        if !requests_to_sync.is_empty() {
            let sync_message = PeerMessage::SyncRequest {
                requests: requests_to_sync.clone(),
            };
            
            if let Ok(message_json) = serde_json::to_string(&sync_message) {
                let message = Message::Text(message_json);
                
                for (peer_addr, sender) in &self.connected_outbound_peers {
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
        }
    }
    
    async fn send_full_sync_request(&mut self, leader_machine_id: String) {
        info!(
            leader_machine_id = %leader_machine_id,
            "Grace period expired, sending FullSyncRequest to leader"
        );
        
        self.full_sync_request_pending = true;
        
        let full_sync_request = PeerMessage::FullSyncRequest {
            machine_id: self.config.machine.clone(),
        };
        
        if let Ok(request_json) = serde_json::to_string(&full_sync_request) {
            let message = Message::Text(request_json);
            
            let mut sent = false;
            for (peer_addr, sender) in &self.connected_outbound_peers {
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
                    break;
                }
            }
            
            if !sent {
                warn!("No connected outbound peers available to send FullSyncRequest");
                self.full_sync_request_pending = false;
            }
        } else {
            error!("Failed to serialize FullSyncRequest");
            self.full_sync_request_pending = false;
        }
    }

    async fn process_peer_message_internal(&mut self, peer_msg: PeerMessage, _peer_addr: String) -> Option<PeerMessage> {
        match peer_msg {
            PeerMessage::Startup { .. } => {
                // Startup messages are handled separately through PeerConnected
                None
            }
            
            PeerMessage::FullSyncRequest { machine_id } => {
                info!(requesting_machine = %machine_id, "Received full sync request, preparing snapshot");
                
                if let Some(services) = &self.services {
                    // Take a snapshot and send it
                    if let Some(snapshot) = services.store_handle.take_snapshot().await {
                        info!(
                            requesting_machine = %machine_id,
                            snapshot_entities = snapshot.entities.len(),
                            "Snapshot prepared for full sync response"
                        );
                        
                        Some(PeerMessage::FullSyncResponse { snapshot })
                    } else {
                        error!("Failed to take snapshot for full sync request");
                        None
                    }
                } else {
                    error!("Services not available for full sync request");
                    None
                }
            }
            
            PeerMessage::FullSyncResponse { snapshot } => {
                info!("Processing full sync response, applying snapshot");
                
                if let Some(services) = &self.services {
                    // Apply the snapshot to the store
                    services.store_handle.disable_notifications().await;
                    services.store_handle.inner_restore_snapshot(snapshot.clone()).await;
                    services.store_handle.enable_notifications().await;
                    
                    // Save the snapshot to disk for persistence
                    match services.snapshot_handle.save(snapshot).await {
                        Ok(snapshot_counter) => {
                            info!(
                                snapshot_counter = snapshot_counter,
                                "Snapshot saved to disk during full sync"
                            );
                            
                            // Write a snapshot marker to the WAL to indicate the sync point
                            let snapshot_request = qlib_rs::Request::Snapshot {
                                snapshot_counter,
                                timestamp: Some(now()),
                                originator: Some(self.config.machine.clone()),
                            };
                            
                            if let Err(e) = services.wal_handle.write_request(snapshot_request).await {
                                error!(
                                    error = %e,
                                    snapshot_counter = snapshot_counter,
                                    "Failed to write snapshot marker to WAL"
                                );
                            }
                        }
                        Err(e) => {
                            error!(error = %e, "Failed to save snapshot during full sync");
                        }
                    }
                    
                    // Update state to indicate full sync completion
                    self.is_fully_synced = true;
                    self.full_sync_request_pending = false;
                    self.availability_state = AvailabilityState::Available;
                    self.became_unavailable_at = None;
                    
                    info!("Successfully applied full sync snapshot, instance is now fully synchronized");
                } else {
                    error!("Services not available for full sync response");
                }
                
                None
            }
            
            PeerMessage::SyncRequest { requests } => {
                // Handle data synchronization (existing functionality)
                if let Some(services) = &self.services {
                    let our_machine_id = &self.config.machine;
                    
                    // Filter requests to only include those with valid originators (different from our machine)
                    let mut requests_to_apply: Vec<_> = requests.into_iter()
                        .filter(|request| {
                            if let Some(originator) = request.originator() {
                                originator != our_machine_id
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
                        if let Err(e) = services.store_handle.perform_mut(&mut requests_to_apply).await {
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
                } else {
                    error!("Services not available for sync request");
                }
                
                None
            }
        }
    }
}

/// Handle a single peer WebSocket connection
#[instrument(skip(stream, handle), fields(peer_addr = %peer_addr))]
async fn handle_inbound_peer_connection(
    stream: TcpStream,
    peer_addr: std::net::SocketAddr,
    handle: PeerHandle,
) -> Result<()> {
    info!("Accepting inbound peer connection");
    
    let ws_stream = accept_async(stream).await?;
    debug!("WebSocket handshake completed");
    
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    
    while let Some(msg) = ws_receiver.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                if let Ok(peer_msg) = serde_json::from_str::<PeerMessage>(&text) {
                    handle_peer_message(peer_msg, &peer_addr, &mut ws_sender, &handle).await;
                }
            }
            Ok(Message::Binary(data)) => {
                if let Ok(peer_msg) = bincode::deserialize::<PeerMessage>(&data) {
                    handle_peer_message(peer_msg, &peer_addr, &mut ws_sender, &handle).await;
                }
            }
            Ok(Message::Ping(payload)) => {
                let _ = ws_sender.send(Message::Pong(payload)).await;
            }
            Ok(Message::Close(_)) => break,
            Err(e) => {
                error!(error = %e, "WebSocket error with peer");
                break;
            }
            _ => {}
        }
    }
    
    handle.peer_disconnected(peer_addr.to_string());
    info!("Peer connection terminated");
    Ok(())
}

async fn handle_peer_message(
    peer_msg: PeerMessage,
    peer_addr: &std::net::SocketAddr,
    ws_sender: &mut futures_util::stream::SplitSink<tokio_tungstenite::WebSocketStream<TcpStream>, Message>,
    handle: &PeerHandle,
) {
    match peer_msg {
        PeerMessage::Startup { machine_id, startup_time } => {
            info!(
                remote_machine_id = %machine_id,
                remote_startup_time = startup_time,
                "Processing startup message from peer"
            );
            
            handle.peer_connected(peer_addr.to_string(), machine_id, startup_time);
        }
        
        other_msg => {
            // Process the message and get potential response
            if let Some(response_msg) = handle.process_peer_message(other_msg, peer_addr.to_string()).await {
                match &response_msg {
                    PeerMessage::FullSyncResponse { .. } => {
                        // Use binary serialization for the snapshot since it contains complex key types
                        match bincode::serialize(&response_msg) {
                            Ok(response_binary) => {
                                info!(
                                    response_size = response_binary.len(),
                                    "Snapshot serialized to binary, sending response to requesting peer"
                                );
                                
                                let message = Message::Binary(response_binary);
                                if let Err(e) = ws_sender.send(message).await {
                                    error!(
                                        error = %e,
                                        "Failed to send FullSyncResponse to requesting peer"
                                    );
                                } else {
                                    info!("Successfully sent FullSyncResponse to requesting peer");
                                }
                            }
                            Err(e) => {
                                error!(
                                    error = %e,
                                    "Failed to serialize full sync response"
                                );
                            }
                        }
                    }
                    _ => {
                        // For other response types, use JSON serialization
                        if let Ok(response_json) = serde_json::to_string(&response_msg) {
                            if let Err(e) = ws_sender.send(Message::Text(response_json)).await {
                                error!(error = %e, "Failed to send response to peer");
                            }
                        }
                    }
                }
            }
        }
    }
}

async fn start_inbound_peer_server(
    config: PeerConfig,
    handle: PeerHandle,
) -> Result<()> {
    let addr = format!("0.0.0.0:{}", config.peer_port);
    let listener = TcpListener::bind(&addr).await?;
    info!(bind_address = %addr, "Peer WebSocket server started");
    
    loop {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                let handle_clone = handle.clone();
                
                tokio::spawn(async move {
                    if let Err(e) = handle_inbound_peer_connection(stream, peer_addr, handle_clone).await {
                        error!(error = %e, peer_addr = %peer_addr, "Error handling peer connection");
                    }
                });
            }
            Err(e) => {
                error!(error = %e, "Failed to accept peer connection");
            }
        }
    }
}

async fn manage_outbound_peer_connections(config: PeerConfig, handle: PeerHandle) -> Result<()> {
    info!("Starting outbound peer connection manager");
    
    let reconnect_interval = Duration::from_secs(config.peer_reconnect_interval_secs);
    let mut interval = tokio::time::interval(reconnect_interval);
    
    loop {
        interval.tick().await;
        
        for peer_addr in &config.peer_addresses {
            // Check if we already have an active connection to this peer
            if handle.is_outbound_peer_connected(peer_addr).await {
                continue;
            }
            
            let peer_addr_clone = peer_addr.clone();
            let handle_clone = handle.clone();
            
            tokio::spawn(async move {
                if let Err(e) = handle_outbound_peer_connection(&peer_addr_clone, handle_clone).await {
                    error!(peer_addr = %peer_addr_clone, error = %e, "Failed to connect to peer");
                }
            });
        }
    }
}

async fn handle_outbound_peer_connection(peer_addr: &str, handle: PeerHandle) -> Result<()> {
    let ws_url = format!("ws://{}", peer_addr);
    let (ws_stream, _) = connect_async(&ws_url).await?;
    info!(peer_addr = %peer_addr, "Connected to outbound peer");
    
    let (ws_sender, mut ws_receiver) = ws_stream.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();
    
    // Notify the service about the connection - it will handle sending the startup message
    handle.outbound_peer_connected(peer_addr.to_string(), tx.clone());
    
    let peer_addr_clone = peer_addr.to_string();
    let handle_clone = handle.clone();
    
    // Spawn sender task
    let sender_task = tokio::spawn(async move {
        let mut ws_sender = ws_sender;
        while let Some(message) = rx.recv().await {
            if ws_sender.send(message).await.is_err() {
                break;
            }
        }
    });
    
    // Handle incoming messages (mainly FullSyncResponse)
    while let Some(msg) = ws_receiver.next().await {
        match msg {
            Ok(Message::Binary(data)) => {
                if let Ok(peer_msg) = bincode::deserialize::<PeerMessage>(&data) {
                    match peer_msg {
                        PeerMessage::FullSyncResponse { .. } => {
                            // Handle FullSyncResponse - process it through the service
                            info!("Received FullSyncResponse via outbound connection");
                            if let Some(_) = handle_clone.process_peer_message(peer_msg, peer_addr_clone.clone()).await {
                                // FullSyncResponse processing is handled internally
                            }
                        }
                        _ => {
                            // Other peer messages - ignore (handled via inbound connection)
                            debug!("Ignoring received binary peer message from outbound peer (handled via inbound connection)");
                        }
                    }
                } else {
                    // Not a peer message - ignore
                    debug!("Ignoring received binary data from outbound peer (not a peer message)");
                }
            }
            Ok(Message::Close(_)) => break,
            Err(_) => break,
            _ => {}
        }
    }
    
    sender_task.abort();
    handle_clone.outbound_peer_disconnected(peer_addr_clone);
    
    Ok(())
}