use crossbeam::channel::{Sender, Receiver, bounded, unbounded};
use qlib_rs::now;
use std::net::{TcpListener, TcpStream};
use tungstenite::{accept, connect, Message, WebSocket};
use std::time::Duration;
use tracing::{info, warn, error, debug, instrument};
use anyhow::Result;
use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use std::thread;
use std::sync::{Arc, Mutex};

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
    },
    GetAvailabilityState,
    GetLeadershipInfo,
    IsOutboundPeerConnected {
        peer_addr: String,
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
        sender: Sender<Message>,
    },
    OutboundPeerDisconnected {
        peer_addr: String,
    },
    ProcessPeerMessage {
        peer_msg: PeerMessage,
        peer_addr: String,
    },
    SetSnapshotHandle {
        handle: crate::snapshot::SnapshotHandle,
    },
    TakeSnapshot,
    RestoreSnapshot {
        snapshot: qlib_rs::Snapshot,
    },
}

/// Response types for peer requests
#[derive(Debug)]
pub enum PeerResponse {
    Unit,
    AvailabilityState(AvailabilityState),
    LeadershipInfo(bool, Option<String>), // (is_leader, current_leader)
    Bool(bool),
    PeerMessage(Option<PeerMessage>),
    Snapshot(Option<qlib_rs::Snapshot>),
}

/// Handle for communicating with peer service
#[derive(Debug, Clone)]
pub struct PeerHandle {
    request_sender: Sender<(PeerRequest, Sender<PeerResponse>)>,
}

impl PeerHandle {
    pub fn send_sync_message(&self, requests: Vec<qlib_rs::Request>) {
        let (response_tx, _response_rx) = unbounded();
        if let Err(e) = self.request_sender.send((PeerRequest::SendSyncMessage { requests }, response_tx)) {
            error!(error = %e, "Failed to send SendSyncMessage request");
        }
    }

    pub fn get_availability_state(&self) -> AvailabilityState {
        let (response_tx, response_rx) = unbounded();
        if let Ok(_) = self.request_sender.send((PeerRequest::GetAvailabilityState, response_tx)) {
            if let Ok(PeerResponse::AvailabilityState(state)) = response_rx.recv() {
                return state;
            }
        }
        AvailabilityState::Unavailable
    }

    pub fn get_leadership_info(&self) -> (bool, Option<String>) {
        let (response_tx, response_rx) = unbounded();
        if let Ok(_) = self.request_sender.send((PeerRequest::GetLeadershipInfo, response_tx)) {
            if let Ok(PeerResponse::LeadershipInfo(is_leader, current_leader)) = response_rx.recv() {
                return (is_leader, current_leader);
            }
        }
        (false, None)
    }

    pub fn peer_connected(&self, peer_addr: String, machine_id: String, startup_time: u64) {
        let (response_tx, _response_rx) = unbounded();
        if let Err(e) = self.request_sender.send((PeerRequest::PeerConnected {
            peer_addr,
            machine_id,
            startup_time,
        }, response_tx)) {
            error!(error = %e, "Failed to send PeerConnected request");
        }
    }

    pub fn peer_disconnected(&self, peer_addr: String) {
        let (response_tx, _response_rx) = unbounded();
        if let Err(e) = self.request_sender.send((PeerRequest::PeerDisconnected { peer_addr }, response_tx)) {
            error!(error = %e, "Failed to send PeerDisconnected request");
        }
    }

    pub fn outbound_peer_connected(&self, peer_addr: String, sender: Sender<Message>) {
        let (response_tx, _response_rx) = unbounded();
        if let Err(e) = self.request_sender.send((PeerRequest::OutboundPeerConnected {
            peer_addr,
            sender,
        }, response_tx)) {
            error!(error = %e, "Failed to send OutboundPeerConnected request");
        }
    }

    pub fn outbound_peer_disconnected(&self, peer_addr: String) {
        let (response_tx, _response_rx) = unbounded();
        if let Err(e) = self.request_sender.send((PeerRequest::OutboundPeerDisconnected { peer_addr }, response_tx)) {
            error!(error = %e, "Failed to send OutboundPeerDisconnected request");
        }
    }

    pub fn process_peer_message(&self, peer_msg: PeerMessage, peer_addr: String) -> Option<PeerMessage> {
        let (response_tx, response_rx) = unbounded();
        if let Ok(_) = self.request_sender.send((PeerRequest::ProcessPeerMessage {
            peer_msg,
            peer_addr,
        }, response_tx)) {
            if let Ok(PeerResponse::PeerMessage(response)) = response_rx.recv() {
                return response;
            }
        }
        None
    }

    /// Check if we already have an outbound connection to a peer
    pub fn is_outbound_peer_connected(&self, peer_addr: &str) -> bool {
        let (response_tx, response_rx) = unbounded();
        if let Ok(_) = self.request_sender.send((PeerRequest::IsOutboundPeerConnected {
            peer_addr: peer_addr.to_string(),
        }, response_tx)) {
            if let Ok(PeerResponse::Bool(connected)) = response_rx.recv() {
                return connected;
            }
        }
        false
    }

    /// Set snapshot handle for dependencies
    pub fn set_snapshot_handle(&self, handle: crate::snapshot::SnapshotHandle) {
        let (response_tx, _response_rx) = unbounded();
        if let Err(e) = self.request_sender.send((PeerRequest::SetSnapshotHandle { handle }, response_tx)) {
            error!(error = %e, "Failed to send SetSnapshotHandle request");
        }
    }
    
    /// Take a snapshot
    pub fn take_snapshot(&self) -> Option<qlib_rs::Snapshot> {
        let (response_tx, response_rx) = unbounded();
        if let Ok(_) = self.request_sender.send((PeerRequest::TakeSnapshot, response_tx)) {
            if let Ok(PeerResponse::Snapshot(snapshot)) = response_rx.recv() {
                return snapshot;
            }
        }
        None
    }
    
    /// Restore from snapshot
    pub fn restore_snapshot(&self, snapshot: qlib_rs::Snapshot) {
        let (response_tx, _response_rx) = unbounded();
        if let Err(e) = self.request_sender.send((PeerRequest::RestoreSnapshot { snapshot }, response_tx)) {
            error!(error = %e, "Failed to send RestoreSnapshot request");
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
    connected_outbound_peers: HashMap<String, Sender<Message>>,
    snapshot_handle: Option<crate::snapshot::SnapshotHandle>,
}

impl PeerService {
    pub fn spawn(config: PeerConfig) -> PeerHandle {
        let (request_sender, request_receiver) = bounded(1024);
        
        let handle = PeerHandle {
            request_sender: request_sender.clone(),
        };
        
        // Spawn the service thread
        let service_config = config.clone();
        thread::spawn(move || {
            let mut service = Self {
                config: service_config.clone(),
                startup_time: now(),
                availability_state: AvailabilityState::Unavailable,
                is_leader: false,
                current_leader: None,
                is_fully_synced: false,
                became_unavailable_at: None,
                full_sync_request_pending: false,
                peer_info: HashMap::new(),
                connected_outbound_peers: HashMap::new(),
                snapshot_handle: None,
            };
            
            // Start the inbound server thread
            let inbound_handle = handle.clone();
            let inbound_config = service_config.clone();
            thread::spawn(move || {
                if let Err(e) = start_inbound_peer_server(inbound_config, inbound_handle) {
                    error!(error = %e, "Inbound peer server failed");
                }
            });
            
            // Start the outbound connection manager thread  
            let outbound_handle = handle.clone();
            let outbound_config = service_config.clone();
            thread::spawn(move || {
                if let Err(e) = manage_outbound_peer_connections(outbound_config, outbound_handle) {
                    error!(error = %e, "Outbound peer connection manager failed");
                }
            });
            
            // Main service loop
            let leadership_check_interval = Duration::from_secs(1);
            let mut last_leadership_check = std::time::Instant::now();
            
            loop {
                // Handle requests with timeout
                let timeout = Duration::from_millis(100);
                match request_receiver.recv_timeout(timeout) {
                    Ok((request, response_sender)) => {
                        let response = service.handle_request(request);
                        if let Err(_) = response_sender.send(response) {
                            error!("Failed to send response back to caller");
                        }
                    }
                    Err(crossbeam::channel::RecvTimeoutError::Timeout) => {
                        // Handle periodic tasks
                    }
                    Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                        warn!("Peer service request channel disconnected");
                        break;
                    }
                }
                
                // Periodic leadership check
                let now = std::time::Instant::now();
                if now.duration_since(last_leadership_check) >= leadership_check_interval {
                    service.check_leader_election_and_sync();
                    last_leadership_check = now;
                }
            }
            
            error!("Peer service has stopped unexpectedly");
        });

        handle
    }
    
    fn handle_request(&mut self, request: PeerRequest) -> PeerResponse {
        match request {
            PeerRequest::SendSyncMessage { requests } => {
                self.send_sync_message_to_peers(requests);
                PeerResponse::Unit
            }
            PeerRequest::GetAvailabilityState => {
                PeerResponse::AvailabilityState(self.availability_state.clone())
            }
            PeerRequest::GetLeadershipInfo => {
                PeerResponse::LeadershipInfo(self.is_leader, self.current_leader.clone())
            }
            PeerRequest::IsOutboundPeerConnected { peer_addr } => {
                PeerResponse::Bool(self.connected_outbound_peers.contains_key(&peer_addr))
            }
            PeerRequest::PeerConnected { peer_addr, machine_id, startup_time } => {
                self.handle_peer_connected(peer_addr, machine_id, startup_time);
                PeerResponse::Unit
            }
            PeerRequest::PeerDisconnected { peer_addr } => {
                self.handle_peer_disconnected(peer_addr);
                PeerResponse::Unit
            }
            PeerRequest::OutboundPeerConnected { peer_addr, sender } => {
                self.connected_outbound_peers.insert(peer_addr, sender);
                PeerResponse::Unit
            }
            PeerRequest::OutboundPeerDisconnected { peer_addr } => {
                self.connected_outbound_peers.remove(&peer_addr);
                PeerResponse::Unit
            }
            PeerRequest::ProcessPeerMessage { peer_msg, peer_addr } => {
                let response = self.process_peer_message_internal(peer_msg, peer_addr);
                PeerResponse::PeerMessage(response)
            }
            PeerRequest::SetSnapshotHandle { handle } => {
                self.snapshot_handle = Some(handle);
                PeerResponse::Unit
            }
            PeerRequest::TakeSnapshot => {
                let snapshot = if let Some(handle) = &self.snapshot_handle {
                    // For now, return None - in a full implementation this would
                    // coordinate with the core service to take a snapshot
                    None
                } else {
                    None
                };
                PeerResponse::Snapshot(snapshot)
            }
            PeerRequest::RestoreSnapshot { snapshot } => {
                // For now, just log - in a full implementation this would
                // coordinate with the core service to restore the snapshot
                info!("Received snapshot restore request");
                PeerResponse::Unit
            }
        }
    }
    
    fn check_leader_election_and_sync(&mut self) {
        // Check if we need to sync
        if !self.is_fully_synced && !self.full_sync_request_pending {
            if let Some(became_unavailable_at) = self.became_unavailable_at {
                let grace_period_elapsed = (now() - became_unavailable_at) >= self.config.full_sync_grace_period_secs;
                
                if grace_period_elapsed {
                    if let Some(leader) = &self.current_leader {
                        self.send_full_sync_request(leader.clone());
                    }
                }
            }
        }
        
        // Update leadership
        self.determine_leadership();
        
        // Update availability state
        if self.is_leader || self.is_fully_synced {
            if self.availability_state == AvailabilityState::Unavailable {
                info!("Transitioning to Available state");
                self.availability_state = AvailabilityState::Available;
            }
        }
    }
    
    fn handle_peer_connected(&mut self, peer_addr: String, machine_id: String, startup_time: u64) {
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
        
        self.determine_leadership();
    }
    
    fn handle_peer_disconnected(&mut self, peer_addr: String) {
        info!(
            peer_addr = %peer_addr,
            "Peer disconnected, updating leadership"
        );
        
        self.peer_info.remove(&peer_addr);
        self.connected_outbound_peers.remove(&peer_addr);
        
        self.determine_leadership();
    }
    
    fn determine_leadership(&mut self) {
        // Include ourselves in the leadership calculation
        let mut all_peers = self.peer_info.values().cloned().collect::<Vec<_>>();
        all_peers.push(PeerInfo {
            machine_id: self.config.machine.clone(),
            startup_time: self.startup_time,
        });
        
        // Sort by startup time (earliest first), then by machine ID for tie-breaking
        all_peers.sort_by(|a, b| {
            match a.startup_time.cmp(&b.startup_time) {
                std::cmp::Ordering::Equal => a.machine_id.cmp(&b.machine_id),
                other => other,
            }
        });
        
        let was_leader = self.is_leader;
        let previous_leader = self.current_leader.clone();
        
        if let Some(leader_peer) = all_peers.first() {
            self.current_leader = Some(leader_peer.machine_id.clone());
            self.is_leader = leader_peer.machine_id == self.config.machine;
            
            if self.is_leader && !was_leader {
                info!(
                    machine_id = %self.config.machine,
                    startup_time = self.startup_time,
                    "Became leader"
                );
                
                // Immediately consider ourselves fully synced when we become leader
                self.is_fully_synced = true;
                self.became_unavailable_at = None;
                self.full_sync_request_pending = false;
            } else if !self.is_leader && was_leader {
                info!(
                    machine_id = %self.config.machine,
                    new_leader = %leader_peer.machine_id,
                    "No longer leader"
                );
                
                // Mark as not fully synced when we lose leadership
                self.is_fully_synced = false;
                self.became_unavailable_at = Some(now());
            } else if !self.is_leader && previous_leader != self.current_leader {
                info!(
                    new_leader = %leader_peer.machine_id,
                    "Leader changed"
                );
                
                // Reset sync state when leader changes
                self.is_fully_synced = false;
                self.became_unavailable_at = Some(now());
                self.full_sync_request_pending = false;
            }
        } else {
            // No peers, wait for self-promotion delay before becoming leader
            let startup_elapsed = now() - self.startup_time;
            if startup_elapsed >= self.config.self_promotion_delay_secs {
                if !self.is_leader {
                    info!(
                        machine_id = %self.config.machine,
                        startup_time = self.startup_time,
                        "Self-promoting to leader (no peers available)"
                    );
                    
                    self.is_leader = true;
                    self.current_leader = Some(self.config.machine.clone());
                    self.is_fully_synced = true;
                    self.became_unavailable_at = None;
                    self.full_sync_request_pending = false;
                }
            }
        }
    }
    
    fn send_sync_message_to_peers(&self, requests: Vec<qlib_rs::Request>) {
        if requests.is_empty() {
            return;
        }
        
        let sync_message = PeerMessage::SyncRequest { requests };
        
        // Serialize the message
        let message_text = match serde_json::to_string(&sync_message) {
            Ok(text) => text,
            Err(e) => {
                error!(error = %e, "Failed to serialize sync message");
                return;
            }
        };
        
        let message = Message::Text(message_text);
        
        // Send to all connected outbound peers
        for (peer_addr, sender) in &self.connected_outbound_peers {
            if let Err(e) = sender.send(message.clone()) {
                error!(
                    peer_addr = %peer_addr,
                    error = %e,
                    "Failed to send sync message to peer"
                );
            }
        }
        
        debug!(
            peer_count = self.connected_outbound_peers.len(),
            "Sent sync message to peers"
        );
    }
    
    fn send_full_sync_request(&mut self, leader_machine_id: String) {
        info!(
            leader = %leader_machine_id,
            "Sending full sync request to leader"
        );
        
        let sync_request = PeerMessage::FullSyncRequest {
            machine_id: self.config.machine.clone(),
        };
        
        let message_text = match serde_json::to_string(&sync_request) {
            Ok(text) => text,
            Err(e) => {
                error!(error = %e, "Failed to serialize full sync request");
                return;
            }
        };
        
        let message = Message::Text(message_text);
        
        // Find the peer connection for the leader and send the request
        for (peer_addr, sender) in &self.connected_outbound_peers {
            // TODO: We need a way to map machine_id to peer_addr
            // For now, send to all peers (leader will respond, others will ignore)
            if let Err(e) = sender.send(message.clone()) {
                error!(
                    peer_addr = %peer_addr,
                    error = %e,
                    "Failed to send full sync request to peer"
                );
            }
        }
        
        self.full_sync_request_pending = true;
    }

    fn process_peer_message_internal(&mut self, peer_msg: PeerMessage, _peer_addr: String) -> Option<PeerMessage> {
        match peer_msg {
            PeerMessage::Startup { .. } => {
                // Startup messages are handled by the connection management code
                None
            }
            
            PeerMessage::SyncRequest { requests } => {
                debug!(
                    request_count = requests.len(),
                    "Processing sync request from peer"
                );
                
                // Apply the requests to our store if we're available
                if self.availability_state == AvailabilityState::Available {
                    // TODO: Forward to core service for processing
                    info!("Would process sync requests in core service");
                }
                
                None
            }
            
            PeerMessage::FullSyncRequest { machine_id } => {
                info!(
                    requester = %machine_id,
                    "Processing full sync request"
                );
                
                // Only respond if we're the leader
                if self.is_leader {
                    // TODO: Get snapshot from core service
                    if let Some(snapshot) = self.take_snapshot() {
                        Some(PeerMessage::FullSyncResponse { snapshot })
                    } else {
                        error!("Failed to take snapshot for full sync response");
                        None
                    }
                } else {
                    debug!("Ignoring full sync request (not leader)");
                    None
                }
            }
            
            PeerMessage::FullSyncResponse { snapshot } => {
                if self.full_sync_request_pending {
                    info!("Received full sync response, applying snapshot");
                    
                    // TODO: Forward to core service for restoration
                    // self.restore_snapshot(snapshot);
                    
                    self.is_fully_synced = true;
                    self.became_unavailable_at = None;
                    self.full_sync_request_pending = false;
                    
                    info!("Full sync completed successfully");
                } else {
                    warn!("Received unexpected full sync response");
                }
                
                None
            }
        }
    }
}

/// Handle a single peer WebSocket connection
fn handle_inbound_peer_connection(
    stream: TcpStream,
    peer_addr: std::net::SocketAddr,
    handle: PeerHandle,
) -> Result<()> {
    info!(peer_addr = %peer_addr, "Accepting inbound peer connection");
    
    let mut websocket = accept(stream)?;
    debug!("WebSocket handshake completed");
    
    loop {
        match websocket.read() {
            Ok(Message::Text(text)) => {
                match serde_json::from_str::<PeerMessage>(&text) {
                    Ok(peer_msg) => {
                        handle_peer_message(peer_msg, &peer_addr, &mut websocket, &handle);
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to deserialize text message from peer");
                    }
                }
            }
            Ok(Message::Binary(data)) => {
                match bincode::deserialize::<PeerMessage>(&data) {
                    Ok(peer_msg) => {
                        handle_peer_message(peer_msg, &peer_addr, &mut websocket, &handle);
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to deserialize binary message from peer");
                    }
                }
            }
            Ok(Message::Ping(payload)) => {
                if let Err(e) = websocket.send(Message::Pong(payload)) {
                    error!(error = %e, "Failed to respond to ping from peer");
                    break;
                }
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

fn handle_peer_message(
    peer_msg: PeerMessage,
    peer_addr: &std::net::SocketAddr,
    websocket: &mut WebSocket<TcpStream>,
    handle: &PeerHandle,
) {
    match &peer_msg {
        PeerMessage::Startup { machine_id, startup_time } => {
            info!(
                remote_machine_id = %machine_id,
                remote_startup_time = startup_time,
                "Processing startup message from peer"
            );
            
            handle.peer_connected(peer_addr.to_string(), machine_id.clone(), *startup_time);
        }
        
        _ => {
            // Process the message and get potential response
            if let Some(response_msg) = handle.process_peer_message(peer_msg, peer_addr.to_string()) {
                match &response_msg {
                    PeerMessage::FullSyncResponse { .. } => {
                        // Use binary serialization for the snapshot since it contains complex key types
                        match bincode::serialize(&response_msg) {
                            Ok(response_binary) => {
                                info!(
                                    response_size = response_binary.len(),
                                    "Sending full sync response (binary)"
                                );
                                
                                if let Err(e) = websocket.send(Message::Binary(response_binary)) {
                                    error!(error = %e, "Failed to send full sync response to peer");
                                }
                            }
                            Err(e) => {
                                error!(error = %e, "Failed to serialize full sync response");
                            }
                        }
                    }
                    
                    _ => {
                        // Use JSON for other message types
                        match serde_json::to_string(&response_msg) {
                            Ok(response_text) => {
                                if let Err(e) = websocket.send(Message::Text(response_text)) {
                                    error!(error = %e, "Failed to send response to peer");
                                }
                            }
                            Err(e) => {
                                error!(error = %e, "Failed to serialize response message");
                            }
                        }
                    }
                }
            }
        }
    }
}

fn start_inbound_peer_server(
    config: PeerConfig,
    handle: PeerHandle,
) -> Result<()> {
    let bind_addr = format!("0.0.0.0:{}", config.peer_port);
    let listener = TcpListener::bind(&bind_addr)?;
    
    info!(bind_address = %bind_addr, "Starting inbound peer server");
    
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let peer_addr = stream.peer_addr()?;
                let handle_clone = handle.clone();
                
                thread::spawn(move || {
                    if let Err(e) = handle_inbound_peer_connection(stream, peer_addr, handle_clone) {
                        error!(peer_addr = %peer_addr, error = %e, "Inbound peer connection failed");
                    }
                });
            }
            Err(e) => {
                error!(error = %e, "Failed to accept inbound connection");
            }
        }
    }
    
    Ok(())
}

fn manage_outbound_peer_connections(config: PeerConfig, handle: PeerHandle) -> Result<()> {
    info!("Starting outbound peer connection manager");
    
    let reconnect_interval = Duration::from_secs(config.peer_reconnect_interval_secs);
    
    loop {
        thread::sleep(reconnect_interval);
        
        for peer_addr in &config.peer_addresses {
            // Check if we already have an active connection to this peer
            if handle.is_outbound_peer_connected(peer_addr) {
                continue;
            }
            
            let peer_addr_clone = peer_addr.clone();
            let handle_clone = handle.clone();
            
            thread::spawn(move || {
                if let Err(e) = handle_outbound_peer_connection(&peer_addr_clone, handle_clone) {
                    error!(peer_addr = %peer_addr_clone, error = %e, "Failed to connect to peer");
                }
            });
        }
    }
}

fn handle_outbound_peer_connection(peer_addr: &str, handle: PeerHandle) -> Result<()> {
    info!(peer_addr = %peer_addr, "Connecting to peer");
    
    let url = format!("ws://{}", peer_addr);
    let (mut websocket, _response) = connect(&url)?;
    
    info!(peer_addr = %peer_addr, "Connected to peer, sending startup message");
    
    // Send startup message
    let startup_msg = PeerMessage::Startup {
        machine_id: "machine_id".to_string(), // TODO: Get from config properly
        startup_time: now(),
    };
    
    let startup_text = serde_json::to_string(&startup_msg)?;
    websocket.send(Message::Text(startup_text))?;
    
    // Create channel for sending messages to this peer
    let (sender, receiver) = unbounded::<Message>();
    handle.outbound_peer_connected(peer_addr.to_string(), sender);
    
    // Spawn a thread to handle outgoing messages
    let peer_addr_clone = peer_addr.to_string();
    let handle_clone = handle.clone();
    thread::spawn(move || {
        while let Ok(message) = receiver.recv() {
            if let Err(e) = websocket.send(message) {
                error!(peer_addr = %peer_addr_clone, error = %e, "Failed to send message to peer");
                break;
            }
        }
        handle_clone.outbound_peer_disconnected(peer_addr_clone);
    });
    
    // Handle incoming messages (this blocks until connection is lost)
    loop {
        match websocket.read() {
            Ok(message) => {
                // Process incoming message
                match message {
                    Message::Text(text) => {
                        if let Ok(peer_msg) = serde_json::from_str::<PeerMessage>(&text) {
                            if let Some(response) = handle.process_peer_message(peer_msg, peer_addr.to_string()) {
                                // Send response back
                                let response_text = serde_json::to_string(&response)?;
                                websocket.send(Message::Text(response_text))?;
                            }
                        }
                    }
                    Message::Binary(data) => {
                        if let Ok(peer_msg) = bincode::deserialize::<PeerMessage>(&data) {
                            if let Some(response) = handle.process_peer_message(peer_msg, peer_addr.to_string()) {
                                // Send response back
                                let response_binary = bincode::serialize(&response)?;
                                websocket.send(Message::Binary(response_binary))?;
                            }
                        }
                    }
                    Message::Close(_) => break,
                    _ => {}
                }
            }
            Err(e) => {
                error!(peer_addr = %peer_addr, error = %e, "WebSocket error");
                break;
            }
        }
    }
    
    handle.outbound_peer_disconnected(peer_addr.to_string());
    info!(peer_addr = %peer_addr, "Outbound peer connection terminated");
    Ok(())
}