use std::net::ToSocketAddrs;
use std::collections::{HashMap, VecDeque};
use std::io::{Read, Write};
use mio::net::TcpStream as MioTcpStream;
use tracing::{info, warn, error, debug};
use anyhow::Result;
use serde::{Serialize, Deserialize};
use std::thread;
use crossbeam::channel::{Sender, Receiver, unbounded};
use qlib_rs::now;
use qlib_rs::protocol::{MessageBuffer, ProtocolMessage, ProtocolCodec, PeerStartup, PeerSyncRequest};

/// Configuration for the peer manager
#[derive(Debug, Clone)]
pub struct PeerConfig {
    /// Machine ID (unique identifier for this instance)
    pub machine: String,
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

/// Result of an asynchronous connection attempt
#[derive(Debug)]
pub enum ConnectionResult {
    OutboundSuccess {
        peer_addr: String,
        stream: MioTcpStream,
    },
    OutboundFailure {
        peer_addr: String,
        error: String,
    },
}

/// Request for an outbound connection attempt
#[derive(Debug)]
enum ConnectionRequest {
    OutboundConnection {
        peer_addr: String,
    },
}

/// Connection state for peer connections
#[derive(Debug)]
enum PeerConnectionState {
    Connected(MioTcpStream),
}

/// Information about a peer connection
#[derive(Debug)]
pub struct PeerConnection {
    pub addr_string: String,
    pub state: PeerConnectionState,
    pub outbound_messages: VecDeque<Vec<u8>>, // Raw bytes for TCP
    pub message_buffer: MessageBuffer,        // For handling partial reads
}

impl PeerConnection {
    pub fn new(addr_string: String, stream: MioTcpStream) -> Self {
        Self {
            addr_string,
            state: PeerConnectionState::Connected(stream),
            outbound_messages: VecDeque::new(),
            message_buffer: MessageBuffer::new(),
        }
    }

    pub fn get_stream_mut(&mut self) -> &mut MioTcpStream {
        match &mut self.state {
            PeerConnectionState::Connected(stream) => stream,
        }
    }

    pub fn queue_message(&mut self, message_data: Vec<u8>) {
        self.outbound_messages.push_back(message_data);
    }
}

/// Peer manager that handles peer connections and leader election logic
/// This is designed to be called from the main event loop, not run in its own thread
pub struct PeerManager {
    config: PeerConfig,
    startup_time: u64,
    availability_state: AvailabilityState,
    is_leader: bool,
    current_leader: Option<String>,
    is_fully_synced: bool,
    became_unavailable_at: Option<u64>,
    full_sync_request_pending: bool,
    peer_info: HashMap<String, PeerInfo>,
    
    // Dependencies on other services
    snapshot_handle: Option<crate::snapshot::SnapshotHandle>,
    core_handle: Option<crate::core::CoreHandle>,
    
    // Timing for periodic operations
    last_leadership_check: std::time::Instant,
    last_reconnect_attempt: std::time::Instant,
    
    // Connection worker communication
    connection_request_sender: Sender<ConnectionRequest>,
    connection_result_receiver: Receiver<ConnectionResult>,
}

const LEADERSHIP_CHECK_INTERVAL_SECS: u64 = 1;

/// Worker function that handles all outbound connection attempts in a single thread
fn connection_worker(
    connection_request_receiver: Receiver<ConnectionRequest>,
    connection_result_sender: Sender<ConnectionResult>,
) {
    debug!("Connection worker thread started");
    
    while let Ok(request) = connection_request_receiver.recv() {
        match request {
            ConnectionRequest::OutboundConnection { peer_addr } => {
                debug!(peer_addr = %peer_addr, "Processing outbound connection request");
                
                // Try to resolve the address (handles both IP addresses and hostnames like localhost)
                let socket_addr = match peer_addr.to_socket_addrs() {
                    Ok(mut addrs) => addrs.next(),
                    Err(e) => {
                        let _ = connection_result_sender.send(ConnectionResult::OutboundFailure {
                            peer_addr: peer_addr.clone(),
                            error: format!("Address resolution failed: {}", e),
                        });
                        continue;
                    }
                };
                
                if let Some(addr) = socket_addr {
                    match MioTcpStream::connect(addr) {
                        Ok(stream) => {
                            let _ = connection_result_sender.send(ConnectionResult::OutboundSuccess {
                                peer_addr,
                                stream,
                            });
                        }
                        Err(e) => {
                            let _ = connection_result_sender.send(ConnectionResult::OutboundFailure {
                                peer_addr,
                                error: format!("TCP connection failed: {}", e),
                            });
                        }
                    }
                } else {
                    let _ = connection_result_sender.send(ConnectionResult::OutboundFailure {
                        peer_addr,
                        error: "No socket addresses resolved".to_string(),
                    });
                }
            }
        }
    }
    
    debug!("Connection worker thread exiting");
}

impl PeerManager {
    pub fn new(config: PeerConfig) -> Self {
        let now_instant = std::time::Instant::now();
        
        // Create channel for connection results from background threads
        let (connection_result_sender, connection_result_receiver) = unbounded();
        
        // Create channel for connection requests to the connection worker thread
        let (connection_request_sender, connection_request_receiver) = unbounded();
        
        // Spawn connection worker thread
        thread::spawn(move || {
            connection_worker(connection_request_receiver, connection_result_sender);
        });
        
        Self {
            config,
            startup_time: now().unix_timestamp() as u64,
            availability_state: AvailabilityState::Unavailable,
            is_leader: false,
            current_leader: None,
            is_fully_synced: false,
            became_unavailable_at: None,
            full_sync_request_pending: false,
            peer_info: HashMap::new(),
            snapshot_handle: None,
            core_handle: None,
            last_leadership_check: now_instant,
            last_reconnect_attempt: now_instant,
            connection_request_sender,
            connection_result_receiver,
        }
    }

    /// Set snapshot handle for dependencies
    pub fn set_snapshot_handle(&mut self, handle: crate::snapshot::SnapshotHandle) {
        self.snapshot_handle = Some(handle);
    }

    /// Set core handle for dependencies
    pub fn set_core_handle(&mut self, handle: crate::core::CoreHandle) {
        self.core_handle = Some(handle);
    }

    /// Send sync message to peers (called by core service)
    pub fn send_sync_message(&self, requests: Vec<qlib_rs::Request>) -> Vec<(String, Vec<u8>)> {
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
            // Convert to protocol message and encode
            let protocol_message = ProtocolMessage::PeerSyncRequest(
                PeerSyncRequest {
                    requests_data: bincode::serialize(&requests_to_sync).unwrap_or_default(),
                }
            );
            
            if let Ok(encoded_data) = ProtocolCodec::encode(&protocol_message) {
                // Return message for all peer connections
                return vec![("*".to_string(), encoded_data)]; // "*" means send to all peers
            }
        }
        
        vec![]
    }

    /// Get leadership information
    pub fn get_leadership_info(&self) -> (bool, Option<String>) {
        (self.is_leader, self.current_leader.clone())
    }

    /// Handle periodic operations (should be called regularly from main loop)
    /// Returns (outbound_connection_results, needed_peer_addrs_for_existing_connections)
    pub fn handle_periodic_operations(&mut self, existing_peer_connections: &[String]) -> (Vec<ConnectionResult>, Vec<String>) {
        let mut connection_results = Vec::new();
        let mut needed_connections = Vec::new();
        let now = std::time::Instant::now();
        
        // Process any connection results from the worker thread
        while let Ok(result) = self.connection_result_receiver.try_recv() {
            connection_results.push(result);
        }
        
        // Leadership check
        if now.duration_since(self.last_leadership_check) >= std::time::Duration::from_secs(LEADERSHIP_CHECK_INTERVAL_SECS) {
            self.check_leader_election_and_sync();
            self.last_leadership_check = now;
        }
        
        // Reconnect attempts
        let reconnect_interval = std::time::Duration::from_secs(self.config.peer_reconnect_interval_secs);
        if now.duration_since(self.last_reconnect_attempt) >= reconnect_interval {
            needed_connections = self.get_needed_outbound_connections(existing_peer_connections);
            
            // Queue connection requests for missing peers
            for peer_addr in &needed_connections {
                if let Err(e) = self.connection_request_sender.send(ConnectionRequest::OutboundConnection {
                    peer_addr: peer_addr.clone(),
                }) {
                    warn!(peer_addr = %peer_addr, error = %e, "Failed to queue outbound connection request");
                }
            }
            
            self.last_reconnect_attempt = now;
        }
        
        (connection_results, needed_connections)
    }

    /// Handle a new peer connection
    pub fn handle_new_peer_connection(&mut self, peer_addr: String, stream: MioTcpStream) -> PeerConnection {
        info!(peer_addr = %peer_addr, "New peer connection established");
        
        let connection = PeerConnection::new(peer_addr.clone(), stream);
        
        // Note: The caller should send our startup message after registering the connection
        connection
    }
    
    /// Get our startup message for sending to peers
    pub fn get_startup_message(&self) -> Vec<u8> {
        let startup_message = PeerMessage::Startup {
            machine_id: self.config.machine.clone(),
            startup_time: self.startup_time,
        };
        
        self.encode_peer_message(startup_message).unwrap_or_default()
    }

    /// Handle peer connection read events
    pub fn handle_peer_read(&mut self, connection: &mut PeerConnection) -> Result<Vec<PeerMessage>> {
        let mut messages_to_process = Vec::new();
        
        // Try to read data into the message buffer
        let mut temp_buffer = [0u8; 8192];
        loop {
            match connection.get_stream_mut().read(&mut temp_buffer) {
                Ok(0) => {
                    debug!(peer_addr = %connection.addr_string, "Peer connection closed");
                    return Err(anyhow::anyhow!("Connection closed"));
                }
                Ok(bytes_read) => {
                    connection.message_buffer.add_data(&temp_buffer[..bytes_read]);
                    
                    // Try to decode messages
                    while let Some(message) = connection.message_buffer.try_decode()? {
                        match message {
                            ProtocolMessage::PeerStartup(startup) => {
                                messages_to_process.push(PeerMessage::Startup {
                                    machine_id: startup.machine_id,
                                    startup_time: startup.startup_time,
                                });
                            }
                            ProtocolMessage::PeerFullSyncRequest { machine_id } => {
                                messages_to_process.push(PeerMessage::FullSyncRequest { machine_id });
                            }
                            ProtocolMessage::PeerFullSyncResponse { snapshot } => {
                                messages_to_process.push(PeerMessage::FullSyncResponse { snapshot });
                            }
                            ProtocolMessage::PeerSyncRequest(sync_req) => {
                                if let Ok(requests) = bincode::deserialize::<Vec<qlib_rs::Request>>(&sync_req.requests_data) {
                                    messages_to_process.push(PeerMessage::SyncRequest { requests });
                                }
                            }
                            _ => {
                                warn!(peer_addr = %connection.addr_string, "Received non-peer message on peer connection");
                            }
                        }
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // No more data available
                    break;
                }
                Err(e) => {
                    error!(peer_addr = %connection.addr_string, error = %e, "Error reading from peer connection");
                    return Err(anyhow::anyhow!("Read error: {}", e));
                }
            }
        }
        
        Ok(messages_to_process)
    }

    /// Handle peer connection write events
    pub fn handle_peer_write(&mut self, connection: &mut PeerConnection) -> Result<()> {
        // Send any pending outbound messages
        while let Some(message_data) = connection.outbound_messages.pop_front() {
            match connection.get_stream_mut().write(&message_data) {
                Ok(bytes_written) => {
                    if bytes_written < message_data.len() {
                        // Partial write - put the remaining data back at the front
                        let remaining = message_data[bytes_written..].to_vec();
                        connection.outbound_messages.push_front(remaining);
                        break; // Can't write more right now
                    }
                    // Full message sent
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // Can't write right now - put the message back
                    connection.outbound_messages.push_front(message_data);
                    break;
                }
                Err(e) => {
                    error!(peer_addr = %connection.addr_string, error = %e, "Error writing to peer connection");
                    return Err(anyhow::anyhow!("Write error: {}", e));
                }
            }
        }
        
        Ok(())
    }

    /// Process a peer message
    pub fn handle_peer_message(&mut self, message: PeerMessage, peer_addr: &str) -> Vec<(String, Vec<u8>)> {
        let mut outbound_messages = Vec::new();
        
        match message {
            PeerMessage::Startup { machine_id, startup_time } => {
                info!(peer_addr = %peer_addr, machine_id = %machine_id, startup_time = %startup_time, "Received startup message from peer");
                
                // Update peer info
                self.peer_info.insert(peer_addr.to_string(), PeerInfo { machine_id: machine_id.clone(), startup_time });
                
                // Re-determine leadership
                self.determine_leadership();
                
                // Send our startup message back
                let our_startup_message = PeerMessage::Startup {
                    machine_id: self.config.machine.clone(),
                    startup_time: self.startup_time,
                };
                
                if let Some(message_data) = self.encode_peer_message(our_startup_message) {
                    outbound_messages.push((peer_addr.to_string(), message_data));
                }
            }
            PeerMessage::FullSyncRequest { machine_id } => {
                if self.is_leader {
                    info!(requesting_machine = %machine_id, "Received full sync request, sending snapshot");
                    
                    if let Some(ref snapshot_handle) = self.snapshot_handle {
                        if let Ok(Some((snapshot, _))) = snapshot_handle.load_latest() {
                            let response = PeerMessage::FullSyncResponse { snapshot };
                            if let Some(message_data) = self.encode_peer_message(response) {
                                outbound_messages.push((peer_addr.to_string(), message_data));
                            }
                        } else {
                            warn!("No snapshot available for full sync response");
                        }
                    } else {
                        error!("No snapshot handle available for full sync response");
                    }
                } else {
                    warn!(requesting_machine = %machine_id, "Received full sync request but we're not the leader");
                }
            }
            PeerMessage::FullSyncResponse { snapshot } => {
                info!(peer_addr = %peer_addr, "Received full sync response from peer");
                
                if let Some(ref core_handle) = self.core_handle {
                    match core_handle.restore_snapshot(snapshot) {
                        Ok(()) => {
                            info!("Successfully applied full sync snapshot");
                            self.is_fully_synced = true;
                            self.availability_state = AvailabilityState::Available;
                            self.became_unavailable_at = None;
                            self.full_sync_request_pending = false;
                        }
                        Err(e) => {
                            error!(error = %e, "Failed to apply full sync snapshot");
                        }
                    }
                } else {
                    error!("No core handle available for snapshot restoration");
                }
            }
            PeerMessage::SyncRequest { requests } => {
                debug!(peer_addr = %peer_addr, request_count = %requests.len(), "Received sync request from peer");
                
                if let Some(ref core_handle) = self.core_handle {
                    for request in requests {
                        if let Err(e) = core_handle.write_request(request) {
                            error!(error = %e, "Failed to process sync request");
                        }
                    }
                } else {
                    error!("No core handle available for sync request processing");
                }
            }
        }
        
        outbound_messages
    }

    /// Remove peer connection and handle leader changes
    pub fn handle_peer_disconnection(&mut self, peer_addr: &str) {
        info!(peer_addr = %peer_addr, "Handling peer disconnection");
        
        // Check if the disconnected peer was the current leader
        let disconnected_machine_id = self.peer_info.get(peer_addr)
            .map(|info| info.machine_id.clone());
        
        // Clean up peer info
        self.peer_info.remove(peer_addr);
        
        // Check if the disconnected peer was the current leader
        if let Some(disconnected_machine_id) = disconnected_machine_id {
            if let Some(ref current_leader) = self.current_leader {
                if *current_leader == disconnected_machine_id {
                    info!(disconnected_leader = %disconnected_machine_id, "Current leader disconnected, re-determining leadership");
                    self.determine_leadership();
                }
            }
        }
    }

    /// Get a list of peer addresses that need outbound connections
    fn get_needed_outbound_connections(&self, existing_peer_connections: &[String]) -> Vec<String> {
        self.config.peer_addresses.iter()
            .filter(|&peer_addr| !existing_peer_connections.contains(peer_addr))
            .cloned()
            .collect()
    }

    /// Check leader election and sync logic
    fn check_leader_election_and_sync(&mut self) {
        let mut leadership_changed = false;
        
        // Self-promotion logic
        if !self.is_leader && 
           (self.config.peer_addresses.is_empty() || self.peer_info.is_empty()) {
            
            let current_time = now().unix_timestamp() as u64;
            let time_since_startup = current_time.saturating_sub(self.startup_time);
            
            if time_since_startup >= self.config.self_promotion_delay_secs {
                info!(
                    time_since_startup = time_since_startup,
                    self_promotion_delay = self.config.self_promotion_delay_secs,
                    "No peers available and self-promotion delay elapsed, promoting to leader"
                );
                
                self.is_leader = true;
                self.current_leader = Some(self.config.machine.clone());
                self.is_fully_synced = true;
                self.availability_state = AvailabilityState::Available;
                self.became_unavailable_at = None;
                leadership_changed = true;
            }
        }
        
        // Only re-determine leadership if there are actual peers connected or we're not yet leader
        if !self.peer_info.is_empty() || !self.is_leader || leadership_changed {
            self.determine_leadership();
        }
        
        // Full sync request logic
        if matches!(self.availability_state, AvailabilityState::Unavailable) &&
           !self.is_leader &&
           !self.is_fully_synced &&
           !self.full_sync_request_pending {
            
            if let Some(became_unavailable_at) = self.became_unavailable_at {
                let current_time = now().unix_timestamp() as u64;
                let time_since_unavailable = current_time.saturating_sub(became_unavailable_at);
                
                if time_since_unavailable >= self.config.full_sync_grace_period_secs {
                    if let Some(ref current_leader) = self.current_leader {
                        self.send_full_sync_request(current_leader.clone());
                    }
                }
            }
        }
    }

    /// Determine leadership based on startup times and machine IDs
    fn determine_leadership(&mut self) {
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
            self.became_unavailable_at = Some(now().unix_timestamp() as u64);
            
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
            if let Some(ref core_handle) = self.core_handle {
                if let Err(e) = core_handle.force_disconnect_all_clients() {
                    error!(error = %e, "Failed to force disconnect clients");
                }
            }
        }
    }

    /// Send full sync request to leader
    fn send_full_sync_request(&mut self, leader_machine_id: String) {
        info!(
            leader_machine_id = %leader_machine_id,
            "Grace period expired, sending FullSyncRequest to leader"
        );
        
        self.full_sync_request_pending = true;
        
        // This will be handled by the caller to send to the appropriate connection
        // For now, we just mark the request as pending
    }

    /// Encode a peer message for transmission
    fn encode_peer_message(&self, message: PeerMessage) -> Option<Vec<u8>> {
        // Convert PeerMessage to appropriate ProtocolMessage variant
        let protocol_message = match message {
            PeerMessage::Startup { machine_id, startup_time } => {
                ProtocolMessage::PeerStartup(PeerStartup {
                    machine_id,
                    startup_time,
                })
            }
            PeerMessage::FullSyncRequest { machine_id } => {
                ProtocolMessage::PeerFullSyncRequest { machine_id }
            }
            PeerMessage::FullSyncResponse { snapshot } => {
                ProtocolMessage::PeerFullSyncResponse { snapshot }
            }
            PeerMessage::SyncRequest { requests } => {
                // Serialize requests for transport
                if let Ok(requests_data) = bincode::serialize(&requests) {
                    ProtocolMessage::PeerSyncRequest(PeerSyncRequest { requests_data })
                } else {
                    error!("Failed to serialize sync requests");
                    return None;
                }
            }
        };
        
        // Encode the protocol message
        match ProtocolCodec::encode(&protocol_message) {
            Ok(encoded_data) => Some(encoded_data),
            Err(e) => {
                error!(error = %e, "Failed to encode peer message");
                None
            }
        }
    }

    /// Get the current availability state
    pub fn get_availability_state(&self) -> &AvailabilityState {
        &self.availability_state
    }

    /// Check if this instance is the leader
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}