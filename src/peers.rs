use crossbeam::channel::{Sender, Receiver, bounded, unbounded};
use crossbeam::queue::SegQueue;
use std::net::ToSocketAddrs;
use std::collections::{HashMap, VecDeque};
use std::time::Duration;
use std::sync::Arc;
use mio::{Poll, Interest, Token, Events};
use mio::net::{TcpListener as MioTcpListener, TcpStream as MioTcpStream};
use tungstenite::{WebSocket, Message};
use tracing::{info, warn, error, debug};
use anyhow::Result;
use serde::{Serialize, Deserialize};
use std::thread;
use qlib_rs::now;

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

/// Result of an asynchronous connection attempt
#[derive(Debug)]
enum ConnectionResult {
    OutboundSuccess {
        peer_addr: String,
        websocket: WebSocket<MioTcpStream>,
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

/// Fire-and-forget peer service requests (no response needed)
#[derive(Debug)]
pub enum PeerCommand {
    SendSyncMessage {
        requests: Vec<qlib_rs::Request>,
    },
    SetSnapshotHandle {
        handle: crate::snapshot::SnapshotHandle,
    },
    SetCoreHandle {
        handle: crate::core::CoreHandle,
    },
}

/// Peer service request types that require responses
#[derive(Debug)]
pub enum PeerRequest {
    GetAvailabilityState,
    GetLeadershipInfo,
}

/// Response types for peer requests
#[derive(Debug)]
pub enum PeerResponse {
    AvailabilityState(AvailabilityState),
    LeadershipInfo(bool, Option<String>), // (is_leader, current_leader)
}

/// Handle for communicating with peer service
#[derive(Debug, Clone)]
pub struct PeerHandle {
    request_sender: Sender<(PeerRequest, Sender<PeerResponse>)>,
    command_queue: Arc<SegQueue<PeerCommand>>,
}

impl PeerHandle {
    pub fn send_sync_message(&self, requests: Vec<qlib_rs::Request>) {
        self.command_queue.push(PeerCommand::SendSyncMessage { requests });
    }

    pub fn get_availability_state(&self) -> AvailabilityState {
        let (response_tx, response_rx) = unbounded();
        if let Err(e) = self.request_sender.send((PeerRequest::GetAvailabilityState, response_tx)) {
            error!(error = %e, "Failed to send availability state request to peer service");
            return AvailabilityState::Unavailable;
        }
        
        match response_rx.recv() {
            Ok(PeerResponse::AvailabilityState(state)) => state,
            _ => {
                error!("Failed to receive availability state response from peer service");
                AvailabilityState::Unavailable
            }
        }
    }

    pub fn get_leadership_info(&self) -> (bool, Option<String>) {
        let (response_tx, response_rx) = unbounded();
        if let Err(e) = self.request_sender.send((PeerRequest::GetLeadershipInfo, response_tx)) {
            error!(error = %e, "Failed to send leadership info request to peer service");
            return (false, None);
        }
        
        match response_rx.recv() {
            Ok(PeerResponse::LeadershipInfo(is_leader, current_leader)) => (is_leader, current_leader),
            _ => {
                error!("Failed to receive leadership info response from peer service");
                (false, None)
            }
        }
    }

    /// Set snapshot handle for dependencies
    pub fn set_snapshot_handle(&self, handle: crate::snapshot::SnapshotHandle) {
        self.command_queue.push(PeerCommand::SetSnapshotHandle { handle });
    }

    /// Set core handle for dependencies
    pub fn set_core_handle(&self, handle: crate::core::CoreHandle) {
        self.command_queue.push(PeerCommand::SetCoreHandle { handle });
    }
}

/// Connection state for peer connections
#[derive(Debug)]
enum PeerConnectionState {
    Connected(WebSocket<MioTcpStream>),
}

/// Information about a peer connection
#[derive(Debug)]
struct PeerConnection {
    addr_string: String,
    state: PeerConnectionState,
    outbound_messages: VecDeque<String>,
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
    snapshot_handle: Option<crate::snapshot::SnapshotHandle>,
    core_handle: Option<crate::core::CoreHandle>,
    
    // Mio-based networking
    poll: Poll,
    listener: MioTcpListener,
    connections: HashMap<Token, PeerConnection>,
    next_token: usize,
    
    // Timing for periodic operations
    last_leadership_check: std::time::Instant,
    last_reconnect_attempt: std::time::Instant,
    
    // Channel for receiving connection results from background threads
    connection_result_receiver: Receiver<ConnectionResult>,
    
    // Channel for sending connection requests to the connection worker thread
    connection_request_sender: Sender<ConnectionRequest>,
}

const LISTENER_TOKEN: Token = Token(0);
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
                    Err(_) => None,
                };
                
                if let Some(addr) = socket_addr {
                    match std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(2)) {
                        Ok(std_stream) => {
                            if let Err(e) = std_stream.set_nonblocking(true) {
                                let _ = connection_result_sender.send(ConnectionResult::OutboundFailure {
                                    peer_addr: peer_addr.clone(),
                                    error: format!("Failed to set non-blocking: {}", e),
                                });
                                continue;
                            }
                            
                            let mio_stream = MioTcpStream::from_std(std_stream);
                            let url = format!("ws://{}", peer_addr);
                            
                            match tungstenite::client(&url, mio_stream) {
                                Ok((websocket, _response)) => {
                                    info!(peer_addr = %peer_addr, "Outbound peer connection established");
                                    let _ = connection_result_sender.send(ConnectionResult::OutboundSuccess {
                                        peer_addr,
                                        websocket,
                                    });
                                }
                                Err(e) => {
                                    debug!(peer_addr = %peer_addr, error = %e, "Failed to establish outbound WebSocket connection");
                                    let _ = connection_result_sender.send(ConnectionResult::OutboundFailure {
                                        peer_addr,
                                        error: format!("WebSocket client handshake failed: {}", e),
                                    });
                                }
                            }
                        }
                        Err(e) => {
                            debug!(peer_addr = %peer_addr, error = %e, "Failed to connect to peer");
                            let _ = connection_result_sender.send(ConnectionResult::OutboundFailure {
                                peer_addr,
                                error: format!("TCP connection failed: {}", e),
                            });
                        }
                    }
                } else {
                    let _ = connection_result_sender.send(ConnectionResult::OutboundFailure {
                        peer_addr,
                        error: "Invalid peer address format".to_string(),
                    });
                }
            }
        }
    }
    
    debug!("Connection worker thread exiting");
}

impl PeerService {
    pub fn spawn(config: PeerConfig) -> PeerHandle {
        let (request_sender, request_receiver) = bounded(1024);
        let command_queue = Arc::new(SegQueue::new());
        
        let handle = PeerHandle { 
            request_sender,
            command_queue: command_queue.clone(),
        };
        
        // Spawn the service thread
        let service_config = config.clone();
        thread::spawn(move || {
            if let Err(e) = Self::run_service(service_config, request_receiver, command_queue) {
                error!(error = %e, "Peer service failed");
            }
            error!("Peer service has stopped unexpectedly");
        });

        handle
    }
    
    fn run_service(
        config: PeerConfig, 
        request_receiver: Receiver<(PeerRequest, Sender<PeerResponse>)>,
        command_queue: Arc<SegQueue<PeerCommand>>,
    ) -> Result<()> {
        let addr = format!("0.0.0.0:{}", config.peer_port).parse()?;
        let mut listener = MioTcpListener::bind(addr)?;
        let poll = Poll::new()?;
        
        poll.registry().register(&mut listener, LISTENER_TOKEN, Interest::READABLE)?;
        
        info!(bind_address = %addr, "Peer service WebSocket server initialized");
        
        // Create channel for connection results from background threads
        let (connection_result_sender, connection_result_receiver) = unbounded();
        
        // Create channel for connection requests to the connection worker thread
        let (connection_request_sender, connection_request_receiver) = unbounded();
        
        // Spawn connection worker thread
        let worker_result_sender = connection_result_sender.clone();
        thread::spawn(move || {
            connection_worker(connection_request_receiver, worker_result_sender);
        });
        
        let now_instant = std::time::Instant::now();
        let mut service = Self {
            config: config.clone(),
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
            poll,
            listener,
            connections: HashMap::new(),
            next_token: 1, // Start from 1, as 0 is reserved for listener
            last_leadership_check: now_instant,
            last_reconnect_attempt: now_instant,
            connection_result_receiver,
            connection_request_sender,
        };
        
        let mut events = Events::with_capacity(128);
        
        loop {
            // Poll for network events with timeout
            let poll_timeout = Duration::from_millis(100);
            service.poll.poll(&mut events, Some(poll_timeout))?;
            
            // Handle network events
            for event in events.iter() {
                match event.token() {
                    LISTENER_TOKEN => {
                        service.handle_new_peer_connection(&connection_result_sender)?;
                    }
                    token => {
                        service.handle_peer_event(token, event.is_readable(), event.is_writable())?;
                    }
                }
            }
            
            // Handle service requests (non-blocking)
            while let Ok((request, response_sender)) = request_receiver.try_recv() {
                let response = service.handle_request(request);
                if let Err(_) = response_sender.send(response) {
                    error!("Failed to send response back to caller");
                }
            }
            
            // Handle commands (non-blocking)
            while let Some(command) = command_queue.pop() {
                service.handle_command(command);
            }
            
            // Handle connection results from background threads (non-blocking)
            while let Ok(connection_result) = service.connection_result_receiver.try_recv() {
                service.handle_connection_result(connection_result, &connection_result_sender)?;
            }
            
            // Periodic operations
            let now = std::time::Instant::now();
            
            // Leadership check
            if now.duration_since(service.last_leadership_check) >= Duration::from_secs(LEADERSHIP_CHECK_INTERVAL_SECS) {
                service.check_leader_election_and_sync();
                service.last_leadership_check = now;
            }
            
            // Reconnect attempts
            let reconnect_interval = Duration::from_secs(service.config.peer_reconnect_interval_secs);
            if now.duration_since(service.last_reconnect_attempt) >= reconnect_interval {
                service.attempt_outbound_connections(&connection_result_sender)?;
                service.last_reconnect_attempt = now;
            }
        }
    }
    
    fn handle_new_peer_connection(&mut self, _connection_result_sender: &Sender<ConnectionResult>) -> Result<()> {
        loop {
            match self.listener.accept() {
                Ok((stream, addr)) => {
                    info!(peer_addr = %addr, "Accepting inbound peer connection");
                    
                    // Complete WebSocket handshake directly - this may block briefly but should be fast
                    match tungstenite::accept(stream) {
                        Ok(mut websocket) => {
                            let token = Token(self.next_token);
                            self.next_token += 1;
                            
                            // Register the websocket stream for read/write events
                            if let Err(e) = self.poll.registry().register(
                                websocket.get_mut(), 
                                token, 
                                Interest::READABLE | Interest::WRITABLE
                            ) {
                                error!(peer_addr = %addr, error = %e, "Failed to register websocket with poll");
                                continue;
                            }
                            
                            let connection = PeerConnection {
                                addr_string: addr.to_string(),
                                state: PeerConnectionState::Connected(websocket),
                                outbound_messages: VecDeque::new(),
                            };
                            
                            self.connections.insert(token, connection);
                            
                            // Send our startup message to the new peer
                            self.send_startup_message_to_peer(&addr.to_string());
                        }
                        Err(e) => {
                            debug!(peer_addr = %addr, error = %e, "Failed to complete WebSocket handshake for inbound connection");
                        }
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    break; // No more connections to accept
                }
                Err(e) => {
                    error!(error = %e, "Failed to accept peer connection");
                    break;
                }
            }
        }
        Ok(())
    }
    
    fn handle_peer_event(&mut self, token: Token, readable: bool, writable: bool) -> Result<()> {
        let should_remove = {
            let mut should_remove = false;
            
            if readable {
                match self.handle_peer_read(token) {
                    Ok(keep_connection) => {
                        if !keep_connection {
                            should_remove = true;
                        }
                    }
                    Err(e) => {
                        if let Some(connection) = self.connections.get(&token) {
                            error!(peer_addr = %connection.addr_string, error = %e, "Failed to handle peer read");
                        }
                        should_remove = true;
                    }
                }
            }
            
            if writable && !should_remove {
                if let Err(e) = self.handle_peer_write(token) {
                    if let Some(connection) = self.connections.get(&token) {
                        error!(peer_addr = %connection.addr_string, error = %e, "Failed to handle peer write");
                    }
                    should_remove = true;
                }
            }
            
            should_remove
        };
        
        if should_remove {
            self.remove_peer_connection(token);
        }
        
        Ok(())
    }
    
    fn handle_peer_read(&mut self, token: Token) -> Result<bool> {
        let mut messages_to_process = Vec::new();
        let peer_addr_string = if let Some(connection) = self.connections.get_mut(&token) {
            let PeerConnectionState::Connected(websocket) = &mut connection.state;
            loop {
                match websocket.read() {
                    Ok(Message::Text(text)) => {
                        // Check if this is a binary wrapper message
                        if let Ok(wrapper) = serde_json::from_str::<serde_json::Value>(&text) {
                            if let Some(base64_data) = wrapper.get("binary_message").and_then(|v| v.as_str()) {
                                // Decode binary message
                                use base64::Engine;
                                if let Ok(binary_data) = base64::engine::general_purpose::STANDARD.decode(base64_data) {
                                    if let Ok(peer_message) = bincode::deserialize::<PeerMessage>(&binary_data) {
                                        messages_to_process.push(peer_message);
                                    } else {
                                        warn!(peer_addr = %connection.addr_string, "Failed to deserialize binary peer message");
                                    }
                                } else {
                                    warn!(peer_addr = %connection.addr_string, "Failed to decode base64 binary message");
                                }
                            } else {
                                // Regular JSON message
                                if let Ok(peer_message) = serde_json::from_str::<PeerMessage>(&text) {
                                    messages_to_process.push(peer_message);
                                } else {
                                    warn!(peer_addr = %connection.addr_string, "Received invalid JSON from peer");
                                }
                            }
                        } else {
                            // Try to parse as regular peer message
                            if let Ok(peer_message) = serde_json::from_str::<PeerMessage>(&text) {
                                messages_to_process.push(peer_message);
                            } else {
                                warn!(peer_addr = %connection.addr_string, "Received invalid JSON from peer");
                            }
                        }
                    }
                    Ok(Message::Binary(_)) => {
                        warn!(peer_addr = %connection.addr_string, "Received unexpected binary message from peer");
                    }
                    Ok(Message::Close(_)) => {
                        info!(peer_addr = %connection.addr_string, "Peer closed connection");
                        return Ok(false); // Should remove connection
                    }
                    Ok(_) => {
                        // Ping/Pong frames, ignore
                    }
                    Err(tungstenite::Error::Io(ref e)) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        break; // No more messages to read
                    }
                    Err(e) => {
                        error!(peer_addr = %connection.addr_string, error = %e, "Error reading from peer WebSocket");
                        return Ok(false); // Should remove connection
                    }
                }
            }
            connection.addr_string.clone()
        } else {
            return Ok(false);
        };
        
        // Process collected messages
        for message in messages_to_process {
            self.handle_peer_message_internal(message, &peer_addr_string);
        }
        
        Ok(true) // Keep connection
    }
    
    fn handle_peer_write(&mut self, token: Token) -> Result<()> {
        if let Some(connection) = self.connections.get_mut(&token) {
            let PeerConnectionState::Connected(websocket) = &mut connection.state;
            // Send any pending outbound messages
            while let Some(message) = connection.outbound_messages.pop_front() {
                match websocket.send(Message::Text(message.clone())) {
                    Ok(()) => {
                        // Message sent successfully
                    }
                    Err(tungstenite::Error::Io(ref e)) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        // Put the message back and try again later
                        connection.outbound_messages.push_front(message);
                        break;
                    }
                    Err(e) => {
                        error!(peer_addr = %connection.addr_string, error = %e, "Error writing to peer WebSocket");
                        return Err(e.into());
                    }
                }
            }
        }
        
        Ok(())
    }
    
    fn remove_peer_connection(&mut self, token: Token) {
        if let Some(connection) = self.connections.remove(&token) {
            info!(peer_addr = %connection.addr_string, "Removed peer connection");
            
            // Check if the disconnected peer was the current leader
            let disconnected_machine_id = self.peer_info.get(&connection.addr_string)
                .map(|info| info.machine_id.clone());
            
            // Clean up peer info
            self.peer_info.remove(&connection.addr_string);
            
            // Check if the disconnected peer was the current leader
            if let Some(disconnected_machine_id) = disconnected_machine_id {
                if let Some(ref current_leader) = self.current_leader {
                    if *current_leader == disconnected_machine_id {
                        info!(
                            disconnected_machine = %disconnected_machine_id,
                            "Current leader disconnected, retriggering leader election"
                        );
                        self.determine_leadership();
                    }
                }
            }
        }
    }
    
    fn attempt_outbound_connections(&mut self, connection_result_sender: &Sender<ConnectionResult>) -> Result<()> {
        for peer_addr in &self.config.peer_addresses.clone() {
            // Check if we already have a connection to this peer address
            let already_connected = self.connections.values()
                .any(|conn| &conn.addr_string == peer_addr);
            
            if !already_connected {
                self.attempt_outbound_connection(peer_addr, connection_result_sender)?;
            }
        }
        Ok(())
    }
    
    fn attempt_outbound_connection(&mut self, peer_addr: &str, _connection_result_sender: &Sender<ConnectionResult>) -> Result<()> {
        debug!(peer_addr = %peer_addr, "Queuing outbound connection request");
        
        // Send connection request to the connection worker thread
        if let Err(e) = self.connection_request_sender.send(ConnectionRequest::OutboundConnection {
            peer_addr: peer_addr.to_string(),
        }) {
            warn!(peer_addr = %peer_addr, error = %e, "Failed to queue outbound connection request");
        }
        
        Ok(())
    }
    
    fn handle_connection_result(&mut self, result: ConnectionResult, _connection_result_sender: &Sender<ConnectionResult>) -> Result<()> {
        match result {
            ConnectionResult::OutboundSuccess { peer_addr, mut websocket } => {
                let token = Token(self.next_token);
                self.next_token += 1;
                
                // Register the websocket stream for read/write events
                if let Err(e) = self.poll.registry().register(
                    websocket.get_mut(), 
                    token, 
                    Interest::READABLE | Interest::WRITABLE
                ) {
                    error!(peer_addr = %peer_addr, error = %e, "Failed to register outbound websocket with poll");
                    return Ok(());
                }
                
                let connection = PeerConnection {
                    addr_string: peer_addr.clone(),
                    state: PeerConnectionState::Connected(websocket),
                    outbound_messages: VecDeque::new(),
                };
                
                self.connections.insert(token, connection);
                info!(peer_addr = %peer_addr, "Outbound peer connection registered");
                
                // Send our startup message to the new peer
                self.send_startup_message_to_peer(&peer_addr);
            }
            ConnectionResult::OutboundFailure { peer_addr, error } => {
                debug!(peer_addr = %peer_addr, error = %error, "Outbound connection failed");
            }
        }
        Ok(())
    }
    
    fn send_startup_message_to_peer(&mut self, peer_addr: &str) {
        let startup_message = PeerMessage::Startup {
            machine_id: self.config.machine.clone(),
            startup_time: self.startup_time,
        };
        self.send_message_to_peer(peer_addr, startup_message);
    }
    
    fn handle_request(&mut self, request: PeerRequest) -> PeerResponse {
        match request {
            PeerRequest::GetAvailabilityState => {
                PeerResponse::AvailabilityState(self.availability_state.clone())
            }
            PeerRequest::GetLeadershipInfo => {
                PeerResponse::LeadershipInfo(self.is_leader, self.current_leader.clone())
            }
        }
    }
    
    fn handle_command(&mut self, command: PeerCommand) {
        match command {
            PeerCommand::SendSyncMessage { requests } => {
                self.send_sync_message_to_peers(requests);
            }
            PeerCommand::SetSnapshotHandle { handle } => {
                self.snapshot_handle = Some(handle);
            }
            PeerCommand::SetCoreHandle { handle } => {
                self.core_handle = Some(handle);
            }
        }
    }
    
    fn handle_peer_message_internal(&mut self, message: PeerMessage, peer_addr: &str) {
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
                self.send_message_to_peer(peer_addr, our_startup_message);
            }
            PeerMessage::FullSyncRequest { machine_id } => {
                if self.is_leader {
                    info!(peer_addr = %peer_addr, machine_id = %machine_id, "Received full sync request from peer");
                    
                    // Take a snapshot and send it
                    if let Some(ref core_handle) = self.core_handle {
                        match core_handle.take_snapshot() {
                            Ok(snapshot) => {
                                info!(
                                    requesting_machine = %machine_id,
                                    snapshot_entities = snapshot.entities.len(),
                                    "Snapshot prepared for full sync response"
                                );
                                
                                let full_sync_response = PeerMessage::FullSyncResponse { snapshot };
                                self.send_message_to_peer_binary(peer_addr, full_sync_response);
                            }
                            Err(e) => {
                                error!(error = %e, "Failed to take snapshot for full sync request");
                            }
                        }
                    } else {
                        error!("Core handle not available for full sync request");
                    }
                }
            }
            PeerMessage::FullSyncResponse { snapshot } => {
                info!(peer_addr = %peer_addr, "Received full sync response from peer");
                
                if let Some(ref core_handle) = self.core_handle {
                    // Apply the snapshot
                    if let Err(e) = core_handle.restore_snapshot(snapshot.clone()) {
                        error!(error = %e, "Failed to restore snapshot during full sync");
                        return;
                    }
                    
                    // Save the snapshot to disk for persistence
                    if let Some(ref snapshot_handle) = self.snapshot_handle {
                        match snapshot_handle.save(snapshot) {
                            Ok(snapshot_counter) => {
                                info!(
                                    snapshot_counter = snapshot_counter,
                                    "Snapshot saved to disk during full sync"
                                );
                                
                                // Write a snapshot marker to the WAL
                                let snapshot_request = qlib_rs::Request::Snapshot {
                                    snapshot_counter,
                                    timestamp: Some(now()),
                                    originator: Some(self.config.machine.clone()),
                                };
                                
                                if let Err(e) = core_handle.write_request(snapshot_request) {
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
                    }
                    
                    // Update state to indicate full sync completion
                    self.is_fully_synced = true;
                    self.full_sync_request_pending = false;
                    self.availability_state = AvailabilityState::Available;
                    self.became_unavailable_at = None;
                    
                    info!("Successfully applied full sync snapshot, instance is now fully synchronized");
                } else {
                    error!("Core handle not available for full sync response");
                }
            }
            PeerMessage::SyncRequest { requests } => {
                debug!(peer_addr = %peer_addr, request_count = %requests.len(), "Received sync request from peer");
                
                if let Some(ref core_handle) = self.core_handle {
                    let our_machine_id = &self.config.machine;
                    
                    // Filter requests to only include those with valid originators (different from our machine)
                    let requests_to_apply: Vec<_> = requests.into_iter()
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
                    
                    let req_len = requests_to_apply.len();
                    if !requests_to_apply.is_empty() {
                        // Apply requests through the core handle
                        for request in requests_to_apply {
                            if let Err(e) = core_handle.write_request(request) {
                                error!(error = %e, "Failed to apply sync request from peer");
                            }
                        }
                        
                        debug!(
                            request_count = req_len,
                            "Successfully applied sync requests from peer"
                        );
                    } else {
                        debug!("No valid sync requests to apply from peer");
                    }
                } else {
                    error!("Core handle not available for sync request");
                }
            }
        }
    }
    
    fn send_message_to_peer(&mut self, peer_addr: &str, message: PeerMessage) {
        if let Ok(json) = serde_json::to_string(&message) {
            // Find the connection for this peer and queue the message
            for connection in self.connections.values_mut() {
                if connection.addr_string == peer_addr {
                    connection.outbound_messages.push_back(json);
                    break;
                }
            }
        }
    }
    
    fn send_message_to_peer_binary(&mut self, peer_addr: &str, message: PeerMessage) {
        // Use binary serialization for large messages like FullSyncResponse
        match bincode::serialize(&message) {
            Ok(binary_data) => {
                // Encode as base64 and wrap in JSON for consistent transport
                use base64::Engine;
                let base64_data = base64::engine::general_purpose::STANDARD.encode(&binary_data);
                let wrapper = serde_json::json!({
                    "binary_message": base64_data
                });
                
                if let Ok(json) = serde_json::to_string(&wrapper) {
                    for connection in self.connections.values_mut() {
                        if connection.addr_string == peer_addr {
                            connection.outbound_messages.push_back(json);
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                error!(error = %e, "Failed to serialize binary message");
            }
        }
    }
    
    fn send_sync_message_to_peers(&mut self, requests: Vec<qlib_rs::Request>) {
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
            let message = PeerMessage::SyncRequest { requests: requests_to_sync.clone() };
            if let Ok(json) = serde_json::to_string(&message) {
                // Send to all connected peers
                for connection in self.connections.values_mut() {
                    let PeerConnectionState::Connected(_) = connection.state;
                    connection.outbound_messages.push_back(json.clone());
                    debug!(
                        peer_addr = %connection.addr_string,
                        count = requests_to_sync.len(),
                        "Sent sync requests to peer"
                    );
                }
            }
        }
    }
    
    fn check_leader_election_and_sync(&mut self) {
        let mut leadership_changed = false;
        
        // Self-promotion logic
        if !self.is_leader && 
           (self.config.peer_addresses.is_empty() || self.connections.is_empty()) &&
           self.peer_info.is_empty() {
            
            let current_time = now().unix_timestamp() as u64;
            let time_since_startup = current_time.saturating_sub(self.startup_time);
            
            if time_since_startup >= self.config.self_promotion_delay_secs {
                info!("Self-promoting to leader due to no peer connections");
                self.is_leader = true;
                self.current_leader = Some(self.config.machine.clone());
                self.availability_state = AvailabilityState::Available;
                self.is_fully_synced = true;
                self.became_unavailable_at = None;
                leadership_changed = true;
                
                // Force disconnect all clients during transition
                if let Some(ref core_handle) = self.core_handle {
                    if let Err(e) = core_handle.force_disconnect_all_clients() {
                        error!(error = %e, "Failed to force disconnect clients during leader promotion");
                    }
                }
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
                let elapsed = current_time.saturating_sub(became_unavailable_at);
                
                if elapsed >= self.config.full_sync_grace_period_secs {
                    if let Some(ref leader_machine_id) = self.current_leader.clone() {
                        self.send_full_sync_request(leader_machine_id.clone());
                    }
                }
            }
        }
    }
    
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
                    error!(error = %e, "Failed to force disconnect clients due to unavailability");
                }
                info!("Disconnected all clients due to unavailability");
            }
        }
    }
    
    fn send_full_sync_request(&mut self, leader_machine_id: String) {
        info!(
            leader_machine_id = %leader_machine_id,
            "Grace period expired, sending FullSyncRequest to leader"
        );
        
        self.full_sync_request_pending = true;
        
        let full_sync_request = PeerMessage::FullSyncRequest {
            machine_id: self.config.machine.clone(),
        };
        
        if let Ok(request_json) = serde_json::to_string(&full_sync_request) {
            let mut sent = false;
            for connection in self.connections.values_mut() {
                if let Some(peer_info) = self.peer_info.get(&connection.addr_string) {
                    if peer_info.machine_id == leader_machine_id {
                        connection.outbound_messages.push_back(request_json);
                        info!(peer_addr = %connection.addr_string, "Sent FullSyncRequest to leader");
                        sent = true;
                        break;
                    }
                }
            }
            
            if !sent {
                warn!("No connected peer available to send FullSyncRequest to leader");
                self.full_sync_request_pending = false;
            }
        } else {
            error!("Failed to serialize FullSyncRequest");
            self.full_sync_request_pending = false;
        }
    }
}