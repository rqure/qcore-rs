use crossbeam::channel::{Sender, Receiver, bounded, unbounded};
use std::net::SocketAddr;
use std::collections::{HashMap, VecDeque};
use std::time::Duration;
use mio::{Poll, Interest, Token, Events};
use mio::net::{TcpListener as MioTcpListener, TcpStream as MioTcpStream};
use tungstenite::{WebSocket, Message};
use tracing::{info, warn, error, debug};
use anyhow::Result;
use serde::{Serialize, Deserialize};
use std::thread;

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
        message: PeerMessage,
        peer_addr: String,
    },
    SetSnapshotHandle {
        handle: crate::snapshot::SnapshotHandle,
    },
    SetCoreHandle {
        handle: crate::core::CoreHandle,
    },
}

/// Response types for peer requests
#[derive(Debug)]
pub enum PeerResponse {
    Unit,
    AvailabilityState(AvailabilityState),
    LeadershipInfo(bool, Option<String>), // (is_leader, current_leader)
    Bool(bool),
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
            error!(error = %e, "Failed to send sync message request to peer service");
        }
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

    pub fn is_outbound_peer_connected(&self, peer_addr: String) -> bool {
        let (response_tx, response_rx) = unbounded();
        if let Err(e) = self.request_sender.send((PeerRequest::IsOutboundPeerConnected { peer_addr }, response_tx)) {
            error!(error = %e, "Failed to send peer connection check request to peer service");
            return false;
        }
        
        match response_rx.recv() {
            Ok(PeerResponse::Bool(connected)) => connected,
            _ => {
                error!("Failed to receive peer connection check response from peer service");
                false
            }
        }
    }

    pub fn peer_connected(&self, peer_addr: String, machine_id: String, startup_time: u64) {
        let (response_tx, _response_rx) = unbounded();
        if let Err(e) = self.request_sender.send((PeerRequest::PeerConnected { peer_addr, machine_id, startup_time }, response_tx)) {
            error!(error = %e, "Failed to send peer connected notification to peer service");
        }
    }

    pub fn peer_disconnected(&self, peer_addr: String) {
        let (response_tx, _response_rx) = unbounded();
        if let Err(e) = self.request_sender.send((PeerRequest::PeerDisconnected { peer_addr }, response_tx)) {
            error!(error = %e, "Failed to send peer disconnected notification to peer service");
        }
    }

    pub fn outbound_peer_connected(&self, peer_addr: String, sender: Sender<Message>) {
        let (response_tx, _response_rx) = unbounded();
        if let Err(e) = self.request_sender.send((PeerRequest::OutboundPeerConnected { peer_addr, sender }, response_tx)) {
            error!(error = %e, "Failed to send outbound peer connected notification to peer service");
        }
    }

    pub fn outbound_peer_disconnected(&self, peer_addr: String) {
        let (response_tx, _response_rx) = unbounded();
        if let Err(e) = self.request_sender.send((PeerRequest::OutboundPeerDisconnected { peer_addr }, response_tx)) {
            error!(error = %e, "Failed to send outbound peer disconnected notification to peer service");
        }
    }

    pub fn process_peer_message(&self, message: PeerMessage, peer_addr: String) {
        let (response_tx, _response_rx) = unbounded();
        if let Err(e) = self.request_sender.send((PeerRequest::ProcessPeerMessage { message, peer_addr }, response_tx)) {
            error!(error = %e, "Failed to send process peer message request to peer service");
        }
    }

    /// Set snapshot handle for dependencies
    pub fn set_snapshot_handle(&self, handle: crate::snapshot::SnapshotHandle) {
        let (response_tx, _response_rx) = unbounded();
        if let Err(e) = self.request_sender.send((PeerRequest::SetSnapshotHandle { handle }, response_tx)) {
            error!(error = %e, "Failed to send set snapshot handle request to peer service");
        }
    }

    /// Set core handle for dependencies
    pub fn set_core_handle(&self, handle: crate::core::CoreHandle) {
        let (response_tx, _response_rx) = unbounded();
        if let Err(e) = self.request_sender.send((PeerRequest::SetCoreHandle { handle }, response_tx)) {
            error!(error = %e, "Failed to send set core handle request to peer service");
        }
    }
}

/// Connection state for peer connections
#[derive(Debug)]
enum PeerConnectionState {
    Connected(WebSocket<MioTcpStream>),
    Failed,
}

/// Information about a peer connection
#[derive(Debug)]
struct PeerConnection {
    addr: SocketAddr,
    addr_string: String,
    state: PeerConnectionState,
    outbound_messages: VecDeque<String>,
    is_outbound: bool,
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
    core_handle: Option<crate::core::CoreHandle>,
    
    // Mio-based networking
    poll: Poll,
    listener: MioTcpListener,
    connections: HashMap<Token, PeerConnection>,
    next_token: usize,
    
    // Timing for periodic operations
    last_leadership_check: std::time::Instant,
    last_reconnect_attempt: std::time::Instant,
}

const LISTENER_TOKEN: Token = Token(0);
const LEADERSHIP_CHECK_INTERVAL_SECS: u64 = 1;

impl PeerService {
    pub fn spawn(config: PeerConfig) -> PeerHandle {
        let (request_sender, request_receiver) = bounded(1024);
        
        let handle = PeerHandle { request_sender };
        
        // Spawn the service thread
        let service_config = config.clone();
        thread::spawn(move || {
            if let Err(e) = Self::run_service(service_config, request_receiver) {
                error!(error = %e, "Peer service failed");
            }
            error!("Peer service has stopped unexpectedly");
        });

        handle
    }
    
    fn run_service(
        config: PeerConfig, 
        request_receiver: Receiver<(PeerRequest, Sender<PeerResponse>)>
    ) -> Result<()> {
        let addr = format!("0.0.0.0:{}", config.peer_port).parse()?;
        let mut listener = MioTcpListener::bind(addr)?;
        let poll = Poll::new()?;
        
        poll.registry().register(&mut listener, LISTENER_TOKEN, Interest::READABLE)?;
        
        info!(bind_address = %addr, "Peer service WebSocket server initialized");
        
        let now = std::time::Instant::now();
        let mut service = Self {
            config: config.clone(),
            startup_time: qlib_rs::now().unix_timestamp() as u64,
            availability_state: AvailabilityState::Unavailable,
            is_leader: false,
            current_leader: None,
            is_fully_synced: false,
            became_unavailable_at: None,
            full_sync_request_pending: false,
            peer_info: HashMap::new(),
            connected_outbound_peers: HashMap::new(),
            snapshot_handle: None,
            core_handle: None,
            poll,
            listener,
            connections: HashMap::new(),
            next_token: 1, // Start from 1, as 0 is reserved for listener
            last_leadership_check: now,
            last_reconnect_attempt: now,
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
                        service.handle_new_peer_connection()?;
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
                service.attempt_outbound_connections()?;
                service.last_reconnect_attempt = now;
            }
        }
    }
    
    fn handle_new_peer_connection(&mut self) -> Result<()> {
        loop {
            match self.listener.accept() {
                Ok((stream, addr)) => {
                    let token = Token(self.next_token);
                    self.next_token += 1;
                    
                    info!(peer_addr = %addr, "Accepting inbound peer connection");
                    
                    // Handle the WebSocket handshake synchronously for inbound connections
                    match tungstenite::accept(stream) {
                        Ok(mut websocket) => {
                            // Register the websocket stream for read/write events
                            self.poll.registry().register(
                                websocket.get_mut(), 
                                token, 
                                Interest::READABLE | Interest::WRITABLE
                            )?;
                            
                            let connection = PeerConnection {
                                addr,
                                addr_string: addr.to_string(),
                                state: PeerConnectionState::Connected(websocket),
                                outbound_messages: VecDeque::new(),
                                is_outbound: false,
                            };
                            
                            self.connections.insert(token, connection);
                            
                            // Send our startup message to the new peer
                            self.send_startup_message_to_peer(&addr.to_string());
                        }
                        Err(e) => {
                            error!(peer_addr = %addr, error = %e, "Failed to complete WebSocket handshake for inbound connection");
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
            if let PeerConnectionState::Connected(websocket) = &mut connection.state {
                loop {
                    match websocket.read() {
                        Ok(Message::Text(text)) => {
                            if let Ok(peer_message) = serde_json::from_str::<PeerMessage>(&text) {
                                messages_to_process.push(peer_message);
                            } else {
                                warn!(peer_addr = %connection.addr_string, "Received invalid JSON from peer");
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
            if let PeerConnectionState::Connected(websocket) = &mut connection.state {
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
        }
        
        Ok(())
    }
    
    fn remove_peer_connection(&mut self, token: Token) {
        if let Some(connection) = self.connections.remove(&token) {
            info!(peer_addr = %connection.addr_string, "Removed peer connection");
            
            // Notify about disconnection
            if connection.is_outbound {
                self.connected_outbound_peers.remove(&connection.addr_string);
            }
            
            self.peer_info.remove(&connection.addr_string);
        }
    }
    
    fn attempt_outbound_connections(&mut self) -> Result<()> {
        for peer_addr in &self.config.peer_addresses.clone() {
            if !self.connected_outbound_peers.contains_key(peer_addr) {
                self.attempt_outbound_connection(peer_addr)?;
            }
        }
        Ok(())
    }
    
    fn attempt_outbound_connection(&mut self, peer_addr: &str) -> Result<()> {
        if let Ok(addr) = peer_addr.parse::<SocketAddr>() {
            debug!(peer_addr = %peer_addr, "Attempting outbound connection");
            
            // For simplicity, we'll use blocking connection and handshake
            // In a more complex implementation, you'd want to make this non-blocking
            match std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(5)) {
                Ok(std_stream) => {
                    std_stream.set_nonblocking(true)?;
                    let mio_stream = MioTcpStream::from_std(std_stream);
                    
                    let url = format!("ws://{}", peer_addr);
                    match tungstenite::client(&url, mio_stream) {
                        Ok((mut websocket, _response)) => {
                            let token = Token(self.next_token);
                            self.next_token += 1;
                            
                            // Register the websocket stream for read/write events
                            self.poll.registry().register(
                                websocket.get_mut(), 
                                token, 
                                Interest::READABLE | Interest::WRITABLE
                            )?;
                            
                            let connection = PeerConnection {
                                addr,
                                addr_string: peer_addr.to_string(),
                                state: PeerConnectionState::Connected(websocket),
                                outbound_messages: VecDeque::new(),
                                is_outbound: true,
                            };
                            
                            info!(peer_addr = %peer_addr, "Outbound peer connection established");
                            self.connections.insert(token, connection);
                            
                            // Send our startup message to the new peer
                            self.send_startup_message_to_peer(peer_addr);
                        }
                        Err(e) => {
                            debug!(peer_addr = %peer_addr, error = %e, "Failed to establish outbound WebSocket connection");
                        }
                    }
                }
                Err(e) => {
                    debug!(peer_addr = %peer_addr, error = %e, "Failed to connect to peer");
                }
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
                self.peer_info.insert(peer_addr.clone(), PeerInfo { machine_id, startup_time });
                PeerResponse::Unit
            }
            PeerRequest::PeerDisconnected { peer_addr } => {
                self.peer_info.remove(&peer_addr);
                self.connected_outbound_peers.remove(&peer_addr);
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
            PeerRequest::ProcessPeerMessage { message, peer_addr } => {
                self.handle_peer_message_internal(message, &peer_addr);
                PeerResponse::Unit
            }
            PeerRequest::SetSnapshotHandle { handle } => {
                self.snapshot_handle = Some(handle);
                PeerResponse::Unit
            }
            PeerRequest::SetCoreHandle { handle } => {
                self.core_handle = Some(handle);
                PeerResponse::Unit
            }
        }
    }
    
    fn handle_peer_message_internal(&mut self, message: PeerMessage, peer_addr: &str) {
        match message {
            PeerMessage::Startup { machine_id, startup_time } => {
                info!(peer_addr = %peer_addr, machine_id = %machine_id, startup_time = %startup_time, "Received startup message from peer");
                self.peer_info.insert(peer_addr.to_string(), PeerInfo { machine_id, startup_time });
                
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
                    // TODO: Send snapshot to peer
                }
            }
            PeerMessage::FullSyncResponse { snapshot: _ } => {
                info!(peer_addr = %peer_addr, "Received full sync response from peer");
                // TODO: Apply snapshot
            }
            PeerMessage::SyncRequest { requests } => {
                debug!(peer_addr = %peer_addr, request_count = %requests.len(), "Received sync request from peer");
                // TODO: Apply requests
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
    
    fn send_sync_message_to_peers(&mut self, requests: Vec<qlib_rs::Request>) {
        let message = PeerMessage::SyncRequest { requests };
        if let Ok(json) = serde_json::to_string(&message) {
            // Send to all connected peers
            for connection in self.connections.values_mut() {
                if let PeerConnectionState::Connected(_) = connection.state {
                    connection.outbound_messages.push_back(json.clone());
                }
            }
        }
    }
    
    fn check_leader_election_and_sync(&mut self) {
        // Simple leader election: highest startup time wins
        let mut highest_startup_time = self.startup_time;
        let mut leader_machine_id = self.config.machine.clone();
        
        for peer_info in self.peer_info.values() {
            if peer_info.startup_time > highest_startup_time {
                highest_startup_time = peer_info.startup_time;
                leader_machine_id = peer_info.machine_id.clone();
            }
        }
        
        let was_leader = self.is_leader;
        self.is_leader = leader_machine_id == self.config.machine;
        self.current_leader = Some(leader_machine_id);
        
        if self.is_leader != was_leader {
            if self.is_leader {
                info!("Became leader");
                self.availability_state = AvailabilityState::Available;
            } else {
                info!(current_leader = %self.current_leader.as_ref().unwrap(), "Leader changed");
                if !self.is_fully_synced {
                    self.availability_state = AvailabilityState::Unavailable;
                }
            }
        }
        
        // Handle availability and syncing logic
        if !self.is_leader && !self.is_fully_synced && !self.full_sync_request_pending {
            // Request full sync from leader if available
            if let Some(ref leader) = self.current_leader {
                let full_sync_message = PeerMessage::FullSyncRequest {
                    machine_id: self.config.machine.clone(),
                };
                
                // Find leader connection and send request
                for connection in self.connections.values_mut() {
                    if let Some(peer_info) = self.peer_info.get(&connection.addr_string) {
                        if peer_info.machine_id == *leader {
                            if let Ok(json) = serde_json::to_string(&full_sync_message) {
                                connection.outbound_messages.push_back(json);
                                self.full_sync_request_pending = true;
                                info!(leader = %leader, "Requested full sync from leader");
                            }
                            break;
                        }
                    }
                }
            }
        }
    }
}