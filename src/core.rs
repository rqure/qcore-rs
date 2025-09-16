use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{Read, Write};
use mio::{Poll, Interest, Token, Events};
use mio::net::{TcpListener as MioTcpListener, TcpStream as MioTcpStream};
use tracing::{info, warn, error, debug};
use anyhow::Result;
use crossbeam::channel::{Sender, Receiver, unbounded};
use std::thread;
use qlib_rs::{
    StoreMessage, EntityId, NotificationQueue, NotifyConfig,
    AuthenticationResult, Notification, Store, Cache, CelExecutor,
    PushCondition, Value, Request, Snapshot, Snowflake,
    AuthConfig, EntityType, FieldType, PageOpts, PageResult, 
    auth::{authenticate_subject, get_scope, AuthorizationScope}
};
use crate::peer_manager::AvailabilityState;
use crate::peer_manager::PeerManager;
use qlib_rs::protocol::{ProtocolMessage, ProtocolCodec, MessageBuffer};

/// Configuration for the core service
#[derive(Debug, Clone)]
pub struct CoreConfig {
    /// Port for unified client and peer communication
    pub port: u16,
    /// Machine ID for request origination
    pub machine_id: String,
}

impl From<&crate::Config> for CoreConfig {
    fn from(config: &crate::Config) -> Self {
        Self {
            port: config.port,
            machine_id: config.machine.clone(),
        }
    }
}

/// Fire-and-forget request types (no response needed)
#[derive(Debug)]
pub enum CoreCommand {
    HandleMiscOperations,
    HandleHeartbeat,
    HandlePeerOperations,
}

/// Core service request types
#[derive(Debug)]
pub enum CoreRequest {
    WriteRequest {
        request: Request,
    },
    TakeSnapshot,
    RestoreSnapshot {
        snapshot: Snapshot,
    },
    ForceDisconnectAllClients,
    SetHandles {
        snapshot_handle: crate::snapshot::SnapshotHandle,
        wal_handle: crate::wal::WalHandle,
    },
    InitializeStore,
}

/// Response types for core requests
#[derive(Debug)]
pub enum CoreResponse {
    WriteResult(Result<()>),
    Snapshot(Snapshot),
    Unit,
}

/// Handle for communicating with core service
#[derive(Debug, Clone)]
pub struct CoreHandle {
    request_sender: Sender<(CoreRequest, Sender<CoreResponse>)>,
}

impl CoreHandle {
    pub fn write_request(&self, request: Request) -> Result<()> {
        let (response_tx, response_rx) = unbounded();
        self.request_sender.send((CoreRequest::WriteRequest { request }, response_tx))
            .map_err(|e| anyhow::anyhow!("Core service has stopped: {}", e))?;
        
        match response_rx.recv()
            .map_err(|e| anyhow::anyhow!("Core service response channel closed: {}", e))?
        {
            CoreResponse::WriteResult(result) => result,
            _ => Err(anyhow::anyhow!("Unexpected response type")),
        }
    }

    pub fn take_snapshot(&self) -> Result<Snapshot> {
        let (response_tx, response_rx) = unbounded();
        self.request_sender.send((CoreRequest::TakeSnapshot, response_tx))
            .map_err(|e| anyhow::anyhow!("Core service has stopped: {}", e))?;
        
        match response_rx.recv()
            .map_err(|e| anyhow::anyhow!("Core service response channel closed: {}", e))?
        {
            CoreResponse::Snapshot(snapshot) => Ok(snapshot),
            _ => Err(anyhow::anyhow!("Unexpected response type")),
        }
    }

    pub fn restore_snapshot(&self, snapshot: Snapshot) -> Result<()> {
        let (response_tx, response_rx) = unbounded();
        self.request_sender.send((CoreRequest::RestoreSnapshot { snapshot }, response_tx))
            .map_err(|e| anyhow::anyhow!("Core service has stopped: {}", e))?;
        
        match response_rx.recv()
            .map_err(|e| anyhow::anyhow!("Core service response channel closed: {}", e))?
        {
            CoreResponse::Unit => Ok(()),
            _ => Err(anyhow::anyhow!("Unexpected response type")),
        }
    }

    pub fn force_disconnect_all_clients(&self) -> Result<()> {
        let (response_tx, response_rx) = unbounded();
        self.request_sender.send((CoreRequest::ForceDisconnectAllClients, response_tx))
            .map_err(|e| anyhow::anyhow!("Core service has stopped: {}", e))?;
        
        match response_rx.recv()
            .map_err(|e| anyhow::anyhow!("Core service response channel closed: {}", e))?
        {
            CoreResponse::Unit => Ok(()),
            _ => Err(anyhow::anyhow!("Unexpected response type")),
        }
    }

    /// Set handles to other services (called from main)
    pub fn set_handles(
        &self,
        snapshot_handle: crate::snapshot::SnapshotHandle,
        wal_handle: crate::wal::WalHandle,
    ) -> Result<()> {
        let (response_tx, response_rx) = unbounded();
        self.request_sender.send((CoreRequest::SetHandles { snapshot_handle, wal_handle }, response_tx))
            .map_err(|e| anyhow::anyhow!("Core service has stopped: {}", e))?;
        
        match response_rx.recv()
            .map_err(|e| anyhow::anyhow!("Core service response channel closed: {}", e))?
        {
            CoreResponse::Unit => Ok(()),
            _ => Err(anyhow::anyhow!("Unexpected response type")),
        }
    }

    /// Initialize store from snapshots and WAL replay
    pub fn initialize_store(&self) -> Result<()> {
        let (response_tx, response_rx) = unbounded();
        self.request_sender.send((CoreRequest::InitializeStore, response_tx))
            .map_err(|e| anyhow::anyhow!("Core service has stopped: {}", e))?;
        
        match response_rx.recv()
            .map_err(|e| anyhow::anyhow!("Core service response channel closed: {}", e))?
        {
            CoreResponse::Unit => Ok(()),
            _ => Err(anyhow::anyhow!("Unexpected response type")),
        }
    }
}

/// Client connection information  
#[derive(Debug)]
struct ClientConnection {
    stream: MioTcpStream,
    addr_string: String,
    authenticated: bool,
    client_id: Option<EntityId>,
    notification_queue: NotificationQueue,
    notification_configs: HashSet<NotifyConfig>,
    pending_notifications: VecDeque<Notification>,
    outbound_messages: VecDeque<Vec<u8>>, // Raw bytes for TCP
    message_buffer: MessageBuffer,        // For handling partial reads
}

/// Unknown connection that hasn't been classified yet
#[derive(Debug)]
struct UnknownConnection {
    stream: MioTcpStream,
    addr_string: String,
    outbound_messages: VecDeque<Vec<u8>>, // Raw bytes for TCP
    message_buffer: MessageBuffer,        // For handling partial reads
}

/// Connection type to distinguish between clients, peers, and unknown connections
#[derive(Debug)]
enum Connection {
    Unknown(UnknownConnection),
    Client(ClientConnection),
    Peer(crate::peer_manager::PeerConnection),
}

impl Connection {
    fn get_stream_mut(&mut self) -> &mut MioTcpStream {
        match self {
            Connection::Unknown(unknown) => &mut unknown.stream,
            Connection::Client(client) => &mut client.stream,
            Connection::Peer(peer) => peer.get_stream_mut(),
        }
    }
    
    fn addr_string(&self) -> &str {
        match self {
            Connection::Unknown(unknown) => &unknown.addr_string,
            Connection::Client(client) => &client.addr_string,
            Connection::Peer(peer) => &peer.addr_string,
        }
    }
    
    fn get_message_buffer_mut(&mut self) -> &mut MessageBuffer {
        match self {
            Connection::Unknown(unknown) => &mut unknown.message_buffer,
            Connection::Client(client) => &mut client.message_buffer,
            Connection::Peer(peer) => &mut peer.message_buffer,
        }
    }
    
    fn queue_outbound_message(&mut self, data: Vec<u8>) {
        match self {
            Connection::Unknown(unknown) => unknown.outbound_messages.push_back(data),
            Connection::Client(client) => client.outbound_messages.push_back(data),
            Connection::Peer(peer) => peer.queue_message(data),
        }
    }
    
    fn get_outbound_messages_mut(&mut self) -> &mut VecDeque<Vec<u8>> {
        match self {
            Connection::Unknown(unknown) => &mut unknown.outbound_messages,
            Connection::Client(client) => &mut client.outbound_messages,
            Connection::Peer(peer) => &mut peer.outbound_messages,
        }
    }
    
    // Client-specific accessors
    fn authenticated(&self) -> bool {
        match self {
            Connection::Unknown(_) => false,
            Connection::Client(client) => client.authenticated,
            Connection::Peer(_) => false, // Peers don't use the same auth model
        }
    }
    
    fn set_authenticated(&mut self, auth: bool) {
        if let Connection::Client(client) = self {
            client.authenticated = auth;
        }
    }
    
    fn client_id(&self) -> &Option<EntityId> {
        match self {
            Connection::Unknown(_) => &None,
            Connection::Client(client) => &client.client_id,
            Connection::Peer(_) => &None,
        }
    }
    
    fn set_client_id(&mut self, id: Option<EntityId>) {
        if let Connection::Client(client) = self {
            client.client_id = id;
        }
    }
    
    fn notification_queue(&self) -> Option<&NotificationQueue> {
        match self {
            Connection::Unknown(_) => None,
            Connection::Client(client) => Some(&client.notification_queue),
            Connection::Peer(_) => None,
        }
    }
    
    fn notification_configs_mut(&mut self) -> Option<&mut HashSet<NotifyConfig>> {
        match self {
            Connection::Unknown(_) => None,
            Connection::Client(client) => Some(&mut client.notification_configs),
            Connection::Peer(_) => None,
        }
    }
    
    fn is_client(&self) -> bool {
        matches!(self, Connection::Client(_))
    }
    
    fn is_peer(&self) -> bool {
        matches!(self, Connection::Peer(_))
    }
    
    fn is_unknown(&self) -> bool {
        matches!(self, Connection::Unknown(_))
    }
}

/// Core service that handles both client and peer connections
pub struct CoreService {
    config: CoreConfig,
    listener: MioTcpListener,
    poll: Poll,
    connections: HashMap<Token, Connection>,
    next_token: usize,
    
    // Store and related components (replacing StoreService)
    store: Store,
    permission_cache: Cache,
    cel_executor: CelExecutor,
    
    // Peer management
    peer_manager: PeerManager,
    
    // Handles to other services  
    snapshot_handle: Option<crate::snapshot::SnapshotHandle>,
    wal_handle: Option<crate::wal::WalHandle>,
    
    // Channel for receiving requests from other services
    request_receiver: Receiver<(CoreRequest, Sender<CoreResponse>)>,
    
    // Channel for sending requests to other services
    request_sender: Sender<(CoreRequest, Sender<CoreResponse>)>,
}

const LISTENER_TOKEN: Token = Token(0);

impl CoreService {
    /// Create a new core service with peer configuration
    pub fn new(
        config: CoreConfig,
        peer_config: crate::peer_manager::PeerConfig,
        request_receiver: Receiver<(CoreRequest, Sender<CoreResponse>)>,
        request_sender: Sender<(CoreRequest, Sender<CoreResponse>)>
    ) -> Result<Self> {
        let addr = format!("0.0.0.0:{}", config.port).parse()?;
        let mut listener = MioTcpListener::bind(addr)?;
        let poll = Poll::new()?;
        
        poll.registry().register(&mut listener, LISTENER_TOKEN, Interest::READABLE)?;
        
        info!(bind_address = %addr, "Core service unified TCP server initialized");
        
        // Initialize store with snowflake
        let snowflake = Snowflake::new(); // TODO: configure these properly
        let mut store = Store::new(snowflake);
        
        let (permission_cache, _notification_queue) = Cache::new(
            &mut store,
            qlib_rs::et::permission(),
            vec![qlib_rs::ft::resource_type(), qlib_rs::ft::resource_field()],
            vec![qlib_rs::ft::scope(), qlib_rs::ft::condition()]
        ).map_err(|e| anyhow::anyhow!("Failed to create permission cache: {}", e))?;
        let cel_executor = CelExecutor::new();
        
        Ok(Self {
            config,
            listener,
            poll,
            connections: HashMap::new(),
            next_token: 1,
            store,
            permission_cache,
            cel_executor,
            peer_manager: PeerManager::new(peer_config),
            snapshot_handle: None,
            wal_handle: None,
            request_receiver,
            request_sender,
        })
    }

    /// Spawn the core service in its own thread and return a handle
    pub fn spawn(config: CoreConfig, peer_config: crate::peer_manager::PeerConfig) -> Result<CoreHandle> {
        let (request_sender, request_receiver) = unbounded();
        
        let handle = CoreHandle { 
            request_sender: request_sender.clone(),
        };
        
        // Spawn the main core service thread (I/O event loop)
        let request_sender_for_service = request_sender.clone();
        thread::spawn(move || {
            let mut service = match Self::new(config, peer_config, request_receiver, request_sender_for_service) {
                Ok(s) => s,
                Err(e) => {
                    error!("Failed to create core service: {}", e);
                    return;
                }
            };
            
            if let Err(e) = service.run() {
                error!("Core service error: {}", e);
            }
            
            error!("Core service has stopped unexpectedly");
        });
        
        Ok(handle)
    }
    
    /// Initialize the store from snapshots and WAL replay
    pub fn initialize_store_internal(&mut self) -> Result<()> {
        info!("Initializing store from persistent storage");
        
        // Try to load latest snapshot first
        if let Some(snapshot_handle) = &self.snapshot_handle {
            if let Ok(Some((snapshot, snapshot_counter))) = snapshot_handle.load_latest() {
                info!(snapshot_counter = %snapshot_counter, "Restoring from snapshot");
                self.store.restore_snapshot(snapshot);
            } else {
                info!("No snapshots found, starting with empty store");
            }
        }
        
        // Replay WAL entries to bring store up to date
        if let Some(wal_handle) = &self.wal_handle {
            if let Ok(requests) = wal_handle.replay() {
                info!(request_count = %requests.len(), "Replaying WAL entries");
                for request in requests {
                    if let Err(e) = self.store.perform_mut(vec![request]) {
                        error!(error = %e, "Failed to replay WAL entry");
                    }
                }
            } else {
                warn!("Failed to replay WAL entries");
            }
        }
        
        info!("Store initialization complete");
        Ok(())
    }
    
    /// Run the main event loop (pure I/O event handling)
    pub fn run(&mut self) -> Result<()> {
        let mut events = Events::with_capacity(1024);
        
        loop {
            // Handle incoming requests from other services (non-blocking)
            while let Ok((request, response_sender)) = self.request_receiver.try_recv() {
                let response = self.handle_request(request);
                if let Err(_) = response_sender.send(response) {
                    warn!("Failed to send response to requesting service");
                }
            }
            
            // Process notifications and send them to clients
            self.process_notifications()?;
            
            // Process any pending write requests from the store
            self.process_write_requests()?;
            
            // Poll for I/O events with no timeout for maximum responsiveness
            // Periodic operations are now handled by the self-connecting periodic client
            self.poll.poll(&mut events, None)?;
            
            // Handle all mio events
            for event in events.iter() {
                match event.token() {
                    LISTENER_TOKEN => {
                        if event.is_readable() {
                            self.handle_new_connection()?;
                        }
                    }
                    token => {
                        self.handle_connection_event(token, event.is_readable(), event.is_writable())?;
                    }
                }
            }
        }
    }
    
    /// Handle requests from other services
    fn handle_request(&mut self, request: CoreRequest) -> CoreResponse {
        match request {
            CoreRequest::WriteRequest { request } => {
                match self.wal_handle.as_ref() {
                    Some(wal_handle) => {
                        CoreResponse::WriteResult(wal_handle.write_request(request))
                    }
                    None => {
                        CoreResponse::WriteResult(Err(anyhow::anyhow!("WAL service not available")))
                    }
                }
            }
            CoreRequest::TakeSnapshot => {
                let snapshot = self.store.take_snapshot();
                CoreResponse::Snapshot(snapshot)
            }
            CoreRequest::RestoreSnapshot { snapshot } => {
                self.store.restore_snapshot(snapshot);
                CoreResponse::Unit
            }
            CoreRequest::ForceDisconnectAllClients => {
                self.disconnect_all_clients();
                CoreResponse::Unit
            }
            CoreRequest::SetHandles { snapshot_handle, wal_handle } => {
                self.snapshot_handle = Some(snapshot_handle.clone());
                self.wal_handle = Some(wal_handle);
                
                // Set handles for peer manager
                self.peer_manager.set_snapshot_handle(snapshot_handle);
                self.peer_manager.set_core_handle(CoreHandle {
                    request_sender: self.request_sender.clone(),
                });
                
                CoreResponse::Unit
            }
            CoreRequest::InitializeStore => {
                match self.initialize_store_internal() {
                    Ok(()) => CoreResponse::Unit,
                    Err(e) => {
                        error!("Failed to initialize store: {}", e);
                        CoreResponse::Unit // Still return unit, but log the error
                    }
                }
            }
        }
    }
    
    /// Handle fire-and-forget commands
    fn handle_command(&mut self, command: CoreCommand) -> bool {
        match command {
            CoreCommand::HandleMiscOperations => {
                if let Err(e) = self.handle_misc_operations() {
                    error!(error = %e, "Failed to handle misc operations");
                }
                false
            }
            CoreCommand::HandleHeartbeat => {
                if let Err(e) = self.handle_heartbeat() {
                    error!(error = %e, "Failed to handle heartbeat");
                }
                false
            }
            CoreCommand::HandlePeerOperations => {
                if let Err(e) = self.handle_peer_operations() {
                    error!(error = %e, "Failed to handle peer operations");
                }
                false
            }
        }
    }
    
    /// Force disconnect all clients
    fn disconnect_all_clients(&mut self) {
        let client_tokens: Vec<Token> = self.connections
            .iter()
            .filter_map(|(token, connection)| {
                if connection.is_client() || connection.is_unknown() {
                    Some(*token)
                } else {
                    None
                }
            })
            .collect();
            
        for token in client_tokens {
            info!(token = ?token, "Force disconnecting client due to unavailability");
            self.remove_client(token);
        }
    }
    
    fn handle_new_connection(&mut self) -> Result<()> {
        loop {
            match self.listener.accept() {
                Ok((mut stream, addr)) => {
                    info!(addr = %addr, "Accepting new connection (type to be determined)");
                    
                    let token = Token(self.next_token);
                    self.next_token += 1;
                    
                    // Register the stream with mio for TCP handling
                    self.poll.registry().register(&mut stream, token, Interest::READABLE | Interest::WRITABLE)?;
                    
                    // Start with unknown connection type - will be determined by first message
                    let connection = UnknownConnection {
                        stream,
                        addr_string: addr.to_string(),
                        outbound_messages: VecDeque::new(),
                        message_buffer: MessageBuffer::new(),
                    };
                    
                    self.connections.insert(token, Connection::Unknown(connection));
                    debug!(addr = %addr, token = ?token, "Connection established as Unknown, awaiting classification");
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    break;
                }
                Err(e) => {
                    error!(error = %e, "Failed to accept connection");
                    break;
                }
            }
        }
        Ok(())
    }
    
    fn handle_connection_event(&mut self, token: Token, readable: bool, writable: bool) -> Result<()> {
        if !self.connections.contains_key(&token) {
            return Ok(());
        }
        
        let mut should_remove = false;
        let mut messages_processed = false;
        
        // Determine connection type and handle appropriately
        let connection_type = self.connections.get(&token).map(|c| {
            if c.is_client() { "client" } 
            else if c.is_peer() { "peer" } 
            else { "unknown" }
        }).unwrap_or("missing");
        
        if readable {
            match connection_type {
                "client" => {
                    match self.handle_client_read(token) {
                        Ok(false) => should_remove = true,
                        Err(e) => {
                            if let Some(connection) = self.connections.get(&token) {
                                error!(
                                    client_addr = %connection.addr_string(),
                                    error = %e,
                                    "Error reading from client"
                                );
                            }
                            should_remove = true;
                        }
                        Ok(true) => {
                            messages_processed = true;
                        }
                    }
                }
                "peer" => {
                    match self.handle_peer_read(token) {
                        Ok(false) => should_remove = true,
                        Err(e) => {
                            if let Some(connection) = self.connections.get(&token) {
                                error!(
                                    peer_addr = %connection.addr_string(),
                                    error = %e,
                                    "Error reading from peer"
                                );
                            }
                            should_remove = true;
                        }
                        Ok(true) => {
                            messages_processed = true;
                        }
                    }
                }
                "unknown" => {
                    match self.handle_unknown_connection_read(token) {
                        Ok(false) => should_remove = true,
                        Err(e) => {
                            if let Some(connection) = self.connections.get(&token) {
                                error!(
                                    addr = %connection.addr_string(),
                                    error = %e,
                                    "Error reading from unknown connection"
                                );
                            }
                            should_remove = true;
                        }
                        Ok(true) => {
                            messages_processed = true;
                        }
                    }
                }
                _ => {
                    warn!(token = ?token, "Connection not found, removing");
                    should_remove = true;
                }
            }
        }
        
        // If we processed messages or if writable event occurred, try to write
        if (writable || messages_processed) && !should_remove {
            let connection_type = self.connections.get(&token).map(|c| {
                if c.is_client() { "client" } 
                else if c.is_peer() { "peer" } 
                else { "unknown" }
            }).unwrap_or("missing");
            
            match connection_type {
                "client" => {
                    if let Err(e) = self.handle_client_write(token) {
                        if let Some(connection) = self.connections.get(&token) {
                            error!(
                                client_addr = %connection.addr_string(),
                                error = %e,
                                "Error writing to client"
                            );
                        }
                        should_remove = true;
                    }
                }
                "peer" => {
                    if let Err(e) = self.handle_peer_write(token) {
                        if let Some(connection) = self.connections.get(&token) {
                            error!(
                                peer_addr = %connection.addr_string(),
                                error = %e,
                                "Error writing to peer"
                            );
                        }
                        should_remove = true;
                    }
                }
                "unknown" => {
                    if let Err(e) = self.handle_unknown_connection_write(token) {
                        if let Some(connection) = self.connections.get(&token) {
                            error!(
                                addr = %connection.addr_string(),
                                error = %e,
                                "Error writing to unknown connection"
                            );
                        }
                        should_remove = true;
                    }
                }
                _ => {
                    // Connection doesn't exist, should remove
                    should_remove = true;
                }
            }
        }
        
        if should_remove {
            let connection_type = self.connections.get(&token).map(|c| {
                if c.is_client() { "client" } 
                else if c.is_peer() { "peer" } 
                else { "unknown" }
            }).unwrap_or("missing");
            
            match connection_type {
                "client" | "unknown" => {
                    self.remove_client(token);
                }
                "peer" => {
                    self.remove_peer_connection(token);
                }
                _ => {
                    // Unknown connection type, just remove it
                    self.connections.remove(&token);
                }
            }
        }
        
        Ok(())
    }
    
    fn handle_unknown_connection_read(&mut self, token: Token) -> Result<bool> {
        let mut read_buffer = [0u8; 8192];
        let mut should_close = false;
        let mut messages_to_classify = Vec::new();
        
        // Read data from unknown connection
        if let Some(Connection::Unknown(unknown_conn)) = self.connections.get_mut(&token) {
            loop {
                match unknown_conn.stream.read(&mut read_buffer) {
                    Ok(0) => {
                        // Connection closed
                        should_close = true;
                        break;
                    }
                    Ok(bytes_read) => {
                        // Add data to message buffer
                        unknown_conn.message_buffer.add_data(&read_buffer[0..bytes_read]);
                        
                        // Try to extract complete messages for classification
                        while let Ok(Some(message)) = unknown_conn.message_buffer.try_decode() {
                            messages_to_classify.push(message);
                        }
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        break;
                    }
                    Err(e) => {
                        error!(error = %e, "Error reading from unknown connection");
                        should_close = true;
                        break;
                    }
                }
            }
        } else {
            return Ok(false);
        }
        
        if should_close {
            return Ok(false);
        }
        
        // Try to classify the connection based on the first complete message
        for protocol_message in messages_to_classify {
            let classified = self.classify_connection_by_message(token, &protocol_message)?;
            if classified {
                // Connection has been reclassified, handle the message with the new type
                return self.handle_reclassified_connection(token, protocol_message);
            }
        }
        
        Ok(true)
    }
    
    fn handle_unknown_connection_write(&mut self, token: Token) -> Result<()> {
        if let Some(Connection::Unknown(unknown_conn)) = self.connections.get_mut(&token) {
            while let Some(message_data) = unknown_conn.outbound_messages.pop_front() {
                match unknown_conn.stream.write_all(&message_data) {
                    Ok(_) => {},
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        // Put the message back and break
                        unknown_conn.outbound_messages.push_front(message_data);
                        break;
                    }
                    Err(e) => {
                        return Err(anyhow::anyhow!("Failed to write to unknown connection: {}", e));
                    }
                }
            }
        }
        Ok(())
    }
    
    fn classify_connection_by_message(&mut self, token: Token, protocol_message: &ProtocolMessage) -> Result<bool> {
        // Determine if this is a peer or client message
        let is_peer_message = matches!(protocol_message,
            ProtocolMessage::PeerStartup(_) |
            ProtocolMessage::PeerFullSyncRequest { .. } |
            ProtocolMessage::PeerFullSyncResponse { .. } |
            ProtocolMessage::PeerSyncRequest(_)
        );
        
        if let Some(Connection::Unknown(unknown_conn)) = self.connections.remove(&token) {
            if is_peer_message {
                info!(addr = %unknown_conn.addr_string, "Classifying connection as peer");
                
                // Convert to peer connection
                let peer_connection = self.peer_manager.handle_new_peer_connection(
                    unknown_conn.addr_string.clone(), 
                    unknown_conn.stream
                );
                
                // Transfer any pending outbound messages
                // Note: peers typically don't have pending messages at this stage
                
                self.connections.insert(token, Connection::Peer(peer_connection));
                
                // Send startup message to new peer
                let startup_message = self.peer_manager.get_startup_message();
                if let Some(Connection::Peer(peer_conn)) = self.connections.get_mut(&token) {
                    peer_conn.queue_message(startup_message);
                }
                
                Ok(true)
            } else {
                info!(addr = %unknown_conn.addr_string, "Classifying connection as client");
                
                // Convert to client connection
                let client_connection = ClientConnection {
                    stream: unknown_conn.stream,
                    addr_string: unknown_conn.addr_string,
                    authenticated: false,
                    client_id: None,
                    notification_queue: NotificationQueue::new(),
                    notification_configs: HashSet::new(),
                    pending_notifications: VecDeque::new(),
                    outbound_messages: unknown_conn.outbound_messages,
                    message_buffer: unknown_conn.message_buffer,
                };
                
                self.connections.insert(token, Connection::Client(client_connection));
                Ok(true)
            }
        } else {
            Ok(false)
        }
    }
    
    fn handle_reclassified_connection(&mut self, token: Token, protocol_message: ProtocolMessage) -> Result<bool> {
        // Handle the message that caused the reclassification
        if let Some(connection) = self.connections.get(&token) {
            if connection.is_peer() {
                // Handle as peer message
                if let Some(peer_message) = self.protocol_to_peer_message(protocol_message) {
                    let peer_addr = connection.addr_string().to_string();
                    let responses = self.peer_manager.handle_peer_message(peer_message, &peer_addr);
                    
                    // Queue responses
                    if let Some(Connection::Peer(peer_conn)) = self.connections.get_mut(&token) {
                        for (_, response_data) in responses {
                            peer_conn.queue_message(response_data);
                        }
                    }
                }
            } else if connection.is_client() {
                // Handle as client message
                if let ProtocolMessage::Store(store_message) = protocol_message {
                    let response = self.process_store_message(store_message, token)?;
                    let response_protocol = ProtocolMessage::Store(response);
                    
                    if let Ok(response_bytes) = ProtocolCodec::encode(&response_protocol) {
                        if let Some(Connection::Client(client_conn)) = self.connections.get_mut(&token) {
                            client_conn.outbound_messages.push_back(response_bytes);
                        }
                    }
                }
            }
        }
        
        Ok(true)
    }
    
    fn protocol_to_peer_message(&self, protocol_message: ProtocolMessage) -> Option<crate::peer_manager::PeerMessage> {
        match protocol_message {
            ProtocolMessage::PeerStartup(startup) => {
                Some(crate::peer_manager::PeerMessage::Startup {
                    machine_id: startup.machine_id,
                    startup_time: startup.startup_time,
                })
            }
            ProtocolMessage::PeerFullSyncRequest { machine_id } => {
                Some(crate::peer_manager::PeerMessage::FullSyncRequest { machine_id })
            }
            ProtocolMessage::PeerFullSyncResponse { snapshot } => {
                Some(crate::peer_manager::PeerMessage::FullSyncResponse { snapshot })
            }
            ProtocolMessage::PeerSyncRequest(sync_req) => {
                if let Ok(requests) = bincode::deserialize::<Vec<qlib_rs::Request>>(&sync_req.requests_data) {
                    Some(crate::peer_manager::PeerMessage::SyncRequest { requests })
                } else {
                    None
                }
            }
            _ => None,
        }
    }
    
    fn handle_client_read(&mut self, token: Token) -> Result<bool> {
        let mut messages_to_process = Vec::new();
        let mut should_close = false;
        
        // First pass: read data and parse messages
        if let Some(connection) = self.connections.get_mut(&token) {
            let mut read_buffer = [0u8; 8192]; // 8KB read buffer
            
            loop {
                match connection.get_stream_mut().read(&mut read_buffer) {
                    Ok(0) => {
                        // Connection closed by client
                        should_close = true;
                        break;
                    }
                    Ok(bytes_read) => {
                        debug!(
                            client_addr = %connection.addr_string(),
                            bytes_read = bytes_read,
                            "Read data from TCP client"
                        );
                        
                        // Add data to buffer
                        connection.get_message_buffer_mut().add_data(&read_buffer[..bytes_read]);
                        
                        // Try to decode messages from buffer
                        while let Some(protocol_message) = connection.get_message_buffer_mut().try_decode()? {
                            match protocol_message {
                                ProtocolMessage::Store(store_msg) => {
                                    messages_to_process.push(store_msg);
                                }
                                ProtocolMessage::Error { id: _, message } => {
                                    error!(
                                        client_addr = %connection.addr_string(),
                                        error = %message,
                                        "Received error message from client"
                                    );
                                }
                                _ => {
                                    warn!(
                                        client_addr = %connection.addr_string(),
                                        "Received unexpected message type from client"
                                    );
                                }
                            }
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        // No more data available
                        break;
                    }
                    Err(e) => {
                        error!(
                            client_addr = %connection.addr_string(),
                            error = %e,
                            "TCP read error"
                        );
                        should_close = true;
                        break;
                    }
                }
            }
        }
        
        // Second pass: process messages
        for store_msg in messages_to_process {
            let response = self.process_store_message(store_msg, token)?;
            let protocol_message = ProtocolMessage::Store(response);
            match ProtocolCodec::encode(&protocol_message) {
                Ok(response_bytes) => {
                    if let Some(conn) = self.connections.get_mut(&token) {
                        debug!(
                            client_addr = %conn.addr_string(),
                            message_length = response_bytes.len(),
                            "Queueing response message"
                        );
                        conn.queue_outbound_message(response_bytes);
                    }
                }
                Err(e) => {
                    error!(error = %e, "Failed to encode response message");
                }
            }
        }
        
        Ok(!should_close)
    }
    
    fn handle_client_write(&mut self, token: Token) -> Result<()> {
        if let Some(connection) = self.connections.get_mut(&token) {
            while let Some(message_bytes) = connection.get_outbound_messages_mut().pop_front() {
                match connection.get_stream_mut().write(&message_bytes) {
                    Ok(bytes_written) => {
                        if bytes_written < message_bytes.len() {
                            // Partial write - put remaining bytes back
                            let remaining = message_bytes[bytes_written..].to_vec();
                            connection.get_outbound_messages_mut().push_front(remaining);
                            break;
                        }
                        debug!(
                            client_addr = %connection.addr_string(),
                            bytes_written = bytes_written,
                            "Sent message to TCP client"
                        );
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        // Put the message back and try again later
                        connection.get_outbound_messages_mut().push_front(message_bytes);
                        break;
                    }
                    Err(e) => {
                        error!(
                            client_addr = %connection.addr_string(),
                            error = %e,
                            "Failed to write to TCP client"
                        );
                        return Err(e.into());
                    }
                }
            }
        }
        Ok(())
    }
    
    fn remove_client(&mut self, token: Token) {
        if let Some(connection) = self.connections.remove(&token) {
            info!(client_addr = %connection.addr_string(), "Removing client connection");
            
            // Handle peer disconnection
            if let Connection::Peer(_) = connection {
                self.peer_manager.handle_peer_disconnection(&connection.addr_string());
            } else if let Connection::Client(client_conn) = connection {
                // Only unregister notifications for client connections
                for config in &client_conn.notification_configs {
                    self.store.unregister_notification(config, &client_conn.notification_queue);
                }
            }
        }
    }
    
    fn process_notifications(&mut self) -> Result<()> {
        // Check each client for new notifications and queue them for sending
        for connection in self.connections.values_mut() {
            if let Connection::Client(client_conn) = connection {
                // Pop notifications from the queue and add to pending
                while let Some(notification) = client_conn.notification_queue.pop() {
                    client_conn.pending_notifications.push_back(notification);
                }
                
                // Send pending notifications as messages
                while let Some(notification) = client_conn.pending_notifications.pop_front() {
                    let notification_message = StoreMessage::Notification { notification };
                    let protocol_message = ProtocolMessage::Store(notification_message);
                    
                    match ProtocolCodec::encode(&protocol_message) {
                        Ok(message_bytes) => {
                            client_conn.outbound_messages.push_back(message_bytes);
                        }
                        Err(e) => {
                            error!(
                                client_addr = %client_conn.addr_string,
                                error = %e,
                                "Failed to encode notification message"
                            );
                        }
                    }
                }
            }
        }
        Ok(())
    }
    
    fn process_store_message(&mut self, message: StoreMessage, token: Token) -> Result<StoreMessage> {
        // Check availability state first - reject clients if unavailable
        let availability_state = self.peer_manager.get_availability_state();
        if matches!(availability_state, AvailabilityState::Unavailable) {
            warn!("Rejecting client request - service is unavailable");
            // Return appropriate error response based on message type
            return Ok(match &message {
                StoreMessage::Authenticate { id, .. } => StoreMessage::AuthenticateResponse {
                    id: id.clone(),
                    response: Err("Service unavailable - system is synchronizing".to_string()),
                },
                StoreMessage::Perform { id, .. } => StoreMessage::PerformResponse {
                    id: id.clone(),
                    response: Err("Service unavailable - system is synchronizing".to_string()),
                },
                StoreMessage::GetEntitySchema { id, .. } => StoreMessage::GetEntitySchemaResponse {
                    id: id.clone(),
                    response: Err("Service unavailable - system is synchronizing".to_string()),
                },
                StoreMessage::GetCompleteEntitySchema { id, .. } => StoreMessage::GetCompleteEntitySchemaResponse {
                    id: id.clone(),
                    response: Err("Service unavailable - system is synchronizing".to_string()),
                },
                StoreMessage::GetFieldSchema { id, .. } => StoreMessage::GetFieldSchemaResponse {
                    id: id.clone(),
                    response: Err("Service unavailable - system is synchronizing".to_string()),
                },
                _ => {
                    // For other message types, return a generic Perform response
                    let id = Snowflake::new().generate().to_string();
                    StoreMessage::PerformResponse {
                        id,
                        response: Err("Service unavailable - system is synchronizing".to_string()),
                    }
                }
            });
        }
        
        // Get connection info
        let (addr_string, authenticated, client_id) = {
            if let Some(connection) = self.connections.get(&token) {
                (connection.addr_string().to_string(), connection.authenticated(), connection.client_id().clone())
            } else {
                return Err(anyhow::anyhow!("Client connection not found"));
            }
        };

        match message {
            StoreMessage::Authenticate { id, subject_name, credential } => {
                info!(
                    client_addr = %addr_string,
                    subject_name = %subject_name,
                    "Processing authentication request"
                );
                
                match self.authenticate_subject(&subject_name, &credential) {
                    Ok(subject_id) => {
                        // Update connection state
                        if let Some(connection) = self.connections.get_mut(&token) {
                            connection.set_authenticated(true);
                            connection.set_client_id(Some(subject_id.clone()));
                        }
                        
                        info!(
                            client_addr = %addr_string,
                            subject_id = %subject_id,
                            "Client authenticated successfully"
                        );
                        
                        Ok(StoreMessage::AuthenticateResponse {
                            id,
                            response: Ok(AuthenticationResult {
                                subject_id,
                                subject_type: "User".to_string(), // TODO: determine actual subject type
                            }),
                        })
                    }
                    Err(e) => {
                        warn!(
                            client_addr = %addr_string,
                            error = %e,
                            "Authentication failed"
                        );
                        
                        Ok(StoreMessage::AuthenticateResponse {
                            id,
                            response: Err(format!("Authentication failed: {}", e)),
                        })
                    }
                }
            }
            
            StoreMessage::AuthenticateResponse { .. } => {
                error!(
                    client_addr = %addr_string,
                    "Unexpected AuthenticateResponse from client"
                );
                Err(anyhow::anyhow!("Unexpected AuthenticateResponse from client"))
            }
            
            // All other messages require authentication
            _ => {
                if !authenticated {
                    error!(
                        client_addr = %addr_string,
                        "Unauthenticated client attempted to send message"
                    );
                    return Err(anyhow::anyhow!("Client must authenticate first"));
                }
                
                let client_id = client_id.ok_or_else(|| {
                    anyhow::anyhow!("Authenticated client missing client_id")
                })?;
                
                // Process the store message with the direct store
                match message {
                    StoreMessage::Perform { id, requests } => {
                        debug!(
                            client_addr = %addr_string,
                            request_count = requests.len(),
                            "Processing Perform request"
                        );
                        
                        match self.check_requests_authorization(client_id, requests) {
                            Ok(authorized_requests) => {
                                // Check if any requests are write operations
                                let has_writes = authorized_requests.iter().any(|req| match req {
                                    Request::Write { .. } | Request::Create { .. } | Request::Delete { .. } => true,
                                    _ => false,
                                });
                                
                                let result = if has_writes {
                                    self.store.perform_mut(authorized_requests)
                                } else {
                                    self.store.perform(authorized_requests)
                                };
                                
                                match result {
                                    Ok(results) => {
                                        Ok(StoreMessage::PerformResponse { 
                                            id, 
                                            response: Ok(results) 
                                        })
                                    }
                                    Err(e) => {
                                        error!(error = %e, "Store perform failed");
                                        Ok(StoreMessage::PerformResponse { 
                                            id, 
                                            response: Err(format!("Store error: {}", e)) 
                                        })
                                    }
                                }
                            }
                            Err(e) => {
                                error!(error = %e, "Authorization check failed");
                                Ok(StoreMessage::PerformResponse { 
                                    id, 
                                    response: Err(format!("Authorization error: {}", e)) 
                                })
                            }
                        }
                    }
                    
                    StoreMessage::GetEntitySchema { id, entity_type } => {
                        debug!(
                            client_addr = %addr_string,
                            entity_type = %entity_type,
                            "Processing GetEntitySchema request"
                        );
                        
                        match self.get_entity_schema(&entity_type) {
                            Ok(schema) => Ok(StoreMessage::GetEntitySchemaResponse {
                                id,
                                response: Ok(Some(schema)),
                            }),
                            Err(e) => Ok(StoreMessage::GetEntitySchemaResponse {
                                id,
                                response: Err(format!("Schema error: {}", e)),
                            }),
                        }
                    }
                    
                    StoreMessage::GetCompleteEntitySchema { id, entity_type } => {
                        debug!(
                            client_addr = %addr_string,
                            entity_type = %entity_type,
                            "Processing GetCompleteEntitySchema request"
                        );
                        
                        match self.get_complete_entity_schema(&entity_type) {
                            Ok(schema) => Ok(StoreMessage::GetCompleteEntitySchemaResponse {
                                id,
                                response: Ok(schema),
                            }),
                            Err(e) => Ok(StoreMessage::GetCompleteEntitySchemaResponse {
                                id,
                                response: Err(format!("Schema error: {}", e)),
                            }),
                        }
                    }
                    
                    StoreMessage::GetFieldSchema { id, entity_type, field_type } => {
                        debug!(
                            client_addr = %addr_string,
                            entity_type = %entity_type,
                            field_type = %field_type,
                            "Processing GetFieldSchema request"
                        );
                        
                        match self.get_field_schema(&entity_type, &field_type) {
                            Ok(schema) => Ok(StoreMessage::GetFieldSchemaResponse {
                                id,
                                response: Ok(Some(schema)),
                            }),
                            Err(e) => Ok(StoreMessage::GetFieldSchemaResponse {
                                id,
                                response: Err(format!("Schema error: {}", e)),
                            }),
                        }
                    }
                    
                    StoreMessage::EntityExists { id, entity_id } => {
                        debug!(
                            client_addr = %addr_string,
                            entity_id = %entity_id,
                            "Processing EntityExists request"
                        );
                        
                        let exists = self.entity_exists(&entity_id);
                        Ok(StoreMessage::EntityExistsResponse {
                            id,
                            response: exists,
                        })
                    }
                    
                    StoreMessage::FieldExists { id, entity_type, field_type } => {
                        debug!(
                            client_addr = %addr_string,
                            entity_type = %entity_type,
                            field_type = %field_type,
                            "Processing FieldExists request"
                        );
                        
                        let exists = self.field_exists(&entity_type, &field_type);
                        Ok(StoreMessage::FieldExistsResponse {
                            id,
                            response: exists,
                        })
                    }
                    
                    StoreMessage::FindEntities { id, entity_type, page_opts, filter } => {
                        debug!(
                            client_addr = %addr_string,
                            entity_type = %entity_type,
                            "Processing FindEntities request"
                        );
                        
                        match self.find_entities_paginated(&entity_type, page_opts, filter) {
                            Ok(result) => Ok(StoreMessage::FindEntitiesResponse {
                                id,
                                response: Ok(result),
                            }),
                            Err(e) => Ok(StoreMessage::FindEntitiesResponse {
                                id,
                                response: Err(format!("Find error: {}", e)),
                            }),
                        }
                    }
                    
                    StoreMessage::FindEntitiesExact { id, entity_type, page_opts, filter } => {
                        debug!(
                            client_addr = %addr_string,
                            entity_type = %entity_type,
                            "Processing FindEntitiesExact request"
                        );
                        
                        match self.find_entities_exact(&entity_type, page_opts, filter) {
                            Ok(result) => Ok(StoreMessage::FindEntitiesExactResponse {
                                id,
                                response: Ok(result),
                            }),
                            Err(e) => Ok(StoreMessage::FindEntitiesExactResponse {
                                id,
                                response: Err(format!("Find error: {}", e)),
                            }),
                        }
                    }
                    
                    StoreMessage::GetEntityTypes { id, page_opts } => {
                        debug!(
                            client_addr = %addr_string,
                            "Processing GetEntityTypes request"
                        );
                        
                        match self.get_entity_types_paginated(page_opts) {
                            Ok(result) => Ok(StoreMessage::GetEntityTypesResponse {
                                id,
                                response: Ok(result),
                            }),
                            Err(e) => Ok(StoreMessage::GetEntityTypesResponse {
                                id,
                                response: Err(format!("Get entity types error: {}", e)),
                            }),
                        }
                    }
                    
                    StoreMessage::RegisterNotification { id, config } => {
                        debug!(
                            client_addr = %addr_string,
                            "Processing RegisterNotification request"
                        );
                        
                        match self.register_notification_for_client(token, config) {
                            Ok(_) => Ok(StoreMessage::RegisterNotificationResponse {
                                id,
                                response: Ok(()),
                            }),
                            Err(e) => Ok(StoreMessage::RegisterNotificationResponse {
                                id,
                                response: Err(format!("Notification error: {}", e)),
                            }),
                        }
                    }
                    
                    StoreMessage::UnregisterNotification { id, config } => {
                        debug!(
                            client_addr = %addr_string,
                            "Processing UnregisterNotification request"
                        );
                        
                        let unregistered = self.unregister_notification_for_client(token, config);
                        Ok(StoreMessage::UnregisterNotificationResponse {
                            id,
                            response: unregistered,
                        })
                    }
                    
                    // Add other message types as needed...
                    _ => {
                        warn!("Unhandled store message type");
                        Err(anyhow::anyhow!("Unhandled message type"))
                    }
                }
            }
        }
    }
    
    /// Process any pending write requests from the store
    fn process_write_requests(&mut self) -> Result<()> {
        let mut requests_to_write = Vec::new();
        
        // Drain the write queue from the store
        while let Some(request) = self.store.write_queue.pop() {
            requests_to_write.push(request);
        }
        
        // Write to WAL if we have requests
        if !requests_to_write.is_empty() {
            if let Some(wal_handle) = &self.wal_handle {
                for request in &requests_to_write {
                    if let Err(e) = wal_handle.write_request(request.clone()) {
                        error!(error = %e, "Failed to write request to WAL");
                    }
                }
            }
            
            // Send to peers for synchronization
            let sync_messages = self.peer_manager.send_sync_message(requests_to_write);
            
            // Queue sync messages to all connected peers
            for (target_peer, message_data) in sync_messages {
                // Find peer connection and queue message
                for connection in self.connections.values_mut() {
                    if let Connection::Peer(peer_conn) = connection {
                        if peer_conn.addr_string == target_peer {
                            peer_conn.queue_message(message_data.clone());
                            break;
                        }
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Handle misc operations (fault tolerance, etc.)
    fn handle_misc_operations(&mut self) -> Result<()> {
        // Check leadership and handle fault tolerance if we're the leader
        let (is_leader, _) = self.peer_manager.get_leadership_info();
        if is_leader {
            self.handle_fault_tolerance_management()?;
        }
        
        Ok(())
    }
    
    /// Handle peer management operations
    fn handle_peer_operations(&mut self) -> Result<()> {
        // Get list of existing peer connections
        let existing_peer_connections: Vec<String> = self.connections
            .values()
            .filter_map(|conn| {
                if let Connection::Peer(peer_conn) = conn {
                    Some(peer_conn.addr_string.clone())
                } else {
                    None
                }
            })
            .collect();
        
        // Handle periodic peer operations (leadership, reconnection, etc.)
        let (connection_results, disconnected_peers) = self.peer_manager
            .handle_periodic_operations(&existing_peer_connections);
        
        // Process new outbound connections
        for result in connection_results {
            match result {
                crate::peer_manager::ConnectionResult::OutboundSuccess { peer_addr, stream } => {
                    // Register the new peer connection
                    let token = Token(self.next_token);
                    self.next_token += 1;
                    
                    let peer_connection = self.peer_manager.handle_new_peer_connection(peer_addr.clone(), stream);
                    self.connections.insert(token, Connection::Peer(peer_connection));
                    
                    // Register with poll for events
                    if let Some(Connection::Peer(peer_conn)) = self.connections.get_mut(&token) {
                        self.poll.registry().register(
                            peer_conn.get_stream_mut(),
                            token,
                            Interest::READABLE | Interest::WRITABLE
                        )?;
                    }
                    
                    info!(
                        peer_addr = %peer_addr,
                        token = ?token,
                        "New outbound peer connection established"
                    );
                }
                crate::peer_manager::ConnectionResult::OutboundFailure { peer_addr, error } => {
                    warn!(
                        peer_addr = %peer_addr,
                        error = %error,
                        "Failed to establish outbound peer connection"
                    );
                }
            }
        }
        
        // Handle disconnected peers
        for peer_addr in disconnected_peers {
            // Find and remove the connection
            let mut token_to_remove = None;
            for (token, connection) in &self.connections {
                if let Connection::Peer(peer_conn) = connection {
                    if peer_conn.addr_string == peer_addr {
                        token_to_remove = Some(*token);
                        break;
                    }
                }
            }
            
            if let Some(token) = token_to_remove {
                self.connections.remove(&token);
                info!(
                    peer_addr = %peer_addr,
                    token = ?token,
                    "Peer connection removed"
                );
            }
        }
        
        Ok(())
    }
    
    /// Handle heartbeat writing
    fn handle_heartbeat(&mut self) -> Result<()> {
        self.write_heartbeat()?;
        Ok(())
    }
    
    /// Handle fault tolerance and leader management when this instance is the leader
    fn handle_fault_tolerance_management(&mut self) -> Result<()> {
        // Find us as a candidate
        let me_as_candidate = {
            let machine = &self.config.machine_id;
            
            let candidates = self.store.find_entities_paginated(
                &qlib_rs::et::candidate(), 
                None,
                Some(format!("Name == 'qcore' && Parent->Name == '{}'", machine))
            )?;
            
            candidates.items.first().cloned()
        };
        
        // Update available list and current leader for all fault tolerance entities
        let fault_tolerances = self.store.find_entities_paginated(
            &qlib_rs::et::fault_tolerance(), 
            None,
            None
        )?;
        
        for ft_entity_id in fault_tolerances.items {
            let ft_fields = self.store.perform(vec![
                qlib_rs::sread!(ft_entity_id.clone(), qlib_rs::ft::candidate_list()),
                qlib_rs::sread!(ft_entity_id.clone(), qlib_rs::ft::available_list()),
                qlib_rs::sread!(ft_entity_id.clone(), qlib_rs::ft::current_leader())
            ])?;
            
            let default_candidates = Vec::new();
            let candidates = ft_fields
                .get(0)
                .and_then(|r| match r {
                    Request::Read { value: Some(value), .. } => value.as_entity_list(),
                    _ => None,
                })
                .unwrap_or(&default_candidates);
            
            let mut available = Vec::new();
            for candidate_id in candidates.iter() {
                let candidate_fields = self.store.perform(vec![
                    qlib_rs::sread!(candidate_id.clone(), qlib_rs::ft::heartbeat()),
                    qlib_rs::sread!(candidate_id.clone(), qlib_rs::ft::make_me()),
                    qlib_rs::sread!(candidate_id.clone(), qlib_rs::ft::death_detection_timeout()),
                ])?;
                
                // Process candidate availability logic
                if let (Some(heartbeat_req), Some(make_me_req), Some(timeout_req)) = (
                    candidate_fields.get(0),
                    candidate_fields.get(1),
                    candidate_fields.get(2)
                ) {
                    if let (
                        Request::Read { write_time: Some(heartbeat_time), .. },
                        Request::Read { value: Some(make_me_value), .. },
                        Request::Read { value: Some(timeout_value), .. }
                    ) = (heartbeat_req, make_me_req, timeout_req) {
                        if let (Some(make_me_choice), Some(timeout_millis)) = (
                            make_me_value.as_choice(),
                            timeout_value.as_int()
                        ) {
                            let desired_availability = match make_me_choice {
                                1 => AvailabilityState::Available,
                                _ => AvailabilityState::Unavailable,
                            };
                            
                            let now = qlib_rs::now();
                            let death_timeout = time::Duration::milliseconds(timeout_millis);
                            
                            if desired_availability == AvailabilityState::Available &&
                               *heartbeat_time + death_timeout > now {
                                available.push(candidate_id.clone());
                            }
                        }
                    }
                }
            }
            
            // Update the fault tolerance entity
            let mut requests = vec![
                qlib_rs::swrite!(ft_entity_id.clone(), qlib_rs::ft::available_list(), Some(Value::EntityList(available.clone())), PushCondition::Changes),
            ];
            
            // Handle leadership assignment
            if let Some(me_as_candidate) = &me_as_candidate {
                if candidates.contains(me_as_candidate) {
                    requests.push(
                        qlib_rs::swrite!(ft_entity_id.clone(), qlib_rs::ft::current_leader(), qlib_rs::sref!(Some(me_as_candidate.clone())), PushCondition::Changes)
                    );
                }
            }
            
            self.store.perform_mut(requests)?;
        }
        
        Ok(())
    }
    
    /// Handle heartbeat writing for this machine
    fn write_heartbeat(&mut self) -> Result<()> {
        let machine = &self.config.machine_id;
        
        let candidates = self.store.find_entities_paginated(
            &qlib_rs::et::candidate(), 
            None,
            Some(format!("Name == 'qcore' && Parent->Name == '{}'", machine))
        )?;
        
        if let Some(candidate) = candidates.items.first() {
            self.store.perform_mut(vec![
                qlib_rs::swrite!(candidate.clone(), qlib_rs::ft::heartbeat(), qlib_rs::schoice!(0)),
                qlib_rs::swrite!(candidate.clone(), qlib_rs::ft::make_me(), qlib_rs::schoice!(1), PushCondition::Changes)
            ])?;
        }
        
        Ok(())
    }
    
    /// Check authorization for a list of requests and return only authorized ones
    fn check_requests_authorization(
        &mut self,
        client_id: EntityId,
        requests: Vec<Request>,
    ) -> Result<Vec<Request>> {
        let mut authorized_requests = Vec::new();
        
        for request in requests {
            // Extract entity_id and field_type from the request
            let authorization_needed = match &request {
                Request::Read { entity_id, field_type, .. } => Some((entity_id, field_type)),
                Request::Write { entity_id, field_type, .. } => Some((entity_id, field_type)),
                Request::Create { .. } => None, // No field-level authorization for creation
                Request::Delete { .. } => None, // No field-level authorization for deletion
                Request::SchemaUpdate { .. } => None, // No field-level authorization for schema updates
                _ => None, // For other request types, skip authorization check
            };
            
            if let Some((entity_id, field_type)) = authorization_needed {
                let scope = get_scope(&self.store, &mut self.cel_executor, &self.permission_cache, &client_id, entity_id, field_type)?;
                
                match scope {
                    AuthorizationScope::ReadOnly | AuthorizationScope::ReadWrite => {
                        authorized_requests.push(request);
                    }
                    AuthorizationScope::None => {
                        // Skip unauthorized requests
                        continue;
                    }
                }
            } else {
                // No authorization needed for this request type
                authorized_requests.push(request);
            }
        }
        
        Ok(authorized_requests)
    }
    
    /// Authenticate a subject with credentials
    fn authenticate_subject(
        &mut self,
        subject_name: &str,
        credential: &str,
    ) -> Result<EntityId> {
        let auth_config = AuthConfig::default(); // Use default auth config
        authenticate_subject(&mut self.store, subject_name, credential, &auth_config)
            .map_err(|e| anyhow::anyhow!("Authentication error: {}", e))
    }
    
    /// Get entity schema
    pub fn get_entity_schema(&self, entity_type: &EntityType) -> Result<qlib_rs::EntitySchema<qlib_rs::Single>> {
        self.store.get_entity_schema(entity_type)
            .map_err(|e| anyhow::anyhow!("Failed to get entity schema: {}", e))
    }
    
    /// Get complete entity schema
    pub fn get_complete_entity_schema(&self, entity_type: &EntityType) -> Result<qlib_rs::EntitySchema<qlib_rs::Complete>> {
        self.store.get_complete_entity_schema(entity_type)
            .map_err(|e| anyhow::anyhow!("Failed to get complete entity schema: {}", e))
    }
    
    /// Get field schema
    pub fn get_field_schema(&self, entity_type: &EntityType, field_type: &FieldType) -> Result<qlib_rs::FieldSchema> {
        self.store.get_field_schema(entity_type, field_type)
            .map_err(|e| anyhow::anyhow!("Failed to get field schema: {}", e))
    }
    
    /// Check if entity exists
    pub fn entity_exists(&self, entity_id: &EntityId) -> bool {
        self.store.entity_exists(entity_id)
    }
    
    /// Check if field exists
    pub fn field_exists(&self, entity_type: &EntityType, field_type: &FieldType) -> bool {
        self.store.field_exists(entity_type, field_type)
    }
    
    /// Find entities with pagination
    pub fn find_entities_paginated(&self, entity_type: &EntityType, page_opts: Option<PageOpts>, filter: Option<String>) -> Result<PageResult<EntityId>> {
        self.store.find_entities_paginated(entity_type, page_opts, filter)
            .map_err(|e| anyhow::anyhow!("Failed to find entities: {}", e))
    }
    
    /// Find entities exact match
    pub fn find_entities_exact(&self, entity_type: &EntityType, page_opts: Option<PageOpts>, filter: Option<String>) -> Result<PageResult<EntityId>> {
        self.store.find_entities_exact(entity_type, page_opts, filter)
            .map_err(|e| anyhow::anyhow!("Failed to find entities exact: {}", e))
    }
    
    /// Get entity types with pagination
    pub fn get_entity_types_paginated(&self, page_opts: Option<PageOpts>) -> Result<PageResult<EntityType>> {
        self.store.get_entity_types_paginated(page_opts)
            .map_err(|e| anyhow::anyhow!("Failed to get entity types: {}", e))
    }
    
    /// Register notification configuration for a specific client
    fn register_notification_for_client(&mut self, token: Token, config: NotifyConfig) -> Result<()> {
        if let Some(connection) = self.connections.get_mut(&token) {
            // Register the notification with the store
            self.store.register_notification(config.clone(), connection.notification_queue().unwrap().clone())?;
            
            // Track this config for the client
            connection.notification_configs_mut().unwrap().insert(config);
            
            debug!(
                client_addr = %connection.addr_string(),
                "Registered notification configuration for client"
            );
            
            Ok(())
        } else {
            Err(anyhow::anyhow!("Client connection not found"))
        }
    }
    
    /// Unregister notification configuration for a specific client
    fn unregister_notification_for_client(&mut self, token: Token, config: NotifyConfig) -> bool {
        if let Some(connection) = self.connections.get_mut(&token) {
            // Unregister from store
            let unregistered = self.store.unregister_notification(&config, connection.notification_queue().unwrap());
            
            // Remove from client's config set
            connection.notification_configs_mut().unwrap().remove(&config);
            
            debug!(
                client_addr = %connection.addr_string(),
                unregistered = unregistered,
                "Unregistered notification configuration for client"
            );
            
            unregistered
        } else {
            false
        }
    }
    
    /// Handle reading from peer connection
    fn handle_peer_read(&mut self, token: Token) -> Result<bool> {
        let mut messages_to_process = Vec::new();
        let mut should_close = false;
        let mut peer_addr = String::new();
        
        if let Some(Connection::Peer(peer_conn)) = self.connections.get_mut(&token) {
            peer_addr = peer_conn.addr_string.clone();
            match self.peer_manager.handle_peer_read(peer_conn) {
                Ok(peer_messages) => {
                    for message in peer_messages {
                        messages_to_process.push(message);
                    }
                }
                Err(e) => {
                    error!(
                        peer_addr = %peer_conn.addr_string,
                        error = %e,
                        "Failed to read from peer"
                    );
                    should_close = true;
                }
            }
        }
        
        // Process peer messages
        for message in messages_to_process {
            let responses = self.peer_manager.handle_peer_message(message, &peer_addr);
            
            // Queue responses back to peers
            for (target_peer, response_data) in responses {
                if target_peer == peer_addr {
                    // Queue message for this peer
                    if let Some(Connection::Peer(peer_conn)) = self.connections.get_mut(&token) {
                        peer_conn.queue_message(response_data);
                    }
                } else {
                    // Find and queue message for other peer
                    for connection in self.connections.values_mut() {
                        if let Connection::Peer(other_peer) = connection {
                            if other_peer.addr_string == target_peer {
                                other_peer.queue_message(response_data.clone());
                                break;
                            }
                        }
                    }
                }
            }
        }
        
        Ok(!should_close)
    }
    
    /// Handle writing to peer connection
    fn handle_peer_write(&mut self, token: Token) -> Result<()> {
        if let Some(Connection::Peer(peer_conn)) = self.connections.get_mut(&token) {
            self.peer_manager.handle_peer_write(peer_conn)?;
        }
        Ok(())
    }
    
    /// Remove peer connection and notify peer manager
    fn remove_peer_connection(&mut self, token: Token) {
        if let Some(Connection::Peer(peer_conn)) = self.connections.remove(&token) {
            info!(
                peer_addr = %peer_conn.addr_string,
                token = ?token,
                "Removing peer connection"
            );
            
            // Notify peer manager about disconnection
            self.peer_manager.handle_peer_disconnection(&peer_conn.addr_string);
        }
    }
}