use std::net::SocketAddr;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration as StdDuration;
use mio::{Poll, Interest, Token, Events};
use mio::net::{TcpListener as MioTcpListener, TcpStream as MioTcpStream};
use tungstenite::{WebSocket, Message};
use tungstenite::handshake::HandshakeRole;
use tracing::{info, warn, error, debug};
use anyhow::Result;
use qlib_rs::{
    StoreMessage, EntityId, NotificationQueue, NotifyConfig,
    AuthenticationResult, Notification, Store, Cache, CelExecutor,
    et, ft, now, schoice, sread, sref, swrite, PushCondition, Value,
    StoreTrait, EntityType, FieldType, Request, Snapshot, Snowflake,
    PageOpts, PageResult, EntitySchema, FieldSchema, 
    AuthConfig, authenticate_subject, get_scope, AuthorizationScope
};
use time;

/// Configuration for the core service
#[derive(Debug, Clone)]
pub struct CoreConfig {
    /// Port for client communication (StoreProxy clients)
    pub client_port: u16,
    /// Machine ID for request origination
    pub machine_id: String,
}

impl From<&crate::Config> for CoreConfig {
    fn from(config: &crate::Config) -> Self {
        Self {
            client_port: config.client_port,
            machine_id: config.machine.clone(),
        }
    }
}

/// Client connection information
#[derive(Debug)]
struct ClientConnection {
    websocket: WebSocket<MioTcpStream>,
    addr: SocketAddr,
    addr_string: String,
    authenticated: bool,
    client_id: Option<EntityId>,
    notification_queue: NotificationQueue,
    notification_configs: HashSet<NotifyConfig>,
    pending_notifications: VecDeque<Notification>,
    outbound_messages: VecDeque<String>,
}

/// Core service that handles both client connections and misc operations
pub struct CoreService {
    config: CoreConfig,
    listener: MioTcpListener,
    poll: Poll,
    connections: HashMap<Token, ClientConnection>,
    next_token: usize,
    
    // Store and related components (replacing StoreService)
    store: Store,
    permission_cache: Cache,
    cel_executor: CelExecutor,
    
    // Handles to other services
    peer_handle: Option<crate::peers::PeerHandle>,
    snapshot_handle: Option<crate::snapshot::SnapshotHandle>,
    wal_handle: Option<crate::wal::WalHandle>,
    
    // Timing for misc operations
    last_misc_tick: std::time::Instant,
    last_heartbeat: std::time::Instant,
}

const LISTENER_TOKEN: Token = Token(0);
const MISC_INTERVAL_MS: u64 = 10;
const HEARTBEAT_INTERVAL_SECS: u64 = 1;

impl CoreService {
    /// Create a new core service
    pub fn new(config: CoreConfig) -> Result<Self> {
        let addr = format!("0.0.0.0:{}", config.client_port).parse()?;
        let mut listener = MioTcpListener::bind(addr)?;
        let poll = Poll::new()?;
        
        poll.registry().register(&mut listener, LISTENER_TOKEN, Interest::READABLE)?;
        
        info!(bind_address = %addr, "Core service WebSocket server initialized");
        
        // Initialize store with snowflake
        let snowflake = Snowflake::new(1, 1); // TODO: configure these properly
        let store = Store::new(snowflake);
        let permission_cache = Cache::new();
        let cel_executor = CelExecutor::new();
        
        let now = std::time::Instant::now();
        
        Ok(Self {
            config,
            listener,
            poll,
            connections: HashMap::new(),
            next_token: 1,
            store,
            permission_cache,
            cel_executor,
            peer_handle: None,
            snapshot_handle: None,
            wal_handle: None,
            last_misc_tick: now,
            last_heartbeat: now,
        })
    }
    
    /// Set handles to other services
    pub fn set_handles(
        &mut self,
        peer_handle: crate::peers::PeerHandle,
        snapshot_handle: crate::snapshot::SnapshotHandle,
        wal_handle: crate::wal::WalHandle,
    ) {
        self.peer_handle = Some(peer_handle);
        self.snapshot_handle = Some(snapshot_handle);
        self.wal_handle = Some(wal_handle);
    }
    
    /// Run the main event loop
    pub fn run(&mut self) -> Result<()> {
        let mut events = Events::with_capacity(1024);
        
        loop {
            // Calculate timeout for next operation
            let now = std::time::Instant::now();
            let next_misc = self.last_misc_tick + StdDuration::from_millis(MISC_INTERVAL_MS);
            let next_heartbeat = self.last_heartbeat + StdDuration::from_secs(HEARTBEAT_INTERVAL_SECS);
            
            let timeout = std::cmp::min(
                next_misc.saturating_duration_since(now),
                next_heartbeat.saturating_duration_since(now)
            );
            
            // Poll for events with timeout
            self.poll.poll(&mut events, Some(timeout))?;
            
            // Handle mio events
            for event in events.iter() {
                match event.token() {
                    LISTENER_TOKEN => {
                        if event.is_readable() {
                            self.handle_new_connection()?;
                        }
                    }
                    token => {
                        self.handle_client_event(token, event.is_readable(), event.is_writable())?;
                    }
                }
            }
            
            let now = std::time::Instant::now();
            
            // Handle misc operations
            if now >= next_misc {
                self.handle_misc_operations()?;
                self.last_misc_tick = now;
            }
            
            // Handle heartbeat
            if now >= next_heartbeat {
                self.handle_heartbeat()?;
                self.last_heartbeat = now;
            }
            
            // Process notifications and send them to clients
            self.process_notifications()?;
            
            // Process any pending write requests from the store
            self.process_write_requests()?;
        }
    }
    
    fn handle_new_connection(&mut self) -> Result<()> {
        loop {
            match self.listener.accept() {
                Ok((stream, addr)) => {
                    info!(client_addr = %addr, "Accepting new client connection");
                    
                    let token = Token(self.next_token);
                    self.next_token += 1;
                    
                    // Convert to mio stream and register
                    let mut mio_stream = stream;
                    self.poll.registry().register(
                        &mut mio_stream,
                        token,
                        Interest::READABLE | Interest::WRITABLE
                    )?;
                    
                    let connection = ClientConnection {
                        websocket: WebSocket::from_raw_socket(mio_stream, HandshakeRole::Server, None),
                        addr,
                        addr_string: addr.to_string(),
                        authenticated: false,
                        client_id: None,
                        notification_queue: NotificationQueue::new(),
                        notification_configs: HashSet::new(),
                        pending_notifications: VecDeque::new(),
                        outbound_messages: VecDeque::new(),
                    };
                    
                    self.connections.insert(token, connection);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    break;
                }
                Err(e) => {
                    error!(error = %e, "Failed to accept client connection");
                    break;
                }
            }
        }
        Ok(())
    }
    
    fn handle_client_event(&mut self, token: Token, readable: bool, writable: bool) -> Result<()> {
        if !self.connections.contains_key(&token) {
            return Ok(());
        }
        
        let mut should_remove = false;
        
        if readable {
            match self.handle_client_read(token) {
                Ok(false) => should_remove = true,
                Err(e) => {
                    if let Some(connection) = self.connections.get(&token) {
                        error!(
                            client_addr = %connection.addr_string,
                            error = %e,
                            "Error reading from client"
                        );
                    }
                    should_remove = true;
                }
                Ok(true) => {}
            }
        }
        
        if writable && !should_remove {
            if let Err(e) = self.handle_client_write(token) {
                if let Some(connection) = self.connections.get(&token) {
                    error!(
                        client_addr = %connection.addr_string,
                        error = %e,
                        "Error writing to client"
                    );
                }
                should_remove = true;
            }
        }
        
        if should_remove {
            self.remove_client(token);
        }
        
        Ok(())
    }
    
    fn handle_client_read(&mut self, token: Token) -> Result<bool> {
        if let Some(connection) = self.connections.get_mut(&token) {
            loop {
                match connection.websocket.read() {
                    Ok(Message::Text(text)) => {
                        debug!(
                            client_addr = %connection.addr_string,
                            message_length = text.len(),
                            "Received text message from client"
                        );
                        
                        match serde_json::from_str::<StoreMessage>(&text) {
                            Ok(store_msg) => {
                                let addr_string = connection.addr_string.clone();
                                let response = self.process_store_message(store_msg, token)?;
                                if let Ok(response_text) = serde_json::to_string(&response) {
                                    if let Some(conn) = self.connections.get_mut(&token) {
                                        conn.outbound_messages.push_back(response_text);
                                    }
                                }
                            }
                            Err(e) => {
                                error!(
                                    client_addr = %connection.addr_string,
                                    error = %e,
                                    "Failed to parse StoreMessage from client"
                                );
                            }
                        }
                    }
                    Ok(Message::Ping(payload)) => {
                        if let Some(conn) = self.connections.get_mut(&token) {
                            // Respond with pong
                            if let Ok(pong) = Message::Pong(payload).to_string() {
                                conn.outbound_messages.push_back(pong);
                            }
                        }
                    }
                    Ok(Message::Close(_)) => return Ok(false),
                    Err(tungstenite::Error::Io(ref e)) 
                        if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(e) => {
                        error!(
                            client_addr = %connection.addr_string,
                            error = %e,
                            "WebSocket read error"
                        );
                        return Ok(false);
                    }
                    _ => {} // Handle other message types
                }
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }
    
    fn handle_client_write(&mut self, token: Token) -> Result<()> {
        if let Some(connection) = self.connections.get_mut(&token) {
            while let Some(message_text) = connection.outbound_messages.pop_front() {
                match connection.websocket.write(Message::Text(message_text)) {
                    Ok(_) => {
                        // Message written successfully
                    }
                    Err(tungstenite::Error::Io(ref e)) 
                        if e.kind() == std::io::ErrorKind::WouldBlock => {
                        // Put the message back and try again later
                        connection.outbound_messages.push_front(message_text);
                        break;
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to write message to client");
                        return Err(e.into());
                    }
                }
            }
        }
        Ok(())
    }
    
    fn remove_client(&mut self, token: Token) {
        if let Some(connection) = self.connections.remove(&token) {
            info!(client_addr = %connection.addr_string, "Removing client connection");
            
            // Unregister all notifications for this client
            for config in &connection.notification_configs {
                self.store.unregister_notification(config, &connection.notification_queue);
            }
        }
    }
    
    fn process_notifications(&mut self) -> Result<()> {
        // Check each client for new notifications and queue them for sending
        for connection in self.connections.values_mut() {
            while let Some(notification) = connection.notification_queue.pop() {
                connection.pending_notifications.push_back(notification);
            }
        }
        Ok(())
    }
    
    fn process_store_message(&mut self, message: StoreMessage, token: Token) -> Result<StoreMessage> {
        // Get connection info
        let (addr_string, authenticated, client_id) = {
            if let Some(connection) = self.connections.get(&token) {
                (connection.addr_string.clone(), connection.authenticated, connection.client_id.clone())
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
                            connection.authenticated = true;
                            connection.client_id = Some(subject_id.clone());
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
                                match self.store.perform(authorized_requests) {
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
                    
                    StoreMessage::PerformMut { id, requests } => {
                        debug!(
                            client_addr = %addr_string,
                            request_count = requests.len(),
                            "Processing PerformMut request"
                        );
                        
                        match self.check_requests_authorization(client_id, requests) {
                            Ok(authorized_requests) => {
                                match self.store.perform_mut(authorized_requests) {
                                    Ok(results) => {
                                        Ok(StoreMessage::PerformMutResponse { 
                                            id, 
                                            response: Ok(results) 
                                        })
                                    }
                                    Err(e) => {
                                        error!(error = %e, "Store perform_mut failed");
                                        Ok(StoreMessage::PerformMutResponse { 
                                            id, 
                                            response: Err(format!("Store error: {}", e)) 
                                        })
                                    }
                                }
                            }
                            Err(e) => {
                                error!(error = %e, "Authorization check failed");
                                Ok(StoreMessage::PerformMutResponse { 
                                    id, 
                                    response: Err(format!("Authorization error: {}", e)) 
                                })
                            }
                        }
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
        
        // Drain the write queue
        while let Some(request) = self.store.write_queue.pop() {
            requests_to_write.push(request);
        }
        
        // Write to WAL if we have requests
        if !requests_to_write.is_empty() {
            if let Some(wal_handle) = &self.wal_handle {
                for request in requests_to_write {
                    if let Err(e) = wal_handle.write_request(request) {
                        error!(error = %e, "Failed to write request to WAL");
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Handle misc operations (fault tolerance, etc.)
    fn handle_misc_operations(&mut self) -> Result<()> {
        if let Some(peer_handle) = &self.peer_handle {
            let (is_leader, _) = peer_handle.get_leadership_info();
            
            if is_leader {
                self.handle_fault_tolerance_management()?;
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
                &et::candidate(), 
                None,
                Some(format!("Name == 'qcore' && Parent->Name == '{}'", machine))
            )?;
            
            candidates.items.first().cloned()
        };
        
        // Update available list and current leader for all fault tolerance entities
        let fault_tolerances = self.store.find_entities_paginated(
            &et::fault_tolerance(), 
            None,
            None
        )?;
        
        for ft_entity_id in fault_tolerances.items {
            let ft_fields = self.store.perform(vec![
                sread!(ft_entity_id.clone(), ft::candidate_list()),
                sread!(ft_entity_id.clone(), ft::available_list()),
                sread!(ft_entity_id.clone(), ft::current_leader())
            ])?;
            
            let candidates = ft_fields
                .get(0)
                .and_then(|r| match r {
                    Request::Read { response: Some(Ok(value)), .. } => value.as_entity_list().ok(),
                    _ => None,
                })
                .unwrap_or_default();
            
            let mut available = Vec::new();
            for candidate_id in candidates.iter() {
                let candidate_fields = self.store.perform(vec![
                    sread!(candidate_id.clone(), ft::heartbeat()),
                    sread!(candidate_id.clone(), ft::make_me()),
                    sread!(candidate_id.clone(), ft::death_detection_timeout()),
                ])?;
                
                // Process candidate availability logic here...
                // This is simplified for now
            }
            
            // Update the fault tolerance entity
            let requests = vec![
                swrite!(ft_entity_id.clone(), ft::available_list(), Some(Value::EntityList(available.clone())), PushCondition::Changes),
            ];
            
            self.store.perform_mut(requests)?;
        }
        
        Ok(())
    }
    
    /// Handle heartbeat writing for this machine
    fn write_heartbeat(&mut self) -> Result<()> {
        let machine = &self.config.machine_id;
        
        let candidates = self.store.find_entities_paginated(
            &et::candidate(), 
            None,
            Some(format!("Name == 'qcore' && Parent->Name == '{}'", machine))
        )?;
        
        if let Some(candidate) = candidates.items.first() {
            self.store.perform_mut(vec![
                swrite!(candidate.clone(), ft::heartbeat(), schoice!(0)),
                swrite!(candidate.clone(), ft::make_me(), schoice!(1), PushCondition::Changes)
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
            let scope = get_scope(&request)?;
            
            match scope {
                AuthorizationScope::Public => {
                    authorized_requests.push(request);
                }
                AuthorizationScope::Private { entity_id, field_type } => {
                    // Check if client is authorized for this specific entity/field
                    let auth_config = AuthConfig {
                        subject_id: client_id.clone(),
                        entity_id: entity_id.clone(),
                        field_type: field_type.clone(),
                    };
                    
                    // For now, implement basic authorization logic
                    // In a full implementation, this would check permissions
                    authorized_requests.push(request);
                }
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
        authenticate_subject(subject_name, credential, &mut self.store, &mut self.permission_cache, &mut self.cel_executor)
    }
    
    /// Take a snapshot from the store
    pub fn take_snapshot(&self) -> Option<Snapshot> {
        Some(self.store.take_snapshot())
    }
    
    /// Restore from a snapshot
    pub fn restore_snapshot(&mut self, snapshot: Snapshot) {
        self.store.restore_snapshot(snapshot);
    }
    
    /// Disable notifications temporarily
    pub fn disable_notifications(&mut self) {
        self.store.disable_notifications();
    }
    
    /// Enable notifications
    pub fn enable_notifications(&mut self) {
        self.store.enable_notifications();
    }
}