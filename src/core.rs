use std::net::SocketAddr;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration as StdDuration;
use mio::{Poll, Interest, Token, Events};
use mio::net::{TcpListener as MioTcpListener, TcpStream as MioTcpStream};
use tungstenite::{WebSocket, Message};
use tracing::{info, warn, error, debug};
use anyhow::Result;
use qlib_rs::{
    StoreMessage, EntityId, NotificationQueue, NotifyConfig,
    AuthenticationResult, Notification, Store, Cache, CelExecutor,
    PushCondition, Value, Request, Snapshot, Snowflake, StoreTrait,
    AuthConfig, EntityType, FieldType, PageOpts, PageResult, 
    auth::{authenticate_subject, get_scope, AuthorizationScope}
};
use crate::peers::AvailabilityState;

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
        let snowflake = Snowflake::new(); // TODO: configure these properly
        let mut store = Store::new(snowflake);
        
        let (permission_cache, _notification_queue) = Cache::new(
            &mut store,
            qlib_rs::et::permission(),
            vec![qlib_rs::ft::resource_type(), qlib_rs::ft::resource_field()],
            vec![qlib_rs::ft::scope(), qlib_rs::ft::condition()]
        ).map_err(|e| anyhow::anyhow!("Failed to create permission cache: {}", e))?;
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
                        websocket: WebSocket::from_partially_read(mio_stream, Vec::new(), tungstenite::protocol::Role::Server, None),
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
        let mut messages_to_process = Vec::new();
        let mut should_close = false;
        
        // First pass: collect messages
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
                                messages_to_process.push(store_msg);
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
                    Ok(Message::Ping(_payload)) => {
                        // WebSocket ping/pong is handled automatically by tungstenite
                        // We don't need to manually respond to pings
                        debug!("Received WebSocket ping");
                    }
                    Ok(Message::Close(_)) => {
                        should_close = true;
                        break;
                    }
                    Err(tungstenite::Error::Io(ref e)) 
                        if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(e) => {
                        error!(
                            client_addr = %connection.addr_string,
                            error = %e,
                            "WebSocket read error"
                        );
                        should_close = true;
                        break;
                    }
                    _ => {} // Handle other message types
                }
            }
        }
        
        // Second pass: process messages
        for store_msg in messages_to_process {
            let response = self.process_store_message(store_msg, token)?;
            if let Ok(response_text) = serde_json::to_string(&response) {
                if let Some(conn) = self.connections.get_mut(&token) {
                    conn.outbound_messages.push_back(response_text);
                }
            }
        }
        
        Ok(!should_close)
    }
    
    fn handle_client_write(&mut self, token: Token) -> Result<()> {
        if let Some(connection) = self.connections.get_mut(&token) {
            while let Some(message_text) = connection.outbound_messages.pop_front() {
                match connection.websocket.write(Message::Text(message_text.clone())) {
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
    
    /// Set field schema
    pub fn set_field_schema(&mut self, entity_type: &EntityType, field_type: &FieldType, schema: qlib_rs::FieldSchema) -> Result<()> {
        self.store.set_field_schema(entity_type, field_type, schema)
            .map_err(|e| anyhow::anyhow!("Failed to set field schema: {}", e))
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
    
    /// Perform map operation
    pub fn perform_map(&mut self, requests: Vec<Request>) -> Result<HashMap<FieldType, Request>> {
        self.store.perform_map(requests)
            .map_err(|e| anyhow::anyhow!("Failed to perform map: {}", e))
    }
    
    /// Register notification configuration
    pub fn register_notification(&mut self, _client_id: EntityId, _config: NotifyConfig) -> Result<()> {
        // Add notification logic here when implemented
        Ok(())
    }
    
    /// Unregister notification configuration
    pub fn unregister_notification(&mut self, _client_id: EntityId, _config: NotifyConfig) -> bool {
        // Add notification logic here when implemented
        false
    }
}