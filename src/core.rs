use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration as StdDuration;
use std::sync::Arc;
use mio::{Poll, Interest, Token, Events};
use mio::net::{TcpListener as MioTcpListener, TcpStream as MioTcpStream};
use tungstenite::{WebSocket, Message};
use tracing::{info, warn, error, debug};
use anyhow::Result;
use crossbeam::channel::{Sender, Receiver, unbounded};
use crossbeam::queue::SegQueue;
use std::thread;
use qlib_rs::{
    StoreMessage, EntityId, NotificationQueue, NotifyConfig,
    AuthenticationResult, Notification, Store, Cache, CelExecutor,
    PushCondition, Value, Request, Snapshot, Snowflake,
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

/// Fire-and-forget request types (no response needed)
#[derive(Debug)]
pub enum CoreCommand {
    HandleMiscOperations,
    HandleHeartbeat,
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
        peer_handle: crate::peers::PeerHandle,
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
    command_queue: Arc<SegQueue<CoreCommand>>,
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
        peer_handle: crate::peers::PeerHandle,
        snapshot_handle: crate::snapshot::SnapshotHandle,
        wal_handle: crate::wal::WalHandle,
    ) -> Result<()> {
        let (response_tx, response_rx) = unbounded();
        self.request_sender.send((CoreRequest::SetHandles { peer_handle, snapshot_handle, wal_handle }, response_tx))
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
    websocket: WebSocket<MioTcpStream>,
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
    
    // Channel for receiving requests from other services
    request_receiver: Receiver<(CoreRequest, Sender<CoreResponse>)>,
    
    // Queue for fire-and-forget commands
    command_queue: Arc<SegQueue<CoreCommand>>,
}

const LISTENER_TOKEN: Token = Token(0);
const MISC_INTERVAL_MS: u64 = 10;
const HEARTBEAT_INTERVAL_SECS: u64 = 1;

impl CoreService {
    /// Create a new core service
    pub fn new(
        config: CoreConfig, 
        request_receiver: Receiver<(CoreRequest, Sender<CoreResponse>)>,
        command_queue: Arc<SegQueue<CoreCommand>>
    ) -> Result<Self> {
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
            request_receiver,
            command_queue,
        })
    }

    /// Spawn the core service in its own thread and return a handle
    pub fn spawn(config: CoreConfig) -> Result<CoreHandle> {
        let (request_sender, request_receiver) = unbounded();
        let command_queue = Arc::new(SegQueue::new());
        
        let handle = CoreHandle { 
            request_sender: request_sender.clone(),
            command_queue: command_queue.clone(),
        };
        
        // Spawn the main core service thread (I/O event loop)
        let command_queue_for_service = command_queue.clone();
        thread::spawn(move || {
            let mut service = match Self::new(config, request_receiver, command_queue_for_service) {
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
        
        // Spawn background scheduler thread for misc operations
        let misc_handle = handle.clone();
        thread::spawn(move || {
            let mut last_misc_tick = std::time::Instant::now();
            
            loop {
                thread::sleep(StdDuration::from_millis(MISC_INTERVAL_MS));
                
                let now = std::time::Instant::now();
                if now.duration_since(last_misc_tick) >= StdDuration::from_millis(MISC_INTERVAL_MS) {
                    // Send misc operations command to core (fire-and-forget)
                    misc_handle.command_queue.push(CoreCommand::HandleMiscOperations);
                    last_misc_tick = now;
                }
            }
        });
        
        // Spawn background scheduler thread for heartbeat
        let heartbeat_handle = handle.clone();
        thread::spawn(move || {
            let mut last_heartbeat = std::time::Instant::now();
            
            loop {
                thread::sleep(StdDuration::from_secs(HEARTBEAT_INTERVAL_SECS));
                
                let now = std::time::Instant::now();
                if now.duration_since(last_heartbeat) >= StdDuration::from_secs(HEARTBEAT_INTERVAL_SECS) {
                    // Send heartbeat command to core (fire-and-forget)
                    heartbeat_handle.command_queue.push(CoreCommand::HandleHeartbeat);
                    last_heartbeat = now;
                }
            }
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
            
            // Handle fire-and-forget commands from the queue (non-blocking)
            while let Some(command) = self.command_queue.pop() {
                if self.handle_command(command) {
                    info!("Core service shutting down");
                    return Ok(());
                }
            }
            
            // Process notifications and send them to clients
            self.process_notifications()?;
            
            // Process any pending write requests from the store
            self.process_write_requests()?;
            
            // Calculate timeout - use immediate poll if we have pending work
            let timeout = if self.has_pending_work() {
                Some(StdDuration::from_millis(0)) // Poll immediately
            } else {
                Some(StdDuration::from_millis(100)) // Longer timeout, but still responsive
            };
            
            // Poll for OS events - rely on OS notifications
            self.poll.poll(&mut events, timeout)?;
            
            // Handle all mio events
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
        }
    }
    
    /// Check if there's any pending work that requires immediate attention
    fn has_pending_work(&self) -> bool {
        // Check for pending outbound messages
        self.connections.values().any(|conn| !conn.outbound_messages.is_empty()) ||
        // Check for pending requests
        !self.request_receiver.is_empty()
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
            CoreRequest::SetHandles { peer_handle, snapshot_handle, wal_handle } => {
                self.peer_handle = Some(peer_handle);
                self.snapshot_handle = Some(snapshot_handle);
                self.wal_handle = Some(wal_handle);
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
        }
    }
    
    /// Force disconnect all clients
    fn disconnect_all_clients(&mut self) {
        let tokens_to_remove: Vec<Token> = self.connections.keys().cloned().collect();
        for token in tokens_to_remove {
            info!(token = ?token, "Force disconnecting client");
            self.remove_client(token);
        }
    }
    
    fn handle_new_connection(&mut self) -> Result<()> {
        loop {
            match self.listener.accept() {
                Ok((stream, addr)) => {
                    info!(client_addr = %addr, "Accepting new client connection");
                    
                    let token = Token(self.next_token);
                    self.next_token += 1;
                    
                    // Perform WebSocket handshake (this might block, but it should be quick)
                    match tungstenite::accept(stream) {
                        Ok(websocket) => {
                            // Register the underlying stream with mio
                            // Note: We can't easily register the websocket's stream after handshake
                            // This is a limitation of the current approach
                            
                            let connection = ClientConnection {
                                websocket,
                                addr_string: addr.to_string(),
                                authenticated: false,
                                client_id: None,
                                notification_queue: NotificationQueue::new(),
                                notification_configs: HashSet::new(),
                                pending_notifications: VecDeque::new(),
                                outbound_messages: VecDeque::new(),
                            };
                            
                            self.connections.insert(token, connection);
                            debug!(client_addr = %addr, "WebSocket handshake completed");
                        }
                        Err(e) => {
                            error!(client_addr = %addr, error = %e, "WebSocket handshake failed");
                        }
                    }
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
        let mut messages_processed = false;
        
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
                Ok(true) => {
                    messages_processed = true;
                }
            }
        }
        
        // If we processed messages or if writable event occurred, try to write
        if (writable || messages_processed) && !should_remove {
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
                    debug!(
                        client_addr = %conn.addr_string,
                        message_length = response_text.len(),
                        "Queueing response message"
                    );
                    conn.outbound_messages.push_back(response_text);
                }
            } else {
                error!("Failed to serialize response message");
            }
        }
        
        Ok(!should_close)
    }
    
    fn handle_client_write(&mut self, token: Token) -> Result<()> {
        if let Some(connection) = self.connections.get_mut(&token) {
            while let Some(message_text) = connection.outbound_messages.pop_front() {
                match connection.websocket.write(Message::Text(message_text.clone())) {
                    Ok(_) => {
                        debug!(
                            client_addr = %connection.addr_string,
                            "Sent message to client"
                        );
                        // Try to flush the websocket to ensure data is sent
                        if let Err(e) = connection.websocket.flush() {
                            match e {
                                tungstenite::Error::Io(ref io_err) 
                                    if io_err.kind() == std::io::ErrorKind::WouldBlock => {
                                    // Put the message back and try again later
                                    connection.outbound_messages.push_front(message_text);
                                    break;
                                }
                                _ => {
                                    error!(error = %e, "Failed to flush websocket");
                                    return Err(e.into());
                                }
                            }
                        }
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
            // Pop notifications from the queue and add to pending
            while let Some(notification) = connection.notification_queue.pop() {
                connection.pending_notifications.push_back(notification);
            }
            
            // Send pending notifications as messages
            while let Some(notification) = connection.pending_notifications.pop_front() {
                let notification_message = StoreMessage::Notification { notification };
                
                if let Ok(message_text) = serde_json::to_string(&notification_message) {
                    connection.outbound_messages.push_back(message_text);
                } else {
                    error!(
                        client_addr = %connection.addr_string,
                        "Failed to serialize notification message"
                    );
                }
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
            if let Some(peer_handle) = &self.peer_handle {
                peer_handle.send_sync_message(requests_to_write);
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
            self.store.register_notification(config.clone(), connection.notification_queue.clone())?;
            
            // Track this config for the client
            connection.notification_configs.insert(config);
            
            debug!(
                client_addr = %connection.addr_string,
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
            let unregistered = self.store.unregister_notification(&config, &connection.notification_queue);
            
            // Remove from client's config set
            connection.notification_configs.remove(&config);
            
            debug!(
                client_addr = %connection.addr_string,
                unregistered = unregistered,
                "Unregistered notification configuration for client"
            );
            
            unregistered
        } else {
            false
        }
    }
}