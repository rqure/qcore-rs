use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{Read, Write};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use ahash::AHashMap;
use crossbeam::channel::Sender;
use mio::{Poll, Interest, Token, Events, event::Event};
use mio::net::{TcpListener as MioTcpListener, TcpStream as MioTcpStream};
use qlib_rs::{et, schoice, sfield, sreq, sread, swrite, Requests, PushCondition};
use rustc_hash::FxHashMap;
use tracing::{info, warn, error, debug};
use anyhow::Result;
use std::thread;
use qlib_rs::{
    StoreMessage, EntityId, NotificationQueue, NotifyConfig,
    AuthenticationResult, Store, Cache, CelExecutor,
    Request, Snapshot, AuthConfig, StoreTrait,
    auth::{authenticate_subject, get_scope, AuthorizationScope}
};
use qlib_rs::protocol::{ProtocolMessage, ProtocolCodec, MessageBuffer};

use crate::snapshot::SnapshotHandle;
use crate::wal::WalHandle;

/// Configuration for the core service
#[derive(Debug, Clone)]
pub struct CoreConfig {
    /// Port for unified client and peer communication
    pub port: u16,
    /// Machine ID for request origination
    pub machine: String,
    /// Peer mapping (machine_id -> address)
    pub peers: HashMap<String, String>,
}

impl From<&crate::Config> for CoreConfig {
    fn from(config: &crate::Config) -> Self {
        let peers = config.peer_addresses
            .as_ref()
            .map(|p| p.peers.clone())
            .unwrap_or_default();
            
        Self {
            port: config.port,
            machine: config.machine.clone(),
            peers,
        }
    }
}

/// Core service request types
#[derive(Debug)]
pub enum CoreCommand {
    Perform {
        requests: Requests,
    },
    TakeSnapshot,
    RestoreSnapshot {
        snapshot: Snapshot,
    },
    SetSnapshotHandle {
        snapshot_handle: SnapshotHandle,
    },
    SetWalHandle {
        wal_handle: crate::wal::WalHandle,
    },
    PeerConnected {
        machine_id: String,
        stream: MioTcpStream,
    },
    GetPeers {
        respond_to: Sender<AHashMap<String, (Option<Token>, Option<EntityId>)>>,
    },
}

/// Handle for communicating with core service
#[derive(Debug, Clone)]
pub struct CoreHandle {
    sender: Sender<CoreCommand>,
}

impl CoreHandle {
    pub fn perform(&self, requests: Requests) {
        self.sender.send(CoreCommand::Perform { requests }).unwrap();
    }

    pub fn take_snapshot(&self) {
        self.sender.send(CoreCommand::TakeSnapshot).unwrap();
    }

    pub fn restore_snapshot(&self, snapshot: Snapshot) {
        self.sender.send(CoreCommand::RestoreSnapshot { snapshot }).unwrap();
    }

    pub fn set_snapshot_handle(&self, snapshot_handle: SnapshotHandle) {
        self.sender.send(CoreCommand::SetSnapshotHandle { snapshot_handle }).unwrap();
    }

    pub fn set_wal_handle(&self, wal_handle: crate::wal::WalHandle) {
        self.sender.send(CoreCommand::SetWalHandle { wal_handle }).unwrap();
    }

    pub fn peer_connected(&self, machine_id: String, stream: MioTcpStream) {
        self.sender.send(CoreCommand::PeerConnected { machine_id, stream }).unwrap();
    }

    pub fn get_peers(&self) -> AHashMap<String, (Option<Token>, Option<EntityId>)> {
        let (resp_sender, resp_receiver) = crossbeam::channel::bounded(1);
        self.sender.send(CoreCommand::GetPeers { respond_to: resp_sender }).unwrap();
        resp_receiver.recv().unwrap_or_default()
    }
}

/// Peer connection information
#[derive(Debug, Clone)]
struct PeerInfo {
    token: Option<Token>,
    entity_id: Option<EntityId>,
    start_time: Option<u64>,
}

impl PeerInfo {
    fn new() -> Self {
        Self {
            token: None,
            entity_id: None,
            start_time: None,
        }
    }
}

/// Client connection information  
#[derive(Debug)]
struct Connection {
    stream: MioTcpStream,
    addr_string: String,
    authenticated: bool,
    client_id: Option<EntityId>,
    notification_queue: NotificationQueue,
    notification_configs: HashSet<NotifyConfig>,
    outbound_messages: VecDeque<Vec<u8>>,
    message_buffer: MessageBuffer,
}

/// Core service that handles both client and peer connections
pub struct CoreService {
    config: CoreConfig,
    listener: MioTcpListener,
    poll: Poll,
    connections: FxHashMap<Token, Connection>,
    peers: AHashMap<String, PeerInfo>,
    next_token: usize,
    start_time: u64, // Unix timestamp when this service started
    last_heartbeat_tick: Instant,
    last_fault_tolerance_tick: Instant,
    is_leader: bool, // Whether this service is currently the leader
    
    // Store and related components (replacing StoreService)
    store: Store,
    permission_cache: Option<Cache>,
    cel_executor: CelExecutor,
    
    // Handles to other services  
    snapshot_handle: Option<SnapshotHandle>,
    wal_handle: Option<WalHandle>,
}

const LISTENER_TOKEN: Token = Token(0);

impl CoreService {
    /// Attempt to create a permission cache with the current store state
    fn create_permission_cache(&mut self) -> Option<Cache> {
        // Get values needed for cache creation
        let permission_entity_type = match self.store.get_entity_type(qlib_rs::et::PERMISSION) {
            Ok(et) => et,
            Err(e) => {
                debug!("Failed to get permission entity type: {}", e);
                return None;
            }
        };
        
        let resource_type_field = match self.store.get_field_type(qlib_rs::ft::RESOURCE_TYPE) {
            Ok(ft) => ft,
            Err(e) => {
                debug!("Failed to get resource type field: {}", e);
                return None;
            }
        };
        
        let resource_field_field = match self.store.get_field_type(qlib_rs::ft::RESOURCE_FIELD) {
            Ok(ft) => ft,
            Err(e) => {
                debug!("Failed to get resource field field: {}", e);
                return None;
            }
        };
        
        let scope_field = match self.store.get_field_type(qlib_rs::ft::SCOPE) {
            Ok(ft) => ft,
            Err(e) => {
                debug!("Failed to get scope field: {}", e);
                return None;
            }
        };
        
        let condition_field = match self.store.get_field_type(qlib_rs::ft::CONDITION) {
            Ok(ft) => ft,
            Err(e) => {
                debug!("Failed to get condition field: {}", e);
                return None;
            }
        };
        
        match Cache::new(
            &mut self.store,
            permission_entity_type,
            vec![resource_type_field, resource_field_field],
            vec![scope_field, condition_field]
        ) {
            Ok((cache, _notification_queue)) => {
                debug!("Successfully created permission cache");
                Some(cache)
            }
            Err(e) => {
                debug!("Failed to create permission cache: {}", e);
                None
            }
        }
    }

    /// Create a new core service with peer configuration
    pub fn new(
        config: CoreConfig,
    ) -> Result<Self> {
        let addr = format!("0.0.0.0:{}", config.port).parse()?;
        let mut listener = MioTcpListener::bind(addr)?;
        let poll = Poll::new()?;
        
        poll.registry().register(&mut listener, LISTENER_TOKEN, Interest::READABLE)?;
        
        info!(bind_address = %addr, "Core service unified TCP server initialized");
        
        let store = Store::new();
        let cel_executor = CelExecutor::new();
        
        // Capture start time as Unix timestamp
        let start_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        let mut service = Self {
            config,
            listener,
            poll,
            connections: FxHashMap::default(),
            peers: AHashMap::default(),
            next_token: 1,
            start_time,
            last_heartbeat_tick: Instant::now(),
            last_fault_tolerance_tick: Instant::now(),
            is_leader: true, // Start as leader until we learn about older peers
            store,
            permission_cache: None,
            cel_executor,
            snapshot_handle: None,
            wal_handle: None,
        };
        
        // Attempt to create permission cache
        service.permission_cache = service.create_permission_cache();

        // Create initial peer entries
        for (machine_id, _address) in &service.config.peers {
            service.peers.insert(machine_id.clone(), PeerInfo::new());
        }
        
        Ok(service)
    }

    /// Spawn the core service in its own thread and return a handle
    pub fn spawn(config: CoreConfig) -> CoreHandle {
        let (sender, receiver) = crossbeam::channel::unbounded();
        let handle = CoreHandle { sender };

        // Start peer connection thread
        let peer_handle = handle.clone();
        let peer_config = config.clone();
        thread::spawn(move || {
            Self::peer_connection_thread(peer_handle, peer_config);
        });

        thread::spawn(move || {
            let mut service = match Self::new(config) {
                Ok(s) => s,
                Err(e) => {
                    error!("Failed to create core service: {}", e);
                    return;
                }
            };
            
            let mut events = Events::with_capacity(1024);
            
            loop {                
                // Poll for I/O events with no timeout for maximum responsiveness
                // Periodic operations are now handled by the self-connecting periodic client
                service.poll.poll(&mut events, Some(Duration::from_millis(100))).unwrap();
                
                // Handle all mio events
                for event in events.iter() {
                    match event.token() {
                        LISTENER_TOKEN => {
                            if event.is_readable() {
                                service.accept_new_connections();
                            }
                        }
                        token => {
                            service.handle_connection_event(token, event);
                        }
                    }
                }

                // Run periodic maintenance tasks before processing commands
                let now = Instant::now();
                if now.duration_since(service.last_heartbeat_tick) >= Duration::from_secs(1) {
                    service.last_heartbeat_tick = now;
                    service.write_heartbeat();
                }

                if now.duration_since(service.last_fault_tolerance_tick) >= Duration::from_millis(100) {
                    service.last_fault_tolerance_tick = now;
                    service.manage_fault_tolerance();
                }

                while let Ok(request) = receiver.try_recv() {
                    service.handle_command(request);
                }

                if let Some(cache) = &mut service.permission_cache {
                    cache.process_notifications();
                }

                // Process pending notifications for all connections
                service.process_notifications();

                // Drain the write queue from the store
                while let Some(requests) = service.store.write_queue.pop_front() {
                    if let Some(wal_handle) = &service.wal_handle {
                        wal_handle.append_requests(requests.clone());
                    }
                }
            }
        });

        handle
    }
    
    /// Peer connection thread that connects to peers with higher machine IDs
    fn peer_connection_thread(handle: CoreHandle, config: CoreConfig) {
        info!("Starting peer connection thread for machine {}", config.machine);
        
        loop {
            // Find peers with higher machine IDs that we should connect to
            let all_peers = handle.get_peers();
            let mut target_peers = Vec::new();
            for (machine_id, (token_opt, _entity_id)) in &all_peers {
                if machine_id > &config.machine {
                    if token_opt.is_none() {
                        if let Some(address) = config.peers.get(machine_id) {
                            target_peers.push((machine_id.clone(), address.clone()));
                        } else {
                            debug!("No address found for peer {}, skipping connection attempt", machine_id);
                        }
                    } else {
                        debug!("Already connected to peer {}, skipping connection attempt", machine_id);
                    }
                }
            }

            info!("Will attempt to connect to {} peers: {:?}", target_peers.len(), target_peers);

            for (machine_id, address) in &target_peers {
                match std::net::TcpStream::connect(address) {
                    Ok(std_stream) => {
                        info!("Successfully connected to peer {} at {}", machine_id, address);
                        
                        // Set non-blocking and convert to mio stream
                        if let Err(e) = std_stream.set_nonblocking(true) {
                            error!("Failed to set peer connection to non-blocking: {}", e);
                            continue;
                        }
                        
                        let mio_stream = MioTcpStream::from_std(std_stream);
                        
                        // Send the connected peer to the main service
                        handle.peer_connected(machine_id.clone(), mio_stream)
                    }
                    Err(e) => {
                        debug!("Failed to connect to peer {} at {}: {}", machine_id, address, e);
                    }
                }
            }
            
            // Wait before retrying connections
            thread::sleep(Duration::from_secs(3));
        }
    }
    
    
    /// Check authorization for a list of requests and return only authorized ones
    fn check_requests_authorization(
        &mut self,
        client_id: EntityId,
        requests: Requests,
    ) -> Result<Requests> {
        for request in requests.read().iter() {
            // Extract entity_id and field_type from the request
            let authorization_needed = match &request {
                Request::Read { entity_id, field_types, .. } => Some((entity_id, field_types)),
                Request::Write { entity_id, field_types, .. } => Some((entity_id, field_types)),
                Request::Create { .. } => None, // No field-level authorization for creation
                Request::Delete { .. } => None, // No field-level authorization for deletion
                Request::SchemaUpdate { .. } => None, // No field-level authorization for schema updates
                _ => None, // For other request types, skip authorization check
            };
            
            if let Some((entity_id, field_types)) = authorization_needed {
                // If we don't have a permission cache, skip authorization
                let cache = match &self.permission_cache {
                    Some(cache) => cache,
                    None => {
                        debug!("No permission cache available, skipping authorization check");
                        continue;
                    }
                };
                
                // For authorization, we check against the final field in the indirection chain
                let (final_entity_id, final_field_type) = self.store.resolve_indirection(*entity_id, field_types)?;
                
                let scope = get_scope(&self.store, &mut self.cel_executor, cache, client_id, final_entity_id, final_field_type)?;
                
                match scope {
                    AuthorizationScope::None => {
                        return Err(anyhow::anyhow!("Client not authorized for request: {}", request));
                    }
                    AuthorizationScope::ReadOnly => {
                        if let Request::Write { .. } = request {
                            return Err(anyhow::anyhow!("Client not authorized for write request: {}", request));
                        } else if let Request::Create { .. } = request {
                            return Err(anyhow::anyhow!("Client not authorized for create request: {}", request));
                        } else if let Request::Delete { .. } = request {
                            return Err(anyhow::anyhow!("Client not authorized for delete request: {}", request));
                        } else if let Request::SchemaUpdate { .. } = request {
                            return Err(anyhow::anyhow!("Client not authorized for schema update request: {}", request));
                        }
                    }
                    _ => {}
                }
            }
        }
        
        Ok(requests)
    }
    
    /// Accept new incoming connections
    fn accept_new_connections(&mut self) {
        loop {
            match self.listener.accept() {
                Ok((mut stream, addr)) => {
                    info!("Accepted new connection from {}", addr);
                    
                    let token = Token(self.next_token);
                    self.next_token += 1;
                    
                    // Register for read events
                    if let Err(e) = self.poll.registry().register(
                        &mut stream,
                        token,
                        Interest::READABLE
                    ) {
                        error!("Failed to register connection: {}", e);
                        continue;
                    }
                    
                    let connection = Connection {
                        stream,
                        addr_string: addr.to_string(),
                        authenticated: false,
                        client_id: None,
                        notification_queue: NotificationQueue::new(),
                        notification_configs: HashSet::new(),
                        outbound_messages: VecDeque::new(),
                        message_buffer: MessageBuffer::new(),
                    };
                    
                    self.connections.insert(token, connection);
                    debug!("Connection {} registered with token {:?}", addr, token);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // No more connections to accept
                    break;
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                    break;
                }
            }
        }
    }
    
    /// Handle events for existing connections
    fn handle_connection_event(&mut self, token: Token, event: &Event) {
        let should_remove = {
            let connection_exists = self.connections.contains_key(&token);
            if !connection_exists {
                warn!("Received event for unknown token {:?}", token);
                return;
            }
            
            let mut should_remove = false;
            
            if event.is_readable() {
                if let Err(e) = self.handle_connection_read(token) {
                    if let Some(connection) = self.connections.get(&token) {
                        error!("Error reading from connection {}: {}", connection.addr_string, e);
                    }
                    should_remove = true;
                }
            }
            
            if event.is_writable() {
                if let Err(e) = self.handle_connection_write(token) {
                    if let Some(connection) = self.connections.get(&token) {
                        error!("Error writing to connection {}: {}", connection.addr_string, e);
                    }
                    should_remove = true;
                }
            }
            
            if event.is_error() {
                if let Some(connection) = self.connections.get(&token) {
                    warn!("Connection error for {}", connection.addr_string);
                }
                should_remove = true;
            }
            
            should_remove
        };
        
        if should_remove {
            self.remove_connection(token);
        }
    }
    
    /// Handle reading data from a connection
    fn handle_connection_read(&mut self, token: Token) -> Result<()> {
        let mut buffer = [0u8; 8192];
        let mut messages_to_process = Vec::new();
        
        // First, read data and decode messages
        {
            let connection = self.connections.get_mut(&token)
                .ok_or_else(|| anyhow::anyhow!("Connection not found"))?;
            
            loop {
                match connection.stream.read(&mut buffer) {
                    Ok(0) => {
                        // Connection closed by peer
                        return Err(anyhow::anyhow!("Connection closed by peer"));
                    }
                    Ok(n) => {
                        connection.message_buffer.add_data(&buffer[0..n]);
                        
                        // Try to decode messages
                        while let Some(message) = connection.message_buffer.try_decode()? {
                            messages_to_process.push(message);
                        }
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        // No more data available right now
                        break;
                    }
                    Err(e) => {
                        return Err(anyhow::anyhow!("Read error: {}", e));
                    }
                }
            }
        }
        
        // Now process all decoded messages
        for message in messages_to_process {
            self.handle_protocol_message(token, message)?;
        }
        
        Ok(())
    }
    
    /// Handle writing data to a connection
    fn handle_connection_write(&mut self, token: Token) -> Result<()> {
        let connection = self.connections.get_mut(&token)
            .ok_or_else(|| anyhow::anyhow!("Connection not found"))?;
            
        while let Some(message_data) = connection.outbound_messages.pop_front() {
            match connection.stream.write(&message_data) {
                Ok(n) if n == message_data.len() => {
                    // Full message written
                    continue;
                }
                Ok(n) => {
                    // Partial write - put remaining data back at front of queue
                    connection.outbound_messages.push_front(message_data[n..].to_vec());
                    break;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // Can't write more right now - put message back
                    connection.outbound_messages.push_front(message_data);
                    break;
                }
                Err(e) => {
                    return Err(anyhow::anyhow!("Write error: {}", e));
                }
            }
        }
        
        // If no more messages to write, stop watching for write events
        if connection.outbound_messages.is_empty() {
            self.poll.registry().reregister(
                &mut connection.stream,
                token,
                Interest::READABLE
            )?;
        }
        
        Ok(())
    }
    
    /// Handle complete snapshot restoration including store, cache, and peer reinitialization
    fn handle_snapshot_restoration(&mut self, snapshot: Snapshot) {
        // Restore the snapshot to our store
        self.store.restore_snapshot(snapshot);
        
        // Recreate permission cache after restoration
        self.permission_cache = self.create_permission_cache();
        
        if self.permission_cache.is_some() {
            debug!("Permission cache recreated after restore");
        } else {
            warn!("Failed to recreate permission cache after restore");
        }

        // Clear peer start times since we have new data
        for (_machine_id, peer_info) in self.peers.iter_mut() {
            peer_info.start_time = None;
        }

        // Clear machine ids in peers to force re-resolution
        for (_machine_id, peer_info) in self.peers.iter_mut() {
            peer_info.entity_id = None;
            if let Some(token) = peer_info.token {
                self.connections.get_mut(&token).map(|conn| {
                    conn.client_id = None;
                    conn.authenticated = false;
                });
            }
        }

        // Update peer entity ids based on restored data
        if let Ok(etype) = self.store.get_entity_type(et::MACHINE) {
            for (machine_id, peer_info) in self.peers.iter_mut() {
                if let Some(machines) = self.store.find_entities(etype, Some(&format!("Name == '{}'", machine_id))).ok() {
                    if let Some(machine) = machines.first() {
                        peer_info.entity_id = Some(*machine);
                        if let Some(token) = peer_info.token {
                            if let Some(connection) = self.connections.get_mut(&token) {
                                connection.client_id = Some(*machine);
                                connection.authenticated = true;
                            }
                        }
                        debug!("Updated entity ID for peer {} after restore: {:?}", machine_id, machine);
                    } else {
                        warn!("No entity found for peer {} after restore", machine_id);
                    }
                } else {
                    warn!("Failed to query entity for peer {} after restore", machine_id);
                }
            }
        }

        // Re-evaluate leadership after restoration
        self.evaluate_leadership();
    }

    /// Handle peer handshake message
    fn handle_peer_handshake(&mut self, token: Token, peer_start_time: u64) -> Result<()> {
        // Find the machine ID for this peer connection
        let peer_machine_id = self.peers.iter()
            .find(|(_, peer_info)| peer_info.token.map_or(false, |t| t == token))
            .map(|(machine_id, _)| machine_id.clone())
            .unwrap_or_else(|| "unknown".to_string());
            
        info!("Received handshake from peer {} with start time {}", peer_machine_id, peer_start_time);
        
        // Store this peer's start time
        if let Some(peer_info) = self.peers.get_mut(&peer_machine_id) {
            peer_info.start_time = Some(peer_start_time);
        }
        
        // Send our handshake back with our start time
        let handshake_response = ProtocolMessage::PeerHandshake {
            start_time: self.start_time,
        };
        
        if let Err(e) = self.send_protocol_message(token, handshake_response) {
            error!("Failed to send handshake response to peer {}: {}", peer_machine_id, e);
        } else {
            info!("Sent handshake response to peer {} with our start time {}", peer_machine_id, self.start_time);
        }
        
        // Check if we should sync based on all known peers
        self.evaluate_sync_needs();
        
        Ok(())
    }
    
    /// Evaluate leadership status based on start times
    fn evaluate_leadership(&mut self) {
        // Find the peer with the earliest start time (longest running)
        let mut oldest_start_time = self.start_time;
        let mut has_older_peer = false;
        
        for (_machine_id, peer_info) in &self.peers {
            if let Some(start_time) = peer_info.start_time {
                if start_time < oldest_start_time {
                    oldest_start_time = start_time;
                    has_older_peer = true;
                }
            }
        }
        
        let was_leader = self.is_leader;
        self.is_leader = !has_older_peer;
        
        if was_leader != self.is_leader {
            if self.is_leader {
                info!("This service is now the leader (started at {})", self.start_time);
            } else {
                info!("This service is no longer the leader (older peer found with start time {})", oldest_start_time);
            }
        }
    }
    
    /// Evaluate if we need to sync and from which peer
    fn evaluate_sync_needs(&mut self) {
        // Find the peer with the earliest start time (longest running)
        let mut oldest_peer: Option<(String, u64)> = None;
        let mut oldest_start_time = self.start_time;
        
        for (machine_id, peer_info) in &self.peers {
            if let Some(start_time) = peer_info.start_time {
                if start_time < oldest_start_time {
                    oldest_start_time = start_time;
                    oldest_peer = Some((machine_id.clone(), start_time));
                }
            }
        }
        
        // If we found an older peer, sync from them
        if let Some((oldest_machine_id, oldest_time)) = oldest_peer {
            info!("Found older peer {} (started at {}), requesting full sync", oldest_machine_id, oldest_time);
            
            // Find the token for this peer
            if let Some(peer_info) = self.peers.get(&oldest_machine_id) {
                if let Some(peer_token) = peer_info.token {
                    let sync_request = ProtocolMessage::PeerFullSyncRequest;
                    
                    if let Err(e) = self.send_protocol_message(peer_token, sync_request) {
                        error!("Failed to send full sync request to older peer {}: {}", oldest_machine_id, e);
                    } else {
                        info!("Requested full sync from older peer {}", oldest_machine_id);
                    }
                } else {
                    warn!("Could not find connection token for older peer {}", oldest_machine_id);
                }
            } else {
                warn!("Could not find connection token for older peer {}", oldest_machine_id);
            }
        } else {
            // We are the oldest peer, no need to sync
            debug!("We are the oldest peer (started at {}), no sync needed", self.start_time);
        }
        
        // Always evaluate leadership after checking sync needs
        self.evaluate_leadership();
    }
    
    /// Handle peer full sync request
    fn handle_peer_full_sync_request(&mut self, token: Token) -> Result<()> {
        // Find the machine ID for this peer connection
        let requesting_machine_id = self.peers.iter()
            .find(|(_, peer_info)| peer_info.token.map_or(false, |t| t == token))
            .map(|(machine_id, _)| machine_id.clone())
            .unwrap_or_else(|| "unknown".to_string());
            
        info!("Received full sync request from peer {}", requesting_machine_id);
        
        // Take a snapshot of our current store
        let snapshot = self.store.take_snapshot();
        
        let sync_response = ProtocolMessage::PeerFullSyncResponse {
            snapshot,
        };
        
        if let Err(e) = self.send_protocol_message(token, sync_response) {
            error!("Failed to send full sync response to peer {}: {}", requesting_machine_id, e);
        } else {
            info!("Sent full sync response to peer {}", requesting_machine_id);
        }
        
        Ok(())
    }
    
    /// Handle peer full sync response
    fn handle_peer_full_sync_response(&mut self, _token: Token, snapshot: Snapshot) -> Result<()> {
        info!("Received full sync response, restoring snapshot");
        
        // Handle complete snapshot restoration
        self.handle_snapshot_restoration(snapshot);
        
        info!("Full sync completed successfully");
        
        Ok(())
    }
    
    /// Handle a decoded protocol message
    fn handle_protocol_message(&mut self, token: Token, message: ProtocolMessage) -> Result<()> {
        match message {
            ProtocolMessage::Store(store_message) => {
                self.handle_store_message(token, store_message)
            }
            ProtocolMessage::PeerHandshake { start_time } => {
                self.handle_peer_handshake(token, start_time)
            }
            ProtocolMessage::PeerFullSyncRequest => {
                self.handle_peer_full_sync_request(token)
            }
            ProtocolMessage::PeerFullSyncResponse { snapshot } => {
                self.handle_peer_full_sync_response(token, snapshot)
            }
            _ => {
                warn!("Received unsupported protocol message from client");
                Ok(())
            }
        }
    }
    
    /// Handle a store message from a client
    fn handle_store_message(&mut self, token: Token, message: StoreMessage) -> Result<()> {
        let response = match &message {
            StoreMessage::Authenticate { id, subject_name, credential } => {
                self.handle_authentication(token, *id, subject_name.clone(), credential.clone())?
            }
            _ => {
                // All other messages require authentication
                let is_authenticated = self.connections.get(&token)
                    .map(|conn| conn.authenticated)
                    .unwrap_or(false);
                    
                if !is_authenticated {
                    StoreMessage::Error {
                        id: self.extract_message_id(&message).unwrap_or(0),
                        error: "Authentication required".to_string(),
                    }
                } else {
                    self.handle_authenticated_message(token, message)?
                }
            }
        };
        
        self.send_response(token, response)?;
        Ok(())
    }
    
    /// Handle authentication message
    fn handle_authentication(&mut self, token: Token, id: u64, subject_name: String, credential: String) -> Result<StoreMessage> {
        let auth_config = AuthConfig::default();
        
        match authenticate_subject(&mut self.store, &subject_name, &credential, &auth_config) {
            Ok(subject_id) => {
                // Mark connection as authenticated
                if let Some(connection) = self.connections.get_mut(&token) {
                    connection.authenticated = true;
                    connection.client_id = Some(subject_id.clone());
                    info!("Client {} authenticated as {:?}", connection.addr_string, subject_id);
                }
                
                // Check if this authenticated client is a peer and update the peers map
                let mut peer_handshake_info: Option<(Token, String)> = None;
                for (machine_id, peer_info) in self.peers.iter_mut() {
                    if let Some(peer_entity_id) = peer_info.entity_id {
                        if peer_entity_id == subject_id {
                            info!("Authenticated client is known peer {}, updating token", machine_id);
                            peer_info.token = Some(token);
                            debug!("Updated peer {} with token {:?}", machine_id, token);
                            peer_handshake_info = Some((token, machine_id.clone()));
                            break;
                        }
                    }
                }
                
                // Send handshake message if this was a peer
                if let Some((peer_token, peer_machine_id)) = peer_handshake_info {
                    let handshake_message = ProtocolMessage::PeerHandshake {
                        start_time: self.start_time,
                    };
                    
                    if let Err(e) = self.send_protocol_message(peer_token, handshake_message) {
                        error!("Failed to send handshake to peer {}: {}", peer_machine_id, e);
                    } else {
                        info!("Sent handshake to peer {} with start time {}", peer_machine_id, self.start_time);
                    }
                }
                
                let auth_result = AuthenticationResult {
                    subject_id: subject_id.clone(),
                };
                
                Ok(StoreMessage::AuthenticateResponse {
                    id,
                    response: Ok(auth_result),
                })
            }
            Err(e) => {
                warn!("Authentication failed for {}: {}", subject_name, e);
                Ok(StoreMessage::AuthenticateResponse {
                    id,
                    response: Err(format!("Authentication failed: {}", e)),
                })
            }
        }
    }
    
    /// Handle messages from authenticated clients
    fn handle_authenticated_message(&mut self, token: Token, message: StoreMessage) -> Result<StoreMessage> {
        let connection = self.connections.get(&token)
            .ok_or_else(|| anyhow::anyhow!("Connection not found"))?;
        
        let client_id = connection.client_id.clone()
            .ok_or_else(|| anyhow::anyhow!("Client ID not found"))?;
        
        match message {
            StoreMessage::GetEntitySchema { id, entity_type } => {
                match self.store.get_entity_schema(entity_type) {
                    Ok(schema) => Ok(StoreMessage::GetEntitySchemaResponse {
                        id,
                        response: Ok(Some(schema)),
                    }),
                    Err(e) => Ok(StoreMessage::GetEntitySchemaResponse {
                        id,
                        response: Err(format!("{:?}", e)),
                    }),
                }
            }
            
            StoreMessage::GetCompleteEntitySchema { id, entity_type } => {
                match self.store.get_complete_entity_schema(entity_type) {
                    Ok(schema) => Ok(StoreMessage::GetCompleteEntitySchemaResponse {
                        id,
                        response: Ok(schema.clone()),
                    }),
                    Err(e) => Ok(StoreMessage::GetCompleteEntitySchemaResponse {
                        id,
                        response: Err(format!("{:?}", e)),
                    }),
                }
            }
            
            StoreMessage::GetFieldSchema { id, entity_type, field_type } => {
                match self.store.get_field_schema(entity_type, field_type) {
                    Ok(schema) => Ok(StoreMessage::GetFieldSchemaResponse {
                        id,
                        response: Ok(Some(schema)),
                    }),
                    Err(e) => Ok(StoreMessage::GetFieldSchemaResponse {
                        id,
                        response: Err(format!("{:?}", e)),
                    }),
                }
            }
            
            StoreMessage::EntityExists { id, entity_id } => {
                let exists = self.store.entity_exists(entity_id);
                Ok(StoreMessage::EntityExistsResponse {
                    id,
                    response: exists,
                })
            }
            
            StoreMessage::FieldExists { id, entity_type, field_type } => {
                let exists = self.store.field_exists(entity_type, field_type);
                Ok(StoreMessage::FieldExistsResponse {
                    id,
                    response: exists,
                })
            }
            
            StoreMessage::Perform { id, requests } => {
                match self.check_requests_authorization(client_id, requests) {
                    Ok(authorized_requests) => {
                        match self.store.perform_mut(authorized_requests) {
                            Ok(results) => Ok(StoreMessage::PerformResponse {
                                id,
                                response: Ok(results),
                            }),
                            Err(e) => Ok(StoreMessage::PerformResponse {
                                id,
                                response: Err(format!("{:?}", e)),
                            }),
                        }
                    }
                    Err(e) => Ok(StoreMessage::PerformResponse {
                        id,
                        response: Err(format!("Authorization failed: {}", e)),
                    }),
                }
            }
            
            StoreMessage::FindEntities { id, entity_type, page_opts, filter } => {
                match self.store.find_entities_paginated(entity_type, page_opts, filter) {
                    Ok(result) => Ok(StoreMessage::FindEntitiesResponse {
                        id,
                        response: Ok(result),
                    }),
                    Err(e) => Ok(StoreMessage::FindEntitiesResponse {
                        id,
                        response: Err(format!("{:?}", e)),
                    }),
                }
            }
            
            StoreMessage::FindEntitiesExact { id, entity_type, page_opts, filter } => {
                match self.store.find_entities_exact(entity_type, page_opts, filter) {
                    Ok(result) => Ok(StoreMessage::FindEntitiesExactResponse {
                        id,
                        response: Ok(result),
                    }),
                    Err(e) => Ok(StoreMessage::FindEntitiesExactResponse {
                        id,
                        response: Err(format!("{:?}", e)),
                    }),
                }
            }
            
            StoreMessage::GetEntityTypes { id, page_opts } => {
                match self.store.get_entity_types_paginated(page_opts) {
                    Ok(result) => Ok(StoreMessage::GetEntityTypesResponse {
                        id,
                        response: Ok(result),
                    }),
                    Err(e) => Ok(StoreMessage::GetEntityTypesResponse {
                        id,
                        response: Err(format!("{:?}", e)),
                    }),
                }
            }
            
            StoreMessage::RegisterNotification { id, config } => {
                if let Some(connection) = self.connections.get_mut(&token) {
                    connection.notification_configs.insert(config.clone());
                    
                    match self.store.register_notification(config.clone(), connection.notification_queue.clone()) {
                        Ok(_) => Ok(StoreMessage::RegisterNotificationResponse {
                            id,
                            response: Ok(()),
                        }),
                        Err(e) => Ok(StoreMessage::RegisterNotificationResponse {
                            id,
                            response: Err(format!("{:?}", e)),
                        }),
                    }
                } else {
                    Ok(StoreMessage::RegisterNotificationResponse {
                        id,
                        response: Err("Connection not found".to_string()),
                    })
                }
            }
            
            StoreMessage::UnregisterNotification { id, config } => {
                if let Some(connection) = self.connections.get_mut(&token) {
                    connection.notification_configs.remove(&config);
                    
                    let removed = self.store.unregister_notification(&config, &connection.notification_queue);
                    Ok(StoreMessage::UnregisterNotificationResponse {
                        id,
                        response: removed,
                    })
                } else {
                    Ok(StoreMessage::UnregisterNotificationResponse {
                        id,
                        response: false,
                    })
                }
            }
            
            StoreMessage::GetEntityType { id, name } => {
                match self.store.get_entity_type(&name) {
                    Ok(entity_type) => Ok(StoreMessage::GetEntityTypeResponse {
                        id,
                        response: Ok(entity_type),
                    }),
                    Err(e) => Ok(StoreMessage::GetEntityTypeResponse {
                        id,
                        response: Err(format!("{:?}", e)),
                    }),
                }
            }
            
            StoreMessage::ResolveEntityType { id, entity_type } => {
                match self.store.resolve_entity_type(entity_type) {
                    Ok(name) => Ok(StoreMessage::ResolveEntityTypeResponse {
                        id,
                        response: Ok(name),
                    }),
                    Err(e) => Ok(StoreMessage::ResolveEntityTypeResponse {
                        id,
                        response: Err(format!("{:?}", e)),
                    }),
                }
            }
            
            StoreMessage::GetFieldType { id, name } => {
                match self.store.get_field_type(&name) {
                    Ok(field_type) => Ok(StoreMessage::GetFieldTypeResponse {
                        id,
                        response: Ok(field_type),
                    }),
                    Err(e) => Ok(StoreMessage::GetFieldTypeResponse {
                        id,
                        response: Err(format!("{:?}", e)),
                    }),
                }
            }
            
            StoreMessage::ResolveFieldType { id, field_type } => {
                match self.store.resolve_field_type(field_type) {
                    Ok(name) => Ok(StoreMessage::ResolveFieldTypeResponse {
                        id,
                        response: Ok(name),
                    }),
                    Err(e) => Ok(StoreMessage::ResolveFieldTypeResponse {
                        id,
                        response: Err(format!("{:?}", e)),
                    }),
                }
            }
            
            _ => {
                Ok(StoreMessage::Error {
                    id: self.extract_message_id(&message).unwrap_or(0),
                    error: "Unsupported message type".to_string(),
                })
            }
        }
    }
    
    /// Send a protocol message to a connection
    fn send_protocol_message(&mut self, token: Token, message: ProtocolMessage) -> Result<()> {
        let encoded = ProtocolCodec::encode(&message)?;
        
        if let Some(connection) = self.connections.get_mut(&token) {
            connection.outbound_messages.push_back(encoded);
            
            // Register for write events if we have messages to send
            self.poll.registry().reregister(
                &mut connection.stream,
                token,
                Interest::READABLE | Interest::WRITABLE
            )?;
        }
        
        Ok(())
    }
    
    /// Send a response message to a client
    fn send_response(&mut self, token: Token, response: StoreMessage) -> Result<()> {
        let protocol_message = ProtocolMessage::Store(response);
        let encoded = ProtocolCodec::encode(&protocol_message)?;
        
        if let Some(connection) = self.connections.get_mut(&token) {
            connection.outbound_messages.push_back(encoded);
            
            // Register for write events if we have messages to send
            self.poll.registry().reregister(
                &mut connection.stream,
                token,
                Interest::READABLE | Interest::WRITABLE
            )?;
        }
        
        Ok(())
    }
    
    /// Remove a connection and clean up resources
    fn remove_connection(&mut self, token: Token) {
        if let Some(connection) = self.connections.remove(&token) {
            info!("Removing connection {}", connection.addr_string);
            
            // Clean up notifications
            for config in &connection.notification_configs {
                self.store.unregister_notification(config, &connection.notification_queue);
            }
            
            // If this is a peer connection, clear the token from peers mapping and start times
            if connection.authenticated && connection.client_id.is_some() {
                for (machine_id, peer_info) in self.peers.iter_mut() {
                    if let Some(peer_token) = peer_info.token {
                        if peer_token == token {
                            info!("Clearing token for disconnected peer {}", machine_id);
                            peer_info.token = None;
                            
                            // Remove the peer's start time
                            peer_info.start_time = None;
                            
                            // Re-evaluate sync needs and leadership with remaining peers
                            self.evaluate_sync_needs();
                            
                            break;
                        }
                    }
                }
            }
        }
    }
    
    /// Handle commands from other actors
    fn handle_command(&mut self, command: CoreCommand) {
        match command {
            CoreCommand::Perform { requests } => {
                debug!("Handling perform command with {} requests", requests.len());
                if let Err(e) = self.store.perform_mut(requests) {
                    error!("Error performing requests: {}", e);
                }
            }
            CoreCommand::TakeSnapshot => {
                debug!("Handling take snapshot command");
                if let Some(snapshot_handle) = &self.snapshot_handle {
                    let snapshot = self.store.take_snapshot();
                    snapshot_handle.save(snapshot);
                }
            }
            CoreCommand::RestoreSnapshot { snapshot } => {
                debug!("Handling restore snapshot command");
                
                // Handle complete snapshot restoration
                self.handle_snapshot_restoration(snapshot);
            }
            CoreCommand::SetSnapshotHandle { snapshot_handle } => {
                debug!("Setting snapshot handle");
                self.snapshot_handle = Some(snapshot_handle);
            }
            CoreCommand::SetWalHandle { wal_handle } => {
                debug!("Setting WAL handle");
                self.wal_handle = Some(wal_handle);
            }
            CoreCommand::PeerConnected { machine_id, mut stream } => {
                debug!("Adding peer connection for machine {}", machine_id);
                
                // Check if peer is already connected
                if let Some(peer_info) = self.peers.get(&machine_id) {
                    if peer_info.token.is_some() {
                        warn!("Peer {} is already connected, ignoring new connection", machine_id);
                        return;
                    }
                }
                
                let token = Token(self.next_token);
                self.next_token += 1;
                
                // Register for read events
                if let Err(e) = self.poll.registry().register(
                    &mut stream,
                    token,
                    Interest::READABLE
                ) {
                    error!("Failed to register peer connection: {}", e);
                    return;
                }

                let addr = stream.peer_addr().map(|addr| addr.to_string()).expect("Failed to get peer address");
                let client_id = if let Some(peer_info) = self.peers.get_mut(&machine_id) {
                    // Update the token in the peers mapping
                    peer_info.token = Some(token);
                    peer_info.entity_id
                } else {
                    None
                };

                let connection = Connection {
                    stream,
                    addr_string: addr,
                    authenticated: false, // Start as unauthenticated
                    client_id,
                    notification_queue: NotificationQueue::new(),
                    notification_configs: HashSet::new(),
                    outbound_messages: VecDeque::new(),
                    message_buffer: MessageBuffer::new()
                };
                
                self.connections.insert(token, connection);
                info!("Peer {} connected with token {:?}", machine_id, token);
                
                // Send authentication message to the peer
                let auth_message = StoreMessage::Authenticate {
                    id: 1, // Simple ID for peer authentication
                    subject_name: self.config.machine.clone(),
                    credential: "peer".to_string(), // Simple peer credential
                };
                
                if let Err(e) = self.send_response(token, auth_message) {
                    error!("Failed to send authentication to peer {}: {}", machine_id, e);
                    self.remove_connection(token);
                }
            }
            CoreCommand::GetPeers { respond_to } => {
                // Convert PeerInfo to the expected tuple format
                let peers_response: AHashMap<String, (Option<Token>, Option<EntityId>)> = 
                    self.peers.iter().map(|(machine_id, peer_info)| {
                        (machine_id.clone(), (peer_info.token, peer_info.entity_id))
                    }).collect();
                
                if let Err(e) = respond_to.send(peers_response) {
                    error!("Failed to send peers response: {}", e);
                }
            }
        }
    }
    
    /// Extract message ID from a store message
    fn extract_message_id(&self, message: &StoreMessage) -> Option<u64> {
        qlib_rs::data::extract_message_id(message)
    }
    
    /// Process notifications for all connections
    fn process_notifications(&mut self) {
        let mut tokens_to_remove = Vec::new();
        let mut messages_to_send = Vec::new();
        
        // Collect notifications and prepare messages for sending
        for (token, connection) in &mut self.connections {
            // Send notifications immediately as they arrive
            while let Some(notification) = connection.notification_queue.pop() {
                let notification_msg = StoreMessage::Notification { notification };
                messages_to_send.push((*token, notification_msg));
            }
        }
        
        // Send all notifications
        for (token, notification_msg) in messages_to_send {
            if let Err(e) = self.send_response(token, notification_msg) {
                if let Some(connection) = self.connections.get(&token) {
                    error!("Failed to send notification to {}: {}", connection.addr_string, e);
                }
                tokens_to_remove.push(token);
            }
        }
        
        // Remove connections that failed to receive notifications
        for token in tokens_to_remove {
            self.remove_connection(token);
        }
    }

    fn write_heartbeat(&mut self) {
        let et_candidate = {
            if let Some(et) = self.store.et.as_ref() {
                if let Some(candidate) = et.candidate {
                    candidate
                } else {
                    warn!("Entity type 'Candidate' not found in store");
                    return;
                }
            } else {
                warn!("Entity type 'Candidate' not found in store");
                return;
            }
        };

        let ft_heartbeat = {
            if let Some(ft) = self.store.ft.as_ref() {
                if let Some(field) = ft.heartbeat {
                    field
                } else {
                    warn!("Field type 'Heartbeat' not found in store");
                    return;
                }
            } else {
                warn!("Field type 'Heartbeat' not found in store");
                return;
            }
        };

        let ft_make_me = {
            if let Some(ft) = self.store.ft.as_ref() {
                if let Some(field) = ft.make_me {
                    field
                } else {
                    warn!("Field type 'MakeMe' not found in store");
                    return;
                }
            } else {
                warn!("Field type 'MakeMe' not found in store");
                return;
            }
        };

        let machine = &self.config.machine;

        let candidates = {
            let result = self.store.find_entities(
            et_candidate, 
            Some(&format!("Name == 'qcore' && Parent->Name == '{}'", machine)));

            match result {
                Ok(ents) => ents,
                Err(e) => {
                    warn!("Failed to query candidate entity for heartbeat: {}", e);
                    return;
                }
            }
        };

        if let Some(candidate) = candidates.first() {
            if let Err(e) = self.store.perform_mut(sreq![
                swrite!(*candidate, sfield![ft_heartbeat], schoice!(0)),
                swrite!(*candidate, sfield![ft_make_me], schoice!(1), PushCondition::Changes)
            ]) {
                warn!("Failed to write heartbeat fields: {}", e);
            }
        }
    }

    fn manage_fault_tolerance(&mut self) {
        if !self.is_leader {
            return;
        }

        // Get required entity and field types
        let (et_candidate, et_fault_tolerance, ft_candidate_list, ft_available_list, ft_current_leader, 
             ft_make_me, ft_heartbeat, ft_death_detection_timeout) = {
            let et = match self.store.et.as_ref() {
                Some(et) => et,
                None => {
                    warn!("Entity types not available");
                    return;
                }
            };
            
            let ft = match self.store.ft.as_ref() {
                Some(ft) => ft,
                None => {
                    warn!("Field types not available");
                    return;
                }
            };

            let et_candidate = match et.candidate {
                Some(candidate) => candidate,
                None => {
                    warn!("Entity type 'Candidate' not found");
                    return;
                }
            };

            let et_fault_tolerance = match et.fault_tolerance {
                Some(ft) => ft,
                None => {
                    warn!("Entity type 'FaultTolerance' not found");
                    return;
                }
            };

            let ft_candidate_list = match ft.candidate_list {
                Some(field) => field,
                None => {
                    warn!("Field type 'CandidateList' not found");
                    return;
                }
            };

            let ft_available_list = match ft.available_list {
                Some(field) => field,
                None => {
                    warn!("Field type 'AvailableList' not found");
                    return;
                }
            };

            let ft_current_leader = match ft.current_leader {
                Some(field) => field,
                None => {
                    warn!("Field type 'CurrentLeader' not found");
                    return;
                }
            };

            let ft_make_me = match ft.make_me {
                Some(field) => field,
                None => {
                    warn!("Field type 'MakeMe' not found");
                    return;
                }
            };

            let ft_heartbeat = match ft.heartbeat {
                Some(field) => field,
                None => {
                    warn!("Field type 'Heartbeat' not found");
                    return;
                }
            };

            let ft_death_detection_timeout = match ft.death_detection_timeout {
                Some(field) => field,
                None => {
                    warn!("Field type 'DeathDetectionTimeout' not found");
                    return;
                }
            };

            (et_candidate, et_fault_tolerance, ft_candidate_list, ft_available_list, ft_current_leader,
             ft_make_me, ft_heartbeat, ft_death_detection_timeout)
        };

        // Find us as a candidate
        let me_as_candidate = {
            let machine = &self.config.machine;
            let candidates_result = self.store.find_entities(
                et_candidate, 
                Some(&format!("Name == 'qcore' && Parent->Name == '{}'", machine))
            );

            match candidates_result {
                Ok(mut candidates) => candidates.pop(),
                Err(e) => {
                    warn!("Failed to find candidate entity: {}", e);
                    return;
                }
            }
        };

        // Get all fault tolerance entities
        let fault_tolerances = match self.store.find_entities(et_fault_tolerance, None) {
            Ok(entities) => entities,
            Err(e) => {
                warn!("Failed to find fault tolerance entities: {}", e);
                return;
            }
        };

        let now = qlib_rs::data::now();

        for ft_entity_id in fault_tolerances {
            // Read fault tolerance fields
            let ft_read_requests = sreq![
                sread!(ft_entity_id, sfield![ft_candidate_list]),
                sread!(ft_entity_id, sfield![ft_available_list]),
                sread!(ft_entity_id, sfield![ft_current_leader])
            ];

            let ft_results = match self.store.perform_mut(ft_read_requests) {
                Ok(results) => results,
                Err(e) => {
                    warn!("Failed to read fault tolerance fields: {}", e);
                    continue;
                }
            };

            let candidates = if let Some(Request::Read { value: Some(qlib_rs::Value::EntityList(candidates)), .. }) = ft_results.read().get(0) {
                candidates.clone()
            } else {
                warn!("Failed to get candidate list");
                continue;
            };

            let current_leader = if let Some(Request::Read { value: Some(qlib_rs::Value::EntityReference(leader)), .. }) = ft_results.read().get(2) {
                leader.clone()
            } else {
                None
            };

            // Check availability of each candidate
            let mut available = Vec::new();
            for candidate_id in &candidates {
                let candidate_read_requests = sreq![
                    sread!(*candidate_id, sfield![ft_make_me]),
                    sread!(*candidate_id, sfield![ft_heartbeat]),
                    sread!(*candidate_id, sfield![ft_death_detection_timeout])
                ];

                let candidate_results = match self.store.perform_mut(candidate_read_requests) {
                    Ok(results) => results,
                    Err(e) => {
                        warn!("Failed to read candidate fields for {:?}: {}", candidate_id, e);
                        continue;
                    }
                };

                let make_me = if let Some(Request::Read { value: Some(qlib_rs::Value::Choice(choice)), .. }) = candidate_results.read().get(0) {
                    *choice
                } else {
                    0 // Default to unavailable
                };

                let heartbeat_time = if let Some(Request::Read { write_time: Some(time), .. }) = candidate_results.read().get(1) {
                    *time
                } else {
                    qlib_rs::data::epoch() // Default to epoch to make it invalid
                };

                let death_detection_timeout_millis = if let Some(Request::Read { value: Some(qlib_rs::Value::Int(timeout)), .. }) = candidate_results.read().get(2) {
                    *timeout
                } else {
                    5000 // Default 5 seconds
                };

                // Check if candidate wants to be available and has recent heartbeat
                let death_detection_timeout = time::Duration::milliseconds(death_detection_timeout_millis);
                if make_me == 1 && heartbeat_time + death_detection_timeout > now {
                    available.push(*candidate_id);
                }
            }

            // Update available list
            if let Err(e) = self.store.perform_mut(sreq![
                swrite!(ft_entity_id, sfield![ft_available_list], Some(qlib_rs::Value::EntityList(available.clone())), PushCondition::Changes)
            ]) {
                warn!("Failed to update available list: {}", e);
                continue;
            }

            // Handle leadership
            let mut handle_me_as_candidate = false;
            if let Some(me_as_candidate) = &me_as_candidate {
                // If we're in the candidate list, we can be leader
                if candidates.contains(me_as_candidate) {
                    handle_me_as_candidate = true;

                    if let Err(e) = self.store.perform_mut(sreq![
                        swrite!(ft_entity_id, sfield![ft_current_leader], Some(qlib_rs::Value::EntityReference(Some(*me_as_candidate))), PushCondition::Changes)
                    ]) {
                        warn!("Failed to set current leader: {}", e);
                    }
                }
            }

            if !handle_me_as_candidate {
                // Promote an available candidate to leader if needed
                if current_leader.is_none() {
                    // No current leader, pick first available
                    let new_leader = available.first().cloned();
                    if let Err(e) = self.store.perform_mut(sreq![
                        swrite!(ft_entity_id, sfield![ft_current_leader], Some(qlib_rs::Value::EntityReference(new_leader)), PushCondition::Changes)
                    ]) {
                        warn!("Failed to set new leader: {}", e);
                    }
                } else if let Some(current_leader_id) = current_leader {
                    if !available.contains(&current_leader_id) {
                        // Current leader is not available, find next one
                        let next_leader = if let Some(current_idx) = candidates.iter().position(|c| *c == current_leader_id) {
                            // Find next available candidate after current leader
                            let mut next_leader = None;
                            
                            // Search after current position
                            for i in (current_idx + 1)..candidates.len() {
                                if available.contains(&candidates[i]) {
                                    next_leader = Some(candidates[i]);
                                    break;
                                }
                            }
                            
                            // Wrap around to beginning if needed
                            if next_leader.is_none() {
                                for i in 0..=current_idx {
                                    if available.contains(&candidates[i]) {
                                        next_leader = Some(candidates[i]);
                                        break;
                                    }
                                }
                            }
                            
                            next_leader
                        } else {
                            // Current leader not in candidates list, pick first available
                            available.first().cloned()
                        };

                        if let Err(e) = self.store.perform_mut(sreq![
                            swrite!(ft_entity_id, sfield![ft_current_leader], Some(qlib_rs::Value::EntityReference(next_leader)), PushCondition::Changes)
                        ]) {
                            warn!("Failed to update leader: {}", e);
                        }
                    }
                }
            }
        }
    }
    
}