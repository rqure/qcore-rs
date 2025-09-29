use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{Read, Write};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use ahash::AHashMap;
use crossbeam::channel::Sender;
use mio::{Poll, Interest, Token, Events, event::Event};
use mio::net::{TcpListener as MioTcpListener, TcpStream as MioTcpStream};
use rustc_hash::FxHashMap;
use tracing::{info, warn, error, debug};
use anyhow::Result;
use std::thread;
use serde_json;

use qlib_rs::{
    EntityId, NotificationQueue, NotifyConfig, Store, Snapshot, StoreTrait,
    EntityType, FieldType, Value,
    PageOpts, Requests
};
use qlib_rs::data::resp::{
    RespValue, RespCommand, RespEncode, RespDecode,
    ReadCommand, WriteCommand, CreateEntityCommand, DeleteEntityCommand,
    ReadResponse, PaginatedEntityResponse, PaginatedEntityTypeResponse,
    EntityListResponse, EntityTypeListResponse, BooleanResponse, IntegerResponse,
    StringResponse, CreateEntityResponse,
    UpdateSchemaCommand, SetFieldSchemaCommand,
    // Peer protocol RESP commands
    PeerHandshakeCommand, FullSyncRequestCommand, FullSyncResponseCommand,
    SyncWriteCommand, NotificationCommand,
};
use qlib_rs::data::entity_schema::{EntitySchemaResp, FieldSchemaResp};

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
    PerformRequests {
        requests: qlib_rs::Requests,
    },
}

/// Handle for communicating with core service
#[derive(Debug, Clone)]
pub struct CoreHandle {
    sender: Sender<CoreCommand>,
}

impl CoreHandle {
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

    pub fn perform(&self, requests: qlib_rs::Requests) {
        self.sender.send(CoreCommand::PerformRequests { requests }).unwrap();
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
    client_id: Option<EntityId>,
    notification_queue: NotificationQueue,
    notification_configs: HashSet<NotifyConfig>,
    outbound_messages: VecDeque<Vec<u8>>,
    read_buffer: Vec<u8>,
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
    
    // Cached candidate entity ID for this machine
    candidate_entity_id: Option<EntityId>,
    
    // Handles to other services  
    snapshot_handle: Option<SnapshotHandle>,
    wal_handle: Option<WalHandle>,
}

const LISTENER_TOKEN: Token = Token(0);

impl CoreService {
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
            candidate_entity_id: None,
            snapshot_handle: None,
            wal_handle: None,
        };

        // Create initial peer entries without entity IDs (will be resolved after snapshot restore)
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

                // Process pending notifications for all connections
                service.process_notifications();

                // Drain the write queue from the store (WriteInfo items for internal tracking)
                while let Some(_write_info) = service.store.write_queue.pop_front() {
                    // WriteInfo items are internal store operations for tracking changes
                    // They don't need peer sync as that happens at the request level
                    // The actual peer synchronization is handled when requests are processed
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

            debug!("Will attempt to connect to {} peers: {:?}", target_peers.len(), target_peers);

            for (machine_id, address) in &target_peers {
                match std::net::TcpStream::connect(address) {
                    Ok(std_stream) => {
                        debug!("Successfully connected to peer {} at {}", machine_id, address);
                        
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
    
    /// Accept new incoming connections
    fn accept_new_connections(&mut self) {
        loop {
            match self.listener.accept() {
                Ok((mut stream, addr)) => {
                    debug!("Accepted new connection from {}", addr);
                    
                    // Optimize TCP socket for low latency
                    if let Err(e) = stream.set_nodelay(true) {
                        warn!("Failed to set TCP_NODELAY on connection from {}: {}", addr, e);
                        // Continue anyway, this is just an optimization
                    }
                    
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
                    client_id: None,
                    notification_queue: NotificationQueue::new(),
                    notification_configs: HashSet::new(),
                    outbound_messages: VecDeque::new(),
                    read_buffer: Vec::with_capacity(8192),
                };                    self.connections.insert(token, connection);
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
        
        loop {
            let (bytes_read, should_continue) = {
                let connection = self.connections.get_mut(&token)
                    .ok_or_else(|| anyhow::anyhow!("Connection not found"))?;
                
                match connection.stream.read(&mut buffer) {
                    Ok(0) => {
                        // Connection closed by peer
                        return Err(anyhow::anyhow!("Connection closed by peer"));
                    }
                    Ok(n) => {
                        // Append new data to the read buffer
                        connection.read_buffer.extend_from_slice(&buffer[0..n]);
                        (n, true)
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        // No more data available right now
                        (0, false)
                    }
                    Err(e) => {
                        return Err(anyhow::anyhow!("Read error: {}", e));
                    }
                }
            };

            // Process any complete commands after reading new data
            if bytes_read > 0 {
                let mut total_consumed = 0;
                
                // Parse all complete commands from the buffer
                loop {
                    // Check if we have data to process
                    let has_data = {
                        if let Some(connection) = self.connections.get(&token) {
                            total_consumed < connection.read_buffer.len()
                        } else {
                            return Err(anyhow::anyhow!("Connection not found"));
                        }
                    };
                    
                    if !has_data {
                        break;
                    }
                    
                    // Try to decode a command (this creates a copy of the needed data)
                    let decode_result = {
                        if let Some(connection) = self.connections.get(&token) {
                            RespValue::decode(&connection.read_buffer[total_consumed..])
                        } else {
                            return Err(anyhow::anyhow!("Connection not found"));
                        }
                    };
                    
                    match decode_result {
                        Ok((resp_value, remaining)) => {
                            let buffer_len = {
                                if let Some(connection) = self.connections.get(&token) {
                                    connection.read_buffer.len()
                                } else {
                                    return Err(anyhow::anyhow!("Connection not found"));
                                }
                            };
                            
                            let consumed = buffer_len - total_consumed - remaining.len();
                            if consumed == 0 {
                                break; // No progress, wait for more data
                            }

                            // Now we can safely call methods that mutably borrow self
                            // Inlined handle_resp_command logic
                            let resp_command_result = {
                                // Expect an array representing a command
                                if let RespValue::Array(args) = resp_value {
                                    if args.is_empty() {
                                        Err(anyhow::anyhow!("Empty command"))
                                    } else {
                                        // First element should be the command name
                                        let cmd_name = match &args[0] {
                                            RespValue::BulkString(name) => std::str::from_utf8(name)?,
                                            RespValue::SimpleString(name) => name,
                                            _ => return Err(anyhow::anyhow!("Command name must be a string")),
                                        };
                                        
                                        // Handle different commands
                                        match cmd_name.to_uppercase().as_str() {
                                            "READ" => {
                                                // Decode the command using the ReadCommand struct
                                                let encoded = RespValue::Array(args[1..].to_vec()).encode();
                                                let (command, _) = ReadCommand::decode(&encoded)
                                                    .map_err(|e| anyhow::anyhow!("Failed to decode READ command: {}", e))?;

                                                // Execute read
                                                match self.store.read(command.entity_id, &command.field_path) {
                                                    Ok((value, timestamp, writer_id)) => {
                                                        let response_struct = ReadResponse {
                                                            value,
                                                            timestamp,
                                                            writer_id,
                                                        };
                                                        let response_bytes = response_struct.encode();
                                                        let (response, _) = RespValue::decode(&response_bytes)
                                                            .map_err(|e| anyhow::anyhow!("Failed to encode response: {}", e))?;
                                                        self.send_resp_response(token, response)
                                                    }
                                                    Err(e) => {
                                                        let error_str = format!("Read error: {}", e);
                                                        self.send_resp_response(token, RespValue::Error(&error_str))
                                                    }
                                                }
                                            },
                                            "WRITE" => {
                                                // Decode the command using the WriteCommand struct
                                                let encoded = RespValue::Array(args[1..].to_vec()).encode();
                                                let (command, _) = WriteCommand::decode(&encoded)
                                                    .map_err(|e| anyhow::anyhow!("Failed to decode WRITE command: {}", e))?;

                                                // Execute write
                                                match self.store.write(command.entity_id, &command.field_path, command.value, command.writer_id, command.write_time, command.push_condition, command.adjust_behavior) {
                                                    Ok(_) => {
                                                        self.send_resp_response(token, RespValue::SimpleString("OK"))
                                                    }
                                                    Err(e) => {
                                                        let error_str = format!("Write error: {}", e);
                                                        self.send_resp_response(token, RespValue::Error(&error_str))
                                                    }
                                                }
                                            },
                                            "CREATE_ENTITY" => {
                                                // Decode the command using the CreateEntityCommand struct
                                                let encoded = RespValue::Array(args[1..].to_vec()).encode();
                                                let (command, _) = CreateEntityCommand::decode(&encoded)
                                                    .map_err(|e| anyhow::anyhow!("Failed to decode CREATE_ENTITY command: {}", e))?;

                                                // Execute create entity
                                                match self.store.create_entity(command.entity_type, command.parent_id, &command.name) {
                                                    Ok(entity_id) => {
                                                        let response_struct = CreateEntityResponse {
                                                            entity_id,
                                                        };
                                                        let response_bytes = response_struct.encode();
                                                        let (response, _) = RespValue::decode(&response_bytes)
                                                            .map_err(|e| anyhow::anyhow!("Failed to encode response: {}", e))?;
                                                        self.send_resp_response(token, response)
                                                    }
                                                    Err(e) => {
                                                        let error_str = format!("Create entity error: {}", e);
                                                        self.send_resp_response(token, RespValue::Error(&error_str))
                                                    }
                                                }
                                            },
                                            "DELETE_ENTITY" => {
                                                // Decode the command using the DeleteEntityCommand struct
                                                let encoded = RespValue::Array(args[1..].to_vec()).encode();
                                                let (command, _) = DeleteEntityCommand::decode(&encoded)
                                                    .map_err(|e| anyhow::anyhow!("Failed to decode DELETE_ENTITY command: {}", e))?;

                                                // Execute delete entity
                                                match self.store.delete_entity(command.entity_id) {
                                                    Ok(_) => {
                                                        self.send_resp_response(token, RespValue::SimpleString("OK"))
                                                    }
                                                    Err(e) => {
                                                        let error_str = format!("Delete entity error: {}", e);
                                                        self.send_resp_response(token, RespValue::Error(&error_str))
                                                    }
                                                }
                                            },
                                            "GET_ENTITY_TYPE" => {
                                                if args.len() <= 1 {
                                                    return Err(anyhow::anyhow!("GET_ENTITY_TYPE requires name"));
                                                }
                                                
                                                // Parse name
                                                let name = match &args[1] {
                                                    RespValue::BulkString(s) => std::str::from_utf8(s)?,
                                                    RespValue::SimpleString(s) => s,
                                                    _ => return Err(anyhow::anyhow!("Invalid name format")),
                                                };
                                                
                                                // Execute get entity type
                                                match self.store.get_entity_type(name) {
                                                    Ok(entity_type) => {
                                                        let response_struct = IntegerResponse {
                                                            value: entity_type.0 as i64,
                                                        };
                                                        let response_bytes = response_struct.encode();
                                                        let (response, _) = RespValue::decode(&response_bytes)
                                                            .map_err(|e| anyhow::anyhow!("Failed to encode response: {}", e))?;
                                                        self.send_resp_response(token, response)
                                                    }
                                                    Err(e) => {
                                                        let error_str = format!("Get entity type error: {}", e);
                                                        self.send_resp_response(token, RespValue::Error(&error_str))
                                                    }
                                                }
                                            },
                                            "RESOLVE_ENTITY_TYPE" => {
                                                if args.len() <= 1 {
                                                    return Err(anyhow::anyhow!("RESOLVE_ENTITY_TYPE requires entity_type"));
                                                }
                                                
                                                // Parse entity_type
                                                let entity_type_str = match &args[1] {
                                                    RespValue::BulkString(s) => std::str::from_utf8(s)?,
                                                    RespValue::SimpleString(s) => s,
                                                    RespValue::Integer(i) => &i.to_string(),
                                                    _ => return Err(anyhow::anyhow!("Invalid entity_type format")),
                                                };
                                                let entity_type: EntityType = EntityType(entity_type_str.parse::<u32>()
                                                    .map_err(|_| anyhow::anyhow!("Invalid entity_type"))?);
                                                
                                                // Execute resolve entity type
                                                match self.store.resolve_entity_type(entity_type) {
                                                    Ok(name) => {
                                                        let response_struct = StringResponse {
                                                            value: name,
                                                        };
                                                        let response_bytes = response_struct.encode();
                                                        let (response, _) = RespValue::decode(&response_bytes)
                                                            .map_err(|e| anyhow::anyhow!("Failed to encode response: {}", e))?;
                                                        self.send_resp_response(token, response)
                                                    }
                                                    Err(e) => {
                                                        let error_str = format!("Resolve entity type error: {}", e);
                                                        self.send_resp_response(token, RespValue::Error(&error_str))
                                                    }
                                                }
                                            },
                                            "GET_FIELD_TYPE" => {
                                                if args.len() <= 1 {
                                                    return Err(anyhow::anyhow!("GET_FIELD_TYPE requires name"));
                                                }
                                                
                                                // Parse name
                                                let name = match &args[1] {
                                                    RespValue::BulkString(s) => std::str::from_utf8(s)?,
                                                    RespValue::SimpleString(s) => s,
                                                    _ => return Err(anyhow::anyhow!("Invalid name format")),
                                                };
                                                
                                                // Execute get field type
                                                match self.store.get_field_type(name) {
                                                    Ok(field_type) => {
                                                        let response_struct = IntegerResponse {
                                                            value: field_type.0 as i64,
                                                        };
                                                        let response_bytes = response_struct.encode();
                                                        let (response, _) = RespValue::decode(&response_bytes)
                                                            .map_err(|e| anyhow::anyhow!("Failed to encode response: {}", e))?;
                                                        self.send_resp_response(token, response)
                                                    }
                                                    Err(e) => {
                                                        let error_str = format!("Get field type error: {}", e);
                                                        self.send_resp_response(token, RespValue::Error(&error_str))
                                                    }
                                                }
                                            },
                                            "RESOLVE_FIELD_TYPE" => {
                                                if args.len() <= 1 {
                                                    return Err(anyhow::anyhow!("RESOLVE_FIELD_TYPE requires field_type"));
                                                }
                                                
                                                // Parse field_type
                                                let field_type_str = match &args[1] {
                                                    RespValue::BulkString(s) => std::str::from_utf8(s)?,
                                                    RespValue::SimpleString(s) => s,
                                                    RespValue::Integer(i) => &i.to_string(),
                                                    _ => return Err(anyhow::anyhow!("Invalid field_type format")),
                                                };
                                                let field_type: FieldType = FieldType(field_type_str.parse::<u64>()
                                                    .map_err(|_| anyhow::anyhow!("Invalid field_type"))?);
                                                
                                                // Execute resolve field type
                                                match self.store.resolve_field_type(field_type) {
                                                    Ok(name) => {
                                                        let response_struct = StringResponse {
                                                            value: name,
                                                        };
                                                        let response_bytes = response_struct.encode();
                                                        let (response, _) = RespValue::decode(&response_bytes)
                                                            .map_err(|e| anyhow::anyhow!("Failed to encode response: {}", e))?;
                                                        self.send_resp_response(token, response)
                                                    }
                                                    Err(e) => {
                                                        let error_str = format!("Resolve field type error: {}", e);
                                                        self.send_resp_response(token, RespValue::Error(&error_str))
                                                    }
                                                }
                                            },
                                            "GET_ENTITY_SCHEMA" => self.handle_get_entity_schema_command(token, &args[1..]),
                                            "UPDATE_SCHEMA" => self.handle_update_schema_command(token, &args[1..]),
                                            "GET_FIELD_SCHEMA" => self.handle_get_field_schema_command(token, &args[1..]),
                                            "SET_FIELD_SCHEMA" => self.handle_set_field_schema_command(token, &args[1..]),
                                            "FIND_ENTITIES" => self.handle_find_entities_command(token, &args[1..]),
                                            "FIND_ENTITIES_EXACT" => self.handle_find_entities_exact_command(token, &args[1..]),
                                            "FIND_ENTITIES_PAGINATED" => self.handle_find_entities_paginated_command(token, &args[1..]),
                                            "GET_ENTITY_TYPES" => {
                                                // Execute get entity types
                                                match self.store.get_entity_types() {
                                                    Ok(entity_types) => {
                                                        let response_struct = EntityTypeListResponse {
                                                            entity_types,
                                                        };
                                                        let response_bytes = response_struct.encode();
                                                        let (response, _) = RespValue::decode(&response_bytes)
                                                            .map_err(|e| anyhow::anyhow!("Failed to encode response: {}", e))?;
                                                        self.send_resp_response(token, response)
                                                    }
                                                    Err(e) => {
                                                        let error_str = format!("Get entity types error: {}", e);
                                                        self.send_resp_response(token, RespValue::Error(&error_str))
                                                    }
                                                }
                                            },
                                            "GET_ENTITY_TYPES_PAGINATED" => self.handle_get_entity_types_paginated_command(token, &args[1..]),
                                            "ENTITY_EXISTS" => {
                                                if args.len() <= 1 {
                                                    return Err(anyhow::anyhow!("ENTITY_EXISTS requires entity_id"));
                                                }
                                                
                                                // Parse entity_id
                                                let entity_id_str = match &args[1] {
                                                    RespValue::BulkString(s) => std::str::from_utf8(s)?,
                                                    RespValue::SimpleString(s) => s,
                                                    _ => return Err(anyhow::anyhow!("Invalid entity_id format")),
                                                };
                                                let entity_id: EntityId = EntityId(entity_id_str.parse::<u64>()
                                                    .map_err(|_| anyhow::anyhow!("Invalid entity_id"))?);
                                                
                                                // Execute entity exists check
                                                let exists = self.store.entity_exists(entity_id);
                                                let response_struct = BooleanResponse {
                                                    result: exists,
                                                };
                                                let response_bytes = response_struct.encode();
                                                let (response, _) = RespValue::decode(&response_bytes)
                                                    .map_err(|e| anyhow::anyhow!("Failed to encode response: {}", e))?;
                                                self.send_resp_response(token, response)
                                            },
                                            "FIELD_EXISTS" => {
                                                if args.len() < 3 {
                                                    return Err(anyhow::anyhow!("FIELD_EXISTS requires entity_type and field_type"));
                                                }
                                                
                                                // Parse entity_type
                                                let entity_type_str = match &args[1] {
                                                    RespValue::BulkString(s) => std::str::from_utf8(s)?,
                                                    RespValue::SimpleString(s) => s,
                                                    RespValue::Integer(i) => &i.to_string(),
                                                    _ => return Err(anyhow::anyhow!("Invalid entity_type format")),
                                                };
                                                let entity_type: EntityType = EntityType(entity_type_str.parse::<u32>()
                                                    .map_err(|_| anyhow::anyhow!("Invalid entity_type"))?);
                                                    
                                                // Parse field_type
                                                let field_type_str = match &args[2] {
                                                    RespValue::BulkString(s) => std::str::from_utf8(s)?,
                                                    RespValue::SimpleString(s) => s,
                                                    RespValue::Integer(i) => &i.to_string(),
                                                    _ => return Err(anyhow::anyhow!("Invalid field_type format")),
                                                };
                                                let field_type: FieldType = FieldType(field_type_str.parse::<u64>()
                                                    .map_err(|_| anyhow::anyhow!("Invalid field_type"))?);
                                                
                                                // Execute field exists check
                                                let exists = self.store.field_exists(entity_type, field_type);
                                                let response_struct = BooleanResponse {
                                                    result: exists,
                                                };
                                                let response_bytes = response_struct.encode();
                                                let (response, _) = RespValue::decode(&response_bytes)
                                                    .map_err(|e| anyhow::anyhow!("Failed to encode response: {}", e))?;
                                                self.send_resp_response(token, response)
                                            },
                                            "RESOLVE_INDIRECTION" => {
                                                if args.len() < 3 {
                                                    return Err(anyhow::anyhow!("RESOLVE_INDIRECTION requires entity_id and field_path"));
                                                }
                                                
                                                // Parse entity_id
                                                let entity_id_str = match &args[1] {
                                                    RespValue::BulkString(s) => std::str::from_utf8(s)?,
                                                    RespValue::SimpleString(s) => s,
                                                    _ => return Err(anyhow::anyhow!("Invalid entity_id format")),
                                                };
                                                let entity_id: EntityId = EntityId(entity_id_str.parse::<u64>()
                                                    .map_err(|_| anyhow::anyhow!("Invalid entity_id"))?);
                                                    
                                                // Parse field_path
                                                let field_path_str = match &args[2] {
                                                    RespValue::BulkString(s) => std::str::from_utf8(s)?,
                                                    RespValue::SimpleString(s) => s,
                                                    _ => return Err(anyhow::anyhow!("Invalid field_path format")),
                                                };
                                                
                                                let field_path: Vec<FieldType> = if field_path_str.is_empty() {
                                                    vec![]
                                                } else {
                                                    field_path_str.split(',')
                                                        .map(|s| s.trim().parse::<u64>().map(|v| FieldType(v)))
                                                        .collect::<std::result::Result<Vec<_>, _>>()
                                                        .map_err(|_| anyhow::anyhow!("Invalid field_path"))?
                                                };
                                                
                                                // Execute resolve indirection
                                                match self.store.resolve_indirection(entity_id, &field_path) {
                                                    Ok((resolved_entity_id, resolved_field_type)) => {
                                                        let entity_id_bytes = format!("{}", resolved_entity_id.0).into_bytes();
                                                        let response = RespValue::Array(vec![
                                                            RespValue::BulkString(&entity_id_bytes),
                                                            RespValue::Integer(resolved_field_type.0 as i64),
                                                        ]);
                                                        self.send_resp_response(token, response)
                                                    }
                                                    Err(e) => {
                                                        let error_str = format!("Resolve indirection error: {}", e);
                                                        self.send_resp_response(token, RespValue::Error(&error_str))
                                                    }
                                                }
                                            },
                                            "TAKE_SNAPSHOT" => {
                                                // Execute take snapshot
                                                let snapshot = self.store.take_snapshot();
                                                let encoded_snapshot = serde_json::to_vec(&snapshot)
                                                    .map_err(|e| anyhow::anyhow!("Failed to serialize snapshot: {}", e))?;
                                                let response = RespValue::BulkString(&encoded_snapshot);
                                                self.send_resp_response(token, response)
                                            },
                                            "REGISTER_NOTIFICATION" => self.handle_register_notification_command(token, &args[1..]),
                                            "UNREGISTER_NOTIFICATION" => self.handle_unregister_notification_command(token, &args[1..]),
                                            // Peer protocol commands
                                            "PEER_HANDSHAKE" => {
                                                let encoded = RespValue::Array(args[1..].to_vec()).encode();
                                                let (command, _) = PeerHandshakeCommand::decode(&encoded)
                                                    .map_err(|e| anyhow::anyhow!("Failed to decode PEER_HANDSHAKE command: {}", e))?;

                                                self.handle_peer_handshake(token, command.start_time, command.is_response, command.machine_id)?;
                                                
                                                // Send OK response
                                                let ok_response = RespValue::SimpleString("OK");
                                                self.send_resp_response(token, ok_response)
                                            },
                                            "FULL_SYNC_REQUEST" => {
                                                self.handle_peer_full_sync_request(token)
                                            },
                                            "FULL_SYNC_RESPONSE" => {
                                                let encoded = RespValue::Array(args[1..].to_vec()).encode();
                                                let (command, _) = FullSyncResponseCommand::decode(&encoded)
                                                    .map_err(|e| anyhow::anyhow!("Failed to decode FULL_SYNC_RESPONSE command: {}", e))?;

                                                self.handle_peer_full_sync_response(token, command.snapshot_data)
                                            },
                                            "SYNC_WRITE" => {
                                                let encoded = RespValue::Array(args[1..].to_vec()).encode();
                                                let (command, _) = SyncWriteCommand::decode(&encoded)
                                                    .map_err(|e| anyhow::anyhow!("Failed to decode SYNC_WRITE command: {}", e))?;

                                                self.handle_peer_sync_write(token, command.requests_data)
                                            },
                                            "NOTIFICATION" => {
                                                let encoded = RespValue::Array(args[1..].to_vec()).encode();
                                                let (command, _) = NotificationCommand::decode(&encoded)
                                                    .map_err(|e| anyhow::anyhow!("Failed to decode NOTIFICATION command: {}", e))?;

                                                // Deserialize the notification
                                                let notification: qlib_rs::Notification = serde_json::from_str(&command.notification_data)
                                                    .map_err(|e| anyhow::anyhow!("Failed to deserialize notification: {}", e))?;
                                                
                                                debug!("Received notification from peer: {:?}", notification);
                                                
                                                // Forward the notification to local clients if needed
                                                // This might require additional logic based on your notification system
                                                
                                                Ok(())
                                            },
                                            _ => {
                                                let error_str = format!("Unknown command: {}", cmd_name);
                                                self.send_resp_response(token, RespValue::Error(&error_str))
                                            }
                                        }
                                    }
                                } else {
                                    Err(anyhow::anyhow!("Expected array for command"))
                                }
                            };
                            
                            if let Err(e) = resp_command_result {
                                error!("Error handling RESP command: {}", e);
                                let error_str = format!("Error: {}", e);
                                let error_response = RespValue::Error(&error_str);
                                self.send_resp_response(token, error_response)?;
                            }
                            total_consumed += consumed;
                        }
                        Err(_) => break, // Incomplete command
                    }
                }
                
                // Remove processed data from the buffer
                if total_consumed > 0 {
                    if let Some(connection) = self.connections.get_mut(&token) {
                        connection.read_buffer.drain(0..total_consumed);
                    }
                }
            }
            
            if !should_continue {
                break;
            }
        }
        
        Ok(())
    }

    
    /// Send a RESP command to a peer connection
    fn send_peer_command<T: RespCommand<'static>>(&mut self, token: Token, command: &T) -> Result<()> {
        let encoded = command.encode();
        
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

    /// Send a response message to a client connection
    fn send_response<T: RespEncode>(&mut self, token: Token, response: &T) -> Result<()> {
        let encoded = response.encode();
        
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
    
    /// Send a notification message to a client connection
    fn send_notification(&mut self, token: Token, notification: &qlib_rs::Notification) -> Result<()> {
        let notification_json = serde_json::to_string(notification)
            .map_err(|e| anyhow::anyhow!("Failed to serialize notification: {}", e))?;
        
        let command = NotificationCommand {
            notification_data: notification_json,
            _marker: std::marker::PhantomData,
        };
        
        self.send_response(token, &command)
    }
    
    /// Send a raw RESP response to a connection
    fn send_resp_response(&mut self, token: Token, response: RespValue) -> Result<()> {
        let encoded = response.encode();
        
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

    /// Handle READ command
    fn handle_read_command(&mut self, token: Token, args: &[RespValue]) -> Result<()> {
        // Decode the command using the ReadCommand struct
        let encoded = RespValue::Array(args.to_vec()).encode();
        let (command, _) = ReadCommand::decode(&encoded)
            .map_err(|e| anyhow::anyhow!("Failed to decode READ command: {}", e))?;

        // Execute read
        match self.store.read(command.entity_id, &command.field_path) {
            Ok((value, timestamp, writer_id)) => {
                let response_struct = ReadResponse {
                    value,
                    timestamp,
                    writer_id,
                };
                let response_bytes = response_struct.encode();
                let (response, _) = RespValue::decode(&response_bytes)
                    .map_err(|e| anyhow::anyhow!("Failed to encode response: {}", e))?;
                self.send_resp_response(token, response)?;
            }
            Err(e) => {
                let error_str = format!("Read error: {}", e);
                self.send_resp_response(token, RespValue::Error(&error_str))?;
            }
        }

        Ok(())
    }

    /// Handle WRITE command  
    fn handle_write_command(&mut self, token: Token, args: &[RespValue]) -> Result<()> {
        // Decode the command using the WriteCommand struct
        let encoded = RespValue::Array(args.to_vec()).encode();
        let (command, _) = WriteCommand::decode(&encoded)
            .map_err(|e| anyhow::anyhow!("Failed to decode WRITE command: {}", e))?;

        // Execute write
        match self.store.write(command.entity_id, &command.field_path, command.value, command.writer_id, command.write_time, command.push_condition, command.adjust_behavior) {
            Ok(_) => {
                self.send_resp_response(token, RespValue::SimpleString("OK"))?;
            }
            Err(e) => {
                let error_str = format!("Write error: {}", e);
                self.send_resp_response(token, RespValue::Error(&error_str))?;
            }
        }
        
        Ok(())
    }

    /// Handle CREATE_ENTITY command
    fn handle_create_entity_command(&mut self, token: Token, args: &[RespValue]) -> Result<()> {
        // Decode the command using the CreateEntityCommand struct
        let encoded = RespValue::Array(args.to_vec()).encode();
        let (command, _) = CreateEntityCommand::decode(&encoded)
            .map_err(|e| anyhow::anyhow!("Failed to decode CREATE_ENTITY command: {}", e))?;

        // Execute create entity
        match self.store.create_entity(command.entity_type, command.parent_id, &command.name) {
            Ok(entity_id) => {
                let response_struct = CreateEntityResponse {
                    entity_id,
                };
                let response_bytes = response_struct.encode();
                let (response, _) = RespValue::decode(&response_bytes)
                    .map_err(|e| anyhow::anyhow!("Failed to encode response: {}", e))?;
                self.send_resp_response(token, response)?;
            }
            Err(e) => {
                let error_str = format!("Create entity error: {}", e);
                self.send_resp_response(token, RespValue::Error(&error_str))?;
            }
        }
        
        Ok(())
    }

    /// Handle DELETE_ENTITY command
    fn handle_delete_entity_command(&mut self, token: Token, args: &[RespValue]) -> Result<()> {
        // Decode the command using the DeleteEntityCommand struct
        let encoded = RespValue::Array(args.to_vec()).encode();
        let (command, _) = DeleteEntityCommand::decode(&encoded)
            .map_err(|e| anyhow::anyhow!("Failed to decode DELETE_ENTITY command: {}", e))?;

        // Execute delete entity
        match self.store.delete_entity(command.entity_id) {
            Ok(_) => {
                self.send_resp_response(token, RespValue::SimpleString("OK"))?;
            }
            Err(e) => {
                let error_str = format!("Delete entity error: {}", e);
                self.send_resp_response(token, RespValue::Error(&error_str))?;
            }
        }
        
        Ok(())
    }

    /// Handle GET_ENTITY_TYPE command
    fn handle_get_entity_type_command(&mut self, token: Token, args: &[RespValue]) -> Result<()> {
        if args.is_empty() {
            return Err(anyhow::anyhow!("GET_ENTITY_TYPE requires name"));
        }
        
        // Parse name
        let name = match &args[0] {
            RespValue::BulkString(s) => std::str::from_utf8(s)?,
            RespValue::SimpleString(s) => s,
            _ => return Err(anyhow::anyhow!("Invalid name format")),
        };
        
        // Execute get entity type
        match self.store.get_entity_type(name) {
            Ok(entity_type) => {
                let response_struct = IntegerResponse {
                    value: entity_type.0 as i64,
                };
                let response_bytes = response_struct.encode();
                let (response, _) = RespValue::decode(&response_bytes)
                    .map_err(|e| anyhow::anyhow!("Failed to encode response: {}", e))?;
                self.send_resp_response(token, response)?;
            }
            Err(e) => {
                let error_str = format!("Get entity type error: {}", e);
                self.send_resp_response(token, RespValue::Error(&error_str))?;
            }
        }
        
        Ok(())
    }

    /// Handle RESOLVE_ENTITY_TYPE command
    fn handle_resolve_entity_type_command(&mut self, token: Token, args: &[RespValue]) -> Result<()> {
        if args.is_empty() {
            return Err(anyhow::anyhow!("RESOLVE_ENTITY_TYPE requires entity_type"));
        }
        
        // Parse entity_type
        let entity_type_str = match &args[0] {
            RespValue::BulkString(s) => std::str::from_utf8(s)?,
            RespValue::SimpleString(s) => s,
            RespValue::Integer(i) => &i.to_string(),
            _ => return Err(anyhow::anyhow!("Invalid entity_type format")),
        };
        let entity_type: EntityType = EntityType(entity_type_str.parse::<u32>()
            .map_err(|_| anyhow::anyhow!("Invalid entity_type"))?);
        
        // Execute resolve entity type
        match self.store.resolve_entity_type(entity_type) {
            Ok(name) => {
                let response_struct = StringResponse {
                    value: name,
                };
                let response_bytes = response_struct.encode();
                let (response, _) = RespValue::decode(&response_bytes)
                    .map_err(|e| anyhow::anyhow!("Failed to encode response: {}", e))?;
                self.send_resp_response(token, response)?;
            }
            Err(e) => {
                let error_str = format!("Resolve entity type error: {}", e);
                self.send_resp_response(token, RespValue::Error(&error_str))?;
            }
        }
        
        Ok(())
    }

    /// Handle GET_FIELD_TYPE command
    fn handle_get_field_type_command(&mut self, token: Token, args: &[RespValue]) -> Result<()> {
        if args.is_empty() {
            return Err(anyhow::anyhow!("GET_FIELD_TYPE requires name"));
        }
        
        // Parse name
        let name = match &args[0] {
            RespValue::BulkString(s) => std::str::from_utf8(s)?,
            RespValue::SimpleString(s) => s,
            _ => return Err(anyhow::anyhow!("Invalid name format")),
        };
        
        // Execute get field type
        match self.store.get_field_type(name) {
            Ok(field_type) => {
                let response_struct = IntegerResponse {
                    value: field_type.0 as i64,
                };
                let response_bytes = response_struct.encode();
                let (response, _) = RespValue::decode(&response_bytes)
                    .map_err(|e| anyhow::anyhow!("Failed to encode response: {}", e))?;
                self.send_resp_response(token, response)?;
            }
            Err(e) => {
                let error_str = format!("Get field type error: {}", e);
                self.send_resp_response(token, RespValue::Error(&error_str))?;
            }
        }
        
        Ok(())
    }

    /// Handle RESOLVE_FIELD_TYPE command
    fn handle_resolve_field_type_command(&mut self, token: Token, args: &[RespValue]) -> Result<()> {
        if args.is_empty() {
            return Err(anyhow::anyhow!("RESOLVE_FIELD_TYPE requires field_type"));
        }
        
        // Parse field_type
        let field_type_str = match &args[0] {
            RespValue::BulkString(s) => std::str::from_utf8(s)?,
            RespValue::SimpleString(s) => s,
            RespValue::Integer(i) => &i.to_string(),
            _ => return Err(anyhow::anyhow!("Invalid field_type format")),
        };
        let field_type: FieldType = FieldType(field_type_str.parse::<u64>()
            .map_err(|_| anyhow::anyhow!("Invalid field_type"))?);
        
        // Execute resolve field type
        match self.store.resolve_field_type(field_type) {
            Ok(name) => {
                let response_struct = StringResponse {
                    value: name,
                };
                let response_bytes = response_struct.encode();
                let (response, _) = RespValue::decode(&response_bytes)
                    .map_err(|e| anyhow::anyhow!("Failed to encode response: {}", e))?;
                self.send_resp_response(token, response)?;
            }
            Err(e) => {
                let error_str = format!("Resolve field type error: {}", e);
                self.send_resp_response(token, RespValue::Error(&error_str))?;
            }
        }
        
        Ok(())
    }

    /// Handle GET_ENTITY_SCHEMA command
    fn handle_get_entity_schema_command(&mut self, token: Token, args: &[RespValue]) -> Result<()> {
        if args.is_empty() {
            return Err(anyhow::anyhow!("GET_ENTITY_SCHEMA requires entity_type"));
        }
        
        // Parse entity_type
        let entity_type_str = match &args[0] {
            RespValue::BulkString(s) => std::str::from_utf8(s)?,
            RespValue::SimpleString(s) => s,
            RespValue::Integer(i) => &i.to_string(),
            _ => return Err(anyhow::anyhow!("Invalid entity_type format")),
        };
        let entity_type: EntityType = EntityType(entity_type_str.parse::<u32>()
            .map_err(|_| anyhow::anyhow!("Invalid entity_type"))?);
        
        // Execute get entity schema
        match self.store.get_entity_schema(entity_type) {
            Ok(schema) => {
                let schema_resp = EntitySchemaResp::from_entity_schema(&schema, &self.store);
                let encoded_schema = schema_resp.encode();
                let response = RespValue::BulkString(&encoded_schema);
                self.send_resp_response(token, response)?;
            }
            Err(e) => {
                let error_str = format!("Get entity schema error: {}", e);
                self.send_resp_response(token, RespValue::Error(&error_str))?;
            }
        }
        
        Ok(())
    }

    /// Handle UPDATE_SCHEMA command
    fn handle_update_schema_command(&mut self, token: Token, args: &[RespValue]) -> Result<()> {
        // Decode the command using the UpdateSchemaCommand struct
        let encoded = RespValue::Array(args.to_vec()).encode();
        let (command, _) = UpdateSchemaCommand::decode(&encoded)
            .map_err(|e| anyhow::anyhow!("Failed to decode UPDATE_SCHEMA command: {}", e))?;

        // Convert EntitySchemaResp back to EntitySchema<Single, String, String>
        let schema_string = command.schema.to_entity_schema(&self.store)?;
        
        // Execute update schema
        match self.store.update_schema(schema_string) {
            Ok(_) => {
                self.send_resp_response(token, RespValue::SimpleString("OK"))?;
            }
            Err(e) => {
                let error_str = format!("Update schema error: {}", e);
                self.send_resp_response(token, RespValue::Error(&error_str))?;
            }
        }
        
        Ok(())
    }

    /// Handle GET_FIELD_SCHEMA command
    fn handle_get_field_schema_command(&mut self, token: Token, args: &[RespValue]) -> Result<()> {
        if args.len() < 2 {
            return Err(anyhow::anyhow!("GET_FIELD_SCHEMA requires entity_type and field_type"));
        }
        
        // Parse entity_type
        let entity_type_str = match &args[0] {
            RespValue::BulkString(s) => std::str::from_utf8(s)?,
            RespValue::SimpleString(s) => s,
            RespValue::Integer(i) => &i.to_string(),
            _ => return Err(anyhow::anyhow!("Invalid entity_type format")),
        };
        let entity_type: EntityType = EntityType(entity_type_str.parse::<u32>()
            .map_err(|_| anyhow::anyhow!("Invalid entity_type"))?);
            
        // Parse field_type
        let field_type_str = match &args[1] {
            RespValue::BulkString(s) => std::str::from_utf8(s)?,
            RespValue::SimpleString(s) => s,
            RespValue::Integer(i) => &i.to_string(),
            _ => return Err(anyhow::anyhow!("Invalid field_type format")),
        };
        let field_type: FieldType = FieldType(field_type_str.parse::<u64>()
            .map_err(|_| anyhow::anyhow!("Invalid field_type"))?);
        
        // Execute get field schema
        match self.store.get_field_schema(entity_type, field_type) {
            Ok(field_schema) => {
                let field_schema_resp = FieldSchemaResp::from_field_schema(&field_schema, &self.store);
                let encoded_schema = field_schema_resp.encode();
                let response = RespValue::BulkString(&encoded_schema);
                self.send_resp_response(token, response)?;
            }
            Err(e) => {
                let error_str = format!("Get field schema error: {}", e);
                self.send_resp_response(token, RespValue::Error(&error_str))?;
            }
        }
        
        Ok(())
    }

    /// Handle SET_FIELD_SCHEMA command
    fn handle_set_field_schema_command(&mut self, token: Token, args: &[RespValue]) -> Result<()> {
        // Decode the command using the SetFieldSchemaCommand struct
        let encoded = RespValue::Array(args.to_vec()).encode();
        let (command, _) = SetFieldSchemaCommand::decode(&encoded)
            .map_err(|e| anyhow::anyhow!("Failed to decode SET_FIELD_SCHEMA command: {}", e))?;

        // Convert FieldSchemaResp back to FieldSchema
        let field_schema_string = command.schema.to_field_schema();
        let field_schema = qlib_rs::FieldSchema::from_string_schema(field_schema_string, &self.store);
        
        // Execute set field schema
        match self.store.set_field_schema(command.entity_type, command.field_type, field_schema) {
            Ok(_) => {
                self.send_resp_response(token, RespValue::SimpleString("OK"))?;
            }
            Err(e) => {
                let error_str = format!("Set field schema error: {}", e);
                self.send_resp_response(token, RespValue::Error(&error_str))?;
            }
        }
        
        Ok(())
    }

    /// Handle RESOLVE_INDIRECTION command
    fn handle_resolve_indirection_command(&mut self, token: Token, args: &[RespValue]) -> Result<()> {
        if args.len() < 2 {
            return Err(anyhow::anyhow!("RESOLVE_INDIRECTION requires entity_id and field_path"));
        }
        
        // Parse entity_id
        let entity_id_str = match &args[0] {
            RespValue::BulkString(s) => std::str::from_utf8(s)?,
            RespValue::SimpleString(s) => s,
            _ => return Err(anyhow::anyhow!("Invalid entity_id format")),
        };
        let entity_id: EntityId = EntityId(entity_id_str.parse::<u64>()
            .map_err(|_| anyhow::anyhow!("Invalid entity_id"))?);
            
        // Parse field_path
        let field_path_str = match &args[1] {
            RespValue::BulkString(s) => std::str::from_utf8(s)?,
            RespValue::SimpleString(s) => s,
            _ => return Err(anyhow::anyhow!("Invalid field_path format")),
        };
        
        let field_path: Vec<FieldType> = if field_path_str.is_empty() {
            vec![]
        } else {
            field_path_str.split(',')
                .map(|s| s.trim().parse::<u64>().map(|v| FieldType(v)))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|_| anyhow::anyhow!("Invalid field_path"))?
        };
        
        // Execute resolve indirection
        match self.store.resolve_indirection(entity_id, &field_path) {
            Ok((resolved_entity_id, resolved_field_type)) => {
                let entity_id_bytes = format!("{}", resolved_entity_id.0).into_bytes();
                let response = RespValue::Array(vec![
                    RespValue::BulkString(&entity_id_bytes),
                    RespValue::Integer(resolved_field_type.0 as i64),
                ]);
                self.send_resp_response(token, response)?;
            }
            Err(e) => {
                let error_str = format!("Resolve indirection error: {}", e);
                self.send_resp_response(token, RespValue::Error(&error_str))?;
            }
        }
        
        Ok(())
    }

    /// Handle FIELD_EXISTS command
    fn handle_field_exists_command(&mut self, token: Token, args: &[RespValue]) -> Result<()> {
        if args.len() < 2 {
            return Err(anyhow::anyhow!("FIELD_EXISTS requires entity_type and field_type"));
        }
        
        // Parse entity_type
        let entity_type_str = match &args[0] {
            RespValue::BulkString(s) => std::str::from_utf8(s)?,
            RespValue::SimpleString(s) => s,
            RespValue::Integer(i) => &i.to_string(),
            _ => return Err(anyhow::anyhow!("Invalid entity_type format")),
        };
        let entity_type: EntityType = EntityType(entity_type_str.parse::<u32>()
            .map_err(|_| anyhow::anyhow!("Invalid entity_type"))?);
            
        // Parse field_type
        let field_type_str = match &args[1] {
            RespValue::BulkString(s) => std::str::from_utf8(s)?,
            RespValue::SimpleString(s) => s,
            RespValue::Integer(i) => &i.to_string(),
            _ => return Err(anyhow::anyhow!("Invalid field_type format")),
        };
        let field_type: FieldType = FieldType(field_type_str.parse::<u64>()
            .map_err(|_| anyhow::anyhow!("Invalid field_type"))?);
        
        // Execute field exists check
        let exists = self.store.field_exists(entity_type, field_type);
        let response_struct = BooleanResponse {
            result: exists,
        };
        let response_bytes = response_struct.encode();
        let (response, _) = RespValue::decode(&response_bytes)
            .map_err(|e| anyhow::anyhow!("Failed to encode response: {}", e))?;
        self.send_resp_response(token, response)?;
        
        Ok(())
    }

    /// Handle FIND_ENTITIES_EXACT command
    fn handle_find_entities_exact_command(&mut self, token: Token, args: &[RespValue]) -> Result<()> {
        if args.is_empty() {
            return Err(anyhow::anyhow!("FIND_ENTITIES_EXACT requires entity_type"));
        }
        
        // Parse entity_type
        let entity_type_str = match &args[0] {
            RespValue::BulkString(s) => std::str::from_utf8(s)?,
            RespValue::SimpleString(s) => s,
            RespValue::Integer(i) => &i.to_string(),
            _ => return Err(anyhow::anyhow!("Invalid entity_type format")),
        };
        let entity_type: EntityType = EntityType(entity_type_str.parse::<u32>()
            .map_err(|_| anyhow::anyhow!("Invalid entity_type"))?);
        
        // Parse optional pagination options and filter
        let page_opts = if args.len() > 1 {
            match &args[1] {
                RespValue::BulkString(s) => {
                    let opts_str = std::str::from_utf8(s)?;
                    if opts_str == "null" || opts_str.is_empty() {
                        None
                    } else {
                        // Simple format: "limit,cursor" or just "limit"
                        let parts: Vec<&str> = opts_str.split(',').collect();
                        let limit = parts[0].parse::<usize>().unwrap_or(100);
                        let cursor = if parts.len() > 1 { 
                            Some(parts[1].parse::<usize>().unwrap_or(0))
                        } else {
                            None
                        };
                        Some(PageOpts::new(limit, cursor))
                    }
                }
                RespValue::Null => None,
                _ => return Err(anyhow::anyhow!("Invalid page_opts format")),
            }
        } else {
            None
        };
        
        let filter = if args.len() > 2 {
            match &args[2] {
                RespValue::BulkString(s) => {
                    let filter_str = std::str::from_utf8(s)?;
                    if filter_str == "null" || filter_str.is_empty() {
                        None
                    } else {
                        Some(filter_str)
                    }
                }
                RespValue::Null => None,
                _ => return Err(anyhow::anyhow!("Invalid filter format")),
            }
        } else {
            None
        };
        
        // Execute find entities exact
        match self.store.find_entities_exact(entity_type, page_opts.as_ref(), filter) {
            Ok(page_result) => {
                let response_struct = PaginatedEntityResponse {
                    items: page_result.items,
                    total: page_result.total,
                    next_cursor: page_result.next_cursor,
                };
                let response_bytes = response_struct.encode();
                let (response, _) = RespValue::decode(&response_bytes)
                    .map_err(|e| anyhow::anyhow!("Failed to encode response: {}", e))?;
                self.send_resp_response(token, response)?;
            }
            Err(e) => {
                let error_str = format!("Find entities exact error: {}", e);
                self.send_resp_response(token, RespValue::Error(&error_str))?;
            }
        }
        
        Ok(())
    }

    /// Handle GET_ENTITY_TYPES command
    fn handle_get_entity_types_command(&mut self, token: Token, _args: &[RespValue]) -> Result<()> {
        // Execute get entity types
        match self.store.get_entity_types() {
            Ok(entity_types) => {
                let response_struct = EntityTypeListResponse {
                    entity_types,
                };
                let response_bytes = response_struct.encode();
                let (response, _) = RespValue::decode(&response_bytes)
                    .map_err(|e| anyhow::anyhow!("Failed to encode response: {}", e))?;
                self.send_resp_response(token, response)?;
            }
            Err(e) => {
                let error_str = format!("Get entity types error: {}", e);
                self.send_resp_response(token, RespValue::Error(&error_str))?;
            }
        }
        
        Ok(())
    }

    /// Handle GET_ENTITY_TYPES_PAGINATED command
    fn handle_get_entity_types_paginated_command(&mut self, token: Token, args: &[RespValue]) -> Result<()> {
        // Parse optional pagination options
        let page_opts = if !args.is_empty() {
            match &args[0] {
                RespValue::BulkString(s) => {
                    let opts_str = std::str::from_utf8(s)?;
                    if opts_str == "null" || opts_str.is_empty() {
                        None
                    } else {
                        // Simple format: "limit,cursor" or just "limit"
                        let parts: Vec<&str> = opts_str.split(',').collect();
                        let limit = parts[0].parse::<usize>().unwrap_or(100);
                        let cursor = if parts.len() > 1 { 
                            Some(parts[1].parse::<usize>().unwrap_or(0))
                        } else {
                            None
                        };
                        Some(PageOpts::new(limit, cursor))
                    }
                }
                RespValue::Null => None,
                _ => return Err(anyhow::anyhow!("Invalid page_opts format")),
            }
        } else {
            None
        };
        
        // Execute get entity types paginated
        match self.store.get_entity_types_paginated(page_opts.as_ref()) {
            Ok(page_result) => {
                let response_struct = PaginatedEntityTypeResponse {
                    items: page_result.items,
                    total: page_result.total,
                    next_cursor: page_result.next_cursor,
                };
                let response_bytes = response_struct.encode();
                let (response, _) = RespValue::decode(&response_bytes)
                    .map_err(|e| anyhow::anyhow!("Failed to encode response: {}", e))?;
                self.send_resp_response(token, response)?;
            }
            Err(e) => {
                let error_str = format!("Get entity types paginated error: {}", e);
                self.send_resp_response(token, RespValue::Error(&error_str))?;
            }
        }
        
        Ok(())
    }

    /// Handle FIND_ENTITIES command
    fn handle_find_entities_command(&mut self, token: Token, args: &[RespValue]) -> Result<()> {
        if args.is_empty() {
            return Err(anyhow::anyhow!("FIND_ENTITIES requires entity_type"));
        }
        
        // Parse entity_type
        let entity_type_str = match &args[0] {
            RespValue::BulkString(s) => std::str::from_utf8(s)?,
            RespValue::SimpleString(s) => s,
            RespValue::Integer(i) => &i.to_string(),
            _ => return Err(anyhow::anyhow!("Invalid entity_type format")),
        };
        let entity_type: EntityType = EntityType(entity_type_str.parse::<u32>()
            .map_err(|_| anyhow::anyhow!("Invalid entity_type"))?);
        
        // Parse optional filter
        let filter = if args.len() > 1 {
            match &args[1] {
                RespValue::BulkString(s) => {
                    let filter_str = std::str::from_utf8(s)?;
                    if filter_str == "null" || filter_str.is_empty() {
                        None
                    } else {
                        Some(filter_str)
                    }
                }
                RespValue::Null => None,
                _ => return Err(anyhow::anyhow!("Invalid filter format")),
            }
        } else {
            None
        };
        
        // Execute find entities
        match self.store.find_entities(entity_type, filter) {
            Ok(entities) => {
                let response_struct = EntityListResponse {
                    entities,
                };
                let response_bytes = response_struct.encode();
                let (response, _) = RespValue::decode(&response_bytes)
                    .map_err(|e| anyhow::anyhow!("Failed to encode response: {}", e))?;
                self.send_resp_response(token, response)?;
            }
            Err(e) => {
                let error_str = format!("Find entities error: {}", e);
                self.send_resp_response(token, RespValue::Error(&error_str))?;
            }
        }
        
        Ok(())
    }

    /// Handle FIND_ENTITIES_PAGINATED command
    fn handle_find_entities_paginated_command(&mut self, token: Token, args: &[RespValue]) -> Result<()> {
        if args.is_empty() {
            return Err(anyhow::anyhow!("FIND_ENTITIES_PAGINATED requires entity_type"));
        }
        
        // Parse entity_type
        let entity_type_str = match &args[0] {
            RespValue::BulkString(s) => std::str::from_utf8(s)?,
            RespValue::SimpleString(s) => s,
            RespValue::Integer(i) => &i.to_string(),
            _ => return Err(anyhow::anyhow!("Invalid entity_type format")),
        };
        let entity_type: EntityType = EntityType(entity_type_str.parse::<u32>()
            .map_err(|_| anyhow::anyhow!("Invalid entity_type"))?);
        
        // Parse optional pagination options and filter
        let page_opts = if args.len() > 1 {
            match &args[1] {
                RespValue::BulkString(s) => {
                    let opts_str = std::str::from_utf8(s)?;
                    if opts_str == "null" || opts_str.is_empty() {
                        None
                    } else {
                        // Simple format: "limit,cursor" or just "limit"
                        let parts: Vec<&str> = opts_str.split(',').collect();
                        let limit = parts[0].parse::<usize>().unwrap_or(100);
                        let cursor = if parts.len() > 1 { 
                            Some(parts[1].parse::<usize>().unwrap_or(0))
                        } else {
                            None
                        };
                        Some(qlib_rs::PageOpts::new(limit, cursor))
                    }
                }
                RespValue::Null => None,
                _ => return Err(anyhow::anyhow!("Invalid page_opts format")),
            }
        } else {
            None
        };
        
        let filter = if args.len() > 2 {
            match &args[2] {
                RespValue::BulkString(s) => {
                    let filter_str = std::str::from_utf8(s)?;
                    if filter_str == "null" || filter_str.is_empty() {
                        None
                    } else {
                        Some(filter_str)
                    }
                }
                RespValue::Null => None,
                _ => return Err(anyhow::anyhow!("Invalid filter format")),
            }
        } else {
            None
        };
        
        // Execute find entities paginated
        match self.store.find_entities_paginated(entity_type, page_opts.as_ref(), filter) {
            Ok(page_result) => {
                let response_struct = PaginatedEntityResponse {
                    items: page_result.items,
                    total: page_result.total,
                    next_cursor: page_result.next_cursor,
                };
                let response_bytes = response_struct.encode();
                let (response, _) = RespValue::decode(&response_bytes)
                    .map_err(|e| anyhow::anyhow!("Failed to encode response: {}", e))?;
                self.send_resp_response(token, response)?;
            }
            Err(e) => {
                let error_str = format!("Find entities paginated error: {}", e);
                self.send_resp_response(token, RespValue::Error(&error_str))?;
            }
        }
        
        Ok(())
    }

    /// Handle ENTITY_EXISTS command
    fn handle_entity_exists_command(&mut self, token: Token, args: &[RespValue]) -> Result<()> {
        if args.is_empty() {
            return Err(anyhow::anyhow!("ENTITY_EXISTS requires entity_id"));
        }
        
        // Parse entity_id
        let entity_id_str = match &args[0] {
            RespValue::BulkString(s) => std::str::from_utf8(s)?,
            RespValue::SimpleString(s) => s,
            _ => return Err(anyhow::anyhow!("Invalid entity_id format")),
        };
        let entity_id: EntityId = EntityId(entity_id_str.parse::<u64>()
            .map_err(|_| anyhow::anyhow!("Invalid entity_id"))?);
        
        // Execute entity exists check
        let exists = self.store.entity_exists(entity_id);
        let response_struct = BooleanResponse {
            result: exists,
        };
        let response_bytes = response_struct.encode();
        let (response, _) = RespValue::decode(&response_bytes)
            .map_err(|e| anyhow::anyhow!("Failed to encode response: {}", e))?;
        self.send_resp_response(token, response)?;
        
        Ok(())
    }

    /// Handle TAKE_SNAPSHOT command
    fn handle_take_snapshot_command(&mut self, token: Token, _args: &[RespValue]) -> Result<()> {
        // Execute take snapshot
        let snapshot = self.store.take_snapshot();
        let encoded_snapshot = serde_json::to_vec(&snapshot)
            .map_err(|e| anyhow::anyhow!("Failed to serialize snapshot: {}", e))?;
        let response = RespValue::BulkString(&encoded_snapshot);
        self.send_resp_response(token, response)?;
        
        Ok(())
    }

    /// Handle REGISTER_NOTIFICATION command
    fn handle_register_notification_command(&mut self, token: Token, args: &[RespValue]) -> Result<()> {
        if args.is_empty() {
            return Err(anyhow::anyhow!("REGISTER_NOTIFICATION requires config"));
        }
        
        // Parse notification config
        let config_bytes = match &args[0] {
            RespValue::BulkString(s) => s,
            _ => return Err(anyhow::anyhow!("Invalid config format")),
        };
        let (config, _) = NotifyConfig::decode(config_bytes)
            .map_err(|_| anyhow::anyhow!("Invalid config encoding"))?;
        
        // Get the connection's notification queue
        if let Some(connection) = self.connections.get_mut(&token) {
            connection.notification_configs.insert(config.clone());
            
            match self.store.register_notification(config.clone(), connection.notification_queue.clone()) {
                Ok(_) => {
                    self.send_resp_response(token, RespValue::SimpleString("OK"))?;
                }
                Err(e) => {
                    let error_str = format!("Register notification error: {}", e);
                    self.send_resp_response(token, RespValue::Error(&error_str))?;
                }
            }
        } else {
            self.send_resp_response(token, RespValue::Error("Connection not found"))?;
        }
        
        Ok(())
    }

    /// Handle UNREGISTER_NOTIFICATION command
    fn handle_unregister_notification_command(&mut self, token: Token, args: &[RespValue]) -> Result<()> {
        if args.is_empty() {
            return Err(anyhow::anyhow!("UNREGISTER_NOTIFICATION requires config"));
        }
        
        // Parse notification config
        let config_bytes = match &args[0] {
            RespValue::BulkString(s) => s,
            _ => return Err(anyhow::anyhow!("Invalid config format")),
        };
        let (config, _) = NotifyConfig::decode(config_bytes)
            .map_err(|_| anyhow::anyhow!("Invalid config encoding"))?;
        
        // Get the connection's notification queue
        if let Some(connection) = self.connections.get_mut(&token) {
            connection.notification_configs.remove(&config);
            
            let removed = self.store.unregister_notification(&config, &connection.notification_queue);
            let response = RespValue::Integer(if removed { 1 } else { 0 });
            self.send_resp_response(token, response)?;
        } else {
            self.send_resp_response(token, RespValue::Error("Connection not found"))?;
        }
        
        Ok(())
    }
    
    
    /// Handle peer handshake RESP command
    fn handle_peer_handshake_command(&mut self, token: Token, args: &[RespValue]) -> Result<()> {
        let encoded = RespValue::Array(args.to_vec()).encode();
        let (command, _) = PeerHandshakeCommand::decode(&encoded)
            .map_err(|e| anyhow::anyhow!("Failed to decode PEER_HANDSHAKE command: {}", e))?;

        self.handle_peer_handshake(token, command.start_time, command.is_response, command.machine_id)?;
        
        // Send OK response
        let ok_response = RespValue::SimpleString("OK");
        self.send_resp_response(token, ok_response)?;
        
        Ok(())
    }

    /// Handle full sync request RESP command
    fn handle_peer_full_sync_request_command(&mut self, token: Token, _args: &[RespValue]) -> Result<()> {
        self.handle_peer_full_sync_request(token)?;
        Ok(())
    }

    /// Handle full sync response RESP command  
    fn handle_peer_full_sync_response_command(&mut self, token: Token, args: &[RespValue]) -> Result<()> {
        let encoded = RespValue::Array(args.to_vec()).encode();
        let (command, _) = FullSyncResponseCommand::decode(&encoded)
            .map_err(|e| anyhow::anyhow!("Failed to decode FULL_SYNC_RESPONSE command: {}", e))?;

        self.handle_peer_full_sync_response(token, command.snapshot_data)?;
        Ok(())
    }

    /// Handle sync write RESP command
    fn handle_peer_sync_write_command(&mut self, token: Token, args: &[RespValue]) -> Result<()> {
        let encoded = RespValue::Array(args.to_vec()).encode();
        let (command, _) = SyncWriteCommand::decode(&encoded)
            .map_err(|e| anyhow::anyhow!("Failed to decode SYNC_WRITE command: {}", e))?;

        self.handle_peer_sync_write(token, command.requests_data)?;
        Ok(())
    }

    /// Handle notification RESP command (from other nodes)
    fn handle_notification_command(&mut self, _token: Token, args: &[RespValue]) -> Result<()> {
        let encoded = RespValue::Array(args.to_vec()).encode();
        let (command, _) = NotificationCommand::decode(&encoded)
            .map_err(|e| anyhow::anyhow!("Failed to decode NOTIFICATION command: {}", e))?;

        // Deserialize the notification
        let notification: qlib_rs::Notification = serde_json::from_str(&command.notification_data)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize notification: {}", e))?;
        
        debug!("Received notification from peer: {:?}", notification);
        
        // Forward the notification to local clients if needed
        // This might require additional logic based on your notification system
        
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
    
    /// Resolve candidate entity IDs for ourselves and all peers
    /// This should be called whenever store.et becomes available (e.g., after snapshot restoration)
    fn resolve_candidate_entity_ids(&mut self) {
        // Clear existing candidate entity IDs since they may have changed
        self.candidate_entity_id = None;
        for (_, peer_info) in self.peers.iter_mut() {
            peer_info.entity_id = None;
        }

        if let Some(et) = self.store.et.as_ref() {
            if let Some(candidate_etype) = et.candidate {
                // Resolve our own candidate entity ID
                if let Ok(candidates) = self.store.find_entities(
                    candidate_etype, 
                    Some(&format!("Name == 'qcore' && Parent->Name == '{}'", self.config.machine))
                ) {
                    if let Some(candidate_id) = candidates.first() {
                        self.candidate_entity_id = Some(*candidate_id);
                        debug!("Resolved our candidate entity ID: {:?}", candidate_id);
                    } else {
                        warn!("No candidate entity found for our machine {}", self.config.machine);
                    }
                } else {
                    warn!("Failed to query candidate entity for our machine {}", self.config.machine);
                }

                // Resolve peer candidate entity IDs
                for (machine_id, peer_info) in self.peers.iter_mut() {
                    if let Ok(peer_candidates) = self.store.find_entities(
                        candidate_etype, 
                        Some(&format!("Name == 'qcore' && Parent->Name == '{}'", machine_id))
                    ) {
                        if let Some(candidate) = peer_candidates.first() {
                            peer_info.entity_id = Some(*candidate);
                            // Update connected peer connections
                            if let Some(token) = peer_info.token {
                                if let Some(connection) = self.connections.get_mut(&token) {
                                    connection.client_id = Some(*candidate);
                                }
                            }
                            debug!("Resolved candidate entity ID for peer {}: {:?}", machine_id, candidate);
                        } else {
                            warn!("No candidate entity found for peer {}", machine_id);
                        }
                    } else {
                        warn!("Failed to query candidate entity for peer {}", machine_id);
                    }
                }
            } else {
                warn!("Candidate entity type not found in store schema");
            }
        } else {
            debug!("Store schema (et) not yet available, candidate entity IDs will be resolved later");
        }
    }

    /// Handle complete snapshot restoration including store, cache, and peer reinitialization
    fn handle_snapshot_restoration(&mut self, snapshot: Snapshot) {
        // Restore the snapshot to our store
        self.store.restore_snapshot(snapshot);

        // Disconnect all non-peer clients since snapshot restoration invalidates their state
        let mut non_peer_tokens = Vec::new();
        let peer_tokens: HashSet<Token> = self.peers.values()
            .filter_map(|peer_info| peer_info.token)
            .collect();
        
        for (&token, connection) in &self.connections {
            if !peer_tokens.contains(&token) {
                non_peer_tokens.push(token);
                info!("Disconnecting non-peer client {} due to snapshot restoration", connection.addr_string);
            }
        }
        
        // Remove non-peer connections
        for token in non_peer_tokens {
            self.remove_connection(token);
        }

        // Update peer entity IDs for already connected peers
        for (_machine_id, peer_info) in self.peers.iter_mut() {
            if let Some(token) = peer_info.token {
                self.connections.get_mut(&token).map(|conn| {
                    conn.client_id = peer_info.entity_id;
                    // Peers remain authenticated since they don't need explicit authentication
                });
            }
        }

        // Resolve all candidate entity IDs (ours and peers) now that schema is available
        self.resolve_candidate_entity_ids();
        self.store.default_writer_id = self.candidate_entity_id.clone();

        // Re-evaluate leadership after restoration
        self.evaluate_leadership();
    }

    /// Handle peer handshake message
    fn handle_peer_handshake(&mut self, token: Token, peer_start_time: u64, is_response: bool, peer_machine_id: String) -> Result<()> {
        debug!("Received handshake from peer {} with start time {}", peer_machine_id, peer_start_time);
        
        // Update peer info with the token if this is a new connection
        if let Some(peer_info) = self.peers.get_mut(&peer_machine_id) {
            if peer_info.token.is_none() {
                peer_info.token = Some(token);
                debug!("Associated token {:?} with peer {}", token, peer_machine_id);
                
                // Update connection with peer entity ID if available
                if let Some(connection) = self.connections.get_mut(&token) {
                    connection.client_id = peer_info.entity_id;
                }
            }
            peer_info.start_time = Some(peer_start_time);
        } else {
            warn!("Received handshake from unknown peer: {}", peer_machine_id);
        }
        
        // Only send a response if this was a request, not a response
        if !is_response {
            let handshake_response = PeerHandshakeCommand {
                start_time: self.start_time,
                is_response: true,
                machine_id: self.config.machine.clone(),
                _marker: std::marker::PhantomData,
            };
            
            if let Err(e) = self.send_peer_command(token, &handshake_response) {
                error!("Failed to send handshake response to peer {}: {}", peer_machine_id, e);
            } else {
                debug!("Sent handshake response to peer {} with our start time {}", peer_machine_id, self.start_time);
            }
        } else {
            debug!("Received handshake response from peer {}, no reply needed", peer_machine_id);
        }
        
        // Check if we should sync based on all known peers
        self.evaluate_sync_needs();
        
        Ok(())
    }
    
    /// Evaluate leadership status based on start times, with machine ID as tie-breaker
    fn evaluate_leadership(&mut self) {
        // Find the peer with the earliest start time, using machine ID as tie-breaker
        let mut oldest_start_time = self.start_time;
        let mut oldest_machine_id = self.config.machine.clone();
        let mut has_older_peer = false;
        
        debug!("Evaluating leadership: our start_time={}, machine={}", self.start_time, self.config.machine);
        
        for (machine_id, peer_info) in &self.peers {
            if let Some(start_time) = peer_info.start_time {
                debug!("Peer {} has start_time={}", machine_id, start_time);
                let is_older = start_time < oldest_start_time || 
                               (start_time == oldest_start_time && machine_id < &oldest_machine_id);
                               
                if is_older {
                    debug!("Peer {} is older than current oldest", machine_id);
                    oldest_start_time = start_time;
                    oldest_machine_id = machine_id.clone();
                    has_older_peer = true;
                }
            } else {
                debug!("Peer {} has no start_time yet", machine_id);
            }
        }
        
        let was_leader = self.is_leader;
        self.is_leader = !has_older_peer;
        
        debug!("Leadership evaluation result: has_older_peer={}, is_leader={}", has_older_peer, self.is_leader);
        
        if was_leader != self.is_leader {
            if self.is_leader {
                info!("This service is now the leader (started at {}, machine: {})", self.start_time, self.config.machine);
            } else {
                info!("This service is no longer the leader (older peer found: start_time={}, machine={})", oldest_start_time, oldest_machine_id);
            }
        }
    }
    
    /// Evaluate if we need to sync and from which peer
    fn evaluate_sync_needs(&mut self) {
        // Find the peer with the earliest start time, using machine ID as tie-breaker
        let mut oldest_peer: Option<(String, u64)> = None;
        let mut oldest_start_time = self.start_time;
        let mut oldest_machine_id = self.config.machine.clone();
        
        for (machine_id, peer_info) in &self.peers {
            if let Some(start_time) = peer_info.start_time {
                let is_older = start_time < oldest_start_time ||
                               (start_time == oldest_start_time && machine_id < &oldest_machine_id);
                               
                if is_older {
                    oldest_start_time = start_time;
                    oldest_machine_id = machine_id.clone();
                    oldest_peer = Some((machine_id.clone(), start_time));
                }
            }
        }
        
        // If we found an older peer, sync from them
        if let Some((oldest_machine_id, oldest_time)) = oldest_peer {
            debug!("Found older peer {} (started at {}), requesting full sync", oldest_machine_id, oldest_time);
            
            // Find the token for this peer
            if let Some(peer_info) = self.peers.get(&oldest_machine_id) {
                if let Some(peer_token) = peer_info.token {
                    let sync_request = FullSyncRequestCommand {
                        _marker: std::marker::PhantomData,
                    };
                    
                    if let Err(e) = self.send_peer_command(peer_token, &sync_request) {
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
            debug!("We are the oldest peer (started at {}, machine: {}), no sync needed", self.start_time, self.config.machine);
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
            
        debug!("Received full sync request from peer {}", requesting_machine_id);
        
        // Take a snapshot of our current store
        let snapshot = self.store.take_snapshot();
        let snapshot_json = serde_json::to_string(&snapshot)
            .map_err(|e| anyhow::anyhow!("Failed to serialize snapshot: {}", e))?;
        
        let sync_response = FullSyncResponseCommand {
            snapshot_data: snapshot_json,
            _marker: std::marker::PhantomData,
        };
        
        if let Err(e) = self.send_peer_command(token, &sync_response) {
            error!("Failed to send full sync response to peer {}: {}", requesting_machine_id, e);
        } else {
            info!("Sent full sync response to peer {}", requesting_machine_id);
        }
        
        Ok(())
    }
    
    /// Handle peer full sync response
    fn handle_peer_full_sync_response(&mut self, _token: Token, snapshot_json: String) -> Result<()> {
        debug!("Received full sync response, restoring snapshot");
        
        // Deserialize snapshot from JSON
        let snapshot: Snapshot = serde_json::from_str(&snapshot_json)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize snapshot: {}", e))?;
        
        // Handle complete snapshot restoration
        self.handle_snapshot_restoration(snapshot);
        
        info!("Full sync completed successfully");
        
        Ok(())
    }
    
    /// Handle peer sync write message
    fn handle_peer_sync_write(&mut self, _token: Token, requests_json: String) -> Result<()> {
        debug!("Received sync write from peer");
        
        // Deserialize requests from JSON
        let requests: Requests = serde_json::from_str(&requests_json)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize requests: {}", e))?;
        
        debug!("Applying {} requests from peer", requests.len());
        
        // Apply the requests to our store
        self.perform_requests(requests);
        
        Ok(())
    }
    
    /// Apply a batch of requests to the store
    fn perform_requests(&mut self, requests: qlib_rs::Requests) {
        for request in requests.read().iter() {
            match request {
                qlib_rs::Request::Read { entity_id, field_types, value, write_time: _write_time, writer_id: _writer_id } => {
                    // For read requests, we don't need to do anything as they don't modify the store
                    // But we should update the request with the result
                    if let Some(_value_ref) = value {
                        // The request already has a value, no need to re-read
                        continue;
                    }
                    
                    match self.store.read(*entity_id, field_types) {
                        Ok((_read_value, _timestamp, _writer)) => {
                            // Note: We can't modify the request here because Requests uses Arc<RwLock>
                            // The read result would need to be stored somewhere else
                            // For now, we'll just log that we processed the read
                            debug!("Processed read request for entity {:?}, field {:?}", entity_id, field_types);
                        }
                        Err(e) => {
                            warn!("Failed to process read request: {}", e);
                        }
                    }
                }
                qlib_rs::Request::Write { entity_id, field_types, value, push_condition, adjust_behavior, write_time, writer_id, write_processed: _write_processed } => {
                    match self.store.write(*entity_id, field_types, value.clone().unwrap_or(Value::String("".to_string())), *writer_id, *write_time, Some(push_condition.clone()), Some(adjust_behavior.clone())) {
                        Ok(_) => {
                            debug!("Processed write request for entity {:?}, field {:?}", entity_id, field_types);
                        }
                        Err(e) => {
                            warn!("Failed to process write request: {}", e);
                        }
                    }
                }
                qlib_rs::Request::Create { entity_type, parent_id, name, created_entity_id: _created_entity_id, timestamp: _timestamp } => {
                    match self.store.create_entity(*entity_type, *parent_id, name) {
                        Ok(entity_id) => {
                            debug!("Processed create request, created entity {:?}", entity_id);
                        }
                        Err(e) => {
                            warn!("Failed to process create request: {}", e);
                        }
                    }
                }
                qlib_rs::Request::Delete { entity_id, timestamp: _timestamp } => {
                    match self.store.delete_entity(*entity_id) {
                        Ok(_) => {
                            debug!("Processed delete request for entity {:?}", entity_id);
                        }
                        Err(e) => {
                            warn!("Failed to process delete request: {}", e);
                        }
                    }
                }
                qlib_rs::Request::SchemaUpdate { schema, timestamp: _timestamp } => {
                    match self.store.update_schema(schema.clone()) {
                        Ok(_) => {
                            debug!("Processed schema update request");
                        }
                        Err(e) => {
                            warn!("Failed to process schema update request: {}", e);
                        }
                    }
                }
                qlib_rs::Request::Snapshot { snapshot_counter: _snapshot_counter, timestamp: _timestamp } => {
                    // Snapshot requests don't modify the store, they're just markers
                    debug!("Processed snapshot marker request");
                }
                qlib_rs::Request::GetEntityType { name: _name, entity_type: _entity_type } => {
                    // These are read-only operations, no store modification needed
                    debug!("Processed get entity type request");
                }
                qlib_rs::Request::ResolveEntityType { entity_type: _entity_type, name: _name } => {
                    debug!("Processed resolve entity type request");
                }
                qlib_rs::Request::GetFieldType { name: _name, field_type: _field_type } => {
                    debug!("Processed get field type request");
                }
                qlib_rs::Request::ResolveFieldType { field_type: _field_type, name: _name } => {
                    debug!("Processed resolve field type request");
                }
                qlib_rs::Request::GetEntitySchema { entity_type: _entity_type, schema: _schema } => {
                    debug!("Processed get entity schema request");
                }
                qlib_rs::Request::GetCompleteEntitySchema { entity_type: _entity_type, schema: _schema } => {
                    debug!("Processed get complete entity schema request");
                }
                qlib_rs::Request::GetFieldSchema { entity_type: _entity_type, field_type: _field_type, schema: _schema } => {
                    debug!("Processed get field schema request");
                }
                qlib_rs::Request::EntityExists { entity_id: _entity_id, exists: _exists } => {
                    debug!("Processed entity exists request");
                }
                qlib_rs::Request::FieldExists { entity_type: _entity_type, field_type: _field_type, exists: _exists } => {
                    debug!("Processed field exists request");
                }
                qlib_rs::Request::FindEntities { entity_type: _entity_type, page_opts: _page_opts, filter: _filter, result: _result } => {
                    debug!("Processed find entities request");
                }
                qlib_rs::Request::FindEntitiesExact { entity_type: _entity_type, page_opts: _page_opts, filter: _filter, result: _result } => {
                    debug!("Processed find entities exact request");
                }
                qlib_rs::Request::GetEntityTypes { page_opts: _page_opts, result: _result } => {
                    debug!("Processed get entity types request");
                }
            }
        }
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
            if connection.client_id.is_some() {
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
                
                // Optimize TCP socket for low latency
                if let Err(e) = stream.set_nodelay(true) {
                    warn!("Failed to set TCP_NODELAY on peer connection from {}: {}", machine_id, e);
                    // Continue anyway, this is just an optimization
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
                    client_id,
                    notification_queue: NotificationQueue::new(),
                    notification_configs: HashSet::new(),
                    outbound_messages: VecDeque::new(),
                    read_buffer: Vec::with_capacity(8192),
                };
                
                self.connections.insert(token, connection);
                info!("Peer {} connected with token {:?}", machine_id, token);
                
                // Send initial handshake with RESP protocol
                let handshake = PeerHandshakeCommand {
                    start_time: self.start_time,
                    is_response: false,
                    machine_id: self.config.machine.clone(),
                    _marker: std::marker::PhantomData,
                };
                
                if let Err(e) = self.send_peer_command(token, &handshake) {
                    error!("Failed to send handshake to peer {}: {}", machine_id, e);
                } else {
                    debug!("Sent handshake to peer {} with our start time {}", machine_id, self.start_time);
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
            CoreCommand::PerformRequests { requests } => {
                debug!("Handling perform requests command with {} requests", requests.len());
                self.perform_requests(requests);
            }
        }
    }
    
    /// Process notifications for all connections
    fn process_notifications(&mut self) {
        let mut notifications_to_send = Vec::new();
        
        // Collect notifications that need to be sent
        for (token, connection) in &mut self.connections {
            while let Some(notification) = connection.notification_queue.pop() {
                notifications_to_send.push((*token, notification, connection.addr_string.clone()));
            }
        }
        
        // Send all collected notifications
        for (token, notification, addr_string) in notifications_to_send {
            if let Err(e) = self.send_notification(token, &notification) {
                error!("Failed to send notification to {}: {}", addr_string, e);
            }
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
            debug!("Writing heartbeat fields for candidate {:?}", candidate);
            // Update heartbeat fields using direct Store API calls
            if let Err(e) = self.store.write(*candidate, &[ft_heartbeat], Value::Choice(0), None, None, None, None) {
                warn!("Failed to write heartbeat field: {}", e);
            }
            if let Err(e) = self.store.write(*candidate, &[ft_make_me], Value::Choice(1), None, None, Some(qlib_rs::PushCondition::Changes), None) {
                warn!("Failed to write make_me field: {}", e);
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
            // Read fault tolerance fields using direct Store API calls
            let candidate_list_result = self.store.read(ft_entity_id, &[ft_candidate_list]);
            let available_list_result = self.store.read(ft_entity_id, &[ft_available_list]);
            let current_leader_result = self.store.read(ft_entity_id, &[ft_current_leader]);

            let candidates = match candidate_list_result {
                Ok((value, _, _)) => {
                    if let Value::EntityList(list) = value {
                        list
                    } else {
                        warn!("Invalid candidate list format");
                        continue;
                    }
                }
                Err(e) => {
                    warn!("Failed to read candidate list: {}", e);
                    continue;
                }
            };

            if candidates.is_empty() {
                warn!("Failed to get candidate list");
                continue;
            }

            let current_available = match available_list_result {
                Ok((value, _, _)) => {
                    if let Value::EntityList(list) = value {
                        list
                    } else {
                        Vec::new()
                    }
                }
                Err(_) => Vec::new(),
            };

            let current_leader = match current_leader_result {
                Ok((value, _, _)) => {
                    if let Value::EntityReference(Some(id)) = value {
                        Some(id)
                    } else {
                        None
                    }
                }
                Err(_) => None,
            };

            // Check availability of each candidate
            let mut available = Vec::new();
            for candidate_id in &candidates {
                // Read candidate fields using direct Store API calls
                let make_me_result = self.store.read(*candidate_id, &[ft_make_me]);
                let heartbeat_result = self.store.read(*candidate_id, &[ft_heartbeat]);
                let timeout_result = self.store.read(*candidate_id, &[ft_death_detection_timeout]);

                let make_me = match make_me_result {
                    Ok((value, _, _)) => {
                        if let Value::Choice(choice) = value {
                            choice
                        } else {
                            0
                        }
                    }
                    Err(_) => 0,
                };

                let heartbeat_time = match heartbeat_result {
                    Ok((_, timestamp, _)) => timestamp,
                    Err(_) => qlib_rs::data::epoch(),
                };

                let death_detection_timeout_millis = match timeout_result {
                    Ok((value, _, _)) => {
                        if let Value::Int(timeout) = value {
                            timeout
                        } else {
                            5000
                        }
                    }
                    Err(_) => 5000,
                };

                // Check if candidate wants to be available and has recent heartbeat
                let death_detection_timeout = time::Duration::milliseconds(death_detection_timeout_millis);
                if make_me == 1 && heartbeat_time + death_detection_timeout > now {
                    available.push(*candidate_id);
                }
            }

            // Update available list only if it has changed
            if current_available != available {
                debug!("Updating available list for fault tolerance entity {:?}: {:?}", ft_entity_id, available);
                if let Err(e) = self.store.write(ft_entity_id, &[ft_available_list], Value::EntityList(available.clone()), None, None, None, None) {
                    warn!("Failed to update available list: {}", e);
                    continue;
                }
            }

            // Handle leadership
            let mut handle_me_as_candidate = false;
            if let Some(me_as_candidate) = &me_as_candidate {
                // If we're in the candidate list, we can be leader
                if candidates.contains(me_as_candidate) {
                    handle_me_as_candidate = true;

                    // Only write if the current leader is different
                    if current_leader != Some(*me_as_candidate) {
                        debug!("Setting current leader to {:?} for fault tolerance entity {:?}", me_as_candidate, ft_entity_id);
                        if let Err(e) = self.store.write(ft_entity_id, &[ft_current_leader], Value::EntityReference(Some(*me_as_candidate)), None, None, None, None) {
                            warn!("Failed to set current leader: {}", e);
                        }
                    }
                }
            }

            if !handle_me_as_candidate {
                // Promote an available candidate to leader if needed
                if current_leader.is_none() {
                    // No current leader, pick first available
                    let new_leader = available.first().cloned();
                    if new_leader.is_some() {
                        debug!("Setting new leader {:?} for fault tolerance entity {:?} (no current leader)", new_leader, ft_entity_id);
                        if let Err(e) = self.store.write(ft_entity_id, &[ft_current_leader], Value::EntityReference(new_leader), None, None, None, None) {
                            warn!("Failed to set new leader: {}", e);
                        }
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

                        // Only write if the leader actually changes
                        if current_leader != next_leader {
                            debug!("Updating leader from {:?} to {:?} for fault tolerance entity {:?} (current leader unavailable)", current_leader, next_leader, ft_entity_id);
                            if let Err(e) = self.store.write(ft_entity_id, &[ft_current_leader], Value::EntityReference(next_leader), None, None, None, None) {
                                warn!("Failed to update leader: {}", e);
                            }
                        }
                    }
                }
            }
        }
    }
    
}