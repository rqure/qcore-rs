use ahash::AHashMap;
use anyhow::Result;
use bytes::{Buf, BytesMut};
use crossbeam::channel::Sender;
use mio::net::{TcpListener as MioTcpListener, TcpStream as MioTcpStream};
use mio::{Events, Interest, Poll, Token, event::Event};
use rustc_hash::FxHashMap;
use serde_json;
use std::collections::{HashMap, HashSet};

use std::io::{Read, Write};
use std::os::unix::io::AsRawFd;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};

use qlib_rs::data::entity_schema::EntitySchemaResp;
use qlib_rs::data::resp::{
    BooleanResponse, CreateEntityCommand, CreateEntityResponse, DeleteEntityCommand,
    EntityExistsCommand, EntityListResponse, EntityTypeListResponse, FieldExistsCommand,
    FieldSchemaResponse, FindEntitiesCommand, FindEntitiesExactCommand,
    FindEntitiesPaginatedCommand, FullSyncRequestCommand, FullSyncResponseCommand,
    GetEntitySchemaCommand, GetEntityTypeCommand, GetEntityTypesCommand,
    GetEntityTypesPaginatedCommand, GetFieldSchemaCommand, GetFieldTypeCommand, IntegerResponse,
    NotificationCommand, OwnedRespValue, PaginatedEntityResponse, PaginatedEntityTypeResponse,
    PeerHandshakeCommand, ReadCommand, ReadResponse, RegisterNotificationCommand,
    ResolveEntityTypeCommand, ResolveFieldTypeCommand, ResolveIndirectionCommand,
    ResolveIndirectionResponse, RespCommand, RespDecode, RespEncode, RespParser, RespToBytes,
    SetFieldSchemaCommand, SnapshotResponse, StringResponse, SyncWriteCommand, TakeSnapshotCommand,
    UnregisterNotificationCommand, UpdateSchemaCommand, WriteCommand,
};
use qlib_rs::{
    EntityId, NotificationQueue, NotifyConfig, Snapshot, Store, StoreTrait, Value, WriteInfo,
};

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
        let peers = config
            .peer_addresses
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
    Replay {
        writes: Vec<WriteInfo>,
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
        self.sender
            .send(CoreCommand::RestoreSnapshot { snapshot })
            .unwrap();
    }

    pub fn set_snapshot_handle(&self, snapshot_handle: SnapshotHandle) {
        self.sender
            .send(CoreCommand::SetSnapshotHandle { snapshot_handle })
            .unwrap();
    }

    pub fn set_wal_handle(&self, wal_handle: crate::wal::WalHandle) {
        self.sender
            .send(CoreCommand::SetWalHandle { wal_handle })
            .unwrap();
    }

    pub fn peer_connected(&self, machine_id: String, stream: MioTcpStream) {
        self.sender
            .send(CoreCommand::PeerConnected { machine_id, stream })
            .unwrap();
    }

    pub fn get_peers(&self) -> AHashMap<String, (Option<Token>, Option<EntityId>)> {
        let (resp_sender, resp_receiver) = crossbeam::channel::bounded(1);
        self.sender
            .send(CoreCommand::GetPeers {
                respond_to: resp_sender,
            })
            .unwrap();
        resp_receiver.recv().unwrap_or_default()
    }

    pub fn replay(&self, writes: Vec<WriteInfo>) {
        self.sender.send(CoreCommand::Replay { writes }).unwrap();
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
    is_peer_connection: bool, // Track if this is a peer vs client connection
    notification_queue: NotificationQueue,
    notification_configs: HashSet<NotifyConfig>,
    outbound_buffer: BytesMut,
    read_buffer: BytesMut,
    static_read_buffer: [u8; 65_536],
    needs_write_interest: bool,
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
    pub fn new(config: CoreConfig) -> Result<Self> {
        let addr = format!("0.0.0.0:{}", config.port).parse()?;
        let mut listener = MioTcpListener::bind(addr)?;
        let poll = Poll::new()?;

        poll.registry()
            .register(&mut listener, LISTENER_TOKEN, Interest::READABLE)?;

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
                service
                    .poll
                    .poll(&mut events, Some(Duration::from_millis(10)))
                    .unwrap();

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

                if now.duration_since(service.last_fault_tolerance_tick)
                    >= Duration::from_millis(100)
                {
                    service.last_fault_tolerance_tick = now;
                    service.manage_fault_tolerance();
                }

                while let Ok(request) = receiver.try_recv() {
                    service.handle_command(request);
                }

                // Process pending notifications for all connections
                service.process_notifications();

                // Apply batched write interest updates (optimization: do once per loop instead of per send)
                service.apply_write_interest_updates();

                // Drain the write queue from the store (WriteInfo items for internal tracking)
                while let Some(write_info) = service.store.write_queue.pop_front() {
                    // Send to WAL
                    if let Some(wal_handle) = &service.wal_handle {
                        wal_handle.log_write(write_info);
                    }
                }
            }
        });

        handle
    }

    /// Peer connection thread that connects to peers with higher machine IDs
    fn peer_connection_thread(handle: CoreHandle, config: CoreConfig) {
        info!(
            "Starting peer connection thread for machine {}",
            config.machine
        );

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
                            debug!(
                                "No address found for peer {}, skipping connection attempt",
                                machine_id
                            );
                        }
                    } else {
                        debug!(
                            "Already connected to peer {}, skipping connection attempt",
                            machine_id
                        );
                    }
                }
            }

            debug!(
                "Will attempt to connect to {} peers: {:?}",
                target_peers.len(),
                target_peers
            );

            for (machine_id, address) in &target_peers {
                match std::net::TcpStream::connect(address) {
                    Ok(std_stream) => {
                        debug!(
                            "Successfully connected to peer {} at {}",
                            machine_id, address
                        );

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
                        debug!(
                            "Failed to connect to peer {} at {}: {}",
                            machine_id, address, e
                        );
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
                    if let Err(e) = Self::optimize_socket(&mut stream) {
                        warn!(
                            "Failed to optimize socket for connection from {}: {}",
                            addr, e
                        );
                        // Continue anyway, this is just an optimization
                    }

                    let token = Token(self.next_token);
                    self.next_token += 1;

                    // Register for read events
                    if let Err(e) =
                        self.poll
                            .registry()
                            .register(&mut stream, token, Interest::READABLE)
                    {
                        error!("Failed to register connection: {}", e);
                        continue;
                    }

                    let connection = Connection {
                        stream,
                        addr_string: addr.to_string(),
                        client_id: None,
                        is_peer_connection: false, // New connections start as client connections
                        notification_queue: NotificationQueue::new(),
                        notification_configs: HashSet::new(),
                        outbound_buffer: BytesMut::with_capacity(65536),
                        read_buffer: BytesMut::with_capacity(65536),
                        static_read_buffer: [0u8; 65536],
                        needs_write_interest: false,
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
                        error!(
                            "Error reading from connection {}: {}",
                            connection.addr_string, e
                        );
                    }
                    should_remove = true;
                }
            }

            if event.is_writable() {
                if let Err(e) = self.handle_connection_write(token) {
                    if let Some(connection) = self.connections.get(&token) {
                        error!(
                            "Error writing to connection {}: {}",
                            connection.addr_string, e
                        );
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
        // Perform a SINGLE read() call - Redis optimization
        let bytes_read = {
            let connection = self
                .connections
                .get_mut(&token)
                .ok_or_else(|| anyhow::anyhow!("Connection not found"))?;

            match connection.stream.read(&mut connection.static_read_buffer) {
                Ok(0) => {
                    // Connection closed by peer
                    return Err(anyhow::anyhow!("Connection closed by peer"));
                }
                Ok(n) => {
                    // Append new data to the read buffer
                    connection
                        .read_buffer
                        .extend_from_slice(&connection.static_read_buffer[0..n]);
                    n
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // No more data available right now
                    0
                }
                Err(e) => {
                    return Err(anyhow::anyhow!("Read error: {}", e));
                }
            }
        };

        // Process ALL complete commands from the buffer - Redis optimization
        if bytes_read > 0 {
            // Parse all complete commands from the buffer
            loop {
                // Check if we have data to process
                let buffer: &[u8] = {
                    if let Some(connection) = self.connections.get(&token) {
                        &connection.read_buffer
                    } else {
                        return Err(anyhow::anyhow!("Connection not found"));
                    }
                };

                if buffer.is_empty() {
                    break;
                }

                // First, parse the RESP value from the buffer
                let (resp_value, remaining_bytes) = match RespParser::parse_value(buffer) {
                    Ok((val, rem)) => (val, rem),
                    Err(_) => {
                        // Incomplete message, wait for more data
                        break;
                    }
                };

                let consumed = buffer.len() - remaining_bytes.len();
                if let Ok(command) = ReadCommand::decode(resp_value.clone()) {
                    match self.store.read(command.entity_id, &command.field_path) {
                        Ok((value, timestamp, writer_id)) => {
                            let response = ReadResponse {
                                value,
                                timestamp,
                                writer_id,
                            };
                            self.send_response(token, &response)?;
                        }
                        Err(e) => {
                            self.send_error(token, format!("Read error: {}", e))?;
                        }
                    }
                } else if let Ok(command) = WriteCommand::decode(resp_value.clone()) {
                    // Check if this is from a client (non-peer) connection
                    let is_client_write = self
                        .connections
                        .get(&token)
                        .map(|conn| !conn.is_peer_connection)
                        .unwrap_or(false);

                    // Save values needed for broadcasting before they're moved
                    let push_cond = command.push_condition.clone().unwrap_or(qlib_rs::PushCondition::Always);
                    let adjust_behav = command.adjust_behavior.clone().unwrap_or(qlib_rs::AdjustBehavior::Set);
                    let field_type = command.field_path[0]; // Assuming single field path for now
                    let value_clone = command.value.clone();

                    match self.store.write(
                        command.entity_id,
                        &command.field_path,
                        command.value.clone(),
                        command.writer_id,
                        command.write_time,
                        Some(push_cond.clone()),
                        Some(adjust_behav.clone()),
                    ) {
                        Ok(_) => {
                            // Broadcast to peers if this was a client write
                            if is_client_write {
                                let write_info = WriteInfo::FieldUpdate {
                                    entity_id: command.entity_id,
                                    field_type,
                                    value: Some(value_clone),
                                    push_condition: push_cond,
                                    adjust_behavior: adjust_behav,
                                    write_time: command.write_time,
                                    writer_id: command.writer_id,
                                };
                                self.broadcast_write_to_peers(write_info);
                            }
                            self.send_ok(token)?;
                        }
                        Err(e) => {
                            self.send_error(token, format!("Write error: {}", e))?;
                        }
                    }
                } else if let Ok(command) = CreateEntityCommand::decode(resp_value.clone()) {
                    // Check if this is from a client (non-peer) connection
                    let is_client_write = self
                        .connections
                        .get(&token)
                        .map(|conn| !conn.is_peer_connection)
                        .unwrap_or(false);

                    match self.store.create_entity(
                        command.entity_type,
                        command.parent_id,
                        &command.name,
                    ) {
                        Ok(entity_id) => {
                            // Broadcast to peers if this was a client write
                            if is_client_write {
                                let write_info = WriteInfo::CreateEntity {
                                    entity_type: command.entity_type,
                                    parent_id: command.parent_id,
                                    name: command.name.clone(),
                                    created_entity_id: entity_id,
                                    timestamp: qlib_rs::data::now(),
                                };
                                self.broadcast_write_to_peers(write_info);
                            }
                            let response = CreateEntityResponse { entity_id };
                            self.send_response(token, &response)?;
                        }
                        Err(e) => {
                            self.send_error(token, format!("Create entity error: {}", e))?;
                        }
                    }
                } else if let Ok(command) = DeleteEntityCommand::decode(resp_value.clone()) {
                    // Check if this is from a client (non-peer) connection
                    let is_client_write = self
                        .connections
                        .get(&token)
                        .map(|conn| !conn.is_peer_connection)
                        .unwrap_or(false);

                    match self.store.delete_entity(command.entity_id) {
                        Ok(_) => {
                            // Broadcast to peers if this was a client write
                            if is_client_write {
                                let write_info = WriteInfo::DeleteEntity {
                                    entity_id: command.entity_id,
                                    timestamp: qlib_rs::data::now(),
                                };
                                self.broadcast_write_to_peers(write_info);
                            }
                            self.send_ok(token)?;
                        }
                        Err(e) => {
                            self.send_error(token, format!("Delete entity error: {}", e))?;
                        }
                    }
                } else if let Ok(command) = GetEntityTypeCommand::decode(resp_value.clone()) {
                    match self.store.get_entity_type(&command.name) {
                        Ok(entity_type) => {
                            let response = IntegerResponse {
                                value: entity_type.0 as i64,
                            };
                            self.send_response(token, &response)?;
                        }
                        Err(e) => {
                            self.send_error(token, format!("Get entity type error: {}", e))?;
                        }
                    }
                } else if let Ok(command) = ResolveEntityTypeCommand::decode(resp_value.clone()) {
                    match self.store.resolve_entity_type(command.entity_type) {
                        Ok(name) => {
                            let response = StringResponse { value: name };
                            self.send_response(token, &response)?;
                        }
                        Err(e) => {
                            self.send_error(token, format!("Resolve entity type error: {}", e))?;
                        }
                    }
                } else if let Ok(command) = GetFieldTypeCommand::decode(resp_value.clone()) {
                    match self.store.get_field_type(&command.name) {
                        Ok(field_type) => {
                            let response = IntegerResponse {
                                value: field_type.0 as i64,
                            };
                            self.send_response(token, &response)?;
                        }
                        Err(e) => {
                            self.send_error(token, format!("Get field type error: {}", e))?;
                        }
                    }
                } else if let Ok(command) = ResolveFieldTypeCommand::decode(resp_value.clone()) {
                    match self.store.resolve_field_type(command.field_type) {
                        Ok(name) => {
                            let response = StringResponse { value: name };
                            self.send_response(token, &response)?;
                        }
                        Err(e) => {
                            self.send_error(token, format!("Resolve field type error: {}", e))?;
                        }
                    }
                } else if let Ok(command) = GetEntitySchemaCommand::decode(resp_value.clone()) {
                    match self.store.get_entity_schema(command.entity_type) {
                        Ok(schema) => {
                            let response =
                                EntitySchemaResp::from_entity_schema(&schema, &self.store);
                            self.send_response(token, &response)?;
                        }
                        Err(e) => {
                            self.send_error(token, format!("Get entity schema error: {}", e))?;
                        }
                    }
                } else if let Ok(command) = UpdateSchemaCommand::decode(resp_value.clone()) {
                    // Check if this is from a client (non-peer) connection
                    let is_client_write = self
                        .connections
                        .get(&token)
                        .map(|conn| !conn.is_peer_connection)
                        .unwrap_or(false);

                    match command.schema.to_entity_schema(&self.store) {
                        Ok(schema_string) => {
                            // Convert to FieldType-based schema for WriteInfo
                            let schema_for_sync = qlib_rs::EntitySchema::from_string_schema(schema_string.clone(), &self.store);
                            
                            match self.store.update_schema(schema_string) {
                                Ok(_) => {
                                    // Broadcast to peers if this was a client write
                                    if is_client_write {
                                        let write_info = WriteInfo::SchemaUpdate {
                                            schema: schema_for_sync,
                                            timestamp: qlib_rs::data::now(),
                                        };
                                        self.broadcast_write_to_peers(write_info);
                                    }
                                    self.send_ok(token)?;
                                }
                                Err(e) => {
                                    self.send_error(token, format!("Update schema error: {}", e))?;
                                }
                            }
                        },
                        Err(e) => {
                            self.send_error(token, format!("Schema conversion error: {}", e))?;
                        }
                    }
                } else if let Ok(command) = GetFieldSchemaCommand::decode(resp_value.clone()) {
                    match self
                        .store
                        .get_field_schema(command.entity_type, command.field_type)
                    {
                        Ok(schema) => {
                            let response = FieldSchemaResponse {
                                schema:
                                    qlib_rs::data::entity_schema::FieldSchemaResp::from_field_schema(
                                        &schema,
                                        &self.store,
                                    ),
                            };
                            self.send_response(token, &response)?;
                        }
                        Err(e) => {
                            self.send_error(token, format!("Get field schema error: {}", e))?;
                        }
                    }
                } else if let Ok(command) = SetFieldSchemaCommand::decode(resp_value.clone()) {
                    let field_schema = qlib_rs::FieldSchema::from_string_schema(
                        command.schema.to_field_schema(),
                        &self.store,
                    );
                    match self.store.set_field_schema(
                        command.entity_type,
                        command.field_type,
                        field_schema,
                    ) {
                        Ok(_) => {
                            self.send_ok(token)?;
                        }
                        Err(e) => {
                            self.send_error(token, format!("Set field schema error: {}", e))?;
                        }
                    }
                } else if let Ok(command) = FindEntitiesCommand::decode(resp_value.clone()) {
                    match self
                        .store
                        .find_entities(command.entity_type, command.filter.as_deref())
                    {
                        Ok(entities) => {
                            let response = EntityListResponse { entities };
                            self.send_response(token, &response)?;
                        }
                        Err(e) => {
                            self.send_error(token, format!("Find entities error: {}", e))?;
                        }
                    }
                } else if let Ok(command) = FindEntitiesExactCommand::decode(resp_value.clone()) {
                    match self.store.find_entities_exact(
                        command.entity_type,
                        None,
                        command.filter.as_deref(),
                    ) {
                        Ok(result) => {
                            let response = EntityListResponse {
                                entities: result.items,
                            };
                            self.send_response(token, &response)?;
                        }
                        Err(e) => {
                            self.send_error(token, format!("Find entities exact error: {}", e))?;
                        }
                    }
                } else if let Ok(command) = FindEntitiesPaginatedCommand::decode(resp_value.clone())
                {
                    match self.store.find_entities_paginated(
                        command.entity_type,
                        command.page_opts.as_ref(),
                        command.filter.as_deref(),
                    ) {
                        Ok(result) => {
                            let response = PaginatedEntityResponse {
                                items: result.items,
                                total: result.total,
                                next_cursor: result.next_cursor,
                            };
                            self.send_response(token, &response)?;
                        }
                        Err(e) => {
                            self.send_error(
                                token,
                                format!("Find entities paginated error: {}", e),
                            )?;
                        }
                    }
                } else if let Ok(_command) = GetEntityTypesCommand::decode(resp_value.clone()) {
                    match self.store.get_entity_types() {
                        Ok(entity_types) => {
                            let response = EntityTypeListResponse { entity_types };
                            self.send_response(token, &response)?;
                        }
                        Err(e) => {
                            self.send_error(token, format!("Get entity types error: {}", e))?;
                        }
                    }
                } else if let Ok(command) =
                    GetEntityTypesPaginatedCommand::decode(resp_value.clone())
                {
                    match self
                        .store
                        .get_entity_types_paginated(command.page_opts.as_ref())
                    {
                        Ok(result) => {
                            let response = PaginatedEntityTypeResponse {
                                items: result.items,
                                total: result.total,
                                next_cursor: result.next_cursor,
                            };
                            self.send_response(token, &response)?;
                        }
                        Err(e) => {
                            self.send_error(
                                token,
                                format!("Get entity types paginated error: {}", e),
                            )?;
                        }
                    }
                } else if let Ok(command) = EntityExistsCommand::decode(resp_value.clone()) {
                    let exists = self.store.entity_exists(command.entity_id);
                    let response = BooleanResponse { result: exists };
                    self.send_response(token, &response)?;
                } else if let Ok(command) = FieldExistsCommand::decode(resp_value.clone()) {
                    let exists = self
                        .store
                        .field_exists(command.entity_type, command.field_type);
                    let response = BooleanResponse { result: exists };
                    self.send_response(token, &response)?;
                } else if let Ok(command) = ResolveIndirectionCommand::decode(resp_value.clone()) {
                    match self
                        .store
                        .resolve_indirection(command.entity_id, &command.fields)
                    {
                        Ok((entity_id, field_type)) => {
                            let response = ResolveIndirectionResponse {
                                entity_id,
                                field_type,
                            };
                            self.send_response(token, &response)?;
                        }
                        Err(e) => {
                            self.send_error(token, format!("Resolve indirection error: {}", e))?;
                        }
                    }
                } else if let Ok(_command) = TakeSnapshotCommand::decode(resp_value.clone()) {
                    let snapshot = self.store.take_snapshot();
                    if let Some(snapshot_handle) = &self.snapshot_handle {
                        snapshot_handle.save(snapshot.clone());
                    }
                    let response = SnapshotResponse {
                        data: serde_json::to_string(&snapshot).unwrap_or_default(),
                    };
                    self.send_response(token, &response)?;
                } else if let Ok(command) = RegisterNotificationCommand::decode(resp_value.clone())
                {
                    // Parse notification config
                    let config = command.config;

                    // Get the connection's notification queue
                    if let Some(connection) = self.connections.get_mut(&token) {
                        connection.notification_configs.insert(config.clone());

                        match self.store.register_notification(
                            config.clone(),
                            connection.notification_queue.clone(),
                        ) {
                            Ok(_) => self.send_ok(token)?,
                            Err(e) => self
                                .send_error(token, format!("Register notification error: {}", e))?,
                        }
                    } else {
                        self.send_error(token, "Connection not found".to_string())?
                    }
                } else if let Ok(command) =
                    UnregisterNotificationCommand::decode(resp_value.clone())
                {
                    // Get the connection's notification queue
                    if let Some(connection) = self.connections.get_mut(&token) {
                        connection.notification_configs.remove(&command.config);
                        let removed = self.store.unregister_notification(
                            &command.config,
                            &connection.notification_queue,
                        );
                        let response = IntegerResponse {
                            value: if removed { 1 } else { 0 },
                        };
                        self.send_response(token, &response)?
                    } else {
                        self.send_error(token, "Connection not found".to_string())?
                    }
                } else if let Ok(command) = PeerHandshakeCommand::decode(resp_value.clone()) {
                    self.handle_peer_handshake(
                        token,
                        command.start_time,
                        command.is_response,
                        command.machine_id,
                    )?;
                    // No response needed for peer commands
                } else if let Ok(_) = FullSyncRequestCommand::decode(resp_value.clone()) {
                    self.handle_peer_full_sync_request(token)?;
                } else if let Ok(command) = FullSyncResponseCommand::decode(resp_value.clone()) {
                    self.handle_peer_full_sync_response(token, command.snapshot_data)?;
                } else if let Ok(command) = SyncWriteCommand::decode(resp_value.clone()) {
                    self.handle_peer_sync_write(token, command.requests_data)?;
                } else {
                    // No complete command could be parsed
                    break;
                }

                // Update the buffer to remove consumed data
                if consumed > 0 {
                    if let Some(connection) = self.connections.get_mut(&token) {
                        connection.read_buffer.advance(consumed);
                    }
                }
            }
        }

        Ok(())
    }

    /// Broadcast a write to all peer connections (called after processing client writes)
    fn broadcast_write_to_peers(&mut self, write_info: WriteInfo) {
        // Serialize the write_info to JSON
        let write_json = match serde_json::to_string(&write_info) {
            Ok(json) => json,
            Err(e) => {
                warn!("Failed to serialize write_info for peer sync: {}", e);
                return;
            }
        };

        let sync_write_cmd = SyncWriteCommand {
            requests_data: write_json,
            _marker: std::marker::PhantomData,
        };

        // Send to all peer connections
        let peer_tokens: Vec<_> = self
            .peers
            .values()
            .filter_map(|peer_info| peer_info.token)
            .collect();

        let mut tokens_to_remove = Vec::new();
        for token in peer_tokens {
            if let Err(e) = self.send_peer_command(token, &sync_write_cmd) {
                warn!("Failed to send SyncWrite to peer: {}", e);
                tokens_to_remove.push(token);
            }
        }

        // Remove failed peer connections
        for token in tokens_to_remove {
            self.remove_connection(token);
        }
    }

    /// Send a RESP command to a peer connection
    fn send_peer_command<T: RespCommand<'static>>(
        &mut self,
        token: Token,
        command: &T,
    ) -> Result<()> {
        let encoded = command.encode();
        let encoded_bytes = encoded.to_bytes();

        if let Some(connection) = self.connections.get_mut(&token) {
            connection.outbound_buffer.extend_from_slice(&encoded_bytes);
            connection.needs_write_interest = true;
        }

        Ok(())
    }

    /// Send a response message to a client connection
    fn send_response<T: RespEncode>(&mut self, token: Token, response: &T) -> Result<()> {
        let encoded = response.encode();
        let encoded_bytes = encoded.to_bytes();

        if let Some(connection) = self.connections.get_mut(&token) {
            connection.outbound_buffer.extend_from_slice(&encoded_bytes);
            connection.needs_write_interest = true;
        }

        Ok(())
    }

    /// Send an error response to a client connection
    fn send_error(&mut self, token: Token, error_msg: String) -> Result<()> {
        let error_response = OwnedRespValue::Error(error_msg);
        let encoded_bytes = error_response.to_bytes();

        if let Some(connection) = self.connections.get_mut(&token) {
            connection.outbound_buffer.extend_from_slice(&encoded_bytes);
            connection.needs_write_interest = true;
        }

        Ok(())
    }

    /// Send an OK response to a client connection
    fn send_ok(&mut self, token: Token) -> Result<()> {
        let ok_response = OwnedRespValue::SimpleString("OK".to_string());
        let encoded_bytes = ok_response.to_bytes();

        if let Some(connection) = self.connections.get_mut(&token) {
            connection.outbound_buffer.extend_from_slice(&encoded_bytes);
            connection.needs_write_interest = true;
        }

        Ok(())
    }

    /// Send a notification message to a client connection
    fn send_notification(
        &mut self,
        token: Token,
        notification: &qlib_rs::Notification,
    ) -> Result<()> {
        let notification_json = serde_json::to_string(notification)
            .map_err(|e| anyhow::anyhow!("Failed to serialize notification: {}", e))?;

        let command = NotificationCommand {
            notification_data: notification_json,
            _marker: std::marker::PhantomData,
        };

        self.send_response(token, &command)
    }

    /// Apply batched write interest updates to all connections that need it
    fn apply_write_interest_updates(&mut self) {
        // Process all connections that need write interest
        for (token, connection) in self.connections.iter_mut() {
            if connection.needs_write_interest && !connection.outbound_buffer.is_empty() {
                connection.needs_write_interest = false;
                if let Err(e) = self.poll.registry().reregister(
                    &mut connection.stream,
                    *token,
                    Interest::READABLE | Interest::WRITABLE,
                ) {
                    error!(
                        "Failed to reregister connection {:?} for writing: {}",
                        token, e
                    );
                }
            }
        }
    }

    /// Handle writing data to a connection
    fn handle_connection_write(&mut self, token: Token) -> Result<()> {
        let connection = self
            .connections
            .get_mut(&token)
            .ok_or_else(|| anyhow::anyhow!("Connection not found"))?;

        while !connection.outbound_buffer.is_empty() {
            match connection.stream.write(&connection.outbound_buffer) {
                Ok(n) => {
                    // Advance the buffer by the number of bytes written
                    connection.outbound_buffer.advance(n);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // Can't write more right now
                    break;
                }
                Err(e) => {
                    return Err(anyhow::anyhow!("Write error: {}", e));
                }
            }
        }

        // If no more data to write, stop watching for write events
        if connection.outbound_buffer.is_empty() {
            connection.needs_write_interest = false;
            self.poll
                .registry()
                .reregister(&mut connection.stream, token, Interest::READABLE)?;
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
                    Some(&format!(
                        "Name == 'qcore' && Parent->Name == '{}'",
                        self.config.machine
                    )),
                ) {
                    if let Some(candidate_id) = candidates.first() {
                        self.candidate_entity_id = Some(*candidate_id);
                        debug!("Resolved our candidate entity ID: {:?}", candidate_id);
                    } else {
                        warn!(
                            "No candidate entity found for our machine {}",
                            self.config.machine
                        );
                    }
                } else {
                    warn!(
                        "Failed to query candidate entity for our machine {}",
                        self.config.machine
                    );
                }

                // Resolve peer candidate entity IDs
                for (machine_id, peer_info) in self.peers.iter_mut() {
                    if let Ok(peer_candidates) = self.store.find_entities(
                        candidate_etype,
                        Some(&format!(
                            "Name == 'qcore' && Parent->Name == '{}'",
                            machine_id
                        )),
                    ) {
                        if let Some(candidate) = peer_candidates.first() {
                            peer_info.entity_id = Some(*candidate);
                            // Update connected peer connections
                            if let Some(token) = peer_info.token {
                                if let Some(connection) = self.connections.get_mut(&token) {
                                    connection.client_id = Some(*candidate);
                                }
                            }
                            debug!(
                                "Resolved candidate entity ID for peer {}: {:?}",
                                machine_id, candidate
                            );
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
            debug!(
                "Store schema (et) not yet available, candidate entity IDs will be resolved later"
            );
        }
    }

    /// Handle complete snapshot restoration including store, cache, and peer reinitialization
    fn handle_snapshot_restoration(&mut self, snapshot: Snapshot) {
        // Restore the snapshot to our store
        self.store.restore_snapshot(snapshot);

        // Disconnect all non-peer clients since snapshot restoration invalidates their state
        let mut non_peer_tokens = Vec::new();
        let peer_tokens: HashSet<Token> = self
            .peers
            .values()
            .filter_map(|peer_info| peer_info.token)
            .collect();

        for (&token, connection) in &self.connections {
            if !peer_tokens.contains(&token) {
                non_peer_tokens.push(token);
                info!(
                    "Disconnecting non-peer client {} due to snapshot restoration",
                    connection.addr_string
                );
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
    fn handle_peer_handshake(
        &mut self,
        token: Token,
        peer_start_time: u64,
        is_response: bool,
        peer_machine_id: String,
    ) -> Result<()> {
        debug!(
            "Received handshake from peer {} with start time {}",
            peer_machine_id, peer_start_time
        );

        // Update peer info with the token if this is a new connection
        if let Some(peer_info) = self.peers.get_mut(&peer_machine_id) {
            if peer_info.token.is_none() {
                peer_info.token = Some(token);
                debug!("Associated token {:?} with peer {}", token, peer_machine_id);

                // Update connection with peer entity ID if available and mark as peer connection
                if let Some(connection) = self.connections.get_mut(&token) {
                    connection.client_id = peer_info.entity_id;
                    connection.is_peer_connection = true;
                    debug!("Marked connection {:?} as peer connection for {}", token, peer_machine_id);
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
                error!(
                    "Failed to send handshake response to peer {}: {}",
                    peer_machine_id, e
                );
            } else {
                debug!(
                    "Sent handshake response to peer {} with our start time {}",
                    peer_machine_id, self.start_time
                );
            }
        } else {
            debug!(
                "Received handshake response from peer {}, no reply needed",
                peer_machine_id
            );
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

        debug!(
            "Evaluating leadership: our start_time={}, machine={}",
            self.start_time, self.config.machine
        );

        for (machine_id, peer_info) in &self.peers {
            if let Some(start_time) = peer_info.start_time {
                debug!("Peer {} has start_time={}", machine_id, start_time);
                let is_older = start_time < oldest_start_time
                    || (start_time == oldest_start_time && machine_id < &oldest_machine_id);

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

        debug!(
            "Leadership evaluation result: has_older_peer={}, is_leader={}",
            has_older_peer, self.is_leader
        );

        if was_leader != self.is_leader {
            if self.is_leader {
                info!(
                    "This service is now the leader (started at {}, machine: {})",
                    self.start_time, self.config.machine
                );
            } else {
                info!(
                    "This service is no longer the leader (older peer found: start_time={}, machine={})",
                    oldest_start_time, oldest_machine_id
                );
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
                let is_older = start_time < oldest_start_time
                    || (start_time == oldest_start_time && machine_id < &oldest_machine_id);

                if is_older {
                    oldest_start_time = start_time;
                    oldest_machine_id = machine_id.clone();
                    oldest_peer = Some((machine_id.clone(), start_time));
                }
            }
        }

        // If we found an older peer, sync from them
        if let Some((oldest_machine_id, oldest_time)) = oldest_peer {
            debug!(
                "Found older peer {} (started at {}), requesting full sync",
                oldest_machine_id, oldest_time
            );

            // Find the token for this peer
            if let Some(peer_info) = self.peers.get(&oldest_machine_id) {
                if let Some(peer_token) = peer_info.token {
                    let sync_request = FullSyncRequestCommand {
                        _marker: std::marker::PhantomData,
                    };

                    if let Err(e) = self.send_peer_command(peer_token, &sync_request) {
                        error!(
                            "Failed to send full sync request to older peer {}: {}",
                            oldest_machine_id, e
                        );
                    } else {
                        info!("Requested full sync from older peer {}", oldest_machine_id);
                    }
                } else {
                    warn!(
                        "Could not find connection token for older peer {}",
                        oldest_machine_id
                    );
                }
            } else {
                warn!(
                    "Could not find connection token for older peer {}",
                    oldest_machine_id
                );
            }
        } else {
            // We are the oldest peer, no need to sync
            debug!(
                "We are the oldest peer (started at {}, machine: {}), no sync needed",
                self.start_time, self.config.machine
            );
        }

        // Always evaluate leadership after checking sync needs
        self.evaluate_leadership();
    }

    /// Handle peer full sync request
    fn handle_peer_full_sync_request(&mut self, token: Token) -> Result<()> {
        // Find the machine ID for this peer connection
        let requesting_machine_id = self
            .peers
            .iter()
            .find(|(_, peer_info)| peer_info.token.map_or(false, |t| t == token))
            .map(|(machine_id, _)| machine_id.clone())
            .unwrap_or_else(|| "unknown".to_string());

        debug!(
            "Received full sync request from peer {}",
            requesting_machine_id
        );

        // Take a snapshot of our current store
        let snapshot = self.store.take_snapshot();
        let snapshot_json = serde_json::to_string(&snapshot)
            .map_err(|e| anyhow::anyhow!("Failed to serialize snapshot: {}", e))?;

        let sync_response = FullSyncResponseCommand {
            snapshot_data: snapshot_json,
            _marker: std::marker::PhantomData,
        };

        if let Err(e) = self.send_peer_command(token, &sync_response) {
            error!(
                "Failed to send full sync response to peer {}: {}",
                requesting_machine_id, e
            );
        } else {
            info!("Sent full sync response to peer {}", requesting_machine_id);
            // Apply write interest immediately to ensure response is sent
            self.apply_write_interest_updates();
        }

        Ok(())
    }

    /// Handle peer full sync response
    fn handle_peer_full_sync_response(
        &mut self,
        _token: Token,
        snapshot_json: String,
    ) -> Result<()> {
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
    fn handle_peer_sync_write(&mut self, token: Token, requests_json: String) -> Result<()> {
        // Find the machine ID for this peer connection
        let peer_machine_id = self
            .peers
            .iter()
            .find(|(_, peer_info)| peer_info.token.map_or(false, |t| t == token))
            .map(|(machine_id, _)| machine_id.clone())
            .unwrap_or_else(|| "unknown".to_string());

        debug!("Received sync write from peer {}", peer_machine_id);

        // Deserialize requests from JSON
        let write_info: WriteInfo = serde_json::from_str(&requests_json)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize requests: {}", e))?;

        debug!("Applying sync write from peer {}", peer_machine_id);

        // Apply the write to our store
        match &write_info {
            WriteInfo::CreateEntity {
                entity_type,
                parent_id,
                name,
                created_entity_id,
                ..
            } => {
                self
                    .store
                    .create_entity_with_id(
                        *entity_type,
                        *parent_id,
                        &mut Some(*created_entity_id),
                        name.as_str(),
                    )
                    .map_err(|e| {
                        anyhow::anyhow!("Failed to apply CreateEntity from peer: {}", e)
                    })?;
            }
            WriteInfo::DeleteEntity { entity_id, .. } => {
                self.store.delete_entity(*entity_id).map_err(|e| {
                    anyhow::anyhow!("Failed to apply DeleteEntity from peer: {}", e)
                })?;
            }
            WriteInfo::FieldUpdate {
                entity_id,
                field_type,
                value,
                push_condition,
                adjust_behavior,
                write_time,
                writer_id,
            } => {
                if let Some(value) = value {
                    self.store
                        .write(
                            *entity_id,
                            &[*field_type],
                            value.clone(),
                            *writer_id,
                            *write_time,
                            Some(push_condition.clone()),
                            Some(adjust_behavior.clone()),
                        )
                        .map_err(|e| {
                            anyhow::anyhow!("Failed to apply FieldUpdate from peer: {}", e)
                        })?;
                }
            }
            WriteInfo::SchemaUpdate { schema, .. } => {
                let string_schema = schema.to_string_schema(&self.store);
                self.store.update_schema(string_schema).map_err(|e| {
                    anyhow::anyhow!("Failed to apply SchemaUpdate from peer: {}", e)
                })?;
            }
            WriteInfo::Snapshot { .. } => {
                // Ignore snapshot writes in sync
            }
        }

        Ok(())
    }

    /// Remove a connection and clean up resources
    fn remove_connection(&mut self, token: Token) {
        if let Some(connection) = self.connections.remove(&token) {
            info!("Removing connection {}", connection.addr_string);

            // Clean up notifications
            for config in &connection.notification_configs {
                self.store
                    .unregister_notification(config, &connection.notification_queue);
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
            CoreCommand::PeerConnected {
                machine_id,
                mut stream,
            } => {
                debug!("Adding peer connection for machine {}", machine_id);

                // Check if peer is already connected
                if let Some(peer_info) = self.peers.get(&machine_id) {
                    if peer_info.token.is_some() {
                        warn!(
                            "Peer {} is already connected, ignoring new connection",
                            machine_id
                        );
                        return;
                    }
                }

                // Optimize TCP socket for low latency
                if let Err(e) = Self::optimize_socket(&mut stream) {
                    warn!(
                        "Failed to optimize socket for peer connection from {}: {}",
                        machine_id, e
                    );
                    // Continue anyway, this is just an optimization
                }

                let token = Token(self.next_token);
                self.next_token += 1;

                // Register for read events
                if let Err(e) =
                    self.poll
                        .registry()
                        .register(&mut stream, token, Interest::READABLE)
                {
                    error!("Failed to register peer connection: {}", e);
                    return;
                }

                let addr = stream
                    .peer_addr()
                    .map(|addr| addr.to_string())
                    .expect("Failed to get peer address");
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
                    is_peer_connection: true, // Mark as peer connection
                    notification_queue: NotificationQueue::new(),
                    notification_configs: HashSet::new(),
                    outbound_buffer: BytesMut::with_capacity(65536),
                    read_buffer: BytesMut::with_capacity(65536),
                    static_read_buffer: [0u8; 65536],
                    needs_write_interest: false,
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
                    debug!(
                        "Sent handshake to peer {} with our start time {}",
                        machine_id, self.start_time
                    );
                }
            }
            CoreCommand::GetPeers { respond_to } => {
                // Convert PeerInfo to the expected tuple format
                let peers_response: AHashMap<String, (Option<Token>, Option<EntityId>)> = self
                    .peers
                    .iter()
                    .map(|(machine_id, peer_info)| {
                        (machine_id.clone(), (peer_info.token, peer_info.entity_id))
                    })
                    .collect();

                if let Err(e) = respond_to.send(peers_response) {
                    error!("Failed to send peers response: {}", e);
                }
            }
            CoreCommand::Replay { writes } => {
                debug!("Replaying {} WAL writes", writes.len());
                for write_info in writes {
                    match write_info {
                        WriteInfo::CreateEntity {
                            entity_type,
                            parent_id,
                            name,
                            created_entity_id: _,
                            timestamp: _,
                        } => {
                            if let Err(e) = self.store.create_entity(entity_type, parent_id, &name)
                            {
                                warn!("Failed to replay CreateEntity for {}: {}", name, e);
                            }
                        }
                        WriteInfo::DeleteEntity {
                            entity_id,
                            timestamp: _,
                        } => {
                            if let Err(e) = self.store.delete_entity(entity_id) {
                                warn!("Failed to replay DeleteEntity for {:?}: {}", entity_id, e);
                            }
                        }
                        WriteInfo::FieldUpdate {
                            entity_id,
                            field_type,
                            value,
                            push_condition,
                            adjust_behavior,
                            write_time,
                            writer_id,
                        } => {
                            if let Some(value) = value {
                                if let Err(e) = self.store.write(
                                    entity_id,
                                    &[field_type],
                                    value,
                                    writer_id,
                                    write_time,
                                    Some(push_condition),
                                    Some(adjust_behavior),
                                ) {
                                    warn!(
                                        "Failed to replay FieldUpdate for {:?}: {}",
                                        entity_id, e
                                    );
                                }
                            }
                        }
                        WriteInfo::SchemaUpdate {
                            schema,
                            timestamp: _,
                        } => {
                            let string_schema = schema.to_string_schema(&self.store);
                            if let Err(e) = self.store.update_schema(string_schema) {
                                warn!("Failed to replay SchemaUpdate: {}", e);
                            }
                        }
                        WriteInfo::Snapshot {
                            snapshot_counter,
                            timestamp: _,
                        } => {
                            warn!(
                                "Skipping replay of Snapshot write (counter {})",
                                snapshot_counter
                            );
                        }
                    }
                }
                info!("WAL replay completed");
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
                Some(&format!("Name == 'qcore' && Parent->Name == '{}'", machine)),
            );

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
            if let Err(e) = self.store.write(
                *candidate,
                &[ft_heartbeat],
                Value::Choice(0),
                None,
                None,
                None,
                None,
            ) {
                warn!("Failed to write heartbeat field: {}", e);
            }
            if let Err(e) = self.store.write(
                *candidate,
                &[ft_make_me],
                Value::Choice(1),
                None,
                None,
                Some(qlib_rs::PushCondition::Changes),
                None,
            ) {
                warn!("Failed to write make_me field: {}", e);
            }
        }
    }

    fn manage_fault_tolerance(&mut self) {
        if !self.is_leader {
            return;
        }

        // Get required entity and field types
        let (
            et_candidate,
            et_fault_tolerance,
            ft_candidate_list,
            ft_available_list,
            ft_current_leader,
            ft_make_me,
            ft_heartbeat,
            ft_death_detection_timeout,
        ) = {
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

            (
                et_candidate,
                et_fault_tolerance,
                ft_candidate_list,
                ft_available_list,
                ft_current_leader,
                ft_make_me,
                ft_heartbeat,
                ft_death_detection_timeout,
            )
        };

        // Find us as a candidate
        let me_as_candidate = {
            let machine = &self.config.machine;
            let candidates_result = self.store.find_entities(
                et_candidate,
                Some(&format!("Name == 'qcore' && Parent->Name == '{}'", machine)),
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
                let timeout_result = self
                    .store
                    .read(*candidate_id, &[ft_death_detection_timeout]);

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
                let death_detection_timeout =
                    time::Duration::milliseconds(death_detection_timeout_millis);
                if make_me == 1 && heartbeat_time + death_detection_timeout > now {
                    available.push(*candidate_id);
                }
            }

            // Update available list only if it has changed
            if current_available != available {
                debug!(
                    "Updating available list for fault tolerance entity {:?}: {:?}",
                    ft_entity_id, available
                );
                if let Err(e) = self.store.write(
                    ft_entity_id,
                    &[ft_available_list],
                    Value::EntityList(available.clone()),
                    None,
                    None,
                    None,
                    None,
                ) {
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
                        debug!(
                            "Setting current leader to {:?} for fault tolerance entity {:?}",
                            me_as_candidate, ft_entity_id
                        );
                        if let Err(e) = self.store.write(
                            ft_entity_id,
                            &[ft_current_leader],
                            Value::EntityReference(Some(*me_as_candidate)),
                            None,
                            None,
                            None,
                            None,
                        ) {
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
                        debug!(
                            "Setting new leader {:?} for fault tolerance entity {:?} (no current leader)",
                            new_leader, ft_entity_id
                        );
                        if let Err(e) = self.store.write(
                            ft_entity_id,
                            &[ft_current_leader],
                            Value::EntityReference(new_leader),
                            None,
                            None,
                            None,
                            None,
                        ) {
                            warn!("Failed to set new leader: {}", e);
                        }
                    }
                } else if let Some(current_leader_id) = current_leader {
                    if !available.contains(&current_leader_id) {
                        // Current leader is not available, find next one
                        let next_leader = if let Some(current_idx) =
                            candidates.iter().position(|c| *c == current_leader_id)
                        {
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
                            debug!(
                                "Updating leader from {:?} to {:?} for fault tolerance entity {:?} (current leader unavailable)",
                                current_leader, next_leader, ft_entity_id
                            );
                            if let Err(e) = self.store.write(
                                ft_entity_id,
                                &[ft_current_leader],
                                Value::EntityReference(next_leader),
                                None,
                                None,
                                None,
                                None,
                            ) {
                                warn!("Failed to update leader: {}", e);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Optimize TCP socket for low latency
    fn optimize_socket(stream: &mut MioTcpStream) -> Result<()> {
        // Set TCP_NODELAY to disable Nagle's algorithm for lower latency
        stream.set_nodelay(true)?;

        // Set send/receive buffer sizes for better throughput
        // Using unsafe to call libc functions directly for fine-grained control
        let socket = stream.as_raw_fd();
        unsafe {
            let buf_size: libc::c_int = 65536;

            // Set receive buffer size
            let ret = libc::setsockopt(
                socket,
                libc::SOL_SOCKET,
                libc::SO_RCVBUF,
                &buf_size as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
            if ret != 0 {
                return Err(anyhow::anyhow!("Failed to set SO_RCVBUF"));
            }

            // Set send buffer size
            let ret = libc::setsockopt(
                socket,
                libc::SOL_SOCKET,
                libc::SO_SNDBUF,
                &buf_size as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
            if ret != 0 {
                return Err(anyhow::anyhow!("Failed to set SO_SNDBUF"));
            }
        }

        Ok(())
    }
}
