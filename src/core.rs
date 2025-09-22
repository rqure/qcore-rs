use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{Read, Write};
use std::time::{Duration};
use crossbeam::channel::Sender;
use mio::{Poll, Interest, Token, Events, event::Event};
use mio::net::{TcpListener as MioTcpListener, TcpStream as MioTcpStream};
use qlib_rs::{Requests};
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
    #[allow(dead_code)]
    pub machine: String,
}

impl From<&crate::Config> for CoreConfig {
    fn from(config: &crate::Config) -> Self {
        Self {
            port: config.port,
            machine: config.machine.clone(),
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
    #[allow(dead_code)]
    config: CoreConfig,
    listener: MioTcpListener,
    poll: Poll,
    connections: HashMap<Token, Connection>,
    next_token: usize,
    
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
        
        let mut service = Self {
            config,
            listener,
            poll,
            connections: HashMap::new(),
            next_token: 1,
            store,
            permission_cache: None,
            cel_executor,
            snapshot_handle: None,
            wal_handle: None,
        };
        
        // Attempt to create permission cache
        service.permission_cache = service.create_permission_cache();
        
        Ok(service)
    }

    /// Spawn the core service in its own thread and return a handle
    pub fn spawn(config: CoreConfig) -> CoreHandle {
        let (sender, receiver) = crossbeam::channel::unbounded();
        let handle = CoreHandle { sender };

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
    
    /// Handle a decoded protocol message
    fn handle_protocol_message(&mut self, token: Token, message: ProtocolMessage) -> Result<()> {
        match message {
            ProtocolMessage::Store(store_message) => {
                self.handle_store_message(token, store_message)
            }
            _ => {
                warn!("Received non-store protocol message from client");
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
        }
    }
    
    /// Handle commands from other actors
    fn handle_command(&mut self, command: CoreCommand) {
        match command {
            CoreCommand::Perform { requests } => {
                debug!("Handling perform command with {} requests", requests.len());
                if let Err(e) = self.store.perform(requests) {
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
                self.store.restore_snapshot(snapshot);
                
                // Recreate the permission cache after restoring the snapshot
                self.permission_cache = self.create_permission_cache();
                
                if self.permission_cache.is_some() {
                    debug!("Permission cache recreated after snapshot restore");
                } else {
                    warn!("Failed to recreate permission cache after snapshot restore");
                }
            }
            CoreCommand::SetSnapshotHandle { snapshot_handle } => {
                debug!("Setting snapshot handle");
                self.snapshot_handle = Some(snapshot_handle);
            }
            CoreCommand::SetWalHandle { wal_handle } => {
                debug!("Setting WAL handle");
                self.wal_handle = Some(wal_handle);
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
    
}