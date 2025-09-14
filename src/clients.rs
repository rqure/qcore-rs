use std::net::SocketAddr;
use std::collections::{HashMap, HashSet, VecDeque};
use mio::{Poll, Interest, Token, Events};
use mio::net::{TcpListener as MioTcpListener, TcpStream as MioTcpStream};
use tungstenite::{WebSocket, Message};
use tungstenite::handshake::HandshakeRole;
use tracing::{info, warn, error, debug};
use anyhow::Result;
use qlib_rs::{
    StoreMessage, EntityId, NotificationQueue, NotifyConfig,
    AuthenticationResult, Notification
};

use crate::Services;

/// Configuration for the client service
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Port for client communication (StoreProxy clients)
    pub client_port: u16,
    /// Machine ID for request origination
    pub machine_id: String,
}

impl From<&crate::Config> for ClientConfig {
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

/// Non-async client service that manages WebSocket connections using mio
pub struct ClientService {
    config: ClientConfig,
    listener: MioTcpListener,
    poll: Poll,
    connections: HashMap<Token, ClientConnection>,
    next_token: usize,
    services: Option<Services>,
}

const LISTENER_TOKEN: Token = Token(0);

/// Handle for the client service (placeholder for compatibility)
#[derive(Clone)]
pub struct ClientHandle {
    // For now, this is just a placeholder. In a full implementation,
    // you'd need to communicate with the running service
}

impl ClientHandle {
    pub fn set_services(&self, _services: Services) {
        // Placeholder - in the actual implementation this would communicate
        // with the service to set dependencies
    }
}

impl ClientService {
    /// Create a new client service
    pub fn new(config: ClientConfig) -> Result<Self> {
        let addr = format!("0.0.0.0:{}", config.client_port).parse()?;
        let mut listener = MioTcpListener::bind(addr)?;
        let poll = Poll::new()?;
        
        poll.registry().register(&mut listener, LISTENER_TOKEN, Interest::READABLE)?;
        
        info!(bind_address = %addr, "Client WebSocket server initialized");
        
        Ok(Self {
            config,
            listener,
            poll,
            connections: HashMap::new(),
            next_token: 1,
            services: None,
        })
    }
    
    /// Compatibility method for the old spawn interface
    pub fn spawn(config: ClientConfig) -> ClientHandle {
        // For now, just return a placeholder handle
        // In the actual implementation, you'd start the service in a thread
        // and return a handle to communicate with it
        ClientHandle {}
    }
    
    /// Set the services for dependency injection
    pub fn set_services(&mut self, services: Services) {
        self.services = Some(services);
    }
    
    /// Run the main event loop
    pub fn run(&mut self) -> Result<()> {
        let mut events = Events::with_capacity(1024);
        
        loop {
            self.poll.poll(&mut events, None)?;
            
            for event in events.iter() {
                match event.token() {
                    LISTENER_TOKEN => {
                        self.handle_new_connection()?;
                    }
                    token => {
                        self.handle_client_event(token, event.is_readable(), event.is_writable())?;
                    }
                }
            }
            
            // Process notifications and send them to clients
            self.process_notifications()?;
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
                                let error_msg = StoreMessage::Error {
                                    id: uuid::Uuid::new_v4().to_string(),
                                    error: format!("Failed to parse message: {}", e),
                                };
                                if let Ok(error_text) = serde_json::to_string(&error_msg) {
                                    connection.outbound_messages.push_back(error_text);
                                }
                            }
                        }
                    }
                    Ok(Message::Close(_)) => {
                        info!(client_addr = %connection.addr_string, "Client closed connection gracefully");
                        return Ok(false);
                    }
                    Ok(Message::Ping(payload)) => {
                        debug!(client_addr = %connection.addr_string, "Received ping from client");
                        let pong_msg = Message::Pong(payload);
                        if let Ok(pong_text) = serde_json::to_string(&pong_msg) {
                            connection.outbound_messages.push_back(pong_text);
                        }
                    }
                    Ok(_) => {
                        // Handle other message types (binary, pong, etc.)
                        debug!(client_addr = %connection.addr_string, "Received other message type from client");
                    }
                    Err(tungstenite::Error::Io(ref e)) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        break;
                    }
                    Err(e) => {
                        error!(
                            client_addr = %connection.addr_string,
                            error = %e,
                            "WebSocket error with client"
                        );
                        return Ok(false);
                    }
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
                match connection.websocket.send(Message::Text(message_text.clone())) {
                    Ok(()) => {}
                    Err(tungstenite::Error::Io(ref e)) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        // Put the message back and try again later
                        connection.outbound_messages.push_front(message_text);
                        break;
                    }
                    Err(e) => {
                        error!(
                            client_addr = %connection.addr_string,
                            error = %e,
                            "Failed to send message to client"
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
            info!(client_addr = %connection.addr_string, "Removing client connection");
            
            // Unregister all notifications for this client
            if let Some(services) = &self.services {
                for config in &connection.notification_configs {
                    let removed = services.store_handle.unregister_notification(config, &connection.notification_queue);
                    if removed {
                        debug!(
                            client_addr = %connection.addr_string,
                            config = ?config,
                            "Cleaned up notification config for disconnected client"
                        );
                    }
                }
            }
        }
    }
    
    fn process_notifications(&mut self) -> Result<()> {
        // Check each client for new notifications and queue them for sending
        for connection in self.connections.values_mut() {
            while let Some(notification) = connection.notification_queue.pop() {
                let notification_msg = StoreMessage::Notification { notification };
                if let Ok(notification_text) = serde_json::to_string(&notification_msg) {
                    connection.outbound_messages.push_back(notification_text);
                }
            }
        }
        Ok(())
    }
    
    fn process_store_message(&mut self, message: StoreMessage, token: Token) -> Result<StoreMessage> {
        // Get the services first
        let services = match &self.services {
            Some(services) => services,
            None => {
                return Ok(StoreMessage::Error {
                    id: "unknown".to_string(),
                    error: "Services not available".to_string(),
                });
            }
        };

        // Get connection info
        let (addr_string, authenticated, client_id) = {
            if let Some(connection) = self.connections.get(&token) {
                (connection.addr_string.clone(), connection.authenticated, connection.client_id.clone())
            } else {
                return Ok(StoreMessage::Error {
                    id: "unknown".to_string(),
                    error: "Connection not found".to_string(),
                });
            }
        };

        match message {
            StoreMessage::Authenticate { id, subject_name, credential } => {
                // Perform authentication via the store service
                match services.store_handle.authenticate_subject(&subject_name, &credential) {
                    Ok(subject_id) => {
                        info!(
                            subject_name = %subject_name,
                            subject_id = %subject_id,
                            client_addr = %addr_string,
                            "Client authenticated successfully"
                        );
                        
                        // Update connection state
                        if let Some(connection) = self.connections.get_mut(&token) {
                            connection.authenticated = true;
                            connection.client_id = Some(subject_id.clone());
                        }
                        
                        let auth_result = AuthenticationResult {
                            subject_id: subject_id.clone(),
                            subject_type: subject_id.get_type().to_string(),
                        };
                        
                        Ok(StoreMessage::AuthenticateResponse {
                            id,
                            response: Ok(auth_result),
                        })
                    }
                    Err(e) => {
                        Ok(StoreMessage::AuthenticateResponse {
                            id,
                            response: Err(format!("Authentication failed: {:?}", e)),
                        })
                    }
                }
            }
            
            StoreMessage::AuthenticateResponse { .. } => {
                // This should not be sent by clients, only by server
                Ok(StoreMessage::Error {
                    id: "unknown".to_string(),
                    error: "Invalid message type".to_string(),
                })
            }
            
            // All other messages require authentication
            _ => {
                if !authenticated || client_id.is_none() {
                    return Ok(StoreMessage::Error {
                        id: "unknown".to_string(),
                        error: "Authentication required".to_string(),
                    });
                }
                let client_id = client_id.unwrap();
                
                match message {
                    StoreMessage::Authenticate { .. } |
                    StoreMessage::AuthenticateResponse { .. } => {
                        // These are handled in the outer match, should not reach here
                        Ok(StoreMessage::Error {
                            id: "unknown".to_string(),
                            error: "Authentication messages should not reach this point".to_string(),
                        })
                    }
            
                    StoreMessage::GetEntitySchema { id, entity_type } => {
                        match services.store_handle.get_entity_schema(&entity_type) {
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
                        match services.store_handle.get_complete_entity_schema(&entity_type) {
                            Ok(schema) => Ok(StoreMessage::GetCompleteEntitySchemaResponse {
                                id,
                                response: Ok(schema),
                            }),
                            Err(e) => Ok(StoreMessage::GetCompleteEntitySchemaResponse {
                                id,
                                response: Err(format!("{:?}", e)),
                            }),
                        }
                    }
                    
                    StoreMessage::GetFieldSchema { id, entity_type, field_type } => {
                        match services.store_handle.get_field_schema(&entity_type, &field_type) {
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
                        let exists = services.store_handle.entity_exists(&entity_id);
                        Ok(StoreMessage::EntityExistsResponse {
                            id,
                            response: exists,
                        })
                    }
                    
                    StoreMessage::FieldExists { id, entity_type, field_type } => {
                        let exists = services.store_handle.field_exists(&entity_type, &field_type);
                        Ok(StoreMessage::FieldExistsResponse {
                            id,
                            response: exists,
                        })
                    }
                    
                    StoreMessage::Perform { id, mut requests } => {
                        // Check authorization for requests
                        match services.store_handle.check_requests_authorization(&client_id, requests.clone()) {
                            Ok(authorized_requests) => {
                                if authorized_requests.len() != requests.len() {
                                    Ok(StoreMessage::PerformResponse {
                                        id,
                                        response: Err("Some requests were not authorized".to_string()),
                                    })
                                } else {
                                    // Set originator and writer_id for the requests
                                    requests.iter_mut().for_each(|req| {
                                        req.try_set_originator(self.config.machine_id.clone());
                                        req.try_set_writer_id(client_id.clone());
                                    });

                                    match services.store_handle.perform_mut(requests) {
                                        Ok(response) => Ok(StoreMessage::PerformResponse {
                                            id,
                                            response: Ok(response),
                                        }),
                                        Err(e) => Ok(StoreMessage::PerformResponse {
                                            id,
                                            response: Err(format!("{:?}", e)),
                                        }),
                                    }
                                }
                            }
                            Err(e) => Ok(StoreMessage::PerformResponse {
                                id,
                                response: Err(format!("Authorization check failed: {:?}", e)),
                            }),
                        }
                    }
                    
                    StoreMessage::FindEntities { id, entity_type, page_opts, filter } => {
                        match services.store_handle.find_entities_paginated(&entity_type, page_opts, filter) {
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
                        match services.store_handle.find_entities_exact(&entity_type, page_opts, filter) {
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
                        match services.store_handle.get_entity_types_paginated(page_opts) {
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
                        let notification_queue = if let Some(connection) = self.connections.get(&token) {
                            connection.notification_queue.clone()
                        } else {
                            return Ok(StoreMessage::RegisterNotificationResponse {
                                id,
                                response: Err("Connection not found".to_string()),
                            });
                        };
                        
                        match services.store_handle.register_notification(config.clone(), notification_queue) {
                            Ok(()) => {
                                if let Some(connection) = self.connections.get_mut(&token) {
                                    connection.notification_configs.insert(config.clone());
                                }
                                
                                debug!(
                                    client_addr = %addr_string,
                                    config = ?config,
                                    "Registered notification for client"
                                );
                                Ok(StoreMessage::RegisterNotificationResponse {
                                    id,
                                    response: Ok(()),
                                })
                            }
                            Err(e) => {
                                error!(
                                    client_addr = %addr_string,
                                    error = ?e,
                                    "Failed to register notification for client"
                                );
                                Ok(StoreMessage::RegisterNotificationResponse {
                                    id,
                                    response: Err(format!("Failed to register notification: {:?}", e)),
                                })
                            }
                        }
                    }
                    
                    StoreMessage::UnregisterNotification { id, config } => {
                        let (notification_queue, result) = if let Some(connection) = self.connections.get(&token) {
                            let queue = connection.notification_queue.clone();
                            let removed = services.store_handle.unregister_notification(&config, &queue);
                            (Some(queue), removed)
                        } else {
                            (None, false)
                        };
                        
                        // Remove from client's tracked configs if successfully unregistered
                        if result {
                            if let Some(connection) = self.connections.get_mut(&token) {
                                connection.notification_configs.remove(&config);
                            }
                        }
                        
                        debug!(
                            client_addr = %addr_string,
                            config = ?config,
                            removed = result,
                            "Unregistered notification for client"
                        );
                        Ok(StoreMessage::UnregisterNotificationResponse {
                            id,
                            response: result,
                        })
                    }
                    
                    // These message types should not be received by the server
                    StoreMessage::GetEntitySchemaResponse { id, .. } |
                    StoreMessage::GetCompleteEntitySchemaResponse { id, .. } |
                    StoreMessage::GetFieldSchemaResponse { id, .. } |
                    StoreMessage::EntityExistsResponse { id, .. } |
                    StoreMessage::FieldExistsResponse { id, .. } |
                    StoreMessage::PerformResponse { id, .. } |
                    StoreMessage::FindEntitiesResponse { id, .. } |
                    StoreMessage::FindEntitiesExactResponse { id, .. } |
                    StoreMessage::GetEntityTypesResponse { id, .. } |
                    StoreMessage::RegisterNotificationResponse { id, .. } |
                    StoreMessage::UnregisterNotificationResponse { id, .. } => {
                        Ok(StoreMessage::Error {
                            id,
                            error: "Received response message on server - this should not happen".to_string(),
                        })
                    }
                    
                    StoreMessage::Notification { .. } => {
                        Ok(StoreMessage::Error {
                            id: uuid::Uuid::new_v4().to_string(),
                            error: "Received notification message on server - this should not happen".to_string(),
                        })
                    }
                    
                    StoreMessage::Error { id, error } => {
                        warn!(
                            message_id = %id,
                            error_message = %error,
                            "Received error message from client"
                        );
                        Ok(StoreMessage::Error {
                            id: uuid::Uuid::new_v4().to_string(),
                            error: "Server received error message from client".to_string(),
                        })
                    }
                }
            }
        }
    }
}