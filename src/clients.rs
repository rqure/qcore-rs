use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn, error, debug, instrument};
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use qlib_rs::{notification_channel, StoreMessage, EntityId, NotificationSender, NotifyConfig};

use crate::Services;

/// Configuration for the client service
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Port for client communication (StoreProxy clients)
    pub client_port: u16,
}

impl From<&crate::Config> for ClientConfig {
    fn from(config: &crate::Config) -> Self {
        Self {
            client_port: config.client_port,
        }
    }
}

/// Client service request types
#[derive(Debug)]
pub enum ClientRequest {
    ClientConnected {
        client_addr: String,
        sender: mpsc::UnboundedSender<Message>,
        notification_sender: NotificationSender,
    },
    ClientDisconnected {
        client_addr: String,
    },
    ClientAuthenticated {
        client_addr: String,
        client_id: EntityId,
    },
    ProcessStoreMessage {
        message: StoreMessage,
        client_addr: Option<String>,
        response: oneshot::Sender<StoreMessage>,
    },
    RegisterNotification {
        client_addr: String,
        config: NotifyConfig,
        response: oneshot::Sender<Result<()>>,
    },
    UnregisterNotification {
        client_addr: String,
        config: NotifyConfig,
        response: oneshot::Sender<bool>,
    },
    ForceDisconnectAll {
        response: oneshot::Sender<()>,
    },
    SetServices {
        services: Services,
        response: oneshot::Sender<()>,
    },
}

/// Handle for communicating with client service
#[derive(Debug, Clone)]
pub struct ClientHandle {
    sender: mpsc::UnboundedSender<ClientRequest>,
}

impl ClientHandle {
    pub fn client_connected(&self, client_addr: String, sender: mpsc::UnboundedSender<Message>, notification_sender: NotificationSender) {
        let _ = self.sender.send(ClientRequest::ClientConnected {
            client_addr,
            sender,
            notification_sender,
        });
    }

    pub fn client_disconnected(&self, client_addr: String) {
        let _ = self.sender.send(ClientRequest::ClientDisconnected { client_addr });
    }

    pub fn client_authenticated(&self, client_addr: String, client_id: EntityId) {
        let _ = self.sender.send(ClientRequest::ClientAuthenticated {
            client_addr,
            client_id,
        });
    }

    pub async fn process_store_message(&self, message: StoreMessage, client_addr: Option<String>) -> StoreMessage {
        let (response_tx, response_rx) = oneshot::channel();
        if self.sender.send(ClientRequest::ProcessStoreMessage {
            message,
            client_addr,
            response: response_tx,
        }).is_ok() {
            response_rx.await.unwrap_or_else(|_| StoreMessage::Error {
                id: "unknown".to_string(),
                error: "Client service unavailable".to_string(),
            })
        } else {
            StoreMessage::Error {
                id: "unknown".to_string(),
                error: "Client service unavailable".to_string(),
            }
        }
    }

    pub async fn register_notification(&self, client_addr: String, config: NotifyConfig) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        if self.sender.send(ClientRequest::RegisterNotification {
            client_addr,
            config,
            response: response_tx,
        }).is_ok() {
            response_rx.await.unwrap_or_else(|_| Err(anyhow::anyhow!("Client service unavailable")))
        } else {
            Err(anyhow::anyhow!("Client service unavailable"))
        }
    }

    pub async fn unregister_notification(&self, client_addr: String, config: NotifyConfig) -> bool {
        let (response_tx, response_rx) = oneshot::channel();
        if self.sender.send(ClientRequest::UnregisterNotification {
            client_addr,
            config,
            response: response_tx,
        }).is_ok() {
            response_rx.await.unwrap_or(false)
        } else {
            false
        }
    }

    pub async fn force_disconnect_all(&self) {
        let (response_tx, response_rx) = oneshot::channel();
        if self.sender.send(ClientRequest::ForceDisconnectAll {
            response: response_tx,
        }).is_ok() {
            let _ = response_rx.await;
        }
    }

    /// Set services for dependencies
    pub async fn set_services(&self, services: Services) {
        let (response_tx, response_rx) = oneshot::channel();
        if self.sender.send(ClientRequest::SetServices {
            services,
            response: response_tx,
        }).is_ok() {
            let _ = response_rx.await;
        }
    }
}

pub struct ClientService {
    config: ClientConfig,
    connected_clients: HashMap<String, mpsc::UnboundedSender<Message>>,
    client_notification_senders: HashMap<String, NotificationSender>,
    client_notification_configs: HashMap<String, HashSet<NotifyConfig>>,
    authenticated_clients: HashMap<String, EntityId>,
    services: Option<Services>,
}

impl ClientService {
    pub fn spawn(config: ClientConfig) -> ClientHandle {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        
        let config_clone = config.clone();
        tokio::spawn(async move {
            let mut service = ClientService {
                config,
                connected_clients: HashMap::new(),
                client_notification_senders: HashMap::new(),
                client_notification_configs: HashMap::new(),
                authenticated_clients: HashMap::new(),
                services: None,
            };

            while let Some(request) = receiver.recv().await {
                service.handle_request(request).await;
            }
        });

        // Start the client WebSocket server
        let handle_clone = ClientHandle { sender: sender.clone() };
        tokio::spawn(start_client_server(config_clone, handle_clone));

        ClientHandle { sender }
    }
    
    async fn handle_request(&mut self, request: ClientRequest) {
        match request {
            ClientRequest::ClientConnected { client_addr, sender, notification_sender } => {
                self.connected_clients.insert(client_addr.clone(), sender);
                self.client_notification_senders.insert(client_addr, notification_sender);
            }
            ClientRequest::ClientDisconnected { client_addr } => {
                self.handle_client_disconnected(client_addr).await;
            }
            ClientRequest::ClientAuthenticated { client_addr, client_id } => {
                self.authenticated_clients.insert(client_addr, client_id);
            }
            ClientRequest::ProcessStoreMessage { message, client_addr, response } => {
                let result = self.process_store_message(message, client_addr).await;
                let _ = response.send(result);
            }
            ClientRequest::RegisterNotification { client_addr, config, response } => {
                let result = self.register_notification_internal(client_addr, config).await;
                let _ = response.send(result);
            }
            ClientRequest::UnregisterNotification { client_addr, config, response } => {
                let result = self.unregister_notification_internal(client_addr, config).await;
                let _ = response.send(result);
            }
            ClientRequest::ForceDisconnectAll { response } => {
                self.force_disconnect_all_clients().await;
                let _ = response.send(());
            }
            ClientRequest::SetServices { services, response } => {
                self.services = Some(services);
                let _ = response.send(());
            }
        }
    }
    
    async fn handle_client_disconnected(&mut self, client_addr: String) {
        // Remove client from all tracking structures
        self.connected_clients.remove(&client_addr);
        
        // Get the notification sender and configurations for this client
        let notification_sender = self.client_notification_senders.remove(&client_addr);
        let client_configs = self.client_notification_configs.remove(&client_addr);
        
        // Remove authentication state for this client
        self.authenticated_clients.remove(&client_addr);
        
        // Unregister all notifications for this client from the store
        if let Some(configs) = client_configs {
            if let Some(sender) = notification_sender {
                for config in configs {
                    let removed = self.services.as_ref().unwrap().store_handle.unregister_notification(&config, &sender).await;
                    if removed {
                        debug!(
                            client_addr = %client_addr,
                            config = ?config,
                            "Cleaned up notification config for disconnected client"
                        );
                    } else {
                        warn!(
                            client_addr = %client_addr,
                            config = ?config,
                            "Failed to clean up notification config for disconnected client"
                        );
                    }
                }
                // The notification sender being dropped will close the channel
                drop(sender);
            }
        } else if let Some(sender) = notification_sender {
            // Just drop the sender if no configs were tracked
            drop(sender);
        }
    }
    
    async fn process_store_message(&mut self, message: StoreMessage, client_addr: Option<String>) -> StoreMessage {
        // Extract client notification sender if needed
        let client_notification_sender = if let Some(ref addr) = client_addr {
            self.client_notification_senders.get(addr).cloned()
        } else {
            None
        };

        match message {
            StoreMessage::Authenticate { id, subject_name: _, credential: _ } => {
                // TODO: Implement proper authentication
                // For now, we'll simulate successful authentication
                if let Some(ref addr) = client_addr {
                    let client_id = EntityId::new("User", 0); // Temporary ID
                    self.authenticated_clients.insert(addr.clone(), client_id.clone());
                    
                    StoreMessage::AuthenticateResponse {
                        id,
                        response: Ok(qlib_rs::AuthenticationResult {
                            subject_id: client_id,
                            subject_type: "User".to_string(),
                        }),
                    }
                } else {
                    StoreMessage::AuthenticateResponse {
                        id,
                        response: Err("No client address provided".to_string()),
                    }
                }
            }
            
            StoreMessage::AuthenticateResponse { .. } => {
                // This should not be sent by clients, only by server
                StoreMessage::Error {
                    id: "unknown".to_string(),
                    error: "AuthenticateResponse is not a valid client message".to_string(),
                }
            }
            
            // Handle other store messages by delegating to the store
            _ => {
                // Get the client ID if the client is authenticated
                let _client_id = if let Some(ref addr) = client_addr {
                    self.authenticated_clients.get(addr).cloned()
                } else {
                    None
                };
                
                // Process the message with the store
                match &message {
                    StoreMessage::RegisterNotification { id, config } => {
                        if let Some(ref addr) = client_addr {
                            if let Some(notification_sender) = client_notification_sender {
                                match self.services.as_ref().unwrap().store_handle.register_notification(config.clone(), notification_sender).await {
                            Ok(()) => {
                                // Track this config for cleanup on disconnect
                                self.client_notification_configs
                                    .entry(addr.clone())
                                    .or_insert_with(HashSet::new)
                                    .insert(config.clone());
                                
                                StoreMessage::RegisterNotificationResponse {
                                    id: id.clone(),
                                    response: Ok(()),
                                }
                            }
                            Err(e) => {
                                StoreMessage::Error {
                                    id: id.clone(),
                                    error: format!("Failed to register notification: {}", e),
                                }
                            }
                        }
                            } else {
                                StoreMessage::Error {
                                    id: id.clone(),
                                    error: "No notification channel available for client".to_string(),
                                }
                            }
                        } else {
                            StoreMessage::Error {
                                id: id.clone(),
                                error: "No client address provided".to_string(),
                            }
                        }
                    }
                    
                    StoreMessage::UnregisterNotification { id, config } => {
                        if let Some(ref addr) = client_addr {
                            if let Some(notification_sender) = client_notification_sender {
                                let removed = self.services.as_ref().unwrap().store_handle.unregister_notification(config, &notification_sender).await;
                                if removed {
                                    // Remove from our tracking
                                    if let Some(configs) = self.client_notification_configs.get_mut(addr) {
                                        configs.remove(config);
                                    }
                                }
                                
                                StoreMessage::UnregisterNotificationResponse {
                                    id: id.clone(),
                                    response: removed,
                                }
                            } else {
                                StoreMessage::Error {
                                    id: id.clone(),
                                    error: "No notification channel available for client".to_string(),
                                }
                            }
                        } else {
                            StoreMessage::Error {
                                id: id.clone(),
                                error: "No client address provided".to_string(),
                            }
                        }
                    }
                    
                    // For all other messages, we need to implement delegation to the store
                    // This is a simplified version - you may need to implement more specific handling
                    _ => {
                        // TODO: Implement delegation to store for other message types
                        StoreMessage::Error {
                            id: "unknown".to_string(),
                            error: "Message type not yet implemented in ClientService".to_string(),
                        }
                    }
                }
            }
        }
    }
    
    async fn register_notification_internal(&mut self, client_addr: String, config: NotifyConfig) -> Result<()> {
        if let Some(notification_sender) = self.client_notification_senders.get(&client_addr) {
            self.services.as_ref().unwrap().store_handle.register_notification(config.clone(), notification_sender.clone()).await?;
            
            // Track this config for cleanup on disconnect
            self.client_notification_configs
                .entry(client_addr)
                .or_insert_with(HashSet::new)
                .insert(config);
            
            Ok(())
        } else {
            Err(anyhow::anyhow!("No notification channel available for client"))
        }
    }
    
    async fn unregister_notification_internal(&mut self, client_addr: String, config: NotifyConfig) -> bool {
        if let Some(notification_sender) = self.client_notification_senders.get(&client_addr) {
            let removed = self.services.as_ref().unwrap().store_handle.unregister_notification(&config, notification_sender).await;
            if removed {
                // Remove from our tracking
                if let Some(configs) = self.client_notification_configs.get_mut(&client_addr) {
                    configs.remove(&config);
                }
            }
            removed
        } else {
            false
        }
    }
    
    async fn force_disconnect_all_clients(&mut self) {
        if self.connected_clients.is_empty() {
            return;
        }

        let client_count = self.connected_clients.len();
        info!(
            client_count = client_count,
            "Force disconnecting clients due to unavailable state"
        );
        
        // Send close messages to all connected clients
        let disconnect_message = Message::Close(None);
        for (client_addr, sender) in &self.connected_clients {
            if let Err(e) = sender.send(disconnect_message.clone()) {
                warn!(
                    client_addr = %client_addr,
                    error = %e,
                    "Failed to send close message to client"
                );
            }
        }
        
        // Clear all client-related data structures
        self.connected_clients.clear();
        self.client_notification_senders.clear();
        self.client_notification_configs.clear();
        self.authenticated_clients.clear();
    }
}

/// Handle a single client WebSocket connection
#[instrument(skip(stream, handle), fields(client_addr = %client_addr))]
async fn handle_client_connection(
    stream: TcpStream,
    client_addr: std::net::SocketAddr,
    handle: ClientHandle,
) -> Result<()> {
    info!("Accepting client connection");
    
    let ws_stream = accept_async(stream).await?;
    debug!("WebSocket handshake completed");
    
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    
    // Wait for authentication message as the first message
    let auth_timeout = tokio::time::timeout(Duration::from_secs(10), ws_receiver.next()).await;
    
    let first_message = match auth_timeout {
        Ok(Some(Ok(Message::Text(text)))) => text,
        Ok(Some(Ok(Message::Close(_)))) => {
            info!("Client closed connection before authentication");
            return Ok(());
        }
        Ok(Some(Err(e))) => {
            error!(error = %e, "WebSocket error from client during authentication");
            return Ok(());
        }
        Ok(None) => {
            info!("Client closed connection before authentication");
            return Ok(());
        }
        Ok(Some(Ok(_))) => {
            error!("Client sent non-text message during authentication");
            let _ = ws_sender.close().await;
            return Ok(());
        }
        Err(_) => {
            info!("Client authentication timeout");
            let _ = ws_sender.close().await;
            return Ok(());
        }
    };
    
    // Parse and validate authentication message
    let auth_message = match serde_json::from_str::<StoreMessage>(&first_message) {
        Ok(StoreMessage::Authenticate { .. }) => {
            serde_json::from_str::<StoreMessage>(&first_message).unwrap()
        }
        _ => {
            error!("Client first message was not authentication");
            let _ = ws_sender.close().await;
            return Ok(());
        }
    };
    
    // Process authentication
    let auth_response = handle.process_store_message(auth_message, Some(client_addr.to_string())).await;
    
    // Send authentication response
    let auth_response_text = match serde_json::to_string(&auth_response) {
        Ok(text) => text,
        Err(e) => {
            error!(error = %e, "Failed to serialize authentication response");
            let _ = ws_sender.close().await;
            return Ok(());
        }
    };
    
    if let Err(e) = ws_sender.send(Message::Text(auth_response_text)).await {
        error!(error = %e, "Failed to send authentication response to client");
        return Ok(());
    }
    
    // Check if authentication was successful
    let is_authenticated = matches!(auth_response, StoreMessage::AuthenticateResponse { response: Ok(_), .. });
    
    if !is_authenticated {
        info!("Client authentication failed, closing connection");
        let _ = ws_sender.close().await;
        return Ok(());
    }
    
    info!("Client authenticated successfully");
    
    // Create a channel for sending messages to this client
    let (tx, rx) = mpsc::unbounded_channel::<Message>();
    
    // Create a notification channel for this client
    let (notification_sender, mut notification_receiver) = notification_channel();
    
    // Register client with the service
    handle.client_connected(client_addr.to_string(), tx.clone(), notification_sender);
    
    // Spawn a task to handle notifications for this client
    let client_addr_clone_notif = client_addr.to_string();
    let tx_clone_notif = tx.clone();
    let notification_task = tokio::spawn(async move {
        while let Some(notification) = notification_receiver.recv().await {
            // Convert notification to StoreMessage and send to client
            let notification_msg = StoreMessage::Notification { notification };
            if let Ok(notification_text) = serde_json::to_string(&notification_msg) {
                if let Err(e) = tx_clone_notif.send(Message::Text(notification_text)) {
                    error!(
                        client_addr = %client_addr_clone_notif,
                        error = %e,
                        "Failed to send notification to client"
                    );
                    break;
                }
            } else {
                error!(
                    client_addr = %client_addr_clone_notif,
                    "Failed to serialize notification for client"
                );
            }
        }
        debug!(
            client_addr = %client_addr_clone_notif,
            "Notification task ended for client"
        );
    });
    
    // Spawn a task to handle outgoing messages to the client
    let client_addr_clone = client_addr.to_string();
    let handle_clone = handle.clone();
    let outgoing_task = tokio::spawn(async move {
        let mut ws_sender = ws_sender;
        let mut rx = rx;
        
        while let Some(message) = rx.recv().await {
            if let Err(e) = ws_sender.send(message).await {
                error!(
                    client_addr = %client_addr_clone,
                    error = %e,
                    "Failed to send message to client"
                );
                break;
            }
        }
        
        // Notify service that client disconnected
        handle_clone.client_disconnected(client_addr_clone);
    });
    
    // Handle incoming messages from client (after successful authentication)
    while let Some(msg) = ws_receiver.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                debug!(
                    message_length = text.len(),
                    "Received text message from client"
                );
                
                // Parse the StoreMessage
                match serde_json::from_str::<StoreMessage>(&text) {
                    Ok(store_msg) => {
                        // Process the message and generate response
                        let response_msg = handle.process_store_message(store_msg, Some(client_addr.to_string())).await;
                        
                        // Send response back to client using the channel
                        let response_text = match serde_json::to_string(&response_msg) {
                            Ok(text) => text,
                            Err(e) => {
                                error!(error = %e, "Failed to serialize response");
                                continue;
                            }
                        };
                        
                        if let Err(e) = tx.send(Message::Text(response_text)) {
                            error!(error = %e, "Failed to send response to client");
                            break;
                        }
                    }
                    Err(e) => {
                        error!(
                            error = %e,
                            message_length = text.len(),
                            "Failed to parse StoreMessage from client"
                        );
                        // Send error response
                        let error_msg = StoreMessage::Error {
                            id: uuid::Uuid::new_v4().to_string(),
                            error: format!("Failed to parse message: {}", e),
                        };
                        if let Ok(error_text) = serde_json::to_string(&error_msg) {
                            let _ = tx.send(Message::Text(error_text));
                        }
                    }
                }
            }
            Ok(Message::Binary(_data)) => {
                debug!("Received binary data from client");
                // For now, we only handle text messages for StoreProxy protocol
            }
            Ok(Message::Ping(payload)) => {
                debug!("Received ping from client");
                if let Err(e) = tx.send(Message::Pong(payload)) {
                    error!(error = %e, "Failed to send pong to client");
                    break;
                }
            }
            Ok(Message::Pong(_)) => {
                debug!("Received pong from client");
            }
            Ok(Message::Close(_)) => {
                info!("Client closed connection gracefully");
                break;
            }
            Ok(Message::Frame(_)) => {
                debug!("Received raw frame from client");
            }
            Err(e) => {
                error!(error = %e, "WebSocket error with client");
                break;
            }
        }
    }
    
    // Notify service that client disconnected
    handle.client_disconnected(client_addr.to_string());
    
    // Abort the outgoing task and notification task
    outgoing_task.abort();
    notification_task.abort();
    
    info!("Client connection terminated");
    Ok(())
}

/// Start the client WebSocket server
async fn start_client_server(config: ClientConfig, handle: ClientHandle) -> Result<()> {
    let addr = format!("0.0.0.0:{}", config.client_port);
    let listener = TcpListener::bind(&addr).await?;
    info!(bind_address = %addr, "Client WebSocket server started");
    
    loop {
        match listener.accept().await {
            Ok((stream, client_addr)) => {
                debug!(client_addr = %client_addr, "Accepted new client connection");
                
                let handle_clone = handle.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_client_connection(stream, client_addr, handle_clone).await {
                        error!(
                            error = %e,
                            client_addr = %client_addr,
                            "Error handling client connection"
                        );
                    }
                });
            }
            Err(e) => {
                error!(error = %e, "Failed to accept client connection");
                // Continue listening despite individual connection errors
            }
        }
    }
}