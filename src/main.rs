mod persistance;
mod states;
mod clients;
mod peers;
mod misc;
mod store;

use qlib_rs::{et, ft, notification_channel, now, schoice, sread, sref, swrite, AuthConfig, AuthenticationResult, Cache, PushCondition, StoreMessage, StoreTrait, Snowflake, CelExecutor};
use qlib_rs::auth::{AuthorizationScope, get_scope};
use tokio::signal;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, connect_async, tungstenite::Message};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, Mutex};
use tracing::{info, warn, error, debug, instrument};
use anyhow::Result;
use std::collections::{HashSet};
use std::sync::Arc;
use std::vec;
use std::time::Duration;
use time;
use clap::Parser;

use crate::persistance::{SnapshotService, WalService, SnapshotTrait, WalTrait, WalConfig, SnapshotConfig};
use crate::states::{AppState, AppStateLocks, AvailabilityState, Config, LockRequest, PeerInfo, PeerMessage};
use crate::store::{StoreService, StoreHandle};
use crate::peers::PeerService;





/// Handle a single client WebSocket connection that uses StoreProxy protocol
#[instrument(skip(stream, app_state), fields(client_addr = %client_addr))]
async fn handle_client_connection(stream: TcpStream, client_addr: std::net::SocketAddr, app_state: Arc<AppState>) -> Result<()> {
    info!("Accepting client connection");
    
    // Check if the application is available for client connections
    {
        let mut locks = app_state.acquire_locks(LockRequest {
            core_state: true,
            ..Default::default()
        }).await;

        let state_snapshot = locks.core_state().get_state_snapshot();
        if !state_snapshot.is_available() {
            info!("Rejecting client connection - application unavailable");
            // Don't accept the WebSocket connection, just return
            return Ok(());
        }
    }
    
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
    let auth_response = {
        let mut locks = app_state.acquire_locks(LockRequest {
            connections: true,
            permission_cache: true,
            core_state: true,
            cel_executor: true,
            ..Default::default()
        }).await;
        process_store_message(auth_message, Some(client_addr.to_string()), &mut locks, &app_state.store).await
    };
    
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
    let is_authenticated = {
        let mut locks = app_state.acquire_locks(LockRequest {
            connections: true,
            ..Default::default()
        }).await;

        locks.connections().authenticated_clients.contains_key(&client_addr.to_string())
    };
    
    if !is_authenticated {
        info!("Client authentication failed, closing connection");
        let _ = ws_sender.close().await;
        return Ok(());
    }
    
    info!("Client authenticated successfully");
    
    // Now proceed with normal client handling
    // Create a channel for sending messages to this client
    let (tx, rx) = mpsc::unbounded_channel::<Message>();
    
    // Create a notification channel for this client
    let (notification_sender, mut notification_receiver) = notification_channel();
    
    // Store the sender and notification sender in the connected_clients HashMap
    {
        let mut locks = app_state.acquire_locks(LockRequest {
            connections: true,
            ..Default::default()
        }).await;
        let connections = locks.connections();
        connections.connected_clients.insert(client_addr.to_string(), tx.clone());
        connections.client_notification_senders.insert(client_addr.to_string(), notification_sender);
    }
    
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
    let app_state_clone = Arc::clone(&app_state);
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
        
        // Remove client from connected_clients when outgoing task ends
        let mut locks = app_state_clone.acquire_locks(LockRequest {
            connections: true,
            ..Default::default()
        }).await;
        let connections = locks.connections();
        connections.connected_clients.remove(&client_addr_clone);
        connections.authenticated_clients.remove(&client_addr_clone);
        connections.client_notification_senders.remove(&client_addr_clone);
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
                        let response_msg = {
                            let mut locks = app_state.acquire_locks(LockRequest {
                                connections: true,
                                permission_cache: true,
                                core_state: true,
                                cel_executor: true,
                                ..Default::default()
                            }).await;
                            process_store_message(store_msg, Some(client_addr.to_string()), &mut locks, &app_state.store).await
                        };
                        
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
    
    // Remove client from connected_clients and cleanup notifications when connection ends
    {
        let mut locks = app_state.acquire_locks(LockRequest {
            connections: true,
            store: true,
            ..Default::default()
        }).await;
        let client_addr_string = client_addr.to_string();
        locks.connections().connected_clients.remove(&client_addr_string);

        // Get the notification sender and configurations for this client
        let notification_sender = locks.connections().client_notification_senders.remove(&client_addr_string);
        let client_configs = locks.connections().client_notification_configs.remove(&client_addr_string);

        // Remove authentication state for this client
        locks.connections().authenticated_clients.remove(&client_addr_string);

        // Unregister all notifications for this client from the store
        if let Some(configs) = client_configs {
            if let Some(sender) = notification_sender {
                {
                    let store_guard = locks.store();
                    
                    for config in configs {
                        let removed = store_guard.unregister_notification(&config, &sender).await;
                        if removed {
                            debug!(
                                client_addr = %client_addr_string,
                                config = ?config,
                                "Cleaned up notification config for disconnected client"
                            );
                        } else {
                            warn!(
                                client_addr = %client_addr_string,
                                config = ?config,
                                "Failed to clean up notification config for disconnected client"
                            );
                        }
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
    
    // Abort the outgoing task and notification task
    outgoing_task.abort();
    notification_task.abort();
    
    info!("Client connection terminated");
    Ok(())
}

/// Process a StoreMessage and generate the appropriate response
async fn process_store_message(message: StoreMessage, client_addr: Option<String>, locks: &mut AppStateLocks<'_>, store: &StoreHandle) -> StoreMessage {
    // Extract client notification sender if needed (before accessing store)
    let client_notification_sender = if let Some(ref addr) = client_addr {
        locks.connections().client_notification_senders.get(addr).cloned()
    } else {
        None
    };

    match message {
        StoreMessage::Authenticate { id, subject_name, credential } => {
            // Perform authentication
            let auth_config = AuthConfig::default();
            // TODO: Authentication needs to be implemented in StoreHandle
            // For now, we'll use a placeholder that always fails
            StoreMessage::AuthenticateResponse {
                id,
                response: Err("Authentication not yet implemented with StoreHandle".to_string()),
            }
        }
        
        StoreMessage::AuthenticateResponse { .. } => {
            // This should not be sent by clients, only by server
            StoreMessage::Error {
                id: "unknown".to_string(),
                error: "Invalid message type".to_string(),
            }
        }
        
        // All other messages require authentication
        _ => {
            // Check if client is authenticated
            let client_id = if let Some(ref addr) = client_addr {
                locks.connections().authenticated_clients.get(addr).cloned()
            } else {
                None // No client address means not authenticated
            };
            
            if client_id.is_none() {
                return StoreMessage::Error {
                    id: "unknown".to_string(),
                    error: "Authentication required".to_string(),
                };
            }
            let client_id = client_id.unwrap();
            
            match message {
                StoreMessage::Authenticate { .. } |
                StoreMessage::AuthenticateResponse { .. } => {
                    // These are handled in the outer match, should not reach here
                    StoreMessage::Error {
                        id: "unknown".to_string(),
                        error: "Authentication messages should not reach this point".to_string(),
                    }
                }
        
                StoreMessage::GetEntitySchema { id, entity_type } => {
                    match store.get_entity_schema(&entity_type).await {
                        Ok(schema) => StoreMessage::GetEntitySchemaResponse {
                            id,
                            response: Ok(Some(schema)),
                        },
                        Err(e) => StoreMessage::GetEntitySchemaResponse {
                            id,
                            response: Err(format!("{:?}", e)),
                        },
                    }
                }
                
                StoreMessage::GetCompleteEntitySchema { id, entity_type } => {
                    match store.get_complete_entity_schema(&entity_type).await {
                        Ok(schema) => StoreMessage::GetCompleteEntitySchemaResponse {
                            id,
                            response: Ok(schema),
                        },
                        Err(e) => StoreMessage::GetCompleteEntitySchemaResponse {
                            id,
                            response: Err(format!("{:?}", e)),
                        },
                    }
                }
                
                StoreMessage::GetFieldSchema { id, entity_type, field_type } => {
                    match store.get_field_schema(&entity_type, &field_type).await {
                        Ok(schema) => StoreMessage::GetFieldSchemaResponse {
                            id,
                            response: Ok(Some(schema)),
                        },
                        Err(e) => StoreMessage::GetFieldSchemaResponse {
                            id,
                            response: Err(format!("{:?}", e)),
                        },
                    }
                }
                
                StoreMessage::EntityExists { id, entity_id } => {
                    let exists = store.entity_exists(&entity_id).await;
                    StoreMessage::EntityExistsResponse {
                        id,
                        response: exists,
                    }
                }
                
                StoreMessage::FieldExists { id, entity_type, field_type } => {
                    let exists = store.field_exists(&entity_type, &field_type).await;
                    StoreMessage::FieldExistsResponse {
                        id,
                        response: exists,
                    }
                }
                
                StoreMessage::Perform { id, mut requests } => {
                    // Get the client_id from authenticated clients if available
                    let client_id = if let Some(ref addr) = client_addr {
                        locks.connections().authenticated_clients.get(addr).cloned()
                    } else {
                        None
                    };

                    let machine = locks.core_state().config.machine.clone();

                    requests.iter_mut().for_each(|req| {
                        req.try_set_originator(machine.clone());
                        if let Some(ref client_id) = client_id {
                            req.try_set_writer_id(client_id.clone());
                        }
                    });

                    // Use authorization if client_id is available, otherwise perform without authorization
                    let result = if client_id.is_some() {
                        store.perform_mut_with_auth(requests, client_id).await
                    } else {
                        store.perform_mut(requests).await
                    };

                    match result {
                        Ok(requests) => StoreMessage::PerformResponse {
                            id,
                            response: Ok(requests),
                        },
                        Err(e) => StoreMessage::PerformResponse {
                            id,
                            response: Err(format!("{:?}", e)),
                        },
                    }
                }
                
                StoreMessage::FindEntities { id, entity_type, page_opts, filter } => {
                    match store.find_entities_paginated(&entity_type, page_opts, filter).await {
                        Ok(result) => StoreMessage::FindEntitiesResponse {
                            id,
                            response: Ok(result),
                        },
                        Err(e) => StoreMessage::FindEntitiesResponse {
                            id,
                            response: Err(format!("{:?}", e)),
                        },
                    }
                }
                
                StoreMessage::FindEntitiesExact { id, entity_type, page_opts, filter } => {
                    match store.find_entities_exact(&entity_type, page_opts, filter).await {
                        Ok(result) => StoreMessage::FindEntitiesExactResponse {
                            id,
                            response: Ok(result),
                        },
                        Err(e) => StoreMessage::FindEntitiesExactResponse {
                            id,
                            response: Err(format!("{:?}", e)),
                        },
                    }
                }
                
                StoreMessage::GetEntityTypes { id, page_opts } => {
                    match store.get_entity_types_paginated(page_opts).await {
                        Ok(result) => StoreMessage::GetEntityTypesResponse {
                            id,
                            response: Ok(result),
                        },
                        Err(e) => StoreMessage::GetEntityTypesResponse {
                            id,
                            response: Err(format!("{:?}", e)),
                        },
                    }
                }
                
                StoreMessage::RegisterNotification { id, config } => {
                    // Register notification for this client
                    if let Some(client_addr) = &client_addr {
                        if let Some(ref notification_sender) = client_notification_sender {
                            match store.register_notification(config.clone(), notification_sender.clone()).await {
                                Ok(()) => {
                                    locks
                                        .connections()
                                        .client_notification_configs
                                        .entry(client_addr.clone())
                                        .or_insert_with(HashSet::new)
                                        .insert(config.clone());
                                    
                                    debug!(
                                        client_addr = %client_addr,
                                        config = ?config,
                                        "Registered notification for client"
                                    );
                                    StoreMessage::RegisterNotificationResponse {
                                        id,
                                        response: Ok(()),
                                    }
                                }
                                Err(e) => {
                                    error!(
                                        client_addr = %client_addr,
                                        error = ?e,
                                        "Failed to register notification for client"
                                    );
                                    StoreMessage::RegisterNotificationResponse {
                                        id,
                                        response: Err(format!("Failed to register notification: {:?}", e)),
                                    }
                                }
                            }
                        } else {
                            error!(
                                client_addr = %client_addr,
                                "No notification sender found for client"
                            );
                            StoreMessage::RegisterNotificationResponse {
                                id,
                                response: Err("Client notification sender not found".to_string()),
                            }
                        }
                    } else {
                        StoreMessage::RegisterNotificationResponse {
                            id,
                            response: Err("Client address not provided".to_string()),
                        }
                    }
                }
                
                StoreMessage::UnregisterNotification { id, config } => {
                    // Unregister notification for this client
                    if let Some(client_addr) = &client_addr {
                        if let Some(ref notification_sender) = client_notification_sender {
                            let removed = store.unregister_notification(&config, notification_sender).await;
                            
                            // Remove from client's tracked configs if successfully unregistered
                            if removed {
                                if let Some(client_configs) = locks.connections().client_notification_configs.get_mut(client_addr) {
                                    client_configs.remove(&config);
                                }
                            }
                            
                            debug!(
                                client_addr = %client_addr,
                                config = ?config,
                                removed = removed,
                                "Unregistered notification for client"
                            );
                            StoreMessage::UnregisterNotificationResponse {
                                id,
                                response: removed,
                            }
                        } else {
                            error!(
                                client_addr = %client_addr,
                                "No notification sender found for client"
                            );
                            StoreMessage::UnregisterNotificationResponse {
                                id,
                                response: false,
                            }
                        }
                    } else {
                        StoreMessage::UnregisterNotificationResponse {
                            id,
                            response: false,
                        }
                    }
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
                    StoreMessage::Error {
                        id,
                        error: "Received response message on server - this should not happen".to_string(),
                    }
                }
                
                StoreMessage::Notification { .. } => {
                    StoreMessage::Error {
                        id: uuid::Uuid::new_v4().to_string(),
                        error: "Received notification message on server - this should not happen".to_string(),
                    }
                }
                
                StoreMessage::Error { id, error } => {
                    warn!(
                        message_id = %id,
                        error_message = %error,
                        "Received error message from client"
                    );
                    StoreMessage::Error {
                        id: uuid::Uuid::new_v4().to_string(),
                        error: "Server received error message from client".to_string(),
                    }
                }
            }
        }
    }
}

/// Start the client WebSocket server task
#[instrument(skip(app_state))]
async fn start_client_server(app_state: Arc<AppState>) -> Result<()> {
    let addr = {
        let mut locks = app_state.acquire_locks(LockRequest {
            core_state: true,
            ..Default::default()
        }).await;
        format!("0.0.0.0:{}", locks.core_state().config.client_port)
    };
    
    let listener = TcpListener::bind(&addr).await?;
    info!(bind_address = %addr, "Client WebSocket server started");
    
    loop {
        match listener.accept().await {
            Ok((stream, client_addr)) => {
                debug!(client_addr = %client_addr, "Accepted new client connection");
                
                let app_state_clone = Arc::clone(&app_state);
                tokio::spawn(async move {
                    if let Err(e) = handle_client_connection(stream, client_addr, app_state_clone).await {
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





/// Consume and process requests from the store's write channel
async fn consume_write_channel(app_state: Arc<AppState>) -> Result<()> {
    info!("Starting write channel consumer");
    
    // Get a clone of the write channel receiver
    let receiver = {
        let mut locks = app_state.acquire_locks(LockRequest {
            store: true,
            ..Default::default()
        }).await;
        locks.store().inner().get_write_channel_receiver()
    };
    
    loop {
        // Wait for a batch of requests from the write channel without holding any store locks
        let requests = {
            let mut receiver_guard = receiver.lock().await;
            receiver_guard.recv().await
        };
        
        match requests {
            Some(mut requests) => {     
                let mut locks = app_state.acquire_locks(LockRequest {
                    core_state: true,
                    store: true,
                    connections: true,
                    ..Default::default()
                }).await;

                // Ensure the originator is set for all requests
                let current_machine = locks.core_state().config.machine.clone();
                requests.iter_mut().for_each(|req| req.try_set_originator(current_machine.clone()));
                
                // Write all requests to the WAL file - these requests have already been applied to the store
                for request in &requests {
                    if let Err(e) = write_request_to_wal(request, &mut locks, false).await {
                        error!(
                            error = %e,
                            "Failed to write request to WAL"
                        );
                    }
                }
                
                // Send batch of requests to peers for synchronization if we have any
                let requests_to_sync: Vec<qlib_rs::Request> = requests.iter()
                    .filter(|request| {
                        if let Some(originator) = request.originator() {
                            originator == &current_machine
                        } else {
                            false
                        }
                    })
                    .cloned()
                    .collect();
                
                if !requests_to_sync.is_empty() {                    
                    // Send to all connected outbound peers using PeerMessage
                    let peers_to_notify = locks.connections().connected_outbound_peers.clone();

                    // Create a batch sync message
                    let sync_message = PeerMessage::SyncRequest {
                        requests: requests_to_sync.clone(),
                    };
                    
                    // Serialize the sync message to JSON for transmission
                    match serde_json::to_string(&sync_message) {
                        Ok(message_json) => {
                            let message = Message::Text(message_json);
                            
                            for (peer_addr, sender) in &peers_to_notify {
                                if let Err(e) = sender.send(message.clone()) {
                                    warn!(
                                        peer_addr = %peer_addr,
                                        error = %e,
                                        "Failed to send sync requests to peer"
                                    );
                                } else {
                                    debug!(
                                        peer_addr = %peer_addr,
                                        count = requests_to_sync.len(),
                                        "Sent sync requests to peer"
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            error!(
                                error = %e,
                                "Failed to serialize sync message for peer synchronization"
                            );
                        }
                    }
                }
            }
            None => {
                warn!("Write channel closed, stopping consumer");
                return Ok(());
            }
        }
    }
}

/// Write data to WAL with length prefix and handle file creation/rotation
async fn write_request_to_wal(request: &qlib_rs::Request, locks: &mut AppStateLocks<'_>, direct_mode: bool) -> Result<()> {
    let wal_config = WalConfig {
        wal_dir: locks.core_state().get_wal_dir(),
        max_file_size: locks.core_state().config.wal_max_file_size * 1024 * 1024,
        max_files: locks.core_state().config.wal_max_files,
        snapshot_wal_interval: locks.core_state().config.snapshot_wal_interval,
        machine_id: locks.core_state().config.machine.clone(),
        snapshots_dir: locks.core_state().get_snapshots_dir(),
        snapshot_max_files: locks.core_state().config.snapshot_max_files,
    };
    let mut wal_manager = WalService::new_default(wal_config);
    wal_manager.write_request(request, locks, direct_mode).await
}

/// Save a snapshot to disk and return the snapshot counter that was used
#[instrument(skip(snapshot, locks))]
async fn save_snapshot(snapshot: &qlib_rs::Snapshot, locks: &mut AppStateLocks<'_>) -> Result<u64> {
    let snapshot_config = SnapshotConfig {
        snapshots_dir: locks.core_state().get_snapshots_dir(),
        max_files: locks.core_state().config.snapshot_max_files,
    };
    let mut snapshot_manager = SnapshotService::new_default(snapshot_config);
    snapshot_manager.save(snapshot, locks).await
}

/// Replay WAL files to restore store state
async fn replay_wal_files(locks: &mut AppStateLocks<'_>) -> Result<()> {
    let wal_config = WalConfig {
        wal_dir: locks.core_state().get_wal_dir(),
        max_file_size: locks.core_state().config.wal_max_file_size * 1024 * 1024,
        max_files: locks.core_state().config.wal_max_files,
        snapshot_wal_interval: locks.core_state().config.snapshot_wal_interval,
        machine_id: locks.core_state().config.machine.clone(),
        snapshots_dir: locks.core_state().get_snapshots_dir(),
        snapshot_max_files: locks.core_state().config.snapshot_max_files,
    };
    let wal_manager = WalService::new_default(wal_config);
    wal_manager.replay(locks).await
}

/// Handle miscellaneous periodic tasks that run every 10ms
async fn handle_misc_tasks(app_state: Arc<AppState>) -> Result<()> {
    info!("Starting miscellaneous tasks handler (10ms interval)");
    
    let mut interval = tokio::time::interval(Duration::from_millis(10));
    
    loop {
        interval.tick().await;

        let mut locks = app_state.acquire_locks(LockRequest {
            core_state: true,
            store: true,
            permission_cache: true,
            connections: true,
            peer_info: true,
            ..Default::default()
        }).await;

        // Check if we should self-promote to leader when no peers are connected
        {
            let ((connections, core), peer_info) = locks
                .connections.as_ref()
                .zip(locks.core_state.as_mut())
                .zip(locks.peer_info.as_ref())
                .unwrap();

            // Self-promote to leader if:
            // 1. We're not already the leader
            // 2. No peer addresses are configured OR no outbound peers are connected
            // 3. No peer info is tracked (no inbound peers)
            // 4. We've waited at least 5 seconds since startup to give other nodes time to connect
            let should_self_promote = !core.is_leader && 
                (core.config.peer_addresses.is_empty() || connections.connected_outbound_peers.is_empty());
            
            if should_self_promote {
                let current_time = time::OffsetDateTime::now_utc().unix_timestamp() as u64;
                let startup_time = core.startup_time;
                let time_since_startup = current_time.saturating_sub(startup_time);
                
                // Wait for the configured delay since startup before self-promoting
                let self_promotion_delay = core.config.self_promotion_delay_secs;
                if time_since_startup >= self_promotion_delay {
                    if peer_info.is_empty() {
                        
                        info!(
                            delay_secs = self_promotion_delay,
                            time_since_startup = time_since_startup,
                            "No peers connected after self-promotion delay, promoting to leader"
                        );
                        
                        let our_machine_id = core.config.machine.clone();
                        core.is_leader = true;
                        core.current_leader = Some(our_machine_id.clone());
                        core.availability_state = AvailabilityState::Available;
                        core.is_fully_synced = true;

                        info!(
                            machine_id = %our_machine_id,
                            "Self-promoted to leader due to no peer connections"
                        );
                    }
                }
            }
        }

        // Check if we need to send a full sync request after grace period
        let (should_send_full_sync, is_leader) = {
            let core = locks.core_state();

            // Only check if we're unavailable, not the leader, not fully synced, and haven't sent a request yet
            if matches!(core.availability_state, AvailabilityState::Unavailable) &&
               !core.is_leader &&
               !core.is_fully_synced &&
               !core.full_sync_request_pending {
                
                if let Some(became_unavailable_at) = core.became_unavailable_at {
                    let current_time = time::OffsetDateTime::now_utc().unix_timestamp() as u64;
                    
                    let grace_period_secs = core.config.full_sync_grace_period_secs;
                    let elapsed = current_time.saturating_sub(became_unavailable_at);
                    
                    if elapsed >= grace_period_secs {
                        // Grace period has expired, check if we have a known leader
                        (core.current_leader.clone(), core.is_leader)
                    } else {
                        (None, core.is_leader)
                    }
                } else {
                    (None, core.is_leader)
                }
            } else {
                (None, core.is_leader)
            }
        };
        
        if let Some(leader_machine_id) = should_send_full_sync {
            let (core, connections) = locks.core_state.as_mut().zip(locks.connections.as_ref()).unwrap();

            info!(
                leader_machine_id = %leader_machine_id,
                "Grace period expired, sending FullSyncRequest to leader"
            );
            
            // Mark that we're sending a request to avoid duplicates
            core.full_sync_request_pending = true;
            
            // Send FullSyncRequest to the leader through any connected outbound peer
            let full_sync_request = PeerMessage::FullSyncRequest {
                machine_id: core.config.machine.clone(),
            };
            
            if let Ok(request_json) = serde_json::to_string(&full_sync_request) {
                let message = Message::Text(request_json);
                
                // Try to send to any connected outbound peer
                let sent = {
                    let mut sent = false;
                    
                    for (peer_addr, sender) in &connections.connected_outbound_peers {
                        if let Err(e) = sender.send(message.clone()) {
                            warn!(
                                peer_addr = %peer_addr,
                                error = %e,
                                "Failed to send FullSyncRequest to peer"
                            );
                        } else {
                            info!(
                                peer_addr = %peer_addr,
                                "Sent FullSyncRequest to peer"
                            );
                            sent = true;
                            break; // Only need to send to one peer
                        }
                    }
                    sent
                };
                
                if !sent {
                    warn!(
                        leader_machine_id = %leader_machine_id,
                        "No connected outbound peers available to send FullSyncRequest to leader"
                    );

                    // Reset the pending flag so we can try again later
                    core.full_sync_request_pending = false;
                }
            } else {
                error!("Failed to serialize FullSyncRequest");
                
                core.full_sync_request_pending = false;
            }
        }

        // Process cache notifications
        {
            let permission_cache = locks.permission_cache();
            if let Some(cache) = permission_cache.as_mut() {
                cache.process_notifications();
            }
        }

        if is_leader {
            let (store, core) = locks.store.as_mut().zip(locks.core_state.as_ref()).unwrap();

            // Find us as a candidate
            let me_as_candidate = {
                let machine = &core.config.machine;

                let mut candidates = store.find_entities(
                    &et::candidate(), 
                    Some(format!("Name == 'qcore' && Parent->Name == '{}'", machine))).await?;

                candidates.pop()
            };

            // Update available list and current leader
            {
                let fault_tolerances = store.find_entities(&et::fault_tolerance(), None).await?;
                for ft_entity_id in fault_tolerances {
                    let ft_fields = store.perform_map(&mut vec![
                        sread!(ft_entity_id.clone(), ft::candidate_list()),
                        sread!(ft_entity_id.clone(), ft::available_list()),
                        sread!(ft_entity_id.clone(), ft::current_leader())
                    ]).await?;

                    let candidates = ft_fields
                        .get(&ft::candidate_list())
                        .unwrap()
                        .value()
                        .unwrap()
                        .expect_entity_list()?;

                    let mut available = Vec::new();
                    for candidate_id in candidates.iter() {
                        let candidate_fields = store.perform_map(&mut vec![
                            sread!(candidate_id.clone(), ft::make_me()),
                            sread!(candidate_id.clone(), ft::heartbeat()),
                            sread!(candidate_id.clone(), ft::death_detection_timeout()),
                        ]).await?;

                        let heartbeat_time = candidate_fields
                            .get(&ft::heartbeat())
                            .unwrap()
                            .write_time()
                            .unwrap();

                        let make_me = candidate_fields
                            .get(&ft::make_me())
                            .unwrap()
                            .value()
                            .unwrap()
                            .expect_choice()?;

                        let death_detection_timeout_millis = candidate_fields
                            .get(&ft::death_detection_timeout())
                            .unwrap()
                            .value()
                            .unwrap()
                            .expect_int()?;
                        
                        let death_detection_timeout_duration = time::Duration::milliseconds(death_detection_timeout_millis);

                        let desired_availability = match make_me {
                            1 => AvailabilityState::Available,
                            _ => AvailabilityState::Unavailable,
                        };

                        if desired_availability == AvailabilityState::Available && 
                           heartbeat_time + death_detection_timeout_duration > now() {
                            available.push(candidate_id.clone());
                        }
                    }

                    store.perform_mut(&mut vec![
                        swrite!(ft_entity_id.clone(), ft::available_list(), Some(qlib_rs::Value::EntityList(available.clone())), PushCondition::Changes),
                    ]).await?;

                    let mut handle_me_as_candidate = false;
                    if let Some(me_as_candidate) = &me_as_candidate {
                        // If we're not in the candidate list, we can't be leader
                        if candidates.contains(me_as_candidate) {
                            handle_me_as_candidate = true;

                            store.perform_mut(&mut vec![
                                swrite!(ft_entity_id.clone(), ft::current_leader(), sref!(Some(me_as_candidate.clone())), PushCondition::Changes)
                            ]).await?;
                        }
                    }

                    if !handle_me_as_candidate {
                        // Now we must promote an available candidate to leader
                        // if the current leader is no longer available.
                        // Note that we want to promote to the next available leader in the candidate list
                        // rather than the first available candidate.
                        let current_leader = ft_fields
                            .get(&ft::current_leader())
                            .unwrap()
                            .value()
                            .unwrap()
                            .expect_entity_reference()?;

                        if current_leader.is_none() {
                            store.perform_mut(&mut vec![
                                swrite!(ft_entity_id.clone(), ft::current_leader(), sref!(available.first().cloned()), PushCondition::Changes),
                            ]).await?;
                        }
                        else if let Some(current_leader) = current_leader {
                            if !available.contains(&current_leader) {
                                // Find the position of the current leader in the candidate list
                                let current_leader_idx = candidates.iter().position(|c| c.clone() == current_leader.clone());
                                
                                if let Some(current_idx) = current_leader_idx {
                                    // Find the next available candidate after the current leader in the candidate list
                                    let mut next_leader = None;
                                    
                                    // Start searching from the position after the current leader
                                    for i in (current_idx + 1)..candidates.len() {
                                        if available.contains(&candidates[i]) {
                                            next_leader = Some(candidates[i].clone());
                                            break;
                                        }
                                    }
                                    
                                    // If no leader found after current position, wrap around to the beginning
                                    if next_leader.is_none() {
                                        for i in 0..=current_idx {
                                            if available.contains(&candidates[i]) {
                                                next_leader = Some(candidates[i].clone());
                                                break;
                                            }
                                        }
                                    }
                                    
                                    store.perform_mut(&mut vec![
                                        swrite!(ft_entity_id.clone(), ft::current_leader(), sref!(next_leader), PushCondition::Changes),
                                    ]).await?;
                                } else {
                                    // Current leader not found in candidates list, just pick the first available
                                    store.perform_mut(&mut vec![
                                        swrite!(ft_entity_id.clone(), ft::current_leader(), sref!(available.first().cloned()), PushCondition::Changes),
                                    ]).await?;
                                }
                            }
                        }
                    }
                }
            }
        }

        tokio::task::yield_now().await;
    }
}

/// Handle heartbeat writing
async fn handle_heartbeat_writing(app_state: Arc<AppState>) -> Result<()> {
    info!("Starting heartbeat writer");
    
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    
    loop {
        interval.tick().await;

        let mut locks = app_state.acquire_locks(LockRequest {
            store: true,
            core_state: true,
            ..Default::default()
        }).await;

        let (store, core) = locks.store.as_mut().zip(locks.core_state.as_ref()).unwrap();
        let machine = &core.config.machine;

        let candidates = store.find_entities(
            &et::candidate(), 
            Some(format!("Name == 'qcore' && Parent->Name == '{}'", machine))).await?;

        if let Some(candidate) = candidates.first() {
            store.perform_mut(&mut vec![
                swrite!(candidate.clone(), ft::heartbeat(), schoice!(0)),
                swrite!(candidate.clone(), ft::make_me(), schoice!(1), PushCondition::Changes)
            ]).await?;
        }

        tokio::task::yield_now().await;
    }
}

/// Reinitialize caches after a full sync
pub async fn reinit_caches(locks: &mut AppStateLocks<'_>) -> Result<()> {
    // Reset the permission cache if it exists
    if let Some(cache) = locks.permission_cache().as_mut() {
        // Just clear the cache to force re-population
        // In a real implementation, you might want to reinitialize it properly
        *cache = Cache::new("permission", 1000); // Use a default cache size
    }
    
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::parse();

    // Initialize tracing with better structured logging
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "qcore_rs=debug,tokio=warn,tokio_tungstenite=warn".to_string())
        )
        .with_target(true)
        .with_thread_ids(true)
        .with_file(cfg!(debug_assertions))
        .with_line_number(cfg!(debug_assertions))
        .init();

    let machine_id = &config.machine;
    let peer_port = config.peer_port;
    let client_port = config.client_port;
    
    info!(
        machine_id = %machine_id,
        peer_port = peer_port,
        client_port = client_port,
        data_dir = %config.data_dir,
        "Starting QCore service"
    );

    // Create the store handle
    let store_handle = StoreService::spawn();

    // Create shared application state
    let app_state = Arc::new(AppState::new(config, store_handle)?);

    // Initialize the WAL file counter based on existing files
    {
        let mut locks = app_state.acquire_locks(LockRequest {
            core_state: true,
            ..Default::default()
        }).await;
        let wal_config = WalConfig {
            wal_dir: locks.core_state().get_wal_dir(),
            max_file_size: locks.core_state().config.wal_max_file_size * 1024 * 1024,
            max_files: locks.core_state().config.wal_max_files,
            snapshot_wal_interval: locks.core_state().config.snapshot_wal_interval,
            machine_id: locks.core_state().config.machine.clone(),
        };
        let wal_handle = WalService::spawn(wal_config, snapshot_handle, store_handle);
    }

    // Load the latest snapshot if available
    {
        let mut locks = app_state.acquire_locks(LockRequest {
            core_state: true,
            ..Default::default()
        }).await;

        let snapshot_config = SnapshotConfig {
            snapshots_dir: locks.core_state().get_snapshots_dir(),
            max_files: locks.core_state().config.snapshot_max_files,
        };
        let mut snapshot_manager = SnapshotService::new_default(snapshot_config);
        if let Some((snapshot, snapshot_counter)) = snapshot_manager.load_latest(&mut locks).await? {
            info!(
                snapshot_counter = snapshot_counter,
                "Restoring store from snapshot"
            );
            
            // Initialize the snapshot file counter to continue from the next number
            snapshot_manager.initialize_counter(&mut locks).await?;
            
            {
                let store_guard = locks.store();
                store_guard.inner_mut().disable_notifications();
                store_guard.inner_mut().restore_snapshot(snapshot);
                store_guard.inner_mut().enable_notifications();
            }
        } else {
            info!("No snapshot found, starting with empty store");
            
            // Initialize the snapshot file counter
            snapshot_manager.initialize_counter(&mut locks).await?;
        }
    }

    // Start the write channel consumer task
    let app_state_clone = Arc::clone(&app_state);
    let mut write_channel_task = tokio::spawn(async move {
        if let Err(e) = consume_write_channel(app_state_clone).await {
            error!(
                error = %e,
                "Write channel consumer failed"
            );
        }
    });

    // Create the peer service handle
    let peer_handle = PeerService::spawn(Arc::clone(&app_state));

    // Start the peer WebSocket server task
    let peer_handle_clone = peer_handle.clone();
    let mut peer_server_task = tokio::spawn(async move {
        if let Err(e) = peer_handle_clone.start_inbound_server().await {
            error!(
                error = %e,
                "Peer server failed"
            );
        }
    });

    // Start the client WebSocket server task
    let app_state_clone = Arc::clone(&app_state);
    let mut client_server_task = tokio::spawn(async move {
        if let Err(e) = start_client_server(app_state_clone).await {
            error!(
                error = %e,
                "Client server failed"
            );
        }
    });

    // Start the outbound peer connection manager task
    let peer_handle_clone = peer_handle.clone();
    let mut outbound_peer_task = tokio::spawn(async move {
        if let Err(e) = peer_handle_clone.manage_outbound_connections().await {
            error!(
                error = %e,
                "Outbound peer connection manager failed"
            );
        }
    });

    // Start the misc tasks handler
    let app_state_clone = Arc::clone(&app_state);
    let mut misc_task = tokio::spawn(async move {
        if let Err(e) = handle_misc_tasks(app_state_clone).await {
            error!(
                error = %e,
                "Misc tasks handler failed"
            );
        }
    });

    // Start the heartbeat writer
    let app_state_clone = Arc::clone(&app_state);
    let mut heartbeat_task = tokio::spawn(async move {
        if let Err(e) = handle_heartbeat_writing(app_state_clone).await {
            error!(
                error = %e,
                "Heartbeat writer failed"
            );
        }
    });

    // Wait for either shutdown signal or any critical task to complete/fail
    tokio::select! {
        _ = signal::ctrl_c() => {
            warn!("Received shutdown signal, initiating graceful shutdown");
        }
        result = &mut write_channel_task => {
            match result {
                Ok(_) => error!("Write channel task exited unexpectedly"),
                Err(e) => error!(error = %e, "Write channel task failed"),
            }
            warn!("Critical task failure detected, initiating shutdown");
        }
        result = &mut peer_server_task => {
            match result {
                Ok(_) => error!("Peer server task exited unexpectedly"),
                Err(e) => error!(error = %e, "Peer server task failed"),
            }
            warn!("Critical task failure detected, initiating shutdown");
        }
        result = &mut client_server_task => {
            match result {
                Ok(_) => error!("Client server task exited unexpectedly"),
                Err(e) => error!(error = %e, "Client server task failed"),
            }
            warn!("Critical task failure detected, initiating shutdown");
        }
        result = &mut outbound_peer_task => {
            match result {
                Ok(_) => error!("Outbound peer task exited unexpectedly"),
                Err(e) => error!(error = %e, "Outbound peer task failed"),
            }
            warn!("Critical task failure detected, initiating shutdown");
        }
        result = &mut misc_task => {
            match result {
                Ok(_) => error!("Misc task exited unexpectedly"),
                Err(e) => error!(error = %e, "Misc task failed"),
            }
            warn!("Critical task failure detected, initiating shutdown");
        }
        result = &mut heartbeat_task => {
            match result {
                Ok(_) => error!("Heartbeat task exited unexpectedly"),
                Err(e) => error!(error = %e, "Heartbeat task failed"),
            }
            warn!("Critical task failure detected, initiating shutdown");
        }
    }

    let mut locks = app_state.acquire_locks(LockRequest {
        core_state: true,
        store: true,
        ..Default::default()
    }).await;

    // Take a final snapshot before shutting down
    info!("Taking final snapshot before shutdown");
    let snapshot = locks.store().inner().take_snapshot();
    
    match save_snapshot(&snapshot, &mut locks).await {
        Ok(snapshot_counter) => {
            info!(
                snapshot_counter = snapshot_counter,
                "Final snapshot saved successfully"
            );
            
            // Write a snapshot marker to the WAL to indicate the final snapshot point
            // This helps during replay to know that the state was snapshotted at shutdown
            let snapshot_request = qlib_rs::Request::Snapshot {
                snapshot_counter,
                timestamp: Some(now()),
                originator: Some({
                    let core = locks.core_state();
                    core.config.machine.clone()
                }),
            };
            
            if let Err(e) = write_request_to_wal(&snapshot_request, &mut locks, true).await {
                error!(error = %e, "Failed to write final snapshot marker to WAL");
            } else {
                info!("Final snapshot marker written to WAL");
            }
        }
        Err(e) => {
            error!(error = %e, "Failed to save final snapshot");
        }
    }

    // Abort all tasks
    info!("Stopping all background tasks");
    write_channel_task.abort();
    peer_server_task.abort();
    client_server_task.abort();
    outbound_peer_task.abort();
    misc_task.abort();
    heartbeat_task.abort();

    info!("QCore service shutdown complete");
    Ok(())
}
