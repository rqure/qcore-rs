use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use uuid::Uuid;

use crate::app::App;
use crate::store::{CommandRequest, CommandResponse};
use qlib_rs::{Context, NotifyConfig, NotifyToken, NotificationCallback, Notification};

#[derive(Debug, Serialize, Deserialize)]
pub enum WebSocketMessage {
    // Raft internal messages
    RaftVote {
        id: String,
        request: openraft::raft::VoteRequest<u64>,
    },
    RaftVoteResponse {
        id: String,
        response: Result<openraft::raft::VoteResponse<u64>, String>,
    },
    RaftAppend {
        id: String,
        request: openraft::raft::AppendEntriesRequest<crate::app::TypeConfig>,
    },
    RaftAppendResponse {
        id: String,
        response: Result<openraft::raft::AppendEntriesResponse<u64>, String>,
    },
    RaftSnapshot {
        id: String,
        request: openraft::raft::InstallSnapshotRequest<crate::app::TypeConfig>,
    },
    RaftSnapshotResponse {
        id: String,
        response: Result<openraft::raft::InstallSnapshotResponse<u64>, String>,
    },

    // Management API messages
    Init {
        id: String,
        nodes: Vec<(u64, String)>,
    },
    InitResponse {
        id: String,
        response: Result<(), String>,
    },
    AddLearner {
        id: String,
        node_id: u64,
        node_addr: String,
    },
    AddLearnerResponse {
        id: String,
        response: Result<String, String>, // Simplified response
    },
    ChangeMembership {
        id: String,
        members: std::collections::BTreeSet<u64>,
    },
    ChangeMembershipResponse {
        id: String,
        response: Result<String, String>, // Simplified response
    },
    GetMetrics {
        id: String,
    },
    GetMetricsResponse {
        id: String,
        response: openraft::RaftMetrics<u64, openraft::BasicNode>,
    },

    // Application API messages
    Perform {
        id: String,
        request: CommandRequest,
    },
    PerformResponse {
        id: String,
        response: CommandResponse,
    },

    // Store proxy messages - same as qlib_rs::StoreMessage but with WebSocket prefix
    StoreCreateEntity {
        id: String,
        entity_type: qlib_rs::EntityType,
        parent_id: Option<qlib_rs::EntityId>,
        name: String,
    },
    StoreCreateEntityResponse {
        id: String,
        response: Result<qlib_rs::Entity, String>,
    },
    
    StoreDeleteEntity {
        id: String,
        entity_id: qlib_rs::EntityId,
    },
    StoreDeleteEntityResponse {
        id: String,
        response: Result<(), String>,
    },
    
    StoreSetEntitySchema {
        id: String,
        schema: qlib_rs::EntitySchema<qlib_rs::Single>,
    },
    StoreSetEntitySchemaResponse {
        id: String,
        response: Result<(), String>,
    },
    
    StoreGetEntitySchema {
        id: String,
        entity_type: qlib_rs::EntityType,
    },
    StoreGetEntitySchemaResponse {
        id: String,
        response: Result<Option<qlib_rs::EntitySchema<qlib_rs::Single>>, String>,
    },
    
    StoreGetCompleteEntitySchema {
        id: String,
        entity_type: qlib_rs::EntityType,
    },
    StoreGetCompleteEntitySchemaResponse {
        id: String,
        response: Result<qlib_rs::EntitySchema<qlib_rs::Complete>, String>,
    },
    
    StoreSetFieldSchema {
        id: String,
        entity_type: qlib_rs::EntityType,
        field_type: qlib_rs::FieldType,
        schema: qlib_rs::FieldSchema,
    },
    StoreSetFieldSchemaResponse {
        id: String,
        response: Result<(), String>,
    },
    
    StoreGetFieldSchema {
        id: String,
        entity_type: qlib_rs::EntityType,
        field_type: qlib_rs::FieldType,
    },
    StoreGetFieldSchemaResponse {
        id: String,
        response: Result<Option<qlib_rs::FieldSchema>, String>,
    },
    
    StoreEntityExists {
        id: String,
        entity_id: qlib_rs::EntityId,
    },
    StoreEntityExistsResponse {
        id: String,
        response: bool,
    },
    
    StoreFieldExists {
        id: String,
        entity_id: qlib_rs::EntityId,
        field_type: qlib_rs::FieldType,
    },
    StoreFieldExistsResponse {
        id: String,
        response: bool,
    },
    
    StorePerform {
        id: String,
        requests: Vec<qlib_rs::Request>,
    },
    StorePerformResponse {
        id: String,
        response: Result<Vec<qlib_rs::Request>, String>,
    },
    
    StoreFindEntities {
        id: String,
        entity_type: qlib_rs::EntityType,
        parent_id: Option<qlib_rs::EntityId>,
        page_opts: Option<qlib_rs::PageOpts>,
    },
    StoreFindEntitiesResponse {
        id: String,
        response: Result<qlib_rs::PageResult<qlib_rs::EntityId>, String>,
    },
    
    StoreFindEntitiesExact {
        id: String,
        entity_type: qlib_rs::EntityType,
        parent_id: Option<qlib_rs::EntityId>,
        page_opts: Option<qlib_rs::PageOpts>,
    },
    StoreFindEntitiesExactResponse {
        id: String,
        response: Result<qlib_rs::PageResult<qlib_rs::EntityId>, String>,
    },
    
    StoreGetEntityTypes {
        id: String,
        parent_type: Option<qlib_rs::EntityType>,
        page_opts: Option<qlib_rs::PageOpts>,
    },
    StoreGetEntityTypesResponse {
        id: String,
        response: Result<qlib_rs::PageResult<qlib_rs::EntityType>, String>,
    },
    
    StoreTakeSnapshot {
        id: String,
    },
    StoreTakeSnapshotResponse {
        id: String,
        response: qlib_rs::Snapshot,
    },
    
    StoreRestoreSnapshot {
        id: String,
        snapshot: qlib_rs::Snapshot,
    },
    StoreRestoreSnapshotResponse {
        id: String,
        response: Result<(), String>,
    },
    
    // Notification support
    StoreRegisterNotification {
        id: String,
        config: qlib_rs::NotifyConfig,
    },
    StoreRegisterNotificationResponse {
        id: String,
        response: Result<qlib_rs::NotifyToken, String>,
    },
    
    StoreUnregisterNotification {
        id: String,
        token: qlib_rs::NotifyToken,
    },
    StoreUnregisterNotificationResponse {
        id: String,
        response: bool,
    },
    
    StoreGetNotificationConfigs {
        id: String,
    },
    StoreGetNotificationConfigsResponse {
        id: String,
        response: Vec<(qlib_rs::NotifyToken, qlib_rs::NotifyConfig)>,
    },
    
    // Notification delivery
    StoreNotification {
        notification: qlib_rs::Notification,
    },

    // Connection management
    Error {
        id: String,
        error: String,
    },
}

pub async fn start_websocket_server(addr: String, app: Arc<App>) -> std::io::Result<()> {
    let listener = TcpListener::bind(&addr).await?;
    log::info!("WebSocket server listening on: {}", addr);

    while let Ok((stream, addr)) = listener.accept().await {
        let app_clone = app.clone();
        tokio::spawn(handle_connection(stream, addr, app_clone));
    }

    Ok(())
}

async fn handle_connection(stream: TcpStream, addr: SocketAddr, app: Arc<App>) {
    log::info!("New WebSocket connection from: {}", addr);

    let ws_stream = match accept_async(stream).await {
        Ok(ws) => ws,
        Err(e) => {
            log::error!("Failed to accept WebSocket connection: {}", e);
            return;
        }
    };

    let (mut ws_sender, mut ws_receiver) = ws_stream.split();

    while let Some(msg) = ws_receiver.next().await {
        match msg {
            Ok(Message::Text(text)) => match serde_json::from_str::<WebSocketMessage>(&text) {
                Ok(ws_msg) => {
                    let response = handle_websocket_message(ws_msg, &app).await;
                    if let Some(response_msg) = response {
                        match serde_json::to_string(&response_msg) {
                            Ok(response_text) => {
                                if let Err(e) = ws_sender.send(Message::Text(response_text)).await {
                                    log::error!("Failed to send WebSocket response: {}", e);
                                    break;
                                }
                            }
                            Err(e) => {
                                log::error!("Failed to serialize response: {}", e);
                            }
                        }
                    }
                }
                Err(e) => {
                    log::error!("Failed to parse WebSocket message: {}", e);
                    let error_msg = WebSocketMessage::Error {
                        id: Uuid::new_v4().to_string(),
                        error: format!("Failed to parse message: {}", e),
                    };
                    if let Ok(error_text) = serde_json::to_string(&error_msg) {
                        let _ = ws_sender.send(Message::Text(error_text)).await;
                    }
                }
            },
            Ok(Message::Binary(_)) => {
                log::warn!("Received binary message, ignoring");
            }
            Ok(Message::Close(_)) => {
                log::info!("WebSocket connection closed by client");
                break;
            }
            Err(e) => {
                log::error!("WebSocket error: {}", e);
                break;
            }
            _ => {}
        }
    }

    log::info!("WebSocket connection closed: {}", addr);
}

async fn handle_websocket_message(
    msg: WebSocketMessage,
    app: &Arc<App>,
) -> Option<WebSocketMessage> {
    match msg {
        // Raft internal messages
        WebSocketMessage::RaftVote { id, request } => {
            match app.raft.vote(request).await {
                Ok(response) => Some(WebSocketMessage::RaftVoteResponse {
                    id,
                    response: Ok(response),
                }),
                Err(e) => {
                    let error_msg = format!("{:?}", e); // Use Debug to avoid potential Display panics
                    log::error!("Vote request failed: {}", error_msg);
                    Some(WebSocketMessage::RaftVoteResponse {
                        id,
                        response: Err(error_msg),
                    })
                }
            }
        }
        WebSocketMessage::RaftAppend { id, request } => {
            match app.raft.append_entries(request).await {
                Ok(response) => Some(WebSocketMessage::RaftAppendResponse {
                    id,
                    response: Ok(response),
                }),
                Err(e) => {
                    let error_msg = format!("{:?}", e); // Use Debug to avoid potential Display panics
                    log::error!("Append request failed: {}", error_msg);
                    Some(WebSocketMessage::RaftAppendResponse {
                        id,
                        response: Err(error_msg),
                    })
                }
            }
        }
        WebSocketMessage::RaftSnapshot { id, request } => {
            match app.raft.install_snapshot(request).await {
                Ok(response) => Some(WebSocketMessage::RaftSnapshotResponse {
                    id,
                    response: Ok(response),
                }),
                Err(e) => {
                    let error_msg = format!("{:?}", e); // Use Debug to avoid potential Display panics
                    log::error!("Snapshot request failed: {}", error_msg);
                    Some(WebSocketMessage::RaftSnapshotResponse {
                        id,
                        response: Err(error_msg),
                    })
                }
            }
        }

        // Management API messages
        WebSocketMessage::Init { id, nodes } => {
            use openraft::BasicNode;
            use std::collections::BTreeMap;

            let mut node_map = BTreeMap::new();
            if nodes.is_empty() {
                node_map.insert(
                    app.id,
                    BasicNode {
                        addr: app.addr.clone(),
                    },
                );
            } else {
                for (node_id, addr) in nodes {
                    node_map.insert(node_id, BasicNode { addr });
                }
            }
            let response = app.raft.initialize(node_map).await;
            let simplified_response = response.map(|_| ()).map_err(|e| e.to_string());
            Some(WebSocketMessage::InitResponse {
                id,
                response: simplified_response,
            })
        }
        WebSocketMessage::AddLearner {
            id,
            node_id,
            node_addr,
        } => {
            use openraft::BasicNode;
            let node = BasicNode { addr: node_addr };
            let response = app.raft.add_learner(node_id, node, true).await;
            let simplified_response = response
                .map(|_| "Success".to_string())
                .map_err(|e| e.to_string());
            Some(WebSocketMessage::AddLearnerResponse {
                id,
                response: simplified_response,
            })
        }
        WebSocketMessage::ChangeMembership { id, members } => {
            let response = app.raft.change_membership(members, false).await;
            let simplified_response = response
                .map(|_| "Success".to_string())
                .map_err(|e| e.to_string());
            Some(WebSocketMessage::ChangeMembershipResponse {
                id,
                response: simplified_response,
            })
        }
        WebSocketMessage::GetMetrics { id } => {
            let metrics = app.raft.metrics().borrow().clone();
            Some(WebSocketMessage::GetMetricsResponse {
                id,
                response: metrics,
            })
        }

        // Application API messages
        WebSocketMessage::Perform { id, request } => {
            let response = handle_perform_request(request, app).await;
            Some(WebSocketMessage::PerformResponse { id, response })
        }

        // Store proxy messages - handle direct store operations
        WebSocketMessage::StoreCreateEntity { id, entity_type, parent_id, name } => {
            let request = CommandRequest::CreateEntity { entity_type, parent_id, name };
            let response = handle_perform_request(request, app).await;
            match response {
                CommandResponse::CreateEntity { response, error } => {
                    if let Some(err) = error {
                        Some(WebSocketMessage::StoreCreateEntityResponse {
                            id,
                            response: Err(err),
                        })
                    } else if let Some(entity) = response {
                        Some(WebSocketMessage::StoreCreateEntityResponse {
                            id,
                            response: Ok(entity),
                        })
                    } else {
                        Some(WebSocketMessage::StoreCreateEntityResponse {
                            id,
                            response: Err("No entity returned".to_string()),
                        })
                    }
                }
                _ => Some(WebSocketMessage::StoreCreateEntityResponse {
                    id,
                    response: Err("Unexpected response type".to_string()),
                }),
            }
        }

        WebSocketMessage::StoreDeleteEntity { id, entity_id } => {
            let request = CommandRequest::DeleteEntity { entity_id };
            let response = handle_perform_request(request, app).await;
            match response {
                CommandResponse::DeleteEntity { error } => {
                    if let Some(err) = error {
                        Some(WebSocketMessage::StoreDeleteEntityResponse {
                            id,
                            response: Err(err),
                        })
                    } else {
                        Some(WebSocketMessage::StoreDeleteEntityResponse {
                            id,
                            response: Ok(()),
                        })
                    }
                }
                _ => Some(WebSocketMessage::StoreDeleteEntityResponse {
                    id,
                    response: Err("Unexpected response type".to_string()),
                }),
            }
        }

        WebSocketMessage::StoreSetEntitySchema { id, schema } => {
            let request = CommandRequest::SetSchema { entity_schema: schema };
            let response = handle_perform_request(request, app).await;
            match response {
                CommandResponse::SetSchema { error } => {
                    if let Some(err) = error {
                        Some(WebSocketMessage::StoreSetEntitySchemaResponse {
                            id,
                            response: Err(err),
                        })
                    } else {
                        Some(WebSocketMessage::StoreSetEntitySchemaResponse {
                            id,
                            response: Ok(()),
                        })
                    }
                }
                _ => Some(WebSocketMessage::StoreSetEntitySchemaResponse {
                    id,
                    response: Err("Unexpected response type".to_string()),
                }),
            }
        }

        WebSocketMessage::StoreGetEntitySchema { id, entity_type } => {
            let request = CommandRequest::GetSchema { entity_type };
            let response = handle_perform_request(request, app).await;
            match response {
                CommandResponse::GetSchema { response, error } => {
                    if let Some(err) = error {
                        Some(WebSocketMessage::StoreGetEntitySchemaResponse {
                            id,
                            response: Err(err),
                        })
                    } else {
                        Some(WebSocketMessage::StoreGetEntitySchemaResponse {
                            id,
                            response: Ok(response),
                        })
                    }
                }
                _ => Some(WebSocketMessage::StoreGetEntitySchemaResponse {
                    id,
                    response: Err("Unexpected response type".to_string()),
                }),
            }
        }

        WebSocketMessage::StoreGetCompleteEntitySchema { id, entity_type } => {
            let request = CommandRequest::GetCompleteSchema { entity_type };
            let response = handle_perform_request(request, app).await;
            match response {
                CommandResponse::GetCompleteSchema { response, error } => {
                    if let Some(err) = error {
                        Some(WebSocketMessage::StoreGetCompleteEntitySchemaResponse {
                            id,
                            response: Err(err),
                        })
                    } else if let Some(schema) = response {
                        Some(WebSocketMessage::StoreGetCompleteEntitySchemaResponse {
                            id,
                            response: Ok(schema),
                        })
                    } else {
                        Some(WebSocketMessage::StoreGetCompleteEntitySchemaResponse {
                            id,
                            response: Err("No schema returned".to_string()),
                        })
                    }
                }
                _ => Some(WebSocketMessage::StoreGetCompleteEntitySchemaResponse {
                    id,
                    response: Err("Unexpected response type".to_string()),
                }),
            }
        }

        WebSocketMessage::StoreEntityExists { id, entity_id } => {
            let request = CommandRequest::EntityExists { entity_id };
            let response = handle_perform_request(request, app).await;
            match response {
                CommandResponse::EntityExists { response } => {
                    Some(WebSocketMessage::StoreEntityExistsResponse { id, response })
                }
                _ => Some(WebSocketMessage::StoreEntityExistsResponse {
                    id,
                    response: false,
                }),
            }
        }

        WebSocketMessage::StoreFieldExists { id, entity_id, field_type } => {
            let request = CommandRequest::FieldExists { entity_id, field_type };
            let response = handle_perform_request(request, app).await;
            match response {
                CommandResponse::FieldExists { response } => {
                    Some(WebSocketMessage::StoreFieldExistsResponse { id, response })
                }
                _ => Some(WebSocketMessage::StoreFieldExistsResponse {
                    id,
                    response: false,
                }),
            }
        }

        WebSocketMessage::StorePerform { id, requests } => {
            let request = CommandRequest::UpdateEntity { request: requests };
            let response = handle_perform_request(request, app).await;
            match response {
                CommandResponse::UpdateEntity { response, error } => {
                    if let Some(err) = error {
                        Some(WebSocketMessage::StorePerformResponse {
                            id,
                            response: Err(err),
                        })
                    } else {
                        Some(WebSocketMessage::StorePerformResponse {
                            id,
                            response: Ok(response),
                        })
                    }
                }
                _ => Some(WebSocketMessage::StorePerformResponse {
                    id,
                    response: Err("Unexpected response type".to_string()),
                }),
            }
        }

        WebSocketMessage::StoreFindEntities { id, entity_type, parent_id, page_opts } => {
            let request = CommandRequest::FindEntities { entity_type, parent_id, page_opts };
            let response = handle_perform_request(request, app).await;
            match response {
                CommandResponse::FindEntities { response } => {
                    Some(WebSocketMessage::StoreFindEntitiesResponse { id, response })
                }
                _ => Some(WebSocketMessage::StoreFindEntitiesResponse {
                    id,
                    response: Err("Unexpected response type".to_string()),
                }),
            }
        }

        WebSocketMessage::StoreFindEntitiesExact { id, entity_type, parent_id, page_opts } => {
            let request = CommandRequest::FindEntitiesExact { entity_type, parent_id, page_opts };
            let response = handle_perform_request(request, app).await;
            match response {
                CommandResponse::FindEntitiesExact { response } => {
                    Some(WebSocketMessage::StoreFindEntitiesExactResponse { id, response })
                }
                _ => Some(WebSocketMessage::StoreFindEntitiesExactResponse {
                    id,
                    response: Err("Unexpected response type".to_string()),
                }),
            }
        }

        WebSocketMessage::StoreGetEntityTypes { id, parent_type, page_opts } => {
            let request = CommandRequest::GetEntityTypes { parent_type, page_opts };
            let response = handle_perform_request(request, app).await;
            match response {
                CommandResponse::GetEntityTypes { response } => {
                    Some(WebSocketMessage::StoreGetEntityTypesResponse { id, response })
                }
                _ => Some(WebSocketMessage::StoreGetEntityTypesResponse {
                    id,
                    response: Err("Unexpected response type".to_string()),
                }),
            }
        }

        WebSocketMessage::StoreTakeSnapshot { id } => {
            let request = CommandRequest::TakeSnapshot;
            let response = handle_perform_request(request, app).await;
            match response {
                CommandResponse::TakeSnapshot { response } => {
                    Some(WebSocketMessage::StoreTakeSnapshotResponse { id, response })
                }
                _ => {
                    // Fallback to empty snapshot
                    Some(WebSocketMessage::StoreTakeSnapshotResponse {
                        id,
                        response: qlib_rs::Snapshot::default(),
                    })
                }
            }
        }

        WebSocketMessage::StoreRestoreSnapshot { id, snapshot } => {
            let request = CommandRequest::RestoreSnapshot { snapshot };
            let response = handle_perform_request(request, app).await;
            match response {
                CommandResponse::RestoreSnapshot { error } => {
                    if let Some(err) = error {
                        Some(WebSocketMessage::StoreRestoreSnapshotResponse {
                            id,
                            response: Err(err),
                        })
                    } else {
                        Some(WebSocketMessage::StoreRestoreSnapshotResponse {
                            id,
                            response: Ok(()),
                        })
                    }
                }
                _ => Some(WebSocketMessage::StoreRestoreSnapshotResponse {
                    id,
                    response: Err("Unexpected response type".to_string()),
                }),
            }
        }

        WebSocketMessage::StoreRegisterNotification { id, config } => {
            let request = CommandRequest::RegisterNotification { config };
            let response = handle_perform_request(request, app).await;
            match response {
                CommandResponse::RegisterNotification { response } => {
                    Some(WebSocketMessage::StoreRegisterNotificationResponse { id, response })
                }
                _ => Some(WebSocketMessage::StoreRegisterNotificationResponse {
                    id,
                    response: Err("Unexpected response type".to_string()),
                }),
            }
        }

        WebSocketMessage::StoreUnregisterNotification { id, token } => {
            let request = CommandRequest::UnregisterNotification { token };
            let response = handle_perform_request(request, app).await;
            match response {
                CommandResponse::UnregisterNotification { response } => {
                    Some(WebSocketMessage::StoreUnregisterNotificationResponse { id, response })
                }
                _ => Some(WebSocketMessage::StoreUnregisterNotificationResponse {
                    id,
                    response: false,
                }),
            }
        }

        WebSocketMessage::StoreGetNotificationConfigs { id } => {
            let request = CommandRequest::GetNotificationConfigs;
            let response = handle_perform_request(request, app).await;
            match response {
                CommandResponse::GetNotificationConfigs { response } => {
                    Some(WebSocketMessage::StoreGetNotificationConfigsResponse { id, response })
                }
                _ => Some(WebSocketMessage::StoreGetNotificationConfigsResponse {
                    id,
                    response: Vec::new(),
                }),
            }
        }

        // Response messages don't need handling here as they're sent by the client
        _ => None,
    }
}

async fn handle_perform_request(req: CommandRequest, app: &Arc<App>) -> CommandResponse {
    // Handle read-only requests directly without going through Raft
    match &req {
        CommandRequest::UpdateEntity { request } => {
            // Check if it's a read-only request (UpdateEntity with only Read requests)
            if request
                .iter()
                .all(|r| matches!(r, qlib_rs::Request::Read { .. }))
            {
                let mut store_req = request.clone();
                let mut state_machine = app.state_machine_store.state_machine.write().await;
                return match state_machine.data.perform(&Context {}, &mut store_req) {
                    Ok(_) => CommandResponse::UpdateEntity {
                        response: store_req,
                        error: None,
                    },
                    Err(e) => CommandResponse::UpdateEntity {
                        response: vec![],
                        error: Some(e.to_string()),
                    },
                };
            }
        }
        CommandRequest::GetSchema { entity_type } => {
            let state_machine = app.state_machine_store.state_machine.read().await;
            return match state_machine
                .data
                .get_entity_schema(&Context {}, entity_type)
            {
                Ok(schema) => CommandResponse::GetSchema {
                    response: Some(schema),
                    error: None,
                },
                Err(e) => CommandResponse::GetSchema {
                    response: None,
                    error: Some(e.to_string()),
                },
            };
        }
        CommandRequest::GetCompleteSchema { entity_type } => {
            let state_machine = app.state_machine_store.state_machine.read().await;
            return match state_machine
                .data
                .get_complete_entity_schema(&Context {}, entity_type)
            {
                Ok(schema) => CommandResponse::GetCompleteSchema {
                    response: Some(schema),
                    error: None,
                },
                Err(e) => CommandResponse::GetCompleteSchema {
                    response: None,
                    error: Some(e.to_string()),
                },
            };
        }
        CommandRequest::GetFieldSchema { entity_type, field_type } => {
            let state_machine = app.state_machine_store.state_machine.read().await;
            return match state_machine
                .data
                .get_field_schema(&Context {}, entity_type, field_type)
            {
                Ok(schema) => CommandResponse::GetFieldSchema {
                    response: Some(schema),
                    error: None,
                },
                Err(e) => CommandResponse::GetFieldSchema {
                    response: None,
                    error: Some(e.to_string()),
                },
            };
        }
        CommandRequest::EntityExists { entity_id } => {
            let state_machine = app.state_machine_store.state_machine.read().await;
            let exists = state_machine.data.entity_exists(&Context {}, entity_id);
            return CommandResponse::EntityExists { response: exists };
        }
        CommandRequest::FieldExists { entity_id, field_type } => {
            let state_machine = app.state_machine_store.state_machine.read().await;
            let exists = state_machine.data.field_exists(&Context {}, entity_id, field_type);
            return CommandResponse::FieldExists { response: exists };
        }
        CommandRequest::FindEntities { entity_type, parent_id: _, page_opts } => {
            let state_machine = app.state_machine_store.state_machine.read().await;
            return match state_machine
                .data
                .find_entities(&Context {}, entity_type, page_opts.as_ref())
            {
                Ok(result) => CommandResponse::FindEntities {
                    response: Ok(result),
                },
                Err(e) => CommandResponse::FindEntities {
                    response: Err(e.to_string()),
                },
            };
        }
        CommandRequest::FindEntitiesExact { entity_type, parent_id: _, page_opts } => {
            let state_machine = app.state_machine_store.state_machine.read().await;
            return match state_machine
                .data
                .find_entities_exact(&Context {}, entity_type, page_opts.as_ref())
            {
                Ok(result) => CommandResponse::FindEntitiesExact {
                    response: Ok(result),
                },
                Err(e) => CommandResponse::FindEntitiesExact {
                    response: Err(e.to_string()),
                },
            };
        }
        CommandRequest::GetEntityTypes { parent_type: _, page_opts } => {
            let state_machine = app.state_machine_store.state_machine.read().await;
            return match state_machine
                .data
                .get_entity_types(&Context {}, page_opts.as_ref())
            {
                Ok(result) => CommandResponse::GetEntityTypes {
                    response: Ok(result),
                },
                Err(e) => CommandResponse::GetEntityTypes {
                    response: Err(e.to_string()),
                },
            };
        }
        CommandRequest::TakeSnapshot => {
            let state_machine = app.state_machine_store.state_machine.read().await;
            let snapshot = state_machine.data.take_snapshot(&Context {});
            return CommandResponse::TakeSnapshot {
                response: snapshot,
            };
        }
        CommandRequest::GetNotificationConfigs => {
            let state_machine = app.state_machine_store.state_machine.read().await;
            let configs = state_machine.data.get_notification_configs(&Context {});
            return CommandResponse::GetNotificationConfigs {
                response: configs,
            };
        }
        _ => {
            // Fall through to write operations handling below
        }
    }

    // For write operations, check if we're the leader first
    let metrics = app.raft.metrics().borrow().clone();
    
    // If we're not the leader and there is a leader, forward the request
    if let Some(leader_id) = metrics.current_leader {
        if leader_id != app.id {
            // We're not the leader, try to forward the request
            let leader_node = metrics.membership_config.nodes()
                .find(|(id, _)| **id == leader_id)
                .map(|(_, node)| node);
                
            if let Some(leader_node) = leader_node {
                log::info!("Forwarding write request to leader {} at {}", leader_id, leader_node.addr);
                match forward_request_to_leader(req.clone(), leader_id, app).await {
                    Ok(response) => return response,
                    Err(e) => {
                        log::warn!("Failed to forward request to leader: {}, falling back to local Raft write", e);
                        // Fall through to local Raft write attempt
                    }
                }
            } else {
                log::warn!("Leader node {} not found in membership config", leader_id);
            }
        }
    } else {
        log::debug!("No current leader or we are the leader, processing request locally");
    }

    // For all other requests (write operations), forward to the Raft client
    match app.raft.client_write(req.clone()).await {
        Ok(response) => response.response().clone(),
        Err(err) => {
            // Map error to appropriate response type based on request
            match req {
                CommandRequest::UpdateEntity { .. } => CommandResponse::UpdateEntity {
                    response: vec![],
                    error: Some(err.to_string()),
                },
                CommandRequest::CreateEntity { .. } => CommandResponse::CreateEntity {
                    response: None,
                    error: Some(err.to_string()),
                },
                CommandRequest::DeleteEntity { .. } => CommandResponse::DeleteEntity {
                    error: Some(err.to_string()),
                },
                CommandRequest::SetSchema { .. } => CommandResponse::SetSchema {
                    error: Some(err.to_string()),
                },
                _ => CommandResponse::UpdateEntity {
                    response: vec![],
                    error: Some(err.to_string()),
                },
            }
        }
    }
}

async fn forward_request_to_leader(
    req: CommandRequest,
    leader_id: u64,
    app: &Arc<App>,
) -> Result<CommandResponse, Box<dyn std::error::Error + Send + Sync>> {
    // Get the leader's address from the membership configuration
    let metrics = app.raft.metrics().borrow().clone();
    
    let leader_node = metrics.membership_config.nodes()
        .find(|(id, _)| **id == leader_id)
        .map(|(_, node)| node);
        
    if let Some(leader_node) = leader_node {
        let leader_addr = &leader_node.addr;
        
        // Use the existing network to get or create a connection to the leader
        let connection = app.network.get_or_create_connection(leader_id, leader_addr).await?;
        
        // Send the request using the persistent connection
        let response = connection.send_application_request(req).await?;
        
        Ok(response)
    } else {
        Err(format!("Leader node {} not found in membership", leader_id).into())
    }
}
