use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use uuid::Uuid;

use crate::app::App;
use crate::store::{CommandRequest, CommandResponse};
use qlib_rs::Context;

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

        // Response messages don't need handling here as they're sent by the client
        _ => None,
    }
}

async fn handle_perform_request(req: CommandRequest, app: &Arc<App>) -> CommandResponse {
    // Check if it's a read-only request (UpdateEntity with only Read requests)
    if let CommandRequest::UpdateEntity { request } = &req {
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

    if let CommandRequest::GetSchema { entity_type } = &req {
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

    if let CommandRequest::GetCompleteSchema { entity_type } = &req {
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
