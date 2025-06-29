use openraft::error::InstallSnapshotError;
use openraft::error::NetworkError;
use openraft::error::Unreachable;
use openraft::network::RPCOption;
use openraft::network::RaftNetwork;
use openraft::network::RaftNetworkFactory;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::BasicNode;
use serde::de::DeserializeOwned;
use serde::Serialize;
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::app::typ;
use crate::app::NodeId;
use crate::app::TypeConfig;
use crate::websocket::WebSocketMessage;

pub struct Network {
    connections: Arc<RwLock<HashMap<NodeId, tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>>>>,
}

impl Default for Network {
    fn default() -> Self {
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Network {
    pub async fn send_rpc<Req, Resp, Err>(
        &self,
        _target: NodeId,
        target_node: &BasicNode,
        uri: &str,
        req: Req,
    ) -> Result<Resp, openraft::error::RPCError<NodeId, BasicNode, Err>>
    where
        Req: Serialize + Send + 'static,
        Err: std::error::Error + DeserializeOwned + Send + 'static,
        Resp: DeserializeOwned + Send + 'static,
    {
        // For WebSocket communication, we'll use a simplified approach
        // In a real implementation, you might want to maintain persistent connections
        let addr = &target_node.addr;
        let url = format!("ws://{}", addr);

        match tokio_tungstenite::connect_async(url).await {
            Ok((ws_stream, _)) => {
                let (mut ws_sender, mut ws_receiver) = ws_stream.split();
                
                // Create a unique ID for this request
                let request_id = uuid::Uuid::new_v4().to_string();
                
                // Create the appropriate WebSocket message based on the URI
                let ws_message = match uri {
                    "raft/vote" => {
                        if let Ok(vote_req) = serde_json::from_str::<VoteRequest<NodeId>>(&serde_json::to_string(&req).unwrap()) {
                            WebSocketMessage::RaftVote {
                                id: request_id.clone(),
                                request: vote_req,
                            }
                        } else {
                            return Err(openraft::error::RPCError::Network(NetworkError::new(&std::io::Error::new(std::io::ErrorKind::InvalidData, "Failed to serialize vote request"))));
                        }
                    }
                    "raft/append" => {
                        if let Ok(append_req) = serde_json::from_str::<AppendEntriesRequest<TypeConfig>>(&serde_json::to_string(&req).unwrap()) {
                            WebSocketMessage::RaftAppend {
                                id: request_id.clone(),
                                request: append_req,
                            }
                        } else {
                            return Err(openraft::error::RPCError::Network(NetworkError::new(&std::io::Error::new(std::io::ErrorKind::InvalidData, "Failed to serialize append request"))));
                        }
                    }
                    "raft/snapshot" => {
                        if let Ok(snapshot_req) = serde_json::from_str::<InstallSnapshotRequest<TypeConfig>>(&serde_json::to_string(&req).unwrap()) {
                            WebSocketMessage::RaftSnapshot {
                                id: request_id.clone(),
                                request: snapshot_req,
                            }
                        } else {
                            return Err(openraft::error::RPCError::Network(NetworkError::new(&std::io::Error::new(std::io::ErrorKind::InvalidData, "Failed to serialize snapshot request"))));
                        }
                    }
                    _ => {
                        return Err(openraft::error::RPCError::Network(NetworkError::new(&std::io::Error::new(std::io::ErrorKind::InvalidInput, "Unknown RPC endpoint"))));
                    }
                };

                // Send the request
                let message_json = serde_json::to_string(&ws_message)
                    .map_err(|e| openraft::error::RPCError::Network(NetworkError::new(&e)))?;
                
                ws_sender.send(tokio_tungstenite::tungstenite::Message::Text(message_json)).await
                    .map_err(|e| openraft::error::RPCError::Network(NetworkError::new(&e)))?;

                // Wait for response
                while let Some(msg) = ws_receiver.next().await {
                    match msg {
                        Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                            if let Ok(ws_msg) = serde_json::from_str::<WebSocketMessage>(&text) {
                                match ws_msg {
                                    WebSocketMessage::RaftVoteResponse { id, response } if id == request_id => {
                                        if let Ok(resp_bytes) = serde_json::to_vec(&response) {
                                            if let Ok(typed_response) = serde_json::from_slice::<Resp>(&resp_bytes) {
                                                return Ok(typed_response);
                                            }
                                        }
                                        return Err(openraft::error::RPCError::Network(NetworkError::new(&std::io::Error::new(std::io::ErrorKind::InvalidData, "Failed to deserialize response"))));
                                    }
                                    WebSocketMessage::RaftAppendResponse { id, response } if id == request_id => {
                                        if let Ok(resp_bytes) = serde_json::to_vec(&response) {
                                            if let Ok(typed_response) = serde_json::from_slice::<Resp>(&resp_bytes) {
                                                return Ok(typed_response);
                                            }
                                        }
                                        return Err(openraft::error::RPCError::Network(NetworkError::new(&std::io::Error::new(std::io::ErrorKind::InvalidData, "Failed to deserialize response"))));
                                    }
                                    WebSocketMessage::RaftSnapshotResponse { id, response } if id == request_id => {
                                        if let Ok(resp_bytes) = serde_json::to_vec(&response) {
                                            if let Ok(typed_response) = serde_json::from_slice::<Resp>(&resp_bytes) {
                                                return Ok(typed_response);
                                            }
                                        }
                                        return Err(openraft::error::RPCError::Network(NetworkError::new(&std::io::Error::new(std::io::ErrorKind::InvalidData, "Failed to deserialize response"))));
                                    }
                                    WebSocketMessage::Error { error, .. } => {
                                        return Err(openraft::error::RPCError::Network(NetworkError::new(&std::io::Error::new(std::io::ErrorKind::Other, error))));
                                    }
                                    _ => continue, // Ignore other messages
                                }
                            }
                        }
                        Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => {
                            return Err(openraft::error::RPCError::Network(NetworkError::new(&std::io::Error::new(std::io::ErrorKind::ConnectionAborted, "WebSocket connection closed"))));
                        }
                        Err(e) => {
                            return Err(openraft::error::RPCError::Network(NetworkError::new(&e)));
                        }
                        _ => continue,
                    }
                }

                Err(openraft::error::RPCError::Network(NetworkError::new(&std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "No response received"))))
            }
            Err(e) => {
                // If the error is a connection error, we return `Unreachable` so that connection isn't retried
                // immediately.
                if e.to_string().contains("connect") {
                    return Err(openraft::error::RPCError::Unreachable(Unreachable::new(&e)));
                }
                Err(openraft::error::RPCError::Network(NetworkError::new(&e)))
            }
        }
    }
}

// NOTE: This could be implemented also on `Arc<ExampleNetwork>`, but since it's empty, implemented
// directly.
impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = NetworkConnection;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        NetworkConnection {
            owner: Network::default(),
            target,
            target_node: node.clone(),
        }
    }
}

pub struct NetworkConnection {
    owner: Network,
    target: NodeId,
    target_node: BasicNode,
}

impl RaftNetwork<TypeConfig> for NetworkConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, typ::RPCError> {
        self.owner.send_rpc(self.target, &self.target_node, "raft/append", req).await
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>, typ::RPCError<InstallSnapshotError>> {
        self.owner.send_rpc(self.target, &self.target_node, "raft/snapshot", req).await
    }

    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, typ::RPCError> {
        self.owner.send_rpc(self.target, &self.target_node, "raft/vote", req).await
    }
}