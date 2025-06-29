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
use tokio::sync::{RwLock, Mutex, oneshot};
use std::time::Duration;

use crate::app::typ;
use crate::app::NodeId;
use crate::app::TypeConfig;
use crate::websocket::WebSocketMessage;

type WsStream = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

struct PersistentConnection {
    sender: Arc<Mutex<futures_util::stream::SplitSink<WsStream, tokio_tungstenite::tungstenite::Message>>>,
    pending_requests: Arc<Mutex<HashMap<String, oneshot::Sender<serde_json::Value>>>>,
}

impl PersistentConnection {
    async fn new(addr: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("ws://{}", addr);
        let (ws_stream, _) = tokio_tungstenite::connect_async(url).await?;
        let (ws_sender, mut ws_receiver) = ws_stream.split();
        
        let sender = Arc::new(Mutex::new(ws_sender));
        let pending_requests: Arc<Mutex<HashMap<String, oneshot::Sender<serde_json::Value>>>> = Arc::new(Mutex::new(HashMap::new()));
        
        // Spawn a task to handle incoming messages
        let pending_requests_clone = pending_requests.clone();
        tokio::spawn(async move {
            while let Some(msg) = ws_receiver.next().await {
                match msg {
                    Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                        if let Ok(ws_msg) = serde_json::from_str::<WebSocketMessage>(&text) {
                            let request_id = match &ws_msg {
                                WebSocketMessage::RaftVoteResponse { id, .. } => Some(id.clone()),
                                WebSocketMessage::RaftAppendResponse { id, .. } => Some(id.clone()),
                                WebSocketMessage::RaftSnapshotResponse { id, .. } => Some(id.clone()),
                                WebSocketMessage::Error { id, .. } => Some(id.clone()),
                                _ => None,
                            };
                            
                            if let Some(id) = request_id {
                                let mut pending = pending_requests_clone.lock().await;
                                if let Some(sender) = pending.remove(&id) {
                                    let response_value = serde_json::to_value(&ws_msg).unwrap_or_default();
                                    let _ = sender.send(response_value);
                                }
                            }
                        }
                    }
                    Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => {
                        log::warn!("WebSocket connection closed");
                        break;
                    }
                    Err(e) => {
                        log::error!("WebSocket error: {}", e);
                        break;
                    }
                    _ => {}
                }
            }
        });
        
        Ok(PersistentConnection {
            sender,
            pending_requests,
        })
    }
    
    async fn send_request<Resp>(&self, ws_message: WebSocketMessage, request_id: String) -> Result<Resp, Box<dyn std::error::Error + Send + Sync>>
    where
        Resp: DeserializeOwned,
    {
        let (response_sender, response_receiver) = oneshot::channel();
        
        // Register the pending request
        {
            let mut pending = self.pending_requests.lock().await;
            pending.insert(request_id.clone(), response_sender);
        }
        
        // Send the message
        let message_json = serde_json::to_string(&ws_message)?;
        {
            let mut sender = self.sender.lock().await;
            sender.send(tokio_tungstenite::tungstenite::Message::Text(message_json)).await?;
        }
        
        // Wait for response with timeout
        let response = tokio::time::timeout(Duration::from_secs(30), response_receiver).await??;
        
        // Extract the actual response from the WebSocket message
        match serde_json::from_value::<WebSocketMessage>(response)? {
            WebSocketMessage::RaftVoteResponse { response, .. } => {
                let resp_bytes = serde_json::to_vec(&response)?;
                Ok(serde_json::from_slice::<Resp>(&resp_bytes)?)
            }
            WebSocketMessage::RaftAppendResponse { response, .. } => {
                let resp_bytes = serde_json::to_vec(&response)?;
                Ok(serde_json::from_slice::<Resp>(&resp_bytes)?)
            }
            WebSocketMessage::RaftSnapshotResponse { response, .. } => {
                let resp_bytes = serde_json::to_vec(&response)?;
                Ok(serde_json::from_slice::<Resp>(&resp_bytes)?)
            }
            WebSocketMessage::Error { error, .. } => {
                Err(format!("Remote error: {}", error).into())
            }
            _ => Err("Unexpected response type".into()),
        }
    }
}

pub struct Network {
    connections: Arc<RwLock<HashMap<NodeId, Arc<PersistentConnection>>>>,
}

impl Default for Network {
    fn default() -> Self {
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Network {
    async fn get_or_create_connection(&self, target: NodeId, addr: &str) -> Result<Arc<PersistentConnection>, Box<dyn std::error::Error + Send + Sync>> {
        // First try to get existing connection
        {
            let connections = self.connections.read().await;
            if let Some(conn) = connections.get(&target) {
                return Ok(conn.clone());
            }
        }
        
        // Create new connection
        let new_conn = Arc::new(PersistentConnection::new(addr).await?);
        
        // Store the connection
        {
            let mut connections = self.connections.write().await;
            connections.insert(target, new_conn.clone());
        }
        
        Ok(new_conn)
    }

    pub async fn send_rpc<Req, Resp, Err>(
        &self,
        target: NodeId,
        target_node: &BasicNode,
        uri: &str,
        req: Req,
    ) -> Result<Resp, openraft::error::RPCError<NodeId, BasicNode, Err>>
    where
        Req: Serialize + Send + 'static,
        Err: std::error::Error + DeserializeOwned + Send + 'static,
        Resp: DeserializeOwned + Send + 'static,
    {
        let addr = &target_node.addr;
        
        // Get or create persistent connection
        let connection = match self.get_or_create_connection(target, addr).await {
            Ok(conn) => conn,
            Err(e) => {
                let error_msg = e.to_string();
                if error_msg.contains("connect") {
                    return Err(openraft::error::RPCError::Unreachable(Unreachable::new(&std::io::Error::new(std::io::ErrorKind::ConnectionRefused, error_msg))));
                }
                return Err(openraft::error::RPCError::Network(NetworkError::new(&std::io::Error::new(std::io::ErrorKind::Other, error_msg))));
            }
        };
        
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

        // Send the request using the persistent connection
        match connection.send_request::<Resp>(ws_message, request_id).await {
            Ok(response) => Ok(response),
            Err(e) => {
                // Remove the failed connection so it can be recreated next time
                let mut connections = self.connections.write().await;
                connections.remove(&target);
                
                let error_msg = e.to_string();
                if error_msg.contains("connect") || error_msg.contains("timeout") {
                    Err(openraft::error::RPCError::Unreachable(Unreachable::new(&std::io::Error::new(std::io::ErrorKind::TimedOut, error_msg))))
                } else {
                    Err(openraft::error::RPCError::Network(NetworkError::new(&std::io::Error::new(std::io::ErrorKind::Other, error_msg))))
                }
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