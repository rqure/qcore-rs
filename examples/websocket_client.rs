use std::collections::BTreeSet;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// Copy the WebSocketMessage enum from the main crate
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum WebSocketMessage {
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
        response: Result<String, String>,
    },
    ChangeMembership {
        id: String,
        members: BTreeSet<u64>,
    },
    ChangeMembershipResponse {
        id: String,
        response: Result<String, String>,
    },
    GetMetrics {
        id: String,
    },
    GetMetricsResponse {
        id: String,
        response: serde_json::Value, // Simplified for the example
    },
    
    // Connection management
    Error {
        id: String,
        error: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the WebSocket server
    let url = "ws://127.0.0.1:8080";
    let (ws_stream, _) = connect_async(url).await?;
    println!("Connected to WebSocket server at {}", url);

    let (mut ws_sender, mut ws_receiver) = ws_stream.split();

    // Initialize a single-node cluster
    let init_message = WebSocketMessage::Init {
        id: Uuid::new_v4().to_string(),
        nodes: vec![], // Empty means single-node cluster
    };

    let init_json = serde_json::to_string(&init_message)?;
    ws_sender.send(Message::Text(init_json)).await?;
    println!("Sent init message");

    // Listen for responses
    while let Some(msg) = ws_receiver.next().await {
        match msg? {
            Message::Text(text) => {
                if let Ok(ws_msg) = serde_json::from_str::<WebSocketMessage>(&text) {
                    match ws_msg {
                        WebSocketMessage::InitResponse { id, response } => {
                            println!("Init response ({}): {:?}", id, response);
                            
                            // After successful init, get metrics
                            let metrics_message = WebSocketMessage::GetMetrics {
                                id: Uuid::new_v4().to_string(),
                            };
                            let metrics_json = serde_json::to_string(&metrics_message)?;
                            ws_sender.send(Message::Text(metrics_json)).await?;
                            println!("Sent metrics request");
                        }
                        WebSocketMessage::GetMetricsResponse { id, response } => {
                            println!("Metrics response ({}): {}", id, serde_json::to_string_pretty(&response)?);
                            break; // Exit after getting metrics
                        }
                        WebSocketMessage::Error { id, error } => {
                            eprintln!("Error ({}): {}", id, error);
                            break;
                        }
                        _ => {
                            println!("Received other message: {:?}", ws_msg);
                        }
                    }
                } else {
                    println!("Received non-JSON message: {}", text);
                }
            }
            Message::Close(_) => {
                println!("Connection closed");
                break;
            }
            _ => {}
        }
    }

    Ok(())
}
