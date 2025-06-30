use clap::{Parser, Subcommand};
use futures_util::{SinkExt, StreamExt};
use qlib_rs::{EntityId, EntityType, FieldType, Value, Request, AdjustBehavior, PushCondition};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, oneshot};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use uuid::Uuid;

// Import the message types from the main crate
#[derive(Debug, Serialize, Deserialize)]
pub enum WebSocketMessage {
    // Application API messages
    Perform {
        id: String,
        request: CommandRequest,
    },
    PerformResponse {
        id: String,
        response: CommandResponse,
    },
    
    // Management API messages
    GetMetrics {
        id: String,
    },
    GetMetricsResponse {
        id: String,
        response: openraft::RaftMetrics<u64, openraft::BasicNode>,
    },
    
    // Connection management
    Error {
        id: String,
        error: String,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum CommandRequest {
    UpdateEntity {
        request: Vec<Request>,
    },
    CreateEntity {
        entity_type: EntityType,
        parent_id: Option<EntityId>,
        name: String,
    },
    DeleteEntity {
        entity_id: EntityId,
    },
    SetSchema {
        entity_schema: qlib_rs::EntitySchema<qlib_rs::Single>,
    },
    GetSchema {
        entity_type: EntityType,
    },
    GetCompleteSchema {
        entity_type: EntityType,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum CommandResponse {
    UpdateEntity {
        response: Vec<Request>,
        error: Option<String>,
    },
    CreateEntity {
        response: Option<EntityId>,
        error: Option<String>,
    },
    DeleteEntity {
        error: Option<String>,
    },
    SetSchema {
        error: Option<String>,
    },
    GetSchema {
        response: Option<qlib_rs::EntitySchema<qlib_rs::Single>>,
        error: Option<String>,
    },
    GetCompleteSchema {
        response: Option<qlib_rs::EntitySchema<qlib_rs::Complete>>,
        error: Option<String>,
    },
    Blank {},
}

#[derive(Parser)]
#[command(name = "qcore-client")]
#[command(about = "A client for interacting with qcore-rs clusters")]
#[command(version = "1.0")]
struct Cli {
    #[arg(short, long, default_value = "127.0.0.1:8080")]
    address: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Read a field from an entity
    Read {
        /// Entity ID (format: EntityType$id)
        entity_id: String,
        /// Field name to read
        field: String,
    },
    /// Write a value to an entity field
    Write {
        /// Entity ID (format: EntityType$id)  
        entity_id: String,
        /// Field name to write to
        field: String,
        /// Value to write (JSON format)
        value: String,
        /// Write behavior: set, add, subtract
        #[arg(short, long, default_value = "set")]
        behavior: String,
    },
    /// Create a new entity
    Create {
        /// Entity type
        entity_type: String,
        /// Parent entity ID (optional)
        #[arg(short, long)]
        parent: Option<String>,
        /// Entity name
        name: String,
    },
    /// Delete an entity
    Delete {
        /// Entity ID to delete
        entity_id: String,
    },
    /// Get cluster metrics
    Metrics,
    /// List entities of a given type
    List {
        /// Entity type to list
        entity_type: String,
        /// Maximum number of results
        #[arg(short, long, default_value = "10")]
        limit: usize,
    },
    /// Get schema for an entity type
    Schema {
        /// Entity type
        entity_type: String,
        /// Get complete schema (including inherited fields)
        #[arg(short, long)]
        complete: bool,
    },
}

pub struct QCoreClient {
    ws_sender: futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>
        >,
        Message
    >,
    pending_requests: Arc<Mutex<HashMap<String, oneshot::Sender<serde_json::Value>>>>,
}

impl QCoreClient {
    pub async fn connect(address: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let url = format!("ws://{}", address);
        println!("Connecting to {}", url);
        
        let (ws_stream, _) = connect_async(url).await?;
        let (ws_sender, mut ws_receiver) = ws_stream.split();
        
        let pending_requests: Arc<Mutex<HashMap<String, oneshot::Sender<serde_json::Value>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        
        // Spawn task to handle responses
        let pending_clone = pending_requests.clone();
        tokio::spawn(async move {
            while let Some(msg) = ws_receiver.next().await {
                if let Ok(Message::Text(text)) = msg {
                    if let Ok(ws_msg) = serde_json::from_str::<WebSocketMessage>(&text) {
                        match ws_msg {
                            WebSocketMessage::PerformResponse { id, response } => {
                                let mut pending = pending_clone.lock().await;
                                if let Some(sender) = pending.remove(&id) {
                                    let _ = sender.send(serde_json::to_value(response).unwrap());
                                }
                            }
                            WebSocketMessage::GetMetricsResponse { id, response } => {
                                let mut pending = pending_clone.lock().await;
                                if let Some(sender) = pending.remove(&id) {
                                    let _ = sender.send(serde_json::to_value(response).unwrap());
                                }
                            }
                            WebSocketMessage::Error { id, error } => {
                                println!("Error: {}", error);
                                let mut pending = pending_clone.lock().await;
                                if let Some(sender) = pending.remove(&id) {
                                    let _ = sender.send(serde_json::Value::Null);
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        });
        
        Ok(Self {
            ws_sender,
            pending_requests,
        })
    }
    
    pub async fn send_request<T>(&mut self, msg: WebSocketMessage) -> Result<T, Box<dyn std::error::Error>>
    where
        T: serde::de::DeserializeOwned,
    {
        let id = match &msg {
            WebSocketMessage::Perform { id, .. } => id.clone(),
            WebSocketMessage::GetMetrics { id } => id.clone(),
            _ => return Err("Invalid message type".into()),
        };
        
        let (sender, receiver) = oneshot::channel();
        {
            let mut pending = self.pending_requests.lock().await;
            pending.insert(id.clone(), sender);
        }
        
        let message_json = serde_json::to_string(&msg)?;
        self.ws_sender.send(Message::Text(message_json)).await?;
        
        let response = tokio::time::timeout(Duration::from_secs(30), receiver).await??;
        Ok(serde_json::from_value(response)?)
    }
    
    pub async fn read_field(&mut self, entity_id: &str, field: &str) -> Result<Request, Box<dyn std::error::Error>> {
        let entity_id = EntityId::try_from(entity_id)?;
        let field_type = FieldType::from(field);
        
        let request = Request::Read {
            entity_id,
            field_type,
            value: None,
            write_time: None,
            writer_id: None,
        };
        
        let msg = WebSocketMessage::Perform {
            id: Uuid::new_v4().to_string(),
            request: CommandRequest::UpdateEntity {
                request: vec![request],
            },
        };
        
        let response: CommandResponse = self.send_request(msg).await?;
        
        match response {
            CommandResponse::UpdateEntity { response, error } => {
                if let Some(err) = error {
                    return Err(err.into());
                }
                if let Some(req) = response.into_iter().next() {
                    Ok(req)
                } else {
                    Err("No response received".into())
                }
            }
            _ => Err("Unexpected response type".into()),
        }
    }
    
    pub async fn write_field(&mut self, entity_id: &str, field: &str, value: Value, behavior: AdjustBehavior) -> Result<(), Box<dyn std::error::Error>> {
        let entity_id = EntityId::try_from(entity_id)?;
        let field_type = FieldType::from(field);
        
        let request = Request::Write {
            entity_id,
            field_type,
            value: Some(value),
            push_condition: PushCondition::Always,
            adjust_behavior: behavior,
            write_time: None,
            writer_id: None,
        };
        
        let msg = WebSocketMessage::Perform {
            id: Uuid::new_v4().to_string(),
            request: CommandRequest::UpdateEntity {
                request: vec![request],
            },
        };
        
        let response: CommandResponse = self.send_request(msg).await?;
        
        match response {
            CommandResponse::UpdateEntity { error, .. } => {
                if let Some(err) = error {
                    return Err(err.into());
                }
                Ok(())
            }
            _ => Err("Unexpected response type".into()),
        }
    }
    
    pub async fn create_entity(&mut self, entity_type: &str, parent_id: Option<&str>, name: &str) -> Result<EntityId, Box<dyn std::error::Error>> {
        let entity_type = EntityType::from(entity_type);
        let parent_id = if let Some(p) = parent_id {
            Some(EntityId::try_from(p)?)
        } else {
            None
        };
        
        let msg = WebSocketMessage::Perform {
            id: Uuid::new_v4().to_string(),
            request: CommandRequest::CreateEntity {
                entity_type,
                parent_id,
                name: name.to_string(),
            },
        };
        
        let response: CommandResponse = self.send_request(msg).await?;
        
        match response {
            CommandResponse::CreateEntity { response, error } => {
                if let Some(err) = error {
                    return Err(err.into());
                }
                if let Some(entity_id) = response {
                    Ok(entity_id)
                } else {
                    Err("No entity ID returned".into())
                }
            }
            _ => Err("Unexpected response type".into()),
        }
    }
    
    pub async fn delete_entity(&mut self, entity_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        let entity_id = EntityId::try_from(entity_id)?;
        
        let msg = WebSocketMessage::Perform {
            id: Uuid::new_v4().to_string(),
            request: CommandRequest::DeleteEntity { entity_id },
        };
        
        let response: CommandResponse = self.send_request(msg).await?;
        
        match response {
            CommandResponse::DeleteEntity { error } => {
                if let Some(err) = error {
                    return Err(err.into());
                }
                Ok(())
            }
            _ => Err("Unexpected response type".into()),
        }
    }
    
    pub async fn get_metrics(&mut self) -> Result<openraft::RaftMetrics<u64, openraft::BasicNode>, Box<dyn std::error::Error>> {
        let msg = WebSocketMessage::GetMetrics {
            id: Uuid::new_v4().to_string(),
        };
        
        self.send_request(msg).await
    }
    
    pub async fn get_schema(&mut self, entity_type: &str, complete: bool) -> Result<String, Box<dyn std::error::Error>> {
        let entity_type = EntityType::from(entity_type);
        
        let request = if complete {
            CommandRequest::GetCompleteSchema { entity_type }
        } else {
            CommandRequest::GetSchema { entity_type }
        };
        
        let msg = WebSocketMessage::Perform {
            id: Uuid::new_v4().to_string(),
            request,
        };
        
        let response: CommandResponse = self.send_request(msg).await?;
        
        match response {
            CommandResponse::GetSchema { response, error } => {
                if let Some(err) = error {
                    return Err(err.into());
                }
                if let Some(schema) = response {
                    Ok(serde_json::to_string_pretty(&schema)?)
                } else {
                    Err("No schema returned".into())
                }
            }
            CommandResponse::GetCompleteSchema { response, error } => {
                if let Some(err) = error {
                    return Err(err.into());
                }
                if let Some(schema) = response {
                    Ok(serde_json::to_string_pretty(&schema)?)
                } else {
                    Err("No schema returned".into())
                }
            }
            _ => Err("Unexpected response type".into()),
        }
    }
}

fn parse_value(value_str: &str) -> Result<Value, Box<dyn std::error::Error>> {
    // Try to parse as JSON first
    if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(value_str) {
        match json_value {
            serde_json::Value::String(s) => Ok(Value::String(s)),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(Value::Int(i))
                } else if let Some(f) = n.as_f64() {
                    Ok(Value::Float(f))
                } else {
                    Err("Invalid number".into())
                }
            }
            serde_json::Value::Bool(b) => Ok(Value::Bool(b)),
            serde_json::Value::Array(arr) => {
                // Try to parse as entity list
                let entity_ids: Result<Vec<EntityId>, _> = arr
                    .into_iter()
                    .map(|v| {
                        if let serde_json::Value::String(s) = v {
                            EntityId::try_from(s.as_str())
                        } else {
                            Err("Array elements must be strings for EntityList".to_string())
                        }
                    })
                    .collect();
                Ok(Value::EntityList(entity_ids?))
            }
            serde_json::Value::Null => Ok(Value::EntityReference(None)),
            _ => Err("Unsupported JSON type".into()),
        }
    } else {
        // Try to parse as entity ID
        if let Ok(entity_id) = EntityId::try_from(value_str) {
            Ok(Value::EntityReference(Some(entity_id)))
        } else {
            // Default to string
            Ok(Value::String(value_str.to_string()))
        }
    }
}

fn parse_adjust_behavior(behavior: &str) -> Result<AdjustBehavior, Box<dyn std::error::Error>> {
    match behavior.to_lowercase().as_str() {
        "set" => Ok(AdjustBehavior::Set),
        "add" => Ok(AdjustBehavior::Add),
        "subtract" => Ok(AdjustBehavior::Subtract),
        _ => Err(format!("Invalid behavior: {}. Use 'set', 'add', or 'subtract'", behavior).into()),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    
    let mut client = QCoreClient::connect(&cli.address).await?;
    
    match cli.command {
        Commands::Read { entity_id, field } => {
            println!("Reading field '{}' from entity '{}'", field, entity_id);
            
            match client.read_field(&entity_id, &field).await {
                Ok(request) => {
                    if let Request::Read { value, .. } = request {
                        if let Some(val) = value {
                            println!("Value: {}", serde_json::to_string_pretty(&val)?);
                        } else {
                            println!("Field not set or no value");
                        }
                    }
                }
                Err(e) => println!("Error reading field: {}", e),
            }
        }
        
        Commands::Write { entity_id, field, value, behavior } => {
            println!("Writing to field '{}' on entity '{}'", field, entity_id);
            
            let parsed_value = parse_value(&value)?;
            let adjust_behavior = parse_adjust_behavior(&behavior)?;
            
            match client.write_field(&entity_id, &field, parsed_value, adjust_behavior).await {
                Ok(()) => println!("Successfully wrote value"),
                Err(e) => println!("Error writing field: {}", e),
            }
        }
        
        Commands::Create { entity_type, parent, name } => {
            println!("Creating entity of type '{}' with name '{}'", entity_type, name);
            
            match client.create_entity(&entity_type, parent.as_deref(), &name).await {
                Ok(entity_id) => println!("Created entity: {}", entity_id),
                Err(e) => println!("Error creating entity: {}", e),
            }
        }
        
        Commands::Delete { entity_id } => {
            println!("Deleting entity '{}'", entity_id);
            
            match client.delete_entity(&entity_id).await {
                Ok(()) => println!("Successfully deleted entity"),
                Err(e) => println!("Error deleting entity: {}", e),
            }
        }
        
        Commands::Metrics => {
            println!("Fetching cluster metrics...");
            
            match client.get_metrics().await {
                Ok(metrics) => {
                    println!("Cluster Metrics:");
                    println!("  Current Leader: {:?}", metrics.current_leader);
                    println!("  Current Term: {}", metrics.current_term);
                    println!("  State: {:?}", metrics.state);
                    println!("  Last Log Index: {:?}", metrics.last_log_index);
                    println!("  Last Applied: {:?}", metrics.last_applied);
                    println!("  Membership: {:?}", metrics.membership_config);
                }
                Err(e) => println!("Error fetching metrics: {}", e),
            }
        }
        
        Commands::List { entity_type, limit: _limit } => {
            println!("Listing entities of type '{}'", entity_type);
            println!("Note: List functionality requires additional implementation on the server side");
        }
        
        Commands::Schema { entity_type, complete } => {
            println!("Fetching schema for entity type '{}'", entity_type);
            
            match client.get_schema(&entity_type, complete).await {
                Ok(schema) => {
                    println!("Schema:");
                    println!("{}", schema);
                }
                Err(e) => println!("Error fetching schema: {}", e),
            }
        }
    }
    
    Ok(())
}
