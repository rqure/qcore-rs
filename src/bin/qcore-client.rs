use clap::{Parser, Subcommand};
use qlib_rs::{Context, EntityId, EntityType, FieldType, Value, Request, AdjustBehavior, PushCondition, StoreProxy};

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
    /// Get cluster metrics (Note: Not available through StoreProxy)
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
    /// Execute a script on the cluster
    Script {
        /// Script content to execute (Rhai syntax)
        script: String,
        /// Load script from file instead
        #[arg(short, long)]
        file: Option<String>,
    },
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
    
    let url = format!("ws://{}", cli.address);
    println!("Connecting to {}", url);
    
    let store_proxy = StoreProxy::connect(&url).await
        .map_err(|e| format!("Failed to connect to store: {}", e))?;
    let ctx = Context {};
    
    match cli.command {
        Commands::Read { entity_id, field } => {
            println!("Reading field '{}' from entity '{}'", field, entity_id);
            
            let entity_id = EntityId::try_from(entity_id.as_str())?;
            let field_type = FieldType::from(field.as_str());
            
            let mut requests = vec![Request::Read {
                entity_id,
                field_type,
                value: None,
                write_time: None,
                writer_id: None,
            }];
            
            match store_proxy.perform(&ctx, &mut requests).await {
                Ok(()) => {
                    if let Some(Request::Read { value, .. }) = requests.first() {
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
            let entity_id = EntityId::try_from(entity_id.as_str())?;
            let field_type = FieldType::from(field.as_str());
            
            let mut requests = vec![Request::Write {
                entity_id,
                field_type,
                value: Some(parsed_value),
                push_condition: PushCondition::Always,
                adjust_behavior,
                write_time: None,
                writer_id: None,
            }];
            
            match store_proxy.perform(&ctx, &mut requests).await {
                Ok(_) => println!("Successfully wrote value"),
                Err(e) => println!("Error writing field: {}", e),
            }
        }
        
        Commands::Create { entity_type, parent, name } => {
            println!("Creating entity of type '{}' with name '{}'", entity_type, name);
            
            let entity_type = EntityType::from(entity_type.as_str());
            let parent_id = if let Some(p) = parent {
                Some(EntityId::try_from(p.as_str())?)
            } else {
                None
            };
            
            match store_proxy.create_entity(&ctx, &entity_type, parent_id, &name).await {
                Ok(entity) => println!("Created entity: {}", entity.entity_id),
                Err(e) => println!("Error creating entity: {}", e),
            }
        }
        
        Commands::Delete { entity_id } => {
            println!("Deleting entity '{}'", entity_id);
            
            let entity_id = EntityId::try_from(entity_id.as_str())?;
            
            match store_proxy.delete_entity(&ctx, &entity_id).await {
                Ok(()) => println!("Successfully deleted entity"),
                Err(e) => println!("Error deleting entity: {}", e),
            }
        }
        
        Commands::Metrics => {
            println!("Cluster metrics are not available through StoreProxy.");
            println!("This feature requires direct access to the cluster's Raft metrics.");
        }
        
        Commands::List { entity_type, limit: _limit } => {
            println!("Listing entities of type '{}'", entity_type);
            
            let entity_type = EntityType::from(entity_type.as_str());
            
            match store_proxy.find_entities(&ctx, &entity_type, None).await {
                Ok(page_result) => {
                    println!("Found {} entities:", page_result.items.len());
                    for entity_id in page_result.items {
                        println!("  {}", entity_id);
                    }
                    if page_result.next_cursor.is_some() {
                        println!("  ... and more (use pagination to see all)");
                    }
                }
                Err(e) => println!("Error listing entities: {}", e),
            }
        }
        
        Commands::Schema { entity_type, complete } => {
            println!("Fetching schema for entity type '{}'", entity_type);
            
            let entity_type = EntityType::from(entity_type.as_str());
            
            if complete {
                match store_proxy.get_complete_entity_schema(&ctx, &entity_type).await {
                    Ok(schema) => {
                        println!("Complete Schema:");
                        println!("{}", serde_json::to_string_pretty(&schema)?);
                    }
                    Err(e) => println!("Error fetching complete schema: {}", e),
                }
            } else {
                match store_proxy.get_entity_schema(&ctx, &entity_type).await {
                    Ok(Some(schema)) => {
                        println!("Schema:");
                        println!("{}", serde_json::to_string_pretty(&schema)?);
                    }
                    Ok(None) => println!("No schema found for entity type '{}'", entity_type),
                    Err(e) => println!("Error fetching schema: {}", e),
                }
            }
        }
        
        Commands::Script { script, file } => {
            let script_content = if let Some(file_path) = file {
                println!("Loading script from file: {}", file_path);
                std::fs::read_to_string(&file_path)
                    .map_err(|e| format!("Failed to read script file: {}", e))?
            } else {
                script
            };
            
            println!("Executing script on cluster...");
            
            match store_proxy.execute_script(&ctx, &script_content).await {
                Ok(result) => {
                    println!("Script execution completed.");
                    println!("Result: {}", result);
                }
                Err(e) => println!("Error executing script: {}", e),
            }
        }
    }
    
    Ok(())
}
