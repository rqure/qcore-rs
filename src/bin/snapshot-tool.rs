use anyhow::Result;
use clap::{Parser, Subcommand};
use qlib_rs::{
    StoreProxy, JsonSnapshot, JsonEntitySchema,
    EntityType, EntityId, Error, Request, Snapshot,
    build_json_entity_tree_proxy, FieldType, Field,
    json_value_to_value
};
use serde_json;
use std::path::PathBuf;
use std::collections::HashMap;
use tokio::fs::{read_to_string, write};
use tokio::io::AsyncWriteExt;
use tracing::{info, error, warn};

/// Command-line tool for taking and restoring JSON snapshots from QCore service
#[derive(Parser)]
#[command(name = "snapshot-tool", about = "Tool for taking and restoring JSON snapshots from QCore service")]
struct Config {
    /// QCore service URL (WebSocket endpoint for client connections)
    #[arg(long, default_value = "ws://localhost:9100")]
    core_url: String,

    /// Subcommand to execute
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Take a snapshot of the current store state and save it to a file
    Take {
        /// Output file path for the JSON snapshot
        #[arg(short, long)]
        output: PathBuf,
        
        /// Pretty-print the JSON output (makes it human-readable but larger)
        #[arg(long)]
        pretty: bool,
    },
    /// Restore store state from a JSON snapshot file
    Restore {
        /// Input file path containing the JSON snapshot
        #[arg(short, long)]
        input: PathBuf,
        
        /// Force restoration even if it might overwrite existing data
        #[arg(long)]
        force: bool,
        
        /// Target data directory where snapshot and WAL files will be created
        #[arg(long)]
        data_dir: Option<PathBuf>,
        
        /// Target machine ID for the data directory structure (defaults to "restored")
        #[arg(long, default_value = "restored")]
        machine_id: String,
    },
    /// Validate a JSON snapshot file without restoring it
    Validate {
        /// Input file path containing the JSON snapshot to validate
        #[arg(short, long)]
        input: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::parse();

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "snapshot_tool=info".into()),
        )
        .init();

    info!("Starting snapshot tool with Core URL: {}", config.core_url);

    match config.command {
        Commands::Take { output, pretty } => {
            take_snapshot(&config.core_url, &output, pretty).await?;
        }
        Commands::Restore { input, force, data_dir, machine_id } => {
            restore_snapshot(&input, force, data_dir, machine_id).await?;
        }
        Commands::Validate { input } => {
            validate_snapshot(&input).await?;
        }
    }

    Ok(())
}

/// Take a snapshot from the Core service and save it to a file
async fn take_snapshot(core_url: &str, output_path: &PathBuf, pretty: bool) -> Result<()> {
    info!("Connecting to Core service at: {}", core_url);
    
    // Connect to the Core service with authentication
    // For the snapshot tool, we'll use default admin credentials
    let mut store = StoreProxy::connect_and_authenticate(core_url, "admin", "admin123").await
        .map_err(|e| anyhow::anyhow!("Failed to connect to Core service: {}", e))?;

    info!("Connected successfully. Taking snapshot...");

    // Take the JSON snapshot using a custom implementation for StoreProxy
    let snapshot = take_json_snapshot_proxy(&mut store).await
        .map_err(|e| anyhow::anyhow!("Failed to take snapshot: {}", e))?;

    info!("Snapshot taken successfully. Contains {} schemas and entity tree starting from Root.", 
          snapshot.schemas.len());

    // Serialize the snapshot to JSON
    let json_content = if pretty {
        serde_json::to_string_pretty(&snapshot)?
    } else {
        serde_json::to_string(&snapshot)?
    };

    // Write to file
    write(output_path, json_content.as_bytes()).await?;

    info!("Snapshot saved to: {}", output_path.display());
    info!("File size: {} bytes", json_content.len());

    Ok(())
}

/// Custom implementation of take_json_snapshot for StoreProxy
async fn take_json_snapshot_proxy(store: &mut StoreProxy) -> Result<JsonSnapshot, Error> {
    // Collect all schemas by getting all entity types first
    let mut json_schemas = Vec::new();
    let entity_types = store.get_entity_types().await?;
    for entity_type in entity_types {
        if let Ok(schema) = store.get_entity_schema(&entity_type).await {
            json_schemas.push(JsonEntitySchema::from_entity_schema(&schema));
        }
    }

    // Sort schemas for consistent output
    json_schemas.sort_by(|a, b| a.entity_type.cmp(&b.entity_type));

    // Find the Root entity
    let root_entities = store.find_entities(&EntityType::from("Root")).await?;
    let root_entity_id = root_entities.first()
        .ok_or_else(|| Error::EntityNotFound(EntityId::new("Root", 0)))?;

    // Build the entity tree starting from root using the proxy helper function
    let root_entity = build_json_entity_tree_proxy(store, root_entity_id).await?;

    Ok(JsonSnapshot {
        schemas: json_schemas,
        tree: root_entity,
    })
}

/// Restore a snapshot from a file by generating snapshot and WAL files in the target data directory
async fn restore_snapshot(
    input_path: &PathBuf, 
    force: bool, 
    data_dir: Option<PathBuf>, 
    machine_id: String
) -> Result<()> {
    info!("Loading snapshot from: {}", input_path.display());

    // Read and parse the snapshot file
    let json_content = read_to_string(input_path).await?;
    let snapshot: JsonSnapshot = serde_json::from_str(&json_content)
        .map_err(|e| anyhow::anyhow!("Failed to parse JSON snapshot: {}", e))?;

    info!("Snapshot loaded successfully. Contains {} schemas.", snapshot.schemas.len());

    // Determine target data directory
    let target_data_dir = data_dir.unwrap_or_else(|| PathBuf::from("./data"));
    let machine_data_dir = target_data_dir.join(&machine_id);
    let snapshots_dir = machine_data_dir.join("snapshots");
    let wal_dir = machine_data_dir.join("wal");

    // Check if directories already exist and warn user
    if (snapshots_dir.exists() || wal_dir.exists()) && !force {
        warn!("WARNING: Target directories already exist!");
        warn!("Snapshots dir: {}", snapshots_dir.display());
        warn!("WAL dir: {}", wal_dir.display());
        warn!("This operation will create new files that might conflict with existing data.");
        warn!("Use --force flag to proceed without this warning.");
        
        return Err(anyhow::anyhow!("Restoration cancelled. Use --force flag to proceed."));
    }

    info!("Creating data structure for machine '{}' in: {}", machine_id, target_data_dir.display());

    // Create directories
    tokio::fs::create_dir_all(&snapshots_dir).await?;
    tokio::fs::create_dir_all(&wal_dir).await?;

    // Convert JsonSnapshot to internal Snapshot format
    let internal_snapshot = convert_json_to_internal_snapshot(&snapshot).await?;

    // Generate snapshot binary file
    let snapshot_filename = "snapshot_0000000000.bin";
    let snapshot_path = snapshots_dir.join(snapshot_filename);
    
    info!("Writing snapshot binary to: {}", snapshot_path.display());
    
    // Serialize the snapshot using bincode for consistency with QCore format
    let serialized_snapshot = bincode::serialize(&internal_snapshot)
        .map_err(|e| anyhow::anyhow!("Failed to serialize snapshot: {}", e))?;
    
    tokio::fs::write(&snapshot_path, &serialized_snapshot).await?;
    
    info!("Snapshot binary written: {} bytes", serialized_snapshot.len());

    // Generate WAL file with snapshot marker
    let wal_filename = "wal_0000000000.log";
    let wal_path = wal_dir.join(wal_filename);
    
    info!("Writing WAL file to: {}", wal_path.display());
    
    // Create snapshot marker request
    let snapshot_request = Request::Snapshot {
        snapshot_counter: 0,
        originator: Some("snapshot-tool".to_string()),
    };
    
    // Serialize the request to JSON (matching QCore format)
    let serialized_request = serde_json::to_vec(&snapshot_request)
        .map_err(|e| anyhow::anyhow!("Failed to serialize snapshot request: {}", e))?;
    
    // Write to WAL file with length prefix (matching QCore format)
    let mut wal_file = tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&wal_path)
        .await?;
    
    // Write length prefix (4 bytes little-endian) followed by the serialized data
    let len_bytes = (serialized_request.len() as u32).to_le_bytes();
    wal_file.write_all(&len_bytes).await?;
    wal_file.write_all(&serialized_request).await?;
    wal_file.flush().await?;
    
    info!("WAL file written: {} bytes total", 4 + serialized_request.len());

    info!("Snapshot restoration completed successfully!");
    info!("Files created:");
    info!("  - Snapshot: {}", snapshot_path.display());
    info!("  - WAL: {}", wal_path.display());
    info!("Start QCore with --data-dir {} --machine {} to use these files", 
          target_data_dir.display(), machine_id);

    Ok(())
}

/// Validate a snapshot file without restoring it
async fn validate_snapshot(input_path: &PathBuf) -> Result<()> {
    info!("Validating snapshot file: {}", input_path.display());

    // Read and parse the snapshot file
    let json_content = read_to_string(input_path).await?;
    let snapshot: JsonSnapshot = serde_json::from_str(&json_content)
        .map_err(|e| anyhow::anyhow!("Failed to parse JSON snapshot: {}", e))?;

    info!("JSON structure is valid.");
    info!("Snapshot contains {} schemas:", snapshot.schemas.len());
    
    for schema in &snapshot.schemas {
        info!("  - {} ({} fields)", schema.entity_type, schema.fields.len());
        if let Some(ref inherits) = schema.inherits_from {
            info!("    Inherits from: {:?}", inherits);
        }
    }

    info!("Root entity type: {}", snapshot.tree.entity_type);
    info!("Root entity has {} fields", snapshot.tree.fields.len());

    // Validate that all schemas can be converted to internal format
    for schema in &snapshot.schemas {
        match schema.to_entity_schema() {
            Ok(_) => info!("  ✓ Schema '{}' is valid", schema.entity_type),
            Err(e) => {
                error!("  ✗ Schema '{}' is invalid: {}", schema.entity_type, e);
                return Err(anyhow::anyhow!("Schema validation failed for '{}'", schema.entity_type));
            }
        }
    }

    info!("All validations passed. The snapshot file is valid.");

    Ok(())
}

/// Convert JsonSnapshot to internal Snapshot format
async fn convert_json_to_internal_snapshot(json_snapshot: &JsonSnapshot) -> Result<Snapshot> {
    let mut snapshot = Snapshot::default();
    
    // Convert schemas
    for json_schema in &json_snapshot.schemas {
        let entity_type = EntityType::from(json_schema.entity_type.clone());
        let schema = json_schema.to_entity_schema()
            .map_err(|e| anyhow::anyhow!("Failed to convert schema: {}", e))?;
        snapshot.schemas.insert(entity_type.clone(), schema);
        if !snapshot.types.contains(&entity_type) {
            snapshot.types.push(entity_type);
        }
    }
    
    // Convert the entity tree to individual entities and fields
    let mut entity_counters: HashMap<String, u64> = HashMap::new();
    convert_json_entity_recursive(&json_snapshot.tree, None, &mut snapshot, &mut entity_counters)?;
    
    Ok(snapshot)
}

/// Recursively convert JsonEntity to internal format and populate snapshot
fn convert_json_entity_recursive(
    json_entity: &qlib_rs::JsonEntity, 
    _parent_id: Option<EntityId>,
    snapshot: &mut Snapshot,
    entity_counters: &mut HashMap<String, u64>
) -> Result<EntityId> {
    let entity_type = EntityType::from(json_entity.entity_type.clone());
    
    // Generate a unique entity ID using a counter per type
    let counter = entity_counters.entry(entity_type.as_ref().to_string()).or_insert(0);
    let entity_id = EntityId::new(entity_type.as_ref(), *counter);
    *counter += 1;
    
    // Add entity to the snapshot
    snapshot.entities.entry(entity_type.clone()).or_insert_with(Vec::new).push(entity_id.clone());
    
    // Get the schema for this entity type
    let schema = snapshot.schemas.get(&entity_type)
        .ok_or_else(|| anyhow::anyhow!("Schema not found for entity type: {}", entity_type.as_ref()))?;
    
    // Convert field values
    let mut entity_fields = HashMap::new();
    
    for (field_name, json_value) in &json_entity.fields {
        if field_name == "Children" {
            // Handle children specially - we'll process them after this entity
            continue;
        }
        
        let field_type = FieldType::from(field_name.clone());
        if let Some(field_schema) = schema.fields.get(&field_type) {
            match json_value_to_value(json_value, field_schema) {
                Ok(value) => {
                    let field = Field {
                        field_type: field_type.clone(),
                        value,
                        write_time: std::time::SystemTime::now(),
                        writer_id: None,
                    };
                    entity_fields.insert(field_type, field);
                }
                Err(e) => {
                    warn!("Failed to convert field '{}': {}", field_name, e);
                }
            }
        }
    }
    
    snapshot.fields.insert(entity_id.clone(), entity_fields);
    
    // Handle children recursively
    if let Some(children_json) = json_entity.fields.get("Children") {
        if let Some(children_array) = children_json.as_array() {
            for child_json_value in children_array {
                if let Ok(child_json_entity) = serde_json::from_value::<qlib_rs::JsonEntity>(child_json_value.clone()) {
                    let _ = convert_json_entity_recursive(&child_json_entity, Some(entity_id.clone()), snapshot, entity_counters)?;
                }
            }
        }
    }
    
    Ok(entity_id)
}