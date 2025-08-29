use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use indicatif::{ProgressBar, ProgressStyle};
use qlib_rs::{
    StoreProxy, JsonSnapshot, JsonEntitySchema,
    EntityType, EntityId, Request, Snapshot,
    FieldType, Field, json_value_to_value, take_json_snapshot
};
use serde_json;
use std::path::PathBuf;
use std::collections::HashMap;
use std::time::Duration;
use tokio::fs::{read_to_string, write};
use tokio::io::AsyncWriteExt;

/// Command-line tool for taking and restoring JSON snapshots from QCore service
#[derive(Parser)]
#[command(name = "snapshot-tool", about = "Tool for taking and restoring JSON snapshots from QCore service")]
struct Config {
    /// QCore service URL (WebSocket endpoint for client connections)
    #[arg(long, default_value = "ws://localhost:9100")]
    core_url: String,

    /// Username for authentication (can be set via QCORE_USERNAME env var)
    #[arg(long, default_value = "admin")]
    username: String,

    /// Password for authentication (can be set via QCORE_PASSWORD env var)
    #[arg(long, default_value = "admin123")]
    password: String,

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

/// Progress bar helpers for better UX
struct Progress;

impl Progress {
    fn new_spinner(msg: &str) -> ProgressBar {
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::default_spinner()
                .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"])
                .template("{spinner:.blue} {msg}")
                .unwrap(),
        );
        pb.set_message(msg.to_string());
        pb.enable_steady_tick(Duration::from_millis(80));
        pb
    }

    fn success(msg: &str) {
        println!("✅ {}", msg);
    }

    fn info(msg: &str) {
        println!("ℹ️  {}", msg);
    }

    fn warning(msg: &str) {
        println!("⚠️  {}", msg);
    }

    fn error(msg: &str) {
        eprintln!("❌ {}", msg);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::parse();

    // Get credentials from environment if available
    let username = std::env::var("QCORE_USERNAME").unwrap_or(config.username);
    let password = std::env::var("QCORE_PASSWORD").unwrap_or(config.password);

    let result = match config.command {
        Commands::Take { output, pretty } => {
            take_snapshot(&config.core_url, &username, &password, &output, pretty).await
        }
        Commands::Restore { input, force, data_dir, machine_id } => {
            restore_snapshot(&input, force, data_dir, machine_id).await
        }
        Commands::Validate { input } => {
            validate_snapshot(&input).await
        }
    };

    if let Err(ref e) = result {
        Progress::error(&format!("Operation failed: {}", e));
        for cause in e.chain().skip(1) {
            Progress::error(&format!("  Caused by: {}", cause));
        }
    }

    result
}

/// Take a snapshot from the Core service and save it to a file
async fn take_snapshot(
    core_url: &str, 
    username: &str, 
    password: &str, 
    output_path: &PathBuf, 
    pretty: bool
) -> Result<()> {
    let spinner = Progress::new_spinner("Connecting to QCore service");
    
    // Connect to the Core service with authentication
    let mut store = StoreProxy::connect_and_authenticate(core_url, username, password).await
        .with_context(|| format!("Failed to connect to Core service at {}", core_url))?;

    spinner.finish_with_message("✅ Connected to QCore service");

    let spinner = Progress::new_spinner("Taking snapshot");

    // Take the JSON snapshot using the library function
    let snapshot = take_json_snapshot(&mut store).await
        .context("Failed to take snapshot")?;

    spinner.finish_with_message(format!(
        "✅ Snapshot taken ({} schemas, {} root fields)", 
        snapshot.schemas.len(),
        snapshot.tree.fields.len()
    ));

    let spinner = Progress::new_spinner("Serializing snapshot");

    // Serialize the snapshot to JSON
    let json_content = if pretty {
        serde_json::to_string_pretty(&snapshot)
            .context("Failed to serialize snapshot to pretty JSON")?
    } else {
        serde_json::to_string(&snapshot)
            .context("Failed to serialize snapshot to JSON")?
    };

    spinner.finish_with_message("✅ Snapshot serialized");
    
    let spinner = Progress::new_spinner("Writing to file");

    // Write to file
    write(output_path, json_content.as_bytes()).await
        .with_context(|| format!("Failed to write snapshot to {}", output_path.display()))?;

    spinner.finish_with_message("✅ File written");

    Progress::success(&format!("Snapshot saved to {}", output_path.display()));
    Progress::info(&format!("File size: {} bytes", json_content.len()));

    Ok(())
}

/// Restore a snapshot from a file by generating snapshot and WAL files in the target data directory
async fn restore_snapshot(
    input_path: &PathBuf, 
    force: bool, 
    data_dir: Option<PathBuf>, 
    machine_id: String
) -> Result<()> {
    let spinner = Progress::new_spinner("Loading snapshot file");

    // Read and parse the snapshot file
    let snapshot = load_json_snapshot(input_path).await?;
    
    spinner.finish_with_message(format!("✅ Snapshot loaded ({} schemas)", snapshot.schemas.len()));

    // Determine target directories
    let target_data_dir = data_dir.unwrap_or_else(|| PathBuf::from("./data"));
    let directories = create_target_directories(&target_data_dir, &machine_id, force).await?;

    let spinner = Progress::new_spinner("Converting snapshot format");

    // Convert JsonSnapshot to internal Snapshot format
    let internal_snapshot = convert_json_to_internal_snapshot(&snapshot).await?;

    spinner.finish_with_message("✅ Snapshot converted");

    let spinner = Progress::new_spinner("Writing snapshot binary");

    // Write snapshot binary file
    write_snapshot_file(&directories.snapshots, &internal_snapshot).await?;

    spinner.finish_with_message("✅ Snapshot binary written");

    let spinner = Progress::new_spinner("Writing WAL file");

    // Write WAL file with snapshot marker
    write_wal_file(&directories.wal).await?;

    spinner.finish_with_message("✅ WAL file written");

    Progress::success("Snapshot restoration completed successfully!");
    Progress::info(&format!("Files created in: {}", target_data_dir.display()));
    Progress::info(&format!("Start QCore with: --data-dir {} --machine {}", target_data_dir.display(), machine_id));

    Ok(())
}

/// Directory structure for data restoration
struct DataDirectories {
    snapshots: PathBuf,
    wal: PathBuf,
}

/// Load and parse a JSON snapshot file
async fn load_json_snapshot(input_path: &PathBuf) -> Result<JsonSnapshot> {
    let json_content = read_to_string(input_path).await
        .with_context(|| format!("Failed to read snapshot file: {}", input_path.display()))?;
    
    serde_json::from_str(&json_content)
        .with_context(|| format!("Failed to parse JSON snapshot from: {}", input_path.display()))
}

/// Create target directories for data restoration
async fn create_target_directories(
    target_data_dir: &PathBuf, 
    machine_id: &str, 
    force: bool
) -> Result<DataDirectories> {
    let machine_data_dir = target_data_dir.join(machine_id);
    let snapshots_dir = machine_data_dir.join("snapshots");
    let wal_dir = machine_data_dir.join("wal");

    // Check if directories already exist and warn user
    if (snapshots_dir.exists() || wal_dir.exists()) && !force {
        Progress::warning("Target directories already exist!");
        Progress::warning(&format!("Snapshots dir: {}", snapshots_dir.display()));
        Progress::warning(&format!("WAL dir: {}", wal_dir.display()));
        Progress::warning("This operation will create new files that might conflict with existing data.");
        Progress::warning("Use --force flag to proceed without this warning.");
        
        return Err(anyhow::anyhow!("Restoration cancelled. Use --force flag to proceed."));
    }

    Progress::info(&format!("Creating data structure for machine '{}' in: {}", machine_id, target_data_dir.display()));

    // Create directories
    tokio::fs::create_dir_all(&snapshots_dir).await
        .with_context(|| format!("Failed to create snapshots directory: {}", snapshots_dir.display()))?;
    tokio::fs::create_dir_all(&wal_dir).await
        .with_context(|| format!("Failed to create WAL directory: {}", wal_dir.display()))?;

    Ok(DataDirectories {
        snapshots: snapshots_dir,
        wal: wal_dir,
    })
}

/// Write the internal snapshot to a binary file
async fn write_snapshot_file(snapshots_dir: &PathBuf, internal_snapshot: &Snapshot) -> Result<()> {
    let snapshot_filename = "snapshot_0000000000.bin";
    let snapshot_path = snapshots_dir.join(snapshot_filename);
    
    // Serialize the snapshot using bincode for consistency with QCore format
    let serialized_snapshot = bincode::serialize(internal_snapshot)
        .context("Failed to serialize snapshot")?;
    
    tokio::fs::write(&snapshot_path, &serialized_snapshot).await
        .with_context(|| format!("Failed to write snapshot file: {}", snapshot_path.display()))?;
    
    Progress::info(&format!("Snapshot binary: {} bytes", serialized_snapshot.len()));
    Ok(())
}

/// Write a WAL file with snapshot marker
async fn write_wal_file(wal_dir: &PathBuf) -> Result<()> {
    let wal_filename = "wal_0000000000.log";
    let wal_path = wal_dir.join(wal_filename);
    
    // Create snapshot marker request
    let snapshot_request = Request::Snapshot {
        snapshot_counter: 0,
        originator: Some("snapshot-tool".to_string()),
    };
    
    // Serialize the request to JSON (matching QCore format)
    let serialized_request = serde_json::to_vec(&snapshot_request)
        .context("Failed to serialize snapshot request")?;
    
    // Write to WAL file with length prefix (matching QCore format)
    let mut wal_file = tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&wal_path)
        .await
        .with_context(|| format!("Failed to open WAL file: {}", wal_path.display()))?;
    
    // Write length prefix (4 bytes little-endian) followed by the serialized data
    let len_bytes = (serialized_request.len() as u32).to_le_bytes();
    wal_file.write_all(&len_bytes).await?;
    wal_file.write_all(&serialized_request).await?;
    wal_file.flush().await?;
    
    Progress::info(&format!("WAL file: {} bytes", 4 + serialized_request.len()));
    Ok(())
}

/// Validate a snapshot file without restoring it
async fn validate_snapshot(input_path: &PathBuf) -> Result<()> {
    let spinner = Progress::new_spinner("Validating snapshot file");

    // Read and parse the snapshot file
    let snapshot = load_json_snapshot(input_path).await?;

    spinner.finish_with_message("✅ JSON structure is valid");

    print_snapshot_summary(&snapshot);

    let spinner = Progress::new_spinner("Validating schemas");

    // Validate that all schemas can be converted to internal format
    validate_schemas(&snapshot.schemas)?;

    spinner.finish_with_message("✅ All schemas are valid");

    Progress::success("Snapshot file is valid and ready for use!");
    Ok(())
}

/// Print a summary of the snapshot contents
fn print_snapshot_summary(snapshot: &JsonSnapshot) {
    Progress::info(&format!("Snapshot contains {} schemas:", snapshot.schemas.len()));
    
    for schema in &snapshot.schemas {
        let inheritance = if let Some(ref inherits) = schema.inherits_from {
            format!(" (inherits from {})", inherits)
        } else {
            String::new()
        };
        Progress::info(&format!("  • {} ({} fields){}", schema.entity_type, schema.fields.len(), inheritance));
    }

    Progress::info(&format!("Root entity type: {}", snapshot.tree.entity_type));
    Progress::info(&format!("Root entity has {} fields", snapshot.tree.fields.len()));
}

/// Validate that all schemas can be converted to internal format
fn validate_schemas(schemas: &[JsonEntitySchema]) -> Result<()> {
    for schema in schemas {
        schema.to_entity_schema()
            .with_context(|| format!("Schema validation failed for '{}'", schema.entity_type))?;
    }
    Ok(())
}

/// Convert JsonSnapshot to internal Snapshot format
async fn convert_json_to_internal_snapshot(json_snapshot: &JsonSnapshot) -> Result<Snapshot> {
    let mut snapshot = Snapshot::default();
    
    // Convert schemas
    convert_schemas_to_internal(&mut snapshot, &json_snapshot.schemas)?;
    
    // Convert the entity tree to individual entities and fields
    let mut entity_counters: HashMap<String, u64> = HashMap::new();
    convert_json_entity_recursive(&json_snapshot.tree, None, &mut snapshot, &mut entity_counters)?;
    
    Ok(snapshot)
}

/// Convert JSON schemas to internal format
fn convert_schemas_to_internal(snapshot: &mut Snapshot, schemas: &[JsonEntitySchema]) -> Result<()> {
    for json_schema in schemas {
        let entity_type = EntityType::from(json_schema.entity_type.clone());
        let schema = json_schema.to_entity_schema()
            .with_context(|| format!("Failed to convert schema for entity type: {}", json_schema.entity_type))?;
        
        snapshot.schemas.insert(entity_type.clone(), schema);
        if !snapshot.types.contains(&entity_type) {
            snapshot.types.push(entity_type);
        }
    }
    Ok(())
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
    let entity_fields = convert_entity_fields(json_entity, schema)?;
    snapshot.fields.insert(entity_id.clone(), entity_fields);
    
    // Handle children recursively
    handle_entity_children(json_entity, &entity_id, snapshot, entity_counters)?;
    
    Ok(entity_id)
}

/// Convert field values for an entity
fn convert_entity_fields(
    json_entity: &qlib_rs::JsonEntity,
    schema: &qlib_rs::EntitySchema<qlib_rs::Single>,
) -> Result<HashMap<FieldType, Field>> {
    let mut entity_fields = HashMap::new();
    
    for (field_name, json_value) in &json_entity.fields {
        if field_name == "Children" {
            // Handle children separately
            continue;
        }
        
        let field_type = FieldType::from(field_name.clone());
        if let Some(field_schema) = schema.fields.get(&field_type) {
            if let Ok(value) = json_value_to_value(json_value, field_schema) {
                let field = Field {
                    field_type: field_type.clone(),
                    value,
                    write_time: std::time::SystemTime::now(),
                    writer_id: None,
                };
                entity_fields.insert(field_type, field);
            }
        }
    }
    
    Ok(entity_fields)
}

/// Handle children entities recursively
fn handle_entity_children(
    json_entity: &qlib_rs::JsonEntity,
    entity_id: &EntityId,
    snapshot: &mut Snapshot,
    entity_counters: &mut HashMap<String, u64>
) -> Result<()> {
    if let Some(children_json) = json_entity.fields.get("Children") {
        if let Some(children_array) = children_json.as_array() {
            for child_json_value in children_array {
                if let Ok(child_json_entity) = serde_json::from_value::<qlib_rs::JsonEntity>(child_json_value.clone()) {
                    let _ = convert_json_entity_recursive(&child_json_entity, Some(entity_id.clone()), snapshot, entity_counters)?;
                }
            }
        }
    }
    Ok(())
}