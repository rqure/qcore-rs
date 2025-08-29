use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use indicatif::{ProgressBar, ProgressStyle};
use qlib_rs::{
    StoreProxy, JsonSnapshot, JsonEntitySchema,
    take_json_snapshot, factory_restore_json_snapshot, restore_json_snapshot_via_proxy
};
use serde_json;
use std::path::PathBuf;
use std::collections::HashMap;
use std::time::Duration;
use tokio::fs::{read_to_string, write};

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
    /// Factory restore: Create snapshot and WAL files in target data directory from JSON
    FactoryRestore {
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
    /// Normal restore: Connect to QCore service and apply differences from JSON snapshot
    Restore {
        /// Input file path containing the JSON snapshot
        #[arg(short, long)]
        input: PathBuf,
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
                .tick_strings(&["|", "/", "-", "\\"])
                .template("{spinner} {msg}")
                .unwrap(),
        );
        pb.set_message(msg.to_string());
        pb.enable_steady_tick(Duration::from_millis(120));
        pb
    }

    fn success(msg: &str) {
        println!("[SUCCESS] {}", msg);
    }

    fn info(msg: &str) {
        println!("[INFO] {}", msg);
    }

    fn warning(msg: &str) {
        println!("[WARNING] {}", msg);
    }

    fn error(msg: &str) {
        eprintln!("[ERROR] {}", msg);
    }

    fn step(step: usize, total: usize, msg: &str) {
        println!("[{}/{}] {}", step, total, msg);
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
        Commands::FactoryRestore { input, force, data_dir, machine_id } => {
            factory_restore_snapshot(&input, force, data_dir, machine_id).await
        }
        Commands::Restore { input } => {
            normal_restore_snapshot(&config.core_url, &username, &password, &input).await
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
    Progress::step(1, 4, "Connecting to QCore service");
    let spinner = Progress::new_spinner("Establishing connection...");
    
    // Connect to the Core service with authentication
    let mut store = StoreProxy::connect_and_authenticate(core_url, username, password).await
        .with_context(|| format!("Failed to connect to Core service at {}", core_url))?;

    spinner.finish_with_message("Connected successfully");

    Progress::step(2, 4, "Taking snapshot from QCore service");
    let spinner = Progress::new_spinner("Retrieving schemas and entities...");

    // Take the JSON snapshot using the library function
    let snapshot = take_json_snapshot(&mut store).await
        .context("Failed to take snapshot")?;

    let entity_count = count_entities_in_tree(&snapshot.tree);
    spinner.finish_with_message(format!(
        "Snapshot captured: {} schemas, {} entities in tree", 
        snapshot.schemas.len(),
        entity_count
    ));

    Progress::step(3, 4, "Serializing snapshot data");
    let spinner = Progress::new_spinner("Converting to JSON format...");

    // Serialize the snapshot to JSON
    let json_content = if pretty {
        serde_json::to_string_pretty(&snapshot)
            .context("Failed to serialize snapshot to pretty JSON")?
    } else {
        serde_json::to_string(&snapshot)
            .context("Failed to serialize snapshot to JSON")?
    };

    spinner.finish_with_message(format!("Serialized {} bytes", json_content.len()));
    
    Progress::step(4, 4, "Writing snapshot to file");
    let spinner = Progress::new_spinner("Saving to disk...");

    // Write to file
    write(output_path, json_content.as_bytes()).await
        .with_context(|| format!("Failed to write snapshot to {}", output_path.display()))?;

    spinner.finish_with_message("File saved successfully");

    Progress::success(&format!("Snapshot saved to {}", output_path.display()));
    Progress::info(&format!("File size: {} bytes", json_content.len()));

    Ok(())
}

/// Count total entities in the JSON entity tree
fn count_entities_in_tree(entity: &qlib_rs::JsonEntity) -> usize {
    let mut count = 1; // Count this entity
    
    if let Some(children) = entity.fields.get("Children") {
        if let Some(children_array) = children.as_array() {
            for child_value in children_array {
                if let Ok(child_entity) = serde_json::from_value::<qlib_rs::JsonEntity>(child_value.clone()) {
                    count += count_entities_in_tree(&child_entity);
                }
            }
        }
    }
    
    count
}

/// Count entities by type in the JSON entity tree
fn count_entities_by_type(entity: &qlib_rs::JsonEntity, counts: &mut HashMap<String, usize>) {
    // Count this entity
    *counts.entry(entity.entity_type.clone()).or_insert(0) += 1;
    
    // Recursively count children
    if let Some(children) = entity.fields.get("Children") {
        if let Some(children_array) = children.as_array() {
            for child_value in children_array {
                if let Ok(child_entity) = serde_json::from_value::<qlib_rs::JsonEntity>(child_value.clone()) {
                    count_entities_by_type(&child_entity, counts);
                }
            }
        }
    }
}

/// Factory restore a snapshot from a file by generating snapshot and WAL files in the target data directory
async fn factory_restore_snapshot(
    input_path: &PathBuf, 
    force: bool, 
    data_dir: Option<PathBuf>, 
    machine_id: String
) -> Result<()> {
    Progress::step(1, 4, "Loading snapshot file");
    let spinner = Progress::new_spinner("Reading and parsing JSON...");

    // Read and parse the snapshot file
    let snapshot = load_json_snapshot(input_path).await?;
    let entity_count = count_entities_in_tree(&snapshot.tree);
    
    spinner.finish_with_message(format!("Loaded {} schemas, {} entities", snapshot.schemas.len(), entity_count));

    Progress::step(2, 4, "Preparing target directories");
    // Determine target directories
    let target_data_dir = data_dir.unwrap_or_else(|| PathBuf::from("./data"));
    
    // Check if directories already exist and warn user
    let machine_data_dir = target_data_dir.join(&machine_id);
    let snapshots_dir = machine_data_dir.join("snapshots");
    let wal_dir = machine_data_dir.join("wal");

    if (snapshots_dir.exists() || wal_dir.exists()) && !force {
        Progress::warning("Target directories already exist!");
        Progress::warning(&format!("Snapshots dir: {}", snapshots_dir.display()));
        Progress::warning(&format!("WAL dir: {}", wal_dir.display()));
        Progress::warning("This operation will create new files that might conflict with existing data.");
        Progress::warning("Use --force flag to proceed without this warning.");
        
        return Err(anyhow::anyhow!("Restoration cancelled. Use --force flag to proceed."));
    }

    Progress::info(&format!("Creating data structure for machine '{}' in: {}", machine_id, target_data_dir.display()));

    Progress::step(3, 4, "Performing factory restore");
    let spinner = Progress::new_spinner("Creating snapshot and WAL files...");

    // Use the new factory restore function
    factory_restore_json_snapshot(&snapshot, target_data_dir.clone(), machine_id.clone()).await
        .context("Failed to perform factory restore")?;

    spinner.finish_with_message("Factory restore completed");

    Progress::step(4, 4, "Factory restoration complete");
    Progress::success("Factory restoration completed successfully!");
    Progress::info(&format!("Files created in: {}", target_data_dir.display()));
    Progress::info(&format!("Start QCore with: --data-dir {} --machine {}", target_data_dir.display(), machine_id));

    Ok(())
}

/// Normal restore via StoreProxy: connect to service and apply differences
async fn normal_restore_snapshot(
    core_url: &str,
    username: &str,
    password: &str,
    input_path: &PathBuf,
) -> Result<()> {
    Progress::step(1, 4, "Loading snapshot file");
    let spinner = Progress::new_spinner("Reading and parsing JSON...");

    // Read and parse the snapshot file
    let snapshot = load_json_snapshot(input_path).await?;
    let entity_count = count_entities_in_tree(&snapshot.tree);
    
    spinner.finish_with_message(format!("Loaded {} schemas, {} entities", snapshot.schemas.len(), entity_count));

    Progress::step(2, 4, "Connecting to QCore service");
    let spinner = Progress::new_spinner("Establishing connection...");
    
    // Connect to the Core service with authentication
    let mut store = StoreProxy::connect_and_authenticate(core_url, username, password).await
        .with_context(|| format!("Failed to connect to Core service at {}", core_url))?;

    spinner.finish_with_message("Connected successfully");

    Progress::step(3, 4, "Computing and applying differences");
    let spinner = Progress::new_spinner("Analyzing current state and applying changes...");

    // Use the new restore via proxy function
    restore_json_snapshot_via_proxy(&mut store, &snapshot).await
        .context("Failed to restore snapshot via proxy")?;

    spinner.finish_with_message("Changes applied successfully");

    Progress::step(4, 4, "Normal restoration complete");
    Progress::success("Normal restoration completed successfully!");
    Progress::info("The QCore service has been updated with the snapshot data.");

    Ok(())
}

/// Load and parse a JSON snapshot file
async fn load_json_snapshot(input_path: &PathBuf) -> Result<JsonSnapshot> {
    let json_content = read_to_string(input_path).await
        .with_context(|| format!("Failed to read snapshot file: {}", input_path.display()))?;
    
    serde_json::from_str(&json_content)
        .with_context(|| format!("Failed to parse JSON snapshot from: {}", input_path.display()))
}

/// Validate a snapshot file without restoring it
async fn validate_snapshot(input_path: &PathBuf) -> Result<()> {
    Progress::step(1, 3, "Loading snapshot file");
    let spinner = Progress::new_spinner("Reading and parsing JSON...");

    // Read and parse the snapshot file
    let snapshot = load_json_snapshot(input_path).await?;

    spinner.finish_with_message("JSON structure is valid");

    Progress::step(2, 3, "Analyzing snapshot contents");
    print_snapshot_summary(&snapshot);

    Progress::step(3, 3, "Validating schemas");
    let spinner = Progress::new_spinner("Checking schema definitions...");

    // Validate that all schemas can be converted to internal format
    validate_schemas(&snapshot.schemas)?;

    spinner.finish_with_message("All schemas are valid");

    Progress::success("Snapshot file is valid and ready for use!");
    Ok(())
}

/// Print a summary of the snapshot contents
fn print_snapshot_summary(snapshot: &JsonSnapshot) {
    let total_entity_count = count_entities_in_tree(&snapshot.tree);
    
    Progress::info(&format!("Snapshot contains {} schemas:", snapshot.schemas.len()));
    
    for schema in &snapshot.schemas {
        let inheritance = if let Some(ref inherits) = schema.inherits_from {
            format!(" (inherits from {})", inherits)
        } else {
            String::new()
        };
        Progress::info(&format!("  - {} ({} fields){}", schema.entity_type, schema.fields.len(), inheritance));
    }
    
    // Count entities by type
    let mut entity_counts: HashMap<String, usize> = HashMap::new();
    count_entities_by_type(&snapshot.tree, &mut entity_counts);
    
    Progress::info(&format!("Entity breakdown ({} total):", total_entity_count));
    for (entity_type, count) in entity_counts.iter() {
        Progress::info(&format!("  - {}: {}", entity_type, count));
    }
}

/// Validate that all schemas can be converted to internal format
fn validate_schemas(schemas: &[JsonEntitySchema]) -> Result<()> {
    for schema in schemas {
        schema.to_entity_schema()
            .with_context(|| format!("Schema validation failed for '{}'", schema.entity_type))?;
    }
    Ok(())
}