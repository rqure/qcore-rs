use anyhow::Result;
use clap::{Parser, Subcommand};
use qlib_rs::{
    StoreProxy, restore_json_snapshot_proxy, JsonSnapshot, JsonEntitySchema,
    EntityType, EntityId, Error, Request,
    build_json_entity_tree_proxy, restore_entity_recursive_proxy
};
use serde_json;
use std::path::PathBuf;
use tokio::fs::{read_to_string, write};
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
        Commands::Restore { input, force } => {
            restore_snapshot(&config.core_url, &input, force).await?;
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
    
    // Connect to the Core service
    let mut store = StoreProxy::connect(core_url).await
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

/// Restore a snapshot from a file to the Core service
async fn restore_snapshot(core_url: &str, input_path: &PathBuf, force: bool) -> Result<()> {
    info!("Loading snapshot from: {}", input_path.display());

    // Read and parse the snapshot file
    let json_content = read_to_string(input_path).await?;
    let snapshot: JsonSnapshot = serde_json::from_str(&json_content)
        .map_err(|e| anyhow::anyhow!("Failed to parse JSON snapshot: {}", e))?;

    info!("Snapshot loaded successfully. Contains {} schemas.", snapshot.schemas.len());

    if !force {
        warn!("WARNING: This operation will overwrite existing data in the Core service!");
        warn!("The current store state will be replaced with the snapshot data.");
        warn!("Use --force flag to proceed without this warning.");
        
        // In a real implementation, you might want to prompt for user confirmation here
        // For now, we'll just return an error asking for --force
        return Err(anyhow::anyhow!("Restoration cancelled. Use --force flag to proceed."));
    }

    info!("Connecting to Core service at: {}", core_url);
    
    // Connect to the Core service
    let mut store = StoreProxy::connect(core_url).await
        .map_err(|e| anyhow::anyhow!("Failed to connect to Core service: {}", e))?;

    info!("Connected successfully. Restoring snapshot...");

    // Restore the snapshot
    restore_json_snapshot_proxy!(&mut store, snapshot).await
        .map_err(|e| anyhow::anyhow!("Failed to restore snapshot: {}", e))?;

    info!("Snapshot restored successfully!");

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