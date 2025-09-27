use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use indicatif::{ProgressBar, ProgressStyle};
use qlib_rs::{
    StoreProxy, JsonSnapshot, JsonEntitySchema, JsonEntity,
    take_json_snapshot, factory_restore_json_snapshot, restore_json_snapshot_via_proxy
};
use serde_json;
use std::path::PathBuf;
use std::collections::HashMap;
use std::time::Duration;
use std::fs::{read_to_string, write};
use tracing::{info, warn, error, debug};

/// Command-line tool for taking and restoring JSON snapshots from QCore service
#[derive(Parser)]
#[command(name = "snapshot-tool", about = "Tool for taking and restoring JSON snapshots from QCore service")]
struct Config {
    /// QCore service URL (WebSocket endpoint for client connections)
    #[arg(long, default_value = "localhost:9100")]
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
        #[arg(long)]
        machine: String,
    },
    /// Normal restore: Connect to QCore service and apply differences from JSON snapshot
    Restore {
        /// Input file path containing the JSON snapshot
        #[arg(short, long)]
        input: PathBuf,
    },
    /// Report: Connect to QCore service and show a diff of what would be changed by restore
    Report {
        /// Input file path containing the JSON snapshot
        #[arg(short, long)]
        input: PathBuf,
        
        /// Output the report to a file instead of stdout
        #[arg(short, long)]
        output: Option<PathBuf>,
        
        /// Output format for the report
        #[arg(long, default_value = "text")]
        format: ReportFormat,
    },
    /// Validate a JSON snapshot file without restoring it
    Validate {
        /// Input file path containing the JSON snapshot to validate
        #[arg(short, long)]
        input: PathBuf,
    },
}

#[derive(Debug, Clone, clap::ValueEnum)]
enum ReportFormat {
    /// Human-readable text format
    Text,
    /// JSON format for programmatic use
    Json,
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
        info!(target: "snapshot_tool::success", "{}", msg);
    }

    fn info(msg: &str) {
        info!(target: "snapshot_tool::info", "{}", msg);
    }

    fn warning(msg: &str) {
        warn!(target: "snapshot_tool::warning", "{}", msg);
    }

    fn error(msg: &str) {
        error!(target: "snapshot_tool::error", "{}", msg);
    }

    fn step(step: usize, total: usize, msg: &str) {
        debug!(target: "snapshot_tool::step", step = step, total = total, "{}", msg);
    }
}

fn main() -> Result<()> {
    // Initialize tracing for CLI tools
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "snapshot_tool=info".to_string())
        )
        .with_target(false)
        .without_time()
        .init();

    let config = Config::parse();
    
    info!(
        core_url = %config.core_url,
        command = ?std::mem::discriminant(&config.command),
        "Starting snapshot tool"
    );



    let result = match config.command {
        Commands::Take { output, pretty } => {
            take_snapshot(&config.core_url, &output, pretty)
        }
        Commands::FactoryRestore { input, force, data_dir, machine } => {
            factory_restore_snapshot(&input, force, data_dir, machine)
        }
        Commands::Restore { input } => {
            normal_restore_snapshot(&config.core_url, &input)
        }
        Commands::Report { input, output, format } => {
            report_snapshot_diff(&config.core_url, &input, output, format)
        }
        Commands::Validate { input } => {
            validate_snapshot(&input)
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
fn take_snapshot(
    core_url: &str, 
    output_path: &PathBuf, 
    pretty: bool
) -> Result<()> {
    Progress::step(1, 4, "Connecting to QCore service");
    let spinner = Progress::new_spinner("Establishing connection...");
    
    // Connect to the Core service
    let mut store = StoreProxy::connect(core_url)
        .with_context(|| format!("Failed to connect to Core service at {}", core_url))?;

    spinner.finish_with_message("Connected successfully");

    Progress::step(2, 4, "Taking snapshot from QCore service");
    let spinner = Progress::new_spinner("Retrieving schemas and entities...");

    // Take the JSON snapshot using the library function
    let snapshot = take_json_snapshot(&mut store)
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
    write(output_path, json_content.as_bytes())
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
fn factory_restore_snapshot(
    input_path: &PathBuf, 
    force: bool, 
    data_dir: Option<PathBuf>, 
    machine_id: String
) -> Result<()> {
    Progress::step(1, 4, "Loading snapshot file");
    let spinner = Progress::new_spinner("Reading and parsing JSON...");

    // Read and parse the snapshot file
    let snapshot = load_json_snapshot(input_path)?;
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
    factory_restore_json_snapshot(&snapshot, target_data_dir.clone(), machine_id.clone())
        .context("Failed to perform factory restore")?;

    spinner.finish_with_message("Factory restore completed");

    Progress::step(4, 4, "Factory restoration complete");
    Progress::success("Factory restoration completed successfully!");
    Progress::info(&format!("Files created in: {}", target_data_dir.display()));
    Progress::info(&format!("Start QCore with: --data-dir {} --machine {}", target_data_dir.display(), machine_id));

    Ok(())
}

/// Normal restore via StoreProxy: connect to service and apply differences
fn normal_restore_snapshot(
    core_url: &str,
    input_path: &PathBuf,
) -> Result<()> {
    Progress::step(1, 4, "Loading snapshot file");
    let spinner = Progress::new_spinner("Reading and parsing JSON...");

    // Read and parse the snapshot file
    let snapshot = load_json_snapshot(input_path)?;
    let entity_count = count_entities_in_tree(&snapshot.tree);
    
    spinner.finish_with_message(format!("Loaded {} schemas, {} entities", snapshot.schemas.len(), entity_count));

    Progress::step(2, 4, "Connecting to QCore service");
    let spinner = Progress::new_spinner("Establishing connection...");
    
    // Connect to the Core service
    let mut store = StoreProxy::connect(core_url)
        .with_context(|| format!("Failed to connect to Core service at {}", core_url))?;

    spinner.finish_with_message("Connected successfully");

    Progress::step(3, 4, "Computing and applying differences");
    let spinner = Progress::new_spinner("Analyzing current state and applying changes...");

    // Use the new restore via proxy function
    restore_json_snapshot_via_proxy(&mut store, &snapshot)
        .context("Failed to restore snapshot via proxy")?;

    spinner.finish_with_message("Changes applied successfully");

    Progress::step(4, 4, "Normal restoration complete");
    Progress::success("Normal restoration completed successfully!");
    Progress::info("The QCore service has been updated with the snapshot data.");

    Ok(())
}

/// Generate a report showing what would be changed by a restore operation
fn report_snapshot_diff(
    core_url: &str,
    input_path: &PathBuf,
    output_path: Option<PathBuf>,
    format: ReportFormat,
) -> Result<()> {
    Progress::step(1, 4, "Loading snapshot file");
    let spinner = Progress::new_spinner("Reading and parsing JSON...");

    // Read and parse the snapshot file
    let target_snapshot = load_json_snapshot(input_path)?;
    let entity_count = count_entities_in_tree(&target_snapshot.tree);
    
    spinner.finish_with_message(format!("Loaded {} schemas, {} entities", target_snapshot.schemas.len(), entity_count));

    Progress::step(2, 4, "Connecting to QCore service");
    let spinner = Progress::new_spinner("Establishing connection...");
    
    // Connect to the Core service
    let mut store = StoreProxy::connect(core_url)
        .with_context(|| format!("Failed to connect to Core service at {}", core_url))?;

    spinner.finish_with_message("Connected successfully");

    Progress::step(3, 4, "Taking current snapshot");
    let spinner = Progress::new_spinner("Retrieving current state...");

    // Take current snapshot to compute diff
    let current_snapshot = take_json_snapshot(&mut store)
        .context("Failed to take current snapshot")?;

    spinner.finish_with_message("Current state captured");

    Progress::step(4, 4, "Computing differences");
    let spinner = Progress::new_spinner("Analyzing changes...");

    // Compute differences
    let diff_report = compute_snapshot_diff(&current_snapshot, &target_snapshot)?;

    spinner.finish_with_message("Differences computed");

    // Format and output the report
    let report_content = match format {
        ReportFormat::Text => format_diff_report_text(&diff_report),
        ReportFormat::Json => format_diff_report_json(&diff_report)?,
    };

    if let Some(output) = output_path {
        std::fs::write(&output, report_content.as_bytes())
            .with_context(|| format!("Failed to write report to {}", output.display()))?;
        Progress::success(&format!("Report written to {}", output.display()));
    } else {
        info!("{}", report_content);
    }

    Progress::success("Report generation completed successfully!");

    Ok(())
}

/// Load and parse a JSON snapshot file
fn load_json_snapshot(input_path: &PathBuf) -> Result<JsonSnapshot> {
    let json_content = read_to_string(input_path)
        .with_context(|| format!("Failed to read snapshot file: {}", input_path.display()))?;
    
    serde_json::from_str(&json_content)
        .with_context(|| format!("Failed to parse JSON snapshot from: {}", input_path.display()))
}

/// Validate a snapshot file without restoring it
fn validate_snapshot(input_path: &PathBuf) -> Result<()> {
    Progress::step(1, 3, "Loading snapshot file");
    let spinner = Progress::new_spinner("Reading and parsing JSON...");

    // Read and parse the snapshot file
    let snapshot = load_json_snapshot(input_path)?;

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
        let inheritance = if !schema.inherits_from.is_empty() {
            format!(" (inherits from {})", schema.inherits_from.join(", "))
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

/// Validate that all schemas have proper JSON structure
fn validate_schemas(schemas: &[JsonEntitySchema]) -> Result<()> {
    for schema in schemas {
        // Basic validation - check that the schema has required fields
        if schema.entity_type.is_empty() {
            return Err(anyhow::anyhow!("Schema missing entity_type"));
        }
        // Additional validation could be added here for field schema format
    }
    Ok(())
}

/// Represents the differences between two snapshots
#[derive(Debug, Clone, serde::Serialize)]
struct SnapshotDiff {
    schema_changes: Vec<SchemaChange>,
    entity_changes: Vec<EntityChange>,
}

#[derive(Debug, Clone, serde::Serialize)]
enum SchemaChange {
    Added(JsonEntitySchema),
    Modified { 
        entity_type: String, 
        current: JsonEntitySchema, 
        target: JsonEntitySchema 
    },
    Removed(JsonEntitySchema),
}

#[derive(Debug, Clone, serde::Serialize)]
enum EntityChange {
    Added(JsonEntity),
    Modified { 
        path: String,
        current: JsonEntity, 
        target: JsonEntity,
        field_changes: Vec<FieldChange>,
    },
    Removed(JsonEntity),
}

#[derive(Debug, Clone, serde::Serialize)]
struct FieldChange {
    field_name: String,
    current_value: Option<serde_json::Value>,
    target_value: Option<serde_json::Value>,
}

/// Compute the differences between current and target snapshots
fn compute_snapshot_diff(current: &JsonSnapshot, target: &JsonSnapshot) -> Result<SnapshotDiff> {
    let mut schema_changes = Vec::new();
    let mut entity_changes = Vec::new();

    // Compare schemas
    let current_schemas: std::collections::HashMap<String, &JsonEntitySchema> = current.schemas
        .iter()
        .map(|s| (s.entity_type.clone(), s))
        .collect();
    
    let target_schemas: std::collections::HashMap<String, &JsonEntitySchema> = target.schemas
        .iter()
        .map(|s| (s.entity_type.clone(), s))
        .collect();

    // Find added and modified schemas
    for (entity_type, target_schema) in &target_schemas {
        match current_schemas.get(entity_type) {
            Some(current_schema) => {
                // Check if schema changed
                if !schemas_equal(current_schema, target_schema) {
                    schema_changes.push(SchemaChange::Modified {
                        entity_type: entity_type.clone(),
                        current: (*current_schema).clone(),
                        target: (*target_schema).clone(),
                    });
                }
            }
            _ => {
                schema_changes.push(SchemaChange::Added((*target_schema).clone()));
            }
        }
    }

    // Find removed schemas
    for (entity_type, current_schema) in &current_schemas {
        if !target_schemas.contains_key(entity_type) {
            schema_changes.push(SchemaChange::Removed((*current_schema).clone()));
        }
    }

    // Compare entity trees
    compare_entities_recursive(&current.tree, &target.tree, "Root".to_string(), &mut entity_changes)?;

    Ok(SnapshotDiff {
        schema_changes,
        entity_changes,
    })
}

/// Compare two schemas for equality (simplified)
fn schemas_equal(a: &JsonEntitySchema, b: &JsonEntitySchema) -> bool {
    serde_json::to_string(a).unwrap_or_default() == serde_json::to_string(b).unwrap_or_default()
}

/// Recursively compare entities in the tree
fn compare_entities_recursive(
    current_entity: &JsonEntity,
    target_entity: &JsonEntity,
    path: String,
    changes: &mut Vec<EntityChange>,
) -> Result<()> {
    // Check if entity types match
    if current_entity.entity_type != target_entity.entity_type {
        // This is essentially a remove + add
        changes.push(EntityChange::Removed(current_entity.clone()));
        changes.push(EntityChange::Added(target_entity.clone()));
        return Ok(());
    }

    // Compare field values
    let mut field_changes = Vec::new();
    let mut all_field_names = std::collections::HashSet::new();
    
    for field_name in current_entity.fields.keys() {
        all_field_names.insert(field_name.clone());
    }
    for field_name in target_entity.fields.keys() {
        all_field_names.insert(field_name.clone());
    }

    for field_name in &all_field_names {
        if field_name == "Children" {
            continue; // Handle children separately
        }

        let current_value = current_entity.fields.get(field_name);
        let target_value = target_entity.fields.get(field_name);

        if current_value != target_value {
            field_changes.push(FieldChange {
                field_name: field_name.clone(),
                current_value: current_value.cloned(),
                target_value: target_value.cloned(),
            });
        }
    }

    if !field_changes.is_empty() {
        changes.push(EntityChange::Modified {
            path: path.clone(),
            current: current_entity.clone(),
            target: target_entity.clone(),
            field_changes,
        });
    }

    // Compare children
    let current_children = current_entity.fields.get("Children")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().collect::<Vec<_>>())
        .unwrap_or_default();
    
    let target_children = target_entity.fields.get("Children")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().collect::<Vec<_>>())
        .unwrap_or_default();

    // Simple comparison: match by name and entity type
    let current_children_map: std::collections::HashMap<String, &serde_json::Value> = current_children
        .iter()
        .filter_map(|child| {
            let entity_type = child.get("entityType")?.as_str()?;
            let name = child.get("Name")?.as_str()?;
            Some((format!("{}:{}", entity_type, name), *child))
        })
        .collect();

    let target_children_map: std::collections::HashMap<String, &serde_json::Value> = target_children
        .iter()
        .filter_map(|child| {
            let entity_type = child.get("entityType")?.as_str()?;
            let name = child.get("Name")?.as_str()?;
            Some((format!("{}:{}", entity_type, name), *child))
        })
        .collect();

    // Find added, modified, and removed children
    for (key, target_child) in &target_children_map {
        let child_path = format!("{}/{}", path, key.split(':').nth(1).unwrap_or("Unknown"));
        
        match current_children_map.get(key) {
            Some(current_child) => {
                // Compare this child
                let current_entity = serde_json::from_value::<JsonEntity>((*current_child).clone())
                    .context("Failed to parse current child entity")?;
                let target_entity = serde_json::from_value::<JsonEntity>((*target_child).clone())
                    .context("Failed to parse target child entity")?;
                
                compare_entities_recursive(&current_entity, &target_entity, child_path, changes)?;
            }
            None => {
                // Child added
                let target_entity = serde_json::from_value::<JsonEntity>((*target_child).clone())
                    .context("Failed to parse target child entity")?;
                changes.push(EntityChange::Added(target_entity));
            }
        }
    }

    // Find removed children
    for (key, current_child) in &current_children_map {
        if !target_children_map.contains_key(key) {
            let current_entity = serde_json::from_value::<JsonEntity>((*current_child).clone())
                .context("Failed to parse current child entity")?;
            changes.push(EntityChange::Removed(current_entity));
        }
    }

    Ok(())
}

/// Format the diff report as human-readable text
fn format_diff_report_text(diff: &SnapshotDiff) -> String {
    let mut report = String::new();
    
    report.push_str("=== SNAPSHOT DIFFERENCE REPORT ===\n\n");

    // Schema changes
    if !diff.schema_changes.is_empty() {
        report.push_str("SCHEMA CHANGES:\n");
        report.push_str("===============\n");
        
        for change in &diff.schema_changes {
            match change {
                SchemaChange::Added(schema) => {
                    report.push_str(&format!("+ ADDED SCHEMA: {}\n", schema.entity_type));
                    if !schema.inherits_from.is_empty() {
                        report.push_str(&format!("  Inherits from: {}\n", schema.inherits_from.join(", ")));
                    }
                    report.push_str(&format!("  Fields: {}\n", schema.fields.len()));
                }
                SchemaChange::Modified { entity_type, .. } => {
                    report.push_str(&format!("~ MODIFIED SCHEMA: {}\n", entity_type));
                }
                SchemaChange::Removed(schema) => {
                    report.push_str(&format!("- REMOVED SCHEMA: {}\n", schema.entity_type));
                }
            }
        }
        report.push_str("\n");
    }

    // Entity changes
    if !diff.entity_changes.is_empty() {
        report.push_str("ENTITY CHANGES:\n");
        report.push_str("===============\n");
        
        for change in &diff.entity_changes {
            match change {
                EntityChange::Added(entity) => {
                    let name = entity.fields.get("Name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("Unknown");
                    report.push_str(&format!("+ ADDED ENTITY: {} ({})\n", name, entity.entity_type));
                }
                EntityChange::Modified { path, field_changes, .. } => {
                    report.push_str(&format!("~ MODIFIED ENTITY: {}\n", path));
                    for field_change in field_changes {
                        report.push_str(&format!("  Field '{}': ", field_change.field_name));
                        match (&field_change.current_value, &field_change.target_value) {
                            (Some(current), Some(target)) => {
                                report.push_str(&format!("{:?} -> {:?}\n", current, target));
                            }
                            (None, Some(target)) => {
                                report.push_str(&format!("(none) -> {:?}\n", target));
                            }
                            (Some(current), None) => {
                                report.push_str(&format!("{:?} -> (removed)\n", current));
                            }
                            (None, None) => {
                                report.push_str("(no change)\n");
                            }
                        }
                    }
                }
                EntityChange::Removed(entity) => {
                    let name = entity.fields.get("Name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("Unknown");
                    report.push_str(&format!("- REMOVED ENTITY: {} ({})\n", name, entity.entity_type));
                }
            }
        }
        report.push_str("\n");
    }

    if diff.schema_changes.is_empty() && diff.entity_changes.is_empty() {
        report.push_str("No differences found. The snapshots are identical.\n");
    } else {
        report.push_str(&format!("SUMMARY: {} schema changes, {} entity changes\n", 
            diff.schema_changes.len(), diff.entity_changes.len()));
    }

    report
}

/// Format the diff report as JSON
fn format_diff_report_json(diff: &SnapshotDiff) -> Result<String> {
    serde_json::to_string_pretty(diff)
        .context("Failed to serialize diff report to JSON")
}