use anyhow::{Context, Result};
use clap::Parser;
use qlib_rs::{StoreProxy, EntityType, EntityId, PageOpts, PageResult, Value, FieldType, sread};
use std::collections::HashMap;
use tracing::{info, debug};
use serde_json;
use base64::{Engine as _, engine::general_purpose};
use std::pin::Pin;

/// Command-line tool for querying entities from the QCore data store with CEL filter support
#[derive(Parser)]
#[command(name = "select-tool", about = "Query entities from the QCore data store using CEL expressions")]
struct Config {
    /// QCore service URL (WebSocket endpoint for client connections)
    #[arg(long, default_value = "ws://localhost:9100")]
    core_url: String,

    /// Username for authentication (can be set via QCORE_USERNAME env var)
    #[arg(long, default_value = "qei")]
    username: String,

    /// Password for authentication (can be set via QCORE_PASSWORD env var)
    #[arg(long, default_value = "qei")]
    password: String,

    /// Entity type to search for (e.g., "User", "Project")
    #[arg(long, short)]
    entity_type: String,

    /// CEL expression to filter results
    /// Examples:
    /// - "Name == 'John'"
    /// - "Age > 25 && IsActive == true"
    /// - "Department->Name == 'Engineering'"
    /// - "Score >= 90.0"
    /// - "size(Tags) > 0"
    #[arg(long, short)]
    filter: Option<String>,

    /// Use exact type matching (do not include inherited types)
    #[arg(long)]
    exact: bool,

    /// Maximum number of results to return (0 = unlimited)
    #[arg(long, default_value_t = 0)]
    limit: usize,

    /// Page size for pagination (default: 50)
    #[arg(long, default_value_t = 50)]
    page_size: usize,

    /// Output format
    #[arg(long, default_value = "table")]
    format: OutputFormat,

    /// Fields to display in output (comma-separated)
    /// Examples: "Name,Age,IsActive" or "Name,Department->Name"
    #[arg(long)]
    fields: Option<String>,

    /// Show entity IDs in output
    #[arg(long)]
    show_ids: bool,

    /// Show entity types in output
    #[arg(long)]
    show_types: bool,

    /// Export results to file (JSON format)
    #[arg(long)]
    export: Option<std::path::PathBuf>,

    /// Verbose output (show debug information)
    #[arg(long, short)]
    verbose: bool,
}

#[derive(Debug, Clone, clap::ValueEnum)]
enum OutputFormat {
    /// Tabular format (human-readable)
    Table,
    /// JSON format
    Json,
    /// CSV format
    Csv,
    /// Simple list of entity IDs
    Ids,
    /// Count only (number of matching entities)
    Count,
}

/// Represents a field value for display
#[derive(Debug, Clone)]
enum DisplayValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    EntityRef(String),
    EntityList(Vec<String>),
    Timestamp(String),
    Blob(String), // Base64 encoded
    None,
    Error(String),
}

impl DisplayValue {
    fn to_string(&self) -> String {
        match self {
            DisplayValue::String(s) => s.clone(),
            DisplayValue::Integer(i) => i.to_string(),
            DisplayValue::Float(f) => f.to_string(),
            DisplayValue::Boolean(b) => b.to_string(),
            DisplayValue::EntityRef(s) => s.clone(),
            DisplayValue::EntityList(list) => format!("[{}]", list.join(", ")),
            DisplayValue::Timestamp(s) => s.clone(),
            DisplayValue::Blob(s) => format!("blob({})", s),
            DisplayValue::None => "".to_string(),
            DisplayValue::Error(e) => format!("ERROR: {}", e),
        }
    }

    fn from_value(value: Option<&Value>) -> Self {
        match value {
            Some(Value::String(s)) => DisplayValue::String(s.clone()),
            Some(Value::Int(i)) => DisplayValue::Integer(*i),
            Some(Value::Float(f)) => DisplayValue::Float(*f),
            Some(Value::Bool(b)) => DisplayValue::Boolean(*b),
            Some(Value::EntityReference(Some(entity_id))) => DisplayValue::EntityRef(entity_id.to_string()),
            Some(Value::EntityReference(None)) => DisplayValue::None,
            Some(Value::EntityList(list)) => {
                DisplayValue::EntityList(list.iter().map(|e| e.to_string()).collect())
            },
            Some(Value::Choice(choice)) => DisplayValue::Integer(*choice as i64),
            Some(Value::Timestamp(ts)) => {
                // ts is already an OffsetDateTime
                DisplayValue::Timestamp(ts.format(&time::format_description::well_known::Rfc3339).unwrap_or_else(|_| "Invalid timestamp".to_string()))
            },
            Some(Value::Blob(data)) => {
                // Convert to base64 for display
                DisplayValue::Blob(general_purpose::STANDARD.encode(data))
            },
            None => DisplayValue::None,
        }
    }
}

/// Represents an entity with its field values for display
#[derive(Debug)]
struct EntityDisplay {
    entity_id: EntityId,
    entity_type: String,
    fields: HashMap<String, DisplayValue>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing for CLI tools
    let log_level = if std::env::var("RUST_LOG").is_ok() {
        std::env::var("RUST_LOG").unwrap()
    } else if std::env::args().any(|arg| arg == "--verbose" || arg == "-v") {
        "select_tool=debug".to_string()
    } else {
        "select_tool=info".to_string()
    };

    tracing_subscriber::fmt()
        .with_env_filter(log_level)
        .with_target(false)
        .without_time()
        .init();

    let config = Config::parse();
    
    debug!(
        core_url = %config.core_url,
        entity_type = %config.entity_type,
        filter = ?config.filter,
        exact = config.exact,
        limit = config.limit,
        format = ?config.format,
        "Starting select tool"
    );

    // Get credentials from environment if available
    let username = std::env::var("QCORE_USERNAME").unwrap_or(config.username.clone());
    let password = std::env::var("QCORE_PASSWORD").unwrap_or(config.password.clone());

    info!(core_url = %config.core_url, "Connecting to QCore service");
    
    // Connect to the Core service with authentication
    let mut store = StoreProxy::connect_and_authenticate(&config.core_url, &username, &password).await
        .with_context(|| format!("Failed to connect to Core service at {}", config.core_url))?;

    info!("Connected successfully");

    // Parse entity type
    let entity_type = EntityType::from(config.entity_type.as_str());

    // Execute the query
    let results = execute_query(&mut store, &entity_type, config.filter.as_deref(), config.exact, config.limit, config.page_size).await
        .context("Failed to execute query")?;

    info!("Found {} matching entities", results.len());

    // Parse fields to display
    let fields_to_display = parse_fields(&config.fields);

    // Fetch field values for the results
    let entities_with_data = fetch_entity_data(&mut store, &results, &fields_to_display).await
        .context("Failed to fetch entity data")?;

    // Display results
    display_results(&entities_with_data, &config).await
        .context("Failed to display results")?;

    // Export if requested
    if let Some(export_path) = &config.export {
        export_results(&entities_with_data, export_path).await
            .context("Failed to export results")?;
        info!("Results exported to {}", export_path.display());
    }

    Ok(())
}

/// Execute the query against the store
async fn execute_query(
    store: &mut StoreProxy,
    entity_type: &EntityType,
    filter: Option<&str>,
    exact: bool,
    limit: usize,
    page_size: usize,
) -> Result<Vec<EntityId>> {
    let mut results = Vec::new();
    let mut page_opts: Option<PageOpts> = Some(PageOpts::new(page_size, None));
    let mut total_fetched = 0;

    info!(
        entity_type = %entity_type.as_ref(),
        filter = ?filter,
        exact = exact,
        "Executing query"
    );

    loop {
        let page_result: PageResult<EntityId> = if exact {
            store.find_entities_exact(entity_type, page_opts.clone(), filter.map(|s| s.to_string())).await?
        } else {
            store.find_entities_paginated(entity_type, page_opts.clone(), filter.map(|s| s.to_string())).await?
        };

        if page_result.items.is_empty() {
            break;
        }

        let items_to_take = if limit > 0 {
            std::cmp::min(page_result.items.len(), limit - total_fetched)
        } else {
            page_result.items.len()
        };

        results.extend(page_result.items.into_iter().take(items_to_take));
        total_fetched += items_to_take;

        debug!("Fetched {} entities (total: {})", items_to_take, total_fetched);

        // Check if we've reached the limit or there are no more pages
        if (limit > 0 && total_fetched >= limit) || page_result.next_cursor.is_none() {
            break;
        }

        // Set up next page
        page_opts = Some(PageOpts::new(page_size, page_result.next_cursor));
    }

    Ok(results)
}

/// Parse the fields parameter into a list of field names
fn parse_fields(fields: &Option<String>) -> Vec<String> {
    match fields {
        Some(fields_str) => fields_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
        None => vec!["Name".to_string()], // Default to Name field
    }
}

/// Fetch field data for the given entities
async fn fetch_entity_data(
    store: &mut StoreProxy,
    entity_ids: &[EntityId],
    fields: &[String],
) -> Result<Vec<EntityDisplay>> {
    let mut results = Vec::new();

    for entity_id in entity_ids {
        debug!("Fetching data for entity: {}", entity_id);

        let mut entity_display = EntityDisplay {
            entity_id: entity_id.clone(),
            entity_type: entity_id.get_type().as_ref().to_string(),
            fields: HashMap::new(),
        };

        // Fetch each requested field
        for field_name in fields {
            let display_value = fetch_field_value(store, entity_id, field_name).await;
            entity_display.fields.insert(field_name.clone(), display_value);
        }

        results.push(entity_display);
    }

    Ok(results)
}

/// Fetch a single field value for an entity
fn fetch_field_value<'a>(store: &'a mut StoreProxy, entity_id: &'a EntityId, field_name: &'a str) -> Pin<Box<dyn std::future::Future<Output = DisplayValue> + 'a>> {
    Box::pin(async move {
        // Handle indirection (e.g., "Department->Name")
        if field_name.contains("->") {
            return fetch_indirect_field_value(store, entity_id, field_name).await;
        }

        // Regular field access
        let field_type = FieldType::from(field_name);
        let mut requests = vec![sread!(entity_id.clone(), field_type)];

        match store.perform(&mut requests).await {
            Ok(_) => {
                if let Some(request) = requests.first() {
                    DisplayValue::from_value(request.value())
                } else {
                    DisplayValue::Error("No request returned".to_string())
                }
            }
            Err(e) => {
                debug!("Failed to fetch field '{}' for entity {}: {}", field_name, entity_id, e);
                DisplayValue::Error(format!("Failed to fetch: {}", e))
            }
        }
    })
}

/// Fetch an indirect field value (e.g., "Department->Name")
fn fetch_indirect_field_value<'a>(store: &'a mut StoreProxy, entity_id: &'a EntityId, field_path: &'a str) -> Pin<Box<dyn std::future::Future<Output = DisplayValue> + 'a>> {
    Box::pin(async move {
        let parts: Vec<&str> = field_path.split("->").collect();
        if parts.len() < 2 {
            return DisplayValue::Error("Invalid indirection syntax".to_string());
        }

        let mut current_entity_id = entity_id.clone();

        // Follow the chain of references
        for (i, part) in parts.iter().enumerate() {
            if i == parts.len() - 1 {
                // Last part - fetch the actual field value
                return fetch_field_value(store, &current_entity_id, part).await;
            } else {
                // Intermediate part - follow the reference
                let field_type = FieldType::from(*part);
                let mut requests = vec![sread!(current_entity_id.clone(), field_type)];

                match store.perform(&mut requests).await {
                    Ok(_) => {
                        if let Some(request) = requests.first() {
                            if let Some(Value::EntityReference(Some(ref_entity_id))) = request.value() {
                                current_entity_id = ref_entity_id.clone();
                            } else {
                                return DisplayValue::Error(format!("Field '{}' is not an entity reference or is null", part));
                            }
                        } else {
                            return DisplayValue::Error("No request returned".to_string());
                        }
                    }
                    Err(e) => {
                        return DisplayValue::Error(format!("Failed to follow reference '{}': {}", part, e));
                    }
                }
            }
        }

        DisplayValue::Error("Unexpected end of indirection path".to_string())
    })
}

/// Display the results according to the configured format
async fn display_results(entities: &[EntityDisplay], config: &Config) -> Result<()> {
    match config.format {
        OutputFormat::Count => {
            println!("{}", entities.len());
        }
        OutputFormat::Ids => {
            for entity in entities {
                println!("{}", entity.entity_id);
            }
        }
        OutputFormat::Json => {
            display_json(entities, config)?;
        }
        OutputFormat::Csv => {
            display_csv(entities, config)?;
        }
        OutputFormat::Table => {
            display_table(entities, config)?;
        }
    }

    Ok(())
}

/// Display results in JSON format
fn display_json(entities: &[EntityDisplay], config: &Config) -> Result<()> {
    let mut json_entities = Vec::new();

    for entity in entities {
        let mut json_entity = serde_json::Map::new();

        if config.show_ids {
            json_entity.insert("id".to_string(), serde_json::Value::String(entity.entity_id.to_string()));
        }

        if config.show_types {
            json_entity.insert("type".to_string(), serde_json::Value::String(entity.entity_type.clone()));
        }

        for (field_name, field_value) in &entity.fields {
            let json_value = match field_value {
                DisplayValue::String(s) => serde_json::Value::String(s.clone()),
                DisplayValue::Integer(i) => serde_json::Value::Number(serde_json::Number::from(*i)),
                DisplayValue::Float(f) => {
                    if let Some(num) = serde_json::Number::from_f64(*f) {
                        serde_json::Value::Number(num)
                    } else {
                        serde_json::Value::Null
                    }
                }
                DisplayValue::Boolean(b) => serde_json::Value::Bool(*b),
                DisplayValue::EntityRef(s) => serde_json::Value::String(s.clone()),
                DisplayValue::EntityList(list) => {
                    serde_json::Value::Array(list.iter().map(|s| serde_json::Value::String(s.clone())).collect())
                }
                DisplayValue::Timestamp(s) => serde_json::Value::String(s.clone()),
                DisplayValue::Blob(s) => serde_json::Value::String(s.clone()),
                DisplayValue::None => serde_json::Value::Null,
                DisplayValue::Error(e) => serde_json::Value::String(format!("ERROR: {}", e)),
            };
            json_entity.insert(field_name.clone(), json_value);
        }

        json_entities.push(serde_json::Value::Object(json_entity));
    }

    let output = serde_json::to_string_pretty(&json_entities)
        .context("Failed to serialize results to JSON")?;
    println!("{}", output);

    Ok(())
}

/// Display results in CSV format
fn display_csv(entities: &[EntityDisplay], config: &Config) -> Result<()> {
    if entities.is_empty() {
        return Ok(());
    }

    // Collect all field names
    let mut field_names = std::collections::HashSet::new();
    for entity in entities {
        for field_name in entity.fields.keys() {
            field_names.insert(field_name.clone());
        }
    }

    let mut sorted_fields: Vec<String> = field_names.into_iter().collect();
    sorted_fields.sort();

    // Print header
    let mut header = Vec::new();
    if config.show_ids {
        header.push("id".to_string());
    }
    if config.show_types {
        header.push("type".to_string());
    }
    header.extend(sorted_fields.clone());
    
    println!("{}", header.join(","));

    // Print data rows
    for entity in entities {
        let mut row = Vec::new();

        if config.show_ids {
            row.push(csv_escape(&entity.entity_id.to_string()));
        }

        if config.show_types {
            row.push(csv_escape(&entity.entity_type));
        }

        for field_name in &sorted_fields {
            let value = entity.fields.get(field_name)
                .map(|v| v.to_string())
                .unwrap_or_else(|| "".to_string());
            row.push(csv_escape(&value));
        }

        println!("{}", row.join(","));
    }

    Ok(())
}

/// Escape a string for CSV output
fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

/// Display results in table format
fn display_table(entities: &[EntityDisplay], config: &Config) -> Result<()> {
    if entities.is_empty() {
        println!("No results found.");
        return Ok(());
    }

    // Collect all field names and calculate column widths
    let mut field_names = std::collections::HashSet::new();
    for entity in entities {
        for field_name in entity.fields.keys() {
            field_names.insert(field_name.clone());
        }
    }

    let mut sorted_fields: Vec<String> = field_names.into_iter().collect();
    sorted_fields.sort();

    // Calculate column widths
    let mut column_widths = std::collections::HashMap::new();

    if config.show_ids {
        let mut max_width = 2; // "id"
        for entity in entities {
            max_width = max_width.max(entity.entity_id.to_string().len());
        }
        column_widths.insert("id".to_string(), max_width);
    }

    if config.show_types {
        let mut max_width = 4; // "type"
        for entity in entities {
            max_width = max_width.max(entity.entity_type.len());
        }
        column_widths.insert("type".to_string(), max_width);
    }

    for field_name in &sorted_fields {
        let mut max_width = field_name.len();
        for entity in entities {
            if let Some(value) = entity.fields.get(field_name) {
                max_width = max_width.max(value.to_string().len());
            }
        }
        column_widths.insert(field_name.clone(), max_width);
    }

    // Print header
    let mut header = Vec::new();
    if config.show_ids {
        header.push(format!("{:width$}", "id", width = column_widths["id"]));
    }
    if config.show_types {
        header.push(format!("{:width$}", "type", width = column_widths["type"]));
    }
    for field_name in &sorted_fields {
        header.push(format!("{:width$}", field_name, width = column_widths[field_name]));
    }
    println!("{}", header.join(" | "));

    // Print separator
    let mut separator = Vec::new();
    if config.show_ids {
        separator.push("-".repeat(column_widths["id"]));
    }
    if config.show_types {
        separator.push("-".repeat(column_widths["type"]));
    }
    for field_name in &sorted_fields {
        separator.push("-".repeat(column_widths[field_name]));
    }
    println!("{}", separator.join("-|-"));

    // Print data rows
    for entity in entities {
        let mut row = Vec::new();

        if config.show_ids {
            row.push(format!("{:width$}", entity.entity_id.to_string(), width = column_widths["id"]));
        }

        if config.show_types {
            row.push(format!("{:width$}", entity.entity_type, width = column_widths["type"]));
        }

        for field_name in &sorted_fields {
            let value = entity.fields.get(field_name)
                .map(|v| v.to_string())
                .unwrap_or_else(|| "".to_string());
            let truncated_value = if value.len() > 50 {
                format!("{}...", &value[..47])
            } else {
                value
            };
            row.push(format!("{:width$}", truncated_value, width = column_widths[field_name].min(50)));
        }

        println!("{}", row.join(" | "));
    }

    println!("\nTotal: {} entities", entities.len());

    Ok(())
}

/// Export results to a JSON file
async fn export_results(entities: &[EntityDisplay], export_path: &std::path::PathBuf) -> Result<()> {
    let mut json_entities = Vec::new();

    for entity in entities {
        let mut json_entity = serde_json::Map::new();

        json_entity.insert("id".to_string(), serde_json::Value::String(entity.entity_id.to_string()));
        json_entity.insert("type".to_string(), serde_json::Value::String(entity.entity_type.clone()));

        for (field_name, field_value) in &entity.fields {
            let json_value = match field_value {
                DisplayValue::String(s) => serde_json::Value::String(s.clone()),
                DisplayValue::Integer(i) => serde_json::Value::Number(serde_json::Number::from(*i)),
                DisplayValue::Float(f) => {
                    if let Some(num) = serde_json::Number::from_f64(*f) {
                        serde_json::Value::Number(num)
                    } else {
                        serde_json::Value::Null
                    }
                }
                DisplayValue::Boolean(b) => serde_json::Value::Bool(*b),
                DisplayValue::EntityRef(s) => serde_json::Value::String(s.clone()),
                DisplayValue::EntityList(list) => {
                    serde_json::Value::Array(list.iter().map(|s| serde_json::Value::String(s.clone())).collect())
                }
                DisplayValue::Timestamp(s) => serde_json::Value::String(s.clone()),
                DisplayValue::Blob(s) => serde_json::Value::String(s.clone()),
                DisplayValue::None => serde_json::Value::Null,
                DisplayValue::Error(e) => serde_json::Value::String(format!("ERROR: {}", e)),
            };
            json_entity.insert(field_name.clone(), json_value);
        }

        json_entities.push(serde_json::Value::Object(json_entity));
    }

    let output = serde_json::to_string_pretty(&json_entities)
        .context("Failed to serialize results to JSON")?;

    tokio::fs::write(export_path, output).await
        .with_context(|| format!("Failed to write to {}", export_path.display()))?;

    Ok(())
}