use anyhow::{Context, Result};
use clap::Parser;
use qlib_rs::{sread, EntityId, EntityType, FieldType, PageOpts, PageResult, StoreProxy, Value};
use std::collections::HashMap;
use tracing::{info, debug};
use serde_json;
use base64::{Engine as _, engine::general_purpose};
use std::time::{Instant, Duration};

/// Command-line tool for querying entities from the QCore data store with CEL filter support
#[derive(Parser)]
#[command(name = "select-tool", about = "Query entities from the QCore data store using CEL expressions")]
struct Config {
    /// QCore service URL (TCP endpoint for client connections)
    #[arg(long, default_value = "localhost:9100")]
    core_url: String,

    /// Username for authentication (can be set via QCORE_USERNAME env var)
    #[arg(long, default_value = "qselect")]
    username: String,

    /// Password for authentication (can be set via QCORE_PASSWORD env var)
    #[arg(long, default_value = "qselect")]
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

    /// Show performance metrics (timing and data transfer stats)
    #[arg(long)]
    metrics: bool,
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
        }
    }

    fn from_value(value: Option<&Value>) -> Self {
        match value {
            Some(Value::String(s)) => DisplayValue::String(s.clone()),
            Some(Value::Int(i)) => DisplayValue::Integer(*i),
            Some(Value::Float(f)) => DisplayValue::Float(*f),
            Some(Value::Bool(b)) => DisplayValue::Boolean(*b),
            Some(Value::EntityReference(Some(entity_id))) => DisplayValue::EntityRef(format_entity_id(*entity_id)),
            Some(Value::EntityReference(None)) => DisplayValue::None,
            Some(Value::EntityList(list)) => {
                DisplayValue::EntityList(list.iter().map(|e| format_entity_id(*e)).collect())
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

/// Helper function to format EntityId for display
fn format_entity_id(entity_id: EntityId) -> String {
    format!("{}:{}", entity_id.extract_type().0, entity_id.extract_id())
}

/// Performance metrics for query execution
#[derive(Debug, Clone)]
struct QueryMetrics {
    initialization_time: Duration,
    connection_time: Duration,
    query_time: Duration,
    field_fetch_time: Duration,
    display_time: Duration,
    export_time: Duration,
    total_time: Duration,
    entities_found: usize,
    pages_fetched: usize,
    fields_fetched: usize,
}

impl QueryMetrics {
    fn new() -> Self {
        Self {
            initialization_time: Duration::from_secs(0),
            connection_time: Duration::from_secs(0),
            query_time: Duration::from_secs(0),
            field_fetch_time: Duration::from_secs(0),
            display_time: Duration::from_secs(0),
            export_time: Duration::from_secs(0),
            total_time: Duration::from_secs(0),
            entities_found: 0,
            pages_fetched: 0,
            fields_fetched: 0,
        }
    }

    fn format_duration(d: &Duration) -> String {
        let nanos = d.as_nanos();
        
        if nanos < 1_000 {
            format!("{}ns", nanos)
        } else if nanos < 1_000_000 {
            format!("{:.1}μs", nanos as f64 / 1_000.0)
        } else if nanos < 1_000_000_000 {
            format!("{:.1}ms", nanos as f64 / 1_000_000.0)
        } else {
            format!("{:.2}s", d.as_secs_f64())
        }
    }

    fn print_metrics(&self) {
        println!("\n=== Performance Metrics ===");
        println!("Initialization:  {}", Self::format_duration(&self.initialization_time));
        println!("Connection time: {}", Self::format_duration(&self.connection_time));
        println!("Query execution: {}", Self::format_duration(&self.query_time));
        println!("Field fetching:  {}", Self::format_duration(&self.field_fetch_time));
        println!("Display time:    {}", Self::format_duration(&self.display_time));
        if self.export_time.as_nanos() > 0 {
            println!("Export time:     {}", Self::format_duration(&self.export_time));
        }
        println!("Total time:      {}", Self::format_duration(&self.total_time));
        
        // Calculate unaccounted time
        let accounted_time = self.initialization_time + self.connection_time + self.query_time + self.field_fetch_time + self.display_time + self.export_time;
        let unaccounted_time = self.total_time.saturating_sub(accounted_time);
        if unaccounted_time.as_nanos() > 0 {
            println!("Unaccounted:     {}", Self::format_duration(&unaccounted_time));
        }
        
        println!();
        println!("Entities found:  {}", self.entities_found);
        println!("Pages fetched:   {}", self.pages_fetched);
        println!("Fields fetched:  {}", self.fields_fetched);
        
        if self.entities_found > 0 {
            let avg_query_nanos = self.query_time.as_nanos() as f64 / self.pages_fetched as f64;
            let avg_field_nanos = if self.fields_fetched > 0 {
                self.field_fetch_time.as_nanos() as f64 / self.fields_fetched as f64
            } else {
                0.0
            };
            
            let avg_query_str = if avg_query_nanos < 1_000.0 {
                format!("{:.0}ns", avg_query_nanos)
            } else if avg_query_nanos < 1_000_000.0 {
                format!("{:.1}μs", avg_query_nanos / 1_000.0)
            } else {
                format!("{:.1}ms", avg_query_nanos / 1_000_000.0)
            };
            
            let avg_field_str = if avg_field_nanos < 1_000.0 {
                format!("{:.0}ns", avg_field_nanos)
            } else if avg_field_nanos < 1_000_000.0 {
                format!("{:.1}μs", avg_field_nanos / 1_000.0)
            } else {
                format!("{:.1}ms", avg_field_nanos / 1_000_000.0)
            };
            
            println!();
            println!("Average per page: {}", avg_query_str);
            println!("Average per field: {}", avg_field_str);
        }
        
        let throughput = if self.total_time.as_secs_f64() > 0.0 {
            self.entities_found as f64 / self.total_time.as_secs_f64()
        } else {
            0.0
        };
        println!("Entity throughput: {:.1} entities/sec", throughput);
    }
}

fn main() -> Result<()> {
    let total_start = Instant::now();
    let mut metrics = QueryMetrics::new();

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

    // Mark end of initialization
    metrics.initialization_time = total_start.elapsed();

    info!(core_url = %config.core_url, "Connecting to QCore service");
    
    // Connect to the Core service with authentication
    let connection_start = Instant::now();
    let mut store = StoreProxy::connect_and_authenticate(&config.core_url, &username, &password)
        .with_context(|| format!("Failed to connect to Core service at {}", config.core_url))?;
    metrics.connection_time = connection_start.elapsed();

    info!("Connected successfully");

    // Parse entity type
    let entity_type = store.get_entity_type(&config.entity_type)
        .context("Failed to get entity type")?;

    // Execute the query
    let query_start = Instant::now();
    let (results, pages_fetched) = execute_query(&mut store, entity_type, config.filter.as_deref(), config.exact, config.limit, config.page_size)
        .context("Failed to execute query")?;
    metrics.query_time = query_start.elapsed();
    metrics.entities_found = results.len();
    metrics.pages_fetched = pages_fetched;

    info!("Found {} matching entities", results.len());

    // Parse fields to display
    let fields_to_display = parse_fields(&mut store, &config.fields)
        .context("Failed to parse field types")?;

    // Fetch field values for the results
    let field_fetch_start = Instant::now();
    let (entities_with_data, fields_fetched) = fetch_entity_data(&mut store, &results, &fields_to_display)
        .context("Failed to fetch entity data")?;
    metrics.field_fetch_time = field_fetch_start.elapsed();
    metrics.fields_fetched = fields_fetched;

    // Display results
    let display_start = Instant::now();
    display_results(&entities_with_data, &config)
        .context("Failed to display results")?;
    metrics.display_time = display_start.elapsed();

    // Export if requested
    if let Some(export_path) = &config.export {
        let export_start = Instant::now();
        export_results(&entities_with_data, export_path)
            .context("Failed to export results")?;
        metrics.export_time = export_start.elapsed();
        info!("Results exported to {}", export_path.display());
    }

    // Update total time to include display and export
    metrics.total_time = total_start.elapsed();

    // Show metrics if requested
    if config.metrics {
        metrics.print_metrics();
    }

    Ok(())
}

/// Execute the query against the store
fn execute_query(
    store: &mut StoreProxy,
    entity_type: EntityType,
    filter: Option<&str>,
    exact: bool,
    limit: usize,
    page_size: usize,
) -> Result<(Vec<EntityId>, usize)> {
    let mut results = Vec::new();
    let mut page_opts: Option<PageOpts> = Some(PageOpts::new(page_size, None));
    let mut total_fetched = 0;
    let mut pages_fetched = 0;

    info!(
        entity_type = ?entity_type,
        filter = ?filter,
        exact = exact,
        "Executing query"
    );

    loop {
        let page_result: PageResult<EntityId> = if exact {
            store.find_entities_exact(entity_type, page_opts.clone(), filter.map(|s| s.to_string()))?
        } else {
            store.find_entities_paginated(entity_type, page_opts.clone(), filter.map(|s| s.to_string()))?
        };

        pages_fetched += 1;

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

    Ok((results, pages_fetched))
}

/// Parse the fields parameter into a list of field paths (Vec<FieldType> for each field)
fn parse_fields(store: &mut StoreProxy, fields: &Option<String>) -> Result<Vec<(String, Vec<FieldType>)>> {
    let field_names = match fields {
        Some(fields_str) => fields_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
        None => vec!["Name".to_string()], // Default to Name field
    };

    let mut parsed_fields = Vec::new();
    for field_name in field_names {
        // Parse field indirection syntax (e.g., "Parent->Name")
        let field_parts: Vec<&str> = field_name.split("->").collect();
        let mut field_types = Vec::new();
        
        for part in field_parts {
            let field_type = store.get_field_type(part)
                .with_context(|| format!("Failed to get field type for '{}'", part))?;
            field_types.push(field_type);
        }
        
        parsed_fields.push((field_name, field_types));
    }
    
    Ok(parsed_fields)
}

/// Fetch field data for the given entities
fn fetch_entity_data(
    store: &mut StoreProxy,
    entity_ids: &[EntityId],
    fields: &[(String, Vec<FieldType>)],
) -> Result<(Vec<EntityDisplay>, usize)> {
    let mut results = Vec::new();
    let mut fields_fetched = 0;


    for entity_id in entity_ids {
        debug!("Fetching data for entity: {:?}", entity_id);
        let mut reqs = Vec::new();

        for (_, field_types) in fields {
            reqs.push(sread!(*entity_id, field_types.clone()));
        }

        match store.perform(reqs) {
            Ok(res) => {
                fields_fetched += fields.len();
                results.push(EntityDisplay {
                    entity_id: *entity_id,
                    entity_type: store.resolve_entity_type(entity_id.extract_type())
                        .unwrap_or_else(|_| format!("Unknown({})", entity_id.extract_type().0)),
                    fields: res.into_iter().enumerate().map(|(i, r)| {
                        let field_name = fields[i].0.clone();
                        let display_value = r.value()
                            .map(|v| DisplayValue::from_value(Some(&v)))
                            .unwrap_or(DisplayValue::None);
                        (field_name, display_value)
                    }).collect(),
                });
            },
            Err(e) => {
                return Err(anyhow::anyhow!("Failed to fetch fields: {}", e));
            }
        }
    }

    Ok((results, fields_fetched))
}

/// Display the results according to the configured format
fn display_results(entities: &[EntityDisplay], config: &Config) -> Result<()> {
    match config.format {
        OutputFormat::Count => {
            println!("{}", entities.len());
        }
        OutputFormat::Ids => {
            for entity in entities {
                println!("{}", format_entity_id(entity.entity_id));
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
            json_entity.insert("id".to_string(), serde_json::Value::String(format_entity_id(entity.entity_id)));
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
            row.push(csv_escape(&format_entity_id(entity.entity_id)));
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
            max_width = max_width.max(format_entity_id(entity.entity_id).len());
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
            row.push(format!("{:width$}", format_entity_id(entity.entity_id), width = column_widths["id"]));
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
fn export_results(entities: &[EntityDisplay], export_path: &std::path::PathBuf) -> Result<()> {
    let mut json_entities = Vec::new();

    for entity in entities {
        let mut json_entity = serde_json::Map::new();

        json_entity.insert("id".to_string(), serde_json::Value::String(format_entity_id(entity.entity_id)));
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
            };
            json_entity.insert(field_name.clone(), json_value);
        }

        json_entities.push(serde_json::Value::Object(json_entity));
    }

    let output = serde_json::to_string_pretty(&json_entities)
        .context("Failed to serialize results to JSON")?;

    std::fs::write(export_path, output)
        .with_context(|| format!("Failed to write to {}", export_path.display()))?;

    Ok(())
}