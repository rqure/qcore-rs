use anyhow::{Context, Result};
use clap::Parser;
use qlib_rs::{IndirectFieldType, Request};
use std::path::PathBuf;
use tabled::{Table, Tabled};
use std::fs::{File, read_dir};
use std::io::Read;
use time::OffsetDateTime;
use tracing::{info, warn, error};
use serde_json;

/// Command-line tool for reading and printing WAL (Write-Ahead Log) files
#[derive(Parser)]
#[command(name = "wal-tool", about = "Read and print WAL files from the QCore data store")]
struct Config {
    /// Data directory containing WAL files
    #[arg(long, default_value = "./data")]
    data_dir: String,

    /// Machine ID to read WAL files for
    #[arg(long, default_value = "qos-a")]
    machine: String,

    /// Start time filter (ISO 8601 format, e.g., 2024-01-01T10:00:00Z)
    /// If not specified, uses the current time when the tool was started
    #[arg(long)]
    start_time: Option<String>,

    /// End time filter (ISO 8601 format, e.g., 2024-01-01T20:00:00Z)
    /// If not specified, will follow WAL files forever (useful for live monitoring)
    #[arg(long)]
    end_time: Option<String>,

    /// Follow mode: continuously monitor WAL files for new entries
    #[arg(long, short)]
    follow: bool,

    /// Show only specific request types (comma-separated: Read,Write,Create,Delete,SchemaUpdate,Snapshot)
    #[arg(long, value_delimiter = ',')]
    request_types: Vec<String>,

    /// Show raw JSON instead of formatted output
    #[arg(long)]
    raw: bool,

    /// Show timestamps in relative format (e.g., "2 minutes ago")
    #[arg(long)]
    relative_time: bool,

    /// Filter by entity ID
    #[arg(long)]
    entity_id: Option<String>,

    /// Filter by field type
    #[arg(long)]
    field_type: Option<String>,

    /// Filter by entity type
    #[arg(long)]
    entity_type: Option<String>,

    /// Show verbose output with all request details
    #[arg(long, short)]
    verbose: bool,
}

/// Represents a row in the WAL output table for Read/Write operations
#[derive(Tabled)]
struct WalReadWriteEntry {
    #[tabled(rename = "Timestamp")]
    timestamp: String,
    #[tabled(rename = "Op")]
    operation: String,
    #[tabled(rename = "Entity")]
    entity: String,
    #[tabled(rename = "Field")]
    field: String,
    #[tabled(rename = "Value")]
    value: String,
    #[tabled(rename = "Push")]
    push: String,
    #[tabled(rename = "Adjust")]
    adjust: String,
    #[tabled(rename = "Writer")]
    writer: String,
    #[tabled(rename = "Orig")]
    originator: String,
    #[tabled(rename = "File:Row")]
    location: String,
}

/// Represents a row in the WAL output table for Create operations
#[derive(Tabled)]
struct WalCreateEntry {
    #[tabled(rename = "Timestamp")]
    timestamp: String,
    #[tabled(rename = "Op")]
    operation: String,
    #[tabled(rename = "EntityType")]
    entity_type: String,
    #[tabled(rename = "Name")]
    name: String,
    #[tabled(rename = "Parent")]
    parent: String,
    #[tabled(rename = "CreatedID")]
    created_id: String,
    #[tabled(rename = "Orig")]
    originator: String,
    #[tabled(rename = "File:Row")]
    location: String,
}

/// Represents a row in the WAL output table for Delete operations
#[derive(Tabled)]
struct WalDeleteEntry {
    #[tabled(rename = "Timestamp")]
    timestamp: String,
    #[tabled(rename = "Op")]
    operation: String,
    #[tabled(rename = "Entity")]
    entity: String,
    #[tabled(rename = "Orig")]
    originator: String,
    #[tabled(rename = "File:Row")]
    location: String,
}

/// Represents a row in the WAL output table for Schema/Snapshot operations
#[derive(Tabled)]
struct WalSystemEntry {
    #[tabled(rename = "Timestamp")]
    timestamp: String,
    #[tabled(rename = "Op")]
    operation: String,
    #[tabled(rename = "Target")]
    target: String,
    #[tabled(rename = "Orig")]
    originator: String,
    #[tabled(rename = "File:Row")]
    location: String,
}

fn main() -> Result<()> {
    // Initialize tracing for CLI tools
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "wal_tool=warn".to_string())
        )
        .with_target(false)
        .without_time()
        .init();

    let config = Config::parse();
    
    // Parse time filters
    let start_time = if let Some(ref time_str) = config.start_time {
        Some(parse_time(time_str).context("Failed to parse start time")?)
    } else {
        Some(OffsetDateTime::now_utc()) // Default to current time
    };

    let end_time = if let Some(ref time_str) = config.end_time {
        Some(parse_time(time_str).context("Failed to parse end time")?)
    } else {
        None // No end time means forever
    };

    info!(
        data_dir = %config.data_dir,
        machine = %config.machine,
        start_time = ?start_time,
        end_time = ?end_time,
        follow = config.follow,
        "Starting WAL tool"
    );

    let mut wal_reader = WalReader::new(config)?;
    wal_reader.read_wal_files(start_time, end_time)?;

    Ok(())
}

/// Parse time string in ISO 8601 format
fn parse_time(time_str: &str) -> Result<OffsetDateTime> {
    // Try parsing with different formats
    if let Ok(time) = OffsetDateTime::parse(time_str, &time::format_description::well_known::Iso8601::DEFAULT) {
        return Ok(time);
    }
    
    // Try RFC 3339 format
    if let Ok(time) = OffsetDateTime::parse(time_str, &time::format_description::well_known::Rfc3339) {
        return Ok(time);
    }

    // Try some common formats
    let formats = [
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ];

    for format_str in &formats {
        if let Ok(format) = time::format_description::parse(format_str) {
            if let Ok(time) = OffsetDateTime::parse(time_str, &format) {
                return Ok(time);
            }
        }
    }

    Err(anyhow::anyhow!("Failed to parse time '{}'. Expected ISO 8601 format like '2024-01-01T10:00:00Z'", time_str))
}

/// WAL file reader and processor
struct WalReader {
    config: Config,
    wal_dir: PathBuf,
    request_type_filter: Option<Vec<String>>,
    write_entries: Vec<WalReadWriteEntry>,
    create_entries: Vec<WalCreateEntry>,
    delete_entries: Vec<WalDeleteEntry>,
    system_entries: Vec<WalSystemEntry>,
}

impl WalReader {
    fn new(config: Config) -> Result<Self> {
        let wal_dir = PathBuf::from(&config.data_dir)
            .join(&config.machine)
            .join("wal");

        let request_type_filter = if config.request_types.is_empty() {
            None
        } else {
            Some(config.request_types.clone())
        };

        Ok(Self {
            config,
            wal_dir,
            request_type_filter,
            write_entries: Vec::new(),
            create_entries: Vec::new(),
            delete_entries: Vec::new(),
            system_entries: Vec::new(),
        })
    }

    fn read_wal_files(&mut self, start_time: Option<OffsetDateTime>, end_time: Option<OffsetDateTime>) -> Result<()> {
        if !self.wal_dir.exists() {
            return Err(anyhow::anyhow!("WAL directory does not exist: {}", self.wal_dir.display()));
        }

        // Find all WAL files
        let wal_files = self.find_wal_files()?;
        
        if wal_files.is_empty() {
            info!("No WAL files found in {}", self.wal_dir.display());
            return Ok(());
        }

        info!("Found {} WAL files", wal_files.len());

        // Process each WAL file in order
        for (wal_file, _counter) in &wal_files {
            self.process_wal_file(wal_file, start_time, end_time)?;
        }

        // Print collected entries as tables (if not in follow mode)
        if !self.config.follow {
            if self.config.raw {
                // In raw mode, we already printed JSON
            } else {
                self.print_all_tables();
            }
        }

        // If follow mode is enabled, continue monitoring for new files
        if self.config.follow {
            self.follow_mode(start_time, end_time)?;
        }

        Ok(())
    }

    fn find_wal_files(&self) -> Result<Vec<(PathBuf, u64)>> {
        let entries = read_dir(&self.wal_dir)?;
        let mut wal_files = Vec::new();

        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                if filename.starts_with("wal_") && filename.ends_with(".log") {
                    // Extract counter from filename (wal_NNNNNNNNNN.log)
                    if let Some(counter_str) = filename.strip_prefix("wal_").and_then(|s| s.strip_suffix(".log")) {
                        if let Ok(counter) = counter_str.parse::<u64>() {
                            wal_files.push((path, counter));
                        }
                    }
                }
            }
        }

        // Sort files by counter (creation order)
        wal_files.sort_by_key(|(_, counter)| *counter);
        Ok(wal_files)
    }

    fn process_wal_file(&mut self, wal_path: &PathBuf, start_time: Option<OffsetDateTime>, end_time: Option<OffsetDateTime>) -> Result<()> {
        info!("Processing WAL file: {}", wal_path.display());

        let mut file = File::open(wal_path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        let mut offset = 0;
        let mut entries_processed = 0;

        while offset < buffer.len() {
            // Read length prefix (4 bytes)
            if offset + 4 > buffer.len() {
                warn!("Incomplete length prefix at offset {} in {}", offset, wal_path.display());
                break;
            }

            let len_bytes = [buffer[offset], buffer[offset+1], buffer[offset+2], buffer[offset+3]];
            let len = u32::from_le_bytes(len_bytes) as usize;
            offset += 4;

            // Read the serialized request
            if offset + len > buffer.len() {
                warn!("Incomplete request at offset {} in {}", offset, wal_path.display());
                break;
            }

            let request_data = &buffer[offset..offset + len];
            offset += len;

            // Parse the request
            match serde_json::from_slice::<Request>(request_data) {
                Ok(request) => {
                    if self.should_show_request(&request, start_time, end_time) {
                        if self.config.follow {
                            // In follow mode, print immediately as table
                            self.print_request_immediate(&request, wal_path, entries_processed);
                        } else {
                            // Collect for batch table display
                            self.collect_request(&request, wal_path, entries_processed);
                        }
                        entries_processed += 1;
                    }
                },
                Err(e) => {
                    error!("Failed to parse request in {}: {}", wal_path.display(), e);
                }
            }
        }

        if entries_processed > 0 {
            info!("Processed {} entries from {}", entries_processed, wal_path.display());
        }

        Ok(())
    }

    fn should_show_request(&self, request: &Request, start_time: Option<OffsetDateTime>, end_time: Option<OffsetDateTime>) -> bool {
        // Check request type filter
        if let Some(ref types) = self.request_type_filter {
            let request_type = match request {
                Request::Write { .. } => "Write", 
                Request::Create { .. } => "Create",
                Request::Delete { .. } => "Delete",
                Request::SchemaUpdate { .. } => "SchemaUpdate",
                Request::Snapshot { .. } => "Snapshot",
                _ => "Read"
            };
            
            if !types.contains(&request_type.to_string()) {
                return false;
            }
        }

        // Check time filters
        if let Some(write_time) = request.write_time() {
            if let Some(start) = start_time {
                if write_time < start {
                    return false;
                }
            }
            
            if let Some(end) = end_time {
                if write_time > end {
                    return false;
                }
            }
        }

        // Check entity ID filter
        if let Some(ref filter_entity_id) = self.config.entity_id {
            if let Some(entity_id) = request.entity_id() {
                let formatted_id = format!("{}:{}", entity_id.extract_type().0, entity_id.extract_id());
                if formatted_id != *filter_entity_id {
                    return false;
                }
            } else {
                return false; // Request has no entity_id but we're filtering for one
            }
        }

        // Check field type filter
        if let Some(ref filter_field_type) = self.config.field_type {
            if let Some(field_types) = request.field_type() {
                // For now, we can't easily convert FieldType IDs back to names without a store
                // We'll skip field type filtering or implement a basic check
                // This is a limitation of the current architecture
                let field_ids: Vec<String> = field_types.iter().map(|ft| format!("{}", ft.0)).collect();
                if !field_ids.iter().any(|id| id == filter_field_type) {
                    return false;
                }
            } else {
                return false; // Request has no field_type but we're filtering for one
            }
        }

        // Check entity type filter
        if let Some(ref filter_entity_type) = self.config.entity_type {
            match request {
                Request::Write { entity_id, .. } | Request::Delete { entity_id, .. } => {
                    // For now, we need a way to convert EntityType to string for comparison
                    // This is a limitation since we don't have a store reference here
                    // We'll use the raw type ID for comparison
                    if format!("{}", entity_id.extract_type().0) != *filter_entity_type {
                        return false;
                    }
                },
                Request::Create { entity_type, .. } => {
                    if format!("{}", entity_type.0) != *filter_entity_type {
                        return false;
                    }
                },
                Request::SchemaUpdate { schema, .. } => {
                    if schema.entity_type != *filter_entity_type {
                        return false;
                    }
                },
                _ => {
                    // These request types don't have entity types to filter on
                    return false;
                }
            }
        }

        true
    }

    /// Collect request for batch table display
    fn collect_request(&mut self, request: &Request, wal_path: &PathBuf, entry_index: usize) {
        if self.config.raw {
            // Print raw JSON immediately
            if let Ok(json) = serde_json::to_string(request) {
                println!("{}", json);
            }
            return;
        }

        self.create_and_store_entry(request, wal_path, entry_index);
    }

    /// Print request immediately (for follow mode)
    fn print_request_immediate(&self, request: &Request, wal_path: &PathBuf, entry_index: usize) {
        if self.config.raw {
            // Print raw JSON
            if let Ok(json) = serde_json::to_string(request) {
                println!("{}", json);
            }
            return;
        }

        // For follow mode, create temporary entries and print them immediately
        match request {
            Request::Write { .. } => {
                let entry = self.create_read_write_entry(request, wal_path, entry_index);
                let table = Table::new(&[entry]);
                println!("{}", table);
            },
            Request::Create { .. } => {
                let entry = self.create_create_entry(request, wal_path, entry_index);
                let table = Table::new(&[entry]);
                println!("{}", table);
            },
            Request::Delete { .. } => {
                let entry = self.create_delete_entry(request, wal_path, entry_index);
                let table = Table::new(&[entry]);
                println!("{}", table);
            },
            Request::SchemaUpdate { .. } | Request::Snapshot { .. } => {
                let entry = self.create_system_entry(request, wal_path, entry_index);
                let table = Table::new(&[entry]);
                println!("{}", table);
            },
            _ => {
                // Other request types are unlikely in WAL files
            }
        }
    }

    /// Create and store entries in appropriate collections
    fn create_and_store_entry(&mut self, request: &Request, wal_path: &PathBuf, entry_index: usize) {
        match request {
            Request::Write { .. } => {
                let entry = self.create_read_write_entry(request, wal_path, entry_index);
                self.write_entries.push(entry);
            },
            Request::Create { .. } => {
                let entry = self.create_create_entry(request, wal_path, entry_index);
                self.create_entries.push(entry);
            },
            Request::Delete { .. } => {
                let entry = self.create_delete_entry(request, wal_path, entry_index);
                self.delete_entries.push(entry);
            },
            Request::SchemaUpdate { .. } | Request::Snapshot { .. } => {
                let entry = self.create_system_entry(request, wal_path, entry_index);
                self.system_entries.push(entry);
            },
            _ => {
                // Other request types are unlikely in WAL files
            },
        }
    }

    /// Print all collected tables
    fn print_all_tables(&self) {
        if !self.write_entries.is_empty() {
            println!("\n=== WRITE Operations ===");
            let table = Table::new(&self.write_entries);
            println!("{}", table);
        }

        if !self.create_entries.is_empty() {
            println!("\n=== CREATE Operations ===");
            let table = Table::new(&self.create_entries);
            println!("{}", table);
        }

        if !self.delete_entries.is_empty() {
            println!("\n=== DELETE Operations ===");
            let table = Table::new(&self.delete_entries);
            println!("{}", table);
        }

        if !self.system_entries.is_empty() {
            println!("\n=== SYSTEM Operations (Schema/Snapshot) ===");
            let table = Table::new(&self.system_entries);
            println!("{}", table);
        }
    }

    /// Create a Read/Write entry
    fn create_read_write_entry(&self, request: &Request, wal_path: &PathBuf, entry_index: usize) -> WalReadWriteEntry {
        let timestamp_str = self.format_timestamp(request);
        let location = if self.config.verbose {
            format!("{}:{}", wal_path.file_name().unwrap().to_string_lossy(), entry_index)
        } else {
            String::new()
        };

        match request {
            Request::Read { entity_id, field_types: field_type, value, writer_id, .. } => {
                WalReadWriteEntry {
                    timestamp: timestamp_str,
                    operation: "READ".to_string(),
                    entity: Self::format_entity_id(entity_id),
                    field: Self::format_field_types(field_type),
                    value: Self::format_value_clean(value),
                    push: "-".to_string(),
                    adjust: "-".to_string(),
                    writer: writer_id.as_ref().map(Self::format_entity_id).unwrap_or_else(|| "system".to_string()),
                    originator: "-".to_string(),
                    location,
                }
            },
            Request::Write { entity_id, field_types: field_type, value, push_condition, adjust_behavior, writer_id, .. } => {
                WalReadWriteEntry {
                    timestamp: timestamp_str,
                    operation: "WRITE".to_string(),
                    entity: Self::format_entity_id(entity_id),
                    field: Self::format_field_types(field_type),
                    value: Self::format_value_clean(value),
                    push: format!("{:?}", push_condition),
                    adjust: format!("{}", adjust_behavior),
                    writer: writer_id.as_ref().map(Self::format_entity_id).unwrap_or_else(|| "system".to_string()),
                    originator: "system".to_string(),
                    location,
                }
            },
            _ => unreachable!("create_read_write_entry called with non-read/write request"),
        }
    }

    /// Create a Create entry
    fn create_create_entry(&self, request: &Request, wal_path: &PathBuf, entry_index: usize) -> WalCreateEntry {
        let timestamp_str = self.format_timestamp(request);
        let location = if self.config.verbose {
            format!("{}:{}", wal_path.file_name().unwrap().to_string_lossy(), entry_index)
        } else {
            String::new()
        };

        match request {
            Request::Create { entity_type, parent_id, name, created_entity_id, timestamp: _ } => {
                WalCreateEntry {
                    timestamp: timestamp_str,
                    operation: "CREATE".to_string(),
                    entity_type: format!("{}", entity_type.0),
                    name: name.clone(),
                    parent: parent_id.as_ref().map(Self::format_entity_id).unwrap_or_else(|| "root".to_string()),
                    created_id: created_entity_id.as_ref().map(Self::format_entity_id).unwrap_or_else(|| "auto".to_string()),
                    originator: "system".to_string(),
                    location,
                }
            },
            _ => unreachable!("create_create_entry called with non-create request"),
        }
    }

    /// Create a Delete entry
    fn create_delete_entry(&self, request: &Request, wal_path: &PathBuf, entry_index: usize) -> WalDeleteEntry {
        let timestamp_str = self.format_timestamp(request);
        let location = if self.config.verbose {
            format!("{}:{}", wal_path.file_name().unwrap().to_string_lossy(), entry_index)
        } else {
            String::new()
        };

        match request {
            Request::Delete { entity_id, timestamp: _ } => {
                WalDeleteEntry {
                    timestamp: timestamp_str,
                    operation: "DELETE".to_string(),
                    entity: Self::format_entity_id(entity_id),
                    originator: "system".to_string(),
                    location,
                }
            },
            _ => unreachable!("create_delete_entry called with non-delete request"),
        }
    }

    /// Create a System entry (Schema/Snapshot)
    fn create_system_entry(&self, request: &Request, wal_path: &PathBuf, entry_index: usize) -> WalSystemEntry {
        let timestamp_str = self.format_timestamp(request);
        let location = if self.config.verbose {
            format!("{}:{}", wal_path.file_name().unwrap().to_string_lossy(), entry_index)
        } else {
            String::new()
        };

        match request {
            Request::SchemaUpdate { schema, timestamp: _ } => {
                WalSystemEntry {
                    timestamp: timestamp_str,
                    operation: "SCHEMA".to_string(),
                    target: schema.entity_type.clone(),
                    originator: "system".to_string(),
                    location,
                }
            },
            Request::Snapshot { snapshot_counter, timestamp: _ } => {
                WalSystemEntry {
                    timestamp: timestamp_str,
                    operation: "SNAPSHOT".to_string(),
                    target: format!("#{}", snapshot_counter),
                    originator: "system".to_string(),
                    location,
                }
            },
            _ => unreachable!("create_system_entry called with non-system request"),
        }
    }

    /// Format timestamp consistently
    fn format_timestamp(&self, request: &Request) -> String {
        if let Some(write_time) = request.write_time() {
            if self.config.relative_time {
                format_relative_time(write_time)
            } else {
                // Show full timestamp
                write_time.format(&time::format_description::well_known::Rfc3339)
                    .unwrap_or_else(|_| "Invalid timestamp".to_string())
            }
        } else {
            "No timestamp".to_string()
        }
    }

    /// Format entity ID in a clean, readable way
    fn format_entity_id(entity_id: &qlib_rs::EntityId) -> String {
        format!("{}:{}", entity_id.extract_type().0, entity_id.extract_id())
    }

    /// Format field types as a readable string
    fn format_field_types(field_types: &IndirectFieldType) -> String {
        if field_types.len() == 1 {
            format!("{}", field_types[0].0)
        } else {
            // For indirection, show as field1->field2->...
            field_types.iter()
                .map(|ft| format!("{}", ft.0))
                .collect::<Vec<_>>()
                .join("->")
        }
    }

    /// Format value in a clean way without Rust type annotations
    fn format_value_clean(value: &Option<qlib_rs::Value>) -> String {
        match value {
            Some(qlib_rs::Value::String(s)) => format!("\"{}\"", s),
            Some(qlib_rs::Value::Int(i)) => i.to_string(),
            Some(qlib_rs::Value::Float(f)) => f.to_string(),
            Some(qlib_rs::Value::Bool(b)) => b.to_string(),
            Some(qlib_rs::Value::Choice(c)) => c.to_string(),
            Some(qlib_rs::Value::EntityList(list)) => {
                if list.is_empty() {
                    "[]".to_string()
                } else if list.len() <= 3 {
                    format!("[{}]", list.iter().map(Self::format_entity_id).collect::<Vec<_>>().join(", "))
                } else {
                    format!("[{}, ... {} more]", 
                        list.iter().take(2).map(Self::format_entity_id).collect::<Vec<_>>().join(", "),
                        list.len() - 2)
                }
            },
            Some(qlib_rs::Value::EntityReference(Some(entity_id))) => Self::format_entity_id(entity_id),
            Some(qlib_rs::Value::EntityReference(None)) => "null".to_string(),
            Some(qlib_rs::Value::Blob(blob)) => {
                if blob.len() <= 16 {
                    format!("blob[{}]", blob.len())
                } else {
                    format!("blob[{} bytes]", blob.len())
                }
            },
            Some(qlib_rs::Value::Timestamp(ts)) => {
                ts.format(&time::format_description::well_known::Rfc3339)
                    .unwrap_or_else(|_| "invalid_timestamp".to_string())
            },
            None => "null".to_string(),
        }
    }

    fn follow_mode(&mut self, start_time: Option<OffsetDateTime>, end_time: Option<OffsetDateTime>) -> Result<()> {
        info!("Entering follow mode - monitoring for new WAL entries...");
        
        // Keep track of file sizes to detect new content
        let mut file_positions: std::collections::HashMap<PathBuf, u64> = std::collections::HashMap::new();
        
        // Initialize positions for existing files
        let initial_files = self.find_wal_files()?;
        for (file_path, _counter) in &initial_files {
            if let Ok(metadata) = std::fs::metadata(file_path) {
                file_positions.insert(file_path.clone(), metadata.len());
            }
        }
        
        info!("Following {} WAL files for new entries", file_positions.len());
        
        loop {
            std::thread::sleep(std::time::Duration::from_millis(500));
            
            let current_files = self.find_wal_files()?;
            
            // Check for new files
            for (file_path, _counter) in &current_files {
                if !file_positions.contains_key(file_path) {
                    info!("New WAL file detected: {}", file_path.display());
                    self.process_wal_file(file_path, start_time, end_time)?;
                    
                    // Track this new file
                    if let Ok(metadata) = std::fs::metadata(file_path) {
                        file_positions.insert(file_path.clone(), metadata.len());
                    }
                }
            }
            
            // Check existing files for new content
            for (file_path, last_size) in file_positions.clone().iter() {
                if let Ok(metadata) = std::fs::metadata(file_path) {
                    let current_size = metadata.len();
                    if current_size > *last_size {
                        // File has grown, process new content
                        if let Err(e) = self.process_wal_file_from_offset(file_path, *last_size as usize, start_time, end_time) {
                            warn!("Failed to process new content in {}: {}", file_path.display(), e);
                        } else {
                            file_positions.insert(file_path.clone(), current_size);
                        }
                    }
                }
            }
            
            // Remove files that no longer exist
            file_positions.retain(|path, _| path.exists());
            
            // Check if we should stop due to end time
            if let Some(end) = end_time {
                if OffsetDateTime::now_utc() > end {
                    info!("Reached end time, stopping follow mode");
                    break;
                }
            }
        }
        
        Ok(())
    }

    fn process_wal_file_from_offset(&mut self, wal_path: &PathBuf, start_offset: usize, start_time: Option<OffsetDateTime>, end_time: Option<OffsetDateTime>) -> Result<()> {
        let mut file = File::open(wal_path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        if start_offset >= buffer.len() {
            return Ok(()); // Nothing new to process
        }

        let mut offset = start_offset;
        let mut entries_processed = 0;

        while offset < buffer.len() {
            // Read length prefix (4 bytes)
            if offset + 4 > buffer.len() {
                break; // Not enough data for length prefix
            }

            let len_bytes = [buffer[offset], buffer[offset+1], buffer[offset+2], buffer[offset+3]];
            let len = u32::from_le_bytes(len_bytes) as usize;
            offset += 4;

            // Read the serialized request
            if offset + len > buffer.len() {
                break; // Incomplete request
            }

            let request_data = &buffer[offset..offset + len];
            offset += len;

            // Parse the request
            match serde_json::from_slice::<Request>(request_data) {
                Ok(request) => {
                    if self.should_show_request(&request, start_time, end_time) {
                        self.print_request_immediate(&request, wal_path, entries_processed);
                        entries_processed += 1;
                    }
                },
                Err(e) => {
                    warn!("Failed to parse request in {}: {}", wal_path.display(), e);
                }
            }
        }

        if entries_processed > 0 {
            info!("Processed {} new entries from {}", entries_processed, wal_path.display());
        }

        Ok(())
    }
}

/// Format time relative to now (e.g., "2 minutes ago")
fn format_relative_time(timestamp: OffsetDateTime) -> String {
    let now = OffsetDateTime::now_utc();
    let duration = now - timestamp;
    
    let total_seconds = duration.whole_seconds();
    
    if total_seconds < 0 {
        return "in the future".to_string();
    }
    
    if total_seconds < 60 {
        return format!("{} seconds ago", total_seconds);
    }
    
    let minutes = total_seconds / 60;
    if minutes < 60 {
        return format!("{} minutes ago", minutes);
    }
    
    let hours = minutes / 60;
    if hours < 24 {
        return format!("{} hours ago", hours);
    }
    
    let days = hours / 24;
    format!("{} days ago", days)
}