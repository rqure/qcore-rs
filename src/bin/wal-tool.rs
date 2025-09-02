use anyhow::{Context, Result};
use clap::Parser;
use qlib_rs::Request;
use std::path::PathBuf;
use tokio::fs::{File, read_dir};
use tokio::io::AsyncReadExt;
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

#[tokio::main]
async fn main() -> Result<()> {
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

    let wal_reader = WalReader::new(config)?;
    wal_reader.read_wal_files(start_time, end_time).await?;

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
        })
    }

    async fn read_wal_files(&self, start_time: Option<OffsetDateTime>, end_time: Option<OffsetDateTime>) -> Result<()> {
        if !self.wal_dir.exists() {
            return Err(anyhow::anyhow!("WAL directory does not exist: {}", self.wal_dir.display()));
        }

        // Find all WAL files
        let wal_files = self.find_wal_files().await?;
        
        if wal_files.is_empty() {
            info!("No WAL files found in {}", self.wal_dir.display());
            return Ok(());
        }

        info!("Found {} WAL files", wal_files.len());

        // Process each WAL file in order
        for (wal_file, _counter) in &wal_files {
            self.process_wal_file(wal_file, start_time, end_time).await?;
        }

        // If follow mode is enabled, continue monitoring for new files
        if self.config.follow {
            self.follow_mode(start_time, end_time).await?;
        }

        Ok(())
    }

    async fn find_wal_files(&self) -> Result<Vec<(PathBuf, u64)>> {
        let mut entries = read_dir(&self.wal_dir).await?;
        let mut wal_files = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
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

    async fn process_wal_file(&self, wal_path: &PathBuf, start_time: Option<OffsetDateTime>, end_time: Option<OffsetDateTime>) -> Result<()> {
        info!("Processing WAL file: {}", wal_path.display());

        let mut file = File::open(wal_path).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;

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
                        self.print_request(&request, wal_path, entries_processed);
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
                Request::Read { .. } => "Read",
                Request::Write { .. } => "Write", 
                Request::Create { .. } => "Create",
                Request::Delete { .. } => "Delete",
                Request::SchemaUpdate { .. } => "SchemaUpdate",
                Request::Snapshot { .. } => "Snapshot",
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
                if entity_id.get_id().to_string() != *filter_entity_id {
                    return false;
                }
            } else {
                return false; // Request has no entity_id but we're filtering for one
            }
        }

        // Check field type filter
        if let Some(ref filter_field_type) = self.config.field_type {
            if let Some(field_type) = request.field_type() {
                if field_type.as_ref() != filter_field_type {
                    return false;
                }
            } else {
                return false; // Request has no field_type but we're filtering for one
            }
        }

        // Check entity type filter
        if let Some(ref filter_entity_type) = self.config.entity_type {
            match request {
                Request::Read { entity_id, .. } | Request::Write { entity_id, .. } | Request::Delete { entity_id, .. } => {
                    if entity_id.get_type().as_ref() != filter_entity_type {
                        return false;
                    }
                },
                Request::Create { entity_type, .. } => {
                    if entity_type.as_ref() != filter_entity_type {
                        return false;
                    }
                },
                Request::SchemaUpdate { schema, .. } => {
                    if schema.entity_type.as_ref() != filter_entity_type {
                        return false;
                    }
                },
                Request::Snapshot { .. } => {
                    // Snapshots don't have entity types, so they don't match entity type filters
                    return false;
                }
            }
        }

        true
    }

    fn print_request(&self, request: &Request, wal_path: &PathBuf, entry_index: usize) {
        if self.config.raw {
            // Print raw JSON
            if let Ok(json) = serde_json::to_string(request) {
                println!("{}", json);
            }
            return;
        }

        // Format timestamp
        let timestamp_str = if let Some(write_time) = request.write_time() {
            if self.config.relative_time {
                format_relative_time(write_time)
            } else {
                write_time.format(&time::format_description::well_known::Rfc3339)
                    .unwrap_or_else(|_| "Invalid timestamp".to_string())
            }
        } else {
            "No timestamp".to_string()
        };

        // Format the request based on type
        match request {
            Request::Read { entity_id, field_type, value, writer_id, .. } => {
                if self.config.verbose {
                    println!("[{}] READ {} {} -> {:?} (writer: {:?}) [{}:{}]", 
                        timestamp_str, entity_id.get_id(), field_type.as_ref(), value, writer_id,
                        wal_path.file_name().unwrap().to_string_lossy(), entry_index);
                } else {
                    println!("[{}] READ {} {} -> {:?}", 
                        timestamp_str, entity_id.get_id(), field_type.as_ref(), value);
                }
            },
            Request::Write { entity_id, field_type, value, push_condition, adjust_behavior, writer_id, originator, .. } => {
                if self.config.verbose {
                    println!("[{}] WRITE {} {} = {:?} (push: {:?}, adjust: {}, writer: {:?}, originator: {:?}) [{}:{}]",
                        timestamp_str, entity_id.get_id(), field_type.as_ref(), value, push_condition, adjust_behavior, writer_id, originator,
                        wal_path.file_name().unwrap().to_string_lossy(), entry_index);
                } else {
                    println!("[{}] WRITE {} {} = {:?}",
                        timestamp_str, entity_id.get_id(), field_type.as_ref(), value);
                }
            },
            Request::Create { entity_type, parent_id, name, created_entity_id, originator } => {
                if self.config.verbose {
                    println!("[{}] CREATE {} '{}' (parent: {:?}, id: {:?}, originator: {:?}) [{}:{}]",
                        timestamp_str, entity_type.as_ref(), name, parent_id, created_entity_id, originator,
                        wal_path.file_name().unwrap().to_string_lossy(), entry_index);
                } else {
                    println!("[{}] CREATE {} '{}'", 
                        timestamp_str, entity_type.as_ref(), name);
                }
            },
            Request::Delete { entity_id, originator } => {
                if self.config.verbose {
                    println!("[{}] DELETE {} (originator: {:?}) [{}:{}]",
                        timestamp_str, entity_id.get_id(), originator,
                        wal_path.file_name().unwrap().to_string_lossy(), entry_index);
                } else {
                    println!("[{}] DELETE {}", 
                        timestamp_str, entity_id.get_id());
                }
            },
            Request::SchemaUpdate { schema, originator } => {
                if self.config.verbose {
                    println!("[{}] SCHEMA_UPDATE {} (originator: {:?}) [{}:{}]",
                        timestamp_str, schema.entity_type.as_ref(), originator,
                        wal_path.file_name().unwrap().to_string_lossy(), entry_index);
                } else {
                    println!("[{}] SCHEMA_UPDATE {}", 
                        timestamp_str, schema.entity_type.as_ref());
                }
            },
            Request::Snapshot { snapshot_counter, originator } => {
                if self.config.verbose {
                    println!("[{}] SNAPSHOT #{} (originator: {:?}) [{}:{}]",
                        timestamp_str, snapshot_counter, originator,
                        wal_path.file_name().unwrap().to_string_lossy(), entry_index);
                } else {
                    println!("[{}] SNAPSHOT #{}", 
                        timestamp_str, snapshot_counter);
                }
            }
        }
    }

    async fn follow_mode(&self, start_time: Option<OffsetDateTime>, end_time: Option<OffsetDateTime>) -> Result<()> {
        info!("Entering follow mode - monitoring for new WAL entries...");
        
        // Keep track of file sizes to detect new content
        let mut file_positions: std::collections::HashMap<PathBuf, u64> = std::collections::HashMap::new();
        
        // Initialize positions for existing files
        let initial_files = self.find_wal_files().await?;
        for (file_path, _counter) in &initial_files {
            if let Ok(metadata) = tokio::fs::metadata(file_path).await {
                file_positions.insert(file_path.clone(), metadata.len());
            }
        }
        
        info!("Following {} WAL files for new entries", file_positions.len());
        
        loop {
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            
            let current_files = self.find_wal_files().await?;
            
            // Check for new files
            for (file_path, _counter) in &current_files {
                if !file_positions.contains_key(file_path) {
                    info!("New WAL file detected: {}", file_path.display());
                    self.process_wal_file(file_path, start_time, end_time).await?;
                    
                    // Track this new file
                    if let Ok(metadata) = tokio::fs::metadata(file_path).await {
                        file_positions.insert(file_path.clone(), metadata.len());
                    }
                }
            }
            
            // Check existing files for new content
            for (file_path, last_size) in file_positions.clone().iter() {
                if let Ok(metadata) = tokio::fs::metadata(file_path).await {
                    let current_size = metadata.len();
                    if current_size > *last_size {
                        // File has grown, process new content
                        if let Err(e) = self.process_wal_file_from_offset(file_path, *last_size as usize, start_time, end_time).await {
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

    async fn process_wal_file_from_offset(&self, wal_path: &PathBuf, start_offset: usize, start_time: Option<OffsetDateTime>, end_time: Option<OffsetDateTime>) -> Result<()> {
        let mut file = File::open(wal_path).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;

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
                        self.print_request(&request, wal_path, entries_processed);
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