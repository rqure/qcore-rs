use anyhow::Result;
use bincode;
use clap::Parser;
use serde_json;
use std::path::PathBuf;
use std::fs::{File, read_dir};
use std::io::Read;
use std::time::Duration;
use tracing::info;
use time::OffsetDateTime;
use qlib_rs::WriteInfo;

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

    /// Start time for filtering entries (RFC3339 format, e.g., 2023-01-01T00:00:00Z)
    #[arg(long)]
    start_time: Option<String>,

    /// End time for filtering entries (RFC3339 format, e.g., 2023-01-01T23:59:59Z)
    #[arg(long)]
    end_time: Option<String>,

    /// Follow WAL file in real-time (like tail -f)
    #[arg(long)]
    follow: bool,

    /// Output format: json, pretty, or compact
    #[arg(long, default_value = "pretty")]
    format: OutputFormat,

    /// Show file sizes and basic information only
    #[arg(long)]
    info: bool,
}

#[derive(Clone, Debug)]
enum OutputFormat {
    Json,
    Pretty,
    Compact,
}

impl std::str::FromStr for OutputFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "json" => Ok(OutputFormat::Json),
            "pretty" => Ok(OutputFormat::Pretty),
            "compact" => Ok(OutputFormat::Compact),
            _ => Err(format!("Invalid format: {}. Use json, pretty, or compact", s)),
        }
    }
}

/// Extract timestamp from a WriteInfo entry
fn extract_timestamp(write_info: &WriteInfo) -> Option<OffsetDateTime> {
    match write_info {
        WriteInfo::FieldUpdate { write_time, .. } => *write_time,
        WriteInfo::CreateEntity { timestamp, .. } => Some(*timestamp),
        WriteInfo::DeleteEntity { timestamp, .. } => Some(*timestamp),
        WriteInfo::SchemaUpdate { timestamp, .. } => Some(*timestamp),
        WriteInfo::Snapshot { timestamp, .. } => Some(*timestamp),
    }
}

/// Iterator for reading WAL entries from a buffer
struct WalEntryReader {
    buffer: Vec<u8>,
    offset: usize,
}

impl WalEntryReader {
    fn new(buffer: Vec<u8>) -> Self {
        Self { buffer, offset: 0 }
    }

    fn next_entry(&mut self) -> Option<Result<(WriteInfo, usize), anyhow::Error>> {
        if self.offset >= self.buffer.len() {
            return None;
        }

        // Read length prefix (4 bytes)
        if self.offset + 4 > self.buffer.len() {
            return Some(Err(anyhow::anyhow!("Incomplete length prefix at offset {}", self.offset)));
        }

        let length_bytes = &self.buffer[self.offset..self.offset + 4];
        let length = u32::from_le_bytes([
            length_bytes[0],
            length_bytes[1],
            length_bytes[2],
            length_bytes[3],
        ]) as usize;

        self.offset += 4;

        // Read entry data
        if self.offset + length > self.buffer.len() {
            return Some(Err(anyhow::anyhow!("Incomplete entry data at offset {}", self.offset)));
        }

        let entry_data = &self.buffer[self.offset..self.offset + length];
        let entry_offset = self.offset;
        self.offset += length;

        match bincode::deserialize::<WriteInfo>(entry_data) {
            Ok(write_info) => Some(Ok((write_info, entry_offset))),
            Err(e) => Some(Err(anyhow::anyhow!("Failed to deserialize WAL entry: {}", e))),
        }
    }
}

impl Iterator for WalEntryReader {
    type Item = Result<(WriteInfo, usize), anyhow::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_entry()
    }
}

fn main() -> Result<()> {
    // Initialize tracing for CLI tools
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "wal_tool=info".to_string())
        )
        .with_target(false)
        .without_time()
        .init();

    let config = Config::parse();

    info!(
        data_dir = %config.data_dir,
        machine = %config.machine,
        "Starting WAL tool"
    );

    // Parse time filters
    let start_time = if let Some(start_str) = &config.start_time {
        Some(parse_timestamp(start_str)?)
    } else {
        None
    };

    let end_time = if let Some(end_str) = &config.end_time {
        Some(parse_timestamp(end_str)?)
    } else {
        None
    };

    let wal_dir = PathBuf::from(&config.data_dir)
        .join(&config.machine)
        .join("wal");

    if !wal_dir.exists() {
        return Err(anyhow::anyhow!("WAL directory does not exist: {}", wal_dir.display()));
    }

    // Find all WAL files
    let wal_files = find_wal_files(&wal_dir)?;

    if wal_files.is_empty() {
        info!("No WAL files found in {}", wal_dir.display());
        return Ok(());
    }

    info!("Found {} WAL files", wal_files.len());

    if config.follow {
        // Follow mode - watch the latest WAL file
        let latest_file = wal_files.last().unwrap().0.clone();
        follow_wal_file(&latest_file, &start_time, &end_time, &config.format)?;
    } else {
        // Process all WAL files
        for (wal_file, counter) in &wal_files {
            println!("\n=== WAL File #{}: {} ===", counter, wal_file.display());

            if config.info {
                show_file_info(wal_file)?;
            } else {
                process_wal_file(wal_file, &start_time, &end_time, &config.format)?;
            }
        }
    }

    Ok(())
}

/// Parse RFC3339 timestamp string
fn parse_timestamp(s: &str) -> Result<OffsetDateTime> {
    OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
        .map_err(|e| anyhow::anyhow!("Invalid timestamp format '{}': {}", s, e))
}

/// Process a single WAL file and output entries
fn process_wal_file(
    wal_path: &PathBuf,
    start_time: &Option<OffsetDateTime>,
    end_time: &Option<OffsetDateTime>,
    format: &OutputFormat,
) -> Result<()> {
    let mut file = File::open(wal_path)?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;

    if buffer.is_empty() {
        println!("Empty file");
        return Ok(());
    }

    let mut reader = WalEntryReader::new(buffer);
    let mut entry_count = 0;
    let mut filtered_count = 0;

    while let Some(result) = reader.next() {
        match result {
            Ok((write_info, offset)) => {
                entry_count += 1;

                // Check time filters
                if let Some(timestamp) = extract_timestamp(&write_info) {
                    if let Some(start) = start_time {
                        if timestamp < *start {
                            continue;
                        }
                    }
                    if let Some(end) = end_time {
                        if timestamp > *end {
                            continue;
                        }
                    }
                } else if start_time.is_some() || end_time.is_some() {
                    // Skip entries without timestamps if time filtering is enabled
                    continue;
                }

                filtered_count += 1;
                output_entry(&write_info, offset, format)?;
            }
            Err(e) => {
                eprintln!("Error reading entry at offset {}: {}", reader.offset, e);
                break;
            }
        }
    }

    println!("\nProcessed {} entries, displayed {}", entry_count, filtered_count);
    Ok(())
}

/// Follow a WAL file in real-time
fn follow_wal_file(
    wal_path: &PathBuf,
    start_time: &Option<OffsetDateTime>,
    end_time: &Option<OffsetDateTime>,
    format: &OutputFormat,
) -> Result<()> {
    println!("Following WAL file: {}", wal_path.display());
    println!("Press Ctrl+C to stop following");

    let mut last_size = 0;
    let mut buffer = Vec::new();

    loop {
        // Check file size
        let metadata = std::fs::metadata(wal_path)?;
        let current_size = metadata.len() as usize;

        if current_size > last_size {
            // File has grown, read new data
            let mut file = File::open(wal_path)?;
            buffer.clear();
            file.read_to_end(&mut buffer)?;

            // Process only the new data
            let new_data = &buffer[last_size..];
            if !new_data.is_empty() {
                let mut reader = WalEntryReader::new(new_data.to_vec());
                while let Some(result) = reader.next() {
                    match result {
                        Ok((write_info, offset)) => {
                            // Check time filters
                            if let Some(timestamp) = extract_timestamp(&write_info) {
                                if let Some(start) = start_time {
                                    if timestamp < *start {
                                        continue;
                                    }
                                }
                                if let Some(end) = end_time {
                                    if timestamp > *end {
                                        continue;
                                    }
                                }
                            } else if start_time.is_some() || end_time.is_some() {
                                continue;
                            }

                            output_entry(&write_info, last_size + offset, format)?;
                        }
                        Err(e) => {
                            eprintln!("Error reading entry: {}", e);
                            break;
                        }
                    }
                }
            }

            last_size = current_size;
        }

        // Sleep before checking again
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Output a WAL entry in the specified format
fn output_entry(write_info: &WriteInfo, offset: usize, format: &OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string(write_info)?);
        }
        OutputFormat::Pretty => {
            println!("Offset: {}", offset);
            if let Some(timestamp) = extract_timestamp(write_info) {
                println!("Timestamp: {}", timestamp.format(&time::format_description::well_known::Rfc3339)?);
            }
            println!("Entry: {:#?}", write_info);
            println!("---");
        }
        OutputFormat::Compact => {
            let timestamp_str = if let Some(timestamp) = extract_timestamp(write_info) {
                timestamp.format(&time::format_description::well_known::Rfc3339)?
            } else {
                "unknown".to_string()
            };
            println!("{} [{}] {:?}", timestamp_str, offset, write_info);
        }
    }
    Ok(())
}

fn find_wal_files(wal_dir: &PathBuf) -> Result<Vec<(PathBuf, u64)>> {
    let entries = read_dir(wal_dir)?;
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

fn show_file_info(wal_path: &PathBuf) -> Result<()> {
    let metadata = std::fs::metadata(wal_path)?;
    println!("File size: {} bytes", metadata.len());
    
    if let Ok(modified) = metadata.modified() {
        if let Ok(system_time) = modified.duration_since(std::time::SystemTime::UNIX_EPOCH) {
            let timestamp = time::OffsetDateTime::from_unix_timestamp(system_time.as_secs() as i64)
                .unwrap_or_else(|_| time::OffsetDateTime::UNIX_EPOCH);
            println!("Last modified: {}", timestamp.format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_else(|_| "Unknown".to_string()));
        }
    }
    
    Ok(())
}