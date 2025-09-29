use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use std::fs::{File, read_dir};
use std::io::Read;
use tracing::info;

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

    /// Show hex dump of WAL file contents
    #[arg(long)]
    hex: bool,

    /// Show file sizes and basic information
    #[arg(long)]
    info: bool,
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

    // Process each WAL file in order
    for (wal_file, counter) in &wal_files {
        println!("\n=== WAL File #{}: {} ===", counter, wal_file.display());
        
        if config.info {
            show_file_info(wal_file)?;
        }
        
        if config.hex {
            show_hex_dump(wal_file)?;
        } else {
            show_basic_info(wal_file)?;
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

fn show_hex_dump(wal_path: &PathBuf) -> Result<()> {
    let mut file = File::open(wal_path)?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;

    println!("Hex dump (first 512 bytes):");
    
    let limit = std::cmp::min(buffer.len(), 512);
    for (i, chunk) in buffer[..limit].chunks(16).enumerate() {
        print!("{:08x}  ", i * 16);
        
        // Print hex bytes
        for (j, byte) in chunk.iter().enumerate() {
            if j == 8 {
                print!(" ");
            }
            print!("{:02x} ", byte);
        }
        
        // Pad if chunk is less than 16 bytes
        for j in chunk.len()..16 {
            if j == 8 {
                print!(" ");
            }
            print!("   ");
        }
        
        print!(" |");
        
        // Print ASCII representation
        for byte in chunk {
            if byte.is_ascii_graphic() || *byte == b' ' {
                print!("{}", *byte as char);
            } else {
                print!(".");
            }
        }
        
        println!("|");
    }
    
    if buffer.len() > 512 {
        println!("... ({} more bytes)", buffer.len() - 512);
    }
    
    Ok(())
}

fn show_basic_info(wal_path: &PathBuf) -> Result<()> {
    let mut file = File::open(wal_path)?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;

    if buffer.is_empty() {
        println!("Empty file");
        return Ok(());
    }

    println!("File contains {} bytes of data", buffer.len());
    
    // Try to identify entry structure by looking for length prefixes
    let mut offset = 0;
    let mut entries = 0;
    
    while offset + 4 <= buffer.len() {
        let len_bytes = [buffer[offset], buffer[offset+1], buffer[offset+2], buffer[offset+3]];
        let len = u32::from_le_bytes(len_bytes) as usize;
        
        if len == 0 || len > 1024 * 1024 || offset + 4 + len > buffer.len() {
            // Doesn't look like a valid entry length, maybe different format
            break;
        }
        
        entries += 1;
        offset += 4 + len;
        
        if entries >= 10 {
            println!("Found {} valid entries (showing first 10)...", entries);
            break;
        }
    }
    
    if entries == 0 {
        println!("Could not identify entry structure - may be different format");
        println!("Use --hex to see raw contents");
    } else {
        println!("Identified {} entries with length-prefixed structure", entries);
    }
    
    Ok(())
}