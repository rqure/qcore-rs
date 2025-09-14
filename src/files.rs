use std::path::PathBuf;

use anyhow::Result;
use async_trait::async_trait;
use tokio::fs::{read_dir, remove_file};
use tracing::{info, error};

/// Configuration for numbered file management
#[derive(Debug, Clone)]
pub struct FileConfig {
    pub prefix: String,
    pub suffix: String,
    pub max_files: usize,
}

/// Information about a numbered file
#[derive(Debug, Clone)]
pub struct FileInfo {
    pub path: PathBuf,
    pub counter: u64,
}

/// Standard implementation of file management operations
#[derive(Debug, Clone)]
pub struct FileManager;

/// Trait for managing numbered files (WAL files, snapshots, etc.)
#[async_trait]
pub trait FileManagerTrait: Send + Sync {
    /// Scan directory for files matching the configuration
    fn scan_files(&self, dir: &PathBuf, config: &FileConfig) -> Result<Vec<FileInfo>>;
    
    /// Get the next counter value for numbered files
    fn get_next_counter(&self, dir: &PathBuf, config: &FileConfig) -> Result<u64>;
    
    /// Clean up old files, keeping only the most recent max_files
    fn cleanup_old_files(&self, dir: &PathBuf, config: &FileConfig) -> Result<()>;
}

#[async_trait]
impl FileManagerTrait for FileManager {
    /// Scan directory for files matching the configuration
    fn scan_files(&self, dir: &PathBuf, config: &FileConfig) -> Result<Vec<FileInfo>> {
        if !dir.exists() {
            return Ok(Vec::new());
        }
        
        let mut entries = read_dir(dir)?;
        let mut files = Vec::new();
        
        while let Some(entry) = entries.next_entry()? {
            let path = entry.path();
            if let Some(filename) = path.file_name() {
                if let Some(filename_str) = filename.to_str() {
                    if filename_str.starts_with(&config.prefix) && filename_str.ends_with(&config.suffix) {
                        // Extract counter from filename
                        if let Some(counter_str) = filename_str.strip_prefix(&config.prefix).and_then(|s| s.strip_suffix(&config.suffix)) {
                            if let Ok(counter) = counter_str.parse::<u64>() {
                                files.push(FileInfo { path, counter });
                            }
                        } else {
                            // Files without counter (for compatibility)
                            files.push(FileInfo { path, counter: 0 });
                        }
                    }
                }
            }
        }
        
        files.sort_by_key(|f| f.counter);
        Ok(files)
    }

    /// Get the next counter value for numbered files
    fn get_next_counter(&self, dir: &PathBuf, config: &FileConfig) -> Result<u64> {
        let files = self.scan_files(dir, config)?;
        let max_counter = files.iter().map(|f| f.counter).max().unwrap_or(0);
        Ok(if max_counter == 0 && files.is_empty() { 0 } else { max_counter + 1 })
    }

    /// Clean up old files, keeping only the most recent max_files
    fn cleanup_old_files(&self, dir: &PathBuf, config: &FileConfig) -> Result<()> {
        let files = self.scan_files(dir, config)?;
        
        if files.len() > config.max_files {
            let files_to_remove = files.len() - config.max_files;
            for file_info in &files[0..files_to_remove] {
                info!(file = %file_info.path.display(), "Removing old file");
                if let Err(e) = remove_file(&file_info.path) {
                    error!(file = %file_info.path.display(), error = %e, "Failed to remove old file");
                }
            }
        }
        
        Ok(())
    }
}