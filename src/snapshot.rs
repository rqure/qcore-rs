use std::path::PathBuf;
use crossbeam::channel::{Sender, bounded, unbounded};
use std::thread;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{Read, Write};
use tracing::{info, warn, error};
use anyhow::Result;

use crate::files::{FileConfig, FileManager, FileManagerTrait};

/// Trait for snapshot operations
pub trait SnapshotTrait {
    /// Save a snapshot to disk and return the snapshot counter
    fn save(&mut self, snapshot: &qlib_rs::Snapshot) -> Result<u64>;
    
    /// Load the latest snapshot from disk
    fn load_latest(&self) -> Result<Option<(qlib_rs::Snapshot, u64)>>;
    
    /// Initialize snapshot counter from existing files
    fn initialize_counter(&mut self) -> Result<()>;
}

/// Configuration for snapshot manager operations
#[derive(Debug, Clone)]
pub struct SnapshotConfig {
    /// Snapshots directory path
    pub snapshots_dir: PathBuf,
    /// Maximum number of snapshot files to keep
    pub max_files: usize,
}

/// Snapshot manager request types
#[derive(Debug)]
pub enum SnapshotRequest {
    Save {
        snapshot: qlib_rs::Snapshot,
    },
    LoadLatest,
}

/// Response types for snapshot requests
#[derive(Debug)]
pub enum SnapshotResponse {
    SaveResult(Result<u64>),
    LoadResult(Result<Option<(qlib_rs::Snapshot, u64)>>),
}

/// Handle for communicating with snapshot manager task
#[derive(Debug, Clone)]
pub struct SnapshotHandle {
    request_sender: Sender<(SnapshotRequest, Sender<SnapshotResponse>)>,
}

impl SnapshotHandle {
    pub fn save(&self, snapshot: qlib_rs::Snapshot) -> Result<u64> {
        let (response_tx, response_rx) = unbounded();
        self.request_sender.send((SnapshotRequest::Save { snapshot }, response_tx))
            .map_err(|e| anyhow::anyhow!("Snapshot service has stopped: {}", e))?;
        
        match response_rx.recv()
            .map_err(|e| anyhow::anyhow!("Snapshot service response channel closed: {}", e))?
        {
            SnapshotResponse::SaveResult(result) => result,
            _ => Err(anyhow::anyhow!("Unexpected response type")),
        }
    }
    
    pub fn load_latest(&self) -> Result<Option<(qlib_rs::Snapshot, u64)>> {
        let (response_tx, response_rx) = unbounded();
        self.request_sender.send((SnapshotRequest::LoadLatest, response_tx))
            .map_err(|e| anyhow::anyhow!("Snapshot service has stopped: {}", e))?;
        
        match response_rx.recv()
            .map_err(|e| anyhow::anyhow!("Snapshot service response channel closed: {}", e))?
        {
            SnapshotResponse::LoadResult(result) => result,
            _ => Err(anyhow::anyhow!("Unexpected response type")),
        }
    }
}

pub type SnapshotService = SnapshotManagerTrait<FileManager>;

impl SnapshotService {
    pub fn spawn(config: SnapshotConfig) -> SnapshotHandle {
        let (request_sender, request_receiver) = bounded(1024);

        let handle = SnapshotHandle { request_sender };

        thread::spawn(move || {
            let mut service = SnapshotService::new(FileManager, config);
            match service.initialize_counter() {
                Ok(_) => info!("Snapshot service initialized successfully"),
                Err(e) => {
                    error!(error = %e, "Failed to initialize snapshot service");
                    return;
                },
            }

            while let Ok((request, response_sender)) = request_receiver.recv() {
                let response = match request {
                    SnapshotRequest::Save { snapshot } => {
                        SnapshotResponse::SaveResult(service.save(&snapshot))
                    }
                    SnapshotRequest::LoadLatest => {
                        SnapshotResponse::LoadResult(service.load_latest())
                    }
                };
                
                if let Err(_) = response_sender.send(response) {
                    error!("Failed to send snapshot service response");
                }
            }

            error!("Snapshot service has stopped unexpectedly");
        });

        handle
    }
}

/// Snapshot manager handles snapshot operations
pub struct SnapshotManagerTrait<F: FileManagerTrait> {
    file_manager: F,
    snapshot_config: FileConfig,
    /// Configuration for snapshot operations
    config: SnapshotConfig,
}

impl<F: FileManagerTrait> SnapshotManagerTrait<F> {
    pub fn new(file_manager: F, config: SnapshotConfig) -> Self {
        Self {
            file_manager,
            snapshot_config: FileConfig {
                prefix: "snapshot_".to_string(),
                suffix: ".bin".to_string(),
                max_files: config.max_files,
            },
            config,
        }
    }
}

impl<F: FileManagerTrait> SnapshotTrait for SnapshotManagerTrait<F> {
    /// Save a snapshot to disk and return the snapshot counter
    fn save(&mut self, snapshot: &qlib_rs::Snapshot) -> Result<u64> {
        create_dir_all(&self.config.snapshots_dir)?;
        
        let current_snapshot_counter = self.file_manager.get_next_counter(&self.config.snapshots_dir, &self.snapshot_config)?;

        let snapshot_filename = format!("snapshot_{:010}.bin", current_snapshot_counter);
        let snapshot_path = self.config.snapshots_dir.join(&snapshot_filename);
        
        info!(
            snapshot_file = %snapshot_path.display(),
            snapshot_counter = current_snapshot_counter,
            "Saving snapshot"
        );
        
        let serialized = bincode::serialize(snapshot)?;
        
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&snapshot_path)
            ?;
        
        file.write_all(&serialized)?;
        file.flush()?;
        
        info!(
            snapshot_size_bytes = serialized.len(),
            snapshot_counter = current_snapshot_counter,
            "Snapshot saved successfully"
        );
        
        // Clean up old snapshots
        if let Err(e) = self.file_manager.cleanup_old_files(&self.config.snapshots_dir, &self.snapshot_config) {
            error!(error = %e, "Failed to clean up old snapshots");
        }
        
        Ok(current_snapshot_counter)
    }
    
    /// Load the latest snapshot from disk
    fn load_latest(&self) -> Result<Option<(qlib_rs::Snapshot, u64)>> {
        let snapshot_files = self.file_manager.scan_files(&self.config.snapshots_dir, &self.snapshot_config)?;
        
        if snapshot_files.is_empty() {
            info!("No snapshot files found, starting with empty store");
            return Ok(None);
        }
        
        // Try loading snapshots from latest to oldest
        for file_info in snapshot_files.iter().rev() {
            info!(
                snapshot_file = %file_info.path.display(),
                snapshot_counter = file_info.counter,
                "Attempting to load snapshot"
            );
            
            match self.try_load_snapshot(&file_info.path) {
                Ok(Some(snapshot)) => {
                    info!(
                        snapshot_file = %file_info.path.display(),
                        snapshot_counter = file_info.counter,
                        "Successfully loaded snapshot"
                    );
                    return Ok(Some((snapshot, file_info.counter)));
                }
                Ok(None) => {
                    warn!(
                        snapshot_file = %file_info.path.display(),
                        "Snapshot file is empty or corrupted, trying next"
                    );
                }
                Err(e) => {
                    warn!(
                        snapshot_file = %file_info.path.display(),
                        error = %e,
                        "Failed to load snapshot, trying next"
                    );
                }
            }
        }
        
        warn!("All snapshot files failed to load or were corrupted, starting with empty store");
        Ok(None)
    }
    
    /// Initialize snapshot counter from existing files
    fn initialize_counter(&mut self) -> Result<()> {
        let next_snapshot_counter = self.file_manager.get_next_counter(&self.config.snapshots_dir, &self.snapshot_config)?;
        
        info!(
            snapshot_dir = %self.config.snapshots_dir.display(),
            next_counter = next_snapshot_counter,
            "Initialized snapshot file counter"
        );

        Ok(())
    }
}

impl<F: FileManagerTrait> SnapshotManagerTrait<F> {
    /// Try to load a single snapshot file
    fn try_load_snapshot(&self, snapshot_path: &PathBuf) -> Result<Option<qlib_rs::Snapshot>> {
        match File::open(snapshot_path) {
            Ok(mut file) => {
                let mut buffer = Vec::new();
                file.read_to_end(&mut buffer)?;
                
                if buffer.is_empty() {
                    return Ok(None);
                }
                
                match bincode::deserialize::<qlib_rs::Snapshot>(&buffer) {
                    Ok(snapshot) => Ok(Some(snapshot)),
                    Err(e) => Err(anyhow::anyhow!("Failed to deserialize snapshot: {}", e))
                }
            }
            Err(e) => Err(anyhow::anyhow!("Failed to open snapshot file: {}", e))
        }
    }
}