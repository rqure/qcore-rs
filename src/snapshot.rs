use std::path::PathBuf;

use anyhow::Result;
use async_trait::async_trait;
use tokio::{fs::{create_dir_all, remove_file, File, OpenOptions}, io::{AsyncReadExt, AsyncWriteExt}, sync::{mpsc, oneshot}};
use tracing::{info, instrument, warn, error};

use crate::files::{FileConfig, FileManager, FileManagerTrait};

/// Trait for snapshot operations
#[async_trait]
pub trait SnapshotTrait {
    /// Save a snapshot to disk and return the snapshot counter
    async fn save(&mut self, snapshot: &qlib_rs::Snapshot) -> Result<u64>;
    
    /// Load the latest snapshot from disk
    async fn load_latest(&self) -> Result<Option<(qlib_rs::Snapshot, u64)>>;
    
    /// Initialize snapshot counter from existing files
    async fn initialize_counter(&mut self) -> Result<()>;
}


/// Handle for communicating with snapshot manager task
#[derive(Debug, Clone)]
pub struct SnapshotHandle {
    sender: mpsc::UnboundedSender<SnapshotRequest>,
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
        response: oneshot::Sender<Result<u64>>,
    }
}

impl SnapshotHandle {
    pub async fn save(&self, snapshot: qlib_rs::Snapshot) -> Result<u64> {
        let (response_tx, response_rx) = oneshot::channel();
        self.sender.send(SnapshotRequest::Save {
            snapshot,
            response: response_tx,
        }).map_err(|_| anyhow::anyhow!("Snapshot manager task has stopped"))?;
        response_rx.await.map_err(|_| anyhow::anyhow!("Snapshot manager task response channel closed"))?
    }
}

pub type SnapshotService = SnapshotManagerTrait<FileManager>;

impl SnapshotService {
    pub fn spawn(config: SnapshotConfig) -> SnapshotHandle {
        let (sender, mut receiver) = mpsc::unbounded_channel();

        tokio::spawn(async move {
            let mut service = SnapshotService::new(FileManager, config);
            service.initialize_counter().await;

            while let Some(request) = receiver.recv().await {
                match request {
                    SnapshotRequest::Save { snapshot, response } => {
                        let result = service.save(&snapshot).await;
                        let _ = response.send(result);
                    }
                }
            }
        });

        SnapshotHandle { sender }
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

#[async_trait]
impl<F: FileManagerTrait> SnapshotTrait for SnapshotManagerTrait<F> {
    /// Save a snapshot to disk and return the snapshot counter
    #[instrument(skip(self, snapshot))]
    async fn save(&mut self, snapshot: &qlib_rs::Snapshot) -> Result<u64> {
        create_dir_all(&self.config.snapshots_dir).await?;
        
        let current_snapshot_counter = self.file_manager.get_next_counter(&self.config.snapshots_dir, &self.snapshot_config).await?;

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
            .await?;
        
        file.write_all(&serialized).await?;
        file.flush().await?;
        
        info!(
            snapshot_size_bytes = serialized.len(),
            snapshot_counter = current_snapshot_counter,
            "Snapshot saved successfully"
        );
        
        // Clean up old snapshots
        if let Err(e) = self.file_manager.cleanup_old_files(&self.config.snapshots_dir, &self.snapshot_config).await {
            error!(error = %e, "Failed to clean up old snapshots");
        }
        
        Ok(current_snapshot_counter)
    }
    
    /// Load the latest snapshot from disk
    async fn load_latest(&self) -> Result<Option<(qlib_rs::Snapshot, u64)>> {
        let snapshot_files = self.file_manager.scan_files(&self.config.snapshots_dir, &self.snapshot_config).await?;
        
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
            
            match self.try_load_snapshot(&file_info.path).await {
                Ok(Some(snapshot)) => {
                    info!(
                        snapshot_file = %file_info.path.display(),
                        snapshot_counter = file_info.counter,
                        "Snapshot loaded successfully"
                    );
                    return Ok(Some((snapshot, file_info.counter)));
                }
                Ok(None) => {
                    // File was corrupted and cleaned up, try next
                    continue;
                }
                Err(e) => {
                    error!(
                        error = %e,
                        snapshot_file = %file_info.path.display(),
                        "Failed to load snapshot, trying previous snapshot"
                    );
                    continue;
                }
            }
        }
        
        warn!("All snapshot files failed to load or were corrupted, starting with empty store");
        Ok(None)
    }
    
    /// Initialize snapshot counter from existing files
    async fn initialize_counter(&mut self) -> Result<()> {
        let next_snapshot_counter = self.file_manager.get_next_counter(&self.config.snapshots_dir, &self.snapshot_config).await?;
        
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
    async fn try_load_snapshot(&self, snapshot_path: &PathBuf) -> Result<Option<qlib_rs::Snapshot>> {
        match File::open(snapshot_path).await {
            Ok(mut file) => {
                let mut buffer = Vec::new();
                match file.read_to_end(&mut buffer).await {
                    Ok(_) => {
                        match bincode::deserialize(&buffer) {
                            Ok(snapshot) => Ok(Some(snapshot)),
                            Err(e) => {
                                error!(
                                    error = %e,
                                    snapshot_file = %snapshot_path.display(),
                                    "Failed to deserialize snapshot, marking for cleanup"
                                );
                                // Defensive: Mark corrupted snapshot for cleanup
                                if let Err(cleanup_err) = remove_file(snapshot_path).await {
                                    warn!(
                                        error = %cleanup_err,
                                        snapshot_file = %snapshot_path.display(),
                                        "Failed to remove corrupted snapshot file"
                                    );
                                }
                                Ok(None) // Corrupted file cleaned up
                            }
                        }
                    }
                    Err(e) => Err(anyhow::anyhow!("Failed to read snapshot file: {}", e))
                }
            }
            Err(e) => Err(anyhow::anyhow!("Failed to open snapshot file: {}", e))
        }
    }
}