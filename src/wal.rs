use tokio::sync::mpsc::Sender;
use qlib_rs::{now};
use tracing::{info, warn, error, debug};
use anyhow::Result;
use std::vec;
use tokio::fs::{File, OpenOptions, create_dir_all};
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use std::path::PathBuf;
use async_trait::async_trait;

use crate::files::{FileConfig, FileInfo, FileManager, FileManagerTrait};
use crate::snapshot::SnapshotHandle;
use crate::store::StoreHandle;
use crate::Services;

/// Configuration for WAL manager operations
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// WAL directory path
    pub wal_dir: PathBuf,
    /// Maximum WAL file size in bytes
    pub max_file_size: usize,
    /// Maximum number of WAL files to keep
    pub max_files: usize,
    /// Number of WAL file rollovers before taking a snapshot
    pub snapshot_wal_interval: u64,
    /// Unique machine identifier for snapshot originator
    pub machine_id: String,
}

/// WAL manager request types
#[derive(Debug)]
pub enum WalRequest {
    WriteRequest {
        request: qlib_rs::Request,
        response: tokio::sync::oneshot::Sender<Result<()>>,
    },
    SetServices {
        services: Services,
        response: tokio::sync::oneshot::Sender<()>,
    },
}

/// Handle for communicating with WAL manager task
#[derive(Debug, Clone)]
pub struct WalHandle {
    sender: Sender<WalRequest>,
}

impl WalHandle {
    pub fn write_request(&self, request: qlib_rs::Request) -> Result<()> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        self.sender.send(WalRequest::WriteRequest {
            request,
            response: response_tx,
        }).map_err(|e| anyhow::anyhow!("WAL service has stopped: {}", e))?;
        response_rx.map_err(|e| anyhow::anyhow!("WAL service response channel closed: {}", e))?
    }

    /// Set services for dependencies
    pub fn set_services(&self, services: Services) {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        if let Ok(_) = self.sender.send(WalRequest::SetServices {
            services,
            response: response_tx,
        }) {
            if let Err(_) = response_rx {
                error!("WAL service SetServices response channel closed");
            }
        }
    }
}

pub type WalService = WalManagerTrait<FileManager>;

impl WalService {
    pub fn spawn(config: WalConfig) -> WalHandle {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(131072);
        tokio::spawn(async move {
            let mut service = WalService::new(FileManager, config, None);
            if let Err(e) = service.initialize_counter() {
                warn!(error = %e, "Failed to initialize WAL counter");
            }

            while let Some(request) = receiver.recv() {
                match request {
                    WalRequest::WriteRequest { request, response } => {
                        // For now, we'll need to wait until services are set
                        if let Some(services) = &service.services {
                            let store_handle = services.store_handle.clone();
                            let result = service.write_request(&request, &store_handle);
                            if let Err(_) = response.send(result) {
                                error!("Failed to send WAL write response");
                            }
                        } else {
                            if let Err(_) = response.send(Err(anyhow::anyhow!("Services not yet initialized"))) {
                                error!("Failed to send WAL write response");
                            }
                        }
                    }
                    WalRequest::SetServices { services, response } => {
                        service.services = Some(services.clone());
                        // Now that we have services, we can replay from the store
                        if let Err(_) = service.replay(&services.store_handle) {
                            error!("Failed to replay WAL");
                        }
                        if let Err(_) = response.send(()) {
                            error!("Failed to send SetServices response");
                        }
                    }
                }
            }

            panic!("WAL manager service has stopped unexpectedly");
        });

        WalHandle { sender }
    }
}

/// Entry reader for parsing serialized data
pub trait EntryReaderTrait<T> {
    type Error;
    
    /// Read the next entry from the data source
    fn next_entry(&mut self) -> Option<Result<(T, usize), Self::Error>>;
}

/// Trait for WAL operations
#[async_trait]
pub trait WalTrait {
    /// Write a request to WAL
    fn write_request(&mut self, request: &qlib_rs::Request, store_handle: &StoreHandle) -> Result<()>;

    /// Replay WAL files to restore store state
    fn replay(&self, store_handle: &StoreHandle) -> Result<()>;

    /// Initialize WAL counter from existing files
    fn initialize_counter(&mut self) -> Result<()>;
}

/// Iterator for reading WAL entries from a buffer
pub struct WalEntryReader {
    buffer: Vec<u8>,
    offset: usize,
}

impl WalEntryReader {
    pub fn new(buffer: Vec<u8>) -> Self {
        Self { buffer, offset: 0 }
    }
    
    pub fn from_offset(buffer: Vec<u8>, start_offset: usize) -> Self {
        Self { buffer, offset: start_offset }
    }
}

impl EntryReaderTrait<Vec<u8>> for WalEntryReader {
    type Error = anyhow::Error;
    
    fn next_entry(&mut self) -> Option<Result<(Vec<u8>, usize), Self::Error>> {
        if self.offset >= self.buffer.len() {
            return None;
        }
        
        // Read length prefix (4 bytes)
        if self.offset + 4 > self.buffer.len() {
            return Some(Err(anyhow::anyhow!("Incomplete length prefix at offset {}", self.offset)));
        }
        
        let len = u32::from_le_bytes([
            self.buffer[self.offset],
            self.buffer[self.offset + 1],
            self.buffer[self.offset + 2],
            self.buffer[self.offset + 3]
        ]) as usize;
        
        self.offset += 4;
        
        // Validate length
        if len == 0 {
            return Some(Err(anyhow::anyhow!("Found zero-length entry at offset {}", self.offset - 4)));
        }
        
        if len > 1024 * 1024 * 100 { // 100MB sanity check
            return Some(Err(anyhow::anyhow!("Entry length {} seems too large at offset {}", len, self.offset - 4)));
        }
        
        if self.offset + len > self.buffer.len() {
            return Some(Err(anyhow::anyhow!("Incomplete request at offset {}", self.offset - 4)));
        }
        
        let entry_data = self.buffer[self.offset..self.offset + len].to_vec();
        self.offset += len;
        
        Some(Ok((entry_data, self.offset)))
    }
}

impl Iterator for WalEntryReader {
    type Item = Result<(Vec<u8>, usize)>; // (entry_data, next_offset)
    
    fn next(&mut self) -> Option<Self::Item> {
        self.next_entry()
    }
}

/// WAL manager handles WAL file operations
pub struct WalManagerTrait<F: FileManagerTrait> {
    file_manager: F,
    file_config: FileConfig,
    /// Configuration for WAL operations
    wal_config: WalConfig,
    /// Current WAL file handle and size
    current_wal_file: Option<File>,
    current_wal_size: usize,
    /// Number of WAL files created since last snapshot
    wal_files_since_snapshot: u64,
    /// Handle to communicate with snapshot manager
    snapshot_handle: Option<SnapshotHandle>,
    /// Services for dependencies
    services: Option<Services>,
}

impl<F: FileManagerTrait> WalManagerTrait<F> {
    pub fn new(file_manager: F, config: WalConfig, snapshot_handle: Option<SnapshotHandle>) -> Self {
        Self {
            file_manager,
            file_config: FileConfig {
                prefix: "wal_".to_string(),
                suffix: ".log".to_string(),
                max_files: config.max_files, // Will be overridden by config
            },
            wal_config: config,
            current_wal_file: None,
            current_wal_size: 0,
            wal_files_since_snapshot: 0,
            snapshot_handle,
            services: None,
        }
    }
}

#[async_trait]
impl<F: FileManagerTrait> WalTrait for WalManagerTrait<F> {
    /// Write a request to WAL with file rotation and snapshot handling
    fn write_request(&mut self, request: &qlib_rs::Request, store_handle: &StoreHandle) -> Result<()> {
        let serialized = serde_json::to_vec(request)?;
        let serialized_len = serialized.len();
        
        // Check if we need to create a new WAL file
        let should_create_new_file = self.current_wal_file.is_none() || 
           (self.current_wal_size + serialized_len > self.wal_config.max_file_size);

        if should_create_new_file {
            self.rotate_file(store_handle)?;
        }
        
        // Write the actual data
        self.write_entry(&serialized)?;
        
        Ok(())
    }
    
    /// Replay WAL files to restore store state
    fn replay(&self, store_handle: &StoreHandle) -> Result<()> {
        let wal_files = self.file_manager.scan_files(&self.wal_config.wal_dir, &self.file_config)?;
        
        if wal_files.is_empty() {
            info!("No WAL files found, no replay needed");
            return Ok(());
        }
        
        let most_recent_snapshot = self.find_latest_snapshot_marker(&wal_files)?;
        
        store_handle.disable_notifications();
        info!("Notifications disabled for WAL replay");
        
        // Defensive: Comprehensive error handling for WAL replay with proper cleanup
        let replay_result = self.perform_replay(&wal_files, most_recent_snapshot, store_handle);

        store_handle.enable_notifications();
        info!("Notifications re-enabled after WAL replay");
        
        match replay_result {
            Ok(_) => {
                info!("WAL replay completed successfully");
                Ok(())
            }
            Err(e) => {
                error!(error = %e, "WAL replay failed but continuing with startup");
                Ok(()) // Defensive: Don't fail startup for replay issues
            }
        }
    }
    
    /// Initialize WAL counter from existing files
    fn initialize_counter(&mut self) -> Result<()> {
        let next_wal_counter = self.file_manager.get_next_counter(&self.wal_config.wal_dir, &self.file_config)?;
        // The counter is now managed internally by tracking through the file manager
        info!(
            wal_dir = %self.wal_config.wal_dir.display(),
            next_counter = next_wal_counter,
            "Initialized WAL file counter"
        );
        Ok(())
    }
}

impl<F: FileManagerTrait> WalManagerTrait<F> {
    fn rotate_file(&mut self, store_handle: &StoreHandle) -> Result<()> {
        create_dir_all(&self.wal_config.wal_dir)?;
        
        let wal_file_counter = self.file_manager.get_next_counter(&self.wal_config.wal_dir, &self.file_config)?;
        let wal_filename = format!("wal_{:010}.log", wal_file_counter);
        let wal_path = self.wal_config.wal_dir.join(&wal_filename);

        let current_size = self.current_wal_size;
        let max_size = self.wal_config.max_file_size;

        info!(
            wal_file = %wal_path.display(),
            wal_counter = wal_file_counter,
            current_size = current_size,
            max_size = max_size,
            "Creating new WAL file"
        );
        
        let file = OpenOptions::new().create(true).append(true).open(&wal_path)?;
        self.current_wal_file = Some(file);
        self.current_wal_size = 0;
        
        self.wal_files_since_snapshot += 1;
        self.handle_snapshot_if_needed(store_handle)?;

        if let Err(e) = self.file_manager.cleanup_old_files(&self.wal_config.wal_dir, &self.file_config) {
            error!(error = %e, "Failed to clean up old WAL files");
        }
        
        Ok(())
    }
    
    /// Handle snapshot creation if the interval is reached
    fn handle_snapshot_if_needed(&mut self, store_handle: &StoreHandle) -> Result<()> {
        if self.wal_files_since_snapshot >= self.wal_config.snapshot_wal_interval {
            info!(wal_files_count = self.wal_files_since_snapshot, "Taking snapshot after WAL file rollovers");
            
            // Store current state for potential rollback
            let original_wal_files_since_snapshot = self.wal_files_since_snapshot;
            match store_handle.take_snapshot() {
                Some(snapshot) => {
                    if let Some(snapshot_handle) = &self.snapshot_handle {
                        match snapshot_handle.save(snapshot) {
                            Ok(snapshot_counter) => {
                                self.wal_files_since_snapshot = 0;
                                info!("Snapshot saved successfully after WAL rollover");

                                let snapshot_request = qlib_rs::Request::Snapshot {
                                    snapshot_counter,
                                    timestamp: Some(now()),
                                    originator: Some(self.wal_config.machine_id.clone()),
                                };

                                if let Err(e) = self.write_request(&snapshot_request, store_handle) {
                                    error!(error = %e, "Failed to write snapshot marker to WAL");
                                }
                            }
                            Err(e) => {
                                error!(error = %e, "Failed to save snapshot after WAL rollover");
                                self.wal_files_since_snapshot = original_wal_files_since_snapshot;
                            }
                        }
                    } else {
                        warn!("No snapshot handle available, skipping snapshot creation");
                    }
                },
                None => {
                    warn!("No snapshot generated, skipping snapshot persistence");
                }
            }
        }
        
        Ok(())
    }
    
    /// Write an entry to the current WAL file
    fn write_entry(&mut self, serialized: &[u8]) -> Result<()> {
        if let Some(ref mut wal_file) = self.current_wal_file {
            let len_bytes = (serialized.len() as u32).to_le_bytes();
            wal_file.write_all(&len_bytes)?;
            wal_file.write_all(serialized)?;
            wal_file.flush()?;
            self.current_wal_size += 4 + serialized.len();
        }
        Ok(())
    }
    
    /// Find the most recent snapshot marker across all WAL files
    fn find_latest_snapshot_marker(&self, wal_files: &[FileInfo]) -> Result<Option<(PathBuf, u64, usize)>> {
        let mut most_recent_snapshot: Option<(PathBuf, u64, usize)> = None;
        
        info!(wal_files_count = wal_files.len(), "Scanning WAL files to find the most recent snapshot marker");
        
        for file_info in wal_files {
            if let Ok(snapshot_info) = self.find_snapshot_marker_in_file(&file_info.path) {
                if let Some((offset_after_snapshot, _)) = snapshot_info {
                    if most_recent_snapshot.is_none() || file_info.counter > most_recent_snapshot.as_ref().unwrap().1 {
                        most_recent_snapshot = Some((file_info.path.clone(), file_info.counter, offset_after_snapshot));
                    }
                }
            }
        }
        
        Ok(most_recent_snapshot)
    }
    
    /// Find the most recent snapshot marker in a specific WAL file
    fn find_snapshot_marker_in_file(&self, wal_path: &PathBuf) -> Result<Option<(usize, qlib_rs::Request)>> {
        let mut file = File::open(wal_path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        
        let mut last_snapshot_offset = None;
        let mut last_snapshot_request = None;
        
        let entry_reader = WalEntryReader::new(buffer);
        
        for entry_result in entry_reader {
            match entry_result {
                Ok((entry_data, next_offset)) => {
                    if let Ok(request) = serde_json::from_slice::<qlib_rs::Request>(&entry_data) {
                        if matches!(request, qlib_rs::Request::Snapshot { .. }) {
                            last_snapshot_offset = Some(next_offset);
                            last_snapshot_request = Some(request);
                            debug!(offset = next_offset, wal_file = %wal_path.display(), "Found snapshot marker");
                        }
                    }
                }
                Err(e) => {
                    debug!(error = %e, wal_file = %wal_path.display(), "Error reading WAL entry, continuing");
                    break;
                }
            }
        }
        
        if let (Some(offset), Some(request)) = (last_snapshot_offset, last_snapshot_request) {
            Ok(Some((offset, request)))
        } else {
            Ok(None)
        }
    }
    
    /// Perform the actual replay operation
    fn perform_replay(&self, wal_files: &[FileInfo], most_recent_snapshot: Option<(PathBuf, u64, usize)>, store_handle: &StoreHandle) -> Result<()> {
        match most_recent_snapshot {
            Some((snapshot_wal_file, snapshot_counter, snapshot_offset)) => {
                info!(
                    wal_file = %snapshot_wal_file.display(),
                    wal_counter = snapshot_counter,
                    offset = snapshot_offset,
                    "Starting replay from WAL file with snapshot marker"
                );
                
                // Replay the partial WAL file from the snapshot offset
                self.replay_file_from_offset(&snapshot_wal_file, snapshot_offset, store_handle)?;

                // Replay all subsequent WAL files completely
                for file_info in wal_files {
                    if file_info.counter > snapshot_counter {
                        info!(wal_file = %file_info.path.display(), wal_counter = file_info.counter, "Replaying complete WAL file");
                        if let Err(e) = self.replay_file_from_offset(&file_info.path, 0, store_handle) {
                            error!(wal_file = %file_info.path.display(), error = %e, "Failed to replay WAL file");
                        }
                    }
                }
            }
            None => {
                info!(wal_files_count = wal_files.len(), "No snapshot markers found, replaying all files completely");
                
                for file_info in wal_files {
                    info!(wal_file = %file_info.path.display(), wal_counter = file_info.counter, "Replaying WAL file");
                    if let Err(e) = self.replay_file_from_offset(&file_info.path, 0, store_handle) {
                        error!(wal_file = %file_info.path.display(), error = %e, "Failed to replay WAL file");
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Replay a single WAL file from a specific offset
    fn replay_file_from_offset(&self, wal_path: &PathBuf, start_offset: usize, store_handle: &StoreHandle) -> Result<()> {
        let mut file = File::open(wal_path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        
        if buffer.is_empty() {
            warn!(wal_file = %wal_path.display(), "WAL file is empty, nothing to replay");
            return Ok(());
        }
        
        if start_offset >= buffer.len() {
            warn!(
                start_offset = start_offset, 
                buffer_len = buffer.len(),
                wal_file = %wal_path.display(),
                "Start offset is beyond buffer length, nothing to replay"
            );
            return Ok(());
        }
        
        let adjusted_offset = self.validate_start_offset(&buffer, start_offset, wal_path)?;
        let entry_reader = WalEntryReader::from_offset(buffer, adjusted_offset);
        
        let mut requests_processed = 0;
        info!(wal_file = %wal_path.display(), start_offset = adjusted_offset, "Replaying WAL file from offset");
        
        for entry_result in entry_reader {
            match entry_result {
                Ok((entry_data, _)) => {
                    match self.apply_wal_entry(&entry_data, store_handle) {
                        Ok(true) => requests_processed += 1,
                        Ok(false) => {}, // Processed but not counted (e.g., snapshot markers)
                        Err(e) => {
                            error!(
                                error = %e,
                                "Failed to apply request during replay"
                            );
                            // Defensive: Continue with next entry rather than failing completely
                        }
                    }
                }
                Err(e) => {
                    error!(error = %e, wal_file = %wal_path.display(), "Error reading WAL entry during replay");
                    break;
                }
            }
        }
        
        let replay_type = if start_offset == 0 { "complete" } else { "from offset" };
        info!(
            requests_processed = requests_processed,
            wal_file = %wal_path.display(),
            start_offset = adjusted_offset,
            "Replayed {} requests from WAL file {}",
            requests_processed,
            replay_type
        );
        
        Ok(())
    }
    
    /// Validate and adjust start offset to entry boundary
    fn validate_start_offset(&self, buffer: &[u8], start_offset: usize, wal_path: &PathBuf) -> Result<usize> {
        if start_offset == 0 {
            return Ok(0);
        }
        
        // Scan from beginning to find valid entry boundary
        let mut scan_offset = 0;
        let mut valid_offset = 0;
        
        while scan_offset < start_offset && scan_offset + 4 <= buffer.len() {
            let len = u32::from_le_bytes([buffer[scan_offset], buffer[scan_offset+1], buffer[scan_offset+2], buffer[scan_offset+3]]) as usize;
            scan_offset += 4;
            
            if scan_offset + len > buffer.len() {
                break;
            }
            
            let next_entry_start = scan_offset + len;
            if next_entry_start == start_offset {
                valid_offset = start_offset;
                break;
            } else if next_entry_start > start_offset {
                valid_offset = scan_offset;
                break;
            }
            
            scan_offset = next_entry_start;
        }
        
        if valid_offset != start_offset && start_offset > 0 {
            warn!(
                original_offset = start_offset,
                adjusted_offset = valid_offset,
                wal_file = %wal_path.display(),
                "Adjusted start offset to valid entry boundary"
            );
        }
        
        Ok(valid_offset)
    }
    
    /// Apply a single WAL entry during replay
    fn apply_wal_entry(&self, entry_data: &[u8], store_handle: &StoreHandle) -> Result<bool> {
        match serde_json::from_slice::<qlib_rs::Request>(entry_data) {
            Ok(request) => {
                // Skip snapshot requests during replay (they are just markers)
                if matches!(request, qlib_rs::Request::Snapshot { .. }) {
                    debug!("Skipping snapshot marker during replay");
                    return Ok(false); // Processed but not counted
                }

                if let Err(e) = store_handle.perform_mut(vec![request]) {
                    return Err(anyhow::anyhow!("Failed to apply request during WAL replay: {}", e));
                }
                
                Ok(true) // Successfully processed and should be counted
            }
            Err(e) => {
                Err(anyhow::anyhow!("Failed to deserialize request: {}", e))
            }
        }
    }
}
