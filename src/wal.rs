use anyhow::Result;
use crossbeam::channel::Sender;
use qlib_rs::Requests;
use std::fs::{File, OpenOptions, create_dir_all};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::thread;
use tracing::{debug, error, info, warn};

use crate::core::CoreHandle;
use crate::files::{FileConfig, FileInfo, FileManager, FileManagerTrait};

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
}

/// WAL manager request types
#[derive(Debug)]
pub enum WalCommand {
    AppendRequest { requests: qlib_rs::Requests },
    Replay,
    SetCoreHandle { handle: CoreHandle },
}

/// Trait for WAL operations
pub trait WalTrait {
    /// Write a request to WAL
    fn append_requests(&mut self, requests: &qlib_rs::Requests) -> Result<()>;

    /// Replay WAL files to restore store state
    fn replay(&self) -> Result<Requests>;

    /// Initialize WAL counter from existing files
    fn initialize_counter(&mut self) -> Result<()>;
}

/// Entry reader for parsing serialized data
pub trait EntryReaderTrait<T> {
    type Error;

    /// Read the next entry from the data source
    fn next_entry(&mut self) -> Option<Result<(T, usize), Self::Error>>;
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
        Self {
            buffer,
            offset: start_offset,
        }
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
            return Some(Err(anyhow::anyhow!("Incomplete length prefix")));
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
            return Some(Err(anyhow::anyhow!("Incomplete entry data")));
        }

        let entry_data = self.buffer[self.offset..self.offset + length].to_vec();
        self.offset += length;

        Some(Ok((entry_data, self.offset)))
    }
}

impl Iterator for WalEntryReader {
    type Item = Result<(Vec<u8>, usize)>; // (entry_data, next_offset)

    fn next(&mut self) -> Option<Self::Item> {
        self.next_entry()
    }
}

/// Handle for communicating with WAL manager task
#[derive(Debug, Clone)]
pub struct WalHandle {
    sender: Sender<WalCommand>,
}

impl WalHandle {
    pub fn append_requests(&self, requests: qlib_rs::Requests) {
        self.sender
            .send(WalCommand::AppendRequest { requests })
            .unwrap();
    }

    pub fn replay(&self) {
        self.sender.send(WalCommand::Replay).unwrap();
    }

    pub fn set_core_handle(&self, core: CoreHandle) {
        self.sender
            .send(WalCommand::SetCoreHandle { handle: core })
            .unwrap();
    }
}

pub type WalService = WalManagerTrait<FileManager>;

impl WalService {
    pub fn spawn(config: WalConfig) -> WalHandle {
        let (sender, receiver) = crossbeam::channel::unbounded();
        let handle = WalHandle { sender };

        thread::spawn(move || {
            let mut service = WalService::new(FileManager, config);
            match service.initialize_counter() {
                Ok(_) => info!("WAL service initialized successfully"),
                Err(e) => {
                    error!(error = %e, "Failed to initialize WAL service");
                    return;
                }
            }

            while let Ok(request) = receiver.recv() {
                match request {
                    WalCommand::AppendRequest { requests } => {
                        match service.append_requests(&requests) {
                            Ok(_) => debug!("Wrote request to WAL"),
                            Err(e) => error!(error = %e, "Failed to write request to WAL"),
                        }
                    }
                    WalCommand::Replay => {
                        if let Some(core_handle) = &service.core_handle {
                            match service.replay() {
                                Ok(requests) => core_handle.perform(requests),
                                Err(e) => {
                                    error!(error = %e, "Failed to replay WAL files");
                                }
                            }
                        } else {
                            warn!("Core handle not set, cannot replay WAL");
                        }
                    }
                    WalCommand::SetCoreHandle { handle } => {
                        service.core_handle = Some(handle);
                    }
                }
            }

            panic!("WAL manager service has stopped unexpectedly");
        });

        handle
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
    /// Handle to communicate with core service
    core_handle: Option<CoreHandle>,
}

impl<F: FileManagerTrait> WalManagerTrait<F> {
    pub fn new(file_manager: F, config: WalConfig) -> Self {
        Self {
            file_manager,
            file_config: FileConfig {
                prefix: "wal_".to_string(),
                suffix: ".log".to_string(),
                max_files: config.max_files,
            },
            wal_config: config,
            current_wal_file: None,
            current_wal_size: 0,
            wal_files_since_snapshot: 0,
            core_handle: None,
        }
    }
}

impl<F: FileManagerTrait> WalTrait for WalManagerTrait<F> {
    /// Write a request to WAL with file rotation and snapshot handling
    fn append_requests(&mut self, requests: &qlib_rs::Requests) -> Result<()> {
        for request in requests.read().iter() {
            if let qlib_rs::Request::Read { .. } = request {
                continue;
            } else if let qlib_rs::Request::GetEntityType { .. } = request {
                continue;
            } else if let qlib_rs::Request::GetFieldType { .. } = request {
                continue;
            } else if let qlib_rs::Request::ResolveEntityType { .. } = request {
                continue;
            } else if let qlib_rs::Request::ResolveFieldType { .. } = request {
                continue;
            }

            // Skip read requests
            let serialized = bincode::serialize(request)?;

            // Check if we need to rotate the file
            if self.current_wal_size + serialized.len() + 4 > self.wal_config.max_file_size {
                self.rotate_file()?;
            }

            // Write the entry
            self.write_entry(&serialized)?;
        }

        Ok(())
    }

    /// Replay WAL files to restore store state
    fn replay(&self) -> Result<Requests> {
        let wal_files = self
            .file_manager
            .scan_files(&self.wal_config.wal_dir, &self.file_config)?;

        if wal_files.is_empty() {
            info!("No WAL files found");
            return Ok(Requests::new(vec![]));
        }

        // Find the most recent snapshot marker
        let most_recent_snapshot = self.find_latest_snapshot_marker(&wal_files)?;

        // Perform the actual replay
        self.perform_replay(&wal_files, most_recent_snapshot)
    }

    /// Initialize WAL counter from existing files
    fn initialize_counter(&mut self) -> Result<()> {
        let next_wal_counter = self
            .file_manager
            .get_next_counter(&self.wal_config.wal_dir, &self.file_config)?;

        info!(
            wal_dir = %self.wal_config.wal_dir.display(),
            next_counter = next_wal_counter,
            "Initialized WAL file counter"
        );

        Ok(())
    }
}

impl<F: FileManagerTrait> WalManagerTrait<F> {
    fn rotate_file(&mut self) -> Result<()> {
        // Close current file
        if let Some(mut file) = self.current_wal_file.take() {
            file.flush()?;
        }

        // Reset size counter
        self.current_wal_size = 0;
        self.wal_files_since_snapshot += 1;

        // Handle snapshot if needed
        self.handle_snapshot_if_needed()?;

        // Clean up old files
        if let Err(e) = self
            .file_manager
            .cleanup_old_files(&self.wal_config.wal_dir, &self.file_config)
        {
            error!(error = %e, "Failed to clean up old WAL files");
        }

        Ok(())
    }

    /// Handle snapshot creation if the interval is reached
    fn handle_snapshot_if_needed(&mut self) -> Result<()> {
        if self.wal_files_since_snapshot >= self.wal_config.snapshot_wal_interval {
            info!(
                wal_files_since_snapshot = self.wal_files_since_snapshot,
                snapshot_interval = self.wal_config.snapshot_wal_interval,
                "WAL rollover interval reached, triggering snapshot"
            );

            if let Some(core_handle) = &self.core_handle {
                core_handle.take_snapshot();
                self.wal_files_since_snapshot = 0;
            }
        }

        Ok(())
    }

    /// Write an entry to the current WAL file
    fn write_entry(&mut self, serialized: &[u8]) -> Result<()> {
        // Ensure we have a WAL file open
        if self.current_wal_file.is_none() {
            create_dir_all(&self.wal_config.wal_dir)?;

            let next_counter = self
                .file_manager
                .get_next_counter(&self.wal_config.wal_dir, &self.file_config)?;
            let wal_filename = format!("wal_{:010}.log", next_counter);
            let wal_path = self.wal_config.wal_dir.join(&wal_filename);

            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&wal_path)?;

            self.current_wal_file = Some(file);
            self.current_wal_size = 0;

            info!(wal_file = %wal_path.display(), "Created new WAL file");
        }

        let file = self.current_wal_file.as_mut().unwrap();

        // Write length prefix (4 bytes)
        let length = serialized.len() as u32;
        file.write_all(&length.to_le_bytes())?;

        // Write entry data
        file.write_all(serialized)?;
        file.flush()?;

        self.current_wal_size += 4 + serialized.len();

        Ok(())
    }

    /// Find the most recent snapshot marker across all WAL files
    fn find_latest_snapshot_marker(
        &self,
        wal_files: &[FileInfo],
    ) -> Result<Option<(PathBuf, u64, usize)>> {
        let mut latest_snapshot: Option<(PathBuf, u64, usize)> = None;

        for file_info in wal_files.iter().rev() {
            if let Ok(Some((offset, request))) = self.find_snapshot_marker_in_file(&file_info.path)
            {
                match request {
                    qlib_rs::Request::Snapshot {
                        snapshot_counter, ..
                    } => {
                        latest_snapshot = Some((file_info.path.clone(), snapshot_counter, offset));
                        break;
                    }
                    _ => continue,
                }
            }
        }

        Ok(latest_snapshot)
    }

    /// Find the most recent snapshot marker in a specific WAL file
    fn find_snapshot_marker_in_file(
        &self,
        wal_path: &PathBuf,
    ) -> Result<Option<(usize, qlib_rs::Request)>> {
        let mut file = File::open(wal_path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        let mut reader = WalEntryReader::new(buffer);
        let mut last_snapshot: Option<(usize, qlib_rs::Request)> = None;

        while let Some(result) = reader.next_entry() {
            match result {
                Ok((entry_data, offset)) => {
                    match bincode::deserialize::<qlib_rs::Request>(&entry_data) {
                        Ok(request) => match &request {
                            qlib_rs::Request::Snapshot { .. } => {
                                last_snapshot = Some((offset, request));
                            }
                            _ => {}
                        },
                        Err(_) => continue,
                    }
                }
                Err(_) => break,
            }
        }

        Ok(last_snapshot)
    }

    /// Perform the actual replay operation
    fn perform_replay(
        &self,
        wal_files: &[FileInfo],
        most_recent_snapshot: Option<(PathBuf, u64, usize)>,
    ) -> Result<Requests> {
        let requests = Requests::new(vec![]);

        match most_recent_snapshot {
            Some((snapshot_file, snapshot_counter, offset)) => {
                info!(
                    snapshot_file = %snapshot_file.display(),
                    snapshot_counter = snapshot_counter,
                    offset = offset,
                    "Found snapshot marker, starting replay from offset"
                );

                // Find the file in our list and replay from the offset
                if let Some(file_info) = wal_files.iter().find(|f| f.path == snapshot_file) {
                    let file_requests = self.replay_file_from_offset(&file_info.path, offset)?;
                    requests.extend(file_requests);

                    // Replay all subsequent files completely
                    for file_info in wal_files.iter().filter(|f| f.counter > file_info.counter) {
                        let file_requests = self.replay_file_from_offset(&file_info.path, 0)?;
                        requests.extend(file_requests);
                    }
                }
            }
            None => {
                info!("No snapshot marker found, replaying all WAL files");

                // Replay all files from the beginning
                for file_info in wal_files {
                    let file_requests = self.replay_file_from_offset(&file_info.path, 0)?;
                    requests.extend(file_requests);
                }
            }
        }

        info!(request_count = requests.len(), "WAL replay completed");
        Ok(requests)
    }

    /// Replay a single WAL file from a specific offset
    fn replay_file_from_offset(&self, wal_path: &PathBuf, start_offset: usize) -> Result<Requests> {
        let mut file = File::open(wal_path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        let adjusted_offset = self.validate_start_offset(&buffer, start_offset, wal_path)?;
        let mut reader = WalEntryReader::from_offset(buffer, adjusted_offset);
        let requests = Requests::new(vec![]);

        info!(
            wal_file = %wal_path.display(),
            start_offset = adjusted_offset,
            "Replaying WAL file"
        );

        while let Some(result) = reader.next_entry() {
            match result {
                Ok((entry_data, _)) => {
                    match self.apply_wal_entry(&entry_data) {
                        Ok(Some(request)) => {
                            requests.push(request);
                        }
                        Ok(None) => {
                            // Entry processed but no request to return (e.g., snapshot marker)
                        }
                        Err(e) => {
                            error!(error = %e, "Failed to apply WAL entry");
                        }
                    }
                }
                Err(e) => {
                    error!(
                        wal_file = %wal_path.display(),
                        error = %e,
                        "Failed to read WAL entry, stopping replay"
                    );
                    break;
                }
            }
        }

        debug!(
            wal_file = %wal_path.display(),
            request_count = requests.len(),
            "Completed WAL file replay"
        );

        Ok(requests)
    }

    /// Validate and adjust start offset to entry boundary
    fn validate_start_offset(
        &self,
        buffer: &[u8],
        start_offset: usize,
        wal_path: &PathBuf,
    ) -> Result<usize> {
        if start_offset == 0 {
            return Ok(0);
        }

        if start_offset >= buffer.len() {
            warn!(
                wal_file = %wal_path.display(),
                start_offset = start_offset,
                buffer_len = buffer.len(),
                "Start offset beyond buffer end, starting from beginning"
            );
            return Ok(0);
        }

        // Try to find a valid entry boundary near the offset
        // For simplicity, just use the provided offset (assuming it's correct)
        // In a production system, you'd want more sophisticated boundary detection
        let mut test_reader = WalEntryReader::from_offset(buffer.to_vec(), start_offset);
        match test_reader.next_entry() {
            Some(Ok(_)) => Ok(start_offset),
            _ => {
                warn!(
                    wal_file = %wal_path.display(),
                    start_offset = start_offset,
                    "Invalid start offset, starting from beginning"
                );
                Ok(0)
            }
        }
    }

    /// Apply a single WAL entry during replay
    fn apply_wal_entry(&self, entry_data: &[u8]) -> Result<Option<qlib_rs::Request>> {
        match bincode::deserialize::<qlib_rs::Request>(entry_data) {
            Ok(request) => {
                match &request {
                    qlib_rs::Request::Snapshot { .. } => {
                        debug!("Found snapshot marker in WAL");
                        Ok(None) // Don't return snapshot markers as requests to apply
                    }
                    _ => Ok(Some(request)),
                }
            }
            Err(e) => Err(anyhow::anyhow!("Failed to deserialize WAL entry: {}", e)),
        }
    }
}
