use qlib_rs::{et, ft, notification_channel, now, schoice, sread, sref, swrite, AsyncStore, AuthConfig, AuthenticationResult, Cache, CelExecutor, EntityId, NotificationSender, NotifyConfig, PushCondition, Snowflake, StoreMessage, StoreTrait};
use qlib_rs::auth::{authenticate_subject, AuthorizationScope, get_scope};
use tokio::signal;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, connect_async, tungstenite::Message};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, Mutex, MutexGuard};
use tracing::{info, warn, error, debug, instrument};
use clap::Parser;
use anyhow::Result;
use std::collections::{HashSet, HashMap};
use std::sync::Arc;
use std::vec;
use std::time::Duration;
use tokio::fs::{File, OpenOptions, create_dir_all, read_dir, remove_file};
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use std::path::PathBuf;
use serde::{Serialize, Deserialize};
use time;

/// Helper struct for managing numbered files (WAL files, snapshots, etc.)
struct FileManager;

impl FileManager {
    /// Scan directory for files matching prefix and suffix, returning sorted paths with counters
    async fn scan_files(&self, dir: &PathBuf, prefix: &str, suffix: &str) -> Result<Vec<(PathBuf, u64)>> {
        if !dir.exists() {
            return Ok(Vec::new());
        }
        
        let mut entries = read_dir(dir).await?;
        let mut files = Vec::new();
        
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if let Some(filename) = path.file_name() {
                if let Some(filename_str) = filename.to_str() {
                    if filename_str.starts_with(prefix) && filename_str.ends_with(suffix) {
                        // Extract counter from filename
                        if let Some(counter_str) = filename_str.strip_prefix(prefix).and_then(|s| s.strip_suffix(suffix)) {
                            if let Ok(counter) = counter_str.parse::<u64>() {
                                files.push((path, counter));
                            }
                        } else {
                            // Files without counter (for compatibility)
                            files.push((path, 0));
                        }
                    }
                }
            }
        }
        
        files.sort_by_key(|(_, counter)| *counter);
        Ok(files)
    }

    /// Get the next counter value for numbered files
    async fn get_next_counter(&self, dir: &PathBuf, prefix: &str, suffix: &str) -> Result<u64> {
        let files = self.scan_files(dir, prefix, suffix).await?;
        let max_counter = files.iter().map(|(_, counter)| *counter).max().unwrap_or(0);
        Ok(if max_counter == 0 && files.is_empty() { 0 } else { max_counter + 1 })
    }

    /// Clean up old files, keeping only the most recent max_files
    async fn cleanup_old_files(&self, dir: &PathBuf, prefix: &str, suffix: &str, max_files: usize) -> Result<()> {
        let files = self.scan_files(dir, prefix, suffix).await?;
        
        if files.len() > max_files {
            let files_to_remove = files.len() - max_files;
            for (file_path, _) in &files[0..files_to_remove] {
                info!(file = %file_path.display(), "Removing old file");
                if let Err(e) = remove_file(file_path).await {
                    error!(file = %file_path.display(), error = %e, "Failed to remove old file");
                }
            }
        }
        
        Ok(())
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
    
    fn from_offset(buffer: Vec<u8>, start_offset: usize) -> Self {
        Self { buffer, offset: start_offset }
    }
}

impl Iterator for WalEntryReader {
    type Item = Result<(Vec<u8>, usize)>; // (entry_data, next_offset)
    
    fn next(&mut self) -> Option<Self::Item> {
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

/// WAL manager handles WAL file operations
struct WalManager {
    file_manager: FileManager,
}

impl WalManager {
    fn new() -> Self {
        Self {
            file_manager: FileManager,
        }
    }
    
    /// Write a request to WAL with file rotation and snapshot handling
    async fn write_request(&self, request: &qlib_rs::Request, locks: &mut AppStateLocks<'_>, direct_mode: bool) -> Result<()> {
        let serialized = serde_json::to_vec(request)?;
        let serialized_len = serialized.len();
        
        // Check if we need to create a new WAL file
        let should_create_new_file = locks.wal_state().current_wal_file.is_none() || 
           (!direct_mode && locks.wal_state().current_wal_size + serialized_len > locks.core_state().config.wal_max_file_size * 1024 * 1024);

        if should_create_new_file {
            self.rotate_file(locks, direct_mode).await?;
        }
        
        // Write the actual data
        self.write_entry(&serialized, locks).await?;
        
        Ok(())
    }
    
    /// Rotate the WAL file and handle snapshots if needed
    async fn rotate_file(&self, locks: &mut AppStateLocks<'_>, direct_mode: bool) -> Result<()> {
        let wal_dir = locks.core_state().get_wal_dir();
        create_dir_all(&wal_dir).await?;
        
        let wal_filename = format!("wal_{:010}.log", locks.wal_state().wal_file_counter);
        let wal_path = wal_dir.join(&wal_filename);

        let wal_counter = locks.wal_state().wal_file_counter;
        let current_size = locks.wal_state().current_wal_size;
        let max_size = locks.core_state().config.wal_max_file_size * 1024 * 1024;

        info!(
            wal_file = %wal_path.display(),
            wal_counter = wal_counter,
            current_size = current_size,
            max_size = max_size,
            direct_mode = direct_mode,
            "Creating new WAL file"
        );
        
        let file = OpenOptions::new().create(true).append(true).open(&wal_path).await?;
        locks.wal_state().current_wal_file = Some(file);
        locks.wal_state().current_wal_size = 0;
        locks.wal_state().wal_file_counter += 1;
        
        if !direct_mode {
            locks.wal_state().wal_files_since_snapshot += 1;
            self.handle_snapshot_if_needed(locks).await?;
            self.cleanup_old_wal_files(locks).await?;
        }
        
        Ok(())
    }
    
    /// Handle snapshot creation if the interval is reached
    async fn handle_snapshot_if_needed(&self, locks: &mut AppStateLocks<'_>) -> Result<()> {
        if locks.wal_state().wal_files_since_snapshot >= locks.core_state().config.snapshot_wal_interval {
            info!(wal_files_count = locks.wal_state().wal_files_since_snapshot, "Taking snapshot after WAL file rollovers");
            
            // Store current state for potential rollback
            let original_wal_files_since_snapshot = locks.wal_state().wal_files_since_snapshot;
            let original_snapshot_counter = locks.wal_state().snapshot_file_counter;
            
            // Defensive: Wrap snapshot creation in error handling
            let snapshot_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                locks.store().inner().take_snapshot()
            }));
            
            match snapshot_result {
                Ok(snapshot) => {
                    let snapshot_manager = SnapshotManager::new();
                    match snapshot_manager.save(&snapshot, locks).await {
                        Ok(snapshot_counter) => {
                            locks.wal_state().wal_files_since_snapshot = 0;
                            info!("Snapshot saved successfully after WAL rollover");

                            let snapshot_request = qlib_rs::Request::Snapshot {
                                snapshot_counter,
                                timestamp: Some(now()),
                                originator: Some(locks.core_state().config.machine.clone()),
                            };
                            
                            if let Err(e) = Box::pin(self.write_request(&snapshot_request, locks, true)).await {
                                error!(error = %e, "Failed to write snapshot marker to WAL");
                            }
                        }
                        Err(e) => {
                            error!(error = %e, "Failed to save snapshot after WAL rollover");
                            locks.wal_state().snapshot_file_counter = original_snapshot_counter;
                            locks.wal_state().wal_files_since_snapshot = original_wal_files_since_snapshot;
                        }
                    }
                }
                Err(panic_info) => {
                    error!(
                        panic_info = ?panic_info,
                        "Snapshot creation panicked - continuing with WAL operations"
                    );
                    locks.wal_state().wal_files_since_snapshot = original_wal_files_since_snapshot.saturating_sub(1);
                }
            }
        }
        
        Ok(())
    }
    
    /// Clean up old WAL files
    async fn cleanup_old_wal_files(&self, locks: &mut AppStateLocks<'_>) -> Result<()> {
        let wal_dir = locks.core_state().get_wal_dir();
        if let Err(e) = self.file_manager.cleanup_old_files(&wal_dir, "wal_", ".log", locks.core_state().config.wal_max_files).await {
            error!(error = %e, "Failed to clean up old WAL files");
        }
        Ok(())
    }
    
    /// Write an entry to the current WAL file
    async fn write_entry(&self, serialized: &[u8], locks: &mut AppStateLocks<'_>) -> Result<()> {
        if let Some(ref mut wal_file) = locks.wal_state().current_wal_file {
            let len_bytes = (serialized.len() as u32).to_le_bytes();
            wal_file.write_all(&len_bytes).await?;
            wal_file.write_all(serialized).await?;
            wal_file.flush().await?;
            locks.wal_state().current_wal_size += 4 + serialized.len();
        }
        Ok(())
    }
    
    /// Replay WAL files to restore store state
    async fn replay(&self, locks: &mut AppStateLocks<'_>) -> Result<()> {
        let wal_dir = locks.core_state().get_wal_dir();
        let wal_files = self.file_manager.scan_files(&wal_dir, "wal_", ".log").await?;
        
        if wal_files.is_empty() {
            info!("No WAL files found, no replay needed");
            return Ok(());
        }
        
        let most_recent_snapshot = self.find_latest_snapshot_marker(&wal_files).await?;
        
        locks.store().inner_mut().disable_notifications();
        info!("Notifications disabled for WAL replay");
        
        // Defensive: Comprehensive error handling for WAL replay with proper cleanup
        let replay_result = self.perform_replay(locks, &wal_files, most_recent_snapshot).await;
        
        // Defensive: Always re-enable notifications regardless of replay outcome
        if let Err(e) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            locks.store().inner_mut().enable_notifications();
        })) {
            error!(panic_info = ?e, "Failed to re-enable notifications after WAL replay");
            return Err(anyhow::anyhow!("Critical failure: cannot re-enable notifications"));
        }
        
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
    
    /// Find the most recent snapshot marker across all WAL files
    async fn find_latest_snapshot_marker(&self, wal_files: &[(PathBuf, u64)]) -> Result<Option<(PathBuf, u64, usize)>> {
        let mut most_recent_snapshot: Option<(PathBuf, u64, usize)> = None;
        
        info!(wal_files_count = wal_files.len(), "Scanning WAL files to find the most recent snapshot marker");
        
        for (wal_file, counter) in wal_files {
            if let Ok(snapshot_info) = self.find_snapshot_marker_in_file(wal_file).await {
                if let Some((offset_after_snapshot, _)) = snapshot_info {
                    if most_recent_snapshot.is_none() || counter > &most_recent_snapshot.as_ref().unwrap().1 {
                        most_recent_snapshot = Some((wal_file.clone(), *counter, offset_after_snapshot));
                    }
                }
            }
        }
        
        Ok(most_recent_snapshot)
    }
    
    /// Find the most recent snapshot marker in a specific WAL file
    async fn find_snapshot_marker_in_file(&self, wal_path: &PathBuf) -> Result<Option<(usize, qlib_rs::Request)>> {
        let mut file = File::open(wal_path).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;
        
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
    async fn perform_replay(&self, locks: &mut AppStateLocks<'_>, wal_files: &[(PathBuf, u64)], most_recent_snapshot: Option<(PathBuf, u64, usize)>) -> Result<()> {
        match most_recent_snapshot {
            Some((snapshot_wal_file, snapshot_counter, snapshot_offset)) => {
                info!(
                    wal_file = %snapshot_wal_file.display(),
                    wal_counter = snapshot_counter,
                    offset = snapshot_offset,
                    "Starting replay from WAL file with snapshot marker"
                );
                
                // Replay the partial WAL file from the snapshot offset
                self.replay_file_from_offset(locks, &snapshot_wal_file, snapshot_offset).await?;
                
                // Replay all subsequent WAL files completely
                for (wal_file, counter) in wal_files {
                    if counter > &snapshot_counter {
                        info!(wal_file = %wal_file.display(), wal_counter = counter, "Replaying complete WAL file");
                        if let Err(e) = self.replay_file_from_offset(locks, wal_file, 0).await {
                            error!(wal_file = %wal_file.display(), error = %e, "Failed to replay WAL file");
                        }
                    }
                }
            }
            None => {
                info!(wal_files_count = wal_files.len(), "No snapshot markers found, replaying all files completely");
                
                for (wal_file, counter) in wal_files {
                    info!(wal_file = %wal_file.display(), wal_counter = counter, "Replaying WAL file");
                    if let Err(e) = self.replay_file_from_offset(locks, wal_file, 0).await {
                        error!(wal_file = %wal_file.display(), error = %e, "Failed to replay WAL file");
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Replay a single WAL file from a specific offset
    async fn replay_file_from_offset(&self, locks: &mut AppStateLocks<'_>, wal_path: &PathBuf, start_offset: usize) -> Result<()> {
        let mut file = File::open(wal_path).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;
        
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
                    match self.apply_wal_entry(locks, &entry_data).await {
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
    async fn apply_wal_entry(&self, locks: &mut AppStateLocks<'_>, entry_data: &[u8]) -> Result<bool> {
        match serde_json::from_slice::<qlib_rs::Request>(entry_data) {
            Ok(request) => {
                // Skip snapshot requests during replay (they are just markers)
                if matches!(request, qlib_rs::Request::Snapshot { .. }) {
                    debug!("Skipping snapshot marker during replay");
                    return Ok(false); // Processed but not counted
                }

                let mut requests = vec![request];
                if let Err(e) = locks.store().perform_mut(&mut requests).await {
                    return Err(anyhow::anyhow!("Failed to apply request during WAL replay: {}", e));
                }
                
                Ok(true) // Successfully processed and should be counted
            }
            Err(e) => {
                Err(anyhow::anyhow!("Failed to deserialize request: {}", e))
            }
        }
    }
    
    /// Initialize WAL counter from existing files
    async fn initialize_counter(&self, locks: &mut AppStateLocks<'_>) -> Result<()> {
        let wal_dir = locks.core_state().get_wal_dir();
        let next_wal_counter = self.file_manager.get_next_counter(&wal_dir, "wal_", ".log").await?;
        locks.wal_state().wal_file_counter = next_wal_counter;
        info!(
            wal_dir = %wal_dir.display(),
            next_counter = next_wal_counter,
            "Initialized WAL file counter"
        );
        Ok(())
    }
}

/// Snapshot manager handles snapshot operations
struct SnapshotManager {
    file_manager: FileManager,
}

impl SnapshotManager {
    fn new() -> Self {
        Self {
            file_manager: FileManager,
        }
    }
    
    /// Save a snapshot to disk and return the snapshot counter
    #[instrument(skip(self, snapshot, locks))]
    async fn save(&self, snapshot: &qlib_rs::Snapshot, locks: &mut AppStateLocks<'_>) -> Result<u64> {
        let snapshot_dir = locks.core_state().get_snapshots_dir();
        create_dir_all(&snapshot_dir).await?;
        
        let current_snapshot_counter = locks.wal_state().snapshot_file_counter;
        locks.wal_state().snapshot_file_counter += 1;

        let snapshot_filename = format!("snapshot_{:010}.bin", current_snapshot_counter);
        let snapshot_path = snapshot_dir.join(&snapshot_filename);
        
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
        self.cleanup_old_snapshots(locks).await?;
        
        Ok(current_snapshot_counter)
    }
    
    /// Load the latest snapshot from disk
    async fn load_latest(&self, locks: &mut AppStateLocks<'_>) -> Result<Option<(qlib_rs::Snapshot, u64)>> {
        let snapshot_dir = locks.core_state().get_snapshots_dir();
        let snapshot_files = self.file_manager.scan_files(&snapshot_dir, "snapshot_", ".bin").await?;
        
        if snapshot_files.is_empty() {
            info!("No snapshot files found, starting with empty store");
            return Ok(None);
        }
        
        // Try loading snapshots from latest to oldest
        for (snapshot_path, counter) in snapshot_files.iter().rev() {
            info!(
                snapshot_file = %snapshot_path.display(),
                snapshot_counter = counter,
                "Attempting to load snapshot"
            );
            
            match self.try_load_snapshot(snapshot_path).await {
                Ok(Some(snapshot)) => {
                    info!(
                        snapshot_file = %snapshot_path.display(),
                        snapshot_counter = counter,
                        "Snapshot loaded successfully"
                    );
                    return Ok(Some((snapshot, *counter)));
                }
                Ok(None) => {
                    // File was corrupted and cleaned up, try next
                    continue;
                }
                Err(e) => {
                    error!(
                        error = %e,
                        snapshot_file = %snapshot_path.display(),
                        "Failed to load snapshot, trying previous snapshot"
                    );
                    continue;
                }
            }
        }
        
        // Defensive: If all snapshots failed to load, start with empty store
        warn!("All snapshot files failed to load or were corrupted, starting with empty store");
        Ok(None)
    }
    
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
    
    /// Clean up old snapshot files
    async fn cleanup_old_snapshots(&self, locks: &AppStateLocks<'_>) -> Result<()> {
        let snapshot_dir = locks.core_state.as_ref().unwrap().get_snapshots_dir();
        let max_files = locks.core_state.as_ref().unwrap().config.snapshot_max_files;
        if let Err(e) = self.file_manager.cleanup_old_files(&snapshot_dir, "snapshot_", ".bin", max_files).await {
            error!(error = %e, "Failed to clean up old snapshots");
        }
        Ok(())
    }
    
    /// Initialize snapshot counter from existing files
    async fn initialize_counter(&self, locks: &mut AppStateLocks<'_>) -> Result<()> {
        let snapshot_dir = locks.core_state().get_snapshots_dir();
        let next_snapshot_counter = self.file_manager.get_next_counter(&snapshot_dir, "snapshot_", ".bin").await?;
        locks.wal_state().snapshot_file_counter = next_snapshot_counter;
        info!(
            snapshot_dir = %snapshot_dir.display(),
            next_counter = next_snapshot_counter,
            "Initialized snapshot file counter"
        );
        Ok(())
    }
}

/// Application availability state
#[derive(Debug, Clone, PartialEq)]
enum AvailabilityState {
    /// Application is unavailable - attempting to sync with leader, clients are force disconnected
    Unavailable,
    /// Application is available - clients are allowed to connect and perform operations
    Available,
}

/// Helper structure for extracting data without holding locks
#[derive(Debug, Clone)]
struct StateSnapshot {
    config: Config,
    startup_time: u64,
    availability_state: AvailabilityState,
}

impl StateSnapshot {
    fn is_available(&self) -> bool {
        matches!(self.availability_state, AvailabilityState::Available)
    }
}

/// Connection state separated from main state to reduce lock contention
#[derive(Debug)]
struct ConnectionState {
    /// Connected outbound peers with message senders
    connected_outbound_peers: HashMap<String, mpsc::UnboundedSender<Message>>,
    /// Connected clients with message senders
    connected_clients: HashMap<String, mpsc::UnboundedSender<Message>>,
    /// Client notification senders
    client_notification_senders: HashMap<String, NotificationSender>,
    /// Track notification configurations per client for cleanup on disconnect
    client_notification_configs: HashMap<String, HashSet<NotifyConfig>>,
    /// Track authenticated clients
    authenticated_clients: HashMap<String, EntityId>,
}

impl ConnectionState {
    fn new() -> Self {
        Self {
            connected_outbound_peers: HashMap::new(),
            connected_clients: HashMap::new(),
            client_notification_senders: HashMap::new(),
            client_notification_configs: HashMap::new(),
            authenticated_clients: HashMap::new(),
        }
    }

    /// Force disconnect all connected clients (used when transitioning to unavailable)
    fn force_disconnect_all_clients(&mut self) {
        if self.connected_clients.is_empty() {
            return;
        }

        let client_count = self.connected_clients.len();
        info!(
            client_count = client_count,
            "Force disconnecting clients due to unavailable state"
        );
        
        // Send close messages to all connected clients
        let disconnect_message = Message::Close(None);
        for (client_addr, sender) in &self.connected_clients {
            if let Err(e) = sender.send(disconnect_message.clone()) {
                warn!(
                    client_addr = %client_addr,
                    error = %e,
                    "Failed to send close message to client"
                );
            }
        }
        
        // Clear all client-related data structures
        self.connected_clients.clear();
    }
}

/// WAL state separated to avoid lock contention
#[derive(Debug)]
struct WalState {
    /// Current WAL file handle
    current_wal_file: Option<File>,
    /// Current WAL file size in bytes
    current_wal_size: usize,
    /// WAL file counter for generating unique filenames
    wal_file_counter: u64,
    /// Snapshot file counter for generating unique filenames
    snapshot_file_counter: u64,
    /// Number of WAL files created since last snapshot
    wal_files_since_snapshot: u64,
}

impl WalState {
    fn new() -> Self {
        Self {
            current_wal_file: None,
            current_wal_size: 0,
            wal_file_counter: 0,
            snapshot_file_counter: 0,
            wal_files_since_snapshot: 0,
        }
    }
}

/// Messages exchanged between peers for leader election
#[derive(Serialize, Deserialize, Debug, Clone)]
enum PeerMessage {
    /// Startup message announcing startup time and machine ID
    Startup {
        machine_id: String,
        startup_time: u64, // Timestamp in seconds since UNIX_EPOCH
    },
    /// Request for full synchronization from the leader
    FullSyncRequest {
        machine_id: String,
    },
    /// Response containing a complete snapshot for full synchronization
    FullSyncResponse {
        snapshot: qlib_rs::Snapshot,
    },
    /// Data synchronization request (existing functionality)
    SyncRequest {
        requests: Vec<qlib_rs::Request>,
    },
}

/// Configuration passed via CLI arguments
#[derive(Parser, Clone, Debug)]
#[command(name = "core-service", about = "QOS Core Service runtime datastore")]
struct Config {
    /// Machine ID (unique identifier for this instance)
    #[arg(long)]
    machine: String,

    /// Data directory for storing WAL files and other persistent data
    #[arg(long, default_value = "./data")]
    data_dir: String,

    /// Maximum WAL file size in MB
    #[arg(long, default_value_t = 1)]
    wal_max_file_size: usize,

    /// Maximum number of WAL files to keep
    #[arg(long, default_value_t = 30)]
    wal_max_files: usize,

    /// Number of WAL file rollovers before taking a snapshot
    #[arg(long, default_value_t = 3)]
    snapshot_wal_interval: u64,

    /// Maximum number of snapshot files to keep
    #[arg(long, default_value_t = 5)]
    snapshot_max_files: usize,

    /// Port for peer-to-peer communication
    #[arg(long, default_value_t = 9000)]
    peer_port: u16,

    /// Port for client communication (StoreProxy clients)
    #[arg(long, default_value_t = 9100)]
    client_port: u16,

    /// List of peer addresses to connect to (format: host:port)
    #[arg(long, value_delimiter = ',')]
    peer_addresses: Vec<String>,

    /// Interval in seconds to retry connecting to peers
    #[arg(long, default_value_t = 3)]
    peer_reconnect_interval_secs: u64,

    /// Grace period in seconds to wait after becoming unavailable before requesting full sync
    #[arg(long, default_value_t = 5)]
    full_sync_grace_period_secs: u64,

    /// Delay in seconds after startup before self-promoting to leader when no peers are available
    #[arg(long, default_value_t = 5)]
    self_promotion_delay_secs: u64,
}

/// Application state that is shared across all tasks
#[derive(Debug)]
struct AppState {
    /// Core configuration and leadership state - should be accessed minimally
    core_state: Mutex<CoreState>,
    
    /// Connection-related state - separate lock to reduce contention
    connections: Mutex<ConnectionState>,
    
    /// WAL file state - separate lock for file operations
    wal_state: Mutex<WalState>,
    
    /// Peer information tracking
    peer_info: Mutex<HashMap<String, PeerInfo>>,
    
    /// Data store - kept separate as it has its own locking
    store: Arc<Mutex<AsyncStore>>,
    
    /// Permission Cache - separate as it's accessed frequently
    permission_cache: Mutex<Option<Cache>>,
    
    /// CEL Executor for evaluating authorization conditions
    cel_executor: Mutex<CelExecutor>,
}

/// Core application state
#[derive(Debug)]
struct CoreState {
    /// Configuration
    config: Config,
    
    /// Startup time (timestamp in seconds since UNIX_EPOCH)
    startup_time: u64,
    
    /// Current availability state of the application
    availability_state: AvailabilityState,
    
    /// Whether this instance has been elected as leader
    is_leader: bool,
    
    /// The machine ID of the current leader (if known)
    current_leader: Option<String>,
    
    /// Whether this instance has completed full sync with the leader
    is_fully_synced: bool,
    
    /// Timestamp when we became unavailable (for grace period tracking)
    became_unavailable_at: Option<u64>,

    /// Whether a full sync request is pending (to avoid sending multiple)
    full_sync_request_pending: bool,
}

impl CoreState {
    /// Get a snapshot of the core state without holding locks
    fn get_state_snapshot(&self) -> StateSnapshot {
        StateSnapshot {
            config: self.config.clone(),
            startup_time: self.startup_time,
            availability_state: self.availability_state.clone(),
        }
    }
    
    /// Get the machine-specific data directory
    fn get_machine_data_dir(&self) -> PathBuf {
        PathBuf::from(&self.config.data_dir).join(&self.config.machine)
    }

    /// Get the machine-specific WAL directory
    fn get_wal_dir(&self) -> PathBuf {
        let machine_data_dir = self.get_machine_data_dir();
        machine_data_dir.join("wal")
    }

    /// Get the machine-specific snapshots directory
    fn get_snapshots_dir(&self) -> PathBuf {
        let machine_data_dir = self.get_machine_data_dir();
        machine_data_dir.join("snapshots")
    }
}

/// Information about a peer instance
#[derive(Debug, Clone)]
struct PeerInfo {
    machine_id: String,
    startup_time: u64
}

/// Specifies which AppState locks to acquire
#[derive(Default)]
struct LockRequest {
    pub core_state: bool,
    pub connections: bool,
    pub wal_state: bool,
    pub peer_info: bool,
    pub store: bool,
    pub permission_cache: bool,
    pub cel_executor: bool,
}

/// Contains the acquired locks in the proper order
struct AppStateLocks<'a> {
    pub core_state: Option<MutexGuard<'a, CoreState>>,
    pub connections: Option<MutexGuard<'a, ConnectionState>>,
    pub wal_state: Option<MutexGuard<'a, WalState>>,
    pub peer_info: Option<MutexGuard<'a, HashMap<String, PeerInfo>>>,
    pub store: Option<MutexGuard<'a, AsyncStore>>,
    pub permission_cache: Option<MutexGuard<'a, Option<Cache>>>,
    pub cel_executor: Option<MutexGuard<'a, CelExecutor>>,
}



impl<'a> AppStateLocks<'a> {
    /// Get the core_state lock, panicking if it wasn't requested
    pub fn core_state(&mut self) -> &mut MutexGuard<'a, CoreState> {
        self.core_state.as_mut().expect("core_state lock was not requested")
    }

    /// Get the connections lock, panicking if it wasn't requested
    pub fn connections(&mut self) -> &mut MutexGuard<'a, ConnectionState> {
        self.connections.as_mut().expect("connections lock was not requested")
    }

    /// Get the wal_state lock, panicking if it wasn't requested
    pub fn wal_state(&mut self) -> &mut MutexGuard<'a, WalState> {
        self.wal_state.as_mut().expect("wal_state lock was not requested")
    }

    /// Get the peer_info lock, panicking if it wasn't requested
    pub fn peer_info(&mut self) -> &mut MutexGuard<'a, HashMap<String, PeerInfo>> {
        self.peer_info.as_mut().expect("peer_info lock was not requested")
    }

    /// Get the store lock, panicking if it wasn't requested
    pub fn store(&mut self) -> &mut MutexGuard<'a, AsyncStore> {
        self.store.as_mut().expect("store lock was not requested")
    }

    /// Get the permission_cache lock, panicking if it wasn't requested
    pub fn permission_cache(&mut self) -> &mut MutexGuard<'a, Option<Cache>> {
        self.permission_cache.as_mut().expect("permission_cache lock was not requested")
    }
}

impl AppState {
    async fn new(config: Config) -> Result<Self> {
        let startup_time = time::OffsetDateTime::now_utc().unix_timestamp() as u64;
        let store = Arc::new(Mutex::new(AsyncStore::new(Arc::new(Snowflake::new()))));
        
        Ok(Self {
            core_state: Mutex::new(CoreState {
                config,
                startup_time,
                availability_state: AvailabilityState::Available,
                is_leader: false,
                current_leader: None,
                is_fully_synced: false,
                became_unavailable_at: None,
                full_sync_request_pending: false,
            }),
            connections: Mutex::new(ConnectionState::new()),
            wal_state: Mutex::new(WalState::new()),
            peer_info: Mutex::new(HashMap::new()),
            store,
            permission_cache: Mutex::new(None),
            cel_executor: Mutex::new(CelExecutor::new()),
        })
    }
    
    /// Retrigger leader election when the current leader has disconnected
    async fn retrigger_leader_election(&self, locks: &mut AppStateLocks<'_>) {
        let our_startup_time = locks.core_state().startup_time;
        let our_machine_id = locks.core_state().config.machine.clone();

        // Find the earliest startup time among all known peers including ourselves
        let (should_be_leader, earliest_startup) = {
            let earliest_startup = locks.peer_info().values()
                .map(|p| p.startup_time)
                .min()
                .unwrap_or(our_startup_time)
                .min(our_startup_time);
            
            let mut should_be_leader = our_startup_time <= earliest_startup;
            
            // Handle startup time ties
            if our_startup_time == earliest_startup {
                let peers_with_same_time: Vec<_> = locks.peer_info().values()
                    .filter(|p| p.startup_time == our_startup_time)
                    .collect();
                
                if !peers_with_same_time.is_empty() {
                    // We have a tie, use machine_id as tiebreaker
                    info!(
                        peer_count = peers_with_same_time.len(),
                        startup_time = our_startup_time,
                        "Startup time tie detected during leader re-election, using machine_id as tiebreaker"
                    );
                    
                    let mut all_machine_ids = peers_with_same_time.iter()
                        .map(|p| p.machine_id.as_str())
                        .collect::<Vec<_>>();
                    all_machine_ids.push(our_machine_id.as_str());
                    
                    let min_machine_id = all_machine_ids.iter().min().unwrap();
                    should_be_leader = **min_machine_id == our_machine_id;
                }
            }
            
            (should_be_leader, earliest_startup)
        };
        
        // Update leadership status
        if should_be_leader {
            let was_leader = locks.core_state().is_leader;
            locks.core_state().is_leader = true;
            locks.core_state().current_leader = Some(our_machine_id.clone());
            locks.core_state().is_fully_synced = true;
            let new_state = AvailabilityState::Available;
            let old_state = locks.core_state().availability_state.clone();
            locks.core_state().availability_state = new_state.clone();

            // Clear the unavailable timestamp when becoming available
            if old_state != new_state {
                locks.core_state().became_unavailable_at = None;
            }
            
            if !was_leader {
                info!(
                    our_startup_time = our_startup_time,
                    earliest_startup = earliest_startup,
                    "Re-elected as leader after current leader disconnected"
                );
            }
        } else {
            // Find who should be the leader
            let new_leader = {
                locks.peer_info().values()
                    .filter(|p| p.startup_time <= earliest_startup)
                    .min_by(|a, b| {
                        a.startup_time.cmp(&b.startup_time)
                            .then_with(|| a.machine_id.cmp(&b.machine_id))
                    })
                    .map(|p| p.machine_id.clone())
            };

            locks.core_state().is_leader = false;
            locks.core_state().current_leader = new_leader.clone();
            let new_state = AvailabilityState::Unavailable;
            let old_state = locks.core_state().availability_state.clone();
            locks.core_state().availability_state = new_state.clone();

            // Set unavailable timestamp if transitioning to unavailable
            if old_state != new_state {
                locks.core_state().became_unavailable_at = Some(time::OffsetDateTime::now_utc().unix_timestamp() as u64);
                locks.connections().force_disconnect_all_clients();
            }
            
            info!(
                our_startup_time = our_startup_time,
                earliest_startup = earliest_startup,
                new_leader = ?new_leader,
                "Updated leadership after current leader disconnected"
            );
        }
    }
    
    /// Acquire multiple AppState locks in a consistent order to prevent deadlocks.
    /// 
    /// This function ensures that locks are always acquired in the same order:
    /// 1. core_state
    /// 2. connections  
    /// 3. wal_state
    /// 4. peer_info
    /// 5. store
    /// 6. permission_cache
    /// 7. cel_executor
    /// 
    /// Only the locks specified in the request will be acquired.
    async fn acquire_locks(&self, request: LockRequest) -> AppStateLocks<'_> {
        AppStateLocks {
            core_state: if request.core_state {
                Some(self.core_state.lock().await)
            } else {
                None
            },
            connections: if request.connections {
                Some(self.connections.lock().await)
            } else {
                None
            },
            wal_state: if request.wal_state {
                Some(self.wal_state.lock().await)
            } else {
                None
            },
            peer_info: if request.peer_info {
                Some(self.peer_info.lock().await)
            } else {
                None
            },
            store: if request.store {
                Some(self.store.lock().await)
            } else {
                None
            },
            permission_cache: if request.permission_cache {
                Some(self.permission_cache.lock().await)
            } else {
                None
            },
            cel_executor: if request.cel_executor {
                Some(self.cel_executor.lock().await)
            } else {
                None
            },
        }
    }
}

/// Handle a single peer WebSocket connection
#[instrument(skip(stream, app_state), fields(peer_addr = %peer_addr))]
async fn handle_inbound_peer_connection(stream: TcpStream, peer_addr: std::net::SocketAddr, app_state: Arc<AppState>) -> Result<()> {
    info!("Accepting inbound peer connection");
    
    let ws_stream = accept_async(stream).await?;
    debug!("WebSocket handshake completed");
    
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    
    // Handle incoming messages from peer
    while let Some(msg) = ws_receiver.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                debug!(message_length = text.len(), "Received text message from peer");
                
                // Try to parse as a PeerMessage first
                match serde_json::from_str::<PeerMessage>(&text) {
                    Ok(peer_msg) => {
                        debug!(message_type = ?std::mem::discriminant(&peer_msg), "Processing peer message");
                        let mut locks = app_state.acquire_locks(LockRequest {
                            core_state: true,
                            peer_info: true,
                            store: true,
                            connections: true,
                            permission_cache: true,
                            cel_executor: true,
                            ..Default::default()
                        }).await;
                        handle_peer_message(peer_msg, &peer_addr, &mut ws_sender, &mut locks).await;
                    }
                    Err(_) => {
                        debug!("Received non-peer text message from peer, ignoring")
                    }
                }
            }
            Ok(Message::Binary(data)) => {
                debug!(data_length = data.len(), "Received binary data from peer");
                
                // Try to parse as a PeerMessage (likely FullSyncResponse with binary serialization)
                match bincode::deserialize::<PeerMessage>(&data) {
                    Ok(peer_msg) => {
                        debug!(message_type = ?std::mem::discriminant(&peer_msg), "Processing binary peer message");
                        let mut locks = app_state.acquire_locks(LockRequest {
                            core_state: true,
                            peer_info: true,
                            store: true,
                            connections: true,
                            ..Default::default()
                        }).await;
                        handle_peer_message(peer_msg, &peer_addr, &mut ws_sender, &mut locks).await;
                    }
                    Err(e) => {
                        debug!(error = %e, "Failed to deserialize binary peer message, ignoring");
                    }
                }
            }
            Ok(Message::Ping(payload)) => {
                debug!("Received ping from peer");
                if let Err(e) = ws_sender.send(Message::Pong(payload)).await {
                    error!(error = %e, "Failed to send pong to peer");
                    break;
                }
            }
            Ok(Message::Pong(_)) => {
                debug!("Received pong from peer");
            }
            Ok(Message::Close(_)) => {
                info!("Peer closed connection gracefully");
                break;
            }
            Ok(Message::Frame(_)) => {
                // Handle raw frames if needed - typically not used directly
                debug!("Received raw frame from peer");
            }
            Err(e) => {
                error!(error = %e, "WebSocket error with peer");
                break;
            }
        }
    }

    let mut locks = app_state.acquire_locks(LockRequest {
        core_state: true,
        peer_info: true,
        connections: true,
        ..Default::default()
    }).await;
    
    // Remove peer from connected inbound peers when connection ends
    let disconnected_machine_id = {
        let peer_info = locks.peer_info();
        let disconnected_machine_id = peer_info.iter()
            .find(|(addr, _)| addr == &&peer_addr.to_string())
            .map(|(_, info)| info.machine_id.clone());
        
        peer_info.retain(|addr, _| {
            addr != &peer_addr.to_string()
        });
        
        disconnected_machine_id
    };
    
    // Check if the disconnected peer was the current leader and retrigger election
    if let Some(disconnected_machine_id) = disconnected_machine_id {
        let current_leader = locks.core_state().current_leader.clone();

        if let Some(leader_id) = current_leader {
            if leader_id == disconnected_machine_id {
                info!(
                    disconnected_machine = %disconnected_machine_id,
                    "Current leader disconnected, retriggering leader election"
                );
                app_state.retrigger_leader_election(&mut locks).await;
            }
        }
    }
    
    info!("Peer connection terminated");
    Ok(())
}

/// Handle a peer message and respond appropriately
#[instrument(skip(peer_msg, ws_sender, locks), fields(peer_addr = %peer_addr))]
async fn handle_peer_message(
    peer_msg: PeerMessage,
    peer_addr: &std::net::SocketAddr,
    ws_sender: &mut futures_util::stream::SplitSink<tokio_tungstenite::WebSocketStream<TcpStream>, Message>,
    locks: &mut AppStateLocks<'_>,
) {
    match peer_msg {
        PeerMessage::Startup { machine_id, startup_time } => {
            info!(
                remote_machine_id = %machine_id, 
                remote_startup_time = startup_time,
                "Processing startup message from peer"
            );
            
            // Update peer information
            {
                let peer_info = locks.peer_info();
                peer_info.insert(peer_addr.to_string(), PeerInfo {
                    machine_id: machine_id.clone(),
                    startup_time,
                });
            }
            
            // Get current state snapshot for leadership determination
            let state_snapshot = locks.core_state().get_state_snapshot();
            let our_startup_time = state_snapshot.startup_time;
            let our_machine_id = state_snapshot.config.machine.clone();
            
            // Find the earliest (largest) startup time among all known peers including ourselves
            let (should_be_leader, earliest_startup) = {
                let peer_info = locks.peer_info();
                let earliest_startup = peer_info.values()
                    .map(|p| p.startup_time)
                    .min()
                    .unwrap_or(our_startup_time)
                    .min(our_startup_time);
                
                let mut should_be_leader = our_startup_time <= earliest_startup;
                
                // Handle startup time ties
                if our_startup_time == earliest_startup {
                    let peers_with_same_time: Vec<_> = peer_info.values()
                        .filter(|p| p.startup_time == our_startup_time)
                        .collect();
                    
                    if !peers_with_same_time.is_empty() {
                        // We have a tie, use machine_id as tiebreaker
                        info!(
                            peer_count = peers_with_same_time.len(),
                            startup_time = our_startup_time,
                            "Startup time tie detected, using machine_id as tiebreaker"
                        );
                        
                        let mut all_machine_ids = peers_with_same_time.iter()
                            .map(|p| p.machine_id.as_str())
                            .collect::<Vec<_>>();
                        all_machine_ids.push(our_machine_id.as_str());
                        
                        let min_machine_id = all_machine_ids.iter().min().unwrap();
                        should_be_leader = **min_machine_id == our_machine_id;
                    }
                }

                (should_be_leader, earliest_startup)
            };
            
            // Update leadership status
            if should_be_leader {
                let core_state = locks.core_state();
                core_state.is_leader = true;
                core_state.current_leader = Some(our_machine_id.clone());
                core_state.is_fully_synced = true;
                let new_state = AvailabilityState::Available;
                let old_state = core_state.availability_state.clone();
                core_state.availability_state = new_state.clone();
                
                // Clear the unavailable timestamp when becoming available
                if old_state != new_state {
                    core_state.became_unavailable_at = None;
                }
                
                if old_state != new_state {
                    info!(
                        old_state = ?old_state,
                        new_state = ?new_state,
                        "Availability state transition"
                    );
                }
                info!(
                    our_startup_time = our_startup_time,
                    earliest_startup = earliest_startup,
                    "Elected as leader"
                );
            } else {
                let leader = {
                    // First get leader from peer info
                    let leader = {
                        let peer_info = locks.peer_info();
                        peer_info.values()
                            .filter(|p| p.startup_time <= earliest_startup)
                            .min_by_key(|p| (&p.startup_time, &p.machine_id))
                            .map(|p| p.machine_id.clone())
                    };
                    
                    // Then update core state
                    let core_state = locks.core_state();
                    core_state.is_leader = false;
                    let new_state = AvailabilityState::Unavailable;
                    let old_state = core_state.availability_state.clone();
                    core_state.availability_state = new_state.clone();
                    
                    // Reset sync status when stepping down from leader to ensure we request full sync
                    core_state.is_fully_synced = false;
                    core_state.full_sync_request_pending = false;
                    
                    // Set the timestamp when we became unavailable for grace period tracking
                    if old_state != new_state {
                        core_state.became_unavailable_at = Some(time::OffsetDateTime::now_utc().unix_timestamp() as u64);
                    }
                    
                    core_state.current_leader = leader.clone();
                                
                    if old_state != new_state {
                        info!(
                            old_state = ?old_state,
                            new_state = ?new_state,
                            "Availability state transition"
                        );
                    }

                    leader
                };

                locks.connections().force_disconnect_all_clients();
                info!(
                    leader = ?leader,
                    our_startup_time = our_startup_time,
                    earliest_startup = earliest_startup,
                    "Leader determined, stepping down"
                );
            }
            
            debug!(
                remote_machine_id = %machine_id,
                startup_time = startup_time,
                "Updated peer information"
            );
        }
        
        PeerMessage::FullSyncRequest { machine_id } => {
            // Any peer can respond to sync requests
            info!(requesting_machine = %machine_id, "Received full sync request, preparing snapshot");
            
            // Take a snapshot and send it
            let snapshot = {
                let store_guard = locks.store();
                store_guard.inner().take_snapshot()
            };
            
            info!(
                requesting_machine = %machine_id, 
                snapshot_entities = snapshot.entities.len(),
                "Snapshot prepared, attempting to serialize and send"
            );
            
            let response = PeerMessage::FullSyncResponse { snapshot };
            
            // Use binary serialization for the snapshot since it contains complex key types
            match bincode::serialize(&response) {
                Ok(response_binary) => {
                    info!(
                        requesting_machine = %machine_id,
                        response_size = response_binary.len(),
                        "Snapshot serialized to binary, sending response directly to requesting peer"
                    );
                    
                    // Send the response directly through the inbound connection
                    let message = Message::Binary(response_binary);
                    if let Err(e) = ws_sender.send(message).await {
                        error!(
                            error = %e,
                            requesting_machine = %machine_id,
                            "Failed to send FullSyncResponse to requesting peer"
                        );
                    } else {
                        info!(
                            requesting_machine = %machine_id,
                            "Successfully sent FullSyncResponse to requesting peer"
                        );
                    }
                }
                Err(e) => {
                    error!(
                        error = %e,
                        requesting_machine = %machine_id,
                        "Failed to serialize full sync response"
                    );
                }
            }
        }
        
        PeerMessage::FullSyncResponse { snapshot } => {
            // Apply the snapshot from the leader
            info!("Received full sync response, applying snapshot");
            
            // Apply the snapshot to the store
            {
                let store_guard = locks.store();
                store_guard.inner_mut().disable_notifications();
                store_guard.inner_mut().restore_snapshot(snapshot.clone());
                store_guard.inner_mut().enable_notifications();
            }
            
            // Update core state
            {
                let core_state = locks.core_state();
                core_state.is_fully_synced = true;
                core_state.full_sync_request_pending = false; // Reset pending flag since we got a response
                core_state.availability_state = AvailabilityState::Available;
                core_state.became_unavailable_at = None; // Clear timestamp when becoming available
            }
            
            // Save the snapshot to disk for persistence
            match save_snapshot(&snapshot, locks).await {
                Ok(snapshot_counter) => {
                    info!(
                        snapshot_counter = snapshot_counter,
                        "Snapshot saved to disk during full sync"
                    );
                    
                    // Write a snapshot marker to the WAL to indicate the sync point
                    // This helps during replay to know that the state was synced at this point
                    let machine_id = {
                        let core = locks.core_state();
                        core.config.machine.clone()
                    };
                    
                    let snapshot_request = qlib_rs::Request::Snapshot {
                        snapshot_counter,
                        timestamp: Some(now()),
                        originator: Some(machine_id),
                    };
                    
                    if let Err(e) = write_request_to_wal(&snapshot_request, locks, true).await {
                        error!(
                            error = %e,
                            snapshot_counter = snapshot_counter,
                            "Failed to write snapshot marker to WAL"
                        );
                    }

                    match reinit_caches(locks).await {
                        Ok(()) => {
                            info!("Caches reinitialized successfully");
                        }
                        Err(e) => {
                            error!(error = %e, "Failed to reinitialize caches");
                        }
                    }
                }
                Err(e) => {
                    error!(error = %e, "Failed to save snapshot during full sync");
                }
            }
            
            info!("Successfully applied full sync snapshot, instance is now fully synchronized");
        }
        
        PeerMessage::SyncRequest { requests } => {
            // Handle data synchronization (existing functionality)
            let our_machine_id = {
                let core = locks.core_state();
                core.config.machine.clone()
            };
            
            // Filter requests to only include those with valid originators (different from our machine)
            let mut requests_to_apply: Vec<_> = requests.into_iter()
                .filter(|request| {
                    if let Some(originator) = request.originator() {
                        *originator != our_machine_id
                    } else {
                        false
                    }
                })
                .filter(|request| {
                    match request {
                        qlib_rs::Request::Snapshot { .. } => false, // Ignore snapshot requests from peers
                        _ => true,
                    }
                })
                .collect();
            
            if !requests_to_apply.is_empty() {
                let store_guard = locks.store();
                if let Err(e) = store_guard.perform_mut(&mut requests_to_apply).await {
                    error!(
                        error = %e,
                        request_count = requests_to_apply.len(),
                        "Failed to apply sync requests from peer"
                    );
                } else {
                    debug!(
                        request_count = requests_to_apply.len(),
                        "Successfully applied sync requests from peer"
                    );
                }
            } else {
                debug!("No valid sync requests to apply from peer");
            }
        }
    }
}

/// Handle a single outbound peer WebSocket connection
#[instrument(skip(app_state), fields(peer_addr = %peer_addr))]
async fn handle_outbound_peer_connection(peer_addr: &str, app_state: Arc<AppState>) -> Result<()> {
    info!("Attempting to connect to outbound peer");
    
    let ws_url = format!("ws://{}", peer_addr);
    let (ws_stream, _response) = connect_async(&ws_url).await?;
    info!("Successfully connected to outbound peer");
    
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    
    // Create a channel for sending messages to this peer
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();
    
    // Store the sender in the connected_outbound_peers HashMap
    {
        let mut locks = app_state.acquire_locks(LockRequest {
            connections: true,
            ..Default::default()
        }).await;
        locks.connections().connected_outbound_peers.insert(peer_addr.to_string(), tx);
    }
    
    // Send initial startup message to announce ourselves
    let (machine, startup_time) = {
        let mut locks = app_state.acquire_locks(LockRequest {
            core_state: true,
            ..Default::default()
        }).await;
        let core = locks.core_state();
        (core.config.machine.clone(), core.startup_time)
    };

    let startup = PeerMessage::Startup {
        machine_id: machine.clone(),
        startup_time,
    };

    if let Ok(startup_json) = serde_json::to_string(&startup) {
        if let Err(e) = ws_sender.send(Message::Text(startup_json)).await {
            error!(
                error = %e,
                machine_id = %machine,
                startup_time = startup_time,
                "Failed to send initial startup message"
            );
        } else {
            debug!(
                machine_id = %machine,
                startup_time = startup_time,
                "Sent startup message to peer"
            );
        }
    }
    
    // Spawn a task to handle outgoing messages
    let peer_addr_clone = peer_addr.to_string();
    let outgoing_task = tokio::spawn(async move {
        while let Some(message) = rx.recv().await {
            if let Err(e) = ws_sender.send(message).await {
                error!(
                    error = %e,
                    peer_addr = %peer_addr_clone,
                    "Failed to send message to peer"
                );
                break;
            }
        }
    });
    
    // Handle incoming messages from peer (process FullSyncResponse, ignore others)
    while let Some(msg) = ws_receiver.next().await {
        match msg {
            Ok(Message::Text(_text)) => {
                // Ignore all text messages - message handling is done in handle_inbound_peer_connection
                debug!("Ignoring received text message from outbound peer (handled via inbound connection)");
            }
            Ok(Message::Binary(data)) => {
                // Try to deserialize as PeerMessage to check if it's a FullSyncResponse
                match bincode::deserialize::<PeerMessage>(&data) {
                    Ok(PeerMessage::FullSyncResponse { snapshot }) => {
                        info!("Received FullSyncResponse via outbound connection, applying snapshot");
                        let mut locks = app_state.acquire_locks(LockRequest {
                            wal_state: true,
                            core_state: true,
                            permission_cache: true,
                            store: true,
                            ..Default::default()
                        }).await;
                        
                        // Apply the snapshot to the store
                        locks.store().inner_mut().disable_notifications();
                        locks.store().inner_mut().restore_snapshot(snapshot.clone());
                        locks.store().inner_mut().enable_notifications();

                        // Update core state
                        locks.core_state().is_fully_synced = true;
                        locks.core_state().full_sync_request_pending = false;
                        locks.core_state().availability_state = AvailabilityState::Available;
                        locks.core_state().became_unavailable_at = None;

                        // Save the snapshot to disk for persistence
                        match save_snapshot(&snapshot, &mut locks).await {
                            Ok(snapshot_counter) => {
                                info!(
                                    snapshot_counter = snapshot_counter,
                                    "Saved snapshot to disk after full sync"
                                );
                                
                                // Write a snapshot marker to the WAL
                                let machine_id = locks.core_state().config.machine.clone();

                                let snapshot_request = qlib_rs::Request::Snapshot {
                                    snapshot_counter,
                                    timestamp: Some(time::OffsetDateTime::now_utc()),
                                    originator: Some(machine_id.clone()),
                                };
                                
                                if let Err(e) = write_request_to_wal(&snapshot_request, &mut locks, true).await {
                                    error!(error = %e, "Failed to write snapshot marker to WAL");
                                }

                                match reinit_caches(&mut locks).await {
                                    Ok(_) => info!("Caches reinitialized after full sync"),
                                    Err(e) => error!(error = %e, "Failed to reinitialize caches after full sync"),
                                }
                            }
                            Err(e) => {
                                error!(error = %e, "Failed to save snapshot during full sync");
                            }
                        }
                        
                        info!("Successfully applied full sync snapshot via outbound connection");
                    }
                    Ok(_) => {
                        // Other peer messages - ignore (handled via inbound connection)
                        debug!("Ignoring received binary peer message from outbound peer (handled via inbound connection)");
                    }
                    Err(_) => {
                        // Not a peer message - ignore
                        debug!("Ignoring received binary data from outbound peer (not a peer message)");
                    }
                }
            }
            Ok(Message::Ping(_payload)) => {
                // Respond to pings to keep connection alive
                debug!("Received ping from outbound peer");
                // Note: We can't easily send pong here since ws_sender is in the outgoing task
                // The ping/pong will be handled by the WebSocket implementation
            }
            Ok(Message::Pong(_)) => {
                debug!("Received pong from outbound peer");
            }
            Ok(Message::Close(_)) => {
                info!("Outbound peer closed connection gracefully");
                break;
            }
            Ok(Message::Frame(_)) => {
                // Handle raw frames if needed - typically not used directly
                debug!("Received raw frame from outbound peer");
            }
            Err(e) => {
                error!(error = %e, "WebSocket error with outbound peer");
                break;
            }
        }
    }
    
    outgoing_task.abort();
    
    info!("Outbound peer connection terminated");
    Ok(())
}

/// Handle a single client WebSocket connection that uses StoreProxy protocol
#[instrument(skip(stream, app_state), fields(client_addr = %client_addr))]
async fn handle_client_connection(stream: TcpStream, client_addr: std::net::SocketAddr, app_state: Arc<AppState>) -> Result<()> {
    info!("Accepting client connection");
    
    // Check if the application is available for client connections
    {
        let mut locks = app_state.acquire_locks(LockRequest {
            core_state: true,
            ..Default::default()
        }).await;

        let state_snapshot = locks.core_state().get_state_snapshot();
        if !state_snapshot.is_available() {
            info!("Rejecting client connection - application unavailable");
            // Don't accept the WebSocket connection, just return
            return Ok(());
        }
    }
    
    let ws_stream = accept_async(stream).await?;
    debug!("WebSocket handshake completed");
    
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    
    // Wait for authentication message as the first message
    let auth_timeout = tokio::time::timeout(Duration::from_secs(10), ws_receiver.next()).await;
    
    let first_message = match auth_timeout {
        Ok(Some(Ok(Message::Text(text)))) => text,
        Ok(Some(Ok(Message::Close(_)))) => {
            info!("Client closed connection before authentication");
            return Ok(());
        }
        Ok(Some(Err(e))) => {
            error!(error = %e, "WebSocket error from client during authentication");
            return Ok(());
        }
        Ok(None) => {
            info!("Client closed connection before authentication");
            return Ok(());
        }
        Ok(Some(Ok(_))) => {
            error!("Client sent non-text message during authentication");
            let _ = ws_sender.close().await;
            return Ok(());
        }
        Err(_) => {
            info!("Client authentication timeout");
            let _ = ws_sender.close().await;
            return Ok(());
        }
    };
    
    // Parse and validate authentication message
    let auth_message = match serde_json::from_str::<StoreMessage>(&first_message) {
        Ok(StoreMessage::Authenticate { .. }) => {
            serde_json::from_str::<StoreMessage>(&first_message).unwrap()
        }
        _ => {
            error!("Client first message was not authentication");
            let _ = ws_sender.close().await;
            return Ok(());
        }
    };
    
    // Process authentication
    let auth_response = {
        let mut locks = app_state.acquire_locks(LockRequest {
            store: true,
            connections: true,
            permission_cache: true,
            core_state: true,
            cel_executor: true,
            ..Default::default()
        }).await;
        process_store_message(auth_message, Some(client_addr.to_string()), &mut locks).await
    };
    
    // Send authentication response
    let auth_response_text = match serde_json::to_string(&auth_response) {
        Ok(text) => text,
        Err(e) => {
            error!(error = %e, "Failed to serialize authentication response");
            let _ = ws_sender.close().await;
            return Ok(());
        }
    };
    
    if let Err(e) = ws_sender.send(Message::Text(auth_response_text)).await {
        error!(error = %e, "Failed to send authentication response to client");
        return Ok(());
    }
    
    // Check if authentication was successful
    let is_authenticated = {
        let mut locks = app_state.acquire_locks(LockRequest {
            connections: true,
            ..Default::default()
        }).await;

        locks.connections().authenticated_clients.contains_key(&client_addr.to_string())
    };
    
    if !is_authenticated {
        info!("Client authentication failed, closing connection");
        let _ = ws_sender.close().await;
        return Ok(());
    }
    
    info!("Client authenticated successfully");
    
    // Now proceed with normal client handling
    // Create a channel for sending messages to this client
    let (tx, rx) = mpsc::unbounded_channel::<Message>();
    
    // Create a notification channel for this client
    let (notification_sender, mut notification_receiver) = notification_channel();
    
    // Store the sender and notification sender in the connected_clients HashMap
    {
        let mut locks = app_state.acquire_locks(LockRequest {
            connections: true,
            ..Default::default()
        }).await;
        let connections = locks.connections();
        connections.connected_clients.insert(client_addr.to_string(), tx.clone());
        connections.client_notification_senders.insert(client_addr.to_string(), notification_sender);
    }
    
    // Spawn a task to handle notifications for this client
    let client_addr_clone_notif = client_addr.to_string();
    let tx_clone_notif = tx.clone();
    let notification_task = tokio::spawn(async move {
        while let Some(notification) = notification_receiver.recv().await {
            // Convert notification to StoreMessage and send to client
            let notification_msg = StoreMessage::Notification { notification };
            if let Ok(notification_text) = serde_json::to_string(&notification_msg) {
                if let Err(e) = tx_clone_notif.send(Message::Text(notification_text)) {
                    error!(
                        client_addr = %client_addr_clone_notif,
                        error = %e,
                        "Failed to send notification to client"
                    );
                    break;
                }
            } else {
                error!(
                    client_addr = %client_addr_clone_notif,
                    "Failed to serialize notification for client"
                );
            }
        }
        debug!(
            client_addr = %client_addr_clone_notif,
            "Notification task ended for client"
        );
    });
    
    // Spawn a task to handle outgoing messages to the client
    let client_addr_clone = client_addr.to_string();
    let app_state_clone = Arc::clone(&app_state);
    let outgoing_task = tokio::spawn(async move {
        let mut ws_sender = ws_sender;
        let mut rx = rx;
        
        while let Some(message) = rx.recv().await {
            if let Err(e) = ws_sender.send(message).await {
                error!(
                    client_addr = %client_addr_clone,
                    error = %e,
                    "Failed to send message to client"
                );
                break;
            }
        }
        
        // Remove client from connected_clients when outgoing task ends
        let mut locks = app_state_clone.acquire_locks(LockRequest {
            connections: true,
            ..Default::default()
        }).await;
        let connections = locks.connections();
        connections.connected_clients.remove(&client_addr_clone);
        connections.authenticated_clients.remove(&client_addr_clone);
        connections.client_notification_senders.remove(&client_addr_clone);
    });
    
    // Handle incoming messages from client (after successful authentication)
    while let Some(msg) = ws_receiver.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                debug!(
                    message_length = text.len(),
                    "Received text message from client"
                );
                
                // Parse the StoreMessage
                match serde_json::from_str::<StoreMessage>(&text) {
                    Ok(store_msg) => {
                        // Process the message and generate response
                        let response_msg = {
                            let mut locks = app_state.acquire_locks(LockRequest {
                                store: true,
                                connections: true,
                                permission_cache: true,
                                core_state: true,
                                cel_executor: true,
                                ..Default::default()
                            }).await;
                            process_store_message(store_msg, Some(client_addr.to_string()), &mut locks).await
                        };
                        
                        // Send response back to client using the channel
                        let response_text = match serde_json::to_string(&response_msg) {
                            Ok(text) => text,
                            Err(e) => {
                                error!(error = %e, "Failed to serialize response");
                                continue;
                            }
                        };
                        
                        if let Err(e) = tx.send(Message::Text(response_text)) {
                            error!(error = %e, "Failed to send response to client");
                            break;
                        }
                    }
                    Err(e) => {
                        error!(
                            error = %e,
                            message_length = text.len(),
                            "Failed to parse StoreMessage from client"
                        );
                        // Send error response
                        let error_msg = StoreMessage::Error {
                            id: uuid::Uuid::new_v4().to_string(),
                            error: format!("Failed to parse message: {}", e),
                        };
                        if let Ok(error_text) = serde_json::to_string(&error_msg) {
                            let _ = tx.send(Message::Text(error_text));
                        }
                    }
                }
            }
            Ok(Message::Binary(_data)) => {
                debug!("Received binary data from client");
                // For now, we only handle text messages for StoreProxy protocol
            }
            Ok(Message::Ping(payload)) => {
                debug!("Received ping from client");
                if let Err(e) = tx.send(Message::Pong(payload)) {
                    error!(error = %e, "Failed to send pong to client");
                    break;
                }
            }
            Ok(Message::Pong(_)) => {
                debug!("Received pong from client");
            }
            Ok(Message::Close(_)) => {
                info!("Client closed connection gracefully");
                break;
            }
            Ok(Message::Frame(_)) => {
                debug!("Received raw frame from client");
            }
            Err(e) => {
                error!(error = %e, "WebSocket error with client");
                break;
            }
        }
    }
    
    // Remove client from connected_clients and cleanup notifications when connection ends
    {
        let mut locks = app_state.acquire_locks(LockRequest {
            connections: true,
            store: true,
            ..Default::default()
        }).await;
        let client_addr_string = client_addr.to_string();
        locks.connections().connected_clients.remove(&client_addr_string);

        // Get the notification sender and configurations for this client
        let notification_sender = locks.connections().client_notification_senders.remove(&client_addr_string);
        let client_configs = locks.connections().client_notification_configs.remove(&client_addr_string);

        // Remove authentication state for this client
        locks.connections().authenticated_clients.remove(&client_addr_string);

        // Unregister all notifications for this client from the store
        if let Some(configs) = client_configs {
            if let Some(sender) = notification_sender {
                {
                    let store_guard = locks.store();
                    
                    for config in configs {
                        let removed = store_guard.unregister_notification(&config, &sender).await;
                        if removed {
                            debug!(
                                client_addr = %client_addr_string,
                                config = ?config,
                                "Cleaned up notification config for disconnected client"
                            );
                        } else {
                            warn!(
                                client_addr = %client_addr_string,
                                config = ?config,
                                "Failed to clean up notification config for disconnected client"
                            );
                        }
                    }
                }

                // The notification sender being dropped will close the channel
                drop(sender);
            }
        } else if let Some(sender) = notification_sender {
            // Just drop the sender if no configs were tracked
            drop(sender);
        }
    }
    
    // Abort the outgoing task and notification task
    outgoing_task.abort();
    notification_task.abort();
    
    info!("Client connection terminated");
    Ok(())
}

/// Process a StoreMessage and generate the appropriate response
async fn process_store_message(message: StoreMessage, client_addr: Option<String>, locks: &mut AppStateLocks<'_>) -> StoreMessage {
    // Extract client notification sender if needed (before accessing store)
    let client_notification_sender = if let Some(ref addr) = client_addr {
        locks.connections().client_notification_senders.get(addr).cloned()
    } else {
        None
    };

    match message {
        StoreMessage::Authenticate { id, subject_name, credential } => {
            // Perform authentication
            let auth_config = AuthConfig::default();
            match authenticate_subject(&mut locks.store(), &subject_name, &credential, &auth_config).await {
                Ok(subject_id) => {
                    // Store authentication state
                    if let Some(ref addr) = client_addr {
                        locks
                            .connections()
                            .authenticated_clients.insert(addr.clone(), subject_id.clone());
                    }
                    
                    // Create authentication result
                    let auth_result = AuthenticationResult {
                        subject_id: subject_id.clone(),
                        subject_type: subject_id.get_type().to_string(),
                    };
                    
                    StoreMessage::AuthenticateResponse {
                        id,
                        response: Ok(auth_result),
                    }
                }
                Err(e) => StoreMessage::AuthenticateResponse {
                    id,
                    response: Err(format!("{:?}", e)),
                },
            }
        }
        
        StoreMessage::AuthenticateResponse { .. } => {
            // This should not be sent by clients, only by server
            StoreMessage::Error {
                id: "unknown".to_string(),
                error: "Invalid message type".to_string(),
            }
        }
        
        // All other messages require authentication
        _ => {
            // Check if client is authenticated
            let client_id = if let Some(ref addr) = client_addr {
                locks.connections().authenticated_clients.get(addr).cloned()
            } else {
                None // No client address means not authenticated
            };
            
            if client_id.is_none() {
                return StoreMessage::Error {
                    id: "unknown".to_string(),
                    error: "Authentication required".to_string(),
                };
            }
            let client_id = client_id.unwrap();
            
            match message {
                StoreMessage::Authenticate { .. } |
                StoreMessage::AuthenticateResponse { .. } => {
                    // These are handled in the outer match, should not reach here
                    StoreMessage::Error {
                        id: "unknown".to_string(),
                        error: "Authentication messages should not reach this point".to_string(),
                    }
                }
        
                StoreMessage::GetEntitySchema { id, entity_type } => {
                    match locks.store().get_entity_schema(&entity_type).await {
                        Ok(schema) => StoreMessage::GetEntitySchemaResponse {
                            id,
                            response: Ok(Some(schema)),
                        },
                        Err(e) => StoreMessage::GetEntitySchemaResponse {
                            id,
                            response: Err(format!("{:?}", e)),
                        },
                    }
                }
                
                StoreMessage::GetCompleteEntitySchema { id, entity_type } => {
                    match locks.store().get_complete_entity_schema(&entity_type).await {
                        Ok(schema) => StoreMessage::GetCompleteEntitySchemaResponse {
                            id,
                            response: Ok(schema),
                        },
                        Err(e) => StoreMessage::GetCompleteEntitySchemaResponse {
                            id,
                            response: Err(format!("{:?}", e)),
                        },
                    }
                }
                
                StoreMessage::GetFieldSchema { id, entity_type, field_type } => {
                    match locks.store().get_field_schema(&entity_type, &field_type).await {
                        Ok(schema) => StoreMessage::GetFieldSchemaResponse {
                            id,
                            response: Ok(Some(schema)),
                        },
                        Err(e) => StoreMessage::GetFieldSchemaResponse {
                            id,
                            response: Err(format!("{:?}", e)),
                        },
                    }
                }
                
                StoreMessage::EntityExists { id, entity_id } => {
                    let exists = locks.store().entity_exists(&entity_id).await;
                    StoreMessage::EntityExistsResponse {
                        id,
                        response: exists,
                    }
                }
                
                StoreMessage::FieldExists { id, entity_type, field_type } => {
                    let exists = locks.store().field_exists(&entity_type, &field_type).await;
                    StoreMessage::FieldExistsResponse {
                        id,
                        response: exists,
                    }
                }
                
                StoreMessage::Perform { id, mut requests } => {
                    let ((mut cel_executor, mut store), permission_cache) = locks
                        .cel_executor.as_mut().zip(locks.store.as_mut()).zip(locks.permission_cache.as_ref()).unwrap();

                    if let Some(permission_cache) = &**permission_cache {
                        // Check authorization for each request
                        for request in &requests {
                                if let Some(entity_id) = request.entity_id() {
                                    if let Some(field_type) = request.field_type() {
                                        match get_scope(
                                            &mut store,
                                            &mut cel_executor,
                                            permission_cache,
                                            &client_id,
                                            entity_id,
                                            field_type,
                                        ).await {
                                            Ok(scope) => {
                                                if scope == AuthorizationScope::None {
                                                    return StoreMessage::PerformResponse {
                                                        id,
                                                        response: Err(format!(
                                                            "Access denied: Subject {} is not authorized to access {} on entity {}",
                                                        client_id,
                                                        field_type,
                                                        entity_id
                                                    )),
                                                };
                                            }
                                            // For write operations, check if we have write access
                                            if matches!(request, qlib_rs::Request::Write { .. } | qlib_rs::Request::Create { .. } | qlib_rs::Request::Delete { .. } | qlib_rs::Request::SchemaUpdate { .. }) {
                                                if scope == AuthorizationScope::ReadOnly {
                                                    return StoreMessage::PerformResponse {
                                                        id,
                                                        response: Err(format!(
                                                            "Access denied: Subject {} only has read access to {} on entity {}",
                                                            client_id,
                                                            field_type,
                                                            entity_id
                                                        )),
                                                    };
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            return StoreMessage::PerformResponse {
                                                id,
                                                response: Err(format!("Authorization check failed: {:?}", e)),
                                            };
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        return StoreMessage::PerformResponse {
                            id,
                            response: Err("Authorization cache not available".to_string()),
                        };
                    }

                    let machine = locks.core_state().config.machine.clone();

                    requests.iter_mut().for_each(|req| {
                        req.try_set_originator(machine.clone());
                        req.try_set_writer_id(client_id.clone());
                    });

                    match locks.store().perform_mut(&mut requests).await {
                        Ok(()) => StoreMessage::PerformResponse {
                            id,
                            response: Ok(requests),
                        },
                        Err(e) => StoreMessage::PerformResponse {
                            id,
                            response: Err(format!("{:?}", e)),
                        },
                    }
                }
                
                StoreMessage::FindEntities { id, entity_type, page_opts, filter } => {
                    match locks.store().find_entities_paginated(&entity_type, page_opts, filter).await {
                        Ok(result) => StoreMessage::FindEntitiesResponse {
                            id,
                            response: Ok(result),
                        },
                        Err(e) => StoreMessage::FindEntitiesResponse {
                            id,
                            response: Err(format!("{:?}", e)),
                        },
                    }
                }
                
                StoreMessage::FindEntitiesExact { id, entity_type, page_opts, filter } => {
                    match locks.store().find_entities_exact(&entity_type, page_opts, filter).await {
                        Ok(result) => StoreMessage::FindEntitiesExactResponse {
                            id,
                            response: Ok(result),
                        },
                        Err(e) => StoreMessage::FindEntitiesExactResponse {
                            id,
                            response: Err(format!("{:?}", e)),
                        },
                    }
                }
                
                StoreMessage::GetEntityTypes { id, page_opts } => {
                    match locks.store().get_entity_types_paginated(page_opts).await {
                        Ok(result) => StoreMessage::GetEntityTypesResponse {
                            id,
                            response: Ok(result),
                        },
                        Err(e) => StoreMessage::GetEntityTypesResponse {
                            id,
                            response: Err(format!("{:?}", e)),
                        },
                    }
                }
                
                StoreMessage::RegisterNotification { id, config } => {
                    // Register notification for this client
                    if let Some(client_addr) = &client_addr {
                        if let Some(ref notification_sender) = client_notification_sender {
                            match locks.store().register_notification(config.clone(), notification_sender.clone()).await {
                                Ok(()) => {
                                    locks
                                        .connections()
                                        .client_notification_configs
                                        .entry(client_addr.clone())
                                        .or_insert_with(HashSet::new)
                                        .insert(config.clone());
                                    
                                    debug!(
                                        client_addr = %client_addr,
                                        config = ?config,
                                        "Registered notification for client"
                                    );
                                    StoreMessage::RegisterNotificationResponse {
                                        id,
                                        response: Ok(()),
                                    }
                                }
                                Err(e) => {
                                    error!(
                                        client_addr = %client_addr,
                                        error = ?e,
                                        "Failed to register notification for client"
                                    );
                                    StoreMessage::RegisterNotificationResponse {
                                        id,
                                        response: Err(format!("Failed to register notification: {:?}", e)),
                                    }
                                }
                            }
                        } else {
                            error!(
                                client_addr = %client_addr,
                                "No notification sender found for client"
                            );
                            StoreMessage::RegisterNotificationResponse {
                                id,
                                response: Err("Client notification sender not found".to_string()),
                            }
                        }
                    } else {
                        StoreMessage::RegisterNotificationResponse {
                            id,
                            response: Err("Client address not provided".to_string()),
                        }
                    }
                }
                
                StoreMessage::UnregisterNotification { id, config } => {
                    // Unregister notification for this client
                    if let Some(client_addr) = &client_addr {
                        if let Some(ref notification_sender) = client_notification_sender {
                            let removed = locks.store().unregister_notification(&config, notification_sender).await;
                            
                            // Remove from client's tracked configs if successfully unregistered
                            if removed {
                                if let Some(client_configs) = locks.connections().client_notification_configs.get_mut(client_addr) {
                                    client_configs.remove(&config);
                                }
                            }
                            
                            debug!(
                                client_addr = %client_addr,
                                config = ?config,
                                removed = removed,
                                "Unregistered notification for client"
                            );
                            StoreMessage::UnregisterNotificationResponse {
                                id,
                                response: removed,
                            }
                        } else {
                            error!(
                                client_addr = %client_addr,
                                "No notification sender found for client"
                            );
                            StoreMessage::UnregisterNotificationResponse {
                                id,
                                response: false,
                            }
                        }
                    } else {
                        StoreMessage::UnregisterNotificationResponse {
                            id,
                            response: false,
                        }
                    }
                }
                
                // These message types should not be received by the server
                StoreMessage::GetEntitySchemaResponse { id, .. } |
                StoreMessage::GetCompleteEntitySchemaResponse { id, .. } |
                StoreMessage::GetFieldSchemaResponse { id, .. } |
                StoreMessage::EntityExistsResponse { id, .. } |
                StoreMessage::FieldExistsResponse { id, .. } |
                StoreMessage::PerformResponse { id, .. } |
                StoreMessage::FindEntitiesResponse { id, .. } |
                StoreMessage::FindEntitiesExactResponse { id, .. } |
                StoreMessage::GetEntityTypesResponse { id, .. } |
                StoreMessage::RegisterNotificationResponse { id, .. } |
                StoreMessage::UnregisterNotificationResponse { id, .. } => {
                    StoreMessage::Error {
                        id,
                        error: "Received response message on server - this should not happen".to_string(),
                    }
                }
                
                StoreMessage::Notification { .. } => {
                    StoreMessage::Error {
                        id: uuid::Uuid::new_v4().to_string(),
                        error: "Received notification message on server - this should not happen".to_string(),
                    }
                }
                
                StoreMessage::Error { id, error } => {
                    warn!(
                        message_id = %id,
                        error_message = %error,
                        "Received error message from client"
                    );
                    StoreMessage::Error {
                        id: uuid::Uuid::new_v4().to_string(),
                        error: "Server received error message from client".to_string(),
                    }
                }
            }
        }
    }
}

/// Start the client WebSocket server task
#[instrument(skip(app_state))]
async fn start_client_server(app_state: Arc<AppState>) -> Result<()> {
    let addr = {
        let mut locks = app_state.acquire_locks(LockRequest {
            core_state: true,
            ..Default::default()
        }).await;
        format!("0.0.0.0:{}", locks.core_state().config.client_port)
    };
    
    let listener = TcpListener::bind(&addr).await?;
    info!(bind_address = %addr, "Client WebSocket server started");
    
    loop {
        match listener.accept().await {
            Ok((stream, client_addr)) => {
                debug!(client_addr = %client_addr, "Accepted new client connection");
                
                let app_state_clone = Arc::clone(&app_state);
                tokio::spawn(async move {
                    if let Err(e) = handle_client_connection(stream, client_addr, app_state_clone).await {
                        error!(
                            error = %e,
                            client_addr = %client_addr,
                            "Error handling client connection"
                        );
                    }
                });
            }
            Err(e) => {
                error!(error = %e, "Failed to accept client connection");
                // Continue listening despite individual connection errors
            }
        }
    }
}

/// Start the peer WebSocket server task
#[instrument(skip(app_state))]
async fn start_inbound_peer_server(app_state: Arc<AppState>) -> Result<()> {
    let addr = {
        let mut locks = app_state.acquire_locks(LockRequest {
            core_state: true,
            ..Default::default()
        }).await;

        format!("0.0.0.0:{}", locks.core_state().config.peer_port)
    };
    
    let listener = TcpListener::bind(&addr).await?;
    info!(bind_address = %addr, "Peer WebSocket server started");
    
    loop {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                debug!(peer_addr = %peer_addr, "Accepted new peer connection");
                
                let app_state_clone = Arc::clone(&app_state);
                tokio::spawn(async move {
                    if let Err(e) = handle_inbound_peer_connection(stream, peer_addr, app_state_clone).await {
                        error!(
                            error = %e,
                            peer_addr = %peer_addr,
                            "Error handling peer connection"
                        );
                    }
                });
            }
            Err(e) => {
                error!(error = %e, "Failed to accept peer connection");
                // Continue listening despite individual connection errors
            }
        }
    }
}

/// Manage outbound peer connections - connects to configured peers and maintains connections
async fn manage_outbound_peer_connections(app_state: Arc<AppState>) -> Result<()> {
    info!("Starting outbound peer connection manager");
    
    let reconnect_interval = {
        let mut locks = app_state.acquire_locks(LockRequest {
            core_state: true,
            ..Default::default()
        }).await;
        Duration::from_secs(locks.core_state().config.peer_reconnect_interval_secs)
    };
    
    let mut interval = tokio::time::interval(reconnect_interval);
    
    loop {
        interval.tick().await;
        
        let peers_to_connect = {
            let locks = app_state.acquire_locks(LockRequest {
                connections: true,
                core_state: true,
                ..Default::default()
            }).await;

            let (connections, core_state) = locks.connections.as_ref().zip(locks.core_state.as_ref()).unwrap();
            let connected = &connections.connected_outbound_peers;
            core_state.config.peer_addresses.iter()
                .filter(|addr| !connected.contains_key(*addr))
                .cloned()
                .collect::<Vec<_>>()
        };
        
        for peer_addr in peers_to_connect {
            info!(
                peer_addr = %peer_addr,
                "Attempting to connect to unconnected peer"
            );
            
            let peer_addr_clone = peer_addr.clone();
            let app_state_clone = Arc::clone(&app_state);
            
            tokio::spawn(async move {
                // Attempt connection
                if let Err(e) = handle_outbound_peer_connection(&peer_addr_clone, app_state_clone.clone()).await {
                    error!(
                        peer_addr = %peer_addr_clone,
                        error = %e,
                        "Failed to connect to peer"
                    );
                } else {
                    info!(
                        peer_addr = %peer_addr_clone,
                        "Connection to peer ended"
                    );
                }

                let mut locks = app_state_clone.acquire_locks(LockRequest {
                    connections: true,
                    ..Default::default()
                }).await;
                locks.connections().connected_outbound_peers.remove(&peer_addr_clone);
            });
        }
    }
}

/// Consume and process requests from the store's write channel
async fn consume_write_channel(app_state: Arc<AppState>) -> Result<()> {
    info!("Starting write channel consumer");
    
    // Get a clone of the write channel receiver
    let receiver = {
        let mut locks = app_state.acquire_locks(LockRequest {
            store: true,
            ..Default::default()
        }).await;
        locks.store().inner().get_write_channel_receiver()
    };
    
    loop {
        // Wait for a batch of requests from the write channel without holding any store locks
        let requests = {
            let mut receiver_guard = receiver.lock().await;
            receiver_guard.recv().await
        };
        
        match requests {
            Some(mut requests) => {     
                let mut locks = app_state.acquire_locks(LockRequest {
                    wal_state: true,
                    core_state: true,
                    store: true,
                    connections: true,
                    ..Default::default()
                }).await;

                // Ensure the originator is set for all requests
                let current_machine = locks.core_state().config.machine.clone();
                requests.iter_mut().for_each(|req| req.try_set_originator(current_machine.clone()));
                
                // Write all requests to the WAL file - these requests have already been applied to the store
                for request in &requests {
                    if let Err(e) = write_request_to_wal(request, &mut locks, false).await {
                        error!(
                            error = %e,
                            "Failed to write request to WAL"
                        );
                    }
                }
                
                // Send batch of requests to peers for synchronization if we have any
                let requests_to_sync: Vec<qlib_rs::Request> = requests.iter()
                    .filter(|request| {
                        if let Some(originator) = request.originator() {
                            originator == &current_machine
                        } else {
                            false
                        }
                    })
                    .cloned()
                    .collect();
                
                if !requests_to_sync.is_empty() {                    
                    // Send to all connected outbound peers using PeerMessage
                    let peers_to_notify = locks.connections().connected_outbound_peers.clone();

                    // Create a batch sync message
                    let sync_message = PeerMessage::SyncRequest {
                        requests: requests_to_sync.clone(),
                    };
                    
                    // Serialize the sync message to JSON for transmission
                    match serde_json::to_string(&sync_message) {
                        Ok(message_json) => {
                            let message = Message::Text(message_json);
                            
                            for (peer_addr, sender) in &peers_to_notify {
                                if let Err(e) = sender.send(message.clone()) {
                                    warn!(
                                        peer_addr = %peer_addr,
                                        error = %e,
                                        "Failed to send sync requests to peer"
                                    );
                                } else {
                                    debug!(
                                        peer_addr = %peer_addr,
                                        count = requests_to_sync.len(),
                                        "Sent sync requests to peer"
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            error!(
                                error = %e,
                                "Failed to serialize sync message for peer synchronization"
                            );
                        }
                    }
                }
            }
            None => {
                warn!("Write channel closed, stopping consumer");
                return Ok(());
            }
        }
    }
}

/// Legacy wrapper functions to maintain compatibility during transition

/// Write data to WAL with length prefix and handle file creation/rotation
async fn write_request_to_wal(request: &qlib_rs::Request, locks: &mut AppStateLocks<'_>, direct_mode: bool) -> Result<()> {
    let wal_manager = WalManager::new();
    wal_manager.write_request(request, locks, direct_mode).await
}

/// Save a snapshot to disk and return the snapshot counter that was used
#[instrument(skip(snapshot, locks))]
async fn save_snapshot(snapshot: &qlib_rs::Snapshot, locks: &mut AppStateLocks<'_>) -> Result<u64> {
    let snapshot_manager = SnapshotManager::new();
    snapshot_manager.save(snapshot, locks).await
}

/// Replay WAL files to restore store state
async fn replay_wal_files(locks: &mut AppStateLocks<'_>) -> Result<()> {
    let wal_manager = WalManager::new();
    wal_manager.replay(locks).await
}

/// Handle miscellaneous periodic tasks that run every 10ms
async fn handle_misc_tasks(app_state: Arc<AppState>) -> Result<()> {
    info!("Starting miscellaneous tasks handler (10ms interval)");
    
    let mut interval = tokio::time::interval(Duration::from_millis(10));
    
    loop {
        interval.tick().await;

        let mut locks = app_state.acquire_locks(LockRequest {
            wal_state: true,
            core_state: true,
            store: true,
            permission_cache: true,
            connections: true,
            peer_info: true,
            ..Default::default()
        }).await;

        // Check if we should self-promote to leader when no peers are connected
        {
            let ((connections, core), peer_info) = locks
                .connections.as_ref()
                .zip(locks.core_state.as_mut())
                .zip(locks.peer_info.as_ref())
                .unwrap();

            // Self-promote to leader if:
            // 1. We're not already the leader
            // 2. No peer addresses are configured OR no outbound peers are connected
            // 3. No peer info is tracked (no inbound peers)
            // 4. We've waited at least 5 seconds since startup to give other nodes time to connect
            let should_self_promote = !core.is_leader && 
                (core.config.peer_addresses.is_empty() || connections.connected_outbound_peers.is_empty());
            
            if should_self_promote {
                let current_time = time::OffsetDateTime::now_utc().unix_timestamp() as u64;
                let startup_time = core.startup_time;
                let time_since_startup = current_time.saturating_sub(startup_time);
                
                // Wait for the configured delay since startup before self-promoting
                let self_promotion_delay = core.config.self_promotion_delay_secs;
                if time_since_startup >= self_promotion_delay {
                    if peer_info.is_empty() {
                        
                        info!(
                            delay_secs = self_promotion_delay,
                            time_since_startup = time_since_startup,
                            "No peers connected after self-promotion delay, promoting to leader"
                        );
                        
                        let our_machine_id = core.config.machine.clone();
                        core.is_leader = true;
                        core.current_leader = Some(our_machine_id.clone());
                        core.availability_state = AvailabilityState::Available;
                        core.is_fully_synced = true;

                        info!(
                            machine_id = %our_machine_id,
                            "Self-promoted to leader due to no peer connections"
                        );
                    }
                }
            }
        }

        // Check if we need to send a full sync request after grace period
        let (should_send_full_sync, is_leader) = {
            let core = locks.core_state();

            // Only check if we're unavailable, not the leader, not fully synced, and haven't sent a request yet
            if matches!(core.availability_state, AvailabilityState::Unavailable) &&
               !core.is_leader &&
               !core.is_fully_synced &&
               !core.full_sync_request_pending {
                
                if let Some(became_unavailable_at) = core.became_unavailable_at {
                    let current_time = time::OffsetDateTime::now_utc().unix_timestamp() as u64;
                    
                    let grace_period_secs = core.config.full_sync_grace_period_secs;
                    let elapsed = current_time.saturating_sub(became_unavailable_at);
                    
                    if elapsed >= grace_period_secs {
                        // Grace period has expired, check if we have a known leader
                        (core.current_leader.clone(), core.is_leader)
                    } else {
                        (None, core.is_leader)
                    }
                } else {
                    (None, core.is_leader)
                }
            } else {
                (None, core.is_leader)
            }
        };
        
        if let Some(leader_machine_id) = should_send_full_sync {
            let (core, connections) = locks.core_state.as_mut().zip(locks.connections.as_ref()).unwrap();

            info!(
                leader_machine_id = %leader_machine_id,
                "Grace period expired, sending FullSyncRequest to leader"
            );
            
            // Mark that we're sending a request to avoid duplicates
            core.full_sync_request_pending = true;
            
            // Send FullSyncRequest to the leader through any connected outbound peer
            let full_sync_request = PeerMessage::FullSyncRequest {
                machine_id: core.config.machine.clone(),
            };
            
            if let Ok(request_json) = serde_json::to_string(&full_sync_request) {
                let message = Message::Text(request_json);
                
                // Try to send to any connected outbound peer
                let sent = {
                    let mut sent = false;
                    
                    for (peer_addr, sender) in &connections.connected_outbound_peers {
                        if let Err(e) = sender.send(message.clone()) {
                            warn!(
                                peer_addr = %peer_addr,
                                error = %e,
                                "Failed to send FullSyncRequest to peer"
                            );
                        } else {
                            info!(
                                peer_addr = %peer_addr,
                                "Sent FullSyncRequest to peer"
                            );
                            sent = true;
                            break; // Only need to send to one peer
                        }
                    }
                    sent
                };
                
                if !sent {
                    warn!(
                        leader_machine_id = %leader_machine_id,
                        "No connected outbound peers available to send FullSyncRequest to leader"
                    );

                    // Reset the pending flag so we can try again later
                    core.full_sync_request_pending = false;
                }
            } else {
                error!("Failed to serialize FullSyncRequest");
                
                core.full_sync_request_pending = false;
            }
        }

        // Process cache notifications
        {
            let permission_cache = locks.permission_cache();
            if let Some(cache) = permission_cache.as_mut() {
                cache.process_notifications();
            }
        }

        if is_leader {
            let (store, core) = locks.store.as_mut().zip(locks.core_state.as_ref()).unwrap();

            // Find us as a candidate
            let me_as_candidate = {
                let machine = &core.config.machine;

                let mut candidates = store.find_entities(
                    &et::candidate(), 
                    Some(format!("Name == 'qcore' && Parent->Name == '{}'", machine))).await?;

                candidates.pop()
            };

            // Update available list and current leader
            {
                let fault_tolerances = store.find_entities(&et::fault_tolerance(), None).await?;
                for ft_entity_id in fault_tolerances {
                    let ft_fields = store.perform_map(&mut vec![
                        sread!(ft_entity_id.clone(), ft::candidate_list()),
                        sread!(ft_entity_id.clone(), ft::available_list()),
                        sread!(ft_entity_id.clone(), ft::current_leader())
                    ]).await?;

                    let candidates = ft_fields
                        .get(&ft::candidate_list())
                        .unwrap()
                        .value()
                        .unwrap()
                        .expect_entity_list()?;

                    let mut available = Vec::new();
                    for candidate_id in candidates.iter() {
                        let candidate_fields = store.perform_map(&mut vec![
                            sread!(candidate_id.clone(), ft::make_me()),
                            sread!(candidate_id.clone(), ft::heartbeat()),
                            sread!(candidate_id.clone(), ft::death_detection_timeout()),
                        ]).await?;

                        let heartbeat_time = candidate_fields
                            .get(&ft::heartbeat())
                            .unwrap()
                            .write_time()
                            .unwrap();

                        let make_me = candidate_fields
                            .get(&ft::make_me())
                            .unwrap()
                            .value()
                            .unwrap()
                            .expect_choice()?;

                        let death_detection_timeout_millis = candidate_fields
                            .get(&ft::death_detection_timeout())
                            .unwrap()
                            .value()
                            .unwrap()
                            .expect_int()?;
                        
                        let death_detection_timeout_duration = time::Duration::milliseconds(death_detection_timeout_millis);

                        let desired_availability = match make_me {
                            1 => AvailabilityState::Available,
                            _ => AvailabilityState::Unavailable,
                        };

                        if desired_availability == AvailabilityState::Available && 
                           heartbeat_time + death_detection_timeout_duration > now() {
                            available.push(candidate_id.clone());
                        }
                    }

                    store.perform_mut(&mut vec![
                        swrite!(ft_entity_id.clone(), ft::available_list(), Some(qlib_rs::Value::EntityList(available.clone())), PushCondition::Changes),
                    ]).await?;

                    let mut handle_me_as_candidate = false;
                    if let Some(me_as_candidate) = &me_as_candidate {
                        // If we're not in the candidate list, we can't be leader
                        if candidates.contains(me_as_candidate) {
                            handle_me_as_candidate = true;

                            store.perform_mut(&mut vec![
                                swrite!(ft_entity_id.clone(), ft::current_leader(), sref!(Some(me_as_candidate.clone())), PushCondition::Changes)
                            ]).await?;
                        }
                    }

                    if !handle_me_as_candidate {
                        // Now we must promote an available candidate to leader
                        // if the current leader is no longer available.
                        // Note that we want to promote to the next available leader in the candidate list
                        // rather than the first available candidate.
                        let current_leader = ft_fields
                            .get(&ft::current_leader())
                            .unwrap()
                            .value()
                            .unwrap()
                            .expect_entity_reference()?;

                        if current_leader.is_none() {
                            store.perform_mut(&mut vec![
                                swrite!(ft_entity_id.clone(), ft::current_leader(), sref!(available.first().cloned()), PushCondition::Changes),
                            ]).await?;
                        }
                        else if let Some(current_leader) = current_leader {
                            if !available.contains(&current_leader) {
                                // Find the position of the current leader in the candidate list
                                let current_leader_idx = candidates.iter().position(|c| c.clone() == current_leader.clone());
                                
                                if let Some(current_idx) = current_leader_idx {
                                    // Find the next available candidate after the current leader in the candidate list
                                    let mut next_leader = None;
                                    
                                    // Start searching from the position after the current leader
                                    for i in (current_idx + 1)..candidates.len() {
                                        if available.contains(&candidates[i]) {
                                            next_leader = Some(candidates[i].clone());
                                            break;
                                        }
                                    }
                                    
                                    // If no leader found after current position, wrap around to the beginning
                                    if next_leader.is_none() {
                                        for i in 0..=current_idx {
                                            if available.contains(&candidates[i]) {
                                                next_leader = Some(candidates[i].clone());
                                                break;
                                            }
                                        }
                                    }
                                    
                                    store.perform_mut(&mut vec![
                                        swrite!(ft_entity_id.clone(), ft::current_leader(), sref!(next_leader), PushCondition::Changes),
                                    ]).await?;
                                } else {
                                    // Current leader not found in candidates list, just pick the first available
                                    store.perform_mut(&mut vec![
                                        swrite!(ft_entity_id.clone(), ft::current_leader(), sref!(available.first().cloned()), PushCondition::Changes),
                                    ]).await?;
                                }
                            }
                        }
                    }
                }
            }
        }

        tokio::task::yield_now().await;
    }
}

/// Handle heartbeat writing
async fn handle_heartbeat_writing(app_state: Arc<AppState>) -> Result<()> {
    info!("Starting heartbeat writer");
    
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    
    loop {
        interval.tick().await;

        let mut locks = app_state.acquire_locks(LockRequest {
            store: true,
            core_state: true,
            ..Default::default()
        }).await;

        let (store, core) = locks.store.as_mut().zip(locks.core_state.as_ref()).unwrap();
        let machine = &core.config.machine;

        let candidates = store.find_entities(
            &et::candidate(), 
            Some(format!("Name == 'qcore' && Parent->Name == '{}'", machine))).await?;

        if let Some(candidate) = candidates.first() {
            store.perform_mut(&mut vec![
                swrite!(candidate.clone(), ft::heartbeat(), schoice!(0)),
                swrite!(candidate.clone(), ft::make_me(), schoice!(1), PushCondition::Changes)
            ]).await?;
        }

        tokio::task::yield_now().await;
    }
}

async fn reinit_caches(locks: &mut AppStateLocks<'_>) -> Result<()> {
    let (configs, sender) = locks.permission_cache().as_ref().map(|cache| cache.get_config_sender()).unwrap_or_default();
    if let Some(sender) = sender {
        for config in configs {
            locks.store().unregister_notification(&config, &sender).await;
        }
    }

    **locks.permission_cache() = Some(Cache::new(
        &mut **locks.store(),
        et::permission(),
        vec![ft::resource_type(), ft::resource_field()],
        vec![ft::scope(), ft::condition()]
    ).await?);
    
    let machine = locks.core_state().config.machine.clone();

    {
        let store = locks.store();

        let me_as_candidate = {

            let mut candidates = store.find_entities(
                &et::candidate(), 
                Some(format!("Name == 'qcore' && Parent->Name == '{}'", machine))).await?;

            candidates.pop()
        };

        if let Some(candidate_id) = &me_as_candidate {
            store.inner_mut().default_writer_id = Some(candidate_id.clone());
        } else {
            store.inner_mut().default_writer_id = None;
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::parse();

    // Initialize tracing with better structured logging
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "qcore_rs=debug,tokio=warn,tokio_tungstenite=warn".to_string())
        )
        .with_target(true)
        .with_thread_ids(true)
        .with_file(cfg!(debug_assertions))
        .with_line_number(cfg!(debug_assertions))
        .init();

    let machine_id = &config.machine;
    let peer_port = config.peer_port;
    let client_port = config.client_port;
    
    info!(
        machine_id = %machine_id,
        peer_port = peer_port,
        client_port = client_port,
        data_dir = %config.data_dir,
        "Starting QCore service"
    );

    // Create shared application state
    let app_state = Arc::new(AppState::new(config).await?);

    // Initialize the WAL file counter based on existing files
    {
        let mut locks = app_state.acquire_locks(LockRequest {
            core_state: true,
            wal_state: true,
            ..Default::default()
        }).await;
        let wal_manager = WalManager::new();
        wal_manager.initialize_counter(&mut locks).await?;
    }

    // Load the latest snapshot if available
    {
        let mut locks = app_state.acquire_locks(LockRequest {
            core_state: true,
            store: true,
            wal_state: true,
            ..Default::default()
        }).await;

        let snapshot_manager = SnapshotManager::new();
        if let Some((snapshot, snapshot_counter)) = snapshot_manager.load_latest(&mut locks).await? {
            info!(
                snapshot_counter = snapshot_counter,
                "Restoring store from snapshot"
            );
            
            // Initialize the snapshot file counter to continue from the next number
            snapshot_manager.initialize_counter(&mut locks).await?;
            
            {
                let store_guard = locks.store();
                store_guard.inner_mut().disable_notifications();
                store_guard.inner_mut().restore_snapshot(snapshot);
                store_guard.inner_mut().enable_notifications();
            }
        } else {
            info!("No snapshot found, starting with empty store");
            
            // Initialize the snapshot file counter
            snapshot_manager.initialize_counter(&mut locks).await?;
        }
    }

    // Replay WAL files to bring the store up to date
    // The replay function will automatically find the most recent snapshot marker
    // in the WAL files and start replaying from that point
    info!("Replaying WAL files");
    {
        let mut locks = app_state.acquire_locks(LockRequest {
            store: true,
            core_state: true,
            ..Default::default()
        }).await;
        if let Err(e) = replay_wal_files(&mut locks).await {
            error!(
                error = %e,
                "Failed to replay WAL files"
            );
            return Err(e);
        }
    }

    // Reinitialize caches after WAL replay
    {
        let mut locks = app_state.acquire_locks(LockRequest {
            store: true,
            core_state: true,
            permission_cache: true,
            ..Default::default()
        }).await;
        reinit_caches(&mut locks).await?;
    }

    // Start the write channel consumer task
    let app_state_clone = Arc::clone(&app_state);
    let mut write_channel_task = tokio::spawn(async move {
        if let Err(e) = consume_write_channel(app_state_clone).await {
            error!(
                error = %e,
                "Write channel consumer failed"
            );
        }
    });

    // Start the peer WebSocket server task
    let app_state_clone = Arc::clone(&app_state);
    let mut peer_server_task = tokio::spawn(async move {
        if let Err(e) = start_inbound_peer_server(app_state_clone).await {
            error!(
                error = %e,
                "Peer server failed"
            );
        }
    });

    // Start the client WebSocket server task
    let app_state_clone = Arc::clone(&app_state);
    let mut client_server_task = tokio::spawn(async move {
        if let Err(e) = start_client_server(app_state_clone).await {
            error!(
                error = %e,
                "Client server failed"
            );
        }
    });

    // Start the outbound peer connection manager task
    let app_state_clone = Arc::clone(&app_state);
    let mut outbound_peer_task = tokio::spawn(async move {
        if let Err(e) = manage_outbound_peer_connections(app_state_clone).await {
            error!(
                error = %e,
                "Outbound peer connection manager failed"
            );
        }
    });

    // Start the misc tasks handler
    let app_state_clone = Arc::clone(&app_state);
    let mut misc_task = tokio::spawn(async move {
        if let Err(e) = handle_misc_tasks(app_state_clone).await {
            error!(
                error = %e,
                "Misc tasks handler failed"
            );
        }
    });

    // Start the heartbeat writer
    let app_state_clone = Arc::clone(&app_state);
    let mut heartbeat_task = tokio::spawn(async move {
        if let Err(e) = handle_heartbeat_writing(app_state_clone).await {
            error!(
                error = %e,
                "Heartbeat writer failed"
            );
        }
    });

    // Wait for either shutdown signal or any critical task to complete/fail
    tokio::select! {
        _ = signal::ctrl_c() => {
            warn!("Received shutdown signal, initiating graceful shutdown");
        }
        result = &mut write_channel_task => {
            match result {
                Ok(_) => error!("Write channel task exited unexpectedly"),
                Err(e) => error!(error = %e, "Write channel task failed"),
            }
            warn!("Critical task failure detected, initiating shutdown");
        }
        result = &mut peer_server_task => {
            match result {
                Ok(_) => error!("Peer server task exited unexpectedly"),
                Err(e) => error!(error = %e, "Peer server task failed"),
            }
            warn!("Critical task failure detected, initiating shutdown");
        }
        result = &mut client_server_task => {
            match result {
                Ok(_) => error!("Client server task exited unexpectedly"),
                Err(e) => error!(error = %e, "Client server task failed"),
            }
            warn!("Critical task failure detected, initiating shutdown");
        }
        result = &mut outbound_peer_task => {
            match result {
                Ok(_) => error!("Outbound peer task exited unexpectedly"),
                Err(e) => error!(error = %e, "Outbound peer task failed"),
            }
            warn!("Critical task failure detected, initiating shutdown");
        }
        result = &mut misc_task => {
            match result {
                Ok(_) => error!("Misc task exited unexpectedly"),
                Err(e) => error!(error = %e, "Misc task failed"),
            }
            warn!("Critical task failure detected, initiating shutdown");
        }
        result = &mut heartbeat_task => {
            match result {
                Ok(_) => error!("Heartbeat task exited unexpectedly"),
                Err(e) => error!(error = %e, "Heartbeat task failed"),
            }
            warn!("Critical task failure detected, initiating shutdown");
        }
    }

    let mut locks = app_state.acquire_locks(LockRequest {
        wal_state: true,
        core_state: true,
        store: true,
        ..Default::default()
    }).await;

    // Take a final snapshot before shutting down
    info!("Taking final snapshot before shutdown");
    let snapshot = locks.store().inner().take_snapshot();
    
    match save_snapshot(&snapshot, &mut locks).await {
        Ok(snapshot_counter) => {
            info!(
                snapshot_counter = snapshot_counter,
                "Final snapshot saved successfully"
            );
            
            // Write a snapshot marker to the WAL to indicate the final snapshot point
            // This helps during replay to know that the state was snapshotted at shutdown
            let snapshot_request = qlib_rs::Request::Snapshot {
                snapshot_counter,
                timestamp: Some(now()),
                originator: Some({
                    let core = locks.core_state();
                    core.config.machine.clone()
                }),
            };
            
            if let Err(e) = write_request_to_wal(&snapshot_request, &mut locks, true).await {
                error!(error = %e, "Failed to write final snapshot marker to WAL");
            } else {
                info!("Final snapshot marker written to WAL");
            }
        }
        Err(e) => {
            error!(error = %e, "Failed to save final snapshot");
        }
    }

    // Abort all tasks
    info!("Stopping all background tasks");
    write_channel_task.abort();
    peer_server_task.abort();
    client_server_task.abort();
    outbound_peer_task.abort();
    misc_task.abort();
    heartbeat_task.abort();

    info!("QCore service shutdown complete");
    Ok(())
}
