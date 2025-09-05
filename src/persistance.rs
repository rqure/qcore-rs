use qlib_rs::{now, StoreTrait};
use tracing::{info, warn, error, debug, instrument};
use anyhow::Result;
use std::vec;
use tokio::fs::{File, OpenOptions, create_dir_all, read_dir, remove_file};
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use std::path::PathBuf;

use crate::states::AppStateLocks;

/// Helper struct for managing numbered files (WAL files, snapshots, etc.)
pub struct FileManager;

impl FileManager {
    /// Scan directory for files matching prefix and suffix, returning sorted paths with counters
    pub async fn scan_files(&self, dir: &PathBuf, prefix: &str, suffix: &str) -> Result<Vec<(PathBuf, u64)>> {
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
    pub async fn get_next_counter(&self, dir: &PathBuf, prefix: &str, suffix: &str) -> Result<u64> {
        let files = self.scan_files(dir, prefix, suffix).await?;
        let max_counter = files.iter().map(|(_, counter)| *counter).max().unwrap_or(0);
        Ok(if max_counter == 0 && files.is_empty() { 0 } else { max_counter + 1 })
    }

    /// Clean up old files, keeping only the most recent max_files
    pub async fn cleanup_old_files(&self, dir: &PathBuf, prefix: &str, suffix: &str, max_files: usize) -> Result<()> {
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
pub struct WalEntryReader {
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
pub struct WalManager {
    file_manager: FileManager,
}

impl WalManager {
    pub fn new() -> Self {
        Self {
            file_manager: FileManager,
        }
    }
    
    /// Write a request to WAL with file rotation and snapshot handling
    pub async fn write_request(&self, request: &qlib_rs::Request, locks: &mut AppStateLocks<'_>, direct_mode: bool) -> Result<()> {
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
    pub async fn replay(&self, locks: &mut AppStateLocks<'_>) -> Result<()> {
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
    pub async fn initialize_counter(&self, locks: &mut AppStateLocks<'_>) -> Result<()> {
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
pub struct SnapshotManager {
    file_manager: FileManager,
}

impl SnapshotManager {
    pub fn new() -> Self {
        Self {
            file_manager: FileManager,
        }
    }
    
    /// Save a snapshot to disk and return the snapshot counter
    #[instrument(skip(self, snapshot, locks))]
    pub async fn save(&self, snapshot: &qlib_rs::Snapshot, locks: &mut AppStateLocks<'_>) -> Result<u64> {
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
    pub async fn load_latest(&self, locks: &mut AppStateLocks<'_>) -> Result<Option<(qlib_rs::Snapshot, u64)>> {
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
    pub async fn initialize_counter(&self, locks: &mut AppStateLocks<'_>) -> Result<()> {
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