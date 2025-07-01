use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::path::PathBuf;

use openraft::BasicNode;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::LogId;
use openraft::RaftSnapshotBuilder;
use openraft::RaftTypeConfig;
use openraft::SnapshotMeta;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::StoredMembership;
use openraft::storage::RaftStateMachine;
use openraft::storage::Snapshot;
use qlib_rs::Store;
use serde::Deserialize;
use serde::Serialize;
use std::ops::RangeBounds;
use tokio::sync::RwLock;

use openraft::LogState;
use openraft::RaftLogId;
use openraft::Vote;
use openraft::storage::LogFlushed;
use tokio::sync::Mutex;

use crate::app::NodeId;
use crate::app::TypeConfig;

#[derive(Debug, Clone)]
pub struct LogStoreConfig {
    /// Maximum number of log files to keep on disk
    pub max_log_files: usize,
    /// Maximum total size of log files in MB
    pub max_log_size_mb: u64,
    /// How often to check for cleanup (every N entries)
    pub cleanup_interval: u64,
}

impl Default for LogStoreConfig {
    fn default() -> Self {
        Self {
            max_log_files: 1000,
            max_log_size_mb: 100,
            cleanup_interval: 100,
        }
    }
}

/// RaftLogStore implementation with persistent storage
#[derive(Clone, Debug)]
pub struct LogStore<C: RaftTypeConfig> {
    inner: Arc<Mutex<LogStoreInner<C>>>,
    data_dir: PathBuf,
    config: LogStoreConfig,
}

impl<C: RaftTypeConfig> Default for LogStore<C> 
where 
    C::Entry: serde::Serialize + serde::de::DeserializeOwned + Clone,
    C::NodeId: serde::Serialize + serde::de::DeserializeOwned + Clone,
{
    fn default() -> Self {
        // Use a default node directory - in practice this should be called with explicit node ID
        Self::new_with_config("./raft_data/node_default".into(), LogStoreConfig::default()).expect("Failed to create default LogStore")
    }
}

#[derive(Debug)]
pub struct LogStoreInner<C: RaftTypeConfig> {
    /// The last purged log id.
    last_purged_log_id: Option<LogId<C::NodeId>>,

    /// The Raft log.
    log: BTreeMap<u64, C::Entry>,

    /// The commit log id.
    committed: Option<LogId<C::NodeId>>,

    /// The current granted vote.
    vote: Option<Vote<C::NodeId>>,
}

impl<C: RaftTypeConfig> Default for LogStoreInner<C> {
    fn default() -> Self {
        Self {
            last_purged_log_id: None,
            log: BTreeMap::new(),
            committed: None,
            vote: None,
        }
    }
}

impl<C: RaftTypeConfig> LogStoreInner<C> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
        &mut self,
        range: RB,
    ) -> Result<Vec<C::Entry>, StorageError<C::NodeId>>
    where
        C::Entry: Clone,
    {
        let response = self
            .log
            .range(range.clone())
            .map(|(_, val)| val.clone())
            .collect::<Vec<_>>();
        Ok(response)
    }

    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<C::NodeId>> {
        let last = self
            .log
            .iter()
            .next_back()
            .map(|(_, ent)| ent.get_log_id().clone());

        let last_purged = self.last_purged_log_id.clone();

        let last = match last {
            None => last_purged.clone(),
            Some(x) => Some(x),
        };

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<C::NodeId>>,
    ) -> Result<(), StorageError<C::NodeId>> {
        self.committed = committed;
        Ok(())
    }

    async fn read_committed(
        &mut self,
    ) -> Result<Option<LogId<C::NodeId>>, StorageError<C::NodeId>> {
        Ok(self.committed.clone())
    }

    async fn save_vote(&mut self, vote: &Vote<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        self.vote = Some(vote.clone());
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<C::NodeId>>, StorageError<C::NodeId>> {
        Ok(self.vote.clone())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<C>,
    ) -> Result<(), StorageError<C::NodeId>>
    where
        I: IntoIterator<Item = C::Entry>,
    {
        // Simple implementation that calls the flush-before-return `append_to_log`.
        for entry in entries {
            self.log.insert(entry.get_log_id().index, entry);
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        let keys = self
            .log
            .range(log_id.index..)
            .map(|(k, _v)| *k)
            .collect::<Vec<_>>();
        for key in keys {
            self.log.remove(&key);
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        {
            let ld = &mut self.last_purged_log_id;
            assert!(ld.as_ref() <= Some(&log_id));
            *ld = Some(log_id.clone());
        }

        {
            let keys = self
                .log
                .range(..=log_id.index)
                .map(|(k, _v)| *k)
                .collect::<Vec<_>>();
            for key in keys {
                self.log.remove(&key);
            }
        }

        Ok(())
    }
}

impl<C: RaftTypeConfig> LogStore<C> 
where 
    C::Entry: serde::Serialize + serde::de::DeserializeOwned + Clone,
    C::NodeId: serde::Serialize + serde::de::DeserializeOwned + Clone,
{
    pub fn new_for_node_with_config(base_data_dir: PathBuf, node_id: u64, config: LogStoreConfig) -> Result<Self, std::io::Error> {
        let node_data_dir = base_data_dir.join(format!("node_{}", node_id));
        Self::new_with_config(node_data_dir, config)
    }

    pub fn new_with_config(data_dir: PathBuf, config: LogStoreConfig) -> Result<Self, std::io::Error> {
        // Create data directory if it doesn't exist
        std::fs::create_dir_all(&data_dir)?;
        
        let inner = LogStoreInner::default();
        let store = Self {
            inner: Arc::new(Mutex::new(inner)),
            data_dir: data_dir.clone(),
            config,
        };
        
        Ok(store)
    }
    
    pub async fn load_existing_state(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.load_state().await
    }
    
    async fn load_state(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let vote_path = self.data_dir.join("vote.json");
        let committed_path = self.data_dir.join("committed.json");
        let logs_dir = self.data_dir.join("logs");
        
        // Load vote state
        if vote_path.exists() {
            let vote_data = std::fs::read_to_string(&vote_path)?;
            if !vote_data.is_empty() {
                let vote: Vote<C::NodeId> = serde_json::from_str(&vote_data)?;
                let mut inner_guard = self.inner.lock().await;
                inner_guard.vote = Some(vote);
            }
        }
        
        // Load committed state
        if committed_path.exists() {
            let committed_data = std::fs::read_to_string(&committed_path)?;
            if !committed_data.is_empty() {
                let committed: LogId<C::NodeId> = serde_json::from_str(&committed_data)?;
                let mut inner_guard = self.inner.lock().await;
                inner_guard.committed = Some(committed);
            }
        }
        
        // Load log entries
        if logs_dir.exists() {
            let mut inner_guard = self.inner.lock().await;
            for entry in std::fs::read_dir(&logs_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.extension().map_or(false, |ext| ext == "json") {
                    if let Some(filename) = path.file_stem().and_then(|s| s.to_str()) {
                        if let Ok(index) = filename.parse::<u64>() {
                            let log_data = std::fs::read_to_string(&path)?;
                            if !log_data.is_empty() {
                                let log_entry: C::Entry = serde_json::from_str(&log_data)?;
                                inner_guard.log.insert(index, log_entry);
                            }
                        }
                    }
                }
            }
        }
        
        Ok(())
    }
    
    fn persist_vote(&self, vote: &Vote<C::NodeId>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let vote_path = self.data_dir.join("vote.json");
        let vote_data = serde_json::to_string_pretty(vote)?;
        std::fs::write(vote_path, vote_data)?;
        Ok(())
    }
    
    fn persist_committed(&self, committed: &Option<LogId<C::NodeId>>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let committed_path = self.data_dir.join("committed.json");
        if let Some(committed) = committed {
            let committed_data = serde_json::to_string_pretty(committed)?;
            std::fs::write(committed_path, committed_data)?;
        } else {
            // Remove file if committed is None
            if committed_path.exists() {
                std::fs::remove_file(committed_path)?;
            }
        }
        Ok(())
    }
    
    fn persist_log_entry(&self, index: u64, entry: &C::Entry) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let logs_dir = self.data_dir.join("logs");
        std::fs::create_dir_all(&logs_dir)?;
        
        let log_path = logs_dir.join(format!("{}.json", index));
        let log_data = serde_json::to_string_pretty(entry)?;
        std::fs::write(log_path, log_data)?;
        
        // Periodically clean up old logs to prevent unbounded growth
        // Only check every N entries to avoid excessive file system operations
        if index % self.config.cleanup_interval == 0 {
            // Clean up old logs using configured limits
            if let Err(e) = self.cleanup_old_logs(self.config.max_log_files) {
                log::warn!("Failed to cleanup old logs by count: {}", e);
            }
            if let Err(e) = self.cleanup_logs_by_size(self.config.max_log_size_mb) {
                log::warn!("Failed to cleanup old logs by size: {}", e);
            }
        }
        
        Ok(())
    }
    
    fn remove_log_entry(&self, index: u64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let logs_dir = self.data_dir.join("logs");
        let log_path = logs_dir.join(format!("{}.json", index));
        if log_path.exists() {
            std::fs::remove_file(log_path)?;
        }
        Ok(())
    }

    fn cleanup_old_logs(&self, keep_count: usize) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let logs_dir = self.data_dir.join("logs");
        if !logs_dir.exists() {
            return Ok(());
        }
        
        let mut log_files: Vec<_> = std::fs::read_dir(&logs_dir)?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                if path.extension().map_or(false, |ext| ext == "json") {
                    if let Some(filename) = path.file_stem().and_then(|s| s.to_str()) {
                        if let Ok(index) = filename.parse::<u64>() {
                            return Some((index, path));
                        }
                    }
                }
                None
            })
            .collect();
        
        if log_files.len() <= keep_count {
            return Ok(());
        }
        
        // Sort by index, oldest first
        log_files.sort_by_key(|(index, _)| *index);
        
        // Remove old log files
        let files_to_remove = log_files.len() - keep_count;
        for (index, path) in log_files.iter().take(files_to_remove) {
            if let Err(e) = std::fs::remove_file(path) {
                log::warn!("Failed to remove old log file {:?}: {}", path, e);
            } else {
                log::info!("Removed old log file: {} (index: {})", path.display(), index);
            }
        }
        
        Ok(())
    }
    
    fn get_log_files_size(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let logs_dir = self.data_dir.join("logs");
        if !logs_dir.exists() {
            return Ok(0);
        }
        
        let mut total_size = 0u64;
        for entry in std::fs::read_dir(&logs_dir)? {
            let entry = entry?;
            if entry.path().extension().map_or(false, |ext| ext == "json") {
                total_size += entry.metadata()?.len();
            }
        }
        
        Ok(total_size)
    }
    
    fn cleanup_logs_by_size(&self, max_size_mb: u64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let max_size_bytes = max_size_mb * 1024 * 1024;
        let current_size = self.get_log_files_size()?;
        
        if current_size <= max_size_bytes {
            return Ok(());
        }
        
        let logs_dir = self.data_dir.join("logs");
        let mut log_files: Vec<_> = std::fs::read_dir(&logs_dir)?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                if path.extension().map_or(false, |ext| ext == "json") {
                    if let Some(filename) = path.file_stem().and_then(|s| s.to_str()) {
                        if let Ok(index) = filename.parse::<u64>() {
                            let size = entry.metadata().ok()?.len();
                            return Some((index, path, size));
                        }
                    }
                }
                None
            })
            .collect();
        
        // Sort by index, oldest first
        log_files.sort_by_key(|(index, _, _)| *index);
        
        let mut removed_size = 0u64;
        let target_size = max_size_bytes * 80 / 100; // Remove until we're at 80% of max size
        
        for (index, path, size) in log_files.iter() {
            if current_size - removed_size <= target_size {
                break;
            }
            
            if let Err(e) = std::fs::remove_file(path) {
                log::warn!("Failed to remove old log file {:?}: {}", path, e);
            } else {
                log::info!("Removed old log file for size management: {} (index: {}, size: {} bytes)", 
                          path.display(), index, size);
                removed_size += size;
            }
        }
        
        Ok(())
    }
}

mod impl_log_store {
    use std::fmt::Debug;
    use std::ops::RangeBounds;

    use openraft::LogId;
    use openraft::LogState;
    use openraft::RaftLogReader;
    use openraft::RaftLogId;
    use openraft::RaftTypeConfig;
    use openraft::StorageError;
    use openraft::Vote;
    use openraft::storage::LogFlushed;
    use openraft::storage::RaftLogStorage;

    use crate::store::LogStore;

    impl<C: RaftTypeConfig> RaftLogReader<C> for LogStore<C>
    where
        C::Entry: Clone,
    {
        async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
            &mut self,
            range: RB,
        ) -> Result<Vec<C::Entry>, StorageError<C::NodeId>> {
            let mut inner = self.inner.lock().await;
            inner.try_get_log_entries(range).await
        }
    }

    impl<C: RaftTypeConfig> RaftLogStorage<C> for LogStore<C>
    where
        C::Entry: Clone + RaftLogId<C::NodeId>,
        C::Entry: serde::Serialize + serde::de::DeserializeOwned,
        C::NodeId: serde::Serialize + serde::de::DeserializeOwned + Clone,
    {
        type LogReader = Self;

        async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<C::NodeId>> {
            let mut inner = self.inner.lock().await;
            inner.get_log_state().await
        }

        async fn save_committed(
            &mut self,
            committed: Option<LogId<C::NodeId>>,
        ) -> Result<(), StorageError<C::NodeId>> {
            let mut inner = self.inner.lock().await;
            let result = inner.save_committed(committed.clone()).await;
            
            // Persist to disk
            if result.is_ok() {
                if let Err(e) = self.persist_committed(&committed) {
                    log::error!("Failed to persist committed state: {}", e);
                }
            }
            
            result
        }

        async fn read_committed(
            &mut self,
        ) -> Result<Option<LogId<C::NodeId>>, StorageError<C::NodeId>> {
            let mut inner = self.inner.lock().await;
            inner.read_committed().await
        }

        async fn save_vote(
            &mut self,
            vote: &Vote<C::NodeId>,
        ) -> Result<(), StorageError<C::NodeId>> {
            let mut inner = self.inner.lock().await;
            let result = inner.save_vote(vote).await;
            
            // Persist to disk
            if result.is_ok() {
                if let Err(e) = self.persist_vote(vote) {
                    log::error!("Failed to persist vote: {}", e);
                }
            }
            
            result
        }

        async fn read_vote(&mut self) -> Result<Option<Vote<C::NodeId>>, StorageError<C::NodeId>> {
            let mut inner = self.inner.lock().await;
            inner.read_vote().await
        }

        async fn append<I>(
            &mut self,
            entries: I,
            callback: LogFlushed<C>,
        ) -> Result<(), StorageError<C::NodeId>>
        where
            I: IntoIterator<Item = C::Entry>,
        {
            let mut inner = self.inner.lock().await;
            
            // Collect entries to persist them
            let entries_vec: Vec<C::Entry> = entries.into_iter().collect();
            
            // Persist entries to disk before adding to memory
            for entry in &entries_vec {
                let index = entry.get_log_id().index;
                if let Err(e) = self.persist_log_entry(index, entry) {
                    log::error!("Failed to persist log entry {}: {}", index, e);
                    // Continue anyway - the entry will still be added to memory
                }
            }
            
            inner.append(entries_vec, callback).await
        }

        async fn truncate(
            &mut self,
            log_id: LogId<C::NodeId>,
        ) -> Result<(), StorageError<C::NodeId>> {
            let mut inner = self.inner.lock().await;
            
            // Get the keys that will be removed for cleanup
            let keys_to_remove: Vec<u64> = inner.log
                .range(log_id.index..)
                .map(|(k, _v)| *k)
                .collect();
            
            let result = inner.truncate(log_id).await;
            
            // Remove persisted entries
            if result.is_ok() {
                for key in keys_to_remove {
                    if let Err(e) = self.remove_log_entry(key) {
                        log::error!("Failed to remove persisted log entry {}: {}", key, e);
                    }
                }
            }
            
            result
        }

        async fn purge(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
            let mut inner = self.inner.lock().await;
            
            // Get the keys that will be removed for cleanup
            let keys_to_remove: Vec<u64> = inner.log
                .range(..=log_id.index)
                .map(|(k, _v)| *k)
                .collect();
            
            let result = inner.purge(log_id).await;
            
            // Remove persisted entries
            if result.is_ok() {
                for key in keys_to_remove {
                    if let Err(e) = self.remove_log_entry(key) {
                        log::error!("Failed to remove persisted log entry {}: {}", key, e);
                    }
                }
            }
            
            result
        }

        async fn get_log_reader(&mut self) -> Self::LogReader {
            self.clone()
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum CommandRequest {
    UpdateEntity {
        request: Vec<qlib_rs::Request>,
    },
    CreateEntity {
        entity_type: qlib_rs::EntityType,
        parent_id: Option<qlib_rs::EntityId>,
        name: String,
    },
    DeleteEntity {
        entity_id: qlib_rs::EntityId,
    },
    SetSchema {
        entity_schema: qlib_rs::EntitySchema<qlib_rs::Single>,
    },
    GetSchema {
        entity_type: qlib_rs::EntityType,
    },
    GetCompleteSchema {
        entity_type: qlib_rs::EntityType,
    },
    SetFieldSchema {
        entity_type: qlib_rs::EntityType,
        field_type: qlib_rs::FieldType,
        schema: qlib_rs::FieldSchema,
    },
    GetFieldSchema {
        entity_type: qlib_rs::EntityType,
        field_type: qlib_rs::FieldType,
    },
    EntityExists {
        entity_id: qlib_rs::EntityId,
    },
    FieldExists {
        entity_id: qlib_rs::EntityId,
        field_type: qlib_rs::FieldType,
    },
    FindEntities {
        entity_type: qlib_rs::EntityType,
        parent_id: Option<qlib_rs::EntityId>,
        page_opts: Option<qlib_rs::PageOpts>,
    },
    FindEntitiesExact {
        entity_type: qlib_rs::EntityType,
        parent_id: Option<qlib_rs::EntityId>,
        page_opts: Option<qlib_rs::PageOpts>,
    },
    GetEntityTypes {
        parent_type: Option<qlib_rs::EntityType>,
        page_opts: Option<qlib_rs::PageOpts>,
    },
    TakeSnapshot,
    RestoreSnapshot {
        snapshot: qlib_rs::Snapshot,
    },
    RegisterNotification {
        config: qlib_rs::NotifyConfig,
    },
    UnregisterNotification {
        token: qlib_rs::NotifyToken,
    },
    GetNotificationConfigs,
}

/**
 * Here you will defined what type of answer you expect from reading the data of a node.
 * In this example it will return a optional value from a given key in
 * the `Request.Set`.
 *
 */
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum CommandResponse {
    UpdateEntity {
        response: Vec<qlib_rs::Request>,
        error: Option<String>,
    },
    CreateEntity {
        response: Option<qlib_rs::Entity>,
        error: Option<String>,
    },
    DeleteEntity {
        error: Option<String>,
    },
    SetSchema {
        error: Option<String>,
    },
    GetSchema {
        response: Option<qlib_rs::EntitySchema<qlib_rs::Single>>,
        error: Option<String>,
    },
    GetCompleteSchema {
        response: Option<qlib_rs::EntitySchema<qlib_rs::Complete>>,
        error: Option<String>,
    },
    SetFieldSchema {
        error: Option<String>,
    },
    GetFieldSchema {
        response: Option<qlib_rs::FieldSchema>,
        error: Option<String>,
    },
    EntityExists {
        response: bool,
    },
    FieldExists {
        response: bool,
    },
    FindEntities {
        response: Result<qlib_rs::PageResult<qlib_rs::EntityId>, String>,
    },
    FindEntitiesExact {
        response: Result<qlib_rs::PageResult<qlib_rs::EntityId>, String>,
    },
    GetEntityTypes {
        response: Result<qlib_rs::PageResult<qlib_rs::EntityType>, String>,
    },
    TakeSnapshot {
        response: qlib_rs::Snapshot,
    },
    RestoreSnapshot {
        error: Option<String>,
    },
    RegisterNotification {
        response: Result<qlib_rs::NotifyToken, String>,
    },
    UnregisterNotification {
        response: bool,
    },
    GetNotificationConfigs {
        response: Vec<(qlib_rs::NotifyToken, qlib_rs::NotifyConfig)>,
    },
    Blank {},
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<NodeId, BasicNode>,

    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

/// Data contained in the Raft state machine.
///
/// Note that we are using `serde` to serialize the
/// `data`, which has a implementation to be serialized. Note that for this test we set both the key
/// and value as String, but you could set any type of value that has the serialization impl.
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct StateMachineData {
    pub last_applied_log: Option<LogId<NodeId>>,

    pub last_membership: StoredMembership<NodeId, BasicNode>,

    /// Application data.
    pub data: Store,
}

/// Defines a state machine for the Raft cluster. This state machine represents a copy of the
/// data for this node. Additionally, it is responsible for storing the last snapshot of the data.
#[derive(Debug)]
pub struct StateMachineStore {
    /// The Raft state machine.
    pub state_machine: RwLock<StateMachineData>,

    /// Used in identifier for snapshot.
    ///
    /// Note that concurrently created snapshots and snapshots created on different nodes
    /// are not guaranteed to have sequential `snapshot_idx` values, but this does not matter for
    /// correctness.
    snapshot_idx: AtomicU64,

    /// The last received snapshot.
    current_snapshot: RwLock<Option<StoredSnapshot>>,
    
    /// Directory for persistent storage
    data_dir: PathBuf,
}

impl Default for StateMachineStore {
    fn default() -> Self {
        // Use a default node directory - in practice this should be called with explicit node ID
        Self::new("./raft_data/node_default".into()).expect("Failed to create default StateMachineStore")
    }
}

impl StateMachineStore {
    pub fn new(data_dir: PathBuf) -> Result<Self, std::io::Error> {
        // Create data directory if it doesn't exist
        std::fs::create_dir_all(&data_dir)?;
        
        let store = Self {
            state_machine: RwLock::new(StateMachineData::default()),
            snapshot_idx: AtomicU64::new(0),
            current_snapshot: RwLock::new(None),
            data_dir: data_dir.clone(),
        };
        
        Ok(store)
    }
    
    pub async fn load_existing_state(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.load_latest_snapshot().await
    }
    
    pub fn new_for_node(base_data_dir: PathBuf, node_id: u64) -> Result<Self, std::io::Error> {
        let node_data_dir = base_data_dir.join(format!("node_{}", node_id));
        Self::new(node_data_dir)
    }
    
    async fn load_latest_snapshot(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let snapshots_dir = self.data_dir.join("snapshots");
        if !snapshots_dir.exists() {
            return Ok(());
        }
        
        // Find the most recent snapshot
        let mut snapshot_files: Vec<_> = std::fs::read_dir(&snapshots_dir)?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                if path.extension().map_or(false, |ext| ext == "json") {
                    Some(path)
                } else {
                    None
                }
            })
            .collect();
        
        if snapshot_files.is_empty() {
            return Ok(());
        }
        
        // Sort by modification time, most recent first
        snapshot_files.sort_by(|a, b| {
            let a_meta = std::fs::metadata(a).ok();
            let b_meta = std::fs::metadata(b).ok();
            match (a_meta, b_meta) {
                (Some(a_meta), Some(b_meta)) => {
                    b_meta.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH)
                        .cmp(&a_meta.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH))
                }
                _ => std::cmp::Ordering::Equal,
            }
        });
        
        // Load the most recent snapshot
        if let Some(latest_snapshot_path) = snapshot_files.first() {
            let snapshot_data = std::fs::read_to_string(latest_snapshot_path)?;
            let stored_snapshot: StoredSnapshot = serde_json::from_str(&snapshot_data)?;
            
            // Load the state machine data from the snapshot
            let state_machine_data: StateMachineData = serde_json::from_slice(&stored_snapshot.data)?;
            
            // Update the state machine
            let mut state_machine = self.state_machine.write().await;
            *state_machine = state_machine_data;
            drop(state_machine);
            
            // Update current snapshot
            let mut current_snapshot = self.current_snapshot.write().await;
            *current_snapshot = Some(stored_snapshot);
            
            log::info!("Loaded snapshot from: {:?}", latest_snapshot_path);
        }
        
        Ok(())
    }
    
    fn persist_snapshot(&self, snapshot: &StoredSnapshot) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let snapshots_dir = self.data_dir.join("snapshots");
        std::fs::create_dir_all(&snapshots_dir)?;
        
        // Use timestamp for filename to ensure uniqueness and easy sorting
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();
        let snapshot_path = snapshots_dir.join(format!("snapshot_{}.json", timestamp));
        
        let snapshot_json = serde_json::to_string_pretty(snapshot)?;
        std::fs::write(&snapshot_path, snapshot_json)?;
        
        log::info!("Persisted snapshot to: {:?}", snapshot_path);
        
        // Clean up old snapshots, keep only the 5 most recent ones
        self.cleanup_old_snapshots(5)?;
        
        Ok(())
    }
    
    fn cleanup_old_snapshots(&self, keep_count: usize) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let snapshots_dir = self.data_dir.join("snapshots");
        if !snapshots_dir.exists() {
            return Ok(());
        }
        
        let mut snapshot_files: Vec<_> = std::fs::read_dir(&snapshots_dir)?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                if path.extension().map_or(false, |ext| ext == "json") {
                    Some(path)
                } else {
                    None
                }
            })
            .collect();
        
        if snapshot_files.len() <= keep_count {
            return Ok(());
        }
        
        // Sort by modification time, oldest first
        snapshot_files.sort_by(|a, b| {
            let a_meta = std::fs::metadata(a).ok();
            let b_meta = std::fs::metadata(b).ok();
            match (a_meta, b_meta) {
                (Some(a_meta), Some(b_meta)) => {
                    a_meta.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH)
                        .cmp(&b_meta.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH))
                }
                _ => std::cmp::Ordering::Equal,
            }
        });
        
        // Remove old snapshots
        for old_snapshot in snapshot_files.iter().take(snapshot_files.len() - keep_count) {
            if let Err(e) = std::fs::remove_file(old_snapshot) {
                log::warn!("Failed to remove old snapshot {:?}: {}", old_snapshot, e);
            } else {
                log::info!("Removed old snapshot: {:?}", old_snapshot);
            }
        }
        
        Ok(())
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<StateMachineStore> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        // Serialize the data of the state machine.
        let state_machine = self.state_machine.read().await;
        let data = serde_json::to_vec(&state_machine.data)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;

        let last_applied_log = state_machine.last_applied_log;
        let last_membership = state_machine.last_membership.clone();

        // Lock the current snapshot before releasing the lock on the state machine, to avoid a race
        // condition on the written snapshot
        let mut current_snapshot = self.current_snapshot.write().await;
        drop(state_machine);

        let snapshot_idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = if let Some(last) = last_applied_log {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{}", snapshot_idx)
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        let snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };

        // Persist the snapshot to disk
        if let Err(e) = self.persist_snapshot(&snapshot) {
            log::error!("Failed to persist snapshot: {}", e);
        }

        *current_snapshot = Some(snapshot);

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for Arc<StateMachineStore> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let state_machine = self.state_machine.read().await;
        Ok((
            state_machine.last_applied_log,
            state_machine.last_membership.clone(),
        ))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<CommandResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let mut res = Vec::new(); //No `with_capacity`; do not know `len` of iterator

        let mut sm = self.state_machine.write().await;

        for entry in entries {
            sm.last_applied_log = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => res.push(CommandResponse::Blank {}),
                EntryPayload::Normal(ref req) => {
                    let ctx = qlib_rs::Context {};
                    match req {
                        CommandRequest::CreateEntity {
                            entity_type,
                            parent_id,
                            name,
                        } => {
                            match sm.data.create_entity(
                                &ctx,
                                entity_type,
                                parent_id.clone(),
                                name.as_str(),
                            ) {
                                Ok(entity) => {
                                    res.push(CommandResponse::CreateEntity {
                                        response: Some(entity),
                                        error: None,
                                    });
                                }
                                Err(e) => {
                                    res.push(CommandResponse::CreateEntity {
                                        response: None,
                                        error: Some(format!("Error creating entity: {}", e)),
                                    });
                                }
                            }
                        }
                        CommandRequest::DeleteEntity { entity_id } => {
                            match sm.data.delete_entity(&ctx, entity_id) {
                                Ok(_) => {
                                    res.push(CommandResponse::DeleteEntity { error: None });
                                }
                                Err(e) => {
                                    res.push(CommandResponse::DeleteEntity {
                                        error: Some(format!("Error deleting entity: {}", e)),
                                    });
                                }
                            }
                        }
                        CommandRequest::SetSchema { entity_schema } => {
                            match sm.data.set_entity_schema(&ctx, entity_schema) {
                                Ok(_) => {
                                    res.push(CommandResponse::SetSchema { error: None });
                                }
                                Err(e) => {
                                    res.push(CommandResponse::SetSchema {
                                        error: Some(format!("Error setting schema: {}", e)),
                                    });
                                }
                            }
                        }
                        CommandRequest::UpdateEntity { request } => {
                            // Apply the request to the state machine.
                            let mut req = request.clone();
                            match sm.data.perform(&ctx, &mut req) {
                                Ok(_) => {
                                    res.push(CommandResponse::UpdateEntity {
                                        response: req,
                                        error: None,
                                    });
                                }
                                Err(e) => res.push(CommandResponse::UpdateEntity {
                                    response: Vec::new(),
                                    error: Some(format!("Error applying request: {}", e)),
                                }),
                            }
                        }
                        CommandRequest::GetSchema { entity_type } => {
                            match sm.data.get_entity_schema(&ctx, entity_type) {
                                Ok(schema) => {
                                    res.push(CommandResponse::GetSchema {
                                        response: Some(schema),
                                        error: None,
                                    });
                                }
                                Err(e) => {
                                    res.push(CommandResponse::GetSchema {
                                        response: None,
                                        error: Some(format!("Error getting schema: {}", e)),
                                    });
                                }
                            }
                        }
                        CommandRequest::GetCompleteSchema { entity_type } => {
                            match sm.data.get_complete_entity_schema(&ctx, entity_type) {
                                Ok(schema) => {
                                    res.push(CommandResponse::GetCompleteSchema {
                                        response: Some(schema),
                                        error: None,
                                    });
                                }
                                Err(e) => {
                                    res.push(CommandResponse::GetCompleteSchema {
                                        response: None,
                                        error: Some(format!(
                                            "Error getting complete schema: {}",
                                            e
                                        )),
                                    });
                                }
                            }
                        }
                        CommandRequest::SetFieldSchema { entity_type, field_type, schema } => {
                            match sm.data.set_field_schema(&ctx, entity_type, field_type, schema.clone()) {
                                Ok(_) => {
                                    res.push(CommandResponse::SetFieldSchema { error: None });
                                }
                                Err(e) => {
                                    res.push(CommandResponse::SetFieldSchema {
                                        error: Some(format!("Error setting field schema: {}", e)),
                                    });
                                }
                            }
                        }
                        CommandRequest::RestoreSnapshot { snapshot } => {
                            sm.data.restore_snapshot(&ctx, snapshot.clone());
                            res.push(CommandResponse::RestoreSnapshot { error: None });
                        }
                        CommandRequest::RegisterNotification { config } => {
                            // For now, we'll return an error since notification callbacks can't be serialized
                            // In a real implementation, you'd need a way to handle this
                            res.push(CommandResponse::RegisterNotification {
                                response: Err("Notification registration not supported via Raft".to_string()),
                            });
                        }
                        CommandRequest::UnregisterNotification { token } => {
                            let success = sm.data.unregister_notification_by_token(token);
                            res.push(CommandResponse::UnregisterNotification {
                                response: success,
                            });
                        }
                        // Read-only operations are handled directly and don't go through Raft
                        CommandRequest::GetFieldSchema { .. } |
                        CommandRequest::EntityExists { .. } |
                        CommandRequest::FieldExists { .. } |
                        CommandRequest::FindEntities { .. } |
                        CommandRequest::FindEntitiesExact { .. } |
                        CommandRequest::GetEntityTypes { .. } |
                        CommandRequest::TakeSnapshot |
                        CommandRequest::GetNotificationConfigs => {
                            // These should not reach here as they are handled as read-only operations
                            res.push(CommandResponse::Blank {});
                        }
                    }
                }
                EntryPayload::Membership(ref mem) => {
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    res.push(CommandResponse::Blank {})
                }
            };
        }
        Ok(res)
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<<TypeConfig as RaftTypeConfig>::SnapshotData>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<<TypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<NodeId>> {
        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        // Update the state machine.
        let updated_state_machine_data = serde_json::from_slice(&new_snapshot.data)
            .map_err(|e| StorageIOError::read_snapshot(Some(new_snapshot.meta.signature()), &e))?;
        let updated_state_machine = StateMachineData {
            last_applied_log: meta.last_log_id,
            last_membership: meta.last_membership.clone(),
            data: updated_state_machine_data,
        };
        let mut state_machine = self.state_machine.write().await;
        *state_machine = updated_state_machine;

        // Lock the current snapshot before releasing the lock on the state machine, to avoid a race
        // condition on the written snapshot
        let mut current_snapshot = self.current_snapshot.write().await;
        drop(state_machine);

        // Persist the installed snapshot to disk
        if let Err(e) = self.persist_snapshot(&new_snapshot) {
            log::error!("Failed to persist installed snapshot: {}", e);
        }

        // Update current snapshot.
        *current_snapshot = Some(new_snapshot);
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(Snapshot {
                    meta: snapshot.meta.clone(),
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
            None => Ok(None),
        }
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}
