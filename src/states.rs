use qlib_rs::{AsyncStore, Cache, CelExecutor, EntityId, NotificationSender, NotifyConfig, Snowflake};
use tokio_tungstenite::{tungstenite::Message};
use tokio::sync::{mpsc, Mutex, MutexGuard};
use tracing::{info, warn};
use clap::Parser;
use anyhow::Result;
use std::collections::{HashSet, HashMap};
use std::sync::Arc;
use std::path::PathBuf;
use serde::{Serialize, Deserialize};
use time;

/// Application availability state
#[derive(Debug, Clone, PartialEq)]
pub enum AvailabilityState {
    /// Application is unavailable - attempting to sync with leader, clients are force disconnected
    Unavailable,
    /// Application is available - clients are allowed to connect and perform operations
    Available,
}

/// Helper structure for extracting data without holding locks
#[derive(Debug, Clone)]
pub struct StateSnapshot {
    pub config: Config,
    pub startup_time: u64,
    pub availability_state: AvailabilityState,
}

impl StateSnapshot {
    pub fn is_available(&self) -> bool {
        matches!(self.availability_state, AvailabilityState::Available)
    }
}

/// Connection state separated from main state to reduce lock contention
#[derive(Debug)]
pub struct ConnectionState {
    /// Connected outbound peers with message senders
    pub connected_outbound_peers: HashMap<String, mpsc::UnboundedSender<Message>>,
    /// Connected clients with message senders
    pub connected_clients: HashMap<String, mpsc::UnboundedSender<Message>>,
    /// Client notification senders
    pub client_notification_senders: HashMap<String, NotificationSender>,
    /// Track notification configurations per client for cleanup on disconnect
    pub client_notification_configs: HashMap<String, HashSet<NotifyConfig>>,
    /// Track authenticated clients
    pub authenticated_clients: HashMap<String, EntityId>,
}

impl ConnectionState {
    pub fn new() -> Self {
        Self {
            connected_outbound_peers: HashMap::new(),
            connected_clients: HashMap::new(),
            client_notification_senders: HashMap::new(),
            client_notification_configs: HashMap::new(),
            authenticated_clients: HashMap::new(),
        }
    }

    /// Force disconnect all connected clients (used when transitioning to unavailable)
    pub fn force_disconnect_all_clients(&mut self) {
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

/// Messages exchanged between peers for leader election
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum PeerMessage {
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
pub struct Config {
    /// Machine ID (unique identifier for this instance)
    #[arg(long)]
    pub machine: String,

    /// Data directory for storing WAL files and other persistent data
    #[arg(long, default_value = "./data")]
    pub data_dir: String,

    /// Maximum WAL file size in MB
    #[arg(long, default_value_t = 1)]
    pub wal_max_file_size: usize,

    /// Maximum number of WAL files to keep
    #[arg(long, default_value_t = 30)]
    pub wal_max_files: usize,

    /// Number of WAL file rollovers before taking a snapshot
    #[arg(long, default_value_t = 3)]
    pub snapshot_wal_interval: u64,

    /// Maximum number of snapshot files to keep
    #[arg(long, default_value_t = 5)]
    pub snapshot_max_files: usize,

    /// Port for peer-to-peer communication
    #[arg(long, default_value_t = 9000)]
    pub peer_port: u16,

    /// Port for client communication (StoreProxy clients)
    #[arg(long, default_value_t = 9100)]
    pub client_port: u16,

    /// List of peer addresses to connect to (format: host:port)
    #[arg(long, value_delimiter = ',')]
    pub peer_addresses: Vec<String>,

    /// Interval in seconds to retry connecting to peers
    #[arg(long, default_value_t = 3)]
    pub peer_reconnect_interval_secs: u64,

    /// Grace period in seconds to wait after becoming unavailable before requesting full sync
    #[arg(long, default_value_t = 5)]
    pub full_sync_grace_period_secs: u64,

    /// Delay in seconds after startup before self-promoting to leader when no peers are available
    #[arg(long, default_value_t = 5)]
    pub self_promotion_delay_secs: u64,
}

/// Application state that is shared across all tasks
#[derive(Debug)]
pub struct AppState {
    /// Core configuration and leadership state - should be accessed minimally
    pub core_state: Mutex<CoreState>,
    
    /// Connection-related state - separate lock to reduce contention
    pub connections: Mutex<ConnectionState>,
    
    /// Peer information tracking
    pub peer_info: Mutex<HashMap<String, PeerInfo>>,
    
    /// Data store - kept separate as it has its own locking
    pub store: Arc<Mutex<AsyncStore>>,
    
    /// Permission Cache - separate as it's accessed frequently
    pub permission_cache: Mutex<Option<Cache>>,
    
    /// CEL Executor for evaluating authorization conditions
    pub cel_executor: Mutex<CelExecutor>,
}

/// Core application state
#[derive(Debug)]
pub struct CoreState {
    /// Configuration
    pub config: Config,
    
    /// Startup time (timestamp in seconds since UNIX_EPOCH)
    pub startup_time: u64,
    
    /// Current availability state of the application
    pub availability_state: AvailabilityState,
    
    /// Whether this instance has been elected as leader
    pub is_leader: bool,
    
    /// The machine ID of the current leader (if known)
    pub current_leader: Option<String>,
    
    /// Whether this instance has completed full sync with the leader
    pub is_fully_synced: bool,
    
    /// Timestamp when we became unavailable (for grace period tracking)
    pub became_unavailable_at: Option<u64>,

    /// Whether a full sync request is pending (to avoid sending multiple)
    pub full_sync_request_pending: bool,
}

impl CoreState {
    /// Get a snapshot of the core state without holding locks
    pub fn get_state_snapshot(&self) -> StateSnapshot {
        StateSnapshot {
            config: self.config.clone(),
            startup_time: self.startup_time,
            availability_state: self.availability_state.clone(),
        }
    }
    
    /// Get the machine-specific data directory
    pub fn get_machine_data_dir(&self) -> PathBuf {
        PathBuf::from(&self.config.data_dir).join(&self.config.machine)
    }

    /// Get the machine-specific WAL directory
    pub fn get_wal_dir(&self) -> PathBuf {
        let machine_data_dir = self.get_machine_data_dir();
        machine_data_dir.join("wal")
    }

    /// Get the machine-specific snapshots directory
    pub fn get_snapshots_dir(&self) -> PathBuf {
        let machine_data_dir = self.get_machine_data_dir();
        machine_data_dir.join("snapshots")
    }
}

/// Information about a peer instance
#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub machine_id: String,
    pub startup_time: u64
}

/// Specifies which AppState locks to acquire
#[derive(Default)]
pub struct LockRequest {
    pub core_state: bool,
    pub connections: bool,
    pub peer_info: bool,
    pub store: bool,
    pub permission_cache: bool,
    pub cel_executor: bool,
}

/// Contains the acquired locks in the proper order
pub struct AppStateLocks<'a> {
    pub core_state: Option<MutexGuard<'a, CoreState>>,
    pub connections: Option<MutexGuard<'a, ConnectionState>>,
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
    pub fn new(config: Config) -> Result<Self> {
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
            peer_info: Mutex::new(HashMap::new()),
            store,
            permission_cache: Mutex::new(None),
            cel_executor: Mutex::new(CelExecutor::new()),
        })
    }
    
    /// Retrigger leader election when the current leader has disconnected
    pub async fn retrigger_leader_election(&self, locks: &mut AppStateLocks<'_>) {
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
    /// 3. peer_info
    /// 4. store
    /// 5. permission_cache
    /// 6. cel_executor
    /// 
    /// Only the locks specified in the request will be acquired.
    pub async fn acquire_locks(&self, request: LockRequest) -> AppStateLocks<'_> {
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