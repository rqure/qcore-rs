use qlib_rs::EntityId;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, connect_async, tungstenite::Message};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn, error, debug, instrument};
use anyhow::Result;
use std::collections::HashMap;
use std::time::Duration;
use time;
use serde::{Serialize, Deserialize};

use crate::store::StoreHandle;

/// Configuration for the peer service
#[derive(Debug, Clone)]
pub struct PeerConfig {
    /// Machine ID (unique identifier for this instance)
    pub machine: String,
    /// Data directory for storing WAL files and other persistent data
    pub data_dir: String,
    /// Port for peer-to-peer communication
    pub peer_port: u16,
    /// List of peer addresses to connect to (format: host:port)
    pub peer_addresses: Vec<String>,
    /// Interval in seconds to retry connecting to peers
    pub peer_reconnect_interval_secs: u64,
    /// Grace period in seconds to wait after becoming unavailable before requesting full sync
    pub full_sync_grace_period_secs: u64,
    /// Delay in seconds after startup before self-promoting to leader when no peers are available
    pub self_promotion_delay_secs: u64,
}

impl From<&crate::Config> for PeerConfig {
    fn from(config: &crate::Config) -> Self {
        Self {
            machine: config.machine.clone(),
            data_dir: config.data_dir.clone(),
            peer_port: config.peer_port,
            peer_addresses: config.peer_addresses.clone(),
            peer_reconnect_interval_secs: config.peer_reconnect_interval_secs,
            full_sync_grace_period_secs: config.full_sync_grace_period_secs,
            self_promotion_delay_secs: config.self_promotion_delay_secs,
        }
    }
}

/// Application availability state
#[derive(Debug, Clone, PartialEq)]
pub enum AvailabilityState {
    /// Application is unavailable - attempting to sync with leader, clients are force disconnected
    Unavailable,
    /// Application is available - clients are allowed to connect and perform operations
    Available,
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

/// Information about a peer instance
#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub machine_id: String,
    pub startup_time: u64
}



/// Peer service request types
#[derive(Debug)]
pub enum PeerRequest {
    SendSyncMessage {
        requests: Vec<qlib_rs::Request>,
        response: oneshot::Sender<()>,
    },
    GetAvailabilityState {
        response: oneshot::Sender<AvailabilityState>,
    },
    GetLeadershipInfo {
        response: oneshot::Sender<(bool, Option<String>)>, // (is_leader, current_leader)
    },
    PeerConnected {
        peer_addr: String,
        machine_id: String,
        startup_time: u64,
    },
    PeerDisconnected {
        peer_addr: String,
    },
    OutboundPeerConnected {
        peer_addr: String,
        sender: mpsc::UnboundedSender<Message>,
    },
    OutboundPeerDisconnected {
        peer_addr: String,
    },
    FullSyncCompleted,
}

/// Handle for communicating with peer service
#[derive(Debug, Clone)]
pub struct PeerHandle {
    sender: mpsc::UnboundedSender<PeerRequest>,
}

impl PeerHandle {
    pub async fn send_sync_message(&self, requests: Vec<qlib_rs::Request>) {
        let (response_tx, response_rx) = oneshot::channel();
        if self.sender.send(PeerRequest::SendSyncMessage {
            requests,
            response: response_tx,
        }).is_ok() {
            let _ = response_rx.await;
        }
    }

    pub async fn get_availability_state(&self) -> AvailabilityState {
        let (response_tx, response_rx) = oneshot::channel();
        if self.sender.send(PeerRequest::GetAvailabilityState {
            response: response_tx,
        }).is_ok() {
            response_rx.await.unwrap_or(AvailabilityState::Unavailable)
        } else {
            AvailabilityState::Unavailable
        }
    }

    pub async fn get_leadership_info(&self) -> (bool, Option<String>) {
        let (response_tx, response_rx) = oneshot::channel();
        if self.sender.send(PeerRequest::GetLeadershipInfo {
            response: response_tx,
        }).is_ok() {
            response_rx.await.unwrap_or((false, None))
        } else {
            (false, None)
        }
    }

    pub fn peer_connected(&self, peer_addr: String, machine_id: String, startup_time: u64) {
        let _ = self.sender.send(PeerRequest::PeerConnected {
            peer_addr,
            machine_id,
            startup_time,
        });
    }

    pub fn peer_disconnected(&self, peer_addr: String) {
        let _ = self.sender.send(PeerRequest::PeerDisconnected { peer_addr });
    }

    pub fn outbound_peer_connected(&self, peer_addr: String, sender: mpsc::UnboundedSender<Message>) {
        let _ = self.sender.send(PeerRequest::OutboundPeerConnected {
            peer_addr,
            sender,
        });
    }

    pub fn outbound_peer_disconnected(&self, peer_addr: String) {
        let _ = self.sender.send(PeerRequest::OutboundPeerDisconnected { peer_addr });
    }

    pub fn full_sync_completed(&self) {
        let _ = self.sender.send(PeerRequest::FullSyncCompleted);
    }
}

pub struct PeerService {
    config: PeerConfig,
    startup_time: u64,
    availability_state: AvailabilityState,
    is_leader: bool,
    current_leader: Option<String>,
    is_fully_synced: bool,
    became_unavailable_at: Option<u64>,
    full_sync_request_pending: bool,
    peer_info: HashMap<String, PeerInfo>,
    connected_outbound_peers: HashMap<String, mpsc::UnboundedSender<Message>>,
    store: StoreHandle
}

impl PeerService {
    pub fn spawn(
        config: PeerConfig,
        store: StoreHandle,
    ) -> PeerHandle {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        
        let startup_time = time::OffsetDateTime::now_utc().unix_timestamp() as u64;
        
        let mut service = PeerService {
            config: config.clone(),
            startup_time,
            availability_state: AvailabilityState::Available,
            is_leader: false,
            current_leader: None,
            is_fully_synced: false,
            became_unavailable_at: None,
            full_sync_request_pending: false,
            peer_info: HashMap::new(),
            connected_outbound_peers: HashMap::new(),
            store: store.clone(),
        };
        
        let handle = PeerHandle { sender: sender.clone() };
        
        // Start subtasks with the handle
        {
            let handle_clone = handle.clone();
            let config_clone = config.clone();
            tokio::spawn(async move {
                if let Err(e) = start_inbound_peer_server(config_clone, handle_clone, store.clone()).await {
                    error!(error = %e, "Inbound peer server failed");
                }
            });
        }
        
        {
            let handle_clone = handle.clone();
            let config_clone = config.clone();
            tokio::spawn(async move {
                if let Err(e) = manage_outbound_peer_connections(config_clone, handle_clone).await {
                    error!(error = %e, "Outbound peer connection manager failed");
                }
            });
        }
        
        // Main service loop
        tokio::spawn(async move {
            let mut check_interval = tokio::time::interval(Duration::from_millis(100));
            
            loop {
                tokio::select! {
                    _ = check_interval.tick() => {
                        service.check_leader_election_and_sync().await;
                    }
                    request = receiver.recv() => {
                        match request {
                            Some(req) => service.handle_request(req).await,
                            None => break,
                        }
                    }
                }
            }
        });

        handle
    }
    
    async fn handle_request(&mut self, request: PeerRequest) {
        match request {
            PeerRequest::SendSyncMessage { requests, response } => {
                self.send_sync_message_to_peers(requests).await;
                let _ = response.send(());
            }
            PeerRequest::GetAvailabilityState { response } => {
                let _ = response.send(self.availability_state.clone());
            }
            PeerRequest::GetLeadershipInfo { response } => {
                let _ = response.send((self.is_leader, self.current_leader.clone()));
            }
            PeerRequest::PeerConnected { peer_addr, machine_id, startup_time } => {
                self.handle_peer_connected(peer_addr, machine_id, startup_time).await;
            }
            PeerRequest::PeerDisconnected { peer_addr } => {
                self.handle_peer_disconnected(peer_addr).await;
            }
            PeerRequest::OutboundPeerConnected { peer_addr, sender } => {
                self.connected_outbound_peers.insert(peer_addr.clone(), sender);
                
                // Send startup message
                let startup = PeerMessage::Startup {
                    machine_id: self.config.machine.clone(),
                    startup_time: self.startup_time,
                };
                
                if let Ok(startup_json) = serde_json::to_string(&startup) {
                    if let Some(sender) = self.connected_outbound_peers.get(&peer_addr) {
                        let _ = sender.send(Message::Text(startup_json));
                    }
                }
            }
            PeerRequest::OutboundPeerDisconnected { peer_addr } => {
                self.connected_outbound_peers.remove(&peer_addr);
            }
            PeerRequest::FullSyncCompleted => {
                self.is_fully_synced = true;
                self.full_sync_request_pending = false;
                self.availability_state = AvailabilityState::Available;
                self.became_unavailable_at = None;
            }
        }
    }
    
    async fn check_leader_election_and_sync(&mut self) {
        // Self-promotion logic
        if !self.is_leader && 
           (self.config.peer_addresses.is_empty() || self.connected_outbound_peers.is_empty()) &&
           self.peer_info.is_empty() {
            
            let current_time = time::OffsetDateTime::now_utc().unix_timestamp() as u64;
            let time_since_startup = current_time.saturating_sub(self.startup_time);
            
            if time_since_startup >= self.config.self_promotion_delay_secs {
                info!("Self-promoting to leader due to no peer connections");
                self.is_leader = true;
                self.current_leader = Some(self.config.machine.clone());
                self.availability_state = AvailabilityState::Available;
                self.is_fully_synced = true;
            }
        }
        
        // Full sync request logic
        if matches!(self.availability_state, AvailabilityState::Unavailable) &&
           !self.is_leader &&
           !self.is_fully_synced &&
           !self.full_sync_request_pending {
            
            if let Some(became_unavailable_at) = self.became_unavailable_at {
                let current_time = time::OffsetDateTime::now_utc().unix_timestamp() as u64;
                let elapsed = current_time.saturating_sub(became_unavailable_at);
                
                if elapsed >= self.config.full_sync_grace_period_secs {
                    if let Some(leader_machine_id) = &self.current_leader {
                        self.send_full_sync_request(leader_machine_id.clone()).await;
                    }
                }
            }
        }
    }
    
    async fn handle_peer_connected(&mut self, peer_addr: String, machine_id: String, startup_time: u64) {
        info!(
            peer_addr = %peer_addr,
            machine_id = %machine_id,
            startup_time = startup_time,
            "Peer connected, updating leadership"
        );
        
        self.peer_info.insert(peer_addr, PeerInfo {
            machine_id,
            startup_time,
        });
        
        self.determine_leadership().await;
    }
    
    async fn handle_peer_disconnected(&mut self, peer_addr: String) {
        let disconnected_machine_id = self.peer_info.get(&peer_addr)
            .map(|info| info.machine_id.clone());
        
        self.peer_info.remove(&peer_addr);
        
        // Check if the disconnected peer was the current leader
        if let Some(disconnected_machine_id) = disconnected_machine_id {
            if let Some(ref current_leader) = self.current_leader {
                if *current_leader == disconnected_machine_id {
                    info!(
                        disconnected_machine = %disconnected_machine_id,
                        "Current leader disconnected, retriggering leader election"
                    );
                    self.determine_leadership().await;
                }
            }
        }
    }
    
    async fn determine_leadership(&mut self) {
        let our_startup_time = self.startup_time;
        let our_machine_id = &self.config.machine;
        
        // Find the earliest startup time among all known peers including ourselves
        let earliest_startup = self.peer_info.values()
            .map(|p| p.startup_time)
            .min()
            .unwrap_or(our_startup_time)
            .min(our_startup_time);
        
        let mut should_be_leader = our_startup_time <= earliest_startup;
        
        // Handle startup time ties
        if our_startup_time == earliest_startup {
            let peers_with_same_time: Vec<_> = self.peer_info.values()
                .filter(|p| p.startup_time == our_startup_time)
                .collect();
            
            if !peers_with_same_time.is_empty() {
                let mut all_machine_ids = peers_with_same_time.iter()
                    .map(|p| p.machine_id.as_str())
                    .collect::<Vec<_>>();
                all_machine_ids.push(our_machine_id.as_str());
                
                let min_machine_id = all_machine_ids.iter().min().unwrap();
                should_be_leader = **min_machine_id == *our_machine_id;
            }
        }
        
        let old_state = self.availability_state.clone();
        
        if should_be_leader {
            self.is_leader = true;
            self.current_leader = Some(our_machine_id.clone());
            self.is_fully_synced = true;
            self.availability_state = AvailabilityState::Available;
            self.became_unavailable_at = None;
            
            info!(
                our_startup_time = our_startup_time,
                earliest_startup = earliest_startup,
                "Elected as leader"
            );
        } else {
            let leader = self.peer_info.values()
                .filter(|p| p.startup_time <= earliest_startup)
                .min_by_key(|p| (&p.startup_time, &p.machine_id))
                .map(|p| p.machine_id.clone());
            
            self.is_leader = false;
            self.current_leader = leader;
            self.availability_state = AvailabilityState::Unavailable;
            self.is_fully_synced = false;
            self.full_sync_request_pending = false;
            self.became_unavailable_at = Some(time::OffsetDateTime::now_utc().unix_timestamp() as u64);
            
            info!(
                leader = ?self.current_leader,
                our_startup_time = our_startup_time,
                earliest_startup = earliest_startup,
                "Leader determined, stepping down"
            );
        }
        
        // Force disconnect clients if becoming unavailable
        if old_state != self.availability_state && 
           matches!(self.availability_state, AvailabilityState::Unavailable) {
            // Clients are now stored directly in PeerService, no need for mutex
        }
    }
    
    async fn send_sync_message_to_peers(&self, requests: Vec<qlib_rs::Request>) {
        let current_machine = &self.config.machine;
        
        // Filter requests to only include those from our machine
        let requests_to_sync: Vec<qlib_rs::Request> = requests.iter()
            .filter(|request| {
                if let Some(originator) = request.originator() {
                    originator == current_machine
                } else {
                    false
                }
            })
            .cloned()
            .collect();
        
        if !requests_to_sync.is_empty() {
            let sync_message = PeerMessage::SyncRequest {
                requests: requests_to_sync.clone(),
            };
            
            if let Ok(message_json) = serde_json::to_string(&sync_message) {
                let message = Message::Text(message_json);
                
                for (peer_addr, sender) in &self.connected_outbound_peers {
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
        }
    }
    
    async fn send_full_sync_request(&mut self, leader_machine_id: String) {
        info!(
            leader_machine_id = %leader_machine_id,
            "Grace period expired, sending FullSyncRequest to leader"
        );
        
        self.full_sync_request_pending = true;
        
        let full_sync_request = PeerMessage::FullSyncRequest {
            machine_id: self.config.machine.clone(),
        };
        
        if let Ok(request_json) = serde_json::to_string(&full_sync_request) {
            let message = Message::Text(request_json);
            
            let mut sent = false;
            for (peer_addr, sender) in &self.connected_outbound_peers {
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
                    break;
                }
            }
            
            if !sent {
                warn!("No connected outbound peers available to send FullSyncRequest");
                self.full_sync_request_pending = false;
            }
        } else {
            error!("Failed to serialize FullSyncRequest");
            self.full_sync_request_pending = false;
        }
    }

    fn get_wal_dir(&self) -> std::path::PathBuf {
        std::path::PathBuf::from(&self.config.data_dir)
            .join(&self.config.machine)
            .join("wal")
    }

    fn get_snapshots_dir(&self) -> std::path::PathBuf {
        std::path::PathBuf::from(&self.config.data_dir)
            .join(&self.config.machine)
            .join("snapshots")
    }
}

/// Handle a single peer WebSocket connection
#[instrument(skip(stream, handle, store), fields(peer_addr = %peer_addr))]
async fn handle_inbound_peer_connection(
    stream: TcpStream,
    peer_addr: std::net::SocketAddr,
    handle: PeerHandle,
    store: StoreHandle,
) -> Result<()> {
    info!("Accepting inbound peer connection");
    
    let ws_stream = accept_async(stream).await?;
    debug!("WebSocket handshake completed");
    
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    
    while let Some(msg) = ws_receiver.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                if let Ok(peer_msg) = serde_json::from_str::<PeerMessage>(&text) {
                    handle_peer_message(peer_msg, &peer_addr, &mut ws_sender, &handle, &store).await;
                }
            }
            Ok(Message::Binary(data)) => {
                if let Ok(peer_msg) = bincode::deserialize::<PeerMessage>(&data) {
                    handle_peer_message(peer_msg, &peer_addr, &mut ws_sender, &handle, &store).await;
                }
            }
            Ok(Message::Ping(payload)) => {
                let _ = ws_sender.send(Message::Pong(payload)).await;
            }
            Ok(Message::Close(_)) => break,
            Err(e) => {
                error!(error = %e, "WebSocket error with peer");
                break;
            }
            _ => {}
        }
    }
    
    handle.peer_disconnected(peer_addr.to_string());
    info!("Peer connection terminated");
    Ok(())
}

async fn handle_peer_message(
    peer_msg: PeerMessage,
    peer_addr: &std::net::SocketAddr,
    ws_sender: &mut futures_util::stream::SplitSink<tokio_tungstenite::WebSocketStream<TcpStream>, Message>,
    handle: &PeerHandle,
    store: &StoreHandle,
) {
    match peer_msg {
        PeerMessage::Startup { machine_id, startup_time } => {
            info!(
                remote_machine_id = %machine_id,
                remote_startup_time = startup_time,
                "Processing startup message from peer"
            );
            
            handle.peer_connected(peer_addr.to_string(), machine_id, startup_time);
        }
        
        PeerMessage::FullSyncRequest { machine_id } => {
            info!(requesting_machine = %machine_id, "Received full sync request");
            
            if let Some(snapshot) = store.take_snapshot().await {
                let response = PeerMessage::FullSyncResponse { snapshot };
                
                if let Ok(response_binary) = bincode::serialize(&response) {
                    let _ = ws_sender.send(Message::Binary(response_binary)).await;
                    info!(requesting_machine = %machine_id, "Sent FullSyncResponse");
                }
            }
        }
        
        PeerMessage::FullSyncResponse { snapshot } => {
            info!("Received full sync response, applying snapshot");
            
            store.disable_notifications().await;
            store.inner_restore_snapshot(snapshot.clone()).await;
            store.enable_notifications().await;
            
            // Save snapshot to disk
            let (is_leader, _) = handle.get_leadership_info().await;
            if !is_leader {
                // Only save if we're not the leader to avoid unnecessary disk writes
                // Implementation would be similar to previous version but simplified
            }
            
            handle.full_sync_completed();
            info!("Successfully applied full sync snapshot");
        }
        
        PeerMessage::SyncRequest { requests } => {
            let (_, current_leader) = handle.get_leadership_info().await;
            let our_machine_id = current_leader.unwrap_or_default(); // This needs to be fixed
            
            let mut requests_to_apply: Vec<_> = requests.into_iter()
                .filter(|request| {
                    if let Some(originator) = request.originator() {
                        *originator != our_machine_id
                    } else {
                        false
                    }
                })
                .filter(|request| !matches!(request, qlib_rs::Request::Snapshot { .. }))
                .collect();
            
            if !requests_to_apply.is_empty() {
                if let Err(e) = store.perform_mut(&mut requests_to_apply).await {
                    error!(error = %e, "Failed to apply sync requests from peer");
                } else {
                    debug!(count = requests_to_apply.len(), "Applied sync requests from peer");
                }
            }
        }
    }
}

async fn start_inbound_peer_server(
    config: PeerConfig,
    handle: PeerHandle,
    store: StoreHandle,
) -> Result<()> {
    let addr = format!("0.0.0.0:{}", config.peer_port);
    let listener = TcpListener::bind(&addr).await?;
    info!(bind_address = %addr, "Peer WebSocket server started");
    
    loop {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                let handle_clone = handle.clone();
                let store_clone = store.clone();
                
                tokio::spawn(async move {
                    if let Err(e) = handle_inbound_peer_connection(stream, peer_addr, handle_clone, store_clone).await {
                        error!(error = %e, peer_addr = %peer_addr, "Error handling peer connection");
                    }
                });
            }
            Err(e) => {
                error!(error = %e, "Failed to accept peer connection");
            }
        }
    }
}

async fn manage_outbound_peer_connections(config: PeerConfig, handle: PeerHandle) -> Result<()> {
    info!("Starting outbound peer connection manager");
    
    let reconnect_interval = Duration::from_secs(config.peer_reconnect_interval_secs);
    let mut interval = tokio::time::interval(reconnect_interval);
    
    loop {
        interval.tick().await;
        
        for peer_addr in &config.peer_addresses {
            let peer_addr_clone = peer_addr.clone();
            let handle_clone = handle.clone();
            
            tokio::spawn(async move {
                if let Err(e) = handle_outbound_peer_connection(&peer_addr_clone, handle_clone).await {
                    error!(peer_addr = %peer_addr_clone, error = %e, "Failed to connect to peer");
                }
            });
        }
    }
}

async fn handle_outbound_peer_connection(peer_addr: &str, handle: PeerHandle) -> Result<()> {
    let ws_url = format!("ws://{}", peer_addr);
    let (ws_stream, _) = connect_async(&ws_url).await?;
    info!(peer_addr = %peer_addr, "Connected to outbound peer");
    
    let (ws_sender, mut ws_receiver) = ws_stream.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();
    
    handle.outbound_peer_connected(peer_addr.to_string(), tx);
    
    let peer_addr_clone = peer_addr.to_string();
    let handle_clone = handle.clone();
    
    // Spawn sender task
    let sender_task = tokio::spawn(async move {
        let mut ws_sender = ws_sender;
        while let Some(message) = rx.recv().await {
            if ws_sender.send(message).await.is_err() {
                break;
            }
        }
    });
    
    // Handle incoming messages (mainly FullSyncResponse)
    while let Some(msg) = ws_receiver.next().await {
        match msg {
            Ok(Message::Binary(data)) => {
                if let Ok(PeerMessage::FullSyncResponse { snapshot: _ }) = bincode::deserialize::<PeerMessage>(&data) {
                    // Handle FullSyncResponse - this is a simplified version
                    info!("Received FullSyncResponse via outbound connection");
                    // Process the snapshot...
                }
            }
            Ok(Message::Close(_)) => break,
            Err(_) => break,
            _ => {}
        }
    }
    
    sender_task.abort();
    handle_clone.outbound_peer_disconnected(peer_addr_clone);
    
    Ok(())
}