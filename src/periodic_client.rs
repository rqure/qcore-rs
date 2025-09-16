use std::thread;
use std::time::{Duration, Instant};
use crossbeam::channel::{Receiver, Sender, unbounded, select};
use tracing::{info, warn, error, debug};
use qlib_rs::{StoreProxy, Request, Value, PushCondition, et, ft, sread, swrite, schoice, sref};
use anyhow::Result;

/// Configuration for the periodic client
#[derive(Debug, Clone)]
pub struct PeriodicClientConfig {
    /// The server address to connect to (e.g., "127.0.0.1:8080")
    pub server_address: String,
    /// The machine ID for this instance
    pub machine_id: String,
    /// Subject name for authentication (typically the service name)
    pub subject_name: String,
    /// Credential for authentication (service secret)
    pub credential: String,
    /// Interval for heartbeat operations (in seconds)
    pub heartbeat_interval_secs: u64,
    /// Interval for misc operations (in milliseconds)
    pub misc_interval_ms: u64,
}

impl From<&crate::Config> for PeriodicClientConfig {
    fn from(config: &crate::Config) -> Self {
        Self {
            server_address: format!("127.0.0.1:{}", config.port),
            machine_id: config.machine.clone(),
            subject_name: "qcore-periodic".to_string(), // Service name for periodic operations
            credential: "periodic-secret".to_string(), // TODO: Load from secure config
            heartbeat_interval_secs: 1,
            misc_interval_ms: 10,
        }
    }
}

/// Commands that can be sent to the periodic client
#[derive(Debug)]
pub enum PeriodicClientCommand {
    /// Stop the periodic client
    Stop,
    /// Trigger an immediate heartbeat
    TriggerHeartbeat,
    /// Trigger immediate misc operations
    TriggerMiscOperations,
}

/// Handle for communicating with the periodic client
#[derive(Debug, Clone)]
pub struct PeriodicClientHandle {
    command_sender: Sender<PeriodicClientCommand>,
}

impl PeriodicClientHandle {
    /// Stop the periodic client
    pub fn stop(&self) {
        let _ = self.command_sender.send(PeriodicClientCommand::Stop);
    }
    
    /// Trigger an immediate heartbeat
    pub fn trigger_heartbeat(&self) {
        let _ = self.command_sender.send(PeriodicClientCommand::TriggerHeartbeat);
    }
    
    /// Trigger immediate misc operations
    pub fn trigger_misc_operations(&self) {
        let _ = self.command_sender.send(PeriodicClientCommand::TriggerMiscOperations);
    }
}

/// The periodic client that connects to the server and performs periodic operations
pub struct PeriodicClient {
    config: PeriodicClientConfig,
    command_receiver: Receiver<PeriodicClientCommand>,
}

impl PeriodicClient {
    /// Create a new periodic client
    pub fn new(config: PeriodicClientConfig) -> (Self, PeriodicClientHandle) {
        let (command_sender, command_receiver) = unbounded();
        
        let client = Self {
            config,
            command_receiver,
        };
        
        let handle = PeriodicClientHandle { command_sender };
        
        (client, handle)
    }
    
    /// Spawn the periodic client in its own thread
    pub fn spawn(config: PeriodicClientConfig) -> Result<PeriodicClientHandle> {
        let (mut client, handle) = Self::new(config);
        
        thread::spawn(move || {
            if let Err(e) = client.run() {
                error!("Periodic client error: {}", e);
            }
            info!("Periodic client has stopped");
        });
        
        Ok(handle)
    }
    
    /// Run the periodic client event loop
    pub fn run(&mut self) -> Result<()> {
        info!("Starting periodic client");
        
        // Create store proxy inside this thread
        let mut store_proxy: Option<StoreProxy> = None;
        
        // Try to establish initial connection
        match self.connect_to_server() {
            Ok(proxy) => store_proxy = Some(proxy),
            Err(e) => warn!("Initial connection failed: {}", e),
        }
        
        let heartbeat_interval = Duration::from_secs(self.config.heartbeat_interval_secs);
        let misc_interval = Duration::from_millis(self.config.misc_interval_ms);
        
        let mut last_heartbeat = Instant::now();
        let mut last_misc = Instant::now();
        
        loop {
            let now = Instant::now();
            
            // Calculate next operation times
            let next_heartbeat = last_heartbeat + heartbeat_interval;
            let next_misc = last_misc + misc_interval;
            let next_operation = next_heartbeat.min(next_misc);
            
            // Calculate timeout for select operation
            let timeout = if now >= next_operation {
                Duration::from_millis(0) // Execute immediately
            } else {
                next_operation - now
            };
            
            // Wait for commands or timeout
            select! {
                recv(self.command_receiver) -> command => {
                    match command {
                        Ok(PeriodicClientCommand::Stop) => {
                            info!("Received stop command, shutting down periodic client");
                            break;
                        }
                        Ok(PeriodicClientCommand::TriggerHeartbeat) => {
                            debug!("Received trigger heartbeat command");
                            if let Err(e) = self.perform_heartbeat(&mut store_proxy) {
                                warn!("Triggered heartbeat failed: {}", e);
                            }
                        }
                        Ok(PeriodicClientCommand::TriggerMiscOperations) => {
                            debug!("Received trigger misc operations command");
                            if let Err(e) = self.perform_misc_operations(&mut store_proxy) {
                                warn!("Triggered misc operations failed: {}", e);
                            }
                        }
                        Err(_) => {
                            debug!("Command channel closed, stopping periodic client");
                            break;
                        }
                    }
                }
                default(timeout) => {
                    // Timeout occurred, check which operations need to run
                    let now = Instant::now();
                    
                    if now >= next_heartbeat {
                        if let Err(e) = self.perform_heartbeat(&mut store_proxy) {
                            warn!("Heartbeat operation failed: {}", e);
                            // Try to reconnect on failure
                            if let Err(e) = self.reconnect_to_server(&mut store_proxy) {
                                error!("Failed to reconnect after heartbeat failure: {}", e);
                            }
                        } else {
                            last_heartbeat = now;
                        }
                    }
                    
                    if now >= next_misc {
                        if let Err(e) = self.perform_misc_operations(&mut store_proxy) {
                            warn!("Misc operations failed: {}", e);
                            // Try to reconnect on failure
                            if let Err(e) = self.reconnect_to_server(&mut store_proxy) {
                                error!("Failed to reconnect after misc operations failure: {}", e);
                            }
                        } else {
                            last_misc = now;
                        }
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Connect to the server and authenticate
    fn connect_to_server(&self) -> Result<StoreProxy> {
        info!(
            server_address = %self.config.server_address,
            subject_name = %self.config.subject_name,
            "Connecting to server"
        );
        
        match StoreProxy::connect_and_authenticate(
            &self.config.server_address,
            &self.config.subject_name,
            &self.config.credential,
        ) {
            Ok(proxy) => {
                info!("Successfully connected and authenticated to server");
                Ok(proxy)
            }
            Err(e) => {
                error!("Failed to connect to server: {}", e);
                Err(anyhow::anyhow!("Connection failed: {}", e))
            }
        }
    }
    
    /// Reconnect to the server if connection is lost
    fn reconnect_to_server(&self, store_proxy: &mut Option<StoreProxy>) -> Result<()> {
        warn!("Attempting to reconnect to server");
        *store_proxy = None;
        
        // Exponential backoff for reconnection
        let mut backoff_ms = 100;
        let max_backoff_ms = 5000;
        
        loop {
            match self.connect_to_server() {
                Ok(proxy) => {
                    info!("Successfully reconnected to server");
                    *store_proxy = Some(proxy);
                    return Ok(());
                }
                Err(e) => {
                    warn!("Reconnection attempt failed: {}, retrying in {}ms", e, backoff_ms);
                    thread::sleep(Duration::from_millis(backoff_ms));
                    backoff_ms = (backoff_ms * 2).min(max_backoff_ms);
                }
            }
        }
    }
    
    /// Perform heartbeat operation by writing to the store
    fn perform_heartbeat(&self, store_proxy: &mut Option<StoreProxy>) -> Result<()> {
        if let Some(proxy) = store_proxy {
            debug!("Performing heartbeat operation via store operations");
            
            // Find this machine's candidate entity
            let machine = &self.config.machine_id;
            let candidates = proxy.find_entities(
                &et::candidate(),
                Some(format!("Name == 'qcore' && Parent->Name == '{}'", machine))
            )?;
            
            if let Some(candidate) = candidates.first() {
                // Write heartbeat and make_me fields
                let requests = vec![
                    swrite!(candidate.clone(), ft::heartbeat(), schoice!(0)),
                    swrite!(candidate.clone(), ft::make_me(), schoice!(1), PushCondition::Changes)
                ];
                
                proxy.perform(requests)?;
                debug!("Heartbeat operation completed successfully");
                Ok(())
            } else {
                warn!("No candidate found for machine: {}", machine);
                Err(anyhow::anyhow!("No candidate found for machine: {}", machine))
            }
        } else {
            Err(anyhow::anyhow!("Not connected to server"))
        }
    }
    
    /// Perform misc operations by reading/writing to the store  
    fn perform_misc_operations(&self, store_proxy: &mut Option<StoreProxy>) -> Result<()> {
        if let Some(proxy) = store_proxy {
            debug!("Performing misc operations via store operations");
            
            // Check if we're the leader first by checking fault tolerance entities
            let fault_tolerances = proxy.find_entities(&et::fault_tolerance(), None)?;
            
            // Find our candidate entity
            let machine = &self.config.machine_id;
            let our_candidates = proxy.find_entities(
                &et::candidate(),
                Some(format!("Name == 'qcore' && Parent->Name == '{}'", machine))
            )?;
            
            let me_as_candidate = our_candidates.first();
            
            // Handle fault tolerance management for each fault tolerance entity
            for ft_entity_id in fault_tolerances {
                // Read the fault tolerance entity's fields
                let ft_fields = proxy.perform(vec![
                    sread!(ft_entity_id.clone(), ft::candidate_list()),
                    sread!(ft_entity_id.clone(), ft::available_list()),
                    sread!(ft_entity_id.clone(), ft::current_leader())
                ])?;
                
                // Get the candidate list
                let default_candidates = Vec::new();
                let candidates = ft_fields
                    .get(0)
                    .and_then(|r| match r {
                        Request::Read { value: Some(value), .. } => value.as_entity_list(),
                        _ => None,
                    })
                    .unwrap_or(&default_candidates);
                
                // Determine available candidates by checking their heartbeat
                let mut available = Vec::new();
                for candidate_id in candidates.iter() {
                    let candidate_fields = proxy.perform(vec![
                        sread!(candidate_id.clone(), ft::heartbeat()),
                        sread!(candidate_id.clone(), ft::make_me()),
                        sread!(candidate_id.clone(), ft::death_detection_timeout()),
                    ])?;
                    
                    // Check if candidate is available based on heartbeat timing
                    if let (Some(heartbeat_req), Some(make_me_req), Some(timeout_req)) = (
                        candidate_fields.get(0),
                        candidate_fields.get(1),
                        candidate_fields.get(2)
                    ) {
                        if let (
                            Request::Read { write_time: Some(heartbeat_time), .. },
                            Request::Read { value: Some(make_me_value), .. },
                            Request::Read { value: Some(timeout_value), .. }
                        ) = (heartbeat_req, make_me_req, timeout_req) {
                            if let (Some(make_me_choice), Some(timeout_millis)) = (
                                make_me_value.as_choice(),
                                timeout_value.as_int()
                            ) {
                                // Check if candidate wants to be available and heartbeat is recent
                                if make_me_choice == 1 {
                                    let now = qlib_rs::now();
                                    let death_timeout = time::Duration::milliseconds(timeout_millis);
                                    
                                    if *heartbeat_time + death_timeout > now {
                                        available.push(candidate_id.clone());
                                    }
                                }
                            }
                        }
                    }
                }
                
                // Update the fault tolerance entity
                let mut requests = vec![
                    swrite!(ft_entity_id.clone(), ft::available_list(), Some(Value::EntityList(available.clone())), PushCondition::Changes),
                ];
                
                // If we're in the candidate list, set ourselves as the current leader
                if let Some(me_as_candidate) = me_as_candidate {
                    if candidates.contains(me_as_candidate) {
                        requests.push(
                            swrite!(ft_entity_id.clone(), ft::current_leader(), sref!(Some(me_as_candidate.clone())), PushCondition::Changes)
                        );
                    }
                }
                
                proxy.perform(requests)?;
            }
            
            debug!("Misc operations completed successfully");
            Ok(())
        } else {
            Err(anyhow::anyhow!("Not connected to server"))
        }
    }
}