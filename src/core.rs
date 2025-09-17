use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{Read, Write};
use std::time::Duration;
use crossbeam::channel::Sender;
use mio::{Poll, Interest, Token, Events};
use mio::net::{TcpListener as MioTcpListener, TcpStream as MioTcpStream};
use tracing::{info, warn, error, debug};
use anyhow::Result;
use std::thread;
use qlib_rs::{
    StoreMessage, EntityId, NotificationQueue, NotifyConfig,
    AuthenticationResult, Notification, Store, Cache, CelExecutor,
    PushCondition, Value, Request, Snapshot, Snowflake,
    AuthConfig, EntityType, FieldType, PageOpts, PageResult, 
    auth::{authenticate_subject, get_scope, AuthorizationScope}
};
use qlib_rs::protocol::{ProtocolMessage, ProtocolCodec, MessageBuffer};

use crate::snapshot::SnapshotHandle;
use crate::wal::WalHandle;

/// Configuration for the core service
#[derive(Debug, Clone)]
pub struct CoreConfig {
    /// Port for unified client and peer communication
    pub port: u16,
    /// Machine ID for request origination
    pub machine: String,
}

impl From<&crate::Config> for CoreConfig {
    fn from(config: &crate::Config) -> Self {
        Self {
            port: config.port,
            machine: config.machine.clone(),
        }
    }
}

/// Core service request types
#[derive(Debug)]
pub enum CoreCommand {
    Perform {
        requests: Vec<Request>,
    },
    TakeSnapshot,
    RestoreSnapshot {
        snapshot: Snapshot,
    },
    SetSnapshotHandle {
        snapshot_handle: SnapshotHandle,
    },
    SetWalHandle {
        wal_handle: crate::wal::WalHandle,
    },
}

/// Handle for communicating with core service
#[derive(Debug, Clone)]
pub struct CoreHandle {
    sender: Sender<CoreCommand>,
}

impl CoreHandle {
    pub fn perform(&self, requests: Vec<Request>) {
        self.sender.send(CoreCommand::Perform { requests }).unwrap();
    }

    pub fn take_snapshot(&self) {
        self.sender.send(CoreCommand::TakeSnapshot).unwrap();
    }

    pub fn restore_snapshot(&self, snapshot: Snapshot) {
        self.sender.send(CoreCommand::RestoreSnapshot { snapshot }).unwrap();
    }

    pub fn set_snapshot_handle(&self, snapshot_handle: SnapshotHandle) {
        self.sender.send(CoreCommand::SetSnapshotHandle { snapshot_handle }).unwrap();
    }

    pub fn set_wal_handle(&self, wal_handle: crate::wal::WalHandle) {
        self.sender.send(CoreCommand::SetWalHandle { wal_handle }).unwrap();
    }
}

/// Client connection information  
#[derive(Debug)]
struct Connection {
    stream: MioTcpStream,
    addr_string: String,
    authenticated: bool,
    client_id: Option<EntityId>,
    notification_queue: NotificationQueue,
    notification_configs: HashSet<NotifyConfig>,
    pending_notifications: VecDeque<Notification>,
    outbound_messages: VecDeque<Vec<u8>>,
    message_buffer: MessageBuffer,
}

/// Core service that handles both client and peer connections
pub struct CoreService {
    config: CoreConfig,
    listener: MioTcpListener,
    poll: Poll,
    connections: HashMap<Token, Connection>,
    next_token: usize,
    
    // Store and related components (replacing StoreService)
    store: Store,
    permission_cache: Cache,
    cel_executor: CelExecutor,
    
    // Handles to other services  
    snapshot_handle: Option<SnapshotHandle>,
    wal_handle: Option<WalHandle>,
}

const LISTENER_TOKEN: Token = Token(0);

impl CoreService {
    /// Create a new core service with peer configuration
    pub fn new(
        config: CoreConfig,
    ) -> Result<Self> {
        let addr = format!("0.0.0.0:{}", config.port).parse()?;
        let mut listener = MioTcpListener::bind(addr)?;
        let poll = Poll::new()?;
        
        poll.registry().register(&mut listener, LISTENER_TOKEN, Interest::READABLE)?;
        
        info!(bind_address = %addr, "Core service unified TCP server initialized");
        
        let mut store = Store::new(Snowflake::new());
        
        let (permission_cache, _notification_queue) = Cache::new(
            &mut store,
            qlib_rs::et::permission(),
            vec![qlib_rs::ft::resource_type(), qlib_rs::ft::resource_field()],
            vec![qlib_rs::ft::scope(), qlib_rs::ft::condition()]
        ).map_err(|e| anyhow::anyhow!("Failed to create permission cache: {}", e))?;
        let cel_executor = CelExecutor::new();
        
        Ok(Self {
            config,
            listener,
            poll,
            connections: HashMap::new(),
            next_token: 1,
            store,
            permission_cache,
            cel_executor,
            snapshot_handle: None,
            wal_handle: None,
        })
    }

    /// Spawn the core service in its own thread and return a handle
    pub fn spawn(config: CoreConfig) -> CoreHandle {
        let (sender, receiver) = crossbeam::channel::unbounded();
        let handle = CoreHandle { sender };

        thread::spawn(move || {
            let mut service = match Self::new(config) {
                Ok(s) => s,
                Err(e) => {
                    error!("Failed to create core service: {}", e);
                    return;
                }
            };
            
            let mut events = Events::with_capacity(1024);
            
            loop {                
                // Poll for I/O events with no timeout for maximum responsiveness
                // Periodic operations are now handled by the self-connecting periodic client
                service.poll.poll(&mut events, Some(Duration::from_millis(100))).unwrap();
                
                // Handle all mio events
                for event in events.iter() {
                    match event.token() {
                        LISTENER_TOKEN => {
                            if event.is_readable() {
                                
                            }
                        }
                        token => {
                            
                        }
                    }
                }

                while let Ok(request) = receiver.try_recv() {
                    match request {
                        CoreCommand::Perform { requests } => {}
                    }
                }

                // Drain the write queue from the store
                while let Some(request) = service.store.write_queue.pop() {
                    if let Some(wal_handle) = &service.wal_handle {
                        wal_handle.append_request(request.clone());
                    }
                }
            }
        });

        handle
    }
    
    
    /// Check authorization for a list of requests and return only authorized ones
    fn check_requests_authorization(
        &mut self,
        client_id: EntityId,
        requests: Vec<Request>,
    ) -> Result<Vec<Request>> {
        let mut authorized_requests = Vec::new();
        
        for request in requests {
            // Extract entity_id and field_type from the request
            let authorization_needed = match &request {
                Request::Read { entity_id, field_type, .. } => Some((entity_id, field_type)),
                Request::Write { entity_id, field_type, .. } => Some((entity_id, field_type)),
                Request::Create { .. } => None, // No field-level authorization for creation
                Request::Delete { .. } => None, // No field-level authorization for deletion
                Request::SchemaUpdate { .. } => None, // No field-level authorization for schema updates
                _ => None, // For other request types, skip authorization check
            };
            
            if let Some((entity_id, field_type)) = authorization_needed {
                let scope = get_scope(&self.store, &mut self.cel_executor, &self.permission_cache, &client_id, entity_id, field_type)?;
                
                match scope {
                    AuthorizationScope::ReadOnly | AuthorizationScope::ReadWrite => {
                        authorized_requests.push(request);
                    }
                    AuthorizationScope::None => {
                        return Err(anyhow::anyhow!("Client not authorized for request: {}", request));
                    }
                }
            } else {
                // No authorization needed for this request type
                authorized_requests.push(request);
            }
        }
        
        Ok(authorized_requests)
    }
    
}