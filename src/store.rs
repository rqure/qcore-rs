use qlib_rs::{et, ft, AsyncStore, Cache, CelExecutor, EntityId, EntitySchema, EntityType, FieldSchema, FieldType, NotificationSender, NotifyConfig, PageOpts, PageResult, Request, Snapshot, Snowflake, StoreTrait};
use qlib_rs::auth::{AuthorizationScope, get_scope, authenticate_subject, AuthConfig};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time::{interval, Duration};
use anyhow::Result;
use std::sync::Arc;
use std::collections::HashMap;

use crate::Services;

/// Configuration for the store service
#[derive(Debug, Clone)]
pub struct StoreConfig {
    /// Machine ID (unique identifier for this instance)
    pub machine_id: String,
}

/// Store manager request types
#[derive(Debug)]
pub enum StoreRequest {
    GetEntitySchema {
        entity_type: EntityType,
        response: oneshot::Sender<Result<EntitySchema<qlib_rs::Single>>>,
    },
    GetCompleteEntitySchema {
        entity_type: EntityType,
        response: oneshot::Sender<Result<EntitySchema<qlib_rs::Complete>>>,
    },
    GetFieldSchema {
        entity_type: EntityType,
        field_type: FieldType,
        response: oneshot::Sender<Result<FieldSchema>>,
    },
    SetFieldSchema {
        entity_type: EntityType,
        field_type: FieldType,
        schema: FieldSchema,
        response: oneshot::Sender<Result<()>>,
    },
    EntityExists {
        entity_id: EntityId,
        response: oneshot::Sender<bool>,
    },
    FieldExists {
        entity_type: EntityType,
        field_type: FieldType,
        response: oneshot::Sender<bool>,
    },
    Perform {
        requests: Vec<Request>,
        response: oneshot::Sender<Result<Vec<Request>>>,
    },
    PerformMut {
        requests: Vec<Request>,
        response: oneshot::Sender<Result<Vec<Request>>>,
    },
    PerformMap {
        requests: Vec<Request>,
        response: oneshot::Sender<Result<HashMap<FieldType, Request>>>,
    },
    FindEntitiesPaginated {
        entity_type: EntityType,
        page_opts: Option<PageOpts>,
        filter: Option<String>,
        response: oneshot::Sender<Result<PageResult<EntityId>>>,
    },
    FindEntitiesExact {
        entity_type: EntityType,
        page_opts: Option<PageOpts>,
        filter: Option<String>,
        response: oneshot::Sender<Result<PageResult<EntityId>>>,
    },
    GetEntityTypesPaginated {
        page_opts: Option<PageOpts>,
        response: oneshot::Sender<Result<PageResult<EntityType>>>,
    },
    RegisterNotification {
        config: NotifyConfig,
        sender: NotificationSender,
        response: oneshot::Sender<Result<()>>,
    },
    UnregisterNotification {
        config: NotifyConfig,
        sender: NotificationSender,
        response: oneshot::Sender<bool>,
    },
    // Inner store access methods
    InnerDisableNotifications {
        response: oneshot::Sender<()>,
    },
    InnerEnableNotifications {
        response: oneshot::Sender<()>,
    },
    InnerRestoreSnapshot {
        snapshot: Snapshot,
        response: oneshot::Sender<()>,
    },
    InnerTakeSnapshot {
        response: oneshot::Sender<Snapshot>,
    },
    InnerGetWriteChannelReceiver {
        response: oneshot::Sender<Arc<Mutex<tokio::sync::mpsc::UnboundedReceiver<Vec<Request>>>>>,
    },
    CheckRequestsAuthorization {
        client_id: EntityId,
        requests: Vec<Request>,
        response: oneshot::Sender<Result<Vec<Request>>>,
    },
    AuthenticateSubject {
        subject_name: String,
        credential: String,
        response: oneshot::Sender<Result<EntityId>>,
    },
    SetServices {
        _services: Services,
        response: oneshot::Sender<()>,
    },
}

/// Handle for communicating with store manager task
#[derive(Debug, Clone)]
pub struct StoreHandle {
    sender: mpsc::UnboundedSender<StoreRequest>,
}

impl StoreHandle {
    pub async fn get_entity_schema(&self, entity_type: &EntityType) -> Result<EntitySchema<qlib_rs::Single>> {
        let (response_tx, response_rx) = oneshot::channel();
        self.sender.send(StoreRequest::GetEntitySchema {
            entity_type: entity_type.clone(),
            response: response_tx,
        }).map_err(|_| anyhow::anyhow!("Store service has stopped"))?;
        response_rx.await.map_err(|_| anyhow::anyhow!("Store service response channel closed"))?
    }

    pub async fn get_complete_entity_schema(&self, entity_type: &EntityType) -> Result<EntitySchema<qlib_rs::Complete>> {
        let (response_tx, response_rx) = oneshot::channel();
        self.sender.send(StoreRequest::GetCompleteEntitySchema {
            entity_type: entity_type.clone(),
            response: response_tx,
        }).map_err(|_| anyhow::anyhow!("Store service has stopped"))?;
        response_rx.await.map_err(|_| anyhow::anyhow!("Store service response channel closed"))?
    }

    pub async fn get_field_schema(&self, entity_type: &EntityType, field_type: &FieldType) -> Result<FieldSchema> {
        let (response_tx, response_rx) = oneshot::channel();
        self.sender.send(StoreRequest::GetFieldSchema {
            entity_type: entity_type.clone(),
            field_type: field_type.clone(),
            response: response_tx,
        }).map_err(|_| anyhow::anyhow!("Store service has stopped"))?;
        response_rx.await.map_err(|_| anyhow::anyhow!("Store service response channel closed"))?
    }

    pub async fn set_field_schema(&self, entity_type: &EntityType, field_type: &FieldType, schema: FieldSchema) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.sender.send(StoreRequest::SetFieldSchema {
            entity_type: entity_type.clone(),
            field_type: field_type.clone(),
            schema,
            response: response_tx,
        }).map_err(|_| anyhow::anyhow!("Store service has stopped"))?;
        response_rx.await.map_err(|_| anyhow::anyhow!("Store service response channel closed"))?
    }

    pub async fn entity_exists(&self, entity_id: &EntityId) -> bool {
        let (response_tx, response_rx) = oneshot::channel();
        if self.sender.send(StoreRequest::EntityExists {
            entity_id: entity_id.clone(),
            response: response_tx,
        }).is_err() {
            return false;
        }
        response_rx.await.unwrap_or(false)
    }

    pub async fn field_exists(&self, entity_type: &EntityType, field_type: &FieldType) -> bool {
        let (response_tx, response_rx) = oneshot::channel();
        if self.sender.send(StoreRequest::FieldExists {
            entity_type: entity_type.clone(),
            field_type: field_type.clone(),
            response: response_tx,
        }).is_err() {
            return false;
        }
        response_rx.await.unwrap_or(false)
    }

    pub async fn perform(&self, requests: &mut Vec<Request>) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.sender.send(StoreRequest::Perform {
            requests: requests.clone(),
            response: response_tx,
        }).map_err(|_| anyhow::anyhow!("Store service has stopped"))?;
        match response_rx.await {
            Ok(result) => {
                match result {
                    Ok(response) => {
                        *requests = response;
                        Ok(())
                    },
                    Err(e) => Err(anyhow::anyhow!("Store service error: {}", e)),
                }
            },
            Err(_) => Err(anyhow::anyhow!("Store service response channel closed")),
        }
    }

    pub async fn perform_mut(&self, requests: &mut Vec<Request>) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.sender.send(StoreRequest::PerformMut {
            requests: requests.clone(),
            response: response_tx,
        }).map_err(|_| anyhow::anyhow!("Store service has stopped"))?;
        match response_rx.await {
            Ok(result) => {
                match result {
                    Ok(response) => {
                        *requests = response;
                        Ok(())
                    },
                    Err(e) => Err(anyhow::anyhow!("Store service error: {}", e)),
                }
            },
            Err(_) => Err(anyhow::anyhow!("Store service response channel closed")),
        }
    }

    pub async fn perform_map(&self, requests: Vec<Request>) -> Result<HashMap<FieldType, Request>> {
        let (response_tx, response_rx) = oneshot::channel();
        self.sender.send(StoreRequest::PerformMap {
            requests,
            response: response_tx,
        }).map_err(|_| anyhow::anyhow!("Store service has stopped"))?;
        response_rx.await.map_err(|_| anyhow::anyhow!("Store service response channel closed"))?
    }

    pub async fn find_entities_paginated(&self, entity_type: &EntityType, page_opts: Option<PageOpts>, filter: Option<String>) -> Result<PageResult<EntityId>> {
        let (response_tx, response_rx) = oneshot::channel();
        self.sender.send(StoreRequest::FindEntitiesPaginated {
            entity_type: entity_type.clone(),
            page_opts,
            filter,
            response: response_tx,
        }).map_err(|_| anyhow::anyhow!("Store service has stopped"))?;
        response_rx.await.map_err(|_| anyhow::anyhow!("Store service response channel closed"))?
    }

    pub async fn find_entities_exact(&self, entity_type: &EntityType, page_opts: Option<PageOpts>, filter: Option<String>) -> Result<PageResult<EntityId>> {
        let (response_tx, response_rx) = oneshot::channel();
        self.sender.send(StoreRequest::FindEntitiesExact {
            entity_type: entity_type.clone(),
            page_opts,
            filter,
            response: response_tx,
        }).map_err(|_| anyhow::anyhow!("Store service has stopped"))?;
        response_rx.await.map_err(|_| anyhow::anyhow!("Store service response channel closed"))?
    }

    pub async fn get_entity_types_paginated(&self, page_opts: Option<PageOpts>) -> Result<PageResult<EntityType>> {
        let (response_tx, response_rx) = oneshot::channel();
        self.sender.send(StoreRequest::GetEntityTypesPaginated {
            page_opts,
            response: response_tx,
        }).map_err(|_| anyhow::anyhow!("Store service has stopped"))?;
        response_rx.await.map_err(|_| anyhow::anyhow!("Store service response channel closed"))?
    }

    pub async fn register_notification(&self, config: NotifyConfig, sender: NotificationSender) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.sender.send(StoreRequest::RegisterNotification {
            config,
            sender,
            response: response_tx,
        }).map_err(|_| anyhow::anyhow!("Store service has stopped"))?;
        response_rx.await.map_err(|_| anyhow::anyhow!("Store service response channel closed"))?
    }

    pub async fn unregister_notification(&self, config: &NotifyConfig, sender: &NotificationSender) -> bool {
        let (response_tx, response_rx) = oneshot::channel();
        if self.sender.send(StoreRequest::UnregisterNotification {
            config: config.clone(),
            sender: sender.clone(),
            response: response_tx,
        }).is_err() {
            return false;
        }
        response_rx.await.unwrap_or(false)
    }

    // Inner store access methods
    pub async fn disable_notifications(&self) {
        let (response_tx, response_rx) = oneshot::channel();
        if self.sender.send(StoreRequest::InnerDisableNotifications {
            response: response_tx,
        }).is_ok() {
            let _ = response_rx.await;
        }
    }

    pub async fn enable_notifications(&self) {
        let (response_tx, response_rx) = oneshot::channel();
        if self.sender.send(StoreRequest::InnerEnableNotifications {
            response: response_tx,
        }).is_ok() {
            let _ = response_rx.await;
        }
    }

    pub async fn inner_restore_snapshot(&self, snapshot: Snapshot) {
        let (response_tx, response_rx) = oneshot::channel();
        if self.sender.send(StoreRequest::InnerRestoreSnapshot {
            snapshot,
            response: response_tx,
        }).is_ok() {
            let _ = response_rx.await;
        }
    }

    pub async fn take_snapshot(&self) -> Option<Snapshot> {
        let (response_tx, response_rx) = oneshot::channel();
        if self.sender.send(StoreRequest::InnerTakeSnapshot {
            response: response_tx,
        }).is_ok() {
            response_rx.await.ok()
        } else {
            None
        }
    }

    pub async fn inner_get_write_channel_receiver(&self) -> Option<Arc<Mutex<tokio::sync::mpsc::UnboundedReceiver<Vec<Request>>>>> {
        let (response_tx, response_rx) = oneshot::channel();
        if self.sender.send(StoreRequest::InnerGetWriteChannelReceiver {
            response: response_tx,
        }).is_ok() {
            response_rx.await.ok()
        } else {
            None
        }
    }

    /// Set services for dependencies
    pub async fn set_services(&self, services: Services) {
        let (response_tx, response_rx) = oneshot::channel();
        if self.sender.send(StoreRequest::SetServices {
            _services: services,
            response: response_tx,
        }).is_ok() {
            let _ = response_rx.await;
        }
    }

    /// Check authorization for a list of requests and return only authorized ones
    pub async fn check_requests_authorization(
        &self,
        client_id: &EntityId,
        requests: Vec<Request>,
    ) -> Result<Vec<Request>> {
        let (response_tx, response_rx) = oneshot::channel();
        self.sender.send(StoreRequest::CheckRequestsAuthorization {
            client_id: client_id.clone(),
            requests,
            response: response_tx,
        }).map_err(|_| anyhow::anyhow!("Store service has stopped"))?;
        response_rx.await.map_err(|_| anyhow::anyhow!("Store service response channel closed"))?
    }

    /// Authenticate a subject with credentials
    pub async fn authenticate_subject(
        &self,
        subject_name: &str,
        credential: &str,
    ) -> Result<EntityId> {
        let (response_tx, response_rx) = oneshot::channel();
        self.sender.send(StoreRequest::AuthenticateSubject {
            subject_name: subject_name.to_string(),
            credential: credential.to_string(),
            response: response_tx,
        }).map_err(|_| anyhow::anyhow!("Store service has stopped"))?;
        response_rx.await.map_err(|_| anyhow::anyhow!("Store service response channel closed"))?
    }
}

pub struct StoreService {
    config: StoreConfig,
    store: AsyncStore,
    permission_cache: Option<Cache>,
    cel_executor: CelExecutor,
    services: Option<Services>,
}

impl StoreService {
    pub fn spawn(config: StoreConfig) -> StoreHandle {
        let (sender, receiver) = mpsc::unbounded_channel();
        
        tokio::spawn(async move {
            let mut store = AsyncStore::new(Arc::new(Snowflake::new()));
            
            // Initialize permission cache
            let permission_cache = match Cache::new(
                &mut store,
                et::permission(),
                vec![ft::resource_type(), ft::resource_field()],
                vec![ft::scope(), ft::condition()]
            ).await {
                Ok(cache) => Some(cache),
                Err(e) => {
                    tracing::error!(error = %e, "Failed to create permission cache, authorization will be disabled");
                    None
                }
            };
            
            let mut service = StoreService {
                config,
                store,
                permission_cache,
                cel_executor: CelExecutor::new(),
                services: None,
            };

            let mut receiver = receiver;
            let mut notification_timer = interval(Duration::from_millis(100)); // Process notifications every 100ms

            // Get the write channel receiver for consuming write operations
            let write_channel_receiver = service.store.inner().get_write_channel_receiver();

            loop {
                tokio::select! {
                    request = receiver.recv() => {
                        if let Some(request) = request {
                            service.handle_request(request).await;
                        } else {
                            // Channel closed, exit the loop
                            break;
                        }
                    }
                    _ = notification_timer.tick() => {
                        // Process cache notifications periodically
                        if let Some(ref mut cache) = service.permission_cache {
                            cache.process_notifications();
                        }
                    }
                    // Process write channel requests
                    write_requests = async {
                        let mut receiver_guard = write_channel_receiver.lock().await;
                        receiver_guard.recv().await
                    } => {
                        if let Some(requests) = write_requests {
                            service.process_write_channel_requests(requests).await;
                        }
                    }
                }
            }
        });

        StoreHandle { sender }
    }

    async fn handle_request(&mut self, request: StoreRequest) {
        match request {
            StoreRequest::GetEntitySchema { entity_type, response } => {
                let result = self.store.get_entity_schema(&entity_type).await;
                let _ = response.send(result.map_err(anyhow::Error::from));
            }
            StoreRequest::GetCompleteEntitySchema { entity_type, response } => {
                let result = self.store.get_complete_entity_schema(&entity_type).await;
                let _ = response.send(result.map_err(anyhow::Error::from));
            }
            StoreRequest::GetFieldSchema { entity_type, field_type, response } => {
                let result = self.store.get_field_schema(&entity_type, &field_type).await;
                let _ = response.send(result.map_err(anyhow::Error::from));
            }
            StoreRequest::SetFieldSchema { entity_type, field_type, schema, response } => {
                let result = self.store.set_field_schema(&entity_type, &field_type, schema).await;
                let _ = response.send(result.map_err(anyhow::Error::from));
            }
            StoreRequest::EntityExists { entity_id, response } => {
                let result = self.store.entity_exists(&entity_id).await;
                let _ = response.send(result);
            }
            StoreRequest::FieldExists { entity_type, field_type, response } => {
                let result = self.store.field_exists(&entity_type, &field_type).await;
                let _ = response.send(result);
            }
            StoreRequest::Perform { mut requests, response } => {
                let result = self.store.perform(&mut requests).await.map(|_| requests);
                let _ = response.send(result.map_err(anyhow::Error::from));
            }
            StoreRequest::PerformMut { mut requests, response } => {
                let result = self.store.perform_mut(&mut requests).await.map(|_| requests);
                let _ = response.send(result.map_err(anyhow::Error::from));
            }
            StoreRequest::PerformMap { mut requests, response } => {
                let result = self.store.perform_map(&mut requests).await;
                let _ = response.send(result.map_err(anyhow::Error::from));
            }
            StoreRequest::FindEntitiesPaginated { entity_type, page_opts, filter, response } => {
                let result = self.store.find_entities_paginated(&entity_type, page_opts, filter).await;
                let _ = response.send(result.map_err(anyhow::Error::from));
            }
            StoreRequest::FindEntitiesExact { entity_type, page_opts, filter, response } => {
                let result = self.store.find_entities_exact(&entity_type, page_opts, filter).await;
                let _ = response.send(result.map_err(anyhow::Error::from));
            }
            StoreRequest::GetEntityTypesPaginated { page_opts, response } => {
                let result = self.store.get_entity_types_paginated(page_opts).await;
                let _ = response.send(result.map_err(anyhow::Error::from));
            }
            StoreRequest::RegisterNotification { config, sender, response } => {
                let result = self.store.register_notification(config, sender).await;
                let _ = response.send(result.map_err(anyhow::Error::from));
            }
            StoreRequest::UnregisterNotification { config, sender, response } => {
                let result = self.store.unregister_notification(&config, &sender).await;
                let _ = response.send(result);
            }
            StoreRequest::InnerDisableNotifications { response } => {
                self.store.inner_mut().disable_notifications();
                let _ = response.send(());
            }
            StoreRequest::InnerEnableNotifications { response } => {
                self.store.inner_mut().enable_notifications();
                let _ = response.send(());
            }
            StoreRequest::InnerRestoreSnapshot { snapshot, response } => {
                self.store.inner_mut().restore_snapshot(snapshot);
                let _ = response.send(());
            }
            StoreRequest::InnerTakeSnapshot { response } => {
                let snapshot = self.store.inner().take_snapshot();
                let _ = response.send(snapshot);
            }
            StoreRequest::InnerGetWriteChannelReceiver { response } => {
                let receiver = self.store.inner().get_write_channel_receiver();
                let _ = response.send(receiver);
            }
            StoreRequest::CheckRequestsAuthorization { client_id, requests, response } => {
                let result = self.check_requests_authorization(&client_id, requests).await;
                let _ = response.send(result);
            }
            StoreRequest::AuthenticateSubject { subject_name, credential, response } => {
                let result = self.authenticate_subject(&subject_name, &credential).await;
                let _ = response.send(result);
            }
            StoreRequest::SetServices { _services, response } => {
                self.services = Some(_services);
                let _ = response.send(());
            }
        }
    }
}

impl StoreService {
    async fn check_requests_authorization(
        &mut self,
        client_id: &EntityId,
        requests: Vec<Request>,
    ) -> Result<Vec<Request>> {
        let mut authorized_requests = Vec::new();

        // If permission cache is not available, skip authorization checks
        let permission_cache = match &self.permission_cache {
            Some(cache) => cache,
            None => {
                tracing::warn!("Permission cache not available, skipping authorization checks");
                return Ok(requests);
            }
        };

        for request in requests {
            if let Some(entity_id) = request.entity_id() {
                if let Some(field_type) = request.field_type() {
                    match get_scope(
                        &mut self.store,
                        &mut self.cel_executor,
                        permission_cache,
                        client_id,
                        entity_id,
                        field_type,
                    ).await {
                        Ok(scope) => {
                            if scope == AuthorizationScope::None {
                                return Err(anyhow::anyhow!(
                                    "Access denied: Subject {} is not authorized to access {} on entity {}",
                                    client_id, field_type, entity_id
                                ));
                            }
                            // For write operations, check if we have write access
                            if is_write_operation(&request) && scope == AuthorizationScope::ReadOnly {
                                return Err(anyhow::anyhow!(
                                    "Access denied: Subject {} only has read access to {} on entity {}",
                                    client_id, field_type, entity_id
                                ));
                            }
                            authorized_requests.push(request);
                        }
                        Err(e) => {
                            return Err(anyhow::anyhow!(
                                "Authorization error: {}", e
                            ));
                        }
                    }
                } else {
                    authorized_requests.push(request);
                }
            } else {
                authorized_requests.push(request);
            }
        }

        Ok(authorized_requests)
    }

    async fn authenticate_subject(&mut self, subject_name: &str, credential: &str) -> Result<EntityId> {
        let auth_config = AuthConfig::default();
        authenticate_subject(&mut self.store, subject_name, credential, &auth_config)
            .await
            .map_err(|e| anyhow::anyhow!("Authentication for '{}' failed: {:?}", subject_name, e))
    }

    /// Process a batch of requests from the write channel
    async fn process_write_channel_requests(&mut self, mut requests: Vec<Request>) {
        // Ensure we have services available
        let services = match &self.services {
            Some(services) => services,
            None => {
                tracing::error!("Services not available, cannot process write channel requests");
                return;
            }
        };

        // Get the machine ID from the config
        let machine_id = &self.config.machine_id;

        // Ensure the originator is set for all requests
        requests.iter_mut().for_each(|req| {
            if req.originator().is_none() {
                req.try_set_originator(machine_id.clone());
            }
        });

        // Write all requests to the WAL file - these requests have already been applied to the store
        for request in &requests {
            if let Err(e) = services.wal_handle.write_request(request.clone()).await {
                tracing::error!(
                    error = %e,
                    "Failed to write request to WAL"
                );
            }
        }

        // Send batch of requests to peers for synchronization if we have any
        let requests_to_sync: Vec<qlib_rs::Request> = requests.iter()
            .filter(|request| {
                if let Some(originator) = request.originator() {
                    originator == machine_id
                } else {
                    false
                }
            })
            .cloned()
            .collect();

        if !requests_to_sync.is_empty() {
            tracing::debug!(
                count = requests_to_sync.len(),
                "Sending sync requests to peers"
            );
            
            services.peer_handle.send_sync_message(requests_to_sync).await;
        }
    }
}

/// Helper function to check if a request is a write operation
fn is_write_operation(request: &Request) -> bool {
    matches!(request, 
        Request::Write { .. } | 
        Request::Create { .. } | 
        Request::Delete { .. } | 
        Request::SchemaUpdate { .. }
    )
}