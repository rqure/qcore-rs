use tokio::sync::mpsc::{Receiver, Sender};
use qlib_rs::{et, ft, AsyncStore, Cache, CelExecutor, EntityId, EntitySchema, EntityType, FieldSchema, FieldType, NotificationSender, NotifyConfig, PageOpts, PageResult, Request, Snapshot, Snowflake, StoreTrait};
use qlib_rs::auth::{AuthorizationScope, get_scope, authenticate_subject, AuthConfig};
use tracing::{error};
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
        response: tokio::sync::oneshot::Sender<Result<EntitySchema<qlib_rs::Single>>>,
    },
    GetCompleteEntitySchema {
        entity_type: EntityType,
        response: tokio::sync::oneshot::Sender<Result<EntitySchema<qlib_rs::Complete>>>,
    },
    GetFieldSchema {
        entity_type: EntityType,
        field_type: FieldType,
        response: tokio::sync::oneshot::Sender<Result<FieldSchema>>,
    },
    SetFieldSchema {
        entity_type: EntityType,
        field_type: FieldType,
        schema: FieldSchema,
        response: tokio::sync::oneshot::Sender<Result<()>>,
    },
    EntityExists {
        entity_id: EntityId,
        response: tokio::sync::oneshot::Sender<bool>,
    },
    FieldExists {
        entity_type: EntityType,
        field_type: FieldType,
        response: tokio::sync::oneshot::Sender<bool>,
    },
    Perform {
        requests: Vec<Request>,
        response: tokio::sync::oneshot::Sender<Result<Vec<Request>>>,
    },
    PerformMut {
        requests: Vec<Request>,
        response: tokio::sync::oneshot::Sender<Result<Vec<Request>>>,
    },
    PerformMap {
        requests: Vec<Request>,
        response: tokio::sync::oneshot::Sender<Result<HashMap<FieldType, Request>>>,
    },
    FindEntitiesPaginated {
        entity_type: EntityType,
        page_opts: Option<PageOpts>,
        filter: Option<String>,
        response: tokio::sync::oneshot::Sender<Result<PageResult<EntityId>>>,
    },
    FindEntitiesExact {
        entity_type: EntityType,
        page_opts: Option<PageOpts>,
        filter: Option<String>,
        response: tokio::sync::oneshot::Sender<Result<PageResult<EntityId>>>,
    },
    GetEntityTypesPaginated {
        page_opts: Option<PageOpts>,
        response: tokio::sync::oneshot::Sender<Result<PageResult<EntityType>>>,
    },
    RegisterNotification {
        config: NotifyConfig,
        sender: NotificationSender,
        response: tokio::sync::oneshot::Sender<Result<()>>,
    },
    UnregisterNotification {
        config: NotifyConfig,
        sender: NotificationSender,
        response: tokio::sync::oneshot::Sender<bool>,
    },
    // Inner store access methods
    InnerDisableNotifications {
        response: tokio::sync::oneshot::Sender<()>,
    },
    InnerEnableNotifications {
        response: tokio::sync::oneshot::Sender<()>,
    },
    InnerRestoreSnapshot {
        snapshot: Snapshot,
        response: tokio::sync::oneshot::Sender<()>,
    },
    InnerTakeSnapshot {
        response: tokio::sync::oneshot::Sender<Snapshot>,
    },
    CheckRequestsAuthorization {
        client_id: EntityId,
        requests: Vec<Request>,
        response: tokio::sync::oneshot::Sender<Result<Vec<Request>>>,
    },
    AuthenticateSubject {
        subject_name: String,
        credential: String,
        response: tokio::sync::oneshot::Sender<Result<EntityId>>,
    },
    SetServices {
        services: Services,
        response: tokio::sync::oneshot::Sender<()>,
    },
}

/// Handle for communicating with store manager task
#[derive(Debug, Clone)]
pub struct StoreHandle {
    sender: Sender<StoreRequest>,
}

impl StoreHandle {
    pub async fn get_entity_schema(&self, entity_type: &EntityType) -> Result<EntitySchema<qlib_rs::Single>> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        self.sender.send(StoreRequest::GetEntitySchema {
            entity_type: entity_type.clone(),
            response: response_tx,
        }).await.map_err(|e| anyhow::anyhow!("Store service has stopped: {}", e))?;
        response_rx.await.map_err(|e| anyhow::anyhow!("Store service response channel closed: {}", e))?
    }

    pub async fn get_complete_entity_schema(&self, entity_type: &EntityType) -> Result<EntitySchema<qlib_rs::Complete>> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        self.sender.send(StoreRequest::GetCompleteEntitySchema {
            entity_type: entity_type.clone(),
            response: response_tx,
        }).await.map_err(|e| anyhow::anyhow!("Store service has stopped: {}", e))?;
        response_rx.await.map_err(|e| anyhow::anyhow!("Store service response channel closed: {}", e))?
    }

    pub async fn get_field_schema(&self, entity_type: &EntityType, field_type: &FieldType) -> Result<FieldSchema> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        self.sender.send(StoreRequest::GetFieldSchema {
            entity_type: entity_type.clone(),
            field_type: field_type.clone(),
            response: response_tx,
        }).await.map_err(|e| anyhow::anyhow!("Store service has stopped: {}", e))?;
        response_rx.await.map_err(|e| anyhow::anyhow!("Store service response channel closed: {}", e))?
    }

    pub async fn set_field_schema(&self, entity_type: &EntityType, field_type: &FieldType, schema: FieldSchema) -> Result<()> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        self.sender.send(StoreRequest::SetFieldSchema {
            entity_type: entity_type.clone(),
            field_type: field_type.clone(),
            schema,
            response: response_tx,
        }).await.map_err(|e| anyhow::anyhow!("Store service has stopped: {}", e))?;
        response_rx.await.map_err(|e| anyhow::anyhow!("Store service response channel closed: {}", e))?
    }

    pub async fn entity_exists(&self, entity_id: &EntityId) -> bool {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        if let Ok(_) = self.sender.send(StoreRequest::EntityExists {
            entity_id: entity_id.clone(),
            response: response_tx,
        }).await {
            response_rx.await.unwrap_or(false)
        } else {
            false
        }
    }

    pub async fn field_exists(&self, entity_type: &EntityType, field_type: &FieldType) -> bool {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        if let Ok(_) = self.sender.send(StoreRequest::FieldExists {
            entity_type: entity_type.clone(),
            field_type: field_type.clone(),
            response: response_tx,
        }).await {
            response_rx.await.unwrap_or(false)
        } else {
            false
        }
    }

    pub async fn perform(&self, requests: Vec<Request>) -> Result<Vec<Request>> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        self.sender.send(StoreRequest::Perform {
            requests,
            response: response_tx,
        }).await.map_err(|e| anyhow::anyhow!("Store service has stopped: {}", e))?;
        match response_rx.await {
            Ok(result) => {
                match result {
                    Ok(response) => Ok(response),
                    Err(e) => Err(anyhow::anyhow!("Store service error: {}", e)),
                }
            },
            Err(_) => Err(anyhow::anyhow!("Store service response channel closed")),
        }
    }

    pub async fn perform_mut(&self, requests: Vec<Request>) -> Result<Vec<Request>> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        self.sender.send(StoreRequest::PerformMut {
            requests,
            response: response_tx,
        }).await.map_err(|e| anyhow::anyhow!("Store service has stopped: {}", e))?;
        match response_rx.await {
            Ok(result) => {
                match result {
                    Ok(response) =>  Ok(response),
                    Err(e) => Err(anyhow::anyhow!("Store service error: {}", e)),
                }
            },
            Err(_) => Err(anyhow::anyhow!("Store service response channel closed")),
        }
    }

    pub async fn perform_map(&self, requests: Vec<Request>) -> Result<HashMap<FieldType, Request>> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        self.sender.send(StoreRequest::PerformMap {
            requests,
            response: response_tx,
        }).await.map_err(|e| anyhow::anyhow!("Store service has stopped: {}", e))?;
        response_rx.await.map_err(|e| anyhow::anyhow!("Store service response channel closed: {}", e))?
    }

    pub async fn find_entities_paginated(&self, entity_type: &EntityType, page_opts: Option<PageOpts>, filter: Option<String>) -> Result<PageResult<EntityId>> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        self.sender.send(StoreRequest::FindEntitiesPaginated {
            entity_type: entity_type.clone(),
            page_opts,
            filter,
            response: response_tx,
        }).await.map_err(|e| anyhow::anyhow!("Store service has stopped: {}", e))?;
        response_rx.await.map_err(|e| anyhow::anyhow!("Store service response channel closed: {}", e))?
    }

    pub async fn find_entities_exact(&self, entity_type: &EntityType, page_opts: Option<PageOpts>, filter: Option<String>) -> Result<PageResult<EntityId>> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        self.sender.send(StoreRequest::FindEntitiesExact {
            entity_type: entity_type.clone(),
            page_opts,
            filter,
            response: response_tx,
        }).await.map_err(|e| anyhow::anyhow!("Store service has stopped: {}", e))?;
        response_rx.await.map_err(|e| anyhow::anyhow!("Store service response channel closed: {}", e))?
    }

    pub async fn get_entity_types_paginated(&self, page_opts: Option<PageOpts>) -> Result<PageResult<EntityType>> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        self.sender.send(StoreRequest::GetEntityTypesPaginated {
            page_opts,
            response: response_tx,
        }).await.map_err(|e| anyhow::anyhow!("Store service has stopped: {}", e))?;
        response_rx.await.map_err(|e| anyhow::anyhow!("Store service response channel closed: {}", e))?
    }

    pub async fn register_notification(&self, config: NotifyConfig, sender: NotificationSender) -> Result<()> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        self.sender.send(StoreRequest::RegisterNotification {
            config,
            sender,
            response: response_tx,
        }).await.map_err(|e| anyhow::anyhow!("Store service has stopped: {}", e))?;
        response_rx.await.map_err(|e| anyhow::anyhow!("Store service response channel closed: {}", e))?
    }

    pub async fn unregister_notification(&self, config: &NotifyConfig, sender: &NotificationSender) -> bool {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        if let Ok(_) = self.sender.send(StoreRequest::UnregisterNotification {
            config: config.clone(),
            sender: sender.clone(),
            response: response_tx,
        }).await {
            response_rx.await.unwrap_or(false)
        } else {
            false
        }
    }

    // Inner store access methods
    pub async fn disable_notifications(&self) {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        if let Ok(_) = self.sender.send(StoreRequest::InnerDisableNotifications {
            response: response_tx,
        }).await {
            if let Err(e) = response_rx.await {
                error!(error = %e, "Store service InnerDisableNotifications response channel closed");
            }
        }
    }

    pub async fn enable_notifications(&self) {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        if let Ok(_) = self.sender.send(StoreRequest::InnerEnableNotifications {
            response: response_tx,
        }).await {
            if let Err(e) = response_rx.await {
                error!(error = %e, "Store service InnerEnableNotifications response channel closed");
            }
        }
    }

    pub async fn restore_snapshot(&self, snapshot: Snapshot) {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        if let Ok(_) = self.sender.send(StoreRequest::InnerRestoreSnapshot {
            snapshot,
            response: response_tx,
        }).await {
            if let Err(e) = response_rx.await {
                error!(error = %e, "Store service InnerRestoreSnapshot response channel closed");
            }
        }
    }

    pub async fn take_snapshot(&self) -> Option<Snapshot> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        if let Ok(_) = self.sender.send(StoreRequest::InnerTakeSnapshot {
            response: response_tx,
        }).await {
            response_rx.await.ok()
        } else {
            None
        }
    }

    /// Set services for dependencies
    pub async fn set_services(&self, services: Services) {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        if let Ok(_) = self.sender.send(StoreRequest::SetServices {
            services,
            response: response_tx,
        }).await {
            if let Err(e) = response_rx.await {
                error!(error = %e, "Store service SetServices response channel closed");
            }
        }
    }

    /// Check authorization for a list of requests and return only authorized ones
    pub async fn check_requests_authorization(
        &self,
        client_id: &EntityId,
        requests: Vec<Request>,
    ) -> Result<Vec<Request>> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        self.sender.send(StoreRequest::CheckRequestsAuthorization {
            client_id: client_id.clone(),
            requests,
            response: response_tx,
        }).await.map_err(|e| anyhow::anyhow!("Store service has stopped: {}", e))?;
        response_rx.await.map_err(|e| anyhow::anyhow!("Store service response channel closed: {}", e))?
    }

    /// Authenticate a subject with credentials
    pub async fn authenticate_subject(
        &self,
        subject_name: &str,
        credential: &str,
    ) -> Result<EntityId> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        self.sender.send(StoreRequest::AuthenticateSubject {
            subject_name: subject_name.to_string(),
            credential: credential.to_string(),
            response: response_tx,
        }).await.map_err(|e| anyhow::anyhow!("Store service has stopped: {}", e))?;
        response_rx.await.map_err(|e| anyhow::anyhow!("Store service response channel closed: {}", e))?
    }
}

pub struct StoreService {
    config: StoreConfig,
    store: AsyncStore,
    permission_cache: Cache,
    cel_executor: CelExecutor,
    write_channel_task_spawned: bool,
}

impl StoreService {
    pub fn spawn(config: StoreConfig) -> StoreHandle {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(131072);

        tokio::spawn(async move {   
            let mut store = AsyncStore::new(Arc::new(Snowflake::new()));
            
            // Initialize permission cache
            let (permission_cache, mut permission_notification_receiver) = match Cache::new(
                &mut store,
                et::permission(),
                vec![ft::resource_type(), ft::resource_field()],
                vec![ft::scope(), ft::condition()]
            ).await {
                Ok((cache, receiver)) => (cache, receiver),
                Err(e) => {
                    panic!("Failed to create permission cache, authorization will be disabled: {}", e);
                }
            };
            
            // Get the write channel receiver for consuming write operations            
            let mut service = StoreService {
                config: config.clone(),
                store,
                permission_cache,
                cel_executor: CelExecutor::new(),
                write_channel_task_spawned: false,
            };

            loop {
                tokio::select! {
                    request = receiver.recv() => {
                        match request {
                            Some(request) => {
                                service.handle_request(request).await;
                            }
                            None => {
                                tracing::error!("Store service request channel closed");
                                break;
                            }
                        }
                    }
                    notification = permission_notification_receiver.recv() => {
                        match notification {
                            Some(notification) => {
                                service.permission_cache.process_notification(notification);
                            }
                            None => {
                                tracing::error!("Store service permission notification channel closed");
                            }
                        }
                    }
                }
            }

            panic!("Store service has stopped unexpectedly");
        });

        StoreHandle { sender }
    }

    async fn handle_request(&mut self, request: StoreRequest) {
        match request {
            StoreRequest::GetEntitySchema { entity_type, response } => {
                let result = self.store.get_entity_schema(&entity_type).await;
                if let Err(_) = response.send(result.map_err(anyhow::Error::from)) {
                    tracing::error!("Failed to send GetEntitySchema response");
                }
            }
            StoreRequest::GetCompleteEntitySchema { entity_type, response } => {
                let result = self.store.get_complete_entity_schema(&entity_type).await;
                if let Err(_) = response.send(result.map_err(anyhow::Error::from)) {
                    tracing::error!("Failed to send GetCompleteEntitySchema response");
                }
            }
            StoreRequest::GetFieldSchema { entity_type, field_type, response } => {
                let result = self.store.get_field_schema(&entity_type, &field_type).await;
                if let Err(_) = response.send(result.map_err(anyhow::Error::from)) {
                    tracing::error!("Failed to send GetFieldSchema response");
                }
            }
            StoreRequest::SetFieldSchema { entity_type, field_type, schema, response } => {
                let result = self.store.set_field_schema(&entity_type, &field_type, schema).await;
                if let Err(_) = response.send(result.map_err(anyhow::Error::from)) {
                    tracing::error!("Failed to send SetFieldSchema response");
                }
            }
            StoreRequest::EntityExists { entity_id, response } => {
                let result = self.store.entity_exists(&entity_id).await;
                if let Err(_) = response.send(result) {
                    tracing::error!("Failed to send EntityExists response");
                }
            }
            StoreRequest::FieldExists { entity_type, field_type, response } => {
                let result = self.store.field_exists(&entity_type, &field_type).await;
                if let Err(_) = response.send(result) {
                    tracing::error!("Failed to send FieldExists response");
                }
            }
            StoreRequest::Perform { requests, response } => {
                let result = self.store.perform(requests).await;
                if let Err(_) = response.send(result.map_err(anyhow::Error::from)) {
                    tracing::error!("Failed to send Perform response");
                }
            }
            StoreRequest::PerformMut { requests, response } => {
                let result = self.store.perform_mut(requests).await;
                if let Err(_) = response.send(result.map_err(anyhow::Error::from)) {
                    tracing::error!("Failed to send PerformMut response");
                }
            }
            StoreRequest::PerformMap { requests, response } => {
                let result = self.store.perform_map(requests).await;
                if let Err(_) = response.send(result.map_err(anyhow::Error::from)) {
                    tracing::error!("Failed to send PerformMap response");
                }
            }
            StoreRequest::FindEntitiesPaginated { entity_type, page_opts, filter, response } => {
                let result = self.store.find_entities_paginated(&entity_type, page_opts, filter).await;
                if let Err(_) = response.send(result.map_err(anyhow::Error::from)) {
                    tracing::error!("Failed to send FindEntitiesPaginated response");
                }
            }
            StoreRequest::FindEntitiesExact { entity_type, page_opts, filter, response } => {
                let result = self.store.find_entities_exact(&entity_type, page_opts, filter).await;
                if let Err(_) = response.send(result.map_err(anyhow::Error::from)) {
                    tracing::error!("Failed to send FindEntitiesExact response");
                }
            }
            StoreRequest::GetEntityTypesPaginated { page_opts, response } => {
                let result = self.store.get_entity_types_paginated(page_opts).await;
                if let Err(_) = response.send(result.map_err(anyhow::Error::from)) {
                    tracing::error!("Failed to send GetEntityTypesPaginated response");
                }
            }
            StoreRequest::RegisterNotification { config, sender, response } => {
                let result = self.store.register_notification(config, sender).await;
                if let Err(_) = response.send(result.map_err(anyhow::Error::from)) {
                    tracing::error!("Failed to send RegisterNotification response");
                }
            }
            StoreRequest::UnregisterNotification { config, sender, response } => {
                let result = self.store.unregister_notification(&config, &sender).await;
                if let Err(_) = response.send(result) {
                    tracing::error!("Failed to send UnregisterNotification response");
                }
            }
            StoreRequest::InnerDisableNotifications { response } => {
                self.store.inner_mut().disable_notifications();
                if let Err(_) = response.send(()) {
                    tracing::error!("Failed to send InnerDisableNotifications response");
                }
            }
            StoreRequest::InnerEnableNotifications { response } => {
                self.store.inner_mut().enable_notifications();
                if let Err(_) = response.send(()) {
                    tracing::error!("Failed to send InnerEnableNotifications response");
                }
            }
            StoreRequest::InnerRestoreSnapshot { snapshot, response } => {
                self.store.inner_mut().restore_snapshot(snapshot);

                let me_as_candidate = {
                   let machine = &self.config.machine_id;
                
                    match self.store.find_entities_paginated(
                        &et::candidate(), 
                        None,
                        Some(format!("Name == 'qcore' && Parent->Name == '{}'", machine))
                    ).await {
                        Ok(page) => page.items.iter().find(|id| id.get_type() == &et::candidate()).cloned(),
                        Err(e) => {
                            tracing::error!(error = %e, "Failed to find candidate entity for machine {}", machine);
                            None
                        }
                    }
                };
                self.store.inner_mut().default_writer_id = me_as_candidate;

                if let Err(_) = response.send(()) {
                    tracing::error!("Failed to send InnerRestoreSnapshot response");
                }
            }
            StoreRequest::InnerTakeSnapshot { response } => {
                let snapshot = self.store.inner().take_snapshot();
                if let Err(_) = response.send(snapshot) {
                    tracing::error!("Failed to send InnerTakeSnapshot response");
                }
            }
            StoreRequest::CheckRequestsAuthorization { client_id, requests, response } => {
                let result = self.check_requests_authorization(&client_id, requests).await;
                if let Err(_) = response.send(result) {
                    tracing::error!("Failed to send CheckRequestsAuthorization response");
                }
            }
            StoreRequest::AuthenticateSubject { subject_name, credential, response } => {
                let result = self.authenticate_subject(&subject_name, &credential).await;
                if let Err(_) = response.send(result) {
                    tracing::error!("Failed to send AuthenticateSubject response");
                }
            }
            StoreRequest::SetServices { services, response } => {
                // Spawn the write channel processing task if it hasn't been spawned yet
                if !self.write_channel_task_spawned {
                    let config = self.config.clone();
                    let write_channel_receiver = self.store.inner().get_write_channel_receiver();
                    
                    tokio::spawn(async move {
                        Self::process_write_channel_task(write_channel_receiver, config, services).await;
                    });
                    
                    self.write_channel_task_spawned = true;
                }

                if let Err(_) = response.send(()) {
                    tracing::error!("Failed to send SetServices response");
                }
            }
        }
    }
}

impl StoreService {
    async fn process_write_channel_task(
        write_channel_receiver: Arc<tokio::sync::Mutex<Receiver<Vec<Request>>>>,
        config: StoreConfig,
        services: Services,
    ) {
        loop {
            // Wait for write requests from the channel
            let requests = {
                let mut receiver_guard = write_channel_receiver.lock().await;
                match receiver_guard.recv().await {
                    Some(requests) => requests,
                    None => {
                        tracing::error!("Write channel closed, terminating write channel task");
                        break;
                    }
                }
            };

            Self::process_write_requests(requests, &config, &services).await;
        }
    }

    async fn process_write_requests(
        mut requests: Vec<Request>,
        config: &StoreConfig,
        services: &Services,
    ) {
        // Get the machine ID from the config
        let machine_id = &config.machine_id;

        // Ensure the originator is set for all requests
        requests.iter_mut().for_each(|req| {
            if req.originator().is_none() {
                req.try_set_originator(machine_id.clone());
            }
        });

        // Prepare requests for synchronization first
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

        // Run WAL writing and peer synchronization in parallel
        let wal_task = async {
            for request in &requests {
                if let Err(e) = services.wal_handle.write_request(request.clone()).await {
                    tracing::error!(
                        error = %e,
                        "Failed to write request to WAL"
                    );
                }
            }
        };

        let sync_task = async {
            if !requests_to_sync.is_empty() {
                tracing::debug!(
                    count = requests_to_sync.len(),
                    "Sending sync requests to peers"
                );
                
                services.peer_handle.send_sync_message(requests_to_sync).await;
            }
        };

        // Execute both tasks concurrently
        tokio::join!(wal_task, sync_task);
    }

    async fn check_requests_authorization(
        &mut self,
        client_id: &EntityId,
        requests: Vec<Request>,
    ) -> Result<Vec<Request>> {
        let mut authorized_requests = Vec::new();

        for request in requests {
            if let Some(entity_id) = request.entity_id() {
                if let Some(field_type) = request.field_type() {
                    match get_scope(
                        &mut self.store,
                        &mut self.cel_executor,
                        &self.permission_cache,
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