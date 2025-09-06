use qlib_rs::{et, ft, AsyncStore, Cache, CelExecutor, EntityId, EntitySchema, EntityType, FieldSchema, FieldType, NotificationSender, NotifyConfig, PageOpts, PageResult, Request, Snapshot, Snowflake, StoreTrait};
use qlib_rs::auth::{AuthorizationScope, get_scope};
use tokio::sync::{mpsc, oneshot, Mutex};
use anyhow::Result;
use std::sync::Arc;
use std::collections::HashMap;

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
    PerformMutWithAuth {
        requests: Vec<Request>,
        client_id: Option<EntityId>,
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

    pub async fn perform(&self, requests: Vec<Request>) -> Result<Vec<Request>> {
        let (response_tx, response_rx) = oneshot::channel();
        self.sender.send(StoreRequest::Perform {
            requests,
            response: response_tx,
        }).map_err(|_| anyhow::anyhow!("Store service has stopped"))?;
        response_rx.await.map_err(|_| anyhow::anyhow!("Store service response channel closed"))?
    }

    pub async fn perform_mut(&self, requests: Vec<Request>) -> Result<Vec<Request>> {
        let (response_tx, response_rx) = oneshot::channel();
        self.sender.send(StoreRequest::PerformMut {
            requests,
            response: response_tx,
        }).map_err(|_| anyhow::anyhow!("Store service has stopped"))?;
        response_rx.await.map_err(|_| anyhow::anyhow!("Store service response channel closed"))?
    }

    pub async fn perform_mut_with_auth(&self, requests: Vec<Request>, client_id: Option<EntityId>) -> Result<Vec<Request>> {
        let (response_tx, response_rx) = oneshot::channel();
        self.sender.send(StoreRequest::PerformMutWithAuth {
            requests,
            client_id,
            response: response_tx,
        }).map_err(|_| anyhow::anyhow!("Store service has stopped"))?;
        response_rx.await.map_err(|_| anyhow::anyhow!("Store service response channel closed"))?
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
    pub async fn inner_disable_notifications(&self) {
        let (response_tx, response_rx) = oneshot::channel();
        if self.sender.send(StoreRequest::InnerDisableNotifications {
            response: response_tx,
        }).is_ok() {
            let _ = response_rx.await;
        }
    }

    pub async fn inner_enable_notifications(&self) {
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

    pub async fn inner_take_snapshot(&self) -> Option<Snapshot> {
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
}

pub struct StoreService;

impl StoreService {
    pub fn spawn() -> StoreHandle {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        
        tokio::spawn(async move {
            let mut store = AsyncStore::new(Arc::new(Snowflake::new()));
            let permission_cache = Cache::new(
                &mut store,
                et::permission(),
                vec![ft::resource_type(), ft::resource_field()],
                vec![ft::scope(), ft::condition()]
            ).await.expect("Failed to create permission cache");
            let mut cel_executor = CelExecutor::new();

            while let Some(request) = receiver.recv().await {
                match request {
                    StoreRequest::GetEntitySchema { entity_type, response } => {
                        let result = store.get_entity_schema(&entity_type).await;
                        let _ = response.send(result.map_err(anyhow::Error::from));
                    }
                    StoreRequest::GetCompleteEntitySchema { entity_type, response } => {
                        let result = store.get_complete_entity_schema(&entity_type).await;
                        let _ = response.send(result.map_err(anyhow::Error::from));
                    }
                    StoreRequest::GetFieldSchema { entity_type, field_type, response } => {
                        let result = store.get_field_schema(&entity_type, &field_type).await;
                        let _ = response.send(result.map_err(anyhow::Error::from));
                    }
                    StoreRequest::SetFieldSchema { entity_type, field_type, schema, response } => {
                        let result = store.set_field_schema(&entity_type, &field_type, schema).await;
                        let _ = response.send(result.map_err(anyhow::Error::from));
                    }
                    StoreRequest::EntityExists { entity_id, response } => {
                        let result = store.entity_exists(&entity_id).await;
                        let _ = response.send(result);
                    }
                    StoreRequest::FieldExists { entity_type, field_type, response } => {
                        let result = store.field_exists(&entity_type, &field_type).await;
                        let _ = response.send(result);
                    }
                    StoreRequest::Perform { mut requests, response } => {
                        let result = store.perform(&mut requests).await.map(|_| requests);
                        let _ = response.send(result.map_err(anyhow::Error::from));
                    }
                    StoreRequest::PerformMut { mut requests, response } => {
                        let result = store.perform_mut(&mut requests).await.map(|_| requests);
                        let _ = response.send(result.map_err(anyhow::Error::from));
                    }
                    StoreRequest::PerformMutWithAuth { requests, client_id, response } => {
                        // Perform authorization check if client_id is provided
                        if let Some(client_id) = client_id {
                            // Check authorization for each request
                            let mut authorized_requests = Vec::new();
                            let mut authorization_failed_reason = None;

                            for request in requests {
                                if let Some(entity_id) = request.entity_id() {
                                    if let Some(field_type) = request.field_type() {
                                        match get_scope(
                                            &store,
                                            &mut cel_executor,
                                            &permission_cache,
                                            &client_id,
                                            entity_id,
                                            field_type,
                                        ).await {
                                            Ok(scope) => {
                                                if scope == AuthorizationScope::None {
                                                    authorization_failed_reason = Some(anyhow::anyhow!(
                                                        "Access denied: Subject {} is not authorized to access {} on entity {}",
                                                        client_id, field_type, entity_id
                                                    ));
                                                    break;
                                                }
                                                // For write operations, check if we have write access
                                                if is_write_operation(&request) && scope == AuthorizationScope::ReadOnly {
                                                    authorization_failed_reason = Some(anyhow::anyhow!(
                                                        "Access denied: Subject {} only has read access to {} on entity {}",
                                                        client_id, field_type, entity_id
                                                    ));
                                                    break;
                                                }
                                                authorized_requests.push(request);
                                            }
                                            Err(e) => {
                                                authorization_failed_reason = Some(anyhow::anyhow!(
                                                    "Authorization error: {}", e
                                                ));
                                                break;
                                            }
                                        }
                                    } else {
                                        authorized_requests.push(request);
                                    }
                                } else {
                                    authorized_requests.push(request);
                                }
                            }

                            if let Some(reason) = authorization_failed_reason {
                                let _ = response.send(Err(reason));
                            } else {
                                let result = store.perform_mut(&mut authorized_requests).await.map(|_| authorized_requests);
                                let _ = response.send(result.map_err(anyhow::Error::from));
                            }
                        } else {
                            let _= response.send(Err(anyhow::anyhow!("Client ID is required for authorized operations")));
                        }
                    }
                    StoreRequest::PerformMap { mut requests, response } => {
                        let result = store.perform_map(&mut requests).await;
                        let _ = response.send(result.map_err(anyhow::Error::from));
                    }
                    StoreRequest::FindEntitiesPaginated { entity_type, page_opts, filter, response } => {
                        let result = store.find_entities_paginated(&entity_type, page_opts, filter).await;
                        let _ = response.send(result.map_err(anyhow::Error::from));
                    }
                    StoreRequest::FindEntitiesExact { entity_type, page_opts, filter, response } => {
                        let result = store.find_entities_exact(&entity_type, page_opts, filter).await;
                        let _ = response.send(result.map_err(anyhow::Error::from));
                    }
                    StoreRequest::GetEntityTypesPaginated { page_opts, response } => {
                        let result = store.get_entity_types_paginated(page_opts).await;
                        let _ = response.send(result.map_err(anyhow::Error::from));
                    }
                    StoreRequest::RegisterNotification { config, sender, response } => {
                        let result = store.register_notification(config, sender).await;
                        let _ = response.send(result.map_err(anyhow::Error::from));
                    }
                    StoreRequest::UnregisterNotification { config, sender, response } => {
                        let result = store.unregister_notification(&config, &sender).await;
                        let _ = response.send(result);
                    }
                    StoreRequest::InnerDisableNotifications { response } => {
                        store.inner_mut().disable_notifications();
                        let _ = response.send(());
                    }
                    StoreRequest::InnerEnableNotifications { response } => {
                        store.inner_mut().enable_notifications();
                        let _ = response.send(());
                    }
                    StoreRequest::InnerRestoreSnapshot { snapshot, response } => {
                        store.inner_mut().restore_snapshot(snapshot);
                        let _ = response.send(());
                    }
                    StoreRequest::InnerTakeSnapshot { response } => {
                        let snapshot = store.inner().take_snapshot();
                        let _ = response.send(snapshot);
                    }
                    StoreRequest::InnerGetWriteChannelReceiver { response } => {
                        let receiver = store.inner().get_write_channel_receiver();
                        let _ = response.send(receiver);
                    }
                }
            }
        });

        StoreHandle { sender }
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