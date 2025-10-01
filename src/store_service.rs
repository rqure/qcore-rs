use anyhow::Result;
use crossbeam::channel::Sender;
use std::thread;
use tracing::{error, info};

use qlib_rs::data::entity_schema::EntitySchemaResp;
use qlib_rs::data::resp::{
    BooleanResponse, CreateEntityResponse, EntityListResponse, EntityTypeListResponse,
    FieldSchemaResponse, IntegerResponse, PaginatedEntityResponse,
    PaginatedEntityTypeResponse, ReadResponse, ResolveIndirectionResponse, SnapshotResponse,
    StringResponse,
};
use qlib_rs::data::et::ET;
use qlib_rs::data::ft::FT;
use qlib_rs::{
    EntityId, EntityType, FieldType, PageOpts, Snapshot, Store,
    StoreTrait, Value, WriteInfo,
};

/// Store service request that uses OwnedRespValue for flexibility
#[derive(Debug)]
pub enum StoreRequest {
    // Read operations
    Read {
        entity_id: EntityId,
        field_path: Vec<FieldType>,
        respond_to: Sender<Result<ReadResponse>>,
    },
    GetEntityType {
        name: String,
        respond_to: Sender<Result<IntegerResponse>>,
    },
    ResolveEntityType {
        entity_type: EntityType,
        respond_to: Sender<Result<StringResponse>>,
    },
    GetFieldType {
        name: String,
        respond_to: Sender<Result<IntegerResponse>>,
    },
    ResolveFieldType {
        field_type: FieldType,
        respond_to: Sender<Result<StringResponse>>,
    },
    GetEntitySchema {
        entity_type: EntityType,
        respond_to: Sender<Result<EntitySchemaResp>>,
    },
    GetFieldSchema {
        entity_type: EntityType,
        field_type: FieldType,
        respond_to: Sender<Result<FieldSchemaResponse>>,
    },
    FindEntities {
        entity_type: EntityType,
        filter: Option<String>,
        respond_to: Sender<Result<EntityListResponse>>,
    },
    FindEntitiesExact {
        entity_type: EntityType,
        filter: Option<String>,
        respond_to: Sender<Result<EntityListResponse>>,
    },
    FindEntitiesPaginated {
        entity_type: EntityType,
        page_opts: Option<PageOpts>,
        filter: Option<String>,
        respond_to: Sender<Result<PaginatedEntityResponse>>,
    },
    GetEntityTypes {
        respond_to: Sender<Result<EntityTypeListResponse>>,
    },
    GetEntityTypesPaginated {
        page_opts: Option<PageOpts>,
        respond_to: Sender<Result<PaginatedEntityTypeResponse>>,
    },
    EntityExists {
        entity_id: EntityId,
        respond_to: Sender<BooleanResponse>,
    },
    FieldExists {
        entity_type: EntityType,
        field_type: FieldType,
        respond_to: Sender<BooleanResponse>,
    },
    ResolveIndirection {
        entity_id: EntityId,
        fields: Vec<FieldType>,
        respond_to: Sender<Result<ResolveIndirectionResponse>>,
    },
    TakeSnapshot {
        respond_to: Sender<SnapshotResponse>,
    },
    GetEt {
        respond_to: Sender<Option<qlib_rs::et::ET>>,
    },
    GetFt {
        respond_to: Sender<Option<qlib_rs::ft::FT>>,
    },

    // Write operations
    Write {
        entity_id: EntityId,
        field_path: Vec<FieldType>,
        value: Value,
        writer_id: Option<EntityId>,
        write_time: Option<u64>,
        push_condition: Option<qlib_rs::PushCondition>,
        adjust_behavior: Option<qlib_rs::AdjustBehavior>,
        respond_to: Sender<Result<()>>,
    },
    CreateEntity {
        entity_type: EntityType,
        parent_id: Option<EntityId>,
        name: String,
        respond_to: Sender<Result<CreateEntityResponse>>,
    },
    CreateEntityWithId {
        entity_type: EntityType,
        parent_id: Option<EntityId>,
        created_entity_id: EntityId,
        name: String,
        respond_to: Sender<Result<()>>,
    },
    DeleteEntity {
        entity_id: EntityId,
        respond_to: Sender<Result<()>>,
    },
    UpdateSchema {
        schema: qlib_rs::EntitySchema<qlib_rs::Single, String, String>,
        respond_to: Sender<Result<()>>,
    },
    SetFieldSchema {
        entity_type: EntityType,
        field_type: FieldType,
        schema: qlib_rs::FieldSchema,
        respond_to: Sender<Result<()>>,
    },
    // Bulk operations
    RestoreSnapshot {
        snapshot: Snapshot,
    },
    Replay {
        writes: Vec<WriteInfo>,
    },
    CollectPendingWrites {
        respond_to: Sender<Vec<WriteInfo>>,
    },
}

/// Handle for communicating with store service
#[derive(Debug, Clone)]
pub struct StoreHandle {
    sender: Sender<StoreRequest>,
}

impl StoreHandle {
    // Read operations
    pub fn read(&self, entity_id: EntityId, field_path: Vec<FieldType>) -> Result<ReadResponse> {
        let (resp_sender, resp_receiver) = crossbeam::channel::bounded(1);
        self.sender
            .send(StoreRequest::Read {
                entity_id,
                field_path,
                respond_to: resp_sender,
            })
            .unwrap();
        resp_receiver.recv().unwrap()
    }

    pub fn get_entity_type(&self, name: String) -> Result<IntegerResponse> {
        let (resp_sender, resp_receiver) = crossbeam::channel::bounded(1);
        self.sender
            .send(StoreRequest::GetEntityType {
                name,
                respond_to: resp_sender,
            })
            .unwrap();
        resp_receiver.recv().unwrap()
    }

    pub fn resolve_entity_type(&self, entity_type: EntityType) -> Result<StringResponse> {
        let (resp_sender, resp_receiver) = crossbeam::channel::bounded(1);
        self.sender
            .send(StoreRequest::ResolveEntityType {
                entity_type,
                respond_to: resp_sender,
            })
            .unwrap();
        resp_receiver.recv().unwrap()
    }

    pub fn get_field_type(&self, name: String) -> Result<IntegerResponse> {
        let (resp_sender, resp_receiver) = crossbeam::channel::bounded(1);
        self.sender
            .send(StoreRequest::GetFieldType {
                name,
                respond_to: resp_sender,
            })
            .unwrap();
        resp_receiver.recv().unwrap()
    }

    pub fn resolve_field_type(&self, field_type: FieldType) -> Result<StringResponse> {
        let (resp_sender, resp_receiver) = crossbeam::channel::bounded(1);
        self.sender
            .send(StoreRequest::ResolveFieldType {
                field_type,
                respond_to: resp_sender,
            })
            .unwrap();
        resp_receiver.recv().unwrap()
    }

    pub fn get_entity_schema(&self, entity_type: EntityType) -> Result<EntitySchemaResp> {
        let (resp_sender, resp_receiver) = crossbeam::channel::bounded(1);
        self.sender
            .send(StoreRequest::GetEntitySchema {
                entity_type,
                respond_to: resp_sender,
            })
            .unwrap();
        resp_receiver.recv().unwrap()
    }

    pub fn get_field_schema(
        &self,
        entity_type: EntityType,
        field_type: FieldType,
    ) -> Result<FieldSchemaResponse> {
        let (resp_sender, resp_receiver) = crossbeam::channel::bounded(1);
        self.sender
            .send(StoreRequest::GetFieldSchema {
                entity_type,
                field_type,
                respond_to: resp_sender,
            })
            .unwrap();
        resp_receiver.recv().unwrap()
    }

    pub fn find_entities(
        &self,
        entity_type: EntityType,
        filter: Option<String>,
    ) -> Result<EntityListResponse> {
        let (resp_sender, resp_receiver) = crossbeam::channel::bounded(1);
        self.sender
            .send(StoreRequest::FindEntities {
                entity_type,
                filter,
                respond_to: resp_sender,
            })
            .unwrap();
        resp_receiver.recv().unwrap()
    }

    pub fn find_entities_exact(
        &self,
        entity_type: EntityType,
        filter: Option<String>,
    ) -> Result<EntityListResponse> {
        let (resp_sender, resp_receiver) = crossbeam::channel::bounded(1);
        self.sender
            .send(StoreRequest::FindEntitiesExact {
                entity_type,
                filter,
                respond_to: resp_sender,
            })
            .unwrap();
        resp_receiver.recv().unwrap()
    }

    pub fn find_entities_paginated(
        &self,
        entity_type: EntityType,
        page_opts: Option<PageOpts>,
        filter: Option<String>,
    ) -> Result<PaginatedEntityResponse> {
        let (resp_sender, resp_receiver) = crossbeam::channel::bounded(1);
        self.sender
            .send(StoreRequest::FindEntitiesPaginated {
                entity_type,
                page_opts,
                filter,
                respond_to: resp_sender,
            })
            .unwrap();
        resp_receiver.recv().unwrap()
    }

    pub fn get_entity_types(&self) -> Result<EntityTypeListResponse> {
        let (resp_sender, resp_receiver) = crossbeam::channel::bounded(1);
        self.sender
            .send(StoreRequest::GetEntityTypes {
                respond_to: resp_sender,
            })
            .unwrap();
        resp_receiver.recv().unwrap()
    }

    pub fn get_entity_types_paginated(
        &self,
        page_opts: Option<PageOpts>,
    ) -> Result<PaginatedEntityTypeResponse> {
        let (resp_sender, resp_receiver) = crossbeam::channel::bounded(1);
        self.sender
            .send(StoreRequest::GetEntityTypesPaginated {
                page_opts,
                respond_to: resp_sender,
            })
            .unwrap();
        resp_receiver.recv().unwrap()
    }

    pub fn entity_exists(&self, entity_id: EntityId) -> BooleanResponse {
        let (resp_sender, resp_receiver) = crossbeam::channel::bounded(1);
        self.sender
            .send(StoreRequest::EntityExists {
                entity_id,
                respond_to: resp_sender,
            })
            .unwrap();
        resp_receiver.recv().unwrap()
    }

    pub fn field_exists(&self, entity_type: EntityType, field_type: FieldType) -> BooleanResponse {
        let (resp_sender, resp_receiver) = crossbeam::channel::bounded(1);
        self.sender
            .send(StoreRequest::FieldExists {
                entity_type,
                field_type,
                respond_to: resp_sender,
            })
            .unwrap();
        resp_receiver.recv().unwrap()
    }

    pub fn resolve_indirection(
        &self,
        entity_id: EntityId,
        fields: Vec<FieldType>,
    ) -> Result<ResolveIndirectionResponse> {
        let (resp_sender, resp_receiver) = crossbeam::channel::bounded(1);
        self.sender
            .send(StoreRequest::ResolveIndirection {
                entity_id,
                fields,
                respond_to: resp_sender,
            })
            .unwrap();
        resp_receiver.recv().unwrap()
    }

    pub fn take_snapshot(&self) -> SnapshotResponse {
        let (resp_sender, resp_receiver) = crossbeam::channel::bounded(1);
        self.sender
            .send(StoreRequest::TakeSnapshot {
                respond_to: resp_sender,
            })
            .unwrap();
        resp_receiver.recv().unwrap()
    }

    pub fn get_et(&self) -> Option<qlib_rs::et::ET> {
        let (resp_sender, resp_receiver) = crossbeam::channel::bounded(1);
        self.sender
            .send(StoreRequest::GetEt {
                respond_to: resp_sender,
            })
            .unwrap();
        resp_receiver.recv().unwrap()
    }

    pub fn get_ft(&self) -> Option<qlib_rs::ft::FT> {
        let (resp_sender, resp_receiver) = crossbeam::channel::bounded(1);
        self.sender
            .send(StoreRequest::GetFt {
                respond_to: resp_sender,
            })
            .unwrap();
        resp_receiver.recv().unwrap()
    }

    // Write operations
    pub fn write(
        &self,
        entity_id: EntityId,
        field_path: Vec<FieldType>,
        value: Value,
        writer_id: Option<EntityId>,
        write_time: Option<u64>,
        push_condition: Option<qlib_rs::PushCondition>,
        adjust_behavior: Option<qlib_rs::AdjustBehavior>,
    ) -> Result<()> {
        let (resp_sender, resp_receiver) = crossbeam::channel::bounded(1);
        self.sender
            .send(StoreRequest::Write {
                entity_id,
                field_path,
                value,
                writer_id,
                write_time,
                push_condition,
                adjust_behavior,
                respond_to: resp_sender,
            })
            .unwrap();
        resp_receiver.recv().unwrap()
    }

    pub fn create_entity(
        &self,
        entity_type: EntityType,
        parent_id: Option<EntityId>,
        name: String,
    ) -> Result<CreateEntityResponse> {
        let (resp_sender, resp_receiver) = crossbeam::channel::bounded(1);
        self.sender
            .send(StoreRequest::CreateEntity {
                entity_type,
                parent_id,
                name,
                respond_to: resp_sender,
            })
            .unwrap();
        resp_receiver.recv().unwrap()
    }

    pub fn create_entity_with_id(
        &self,
        entity_type: EntityType,
        parent_id: Option<EntityId>,
        created_entity_id: &mut Option<EntityId>,
        name: &str,
    ) -> Result<()> {
        let (resp_sender, resp_receiver) = crossbeam::channel::bounded(1);
        let entity_id = created_entity_id.expect("created_entity_id must be Some");
        self.sender
            .send(StoreRequest::CreateEntityWithId {
                entity_type,
                parent_id,
                created_entity_id: entity_id,
                name: name.to_string(),
                respond_to: resp_sender,
            })
            .unwrap();
        resp_receiver.recv().unwrap()
    }

    pub fn delete_entity(&self, entity_id: EntityId) -> Result<()> {
        let (resp_sender, resp_receiver) = crossbeam::channel::bounded(1);
        self.sender
            .send(StoreRequest::DeleteEntity {
                entity_id,
                respond_to: resp_sender,
            })
            .unwrap();
        resp_receiver.recv().unwrap()
    }

    pub fn update_schema(
        &self,
        schema: qlib_rs::EntitySchema<qlib_rs::Single, String, String>,
    ) -> Result<()> {
        let (resp_sender, resp_receiver) = crossbeam::channel::bounded(1);
        self.sender
            .send(StoreRequest::UpdateSchema {
                schema,
                respond_to: resp_sender,
            })
            .unwrap();
        resp_receiver.recv().unwrap()
    }

    pub fn set_field_schema(
        &self,
        entity_type: EntityType,
        field_type: FieldType,
        schema: qlib_rs::FieldSchema,
    ) -> Result<()> {
        let (resp_sender, resp_receiver) = crossbeam::channel::bounded(1);
        self.sender
            .send(StoreRequest::SetFieldSchema {
                entity_type,
                field_type,
                schema,
                respond_to: resp_sender,
            })
            .unwrap();
        resp_receiver.recv().unwrap()
    }

    // Bulk operations
    pub fn restore_snapshot(&self, snapshot: Snapshot) {
        self.sender
            .send(StoreRequest::RestoreSnapshot { snapshot })
            .expect("Failed to send restore snapshot request");
    }

    pub fn replay(&self, writes: Vec<WriteInfo>) {
        self.sender.send(StoreRequest::Replay { writes }).unwrap();
    }

    pub fn collect_pending_writes(&self) -> Vec<WriteInfo> {
        let (resp_sender, resp_receiver) = crossbeam::channel::bounded(1);
        self.sender
            .send(StoreRequest::CollectPendingWrites {
                respond_to: resp_sender,
            })
            .unwrap();
        resp_receiver.recv().unwrap()
    }
}

/// Store service that processes store operations in its own thread
pub struct StoreService {
    store: Store,
}

impl StoreService {
    pub fn new() -> Self {
        Self {
            store: Store::new(),
        }
    }

    /// Spawn the store service in its own thread and return a handle
    pub fn spawn() -> StoreHandle {
        let (sender, receiver) = crossbeam::channel::unbounded();
        let handle = StoreHandle { sender };

        thread::spawn(move || {
            let mut service = StoreService::new();
            info!("Store service started");

            while let Ok(request) = receiver.recv() {
                service.handle_request(request);
            }

            error!("Store service stopped unexpectedly");
        });

        handle
    }

    fn handle_request(&mut self, request: StoreRequest) {
        match request {
            // Read operations
            StoreRequest::Read {
                entity_id,
                field_path,
                respond_to,
            } => {
                let result = self.store.read(entity_id, &field_path).map(
                    |(value, timestamp, writer_id)| ReadResponse {
                        value,
                        timestamp,
                        writer_id,
                    },
                ).map_err(|e| anyhow::anyhow!("{}", e));
                let _ = respond_to.send(result);
            }
            StoreRequest::GetEntityType { name, respond_to } => {
                let result = self
                    .store
                    .get_entity_type(&name)
                    .map(|entity_type| IntegerResponse {
                        value: entity_type.0 as i64,
                    })
                    .map_err(|e| anyhow::anyhow!("{}", e));
                let _ = respond_to.send(result);
            }
            StoreRequest::ResolveEntityType {
                entity_type,
                respond_to,
            } => {
                let result = self
                    .store
                    .resolve_entity_type(entity_type)
                    .map(|name| StringResponse { value: name })
                    .map_err(|e| anyhow::anyhow!("{}", e));
                let _ = respond_to.send(result);
            }
            StoreRequest::GetFieldType { name, respond_to } => {
                let result = self
                    .store
                    .get_field_type(&name)
                    .map(|field_type| IntegerResponse {
                        value: field_type.0 as i64,
                    })
                    .map_err(|e| anyhow::anyhow!("{}", e));
                let _ = respond_to.send(result);
            }
            StoreRequest::ResolveFieldType {
                field_type,
                respond_to,
            } => {
                let result = self
                    .store
                    .resolve_field_type(field_type)
                    .map(|name| StringResponse { value: name })
                    .map_err(|e| anyhow::anyhow!("{}", e));
                let _ = respond_to.send(result);
            }
            StoreRequest::GetEntitySchema {
                entity_type,
                respond_to,
            } => {
                let result = self
                    .store
                    .get_entity_schema(entity_type)
                    .map(|schema| EntitySchemaResp::from_entity_schema(&schema, &self.store))
                    .map_err(|e| anyhow::anyhow!("{}", e));
                let _ = respond_to.send(result);
            }
            StoreRequest::GetFieldSchema {
                entity_type,
                field_type,
                respond_to,
            } => {
                let result = self
                    .store
                    .get_field_schema(entity_type, field_type)
                    .map(|schema| FieldSchemaResponse {
                        schema: qlib_rs::data::entity_schema::FieldSchemaResp::from_field_schema(
                            &schema,
                            &self.store,
                        ),
                    })
                    .map_err(|e| anyhow::anyhow!("{}", e));
                let _ = respond_to.send(result);
            }
            StoreRequest::FindEntities {
                entity_type,
                filter,
                respond_to,
            } => {
                let result = self
                    .store
                    .find_entities(entity_type, filter.as_deref())
                    .map(|entities| EntityListResponse { entities })
                    .map_err(|e| anyhow::anyhow!("{}", e));
                let _ = respond_to.send(result);
            }
            StoreRequest::FindEntitiesExact {
                entity_type,
                filter,
                respond_to,
            } => {
                let result = self
                    .store
                    .find_entities_exact(entity_type, None, filter.as_deref())
                    .map(|result| EntityListResponse {
                        entities: result.items,
                    })
                    .map_err(|e| anyhow::anyhow!("{}", e));
                let _ = respond_to.send(result);
            }
            StoreRequest::FindEntitiesPaginated {
                entity_type,
                page_opts,
                filter,
                respond_to,
            } => {
                let result = self
                    .store
                    .find_entities_paginated(entity_type, page_opts.as_ref(), filter.as_deref())
                    .map(|result| PaginatedEntityResponse {
                        items: result.items,
                        total: result.total,
                        next_cursor: result.next_cursor,
                    })
                    .map_err(|e| anyhow::anyhow!("{}", e));
                let _ = respond_to.send(result);
            }
            StoreRequest::GetEntityTypes { respond_to } => {
                let result = self
                    .store
                    .get_entity_types()
                    .map(|entity_types| EntityTypeListResponse { entity_types })
                    .map_err(|e| anyhow::anyhow!("{}", e));
                let _ = respond_to.send(result);
            }
            StoreRequest::GetEntityTypesPaginated {
                page_opts,
                respond_to,
            } => {
                let result = self
                    .store
                    .get_entity_types_paginated(page_opts.as_ref())
                    .map(|result| PaginatedEntityTypeResponse {
                        items: result.items,
                        total: result.total,
                        next_cursor: result.next_cursor,
                    })
                    .map_err(|e| anyhow::anyhow!("{}", e));
                let _ = respond_to.send(result);
            }
            StoreRequest::EntityExists {
                entity_id,
                respond_to,
            } => {
                let exists = self.store.entity_exists(entity_id);
                let _ = respond_to.send(BooleanResponse { result: exists });
            }
            StoreRequest::FieldExists {
                entity_type,
                field_type,
                respond_to,
            } => {
                let exists = self.store.field_exists(entity_type, field_type);
                let _ = respond_to.send(BooleanResponse { result: exists });
            }
            StoreRequest::ResolveIndirection {
                entity_id,
                fields,
                respond_to,
            } => {
                let result = self
                    .store
                    .resolve_indirection(entity_id, &fields)
                    .map(|(entity_id, field_type)| ResolveIndirectionResponse {
                        entity_id,
                        field_type,
                    })
                    .map_err(|e| anyhow::anyhow!("{}", e));
                let _ = respond_to.send(result);
            }
            StoreRequest::TakeSnapshot { respond_to } => {
                let snapshot = self.store.take_snapshot();
                let response = SnapshotResponse {
                    data: serde_json::to_string(&snapshot).unwrap_or_default(),
                };
                let _ = respond_to.send(response);
            }
            StoreRequest::GetEt { respond_to } => {
                let et = self.store.et.as_ref().map(|et| ET {
                    fault_tolerance: et.fault_tolerance,
                    folder: et.folder,
                    machine: et.machine,
                    object: et.object,
                    permission: et.permission,
                    root: et.root,
                    service: et.service,
                    subject: et.subject,
                    user: et.user,
                    candidate: et.candidate,
                });
                let _ = respond_to.send(et);
            }
            StoreRequest::GetFt { respond_to } => {
                let ft = self.store.ft.as_ref().map(|ft| FT {
                    active: ft.active,
                    auth_method: ft.auth_method,
                    available_list: ft.available_list,
                    candidate_list: ft.candidate_list,
                    children: ft.children,
                    condition: ft.condition,
                    current_leader: ft.current_leader,
                    death_detection_timeout: ft.death_detection_timeout,
                    description: ft.description,
                    failed_attempts: ft.failed_attempts,
                    heartbeat: ft.heartbeat,
                    last_login: ft.last_login,
                    locked_until: ft.locked_until,
                    make_me: ft.make_me,
                    name: ft.name,
                    parent: ft.parent,
                    password: ft.password,
                    resource_field: ft.resource_field,
                    resource_type: ft.resource_type,
                    scope: ft.scope,
                    secret: ft.secret,
                    start_time: ft.start_time,
                    status: ft.status,
                    sync_status: ft.sync_status,
                });
                let _ = respond_to.send(ft);
            }

            // Write operations
            StoreRequest::Write {
                entity_id,
                field_path,
                value,
                writer_id,
                write_time,
                push_condition,
                adjust_behavior,
                respond_to,
            } => {
                // Convert u64 write_time back to OffsetDateTime if provided
                let write_time_dt = write_time.map(|ts| {
                    time::OffsetDateTime::from_unix_timestamp_nanos(ts as i128)
                        .unwrap_or_else(|_| qlib_rs::data::now())
                });
                let result = self.store.write(
                    entity_id,
                    &field_path,
                    value,
                    writer_id,
                    write_time_dt,
                    push_condition,
                    adjust_behavior,
                ).map_err(|e| anyhow::anyhow!("{}", e));
                let _ = respond_to.send(result);
            }
            StoreRequest::CreateEntity {
                entity_type,
                parent_id,
                name,
                respond_to,
            } => {
                let result = self
                    .store
                    .create_entity(entity_type, parent_id, &name)
                    .map(|entity_id| CreateEntityResponse { entity_id })
                    .map_err(|e| anyhow::anyhow!("{}", e));
                let _ = respond_to.send(result);
            }
            StoreRequest::CreateEntityWithId {
                entity_type,
                parent_id,
                created_entity_id,
                name,
                respond_to,
            } => {
                let mut entity_id_opt = Some(created_entity_id);
                let result = self
                    .store
                    .create_entity_with_id(entity_type, parent_id, &mut entity_id_opt, &name)
                    .map_err(|e| anyhow::anyhow!("{}", e));
                let _ = respond_to.send(result);
            }
            StoreRequest::DeleteEntity {
                entity_id,
                respond_to,
            } => {
                let result = self.store.delete_entity(entity_id).map_err(|e| anyhow::anyhow!("{}", e));
                let _ = respond_to.send(result);
            }
            StoreRequest::UpdateSchema { schema, respond_to } => {
                let result = self.store.update_schema(schema).map_err(|e| anyhow::anyhow!("{}", e));
                let _ = respond_to.send(result);
            }
            StoreRequest::SetFieldSchema {
                entity_type,
                field_type,
                schema,
                respond_to,
            } => {
                let result = self.store.set_field_schema(entity_type, field_type, schema).map_err(|e| anyhow::anyhow!("{}", e));
                let _ = respond_to.send(result);
            }
            // Bulk operations
            StoreRequest::RestoreSnapshot { snapshot } => {
                self.store.restore_snapshot(snapshot);
            }
            StoreRequest::Replay { writes } => {
                // Process writes one by one through the store's write_queue
                for write in writes {
                    self.store.write_queue.push_back(write);
                }
            }
            StoreRequest::CollectPendingWrites { respond_to } => {
                let mut writes = Vec::new();
                while let Some(write) = self.store.write_queue.pop_front() {
                    writes.push(write);
                }
                let _ = respond_to.send(writes);
            }
        }
    }
}
