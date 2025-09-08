use qlib_rs::{et, ft, AsyncStore, Cache, CelExecutor, EntityId, FieldType, Request, Snowflake};
use qlib_rs::auth::{AuthorizationScope, get_scope};
use tokio::sync::{mpsc, oneshot};
use anyhow::Result;
use std::sync::Arc;

/// Authentication service request types
#[derive(Debug)]
pub enum AuthRequest {
    CheckRequestsAuthorization {
        client_id: EntityId,
        requests: Vec<Request>,
        response: oneshot::Sender<Result<Vec<Request>>>,
    },
}

/// Handle for communicating with authentication service
#[derive(Debug, Clone)]
pub struct AuthHandle {
    sender: mpsc::UnboundedSender<AuthRequest>,
}

impl AuthHandle {
    /// Check authorization for a list of requests and return only authorized ones
    pub async fn check_requests_authorization(
        &self,
        client_id: &EntityId,
        requests: Vec<Request>,
    ) -> Result<Vec<Request>> {
        let (response_tx, response_rx) = oneshot::channel();
        self.sender.send(AuthRequest::CheckRequestsAuthorization {
            client_id: client_id.clone(),
            requests,
            response: response_tx,
        }).map_err(|_| anyhow::anyhow!("Authentication service has stopped"))?;
        response_rx.await.map_err(|_| anyhow::anyhow!("Authentication service response channel closed"))?
    }
}

pub struct AuthenticationService;

impl AuthenticationService {
    pub fn spawn() -> AuthHandle {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        
        tokio::spawn(async move {
            // Create our own store instance for authentication
            let mut store = AsyncStore::new(Arc::new(Snowflake::new()));
            
            // Create permission cache
            let permission_cache = Cache::new(
                &mut store,
                et::permission(),
                vec![ft::resource_type(), ft::resource_field()],
                vec![ft::scope(), ft::condition()]
            ).await.expect("Failed to create permission cache");
            
            let mut cel_executor = CelExecutor::new();

            while let Some(request) = receiver.recv().await {
                match request {
                    AuthRequest::CheckRequestsAuthorization {
                        client_id,
                        requests,
                        response,
                    } => {
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

                        let result = if let Some(reason) = authorization_failed_reason {
                            Err(reason)
                        } else {
                            Ok(authorized_requests)
                        };

                        let _ = response.send(result);
                    }
                }
            }
        });

        AuthHandle { sender }
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