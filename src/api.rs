use actix_web::Responder;
use actix_web::post;
use actix_web::web;
use actix_web::web::Data;
use qlib_rs::Context;
use web::Json;

use crate::app::App;
use crate::store::CommandRequest;
use crate::store::CommandResponse;

/**
 * Application API
 */
#[post("/api/perform")]
pub async fn perform(app: Data<App>, req: Json<CommandRequest>) -> actix_web::Result<impl Responder> {
    // Check if it's a read-only request (UpdateEntity with only Read requests)
    if let CommandRequest::UpdateEntity { request } = &req.0 {
        if request.iter().all(|r| matches!(r, qlib_rs::Request::Read { .. })) {
            let ret = app.raft.ensure_linearizable().await;
            return match ret {
                Ok(_) => {
                    let mut store_req = request.clone();
                    let mut state_machine = app.state_machine_store.state_machine.write().await;
                    match state_machine.data.perform(&Context {}, &mut store_req) {
                        Ok(_) => Ok(Json(CommandResponse::UpdateEntity {
                            response: store_req,
                            error: None,
                        })),
                        Err(e) => Ok(Json(CommandResponse::UpdateEntity {
                            response: vec![],
                            error: Some(e.to_string()),
                        })),
                    }
                }
                Err(e) => {
                    Ok(Json(CommandResponse::UpdateEntity {
                        response: vec![],
                        error: Some(e.to_string()),
                    }))
                }
            };
        }
    }

    if let CommandRequest::GetSchema { entity_type } = &req.0 {
            let ret = app.raft.ensure_linearizable().await;
            return match ret {
                Ok(_) => {
                    let state_machine = app.state_machine_store.state_machine.read().await;
                    match state_machine.data.get_entity_schema(&Context {}, entity_type) {
                        Ok(schema) => Ok(Json(CommandResponse::GetSchema {
                            response: Some(schema),
                            error: None,
                        })),
                        Err(e) => Ok(Json(CommandResponse::GetSchema {
                            response: None,
                            error: Some(e.to_string()),
                        })),
                    }
                }
                Err(e) => {
                    Ok(Json(CommandResponse::GetSchema {
                        response: None,
                        error: Some(e.to_string()),
                    }))
                }
            };
    }

    if let CommandRequest::GetCompleteSchema { entity_type } = &req.0 {
        let ret = app.raft.ensure_linearizable().await;
        return match ret {
            Ok(_) => {
                let state_machine = app.state_machine_store.state_machine.read().await;
                match state_machine.data.get_complete_entity_schema(&Context {}, entity_type) {
                    Ok(schema) => Ok(Json(CommandResponse::GetCompleteSchema {
                        response: Some(schema),
                        error: None,
                    })),
                    Err(e) => Ok(Json(CommandResponse::GetCompleteSchema {
                        response: None,
                        error: Some(e.to_string()),
                    })),
                }
            }
            Err(e) => {
                Ok(Json(CommandResponse::GetCompleteSchema {
                    response: None,
                    error: Some(e.to_string()),
                }))
            }
        };
    }

    // For all other requests (write operations), forward to the Raft client
    match app.raft.client_write(req.0.clone()).await {
        Ok(response) => Ok(Json(response.response().clone())),
        Err(err) => {
            // Map error to appropriate response type based on request
            match req.0 {
                CommandRequest::UpdateEntity { .. } => {
                    Ok(Json(CommandResponse::UpdateEntity {
                        response: vec![],
                        error: Some(err.to_string()),
                    }))
                }
                CommandRequest::CreateEntity { .. } => {
                    Ok(Json(CommandResponse::CreateEntity {
                        response: None,
                        error: Some(err.to_string()),
                    }))
                }
                CommandRequest::DeleteEntity { .. } => {
                    Ok(Json(CommandResponse::DeleteEntity {
                        error: Some(err.to_string()),
                    }))
                }
                CommandRequest::SetSchema { .. } => {
                    Ok(Json(CommandResponse::SetSchema {
                        error: Some(err.to_string()),
                    }))
                }
                CommandRequest::GetSchema { .. } => {
                    Ok(Json(CommandResponse::GetSchema {
                        response: None,
                        error: Some(err.to_string()),
                    }))
                }
                CommandRequest::GetCompleteSchema { .. } => {
                    Ok(Json(CommandResponse::GetCompleteSchema {
                        response: None,
                        error: Some(err.to_string()),
                    }))
                }
            }
        }
    }
}
