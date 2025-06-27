use actix_web::Responder;
use actix_web::post;
use actix_web::web;
use actix_web::web::Data;
use qlib_rs::Context;
use web::Json;

use crate::app::App;
use crate::store::Request;
use crate::store::Response;

/**
 * Application API
 */
#[post("/api/perform")]
pub async fn perform(app: Data<App>, req: Json<Request>) -> actix_web::Result<impl Responder> {
    if req
        .0
        .request
        .iter()
        .all(|r| matches!(r, qlib_rs::Request::Read { .. }))
    {
        let ret = app.raft.ensure_linearizable().await;
        return match ret {
            Ok(_) => {
                let mut store_req = req.0.request;
                let mut state_machine = app.state_machine_store.state_machine.write().await;
                state_machine.data.perform(&Context {}, &mut store_req)?;

                Ok(Json(Response {
                    response: store_req,
                    error: None,
                }))
            }
            Err(e) => {
                Ok(Json(Response {
                    response: vec![],
                    error: Some(e.to_string()),
                }))
            }
        }
    }

    match app.raft.client_write(req.0).await {
        Ok(response) => Ok(Json(response.response().clone())),
        Err(err) => Ok(Json(Response {
            response: vec![],
            error: Some(err.to_string()),
        })),
    }
}
