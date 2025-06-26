use actix_web::post;
use actix_web::web;
use actix_web::web::Data;
use actix_web::Responder;
use qlib_rs::Context;
use web::Json;

use crate::app::App;
use crate::store::Request;
use crate::store::Response;

/**
 * Application API
 *
 * This is where you place your application, you can use the example below to create your
 * API. The current implementation:
 *
 *  - `POST - /write` saves a value in a key and sync the nodes.
 *  - `POST - /read` attempt to find a value from a given key.
 */
#[post("/api/perform")]
pub async fn perform(app: Data<App>, req: Json<Request>) -> actix_web::Result<impl Responder> {
    let req = req.0.request;

    if req
        .iter()
        .all(|r| matches!(r, qlib_rs::Request::Read{..}) )
        {
        let state_machine = app.state_machine_store.state_machine.read().await;
        let value = state_machine.data.perform(&Context{}, &mut req)?;
        return Ok(Json())
    }

    let response = app.raft.client_write(req).await;
    return Ok(response)
}
