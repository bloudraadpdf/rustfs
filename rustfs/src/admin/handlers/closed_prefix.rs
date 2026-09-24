use crate::admin::auth::authorize_admin_request;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::runtime_sources::object_store_from_extensions;
use crate::error::ApiError;
use crate::server::ADMIN_PREFIX;
use http::StatusCode;
use hyper::Method;
use matchit::Params;
use rustfs_ecstore::api::storage::{ClosedPrefixProofV1, ClosedPrefixV1};
use rustfs_policy::policy::action::{Action, AdminAction};
use s3s::{Body, S3Request, S3Response, S3Result, s3_error};
use serde::{Deserialize, Serialize};

pub struct ClosePrefixHandler;
pub struct DeleteClosedObjectsHandler;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeleteClosedObjectsRequest {
    proof: ClosedPrefixProofV1,
    keys: Vec<String>,
}

#[derive(Serialize)]
struct DeleteClosedObjectsResponse {
    proof: ClosedPrefixProofV1,
    keys: Vec<String>,
}

pub fn register_closed_prefix_route(router: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    router.insert(
        Method::POST,
        format!("{ADMIN_PREFIX}/v3/tokoloshe/closed-prefix").as_str(),
        AdminOperation(&ClosePrefixHandler),
    )?;
    router.insert(
        Method::POST,
        format!("{ADMIN_PREFIX}/v3/tokoloshe/closed-prefix/delete").as_str(),
        AdminOperation(&DeleteClosedObjectsHandler),
    )?;
    Ok(())
}

#[async_trait::async_trait]
impl Operation for DeleteClosedObjectsHandler {
    async fn call(&self, mut req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let credential =
            authorize_admin_request(&req, vec![Action::AdminAction(AdminAction::TokolosheDeleteClosedObjectsAction)]).await?;
        let store = object_store_from_extensions(&req.extensions)
            .ok_or_else(|| s3_error!(InternalError, "object store is not initialized"))?;
        let body = req
            .input
            .store_all_limited(rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE)
            .await
            .map_err(|_| s3_error!(InvalidRequest, "invalid request body"))?;
        let requested: DeleteClosedObjectsRequest =
            serde_json::from_slice(&body).map_err(|_| s3_error!(InvalidRequest, "invalid closed-prefix delete"))?;
        let result = super::supervise_admin_mutation("delete_closed_prefix_objects", async move {
            store
                .delete_closed_prefix_objects(&requested.proof, &requested.keys)
                .await
                .map_err(ApiError::from)?;
            Ok(DeleteClosedObjectsResponse {
                proof: requested.proof,
                keys: requested.keys,
            })
        })
        .await?;
        super::admin_json_response(req.uri.path(), &credential.secret_key, StatusCode::OK, &result)
    }
}

#[async_trait::async_trait]
impl Operation for ClosePrefixHandler {
    async fn call(&self, mut req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let credential =
            authorize_admin_request(&req, vec![Action::AdminAction(AdminAction::TokolosheClosePrefixAction)]).await?;
        let store = object_store_from_extensions(&req.extensions)
            .ok_or_else(|| s3_error!(InternalError, "object store is not initialized"))?;
        let body = req
            .input
            .store_all_limited(rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE)
            .await
            .map_err(|_| s3_error!(InvalidRequest, "invalid request body"))?;
        let requested: ClosedPrefixV1 =
            serde_json::from_slice(&body).map_err(|_| s3_error!(InvalidRequest, "invalid closed-prefix request"))?;
        let result = super::supervise_admin_mutation("close_prefix", async move {
            store
                .close_prefix(requested)
                .await
                .map_err(ApiError::from)
                .map_err(Into::into)
        })
        .await?;
        super::admin_json_response(req.uri.path(), &credential.secret_key, StatusCode::OK, &result)
    }
}
