use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::api::dtos::{
    HealthHttpResponse, ReadinessHttpResponse, WellKnownLocksServerHttpResponse,
};
use crate::app_state::{AppState, RuntimeStorageKind};

pub(super) async fn healthz() -> Json<HealthHttpResponse> {
    Json(HealthHttpResponse { status: "ok" })
}

pub(super) async fn readyz(State(state): State<AppState>) -> Response {
    let worker_enabled = state.config().worker.enabled;
    match state.private_runtime_storage_kind() {
        RuntimeStorageKind::InMemory => Json(ReadinessHttpResponse {
            status: "ready",
            runtime_storage: "ephemeral",
            worker_enabled,
        })
        .into_response(),
        RuntimeStorageKind::Postgres => {
            let is_ready = match state.postgres_pool() {
                Some(pool) => sqlx::query_scalar::<_, i32>("SELECT 1")
                    .fetch_one(pool)
                    .await
                    .is_ok(),
                None => false,
            };
            if is_ready {
                Json(ReadinessHttpResponse {
                    status: "ready",
                    runtime_storage: "persisted",
                    worker_enabled,
                })
                .into_response()
            } else {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(ReadinessHttpResponse {
                        status: "not_ready",
                        runtime_storage: "persisted",
                        worker_enabled,
                    }),
                )
                    .into_response()
            }
        }
    }
}

pub(super) async fn well_known_locks_server(
    State(state): State<AppState>,
) -> Json<WellKnownLocksServerHttpResponse> {
    Json(WellKnownLocksServerHttpResponse {
        service: "pubky-locks-server",
        api_version: "0.1",
        lock_server: state.config().credentials.lock_server_public_key.clone(),
    })
}
