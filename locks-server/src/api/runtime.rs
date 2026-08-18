use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::api::dtos::{
    HealthHttpResponse, ReadinessHttpResponse, WellKnownLocksServerHttpResponse,
};
use crate::app_state::{AppState, ReadinessStatus, RuntimeStorageKind};

pub(super) async fn healthz() -> Json<HealthHttpResponse> {
    Json(HealthHttpResponse { status: "ok" })
}

pub(super) async fn readyz(State(state): State<AppState>) -> Response {
    let worker_enabled = state.config().worker.enabled || state.config().deletion_worker.enabled;
    let (runtime_storage, database_ready) = match state.private_runtime_storage_kind() {
        RuntimeStorageKind::InMemory => ("ephemeral", true),
        RuntimeStorageKind::Postgres => {
            let database_ready = match state.postgres_pool() {
                Some(pool) => sqlx::query_scalar::<_, i32>("SELECT 1")
                    .fetch_one(pool)
                    .await
                    .is_ok(),
                None => false,
            };
            ("persisted", database_ready)
        }
    };

    let status = if !database_ready {
        ReadinessStatus::NotReady
    } else {
        state.worker_readiness_status()
    };
    let response = Json(ReadinessHttpResponse {
        status: match status {
            ReadinessStatus::Ready => "ready",
            ReadinessStatus::Degraded => "degraded",
            ReadinessStatus::NotReady => "not_ready",
        },
        runtime_storage,
        worker_enabled,
    });

    match status {
        ReadinessStatus::Ready | ReadinessStatus::Degraded => response.into_response(),
        ReadinessStatus::NotReady => (StatusCode::SERVICE_UNAVAILABLE, response).into_response(),
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
