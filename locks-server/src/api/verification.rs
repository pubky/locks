use std::net::SocketAddr;

use axum::Json;
use axum::extract::rejection::JsonRejection;
use axum::extract::{ConnectInfo, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use locks_core::lock_policy::VerifierType;
use locks_core::verification::SubmittedProofBundle;
use locks_service::application::errors::ApplicationError;
use locks_service::application::use_cases::complete_verification_task::{
    CompleteVerificationTaskRequest, CompleteVerificationTaskUseCase,
};
use locks_service::application::use_cases::get_verification_task::{
    GetVerificationTaskByHandleRequest, GetVerificationTaskByHandleUseCase,
};
use locks_service::application::use_cases::submit_proof_bundle::{
    SubmitProofBundleRequest, SubmitProofBundleUseCase, SubmittedVerificationTask,
};
use locks_service::application::use_cases::validate_paykit_payment_submission::{
    ValidatePaykitPaymentSubmissionRequest, ValidatePaykitPaymentSubmissionUseCase,
};
use locks_service::infrastructure::postgres::PostgresPaykitTaskAdmissionRepository;
use locks_service::infrastructure::verifiers::registry::StaticCriterionVerifierRegistry;

use crate::api::dtos::{
    SubmitProofBundleHttpRequest, VerificationTaskHandleHttpRequest,
    VerificationTaskLifecycleHttpResponse,
};
use crate::api::errors::{ApiError, ApiErrorCode};
use crate::api::extractors::parse_json;
use crate::app_state::AppState;
use crate::paykit_http_client::{PaykitClientError, PaykitInvoiceRequest};
use crate::rate_limit::VerificationSubmissionRateLimitKey;

pub(super) async fn submit_proof_bundle(
    State(state): State<AppState>,
    ConnectInfo(client_address): ConnectInfo<SocketAddr>,
    request: Result<Json<SubmitProofBundleHttpRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let request = parse_json(request)?;
    let creator = request
        .submitted_proof_bundle
        .pubky_lock_resource
        .creator()
        .clone();
    let decision = state.verification_submission_rate_limiter().check(
        &VerificationSubmissionRateLimitKey {
            client_address: client_address.ip(),
            creator,
        },
        state.clock().now(),
    );
    if !decision.allowed {
        return Ok((
            StatusCode::TOO_MANY_REQUESTS,
            [(
                header::RETRY_AFTER,
                decision.retry_after_seconds.unwrap_or_default().to_string(),
            )],
            Json(ApiError::new(ApiErrorCode::RateLimited, "rate limit exceeded").error_response()),
        )
            .into_response());
    }
    let use_case = SubmitProofBundleUseCase::new(
        state.task_ids().as_ref(),
        state.verification_tasks().as_ref(),
        state.clock().as_ref(),
    );
    if let Some(existing) =
        maybe_prepare_paykit_submission(&state, &request.submitted_proof_bundle, &use_case).await?
    {
        return Ok(Json(VerificationTaskLifecycleHttpResponse::from(existing)).into_response());
    }
    let submitted = use_case
        .execute(SubmitProofBundleRequest {
            submitted_proof_bundle: request.submitted_proof_bundle,
        })
        .await?;

    Ok(Json(VerificationTaskLifecycleHttpResponse::from(submitted)).into_response())
}

async fn maybe_prepare_paykit_submission(
    state: &AppState,
    submitted: &SubmittedProofBundle,
    submit_use_case: &SubmitProofBundleUseCase<'_>,
) -> Result<Option<SubmittedVerificationTask>, ApiError> {
    let paykit_proofs: Vec<_> = submitted
        .proofs
        .iter()
        .filter(|proof| proof.verifier_type == VerifierType::PaykitPayment)
        .collect();
    if paykit_proofs.is_empty() {
        return Ok(None);
    }
    if submitted.proofs.len() != 1
        || paykit_proofs.len() != 1
        || !paykit_proofs[0]
            .payload
            .as_object()
            .is_some_and(|object| object.is_empty())
    {
        return Err(ApiError::new(
            ApiErrorCode::InvalidRequest,
            "invalid paykit-payment proof bundle",
        ));
    }
    let reader = submitted.reader_public_key.as_ref().ok_or_else(|| {
        ApiError::new(
            ApiErrorCode::InvalidRequest,
            "paykit-payment requires reader_public_key",
        )
    })?;
    if let Some(pool) = state.postgres_pool() {
        let admissions = PostgresPaykitTaskAdmissionRepository::new(pool.clone());
        if let Some(admission) = admissions.find_existing(submitted).await? {
            if admission.requires_paykit {
                create_paykit_invoice(state, &admission.task.submitted_proof_bundle).await?;
                admissions.mark_ready(&admission.task).await?;
            }
            return Ok(Some(admission.task.into()));
        }
    }
    ValidatePaykitPaymentSubmissionUseCase::new(state.content_locks().as_ref())
        .execute(ValidatePaykitPaymentSubmissionRequest {
            submitted_proof_bundle: submitted.clone(),
        })
        .await?;
    if !state
        .reader_pubky_resolver()
        .reader_has_homeserver(reader)
        .await
    {
        return Err(ApiError::new(
            ApiErrorCode::ReaderPubkyUnresolvable,
            "reader pubky is unresolvable",
        ));
    }
    if let Some(pool) = state.postgres_pool() {
        let task = submit_use_case.prepare_task(submitted.clone()).await?;
        let admissions = PostgresPaykitTaskAdmissionRepository::new(pool.clone());
        let admission = admissions.reserve(task).await?;
        if admission.requires_paykit {
            create_paykit_invoice(state, &admission.task.submitted_proof_bundle).await?;
            admissions.mark_ready(&admission.task).await?;
        }
        return Ok(Some(admission.task.into()));
    }
    if let Some(existing) = submit_use_case.find_existing(submitted).await? {
        return Ok(Some(existing));
    }
    let paykit = state.paykit_http_client().ok_or_else(|| {
        ApiError::new(
            ApiErrorCode::PaykitNotConfigured,
            "paykit is not configured",
        )
    })?;
    paykit
        .create_invoice(&PaykitInvoiceRequest {
            bundle_id: submitted.bundle_id.to_string(),
            lock_resource: submitted.pubky_lock_resource.to_string(),
            reader: reader.to_string(),
        })
        .await
        .map_err(map_paykit_invoice_error)?;
    Ok(None)
}

async fn create_paykit_invoice(
    state: &AppState,
    submitted: &SubmittedProofBundle,
) -> Result<(), ApiError> {
    let reader = submitted.reader_public_key.as_ref().ok_or_else(|| {
        ApiError::new(
            ApiErrorCode::InvalidRequest,
            "paykit-payment requires reader_public_key",
        )
    })?;
    state
        .paykit_http_client()
        .ok_or_else(|| {
            ApiError::new(
                ApiErrorCode::PaykitNotConfigured,
                "paykit is not configured",
            )
        })?
        .create_invoice(&PaykitInvoiceRequest {
            bundle_id: submitted.bundle_id.to_string(),
            lock_resource: submitted.pubky_lock_resource.to_string(),
            reader: reader.to_string(),
        })
        .await
        .map_err(map_paykit_invoice_error)
}

fn map_paykit_invoice_error(error: PaykitClientError) -> ApiError {
    if matches!(
        error,
        PaykitClientError::NonSuccess {
            status: StatusCode::CONFLICT,
            ..
        }
    ) {
        return ApplicationError::VerificationTaskConflict.into();
    }

    ApiError::new(
        ApiErrorCode::PaykitInvoiceCreationFailed,
        "paykit invoice creation failed",
    )
}

pub(super) async fn lookup_verification_task(
    State(state): State<AppState>,
    request: Result<Json<VerificationTaskHandleHttpRequest>, JsonRejection>,
) -> Result<Json<VerificationTaskLifecycleHttpResponse>, ApiError> {
    let request = parse_json(request)?;
    let view = get_task_view_by_handle(&state, request).await?;

    Ok(Json(VerificationTaskLifecycleHttpResponse::from(view)))
}

/// Dev/internal endpoint for manually triggering verifier completion.
///
/// Production should replace or guard this with a worker loop, queue consumer, or
/// authenticated internal control plane; this route deliberately does not add
/// fake production auth.
pub(super) async fn complete_verification_task(
    State(state): State<AppState>,
    request: Result<Json<VerificationTaskHandleHttpRequest>, JsonRejection>,
) -> Result<Json<VerificationTaskLifecycleHttpResponse>, ApiError> {
    let request = parse_json(request)?;
    let task = state
        .verification_tasks()
        .get_verification_task_by_handle(&request.creator, &request.bundle_id)
        .await?
        .ok_or(
            locks_service::application::errors::ApplicationError::MissingRecord {
                record: "verification_task",
            },
        )?;
    let task_id = task.task_id;
    let verifiers = StaticCriterionVerifierRegistry::new()
        .with_dev_static(state.dev_static_verifier().as_ref());
    let use_case = CompleteVerificationTaskUseCase::new(
        state.verification_tasks().as_ref(),
        state.content_locks().as_ref(),
        state.entitlements().as_ref(),
        &verifiers,
        state.clock().as_ref(),
        state.config().credentials.lock_server_public_key.clone(),
    );
    use_case
        .execute(CompleteVerificationTaskRequest { task_id })
        .await?;

    let view = get_task_view_by_handle(&state, request).await?;
    Ok(Json(VerificationTaskLifecycleHttpResponse::from(view)))
}

async fn get_task_view_by_handle(
    state: &AppState,
    request: VerificationTaskHandleHttpRequest,
) -> Result<
    locks_service::application::use_cases::get_verification_task::VerificationTaskLifecycleView,
    ApiError,
> {
    let use_case = GetVerificationTaskByHandleUseCase::new(state.verification_tasks().as_ref());
    let view = use_case
        .execute(GetVerificationTaskByHandleRequest {
            creator: request.creator,
            bundle_id: request.bundle_id,
        })
        .await?;
    Ok(view)
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use serde_json::json;

    use super::map_paykit_invoice_error;
    use crate::paykit_http_client::PaykitClientError;

    #[test]
    fn paykit_invoice_conflict_maps_to_existing_task_conflict() {
        let error = map_paykit_invoice_error(PaykitClientError::NonSuccess {
            operation: "invoice creation",
            status: StatusCode::CONFLICT,
        });

        assert_eq!(error.status_code(), StatusCode::CONFLICT);
        assert_eq!(
            serde_json::to_value(error.error_response()).unwrap(),
            json!({
                "error": {
                    "code": "task_state_conflict",
                    "message": "verification task state conflict"
                }
            })
        );
    }

    #[test]
    fn other_paykit_invoice_failures_remain_generic_bad_gateway() {
        let error = map_paykit_invoice_error(PaykitClientError::NonSuccess {
            operation: "invoice creation",
            status: StatusCode::BAD_REQUEST,
        });

        assert_eq!(error.status_code(), StatusCode::BAD_GATEWAY);
        assert_eq!(
            serde_json::to_value(error.error_response()).unwrap(),
            json!({
                "error": {
                    "code": "paykit_invoice_creation_failed",
                    "message": "paykit invoice creation failed"
                }
            })
        );
    }
}
