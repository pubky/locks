use std::collections::VecDeque;
use std::str::FromStr;
use std::sync::Mutex;

use async_trait::async_trait;
use axum::http::StatusCode;
use locks_core::ids::{BundleId, CreatorPubky, PubkyLockResource, TaskId};
use locks_core::lock_policy::VerifierType;
use locks_core::verification::{Proof, SUBMITTED_PROOF_BUNDLE_VERSION, SubmittedProofBundle};
use locks_service::application::models::{
    INVOICE_ADMISSION_INTENT_VERSION, InvoiceAdmissionIntentV1, InvoiceAdmissionPhase,
    InvoiceAdmissionRetryReason, VerificationTaskRecord, VerificationTaskStatus,
};
use locks_service::application::ports::{Clock, InvoiceAdmissionRepository};
use locks_service::infrastructure::memory::verification_tasks::InMemoryVerificationTaskRepository;
use time::OffsetDateTime;
use time::macros::datetime;
use uuid::Uuid;

use super::invoice::{
    InvoiceAdmissionWorker, InvoiceErrorAction, InvoiceRetryJitter, InvoiceWorkerTick,
    invoice_error_action,
};
use crate::paykit_http_client::{PaykitClientError, PaykitInvoiceCreator, PaykitInvoiceRequest};

#[tokio::test]
async fn invoice_worker_uses_persisted_shape_and_task_id_then_marks_ready() {
    let repo = InMemoryVerificationTaskRepository::new();
    let task = invoice_task();
    repo.insert_invoice_pending_task(task.clone(), invoice_intent(&task))
        .await
        .unwrap();
    let client = RecordingInvoiceCreator::with_results([Ok(())]);
    let clock = FixedClock(task.submitted_at);
    let jitter = FixedJitter(time::Duration::seconds(1));
    let worker = InvoiceAdmissionWorker::new(
        &repo,
        &client,
        &clock,
        &jitter,
        "invoice-worker".to_owned(),
        time::Duration::minutes(1),
    );

    assert_eq!(
        worker.run_once().await.unwrap(),
        InvoiceWorkerTick::Ready(task.task_id)
    );
    assert_eq!(
        client.calls(),
        vec![(
            task.task_id.as_uuid(),
            PaykitInvoiceRequest {
                bundle_id: task.submitted_proof_bundle.bundle_id.to_string(),
                lock_resource: task.submitted_proof_bundle.pubky_lock_resource.to_string(),
                reader: task
                    .submitted_proof_bundle
                    .reader_public_key
                    .as_ref()
                    .unwrap()
                    .to_string(),
            },
        )]
    );
    assert_eq!(
        repo.get_invoice_admission(&task.task_id)
            .await
            .unwrap()
            .unwrap()
            .phase,
        InvoiceAdmissionPhase::Ready
    );
}

#[tokio::test]
async fn invoice_worker_retries_typed_pending_and_generic_proxy_without_losing_task() {
    for (error, expected_reason) in [
        (
            PaykitClientError::InvoiceNonSuccess {
                request_id: Uuid::new_v4(),
                elapsed_ms: 1,
                status: StatusCode::SERVICE_UNAVAILABLE,
                error_code: Some("reader_setup_pending"),
                retry_after_seconds: Some(60),
            },
            Some(InvoiceAdmissionRetryReason::ReaderWalletSetupNeeded),
        ),
        (
            PaykitClientError::InvoiceNonSuccess {
                request_id: Uuid::new_v4(),
                elapsed_ms: 1,
                status: StatusCode::BAD_GATEWAY,
                error_code: Some("reader_registry_malformed"),
                retry_after_seconds: None,
            },
            Some(InvoiceAdmissionRetryReason::ReaderWalletSetupNeeded),
        ),
        (
            PaykitClientError::InvoiceNonSuccess {
                request_id: Uuid::new_v4(),
                elapsed_ms: 1,
                status: StatusCode::BAD_GATEWAY,
                error_code: None,
                retry_after_seconds: None,
            },
            None,
        ),
        (
            PaykitClientError::InvoiceNonSuccess {
                request_id: Uuid::new_v4(),
                elapsed_ms: 1,
                status: StatusCode::SERVICE_UNAVAILABLE,
                error_code: Some("reader_registry_unavailable"),
                retry_after_seconds: None,
            },
            None,
        ),
        (
            PaykitClientError::InvoiceNonSuccess {
                request_id: Uuid::new_v4(),
                elapsed_ms: 1,
                status: StatusCode::SERVICE_UNAVAILABLE,
                error_code: None,
                retry_after_seconds: None,
            },
            None,
        ),
        (
            PaykitClientError::InvoiceNonSuccess {
                request_id: Uuid::new_v4(),
                elapsed_ms: 1,
                status: StatusCode::TOO_MANY_REQUESTS,
                error_code: Some("rate_limited"),
                retry_after_seconds: Some(60),
            },
            None,
        ),
        (
            PaykitClientError::InvoiceNonSuccess {
                request_id: Uuid::new_v4(),
                elapsed_ms: 1,
                status: StatusCode::INTERNAL_SERVER_ERROR,
                error_code: Some("internal_error"),
                retry_after_seconds: None,
            },
            None,
        ),
        (
            PaykitClientError::InvoiceNonSuccess {
                request_id: Uuid::new_v4(),
                elapsed_ms: 1,
                status: StatusCode::GATEWAY_TIMEOUT,
                error_code: None,
                retry_after_seconds: None,
            },
            None,
        ),
        (
            PaykitClientError::InvoiceTransport {
                request_id: Uuid::new_v4(),
                elapsed_ms: 1,
                timeout: true,
                connect: false,
            },
            None,
        ),
        (PaykitClientError::InvoiceBodyTooLarge, None),
        (
            PaykitClientError::InvalidInvoiceJson(
                serde_json::from_slice::<serde_json::Value>(b"{").unwrap_err(),
            ),
            None,
        ),
        (
            PaykitClientError::InvalidInvoiceTimestamp(
                time::OffsetDateTime::parse(
                    "invalid",
                    &time::format_description::well_known::Rfc3339,
                )
                .unwrap_err(),
            ),
            None,
        ),
        (PaykitClientError::InvalidInvoiceTimestamps, None),
    ] {
        let repo = InMemoryVerificationTaskRepository::new();
        let task = invoice_task();
        repo.insert_invoice_pending_task(task.clone(), invoice_intent(&task))
            .await
            .unwrap();
        let client = RecordingInvoiceCreator::with_results([Err(error)]);
        let clock = FixedClock(task.submitted_at);
        let jitter = FixedJitter(time::Duration::seconds(1));
        let worker = InvoiceAdmissionWorker::new(
            &repo,
            &client,
            &clock,
            &jitter,
            "invoice-worker".to_owned(),
            time::Duration::minutes(1),
        );

        assert_eq!(
            worker.run_once().await.unwrap(),
            InvoiceWorkerTick::RetryScheduled(task.task_id)
        );
        let admission = repo
            .get_invoice_admission(&task.task_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(admission.phase, InvoiceAdmissionPhase::InvoicePending);
        assert_eq!(admission.retry_reason, expected_reason);
        assert!(admission.next_attempt_at.is_some());
    }
}

#[tokio::test]
async fn invoice_worker_fails_status_only_conflict() {
    let repo = InMemoryVerificationTaskRepository::new();
    let task = invoice_task();
    repo.insert_invoice_pending_task(task.clone(), invoice_intent(&task))
        .await
        .unwrap();
    let client =
        RecordingInvoiceCreator::with_results([Err(PaykitClientError::InvoiceNonSuccess {
            request_id: task.task_id.as_uuid(),
            elapsed_ms: 1,
            status: StatusCode::CONFLICT,
            error_code: None,
            retry_after_seconds: None,
        })]);
    let clock = FixedClock(task.submitted_at);
    let jitter = FixedJitter(time::Duration::seconds(1));
    let worker = InvoiceAdmissionWorker::new(
        &repo,
        &client,
        &clock,
        &jitter,
        "invoice-worker".to_owned(),
        time::Duration::minutes(1),
    );

    assert_eq!(
        worker.run_once().await.unwrap(),
        InvoiceWorkerTick::Failed(task.task_id)
    );
    assert_eq!(
        repo.get_invoice_admission(&task.task_id)
            .await
            .unwrap()
            .unwrap()
            .task
            .failure_message
            .as_deref(),
        Some("paykit invoice admission failed")
    );

    let expired_repo = InMemoryVerificationTaskRepository::new();
    let expired_task = invoice_task();
    expired_repo
        .insert_invoice_pending_task(expired_task.clone(), invoice_intent(&expired_task))
        .await
        .unwrap();
    let expired_client = RecordingInvoiceCreator::with_results([]);
    let expired_clock = FixedClock(expired_task.submitted_at + time::Duration::minutes(10));
    let expired_worker = InvoiceAdmissionWorker::new(
        &expired_repo,
        &expired_client,
        &expired_clock,
        &jitter,
        "invoice-worker".to_owned(),
        time::Duration::minutes(1),
    );
    assert_eq!(
        expired_worker.run_once().await.unwrap(),
        InvoiceWorkerTick::Failed(expired_task.task_id)
    );
    assert!(expired_client.calls().is_empty());
}

#[tokio::test]
async fn invoice_worker_marks_ready_when_paykit_200_arrives_after_deadline_under_live_claim() {
    let repo = InMemoryVerificationTaskRepository::new();
    let task = invoice_task();
    let inserted = repo
        .insert_invoice_pending_task(task.clone(), invoice_intent(&task))
        .await
        .unwrap();
    let deadline = inserted.admission_deadline_at;
    let clock = SharedClock::new(deadline - time::Duration::seconds(5));
    let client = DeadlineCrossingInvoiceCreator {
        clock: &clock,
        response_at: deadline + time::Duration::seconds(15),
    };
    let jitter = FixedJitter(time::Duration::seconds(1));
    let worker = InvoiceAdmissionWorker::new(
        &repo,
        &client,
        &clock,
        &jitter,
        "invoice-worker".to_owned(),
        time::Duration::minutes(1),
    );

    assert_eq!(
        worker.run_once().await.unwrap(),
        InvoiceWorkerTick::Ready(task.task_id)
    );
    let admission = repo
        .get_invoice_admission(&task.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(admission.phase, InvoiceAdmissionPhase::Ready);
    assert_eq!(admission.task.status, VerificationTaskStatus::Pending);
    assert_eq!(admission.task.failure_message, None);
    assert_eq!(worker.run_once().await.unwrap(), InvoiceWorkerTick::Idle);
}

#[tokio::test]
async fn invoice_worker_fails_unclaimed_expired_admission_without_calling_paykit() {
    let repo = InMemoryVerificationTaskRepository::new();
    let task = invoice_task();
    let inserted = repo
        .insert_invoice_pending_task(task.clone(), invoice_intent(&task))
        .await
        .unwrap();
    let client = RecordingInvoiceCreator::with_results([]);
    let clock = FixedClock(inserted.admission_deadline_at + time::Duration::seconds(1));
    let jitter = FixedJitter(time::Duration::seconds(1));
    let worker = InvoiceAdmissionWorker::new(
        &repo,
        &client,
        &clock,
        &jitter,
        "invoice-worker".to_owned(),
        time::Duration::minutes(1),
    );

    assert_eq!(
        worker.run_once().await.unwrap(),
        InvoiceWorkerTick::Failed(task.task_id)
    );
    let failed = repo
        .get_invoice_admission(&task.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(failed.phase, InvoiceAdmissionPhase::Failed);
    assert_eq!(
        failed.task.failure_message.as_deref(),
        Some("invoice admission deadline exceeded")
    );
    assert!(client.calls().is_empty());
}

#[tokio::test]
async fn invoice_worker_terminalizes_only_typed_conflict_meanings_with_specific_reason() {
    for (error_code, expected_message) in [
        ("reader_not_payable", "reader is not payable"),
        ("invoice_conflict", "paykit invoice conflict"),
    ] {
        let repo = InMemoryVerificationTaskRepository::new();
        let task = invoice_task();
        repo.insert_invoice_pending_task(task.clone(), invoice_intent(&task))
            .await
            .unwrap();
        let client =
            RecordingInvoiceCreator::with_results([Err(PaykitClientError::InvoiceNonSuccess {
                request_id: task.task_id.as_uuid(),
                elapsed_ms: 1,
                status: StatusCode::CONFLICT,
                error_code: Some(error_code),
                retry_after_seconds: None,
            })]);
        let clock = FixedClock(task.submitted_at);
        let jitter = FixedJitter(time::Duration::seconds(1));
        let worker = InvoiceAdmissionWorker::new(
            &repo,
            &client,
            &clock,
            &jitter,
            "invoice-worker".to_owned(),
            time::Duration::minutes(1),
        );

        assert_eq!(
            worker.run_once().await.unwrap(),
            InvoiceWorkerTick::Failed(task.task_id)
        );
        let admission = repo
            .get_invoice_admission(&task.task_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(admission.phase, InvoiceAdmissionPhase::Failed);
        assert_eq!(
            admission.task.failure_message.as_deref(),
            Some(expected_message)
        );
        assert_eq!(admission.task.entitlement_to_publish, None);
    }
}

#[test]
fn invoice_error_classification_retries_only_429_and_all_5xx_statuses() {
    for status in std::iter::once(StatusCode::TOO_MANY_REQUESTS)
        .chain((500..=599).map(|status| StatusCode::from_u16(status).unwrap()))
    {
        let error = PaykitClientError::InvoiceNonSuccess {
            request_id: Uuid::new_v4(),
            elapsed_ms: 1,
            status,
            error_code: None,
            retry_after_seconds: None,
        };
        assert!(matches!(
            invoice_error_action(&error),
            InvoiceErrorAction::Retry(_)
        ));
    }

    for status in [StatusCode::BAD_REQUEST, StatusCode::CONFLICT] {
        let error = PaykitClientError::InvoiceNonSuccess {
            request_id: Uuid::new_v4(),
            elapsed_ms: 1,
            status,
            error_code: None,
            retry_after_seconds: None,
        };
        assert!(matches!(
            invoice_error_action(&error),
            InvoiceErrorAction::Fail("paykit invoice admission failed")
        ));
    }
}

struct FixedClock(OffsetDateTime);

impl Clock for FixedClock {
    fn now(&self) -> OffsetDateTime {
        self.0
    }
}

struct SharedClock(Mutex<OffsetDateTime>);

impl SharedClock {
    fn new(now: OffsetDateTime) -> Self {
        Self(Mutex::new(now))
    }

    fn set(&self, now: OffsetDateTime) {
        *self.0.lock().unwrap() = now;
    }
}

impl Clock for SharedClock {
    fn now(&self) -> OffsetDateTime {
        *self.0.lock().unwrap()
    }
}

struct DeadlineCrossingInvoiceCreator<'a> {
    clock: &'a SharedClock,
    response_at: OffsetDateTime,
}

#[async_trait]
impl PaykitInvoiceCreator for DeadlineCrossingInvoiceCreator<'_> {
    async fn create_invoice(
        &self,
        _request_id: Uuid,
        _request: &PaykitInvoiceRequest,
    ) -> Result<(), PaykitClientError> {
        self.clock.set(self.response_at);
        Ok(())
    }
}

struct FixedJitter(time::Duration);

impl InvoiceRetryJitter for FixedJitter {
    fn delay(&self, _attempt_count: u32) -> time::Duration {
        self.0
    }
}

#[derive(Default)]
struct RecordingInvoiceCreator {
    results: Mutex<VecDeque<Result<(), PaykitClientError>>>,
    calls: Mutex<Vec<(Uuid, PaykitInvoiceRequest)>>,
}

impl RecordingInvoiceCreator {
    fn with_results(results: impl IntoIterator<Item = Result<(), PaykitClientError>>) -> Self {
        Self {
            results: Mutex::new(results.into_iter().collect()),
            calls: Mutex::new(Vec::new()),
        }
    }

    fn calls(&self) -> Vec<(Uuid, PaykitInvoiceRequest)> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl PaykitInvoiceCreator for RecordingInvoiceCreator {
    async fn create_invoice(
        &self,
        request_id: Uuid,
        request: &PaykitInvoiceRequest,
    ) -> Result<(), PaykitClientError> {
        self.calls
            .lock()
            .unwrap()
            .push((request_id, request.clone()));
        self.results.lock().unwrap().pop_front().unwrap()
    }
}

fn invoice_task() -> VerificationTaskRecord {
    VerificationTaskRecord {
        task_id: TaskId::from_str("018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10").unwrap(),
        creator: creator(),
        submitted_proof_bundle: SubmittedProofBundle {
            version: SUBMITTED_PROOF_BUNDLE_VERSION,
            bundle_id: BundleId::from_str("000G40R40M30E209185GR38E1W").unwrap(),
            pubky_lock_resource: PubkyLockResource::from_str(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/app.locks/000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG.json",
            )
            .unwrap(),
            reader_public_key: Some(reader()),
            proofs: vec![Proof {
                criterion_id: "criterion-1".to_owned(),
                verifier_type: VerifierType::PaykitPayment,
                payload: serde_json::json!({}),
            }],
        },
        status: VerificationTaskStatus::Pending,
        submitted_at: datetime!(2026-05-29 12:00:00 UTC),
        started_at: None,
        completed_at: None,
        failure_message: None,
        terminal_reason: None,
        entitlement_to_publish: None,
    }
}

fn invoice_intent(task: &VerificationTaskRecord) -> InvoiceAdmissionIntentV1 {
    InvoiceAdmissionIntentV1 {
        version: INVOICE_ADMISSION_INTENT_VERSION,
        creator: task.creator.clone(),
        bundle_id: task.submitted_proof_bundle.bundle_id.clone(),
        lock_resource: task.submitted_proof_bundle.pubky_lock_resource.clone(),
        reader: task
            .submitted_proof_bundle
            .reader_public_key
            .clone()
            .unwrap(),
    }
}

fn creator() -> CreatorPubky {
    CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
}

fn reader() -> CreatorPubky {
    CreatorPubky::from_str("pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky").unwrap()
}
