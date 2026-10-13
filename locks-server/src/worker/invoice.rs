use locks_core::ids::TaskId;
use locks_service::application::errors::ApplicationError;
use locks_service::application::models::InvoiceAdmissionRetryReason;
use locks_service::application::ports::{Clock, InvoiceAdmissionRepository};
use tokio::sync::watch;

use crate::app_state::AppState;
use crate::paykit_http_client::{PaykitClientError, PaykitInvoiceCreator, PaykitInvoiceRequest};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvoiceWorkerTick {
    Idle,
    Ready(TaskId),
    RetryScheduled(TaskId),
    Failed(TaskId),
}

pub trait InvoiceRetryJitter: Send + Sync {
    fn delay(&self, attempt_count: u32) -> time::Duration;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct OsFullJitter;

impl InvoiceRetryJitter for OsFullJitter {
    fn delay(&self, attempt_count: u32) -> time::Duration {
        use rand::RngCore;

        let exponential = 1_u64.checked_shl(attempt_count.min(6)).unwrap_or(u64::MAX);
        let cap_millis = exponential.min(60).saturating_mul(1_000);
        let mut rng = rand::rngs::OsRng;
        let delay_millis = rng.next_u64() % cap_millis.saturating_add(1);
        time::Duration::milliseconds(i64::try_from(delay_millis).unwrap_or(60_000))
    }
}

pub struct InvoiceAdmissionWorker<'a> {
    admissions: &'a dyn InvoiceAdmissionRepository,
    paykit: &'a dyn PaykitInvoiceCreator,
    clock: &'a dyn Clock,
    jitter: &'a dyn InvoiceRetryJitter,
    worker_id: String,
    claim_ttl: time::Duration,
    poll_interval: std::time::Duration,
}

impl<'a> InvoiceAdmissionWorker<'a> {
    pub fn new(
        admissions: &'a dyn InvoiceAdmissionRepository,
        paykit: &'a dyn PaykitInvoiceCreator,
        clock: &'a dyn Clock,
        jitter: &'a dyn InvoiceRetryJitter,
        worker_id: String,
        claim_ttl: time::Duration,
    ) -> Self {
        Self {
            admissions,
            paykit,
            clock,
            jitter,
            worker_id,
            claim_ttl,
            poll_interval: std::time::Duration::from_secs(1),
        }
    }

    pub fn from_state(
        state: &'a AppState,
        paykit: &'a dyn PaykitInvoiceCreator,
        jitter: &'a dyn InvoiceRetryJitter,
    ) -> Self {
        Self {
            admissions: state.invoice_admissions().as_ref(),
            paykit,
            clock: state.clock().as_ref(),
            jitter,
            worker_id: format!("{}-invoice", state.config().worker.worker_id),
            claim_ttl: claim_timeout(state.config().worker.claim_timeout_seconds),
            poll_interval: std::time::Duration::from_millis(state.config().worker.poll_interval_ms),
        }
    }

    pub async fn run_once(&self) -> Result<InvoiceWorkerTick, ApplicationError> {
        let Some(claim) = self
            .admissions
            .claim_next_invoice_admission(&self.worker_id, self.clock.now(), self.claim_ttl)
            .await?
        else {
            return Ok(InvoiceWorkerTick::Idle);
        };
        let task_id = claim.admission.task.task_id;
        if claim.deadline_expired {
            return self
                .fail(
                    &task_id,
                    &claim.claim_token,
                    "invoice admission deadline exceeded",
                )
                .await;
        }
        let intent = &claim.admission.intent;
        let request = PaykitInvoiceRequest {
            bundle_id: intent.bundle_id.to_string(),
            lock_resource: intent.lock_resource.to_string(),
            reader: intent.reader.to_string(),
        };
        match self
            .paykit
            .create_invoice(task_id.as_uuid(), &request)
            .await
        {
            Ok(()) => {
                let updated = self
                    .admissions
                    .mark_invoice_admission_ready(
                        &task_id,
                        &self.worker_id,
                        &claim.claim_token,
                        self.clock.now(),
                    )
                    .await?;
                Ok(if updated.is_some() {
                    InvoiceWorkerTick::Ready(task_id)
                } else {
                    InvoiceWorkerTick::Idle
                })
            }
            Err(error) => {
                log_invoice_error(&error);
                match invoice_error_action(&error) {
                    InvoiceErrorAction::Retry(retry_after) => {
                        let retry_after = retry_after
                            .unwrap_or_else(|| self.jitter.delay(claim.admission.attempt_count));
                        let updated = self
                            .admissions
                            .schedule_invoice_admission_retry(
                                &task_id,
                                &self.worker_id,
                                &claim.claim_token,
                                self.clock.now(),
                                retry_after,
                                invoice_retry_reason(&error),
                            )
                            .await?;
                        Ok(if updated.is_some() {
                            InvoiceWorkerTick::RetryScheduled(task_id)
                        } else {
                            InvoiceWorkerTick::Idle
                        })
                    }
                    InvoiceErrorAction::Fail(message) => {
                        self.fail(&task_id, &claim.claim_token, message).await
                    }
                }
            }
        }
    }

    async fn fail(
        &self,
        task_id: &TaskId,
        claim_token: &uuid::Uuid,
        message: &str,
    ) -> Result<InvoiceWorkerTick, ApplicationError> {
        let updated = self
            .admissions
            .mark_invoice_admission_failed(
                task_id,
                &self.worker_id,
                claim_token,
                self.clock.now(),
                message,
            )
            .await?;
        Ok(if updated.is_some() {
            InvoiceWorkerTick::Failed(*task_id)
        } else {
            InvoiceWorkerTick::Idle
        })
    }

    pub async fn run_until_shutdown(
        &self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), ApplicationError> {
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            match self.run_once().await {
                Ok(InvoiceWorkerTick::Idle) => {
                    tokio::select! {
                        _ = shutdown.changed() => {
                            if *shutdown.borrow() {
                                return Ok(());
                            }
                        }
                        _ = tokio::time::sleep(self.poll_interval) => {}
                    }
                }
                Err(error) => {
                    tracing::error!(%error, worker_id = %self.worker_id, "invoice admission worker tick failed");
                    tokio::select! {
                        _ = shutdown.changed() => {
                            if *shutdown.borrow() {
                                return Ok(());
                            }
                        }
                        _ = tokio::time::sleep(self.poll_interval) => {}
                    }
                }
                Ok(InvoiceWorkerTick::Ready(_))
                | Ok(InvoiceWorkerTick::RetryScheduled(_))
                | Ok(InvoiceWorkerTick::Failed(_)) => {}
            }
        }
    }
}

pub(super) enum InvoiceErrorAction {
    Retry(Option<time::Duration>),
    Fail(&'static str),
}

pub(super) fn invoice_error_action(error: &PaykitClientError) -> InvoiceErrorAction {
    match error {
        PaykitClientError::InvoiceTransport { .. }
        | PaykitClientError::InvalidInvoiceResponse(_)
        | PaykitClientError::InvalidInvoiceJson(_)
        | PaykitClientError::InvoiceBodyTooLarge
        | PaykitClientError::InvalidInvoiceTimestamp(_)
        | PaykitClientError::InvalidInvoiceTimestamps => InvoiceErrorAction::Retry(None),
        PaykitClientError::InvoiceNonSuccess {
            status,
            retry_after_seconds,
            ..
        } if *status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error() => {
            InvoiceErrorAction::Retry(retry_after_seconds.map(|seconds| {
                time::Duration::seconds(i64::try_from(seconds).unwrap_or(60).min(60))
            }))
        }
        PaykitClientError::InvoiceNonSuccess {
            status: reqwest::StatusCode::CONFLICT,
            error_code: Some("reader_not_payable"),
            ..
        } => InvoiceErrorAction::Fail("reader is not payable"),
        PaykitClientError::InvoiceNonSuccess {
            status: reqwest::StatusCode::CONFLICT,
            error_code: Some("invoice_conflict"),
            ..
        } => InvoiceErrorAction::Fail("paykit invoice conflict"),
        PaykitClientError::InvoiceNonSuccess {
            status: reqwest::StatusCode::CONFLICT,
            ..
        } => InvoiceErrorAction::Fail("paykit invoice admission failed"),
        _ => InvoiceErrorAction::Fail("paykit invoice admission failed"),
    }
}

fn invoice_retry_reason(error: &PaykitClientError) -> Option<InvoiceAdmissionRetryReason> {
    match error {
        PaykitClientError::InvoiceNonSuccess {
            status: reqwest::StatusCode::SERVICE_UNAVAILABLE,
            error_code: Some("reader_setup_pending"),
            ..
        }
        | PaykitClientError::InvoiceNonSuccess {
            status: reqwest::StatusCode::BAD_GATEWAY,
            error_code: Some("reader_registry_malformed"),
            ..
        } => Some(InvoiceAdmissionRetryReason::ReaderWalletSetupNeeded),
        _ => None,
    }
}

fn log_invoice_error(error: &PaykitClientError) {
    match error {
        PaykitClientError::InvoiceNonSuccess {
            request_id,
            elapsed_ms,
            status,
            error_code,
            retry_after_seconds,
        } => tracing::warn!(
            operation = "invoice creation",
            %request_id,
            status = status.as_u16(),
            error_code,
            retry_after_seconds,
            elapsed_ms,
            "Paykit request returned non-success status"
        ),
        PaykitClientError::InvoiceTransport {
            request_id,
            elapsed_ms,
            timeout,
            connect,
        } => tracing::warn!(
            operation = "invoice creation",
            %request_id,
            elapsed_ms,
            timeout,
            connect,
            "Paykit request transport failed"
        ),
        _ => tracing::warn!("Paykit invoice creation failed"),
    }
}

fn claim_timeout(seconds: u64) -> time::Duration {
    time::Duration::seconds(i64::try_from(seconds).unwrap_or(i64::MAX))
}
