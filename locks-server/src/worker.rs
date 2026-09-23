use std::sync::atomic::{AtomicU8, Ordering};

use async_trait::async_trait;
use locks_core::ids::{LockServerPubky, TaskId};
use locks_core::verification::CriterionVerificationResult;
use locks_service::application::errors::ApplicationError;
use locks_service::application::models::{
    ClaimedVerificationTask, CriterionVerificationRequest, VerificationTaskRecord,
};
use locks_service::application::ports::{
    Clock, ContentLockRepository, CriterionVerifier, EntitlementRepository,
    VerificationTaskClaimer, VerificationTaskRepository,
};
use locks_service::application::use_cases::complete_verification_task::{
    CompleteVerificationTaskRequest, CompleteVerificationTaskUseCase,
};
use locks_service::infrastructure::verifiers::registry::StaticCriterionVerifierRegistry;
use tokio::sync::watch;
use tracing::{debug, error, info};

use crate::app_state::{AppState, WorkerKind, WorkerReadiness, WorkerReadinessEvidence};

const PENDING_VERIFICATION_RETRY_DELAY_SECONDS: i64 = 30;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PaykitProviderEvidence {
    None,
    HealthyResponse,
    Unavailable,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum OperationEvidence {
    #[default]
    None,
    Succeeded,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct RepositoryOperationEvidence {
    queue_poll: OperationEvidence,
    mutation: OperationEvidence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct VerificationPoll {
    tick: WorkerTick,
    paykit_provider: PaykitProviderEvidence,
    repository: RepositoryOperationEvidence,
}

impl VerificationPoll {
    fn without_provider_evidence(tick: WorkerTick) -> Self {
        Self {
            tick,
            paykit_provider: PaykitProviderEvidence::None,
            repository: RepositoryOperationEvidence::default(),
        }
    }
}

#[derive(Debug)]
struct VerificationPollError {
    operation: RepositoryOperation,
    error: ApplicationError,
    repository: RepositoryOperationEvidence,
}

#[derive(Default)]
struct RepositoryOperationObserver {
    queue_poll_succeeded: AtomicU8,
    mutation_succeeded: AtomicU8,
}

impl RepositoryOperationObserver {
    fn record_queue_poll_success(&self) {
        self.queue_poll_succeeded.store(1, Ordering::Relaxed);
    }

    fn record_mutation_success(&self) {
        self.mutation_succeeded.store(1, Ordering::Relaxed);
    }

    fn evidence(&self) -> RepositoryOperationEvidence {
        RepositoryOperationEvidence {
            queue_poll: if self.queue_poll_succeeded.load(Ordering::Relaxed) == 1 {
                OperationEvidence::Succeeded
            } else {
                OperationEvidence::None
            },
            mutation: if self.mutation_succeeded.load(Ordering::Relaxed) == 1 {
                OperationEvidence::Succeeded
            } else {
                OperationEvidence::None
            },
        }
    }
}

struct ObservedVerificationTaskClaimer<'a> {
    inner: &'a dyn VerificationTaskClaimer,
    observer: &'a RepositoryOperationObserver,
}

#[async_trait]
impl VerificationTaskClaimer for ObservedVerificationTaskClaimer<'_> {
    async fn begin_claimed_entitlement_publication(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
    ) -> Result<bool, ApplicationError> {
        let result = self
            .inner
            .begin_claimed_entitlement_publication(task_id, worker_id, claim_token)
            .await;
        if matches!(result, Ok(true)) {
            self.observer.record_mutation_success();
        }
        result
    }

    async fn claim_next_verification_task(
        &self,
        worker_id: &str,
        claim_ttl: time::Duration,
    ) -> Result<Option<ClaimedVerificationTask>, ApplicationError> {
        let result = self
            .inner
            .claim_next_verification_task(worker_id, claim_ttl)
            .await;
        if result.is_ok() {
            self.observer.record_queue_poll_success();
        }
        result
    }

    async fn schedule_verification_task_retry(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        retry_after: time::Duration,
    ) -> Result<Option<VerificationTaskRecord>, ApplicationError> {
        let result = self
            .inner
            .schedule_verification_task_retry(task_id, worker_id, claim_token, retry_after)
            .await;
        if matches!(result, Ok(Some(_))) {
            self.observer.record_mutation_success();
        }
        result
    }

    async fn persist_claimed_verification_task_transition(
        &self,
        task: VerificationTaskRecord,
        worker_id: &str,
        claim_token: &uuid::Uuid,
    ) -> Result<Option<VerificationTaskRecord>, ApplicationError> {
        let result = self
            .inner
            .persist_claimed_verification_task_transition(task, worker_id, claim_token)
            .await;
        if matches!(result, Ok(Some(_))) {
            self.observer.record_mutation_success();
        }
        result
    }
}

struct ObservedPaykitVerifier<'a> {
    inner: &'a dyn CriterionVerifier,
    evidence: AtomicU8,
}

impl<'a> ObservedPaykitVerifier<'a> {
    const HEALTHY: u8 = 1;
    const UNAVAILABLE: u8 = 2;

    fn new(inner: &'a dyn CriterionVerifier) -> Self {
        Self {
            inner,
            evidence: AtomicU8::new(0),
        }
    }

    fn evidence(&self) -> PaykitProviderEvidence {
        match self.evidence.load(Ordering::Relaxed) {
            Self::HEALTHY => PaykitProviderEvidence::HealthyResponse,
            Self::UNAVAILABLE => PaykitProviderEvidence::Unavailable,
            _ => PaykitProviderEvidence::None,
        }
    }
}

#[async_trait]
impl CriterionVerifier for ObservedPaykitVerifier<'_> {
    async fn verify(
        &self,
        request: CriterionVerificationRequest,
    ) -> Result<CriterionVerificationResult, ApplicationError> {
        let result = self.inner.verify(request).await;
        match &result {
            Err(ApplicationError::VerificationDependencyUnavailable) => {
                self.evidence.store(Self::UNAVAILABLE, Ordering::Relaxed);
            }
            Ok(_) | Err(ApplicationError::VerificationPending) => {
                self.evidence
                    .compare_exchange(0, Self::HEALTHY, Ordering::Relaxed, Ordering::Relaxed)
                    .ok();
            }
            Err(_) => {}
        }
        result
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VerificationPollErrorDisposition {
    RetryableRepository,
    Fatal(&'static str),
}

fn classify_verification_poll_error(error: &ApplicationError) -> VerificationPollErrorDisposition {
    match error {
        ApplicationError::Storage { .. } => VerificationPollErrorDisposition::RetryableRepository,
        ApplicationError::InvalidVerificationTaskState { .. }
        | ApplicationError::InvalidVerificationTaskTransition { .. }
        | ApplicationError::InvalidVerificationTaskFailureMessage => {
            VerificationPollErrorDisposition::Fatal("invalid_verification_task_state")
        }
        _ => VerificationPollErrorDisposition::Fatal("unexpected_application_error"),
    }
}

fn redacted_fatal_verification_error(error_class: &'static str) -> ApplicationError {
    ApplicationError::InvalidVerificationTaskState {
        message: format!("verification worker terminated after {error_class}"),
    }
}

fn is_terminal_business_failure(error: &ApplicationError) -> bool {
    matches!(
        error,
        ApplicationError::ContentLockUnavailable
            | ApplicationError::EntitlementNotSatisfied
            | ApplicationError::UnsupportedVerifierType { .. }
            | ApplicationError::Verifier { .. }
            | ApplicationError::ContentLockHashMismatch { .. }
            | ApplicationError::ContentLockCanonicalization { .. }
            | ApplicationError::EmptyContentLockCriteria
            | ApplicationError::DuplicateContentLockCriterion { .. }
            | ApplicationError::DuplicateVerificationResultCriterion { .. }
            | ApplicationError::UnknownVerificationResultCriterion { .. }
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RepositoryOperation {
    QueuePoll,
    Mutation,
}

#[derive(Debug, Default)]
struct VerificationReadinessRecovery {
    paykit_provider_degraded: bool,
    queue_poll_degraded: bool,
    mutation_degraded: bool,
}

impl VerificationReadinessRecovery {
    fn record_poll(&mut self, poll: VerificationPoll, readiness: &WorkerReadiness) {
        self.record_repository_evidence(poll.repository, readiness);
        match poll.paykit_provider {
            PaykitProviderEvidence::HealthyResponse => {
                self.paykit_provider_degraded = false;
                self.record_ready_if_healthy(readiness);
            }
            PaykitProviderEvidence::Unavailable => {
                self.paykit_provider_degraded = true;
                readiness.record(
                    WorkerKind::Verification,
                    WorkerReadinessEvidence::TransientDependencyFailure,
                );
            }
            PaykitProviderEvidence::None => {}
        }
    }

    fn record_repository_evidence(
        &mut self,
        evidence: RepositoryOperationEvidence,
        readiness: &WorkerReadiness,
    ) {
        if evidence.queue_poll == OperationEvidence::Succeeded {
            self.record_repository_success(RepositoryOperation::QueuePoll, readiness);
        }
        if evidence.mutation == OperationEvidence::Succeeded {
            self.record_repository_success(RepositoryOperation::Mutation, readiness);
        }
    }

    fn record_repository_failure(
        &mut self,
        operation: RepositoryOperation,
        readiness: &WorkerReadiness,
    ) {
        match operation {
            RepositoryOperation::QueuePoll => self.queue_poll_degraded = true,
            RepositoryOperation::Mutation => self.mutation_degraded = true,
        }
        readiness.record(
            WorkerKind::Verification,
            WorkerReadinessEvidence::TransientDependencyFailure,
        );
    }

    fn record_repository_success(
        &mut self,
        operation: RepositoryOperation,
        readiness: &WorkerReadiness,
    ) {
        match operation {
            RepositoryOperation::QueuePoll => self.queue_poll_degraded = false,
            RepositoryOperation::Mutation => self.mutation_degraded = false,
        }
        self.record_ready_if_healthy(readiness);
    }

    fn record_ready_if_healthy(&self, readiness: &WorkerReadiness) {
        if !self.paykit_provider_degraded && !self.queue_poll_degraded && !self.mutation_degraded {
            readiness.record(
                WorkerKind::Verification,
                WorkerReadinessEvidence::DependencySucceeded,
            );
        }
    }
}

/// Result of one worker polling attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerTick {
    Cancelled,
    Idle,
    Completed(TaskId),
    VerificationPendingRetryScheduled(TaskId),
    RetryScheduled(TaskId),
    Failed(TaskId),
}

/// In-process verification worker.
pub struct VerificationWorker<'a> {
    tasks: &'a dyn VerificationTaskRepository,
    claimer: &'a dyn VerificationTaskClaimer,
    content_locks: &'a dyn ContentLockRepository,
    entitlements: &'a dyn EntitlementRepository,
    dev_static_verifier: &'a dyn CriterionVerifier,
    paykit_payment_verifier: Option<&'a dyn CriterionVerifier>,
    allow_dev_static_verifier: bool,
    clock: &'a dyn Clock,
    verified_by: LockServerPubky,
    worker_id: String,
    poll_interval: std::time::Duration,
    claim_timeout_seconds: u64,
}

impl<'a> VerificationWorker<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tasks: &'a dyn VerificationTaskRepository,
        claimer: &'a dyn VerificationTaskClaimer,
        content_locks: &'a dyn ContentLockRepository,
        entitlements: &'a dyn EntitlementRepository,
        dev_static_verifier: &'a dyn CriterionVerifier,
        paykit_payment_verifier: Option<&'a dyn CriterionVerifier>,
        allow_dev_static_verifier: bool,
        clock: &'a dyn Clock,
        verified_by: LockServerPubky,
        worker_id: String,
        poll_interval: std::time::Duration,
        claim_timeout_seconds: u64,
    ) -> Self {
        Self {
            tasks,
            claimer,
            content_locks,
            entitlements,
            dev_static_verifier,
            paykit_payment_verifier,
            allow_dev_static_verifier,
            clock,
            verified_by,
            worker_id,
            poll_interval,
            claim_timeout_seconds,
        }
    }

    pub fn from_state(state: &'a AppState) -> Self {
        // Dev-static verification is a local/dev verifier only. Production-mode
        // workers intentionally build the registry without it; production
        // completion should happen through this worker path, not through the
        // dev-only HTTP completion route.
        Self::new(
            state.verification_tasks().as_ref(),
            state.verification_task_claimer().as_ref(),
            state.content_locks().as_ref(),
            state.entitlements().as_ref(),
            state.dev_static_verifier().as_ref(),
            state
                .paykit_payment_verifier()
                .map(|verifier| verifier.as_ref() as &dyn CriterionVerifier),
            state.config().runtime.environment.is_development(),
            state.clock().as_ref(),
            state.config().credentials.lock_server_public_key.clone(),
            state.config().worker.worker_id.clone(),
            std::time::Duration::from_millis(state.config().worker.poll_interval_ms),
            state.config().worker.claim_timeout_seconds,
        )
    }

    pub async fn run_once(&self) -> Result<WorkerTick, ApplicationError> {
        self.run_once_with_recovery_evidence()
            .await
            .map(|poll| poll.tick)
            .map_err(|failure| failure.error)
    }

    async fn run_once_with_recovery_evidence(
        &self,
    ) -> Result<VerificationPoll, VerificationPollError> {
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        self.run_once_until_shutdown_with_recovery_evidence(&shutdown_rx)
            .await
    }

    pub async fn run_once_until_shutdown(
        &self,
        shutdown: &watch::Receiver<bool>,
    ) -> Result<WorkerTick, ApplicationError> {
        self.run_once_until_shutdown_with_recovery_evidence(shutdown)
            .await
            .map(|poll| poll.tick)
            .map_err(|failure| failure.error)
    }

    async fn run_once_until_shutdown_with_recovery_evidence(
        &self,
        shutdown: &watch::Receiver<bool>,
    ) -> Result<VerificationPoll, VerificationPollError> {
        let observer = RepositoryOperationObserver::default();
        let claimer = ObservedVerificationTaskClaimer {
            inner: self.claimer,
            observer: &observer,
        };
        match self.run_once_until_shutdown_inner(shutdown, &claimer).await {
            Ok(mut poll) => {
                poll.repository = observer.evidence();
                Ok(poll)
            }
            Err((operation, error)) => Err(VerificationPollError {
                operation,
                error,
                repository: observer.evidence(),
            }),
        }
    }

    async fn schedule_claimed_retry(
        &self,
        claimer: &dyn VerificationTaskClaimer,
        task_id: &TaskId,
        claim_token: &uuid::Uuid,
    ) -> Result<Option<time::OffsetDateTime>, ApplicationError> {
        let retry_scheduled_at = self.clock.now();
        let next_attempt_at = retry_scheduled_at + retry_delay();
        claimer
            .schedule_verification_task_retry(
                task_id,
                &self.worker_id,
                claim_token,
                next_attempt_at - retry_scheduled_at,
            )
            .await
            .map(|scheduled| scheduled.map(|_| next_attempt_at))
    }

    async fn run_once_until_shutdown_inner(
        &self,
        shutdown: &watch::Receiver<bool>,
        claimer: &dyn VerificationTaskClaimer,
    ) -> Result<VerificationPoll, (RepositoryOperation, ApplicationError)> {
        if *shutdown.borrow() {
            return Ok(VerificationPoll::without_provider_evidence(
                WorkerTick::Cancelled,
            ));
        }
        let now = self.clock.now();
        let claim_expires_at = now + claim_timeout(self.claim_timeout_seconds);
        let Some(claim) = claimer
            .claim_next_verification_task(&self.worker_id, (claim_expires_at) - (now))
            .await
            .map_err(|error| (RepositoryOperation::QueuePoll, error))?
        else {
            return Ok(VerificationPoll::without_provider_evidence(
                WorkerTick::Idle,
            ));
        };

        let task_id = claim.task.task_id;
        let claim_token = claim.claim_token;
        if *shutdown.borrow() {
            let _ = claimer
                .schedule_verification_task_retry(
                    &task_id,
                    &self.worker_id,
                    &claim_token,
                    time::Duration::ZERO,
                )
                .await
                .map_err(|error| (RepositoryOperation::Mutation, error))?;
            return Ok(VerificationPoll::without_provider_evidence(
                WorkerTick::Cancelled,
            ));
        }
        debug!(%task_id, worker_id = %self.worker_id, "claimed verification task");
        let mut verifiers = StaticCriterionVerifierRegistry::new();
        if self.allow_dev_static_verifier {
            verifiers = verifiers.with_dev_static(self.dev_static_verifier);
        }
        let observed_paykit_verifier = self
            .paykit_payment_verifier
            .map(ObservedPaykitVerifier::new);
        if let Some(paykit_payment_verifier) = observed_paykit_verifier.as_ref() {
            verifiers = verifiers.with_paykit_payment(paykit_payment_verifier);
        }
        let use_case = CompleteVerificationTaskUseCase::new(
            self.tasks,
            self.content_locks,
            self.entitlements,
            &verifiers,
            self.clock,
            self.verified_by.clone(),
        );

        let tick = match use_case
            .execute_claimed(
                CompleteVerificationTaskRequest { task_id },
                claim,
                &self.worker_id,
                claimer,
            )
            .await
        {
            Ok(completed) => {
                info!(%task_id, status = ?completed.status, "completed verification task");
                WorkerTick::Completed(task_id)
            }
            Err(ApplicationError::VerificationDependencyUnavailable) => {
                let Some(next_attempt_at) = self
                    .schedule_claimed_retry(claimer, &task_id, &claim_token)
                    .await
                    .map_err(|error| (RepositoryOperation::Mutation, error))?
                else {
                    info!(
                        %task_id,
                        worker_id = %self.worker_id,
                        "verification task claim no longer owned; retry not scheduled"
                    );
                    return Ok(VerificationPoll {
                        tick: WorkerTick::Idle,
                        paykit_provider: observed_paykit_verifier.as_ref().map_or(
                            PaykitProviderEvidence::None,
                            ObservedPaykitVerifier::evidence,
                        ),
                        repository: RepositoryOperationEvidence::default(),
                    });
                };
                debug!(
                    %task_id,
                    worker_id = %self.worker_id,
                    %next_attempt_at,
                    "scheduled verification task retry after dependency failure"
                );
                WorkerTick::RetryScheduled(task_id)
            }
            Err(ApplicationError::VerificationPending) => {
                let Some(next_attempt_at) = self
                    .schedule_claimed_retry(claimer, &task_id, &claim_token)
                    .await
                    .map_err(|error| (RepositoryOperation::Mutation, error))?
                else {
                    info!(
                        %task_id,
                        worker_id = %self.worker_id,
                        "verification task claim no longer owned; retry not scheduled"
                    );
                    return Ok(VerificationPoll {
                        tick: WorkerTick::Idle,
                        paykit_provider: observed_paykit_verifier.as_ref().map_or(
                            PaykitProviderEvidence::None,
                            ObservedPaykitVerifier::evidence,
                        ),
                        repository: RepositoryOperationEvidence::default(),
                    });
                };
                debug!(
                    %task_id,
                    worker_id = %self.worker_id,
                    %next_attempt_at,
                    "scheduled verification task retry"
                );
                WorkerTick::VerificationPendingRetryScheduled(task_id)
            }
            Err(ApplicationError::VerificationTaskClaimLost) => {
                info!(
                    %task_id,
                    worker_id = %self.worker_id,
                    "verification task claim no longer owned; terminal state not persisted"
                );
                WorkerTick::Idle
            }
            Err(error) if is_terminal_business_failure(&error) => {
                error!(
                    %task_id,
                    error_class = "terminal_business_failure",
                    retrying = false,
                    "verification task failed"
                );
                WorkerTick::Failed(task_id)
            }
            Err(error) => return Err((RepositoryOperation::Mutation, error)),
        };
        Ok(VerificationPoll {
            tick,
            paykit_provider: observed_paykit_verifier.as_ref().map_or(
                PaykitProviderEvidence::None,
                ObservedPaykitVerifier::evidence,
            ),
            repository: RepositoryOperationEvidence::default(),
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

            match self.run_once_until_shutdown(&shutdown).await? {
                WorkerTick::Cancelled => return Ok(()),
                WorkerTick::Idle => {
                    tokio::select! {
                        _ = shutdown.changed() => {
                            if *shutdown.borrow() {
                                return Ok(());
                            }
                        }
                        _ = tokio::time::sleep(self.poll_interval) => {}
                    }
                }
                WorkerTick::Completed(_)
                | WorkerTick::VerificationPendingRetryScheduled(_)
                | WorkerTick::RetryScheduled(_)
                | WorkerTick::Failed(_) => {}
            }
        }
    }

    pub async fn run_until_shutdown_with_readiness(
        &self,
        mut shutdown: watch::Receiver<bool>,
        readiness: &WorkerReadiness,
    ) -> Result<(), ApplicationError> {
        let mut recovery = VerificationReadinessRecovery::default();
        loop {
            if *shutdown.borrow() {
                readiness.record(WorkerKind::Verification, WorkerReadinessEvidence::Stopped);
                return Ok(());
            }

            match self
                .run_once_until_shutdown_with_recovery_evidence(&shutdown)
                .await
            {
                Ok(VerificationPoll {
                    tick: WorkerTick::Cancelled,
                    ..
                }) => {
                    readiness.record(WorkerKind::Verification, WorkerReadinessEvidence::Stopped);
                    return Ok(());
                }
                Ok(
                    poll @ VerificationPoll {
                        tick: WorkerTick::Idle,
                        ..
                    },
                ) => {
                    recovery.record_poll(poll, readiness);
                    tokio::select! {
                        _ = shutdown.changed() => {}
                        _ = tokio::time::sleep(self.poll_interval) => {}
                    }
                }
                Ok(poll) => recovery.record_poll(poll, readiness),
                Err(failure) => {
                    recovery.record_repository_evidence(failure.repository, readiness);
                    match classify_verification_poll_error(&failure.error) {
                        VerificationPollErrorDisposition::RetryableRepository => {
                            recovery.record_repository_failure(failure.operation, readiness);
                            error!(
                                operation = match failure.operation {
                                    RepositoryOperation::QueuePoll => "verification_queue_poll",
                                    RepositoryOperation::Mutation =>
                                        "verification_repository_mutation",
                                },
                                error_class = "repository_unavailable",
                                retrying = true,
                                "verification worker repository operation failed"
                            );
                            tokio::select! {
                                _ = shutdown.changed() => {}
                                _ = tokio::time::sleep(self.poll_interval) => {}
                            }
                        }
                        VerificationPollErrorDisposition::Fatal(error_class) => {
                            readiness.record(
                                WorkerKind::Verification,
                                WorkerReadinessEvidence::UnexpectedExit,
                            );
                            error!(
                                operation = match failure.operation {
                                    RepositoryOperation::QueuePoll => "verification_queue_poll",
                                    RepositoryOperation::Mutation =>
                                        "verification_repository_mutation",
                                },
                                error_class,
                                retrying = false,
                                "verification worker terminated after unexpected application error"
                            );
                            return Err(redacted_fatal_verification_error(error_class));
                        }
                    }
                }
            }
        }
    }
}

fn claim_timeout(seconds: u64) -> time::Duration {
    time::Duration::seconds(i64::try_from(seconds).unwrap_or(i64::MAX))
}

fn retry_delay() -> time::Duration {
    time::Duration::seconds(PENDING_VERIFICATION_RETRY_DELAY_SECONDS)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use async_trait::async_trait;
    use locks_core::ids::{
        BundleId, CreatorPubky, GuardedResourceHash, LockServerPubky, PubkyLockResource, TaskId,
    };
    use locks_core::lock_policy::{
        AccessPolicy, CONTENT_LOCK_VERSION, ContentLock, Criterion, GuardedResource, LockLogic,
        LockServerConfig, VerifierType,
    };
    use locks_core::verification::{
        CriterionVerificationResult, Proof, SUBMITTED_PROOF_BUNDLE_VERSION, SubmittedProofBundle,
    };
    use locks_service::application::errors::ApplicationError;
    use locks_service::application::models::{
        ClaimedVerificationTask, CriterionVerificationRequest, VerificationTaskRecord,
        VerificationTaskStatus,
    };
    use locks_service::application::ports::{
        ContentLockRepository, CriterionVerifier, EntitlementRepository, VerificationTaskClaimer,
        VerificationTaskRepository,
    };
    use locks_service::infrastructure::memory::{
        content_locks::InMemoryContentLockRepository, entitlements::InMemoryEntitlementRepository,
        verification_task_claims::InMemoryVerificationTaskClaimer,
        verification_tasks::InMemoryVerificationTaskRepository,
    };
    use locks_service::infrastructure::verifiers::dev_static::DevStaticVerifier;
    use locks_service::infrastructure::verifiers::paykit_payment::{
        PaykitPaymentStatus, PaykitPaymentStatusClient, PaykitPaymentStatusError,
        PaykitPaymentStatusKind, PaykitPaymentVerifier,
    };
    use time::macros::datetime;
    use tokio::sync::{Notify, watch};

    use crate::app_state::{AppState, ReadinessStatus, SystemClock, WorkerReadiness};
    use crate::config::{
        ContentLocksConfig, DatabaseConfig, LockServerCredentialsConfig, LockServerRuntimeConfig,
        LoggingConfig, PubkyConfig, RateLimitsConfig, RuntimeConfig, RuntimeEnvironment,
        SecretsConfig, WorkerConfig,
    };
    use crate::worker::{
        VerificationPollErrorDisposition, VerificationWorker, WorkerTick,
        classify_verification_poll_error, redacted_fatal_verification_error, retry_delay,
    };

    const TASK_ID: &str = "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10";
    const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";

    #[tokio::test]
    async fn worker_completes_pending_task() {
        let fixture = WorkerFixture::new(content_lock(true)).await;
        fixture.seed_task().await;
        let worker = fixture.worker();

        assert_eq!(
            worker.run_once().await.unwrap(),
            WorkerTick::Completed(task_id())
        );

        let stored = fixture
            .tasks
            .get_verification_task(&task_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.status, VerificationTaskStatus::Completed);
        assert!(
            fixture
                .entitlements
                .get_verified_proof_bundle(&creator(), &bundle_id())
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn worker_marks_failed_verification_as_failed() {
        let fixture = WorkerFixture::new(content_lock(false)).await;
        fixture.seed_task().await;
        let worker = fixture.worker();

        let tick = worker.run_once().await.unwrap();

        assert_eq!(tick, WorkerTick::Failed(task_id()));
        let stored = fixture
            .tasks
            .get_verification_task(&task_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.status, VerificationTaskStatus::Failed);
        assert_eq!(
            stored.failure_message,
            Some("entitlement not satisfied".to_owned())
        );
    }

    #[tokio::test]
    async fn worker_schedules_pending_verification_without_hot_looping_then_completes() {
        let fixture = WorkerFixture::new(content_lock(true)).await;
        fixture.seed_task().await;
        let verifier = RetryOnceVerifier::default();
        let worker = fixture.worker_with_verifier(&verifier);
        let readiness = WorkerReadiness::new(true, false);
        let mut recovery = super::VerificationReadinessRecovery::default();

        let poll = worker.run_once_with_recovery_evidence().await.unwrap();
        assert_eq!(
            poll.tick,
            WorkerTick::VerificationPendingRetryScheduled(task_id())
        );
        assert_eq!(
            poll.repository.queue_poll,
            super::OperationEvidence::Succeeded
        );
        assert_eq!(
            poll.repository.mutation,
            super::OperationEvidence::Succeeded
        );
        recovery.record_poll(poll, &readiness);
        assert_eq!(readiness.status(), ReadinessStatus::Ready);
        let stored = fixture
            .tasks
            .get_verification_task(&task_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.status, VerificationTaskStatus::Pending);
        assert_eq!(stored.failure_message, None);
        assert_eq!(worker.run_once().await.unwrap(), WorkerTick::Idle);
    }

    #[tokio::test]
    async fn paykit_provider_outage_schedules_retry_and_degrades_readiness() {
        let fixture = WorkerFixture::new(paykit_content_lock()).await;
        fixture.seed_task().await;
        let status_client = FakePaykitStatusClient::failing();
        let verifier = PaykitPaymentVerifier::new(&status_client, 1);
        let worker = fixture.worker_with_paykit_verifier(&verifier);
        let readiness = WorkerReadiness::new(true, false);
        let mut recovery = super::VerificationReadinessRecovery::default();

        let poll = worker.run_once_with_recovery_evidence().await.unwrap();

        assert_eq!(poll.tick, WorkerTick::RetryScheduled(task_id()));
        recovery.record_poll(poll, &readiness);
        assert_eq!(readiness.status(), ReadinessStatus::Degraded);
        assert_eq!(status_client.calls.load(Ordering::SeqCst), 1);
        let stored = fixture
            .tasks
            .get_verification_task(&task_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.status, VerificationTaskStatus::Pending);
        assert_eq!(stored.failure_message, None);
        assert_eq!(worker.run_once().await.unwrap(), WorkerTick::Idle);
    }

    #[tokio::test]
    async fn healthy_paykit_pending_schedules_retry_without_degrading_readiness() {
        let fixture = WorkerFixture::new(paykit_content_lock()).await;
        fixture.seed_task().await;
        let status_client = FakePaykitStatusClient::healthy_pending();
        let verifier = PaykitPaymentVerifier::new(&status_client, 1);
        let worker = fixture.worker_with_paykit_verifier(&verifier);
        let readiness = WorkerReadiness::new(true, false);
        let mut recovery = super::VerificationReadinessRecovery::default();

        let poll = worker.run_once_with_recovery_evidence().await.unwrap();

        assert_eq!(
            poll.tick,
            WorkerTick::VerificationPendingRetryScheduled(task_id())
        );
        recovery.record_poll(poll, &readiness);
        assert_eq!(readiness.status(), ReadinessStatus::Ready);
        assert_eq!(status_client.calls.load(Ordering::SeqCst), 1);
        let stored = fixture
            .tasks
            .get_verification_task(&task_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.status, VerificationTaskStatus::Pending);
        assert_eq!(stored.failure_message, None);
        assert_eq!(worker.run_once().await.unwrap(), WorkerTick::Idle);
    }

    #[tokio::test]
    async fn paykit_degradation_ignores_unrelated_work_until_healthy_paykit_response() {
        let readiness = WorkerReadiness::new(true, false);
        let mut recovery = super::VerificationReadinessRecovery::default();

        let outage_fixture = WorkerFixture::new(paykit_content_lock()).await;
        outage_fixture.seed_task().await;
        let outage_client = FakePaykitStatusClient::failing();
        let outage_verifier = PaykitPaymentVerifier::new(&outage_client, 1);
        let outage_worker = outage_fixture.worker_with_paykit_verifier(&outage_verifier);
        recovery.record_poll(
            outage_worker
                .run_once_with_recovery_evidence()
                .await
                .unwrap(),
            &readiness,
        );
        assert_eq!(readiness.status(), ReadinessStatus::Degraded);

        let completed_fixture = WorkerFixture::new(content_lock(true)).await;
        completed_fixture.seed_task().await;
        recovery.record_poll(
            completed_fixture
                .worker()
                .run_once_with_recovery_evidence()
                .await
                .unwrap(),
            &readiness,
        );
        assert_eq!(readiness.status(), ReadinessStatus::Degraded);

        let pending_fixture = WorkerFixture::new(content_lock(true)).await;
        pending_fixture.seed_task().await;
        let pending_verifier = RetryOnceVerifier::default();
        recovery.record_poll(
            pending_fixture
                .worker_with_verifier(&pending_verifier)
                .run_once_with_recovery_evidence()
                .await
                .unwrap(),
            &readiness,
        );
        assert_eq!(readiness.status(), ReadinessStatus::Degraded);

        let healthy_fixture = WorkerFixture::new(paykit_content_lock()).await;
        healthy_fixture.seed_task().await;
        let healthy_client = FakePaykitStatusClient::healthy_pending();
        let healthy_verifier = PaykitPaymentVerifier::new(&healthy_client, 1);
        recovery.record_poll(
            healthy_fixture
                .worker_with_paykit_verifier(&healthy_verifier)
                .run_once_with_recovery_evidence()
                .await
                .unwrap(),
            &readiness,
        );
        assert_eq!(readiness.status(), ReadinessStatus::Ready);
    }

    #[tokio::test]
    async fn worker_without_dev_static_registration_fails_dev_static_tasks() {
        let fixture = WorkerFixture::new(content_lock(true)).await;
        fixture.seed_task().await;
        let worker = fixture.worker_without_dev_static_registration();

        let tick = worker.run_once().await.unwrap();

        assert_eq!(tick, WorkerTick::Failed(task_id()));
        let stored = fixture
            .tasks
            .get_verification_task(&task_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.status, VerificationTaskStatus::Failed);
        assert_eq!(
            stored.failure_message,
            Some("verification failed".to_owned())
        );
        assert!(
            fixture
                .entitlements
                .get_verified_proof_bundle(&creator(), &bundle_id())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn worker_from_state_registers_dev_static_only_in_dev_mode() {
        let dev_state =
            AppState::new_empty_in_memory(runtime_config(RuntimeEnvironment::Development));
        let production_state =
            AppState::new_empty_in_memory(runtime_config(RuntimeEnvironment::Production));

        assert!(VerificationWorker::from_state(&dev_state).allow_dev_static_verifier);
        assert!(!VerificationWorker::from_state(&production_state).allow_dev_static_verifier);
    }

    #[tokio::test]
    async fn worker_stops_on_shutdown() {
        let fixture = WorkerFixture::empty();
        let worker = fixture.worker();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        shutdown_tx.send(true).unwrap();

        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            worker.run_until_shutdown(shutdown_rx),
        )
        .await
        .expect("worker should stop promptly on shutdown")
        .unwrap();
    }

    #[tokio::test]
    async fn shutdown_during_claim_releases_claim_without_verification_execution() {
        let fixture = WorkerFixture::new(content_lock(true)).await;
        fixture.seed_task().await;
        let blocking = BlockingClaimer::new(&fixture.claimer);
        let worker = fixture.worker_with_claimer(&blocking);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let (result, ()) = tokio::join!(worker.run_once_until_shutdown(&shutdown_rx), async {
            blocking.claim_entered.notified().await;
            shutdown_tx.send(true).unwrap();
            blocking.release_claim.notify_one();
        });

        assert_eq!(result.unwrap(), WorkerTick::Cancelled);
        assert_eq!(blocking.publication_calls.load(Ordering::SeqCst), 0);
        assert_eq!(blocking.retry_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn pending_verification_retry_is_independent_of_queue_polling() {
        assert_eq!(retry_delay(), time::Duration::seconds(30));
    }

    #[tokio::test]
    async fn unexpected_verification_error_terminates_not_ready_with_redacted_class() {
        let fixture = WorkerFixture::new(content_lock(true)).await;
        fixture.seed_task().await;
        let verifier = UnexpectedVerifier("super-secret-verifier-detail".to_owned());
        let worker = fixture.worker_with_verifier(&verifier);
        let readiness = WorkerReadiness::new(true, false);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let error = worker
            .run_until_shutdown_with_readiness(shutdown_rx, &readiness)
            .await
            .unwrap_err();

        assert_eq!(readiness.status(), ReadinessStatus::NotReady);
        assert!(
            error
                .to_string()
                .contains("invalid_verification_task_state")
        );
        assert!(!error.to_string().contains("super-secret-verifier-detail"));
    }

    #[test]
    fn ordinary_verification_pending_retry_does_not_degrade_readiness() {
        let readiness = WorkerReadiness::new(true, false);
        let mut recovery = super::VerificationReadinessRecovery::default();

        recovery.record_repository_success(super::RepositoryOperation::QueuePoll, &readiness);
        recovery.record_poll(
            super::VerificationPoll::without_provider_evidence(
                WorkerTick::VerificationPendingRetryScheduled(task_id()),
            ),
            &readiness,
        );

        assert_eq!(readiness.status(), ReadinessStatus::Ready);
    }

    #[tokio::test]
    async fn successful_idle_poll_reports_only_queue_poll_recovery() {
        let fixture = WorkerFixture::empty();

        let poll = fixture
            .worker()
            .run_once_with_recovery_evidence()
            .await
            .unwrap();

        assert_eq!(poll.tick, WorkerTick::Idle);
        assert_eq!(
            poll.repository.queue_poll,
            super::OperationEvidence::Succeeded
        );
        assert_eq!(poll.repository.mutation, super::OperationEvidence::None);
    }

    #[test]
    fn repository_mutation_failure_ignores_outcomes_until_mutation_succeeds() {
        let readiness = WorkerReadiness::new(true, false);
        let mut recovery = super::VerificationReadinessRecovery::default();

        recovery.record_repository_failure(super::RepositoryOperation::Mutation, &readiness);
        for tick in [
            WorkerTick::Idle,
            WorkerTick::Completed(task_id()),
            WorkerTick::VerificationPendingRetryScheduled(task_id()),
            WorkerTick::Failed(task_id()),
        ] {
            recovery.record_poll(
                super::VerificationPoll::without_provider_evidence(tick),
                &readiness,
            );
            assert_eq!(readiness.status(), ReadinessStatus::Degraded);
        }

        recovery.record_repository_success(super::RepositoryOperation::Mutation, &readiness);
        assert_eq!(readiness.status(), ReadinessStatus::Ready);
    }

    #[test]
    fn queue_poll_failure_recovers_only_from_successful_queue_poll() {
        let readiness = WorkerReadiness::new(true, false);
        let mut recovery = super::VerificationReadinessRecovery::default();

        recovery.record_repository_failure(super::RepositoryOperation::QueuePoll, &readiness);
        recovery.record_repository_success(super::RepositoryOperation::Mutation, &readiness);
        assert_eq!(readiness.status(), ReadinessStatus::Degraded);

        recovery.record_repository_success(super::RepositoryOperation::QueuePoll, &readiness);
        assert_eq!(readiness.status(), ReadinessStatus::Ready);
    }

    #[test]
    fn provider_failure_stays_degraded_across_business_outcomes_until_provider_success() {
        let readiness = WorkerReadiness::new(true, false);
        let mut recovery = super::VerificationReadinessRecovery::default();

        recovery.record_poll(
            super::VerificationPoll {
                tick: WorkerTick::RetryScheduled(task_id()),
                paykit_provider: super::PaykitProviderEvidence::Unavailable,
                repository: super::RepositoryOperationEvidence::default(),
            },
            &readiness,
        );
        recovery.record_poll(
            super::VerificationPoll::without_provider_evidence(WorkerTick::Failed(task_id())),
            &readiness,
        );
        recovery.record_poll(
            super::VerificationPoll::without_provider_evidence(WorkerTick::Idle),
            &readiness,
        );
        assert_eq!(readiness.status(), ReadinessStatus::Degraded);

        recovery.record_poll(
            super::VerificationPoll {
                tick: WorkerTick::VerificationPendingRetryScheduled(task_id()),
                paykit_provider: super::PaykitProviderEvidence::HealthyResponse,
                repository: super::RepositoryOperationEvidence::default(),
            },
            &readiness,
        );
        assert_eq!(readiness.status(), ReadinessStatus::Ready);
    }

    #[test]
    fn verification_errors_retry_only_storage_and_redact_fatal_details() {
        let secret = "postgres://user:password@example.test/locks";
        assert_eq!(
            classify_verification_poll_error(&ApplicationError::Storage {
                message: secret.to_owned(),
            }),
            VerificationPollErrorDisposition::RetryableRepository
        );
        assert_eq!(
            classify_verification_poll_error(&ApplicationError::InvalidVerificationTaskState {
                message: secret.to_owned(),
            }),
            VerificationPollErrorDisposition::Fatal("invalid_verification_task_state")
        );
        assert_eq!(
            classify_verification_poll_error(&ApplicationError::MissingRecord {
                record: "verification_task",
            }),
            VerificationPollErrorDisposition::Fatal("unexpected_application_error")
        );

        let redacted =
            redacted_fatal_verification_error("invalid_verification_task_state").to_string();
        assert!(redacted.contains("invalid_verification_task_state"));
        assert!(!redacted.contains(secret));
    }

    #[tokio::test]
    async fn worker_tick_debug_does_not_expose_submitted_proof_payload() {
        let fixture = WorkerFixture::new(content_lock_with_payload(json_secret_payload())).await;
        fixture.seed_task_with_payload(json_secret_payload()).await;
        let worker = fixture.worker();

        let tick = worker.run_once().await.unwrap();
        let debug = format!("{tick:?}");

        assert!(!debug.contains("super-secret-proof-token"));
    }

    struct WorkerFixture {
        task: VerificationTaskRecord,
        tasks: Arc<InMemoryVerificationTaskRepository>,
        claimer: InMemoryVerificationTaskClaimer,
        content_locks: InMemoryContentLockRepository,
        entitlements: InMemoryEntitlementRepository,
        verifier: DevStaticVerifier,
        clock: SystemClock,
    }

    impl WorkerFixture {
        fn empty() -> Self {
            let tasks = Arc::new(InMemoryVerificationTaskRepository::new());
            Self {
                task: task_for(&content_lock(true), json_secret_payload()),
                claimer: InMemoryVerificationTaskClaimer::with_task_repository(
                    vec![],
                    tasks.clone(),
                ),
                tasks,
                content_locks: InMemoryContentLockRepository::new(),
                entitlements: InMemoryEntitlementRepository::new(),
                verifier: DevStaticVerifier,
                clock: SystemClock,
            }
        }

        async fn new(content_lock: ContentLock) -> Self {
            let task = task_for(&content_lock, json_secret_payload());
            let tasks = Arc::new(InMemoryVerificationTaskRepository::new());
            let fixture = Self {
                task: task.clone(),
                claimer: InMemoryVerificationTaskClaimer::with_task_repository(
                    vec![task],
                    tasks.clone(),
                ),
                tasks,
                content_locks: InMemoryContentLockRepository::new(),
                entitlements: InMemoryEntitlementRepository::new(),
                verifier: DevStaticVerifier,
                clock: SystemClock,
            };
            fixture.seed_content_lock(content_lock).await;
            fixture
        }

        fn worker(&self) -> VerificationWorker<'_> {
            self.worker_with_verifier(&self.verifier)
        }

        fn worker_with_claimer<'a>(
            &'a self,
            claimer: &'a dyn VerificationTaskClaimer,
        ) -> VerificationWorker<'a> {
            VerificationWorker::new(
                self.tasks.as_ref(),
                claimer,
                &self.content_locks,
                &self.entitlements,
                &self.verifier,
                None,
                true,
                &self.clock,
                LockServerPubky::from_str(
                    "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
                )
                .unwrap(),
                "test-worker".to_owned(),
                std::time::Duration::from_millis(10),
                60,
            )
        }

        fn worker_with_verifier<'a>(
            &'a self,
            verifier: &'a dyn CriterionVerifier,
        ) -> VerificationWorker<'a> {
            VerificationWorker::new(
                self.tasks.as_ref(),
                &self.claimer,
                &self.content_locks,
                &self.entitlements,
                verifier,
                None,
                true,
                &self.clock,
                LockServerPubky::from_str(
                    "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
                )
                .unwrap(),
                "test-worker".to_owned(),
                std::time::Duration::from_millis(10),
                60,
            )
        }

        fn worker_with_paykit_verifier<'a>(
            &'a self,
            verifier: &'a dyn CriterionVerifier,
        ) -> VerificationWorker<'a> {
            VerificationWorker::new(
                self.tasks.as_ref(),
                &self.claimer,
                &self.content_locks,
                &self.entitlements,
                &self.verifier,
                Some(verifier),
                false,
                &self.clock,
                LockServerPubky::from_str(
                    "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
                )
                .unwrap(),
                "test-worker".to_owned(),
                std::time::Duration::from_millis(10),
                60,
            )
        }

        fn worker_without_dev_static_registration(&self) -> VerificationWorker<'_> {
            VerificationWorker::new(
                self.tasks.as_ref(),
                &self.claimer,
                &self.content_locks,
                &self.entitlements,
                &self.verifier,
                None,
                false,
                &self.clock,
                LockServerPubky::from_str(
                    "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
                )
                .unwrap(),
                "test-worker".to_owned(),
                std::time::Duration::from_millis(10),
                60,
            )
        }

        async fn seed_task(&self) {
            self.tasks
                .insert_verification_task(self.task.clone())
                .await
                .unwrap();
        }

        async fn seed_task_with_payload(&self, payload: serde_json::Value) {
            let mut task = self.task.clone();
            task.submitted_proof_bundle.proofs[0].payload = payload;
            self.tasks.insert_verification_task(task).await.unwrap();
        }

        async fn seed_content_lock(&self, content_lock: ContentLock) {
            let path = content_lock.content_lock_path().unwrap();
            let creator = creator();
            self.content_locks
                .upsert_content_lock(creator, path, content_lock)
                .await
                .unwrap();
        }
    }

    struct BlockingClaimer<'a> {
        inner: &'a dyn VerificationTaskClaimer,
        claim_entered: Notify,
        release_claim: Notify,
        publication_calls: std::sync::atomic::AtomicUsize,
        retry_calls: std::sync::atomic::AtomicUsize,
    }

    impl<'a> BlockingClaimer<'a> {
        fn new(inner: &'a dyn VerificationTaskClaimer) -> Self {
            Self {
                inner,
                claim_entered: Notify::new(),
                release_claim: Notify::new(),
                publication_calls: std::sync::atomic::AtomicUsize::new(0),
                retry_calls: std::sync::atomic::AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl VerificationTaskClaimer for BlockingClaimer<'_> {
        async fn begin_claimed_entitlement_publication(
            &self,
            task_id: &TaskId,
            worker_id: &str,
            claim_token: &uuid::Uuid,
        ) -> Result<bool, ApplicationError> {
            self.publication_calls.fetch_add(1, Ordering::SeqCst);
            self.inner
                .begin_claimed_entitlement_publication(task_id, worker_id, claim_token)
                .await
        }

        async fn claim_next_verification_task(
            &self,
            worker_id: &str,
            claim_ttl: time::Duration,
        ) -> Result<Option<ClaimedVerificationTask>, ApplicationError> {
            self.claim_entered.notify_one();
            self.release_claim.notified().await;
            self.inner
                .claim_next_verification_task(worker_id, claim_ttl)
                .await
        }

        async fn schedule_verification_task_retry(
            &self,
            task_id: &TaskId,
            worker_id: &str,
            claim_token: &uuid::Uuid,
            retry_after: time::Duration,
        ) -> Result<Option<VerificationTaskRecord>, ApplicationError> {
            self.retry_calls.fetch_add(1, Ordering::SeqCst);
            self.inner
                .schedule_verification_task_retry(task_id, worker_id, claim_token, retry_after)
                .await
        }

        async fn persist_claimed_verification_task_transition(
            &self,
            task: VerificationTaskRecord,
            worker_id: &str,
            claim_token: &uuid::Uuid,
        ) -> Result<Option<VerificationTaskRecord>, ApplicationError> {
            self.inner
                .persist_claimed_verification_task_transition(task, worker_id, claim_token)
                .await
        }
    }

    struct UnexpectedVerifier(String);

    #[async_trait]
    impl CriterionVerifier for UnexpectedVerifier {
        async fn verify(
            &self,
            _request: CriterionVerificationRequest,
        ) -> Result<CriterionVerificationResult, ApplicationError> {
            Err(ApplicationError::InvalidVerificationTaskState {
                message: self.0.clone(),
            })
        }
    }

    #[derive(Default)]
    struct RetryOnceVerifier {
        returned_pending: AtomicBool,
    }

    #[async_trait]
    impl CriterionVerifier for RetryOnceVerifier {
        async fn verify(
            &self,
            request: CriterionVerificationRequest,
        ) -> Result<CriterionVerificationResult, ApplicationError> {
            if !self.returned_pending.swap(true, Ordering::SeqCst) {
                return Err(ApplicationError::VerificationPending);
            }
            DevStaticVerifier.verify(request).await
        }
    }

    struct FakePaykitStatusClient {
        response: Result<PaykitPaymentStatus, PaykitPaymentStatusError>,
        calls: std::sync::atomic::AtomicUsize,
    }

    impl FakePaykitStatusClient {
        fn failing() -> Self {
            Self {
                response: Err(PaykitPaymentStatusError),
                calls: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn healthy_pending() -> Self {
            Self {
                response: Ok(PaykitPaymentStatus {
                    status: PaykitPaymentStatusKind::Detected,
                    confirmations: 0,
                    amount_matched: true,
                }),
                calls: std::sync::atomic::AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl PaykitPaymentStatusClient for &FakePaykitStatusClient {
        async fn transaction_status(
            &self,
            _creator: &CreatorPubky,
            _bundle_id: &BundleId,
        ) -> Result<PaykitPaymentStatus, PaykitPaymentStatusError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.response
        }
    }

    fn task_for(content_lock: &ContentLock, payload: serde_json::Value) -> VerificationTaskRecord {
        VerificationTaskRecord {
            task_id: task_id(),
            creator: creator(),
            submitted_proof_bundle: submitted_proof_bundle_for(content_lock, payload),
            status: VerificationTaskStatus::Pending,
            submitted_at: submitted_at(),
            started_at: None,
            completed_at: None,
            failure_message: None,
        }
    }

    fn submitted_proof_bundle_for(
        content_lock: &ContentLock,
        payload: serde_json::Value,
    ) -> SubmittedProofBundle {
        SubmittedProofBundle {
            version: SUBMITTED_PROOF_BUNDLE_VERSION,
            bundle_id: bundle_id(),
            pubky_lock_resource: PubkyLockResource::new(
                creator(),
                content_lock.content_lock_path().unwrap(),
            ),
            reader_public_key: None,
            proofs: vec![Proof {
                criterion_id: "criterion-1".to_owned(),
                verifier_type: content_lock.criteria[0].verifier_type,
                payload,
            }],
        }
    }

    fn content_lock(satisfied: bool) -> ContentLock {
        content_lock_with_payload(serde_json::json!({ "satisfied": satisfied }))
    }

    fn paykit_content_lock() -> ContentLock {
        let mut content_lock = content_lock_with_payload(serde_json::json!({
            "recipient_pubky": creator().to_string(),
            "amount": "50000",
            "asset": "BTC",
            "payment_in": 24
        }));
        content_lock.criteria[0].verifier_type = VerifierType::PaykitPayment;
        content_lock
    }

    fn content_lock_with_payload(params: serde_json::Value) -> ContentLock {
        ContentLock {
            version: CONTENT_LOCK_VERSION,
            creator: creator(),
            primary_resource: Some(GuardedResource {
                path: "/priv/locks.app/content/hello.txt".to_owned(),
                hash: GuardedResourceHash::from_bytes([7; 32]),
                content_type: "text/plain".to_owned(),
                size: 13,
            }),
            secondary_resources: Default::default(),
            criteria: vec![Criterion {
                criterion_id: "criterion-1".to_owned(),
                verifier_type: VerifierType::DevStatic,
                params,
            }],
            lock_logic: LockLogic::All {
                criteria: vec!["criterion-1".to_owned()],
            },
            access_policy: AccessPolicy {
                requested_credential_ttl_seconds: 900,
            },
            lock_server: LockServerConfig {
                override_: Some(
                    LockServerPubky::from_str(
                        "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
                    )
                    .unwrap(),
                ),
            },
            created_at: submitted_at(),
        }
    }

    fn runtime_config(environment: RuntimeEnvironment) -> LockServerRuntimeConfig {
        LockServerRuntimeConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            credentials: LockServerCredentialsConfig {
                lock_server_secret_key: "/tmp/lock-server-test-secret.sess".into(),
                lock_server_public_key: LockServerPubky::from_str(
                    "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
                )
                .unwrap(),
                max_ttl_seconds: 3600,
            },
            worker: WorkerConfig {
                enabled: true,
                poll_interval_ms: 10,
                claim_timeout_seconds: 60,
                worker_id: "test-worker".to_owned(),
            },
            database: DatabaseConfig {
                url: "postgres://locks:locks@localhost/locks_test".to_owned(),
                max_connections: 1,
                run_migrations_on_startup: false,
            },
            runtime: RuntimeConfig { environment },
            creator_authority_acquisition:
                crate::config::CreatorAuthorityAcquisitionConfig::default(),
            secrets: SecretsConfig::default(),
            logging: LoggingConfig::default(),
            pubky: PubkyConfig::default(),
            pkdns: crate::config::PkdnsConfig::default(),
            rate_limits: RateLimitsConfig::default(),
            content_locks: ContentLocksConfig::default(),
            deletion: crate::config::DeletionConfig::default(),
            deletion_worker: crate::config::DeletionWorkerConfig::default(),
            paykit: None,
        }
    }

    fn json_secret_payload() -> serde_json::Value {
        serde_json::json!({ "token": "super-secret-proof-token" })
    }

    fn task_id() -> TaskId {
        TaskId::from_str(TASK_ID).unwrap()
    }

    fn bundle_id() -> BundleId {
        BundleId::from_str(BUNDLE_ID).unwrap()
    }

    fn creator() -> CreatorPubky {
        CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
    }

    fn submitted_at() -> time::OffsetDateTime {
        datetime!(2026-05-29 12:00:00 UTC)
    }
}
