use std::time::Duration as StdDuration;

use async_trait::async_trait;
use locks_service::application::{
    errors::ApplicationError,
    models::{ClaimedContentLockDeletionJob, ContentLockDeletionFailureCode},
    ports::{Clock, ContentLockDeletionRepository},
    use_cases::{
        drain_lock_payments::DrainLockPaymentsUseCase,
        execute_content_lock_deletion_phase::{
            ContentLockDeletionPhaseExecutor, ContentLockDeletionPhaseExecutorConfig,
            ContentLockDeletionPhaseExecutorDependencies, ContentLockPaymentDrainExecutor,
            DeletionDependencyEvidence, DeletionDependencySource, DeletionDependencyStatus,
            DeletionPhaseExecution, DeletionPhaseExecutionOutcome,
        },
        execute_forced_content_lock_deletion::{
            ExecuteForcedContentLockDeletionDependencies, ExecuteForcedContentLockDeletionUseCase,
            ForcedContentLockDeletionOutcome,
        },
        materialize_final_credentials::MaterializeFinalCredentialsUseCase,
        no_paykit_deletion_drain::NoPaykitDeletionDrainUseCase,
    },
};
use rand::Rng;
use time::{Duration, OffsetDateTime};
use tokio::sync::watch;
use tracing::error;

use crate::app_state::{AppState, WorkerKind, WorkerReadiness, WorkerReadinessEvidence};

/// Executes at most one bounded action for an already claimed deletion job.
#[async_trait]
pub trait ClaimedDeletionExecutor: Send + Sync {
    async fn execute_claimed(
        &self,
        claim: ClaimedContentLockDeletionJob,
        worker_id: &str,
    ) -> DeletionPhaseExecution;
}

#[derive(Clone)]
pub struct RuntimeClaimedDeletionExecutor {
    state: AppState,
}

impl RuntimeClaimedDeletionExecutor {
    pub fn new(state: AppState) -> Self {
        Self { state }
    }

    async fn execute_graceful(
        &self,
        claim: ClaimedContentLockDeletionJob,
        worker_id: &str,
    ) -> DeletionPhaseExecution {
        let materializer = MaterializeFinalCredentialsUseCase::new(
            self.state.access_credentials().as_ref(),
            self.state.credential_generator().as_ref(),
            self.state.clock().as_ref(),
        );
        let no_paykit = NoPaykitDeletionDrainUseCase::new(
            self.state.content_lock_deletions().as_ref(),
            self.state.clock().as_ref(),
        );
        let real_paykit = match (
            self.state.payment_drains(),
            self.state.payment_drain_client(),
            self.state.config().paykit.as_ref(),
        ) {
            (Some(drains), Some(client), Some(config)) => Some(DrainLockPaymentsUseCase::new(
                self.state.content_lock_deletions().as_ref(),
                drains.as_ref(),
                client.as_ref(),
                self.state.entitlements().as_ref(),
                self.state.clock().as_ref(),
                self.state
                    .config()
                    .credentials
                    .lock_server_public_key
                    .clone(),
                config.minimum_confirmations,
            )),
            _ => None,
        };
        let payments: &dyn ContentLockPaymentDrainExecutor =
            real_paykit.as_ref().map_or(&no_paykit, |drain| {
                drain as &dyn ContentLockPaymentDrainExecutor
            });
        let config = &self.state.config().deletion;
        ContentLockDeletionPhaseExecutor::new(
            ContentLockDeletionPhaseExecutorDependencies {
                deletions: self.state.content_lock_deletions().as_ref(),
                action_ownership: self.state.content_lock_deletion_action_ownership().as_ref(),
                tombstones: self.state.content_lock_tombstones().as_ref(),
                guarded_resources: self.state.guarded_resources().as_ref(),
                access_credentials: self.state.access_credentials().as_ref(),
                clock: self.state.clock().as_ref(),
                payments,
                final_credentials: &materializer,
            },
            ContentLockDeletionPhaseExecutorConfig {
                final_credential_issuance_window: Duration::seconds(
                    config.final_credential_issuance_window_seconds as i64,
                ),
                final_read_window: Duration::seconds(config.final_read_window_seconds as i64),
                final_credential_batch_limit: 64,
            },
        )
        .execute_with_evidence(claim, worker_id)
        .await
    }
}

#[async_trait]
impl ClaimedDeletionExecutor for RuntimeClaimedDeletionExecutor {
    async fn execute_claimed(
        &self,
        claim: ClaimedContentLockDeletionJob,
        worker_id: &str,
    ) -> DeletionPhaseExecution {
        if claim.job.force_requested_at.is_some() {
            let execution = ExecuteForcedContentLockDeletionUseCase::new(
                ExecuteForcedContentLockDeletionDependencies {
                    action_ownership: self.state.content_lock_deletion_action_ownership().as_ref(),
                    tombstones: self.state.content_lock_tombstones().as_ref(),
                    guarded_resources: self.state.guarded_resources().as_ref(),
                    deletions: self.state.content_lock_deletions().as_ref(),
                    clock: self.state.clock().as_ref(),
                },
            )
            .execute_with_evidence(claim, worker_id)
            .await;
            let phase_outcome = match execution.outcome {
                ForcedContentLockDeletionOutcome::Completed => {
                    DeletionPhaseExecutionOutcome::Progressed
                }
                ForcedContentLockDeletionOutcome::Deferred => {
                    DeletionPhaseExecutionOutcome::Deferred
                }
                ForcedContentLockDeletionOutcome::ClaimLost => {
                    DeletionPhaseExecutionOutcome::ClaimLost
                }
                ForcedContentLockDeletionOutcome::TransientDependencyFailure => {
                    DeletionPhaseExecutionOutcome::TransientDependencyFailure
                }
                ForcedContentLockDeletionOutcome::FatalFailure => {
                    DeletionPhaseExecutionOutcome::FatalFailure
                }
            };
            return DeletionPhaseExecution::new(phase_outcome).with_evidence(execution.evidence);
        }
        self.execute_graceful(claim, worker_id).await
    }
}

/// Supplies a full-jitter delay in the inclusive range from zero through `cap`.
pub trait FullJitterSource: Send + Sync {
    fn sample(&self, cap: StdDuration) -> StdDuration;
}

/// Production full-jitter source.
#[derive(Debug, Default, Clone, Copy)]
pub struct RandomFullJitter;

impl FullJitterSource for RandomFullJitter {
    fn sample(&self, cap: StdDuration) -> StdDuration {
        let cap_nanos = cap.as_nanos();
        if cap_nanos == 0 {
            return StdDuration::ZERO;
        }
        let sampled = rand::thread_rng().gen_range(0..=cap_nanos);
        StdDuration::new(
            (sampled / 1_000_000_000).try_into().unwrap_or(u64::MAX),
            (sampled % 1_000_000_000) as u32,
        )
    }
}

/// Runtime policy for the deletion worker core.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletionWorkerConfig {
    pub worker_id: String,
    pub poll_interval: StdDuration,
    pub claim_timeout: StdDuration,
    pub retry_max_attempts: u32,
    pub retry_initial_backoff: StdDuration,
    pub retry_max_backoff: StdDuration,
}

/// Secret-free result of one worker iteration. It intentionally carries no job IDs or paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeletionWorkerOutcome {
    Idle,
    Cancelled,
    Progressed,
    Deferred,
    ClaimLost,
    TerminalFailed,
    RetryScheduled,
    RetryExhausted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DeletionWorkerExecution {
    outcome: DeletionWorkerOutcome,
    evidence: DeletionDependencyEvidence,
}

impl DeletionWorkerExecution {
    fn new(outcome: DeletionWorkerOutcome, evidence: DeletionDependencyEvidence) -> Self {
        Self { outcome, evidence }
    }
}

struct DeletionWorkerFailure {
    error: ApplicationError,
    evidence: DeletionDependencyEvidence,
}

impl DeletionWorkerFailure {
    fn repository(
        error: ApplicationError,
        source: DeletionDependencySource,
        prior_evidence: DeletionDependencyEvidence,
    ) -> Self {
        Self {
            error,
            evidence: prior_evidence.merge(DeletionDependencyEvidence::unavailable(source)),
        }
    }

    fn fatal(error: ApplicationError) -> Self {
        Self {
            error,
            evidence: DeletionDependencyEvidence::none(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PollErrorDisposition {
    RetryableRepository,
    Fatal(&'static str),
}

fn classify_poll_error(error: &ApplicationError) -> PollErrorDisposition {
    match error {
        ApplicationError::Storage { .. } => PollErrorDisposition::RetryableRepository,
        ApplicationError::InvalidContentLockDeletionState { .. } => {
            PollErrorDisposition::Fatal("invalid_deletion_state")
        }
        _ => PollErrorDisposition::Fatal("unexpected_application_error"),
    }
}

fn redacted_fatal_poll_error(class: &'static str) -> ApplicationError {
    ApplicationError::InvalidContentLockDeletionState {
        message: format!("deletion worker terminated after {class}"),
    }
}

#[derive(Debug, Default)]
struct DeletionReadinessRecovery {
    degraded: [bool; DeletionDependencySource::ALL.len()],
}

impl DeletionReadinessRecovery {
    fn record_outcome(
        &mut self,
        outcome: DeletionWorkerOutcome,
        evidence: DeletionDependencyEvidence,
        readiness: &WorkerReadiness,
    ) {
        if outcome == DeletionWorkerOutcome::Cancelled {
            return;
        }
        for source in DeletionDependencySource::ALL {
            match evidence.status(source) {
                Some(DeletionDependencyStatus::Healthy) => {
                    self.degraded[source_index(source)] = false
                }
                Some(DeletionDependencyStatus::Unavailable) => {
                    self.degraded[source_index(source)] = true
                }
                None => {}
            }
        }
        readiness.record(
            WorkerKind::Deletion,
            if self.degraded.iter().any(|degraded| *degraded) {
                WorkerReadinessEvidence::TransientDependencyFailure
            } else {
                WorkerReadinessEvidence::DependencySucceeded
            },
        );
    }

    #[cfg(test)]
    fn record_repository_failure(
        &mut self,
        source: DeletionDependencySource,
        readiness: &WorkerReadiness,
    ) {
        self.record_outcome(
            DeletionWorkerOutcome::RetryScheduled,
            DeletionDependencyEvidence::unavailable(source),
            readiness,
        );
    }
}

const fn source_index(source: DeletionDependencySource) -> usize {
    match source {
        DeletionDependencySource::PaymentProvider => 0,
        DeletionDependencySource::PaymentDrainRepository => 1,
        DeletionDependencySource::EntitlementRepository => 2,
        DeletionDependencySource::PubkyWithdrawal => 3,
        DeletionDependencySource::PubkyReadback => 4,
        DeletionDependencySource::PubkyResource => 5,
        DeletionDependencySource::PubkyForcePublic => 6,
        DeletionDependencySource::RepositoryQueueClaim => 7,
        DeletionDependencySource::RepositoryPhaseMutation => 8,
        DeletionDependencySource::RepositoryDefer => 9,
        DeletionDependencySource::RepositoryRetry => 10,
        DeletionDependencySource::RepositoryTerminalMutation => 11,
        DeletionDependencySource::RepositoryActionLock => 12,
        DeletionDependencySource::RepositoryActionLockRelease => 13,
        DeletionDependencySource::RepositoryForceReceipt => 14,
    }
}

/// Polling, retry, and cancellation core for content-lock deletion jobs.
pub struct DeletionWorker<'a> {
    deletions: &'a dyn ContentLockDeletionRepository,
    clock: &'a dyn Clock,
    executor: &'a dyn ClaimedDeletionExecutor,
    jitter: &'a dyn FullJitterSource,
    config: DeletionWorkerConfig,
}

impl<'a> DeletionWorker<'a> {
    pub fn new(
        deletions: &'a dyn ContentLockDeletionRepository,
        clock: &'a dyn Clock,
        executor: &'a dyn ClaimedDeletionExecutor,
        jitter: &'a dyn FullJitterSource,
        config: DeletionWorkerConfig,
    ) -> Self {
        Self {
            deletions,
            clock,
            executor,
            jitter,
            config,
        }
    }

    /// Runs at most one claim. A sticky shutdown signal is checked before claiming and
    /// again after claim acquisition, before any external deletion action can begin.
    pub async fn run_once(
        &self,
        shutdown: &watch::Receiver<bool>,
    ) -> Result<DeletionWorkerOutcome, ApplicationError> {
        self.run_once_with_evidence(shutdown)
            .await
            .map(|execution| execution.outcome)
            .map_err(|failure| failure.error)
    }

    async fn run_once_with_evidence(
        &self,
        shutdown: &watch::Receiver<bool>,
    ) -> Result<DeletionWorkerExecution, DeletionWorkerFailure> {
        if *shutdown.borrow() {
            return Ok(DeletionWorkerExecution::new(
                DeletionWorkerOutcome::Cancelled,
                DeletionDependencyEvidence::none(),
            ));
        }

        let claim_started_at = self.clock.now();
        let claim_expires_at = claim_started_at + to_time_duration(self.config.claim_timeout);
        let repository_healthy =
            DeletionDependencyEvidence::healthy(DeletionDependencySource::RepositoryQueueClaim);
        let Some(claim) = self
            .deletions
            .claim_next(
                &self.config.worker_id,
                (claim_expires_at) - (claim_started_at),
            )
            .await
            .map_err(|error| {
                DeletionWorkerFailure::repository(
                    error,
                    DeletionDependencySource::RepositoryQueueClaim,
                    DeletionDependencyEvidence::none(),
                )
            })?
        else {
            return Ok(DeletionWorkerExecution::new(
                DeletionWorkerOutcome::Idle,
                repository_healthy,
            ));
        };

        if *shutdown.borrow() {
            let outcome = self
                .release_cancelled_claim(&claim)
                .await
                .map_err(|error| {
                    DeletionWorkerFailure::repository(
                        error,
                        DeletionDependencySource::RepositoryDefer,
                        repository_healthy,
                    )
                })?;
            let mutation_evidence =
                mutation_success_evidence(outcome, DeletionDependencySource::RepositoryDefer);
            return Ok(DeletionWorkerExecution::new(
                outcome,
                repository_healthy.merge(mutation_evidence),
            ));
        }

        let claim_for_write = claim.clone();
        let execution = self
            .executor
            .execute_claimed(claim, &self.config.worker_id)
            .await;
        let mut mutation_evidence = DeletionDependencyEvidence::none();
        let outcome = match execution.outcome {
            DeletionPhaseExecutionOutcome::Progressed => DeletionWorkerOutcome::Progressed,
            DeletionPhaseExecutionOutcome::TerminalFailed => DeletionWorkerOutcome::TerminalFailed,
            DeletionPhaseExecutionOutcome::ClaimLost => DeletionWorkerOutcome::ClaimLost,
            DeletionPhaseExecutionOutcome::Deferred => {
                let outcome = self.defer_claim(&claim_for_write).await.map_err(|error| {
                    DeletionWorkerFailure::repository(
                        error,
                        DeletionDependencySource::RepositoryDefer,
                        repository_healthy.merge(execution.evidence),
                    )
                })?;
                mutation_evidence =
                    mutation_success_evidence(outcome, DeletionDependencySource::RepositoryDefer);
                outcome
            }
            DeletionPhaseExecutionOutcome::TransientDependencyFailure => {
                let source = if claim_for_write.job.attempt_count >= self.config.retry_max_attempts
                {
                    DeletionDependencySource::RepositoryTerminalMutation
                } else {
                    DeletionDependencySource::RepositoryRetry
                };
                let outcome = self
                    .retry_or_exhaust(claim_for_write)
                    .await
                    .map_err(|error| {
                        DeletionWorkerFailure::repository(
                            error,
                            source,
                            repository_healthy.merge(execution.evidence),
                        )
                    })?;
                mutation_evidence = mutation_success_evidence(outcome, source);
                outcome
            }
            DeletionPhaseExecutionOutcome::FatalFailure => {
                return Err(DeletionWorkerFailure::fatal(redacted_fatal_poll_error(
                    "fatal_execution_failure",
                )));
            }
        };
        Ok(DeletionWorkerExecution::new(
            outcome,
            execution
                .evidence
                .merge(repository_healthy)
                .merge(mutation_evidence),
        ))
    }

    /// Runs until shutdown, sleeping only after idle polls. Shutdown prevents every future claim,
    /// including when it arrives while claim acquisition is blocked.
    pub async fn run_until_shutdown(
        &self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), ApplicationError> {
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }

            match self.run_once(&shutdown).await? {
                DeletionWorkerOutcome::Cancelled => return Ok(()),
                DeletionWorkerOutcome::Idle => {
                    tokio::select! {
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                return Ok(());
                            }
                        }
                        _ = tokio::time::sleep(self.config.poll_interval) => {}
                    }
                }
                DeletionWorkerOutcome::Progressed
                | DeletionWorkerOutcome::Deferred
                | DeletionWorkerOutcome::ClaimLost
                | DeletionWorkerOutcome::TerminalFailed
                | DeletionWorkerOutcome::RetryScheduled
                | DeletionWorkerOutcome::RetryExhausted => {}
            }
        }
    }

    pub async fn run_until_shutdown_with_readiness(
        &self,
        mut shutdown: watch::Receiver<bool>,
        readiness: &WorkerReadiness,
    ) -> Result<(), ApplicationError> {
        let mut recovery = DeletionReadinessRecovery::default();
        loop {
            if *shutdown.borrow() {
                readiness.record(WorkerKind::Deletion, WorkerReadinessEvidence::Stopped);
                return Ok(());
            }

            match self.run_once_with_evidence(&shutdown).await {
                Ok(DeletionWorkerExecution {
                    outcome: DeletionWorkerOutcome::Cancelled,
                    ..
                }) => {
                    readiness.record(WorkerKind::Deletion, WorkerReadinessEvidence::Stopped);
                    return Ok(());
                }
                Ok(
                    execution @ DeletionWorkerExecution {
                        outcome: DeletionWorkerOutcome::Idle,
                        ..
                    },
                ) => {
                    recovery.record_outcome(execution.outcome, execution.evidence, readiness);
                    tokio::select! {
                        _ = shutdown.changed() => {}
                        _ = tokio::time::sleep(self.config.poll_interval) => {}
                    }
                }
                Ok(execution) => {
                    recovery.record_outcome(execution.outcome, execution.evidence, readiness)
                }
                Err(failure) => match classify_poll_error(&failure.error) {
                    PollErrorDisposition::RetryableRepository => {
                        recovery.record_outcome(
                            DeletionWorkerOutcome::RetryScheduled,
                            failure.evidence,
                            readiness,
                        );
                        error!(
                            operation = "deletion_queue_poll_or_transition",
                            error_class = "repository_unavailable",
                            retrying = true,
                            "deletion worker repository operation failed"
                        );
                        tokio::select! {
                            _ = shutdown.changed() => {}
                            _ = tokio::time::sleep(self.config.poll_interval) => {}
                        }
                    }
                    PollErrorDisposition::Fatal(error_class) => {
                        readiness.record(
                            WorkerKind::Deletion,
                            WorkerReadinessEvidence::UnexpectedExit,
                        );
                        error!(
                            operation = "deletion_queue_poll_or_transition",
                            error_class,
                            retrying = false,
                            "deletion worker terminated after unexpected application error"
                        );
                        return Err(redacted_fatal_poll_error(error_class));
                    }
                },
            }
        }
    }

    async fn release_cancelled_claim(
        &self,
        claim: &ClaimedContentLockDeletionJob,
    ) -> Result<DeletionWorkerOutcome, ApplicationError> {
        let now = self.clock.now();
        self.defer_at(claim, now, now, DeletionWorkerOutcome::Cancelled)
            .await
    }

    async fn defer_claim(
        &self,
        claim: &ClaimedContentLockDeletionJob,
    ) -> Result<DeletionWorkerOutcome, ApplicationError> {
        let now = self.clock.now();
        let due = now + to_time_duration(self.config.poll_interval);
        self.defer_at(claim, now, due, DeletionWorkerOutcome::Deferred)
            .await
    }

    async fn defer_at(
        &self,
        claim: &ClaimedContentLockDeletionJob,
        now: OffsetDateTime,
        due: OffsetDateTime,
        success: DeletionWorkerOutcome,
    ) -> Result<DeletionWorkerOutcome, ApplicationError> {
        let updated = self
            .deletions
            .defer(
                claim.job.job_id,
                &self.config.worker_id,
                claim.claim_token,
                (due) - (now),
            )
            .await?;
        Ok(if updated.is_some() {
            success
        } else {
            DeletionWorkerOutcome::ClaimLost
        })
    }

    async fn retry_or_exhaust(
        &self,
        claim: ClaimedContentLockDeletionJob,
    ) -> Result<DeletionWorkerOutcome, ApplicationError> {
        let now = self.clock.now();
        if claim.job.attempt_count >= self.config.retry_max_attempts {
            let updated = self
                .deletions
                .finish(
                    claim.job.job_id,
                    &self.config.worker_id,
                    claim.claim_token,
                    Some(ContentLockDeletionFailureCode::RetryExhausted),
                )
                .await?;
            return Ok(if updated.is_some() {
                DeletionWorkerOutcome::RetryExhausted
            } else {
                DeletionWorkerOutcome::ClaimLost
            });
        }

        let cap = retry_cap(
            self.config.retry_initial_backoff,
            self.config.retry_max_backoff,
            claim.job.attempt_count,
        );
        // Keep repository scheduling bounded even if a custom source violates its contract.
        let delay = self.jitter.sample(cap).min(cap);
        let next_attempt_at = now + to_time_duration(delay);
        let updated = self
            .deletions
            .schedule_retry(
                claim.job.job_id,
                &self.config.worker_id,
                claim.claim_token,
                (next_attempt_at) - (now),
            )
            .await?;
        Ok(if updated.is_some() {
            DeletionWorkerOutcome::RetryScheduled
        } else {
            DeletionWorkerOutcome::ClaimLost
        })
    }
}

fn mutation_success_evidence(
    outcome: DeletionWorkerOutcome,
    source: DeletionDependencySource,
) -> DeletionDependencyEvidence {
    if outcome == DeletionWorkerOutcome::ClaimLost {
        DeletionDependencyEvidence::none()
    } else {
        DeletionDependencyEvidence::healthy(source)
    }
}

fn retry_cap(initial: StdDuration, maximum: StdDuration, attempt_count: u32) -> StdDuration {
    let mut cap = initial.min(maximum);
    for _ in 1..attempt_count {
        cap = cap.checked_mul(2).unwrap_or(maximum).min(maximum);
        if cap == maximum {
            break;
        }
    }
    cap
}

fn to_time_duration(duration: StdDuration) -> Duration {
    Duration::new(
        i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
        duration.subsec_nanos() as i32,
    )
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        str::FromStr,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration as StdDuration,
    };

    use async_trait::async_trait;
    use locks_core::{
        ids::{CreatorPubky, GuardedResourceHash, LockId},
        lock_policy::{
            AccessPolicy, CONTENT_LOCK_VERSION, ContentLock, GuardedResource, LockLogic,
            LockServerConfig,
        },
    };
    use locks_service::application::{
        errors::ApplicationError,
        models::{
            AdvanceContentLockDeletionPhaseResult, ClaimedContentLockDeletionJob,
            ContentLockDeletionFailureCode, ContentLockDeletionJob, ContentLockDeletionPhase,
            ContentLockDeletionState, PrepareForceDeletionResult,
        },
        ports::{Clock, ContentLockDeletionRepository},
        use_cases::execute_content_lock_deletion_phase::{
            DeletionDependencyEvidence, DeletionDependencySource, DeletionDependencyStatus,
            DeletionPhaseExecution, DeletionPhaseExecutionOutcome,
        },
    };
    use time::{OffsetDateTime, macros::datetime};
    use tokio::sync::{Notify, watch};
    use uuid::Uuid;

    use super::{
        ClaimedDeletionExecutor, DeletionReadinessRecovery, DeletionWorker, DeletionWorkerConfig,
        DeletionWorkerOutcome, FullJitterSource, PollErrorDisposition, classify_poll_error,
        retry_cap,
    };
    use crate::app_state::{ReadinessStatus, WorkerKind, WorkerReadiness, WorkerReadinessState};

    const NOW: OffsetDateTime = datetime!(2026-08-17 12:00:00 UTC);
    const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";

    #[tokio::test]
    async fn active_shutdown_performs_no_claim() {
        let repository = FakeRepository::default();
        let executor = FakeExecutor::new(DeletionPhaseExecutionOutcome::Progressed);
        let worker = worker(&repository, &executor, &FixedJitter(StdDuration::ZERO));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        shutdown_tx.send(true).unwrap();

        assert_eq!(
            worker.run_once(&shutdown_rx).await.unwrap(),
            DeletionWorkerOutcome::Cancelled
        );
        assert_eq!(repository.claim_calls.load(Ordering::SeqCst), 0);
        assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn shutdown_while_claim_is_blocked_releases_claim_without_external_work() {
        let repository = Arc::new(FakeRepository::with_claim(claim(1)));
        repository.block_claim.store(true, Ordering::SeqCst);
        let executor = Arc::new(FakeExecutor::new(DeletionPhaseExecutionOutcome::Progressed));
        let jitter = Arc::new(FixedJitter(StdDuration::ZERO));
        let worker = Arc::new(OwnedWorker::new(
            Arc::clone(&repository),
            Arc::clone(&executor),
            jitter,
        ));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let run = {
            let worker = Arc::clone(&worker);
            tokio::spawn(async move { worker.run_once(&shutdown_rx).await })
        };
        repository.claim_entered.notified().await;
        shutdown_tx.send(true).unwrap();
        repository.claim_release.notify_one();

        assert_eq!(
            run.await.unwrap().unwrap(),
            DeletionWorkerOutcome::Cancelled
        );
        assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
        let defers = repository.defers.lock().unwrap();
        assert_eq!(defers.len(), 1);
        assert_eq!(defers[0].0, NOW);
        assert_eq!(defers[0].1, NOW);
    }

    #[tokio::test]
    async fn shutdown_during_first_claim_prevents_a_second_claim() {
        let repository = Arc::new(FakeRepository::with_claim(claim(1)));
        repository.block_claim.store(true, Ordering::SeqCst);
        let executor = Arc::new(FakeExecutor::new(DeletionPhaseExecutionOutcome::Progressed));
        let worker = Arc::new(OwnedWorker::new(
            Arc::clone(&repository),
            executor,
            Arc::new(FixedJitter(StdDuration::ZERO)),
        ));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let run = {
            let worker = Arc::clone(&worker);
            tokio::spawn(async move { worker.run_until_shutdown(shutdown_rx).await })
        };
        repository.claim_entered.notified().await;
        shutdown_tx.send(true).unwrap();
        repository.claim_release.notify_one();

        run.await.unwrap().unwrap();
        assert_eq!(repository.claim_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn deferred_work_releases_claim_on_poll_schedule_without_retry_write() {
        let repository = FakeRepository::with_claim(claim(3));
        let executor = FakeExecutor::new(DeletionPhaseExecutionOutcome::Deferred);
        let worker = worker(&repository, &executor, &FixedJitter(StdDuration::ZERO));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        assert_eq!(
            worker.run_once(&shutdown_rx).await.unwrap(),
            DeletionWorkerOutcome::Deferred
        );
        assert!(repository.retries.lock().unwrap().is_empty());
        assert!(repository.finishes.lock().unwrap().is_empty());
        assert_eq!(
            repository.defers.lock().unwrap().as_slice(),
            &[(NOW, NOW + time::Duration::seconds(5))]
        );
    }

    #[tokio::test]
    async fn completed_terminal_and_lost_executor_outcomes_do_not_write_retry_state() {
        for (execution, expected) in [
            (
                DeletionPhaseExecutionOutcome::Progressed,
                DeletionWorkerOutcome::Progressed,
            ),
            (
                DeletionPhaseExecutionOutcome::TerminalFailed,
                DeletionWorkerOutcome::TerminalFailed,
            ),
            (
                DeletionPhaseExecutionOutcome::ClaimLost,
                DeletionWorkerOutcome::ClaimLost,
            ),
        ] {
            let repository = FakeRepository::with_claim(claim(1));
            let executor = FakeExecutor::new(execution);
            let worker = worker(&repository, &executor, &FixedJitter(StdDuration::ZERO));
            let (_shutdown_tx, shutdown_rx) = watch::channel(false);

            assert_eq!(worker.run_once(&shutdown_rx).await.unwrap(), expected);
            assert!(repository.defers.lock().unwrap().is_empty());
            assert!(repository.retries.lock().unwrap().is_empty());
            assert!(repository.finishes.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn idle_sleep_wakes_immediately_for_shutdown() {
        let repository = Arc::new(FakeRepository::default());
        let executor = Arc::new(FakeExecutor::new(DeletionPhaseExecutionOutcome::Progressed));
        let worker = Arc::new(OwnedWorker::new(
            Arc::clone(&repository),
            executor,
            Arc::new(FixedJitter(StdDuration::ZERO)),
        ));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let run = {
            let worker = Arc::clone(&worker);
            tokio::spawn(async move { worker.run_until_shutdown(shutdown_rx).await })
        };
        while repository.claim_calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }

        shutdown_tx.send(true).unwrap();
        tokio::time::timeout(StdDuration::from_millis(100), run)
            .await
            .expect("shutdown must interrupt the poll sleep")
            .unwrap()
            .unwrap();
        assert_eq!(repository.claim_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn fatal_executor_failure_terminates_supervised_worker_without_retry_write() {
        let repository = FakeRepository::with_claim(claim(1));
        let executor = FakeExecutor::new(DeletionPhaseExecutionOutcome::FatalFailure);
        let worker = worker(&repository, &executor, &FixedJitter(StdDuration::ZERO));
        let readiness = WorkerReadiness::new(false, true);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let error = worker
            .run_until_shutdown_with_readiness(shutdown_rx, &readiness)
            .await
            .unwrap_err();

        assert_eq!(readiness.status(), ReadinessStatus::NotReady);
        assert!(error.to_string().contains("invalid_deletion_state"));
        assert!(repository.retries.lock().unwrap().is_empty());
        assert!(repository.finishes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn transient_executor_failure_schedules_retry_and_marks_dependency_degraded() {
        let repository = FakeRepository::with_claim(claim(1));
        let executor = FakeExecutor::new(DeletionPhaseExecutionOutcome::TransientDependencyFailure);
        let worker = worker(&repository, &executor, &FixedJitter(StdDuration::ZERO));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        assert_eq!(
            worker.run_once(&shutdown_rx).await.unwrap(),
            DeletionWorkerOutcome::RetryScheduled
        );
        assert_eq!(repository.retries.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn dependency_failure_exhausts_on_exact_max_attempt() {
        let repository = FakeRepository::with_claim(claim(4));
        let executor = FakeExecutor::new(DeletionPhaseExecutionOutcome::TransientDependencyFailure);
        let worker = worker(&repository, &executor, &FixedJitter(StdDuration::ZERO));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        assert_eq!(
            worker.run_once(&shutdown_rx).await.unwrap(),
            DeletionWorkerOutcome::RetryExhausted
        );
        assert!(repository.retries.lock().unwrap().is_empty());
        assert_eq!(
            repository.finishes.lock().unwrap().as_slice(),
            &[Some(ContentLockDeletionFailureCode::RetryExhausted)]
        );
    }

    #[tokio::test]
    async fn full_jitter_is_bounded_by_exponential_cap() {
        assert_eq!(
            retry_cap(StdDuration::from_secs(10), StdDuration::from_secs(25), 1),
            StdDuration::from_secs(10)
        );
        assert_eq!(
            retry_cap(StdDuration::from_secs(10), StdDuration::from_secs(25), 2),
            StdDuration::from_secs(20)
        );
        assert_eq!(
            retry_cap(StdDuration::from_secs(10), StdDuration::from_secs(25), 3),
            StdDuration::from_secs(25)
        );

        let repository = FakeRepository::with_claim(claim(2));
        let executor = FakeExecutor::new(DeletionPhaseExecutionOutcome::TransientDependencyFailure);
        let jitter = FixedJitter(StdDuration::from_secs(99));
        let worker = worker(&repository, &executor, &jitter);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        worker.run_once(&shutdown_rx).await.unwrap();

        assert_eq!(
            repository.retries.lock().unwrap().as_slice(),
            &[(NOW, NOW + time::Duration::seconds(20))]
        );
    }

    #[tokio::test]
    async fn retry_due_time_uses_jitter_not_poll_interval() {
        let repository = FakeRepository::with_claim(claim(1));
        let executor = FakeExecutor::new(DeletionPhaseExecutionOutcome::TransientDependencyFailure);
        let jitter = FixedJitter(StdDuration::from_secs(7));
        let worker = worker(&repository, &executor, &jitter);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        assert_eq!(
            worker.run_once(&shutdown_rx).await.unwrap(),
            DeletionWorkerOutcome::RetryScheduled
        );
        assert_eq!(
            repository.retries.lock().unwrap().as_slice(),
            &[(NOW, NOW + time::Duration::seconds(7))]
        );
    }

    #[tokio::test]
    async fn stale_retry_defer_and_finish_writes_map_to_claim_lost() {
        for (execution, attempts) in [
            (DeletionPhaseExecutionOutcome::Deferred, 1),
            (DeletionPhaseExecutionOutcome::TransientDependencyFailure, 1),
            (DeletionPhaseExecutionOutcome::TransientDependencyFailure, 4),
        ] {
            let repository = FakeRepository::with_claim(claim(attempts));
            repository.stale_writes.store(true, Ordering::SeqCst);
            let executor = FakeExecutor::new(execution);
            let worker = worker(&repository, &executor, &FixedJitter(StdDuration::ZERO));
            let (_shutdown_tx, shutdown_rx) = watch::channel(false);
            assert_eq!(
                worker.run_once(&shutdown_rx).await.unwrap(),
                DeletionWorkerOutcome::ClaimLost
            );
        }
    }

    #[tokio::test]
    async fn successful_worker_mutations_emit_only_their_exact_healthy_slot() {
        for (execution, attempts, expected_source) in [
            (
                DeletionPhaseExecutionOutcome::Deferred,
                1,
                DeletionDependencySource::RepositoryDefer,
            ),
            (
                DeletionPhaseExecutionOutcome::TransientDependencyFailure,
                1,
                DeletionDependencySource::RepositoryRetry,
            ),
            (
                DeletionPhaseExecutionOutcome::TransientDependencyFailure,
                4,
                DeletionDependencySource::RepositoryTerminalMutation,
            ),
        ] {
            let repository = FakeRepository::with_claim(claim(attempts));
            let executor = FakeExecutor::new(execution);
            let worker = worker(&repository, &executor, &FixedJitter(StdDuration::ZERO));
            let (_shutdown_tx, shutdown_rx) = watch::channel(false);

            let execution = match worker.run_once_with_evidence(&shutdown_rx).await {
                Ok(execution) => execution,
                Err(_) => panic!("worker mutation should succeed"),
            };

            assert_eq!(
                execution.evidence.status(expected_source),
                Some(DeletionDependencyStatus::Healthy)
            );
            for skipped in [
                DeletionDependencySource::RepositoryPhaseMutation,
                DeletionDependencySource::RepositoryDefer,
                DeletionDependencySource::RepositoryRetry,
                DeletionDependencySource::RepositoryTerminalMutation,
            ] {
                if skipped != expected_source {
                    assert_eq!(execution.evidence.status(skipped), None);
                }
            }
        }
    }

    #[tokio::test]
    async fn stale_worker_mutations_are_evidence_free_for_the_skipped_slot() {
        for (execution, attempts, skipped_source) in [
            (
                DeletionPhaseExecutionOutcome::Deferred,
                1,
                DeletionDependencySource::RepositoryDefer,
            ),
            (
                DeletionPhaseExecutionOutcome::TransientDependencyFailure,
                1,
                DeletionDependencySource::RepositoryRetry,
            ),
            (
                DeletionPhaseExecutionOutcome::TransientDependencyFailure,
                4,
                DeletionDependencySource::RepositoryTerminalMutation,
            ),
        ] {
            let repository = FakeRepository::with_claim(claim(attempts));
            repository.stale_writes.store(true, Ordering::SeqCst);
            let executor = FakeExecutor::new(execution);
            let worker = worker(&repository, &executor, &FixedJitter(StdDuration::ZERO));
            let (_shutdown_tx, shutdown_rx) = watch::channel(false);

            let execution = match worker.run_once_with_evidence(&shutdown_rx).await {
                Ok(execution) => execution,
                Err(_) => panic!("worker mutation should succeed"),
            };

            assert_eq!(execution.outcome, DeletionWorkerOutcome::ClaimLost);
            assert_eq!(execution.evidence.status(skipped_source), None);
        }
    }

    #[tokio::test]
    async fn worker_mutation_failures_report_their_exact_repository_source() {
        for (execution, attempts, write_failure, expected_source) in [
            (
                DeletionPhaseExecutionOutcome::Deferred,
                1,
                RepositoryWriteFailure::Defer,
                DeletionDependencySource::RepositoryDefer,
            ),
            (
                DeletionPhaseExecutionOutcome::TransientDependencyFailure,
                1,
                RepositoryWriteFailure::Retry,
                DeletionDependencySource::RepositoryRetry,
            ),
            (
                DeletionPhaseExecutionOutcome::TransientDependencyFailure,
                4,
                RepositoryWriteFailure::Terminal,
                DeletionDependencySource::RepositoryTerminalMutation,
            ),
        ] {
            let repository = FakeRepository::with_claim(claim(attempts));
            *repository.write_failure.lock().unwrap() = Some(write_failure);
            let executor = FakeExecutor::new(execution);
            let worker = worker(&repository, &executor, &FixedJitter(StdDuration::ZERO));
            let (_shutdown_tx, shutdown_rx) = watch::channel(false);

            let failure = match worker.run_once_with_evidence(&shutdown_rx).await {
                Ok(_) => panic!("repository mutation should fail"),
                Err(failure) => failure,
            };

            assert_eq!(
                failure.evidence.status(expected_source),
                Some(DeletionDependencyStatus::Unavailable)
            );
            for unrelated in [
                DeletionDependencySource::RepositoryDefer,
                DeletionDependencySource::RepositoryRetry,
                DeletionDependencySource::RepositoryTerminalMutation,
            ] {
                if unrelated != expected_source {
                    assert_eq!(failure.evidence.status(unrelated), None);
                }
            }
        }
    }

    #[test]
    fn healthy_business_deferral_establishes_readiness_without_degradation() {
        let readiness = WorkerReadiness::new(false, true);
        let mut recovery = DeletionReadinessRecovery::default();

        recovery.record_outcome(
            DeletionWorkerOutcome::Deferred,
            DeletionDependencyEvidence::healthy(DeletionDependencySource::RepositoryQueueClaim),
            &readiness,
        );

        assert_eq!(readiness.status(), ReadinessStatus::Ready);
        assert_eq!(
            readiness.worker_state(WorkerKind::Deletion),
            WorkerReadinessState::Ready
        );
    }

    #[test]
    fn paykit_degradation_survives_unrelated_pubky_or_repository_progress() {
        let readiness = WorkerReadiness::new(false, true);
        let mut recovery = DeletionReadinessRecovery::default();

        recovery.record_outcome(
            DeletionWorkerOutcome::RetryScheduled,
            DeletionDependencyEvidence::unavailable(DeletionDependencySource::PaymentProvider),
            &readiness,
        );
        assert_eq!(readiness.status(), ReadinessStatus::Degraded);

        recovery.record_outcome(
            DeletionWorkerOutcome::Progressed,
            DeletionDependencyEvidence::healthy(DeletionDependencySource::PubkyReadback).merge(
                DeletionDependencyEvidence::healthy(DeletionDependencySource::RepositoryQueueClaim),
            ),
            &readiness,
        );

        assert_eq!(
            readiness.worker_state(WorkerKind::Deletion),
            WorkerReadinessState::Degraded
        );
    }

    #[test]
    fn successful_active_paykit_drain_recovers_readiness_while_deferred() {
        let readiness = WorkerReadiness::new(false, true);
        let mut recovery = DeletionReadinessRecovery::default();

        recovery.record_outcome(
            DeletionWorkerOutcome::RetryScheduled,
            DeletionDependencyEvidence::unavailable(DeletionDependencySource::PaymentProvider),
            &readiness,
        );
        recovery.record_outcome(
            DeletionWorkerOutcome::Deferred,
            DeletionDependencyEvidence::healthy(DeletionDependencySource::PaymentProvider).merge(
                DeletionDependencyEvidence::healthy(DeletionDependencySource::RepositoryQueueClaim),
            ),
            &readiness,
        );

        assert_eq!(readiness.status(), ReadinessStatus::Ready);
    }

    #[test]
    fn dependency_sources_recover_independently() {
        let readiness = WorkerReadiness::new(false, true);
        let mut recovery = DeletionReadinessRecovery::default();

        recovery.record_outcome(
            DeletionWorkerOutcome::RetryScheduled,
            DeletionDependencyEvidence::unavailable(DeletionDependencySource::PaymentProvider)
                .merge(DeletionDependencyEvidence::unavailable(
                    DeletionDependencySource::PubkyReadback,
                )),
            &readiness,
        );
        recovery.record_outcome(
            DeletionWorkerOutcome::Progressed,
            DeletionDependencyEvidence::healthy(DeletionDependencySource::PubkyReadback),
            &readiness,
        );
        assert_eq!(readiness.status(), ReadinessStatus::Degraded);

        recovery.record_outcome(
            DeletionWorkerOutcome::Deferred,
            DeletionDependencyEvidence::healthy(DeletionDependencySource::PaymentProvider),
            &readiness,
        );
        assert_eq!(readiness.status(), ReadinessStatus::Ready);
    }

    #[test]
    fn repository_degradation_is_sticky_across_non_recovery_outcomes() {
        for non_recovery in [
            DeletionWorkerOutcome::Deferred,
            DeletionWorkerOutcome::ClaimLost,
            DeletionWorkerOutcome::TerminalFailed,
        ] {
            let readiness = WorkerReadiness::new(false, true);
            let mut recovery = DeletionReadinessRecovery::default();

            recovery.record_repository_failure(
                DeletionDependencySource::RepositoryQueueClaim,
                &readiness,
            );
            recovery.record_outcome(non_recovery, DeletionDependencyEvidence::none(), &readiness);
            assert_eq!(readiness.status(), ReadinessStatus::Degraded);

            recovery.record_outcome(
                DeletionWorkerOutcome::Idle,
                DeletionDependencyEvidence::healthy(DeletionDependencySource::RepositoryQueueClaim),
                &readiness,
            );
            assert_eq!(readiness.status(), ReadinessStatus::Ready);
        }
    }

    #[test]
    fn idle_queue_poll_does_not_clear_repository_mutation_degradation() {
        let readiness = WorkerReadiness::new(false, true);
        let mut recovery = DeletionReadinessRecovery::default();

        recovery.record_outcome(
            DeletionWorkerOutcome::RetryScheduled,
            DeletionDependencyEvidence::unavailable(
                DeletionDependencySource::RepositoryPhaseMutation,
            ),
            &readiness,
        );
        recovery.record_outcome(
            DeletionWorkerOutcome::Idle,
            DeletionDependencyEvidence::healthy(DeletionDependencySource::RepositoryQueueClaim),
            &readiness,
        );

        assert_eq!(readiness.status(), ReadinessStatus::Degraded);
    }

    #[test]
    fn forced_contention_does_not_clear_prior_pubky_degradation() {
        let readiness = WorkerReadiness::new(false, true);
        let mut recovery = DeletionReadinessRecovery::default();

        recovery.record_outcome(
            DeletionWorkerOutcome::RetryScheduled,
            DeletionDependencyEvidence::unavailable(DeletionDependencySource::PubkyReadback),
            &readiness,
        );
        recovery.record_outcome(
            DeletionWorkerOutcome::Deferred,
            DeletionDependencyEvidence::none(),
            &readiness,
        );

        assert_eq!(readiness.status(), ReadinessStatus::Degraded);
    }

    #[test]
    fn retry_exhaustion_is_dependency_failure_evidence_even_without_prior_retry_tick() {
        let readiness = WorkerReadiness::new(false, true);
        let mut recovery = DeletionReadinessRecovery::default();

        recovery.record_outcome(
            DeletionWorkerOutcome::RetryExhausted,
            DeletionDependencyEvidence::unavailable(DeletionDependencySource::PubkyReadback),
            &readiness,
        );

        assert_eq!(readiness.status(), ReadinessStatus::Degraded);
    }

    #[test]
    fn polling_error_classification_retries_only_storage_failures_without_details() {
        let secret = "postgres://user:password@example.test/locks";
        assert_eq!(
            classify_poll_error(&ApplicationError::Storage {
                message: secret.to_owned(),
            }),
            PollErrorDisposition::RetryableRepository
        );
        assert_eq!(
            classify_poll_error(&ApplicationError::InvalidContentLockDeletionState {
                message: secret.to_owned(),
            }),
            PollErrorDisposition::Fatal("invalid_deletion_state")
        );
        assert_eq!(
            classify_poll_error(&ApplicationError::MissingRecord {
                record: "content_lock_deletion",
            }),
            PollErrorDisposition::Fatal("unexpected_application_error")
        );

        for class in ["invalid_deletion_state", "unexpected_application_error"] {
            let redacted = super::redacted_fatal_poll_error(class).to_string();
            assert!(redacted.contains(class));
            assert!(!redacted.contains(secret));
        }
    }

    #[tokio::test]
    async fn unexpected_poll_error_terminates_worker_not_ready_with_redacted_error() {
        let secret = "pubky-secret-path";
        let repository =
            FakeRepository::with_claim_error(ApplicationError::InvalidContentLockDeletionState {
                message: secret.to_owned(),
            });
        let executor = FakeExecutor::new(DeletionPhaseExecutionOutcome::Progressed);
        let worker = worker(&repository, &executor, &FixedJitter(StdDuration::ZERO));
        let readiness = WorkerReadiness::new(false, true);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let error = worker
            .run_until_shutdown_with_readiness(shutdown_rx, &readiness)
            .await
            .unwrap_err();

        assert_eq!(readiness.status(), ReadinessStatus::NotReady);
        assert!(error.to_string().contains("invalid_deletion_state"));
        assert!(!error.to_string().contains(secret));
    }

    #[test]
    fn public_outcome_debug_contains_no_job_identity() {
        for outcome in [
            DeletionWorkerOutcome::Idle,
            DeletionWorkerOutcome::Cancelled,
            DeletionWorkerOutcome::Progressed,
            DeletionWorkerOutcome::Deferred,
            DeletionWorkerOutcome::ClaimLost,
            DeletionWorkerOutcome::TerminalFailed,
            DeletionWorkerOutcome::RetryScheduled,
            DeletionWorkerOutcome::RetryExhausted,
        ] {
            let debug = format!("{outcome:?}");
            assert!(!debug.contains("pubky"));
            assert!(!debug.contains('/'));
            assert!(!debug.contains('-'));
        }
    }

    fn worker<'a>(
        repository: &'a FakeRepository,
        executor: &'a FakeExecutor,
        jitter: &'a dyn FullJitterSource,
    ) -> DeletionWorker<'a> {
        DeletionWorker::new(repository, &FIXED_CLOCK, executor, jitter, config())
    }

    fn config() -> DeletionWorkerConfig {
        DeletionWorkerConfig {
            worker_id: "deletion-worker-test".to_owned(),
            poll_interval: StdDuration::from_secs(5),
            claim_timeout: StdDuration::from_secs(30),
            retry_max_attempts: 4,
            retry_initial_backoff: StdDuration::from_secs(10),
            retry_max_backoff: StdDuration::from_secs(25),
        }
    }

    struct OwnedWorker {
        repository: Arc<FakeRepository>,
        executor: Arc<FakeExecutor>,
        jitter: Arc<FixedJitter>,
    }

    impl OwnedWorker {
        fn new(
            repository: Arc<FakeRepository>,
            executor: Arc<FakeExecutor>,
            jitter: Arc<FixedJitter>,
        ) -> Self {
            Self {
                repository,
                executor,
                jitter,
            }
        }

        async fn run_once(
            &self,
            shutdown: &watch::Receiver<bool>,
        ) -> Result<DeletionWorkerOutcome, ApplicationError> {
            DeletionWorker::new(
                self.repository.as_ref(),
                &FIXED_CLOCK,
                self.executor.as_ref(),
                self.jitter.as_ref(),
                config(),
            )
            .run_once(shutdown)
            .await
        }

        async fn run_until_shutdown(
            &self,
            shutdown: watch::Receiver<bool>,
        ) -> Result<(), ApplicationError> {
            DeletionWorker::new(
                self.repository.as_ref(),
                &FIXED_CLOCK,
                self.executor.as_ref(),
                self.jitter.as_ref(),
                config(),
            )
            .run_until_shutdown(shutdown)
            .await
        }
    }

    struct FixedClock;
    static FIXED_CLOCK: FixedClock = FixedClock;
    impl Clock for FixedClock {
        fn now(&self) -> OffsetDateTime {
            NOW
        }
    }

    struct FixedJitter(StdDuration);
    impl FullJitterSource for FixedJitter {
        fn sample(&self, _cap: StdDuration) -> StdDuration {
            self.0
        }
    }

    struct FakeExecutor {
        outcome: DeletionPhaseExecutionOutcome,
        calls: AtomicUsize,
    }

    impl FakeExecutor {
        fn new(outcome: DeletionPhaseExecutionOutcome) -> Self {
            Self {
                outcome,
                calls: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl ClaimedDeletionExecutor for FakeExecutor {
        async fn execute_claimed(
            &self,
            _claim: ClaimedContentLockDeletionJob,
            _worker_id: &str,
        ) -> DeletionPhaseExecution {
            self.calls.fetch_add(1, Ordering::SeqCst);
            DeletionPhaseExecution::new(self.outcome)
        }
    }

    #[derive(Default)]
    struct FakeRepository {
        claim: Mutex<Option<ClaimedContentLockDeletionJob>>,
        claim_error: Mutex<Option<ApplicationError>>,
        claim_calls: AtomicUsize,
        block_claim: AtomicBool,
        claim_entered: Notify,
        claim_release: Notify,
        stale_writes: AtomicBool,
        defers: Mutex<Vec<(OffsetDateTime, OffsetDateTime)>>,
        retries: Mutex<Vec<(OffsetDateTime, OffsetDateTime)>>,
        finishes: Mutex<Vec<Option<ContentLockDeletionFailureCode>>>,
        write_failure: Mutex<Option<RepositoryWriteFailure>>,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum RepositoryWriteFailure {
        Defer,
        Retry,
        Terminal,
    }

    impl FakeRepository {
        fn with_claim(claim: ClaimedContentLockDeletionJob) -> Self {
            Self {
                claim: Mutex::new(Some(claim)),
                ..Self::default()
            }
        }

        fn with_claim_error(error: ApplicationError) -> Self {
            Self {
                claim_error: Mutex::new(Some(error)),
                ..Self::default()
            }
        }

        fn write_result(&self, job: ContentLockDeletionJob) -> Option<ContentLockDeletionJob> {
            (!self.stale_writes.load(Ordering::SeqCst)).then_some(job)
        }
    }

    #[async_trait]
    impl ContentLockDeletionRepository for FakeRepository {
        async fn begin_publication(
            &self,
            _: &CreatorPubky,
            _: &LockId,
            _: Uuid,
        ) -> Result<(), ApplicationError> {
            unreachable!()
        }
        async fn finish_publication(
            &self,
            _: &CreatorPubky,
            _: &LockId,
            _: Uuid,
        ) -> Result<bool, ApplicationError> {
            unreachable!()
        }
        async fn abandon_publication(
            &self,
            _: &CreatorPubky,
            _: &LockId,
            _: Uuid,
        ) -> Result<bool, ApplicationError> {
            unreachable!()
        }
        async fn publication_in_progress(
            &self,
            _: &CreatorPubky,
            _: &LockId,
        ) -> Result<bool, ApplicationError> {
            unreachable!()
        }
        async fn insert_job(&self, _: ContentLockDeletionJob) -> Result<(), ApplicationError> {
            unreachable!()
        }
        async fn get_job(
            &self,
            _: &CreatorPubky,
            _: &LockId,
        ) -> Result<Option<ContentLockDeletionJob>, ApplicationError> {
            unreachable!()
        }

        async fn claim_next(
            &self,
            _: &str,
            _: time::Duration,
        ) -> Result<Option<ClaimedContentLockDeletionJob>, ApplicationError> {
            self.claim_calls.fetch_add(1, Ordering::SeqCst);
            if let Some(error) = self.claim_error.lock().unwrap().take() {
                return Err(error);
            }
            if self.block_claim.load(Ordering::SeqCst) {
                self.claim_entered.notify_one();
                self.claim_release.notified().await;
            }
            Ok(self.claim.lock().unwrap().take())
        }

        async fn schedule_retry(
            &self,
            _: Uuid,
            _: &str,
            _: Uuid,
            retry_after: time::Duration,
        ) -> Result<Option<ContentLockDeletionJob>, ApplicationError> {
            if self.write_failure.lock().unwrap().take() == Some(RepositoryWriteFailure::Retry) {
                return Err(ApplicationError::Storage {
                    message: "retry unavailable".to_owned(),
                });
            }
            self.retries.lock().unwrap().push((NOW, NOW + retry_after));
            Ok(self.write_result(job(1)))
        }

        async fn defer(
            &self,
            _: Uuid,
            _: &str,
            _: Uuid,
            defer_for: time::Duration,
        ) -> Result<Option<ContentLockDeletionJob>, ApplicationError> {
            if self.write_failure.lock().unwrap().take() == Some(RepositoryWriteFailure::Defer) {
                return Err(ApplicationError::Storage {
                    message: "defer unavailable".to_owned(),
                });
            }
            self.defers.lock().unwrap().push((NOW, NOW + defer_for));
            Ok(self.write_result(job(1)))
        }

        async fn advance_phase(
            &self,
            _: Uuid,
            _: &str,
            _: Uuid,
            _: ContentLockDeletionPhase,
        ) -> Result<AdvanceContentLockDeletionPhaseResult, ApplicationError> {
            Ok(match self.write_result(job(1)) {
                Some(job) => AdvanceContentLockDeletionPhaseResult::Advanced(Box::new(job)),
                None => AdvanceContentLockDeletionPhaseResult::ClaimLost,
            })
        }

        async fn finish(
            &self,
            _: Uuid,
            _: &str,
            _: Uuid,
            code: Option<ContentLockDeletionFailureCode>,
        ) -> Result<Option<ContentLockDeletionJob>, ApplicationError> {
            if self.write_failure.lock().unwrap().take() == Some(RepositoryWriteFailure::Terminal) {
                return Err(ApplicationError::Storage {
                    message: "terminal mutation unavailable".to_owned(),
                });
            }
            self.finishes.lock().unwrap().push(code);
            Ok(self.write_result(job(1)))
        }

        async fn resume_failed_job(
            &self,
            _: &CreatorPubky,
            _: &LockId,
            _: OffsetDateTime,
        ) -> Result<Option<ContentLockDeletionJob>, ApplicationError> {
            unreachable!()
        }
        async fn prepare_force_deletion(
            &self,
            _: &CreatorPubky,
            _: &LockId,
        ) -> Result<PrepareForceDeletionResult, ApplicationError> {
            unreachable!()
        }
        async fn complete_force_deletion(
            &self,
            _: Uuid,
            _: &str,
            _: Uuid,
        ) -> Result<bool, ApplicationError> {
            unreachable!()
        }
        async fn has_force_receipt(
            &self,
            _: &CreatorPubky,
            _: &LockId,
        ) -> Result<bool, ApplicationError> {
            unreachable!()
        }
    }

    fn claim(attempt_count: u32) -> ClaimedContentLockDeletionJob {
        ClaimedContentLockDeletionJob {
            job: job(attempt_count),
            claim_token: Uuid::from_u128(2),
        }
    }

    fn job(attempt_count: u32) -> ContentLockDeletionJob {
        let frozen_content_lock = content_lock();
        ContentLockDeletionJob {
            job_id: Uuid::from_u128(1),
            creator: frozen_content_lock.creator.clone(),
            lock_id: frozen_content_lock.lock_id().unwrap(),
            frozen_content_lock,
            deletion_started_at: NOW,
            state: ContentLockDeletionState::Running,
            phase: ContentLockDeletionPhase::Withdraw,
            attempt_count,
            next_attempt_at: None,
            force_requested_at: None,
            failure_code: None,
        }
    }

    fn content_lock() -> ContentLock {
        ContentLock {
            version: CONTENT_LOCK_VERSION,
            creator: CreatorPubky::from_str(CREATOR).unwrap(),
            primary_resource: Some(
                GuardedResource::new(
                    "/priv/locks.app/content/post.json".to_owned(),
                    GuardedResourceHash::from_bytes([7; 32]),
                    "application/json".to_owned(),
                    42,
                )
                .unwrap(),
            ),
            secondary_resources: BTreeMap::new(),
            criteria: vec![],
            lock_logic: LockLogic::All { criteria: vec![] },
            access_policy: AccessPolicy {
                requested_credential_ttl_seconds: 900,
            },
            lock_server: LockServerConfig { override_: None },
            created_at: NOW,
        }
    }
}
