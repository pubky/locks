use std::collections::BTreeMap;

use async_trait::async_trait;
use locks_core::{
    content_lock_deletion::ContentLockDeletionTombstone,
    ids::{ContentLockPath, GuardedResourceHash},
};
use time::Duration;

use crate::application::{
    errors::ApplicationError,
    models::{
        AdvanceContentLockDeletionPhaseResult, ClaimedContentLockDeletionJob,
        ContentLockDeletionFailureCode, ContentLockDeletionPhase,
        InitializeFinalAccessWindowsResult,
    },
    ports::{
        AccessCredentialStore, Clock, ContentLockDeletionActionAcquireResult,
        ContentLockDeletionActionClaim, ContentLockDeletionActionOwnership,
        ContentLockDeletionRepository, ContentLockTombstoneRepository, GuardedResourceReadback,
        GuardedResourceRepository, TombstoneReadback,
    },
};

use super::{
    drain_lock_payments::DrainLockPaymentsUseCase,
    materialize_final_credentials::{
        MaterializeFinalCredentialsOutcome, MaterializeFinalCredentialsRequest,
        MaterializeFinalCredentialsUseCase,
    },
};

/// Closed result of one bounded deletion-phase execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeletionPhaseExecutionOutcome {
    Progressed,
    Deferred,
    ClaimLost,
    TerminalFailed,
    TransientDependencyFailure,
    FatalFailure,
}

/// Internal, identifier-free dependency classes observed by deletion execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeletionDependencySource {
    PaymentProvider,
    PaymentDrainRepository,
    EntitlementRepository,
    PubkyWithdrawal,
    PubkyReadback,
    PubkyResource,
    PubkyForcePublic,
    RepositoryQueueClaim,
    RepositoryPhaseMutation,
    RepositoryDefer,
    RepositoryRetry,
    RepositoryTerminalMutation,
    RepositoryActionLock,
    RepositoryActionLockRelease,
    RepositoryForceReceipt,
}

/// Health observed from one dependency during a bounded execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeletionDependencyStatus {
    Healthy,
    Unavailable,
}

/// Closed dependency evidence carried from the real phase invocation to worker readiness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DeletionDependencyEvidence {
    statuses: [Option<DeletionDependencyStatus>; DeletionDependencySource::ALL.len()],
}

impl DeletionDependencyEvidence {
    pub const fn none() -> Self {
        Self {
            statuses: [None; DeletionDependencySource::ALL.len()],
        }
    }

    pub fn healthy(source: DeletionDependencySource) -> Self {
        Self::observed(source, DeletionDependencyStatus::Healthy)
    }

    pub fn unavailable(source: DeletionDependencySource) -> Self {
        Self::observed(source, DeletionDependencyStatus::Unavailable)
    }

    pub fn status(self, source: DeletionDependencySource) -> Option<DeletionDependencyStatus> {
        self.statuses[source.index()]
    }

    pub fn merge(mut self, other: Self) -> Self {
        for source in DeletionDependencySource::ALL {
            let index = source.index();
            self.statuses[index] = match (self.statuses[index], other.statuses[index]) {
                (Some(DeletionDependencyStatus::Unavailable), _)
                | (_, Some(DeletionDependencyStatus::Unavailable)) => {
                    Some(DeletionDependencyStatus::Unavailable)
                }
                (Some(status), None) | (None, Some(status)) => Some(status),
                (
                    Some(DeletionDependencyStatus::Healthy),
                    Some(DeletionDependencyStatus::Healthy),
                ) => Some(DeletionDependencyStatus::Healthy),
                (None, None) => None,
            };
        }
        self
    }

    fn observed(source: DeletionDependencySource, status: DeletionDependencyStatus) -> Self {
        let mut evidence = Self::none();
        evidence.statuses[source.index()] = Some(status);
        evidence
    }
}

impl DeletionDependencySource {
    pub const ALL: [Self; 15] = [
        Self::PaymentProvider,
        Self::PaymentDrainRepository,
        Self::EntitlementRepository,
        Self::PubkyWithdrawal,
        Self::PubkyReadback,
        Self::PubkyResource,
        Self::PubkyForcePublic,
        Self::RepositoryQueueClaim,
        Self::RepositoryPhaseMutation,
        Self::RepositoryDefer,
        Self::RepositoryRetry,
        Self::RepositoryTerminalMutation,
        Self::RepositoryActionLock,
        Self::RepositoryActionLockRelease,
        Self::RepositoryForceReceipt,
    ];

    pub const fn index(self) -> usize {
        match self {
            Self::PaymentProvider => 0,
            Self::PaymentDrainRepository => 1,
            Self::EntitlementRepository => 2,
            Self::PubkyWithdrawal => 3,
            Self::PubkyReadback => 4,
            Self::PubkyResource => 5,
            Self::PubkyForcePublic => 6,
            Self::RepositoryQueueClaim => 7,
            Self::RepositoryPhaseMutation => 8,
            Self::RepositoryDefer => 9,
            Self::RepositoryRetry => 10,
            Self::RepositoryTerminalMutation => 11,
            Self::RepositoryActionLock => 12,
            Self::RepositoryActionLockRelease => 13,
            Self::RepositoryForceReceipt => 14,
        }
    }
}

/// One closed execution outcome plus source-aware, secret-free dependency evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeletionPhaseExecution {
    pub outcome: DeletionPhaseExecutionOutcome,
    pub evidence: DeletionDependencyEvidence,
}

impl DeletionPhaseExecution {
    pub fn new(outcome: DeletionPhaseExecutionOutcome) -> Self {
        Self {
            outcome,
            evidence: DeletionDependencyEvidence::none(),
        }
    }

    pub fn with_evidence(mut self, evidence: DeletionDependencyEvidence) -> Self {
        self.evidence = self.evidence.merge(evidence);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DeletionExecutionErrorClass {
    TransientDependency,
    Fatal,
}

pub(super) fn classify_deletion_execution_error(
    error: &ApplicationError,
) -> DeletionExecutionErrorClass {
    match error {
        ApplicationError::Storage { .. } | ApplicationError::Verifier { .. } => {
            DeletionExecutionErrorClass::TransientDependency
        }
        _ => DeletionExecutionErrorClass::Fatal,
    }
}

fn error_outcome(error: &ApplicationError) -> DeletionPhaseExecutionOutcome {
    match classify_deletion_execution_error(error) {
        DeletionExecutionErrorClass::TransientDependency => {
            DeletionPhaseExecutionOutcome::TransientDependencyFailure
        }
        DeletionExecutionErrorClass::Fatal => DeletionPhaseExecutionOutcome::FatalFailure,
    }
}

fn error_execution(
    error: &ApplicationError,
    source: DeletionDependencySource,
) -> DeletionPhaseExecution {
    let outcome = error_outcome(error);
    DeletionPhaseExecution::new(outcome)
        .with_evidence(DeletionDependencyEvidence::unavailable(source))
}

/// Bounded final-access settings used by the phase executor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentLockDeletionPhaseExecutorConfig {
    pub final_credential_issuance_window: Duration,
    pub final_read_window: Duration,
    pub final_credential_batch_limit: usize,
}

/// Object-safe payment-drain collaborator for phase-executor tests and runtime composition.
#[async_trait]
pub trait ContentLockPaymentDrainExecutor: Send + Sync {
    /// Returns only evidence recorded by the concrete remote/repository operations it invoked.
    async fn execute_claimed(
        &self,
        claim: ClaimedContentLockDeletionJob,
        worker_id: &str,
    ) -> DeletionPhaseExecution;
}

#[async_trait]
impl ContentLockPaymentDrainExecutor for DrainLockPaymentsUseCase<'_> {
    async fn execute_claimed(
        &self,
        claim: ClaimedContentLockDeletionJob,
        worker_id: &str,
    ) -> DeletionPhaseExecution {
        DrainLockPaymentsUseCase::execute_claimed_with_evidence(self, claim, worker_id).await
    }
}

/// Object-safe bounded final-credential materialization collaborator.
#[async_trait]
pub trait FinalCredentialMaterializer: Send + Sync {
    async fn materialize(
        &self,
        request: MaterializeFinalCredentialsRequest<'_>,
    ) -> Result<MaterializeFinalCredentialsOutcome, ApplicationError>;
}

#[async_trait]
impl FinalCredentialMaterializer for MaterializeFinalCredentialsUseCase<'_> {
    async fn materialize(
        &self,
        request: MaterializeFinalCredentialsRequest<'_>,
    ) -> Result<MaterializeFinalCredentialsOutcome, ApplicationError> {
        self.execute(request).await
    }
}

/// Dependencies for one graceful-deletion phase executor.
pub struct ContentLockDeletionPhaseExecutorDependencies<'a> {
    pub deletions: &'a dyn ContentLockDeletionRepository,
    pub action_ownership: &'a dyn ContentLockDeletionActionOwnership,
    pub tombstones: &'a dyn ContentLockTombstoneRepository,
    pub guarded_resources: &'a dyn GuardedResourceRepository,
    pub access_credentials: &'a dyn AccessCredentialStore,
    pub clock: &'a dyn Clock,
    pub payments: &'a dyn ContentLockPaymentDrainExecutor,
    pub final_credentials: &'a dyn FinalCredentialMaterializer,
}

/// Executes at most one bounded graceful-deletion phase while holding its action guard.
pub struct ContentLockDeletionPhaseExecutor<'a> {
    dependencies: ContentLockDeletionPhaseExecutorDependencies<'a>,
    config: ContentLockDeletionPhaseExecutorConfig,
}

impl<'a> ContentLockDeletionPhaseExecutor<'a> {
    pub fn new(
        dependencies: ContentLockDeletionPhaseExecutorDependencies<'a>,
        config: ContentLockDeletionPhaseExecutorConfig,
    ) -> Self {
        Self {
            dependencies,
            config,
        }
    }

    pub async fn execute(
        &self,
        claim: ClaimedContentLockDeletionJob,
        worker_id: &str,
    ) -> DeletionPhaseExecutionOutcome {
        self.execute_with_evidence(claim, worker_id).await.outcome
    }

    pub async fn execute_with_evidence(
        &self,
        claim: ClaimedContentLockDeletionJob,
        worker_id: &str,
    ) -> DeletionPhaseExecution {
        let guard = match self
            .dependencies
            .action_ownership
            .try_acquire(ContentLockDeletionActionClaim {
                job_id: claim.job.job_id,
                worker_id,
                claim_token: claim.claim_token,
                expected_phase: claim.job.phase,
                force: false,
            })
            .await
        {
            Ok(ContentLockDeletionActionAcquireResult::Acquired(guard)) => guard,
            Ok(ContentLockDeletionActionAcquireResult::Busy) => {
                return DeletionPhaseExecution::new(DeletionPhaseExecutionOutcome::Deferred);
            }
            Ok(ContentLockDeletionActionAcquireResult::ClaimLost) => {
                return DeletionPhaseExecution::new(DeletionPhaseExecutionOutcome::ClaimLost);
            }
            Err(error) => {
                return error_execution(&error, DeletionDependencySource::RepositoryActionLock);
            }
        };

        let execution = self
            .execute_guarded_with_evidence(claim, worker_id)
            .await
            .with_evidence(DeletionDependencyEvidence::healthy(
                DeletionDependencySource::RepositoryActionLock,
            ));
        if let Err(error) = guard.release().await {
            return error_execution(
                &error,
                DeletionDependencySource::RepositoryActionLockRelease,
            )
            .with_evidence(execution.evidence);
        }
        execution.with_evidence(DeletionDependencyEvidence::healthy(
            DeletionDependencySource::RepositoryActionLockRelease,
        ))
    }

    async fn execute_guarded_with_evidence(
        &self,
        claim: ClaimedContentLockDeletionJob,
        worker_id: &str,
    ) -> DeletionPhaseExecution {
        let phase = claim.job.phase;
        if matches!(
            phase,
            ContentLockDeletionPhase::StartPaymentDrain | ContentLockDeletionPhase::DrainPayments
        ) {
            return self
                .dependencies
                .payments
                .execute_claimed(claim, worker_id)
                .await;
        }
        if phase == ContentLockDeletionPhase::Withdraw {
            return self.withdraw_with_evidence(&claim, worker_id).await;
        }
        if phase == ContentLockDeletionPhase::DeleteContent {
            return self
                .verify_frozen_content_with_evidence(&claim, worker_id)
                .await;
        }
        if phase == ContentLockDeletionPhase::DeleteTombstone {
            return self
                .verify_tombstone_for_purge_with_evidence(&claim, worker_id)
                .await;
        }

        match phase {
            ContentLockDeletionPhase::PurgeOperationalState => {
                DeletionPhaseExecution::new(DeletionPhaseExecutionOutcome::Deferred)
            }
            _ => self.repository_execution(self.execute_guarded(claim, worker_id).await),
        }
    }

    async fn execute_guarded(
        &self,
        claim: ClaimedContentLockDeletionJob,
        worker_id: &str,
    ) -> DeletionPhaseExecutionOutcome {
        match claim.job.phase {
            ContentLockDeletionPhase::Withdraw => self.withdraw(&claim, worker_id).await,
            ContentLockDeletionPhase::StartPaymentDrain
            | ContentLockDeletionPhase::DrainPayments => {
                self.dependencies
                    .payments
                    .execute_claimed(claim, worker_id)
                    .await
                    .outcome
            }
            ContentLockDeletionPhase::DrainExistingCredentials => {
                self.advance_or_defer(
                    &claim,
                    worker_id,
                    ContentLockDeletionPhase::IssueFinalCredentials,
                )
                .await
            }
            ContentLockDeletionPhase::IssueFinalCredentials => {
                self.issue_final_credentials(&claim, worker_id).await
            }
            ContentLockDeletionPhase::DrainFinalReads => {
                self.advance_or_defer(&claim, worker_id, ContentLockDeletionPhase::DeleteContent)
                    .await
            }
            ContentLockDeletionPhase::DeleteContent => {
                self.verify_frozen_content(&claim, worker_id).await
            }
            ContentLockDeletionPhase::DeleteTombstone => {
                self.verify_tombstone_for_purge(&claim, worker_id).await
            }
            ContentLockDeletionPhase::PurgeOperationalState => {
                DeletionPhaseExecutionOutcome::Deferred
            }
        }
    }

    async fn withdraw_with_evidence(
        &self,
        claim: &ClaimedContentLockDeletionJob,
        worker_id: &str,
    ) -> DeletionPhaseExecution {
        let tombstone = tombstone(claim);
        let content_lock_path = ContentLockPath::from_lock_id(claim.job.lock_id.clone());
        let readback = self
            .dependencies
            .tombstones
            .withdraw_content_lock(
                claim.job.creator.clone(),
                content_lock_path,
                &claim.job.frozen_content_lock,
                &tombstone,
            )
            .await;
        let pubky_healthy =
            DeletionDependencyEvidence::healthy(DeletionDependencySource::PubkyWithdrawal);
        match readback {
            Ok(TombstoneReadback::Exact) => self
                .repository_execution(
                    self.advance(
                        claim,
                        worker_id,
                        ContentLockDeletionPhase::StartPaymentDrain,
                    )
                    .await,
                )
                .with_evidence(pubky_healthy),
            Ok(TombstoneReadback::Missing) => self
                .repository_execution(
                    self.finish_terminal(
                        claim,
                        worker_id,
                        ContentLockDeletionFailureCode::TombstoneMissing,
                    )
                    .await,
                )
                .with_evidence(pubky_healthy),
            Ok(TombstoneReadback::Replaced) => self
                .repository_execution(
                    self.finish_terminal(
                        claim,
                        worker_id,
                        ContentLockDeletionFailureCode::TombstoneReplaced,
                    )
                    .await,
                )
                .with_evidence(pubky_healthy),
            Err(error) => error_execution(&error, DeletionDependencySource::PubkyWithdrawal),
        }
    }

    fn repository_execution(
        &self,
        outcome: DeletionPhaseExecutionOutcome,
    ) -> DeletionPhaseExecution {
        let evidence = match outcome {
            DeletionPhaseExecutionOutcome::TransientDependencyFailure => {
                DeletionDependencyEvidence::unavailable(
                    DeletionDependencySource::RepositoryPhaseMutation,
                )
            }
            DeletionPhaseExecutionOutcome::Progressed
            | DeletionPhaseExecutionOutcome::Deferred
            | DeletionPhaseExecutionOutcome::TerminalFailed => DeletionDependencyEvidence::healthy(
                DeletionDependencySource::RepositoryPhaseMutation,
            ),
            DeletionPhaseExecutionOutcome::ClaimLost
            | DeletionPhaseExecutionOutcome::FatalFailure => DeletionDependencyEvidence::none(),
        };
        DeletionPhaseExecution::new(outcome).with_evidence(evidence)
    }

    async fn withdraw(
        &self,
        claim: &ClaimedContentLockDeletionJob,
        worker_id: &str,
    ) -> DeletionPhaseExecutionOutcome {
        let tombstone = tombstone(claim);
        let content_lock_path = ContentLockPath::from_lock_id(claim.job.lock_id.clone());
        let readback = self
            .dependencies
            .tombstones
            .withdraw_content_lock(
                claim.job.creator.clone(),
                content_lock_path,
                &claim.job.frozen_content_lock,
                &tombstone,
            )
            .await;
        match readback {
            Ok(TombstoneReadback::Exact) => {
                self.advance(
                    claim,
                    worker_id,
                    ContentLockDeletionPhase::StartPaymentDrain,
                )
                .await
            }
            Ok(TombstoneReadback::Missing) => {
                self.finish_terminal(
                    claim,
                    worker_id,
                    ContentLockDeletionFailureCode::TombstoneMissing,
                )
                .await
            }
            Ok(TombstoneReadback::Replaced) => {
                self.finish_terminal(
                    claim,
                    worker_id,
                    ContentLockDeletionFailureCode::TombstoneReplaced,
                )
                .await
            }
            Err(error) => error_outcome(&error),
        }
    }

    async fn issue_final_credentials(
        &self,
        claim: &ClaimedContentLockDeletionJob,
        worker_id: &str,
    ) -> DeletionPhaseExecutionOutcome {
        let windows = match self
            .dependencies
            .access_credentials
            .initialize_final_access_windows(
                claim.job.job_id,
                worker_id,
                claim.claim_token,
                self.config.final_credential_issuance_window,
                self.config.final_read_window,
            )
            .await
        {
            Ok(InitializeFinalAccessWindowsResult::Initialized(windows)) => windows,
            Ok(InitializeFinalAccessWindowsResult::ClaimLost) => {
                return DeletionPhaseExecutionOutcome::ClaimLost;
            }
            Err(error) => return error_outcome(&error),
        };

        let materialized = self
            .dependencies
            .final_credentials
            .materialize(MaterializeFinalCredentialsRequest {
                deletion_job_id: claim.job.job_id,
                worker_id,
                claim_token: claim.claim_token,
                now: windows.issuance_started_at,
                batch_limit: self.config.final_credential_batch_limit,
            })
            .await;
        match materialized {
            Ok(outcome)
                if self.config.final_credential_batch_limit > 0
                    && outcome.materialized_count >= self.config.final_credential_batch_limit =>
            {
                DeletionPhaseExecutionOutcome::Deferred
            }
            Ok(_) => {
                self.advance_or_defer(claim, worker_id, ContentLockDeletionPhase::DrainFinalReads)
                    .await
            }
            Err(error) => error_outcome(&error),
        }
    }

    async fn verify_frozen_content_with_evidence(
        &self,
        claim: &ClaimedContentLockDeletionJob,
        worker_id: &str,
    ) -> DeletionPhaseExecution {
        let tombstone = tombstone(claim);
        let content_lock_path = ContentLockPath::from_lock_id(claim.job.lock_id.clone());
        let mut resources = BTreeMap::<String, GuardedResourceHash>::new();
        for (path, resource) in &claim.job.frozen_content_lock.secondary_resources {
            resources.insert(path.clone(), resource.hash);
        }
        if let Some(primary) = &claim.job.frozen_content_lock.primary_resource {
            resources.insert(primary.path.clone(), primary.hash);
        }

        let pubky_healthy =
            DeletionDependencyEvidence::healthy(DeletionDependencySource::PubkyReadback);
        let mut resource_was_read = false;
        for (path, expected_hash) in resources {
            match self
                .dependencies
                .tombstones
                .read_tombstone(&claim.job.creator, &content_lock_path, &tombstone)
                .await
            {
                Ok(TombstoneReadback::Exact) => {}
                Ok(TombstoneReadback::Missing) => {
                    return self
                        .repository_execution(
                            self.finish_terminal(
                                claim,
                                worker_id,
                                ContentLockDeletionFailureCode::TombstoneMissing,
                            )
                            .await,
                        )
                        .with_evidence(pubky_healthy);
                }
                Ok(TombstoneReadback::Replaced) => {
                    return self
                        .repository_execution(
                            self.finish_terminal(
                                claim,
                                worker_id,
                                ContentLockDeletionFailureCode::TombstoneReplaced,
                            )
                            .await,
                        )
                        .with_evidence(pubky_healthy);
                }
                Err(error) => {
                    return error_execution(&error, DeletionDependencySource::PubkyReadback);
                }
            }
            match self
                .dependencies
                .guarded_resources
                .read_guarded_resource_generation(&claim.job.creator, &path, &expected_hash)
                .await
            {
                Ok(GuardedResourceReadback::Exact) => resource_was_read = true,
                Ok(GuardedResourceReadback::Missing) => {
                    return self
                        .repository_execution(
                            self.finish_terminal(
                                claim,
                                worker_id,
                                ContentLockDeletionFailureCode::StateCorrupt,
                            )
                            .await,
                        )
                        .with_evidence(pubky_healthy)
                        .with_evidence(DeletionDependencyEvidence::healthy(
                            DeletionDependencySource::PubkyResource,
                        ));
                }
                Ok(GuardedResourceReadback::Replaced) => {
                    return self
                        .repository_execution(
                            self.finish_terminal(
                                claim,
                                worker_id,
                                ContentLockDeletionFailureCode::ResourceReplaced,
                            )
                            .await,
                        )
                        .with_evidence(pubky_healthy)
                        .with_evidence(DeletionDependencyEvidence::healthy(
                            DeletionDependencySource::PubkyResource,
                        ));
                }
                Err(error) => {
                    return error_execution(&error, DeletionDependencySource::PubkyResource)
                        .with_evidence(pubky_healthy);
                }
            }
        }

        let resource_evidence = if resource_was_read {
            DeletionDependencyEvidence::healthy(DeletionDependencySource::PubkyResource)
        } else {
            DeletionDependencyEvidence::none()
        };

        match self
            .dependencies
            .tombstones
            .read_tombstone(&claim.job.creator, &content_lock_path, &tombstone)
            .await
        {
            Ok(TombstoneReadback::Exact) => self
                .repository_execution(
                    self.advance(claim, worker_id, ContentLockDeletionPhase::DeleteTombstone)
                        .await,
                )
                .with_evidence(pubky_healthy)
                .with_evidence(resource_evidence),
            Ok(TombstoneReadback::Missing) => self
                .repository_execution(
                    self.finish_terminal(
                        claim,
                        worker_id,
                        ContentLockDeletionFailureCode::TombstoneMissing,
                    )
                    .await,
                )
                .with_evidence(pubky_healthy)
                .with_evidence(resource_evidence),
            Ok(TombstoneReadback::Replaced) => self
                .repository_execution(
                    self.finish_terminal(
                        claim,
                        worker_id,
                        ContentLockDeletionFailureCode::TombstoneReplaced,
                    )
                    .await,
                )
                .with_evidence(pubky_healthy)
                .with_evidence(resource_evidence),
            Err(error) => error_execution(&error, DeletionDependencySource::PubkyReadback)
                .with_evidence(resource_evidence),
        }
    }

    async fn verify_tombstone_for_purge_with_evidence(
        &self,
        claim: &ClaimedContentLockDeletionJob,
        worker_id: &str,
    ) -> DeletionPhaseExecution {
        let tombstone = tombstone(claim);
        let content_lock_path = ContentLockPath::from_lock_id(claim.job.lock_id.clone());
        let readback = self
            .dependencies
            .tombstones
            .read_tombstone(&claim.job.creator, &content_lock_path, &tombstone)
            .await;
        let pubky_healthy =
            DeletionDependencyEvidence::healthy(DeletionDependencySource::PubkyReadback);
        match readback {
            Ok(TombstoneReadback::Exact) => self
                .repository_execution(
                    self.advance(
                        claim,
                        worker_id,
                        ContentLockDeletionPhase::PurgeOperationalState,
                    )
                    .await,
                )
                .with_evidence(pubky_healthy),
            Ok(TombstoneReadback::Missing) => self
                .repository_execution(
                    self.finish_terminal(
                        claim,
                        worker_id,
                        ContentLockDeletionFailureCode::TombstoneMissing,
                    )
                    .await,
                )
                .with_evidence(pubky_healthy),
            Ok(TombstoneReadback::Replaced) => self
                .repository_execution(
                    self.finish_terminal(
                        claim,
                        worker_id,
                        ContentLockDeletionFailureCode::TombstoneReplaced,
                    )
                    .await,
                )
                .with_evidence(pubky_healthy),
            Err(error) => error_execution(&error, DeletionDependencySource::PubkyReadback),
        }
    }

    async fn verify_frozen_content(
        &self,
        claim: &ClaimedContentLockDeletionJob,
        worker_id: &str,
    ) -> DeletionPhaseExecutionOutcome {
        let tombstone = tombstone(claim);
        let content_lock_path = ContentLockPath::from_lock_id(claim.job.lock_id.clone());
        let mut resources = BTreeMap::<String, GuardedResourceHash>::new();
        for (path, resource) in &claim.job.frozen_content_lock.secondary_resources {
            resources.insert(path.clone(), resource.hash);
        }
        if let Some(primary) = &claim.job.frozen_content_lock.primary_resource {
            resources.insert(primary.path.clone(), primary.hash);
        }

        for (path, expected_hash) in resources {
            match self
                .dependencies
                .tombstones
                .read_tombstone(&claim.job.creator, &content_lock_path, &tombstone)
                .await
            {
                Ok(TombstoneReadback::Exact) => {}
                Ok(TombstoneReadback::Missing) => {
                    return self
                        .finish_terminal(
                            claim,
                            worker_id,
                            ContentLockDeletionFailureCode::TombstoneMissing,
                        )
                        .await;
                }
                Ok(TombstoneReadback::Replaced) => {
                    return self
                        .finish_terminal(
                            claim,
                            worker_id,
                            ContentLockDeletionFailureCode::TombstoneReplaced,
                        )
                        .await;
                }
                Err(error) => return error_outcome(&error),
            }
            match self
                .dependencies
                .guarded_resources
                .read_guarded_resource_generation(&claim.job.creator, &path, &expected_hash)
                .await
            {
                Ok(GuardedResourceReadback::Exact) => {}
                Ok(GuardedResourceReadback::Missing) => {
                    return self
                        .finish_terminal(
                            claim,
                            worker_id,
                            ContentLockDeletionFailureCode::StateCorrupt,
                        )
                        .await;
                }
                Ok(GuardedResourceReadback::Replaced) => {
                    return self
                        .finish_terminal(
                            claim,
                            worker_id,
                            ContentLockDeletionFailureCode::ResourceReplaced,
                        )
                        .await;
                }
                Err(error) => return error_outcome(&error),
            }
        }

        match self
            .dependencies
            .tombstones
            .read_tombstone(&claim.job.creator, &content_lock_path, &tombstone)
            .await
        {
            Ok(TombstoneReadback::Exact) => {}
            Ok(TombstoneReadback::Missing) => {
                return self
                    .finish_terminal(
                        claim,
                        worker_id,
                        ContentLockDeletionFailureCode::TombstoneMissing,
                    )
                    .await;
            }
            Ok(TombstoneReadback::Replaced) => {
                return self
                    .finish_terminal(
                        claim,
                        worker_id,
                        ContentLockDeletionFailureCode::TombstoneReplaced,
                    )
                    .await;
            }
            Err(error) => return error_outcome(&error),
        }

        self.advance(claim, worker_id, ContentLockDeletionPhase::DeleteTombstone)
            .await
    }

    async fn verify_tombstone_for_purge(
        &self,
        claim: &ClaimedContentLockDeletionJob,
        worker_id: &str,
    ) -> DeletionPhaseExecutionOutcome {
        let tombstone = tombstone(claim);
        let content_lock_path = ContentLockPath::from_lock_id(claim.job.lock_id.clone());
        match self
            .dependencies
            .tombstones
            .read_tombstone(&claim.job.creator, &content_lock_path, &tombstone)
            .await
        {
            Ok(TombstoneReadback::Exact) => {
                self.advance(
                    claim,
                    worker_id,
                    ContentLockDeletionPhase::PurgeOperationalState,
                )
                .await
            }
            Ok(TombstoneReadback::Missing) => {
                self.finish_terminal(
                    claim,
                    worker_id,
                    ContentLockDeletionFailureCode::TombstoneMissing,
                )
                .await
            }
            Ok(TombstoneReadback::Replaced) => {
                self.finish_terminal(
                    claim,
                    worker_id,
                    ContentLockDeletionFailureCode::TombstoneReplaced,
                )
                .await
            }
            Err(error) => error_outcome(&error),
        }
    }

    async fn advance(
        &self,
        claim: &ClaimedContentLockDeletionJob,
        worker_id: &str,
        next_phase: ContentLockDeletionPhase,
    ) -> DeletionPhaseExecutionOutcome {
        match self
            .dependencies
            .deletions
            .advance_phase(claim.job.job_id, worker_id, claim.claim_token, next_phase)
            .await
        {
            Ok(AdvanceContentLockDeletionPhaseResult::Advanced(_)) => {
                DeletionPhaseExecutionOutcome::Progressed
            }
            Ok(AdvanceContentLockDeletionPhaseResult::ClaimLost) => {
                DeletionPhaseExecutionOutcome::ClaimLost
            }
            Ok(AdvanceContentLockDeletionPhaseResult::ObligationsPending) => {
                DeletionPhaseExecutionOutcome::Deferred
            }
            Ok(AdvanceContentLockDeletionPhaseResult::TerminalFailure(failure_code)) => {
                self.finish_terminal(claim, worker_id, failure_code).await
            }
            Err(error) => error_outcome(&error),
        }
    }

    async fn advance_or_defer(
        &self,
        claim: &ClaimedContentLockDeletionJob,
        worker_id: &str,
        next_phase: ContentLockDeletionPhase,
    ) -> DeletionPhaseExecutionOutcome {
        match self
            .dependencies
            .deletions
            .advance_phase(claim.job.job_id, worker_id, claim.claim_token, next_phase)
            .await
        {
            Ok(AdvanceContentLockDeletionPhaseResult::Advanced(_)) => {
                DeletionPhaseExecutionOutcome::Progressed
            }
            Ok(AdvanceContentLockDeletionPhaseResult::ClaimLost) => {
                DeletionPhaseExecutionOutcome::ClaimLost
            }
            Ok(AdvanceContentLockDeletionPhaseResult::ObligationsPending) => {
                DeletionPhaseExecutionOutcome::Deferred
            }
            Ok(AdvanceContentLockDeletionPhaseResult::TerminalFailure(failure_code)) => {
                self.finish_terminal(claim, worker_id, failure_code).await
            }
            Err(error) => error_outcome(&error),
        }
    }

    async fn finish_terminal(
        &self,
        claim: &ClaimedContentLockDeletionJob,
        worker_id: &str,
        failure_code: ContentLockDeletionFailureCode,
    ) -> DeletionPhaseExecutionOutcome {
        match self
            .dependencies
            .deletions
            .finish(
                claim.job.job_id,
                worker_id,
                claim.claim_token,
                Some(failure_code),
            )
            .await
        {
            Ok(Some(_)) => DeletionPhaseExecutionOutcome::TerminalFailed,
            Ok(None) => DeletionPhaseExecutionOutcome::ClaimLost,
            Err(error) => error_outcome(&error),
        }
    }
}

fn tombstone(claim: &ClaimedContentLockDeletionJob) -> ContentLockDeletionTombstone {
    ContentLockDeletionTombstone::new(claim.job.lock_id.clone(), claim.job.deletion_started_at)
}

#[cfg(test)]
mod tests;
