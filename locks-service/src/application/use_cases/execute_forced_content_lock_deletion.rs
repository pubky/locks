use std::collections::BTreeSet;

use locks_core::ids::ContentLockPath;

use crate::application::{
    errors::ApplicationError,
    models::ClaimedContentLockDeletionJob,
    ports::{
        Clock, ContentLockDeletionActionAcquireResult, ContentLockDeletionActionClaim,
        ContentLockDeletionActionOwnership, ContentLockDeletionRepository,
        ContentLockTombstoneRepository, GuardedResourceRepository,
    },
};

use super::execute_content_lock_deletion_phase::{
    DeletionDependencyEvidence, DeletionDependencySource, DeletionExecutionErrorClass,
    classify_deletion_execution_error,
};

/// Closed, secret-free result of one active-force external execution attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForcedContentLockDeletionOutcome {
    Completed,
    Deferred,
    ClaimLost,
    TransientDependencyFailure,
    FatalFailure,
}

fn error_outcome(error: &ApplicationError) -> ForcedContentLockDeletionOutcome {
    match classify_deletion_execution_error(error) {
        DeletionExecutionErrorClass::TransientDependency => {
            ForcedContentLockDeletionOutcome::TransientDependencyFailure
        }
        DeletionExecutionErrorClass::Fatal => ForcedContentLockDeletionOutcome::FatalFailure,
    }
}

/// One force outcome plus exact, identifier-free dependency observations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForcedContentLockDeletionExecution {
    pub outcome: ForcedContentLockDeletionOutcome,
    pub evidence: DeletionDependencyEvidence,
}

impl ForcedContentLockDeletionExecution {
    fn new(outcome: ForcedContentLockDeletionOutcome) -> Self {
        Self {
            outcome,
            evidence: DeletionDependencyEvidence::none(),
        }
    }

    fn with_evidence(mut self, evidence: DeletionDependencyEvidence) -> Self {
        self.evidence = self.evidence.merge(evidence);
        self
    }
}

fn error_execution(
    error: &ApplicationError,
    source: DeletionDependencySource,
) -> ForcedContentLockDeletionExecution {
    ForcedContentLockDeletionExecution::new(error_outcome(error))
        .with_evidence(DeletionDependencyEvidence::unavailable(source))
}

/// External dependencies required by active-force execution.
pub struct ExecuteForcedContentLockDeletionDependencies<'a> {
    pub action_ownership: &'a dyn ContentLockDeletionActionOwnership,
    pub tombstones: &'a dyn ContentLockTombstoneRepository,
    pub guarded_resources: &'a dyn GuardedResourceRepository,
    pub deletions: &'a dyn ContentLockDeletionRepository,
    pub clock: &'a dyn Clock,
}

/// Executes the force path for an already-claimed active deletion job.
pub struct ExecuteForcedContentLockDeletionUseCase<'a> {
    dependencies: ExecuteForcedContentLockDeletionDependencies<'a>,
}

impl<'a> ExecuteForcedContentLockDeletionUseCase<'a> {
    pub fn new(dependencies: ExecuteForcedContentLockDeletionDependencies<'a>) -> Self {
        Self { dependencies }
    }

    pub async fn execute(
        &self,
        claim: ClaimedContentLockDeletionJob,
        worker_id: &str,
    ) -> ForcedContentLockDeletionOutcome {
        self.execute_with_evidence(claim, worker_id).await.outcome
    }

    pub async fn execute_with_evidence(
        &self,
        claim: ClaimedContentLockDeletionJob,
        worker_id: &str,
    ) -> ForcedContentLockDeletionExecution {
        if claim.job.force_requested_at.is_none() {
            return ForcedContentLockDeletionExecution::new(
                ForcedContentLockDeletionOutcome::Deferred,
            );
        }

        let guard = match self
            .dependencies
            .action_ownership
            .try_acquire(ContentLockDeletionActionClaim {
                job_id: claim.job.job_id,
                worker_id,
                claim_token: claim.claim_token,
                expected_phase: claim.job.phase,
                force: true,
            })
            .await
        {
            Ok(ContentLockDeletionActionAcquireResult::Acquired(guard)) => guard,
            Ok(ContentLockDeletionActionAcquireResult::Busy) => {
                return ForcedContentLockDeletionExecution::new(
                    ForcedContentLockDeletionOutcome::Deferred,
                );
            }
            Ok(ContentLockDeletionActionAcquireResult::ClaimLost) => {
                return ForcedContentLockDeletionExecution::new(
                    ForcedContentLockDeletionOutcome::ClaimLost,
                );
            }
            Err(error) => {
                return error_execution(&error, DeletionDependencySource::RepositoryActionLock);
            }
        };

        let execution = self.execute_guarded(&claim, worker_id).await.with_evidence(
            DeletionDependencyEvidence::healthy(DeletionDependencySource::RepositoryActionLock),
        );
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

    async fn execute_guarded(
        &self,
        claim: &ClaimedContentLockDeletionJob,
        worker_id: &str,
    ) -> ForcedContentLockDeletionExecution {
        let public_path = ContentLockPath::from_lock_id(claim.job.lock_id.clone());
        if let Err(error) = self
            .dependencies
            .tombstones
            .force_delete_content_lock_and_verify_absent(&claim.job.creator, &public_path)
            .await
        {
            return error_execution(&error, DeletionDependencySource::PubkyForcePublic);
        }

        let mut evidence =
            DeletionDependencyEvidence::healthy(DeletionDependencySource::PubkyForcePublic);
        for path in frozen_resource_paths(claim) {
            evidence = evidence.merge(
                match self
                    .dependencies
                    .guarded_resources
                    .delete_guarded_resource(&claim.job.creator, &path)
                    .await
                {
                    Ok(_) => {
                        DeletionDependencyEvidence::healthy(DeletionDependencySource::PubkyResource)
                    }
                    Err(_) => DeletionDependencyEvidence::unavailable(
                        DeletionDependencySource::PubkyResource,
                    ),
                },
            );
        }

        let execution = match self
            .dependencies
            .deletions
            .complete_force_deletion(claim.job.job_id, worker_id, claim.claim_token)
            .await
        {
            Ok(true) => {
                ForcedContentLockDeletionExecution::new(ForcedContentLockDeletionOutcome::Completed)
                    .with_evidence(DeletionDependencyEvidence::healthy(
                        DeletionDependencySource::RepositoryForceReceipt,
                    ))
            }
            Ok(false) => {
                ForcedContentLockDeletionExecution::new(ForcedContentLockDeletionOutcome::ClaimLost)
            }
            Err(error) => error_execution(&error, DeletionDependencySource::RepositoryForceReceipt),
        };
        execution.with_evidence(evidence)
    }
}

fn frozen_resource_paths(claim: &ClaimedContentLockDeletionJob) -> BTreeSet<String> {
    let mut paths = claim
        .job
        .frozen_content_lock
        .secondary_resources
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    if let Some(primary) = &claim.job.frozen_content_lock.primary_resource {
        paths.insert(primary.path.clone());
    }
    paths
}

#[cfg(test)]
mod tests;
