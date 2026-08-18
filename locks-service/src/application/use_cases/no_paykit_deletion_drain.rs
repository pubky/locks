use async_trait::async_trait;

use crate::application::{
    errors::ApplicationError,
    models::{
        AdvanceContentLockDeletionPhaseResult, ClaimedContentLockDeletionJob,
        ContentLockDeletionPhase,
    },
    ports::{Clock, ContentLockDeletionRepository},
};

use super::execute_content_lock_deletion_phase::{
    ContentLockPaymentDrainExecutor, DeletionDependencyEvidence, DeletionDependencySource,
    DeletionExecutionErrorClass, DeletionPhaseExecution, DeletionPhaseExecutionOutcome,
    classify_deletion_execution_error,
};

#[async_trait]
pub trait NoPaykitDeletionDrainExecutor: Send + Sync {
    async fn execute_claimed(
        &self,
        claim: ClaimedContentLockDeletionJob,
        worker_id: &str,
    ) -> Result<bool, ApplicationError>;
}

pub struct NoPaykitDeletionDrainUseCase<'a> {
    deletions: &'a dyn ContentLockDeletionRepository,
    clock: &'a dyn Clock,
}

impl<'a> NoPaykitDeletionDrainUseCase<'a> {
    pub fn new(deletions: &'a dyn ContentLockDeletionRepository, clock: &'a dyn Clock) -> Self {
        Self { deletions, clock }
    }

    pub async fn execute_claimed(
        &self,
        claim: ClaimedContentLockDeletionJob,
        worker_id: &str,
    ) -> Result<bool, ApplicationError> {
        let next_phase = match claim.job.phase {
            ContentLockDeletionPhase::StartPaymentDrain => ContentLockDeletionPhase::DrainPayments,
            ContentLockDeletionPhase::DrainPayments => {
                ContentLockDeletionPhase::DrainExistingCredentials
            }
            _ => {
                return Err(ApplicationError::InvalidContentLockDeletionState {
                    message: "non-Paykit drain requires a payment drain phase".to_owned(),
                });
            }
        };
        let _now = self.clock.now();
        if !self
            .deletions
            .expire_unresolved_non_paykit_tasks(claim.job.job_id, worker_id, claim.claim_token)
            .await?
        {
            return Ok(false);
        }
        Ok(matches!(
            self.deletions
                .advance_phase(claim.job.job_id, worker_id, claim.claim_token, next_phase)
                .await?,
            AdvanceContentLockDeletionPhaseResult::Advanced(_)
        ))
    }
}

#[async_trait]
impl NoPaykitDeletionDrainExecutor for NoPaykitDeletionDrainUseCase<'_> {
    async fn execute_claimed(
        &self,
        claim: ClaimedContentLockDeletionJob,
        worker_id: &str,
    ) -> Result<bool, ApplicationError> {
        NoPaykitDeletionDrainUseCase::execute_claimed(self, claim, worker_id).await
    }
}

#[async_trait]
impl ContentLockPaymentDrainExecutor for NoPaykitDeletionDrainUseCase<'_> {
    async fn execute_claimed(
        &self,
        claim: ClaimedContentLockDeletionJob,
        worker_id: &str,
    ) -> DeletionPhaseExecution {
        let next_phase = match claim.job.phase {
            ContentLockDeletionPhase::StartPaymentDrain => ContentLockDeletionPhase::DrainPayments,
            ContentLockDeletionPhase::DrainPayments => {
                ContentLockDeletionPhase::DrainExistingCredentials
            }
            _ => return DeletionPhaseExecution::new(DeletionPhaseExecutionOutcome::FatalFailure),
        };
        let _now = self.clock.now();
        let mut evidence = DeletionDependencyEvidence::none();
        match self
            .deletions
            .expire_unresolved_non_paykit_tasks(claim.job.job_id, worker_id, claim.claim_token)
            .await
        {
            Ok(false) => {
                return DeletionPhaseExecution::new(DeletionPhaseExecutionOutcome::ClaimLost);
            }
            Ok(true) => {
                evidence = evidence.merge(DeletionDependencyEvidence::healthy(
                    DeletionDependencySource::PaymentDrainRepository,
                ));
            }
            Err(error) => {
                return observed_error(
                    &error,
                    DeletionDependencySource::PaymentDrainRepository,
                    evidence,
                );
            }
        }

        match self
            .deletions
            .advance_phase(claim.job.job_id, worker_id, claim.claim_token, next_phase)
            .await
        {
            Ok(AdvanceContentLockDeletionPhaseResult::Advanced(_)) => {
                observed_phase(DeletionPhaseExecutionOutcome::Progressed, evidence)
            }
            Ok(AdvanceContentLockDeletionPhaseResult::ClaimLost) => {
                DeletionPhaseExecution::new(DeletionPhaseExecutionOutcome::ClaimLost)
                    .with_evidence(evidence)
            }
            Ok(AdvanceContentLockDeletionPhaseResult::ObligationsPending) => {
                observed_phase(DeletionPhaseExecutionOutcome::Deferred, evidence)
            }
            Ok(AdvanceContentLockDeletionPhaseResult::TerminalFailure(_)) => {
                observed_phase(DeletionPhaseExecutionOutcome::TerminalFailed, evidence)
            }
            Err(error) => observed_error(
                &error,
                DeletionDependencySource::RepositoryPhaseMutation,
                evidence,
            ),
        }
    }
}

fn observed_phase(
    outcome: DeletionPhaseExecutionOutcome,
    evidence: DeletionDependencyEvidence,
) -> DeletionPhaseExecution {
    DeletionPhaseExecution::new(outcome)
        .with_evidence(evidence)
        .with_evidence(DeletionDependencyEvidence::healthy(
            DeletionDependencySource::RepositoryPhaseMutation,
        ))
}

fn observed_error(
    error: &ApplicationError,
    source: DeletionDependencySource,
    evidence: DeletionDependencyEvidence,
) -> DeletionPhaseExecution {
    let outcome = match classify_deletion_execution_error(error) {
        DeletionExecutionErrorClass::TransientDependency => {
            DeletionPhaseExecutionOutcome::TransientDependencyFailure
        }
        DeletionExecutionErrorClass::Fatal => DeletionPhaseExecutionOutcome::FatalFailure,
    };
    DeletionPhaseExecution::new(outcome)
        .with_evidence(evidence)
        .with_evidence(DeletionDependencyEvidence::unavailable(source))
}
