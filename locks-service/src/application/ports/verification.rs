use async_trait::async_trait;
use locks_core::ids::{BundleId, CreatorPubky, TaskId};
use locks_core::lock_policy::VerifierType;
use locks_core::verification::CriterionVerificationResult;

use crate::application::errors::ApplicationError;
use crate::application::models::{
    ClaimedVerificationTask, CriterionVerificationRequest, VerificationTaskRecord,
};

/// Repository for asynchronous verification task state.
#[async_trait]
pub trait VerificationTaskRepository: Send + Sync {
    /// Inserts a new verification task.
    ///
    /// Returns `DuplicateRecord` if a task with the same Task ID or public
    /// verification attempt handle (`creator`, `bundle_id`) already exists.
    async fn insert_verification_task(
        &self,
        task: VerificationTaskRecord,
    ) -> Result<(), ApplicationError>;

    /// Updates an existing verification task.
    ///
    /// Returns `MissingRecord` when the task does not exist.
    async fn update_verification_task(
        &self,
        task: VerificationTaskRecord,
    ) -> Result<(), ApplicationError>;

    /// Loads a verification task by task ID.
    ///
    /// Returns `Ok(None)` when no task exists.
    async fn get_verification_task(
        &self,
        task_id: &TaskId,
    ) -> Result<Option<VerificationTaskRecord>, ApplicationError>;

    /// Loads a verification task by public verification attempt handle.
    ///
    /// Returns `Ok(None)` when no task exists for the creator and Bundle ID.
    async fn get_verification_task_by_handle(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<Option<VerificationTaskRecord>, ApplicationError> {
        let _ = (creator, bundle_id);
        Err(ApplicationError::Storage {
            message: "verification task handle lookup is not implemented by this repository"
                .to_owned(),
        })
    }

    /// Ensures a verification task is absent.
    ///
    /// Deleting a missing task is successful.
    async fn delete_verification_task(&self, task_id: &TaskId) -> Result<(), ApplicationError>;
}

/// Worker-facing port for claiming verification task leases.
#[async_trait]
pub trait VerificationTaskClaimer: Send + Sync {
    /// Claims one pending or expired in-progress verification task for a worker.
    ///
    /// Returns `Ok(None)` when no task is claimable. Every successful claim includes a fresh
    /// opaque token. The `now` parameter defines expiration comparison time, and
    /// `claim_expires_at` is the new lease expiry assigned to the claimed task.
    async fn claim_next_verification_task(
        &self,
        worker_id: &str,
        now: time::OffsetDateTime,
        claim_expires_at: time::OffsetDateTime,
    ) -> Result<Option<ClaimedVerificationTask>, ApplicationError>;

    /// Returns an actively owned in-progress task to pending with a durable retry due time.
    ///
    /// Returns `Ok(None)` when the task is missing, not in progress, has an expired lease, or is
    /// no longer claimed by the exact `worker_id` and `claim_token` incarnation. Implementations
    /// must clear the active lease without resetting attempt count.
    async fn schedule_verification_task_retry(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        now: time::OffsetDateTime,
        next_attempt_at: time::OffsetDateTime,
    ) -> Result<Option<VerificationTaskRecord>, ApplicationError>;

    /// Persists a terminal task transition only for the exact active lease incarnation.
    ///
    /// Returns `Ok(None)` when the task, worker, token, status, or lease no longer matches.
    async fn persist_claimed_verification_task_transition(
        &self,
        task: VerificationTaskRecord,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        now: time::OffsetDateTime,
    ) -> Result<Option<VerificationTaskRecord>, ApplicationError>;
}

/// Generator for server-owned verification task IDs.
#[async_trait]
pub trait VerificationTaskIdGenerator: Send + Sync {
    /// Generates a new operational verification task ID.
    async fn generate_task_id(&self) -> Result<TaskId, ApplicationError>;
}

/// Adapter boundary for criterion-specific verification logic.
#[async_trait]
pub trait CriterionVerifier: Send + Sync {
    /// Verifies one criterion/proof pair and returns minimal criterion-level evidence.
    async fn verify(
        &self,
        request: CriterionVerificationRequest,
    ) -> Result<CriterionVerificationResult, ApplicationError>;
}

/// Registry that dispatches protocol verifier types to concrete verifier adapters.
pub trait CriterionVerifierRegistry: Send + Sync {
    /// Returns the verifier adapter registered for the protocol verifier type.
    fn verifier_for(&self, verifier_type: VerifierType) -> Option<&dyn CriterionVerifier>;
}
