use async_trait::async_trait;
use time::OffsetDateTime;

use locks_core::ids::{BundleId, CreatorPubky, LockServerPubky, TaskId};
use locks_core::lock_policy::ContentLock;
use locks_core::verification::{
    EntitlementLifetime, VERIFIED_PROOF_BUNDLE_VERSION, VerificationResult, VerifiedProofBundle,
};

use crate::application::entitlement_evaluator::evaluate_entitlement;
use crate::application::errors::ApplicationError;
use crate::application::models::{
    ClaimedVerificationTask, CriterionVerificationRequest, VerificationTaskRecord,
    VerificationTaskStatus,
};
use crate::application::ports::{
    Clock, ContentLockRepository, CriterionVerifierRegistry, EntitlementRepository,
    VerificationTaskClaimer, VerificationTaskRepository, same_entitlement_decision,
};
use crate::application::use_cases::entitlement_check::verify_content_lock_identity;

/// Worker request to complete verifier processing for a submitted proof bundle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompleteVerificationTaskRequest {
    /// Server-generated operational task ID.
    pub task_id: TaskId,
}

/// Worker response after verification task completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompletedVerificationTask {
    /// Server-generated operational task ID.
    pub task_id: TaskId,
    /// Final task status.
    pub status: VerificationTaskStatus,
    /// Timestamp when terminal task state was persisted.
    pub completed_at: OffsetDateTime,
}

/// Worker-owned verification completion orchestration.
pub struct CompleteVerificationTaskUseCase<'a> {
    tasks: &'a dyn VerificationTaskRepository,
    content_locks: &'a dyn ContentLockRepository,
    entitlements: &'a dyn EntitlementRepository,
    verifiers: &'a dyn CriterionVerifierRegistry,
    clock: &'a dyn Clock,
    verified_by: LockServerPubky,
}

struct ClaimFencedTaskRepository<'a> {
    claim: ClaimedVerificationTask,
    claimer: &'a dyn VerificationTaskClaimer,
    worker_id: &'a str,
}

struct ClaimFencedEntitlementRepository<'a> {
    inner: &'a dyn EntitlementRepository,
    claim: &'a ClaimedVerificationTask,
    claimer: &'a dyn VerificationTaskClaimer,
    worker_id: &'a str,
}

#[async_trait]
impl EntitlementRepository for ClaimFencedEntitlementRepository<'_> {
    async fn insert_verified_proof_bundle(
        &self,
        entitlement: VerifiedProofBundle,
    ) -> Result<(), ApplicationError> {
        if !self
            .claimer
            .begin_claimed_entitlement_publication(
                &self.claim.task.task_id,
                self.worker_id,
                &self.claim.claim_token,
            )
            .await?
        {
            return Err(ApplicationError::VerificationTaskClaimLost);
        }
        self.inner.insert_verified_proof_bundle(entitlement).await
    }

    async fn get_verified_proof_bundle(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<Option<VerifiedProofBundle>, ApplicationError> {
        self.inner
            .get_verified_proof_bundle(creator, bundle_id)
            .await
    }

    async fn delete_verified_proof_bundle(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<(), ApplicationError> {
        self.inner
            .delete_verified_proof_bundle(creator, bundle_id)
            .await
    }
}

#[async_trait]
impl VerificationTaskRepository for ClaimFencedTaskRepository<'_> {
    async fn insert_verification_task(
        &self,
        _task: VerificationTaskRecord,
    ) -> Result<(), ApplicationError> {
        unreachable!("claimed completion must not insert tasks")
    }

    async fn update_verification_task(
        &self,
        task: VerificationTaskRecord,
    ) -> Result<(), ApplicationError> {
        self.claimer
            .persist_claimed_verification_task_transition(
                task,
                self.worker_id,
                &self.claim.claim_token,
            )
            .await?
            .ok_or(ApplicationError::VerificationTaskClaimLost)?;
        Ok(())
    }

    async fn get_verification_task(
        &self,
        task_id: &TaskId,
    ) -> Result<Option<VerificationTaskRecord>, ApplicationError> {
        Ok((self.claim.task.task_id == *task_id).then(|| self.claim.task.clone()))
    }

    async fn delete_verification_task(&self, _task_id: &TaskId) -> Result<(), ApplicationError> {
        unreachable!("claimed completion must not delete tasks")
    }
}

impl<'a> CompleteVerificationTaskUseCase<'a> {
    /// Creates a completion use case from its ports.
    pub fn new(
        tasks: &'a dyn VerificationTaskRepository,
        content_locks: &'a dyn ContentLockRepository,
        entitlements: &'a dyn EntitlementRepository,
        verifiers: &'a dyn CriterionVerifierRegistry,
        clock: &'a dyn Clock,
        verified_by: LockServerPubky,
    ) -> Self {
        Self {
            tasks,
            content_locks,
            entitlements,
            verifiers,
            clock,
            verified_by,
        }
    }

    /// Runs verifier work and persists task/entitlement completion state.
    pub async fn execute(
        &self,
        request: CompleteVerificationTaskRequest,
    ) -> Result<CompletedVerificationTask, ApplicationError> {
        let mut task = self
            .tasks
            .get_verification_task(&request.task_id)
            .await?
            .ok_or(ApplicationError::MissingRecord {
                record: "verification_task",
            })?;

        let submitted = task.submitted_proof_bundle.clone();
        let pubky_lock_resource = submitted.pubky_lock_resource.clone();

        let content_lock = match self
            .content_locks
            .get_content_lock(
                pubky_lock_resource.creator(),
                pubky_lock_resource.content_lock_path(),
            )
            .await?
        {
            Some(content_lock) => content_lock,
            None => {
                let error = ApplicationError::ContentLockUnavailable;
                self.persist_failed_task(task, viewer_safe_failure_message(&error).to_owned())
                    .await?;
                return Err(error);
            }
        };

        if let Err(error) =
            verify_content_lock_identity(&content_lock, pubky_lock_resource.content_lock_path())
        {
            self.persist_failed_task(task, viewer_safe_failure_message(&error).to_owned())
                .await?;
            return Err(error);
        }

        let verification_result = match self.verify_criteria(&task, &content_lock).await {
            Ok(verification_result) => verification_result,
            Err(error) => {
                if matches!(
                    error,
                    ApplicationError::VerificationPending
                        | ApplicationError::VerificationDependencyUnavailable
                ) {
                    return Err(error);
                }
                self.persist_failed_task(task, viewer_safe_failure_message(&error).to_owned())
                    .await?;
                return Err(error);
            }
        };

        task = match task.status {
            VerificationTaskStatus::Pending => {
                let in_progress =
                    task.transition_to(VerificationTaskStatus::InProgress, self.clock.now(), None)?;
                self.tasks
                    .update_verification_task(in_progress.clone())
                    .await?;
                in_progress
            }
            VerificationTaskStatus::InProgress => task,
            _ => task,
        };

        match evaluate_entitlement(&content_lock, &verification_result) {
            Ok(true) => {}
            Ok(false) => {
                let error = ApplicationError::EntitlementNotSatisfied;
                self.persist_failed_task(task, viewer_safe_failure_message(&error).to_owned())
                    .await?;
                return Err(error);
            }
            Err(error) => {
                self.persist_failed_task(task, viewer_safe_failure_message(&error).to_owned())
                    .await?;
                return Err(error);
            }
        }

        let entitlement = VerifiedProofBundle {
            version: VERIFIED_PROOF_BUNDLE_VERSION,
            bundle_id: submitted.bundle_id,
            pubky_lock_resource,
            verification_result,
            entitlement_lifetime: EntitlementLifetime::Unbounded,
        };
        if let Err(error) = self
            .entitlements
            .insert_verified_proof_bundle(entitlement.clone())
            .await
        {
            match self
                .entitlements
                .get_verified_proof_bundle(
                    entitlement.pubky_lock_resource.creator(),
                    &entitlement.bundle_id,
                )
                .await
            {
                Ok(Some(existing)) if same_entitlement_decision(&existing, &entitlement) => {}
                Ok(Some(_)) => return Err(error),
                Ok(None) | Err(_) => return Err(ApplicationError::VerificationPending),
            }
        }

        let completed_at = self.clock.now();
        let completed =
            task.transition_to(VerificationTaskStatus::Completed, completed_at, None)?;
        self.tasks.update_verification_task(completed).await?;

        Ok(CompletedVerificationTask {
            task_id: request.task_id,
            status: VerificationTaskStatus::Completed,
            completed_at,
        })
    }

    /// Runs verifier work for a worker-owned lease and fences every terminal write by its token.
    pub async fn execute_claimed(
        &self,
        request: CompleteVerificationTaskRequest,
        claim: ClaimedVerificationTask,
        worker_id: &str,
        claimer: &dyn VerificationTaskClaimer,
    ) -> Result<CompletedVerificationTask, ApplicationError> {
        if claim.task.task_id != request.task_id
            || claim.task.status != VerificationTaskStatus::InProgress
        {
            return Err(ApplicationError::InvalidVerificationTaskState {
                message: "claimed completion requires the matching in-progress task".to_owned(),
            });
        }
        let fenced_tasks = ClaimFencedTaskRepository {
            claim: claim.clone(),
            claimer,
            worker_id,
        };
        let fenced_entitlements = ClaimFencedEntitlementRepository {
            inner: self.entitlements,
            claim: &claim,
            claimer,
            worker_id,
        };
        CompleteVerificationTaskUseCase::new(
            &fenced_tasks,
            self.content_locks,
            &fenced_entitlements,
            self.verifiers,
            self.clock,
            self.verified_by.clone(),
        )
        .execute(request)
        .await
    }

    async fn verify_criteria(
        &self,
        task: &VerificationTaskRecord,
        content_lock: &ContentLock,
    ) -> Result<VerificationResult, ApplicationError> {
        let verified_at = self.clock.now();
        let submitted = &task.submitted_proof_bundle;
        let mut criteria = Vec::new();

        for criterion in &content_lock.criteria {
            let Some(proof) = submitted.proofs.iter().find(|proof| {
                proof.criterion_id == criterion.criterion_id
                    && proof.verifier_type == criterion.verifier_type
            }) else {
                continue;
            };
            let verifier = self.verifiers.verifier_for(criterion.verifier_type).ok_or(
                ApplicationError::UnsupportedVerifierType {
                    verifier_type: criterion.verifier_type,
                },
            )?;
            criteria.push(
                verifier
                    .verify(CriterionVerificationRequest {
                        bundle_id: submitted.bundle_id.clone(),
                        creator: submitted.pubky_lock_resource.creator().clone(),
                        lock_id: submitted.pubky_lock_resource.lock_id().clone(),
                        criterion: criterion.clone(),
                        proof: proof.clone(),
                        verified_by: self.verified_by.clone(),
                        verified_at,
                    })
                    .await?,
            );
        }

        Ok(VerificationResult { criteria })
    }

    async fn persist_failed_task(
        &self,
        task: VerificationTaskRecord,
        failure_message: String,
    ) -> Result<(), ApplicationError> {
        let in_progress = match task.status {
            VerificationTaskStatus::Pending => {
                let in_progress =
                    task.transition_to(VerificationTaskStatus::InProgress, self.clock.now(), None)?;
                self.tasks
                    .update_verification_task(in_progress.clone())
                    .await?;
                in_progress
            }
            VerificationTaskStatus::InProgress => task,
            _ => task,
        };
        let failed = in_progress.transition_to(
            VerificationTaskStatus::Failed,
            self.clock.now(),
            Some(failure_message),
        )?;
        self.tasks.update_verification_task(failed).await
    }
}

fn viewer_safe_failure_message(error: &ApplicationError) -> &'static str {
    match error {
        ApplicationError::ContentLockUnavailable => "content lock unavailable",
        ApplicationError::EntitlementNotSatisfied => "entitlement not satisfied",
        ApplicationError::UnsupportedVerifierType { .. } | ApplicationError::Verifier { .. } => {
            "verification failed"
        }
        ApplicationError::ContentLockHashMismatch { .. }
        | ApplicationError::ContentLockCanonicalization { .. } => "content lock invalid",
        ApplicationError::EmptyContentLockCriteria
        | ApplicationError::DuplicateContentLockCriterion { .. }
        | ApplicationError::DuplicateVerificationResultCriterion { .. }
        | ApplicationError::UnknownVerificationResultCriterion { .. } => {
            "entitlement not satisfied"
        }
        ApplicationError::Storage { .. }
        | ApplicationError::DuplicateRecord { .. }
        | ApplicationError::ContentLockPathConflict { .. }
        | ApplicationError::ContentLockDeletionInProgress
        | ApplicationError::InvalidContentLockDeletionState { .. }
        | ApplicationError::MissingRecord { .. }
        | ApplicationError::InvalidVerificationTaskTransition { .. }
        | ApplicationError::VerificationPending
        | ApplicationError::VerificationDependencyUnavailable
        | ApplicationError::InvalidVerificationTaskState { .. }
        | ApplicationError::VerificationTaskClaimLost
        | ApplicationError::InvalidVerificationTaskFailureMessage
        | ApplicationError::VerificationTaskConflict
        | ApplicationError::RateLimited
        | ApplicationError::UnsupportedCredentialTtl { .. }
        | ApplicationError::CredentialGeneration { .. }
        | ApplicationError::FinalCredentialSecret { .. }
        | ApplicationError::CreatorAuthorityUnavailable
        | ApplicationError::CreatorAuthoritySecret { .. }
        | ApplicationError::InvalidCreatorAuthorityAuthKind { .. }
        | ApplicationError::CreatorConnectFlowUnavailable
        | ApplicationError::CreatorConnectFlowExpired
        | ApplicationError::FrontendSessionCodeUnavailable
        | ApplicationError::FrontendSessionCodeExpired
        | ApplicationError::FrontendSessionCodeAlreadyConsumed
        | ApplicationError::FrontendSessionUnavailable
        | ApplicationError::FrontendSessionExpired
        | ApplicationError::FrontendSessionStateMismatch
        | ApplicationError::InvalidPaykitPaymentSubmission
        | ApplicationError::EntitlementNotFound
        | ApplicationError::GuardedResourceUnavailable
        | ApplicationError::InvalidGuardedResource { .. }
        | ApplicationError::InvalidAccessCredential
        | ApplicationError::ExpiredAccessCredential => "verification failed",
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use serde_json::json;
    use time::OffsetDateTime;
    use time::macros::datetime;

    use locks_core::ids::{
        BundleId, ContentLockPath, CreatorPubky, GuardedResourceHash, LockServerPubky,
        PubkyLockResource, TaskId,
    };
    use locks_core::lock_policy::{
        AccessPolicy, CONTENT_LOCK_VERSION, ContentLock, Criterion, GuardedResource, LockLogic,
        LockServerConfig, VerifierType,
    };
    use locks_core::verification::{
        CriterionVerificationResult, Proof, SUBMITTED_PROOF_BUNDLE_VERSION, SubmittedProofBundle,
        VerifiedProofBundle,
    };

    use super::{CompleteVerificationTaskRequest, CompleteVerificationTaskUseCase};
    use crate::application::errors::ApplicationError;
    use crate::application::models::{
        ClaimedVerificationTask, CriterionVerificationRequest, VerificationTaskRecord,
        VerificationTaskStatus,
    };
    use crate::application::ports::{
        Clock, ContentLockRepository, CriterionVerifier, CriterionVerifierRegistry,
        EntitlementRepository, VerificationTaskClaimer, VerificationTaskRepository,
    };
    use crate::infrastructure::memory::verification_task_claims::InMemoryVerificationTaskClaimer;

    const TASK_ID: &str = "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10";
    const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";

    #[tokio::test]
    async fn claimed_completion_rejects_lost_lease_after_entitlement_publication() {
        let content_lock = content_lock_fixture(true);
        let in_progress = pending_task_for(&content_lock)
            .transition_to(
                VerificationTaskStatus::InProgress,
                datetime!(2026-05-29 12:00:30 UTC),
                None,
            )
            .unwrap();
        let claim = ClaimedVerificationTask {
            task: in_progress,
            claim_token: uuid::Uuid::new_v4(),
        };
        let tasks = FakeTasks::new(None);
        let content_locks = FakeContentLocks::new(Some(content_lock));
        let entitlements = FakeEntitlements::default();
        let verifier = FakeVerifier {
            satisfied: true,
            error: None,
        };
        let registry = FakeRegistry {
            verifier: Some(&verifier),
        };
        let claimer = LostClaimClaimer;
        let clock = SequenceClock::new(vec![
            datetime!(2026-05-29 12:01:00 UTC),
            datetime!(2026-05-29 12:02:00 UTC),
            datetime!(2026-05-29 12:03:00 UTC),
            datetime!(2026-05-29 12:04:00 UTC),
        ]);
        let use_case = CompleteVerificationTaskUseCase::new(
            &tasks,
            &content_locks,
            &entitlements,
            &registry,
            &clock,
            lock_server(),
        );

        let result = use_case
            .execute_claimed(
                CompleteVerificationTaskRequest { task_id: task_id() },
                claim,
                "worker-a",
                &claimer,
            )
            .await;

        assert_eq!(result, Err(ApplicationError::VerificationTaskClaimLost));
        assert_eq!(entitlements.stored().len(), 1);
        assert!(tasks.updates().is_empty());
    }

    #[tokio::test]
    async fn complete_verification_task_accepts_equivalent_existing_entitlement() {
        let content_lock = content_lock_fixture(true);
        let content_locks = FakeContentLocks::new(Some(content_lock.clone()));
        let entitlements = FakeEntitlements::default();
        let verifier = FakeVerifier {
            satisfied: true,
            error: None,
        };
        let registry = FakeRegistry {
            verifier: Some(&verifier),
        };

        for attempt in 0..2 {
            let tasks = FakeTasks::new(Some(pending_task_for(&content_lock)));
            let verified_at = if attempt == 0 {
                datetime!(2026-05-29 12:01:00 UTC)
            } else {
                datetime!(2026-05-29 12:04:00 UTC)
            };
            let clock = SequenceClock::new(vec![
                verified_at,
                datetime!(2026-05-29 12:02:00 UTC),
                datetime!(2026-05-29 12:03:00 UTC),
            ]);
            let completed = CompleteVerificationTaskUseCase::new(
                &tasks,
                &content_locks,
                &entitlements,
                &registry,
                &clock,
                lock_server(),
            )
            .execute(CompleteVerificationTaskRequest { task_id: task_id() })
            .await
            .expect("equivalent existing entitlement is idempotent");

            assert_eq!(completed.status, VerificationTaskStatus::Completed);
        }

        assert_eq!(entitlements.stored().len(), 1);
    }

    #[tokio::test]
    async fn complete_verification_task_recovers_after_ambiguous_entitlement_write_error() {
        let content_lock = content_lock_fixture(true);
        let tasks = FakeTasks::new(Some(pending_task_for(&content_lock)));
        let content_locks = FakeContentLocks::new(Some(content_lock));
        let entitlements = FakeEntitlements::with_ambiguous_write_failure();
        let verifier = FakeVerifier {
            satisfied: true,
            error: None,
        };
        let registry = FakeRegistry {
            verifier: Some(&verifier),
        };
        let clock = SequenceClock::new(vec![
            datetime!(2026-05-29 12:01:00 UTC),
            datetime!(2026-05-29 12:02:00 UTC),
            datetime!(2026-05-29 12:03:00 UTC),
        ]);
        let use_case = CompleteVerificationTaskUseCase::new(
            &tasks,
            &content_locks,
            &entitlements,
            &registry,
            &clock,
            lock_server(),
        );

        let completed = use_case
            .execute(CompleteVerificationTaskRequest { task_id: task_id() })
            .await
            .unwrap();

        assert_eq!(completed.status, VerificationTaskStatus::Completed);
        assert_eq!(entitlements.stored().len(), 1);
        assert_eq!(
            tasks.updates().last().unwrap().status,
            VerificationTaskStatus::Completed
        );
    }

    #[tokio::test]
    async fn ambiguous_entitlement_write_without_read_back_stays_pending() {
        let content_lock = content_lock_fixture(true);
        let tasks = FakeTasks::new(Some(pending_task_for(&content_lock)));
        let content_locks = FakeContentLocks::new(Some(content_lock));
        let entitlements = FakeEntitlements::with_ambiguous_write_and_absent_read_back();
        let verifier = FakeVerifier {
            satisfied: true,
            error: None,
        };
        let registry = FakeRegistry {
            verifier: Some(&verifier),
        };
        let clock = SequenceClock::new(vec![
            datetime!(2026-05-29 12:01:00 UTC),
            datetime!(2026-05-29 12:02:00 UTC),
        ]);

        let result = CompleteVerificationTaskUseCase::new(
            &tasks,
            &content_locks,
            &entitlements,
            &registry,
            &clock,
            lock_server(),
        )
        .execute(CompleteVerificationTaskRequest { task_id: task_id() })
        .await;

        assert_eq!(result, Err(ApplicationError::VerificationPending));
        assert!(
            !tasks
                .updates()
                .iter()
                .any(|task| task.status == VerificationTaskStatus::Failed)
        );
    }

    #[tokio::test]
    async fn ambiguous_entitlement_write_with_failed_read_back_stays_pending() {
        let content_lock = content_lock_fixture(true);
        let tasks = FakeTasks::new(Some(pending_task_for(&content_lock)));
        let content_locks = FakeContentLocks::new(Some(content_lock));
        let entitlements = FakeEntitlements::with_ambiguous_write_and_failed_read_back();
        let verifier = FakeVerifier {
            satisfied: true,
            error: None,
        };
        let registry = FakeRegistry {
            verifier: Some(&verifier),
        };
        let clock = SequenceClock::new(vec![
            datetime!(2026-05-29 12:01:00 UTC),
            datetime!(2026-05-29 12:02:00 UTC),
        ]);

        let result = CompleteVerificationTaskUseCase::new(
            &tasks,
            &content_locks,
            &entitlements,
            &registry,
            &clock,
            lock_server(),
        )
        .execute(CompleteVerificationTaskRequest { task_id: task_id() })
        .await;

        assert_eq!(result, Err(ApplicationError::VerificationPending));
        assert!(
            !tasks
                .updates()
                .iter()
                .any(|task| task.status == VerificationTaskStatus::Failed)
        );
    }

    #[tokio::test]
    async fn current_claim_owner_completes_after_equivalent_entitlement_was_published() {
        let content_lock = content_lock_fixture(true);
        let content_locks = FakeContentLocks::new(Some(content_lock.clone()));
        let entitlements = FakeEntitlements::default();
        let verifier = FakeVerifier {
            satisfied: true,
            error: None,
        };
        let registry = FakeRegistry {
            verifier: Some(&verifier),
        };
        let initial_tasks = FakeTasks::new(Some(pending_task_for(&content_lock)));
        let initial_clock = SequenceClock::new(vec![
            datetime!(2026-05-29 12:01:00 UTC),
            datetime!(2026-05-29 12:02:00 UTC),
            datetime!(2026-05-29 12:03:00 UTC),
        ]);
        CompleteVerificationTaskUseCase::new(
            &initial_tasks,
            &content_locks,
            &entitlements,
            &registry,
            &initial_clock,
            lock_server(),
        )
        .execute(CompleteVerificationTaskRequest { task_id: task_id() })
        .await
        .unwrap();

        let claimer = InMemoryVerificationTaskClaimer::new(vec![pending_task_for(&content_lock)]);
        let claim = claimer
            .claim_next_verification_task(
                "worker-a",
                (datetime!(2026-05-29 13:00:00 UTC)) - (datetime!(2026-05-29 12:04:00 UTC)),
            )
            .await
            .unwrap()
            .unwrap();
        let tasks = FakeTasks::new(None);
        let retry_clock = SequenceClock::new(vec![
            datetime!(2026-05-29 12:05:00 UTC),
            datetime!(2026-05-29 12:06:00 UTC),
            datetime!(2026-05-29 12:07:00 UTC),
            datetime!(2026-05-29 12:08:00 UTC),
        ]);
        let completed = CompleteVerificationTaskUseCase::new(
            &tasks,
            &content_locks,
            &entitlements,
            &registry,
            &retry_clock,
            lock_server(),
        )
        .execute_claimed(
            CompleteVerificationTaskRequest { task_id: task_id() },
            claim,
            "worker-a",
            &claimer,
        )
        .await
        .unwrap();

        assert_eq!(completed.status, VerificationTaskStatus::Completed);
        assert_eq!(entitlements.stored().len(), 1);
        assert!(tasks.updates().is_empty());
    }

    #[tokio::test]
    async fn complete_verification_task_rejects_mismatched_existing_entitlement() {
        let content_lock = content_lock_fixture(true);
        let content_locks = FakeContentLocks::new(Some(content_lock.clone()));
        let entitlements = FakeEntitlements::default();
        let verifier = FakeVerifier {
            satisfied: true,
            error: None,
        };
        let registry = FakeRegistry {
            verifier: Some(&verifier),
        };
        let initial_tasks = FakeTasks::new(Some(pending_task_for(&content_lock)));
        let initial_clock = SequenceClock::new(vec![
            datetime!(2026-05-29 12:01:00 UTC),
            datetime!(2026-05-29 12:02:00 UTC),
            datetime!(2026-05-29 12:03:00 UTC),
        ]);
        CompleteVerificationTaskUseCase::new(
            &initial_tasks,
            &content_locks,
            &entitlements,
            &registry,
            &initial_clock,
            lock_server(),
        )
        .execute(CompleteVerificationTaskRequest { task_id: task_id() })
        .await
        .unwrap();
        entitlements.mutate_stored(|stored| {
            stored.verification_result.criteria[0].satisfied = false;
        });

        let retry_tasks = FakeTasks::new(Some(pending_task_for(&content_lock)));
        let retry_clock = SequenceClock::new(vec![
            datetime!(2026-05-29 12:04:00 UTC),
            datetime!(2026-05-29 12:05:00 UTC),
            datetime!(2026-05-29 12:06:00 UTC),
        ]);
        let result = CompleteVerificationTaskUseCase::new(
            &retry_tasks,
            &content_locks,
            &entitlements,
            &registry,
            &retry_clock,
            lock_server(),
        )
        .execute(CompleteVerificationTaskRequest { task_id: task_id() })
        .await;

        assert_eq!(
            result,
            Err(ApplicationError::DuplicateRecord {
                record: "verified_proof_bundle"
            })
        );
        let updates = retry_tasks.updates();
        let stored = updates.last().unwrap();
        assert_eq!(stored.status, VerificationTaskStatus::InProgress);
        assert_eq!(stored.failure_message, None);
    }

    #[tokio::test]
    async fn complete_verification_task_stores_entitlement_and_marks_completed() {
        let content_lock = content_lock_fixture(true);
        let tasks = FakeTasks::new(Some(pending_task_for(&content_lock)));
        let content_locks = FakeContentLocks::new(Some(content_lock.clone()));
        let entitlements = FakeEntitlements::default();
        let verifier = FakeVerifier {
            satisfied: true,
            error: None,
        };
        let registry = FakeRegistry {
            verifier: Some(&verifier),
        };
        let clock = SequenceClock::new(vec![
            datetime!(2026-05-29 12:01:00 UTC),
            datetime!(2026-05-29 12:02:00 UTC),
            datetime!(2026-05-29 12:03:00 UTC),
        ]);
        let use_case = CompleteVerificationTaskUseCase::new(
            &tasks,
            &content_locks,
            &entitlements,
            &registry,
            &clock,
            lock_server(),
        );

        let completed = use_case
            .execute(CompleteVerificationTaskRequest { task_id: task_id() })
            .await
            .unwrap();

        assert_eq!(completed.task_id, task_id());
        assert_eq!(completed.status, VerificationTaskStatus::Completed);
        assert_eq!(completed.completed_at, datetime!(2026-05-29 12:03:00 UTC));
        let updates = tasks.updates();
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0].status, VerificationTaskStatus::InProgress);
        assert_eq!(
            updates[0].started_at,
            Some(datetime!(2026-05-29 12:02:00 UTC))
        );
        assert_eq!(updates[1].status, VerificationTaskStatus::Completed);
        assert_eq!(
            updates[1].completed_at,
            Some(datetime!(2026-05-29 12:03:00 UTC))
        );
        let stored = entitlements.stored();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].bundle_id, bundle_id());
        assert_eq!(
            stored[0].pubky_lock_resource,
            pubky_lock_resource_for(&content_lock)
        );
        assert_eq!(stored[0].verification_result.criteria.len(), 1);
        assert!(stored[0].verification_result.criteria[0].satisfied);
    }

    #[tokio::test]
    async fn complete_verification_task_returns_missing_record_when_task_absent() {
        let tasks = FakeTasks::new(None);
        let content_locks = FakeContentLocks::new(None);
        let entitlements = FakeEntitlements::default();
        let registry = FakeRegistry { verifier: None };
        let clock = SequenceClock::new(vec![datetime!(2026-05-29 12:00:00 UTC)]);
        let use_case = CompleteVerificationTaskUseCase::new(
            &tasks,
            &content_locks,
            &entitlements,
            &registry,
            &clock,
            lock_server(),
        );

        let result = use_case
            .execute(CompleteVerificationTaskRequest { task_id: task_id() })
            .await;

        assert_eq!(
            result,
            Err(ApplicationError::MissingRecord {
                record: "verification_task"
            })
        );
        assert!(tasks.updates().is_empty());
        assert!(entitlements.stored().is_empty());
    }

    #[tokio::test]
    async fn complete_verification_task_marks_failed_when_entitlement_not_satisfied() {
        let content_lock = content_lock_fixture(false);
        let tasks = FakeTasks::new(Some(pending_task_for(&content_lock)));
        let content_locks = FakeContentLocks::new(Some(content_lock));
        let entitlements = FakeEntitlements::default();
        let verifier = FakeVerifier {
            satisfied: false,
            error: None,
        };
        let registry = FakeRegistry {
            verifier: Some(&verifier),
        };
        let clock = SequenceClock::new(vec![
            datetime!(2026-05-29 12:01:00 UTC),
            datetime!(2026-05-29 12:02:00 UTC),
            datetime!(2026-05-29 12:03:00 UTC),
            datetime!(2026-05-29 12:04:00 UTC),
        ]);
        let use_case = CompleteVerificationTaskUseCase::new(
            &tasks,
            &content_locks,
            &entitlements,
            &registry,
            &clock,
            lock_server(),
        );

        let result = use_case
            .execute(CompleteVerificationTaskRequest { task_id: task_id() })
            .await;

        assert_eq!(result, Err(ApplicationError::EntitlementNotSatisfied));
        assert!(entitlements.stored().is_empty());
        let updates = tasks.updates();
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0].status, VerificationTaskStatus::InProgress);
        assert_eq!(updates[1].status, VerificationTaskStatus::Failed);
        assert_eq!(
            updates[1].failure_message.as_deref(),
            Some("entitlement not satisfied")
        );
    }

    #[tokio::test]
    async fn complete_verification_task_marks_failed_when_verifier_type_is_unregistered() {
        let content_lock = content_lock_fixture(true);
        let tasks = FakeTasks::new(Some(pending_task_for(&content_lock)));
        let content_locks = FakeContentLocks::new(Some(content_lock));
        let entitlements = FakeEntitlements::default();
        let registry = FakeRegistry { verifier: None };
        let clock = SequenceClock::new(vec![
            datetime!(2026-05-29 12:01:00 UTC),
            datetime!(2026-05-29 12:02:00 UTC),
            datetime!(2026-05-29 12:03:00 UTC),
        ]);
        let use_case = CompleteVerificationTaskUseCase::new(
            &tasks,
            &content_locks,
            &entitlements,
            &registry,
            &clock,
            lock_server(),
        );

        let result = use_case
            .execute(CompleteVerificationTaskRequest { task_id: task_id() })
            .await;

        assert_eq!(
            result,
            Err(ApplicationError::UnsupportedVerifierType {
                verifier_type: VerifierType::DevStatic,
            })
        );
        assert!(entitlements.stored().is_empty());
        let updates = tasks.updates();
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0].status, VerificationTaskStatus::InProgress);
        assert_eq!(updates[1].status, VerificationTaskStatus::Failed);
    }

    #[tokio::test]
    async fn complete_verification_task_persists_viewer_safe_failure_message_for_verifier_errors() {
        let content_lock = content_lock_fixture(true);
        let tasks = FakeTasks::new(Some(pending_task_for(&content_lock)));
        let content_locks = FakeContentLocks::new(Some(content_lock));
        let entitlements = FakeEntitlements::default();
        let verifier = FakeVerifier {
            satisfied: false,
            error: Some(ApplicationError::Verifier {
                message: "raw proof token=secret stack=/srv/locks internal-worker-7".to_owned(),
            }),
        };
        let registry = FakeRegistry {
            verifier: Some(&verifier),
        };
        let clock = SequenceClock::new(vec![
            datetime!(2026-05-29 12:01:00 UTC),
            datetime!(2026-05-29 12:02:00 UTC),
            datetime!(2026-05-29 12:03:00 UTC),
        ]);
        let use_case = CompleteVerificationTaskUseCase::new(
            &tasks,
            &content_locks,
            &entitlements,
            &registry,
            &clock,
            lock_server(),
        );

        let result = use_case
            .execute(CompleteVerificationTaskRequest { task_id: task_id() })
            .await;

        assert_eq!(
            result,
            Err(ApplicationError::Verifier {
                message: "raw proof token=secret stack=/srv/locks internal-worker-7".to_owned(),
            })
        );
        let updates = tasks.updates();
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[1].status, VerificationTaskStatus::Failed);
        let failure_message = updates[1].failure_message.as_deref().unwrap();
        assert_eq!(failure_message, "verification failed");
        assert!(!failure_message.contains("secret"));
        assert!(!failure_message.contains("/srv"));
        assert!(!failure_message.contains("worker"));
    }

    #[tokio::test]
    async fn complete_verification_task_leaves_retryable_verification_pending_without_failure() {
        let content_lock = content_lock_fixture(true);
        let tasks = FakeTasks::new(Some(pending_task_for(&content_lock)));
        let content_locks = FakeContentLocks::new(Some(content_lock));
        let entitlements = FakeEntitlements::default();
        let verifier = FakeVerifier {
            satisfied: false,
            error: Some(ApplicationError::VerificationPending),
        };
        let registry = FakeRegistry {
            verifier: Some(&verifier),
        };
        let clock = SequenceClock::new(vec![datetime!(2026-05-29 12:01:00 UTC)]);
        let use_case = CompleteVerificationTaskUseCase::new(
            &tasks,
            &content_locks,
            &entitlements,
            &registry,
            &clock,
            lock_server(),
        );

        let result = use_case
            .execute(CompleteVerificationTaskRequest { task_id: task_id() })
            .await;

        assert_eq!(result, Err(ApplicationError::VerificationPending));
        assert!(tasks.updates().is_empty());
        assert!(entitlements.stored().is_empty());
    }

    #[tokio::test]
    async fn complete_verification_task_marks_failed_when_content_lock_is_missing() {
        let task_content_lock = content_lock_fixture(true);
        let tasks = FakeTasks::new(Some(pending_task_for(&task_content_lock)));
        let content_locks = FakeContentLocks::new(None);
        let entitlements = FakeEntitlements::default();
        let registry = FakeRegistry { verifier: None };
        let clock = SequenceClock::new(vec![
            datetime!(2026-05-29 12:01:00 UTC),
            datetime!(2026-05-29 12:02:00 UTC),
        ]);
        let use_case = CompleteVerificationTaskUseCase::new(
            &tasks,
            &content_locks,
            &entitlements,
            &registry,
            &clock,
            lock_server(),
        );

        let result = use_case
            .execute(CompleteVerificationTaskRequest { task_id: task_id() })
            .await;

        assert_eq!(result, Err(ApplicationError::ContentLockUnavailable));
        assert!(entitlements.stored().is_empty());
        let updates = tasks.updates();
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0].status, VerificationTaskStatus::InProgress);
        assert_eq!(updates[1].status, VerificationTaskStatus::Failed);
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

    fn lock_server() -> LockServerPubky {
        LockServerPubky::from_str("pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo")
            .unwrap()
    }

    fn pubky_lock_resource_for(content_lock: &ContentLock) -> PubkyLockResource {
        PubkyLockResource::new(creator(), content_lock.content_lock_path().unwrap())
    }

    fn pending_task_for(content_lock: &ContentLock) -> VerificationTaskRecord {
        VerificationTaskRecord {
            task_id: task_id(),
            creator: creator(),
            submitted_proof_bundle: SubmittedProofBundle {
                version: SUBMITTED_PROOF_BUNDLE_VERSION,
                bundle_id: bundle_id(),
                pubky_lock_resource: pubky_lock_resource_for(content_lock),
                reader_public_key: None,
                proofs: vec![Proof {
                    criterion_id: "criterion-1".to_owned(),
                    verifier_type: VerifierType::DevStatic,
                    payload: json!({}),
                }],
            },
            status: VerificationTaskStatus::Pending,
            submitted_at: datetime!(2026-05-29 12:00:00 UTC),
            started_at: None,
            completed_at: None,
            failure_message: None,
        }
    }

    fn content_lock_fixture(satisfied: bool) -> ContentLock {
        ContentLock {
            version: CONTENT_LOCK_VERSION,
            creator: creator(),
            primary_resource: Some(GuardedResource {
                path: "/priv/locks.app/content/resource.txt".to_owned(),
                hash: GuardedResourceHash::from_bytes([7; 32]),
                content_type: "text/plain".to_owned(),
                size: 13,
            }),
            secondary_resources: Default::default(),
            lock_logic: LockLogic::All {
                criteria: vec!["criterion-1".to_owned()],
            },
            criteria: vec![Criterion {
                criterion_id: "criterion-1".to_owned(),
                verifier_type: VerifierType::DevStatic,
                params: json!({ "satisfied": satisfied }),
            }],
            access_policy: AccessPolicy {
                requested_credential_ttl_seconds: 900,
            },
            lock_server: LockServerConfig { override_: None },
            created_at: datetime!(2026-05-29 11:00:00 UTC),
        }
    }

    struct SequenceClock {
        times: Mutex<Vec<OffsetDateTime>>,
    }

    impl SequenceClock {
        fn new(mut times: Vec<OffsetDateTime>) -> Self {
            times.reverse();
            Self {
                times: Mutex::new(times),
            }
        }
    }

    impl Clock for SequenceClock {
        fn now(&self) -> OffsetDateTime {
            self.times.lock().unwrap().pop().unwrap()
        }
    }

    struct FakeRegistry<'a> {
        verifier: Option<&'a dyn CriterionVerifier>,
    }

    impl CriterionVerifierRegistry for FakeRegistry<'_> {
        fn verifier_for(&self, verifier_type: VerifierType) -> Option<&dyn CriterionVerifier> {
            match verifier_type {
                VerifierType::DevStatic => self.verifier,
                VerifierType::PaykitPayment => None,
            }
        }
    }

    struct FakeVerifier {
        satisfied: bool,
        error: Option<ApplicationError>,
    }

    #[async_trait]
    impl CriterionVerifier for FakeVerifier {
        async fn verify(
            &self,
            request: CriterionVerificationRequest,
        ) -> Result<CriterionVerificationResult, ApplicationError> {
            if let Some(error) = &self.error {
                return Err(error.clone());
            }
            Ok(CriterionVerificationResult {
                criterion_id: request.criterion.criterion_id,
                satisfied: self.satisfied,
                verified_at: request.verified_at,
                verified_by: request.verified_by,
                verifier_type: request.criterion.verifier_type,
            })
        }
    }

    struct LostClaimClaimer;

    #[async_trait]
    impl VerificationTaskClaimer for LostClaimClaimer {
        async fn begin_claimed_entitlement_publication(
            &self,
            _task_id: &TaskId,
            _worker_id: &str,
            _claim_token: &uuid::Uuid,
        ) -> Result<bool, ApplicationError> {
            Ok(true)
        }

        async fn claim_next_verification_task(
            &self,
            _worker_id: &str,
            _claim_ttl: time::Duration,
        ) -> Result<Option<ClaimedVerificationTask>, ApplicationError> {
            unreachable!("completion must not claim tasks")
        }

        async fn schedule_verification_task_retry(
            &self,
            _task_id: &TaskId,
            _worker_id: &str,
            _claim_token: &uuid::Uuid,
            _retry_after: time::Duration,
        ) -> Result<Option<VerificationTaskRecord>, ApplicationError> {
            unreachable!("completion must not schedule retries")
        }

        async fn persist_claimed_verification_task_transition(
            &self,
            _task: VerificationTaskRecord,
            _worker_id: &str,
            _claim_token: &uuid::Uuid,
        ) -> Result<Option<VerificationTaskRecord>, ApplicationError> {
            Ok(None)
        }
    }

    struct FakeTasks {
        task: Option<VerificationTaskRecord>,
        updates: Mutex<Vec<VerificationTaskRecord>>,
    }

    impl FakeTasks {
        fn new(task: Option<VerificationTaskRecord>) -> Self {
            Self {
                task,
                updates: Mutex::new(Vec::new()),
            }
        }

        fn updates(&self) -> Vec<VerificationTaskRecord> {
            self.updates.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl VerificationTaskRepository for FakeTasks {
        async fn insert_verification_task(
            &self,
            _task: VerificationTaskRecord,
        ) -> Result<(), ApplicationError> {
            unreachable!("completion must not insert tasks")
        }

        async fn update_verification_task(
            &self,
            task: VerificationTaskRecord,
        ) -> Result<(), ApplicationError> {
            self.updates.lock().unwrap().push(task);
            Ok(())
        }

        async fn get_verification_task(
            &self,
            task_id: &TaskId,
        ) -> Result<Option<VerificationTaskRecord>, ApplicationError> {
            Ok(self.task.clone().filter(|task| &task.task_id == task_id))
        }

        async fn delete_verification_task(
            &self,
            _task_id: &TaskId,
        ) -> Result<(), ApplicationError> {
            unreachable!("completion must not delete tasks")
        }
    }

    struct FakeContentLocks {
        content_lock: Option<ContentLock>,
    }

    impl FakeContentLocks {
        fn new(content_lock: Option<ContentLock>) -> Self {
            Self { content_lock }
        }
    }

    #[async_trait]
    impl ContentLockRepository for FakeContentLocks {
        async fn upsert_content_lock(
            &self,
            _creator: CreatorPubky,
            _content_lock_path: ContentLockPath,
            _content_lock: ContentLock,
        ) -> Result<(), ApplicationError> {
            unreachable!("completion must not upsert content locks")
        }

        async fn get_content_lock(
            &self,
            _creator: &CreatorPubky,
            _content_lock_path: &ContentLockPath,
        ) -> Result<Option<ContentLock>, ApplicationError> {
            Ok(self.content_lock.clone())
        }

        async fn delete_content_lock(
            &self,
            _creator: &CreatorPubky,
            _content_lock_path: &ContentLockPath,
        ) -> Result<bool, ApplicationError> {
            unreachable!("completion must not delete content locks")
        }
    }

    #[derive(Default)]
    struct FakeEntitlements {
        stored: Mutex<Vec<VerifiedProofBundle>>,
        fail_after_store: bool,
        hide_stored: bool,
        fail_read: bool,
    }

    impl FakeEntitlements {
        fn with_ambiguous_write_failure() -> Self {
            Self {
                stored: Mutex::new(Vec::new()),
                fail_after_store: true,
                hide_stored: false,
                fail_read: false,
            }
        }

        fn with_ambiguous_write_and_absent_read_back() -> Self {
            Self {
                stored: Mutex::new(Vec::new()),
                fail_after_store: true,
                hide_stored: true,
                fail_read: false,
            }
        }

        fn with_ambiguous_write_and_failed_read_back() -> Self {
            Self {
                stored: Mutex::new(Vec::new()),
                fail_after_store: true,
                hide_stored: false,
                fail_read: true,
            }
        }

        fn stored(&self) -> Vec<VerifiedProofBundle> {
            self.stored.lock().unwrap().clone()
        }

        fn mutate_stored(&self, mutate: impl FnOnce(&mut VerifiedProofBundle)) {
            mutate(&mut self.stored.lock().unwrap()[0]);
        }
    }

    #[async_trait]
    impl EntitlementRepository for FakeEntitlements {
        async fn insert_verified_proof_bundle(
            &self,
            verified_proof_bundle: VerifiedProofBundle,
        ) -> Result<(), ApplicationError> {
            let mut stored = self.stored.lock().unwrap();
            if !stored.is_empty() {
                return Err(ApplicationError::DuplicateRecord {
                    record: "verified_proof_bundle",
                });
            }
            stored.push(verified_proof_bundle);
            if self.fail_after_store {
                return Err(ApplicationError::Storage {
                    message: "ambiguous entitlement write".to_owned(),
                });
            }
            Ok(())
        }

        async fn get_verified_proof_bundle(
            &self,
            creator: &CreatorPubky,
            bundle_id: &BundleId,
        ) -> Result<Option<VerifiedProofBundle>, ApplicationError> {
            if self.fail_read {
                return Err(ApplicationError::Storage {
                    message: "entitlement read unavailable".to_owned(),
                });
            }
            if self.hide_stored {
                return Ok(None);
            }
            Ok(self
                .stored
                .lock()
                .unwrap()
                .iter()
                .find(|bundle| {
                    bundle.pubky_lock_resource.creator() == creator
                        && &bundle.bundle_id == bundle_id
                })
                .cloned())
        }

        async fn delete_verified_proof_bundle(
            &self,
            _creator: &CreatorPubky,
            _bundle_id: &BundleId,
        ) -> Result<(), ApplicationError> {
            unreachable!("completion must not delete entitlements")
        }
    }
}
