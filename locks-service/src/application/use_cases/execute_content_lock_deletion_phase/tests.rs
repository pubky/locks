use std::{
    collections::{BTreeMap, VecDeque},
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use async_trait::async_trait;
use locks_core::{
    content_lock_deletion::ContentLockDeletionTombstone,
    ids::{ContentLockPath, CreatorPubky, GuardedResourceHash, LockId},
    lock_policy::{
        AccessPolicy, CONTENT_LOCK_VERSION, ContentLock, GuardedResource, LockLogic,
        LockServerConfig, SecondaryGuardedResource,
    },
};
use time::{Duration, OffsetDateTime, macros::datetime};
use uuid::Uuid;

use super::*;
use crate::application::{
    models::{
        AccessCredentialLookupKey, AccessCredentialRecord, ContentLockDeletionJob,
        FinalAccessWindows, GuardedResourceRecord, PrepareForceDeletionResult,
    },
    ports::{
        ContentLockDeletionActionAcquireResult, ContentLockDeletionActionClaim,
        ContentLockDeletionActionGuard, GuardedResourceReadback,
    },
};

const NOW: OffsetDateTime = datetime!(2026-08-17 12:00:00 UTC);

#[tokio::test]
async fn withdraw_exact_progresses_and_releases_guard() {
    let h = Harness::new();
    let outcome = h
        .executor()
        .execute(claim(ContentLockDeletionPhase::Withdraw), "worker")
        .await;
    assert_eq!(outcome, DeletionPhaseExecutionOutcome::Progressed);
    assert_eq!(
        h.deletions.advances(),
        vec![ContentLockDeletionPhase::StartPaymentDrain]
    );
    assert!(h.actions.released.load(Ordering::SeqCst));
}

#[tokio::test]
async fn withdraw_reclaim_preserves_replacement_after_phase_advance_failure() {
    let h = Harness::new();
    *h.deletions.advance_error.lock().unwrap() = Some(ApplicationError::Storage {
        message: "phase persistence unavailable after publication".to_owned(),
    });

    let first = h
        .executor()
        .execute(claim(ContentLockDeletionPhase::Withdraw), "worker-a")
        .await;
    assert_eq!(
        first,
        DeletionPhaseExecutionOutcome::TransientDependencyFailure
    );
    assert_eq!(h.tombstones.withdraw_count(), 1);
    assert!(h.deletions.advances().is_empty());

    h.tombstones.replace_public_bytes();

    let mut reclaimed = claim(ContentLockDeletionPhase::Withdraw);
    reclaimed.claim_token = Uuid::from_u128(3);
    let second = h.executor().execute(reclaimed, "worker-b").await;

    assert_eq!(second, DeletionPhaseExecutionOutcome::TerminalFailed);
    assert_eq!(h.tombstones.withdraw_count(), 1);
    assert!(h.tombstones.replacement_is_present());
    assert_eq!(
        h.deletions.finishes(),
        vec![ContentLockDeletionFailureCode::TombstoneReplaced]
    );
    assert_eq!(
        h.deletions.advances(),
        Vec::<ContentLockDeletionPhase>::new()
    );
}

#[tokio::test]
async fn missing_tombstone_finishes_exact_claim_with_stable_code() {
    let h = Harness::new();
    h.tombstones.set_reads([TombstoneReadback::Missing]);
    let outcome = h
        .executor()
        .execute(claim(ContentLockDeletionPhase::DeleteContent), "worker")
        .await;
    assert_eq!(outcome, DeletionPhaseExecutionOutcome::TerminalFailed);
    assert_eq!(
        h.deletions.finishes(),
        vec![ContentLockDeletionFailureCode::TombstoneMissing]
    );
    assert!(h.resources.observed().is_empty());
}

#[tokio::test]
async fn replaced_tombstone_is_terminal() {
    let h = Harness::new();
    h.tombstones.set_reads([TombstoneReadback::Replaced]);
    let outcome = h
        .executor()
        .execute(claim(ContentLockDeletionPhase::DeleteTombstone), "worker")
        .await;
    assert_eq!(outcome, DeletionPhaseExecutionOutcome::TerminalFailed);
    assert_eq!(
        h.deletions.finishes(),
        vec![ContentLockDeletionFailureCode::TombstoneReplaced]
    );
    assert_eq!(h.tombstones.read_count(), 1);
}

#[tokio::test]
async fn exact_checks_verify_every_sorted_deduplicated_content_generation() {
    let h = Harness::new();
    h.tombstones.set_reads([TombstoneReadback::Exact; 4]);
    let outcome = h
        .executor()
        .execute(claim(ContentLockDeletionPhase::DeleteContent), "worker")
        .await;
    assert_eq!(outcome, DeletionPhaseExecutionOutcome::Progressed);
    assert_eq!(
        h.resources.observed(),
        vec![
            "/priv/locks.app/content/a".to_owned(),
            "/priv/locks.app/content/m".to_owned(),
            "/priv/locks.app/content/z".to_owned(),
        ]
    );
    assert_eq!(h.tombstones.read_count(), 4);
    assert_eq!(
        h.deletions.advances(),
        vec![ContentLockDeletionPhase::DeleteTombstone]
    );
}

#[tokio::test]
async fn preexisting_guarded_resource_replacement_is_terminal_and_not_deleted() {
    let h = Harness::new();
    h.resources
        .set_outcomes([GuardedResourceReadback::Replaced]);
    h.tombstones.set_reads([TombstoneReadback::Exact]);

    let outcome = h
        .executor()
        .execute(claim(ContentLockDeletionPhase::DeleteContent), "worker")
        .await;

    assert_eq!(outcome, DeletionPhaseExecutionOutcome::TerminalFailed);
    assert_eq!(
        h.deletions.finishes(),
        vec![ContentLockDeletionFailureCode::ResourceReplaced]
    );
    assert!(h.deletions.advances().is_empty());
    assert_eq!(h.resources.observed().len(), 1);
}

#[tokio::test]
async fn final_tombstone_loss_after_content_deletion_fails_before_phase_advance() {
    let h = Harness::new();
    h.tombstones.set_reads([
        TombstoneReadback::Exact,
        TombstoneReadback::Exact,
        TombstoneReadback::Exact,
        TombstoneReadback::Missing,
    ]);
    let outcome = h
        .executor()
        .execute(claim(ContentLockDeletionPhase::DeleteContent), "worker")
        .await;

    assert_eq!(outcome, DeletionPhaseExecutionOutcome::TerminalFailed);
    assert!(h.deletions.advances().is_empty());
    assert_eq!(
        h.deletions.finishes(),
        vec![ContentLockDeletionFailureCode::TombstoneMissing]
    );
}

#[tokio::test]
async fn exact_tombstone_remains_published_at_purge_handoff() {
    let h = Harness::new();
    h.tombstones.set_reads([TombstoneReadback::Exact]);
    let outcome = h
        .executor()
        .execute(claim(ContentLockDeletionPhase::DeleteTombstone), "worker")
        .await;
    assert_eq!(outcome, DeletionPhaseExecutionOutcome::Progressed);
    assert_eq!(
        h.deletions.advances(),
        vec![ContentLockDeletionPhase::PurgeOperationalState]
    );
}

#[tokio::test]
async fn busy_action_guard_defers_without_side_effects() {
    let h = Harness::new();
    h.actions.busy.store(true, Ordering::SeqCst);
    let outcome = h
        .executor()
        .execute(claim(ContentLockDeletionPhase::Withdraw), "worker")
        .await;
    assert_eq!(outcome, DeletionPhaseExecutionOutcome::Deferred);
    assert!(h.deletions.advances().is_empty());
    assert_eq!(h.tombstones.withdraw_count(), 0);
}

#[tokio::test]
async fn action_ownership_unexpected_error_is_fatal() {
    let h = Harness::new();
    *h.actions.error.lock().unwrap() = Some(ApplicationError::MissingRecord {
        record: "deletion_action_guard",
    });

    let outcome = h
        .executor()
        .execute(claim(ContentLockDeletionPhase::Withdraw), "worker")
        .await;

    assert_eq!(outcome, DeletionPhaseExecutionOutcome::FatalFailure);
    assert_eq!(h.tombstones.withdraw_count(), 0);
}

#[tokio::test]
async fn tombstone_storage_error_is_transient_but_unexpected_error_is_fatal() {
    for (error, expected) in [
        (
            ApplicationError::Storage {
                message: "temporary homeserver outage".to_owned(),
            },
            DeletionPhaseExecutionOutcome::TransientDependencyFailure,
        ),
        (
            ApplicationError::MissingRecord {
                record: "content_lock_tombstone",
            },
            DeletionPhaseExecutionOutcome::FatalFailure,
        ),
    ] {
        let h = Harness::new();
        *h.tombstones.withdraw_error.lock().unwrap() = Some(error);

        let outcome = h
            .executor()
            .execute(claim(ContentLockDeletionPhase::Withdraw), "worker")
            .await;

        assert_eq!(outcome, expected);
    }
}

#[tokio::test]
async fn final_credential_crypto_error_is_fatal() {
    let h = Harness::new();
    *h.finals.error.lock().unwrap() = Some(ApplicationError::FinalCredentialSecret {
        message: "secret detail".to_owned(),
    });

    let outcome = h
        .executor()
        .execute(
            claim(ContentLockDeletionPhase::IssueFinalCredentials),
            "worker",
        )
        .await;

    assert_eq!(outcome, DeletionPhaseExecutionOutcome::FatalFailure);
}

#[tokio::test]
async fn invalid_state_transition_is_fatal_instead_of_business_deferral() {
    let h = Harness::new();
    *h.deletions.advance_error.lock().unwrap() =
        Some(ApplicationError::InvalidContentLockDeletionState {
            message: "invariant detail".to_owned(),
        });

    let outcome = h
        .executor()
        .execute(
            claim(ContentLockDeletionPhase::DrainExistingCredentials),
            "worker",
        )
        .await;

    assert_eq!(outcome, DeletionPhaseExecutionOutcome::FatalFailure);
}

#[tokio::test]
async fn repository_obligations_pending_is_healthy_deferral() {
    let h = Harness::new();
    h.deletions
        .obligations_pending
        .store(true, Ordering::SeqCst);

    let outcome = h
        .executor()
        .execute(
            claim(ContentLockDeletionPhase::DrainExistingCredentials),
            "worker",
        )
        .await;

    assert_eq!(outcome, DeletionPhaseExecutionOutcome::Deferred);
}

#[tokio::test]
async fn missed_final_credential_issuance_deadline_terminalizes_without_advancing() {
    let h = Harness::new();
    h.deletions
        .issuance_deadline_missed
        .store(true, Ordering::SeqCst);

    let outcome = h
        .executor()
        .execute(
            claim(ContentLockDeletionPhase::IssueFinalCredentials),
            "worker",
        )
        .await;

    assert_eq!(outcome, DeletionPhaseExecutionOutcome::TerminalFailed);
    assert_eq!(
        h.deletions.finishes(),
        vec![ContentLockDeletionFailureCode::StateCorrupt]
    );
    assert!(h.deletions.advances().is_empty());
    assert!(h.resources.observed().is_empty());
}

#[tokio::test]
async fn transient_provider_error_remains_retryable_across_payment_boundary() {
    let h = Harness::new();
    *h.payments.error.lock().unwrap() = Some(ApplicationError::Verifier {
        message: "provider detail".to_owned(),
    });

    let outcome = h
        .executor()
        .execute(claim(ContentLockDeletionPhase::DrainPayments), "worker")
        .await;

    assert_eq!(
        outcome,
        DeletionPhaseExecutionOutcome::TransientDependencyFailure
    );
}

#[tokio::test]
async fn paykit_failure_carries_only_paykit_unavailable_evidence() {
    let h = Harness::new();
    *h.payments.error.lock().unwrap() = Some(ApplicationError::Verifier {
        message: "provider detail".to_owned(),
    });

    let execution = h
        .executor()
        .execute_with_evidence(claim(ContentLockDeletionPhase::DrainPayments), "worker")
        .await;

    assert_eq!(
        execution.outcome,
        DeletionPhaseExecutionOutcome::TransientDependencyFailure
    );
    assert_eq!(
        execution
            .evidence
            .status(DeletionDependencySource::PaymentProvider),
        Some(DeletionDependencyStatus::Unavailable)
    );
    assert_eq!(
        execution
            .evidence
            .status(DeletionDependencySource::PubkyReadback),
        None
    );
    assert_eq!(
        execution
            .evidence
            .status(DeletionDependencySource::RepositoryPhaseMutation),
        None
    );
}

#[tokio::test]
async fn successful_active_paykit_drain_is_healthy_evidence_while_deferred() {
    let h = Harness::new();

    let execution = h
        .executor()
        .execute_with_evidence(claim(ContentLockDeletionPhase::DrainPayments), "worker")
        .await;

    assert_eq!(execution.outcome, DeletionPhaseExecutionOutcome::Deferred);
    assert_eq!(
        execution
            .evidence
            .status(DeletionDependencySource::PaymentProvider),
        Some(DeletionDependencyStatus::Healthy)
    );
}

#[tokio::test]
async fn pubky_success_followed_by_repository_failure_preserves_source_independence() {
    let h = Harness::new();
    *h.deletions.advance_error.lock().unwrap() = Some(ApplicationError::Storage {
        message: "local persistence unavailable".to_owned(),
    });

    let execution = h
        .executor()
        .execute_with_evidence(claim(ContentLockDeletionPhase::Withdraw), "worker")
        .await;

    assert_eq!(
        execution.outcome,
        DeletionPhaseExecutionOutcome::TransientDependencyFailure
    );
    assert_eq!(
        execution
            .evidence
            .status(DeletionDependencySource::PubkyReadback),
        None
    );
    assert_eq!(
        execution
            .evidence
            .status(DeletionDependencySource::PubkyWithdrawal),
        Some(DeletionDependencyStatus::Healthy)
    );
    assert_eq!(
        execution
            .evidence
            .status(DeletionDependencySource::RepositoryPhaseMutation),
        Some(DeletionDependencyStatus::Unavailable)
    );
    assert_eq!(
        execution
            .evidence
            .status(DeletionDependencySource::PaymentProvider),
        None
    );
}

#[tokio::test]
async fn delete_content_pubky_success_followed_by_advance_failure_preserves_stage_evidence() {
    let h = Harness::new();
    h.tombstones.set_reads([TombstoneReadback::Exact; 4]);
    *h.deletions.advance_error.lock().unwrap() = Some(ApplicationError::Storage {
        message: "phase persistence unavailable".to_owned(),
    });

    let execution = h
        .executor()
        .execute_with_evidence(claim(ContentLockDeletionPhase::DeleteContent), "worker")
        .await;

    assert_eq!(
        execution.outcome,
        DeletionPhaseExecutionOutcome::TransientDependencyFailure
    );
    assert_eq!(
        execution
            .evidence
            .status(DeletionDependencySource::PubkyReadback),
        Some(DeletionDependencyStatus::Healthy)
    );
    assert_eq!(
        execution
            .evidence
            .status(DeletionDependencySource::PubkyResource),
        Some(DeletionDependencyStatus::Healthy)
    );
    assert_eq!(
        execution
            .evidence
            .status(DeletionDependencySource::RepositoryPhaseMutation),
        Some(DeletionDependencyStatus::Unavailable)
    );
    assert_eq!(
        execution
            .evidence
            .status(DeletionDependencySource::RepositoryQueueClaim),
        None
    );
}

#[tokio::test]
async fn busy_action_guard_defers_without_dependency_health_evidence() {
    let h = Harness::new();
    h.actions.busy.store(true, Ordering::SeqCst);

    let execution = h
        .executor()
        .execute_with_evidence(claim(ContentLockDeletionPhase::Withdraw), "worker")
        .await;

    assert_eq!(execution.outcome, DeletionPhaseExecutionOutcome::Deferred);
    for source in [
        DeletionDependencySource::PaymentProvider,
        DeletionDependencySource::PubkyReadback,
        DeletionDependencySource::RepositoryQueueClaim,
        DeletionDependencySource::RepositoryPhaseMutation,
    ] {
        assert_eq!(execution.evidence.status(source), None);
    }
}

#[tokio::test]
async fn repository_none_is_claim_lost_and_guard_is_released() {
    let h = Harness::new();
    h.deletions.claim_live.store(false, Ordering::SeqCst);
    let outcome = h
        .executor()
        .execute(claim(ContentLockDeletionPhase::Withdraw), "worker")
        .await;
    assert_eq!(outcome, DeletionPhaseExecutionOutcome::ClaimLost);
    assert!(h.actions.released.load(Ordering::SeqCst));
}

#[tokio::test]
async fn purge_is_deferred_task_ten_handoff() {
    let h = Harness::new();
    let outcome = h
        .executor()
        .execute(
            claim(ContentLockDeletionPhase::PurgeOperationalState),
            "worker",
        )
        .await;
    assert_eq!(outcome, DeletionPhaseExecutionOutcome::Deferred);
    assert!(h.deletions.advances().is_empty());
    assert!(h.actions.released.load(Ordering::SeqCst));
}

#[tokio::test]
async fn operation_evidence_does_not_infer_phase_health_from_purge_or_claim_loss() {
    let h = Harness::new();
    let purge = h
        .executor()
        .execute_with_evidence(
            claim(ContentLockDeletionPhase::PurgeOperationalState),
            "worker",
        )
        .await;
    assert_eq!(purge.outcome, DeletionPhaseExecutionOutcome::Deferred);
    assert_eq!(
        purge
            .evidence
            .status(DeletionDependencySource::RepositoryPhaseMutation),
        None
    );

    h.deletions.claim_live.store(false, Ordering::SeqCst);
    let lost = h
        .executor()
        .execute_with_evidence(claim(ContentLockDeletionPhase::Withdraw), "worker")
        .await;
    assert_eq!(lost.outcome, DeletionPhaseExecutionOutcome::ClaimLost);
    assert_eq!(
        lost.evidence
            .status(DeletionDependencySource::RepositoryPhaseMutation),
        None
    );
    assert_eq!(
        lost.evidence
            .status(DeletionDependencySource::PubkyWithdrawal),
        Some(DeletionDependencyStatus::Healthy)
    );
}

struct Harness {
    deletions: FakeDeletions,
    actions: FakeActions,
    tombstones: FakeTombstones,
    resources: FakeResources,
    access: FakeAccess,
    clock: FixedClock,
    payments: FakePayments,
    finals: FakeFinals,
}

impl Harness {
    fn new() -> Self {
        Self {
            deletions: FakeDeletions::new(),
            actions: FakeActions::new(),
            tombstones: FakeTombstones::new(),
            resources: FakeResources::default(),
            access: FakeAccess,
            clock: FixedClock,
            payments: FakePayments::default(),
            finals: FakeFinals::default(),
        }
    }

    fn executor(&self) -> ContentLockDeletionPhaseExecutor<'_> {
        ContentLockDeletionPhaseExecutor::new(
            ContentLockDeletionPhaseExecutorDependencies {
                deletions: &self.deletions,
                action_ownership: &self.actions,
                tombstones: &self.tombstones,
                guarded_resources: &self.resources,
                access_credentials: &self.access,
                clock: &self.clock,
                payments: &self.payments,
                final_credentials: &self.finals,
            },
            ContentLockDeletionPhaseExecutorConfig {
                final_credential_issuance_window: Duration::minutes(15),
                final_read_window: Duration::minutes(15),
                final_credential_batch_limit: 10,
            },
        )
    }
}

#[derive(Default)]
struct FakeDeletions {
    claim_live: AtomicBool,
    obligations_pending: AtomicBool,
    issuance_deadline_missed: AtomicBool,
    advances: Mutex<Vec<ContentLockDeletionPhase>>,
    finishes: Mutex<Vec<ContentLockDeletionFailureCode>>,
    advance_error: Mutex<Option<ApplicationError>>,
}
impl FakeDeletions {
    fn new() -> Self {
        Self {
            claim_live: AtomicBool::new(true),
            ..Self::default()
        }
    }
    fn advances(&self) -> Vec<ContentLockDeletionPhase> {
        self.advances.lock().unwrap().clone()
    }
    fn finishes(&self) -> Vec<ContentLockDeletionFailureCode> {
        self.finishes.lock().unwrap().clone()
    }
    fn result(&self) -> Option<ContentLockDeletionJob> {
        self.claim_live
            .load(Ordering::SeqCst)
            .then(|| claim(ContentLockDeletionPhase::Withdraw).job)
    }
}

#[async_trait]
impl ContentLockDeletionRepository for FakeDeletions {
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
        unreachable!()
    }
    async fn schedule_retry(
        &self,
        _: Uuid,
        _: &str,
        _: Uuid,
        _: time::Duration,
    ) -> Result<Option<ContentLockDeletionJob>, ApplicationError> {
        unreachable!()
    }
    async fn defer(
        &self,
        _: Uuid,
        _: &str,
        _: Uuid,
        _: time::Duration,
    ) -> Result<Option<ContentLockDeletionJob>, ApplicationError> {
        unreachable!()
    }
    async fn advance_phase(
        &self,
        _: Uuid,
        _: &str,
        _: Uuid,
        next: ContentLockDeletionPhase,
    ) -> Result<AdvanceContentLockDeletionPhaseResult, ApplicationError> {
        if let Some(error) = self.advance_error.lock().unwrap().take() {
            return Err(error);
        }
        if self.obligations_pending.load(Ordering::SeqCst) {
            return Ok(AdvanceContentLockDeletionPhaseResult::ObligationsPending);
        }
        if self.issuance_deadline_missed.load(Ordering::SeqCst) {
            return Ok(AdvanceContentLockDeletionPhaseResult::TerminalFailure(
                ContentLockDeletionFailureCode::StateCorrupt,
            ));
        }
        self.advances.lock().unwrap().push(next);
        Ok(match self.result() {
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
        self.finishes.lock().unwrap().push(code.unwrap());
        Ok(self.result())
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

struct FakeGuard(Arc<AtomicBool>);
#[async_trait]
impl ContentLockDeletionActionGuard for FakeGuard {
    async fn release(self: Box<Self>) -> Result<(), ApplicationError> {
        self.0.store(true, Ordering::SeqCst);
        Ok(())
    }
}
struct FakeActions {
    busy: AtomicBool,
    released: Arc<AtomicBool>,
    error: Mutex<Option<ApplicationError>>,
}
impl FakeActions {
    fn new() -> Self {
        Self {
            busy: AtomicBool::new(false),
            released: Arc::new(AtomicBool::new(false)),
            error: Mutex::new(None),
        }
    }
}
#[async_trait]
impl ContentLockDeletionActionOwnership for FakeActions {
    async fn try_acquire(
        &self,
        _: ContentLockDeletionActionClaim<'_>,
    ) -> Result<ContentLockDeletionActionAcquireResult, ApplicationError> {
        if let Some(error) = self.error.lock().unwrap().take() {
            return Err(error);
        }
        if self.busy.load(Ordering::SeqCst) {
            Ok(ContentLockDeletionActionAcquireResult::Busy)
        } else {
            Ok(ContentLockDeletionActionAcquireResult::Acquired(Box::new(
                FakeGuard(self.released.clone()),
            )))
        }
    }
}

struct FakeTombstones {
    public_state: Mutex<FakePublicLockState>,
    withdraw_error: Mutex<Option<ApplicationError>>,
    reads: Mutex<VecDeque<TombstoneReadback>>,
    withdraw_count: Mutex<usize>,
    read_count: Mutex<usize>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FakePublicLockState {
    Original,
    Tombstone,
    Replacement,
}

impl FakeTombstones {
    fn new() -> Self {
        Self {
            public_state: Mutex::new(FakePublicLockState::Original),
            withdraw_error: Mutex::new(None),
            reads: Mutex::new(VecDeque::new()),
            withdraw_count: Mutex::new(0),
            read_count: Mutex::new(0),
        }
    }

    fn set_reads<const N: usize>(&self, values: [TombstoneReadback; N]) {
        *self.reads.lock().unwrap() = values.into();
    }
    fn withdraw_count(&self) -> usize {
        *self.withdraw_count.lock().unwrap()
    }
    fn replace_public_bytes(&self) {
        *self.public_state.lock().unwrap() = FakePublicLockState::Replacement;
    }
    fn replacement_is_present(&self) -> bool {
        *self.public_state.lock().unwrap() == FakePublicLockState::Replacement
    }
    fn read_count(&self) -> usize {
        *self.read_count.lock().unwrap()
    }
}
#[async_trait]
impl ContentLockTombstoneRepository for FakeTombstones {
    async fn withdraw_content_lock(
        &self,
        _: CreatorPubky,
        _: ContentLockPath,
        _: &ContentLock,
        _: &ContentLockDeletionTombstone,
    ) -> Result<TombstoneReadback, ApplicationError> {
        if let Some(error) = self.withdraw_error.lock().unwrap().take() {
            return Err(error);
        }
        let mut state = self.public_state.lock().unwrap();
        match *state {
            FakePublicLockState::Original => {
                *self.withdraw_count.lock().unwrap() += 1;
                *state = FakePublicLockState::Tombstone;
                Ok(TombstoneReadback::Exact)
            }
            FakePublicLockState::Tombstone => Ok(TombstoneReadback::Exact),
            FakePublicLockState::Replacement => Ok(TombstoneReadback::Replaced),
        }
    }
    async fn read_tombstone(
        &self,
        _: &CreatorPubky,
        _: &ContentLockPath,
        _: &ContentLockDeletionTombstone,
    ) -> Result<TombstoneReadback, ApplicationError> {
        *self.read_count.lock().unwrap() += 1;
        Ok(self.reads.lock().unwrap().pop_front().unwrap())
    }

    async fn force_delete_content_lock_and_verify_absent(
        &self,
        _: &CreatorPubky,
        _: &ContentLockPath,
    ) -> Result<(), ApplicationError> {
        unreachable!()
    }
}

#[derive(Default)]
struct FakeResources {
    observed: Mutex<Vec<String>>,
    outcomes: Mutex<VecDeque<GuardedResourceReadback>>,
}
impl FakeResources {
    fn observed(&self) -> Vec<String> {
        self.observed.lock().unwrap().clone()
    }

    fn set_outcomes(&self, outcomes: impl IntoIterator<Item = GuardedResourceReadback>) {
        *self.outcomes.lock().unwrap() = outcomes.into_iter().collect();
    }
}
#[async_trait]
impl GuardedResourceRepository for FakeResources {
    async fn upsert_guarded_resource(
        &self,
        _: GuardedResourceRecord,
    ) -> Result<(), ApplicationError> {
        unreachable!()
    }
    async fn get_guarded_resource(
        &self,
        _: &CreatorPubky,
        _: &str,
        _: &GuardedResourceHash,
    ) -> Result<Option<GuardedResourceRecord>, ApplicationError> {
        unreachable!()
    }
    async fn get_current_guarded_resource(
        &self,
        _: &CreatorPubky,
        _: &str,
    ) -> Result<Option<GuardedResourceRecord>, ApplicationError> {
        unreachable!()
    }
    async fn delete_guarded_resource(
        &self,
        _: &CreatorPubky,
        path: &str,
    ) -> Result<bool, ApplicationError> {
        self.observed.lock().unwrap().push(path.to_owned());
        Ok(false)
    }
    async fn read_guarded_resource_generation(
        &self,
        _: &CreatorPubky,
        path: &str,
        _: &GuardedResourceHash,
    ) -> Result<GuardedResourceReadback, ApplicationError> {
        self.observed.lock().unwrap().push(path.to_owned());
        Ok(self
            .outcomes
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(GuardedResourceReadback::Exact))
    }
}

struct FakeAccess;
#[async_trait]
impl AccessCredentialStore for FakeAccess {
    async fn insert_access_credential(
        &self,
        _: &LockId,
        _: AccessCredentialLookupKey,
        _: AccessCredentialRecord,
    ) -> Result<(), ApplicationError> {
        unreachable!()
    }
    async fn get_access_credential(
        &self,
        _: &AccessCredentialLookupKey,
    ) -> Result<Option<AccessCredentialRecord>, ApplicationError> {
        unreachable!()
    }
    async fn delete_access_credential(
        &self,
        _: &AccessCredentialLookupKey,
    ) -> Result<(), ApplicationError> {
        unreachable!()
    }
    async fn initialize_final_access_windows(
        &self,
        _: Uuid,
        _: &str,
        _: Uuid,
        _: Duration,
        _: Duration,
    ) -> Result<InitializeFinalAccessWindowsResult, ApplicationError> {
        Ok(InitializeFinalAccessWindowsResult::Initialized(
            FinalAccessWindows {
                issuance_started_at: NOW,
                credential_issuance_deadline: NOW + Duration::minutes(15),
                read_deadline: NOW + Duration::minutes(30),
            },
        ))
    }
}
struct FixedClock;
impl Clock for FixedClock {
    fn now(&self) -> OffsetDateTime {
        NOW
    }
}
#[derive(Default)]
struct FakePayments {
    error: Mutex<Option<ApplicationError>>,
}
#[async_trait]
impl ContentLockPaymentDrainExecutor for FakePayments {
    async fn execute_claimed(
        &self,
        _: ClaimedContentLockDeletionJob,
        _: &str,
    ) -> DeletionPhaseExecution {
        if let Some(error) = self.error.lock().unwrap().take() {
            return error_execution(&error, DeletionDependencySource::PaymentProvider);
        }
        DeletionPhaseExecution::new(DeletionPhaseExecutionOutcome::Deferred).with_evidence(
            DeletionDependencyEvidence::healthy(DeletionDependencySource::PaymentProvider),
        )
    }
}
#[derive(Default)]
struct FakeFinals {
    error: Mutex<Option<ApplicationError>>,
}
#[async_trait]
impl FinalCredentialMaterializer for FakeFinals {
    async fn materialize(
        &self,
        _: MaterializeFinalCredentialsRequest<'_>,
    ) -> Result<MaterializeFinalCredentialsOutcome, ApplicationError> {
        if let Some(error) = self.error.lock().unwrap().take() {
            return Err(error);
        }
        Ok(MaterializeFinalCredentialsOutcome {
            materialized_count: 0,
        })
    }
}

fn claim(phase: ContentLockDeletionPhase) -> ClaimedContentLockDeletionJob {
    let mut secondary_resources = BTreeMap::new();
    for path in [
        "/priv/locks.app/content/z",
        "/priv/locks.app/content/a",
        "/priv/locks.app/content/m",
    ] {
        secondary_resources.insert(
            path.to_owned(),
            SecondaryGuardedResource {
                hash: GuardedResourceHash::from_bytes([7; 32]),
                content_type: "text/plain".to_owned(),
                size: 1,
            },
        );
    }
    let lock = ContentLock {
        version: CONTENT_LOCK_VERSION,
        creator: CreatorPubky::from_str(
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
        )
        .unwrap(),
        primary_resource: Some(
            GuardedResource::new(
                "/priv/locks.app/content/m",
                GuardedResourceHash::from_bytes([8; 32]),
                "text/plain",
                1,
            )
            .unwrap(),
        ),
        secondary_resources,
        criteria: vec![],
        lock_logic: LockLogic::All { criteria: vec![] },
        access_policy: AccessPolicy {
            requested_credential_ttl_seconds: 900,
        },
        lock_server: LockServerConfig { override_: None },
        created_at: NOW,
    };
    let mut job = ContentLockDeletionJob::new(Uuid::from_u128(1), lock, NOW).unwrap();
    job.phase = phase;
    ClaimedContentLockDeletionJob {
        job,
        claim_token: Uuid::from_u128(2),
    }
}
