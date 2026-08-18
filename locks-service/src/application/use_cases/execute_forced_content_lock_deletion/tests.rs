use std::{
    collections::HashSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use async_trait::async_trait;
use locks_core::{
    content_lock_deletion::ContentLockDeletionTombstone,
    ids::{ContentLockPath, CreatorPubky, GuardedResourceHash, LockId},
    lock_policy::ContentLock,
};
use time::{OffsetDateTime, macros::datetime};
use uuid::Uuid;

use super::*;
use crate::application::use_cases::deletion_test_fixtures::claimed_deletion_job;
use crate::application::use_cases::execute_content_lock_deletion_phase::{
    DeletionDependencyEvidence, DeletionDependencySource, DeletionDependencyStatus,
};
use crate::application::{
    errors::ApplicationError,
    models::{
        AdvanceContentLockDeletionPhaseResult, ClaimedContentLockDeletionJob,
        ContentLockDeletionFailureCode, ContentLockDeletionJob, ContentLockDeletionPhase,
        GuardedResourceRecord, PrepareForceDeletionResult,
    },
    ports::{
        ContentLockDeletionActionGuard, ContentLockDeletionActionOwnership,
        ContentLockDeletionRepository, ContentLockTombstoneRepository, GuardedResourceRepository,
        TombstoneReadback,
    },
};

const NOW: OffsetDateTime = datetime!(2026-08-17 12:00:00 UTC);

#[test]
fn outcomes_have_closed_path_free_debug_output() {
    assert_eq!(
        format!("{:?}", ForcedContentLockDeletionOutcome::Completed),
        "Completed"
    );
    assert_eq!(
        format!("{:?}", ForcedContentLockDeletionOutcome::Deferred),
        "Deferred"
    );
    assert_eq!(
        format!("{:?}", ForcedContentLockDeletionOutcome::ClaimLost),
        "ClaimLost"
    );
    assert_eq!(
        format!(
            "{:?}",
            ForcedContentLockDeletionOutcome::TransientDependencyFailure
        ),
        "TransientDependencyFailure"
    );
    assert_eq!(
        format!("{:?}", ForcedContentLockDeletionOutcome::FatalFailure),
        "FatalFailure"
    );
}

#[tokio::test]
async fn deletes_public_path_first_then_attempts_sorted_deduplicated_resources_and_completes() {
    let h = Harness::new();

    let outcome = h.execute().await;

    assert_eq!(outcome, ForcedContentLockDeletionOutcome::Completed);
    assert_eq!(
        h.operations(),
        vec![
            "public".to_owned(),
            "resource:/priv/locks.app/content/a".to_owned(),
            "resource:/priv/locks.app/content/m".to_owned(),
            "resource:/priv/locks.app/content/z".to_owned(),
            "complete".to_owned(),
            "release".to_owned(),
        ]
    );
}

#[tokio::test]
async fn resource_errors_are_best_effort_and_still_complete_the_force_receipt() {
    let h = Harness::new();
    h.resources
        .fail_paths
        .lock()
        .unwrap()
        .insert("/priv/locks.app/content/m".to_owned());

    let outcome = h.execute().await;

    assert_eq!(outcome, ForcedContentLockDeletionOutcome::Completed);
    assert_eq!(h.deletions.complete_calls.load(Ordering::SeqCst), 1);
    assert!(
        h.operations()
            .iter()
            .any(|op| op == "resource:/priv/locks.app/content/z")
    );
}

#[tokio::test]
async fn stale_claim_cannot_complete_after_all_external_attempts() {
    let h = Harness::new();
    h.deletions.claim_live.store(false, Ordering::SeqCst);

    let outcome = h.execute().await;

    assert_eq!(outcome, ForcedContentLockDeletionOutcome::ClaimLost);
    assert_eq!(h.deletions.complete_calls.load(Ordering::SeqCst), 1);
    assert_eq!(h.operations().last().map(String::as_str), Some("release"));
}

#[tokio::test]
async fn reclaimed_force_claim_replays_deletions_after_receipt_persistence_failure() {
    let h = Harness::new();
    h.deletions.fail_complete_once.store(true, Ordering::SeqCst);

    let first = h.use_case().execute(force_claim(), "worker-a").await;
    assert_eq!(
        first,
        ForcedContentLockDeletionOutcome::TransientDependencyFailure
    );
    assert_eq!(h.deletions.complete_calls.load(Ordering::SeqCst), 1);

    let mut reclaimed = force_claim();
    reclaimed.claim_token = Uuid::from_u128(3);
    let second = h.use_case().execute(reclaimed, "worker-b").await;

    assert_eq!(second, ForcedContentLockDeletionOutcome::Completed);
    assert_eq!(h.deletions.complete_calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        h.operations()
            .iter()
            .filter(|operation| operation.as_str() == "public")
            .count(),
        2
    );
}

#[tokio::test]
async fn busy_guard_defers_without_effects() {
    let h = Harness::new();
    h.actions.busy.store(true, Ordering::SeqCst);

    let outcome = h.execute().await;

    assert_eq!(outcome, ForcedContentLockDeletionOutcome::Deferred);
    assert!(h.operations().is_empty());
    assert_eq!(h.deletions.complete_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn post_lock_stale_force_claim_is_lost_without_external_action() {
    let h = Harness::new();
    h.actions.claim_live.store(false, Ordering::SeqCst);
    let outcome = h.execute().await;
    assert_eq!(outcome, ForcedContentLockDeletionOutcome::ClaimLost);
    assert!(h.operations().is_empty());
    assert_eq!(h.deletions.complete_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn public_absence_failure_stops_before_resources_and_completion() {
    let h = Harness::new();
    h.tombstones.fail.store(true, Ordering::SeqCst);

    let outcome = h.execute().await;

    assert_eq!(
        outcome,
        ForcedContentLockDeletionOutcome::TransientDependencyFailure
    );
    assert_eq!(
        h.operations(),
        vec!["public".to_owned(), "release".to_owned()]
    );
    assert_eq!(h.deletions.complete_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn busy_guard_has_no_healthy_dependency_evidence() {
    let h = Harness::new();
    h.actions.busy.store(true, Ordering::SeqCst);

    let execution = h.execute_with_evidence().await;

    assert_eq!(
        execution.outcome,
        ForcedContentLockDeletionOutcome::Deferred
    );
    assert_eq!(execution.evidence, DeletionDependencyEvidence::none());
}

#[tokio::test]
async fn force_receipt_failure_reports_pubky_healthy_and_repository_mutation_unavailable() {
    let h = Harness::new();
    h.deletions.fail_complete_once.store(true, Ordering::SeqCst);

    let execution = h.execute_with_evidence().await;

    assert_eq!(
        execution.outcome,
        ForcedContentLockDeletionOutcome::TransientDependencyFailure
    );
    assert_eq!(
        execution
            .evidence
            .status(DeletionDependencySource::PubkyForcePublic),
        Some(DeletionDependencyStatus::Healthy)
    );
    assert_eq!(
        execution
            .evidence
            .status(DeletionDependencySource::RepositoryForceReceipt),
        Some(DeletionDependencyStatus::Unavailable)
    );
}

#[tokio::test]
async fn force_pubky_failure_does_not_degrade_repository_mutation() {
    let h = Harness::new();
    h.tombstones.fail.store(true, Ordering::SeqCst);

    let execution = h.execute_with_evidence().await;

    assert_eq!(
        execution
            .evidence
            .status(DeletionDependencySource::PubkyForcePublic),
        Some(DeletionDependencyStatus::Unavailable)
    );
    assert_ne!(
        execution
            .evidence
            .status(DeletionDependencySource::RepositoryForceReceipt),
        Some(DeletionDependencyStatus::Unavailable)
    );
}

#[tokio::test]
async fn stale_force_receipt_and_guard_release_do_not_infer_force_receipt_health() {
    let h = Harness::new();
    h.deletions.claim_live.store(false, Ordering::SeqCst);

    let execution = h.execute_with_evidence().await;

    assert_eq!(
        execution.outcome,
        ForcedContentLockDeletionOutcome::ClaimLost
    );
    assert_eq!(
        execution
            .evidence
            .status(DeletionDependencySource::RepositoryForceReceipt),
        None
    );
    assert_eq!(
        execution
            .evidence
            .status(DeletionDependencySource::RepositoryActionLockRelease),
        Some(DeletionDependencyStatus::Healthy)
    );
}

struct Harness {
    operations: Arc<Mutex<Vec<String>>>,
    deletions: FakeDeletions,
    actions: FakeActions,
    tombstones: FakeTombstones,
    resources: FakeResources,
    clock: FixedClock,
}

impl Harness {
    fn new() -> Self {
        let operations = Arc::new(Mutex::new(Vec::new()));
        Self {
            deletions: FakeDeletions::new(Arc::clone(&operations)),
            actions: FakeActions::new(Arc::clone(&operations)),
            tombstones: FakeTombstones::new(Arc::clone(&operations)),
            resources: FakeResources::new(Arc::clone(&operations)),
            clock: FixedClock,
            operations,
        }
    }

    async fn execute(&self) -> ForcedContentLockDeletionOutcome {
        self.use_case().execute(force_claim(), "worker").await
    }

    async fn execute_with_evidence(&self) -> ForcedContentLockDeletionExecution {
        self.use_case()
            .execute_with_evidence(force_claim(), "worker")
            .await
    }

    fn use_case(&self) -> ExecuteForcedContentLockDeletionUseCase<'_> {
        ExecuteForcedContentLockDeletionUseCase::new(ExecuteForcedContentLockDeletionDependencies {
            action_ownership: &self.actions,
            tombstones: &self.tombstones,
            guarded_resources: &self.resources,
            deletions: &self.deletions,
            clock: &self.clock,
        })
    }

    fn operations(&self) -> Vec<String> {
        self.operations.lock().unwrap().clone()
    }
}

struct FixedClock;
impl crate::application::ports::Clock for FixedClock {
    fn now(&self) -> OffsetDateTime {
        NOW
    }
}

struct FakeGuard(Arc<Mutex<Vec<String>>>);
#[async_trait]
impl ContentLockDeletionActionGuard for FakeGuard {
    async fn release(self: Box<Self>) -> Result<(), ApplicationError> {
        self.0.lock().unwrap().push("release".to_owned());
        Ok(())
    }
}

struct FakeActions {
    busy: AtomicBool,
    claim_live: AtomicBool,
    operations: Arc<Mutex<Vec<String>>>,
}
impl FakeActions {
    fn new(operations: Arc<Mutex<Vec<String>>>) -> Self {
        Self {
            busy: AtomicBool::new(false),
            claim_live: AtomicBool::new(true),
            operations,
        }
    }
}
#[async_trait]
impl ContentLockDeletionActionOwnership for FakeActions {
    async fn try_acquire(
        &self,
        _: ContentLockDeletionActionClaim<'_>,
    ) -> Result<ContentLockDeletionActionAcquireResult, ApplicationError> {
        if self.busy.load(Ordering::SeqCst) {
            Ok(ContentLockDeletionActionAcquireResult::Busy)
        } else if !self.claim_live.load(Ordering::SeqCst) {
            Ok(ContentLockDeletionActionAcquireResult::ClaimLost)
        } else {
            Ok(ContentLockDeletionActionAcquireResult::Acquired(Box::new(
                FakeGuard(Arc::clone(&self.operations)),
            )))
        }
    }
}

struct FakeTombstones {
    operations: Arc<Mutex<Vec<String>>>,
    fail: AtomicBool,
}
impl FakeTombstones {
    fn new(operations: Arc<Mutex<Vec<String>>>) -> Self {
        Self {
            operations,
            fail: AtomicBool::new(false),
        }
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
        unreachable!()
    }
    async fn read_tombstone(
        &self,
        _: &CreatorPubky,
        _: &ContentLockPath,
        _: &ContentLockDeletionTombstone,
    ) -> Result<TombstoneReadback, ApplicationError> {
        unreachable!()
    }

    async fn force_delete_content_lock_and_verify_absent(
        &self,
        _: &CreatorPubky,
        _: &ContentLockPath,
    ) -> Result<(), ApplicationError> {
        self.operations.lock().unwrap().push("public".to_owned());
        if self.fail.load(Ordering::SeqCst) {
            Err(ApplicationError::Storage {
                message: "public dependency".to_owned(),
            })
        } else {
            Ok(())
        }
    }
}

struct FakeResources {
    operations: Arc<Mutex<Vec<String>>>,
    fail_paths: Mutex<HashSet<String>>,
}
impl FakeResources {
    fn new(operations: Arc<Mutex<Vec<String>>>) -> Self {
        Self {
            operations,
            fail_paths: Mutex::new(HashSet::new()),
        }
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
        self.operations
            .lock()
            .unwrap()
            .push(format!("resource:{path}"));
        if self.fail_paths.lock().unwrap().contains(path) {
            Err(ApplicationError::Storage {
                message: "resource dependency".to_owned(),
            })
        } else {
            Ok(false)
        }
    }
}

struct FakeDeletions {
    operations: Arc<Mutex<Vec<String>>>,
    claim_live: AtomicBool,
    complete_calls: AtomicUsize,
    fail_complete_once: AtomicBool,
}
use std::sync::atomic::AtomicUsize;
impl FakeDeletions {
    fn new(operations: Arc<Mutex<Vec<String>>>) -> Self {
        Self {
            operations,
            claim_live: AtomicBool::new(true),
            complete_calls: AtomicUsize::new(0),
            fail_complete_once: AtomicBool::new(false),
        }
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
        _: ContentLockDeletionPhase,
    ) -> Result<AdvanceContentLockDeletionPhaseResult, ApplicationError> {
        unreachable!()
    }
    async fn finish(
        &self,
        _: Uuid,
        _: &str,
        _: Uuid,
        _: Option<ContentLockDeletionFailureCode>,
    ) -> Result<Option<ContentLockDeletionJob>, ApplicationError> {
        unreachable!()
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
        self.complete_calls.fetch_add(1, Ordering::SeqCst);
        self.operations.lock().unwrap().push("complete".to_owned());
        if self.fail_complete_once.swap(false, Ordering::SeqCst) {
            return Err(ApplicationError::Storage {
                message: "force receipt persistence unavailable after deletion".to_owned(),
            });
        }
        Ok(self.claim_live.load(Ordering::SeqCst))
    }
    async fn has_force_receipt(
        &self,
        _: &CreatorPubky,
        _: &LockId,
    ) -> Result<bool, ApplicationError> {
        unreachable!()
    }
}

fn force_claim() -> ClaimedContentLockDeletionJob {
    claimed_deletion_job(NOW, ContentLockDeletionPhase::Withdraw, true)
}
