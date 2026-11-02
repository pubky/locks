mod support;

use std::{
    str::FromStr,
    sync::{Arc, Mutex},
};

use locks_service::application::graph0conf_reducer::reduce_graph_snapshot;
use locks_service::application::models::{
    GraphDeadlineResult, GraphObservationOutcome, GraphPredicateObservation, GraphRootState,
};
use locks_service::application::ports::{
    Clock, GraphTaskClaimer, GraphTaskRepository, VerificationTaskRepository,
};
use locks_service::infrastructure::memory::graph_tasks::InMemoryGraphTaskRepository;
use locks_service::infrastructure::memory::verification_task_deletion_fence::InMemoryVerificationTaskDeletionFence;
use locks_service::infrastructure::memory::verification_tasks::InMemoryVerificationTaskRepository;
use time::macros::datetime;

const NOW: time::OffsetDateTime = datetime!(2026-10-01 12:00:00 UTC);
const LEASE_END: time::OffsetDateTime = datetime!(2026-10-01 12:05:00 UTC);

#[derive(Debug)]
struct MutableClock(Mutex<time::OffsetDateTime>);

impl MutableClock {
    fn set(&self, now: time::OffsetDateTime) {
        *self.0.lock().unwrap() = now;
    }
}

impl Clock for MutableClock {
    fn now(&self) -> time::OffsetDateTime {
        *self.0.lock().unwrap()
    }
}

fn repository() -> (InMemoryGraphTaskRepository, Arc<MutableClock>) {
    let clock = Arc::new(MutableClock(Mutex::new(NOW)));
    let fence = Arc::new(InMemoryVerificationTaskDeletionFence::with_clock(
        clock.clone(),
    ));
    (
        InMemoryGraphTaskRepository::with_deletion_fence(fence),
        clock,
    )
}

#[tokio::test]
async fn graph_evidence_must_enter_through_a_timely_latest_observation_transition() {
    let (repository, _clock) = repository();
    let mut fabricated_insert = support::graph_task();
    let locks_service::application::models::GraphTaskSnapshot::V1(snapshot) =
        &mut fabricated_insert.snapshot;
    snapshot.predicates[0].first_true_observation = Some(GraphPredicateObservation {
        outcome: GraphObservationOutcome::True,
        observed_at: NOW,
        origin: None,
        http_status: None,
    });
    assert!(
        repository
            .insert_graph_task(fabricated_insert)
            .await
            .is_err()
    );

    let mut terminal_invalid_insert = support::graph_task();
    let locks_service::application::models::GraphTaskSnapshot::V1(snapshot) =
        &mut terminal_invalid_insert.snapshot;
    snapshot.predicates[0].terminal_invalid = true;
    assert!(
        repository
            .insert_graph_task(terminal_invalid_insert)
            .await
            .is_err()
    );

    repository
        .insert_graph_task(support::graph_task())
        .await
        .unwrap();
    let claim = repository
        .claim_next_graph_task("worker-a", NOW, LEASE_END)
        .await
        .unwrap()
        .unwrap();
    let mut fabricated_update = claim.task.snapshot.clone();
    let locks_service::application::models::GraphTaskSnapshot::V1(snapshot) =
        &mut fabricated_update;
    snapshot.predicates[0].first_true_observation = Some(GraphPredicateObservation {
        outcome: GraphObservationOutcome::True,
        observed_at: NOW + time::Duration::seconds(1),
        origin: None,
        http_status: None,
    });
    snapshot.root = GraphRootState::Satisfied;
    assert!(
        repository
            .persist_claimed_graph_snapshot(
                &claim.task.task_id,
                "worker-a",
                &claim.claim_token,
                NOW + time::Duration::seconds(1),
                0,
                fabricated_update,
            )
            .await
            .is_err()
    );

    let mut future = claim.task.snapshot.clone();
    let locks_service::application::models::GraphTaskSnapshot::V1(snapshot) = &mut future;
    snapshot
        .record_observation(
            0,
            GraphObservationOutcome::True,
            NOW + time::Duration::seconds(10),
        )
        .unwrap();
    reduce_graph_snapshot(snapshot, NOW + time::Duration::seconds(10)).unwrap();
    assert!(
        repository
            .persist_claimed_graph_snapshot(
                &claim.task.task_id,
                "worker-a",
                &claim.claim_token,
                NOW + time::Duration::seconds(9),
                0,
                future,
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn whole_snapshot_write_is_claim_token_lease_and_revision_fenced() {
    let (repository, clock) = repository();
    let inserted = repository
        .insert_graph_task(support::graph_task())
        .await
        .unwrap();
    assert_eq!(inserted.evaluator_revision, 0);
    assert!(!repository.has_payment_intent(&inserted.task_id).await);

    let first = repository
        .claim_next_graph_task("worker-a", NOW, LEASE_END)
        .await
        .unwrap()
        .unwrap();
    let mut next = first.task.snapshot.clone();
    let locks_service::application::models::GraphTaskSnapshot::V1(snapshot) = &mut next;
    snapshot
        .record_observation(
            0,
            GraphObservationOutcome::False,
            NOW + time::Duration::seconds(1),
        )
        .unwrap();

    clock.set(NOW + time::Duration::seconds(2));
    let persisted = repository
        .persist_claimed_graph_snapshot(
            &first.task.task_id,
            "worker-a",
            &first.claim_token,
            NOW + time::Duration::seconds(2),
            0,
            next.clone(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(persisted.evaluator_revision, 1);
    assert_eq!(persisted.snapshot, next);
    assert_eq!(
        repository
            .insert_graph_task(support::graph_task())
            .await
            .unwrap(),
        persisted
    );

    assert!(
        repository
            .persist_claimed_graph_snapshot(
                &first.task.task_id,
                "worker-a",
                &first.claim_token,
                NOW + time::Duration::seconds(3),
                1,
                support::graph_task().snapshot,
            )
            .await
            .is_err()
    );
    let mut early_expiry = persisted.snapshot.clone();
    let locks_service::application::models::GraphTaskSnapshot::V1(snapshot) = &mut early_expiry;
    snapshot.root = GraphRootState::Expired(GraphDeadlineResult::False);
    clock.set(NOW + time::Duration::seconds(4));
    assert!(
        repository
            .persist_claimed_graph_snapshot(
                &first.task.task_id,
                "worker-a",
                &first.claim_token,
                NOW + time::Duration::seconds(4),
                1,
                early_expiry,
            )
            .await
            .is_err()
    );

    assert_eq!(
        repository
            .persist_claimed_graph_snapshot(
                &first.task.task_id,
                "worker-a",
                &first.claim_token,
                NOW + time::Duration::seconds(3),
                0,
                next.clone(),
            )
            .await
            .unwrap(),
        None
    );

    let reclaimed_at = LEASE_END;
    clock.set(reclaimed_at);
    let second = repository
        .claim_next_graph_task(
            "worker-a",
            reclaimed_at,
            reclaimed_at + time::Duration::minutes(5),
        )
        .await
        .unwrap()
        .unwrap();
    assert_ne!(first.claim_token, second.claim_token);
    assert_eq!(
        repository
            .persist_claimed_graph_snapshot(
                &first.task.task_id,
                "worker-a",
                &first.claim_token,
                reclaimed_at,
                1,
                next.clone(),
            )
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        repository
            .schedule_graph_task_retry(
                &first.task.task_id,
                "worker-a",
                &first.claim_token,
                reclaimed_at,
                1,
                next,
                reclaimed_at,
            )
            .await
            .unwrap(),
        None,
        "stale ownership must be fenced before candidate retry validation"
    );
}

#[tokio::test]
async fn replay_is_exact_and_snapshot_binding_cannot_change() {
    let (repository, _clock) = repository();
    let task = support::graph_task();
    assert_eq!(
        repository.insert_graph_task(task.clone()).await.unwrap(),
        task
    );
    assert_eq!(
        repository.insert_graph_task(task.clone()).await.unwrap(),
        task
    );
    let mut changed_task_id = task.clone();
    changed_task_id.task_id =
        locks_core::ids::TaskId::from_str("018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d11").unwrap();
    assert!(matches!(
        repository.insert_graph_task(changed_task_id).await,
        Err(locks_service::application::errors::ApplicationError::VerificationTaskConflict)
    ));

    let claim = repository
        .claim_next_graph_task("worker-a", NOW, LEASE_END)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        repository.insert_graph_task(task.clone()).await.unwrap(),
        claim.task
    );
    let mut changed = claim.task.snapshot.clone();
    let locks_service::application::models::GraphTaskSnapshot::V1(snapshot) = &mut changed;
    snapshot.bundle_id = locks_core::ids::BundleId::new_random();

    assert!(
        repository
            .persist_claimed_graph_snapshot(
                &claim.task.task_id,
                "worker-a",
                &claim.claim_token,
                NOW,
                0,
                changed,
            )
            .await
            .is_err()
    );
    assert_eq!(
        repository
            .get_graph_task_by_handle(&support::creator(), &support::bundle_id(),)
            .await
            .unwrap()
            .unwrap(),
        claim.task
    );
}

#[tokio::test]
async fn satisfied_graph_persists_canonical_publication_and_reclaims_after_restart() {
    let (repository, clock) = repository();
    repository
        .insert_graph_task(support::graph_task())
        .await
        .unwrap();
    let claim = repository
        .claim_next_graph_task("worker-a", NOW, LEASE_END)
        .await
        .unwrap()
        .unwrap();
    let mut terminal = claim.task.snapshot.clone();
    let locks_service::application::models::GraphTaskSnapshot::V1(snapshot) = &mut terminal;
    snapshot
        .record_observation(
            0,
            GraphObservationOutcome::True,
            NOW + time::Duration::seconds(1),
        )
        .unwrap();
    reduce_graph_snapshot(snapshot, NOW + time::Duration::seconds(1)).unwrap();
    clock.set(NOW + time::Duration::seconds(2));
    let persisted = repository
        .persist_claimed_graph_snapshot(
            &claim.task.task_id,
            "worker-a",
            &claim.claim_token,
            NOW + time::Duration::seconds(2),
            0,
            terminal,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        persisted.status,
        locks_service::application::models::VerificationTaskStatus::PublishingEntitlement
    );
    let publication = persisted
        .entitlement_to_publish
        .as_ref()
        .expect("satisfied graph stores exact publication before external I/O");
    assert_eq!(
        publication.satisfied_by,
        vec![locks_core::verification::SatisfiedByV2::Graph]
    );
    assert_eq!(publication.creator, support::creator());
    assert_eq!(publication.reader, persisted.snapshot.as_v1().reader);
    let publication_token = persisted
        .entitlement_publication_token
        .expect("publication marker is durable");

    clock.set(LEASE_END);
    let reclaimed = repository
        .claim_next_graph_task(
            "worker-b",
            LEASE_END,
            LEASE_END + time::Duration::minutes(5),
        )
        .await
        .unwrap()
        .expect("ambiguous publication is reclaimable after lease expiry");
    assert_ne!(reclaimed.claim_token, claim.claim_token);
    assert_eq!(
        reclaimed.task.entitlement_publication_token,
        Some(publication_token),
        "reclaim preserves stable publication fence"
    );
    assert_eq!(
        reclaimed.task.entitlement_to_publish,
        Some(publication.clone())
    );

    assert!(
        repository
            .complete_claimed_graph_publication(
                &reclaimed.task.task_id,
                "worker-a",
                &claim.claim_token,
                LEASE_END + time::Duration::seconds(1),
                reclaimed.task.evaluator_revision,
                publication_token,
            )
            .await
            .unwrap()
            .is_none()
    );
    clock.set(LEASE_END + time::Duration::seconds(1));
    let completed = repository
        .complete_claimed_graph_publication(
            &reclaimed.task.task_id,
            "worker-b",
            &reclaimed.claim_token,
            LEASE_END + time::Duration::seconds(1),
            reclaimed.task.evaluator_revision,
            publication_token,
        )
        .await
        .unwrap()
        .expect("current publication claim completes task");
    assert_eq!(
        completed.status,
        locks_service::application::models::VerificationTaskStatus::Completed
    );
    assert_eq!(
        completed.completed_at,
        Some(LEASE_END + time::Duration::seconds(1))
    );
    assert_eq!(completed.entitlement_publication_token, None);
    assert!(
        repository
            .claim_next_graph_task(
                "worker-c",
                LEASE_END + time::Duration::minutes(10),
                LEASE_END + time::Duration::minutes(15),
            )
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn shared_admission_fence_enforces_handle_uniqueness_across_memory_repositories() {
    let fence = Arc::new(InMemoryVerificationTaskDeletionFence::new());
    let first = InMemoryGraphTaskRepository::with_deletion_fence(Arc::clone(&fence));
    let second = InMemoryGraphTaskRepository::with_deletion_fence(Arc::clone(&fence));
    first
        .insert_graph_task(support::graph_task())
        .await
        .unwrap();
    assert_eq!(
        second
            .insert_graph_task(support::graph_task())
            .await
            .unwrap(),
        support::graph_task()
    );

    let third = InMemoryGraphTaskRepository::with_deletion_fence(Arc::clone(&fence));
    let mut reused_task_id = support::graph_task();
    let locks_service::application::models::GraphTaskSnapshot::V1(snapshot) =
        &mut reused_task_id.snapshot;
    snapshot.bundle_id = locks_core::ids::BundleId::new_random();
    assert!(third.insert_graph_task(reused_task_id).await.is_err());
}

#[tokio::test]
async fn legacy_memory_delete_cannot_remove_graph_task_fence() {
    let fence = Arc::new(InMemoryVerificationTaskDeletionFence::new());
    let graph = InMemoryGraphTaskRepository::with_deletion_fence(Arc::clone(&fence));
    let legacy = InMemoryVerificationTaskRepository::with_deletion_fence(fence);
    let task = graph
        .insert_graph_task(support::graph_task())
        .await
        .unwrap();
    legacy
        .delete_verification_task(&task.task_id)
        .await
        .unwrap();
    assert!(
        graph
            .claim_next_graph_task("worker-a", NOW, LEASE_END)
            .await
            .unwrap()
            .is_some()
    );
}
