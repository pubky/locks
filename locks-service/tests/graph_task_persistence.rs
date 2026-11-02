mod support;

use std::{str::FromStr, sync::Arc};

use locks_service::application::graph0conf_reducer::reduce_graph_snapshot;
use locks_service::application::models::{
    GraphDeadlineResult, GraphObservationOutcome, GraphPredicateObservation, GraphRootState,
};
use locks_service::application::ports::{
    GraphTaskClaimer, GraphTaskRepository, VerificationTaskRepository,
};
use locks_service::infrastructure::memory::graph_tasks::InMemoryGraphTaskRepository;
use locks_service::infrastructure::memory::verification_task_deletion_fence::InMemoryVerificationTaskDeletionFence;
use locks_service::infrastructure::memory::verification_tasks::InMemoryVerificationTaskRepository;
use time::macros::datetime;

const NOW: time::OffsetDateTime = datetime!(2026-10-01 12:00:00 UTC);
const LEASE_END: time::OffsetDateTime = datetime!(2026-10-01 12:05:00 UTC);

#[tokio::test]
async fn graph_evidence_must_enter_through_a_timely_latest_observation_transition() {
    let repository = InMemoryGraphTaskRepository::new();
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
    let repository = InMemoryGraphTaskRepository::new();
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
}

#[tokio::test]
async fn replay_is_exact_and_snapshot_binding_cannot_change() {
    let repository = InMemoryGraphTaskRepository::new();
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
async fn terminal_graph_snapshot_is_not_reclaimed_after_lease_expiry() {
    let repository = InMemoryGraphTaskRepository::new();
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
    repository
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
        repository
            .claim_next_graph_task(
                "worker-b",
                LEASE_END,
                LEASE_END + time::Duration::minutes(5),
            )
            .await
            .unwrap(),
        None
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
