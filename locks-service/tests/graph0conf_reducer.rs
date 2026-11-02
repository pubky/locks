use std::str::FromStr;

use locks_core::ids::{BundleId, CreatorPubky, LockHash, LockId, PubkyLockResource};
use locks_core::lock_policy::{
    GraphOperatorV2, GraphParticipantV2, GraphPolicyV2, GraphPredicateV2,
};
use locks_service::application::graph0conf_reducer::reduce_graph_snapshot;
use locks_service::application::models::{
    GRAPH_TASK_SNAPSHOT_VERSION, GraphDeadlineResult, GraphObservationOutcome,
    GraphPredicateObservation, GraphRootState, GraphTaskSnapshot, GraphTaskSnapshotV1,
};
use time::macros::datetime;

const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
const READER: &str = "pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky";
const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";
const ACCEPTED_AT: time::OffsetDateTime = datetime!(2026-10-01 12:00:00 UTC);
const DEADLINE_AT: time::OffsetDateTime = datetime!(2026-10-01 12:01:00 UTC);

#[test]
fn all_and_any_deadline_tables_preserve_false_vs_unavailable() {
    let cases = [
        (
            GraphOperatorV2::And,
            [GraphObservationOutcome::True, GraphObservationOutcome::True],
            GraphRootState::Satisfied,
        ),
        (
            GraphOperatorV2::And,
            [
                GraphObservationOutcome::False,
                GraphObservationOutcome::True,
            ],
            GraphRootState::Expired(GraphDeadlineResult::False),
        ),
        (
            GraphOperatorV2::And,
            [
                GraphObservationOutcome::False,
                GraphObservationOutcome::DependencyUnavailable,
            ],
            GraphRootState::Expired(GraphDeadlineResult::False),
        ),
        (
            GraphOperatorV2::And,
            [
                GraphObservationOutcome::True,
                GraphObservationOutcome::DependencyUnavailable,
            ],
            GraphRootState::Expired(GraphDeadlineResult::DependencyUnavailable),
        ),
        (
            GraphOperatorV2::Or,
            [
                GraphObservationOutcome::True,
                GraphObservationOutcome::DependencyUnavailable,
            ],
            GraphRootState::Satisfied,
        ),
        (
            GraphOperatorV2::Or,
            [
                GraphObservationOutcome::False,
                GraphObservationOutcome::DependencyUnavailable,
            ],
            GraphRootState::Expired(GraphDeadlineResult::DependencyUnavailable),
        ),
        (
            GraphOperatorV2::Or,
            [
                GraphObservationOutcome::False,
                GraphObservationOutcome::False,
            ],
            GraphRootState::Expired(GraphDeadlineResult::False),
        ),
    ];

    for (operator, outcomes, expected) in cases {
        let mut snapshot = snapshot(Some(operator), 2);
        for (index, outcome) in outcomes.into_iter().enumerate() {
            snapshot
                .record_observation(index, outcome, DEADLINE_AT)
                .unwrap();
        }
        reduce_graph_snapshot(&mut snapshot, DEADLINE_AT).unwrap();
        assert_eq!(snapshot.root, expected, "operator={operator:?}");
    }
}

#[test]
fn false_and_unavailable_repoll_while_true_is_sticky() {
    let before_deadline = DEADLINE_AT - time::Duration::seconds(1);
    let mut snapshot = snapshot(None, 1);

    snapshot
        .record_observation(0, GraphObservationOutcome::False, before_deadline)
        .unwrap();
    reduce_graph_snapshot(&mut snapshot, before_deadline).unwrap();
    assert_eq!(snapshot.root, GraphRootState::Pending);

    snapshot
        .record_observation(
            0,
            GraphObservationOutcome::DependencyUnavailable,
            before_deadline,
        )
        .unwrap();
    reduce_graph_snapshot(&mut snapshot, before_deadline).unwrap();
    assert_eq!(snapshot.root, GraphRootState::Pending);

    snapshot
        .record_observation(0, GraphObservationOutcome::True, before_deadline)
        .unwrap();
    snapshot
        .record_observation(0, GraphObservationOutcome::False, DEADLINE_AT)
        .unwrap();
    reduce_graph_snapshot(&mut snapshot, DEADLINE_AT).unwrap();
    assert_eq!(snapshot.root, GraphRootState::Satisfied);
    assert_eq!(
        snapshot.predicates[0]
            .first_true_observation
            .as_ref()
            .unwrap()
            .observed_at,
        before_deadline
    );
}

#[test]
fn evidence_at_deadline_equality_is_timely_but_late_evidence_is_rejected_atomically() {
    let mut timely = snapshot(None, 1);
    timely
        .record_observation(0, GraphObservationOutcome::True, DEADLINE_AT)
        .unwrap();
    reduce_graph_snapshot(&mut timely, DEADLINE_AT).unwrap();
    assert_eq!(timely.root, GraphRootState::Satisfied);

    let mut late = snapshot(None, 1);
    let before = late.clone();
    assert!(
        late.record_observation(
            0,
            GraphObservationOutcome::True,
            DEADLINE_AT + time::Duration::nanoseconds(1),
        )
        .is_err()
    );
    assert_eq!(late, before);
}

#[test]
fn observations_are_time_ordered_and_stale_poll_results_are_rejected_atomically() {
    let mut snapshot = snapshot(None, 1);
    snapshot
        .record_observation(
            0,
            GraphObservationOutcome::False,
            ACCEPTED_AT + time::Duration::seconds(20),
        )
        .unwrap();
    let before = snapshot.clone();
    assert!(
        snapshot
            .record_observation(
                0,
                GraphObservationOutcome::True,
                ACCEPTED_AT + time::Duration::seconds(19),
            )
            .is_err()
    );
    assert_eq!(snapshot, before);
}

#[test]
fn model_rejects_fabricated_sticky_true_and_future_reduction_atomically() {
    let current = snapshot(None, 1);
    let mut no_latest = current.clone();
    no_latest.predicates[0].first_true_observation = Some(GraphPredicateObservation {
        outcome: GraphObservationOutcome::True,
        observed_at: ACCEPTED_AT,
        origin: None,
        http_status: None,
    });
    assert!(no_latest.validate().is_err());

    let mut unrelated = current.clone();
    unrelated.predicates[0].first_true_observation = Some(GraphPredicateObservation {
        outcome: GraphObservationOutcome::True,
        observed_at: ACCEPTED_AT + time::Duration::seconds(1),
        origin: None,
        http_status: None,
    });
    unrelated.predicates[0].latest_observation = Some(GraphPredicateObservation {
        outcome: GraphObservationOutcome::False,
        observed_at: ACCEPTED_AT + time::Duration::seconds(2),
        origin: None,
        http_status: None,
    });
    assert!(unrelated.validate().is_ok());
    assert!(unrelated.validate_progress_from(&current).is_err());

    let mut future = current;
    future
        .record_observation(
            0,
            GraphObservationOutcome::True,
            ACCEPTED_AT + time::Duration::seconds(2),
        )
        .unwrap();
    let before = future.clone();
    assert!(reduce_graph_snapshot(&mut future, ACCEPTED_AT + time::Duration::seconds(1)).is_err());
    assert_eq!(future, before);
}

#[test]
fn snapshot_validation_rejects_submicrosecond_persistence_timestamps() {
    let mut noncanonical = snapshot(None, 1);
    noncanonical.accepted_at += time::Duration::nanoseconds(1);
    noncanonical.deadline_at += time::Duration::nanoseconds(1);
    assert!(noncanonical.validate().is_err());
}

#[test]
fn snapshot_validation_rejects_fabricated_terminal_roots() {
    let mut fabricated_success = snapshot(None, 1);
    fabricated_success.root = GraphRootState::Satisfied;
    assert!(fabricated_success.validate().is_err());

    let mut fabricated_unavailable = snapshot(None, 1);
    fabricated_unavailable.root =
        GraphRootState::Expired(GraphDeadlineResult::DependencyUnavailable);
    assert!(fabricated_unavailable.validate().is_err());
}

#[test]
fn terminal_invalid_is_sticky_and_requires_matching_unavailable_nexus_evidence() {
    let observed_at = ACCEPTED_AT + time::Duration::seconds(1);
    let mut current = snapshot(None, 1);
    current
        .terminalize_invalid_nexus_response(
            0,
            observed_at,
            "https://nexus.example".to_owned(),
            Some(200),
        )
        .unwrap();

    let mut cleared = current.clone();
    cleared.predicates[0].terminal_invalid = false;
    assert!(cleared.validate_progress_from(&current).is_err());

    let mut fabricated = snapshot(None, 1);
    fabricated.predicates[0].terminal_invalid = true;
    assert!(fabricated.validate().is_err());
}

#[test]
fn deadline_creation_is_checked_and_expiry_without_observation_is_false() {
    let mut snapshot = GraphTaskSnapshotV1::new(
        creator(),
        bundle_id(),
        resource(),
        [0; 32],
        reader(),
        ACCEPTED_AT,
        60,
        graph_policy(None, 1),
    )
    .unwrap();
    assert_eq!(snapshot.deadline_at, DEADLINE_AT);

    reduce_graph_snapshot(&mut snapshot, DEADLINE_AT).unwrap();
    assert_eq!(
        snapshot.root,
        GraphRootState::Expired(GraphDeadlineResult::False)
    );

    assert!(
        GraphTaskSnapshotV1::new(
            creator(),
            bundle_id(),
            resource(),
            [0; 32],
            reader(),
            datetime!(9999-12-31 23:59:59 UTC),
            60,
            graph_policy(None, 1),
        )
        .is_err()
    );
}

#[test]
fn snapshot_decode_rejects_unknown_version_corruption_and_lock_binding_mismatch() {
    let snapshot = GraphTaskSnapshot::V1(snapshot(None, 1));
    let value = snapshot.to_json().unwrap();
    assert_eq!(
        GraphTaskSnapshot::from_json(value.clone()).unwrap(),
        snapshot
    );

    let mut unknown = value.clone();
    unknown["schema_version"] = serde_json::json!(GRAPH_TASK_SNAPSHOT_VERSION + 1);
    assert!(GraphTaskSnapshot::from_json(unknown).is_err());

    let mut corrupt = value.clone();
    corrupt["predicates"] = serde_json::json!([]);
    assert!(GraphTaskSnapshot::from_json(corrupt).is_err());

    let mut mismatched = value;
    mismatched["lock_hash"] = serde_json::to_value(vec![1_u8; 32]).unwrap();
    assert!(GraphTaskSnapshot::from_json(mismatched).is_err());

    let mut corrupt_window = snapshot.to_json().unwrap();
    corrupt_window["deadline_at"] = serde_json::json!("2026-10-01T12:00:59Z");
    assert!(GraphTaskSnapshot::from_json(corrupt_window).is_err());
}

fn snapshot(operator: Option<GraphOperatorV2>, count: usize) -> GraphTaskSnapshotV1 {
    GraphTaskSnapshotV1::new(
        creator(),
        bundle_id(),
        resource(),
        [0; 32],
        reader(),
        ACCEPTED_AT,
        60,
        graph_policy(operator, count),
    )
    .unwrap()
}

fn graph_policy(operator: Option<GraphOperatorV2>, count: usize) -> GraphPolicyV2 {
    GraphPolicyV2 {
        predicates: (0..count)
            .map(|index| {
                if index % 2 == 0 {
                    GraphPredicateV2::Follows {
                        follower: GraphParticipantV2::Reader,
                        followed: GraphParticipantV2::Creator,
                    }
                } else {
                    GraphPredicateV2::Tagged {
                        tagger: GraphParticipantV2::Reader,
                        tagged: GraphParticipantV2::Creator,
                        label: format!("trusted{index}"),
                    }
                }
            })
            .collect(),
        operator,
    }
}

fn creator() -> CreatorPubky {
    CreatorPubky::from_str(CREATOR).unwrap()
}

fn reader() -> CreatorPubky {
    CreatorPubky::from_str(READER).unwrap()
}

fn bundle_id() -> BundleId {
    BundleId::from_str(BUNDLE_ID).unwrap()
}

fn resource() -> PubkyLockResource {
    let lock_id = LockId::from_hash(LockHash::from_bytes([0; 32]));
    PubkyLockResource::from_str(&format!("{CREATOR}/pub/app.locks/{lock_id}.json")).unwrap()
}

#[allow(dead_code)]
fn observation(outcome: GraphObservationOutcome) -> GraphPredicateObservation {
    GraphPredicateObservation {
        outcome,
        observed_at: DEADLINE_AT,
        origin: None,
        http_status: None,
    }
}
