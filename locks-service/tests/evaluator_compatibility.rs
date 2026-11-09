use std::str::FromStr;

use locks_core::ids::{BundleId, CreatorPubky, LockHash, LockId, PubkyLockResource};
use locks_core::lock_policy::{
    GraphParticipantV2, GraphPolicyV2, GraphPredicateV2, PaymentPolicyV2,
};
use locks_core::verification::SatisfiedByV2;
use locks_core::verification::{SUBMITTED_PROOF_BUNDLE_V2_VERSION, SubmittedProofBundleV2};
use locks_service::application::models::{
    EvaluatorTaskSnapshotV1, GraphTaskSnapshot, GraphTaskSnapshotV1, PaymentTaskSnapshot,
    PaymentTaskSnapshotV1, VerificationTaskStatus,
};
use locks_service::application::ports::{
    EvaluatorMode, EvaluatorTaskKernel, build_evaluator_publication,
};
use time::macros::datetime;

const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
const READER: &str = "pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky";
const BUNDLE: &str = "000G40R40M30E209185GR38E1W";

fn identity() -> (
    CreatorPubky,
    CreatorPubky,
    BundleId,
    PubkyLockResource,
    [u8; 32],
) {
    let creator = CreatorPubky::from_str(CREATOR).unwrap();
    let reader = CreatorPubky::from_str(READER).unwrap();
    let bundle_id = BundleId::from_str(BUNDLE).unwrap();
    let lock_hash = [7; 32];
    let lock_id = LockId::from_hash(LockHash::from_bytes(lock_hash));
    let resource =
        PubkyLockResource::from_str(&format!("{CREATOR}/pub/app.locks/{lock_id}.json")).unwrap();
    (creator, reader, bundle_id, resource, lock_hash)
}

fn graph_snapshot() -> GraphTaskSnapshot {
    let (creator, reader, bundle_id, resource, lock_hash) = identity();
    GraphTaskSnapshot::V1(
        GraphTaskSnapshotV1::new(
            creator,
            bundle_id,
            resource,
            lock_hash,
            reader,
            datetime!(2026-10-01 12:00:00 UTC),
            300,
            GraphPolicyV2 {
                predicates: vec![GraphPredicateV2::Follows {
                    follower: GraphParticipantV2::Reader,
                    followed: GraphParticipantV2::Creator,
                }],
                operator: None,
            },
        )
        .unwrap(),
    )
}

fn payment_snapshot() -> PaymentTaskSnapshot {
    let (creator, reader, bundle_id, resource, lock_hash) = identity();
    PaymentTaskSnapshot::V1(
        PaymentTaskSnapshotV1::new(
            SubmittedProofBundleV2 {
                version: SUBMITTED_PROOF_BUNDLE_V2_VERSION,
                bundle_id,
                pubky_lock_resource: resource,
                reader_pubky: Some(reader),
            },
            lock_hash,
            PaymentPolicyV2 {
                recipient_pubky: creator,
                amount: "50000".to_owned(),
                asset: "BTC".to_owned(),
                minimum_confirmations: 1,
            },
            datetime!(2026-10-01 12:00:00 UTC),
            300,
        )
        .unwrap(),
    )
}

#[test]
fn released_snapshots_expose_one_shared_identity_contract() {
    let graph = graph_snapshot();
    let payment = payment_snapshot();

    assert_eq!(graph.identity(), payment.identity());
    assert_eq!(graph.identity().creator.to_string(), CREATOR);
    assert_eq!(graph.identity().reader.to_string(), READER);
    assert_eq!(graph.identity().bundle_id.to_string(), BUNDLE);
    assert_eq!(
        graph.identity().accepted_at,
        datetime!(2026-10-01 12:00:00 UTC)
    );
    assert_eq!(
        graph.identity().deadline_at,
        datetime!(2026-10-01 12:05:00 UTC)
    );
    assert_eq!(
        graph.as_v1().evaluation().root,
        &locks_service::application::models::GraphRootState::Pending
    );
    assert_eq!(
        payment.as_v1().evaluation().root,
        &locks_service::application::models::PaymentRootState::Pending
    );
}

#[test]
fn shared_identity_access_does_not_change_released_json_layouts() {
    for value in [
        graph_snapshot().to_json().unwrap(),
        payment_snapshot().to_json().unwrap(),
    ] {
        assert!(value.get("identity").is_none());
        assert!(value.get("inner").is_none());
        for field in [
            "creator",
            "bundle_id",
            "pubky_lock_resource",
            "lock_hash",
            "reader",
            "accepted_at",
            "deadline_at",
            "root",
        ] {
            assert!(
                value.get(field).is_some(),
                "missing released top-level field {field}"
            );
        }
    }
}

#[test]
fn shared_kernel_builds_mode_specific_released_publications() {
    let verified_at = datetime!(2026-10-01 12:00:04 UTC);
    for (mode, expected) in [
        (EvaluatorMode::Graph, SatisfiedByV2::Graph),
        (EvaluatorMode::Payment, SatisfiedByV2::Payment),
    ] {
        let snapshot = if mode == EvaluatorMode::Graph {
            graph_snapshot().identity()
        } else {
            payment_snapshot().identity()
        };
        let publication = build_evaluator_publication(&snapshot, mode, verified_at).unwrap();
        assert_eq!(publication.satisfied_by, [expected]);
        assert_eq!(publication.creator, snapshot.creator);
        assert_eq!(publication.reader, snapshot.reader);
        assert_eq!(publication.bundle_id, snapshot.bundle_id);
        assert_eq!(
            publication.pubky_lock_resource,
            snapshot.pubky_lock_resource
        );
        assert_eq!(publication.verified_at, verified_at);
    }
}

#[test]
fn shared_kernel_owns_revision_claim_and_retry_arithmetic() {
    let now = datetime!(2026-10-01 12:00:00 UTC);
    let deadline = datetime!(2026-10-01 12:05:00 UTC);

    assert_eq!(EvaluatorTaskKernel::next_revision(4).unwrap(), 5);
    assert!(EvaluatorTaskKernel::next_revision(u64::MAX).is_err());
    assert_eq!(
        EvaluatorTaskKernel::claim_ttl(now, now + time::Duration::seconds(30)).unwrap(),
        time::Duration::seconds(30)
    );
    assert!(EvaluatorTaskKernel::claim_ttl(now, now).is_err());
    assert_eq!(
        EvaluatorTaskKernel::retry_due(
            now,
            now + time::Duration::seconds(600),
            now + time::Duration::seconds(3),
            deadline,
        )
        .unwrap(),
        deadline
    );
    let token = uuid::Uuid::new_v4();
    assert!(EvaluatorTaskKernel::claim_is_available(
        VerificationTaskStatus::Pending,
        true,
        false,
        None,
        None,
        now,
    ));
    assert!(EvaluatorTaskKernel::claim_binding_is_live(
        4,
        4,
        Some("worker"),
        "worker",
        Some(&token),
        &token,
        Some(now + time::Duration::seconds(30)),
        now,
    ));
}

#[test]
fn released_snapshot_fixtures_round_trip_without_layout_changes() {
    let graph_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "fixtures/evaluator/graph_snapshot_v1_rc3.json"
    ))
    .unwrap();
    let payment_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "fixtures/evaluator/payment_snapshot_v1_rc3.json"
    ))
    .unwrap();

    let graph = GraphTaskSnapshot::from_json(graph_fixture.clone()).unwrap();
    let payment = PaymentTaskSnapshot::from_json(payment_fixture.clone()).unwrap();

    assert_eq!(graph.to_json().unwrap(), graph_fixture);
    assert_eq!(payment.to_json().unwrap(), payment_fixture);
}

#[test]
fn released_publication_fixtures_remain_byte_exact() {
    let verified_at = datetime!(2026-10-01 12:00:04 UTC);
    for (identity, mode, fixture) in [
        (
            graph_snapshot().identity(),
            EvaluatorMode::Graph,
            include_str!("fixtures/evaluator/graph_publication_v2_rc3.json"),
        ),
        (
            payment_snapshot().identity(),
            EvaluatorMode::Payment,
            include_str!("fixtures/evaluator/payment_publication_v2_rc3.json"),
        ),
    ] {
        let publication = build_evaluator_publication(&identity, mode, verified_at).unwrap();
        assert_eq!(serde_json::to_string(&publication).unwrap(), fixture.trim());
        let decoded: locks_core::verification::VerifiedProofBundleV2 =
            serde_json::from_str(fixture).unwrap();
        assert_eq!(decoded, publication);
    }
}
