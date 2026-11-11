use std::str::FromStr;

use locks_core::ids::{BundleId, CreatorPubky, LockHash, LockId, PubkyLockResource};
use locks_core::lock_policy::{
    GraphParticipantV2, GraphPolicyV2, GraphPredicateV2, PaymentPolicyV2,
};
use locks_core::verification::{
    SUBMITTED_PROOF_BUNDLE_V2_VERSION, SatisfiedByV2, SubmittedProofBundleV2,
};
use locks_service::application::mixed_v2_reducer::reduce_mixed_snapshot;
use locks_service::application::models::{
    BitcoinOutpointV2, GraphObservationOutcome, MixedRootState, MixedTaskSnapshot,
    MixedTaskSnapshotV1, PaymentAssetV2, PaymentProviderStatus, PaymentRequestStateV2,
    PaymentRootState, PaymentSampleStatusV2, PaymentSampleV2, PaymentStateV2, PaymentTaskSnapshot,
    VerificationTerminalReason,
};
use locks_service::application::ports::ProviderV2EvaluatorSnapshot;
use time::macros::datetime;

const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
const READER: &str = "pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky";
const BUNDLE: &str = "000G40R40M30E209185GR38E1W";
const ACCEPTED: time::OffsetDateTime = datetime!(2026-11-05 12:00:00 UTC);
const DEADLINE: time::OffsetDateTime = datetime!(2026-11-05 12:05:00 UTC);

#[test]
fn impossible_satisfaction_path_is_rejected_at_snapshot_boundary() {
    let mut mixed = snapshot();
    mixed.root = MixedRootState::Satisfied;
    mixed.satisfied_by = vec![locks_core::verification::SatisfiedByV2::Payment];

    assert!(mixed.validate().is_err());
}

#[test]
fn graph_satisfies_only_while_same_wave_payment_is_eligible_and_live() {
    let mut mixed = snapshot();
    mixed
        .apply_create_response(
            ACCEPTED + time::Duration::seconds(1),
            ACCEPTED + time::Duration::seconds(60),
            DEADLINE,
        )
        .unwrap();
    mixed
        .apply_payment_status(status(
            PaymentRequestStateV2::Accepted,
            PaymentStateV2::Undetected,
            None,
        ))
        .unwrap();
    mixed
        .record_graph_observation(
            0,
            GraphObservationOutcome::True,
            ACCEPTED + time::Duration::seconds(2),
        )
        .unwrap();

    reduce_mixed_snapshot(&mut mixed, ACCEPTED + time::Duration::seconds(2)).unwrap();

    assert_eq!(mixed.root, MixedRootState::Satisfied);
    assert_eq!(
        mixed.satisfied_by,
        vec![locks_core::verification::SatisfiedByV2::ZeroConfGraph]
    );
    assert_eq!(
        mixed
            .payment
            .provider_status
            .as_ref()
            .unwrap()
            .payment_state,
        PaymentStateV2::Undetected
    );
}

#[test]
fn expired_payment_blocks_graph_and_retained_threshold() {
    let mut mixed = snapshot();
    mixed
        .apply_create_response(
            ACCEPTED + time::Duration::seconds(1),
            ACCEPTED + time::Duration::seconds(60),
            DEADLINE,
        )
        .unwrap();
    let first = PaymentSampleV2 {
        observed_at: ACCEPTED + time::Duration::seconds(2),
        status: PaymentSampleStatusV2::Confirmed,
        outpoint: Some(BitcoinOutpointV2 {
            txid: "ab".repeat(32),
            vout: 1,
        }),
        confirmations: 1,
        amount_matched: true,
    };
    mixed
        .apply_payment_status(status(
            PaymentRequestStateV2::Accepted,
            PaymentStateV2::Expired,
            Some(first.clone()),
        ))
        .unwrap();
    mixed
        .record_graph_observation(
            0,
            GraphObservationOutcome::True,
            ACCEPTED + time::Duration::seconds(3),
        )
        .unwrap();

    reduce_mixed_snapshot(&mut mixed, ACCEPTED + time::Duration::seconds(3)).unwrap();

    assert_eq!(
        mixed.root.terminal_reason(),
        Some(
            locks_service::application::models::VerificationTerminalReason::PaymentDeadlineExpired
        )
    );
    assert!(mixed.satisfied_by.is_empty());
    assert_eq!(
        mixed
            .payment
            .provider_status
            .as_ref()
            .unwrap()
            .first_threshold_satisfied,
        Some(first)
    );
}

#[test]
fn deletion_reconciliation_preserves_graph_completed_snapshot_when_payment_later_satisfies() {
    let mut mixed = snapshot();
    mixed
        .apply_create_response(
            ACCEPTED + time::Duration::seconds(1),
            ACCEPTED + time::Duration::seconds(60),
            DEADLINE,
        )
        .unwrap();
    mixed
        .apply_payment_status(status(
            PaymentRequestStateV2::Accepted,
            PaymentStateV2::Undetected,
            None,
        ))
        .unwrap();
    mixed
        .record_graph_observation(
            0,
            GraphObservationOutcome::True,
            ACCEPTED + time::Duration::seconds(2),
        )
        .unwrap();
    reduce_mixed_snapshot(&mut mixed, ACCEPTED + time::Duration::seconds(2)).unwrap();
    assert_eq!(mixed.satisfied_by, vec![SatisfiedByV2::ZeroConfGraph]);

    let frozen = MixedTaskSnapshot::V1(mixed.clone());
    let mut later_payment = mixed.payment_snapshot().unwrap();
    later_payment
        .apply_status(status(
            PaymentRequestStateV2::Accepted,
            PaymentStateV2::Confirmed,
            Some(PaymentSampleV2 {
                observed_at: ACCEPTED + time::Duration::seconds(3),
                status: PaymentSampleStatusV2::Confirmed,
                outpoint: Some(BitcoinOutpointV2 {
                    txid: "cd".repeat(32),
                    vout: 2,
                }),
                confirmations: 1,
                amount_matched: true,
            }),
        ))
        .unwrap();

    let reconciled = ProviderV2EvaluatorSnapshot::Mixed(frozen.clone())
        .with_payment_snapshot(PaymentTaskSnapshot::V1(later_payment))
        .expect("completed mixed snapshot must remain immutable during deletion reconciliation");
    assert_eq!(reconciled, ProviderV2EvaluatorSnapshot::Mixed(frozen));
}

#[test]
fn deletion_reconciliation_updates_provider_evidence_without_rewriting_pending_roots() {
    let mut mixed = snapshot();
    mixed
        .apply_create_response(
            ACCEPTED + time::Duration::seconds(1),
            ACCEPTED + time::Duration::seconds(60),
            DEADLINE,
        )
        .unwrap();
    let frozen_graph_root = mixed.graph.root;
    let frozen_payment_root = mixed.payment.root;
    let frozen_mixed_root = mixed.root;
    let mut reconciled_payment = mixed.payment_snapshot().unwrap();
    reconciled_payment.root =
        PaymentRootState::Expired(VerificationTerminalReason::ContentLockDeleted);

    let reconciled = ProviderV2EvaluatorSnapshot::Mixed(MixedTaskSnapshot::V1(mixed))
        .with_payment_snapshot(PaymentTaskSnapshot::V1(reconciled_payment))
        .unwrap();
    let ProviderV2EvaluatorSnapshot::Mixed(reconciled) = reconciled else {
        panic!("mixed reconciliation changed evaluator mode");
    };

    assert_eq!(reconciled.as_v1().graph.root, frozen_graph_root);
    assert_eq!(reconciled.as_v1().payment.root, frozen_payment_root);
    assert_eq!(reconciled.as_v1().root, frozen_mixed_root);
    assert!(reconciled.as_v1().payment.provider_status.is_none());
}

fn snapshot() -> MixedTaskSnapshotV1 {
    MixedTaskSnapshotV1::new(
        SubmittedProofBundleV2 {
            version: SUBMITTED_PROOF_BUNDLE_V2_VERSION,
            bundle_id: BundleId::from_str(BUNDLE).unwrap(),
            pubky_lock_resource: resource(),
            reader_pubky: None,
        },
        [0; 32],
        CreatorPubky::from_str(READER).unwrap(),
        GraphPolicyV2 {
            operator: None,
            predicates: vec![GraphPredicateV2::Follows {
                follower: GraphParticipantV2::Reader,
                followed: GraphParticipantV2::Creator,
            }],
        },
        PaymentPolicyV2 {
            recipient_pubky: CreatorPubky::from_str(CREATOR).unwrap(),
            amount: "50000".to_owned(),
            asset: "BTC".to_owned(),
            minimum_confirmations: 1,
        },
        ACCEPTED,
        300,
    )
    .unwrap()
}

fn status(
    request_state: PaymentRequestStateV2,
    payment_state: PaymentStateV2,
    first: Option<PaymentSampleV2>,
) -> PaymentProviderStatus {
    PaymentProviderStatus {
        creator: CreatorPubky::from_str(CREATOR).unwrap(),
        bundle_id: BundleId::from_str(BUNDLE).unwrap(),
        request_state,
        payment_state,
        invoice_created_at: ACCEPTED + time::Duration::seconds(1),
        proposal_expires_at: ACCEPTED + time::Duration::seconds(60),
        deadline: DEADLINE,
        asset: PaymentAssetV2::Btc,
        minimum_confirmations: 1,
        current_sample: first.clone(),
        first_threshold_satisfied: first,
    }
}

fn resource() -> PubkyLockResource {
    let lock_id = LockId::from_hash(LockHash::from_bytes([0; 32]));
    PubkyLockResource::from_str(&format!("{CREATOR}/pub/app.locks/{lock_id}.json")).unwrap()
}
