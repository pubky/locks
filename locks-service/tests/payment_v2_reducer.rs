use std::str::FromStr;

use locks_core::ids::{BundleId, CreatorPubky, PubkyLockResource};
use locks_core::lock_policy::PaymentPolicyV2;
use locks_core::verification::{SUBMITTED_PROOF_BUNDLE_V2_VERSION, SubmittedProofBundleV2};
use locks_service::application::models::{
    BitcoinOutpointV2, PaymentAssetV2, PaymentInvoiceState, PaymentProviderStatus,
    PaymentRequestStateV2, PaymentRootState, PaymentSampleStatusV2, PaymentSampleV2,
    PaymentStateV2, PaymentTaskSnapshotV1, VerificationTerminalReason,
};
use locks_service::application::payment_v2_reducer::reduce_payment_snapshot;
use time::macros::datetime;

const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
const READER: &str = "pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky";
const BUNDLE: &str = "000G40R40M30E209185GR38E1W";
const RESOURCE: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/app.locks/0000000000000000000000000000000000000000000000000000.json";
const ACCEPTED: time::OffsetDateTime = datetime!(2026-11-05 12:00:00 UTC);
const DEADLINE: time::OffsetDateTime = datetime!(2026-11-05 12:05:00 UTC);

fn creator() -> CreatorPubky {
    CreatorPubky::from_str(CREATOR).unwrap()
}
fn reader() -> CreatorPubky {
    CreatorPubky::from_str(READER).unwrap()
}
fn bundle() -> BundleId {
    BundleId::from_str(BUNDLE).unwrap()
}
fn resource() -> PubkyLockResource {
    PubkyLockResource::from_str(RESOURCE).unwrap()
}

fn snapshot(threshold: u8) -> PaymentTaskSnapshotV1 {
    let submitted = SubmittedProofBundleV2 {
        version: SUBMITTED_PROOF_BUNDLE_V2_VERSION,
        bundle_id: bundle(),
        pubky_lock_resource: resource(),
        reader_pubky: Some(reader()),
    };
    PaymentTaskSnapshotV1::new(
        submitted,
        [0; 32],
        PaymentPolicyV2 {
            recipient_pubky: creator(),
            amount: "50000".to_owned(),
            asset: "BTC".to_owned(),
            minimum_confirmations: threshold,
        },
        ACCEPTED,
        300,
    )
    .unwrap()
}

fn sample(observed_at: time::OffsetDateTime, confirmations: u8) -> PaymentSampleV2 {
    PaymentSampleV2 {
        observed_at,
        status: if confirmations == 0 {
            PaymentSampleStatusV2::Detected
        } else {
            PaymentSampleStatusV2::Confirmed
        },
        outpoint: Some(BitcoinOutpointV2 {
            txid: "ab".repeat(32),
            vout: 1,
        }),
        confirmations,
        amount_matched: true,
    }
}

fn status(
    request_state: PaymentRequestStateV2,
    payment_state: PaymentStateV2,
    first: Option<PaymentSampleV2>,
) -> PaymentProviderStatus {
    PaymentProviderStatus {
        creator: creator(),
        bundle_id: bundle(),
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

#[test]
fn exact_deadline_threshold_evidence_is_sticky_across_current_reorg() {
    let mut state = snapshot(1);
    state
        .apply_create_response(
            ACCEPTED + time::Duration::seconds(1),
            ACCEPTED + time::Duration::seconds(60),
            DEADLINE,
        )
        .unwrap();
    let first = sample(DEADLINE, 1);
    state
        .apply_status(status(
            PaymentRequestStateV2::Accepted,
            PaymentStateV2::Confirmed,
            Some(first.clone()),
        ))
        .unwrap();
    reduce_payment_snapshot(&mut state, DEADLINE + time::Duration::seconds(30)).unwrap();
    assert_eq!(state.root, PaymentRootState::Satisfied);

    let mut regressed = state.clone();
    regressed.provider_status.as_mut().unwrap().current_sample = None;
    regressed.validate_progress_from(&state).unwrap();
    assert_eq!(
        regressed.provider_status.unwrap().first_threshold_satisfied,
        Some(first)
    );
}

#[test]
fn reducer_preserves_business_expiry_and_conflict_vocabulary() {
    for (request_state, payment_state, expected) in [
        (
            PaymentRequestStateV2::Rejected,
            PaymentStateV2::Undetected,
            PaymentRootState::Expired(VerificationTerminalReason::PaymentRequestRejected),
        ),
        (
            PaymentRequestStateV2::Canceled,
            PaymentStateV2::Undetected,
            PaymentRootState::Expired(VerificationTerminalReason::PaymentRequestCanceled),
        ),
        (
            PaymentRequestStateV2::ProposalExpired,
            PaymentStateV2::Undetected,
            PaymentRootState::Expired(VerificationTerminalReason::ProposalExpired),
        ),
        (
            PaymentRequestStateV2::Accepted,
            PaymentStateV2::Expired,
            PaymentRootState::Expired(VerificationTerminalReason::PaymentDeadlineExpired),
        ),
        (
            PaymentRequestStateV2::InvalidConflict,
            PaymentStateV2::Undetected,
            PaymentRootState::Failed(VerificationTerminalReason::VerificationConflict),
        ),
    ] {
        let mut state = snapshot(1);
        state.invoice = PaymentInvoiceState::Created {
            invoice_created_at: ACCEPTED + time::Duration::seconds(1),
            proposal_expires_at: ACCEPTED + time::Duration::seconds(60),
        };
        state.provider_status = Some(status(request_state, payment_state, None));
        reduce_payment_snapshot(&mut state, ACCEPTED + time::Duration::seconds(90)).unwrap();
        assert_eq!(state.root, expected);
    }
}

#[test]
fn unresolved_provider_work_fails_only_at_deadline() {
    let mut before = snapshot(1);
    reduce_payment_snapshot(&mut before, DEADLINE - time::Duration::microseconds(1)).unwrap();
    assert_eq!(before.root, PaymentRootState::Pending);

    reduce_payment_snapshot(&mut before, DEADLINE).unwrap();
    assert_eq!(
        before.root,
        PaymentRootState::Failed(VerificationTerminalReason::VerificationUnavailableByDeadline)
    );
}

#[test]
fn contradictory_first_evidence_fails_closed_and_cannot_mutate() {
    let mut state = snapshot(1);
    state.invoice = PaymentInvoiceState::Created {
        invoice_created_at: ACCEPTED + time::Duration::seconds(1),
        proposal_expires_at: ACCEPTED + time::Duration::seconds(60),
    };
    state.provider_status = Some(status(
        PaymentRequestStateV2::Proposed,
        PaymentStateV2::Confirmed,
        Some(sample(ACCEPTED + time::Duration::seconds(30), 1)),
    ));
    reduce_payment_snapshot(&mut state, ACCEPTED + time::Duration::seconds(31)).unwrap();
    assert_eq!(
        state.root,
        PaymentRootState::Failed(VerificationTerminalReason::InvalidVerifierResponse)
    );

    let mut changed = state.clone();
    changed
        .provider_status
        .as_mut()
        .unwrap()
        .first_threshold_satisfied
        .as_mut()
        .unwrap()
        .outpoint
        .as_mut()
        .unwrap()
        .vout = 2;
    assert!(changed.validate_progress_from(&state).is_err());
}

#[test]
fn provider_evidence_before_invoice_is_rejected() {
    let mut state = snapshot(1);
    state
        .apply_create_response(
            ACCEPTED + time::Duration::seconds(1),
            ACCEPTED + time::Duration::seconds(60),
            DEADLINE,
        )
        .unwrap();
    let evidence = sample(ACCEPTED, 1);
    assert!(
        state
            .apply_status(status(
                PaymentRequestStateV2::Accepted,
                PaymentStateV2::Confirmed,
                Some(evidence),
            ))
            .is_err()
    );
}

#[test]
fn exact_admission_binding_survives_mutable_payment_progress() {
    let pristine = snapshot(1);
    let mut progressed = pristine.clone();
    progressed
        .apply_create_response(
            ACCEPTED + time::Duration::seconds(1),
            ACCEPTED + time::Duration::seconds(60),
            DEADLINE,
        )
        .unwrap();
    progressed.operational_failure_count = 1;
    progressed.operational_failure_total = 1;
    progressed.operational_first_failure_at = Some(ACCEPTED + time::Duration::seconds(2));
    progressed.operational_last_failure_at = Some(ACCEPTED + time::Duration::seconds(2));
    progressed.operational_last_category =
        Some(locks_service::application::models::PaymentOperationalErrorCategory::Transport);
    progressed.next_due_at = Some(ACCEPTED + time::Duration::seconds(3));
    progressed.validate().unwrap();

    assert!(progressed.has_same_admission_binding(&pristine));

    let mut changed = pristine;
    changed.payment.amount = "50001".to_owned();
    changed.payment.amount_sats = 50_001;
    assert!(!progressed.has_same_admission_binding(&changed));
}
