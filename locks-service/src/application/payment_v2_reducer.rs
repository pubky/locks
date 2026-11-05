use time::OffsetDateTime;

use crate::application::models::{
    PaymentRequestStateV2, PaymentRootState, PaymentStateV2, PaymentTaskSnapshotV1,
    VerificationTerminalReason,
};

pub fn reduce_payment_snapshot(
    snapshot: &mut PaymentTaskSnapshotV1,
    now: OffsetDateTime,
) -> Result<(), String> {
    snapshot.validate()?;
    if snapshot.root != PaymentRootState::Pending {
        return Ok(());
    }

    let Some(status) = snapshot.provider_status.as_ref() else {
        if now >= snapshot.deadline_at {
            snapshot.root = PaymentRootState::Failed(
                VerificationTerminalReason::VerificationUnavailableByDeadline,
            );
        }
        snapshot.validate()?;
        return Ok(());
    };

    let has_first = status.first_threshold_satisfied.is_some();
    let accepted_state = matches!(
        status.request_state,
        PaymentRequestStateV2::Accepted
            | PaymentRequestStateV2::ProofSubmitted
            | PaymentRequestStateV2::ActiveRecurring
    );

    snapshot.root = if has_first {
        if accepted_state {
            PaymentRootState::Satisfied
        } else {
            PaymentRootState::Failed(VerificationTerminalReason::InvalidVerifierResponse)
        }
    } else {
        match (status.request_state, status.payment_state) {
            (PaymentRequestStateV2::Rejected, _) => {
                PaymentRootState::Expired(VerificationTerminalReason::PaymentRequestRejected)
            }
            (PaymentRequestStateV2::Canceled, _) => {
                PaymentRootState::Expired(VerificationTerminalReason::PaymentRequestCanceled)
            }
            (PaymentRequestStateV2::ProposalExpired, _) => {
                PaymentRootState::Expired(VerificationTerminalReason::ProposalExpired)
            }
            (
                PaymentRequestStateV2::Accepted
                | PaymentRequestStateV2::ProofSubmitted
                | PaymentRequestStateV2::ActiveRecurring,
                PaymentStateV2::Expired,
            ) => PaymentRootState::Expired(VerificationTerminalReason::PaymentDeadlineExpired),
            (PaymentRequestStateV2::InvalidConflict, _) => {
                PaymentRootState::Failed(VerificationTerminalReason::VerificationConflict)
            }
            (PaymentRequestStateV2::RecoveryRequired, _) if now >= snapshot.deadline_at => {
                PaymentRootState::Failed(
                    VerificationTerminalReason::VerificationUnavailableByDeadline,
                )
            }
            (PaymentRequestStateV2::RecoveryRequired, _) => PaymentRootState::Pending,
            (PaymentRequestStateV2::Proposed, PaymentStateV2::Undetected)
                if now < status.proposal_expires_at =>
            {
                PaymentRootState::Pending
            }
            (
                PaymentRequestStateV2::Accepted
                | PaymentRequestStateV2::ProofSubmitted
                | PaymentRequestStateV2::ActiveRecurring,
                PaymentStateV2::Undetected | PaymentStateV2::Detected | PaymentStateV2::Confirmed,
            ) if now < snapshot.deadline_at => PaymentRootState::Pending,
            (PaymentRequestStateV2::Proposed, PaymentStateV2::Undetected)
                if now >= snapshot.deadline_at =>
            {
                PaymentRootState::Failed(
                    VerificationTerminalReason::VerificationUnavailableByDeadline,
                )
            }
            (
                PaymentRequestStateV2::Accepted
                | PaymentRequestStateV2::ProofSubmitted
                | PaymentRequestStateV2::ActiveRecurring,
                PaymentStateV2::Undetected | PaymentStateV2::Detected | PaymentStateV2::Confirmed,
            ) => PaymentRootState::Failed(
                VerificationTerminalReason::VerificationUnavailableByDeadline,
            ),
            _ => PaymentRootState::Failed(VerificationTerminalReason::InvalidVerifierResponse),
        }
    };

    snapshot.validate()
}
