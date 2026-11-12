use locks_core::verification::SatisfiedByV2;
use time::OffsetDateTime;

use crate::application::graph0conf_reducer::reduce_graph_snapshot;
use crate::application::models::{
    GraphDeadlineResult, GraphRootState, MixedRootState, MixedTaskSnapshotV1,
    PaymentRequestStateV2, PaymentRootState, PaymentStateV2, VerificationTerminalReason,
};

pub fn reduce_mixed_snapshot(
    snapshot: &mut MixedTaskSnapshotV1,
    now: OffsetDateTime,
) -> Result<(), String> {
    snapshot.validate()?;
    if snapshot.root != MixedRootState::Pending {
        return Ok(());
    }

    let current = snapshot.clone();
    let Some(status) = snapshot.payment.provider_status.as_ref() else {
        if now >= snapshot.deadline_at {
            snapshot.root = MixedRootState::Failed(
                VerificationTerminalReason::VerificationUnavailableByDeadline,
            );
        }
        snapshot.validate_progress_from(&current)?;
        return Ok(());
    };

    let eligible = matches!(
        status.request_state,
        PaymentRequestStateV2::Accepted
            | PaymentRequestStateV2::ProofSubmitted
            | PaymentRequestStateV2::ActiveRecurring
    );
    let has_threshold = status.first_threshold_satisfied.is_some();

    if has_threshold && !eligible {
        snapshot.root = MixedRootState::Failed(VerificationTerminalReason::InvalidVerifierResponse);
        snapshot.validate_progress_from(&current)?;
        return Ok(());
    }

    snapshot.root = match (status.request_state, status.payment_state) {
        (PaymentRequestStateV2::Rejected, _) => {
            MixedRootState::Expired(VerificationTerminalReason::PaymentRequestRejected)
        }
        (PaymentRequestStateV2::Canceled, _) => {
            MixedRootState::Expired(VerificationTerminalReason::PaymentRequestCanceled)
        }
        (PaymentRequestStateV2::ProposalExpired, _) => {
            MixedRootState::Expired(VerificationTerminalReason::ProposalExpired)
        }
        (PaymentRequestStateV2::InvalidConflict, _) => {
            MixedRootState::Failed(VerificationTerminalReason::VerificationConflict)
        }
        (
            PaymentRequestStateV2::Accepted
            | PaymentRequestStateV2::ProofSubmitted
            | PaymentRequestStateV2::ActiveRecurring,
            PaymentStateV2::Expired,
        ) => MixedRootState::Expired(VerificationTerminalReason::PaymentDeadlineExpired),
        _ => MixedRootState::Pending,
    };
    if snapshot.root != MixedRootState::Pending {
        snapshot.validate_progress_from(&current)?;
        return Ok(());
    }

    if !eligible {
        if now >= snapshot.deadline_at {
            snapshot.root = MixedRootState::Failed(
                VerificationTerminalReason::VerificationUnavailableByDeadline,
            );
        }
        snapshot.validate_progress_from(&current)?;
        return Ok(());
    }

    let mut payment = snapshot.payment_snapshot()?;
    payment.root = if has_threshold {
        PaymentRootState::Satisfied
    } else {
        PaymentRootState::Pending
    };
    snapshot.replace_payment_snapshot(payment);

    let mut graph = snapshot.graph_snapshot()?;
    reduce_graph_snapshot(&mut graph, now)?;
    snapshot.replace_graph_snapshot(graph);

    let graph_satisfied = snapshot.graph.root == GraphRootState::Satisfied;
    if has_threshold || graph_satisfied {
        snapshot.root = MixedRootState::Satisfied;
        snapshot.satisfied_by = match (has_threshold, graph_satisfied) {
            (true, true) => vec![SatisfiedByV2::Payment, SatisfiedByV2::ZeroConfGraph],
            (true, false) => vec![SatisfiedByV2::Payment],
            (false, true) => vec![SatisfiedByV2::ZeroConfGraph],
            (false, false) => unreachable!(),
        };
    } else if now >= snapshot.deadline_at {
        snapshot.root = if snapshot.payment.operational_failure_count > 0 {
            MixedRootState::Failed(VerificationTerminalReason::VerificationUnavailableByDeadline)
        } else {
            match snapshot.graph.root {
                GraphRootState::FailedInvalidResponse => {
                    MixedRootState::Failed(VerificationTerminalReason::InvalidVerifierResponse)
                }
                GraphRootState::Expired(GraphDeadlineResult::False) => MixedRootState::Expired(
                    VerificationTerminalReason::GraphConditionNotObservedByDeadline,
                ),
                GraphRootState::Expired(GraphDeadlineResult::DependencyUnavailable) => {
                    MixedRootState::Failed(
                        VerificationTerminalReason::VerificationUnavailableByDeadline,
                    )
                }
                GraphRootState::Pending => MixedRootState::Failed(
                    VerificationTerminalReason::VerificationUnavailableByDeadline,
                ),
                GraphRootState::Satisfied => unreachable!(),
            }
        };
    }

    snapshot.validate_progress_from(&current)
}
