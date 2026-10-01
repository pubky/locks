use async_trait::async_trait;
use locks_core::ids::{BundleId, CreatorPubky};
use locks_core::lock_policy::VerifierType;
use std::sync::Arc;

use crate::application::errors::ApplicationError;
use crate::application::models::{
    CriterionVerificationOutcome, CriterionVerificationRequest, VerificationTerminalReason,
};
use crate::application::ports::CriterionVerifier;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaykitPaymentRequestState {
    Proposed,
    ProposalExpired,
    Accepted,
    Rejected,
    Canceled,
    ProofSubmitted,
    ActiveRecurring,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaykitPaymentState {
    Undetected,
    Detected,
    Confirmed,
    Expired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PaykitPaymentStatus {
    pub request_state: PaykitPaymentRequestState,
    pub payment_state: PaykitPaymentState,
    pub confirmations: u32,
    pub amount_matched: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaykitPaymentStatusError {
    Unavailable,
    Conflict,
    InvalidResponse,
}

#[async_trait]
pub trait PaykitPaymentStatusClient: Send + Sync {
    async fn payment_request_status(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<PaykitPaymentStatus, PaykitPaymentStatusError>;
}

#[async_trait]
impl<C> PaykitPaymentStatusClient for Arc<C>
where
    C: PaykitPaymentStatusClient + ?Sized,
{
    async fn payment_request_status(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<PaykitPaymentStatus, PaykitPaymentStatusError> {
        (**self).payment_request_status(creator, bundle_id).await
    }
}

#[derive(Debug)]
pub struct PaykitPaymentVerifier<C> {
    client: C,
    minimum_confirmations: u32,
}

impl<C> PaykitPaymentVerifier<C> {
    pub fn new(client: C, minimum_confirmations: u32) -> Self {
        Self {
            client,
            minimum_confirmations,
        }
    }
}

#[async_trait]
impl<C> CriterionVerifier for PaykitPaymentVerifier<C>
where
    C: PaykitPaymentStatusClient,
{
    async fn verify(
        &self,
        request: CriterionVerificationRequest,
    ) -> Result<CriterionVerificationOutcome, ApplicationError> {
        let status = match self
            .client
            .payment_request_status(&request.creator, &request.bundle_id)
            .await
        {
            Ok(status) => Some(status),
            Err(PaykitPaymentStatusError::Conflict) => {
                return Err(ApplicationError::PaykitPaymentStatusConflict);
            }
            Err(PaykitPaymentStatusError::InvalidResponse) => {
                return Err(ApplicationError::PaykitPaymentStatusInvalidResponse);
            }
            Err(PaykitPaymentStatusError::Unavailable) => None,
        };
        Ok(payment_status_decision(
            status,
            self.minimum_confirmations,
            request,
        ))
    }
}

fn payment_status_decision(
    status: Option<PaykitPaymentStatus>,
    minimum_confirmations: u32,
    request: CriterionVerificationRequest,
) -> CriterionVerificationOutcome {
    let Some(status) = status else {
        return CriterionVerificationOutcome::Pending;
    };
    let terminal_reason = match status.request_state {
        PaykitPaymentRequestState::Rejected => {
            Some(VerificationTerminalReason::PaymentRequestRejected)
        }
        PaykitPaymentRequestState::Canceled => {
            Some(VerificationTerminalReason::PaymentRequestCanceled)
        }
        PaykitPaymentRequestState::ProposalExpired => {
            Some(VerificationTerminalReason::ProposalExpired)
        }
        PaykitPaymentRequestState::Accepted
        | PaykitPaymentRequestState::ProofSubmitted
        | PaykitPaymentRequestState::ActiveRecurring
            if status.payment_state == PaykitPaymentState::Expired =>
        {
            Some(VerificationTerminalReason::PaymentDeadlineExpired)
        }
        _ => None,
    };
    if let Some(reason) = terminal_reason {
        return CriterionVerificationOutcome::TerminalUnsatisfied(reason);
    }
    let satisfied = status.amount_matched
        && match (minimum_confirmations, status.payment_state) {
            (0, PaykitPaymentState::Detected | PaykitPaymentState::Confirmed) => true,
            (required, PaykitPaymentState::Confirmed) => status.confirmations >= required,
            _ => false,
        }
        && matches!(
            status.request_state,
            PaykitPaymentRequestState::Accepted
                | PaykitPaymentRequestState::ProofSubmitted
                | PaykitPaymentRequestState::ActiveRecurring
        );
    if satisfied {
        CriterionVerificationOutcome::Satisfied(
            locks_core::verification::CriterionVerificationResult {
                criterion_id: request.criterion.criterion_id,
                satisfied: true,
                verified_at: request.verified_at,
                verified_by: request.verified_by,
                verifier_type: VerifierType::PaykitPayment,
            },
        )
    } else {
        CriterionVerificationOutcome::Pending
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use serde_json::json;
    use time::macros::datetime;

    use locks_core::ids::{BundleId, CreatorPubky, LockId, LockServerPubky};
    use locks_core::lock_policy::{Criterion, VerifierType};
    use locks_core::verification::Proof;

    use super::{
        PaykitPaymentRequestState, PaykitPaymentState, PaykitPaymentStatus,
        PaykitPaymentStatusClient, PaykitPaymentStatusError, PaykitPaymentVerifier,
    };
    use crate::application::errors::ApplicationError;
    use crate::application::models::{
        CriterionVerificationOutcome, CriterionVerificationRequest, VerificationTerminalReason,
    };
    use crate::application::ports::CriterionVerifier;

    const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";
    const LOCK_ID: &str = "000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG";

    #[tokio::test]
    async fn terminal_request_state_wins_over_confirmed_payment_evidence() {
        for (request_state, reason) in [
            (
                PaykitPaymentRequestState::Rejected,
                VerificationTerminalReason::PaymentRequestRejected,
            ),
            (
                PaykitPaymentRequestState::Canceled,
                VerificationTerminalReason::PaymentRequestCanceled,
            ),
            (
                PaykitPaymentRequestState::ProposalExpired,
                VerificationTerminalReason::ProposalExpired,
            ),
        ] {
            let verifier = verifier(
                PaykitPaymentStatus {
                    request_state,
                    payment_state: PaykitPaymentState::Confirmed,
                    confirmations: 6,
                    amount_matched: true,
                },
                1,
            );

            assert_eq!(
                verifier.verify(request()).await.unwrap(),
                CriterionVerificationOutcome::TerminalUnsatisfied(reason)
            );
        }
    }

    #[tokio::test]
    async fn accepted_expired_payment_is_terminal_without_using_local_time() {
        let verifier = verifier(
            PaykitPaymentStatus {
                request_state: PaykitPaymentRequestState::Accepted,
                payment_state: PaykitPaymentState::Expired,
                confirmations: 6,
                amount_matched: true,
            },
            1,
        );

        assert_eq!(
            verifier.verify(request()).await.unwrap(),
            CriterionVerificationOutcome::TerminalUnsatisfied(
                VerificationTerminalReason::PaymentDeadlineExpired
            )
        );
    }

    #[tokio::test]
    async fn zero_confirmations_detected_amount_matched_satisfies_payment() {
        let verifier = verifier(
            PaykitPaymentStatus {
                request_state: PaykitPaymentRequestState::Accepted,
                payment_state: PaykitPaymentState::Detected,
                confirmations: 0,
                amount_matched: true,
            },
            0,
        );

        let CriterionVerificationOutcome::Satisfied(result) =
            verifier.verify(request()).await.unwrap()
        else {
            panic!("detected matched payment should satisfy");
        };

        assert_eq!(result.criterion_id, "criterion-1");
        assert!(result.satisfied);
        assert_eq!(result.verifier_type, VerifierType::PaykitPayment);
        assert_eq!(
            verifier.client.requested_handles(),
            vec![(creator(), bundle_id())]
        );
    }

    #[tokio::test]
    async fn zero_confirmations_undetected_stays_pending() {
        let verifier = verifier(
            PaykitPaymentStatus {
                request_state: PaykitPaymentRequestState::Accepted,
                payment_state: PaykitPaymentState::Undetected,
                confirmations: 0,
                amount_matched: true,
            },
            0,
        );

        assert_eq!(
            verifier.verify(request()).await,
            Ok(CriterionVerificationOutcome::Pending)
        );
    }

    #[tokio::test]
    async fn confirmations_required_detected_stays_pending() {
        let verifier = verifier(
            PaykitPaymentStatus {
                request_state: PaykitPaymentRequestState::Accepted,
                payment_state: PaykitPaymentState::Detected,
                confirmations: 3,
                amount_matched: true,
            },
            1,
        );

        assert_eq!(
            verifier.verify(request()).await,
            Ok(CriterionVerificationOutcome::Pending)
        );
    }

    #[tokio::test]
    async fn confirmed_below_required_confirmations_stays_pending() {
        let verifier = verifier(
            PaykitPaymentStatus {
                request_state: PaykitPaymentRequestState::Accepted,
                payment_state: PaykitPaymentState::Confirmed,
                confirmations: 0,
                amount_matched: true,
            },
            1,
        );

        assert_eq!(
            verifier.verify(request()).await,
            Ok(CriterionVerificationOutcome::Pending)
        );
    }

    #[tokio::test]
    async fn confirmed_at_required_confirmations_satisfies_payment() {
        let verifier = verifier(
            PaykitPaymentStatus {
                request_state: PaykitPaymentRequestState::Accepted,
                payment_state: PaykitPaymentState::Confirmed,
                confirmations: 1,
                amount_matched: true,
            },
            1,
        );

        assert!(matches!(
            verifier.verify(request()).await.unwrap(),
            CriterionVerificationOutcome::Satisfied(result) if result.satisfied
        ));
    }

    #[tokio::test]
    async fn amount_mismatch_stays_pending_even_when_confirmed() {
        let verifier = verifier(
            PaykitPaymentStatus {
                request_state: PaykitPaymentRequestState::Accepted,
                payment_state: PaykitPaymentState::Confirmed,
                confirmations: 6,
                amount_matched: false,
            },
            1,
        );

        assert_eq!(
            verifier.verify(request()).await,
            Ok(CriterionVerificationOutcome::Pending)
        );
    }

    #[tokio::test]
    async fn status_client_errors_leave_task_pending() {
        let verifier = PaykitPaymentVerifier::new(FakeStatusClient::error(), 0);

        assert_eq!(
            verifier.verify(request()).await,
            Ok(CriterionVerificationOutcome::Pending)
        );
    }

    #[tokio::test]
    async fn status_conflict_is_preserved_as_operator_visible_error() {
        let verifier = PaykitPaymentVerifier::new(
            FakeStatusClient::error_with(PaykitPaymentStatusError::Conflict),
            0,
        );

        assert_eq!(
            verifier.verify(request()).await,
            Err(ApplicationError::PaykitPaymentStatusConflict)
        );
    }

    #[tokio::test]
    async fn invalid_status_response_fails_closed() {
        let verifier = PaykitPaymentVerifier::new(
            FakeStatusClient::error_with(PaykitPaymentStatusError::InvalidResponse),
            0,
        );

        assert_eq!(
            verifier.verify(request()).await,
            Err(ApplicationError::PaykitPaymentStatusInvalidResponse)
        );
    }

    fn verifier(
        status: PaykitPaymentStatus,
        minimum_confirmations: u32,
    ) -> PaykitPaymentVerifier<FakeStatusClient> {
        PaykitPaymentVerifier::new(FakeStatusClient::status(status), minimum_confirmations)
    }

    fn request() -> CriterionVerificationRequest {
        CriterionVerificationRequest {
            bundle_id: bundle_id(),
            creator: creator(),
            lock_id: LockId::from_str(LOCK_ID).unwrap(),
            criterion: Criterion {
                criterion_id: "criterion-1".to_owned(),
                verifier_type: VerifierType::PaykitPayment,
                params: json!({
                    "recipient_pubky": "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
                    "amount": "50000",
                    "asset": "BTC"
                }),
            },
            proof: Proof {
                criterion_id: "criterion-1".to_owned(),
                verifier_type: VerifierType::PaykitPayment,
                payload: json!({}),
            },
            verified_by: LockServerPubky::from_str(
                "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
            )
            .unwrap(),
            verified_at: datetime!(2026-05-29 12:00:00 UTC),
        }
    }

    fn bundle_id() -> BundleId {
        BundleId::from_str(BUNDLE_ID).unwrap()
    }

    fn creator() -> CreatorPubky {
        CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
    }

    #[derive(Debug)]
    struct FakeStatusClient {
        response: Result<PaykitPaymentStatus, PaykitPaymentStatusError>,
        requested_handles: Mutex<Vec<(CreatorPubky, BundleId)>>,
    }

    impl FakeStatusClient {
        fn status(status: PaykitPaymentStatus) -> Self {
            Self {
                response: Ok(status),
                requested_handles: Mutex::new(Vec::new()),
            }
        }

        fn error() -> Self {
            Self::error_with(PaykitPaymentStatusError::Unavailable)
        }

        fn error_with(error: PaykitPaymentStatusError) -> Self {
            Self {
                response: Err(error),
                requested_handles: Mutex::new(Vec::new()),
            }
        }

        fn requested_handles(&self) -> Vec<(CreatorPubky, BundleId)> {
            self.requested_handles.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl PaykitPaymentStatusClient for FakeStatusClient {
        async fn payment_request_status(
            &self,
            creator: &CreatorPubky,
            bundle_id: &BundleId,
        ) -> Result<PaykitPaymentStatus, PaykitPaymentStatusError> {
            self.requested_handles
                .lock()
                .unwrap()
                .push((creator.clone(), bundle_id.clone()));
            self.response
        }
    }
}
