use async_trait::async_trait;
use locks_core::ids::{BundleId, CreatorPubky};
use locks_core::lock_policy::VerifierType;
use locks_core::verification::CriterionVerificationResult;
use std::sync::Arc;

use crate::application::errors::ApplicationError;
use crate::application::models::CriterionVerificationRequest;
use crate::application::ports::CriterionVerifier;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaykitPaymentStatusKind {
    Undetected,
    Detected,
    Confirmed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PaykitPaymentStatus {
    pub status: PaykitPaymentStatusKind,
    pub confirmations: u32,
    pub amount_matched: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PaykitPaymentStatusError;

#[async_trait]
pub trait PaykitPaymentStatusClient: Send + Sync {
    async fn transaction_status(
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
    async fn transaction_status(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<PaykitPaymentStatus, PaykitPaymentStatusError> {
        (**self).transaction_status(creator, bundle_id).await
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
    ) -> Result<CriterionVerificationResult, ApplicationError> {
        let status = self
            .client
            .transaction_status(&request.creator, &request.bundle_id)
            .await
            .map_err(|_| ApplicationError::VerificationDependencyUnavailable)?;
        if !payment_status_satisfies(status, self.minimum_confirmations) {
            return Err(ApplicationError::VerificationPending);
        }
        Ok(CriterionVerificationResult {
            criterion_id: request.criterion.criterion_id,
            satisfied: true,
            verified_at: request.verified_at,
            verified_by: request.verified_by,
            verifier_type: VerifierType::PaykitPayment,
        })
    }
}

fn payment_status_satisfies(status: PaykitPaymentStatus, minimum_confirmations: u32) -> bool {
    if !status.amount_matched {
        return false;
    }
    match (minimum_confirmations, status.status) {
        (0, PaykitPaymentStatusKind::Detected | PaykitPaymentStatusKind::Confirmed) => true,
        (required, PaykitPaymentStatusKind::Confirmed) => status.confirmations >= required,
        _ => false,
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
        PaykitPaymentStatus, PaykitPaymentStatusClient, PaykitPaymentStatusError,
        PaykitPaymentStatusKind, PaykitPaymentVerifier,
    };
    use crate::application::errors::ApplicationError;
    use crate::application::models::CriterionVerificationRequest;
    use crate::application::ports::CriterionVerifier;

    const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";
    const LOCK_ID: &str = "000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG";

    #[tokio::test]
    async fn zero_confirmations_detected_amount_matched_satisfies_payment() {
        let verifier = verifier(
            PaykitPaymentStatus {
                status: PaykitPaymentStatusKind::Detected,
                confirmations: 0,
                amount_matched: true,
            },
            0,
        );

        let result = verifier.verify(request()).await.unwrap();

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
                status: PaykitPaymentStatusKind::Undetected,
                confirmations: 0,
                amount_matched: true,
            },
            0,
        );

        assert_eq!(
            verifier.verify(request()).await,
            Err(ApplicationError::VerificationPending)
        );
    }

    #[tokio::test]
    async fn confirmations_required_detected_stays_pending() {
        let verifier = verifier(
            PaykitPaymentStatus {
                status: PaykitPaymentStatusKind::Detected,
                confirmations: 3,
                amount_matched: true,
            },
            1,
        );

        assert_eq!(
            verifier.verify(request()).await,
            Err(ApplicationError::VerificationPending)
        );
    }

    #[tokio::test]
    async fn confirmed_below_required_confirmations_stays_pending() {
        let verifier = verifier(
            PaykitPaymentStatus {
                status: PaykitPaymentStatusKind::Confirmed,
                confirmations: 0,
                amount_matched: true,
            },
            1,
        );

        assert_eq!(
            verifier.verify(request()).await,
            Err(ApplicationError::VerificationPending)
        );
    }

    #[tokio::test]
    async fn confirmed_at_required_confirmations_satisfies_payment() {
        let verifier = verifier(
            PaykitPaymentStatus {
                status: PaykitPaymentStatusKind::Confirmed,
                confirmations: 1,
                amount_matched: true,
            },
            1,
        );

        assert!(verifier.verify(request()).await.unwrap().satisfied);
    }

    #[tokio::test]
    async fn amount_mismatch_stays_pending_even_when_confirmed() {
        let verifier = verifier(
            PaykitPaymentStatus {
                status: PaykitPaymentStatusKind::Confirmed,
                confirmations: 6,
                amount_matched: false,
            },
            1,
        );

        assert_eq!(
            verifier.verify(request()).await,
            Err(ApplicationError::VerificationPending)
        );
    }

    #[tokio::test]
    async fn status_client_errors_remain_distinct_from_healthy_pending() {
        let verifier = PaykitPaymentVerifier::new(FakeStatusClient::error(), 0);

        assert_eq!(
            verifier.verify(request()).await,
            Err(ApplicationError::VerificationDependencyUnavailable)
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
                    "asset": "BTC",
                    "payment_in": 24
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
            Self {
                response: Err(PaykitPaymentStatusError),
                requested_handles: Mutex::new(Vec::new()),
            }
        }

        fn requested_handles(&self) -> Vec<(CreatorPubky, BundleId)> {
            self.requested_handles.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl PaykitPaymentStatusClient for FakeStatusClient {
        async fn transaction_status(
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
