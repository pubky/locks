use locks_core::lock_policy::VerifierType;

use crate::application::ports::{CriterionVerifier, CriterionVerifierRegistry};

/// Static verifier registry for explicitly wired verifier adapters.
#[derive(Default)]
pub struct StaticCriterionVerifierRegistry<'a> {
    dev_static: Option<&'a dyn CriterionVerifier>,
    paykit_payment: Option<&'a dyn CriterionVerifier>,
}

impl<'a> StaticCriterionVerifierRegistry<'a> {
    /// Creates an empty verifier registry.
    pub fn new() -> Self {
        Self {
            dev_static: None,
            paykit_payment: None,
        }
    }

    /// Registers the dev-static verifier adapter.
    pub fn with_dev_static(mut self, verifier: &'a dyn CriterionVerifier) -> Self {
        self.dev_static = Some(verifier);
        self
    }

    /// Registers the paykit-payment verifier adapter.
    pub fn with_paykit_payment(mut self, verifier: &'a dyn CriterionVerifier) -> Self {
        self.paykit_payment = Some(verifier);
        self
    }
}

impl CriterionVerifierRegistry for StaticCriterionVerifierRegistry<'_> {
    fn verifier_for(&self, verifier_type: VerifierType) -> Option<&dyn CriterionVerifier> {
        match verifier_type {
            VerifierType::DevStatic => self.dev_static,
            VerifierType::PaykitPayment => self.paykit_payment,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use async_trait::async_trait;
    use serde_json::json;
    use time::macros::datetime;

    use locks_core::ids::{BundleId, CreatorPubky, LockId, LockServerPubky};
    use locks_core::lock_policy::{Criterion, VerifierType};
    use locks_core::verification::CriterionVerificationResult;

    use crate::application::errors::ApplicationError;
    use crate::application::models::CriterionVerificationRequest;
    use crate::application::ports::{CriterionVerifier, CriterionVerifierRegistry};
    use crate::infrastructure::verifiers::registry::StaticCriterionVerifierRegistry;

    const LOCK_ID: &str = "000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG";

    #[test]
    fn static_registry_returns_registered_dev_static_verifier() {
        let verifier = FakeVerifier;
        let registry = StaticCriterionVerifierRegistry::new().with_dev_static(&verifier);

        assert!(registry.verifier_for(VerifierType::DevStatic).is_some());
    }

    #[test]
    fn static_registry_returns_registered_paykit_payment_verifier() {
        let verifier = FakeVerifier;
        let registry = StaticCriterionVerifierRegistry::new().with_paykit_payment(&verifier);

        assert!(registry.verifier_for(VerifierType::PaykitPayment).is_some());
    }

    #[test]
    fn empty_static_registry_returns_none_for_known_unregistered_verifier_type() {
        let registry = StaticCriterionVerifierRegistry::new();

        assert!(registry.verifier_for(VerifierType::DevStatic).is_none());
        assert!(registry.verifier_for(VerifierType::PaykitPayment).is_none());
    }

    #[tokio::test]
    async fn returned_verifier_is_the_registered_adapter() {
        let verifier = FakeVerifier;
        let registry = StaticCriterionVerifierRegistry::new().with_dev_static(&verifier);
        let registered = registry
            .verifier_for(VerifierType::DevStatic)
            .expect("registered dev-static verifier");

        let result = registered.verify(request()).await.unwrap();

        assert_eq!(result.criterion_id, "criterion-1");
        assert!(result.satisfied);
        assert_eq!(result.verifier_type, VerifierType::DevStatic);
    }

    fn request() -> CriterionVerificationRequest {
        CriterionVerificationRequest {
            bundle_id: BundleId::from_str("000G40R40M30E209185GR38E1W").unwrap(),
            creator: CreatorPubky::from_str(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            )
            .unwrap(),
            lock_id: LockId::from_str(LOCK_ID).unwrap(),
            criterion: Criterion {
                criterion_id: "criterion-1".to_owned(),
                verifier_type: VerifierType::DevStatic,
                params: json!({ "satisfied": true }),
            },
            proof: locks_core::verification::Proof {
                criterion_id: "criterion-1".to_owned(),
                verifier_type: VerifierType::DevStatic,
                payload: json!({}),
            },
            verified_by: LockServerPubky::from_str(
                "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
            )
            .unwrap(),
            verified_at: datetime!(2026-05-29 12:00:00 UTC),
        }
    }

    struct FakeVerifier;

    #[async_trait]
    impl CriterionVerifier for FakeVerifier {
        async fn verify(
            &self,
            request: CriterionVerificationRequest,
        ) -> Result<CriterionVerificationResult, ApplicationError> {
            Ok(CriterionVerificationResult {
                criterion_id: request.criterion.criterion_id,
                satisfied: true,
                verified_at: request.verified_at,
                verified_by: request.verified_by,
                verifier_type: request.criterion.verifier_type,
            })
        }
    }
}
