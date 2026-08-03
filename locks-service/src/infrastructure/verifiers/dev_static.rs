use async_trait::async_trait;

use locks_core::lock_policy::VerifierType;
use locks_core::verification::CriterionVerificationResult;

use crate::application::errors::ApplicationError;
use crate::application::models::CriterionVerificationRequest;
use crate::application::ports::CriterionVerifier;

/// Development-only verifier controlled by `criterion.params.satisfied`.
#[derive(Debug, Clone, Copy, Default)]
pub struct DevStaticVerifier;

#[async_trait]
impl CriterionVerifier for DevStaticVerifier {
    async fn verify(
        &self,
        request: CriterionVerificationRequest,
    ) -> Result<CriterionVerificationResult, ApplicationError> {
        let satisfied = request
            .criterion
            .params
            .get("satisfied")
            .and_then(|value| value.as_bool())
            .ok_or_else(|| ApplicationError::Verifier {
                message: "dev-static criterion params.satisfied must be a boolean".to_owned(),
            })?;

        Ok(CriterionVerificationResult {
            criterion_id: request.criterion.criterion_id,
            satisfied,
            verified_at: request.verified_at,
            verified_by: request.verified_by,
            verifier_type: VerifierType::DevStatic,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use serde_json::json;
    use time::macros::datetime;

    use locks_core::ids::{BundleId, CreatorPubky, LockId, LockServerPubky};
    use locks_core::lock_policy::{Criterion, VerifierType};
    use locks_core::verification::Proof;

    use super::DevStaticVerifier;
    use crate::application::errors::ApplicationError;
    use crate::application::models::CriterionVerificationRequest;
    use crate::application::ports::CriterionVerifier;

    const LOCK_ID: &str = "000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG";

    #[tokio::test]
    async fn dev_static_returns_satisfied_result_when_param_is_true() {
        let result = DevStaticVerifier
            .verify(request(json!({ "satisfied": true })))
            .await
            .unwrap();

        assert_eq!(result.criterion_id, "criterion-1");
        assert!(result.satisfied);
        assert_eq!(result.verified_at, datetime!(2026-05-29 12:00:00 UTC));
        assert_eq!(result.verified_by, server());
        assert_eq!(result.verifier_type, VerifierType::DevStatic);
    }

    #[tokio::test]
    async fn dev_static_returns_unsatisfied_result_when_param_is_false() {
        let result = DevStaticVerifier
            .verify(request(json!({ "satisfied": false })))
            .await
            .unwrap();

        assert_eq!(result.criterion_id, "criterion-1");
        assert!(!result.satisfied);
        assert_eq!(result.verifier_type, VerifierType::DevStatic);
    }

    #[tokio::test]
    async fn dev_static_rejects_missing_satisfied_param() {
        let result = DevStaticVerifier.verify(request(json!({}))).await;

        assert_eq!(
            result,
            Err(ApplicationError::Verifier {
                message: "dev-static criterion params.satisfied must be a boolean".to_owned(),
            })
        );
    }

    #[tokio::test]
    async fn dev_static_rejects_non_boolean_satisfied_param() {
        let result = DevStaticVerifier
            .verify(request(json!({ "satisfied": "true" })))
            .await;

        assert_eq!(
            result,
            Err(ApplicationError::Verifier {
                message: "dev-static criterion params.satisfied must be a boolean".to_owned(),
            })
        );
    }

    fn request(params: serde_json::Value) -> CriterionVerificationRequest {
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
                params,
            },
            proof: Proof {
                criterion_id: "criterion-1".to_owned(),
                verifier_type: VerifierType::DevStatic,
                payload: json!({}),
            },
            verified_by: server(),
            verified_at: datetime!(2026-05-29 12:00:00 UTC),
        }
    }

    fn server() -> LockServerPubky {
        LockServerPubky::from_str("pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo")
            .unwrap()
    }
}
