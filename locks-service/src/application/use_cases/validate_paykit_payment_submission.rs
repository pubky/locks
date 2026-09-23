use locks_core::lock_policy::VerifierType;
use locks_core::verification::SubmittedProofBundle;

use crate::application::errors::ApplicationError;
use crate::application::ports::ContentLockRepository;
use crate::application::use_cases::entitlement_check::verify_content_lock_identity;

/// Canonical preflight input for a submitted Paykit payment proof.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatePaykitPaymentSubmissionRequest {
    /// Payment proof bundle to bind to its referenced public content lock.
    pub submitted_proof_bundle: SubmittedProofBundle,
}

/// Validates payment proof identity before invoice side effects are allowed.
pub struct ValidatePaykitPaymentSubmissionUseCase<'a> {
    content_locks: &'a dyn ContentLockRepository,
}

impl<'a> ValidatePaykitPaymentSubmissionUseCase<'a> {
    /// Creates a canonical payment-submission validator.
    pub fn new(content_locks: &'a dyn ContentLockRepository) -> Self {
        Self { content_locks }
    }

    /// Loads the referenced lock and requires an exact payment criterion match.
    pub async fn execute(
        &self,
        request: ValidatePaykitPaymentSubmissionRequest,
    ) -> Result<(), ApplicationError> {
        let submitted = request.submitted_proof_bundle;
        let content_lock = self
            .content_locks
            .get_content_lock(
                submitted.pubky_lock_resource.creator(),
                submitted.pubky_lock_resource.content_lock_path(),
            )
            .await?
            .ok_or(ApplicationError::ContentLockUnavailable)?;

        verify_content_lock_identity(
            &content_lock,
            submitted.pubky_lock_resource.content_lock_path(),
        )
        .map_err(|_| ApplicationError::InvalidPaykitPaymentSubmission)?;
        content_lock
            .validate_paykit_payment_v1_policy()
            .map_err(|_| ApplicationError::InvalidPaykitPaymentSubmission)?;

        let [proof] = submitted.proofs.as_slice() else {
            return Err(ApplicationError::InvalidPaykitPaymentSubmission);
        };
        if proof.verifier_type != VerifierType::PaykitPayment {
            return Err(ApplicationError::InvalidPaykitPaymentSubmission);
        }
        let criterion_matches = content_lock.criteria.iter().any(|criterion| {
            criterion.criterion_id == proof.criterion_id
                && criterion.verifier_type == VerifierType::PaykitPayment
        });
        if !criterion_matches {
            return Err(ApplicationError::InvalidPaykitPaymentSubmission);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::str::FromStr;

    use async_trait::async_trait;
    use serde_json::json;
    use time::macros::datetime;

    use locks_core::ids::{
        BundleId, ContentLockPath, CreatorPubky, GuardedResourceHash, LockServerPubky,
        PubkyLockResource,
    };
    use locks_core::lock_policy::{
        AccessPolicy, CONTENT_LOCK_VERSION, ContentLock, Criterion, GuardedResource, LockLogic,
        LockServerConfig, VerifierType,
    };
    use locks_core::verification::{Proof, SUBMITTED_PROOF_BUNDLE_VERSION, SubmittedProofBundle};

    use super::{ValidatePaykitPaymentSubmissionRequest, ValidatePaykitPaymentSubmissionUseCase};
    use crate::application::errors::ApplicationError;
    use crate::application::ports::ContentLockRepository;

    const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
    const READER: &str = "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo";
    const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";

    #[tokio::test]
    async fn rejects_missing_canonical_content_lock() {
        let repository = FixedContentLockRepository(None);
        let use_case = ValidatePaykitPaymentSubmissionUseCase::new(&repository);

        let result = use_case
            .execute(ValidatePaykitPaymentSubmissionRequest {
                submitted_proof_bundle: submitted_bundle(&content_lock(), "payment"),
            })
            .await;

        assert_eq!(result, Err(ApplicationError::ContentLockUnavailable));
    }

    #[tokio::test]
    async fn rejects_unknown_payment_criterion() {
        let lock = content_lock();
        let repository = FixedContentLockRepository(Some(lock.clone()));
        let use_case = ValidatePaykitPaymentSubmissionUseCase::new(&repository);

        let result = use_case
            .execute(ValidatePaykitPaymentSubmissionRequest {
                submitted_proof_bundle: submitted_bundle(&lock, "missing"),
            })
            .await;

        assert_eq!(
            result,
            Err(ApplicationError::InvalidPaykitPaymentSubmission)
        );
    }

    #[tokio::test]
    async fn rejects_non_payment_proof() {
        let lock = content_lock();
        let repository = FixedContentLockRepository(Some(lock.clone()));
        let use_case = ValidatePaykitPaymentSubmissionUseCase::new(&repository);
        let mut submitted = submitted_bundle(&lock, "payment");
        submitted.proofs[0].verifier_type = VerifierType::DevStatic;

        let result = use_case
            .execute(ValidatePaykitPaymentSubmissionRequest {
                submitted_proof_bundle: submitted,
            })
            .await;

        assert_eq!(
            result,
            Err(ApplicationError::InvalidPaykitPaymentSubmission)
        );
    }

    #[tokio::test]
    async fn rejects_content_lock_identity_mismatch() {
        let original = content_lock();
        let submitted = submitted_bundle(&original, "payment");
        let mut changed = original;
        changed.created_at = datetime!(2026-07-14 12:01:00 UTC);
        let repository = FixedContentLockRepository(Some(changed));
        let use_case = ValidatePaykitPaymentSubmissionUseCase::new(&repository);

        let result = use_case
            .execute(ValidatePaykitPaymentSubmissionRequest {
                submitted_proof_bundle: submitted,
            })
            .await;

        assert_eq!(
            result,
            Err(ApplicationError::InvalidPaykitPaymentSubmission)
        );
    }

    #[tokio::test]
    async fn accepts_matching_canonical_payment_criterion() {
        let lock = content_lock();
        let repository = FixedContentLockRepository(Some(lock.clone()));
        let use_case = ValidatePaykitPaymentSubmissionUseCase::new(&repository);

        let result = use_case
            .execute(ValidatePaykitPaymentSubmissionRequest {
                submitted_proof_bundle: submitted_bundle(&lock, "payment"),
            })
            .await;

        assert_eq!(result, Ok(()));
    }

    #[tokio::test]
    async fn rejects_canonical_payment_recipient_that_is_not_the_creator() {
        let mut lock = content_lock();
        lock.criteria[0].params["recipient_pubky"] = serde_json::Value::String(READER.to_owned());
        let repository = FixedContentLockRepository(Some(lock.clone()));
        let use_case = ValidatePaykitPaymentSubmissionUseCase::new(&repository);

        let result = use_case
            .execute(ValidatePaykitPaymentSubmissionRequest {
                submitted_proof_bundle: submitted_bundle(&lock, "payment"),
            })
            .await;

        assert_eq!(
            result,
            Err(ApplicationError::InvalidPaykitPaymentSubmission)
        );
    }

    #[tokio::test]
    async fn rejects_mixed_canonical_payment_policy() {
        let mut lock = content_lock();
        lock.criteria.push(Criterion {
            criterion_id: "other".to_owned(),
            verifier_type: VerifierType::DevStatic,
            params: json!({ "satisfied": true }),
        });
        lock.lock_logic = LockLogic::All {
            criteria: vec!["payment".to_owned(), "other".to_owned()],
        };
        let repository = FixedContentLockRepository(Some(lock.clone()));
        let use_case = ValidatePaykitPaymentSubmissionUseCase::new(&repository);

        let result = use_case
            .execute(ValidatePaykitPaymentSubmissionRequest {
                submitted_proof_bundle: submitted_bundle(&lock, "payment"),
            })
            .await;

        assert_eq!(
            result,
            Err(ApplicationError::InvalidPaykitPaymentSubmission)
        );
    }

    struct FixedContentLockRepository(Option<ContentLock>);

    #[async_trait]
    impl ContentLockRepository for FixedContentLockRepository {
        async fn upsert_content_lock(
            &self,
            _creator: CreatorPubky,
            _content_lock_path: ContentLockPath,
            _content_lock: ContentLock,
        ) -> Result<(), ApplicationError> {
            unreachable!("validation tests do not write content locks")
        }

        async fn get_content_lock(
            &self,
            _creator: &CreatorPubky,
            _content_lock_path: &ContentLockPath,
        ) -> Result<Option<ContentLock>, ApplicationError> {
            Ok(self.0.clone())
        }

        async fn delete_content_lock(
            &self,
            _creator: &CreatorPubky,
            _content_lock_path: &ContentLockPath,
        ) -> Result<bool, ApplicationError> {
            unreachable!("validation must not delete content locks")
        }
    }

    fn content_lock() -> ContentLock {
        ContentLock {
            version: CONTENT_LOCK_VERSION,
            creator: creator(),
            primary_resource: Some(GuardedResource {
                path: "/priv/locks.app/content/article.txt".to_owned(),
                hash: GuardedResourceHash::from_bytes([7_u8; 32]),
                content_type: "text/plain".to_owned(),
                size: 12,
            }),
            secondary_resources: BTreeMap::new(),
            criteria: vec![Criterion {
                criterion_id: "payment".to_owned(),
                verifier_type: VerifierType::PaykitPayment,
                params: json!({
                    "recipient_pubky": CREATOR,
                    "amount": "50000",
                    "asset": "BTC",
                    "payment_in": 24
                }),
            }],
            lock_logic: LockLogic::All {
                criteria: vec!["payment".to_owned()],
            },
            access_policy: AccessPolicy {
                requested_credential_ttl_seconds: 900,
            },
            lock_server: LockServerConfig {
                override_: Some(LockServerPubky::from_str(CREATOR).unwrap()),
            },
            created_at: datetime!(2026-07-14 12:00:00 UTC),
        }
    }

    fn submitted_bundle(lock: &ContentLock, criterion_id: &str) -> SubmittedProofBundle {
        let content_lock_path = lock.content_lock_path().unwrap();
        SubmittedProofBundle {
            version: SUBMITTED_PROOF_BUNDLE_VERSION,
            bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
            pubky_lock_resource: PubkyLockResource::from_str(&format!(
                "{}{content_lock_path}",
                lock.creator
            ))
            .unwrap(),
            reader_public_key: Some(CreatorPubky::from_str(READER).unwrap()),
            proofs: vec![Proof {
                criterion_id: criterion_id.to_owned(),
                verifier_type: VerifierType::PaykitPayment,
                payload: json!({}),
            }],
        }
    }

    fn creator() -> CreatorPubky {
        CreatorPubky::from_str(CREATOR).unwrap()
    }
}
