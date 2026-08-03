use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ids::{BundleId, CreatorPubky, LockServerPubky, PubkyLockResource};
use crate::lock_policy::VerifierType;

/// Supported v0 submitted proof bundle payload version.
pub const SUBMITTED_PROOF_BUNDLE_VERSION: u16 = 1;

/// Supported v0 verified proof bundle payload version.
pub const VERIFIED_PROOF_BUNDLE_VERSION: u16 = 1;

/// Viewer-submitted proof material for a content lock.
///
/// A submitted proof bundle is not an entitlement. It references a content lock
/// and carries viewer-provided verifier-specific proof payloads for one or more
/// criteria.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct SubmittedProofBundle {
    /// Top-level protocol payload version.
    pub version: u16,
    /// Viewer-generated durable bundle identifier.
    pub bundle_id: BundleId,
    /// Pubky resource for the content lock the viewer is attempting to satisfy.
    pub pubky_lock_resource: PubkyLockResource,
    /// Reader Pubky identity used by payment-backed invoice flows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reader_public_key: Option<CreatorPubky>,
    /// Viewer-submitted proofs keyed to content lock criteria.
    pub proofs: Vec<Proof>,
}

/// One viewer-submitted proof for one criterion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Proof {
    /// Criterion this proof is intended to satisfy.
    pub criterion_id: String,
    /// Protocol-facing verifier kind expected to evaluate this proof.
    pub verifier_type: VerifierType,
    /// Verifier-specific proof payload submitted by the viewer.
    pub payload: Value,
}

/// Stored entitlement record produced after successful verification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct VerifiedProofBundle {
    /// Top-level protocol payload version.
    pub version: u16,
    /// Viewer-generated bundle identifier anchoring the entitlement.
    pub bundle_id: BundleId,
    /// Pubky resource for the public content lock this entitlement references.
    pub pubky_lock_resource: PubkyLockResource,
    /// Minimal criterion-level entitlement evidence.
    pub verification_result: VerificationResult,
    /// Rule describing how long the entitlement remains usable.
    pub entitlement_lifetime: EntitlementLifetime,
}

/// Minimal criterion-level verification evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct VerificationResult {
    /// Criterion results sufficient to satisfy the lock logic.
    pub criteria: Vec<CriterionVerificationResult>,
}

/// Successful verification result for one criterion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct CriterionVerificationResult {
    /// Criterion identifier that was satisfied.
    pub criterion_id: String,
    /// Whether this criterion was satisfied.
    pub satisfied: bool,
    /// Timestamp when this criterion was verified.
    #[serde(with = "time::serde::rfc3339")]
    pub verified_at: time::OffsetDateTime,
    /// Lock Server identity that produced the verification result.
    pub verified_by: LockServerPubky,
    /// Protocol-facing verifier kind used for this criterion.
    pub verifier_type: VerifierType,
}

/// Lock-type-specific entitlement lifetime rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type", deny_unknown_fields)]
pub enum EntitlementLifetime {
    /// Entitlement remains usable until revoked or the referenced lock disappears/changes.
    Unbounded,
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use serde_json::json;
    use time::macros::datetime;

    use crate::ids::{BundleId, LockServerPubky, PubkyLockResource};
    use crate::lock_policy::VerifierType;
    use crate::verification::{
        CriterionVerificationResult, EntitlementLifetime, Proof, SUBMITTED_PROOF_BUNDLE_VERSION,
        SubmittedProofBundle, VERIFIED_PROOF_BUNDLE_VERSION, VerificationResult,
        VerifiedProofBundle,
    };

    const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";
    const LOCK_ID: &str = "000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG";

    fn test_pubky_identity() -> String {
        pubky::Keypair::random().public_key().to_string()
    }

    fn pubky_lock_resource_fixture() -> PubkyLockResource {
        PubkyLockResource::from_str(&format!(
            "{}/pub/locks.app/{LOCK_ID}.json",
            test_pubky_identity()
        ))
        .unwrap()
    }

    fn submitted_proof_bundle_fixture() -> SubmittedProofBundle {
        SubmittedProofBundle {
            version: SUBMITTED_PROOF_BUNDLE_VERSION,
            bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
            pubky_lock_resource: pubky_lock_resource_fixture(),
            reader_public_key: None,
            proofs: vec![Proof {
                criterion_id: "criterion-1".to_owned(),
                verifier_type: VerifierType::DevStatic,
                payload: json!({ "answer": "viewer-supplied" }),
            }],
        }
    }

    #[test]
    fn submitted_proof_bundle_serializes_exact_snake_case_json_shape() {
        let submitted = submitted_proof_bundle_fixture();

        let serialized = serde_json::to_value(&submitted).unwrap();
        let expected_resource = submitted.pubky_lock_resource.to_string();

        assert_eq!(
            serialized,
            json!({
                "version": SUBMITTED_PROOF_BUNDLE_VERSION,
                "bundle_id": BUNDLE_ID,
                "pubky_lock_resource": expected_resource,
                "proofs": [{
                    "criterion_id": "criterion-1",
                    "verifier_type": "dev-static",
                    "payload": { "answer": "viewer-supplied" }
                }]
            })
        );
    }

    #[test]
    fn submitted_proof_bundle_rejects_unknown_verifier_type() {
        let mut value = serde_json::to_value(submitted_proof_bundle_fixture()).unwrap();
        value["proofs"][0]["verifier_type"] = json!("not-supported");

        let result = serde_json::from_value::<SubmittedProofBundle>(value);

        assert!(result.is_err());
    }

    #[test]
    fn submitted_proof_bundle_rejects_unknown_top_level_fields() {
        let mut value = serde_json::to_value(submitted_proof_bundle_fixture()).unwrap();
        value.as_object_mut().unwrap().insert(
            "verification_result".to_owned(),
            json!({ "not": "an entitlement" }),
        );

        let result = serde_json::from_value::<SubmittedProofBundle>(value);

        assert!(result.is_err());
    }

    #[test]
    fn submitted_proof_bundle_requires_version() {
        let mut value = serde_json::to_value(submitted_proof_bundle_fixture()).unwrap();
        value.as_object_mut().unwrap().remove("version");

        let result = serde_json::from_value::<SubmittedProofBundle>(value);

        assert!(result.is_err());
    }

    #[test]
    fn proof_objects_do_not_require_version_fields() {
        let value = serde_json::to_value(submitted_proof_bundle_fixture()).unwrap();

        assert!(value.get("version").is_some());
        assert!(value["proofs"][0].get("version").is_none());
    }

    fn verified_proof_bundle_fixture() -> VerifiedProofBundle {
        VerifiedProofBundle {
            version: VERIFIED_PROOF_BUNDLE_VERSION,
            bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
            pubky_lock_resource: pubky_lock_resource_fixture(),
            verification_result: VerificationResult {
                criteria: vec![CriterionVerificationResult {
                    criterion_id: "criterion-1".to_owned(),
                    satisfied: true,
                    verified_at: datetime!(2026-05-29 12:30:00 UTC),
                    verified_by: LockServerPubky::from_str(&test_pubky_identity()).unwrap(),
                    verifier_type: VerifierType::DevStatic,
                }],
            },
            entitlement_lifetime: EntitlementLifetime::Unbounded,
        }
    }

    #[test]
    fn verified_proof_bundle_serializes_minimal_entitlement_evidence_shape() {
        let verified = verified_proof_bundle_fixture();

        let serialized = serde_json::to_value(&verified).unwrap();
        let expected_resource = verified.pubky_lock_resource.to_string();
        let expected_verified_by = verified.verification_result.criteria[0]
            .verified_by
            .to_string();

        assert_eq!(
            serialized,
            json!({
                "version": VERIFIED_PROOF_BUNDLE_VERSION,
                "bundle_id": BUNDLE_ID,
                "pubky_lock_resource": expected_resource,
                "verification_result": {
                    "criteria": [{
                        "criterion_id": "criterion-1",
                        "satisfied": true,
                        "verified_at": "2026-05-29T12:30:00Z",
                        "verified_by": expected_verified_by,
                        "verifier_type": "dev-static"
                    }]
                },
                "entitlement_lifetime": {
                    "type": "unbounded"
                }
            })
        );
    }

    #[test]
    fn verified_proof_bundle_rejects_unknown_verifier_type() {
        let mut value = serde_json::to_value(verified_proof_bundle_fixture()).unwrap();
        value["verification_result"]["criteria"][0]["verifier_type"] = json!("not-supported");

        let result = serde_json::from_value::<VerifiedProofBundle>(value);

        assert!(result.is_err());
    }

    #[test]
    fn verified_proof_bundle_rejects_raw_proof_and_arbitrary_metadata_fields() {
        let mut value = serde_json::to_value(verified_proof_bundle_fixture()).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("raw_proof".to_owned(), json!({ "secret": "not stored" }));

        let result = serde_json::from_value::<VerifiedProofBundle>(value);

        assert!(result.is_err());

        let mut value = serde_json::to_value(verified_proof_bundle_fixture()).unwrap();
        value["verification_result"]["criteria"][0]
            .as_object_mut()
            .unwrap()
            .insert("metadata".to_owned(), json!({ "extra": true }));

        let result = serde_json::from_value::<VerifiedProofBundle>(value);

        assert!(result.is_err());
    }

    #[test]
    fn verified_proof_bundle_requires_pubky_lock_resource() {
        let mut value = serde_json::to_value(verified_proof_bundle_fixture()).unwrap();
        value.as_object_mut().unwrap().remove("pubky_lock_resource");

        let result = serde_json::from_value::<VerifiedProofBundle>(value);

        assert!(result.is_err());
    }

    #[test]
    fn verification_result_nested_objects_do_not_require_version_fields() {
        let value = serde_json::to_value(verified_proof_bundle_fixture()).unwrap();

        assert!(value.get("version").is_some());
        assert!(value["verification_result"].get("version").is_none());
        assert!(
            value["verification_result"]["criteria"][0]
                .get("version")
                .is_none()
        );
        assert!(value["entitlement_lifetime"].get("version").is_none());
    }
}
