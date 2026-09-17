use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use crate::ids::{BundleId, CreatorPubky, LockServerPubky, PubkyLockResource};
use crate::lock_policy::VerifierType;

/// Maximum accepted length of a [`ClientReference`] in bytes.
pub const CLIENT_REFERENCE_MAX_BYTES: usize = 64;

/// Errors returned when parsing a relying-service client reference.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ClientReferenceParseError {
    /// The reference was empty.
    #[error("client reference must not be empty")]
    Empty,
    /// The reference exceeded the byte-length bound.
    #[error("client reference must be at most 64 bytes")]
    TooLong,
    /// The reference contained a control character.
    #[error("client reference must not contain control characters")]
    ControlCharacter,
}

/// Opaque relying-service reference persisted with a submitted proof bundle.
///
/// A relying service mints this value server-side (for example per payment or
/// order instance) before the viewer submits the bundle, and requires exact
/// equality at completion. Locks never interprets the value: no trimming, no
/// case folding, no normalization of any kind. The value is bounded to
/// 1..=64 bytes of UTF-8 without control characters.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClientReference(String);

impl ClientReference {
    /// Returns the reference exactly as supplied.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ClientReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Serialize for ClientReference {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for ClientReference {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_str(&value).map_err(serde::de::Error::custom)
    }
}

impl FromStr for ClientReference {
    type Err = ClientReferenceParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty() {
            return Err(ClientReferenceParseError::Empty);
        }
        if value.len() > CLIENT_REFERENCE_MAX_BYTES {
            return Err(ClientReferenceParseError::TooLong);
        }
        if value.chars().any(char::is_control) {
            return Err(ClientReferenceParseError::ControlCharacter);
        }
        Ok(Self(value.to_owned()))
    }
}

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
    /// Opaque relying-service reference binding this bundle to one instance.
    ///
    /// Persisted immutably with the verification task and echoed on the
    /// handle-based lifecycle lookup. Locks never interprets the value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_reference: Option<ClientReference>,
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
        CLIENT_REFERENCE_MAX_BYTES, ClientReference, ClientReferenceParseError,
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
            client_reference: None,
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
    fn client_reference_round_trips_through_submitted_proof_bundle_json() {
        let mut submitted = submitted_proof_bundle_fixture();
        submitted.client_reference =
            Some(ClientReference::from_str("order-instance-018fc6ec").unwrap());

        let serialized = serde_json::to_value(&submitted).unwrap();
        assert_eq!(serialized["client_reference"], "order-instance-018fc6ec");

        let parsed: SubmittedProofBundle = serde_json::from_value(serialized).unwrap();
        assert_eq!(parsed, submitted);
    }

    #[test]
    fn submitted_proof_bundle_accepts_absent_client_reference() {
        let fixture = submitted_proof_bundle_fixture();
        let value = serde_json::to_value(&fixture).unwrap();
        assert!(value.get("client_reference").is_none());

        let parsed: SubmittedProofBundle = serde_json::from_value(value).unwrap();

        assert_eq!(parsed.client_reference, None);
        assert_eq!(parsed, fixture);
    }

    #[test]
    fn client_reference_accepts_single_byte_and_exactly_64_bytes() {
        assert_eq!(ClientReference::from_str("a").unwrap().as_str(), "a");
        let maxed = "b".repeat(CLIENT_REFERENCE_MAX_BYTES);
        assert_eq!(ClientReference::from_str(&maxed).unwrap().as_str(), maxed);
    }

    #[test]
    fn client_reference_accepts_64_bytes_of_multibyte_utf8() {
        // 32 two-byte characters are exactly 64 bytes.
        let value = "é".repeat(32);
        assert_eq!(value.len(), CLIENT_REFERENCE_MAX_BYTES);

        let reference = ClientReference::from_str(&value).unwrap();

        assert_eq!(reference.as_str(), value);
    }

    #[test]
    fn client_reference_rejects_empty_value() {
        assert_eq!(
            ClientReference::from_str(""),
            Err(ClientReferenceParseError::Empty)
        );
    }

    #[test]
    fn client_reference_rejects_65_bytes() {
        let too_long = "c".repeat(CLIENT_REFERENCE_MAX_BYTES + 1);

        assert_eq!(
            ClientReference::from_str(&too_long),
            Err(ClientReferenceParseError::TooLong)
        );
        // 33 two-byte characters exceed the byte bound at 66 bytes.
        let multibyte_too_long = "é".repeat(33);
        assert_eq!(
            ClientReference::from_str(&multibyte_too_long),
            Err(ClientReferenceParseError::TooLong)
        );
    }

    #[test]
    fn client_reference_rejects_control_characters() {
        for value in ["order\n1", "order\t1", "order\u{7}1", "\u{0}order"] {
            assert_eq!(
                ClientReference::from_str(value),
                Err(ClientReferenceParseError::ControlCharacter)
            );
        }
    }

    #[test]
    fn client_reference_deserialization_validates_like_construction() {
        assert_eq!(
            serde_json::from_value::<ClientReference>(json!("order-instance-018fc6ec"))
                .unwrap()
                .as_str(),
            "order-instance-018fc6ec"
        );
        assert!(serde_json::from_value::<ClientReference>(json!("")).is_err());
        assert!(serde_json::from_value::<ClientReference>(json!("order\n1")).is_err());
        assert!(serde_json::from_value::<ClientReference>(json!("c".repeat(65).as_str())).is_err());
    }

    #[test]
    fn submitted_proof_bundle_rejects_invalid_client_reference() {
        let mut value = serde_json::to_value(submitted_proof_bundle_fixture()).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("client_reference".to_owned(), json!("order\n1"));

        let result = serde_json::from_value::<SubmittedProofBundle>(value);

        assert!(result.is_err());
    }

    #[test]
    fn client_reference_does_not_trim_or_fold_case() {
        let reference = ClientReference::from_str("  Order-ABC  ").unwrap();

        assert_eq!(reference.as_str(), "  Order-ABC  ");
        assert_ne!(
            ClientReference::from_str("Order-ABC").unwrap(),
            ClientReference::from_str("order-abc").unwrap()
        );
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
