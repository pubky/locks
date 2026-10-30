use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::ids::{BundleId, CreatorPubky, LockServerPubky, PubkyLockResource};
use crate::lock_policy::{
    CONTENT_LOCK_V2_VERSION, CONTENT_LOCK_VERSION, ContentLockPayload, ContentLockV2, VerifierType,
};

/// Supported v0 submitted proof bundle payload version.
pub const SUBMITTED_PROOF_BUNDLE_VERSION: u16 = 1;

/// Supported GRAPH-0CONF submitted proof bundle payload version.
pub const SUBMITTED_PROOF_BUNDLE_V2_VERSION: u16 = 2;

/// Supported v0 verified proof bundle payload version.
pub const VERIFIED_PROOF_BUNDLE_VERSION: u16 = 1;

/// Supported GRAPH-0CONF verified proof bundle payload version.
pub const VERIFIED_PROOF_BUNDLE_V2_VERSION: u16 = 2;

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

/// Closed version-dispatched submitted proof bundle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum SubmittedProofBundlePayload {
    V1(SubmittedProofBundle),
    V2(SubmittedProofBundleV2),
}

impl<'de> Deserialize<'de> for SubmittedProofBundlePayload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Wire {
            V1(SubmittedProofBundle),
            V2(SubmittedProofBundleV2),
        }

        match Wire::deserialize(deserializer)? {
            Wire::V1(bundle) if bundle.version == SUBMITTED_PROOF_BUNDLE_VERSION => {
                Ok(Self::V1(bundle))
            }
            Wire::V2(bundle) if bundle.version == SUBMITTED_PROOF_BUNDLE_V2_VERSION => {
                Ok(Self::V2(bundle))
            }
            Wire::V1(bundle) => Err(serde::de::Error::custom(format!(
                "unsupported submitted proof bundle version: {}",
                bundle.version
            ))),
            Wire::V2(bundle) => Err(serde::de::Error::custom(format!(
                "unsupported submitted proof bundle version: {}",
                bundle.version
            ))),
        }
    }
}

/// Closed GRAPH-0CONF submitted bundle. Verification evidence remains server-owned.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct SubmittedProofBundleV2 {
    pub version: u16,
    pub bundle_id: BundleId,
    pub pubky_lock_resource: PubkyLockResource,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reader_pubky: Option<CreatorPubky>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct SubmittedProofBundleV2Wire {
    version: u16,
    bundle_id: BundleId,
    pubky_lock_resource: PubkyLockResource,
    #[serde(default)]
    reader_pubky: Option<CreatorPubky>,
}

impl<'de> Deserialize<'de> for SubmittedProofBundleV2 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = SubmittedProofBundleV2Wire::deserialize(deserializer)?;
        if value.version != SUBMITTED_PROOF_BUNDLE_V2_VERSION {
            return Err(serde::de::Error::custom(format!(
                "unsupported submitted proof bundle v2 version: {}",
                value.version
            )));
        }
        Ok(Self {
            version: value.version,
            bundle_id: value.bundle_id,
            pubky_lock_resource: value.pubky_lock_resource,
            reader_pubky: value.reader_pubky,
        })
    }
}

/// Validates exact v1/v1 or v2/v2 dispatch and v2 reader identity placement.
pub fn validate_content_lock_submission(
    content_lock: &ContentLockPayload,
    submitted: &SubmittedProofBundlePayload,
) -> Result<(), VersionPairValidationError> {
    match (content_lock, submitted) {
        (ContentLockPayload::V1(lock), SubmittedProofBundlePayload::V1(bundle)) => {
            if lock.version != CONTENT_LOCK_VERSION
                || bundle.version != SUBMITTED_PROOF_BUNDLE_VERSION
            {
                return Err(VersionPairValidationError::UnsupportedVersion);
            }
            validate_lock_resource(
                &lock.creator,
                lock.lock_id()
                    .map_err(|_| VersionPairValidationError::InvalidContentLock)?,
                &bundle.pubky_lock_resource,
            )
        }
        (ContentLockPayload::V2(lock), SubmittedProofBundlePayload::V2(bundle)) => {
            lock.validate()
                .map_err(|_| VersionPairValidationError::InvalidContentLock)?;
            if lock.version != CONTENT_LOCK_V2_VERSION
                || bundle.version != SUBMITTED_PROOF_BUNDLE_V2_VERSION
            {
                return Err(VersionPairValidationError::UnsupportedVersion);
            }
            validate_lock_resource(
                &lock.creator,
                lock.lock_id()
                    .map_err(|_| VersionPairValidationError::InvalidContentLock)?,
                &bundle.pubky_lock_resource,
            )?;
            match (lock.has_graph(), bundle.reader_pubky.is_some()) {
                (false, true) | (true, false) => Ok(()),
                (false, false) => Err(VersionPairValidationError::PaymentReaderRequired),
                (true, true) => Err(VersionPairValidationError::GraphReaderForbidden),
            }
        }
        _ => Err(VersionPairValidationError::CrossVersion),
    }
}

fn validate_lock_resource(
    creator: &CreatorPubky,
    lock_id: crate::ids::LockId,
    resource: &PubkyLockResource,
) -> Result<(), VersionPairValidationError> {
    if resource.creator() != creator {
        return Err(VersionPairValidationError::ResourceCreatorMismatch);
    }
    if resource.lock_id() != &lock_id {
        return Err(VersionPairValidationError::ResourceLockIdMismatch);
    }
    Ok(())
}

/// Rejected content-lock/submission version or role pairing.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum VersionPairValidationError {
    #[error("content lock and submitted proof bundle versions must match exactly")]
    CrossVersion,
    #[error("unsupported wire version")]
    UnsupportedVersion,
    #[error("invalid v2 content lock")]
    InvalidContentLock,
    #[error("payment-only v2 requires reader_pubky")]
    PaymentReaderRequired,
    #[error("graph-containing v2 must omit reader_pubky")]
    GraphReaderForbidden,
    #[error("submitted pubky_lock_resource creator must match content lock creator")]
    ResourceCreatorMismatch,
    #[error("submitted pubky_lock_resource Lock ID must match content lock identity")]
    ResourceLockIdMismatch,
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

/// Closed version-dispatched verified entitlement publication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum VerifiedProofBundlePayload {
    V1(VerifiedProofBundle),
    V2(VerifiedProofBundleV2),
}

impl<'de> Deserialize<'de> for VerifiedProofBundlePayload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Wire {
            V1(VerifiedProofBundle),
            V2(VerifiedProofBundleV2),
        }

        match Wire::deserialize(deserializer)? {
            Wire::V1(bundle) if bundle.version == VERIFIED_PROOF_BUNDLE_VERSION => {
                Ok(Self::V1(bundle))
            }
            Wire::V2(bundle) if bundle.version == VERIFIED_PROOF_BUNDLE_V2_VERSION => {
                Ok(Self::V2(bundle))
            }
            Wire::V1(bundle) => Err(serde::de::Error::custom(format!(
                "unsupported verified proof bundle version: {}",
                bundle.version
            ))),
            Wire::V2(bundle) => Err(serde::de::Error::custom(format!(
                "unsupported verified proof bundle version: {}",
                bundle.version
            ))),
        }
    }
}

/// Closed policy paths that may satisfy a v2 content lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SatisfiedByV2 {
    Payment,
    Graph,
    ZeroConfGraph,
}

/// Minimal GRAPH-0CONF v2 entitlement publication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct VerifiedProofBundleV2 {
    pub version: u16,
    pub creator: CreatorPubky,
    pub reader: CreatorPubky,
    pub bundle_id: BundleId,
    pub pubky_lock_resource: PubkyLockResource,
    #[serde(with = "time::serde::rfc3339")]
    pub verified_at: time::OffsetDateTime,
    pub satisfied_by: Vec<SatisfiedByV2>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct VerifiedProofBundleV2Wire {
    version: u16,
    creator: CreatorPubky,
    reader: CreatorPubky,
    bundle_id: BundleId,
    pubky_lock_resource: PubkyLockResource,
    #[serde(with = "time::serde::rfc3339")]
    verified_at: time::OffsetDateTime,
    satisfied_by: Vec<SatisfiedByV2>,
}

impl<'de> Deserialize<'de> for VerifiedProofBundleV2 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = VerifiedProofBundleV2Wire::deserialize(deserializer)?;
        let verified = Self {
            version: value.version,
            creator: value.creator,
            reader: value.reader,
            bundle_id: value.bundle_id,
            pubky_lock_resource: value.pubky_lock_resource,
            verified_at: value.verified_at,
            satisfied_by: value.satisfied_by,
        };
        verified.validate().map_err(serde::de::Error::custom)?;
        Ok(verified)
    }
}

impl VerifiedProofBundleV2 {
    /// Validates publication identity and canonical non-empty satisfaction paths.
    pub fn validate(&self) -> Result<(), VerifiedProofBundleV2ValidationError> {
        if self.version != VERIFIED_PROOF_BUNDLE_V2_VERSION {
            return Err(VerifiedProofBundleV2ValidationError::UnsupportedVersion(
                self.version,
            ));
        }
        if self.pubky_lock_resource.creator() != &self.creator {
            return Err(VerifiedProofBundleV2ValidationError::CreatorMismatch);
        }
        let canonical = matches!(
            self.satisfied_by.as_slice(),
            [SatisfiedByV2::Payment]
                | [SatisfiedByV2::Graph]
                | [SatisfiedByV2::ZeroConfGraph]
                | [SatisfiedByV2::Payment, SatisfiedByV2::ZeroConfGraph]
        );
        if !canonical {
            return Err(VerifiedProofBundleV2ValidationError::InvalidSatisfiedBy);
        }
        Ok(())
    }
}

/// Validates a v2 publication against its exact content lock and policy mode.
pub fn validate_verified_content_lock_publication(
    content_lock: &ContentLockV2,
    verified: &VerifiedProofBundleV2,
) -> Result<(), VerifiedProofBundleV2ValidationError> {
    content_lock
        .validate()
        .map_err(|_| VerifiedProofBundleV2ValidationError::InvalidContentLock)?;
    verified.validate()?;
    if verified.creator != content_lock.creator
        || verified.pubky_lock_resource.creator() != &content_lock.creator
    {
        return Err(VerifiedProofBundleV2ValidationError::CreatorMismatch);
    }
    let lock_id = content_lock
        .lock_id()
        .map_err(|_| VerifiedProofBundleV2ValidationError::InvalidContentLock)?;
    if verified.pubky_lock_resource.lock_id() != &lock_id {
        return Err(VerifiedProofBundleV2ValidationError::LockIdMismatch);
    }

    let valid_paths = match (content_lock.payment.is_some(), content_lock.graph.is_some()) {
        (true, false) => matches!(verified.satisfied_by.as_slice(), [SatisfiedByV2::Payment]),
        (false, true) => matches!(verified.satisfied_by.as_slice(), [SatisfiedByV2::Graph]),
        (true, true) => matches!(
            verified.satisfied_by.as_slice(),
            [SatisfiedByV2::Payment]
                | [SatisfiedByV2::ZeroConfGraph]
                | [SatisfiedByV2::Payment, SatisfiedByV2::ZeroConfGraph]
        ),
        (false, false) => false,
    };
    if !valid_paths {
        return Err(VerifiedProofBundleV2ValidationError::InvalidSatisfiedBy);
    }
    Ok(())
}

/// Invalid GRAPH-0CONF v2 entitlement publication.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum VerifiedProofBundleV2ValidationError {
    #[error("unsupported verified proof bundle v2 version: {0}")]
    UnsupportedVersion(u16),
    #[error("verified bundle creator must match pubky_lock_resource creator")]
    CreatorMismatch,
    #[error("verified bundle pubky_lock_resource Lock ID must match content lock identity")]
    LockIdMismatch,
    #[error("invalid v2 content lock")]
    InvalidContentLock,
    #[error("satisfied_by must be a canonical non-empty approved path array")]
    InvalidSatisfiedBy,
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
            "{}/pub/app.locks/{LOCK_ID}.json",
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
