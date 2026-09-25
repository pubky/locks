use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use time::OffsetDateTime;

use crate::ids::{
    BundleId, ContentLockPath, CreatorPubky, GuardedResourceHash, LockHash, LockId, LockServerPubky,
};

/// Supported v0 content lock payload version.
pub const CONTENT_LOCK_VERSION: u16 = 1;

/// Canonical public Locks application namespace on a creator homeserver.
pub const PUBLIC_LOCKS_APP_PATH_PREFIX: &str = "/pub/locks.app/";

/// Canonical private Locks content namespace on a creator homeserver.
pub const PRIVATE_RESOURCE_CONTENT_PATH_PREFIX: &str = "/priv/locks.app/content/";

/// Canonical private Locks proof-bundle namespace on a creator homeserver.
pub const PRIVATE_PROOF_BUNDLE_PATH_PREFIX: &str = "/priv/locks.app/proofs/";

/// Canonical guarded resource content namespace for local creator publishing v0.
pub const GUARDED_RESOURCE_CONTENT_PATH_PREFIX: &str = PRIVATE_RESOURCE_CONTENT_PATH_PREFIX;

/// Builds the canonical private homeserver path for a verified proof bundle.
pub fn verified_proof_bundle_path(bundle_id: &BundleId) -> String {
    format!("{PRIVATE_PROOF_BUNDLE_PATH_PREFIX}{bundle_id}.json")
}

/// Public structured data that describes how to access one guarded resource.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ContentLock {
    /// Top-level protocol payload version.
    pub version: u16,
    /// Pubky identity of the content creator who owns this lock.
    pub creator: CreatorPubky,
    /// Primary guarded resource protected by this content lock, if the application has one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_resource: Option<GuardedResource>,
    /// Secondary guarded resources protected by this content lock, keyed by full guarded path.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub secondary_resources: BTreeMap<String, SecondaryGuardedResource>,
    /// Criteria that can satisfy the lock logic.
    pub criteria: Vec<Criterion>,
    /// Logic expression over criterion identifiers.
    pub lock_logic: LockLogic,
    /// Requested access-credential behavior.
    pub access_policy: AccessPolicy,
    /// Lock Server resolution settings for this content lock.
    pub lock_server: LockServerConfig,
    /// Timestamp when the content lock payload was created.
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

impl ContentLock {
    /// Returns the number of guarded resources protected by this content lock.
    pub fn resource_count(&self) -> usize {
        usize::from(self.primary_resource.is_some()) + self.secondary_resources.len()
    }

    /// Returns the aggregate guarded resource size protected by this content lock.
    pub fn total_resource_size(&self) -> Result<u64, ContentLockValidationError> {
        let mut total = 0u64;
        if let Some(primary_resource) = &self.primary_resource {
            total = total
                .checked_add(primary_resource.size)
                .ok_or(ContentLockValidationError::TotalResourceSizeOverflow)?;
        }
        for secondary_resource in self.secondary_resources.values() {
            total = total
                .checked_add(secondary_resource.size)
                .ok_or(ContentLockValidationError::TotalResourceSizeOverflow)?;
        }
        Ok(total)
    }

    /// Finds a guarded resource descriptor by full guarded resource path.
    pub fn resource_for_path(&self, path: &str) -> Option<GuardedResource> {
        if let Some(primary_resource) = &self.primary_resource
            && primary_resource.path == path
        {
            return Some(primary_resource.clone());
        }

        self.secondary_resources
            .get(path)
            .map(|secondary_resource| GuardedResource {
                path: path.to_owned(),
                hash: secondary_resource.hash,
                content_type: secondary_resource.content_type.clone(),
                size: secondary_resource.size,
            })
    }

    /// Validates resource-set invariants for this content lock.
    pub fn validate_resource_set(&self) -> Result<(), ContentLockValidationError> {
        if self.resource_count() == 0 {
            return Err(ContentLockValidationError::EmptyResourceSet);
        }

        if let Some(primary_resource) = &self.primary_resource {
            GuardedResource::new(
                primary_resource.path.clone(),
                primary_resource.hash,
                primary_resource.content_type.clone(),
                primary_resource.size,
            )
            .map_err(ContentLockValidationError::InvalidPrimaryResource)?;

            if self
                .secondary_resources
                .contains_key(&primary_resource.path)
            {
                return Err(ContentLockValidationError::DuplicatePrimarySecondaryPath {
                    path: primary_resource.path.clone(),
                });
            }
        }

        for (path, secondary_resource) in &self.secondary_resources {
            GuardedResource::new(
                path.clone(),
                secondary_resource.hash,
                secondary_resource.content_type.clone(),
                secondary_resource.size,
            )
            .map_err(|reason| {
                ContentLockValidationError::InvalidSecondaryResource {
                    path: path.clone(),
                    reason,
                }
            })?;
        }

        self.total_resource_size()?;
        Ok(())
    }

    /// Enforces the v1 policy shape for content locks that use `paykit-payment`.
    pub fn validate_paykit_payment_v1_policy(
        &self,
    ) -> Result<(), PaykitPaymentPolicyValidationError> {
        let Some(payment_criterion) = self
            .criteria
            .iter()
            .find(|criterion| criterion.verifier_type == VerifierType::PaykitPayment)
        else {
            return Ok(());
        };

        if self.criteria.len() != 1 {
            return Err(PaykitPaymentPolicyValidationError::MustBeOnlyCriterion);
        }
        payment_criterion
            .validate_params()
            .map_err(PaykitPaymentPolicyValidationError::InvalidParams)?;
        let recipient = payment_criterion
            .params
            .get("recipient_pubky")
            .and_then(Value::as_str)
            .and_then(|value| CreatorPubky::from_str(value).ok())
            .ok_or(PaykitPaymentPolicyValidationError::InvalidParams(
                PaykitPaymentParamsValidationError::InvalidRecipientPubky,
            ))?;
        if recipient != self.creator {
            return Err(PaykitPaymentPolicyValidationError::RecipientMustMatchCreator);
        }

        let criterion_id = &payment_criterion.criterion_id;
        let logic_matches = match &self.lock_logic {
            LockLogic::All { criteria } | LockLogic::Any { criteria } => {
                criteria.len() == 1 && criteria[0] == *criterion_id
            }
        };
        if !logic_matches {
            return Err(PaykitPaymentPolicyValidationError::InvalidLockLogic {
                criterion_id: criterion_id.clone(),
            });
        }

        Ok(())
    }

    /// Serializes this content lock to RFC 8785/JCS-compatible canonical JSON bytes.
    pub fn canonical_json_bytes(&self) -> serde_json::Result<Vec<u8>> {
        serde_json_canonicalizer::to_vec(self)
    }

    /// Serializes this content lock to an RFC 8785/JCS-compatible canonical JSON string.
    pub fn canonical_json_string(&self) -> serde_json::Result<String> {
        serde_json_canonicalizer::to_string(self)
    }

    /// Computes the BLAKE3 lock hash over this content lock's canonical JSON bytes.
    pub fn lock_hash(&self) -> serde_json::Result<LockHash> {
        let hash = blake3::hash(&self.canonical_json_bytes()?);
        Ok(LockHash::from_bytes(*hash.as_bytes()))
    }

    /// Derives the canonical lock identifier from this content lock's hash.
    pub fn lock_id(&self) -> serde_json::Result<LockId> {
        Ok(LockId::from_hash(self.lock_hash()?))
    }

    /// Derives the canonical public content lock path for this content lock.
    pub fn content_lock_path(&self) -> serde_json::Result<ContentLockPath> {
        Ok(ContentLockPath::from_lock_id(self.lock_id()?))
    }
}

/// Guarded resource metadata referenced by a content lock.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct GuardedResource {
    /// Creator-homeserver-relative path to the guarded resource.
    pub path: String,
    /// Hash of the guarded resource payload/version.
    pub hash: GuardedResourceHash,
    /// MIME content type for serving the guarded resource.
    pub content_type: String,
    /// Exact guarded resource byte length. Must be greater than zero.
    pub size: u64,
}

impl GuardedResource {
    /// Creates guarded resource metadata while enforcing protocol invariants.
    pub fn new(
        path: impl Into<String>,
        hash: GuardedResourceHash,
        content_type: impl Into<String>,
        size: u64,
    ) -> Result<Self, GuardedResourceValidationError> {
        let path = path.into();
        if !path.starts_with(GUARDED_RESOURCE_CONTENT_PATH_PREFIX)
            || path == GUARDED_RESOURCE_CONTENT_PATH_PREFIX
            || path.contains("..")
            || path.contains("//")
            || path.contains("://")
        {
            return Err(GuardedResourceValidationError::InvalidPath);
        }

        let content_type = content_type.into();
        content_type
            .parse::<mime::Mime>()
            .map_err(|_| GuardedResourceValidationError::InvalidContentType)?;

        if size == 0 {
            return Err(GuardedResourceValidationError::ZeroSize);
        }

        Ok(Self {
            path,
            hash,
            content_type,
            size,
        })
    }
}

/// Secondary guarded resource metadata referenced by a content lock.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct SecondaryGuardedResource {
    /// Hash of the guarded resource payload/version.
    pub hash: GuardedResourceHash,
    /// MIME content type for serving the guarded resource.
    pub content_type: String,
    /// Exact guarded resource byte length. Must be greater than zero.
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ContentLockValidationError {
    #[error("content lock must protect at least one guarded resource")]
    EmptyResourceSet,
    #[error("invalid primary guarded resource: {0}")]
    InvalidPrimaryResource(GuardedResourceValidationError),
    #[error("invalid secondary guarded resource at {path}: {reason}")]
    InvalidSecondaryResource {
        path: String,
        reason: GuardedResourceValidationError,
    },
    #[error("primary resource path duplicates secondary resource path: {path}")]
    DuplicatePrimarySecondaryPath { path: String },
    #[error("content lock total resource size overflowed u64")]
    TotalResourceSizeOverflow,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum GuardedResourceValidationError {
    #[error("guarded resource path must be under /priv/locks.app/content/")]
    InvalidPath,
    #[error("guarded resource content_type must be a valid MIME type")]
    InvalidContentType,
    #[error("guarded resource size must be greater than zero")]
    ZeroSize,
}

/// Protocol-facing verifier kind used to dispatch criterion verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum VerifierType {
    /// Development-only static verifier for first-slice workflow tests.
    DevStatic,
    /// Paykit-backed payment verifier.
    PaykitPayment,
}

impl fmt::Display for VerifierType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DevStatic => f.write_str("dev-static"),
            Self::PaykitPayment => f.write_str("paykit-payment"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PaykitPaymentParamsValidationError {
    #[error("paykit-payment params must be a JSON object")]
    NotObject,
    #[error("paykit-payment params missing field: {0}")]
    MissingField(&'static str),
    #[error("paykit-payment params contain unknown field: {0}")]
    UnknownField(String),
    #[error("paykit-payment recipient_pubky must be a valid Pubky public key")]
    InvalidRecipientPubky,
    #[error("paykit-payment amount must be a positive decimal integer string")]
    InvalidAmount,
    #[error("paykit-payment asset must be a non-empty string")]
    InvalidAsset,
    #[error("paykit-payment payment_in must be a positive whole-hour JSON u64")]
    InvalidPaymentIn,
}

/// Validated public parameters for a `paykit-payment` criterion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaykitPaymentParams {
    recipient_pubky: CreatorPubky,
    amount: String,
    asset: String,
    payment_in: u64,
}

impl PaykitPaymentParams {
    pub fn recipient_pubky(&self) -> &CreatorPubky {
        &self.recipient_pubky
    }

    pub fn amount(&self) -> &str {
        &self.amount
    }

    pub fn asset(&self) -> &str {
        &self.asset
    }

    pub fn payment_in(&self) -> u64 {
        self.payment_in
    }
}

/// Invalid v1 content-lock policy containing a `paykit-payment` criterion.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PaykitPaymentPolicyValidationError {
    /// Payment cannot be mixed with another criterion in the v1 policy.
    #[error("paykit-payment must be the content lock's only criterion")]
    MustBeOnlyCriterion,
    /// The sole payment criterion contains invalid public parameters.
    #[error("invalid paykit-payment criterion params: {0}")]
    InvalidParams(PaykitPaymentParamsValidationError),
    /// Payment recipient is not the content-lock creator.
    #[error("paykit-payment recipient_pubky must match the content lock creator")]
    RecipientMustMatchCreator,
    /// Lock logic does not reference exactly the sole payment criterion.
    #[error("paykit-payment lock logic must reference only criterion {criterion_id}")]
    InvalidLockLogic { criterion_id: String },
}

/// One verifier-dispatched access requirement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Criterion {
    /// Identifier unique within a content lock.
    pub criterion_id: String,
    /// Protocol-facing verifier kind, such as `dev-static`.
    pub verifier_type: VerifierType,
    /// Verifier-specific public parameters.
    pub params: Value,
}

impl Criterion {
    /// Validates verifier-specific public criterion params.
    pub fn validate_params(&self) -> Result<(), PaykitPaymentParamsValidationError> {
        self.paykit_payment_params().map(|_| ())
    }

    /// Returns typed parameters when this is a `paykit-payment` criterion.
    pub fn paykit_payment_params(
        &self,
    ) -> Result<Option<PaykitPaymentParams>, PaykitPaymentParamsValidationError> {
        match self.verifier_type {
            VerifierType::DevStatic => Ok(None),
            VerifierType::PaykitPayment => validate_paykit_payment_params(&self.params).map(Some),
        }
    }
}

fn validate_paykit_payment_params(
    params: &Value,
) -> Result<PaykitPaymentParams, PaykitPaymentParamsValidationError> {
    let object = params
        .as_object()
        .ok_or(PaykitPaymentParamsValidationError::NotObject)?;

    for key in object.keys() {
        if !matches!(
            key.as_str(),
            "recipient_pubky" | "amount" | "asset" | "payment_in"
        ) {
            return Err(PaykitPaymentParamsValidationError::UnknownField(
                key.clone(),
            ));
        }
    }

    let recipient_pubky = object
        .get("recipient_pubky")
        .and_then(Value::as_str)
        .ok_or(PaykitPaymentParamsValidationError::MissingField(
            "recipient_pubky",
        ))?;
    let recipient_pubky = CreatorPubky::from_str(recipient_pubky)
        .map_err(|_| PaykitPaymentParamsValidationError::InvalidRecipientPubky)?;

    let amount = object
        .get("amount")
        .ok_or(PaykitPaymentParamsValidationError::MissingField("amount"))?
        .as_str()
        .ok_or(PaykitPaymentParamsValidationError::InvalidAmount)?;
    if amount.is_empty()
        || !amount.bytes().all(|byte| byte.is_ascii_digit())
        || !amount.bytes().any(|byte| byte != b'0')
    {
        return Err(PaykitPaymentParamsValidationError::InvalidAmount);
    }

    let asset = object
        .get("asset")
        .ok_or(PaykitPaymentParamsValidationError::MissingField("asset"))?
        .as_str()
        .ok_or(PaykitPaymentParamsValidationError::InvalidAsset)?;
    if asset.is_empty() {
        return Err(PaykitPaymentParamsValidationError::InvalidAsset);
    }

    let payment_in = object
        .get("payment_in")
        .ok_or(PaykitPaymentParamsValidationError::MissingField(
            "payment_in",
        ))?
        .as_u64()
        .filter(|payment_in| *payment_in > 0)
        .ok_or(PaykitPaymentParamsValidationError::InvalidPaymentIn)?;

    Ok(PaykitPaymentParams {
        recipient_pubky,
        amount: amount.to_owned(),
        asset: asset.to_owned(),
        payment_in,
    })
}

/// Logic expression over criterion identifiers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type", deny_unknown_fields)]
pub enum LockLogic {
    /// All listed criteria must be satisfied.
    All {
        /// Criterion identifiers that must all be satisfied.
        criteria: Vec<String>,
    },
    /// At least one listed criterion must be satisfied.
    Any {
        /// Criterion identifiers where at least one must be satisfied.
        criteria: Vec<String>,
    },
}

/// Access-credential behavior requested by a content creator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct AccessPolicy {
    /// Requested access credential TTL in seconds.
    pub requested_credential_ttl_seconds: u64,
}

/// Lock Server discovery settings embedded in a content lock.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct LockServerConfig {
    /// Optional per-lock Lock Server override.
    #[serde(rename = "override")]
    pub override_: Option<LockServerPubky>,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::str::FromStr;

    use serde_json::json;
    use time::macros::datetime;

    use crate::ids::{
        BundleId, ContentLockPath, CreatorPubky, GuardedResourceHash, LockServerPubky,
    };
    use crate::lock_policy::{
        AccessPolicy, CONTENT_LOCK_VERSION, ContentLock, ContentLockValidationError, Criterion,
        GuardedResource, GuardedResourceValidationError, LockLogic, LockServerConfig,
        PRIVATE_PROOF_BUNDLE_PATH_PREFIX, PRIVATE_RESOURCE_CONTENT_PATH_PREFIX,
        PUBLIC_LOCKS_APP_PATH_PREFIX, PaykitPaymentParams, PaykitPaymentParamsValidationError,
        PaykitPaymentPolicyValidationError, SecondaryGuardedResource, VerifierType,
        verified_proof_bundle_path,
    };

    fn test_pubky_identity() -> String {
        pubky::Keypair::random().public_key().to_string()
    }

    fn guarded_resource(
        path: &str,
        hash_byte: u8,
        content_type: &str,
        size: u64,
    ) -> GuardedResource {
        GuardedResource {
            path: path.to_owned(),
            hash: GuardedResourceHash::from_bytes([hash_byte; 32]),
            content_type: content_type.to_owned(),
            size,
        }
    }

    fn secondary_resource(
        hash_byte: u8,
        content_type: &str,
        size: u64,
    ) -> SecondaryGuardedResource {
        SecondaryGuardedResource {
            hash: GuardedResourceHash::from_bytes([hash_byte; 32]),
            content_type: content_type.to_owned(),
            size,
        }
    }

    fn content_lock_fixture() -> ContentLock {
        let mut secondary_resources = BTreeMap::new();
        secondary_resources.insert(
            "/priv/locks.app/content/attachments/image.png".to_owned(),
            secondary_resource(8, "image/png", 9),
        );

        ContentLock {
            version: CONTENT_LOCK_VERSION,
            creator: CreatorPubky::from_str(&test_pubky_identity()).unwrap(),
            primary_resource: Some(guarded_resource(
                "/priv/locks.app/content/post.json",
                7,
                "application/json",
                5,
            )),
            secondary_resources,
            criteria: vec![Criterion {
                criterion_id: "criterion-1".to_owned(),
                verifier_type: VerifierType::DevStatic,
                params: json!({ "satisfied": true }),
            }],
            lock_logic: LockLogic::All {
                criteria: vec!["criterion-1".to_owned()],
            },
            access_policy: AccessPolicy {
                requested_credential_ttl_seconds: 900,
            },
            lock_server: LockServerConfig {
                override_: Some(LockServerPubky::from_str(&test_pubky_identity()).unwrap()),
            },
            created_at: datetime!(2026-05-29 12:00:00 UTC),
        }
    }

    fn paykit_criterion(criterion_id: &str, recipient_pubky: &CreatorPubky) -> Criterion {
        Criterion {
            criterion_id: criterion_id.to_owned(),
            verifier_type: VerifierType::PaykitPayment,
            params: json!({
                "recipient_pubky": recipient_pubky.to_string(),
                "amount": "50000",
                "asset": "BTC",
                "payment_in": 24
            }),
        }
    }

    #[test]
    fn paykit_v1_policy_accepts_exactly_one_payment_criterion_and_matching_logic() {
        let mut content_lock = content_lock_fixture();
        content_lock.criteria = vec![paykit_criterion("payment", &content_lock.creator)];
        content_lock.lock_logic = LockLogic::All {
            criteria: vec!["payment".to_owned()],
        };

        assert_eq!(content_lock.validate_paykit_payment_v1_policy(), Ok(()));
    }

    #[test]
    fn paykit_v1_policy_rejects_mixed_criteria() {
        let mut content_lock = content_lock_fixture();
        content_lock
            .criteria
            .push(paykit_criterion("payment", &content_lock.creator));
        content_lock.lock_logic = LockLogic::All {
            criteria: vec!["criterion-1".to_owned(), "payment".to_owned()],
        };

        assert_eq!(
            content_lock.validate_paykit_payment_v1_policy(),
            Err(PaykitPaymentPolicyValidationError::MustBeOnlyCriterion)
        );
    }

    #[test]
    fn paykit_v1_policy_rejects_multiple_payment_criteria() {
        let mut content_lock = content_lock_fixture();
        content_lock.criteria = vec![
            paykit_criterion("payment-1", &content_lock.creator),
            paykit_criterion("payment-2", &content_lock.creator),
        ];
        content_lock.lock_logic = LockLogic::Any {
            criteria: vec!["payment-1".to_owned(), "payment-2".to_owned()],
        };

        assert_eq!(
            content_lock.validate_paykit_payment_v1_policy(),
            Err(PaykitPaymentPolicyValidationError::MustBeOnlyCriterion)
        );
    }

    #[test]
    fn paykit_v1_policy_rejects_logic_that_does_not_exactly_reference_payment_criterion() {
        let mut content_lock = content_lock_fixture();
        content_lock.criteria = vec![paykit_criterion("payment", &content_lock.creator)];
        content_lock.lock_logic = LockLogic::Any {
            criteria: vec!["payment".to_owned(), "payment".to_owned()],
        };

        assert_eq!(
            content_lock.validate_paykit_payment_v1_policy(),
            Err(PaykitPaymentPolicyValidationError::InvalidLockLogic {
                criterion_id: "payment".to_owned()
            })
        );
    }

    #[test]
    fn paykit_v1_policy_rejects_recipient_that_is_not_the_lock_creator() {
        let mut content_lock = content_lock_fixture();
        let different_recipient = CreatorPubky::from_str(&test_pubky_identity()).unwrap();
        content_lock.criteria = vec![paykit_criterion("payment", &different_recipient)];
        content_lock.lock_logic = LockLogic::All {
            criteria: vec!["payment".to_owned()],
        };

        assert_eq!(
            content_lock.validate_paykit_payment_v1_policy(),
            Err(PaykitPaymentPolicyValidationError::RecipientMustMatchCreator)
        );
    }

    #[test]
    fn content_lock_serializes_exact_snake_case_json_shape() {
        let content_lock = content_lock_fixture();

        let serialized = serde_json::to_value(&content_lock).unwrap();
        let expected_creator = content_lock.creator.to_string();
        let expected_lock_server = content_lock
            .lock_server
            .override_
            .as_ref()
            .unwrap()
            .to_string();
        let expected = json!({
            "version": CONTENT_LOCK_VERSION,
            "creator": expected_creator,
            "primary_resource": {
                "path": "/priv/locks.app/content/post.json",
                "hash": "0W3GE1R70W3GE1R70W3GE1R70W3GE1R70W3GE1R70W3G",
                "content_type": "application/json",
                "size": 5u64
            },
            "secondary_resources": {
                "/priv/locks.app/content/attachments/image.png": {
                    "hash": "1040G2081040G2081040G2081040G2081040G2081040G2081040",
                    "content_type": "image/png",
                    "size": 9u64
                }
            },
            "criteria": [{
                "criterion_id": "criterion-1",
                "verifier_type": "dev-static",
                "params": { "satisfied": true }
            }],
            "lock_logic": {
                "type": "all",
                "criteria": ["criterion-1"]
            },
            "access_policy": {
                "requested_credential_ttl_seconds": 900u64
            },
            "lock_server": {
                "override": expected_lock_server
            },
            "created_at": "2026-05-29T12:00:00Z"
        });

        assert_eq!(serialized["version"], json!(CONTENT_LOCK_VERSION));
        assert_eq!(serialized["creator"], json!(expected_creator));
        assert!(serialized.get("guarded_resource").is_none());
        let primary = &serialized["primary_resource"];
        assert_eq!(
            primary["path"].as_str(),
            Some("/priv/locks.app/content/post.json")
        );
        assert_eq!(
            primary["hash"].as_str(),
            Some("0W3GE1R70W3GE1R70W3GE1R70W3GE1R70W3GE1R70W3GE1R70W3G")
        );
        assert_eq!(primary["content_type"].as_str(), Some("application/json"));
        assert_eq!(primary["size"].as_u64(), Some(5));
        let secondary =
            &serialized["secondary_resources"]["/priv/locks.app/content/attachments/image.png"];
        assert_eq!(
            secondary["hash"].as_str(),
            Some("1040G2081040G2081040G2081040G2081040G2081040G2081040")
        );
        assert_eq!(secondary["content_type"].as_str(), Some("image/png"));
        assert_eq!(secondary["size"].as_u64(), Some(9));
        assert_eq!(serialized["criteria"], expected["criteria"]);
        assert_eq!(serialized["lock_logic"], expected["lock_logic"]);
        assert_eq!(serialized["access_policy"], expected["access_policy"]);
        assert_eq!(serialized["lock_server"], expected["lock_server"]);
        assert_eq!(serialized["created_at"], json!("2026-05-29T12:00:00Z"));
    }

    #[test]
    fn private_locks_paths_define_confirmed_pubky_homeserver_namespaces() {
        assert_eq!(PUBLIC_LOCKS_APP_PATH_PREFIX, "/pub/locks.app/");
        assert_eq!(
            PRIVATE_RESOURCE_CONTENT_PATH_PREFIX,
            "/priv/locks.app/content/"
        );
        assert_eq!(PRIVATE_PROOF_BUNDLE_PATH_PREFIX, "/priv/locks.app/proofs/");
    }

    #[test]
    fn private_locks_paths_build_verified_proof_bundle_path_from_typed_bundle_id() {
        let bundle_id = BundleId::from_str("000G40R40M30E209185GR38E1W").unwrap();

        assert_eq!(
            verified_proof_bundle_path(&bundle_id),
            "/priv/locks.app/proofs/000G40R40M30E209185GR38E1W.json"
        );
    }

    #[test]
    fn guarded_resource_constructor_accepts_mime_content_type_and_positive_size() {
        let guarded_resource = GuardedResource::new(
            "/priv/locks.app/content/hello.txt",
            GuardedResourceHash::from_bytes([7; 32]),
            "text/plain",
            5,
        )
        .unwrap();

        assert_eq!(guarded_resource.path, "/priv/locks.app/content/hello.txt");
        assert_eq!(guarded_resource.content_type, "text/plain");
        assert_eq!(guarded_resource.size, 5);
    }

    #[test]
    fn guarded_resource_path_accepts_content_namespace_paths() {
        for path in [
            "/priv/locks.app/content/example.txt",
            "/priv/locks.app/content/nested/example.json",
        ] {
            let guarded_resource = GuardedResource::new(
                path,
                GuardedResourceHash::from_bytes([7; 32]),
                "text/plain",
                5,
            )
            .unwrap();

            assert_eq!(guarded_resource.path, path);
        }
    }

    #[test]
    fn guarded_resource_path_rejects_paths_outside_content_namespace() {
        for path in [
            "/pub/locks.app/content/example.txt",
            "/priv/locks.app/proofs/example.json",
            "/priv/locks.app/content/",
            "/priv/locks.app/content/../secret.txt",
            "/priv/locks.app/content//double.txt",
            "https://example.com/priv/locks.app/content/file.txt",
            "guarded/locks.app/content/missing-leading-slash.txt",
        ] {
            let err = GuardedResource::new(
                path,
                GuardedResourceHash::from_bytes([7; 32]),
                "text/plain",
                5,
            )
            .unwrap_err();

            assert_eq!(err, GuardedResourceValidationError::InvalidPath, "{path}");
        }
    }

    #[test]
    fn guarded_resource_constructor_rejects_invalid_mime_content_type() {
        let result = GuardedResource::new(
            "/priv/locks.app/content/hello.txt",
            GuardedResourceHash::from_bytes([7; 32]),
            "not a mime type",
            5,
        );

        assert!(result.is_err());
    }

    #[test]
    fn guarded_resource_constructor_rejects_zero_size() {
        let result = GuardedResource::new(
            "/priv/locks.app/content/hello.txt",
            GuardedResourceHash::from_bytes([7; 32]),
            "text/plain",
            0,
        );

        assert!(result.is_err());
    }

    #[test]
    fn content_lock_requires_primary_resource_content_type_when_primary_is_present() {
        let mut value = serde_json::to_value(content_lock_fixture()).unwrap();
        value["primary_resource"]
            .as_object_mut()
            .unwrap()
            .remove("content_type");

        let result = serde_json::from_value::<ContentLock>(value);

        assert!(result.is_err());
    }

    #[test]
    fn content_lock_requires_primary_resource_size_when_primary_is_present() {
        let mut value = serde_json::to_value(content_lock_fixture()).unwrap();
        value["primary_resource"]
            .as_object_mut()
            .unwrap()
            .remove("size");

        let result = serde_json::from_value::<ContentLock>(value);

        assert!(result.is_err());
    }

    #[test]
    fn content_lock_rejects_unknown_verifier_type() {
        let mut value = serde_json::to_value(content_lock_fixture()).unwrap();
        value["criteria"][0]["verifier_type"] = json!("not-supported");

        let result = serde_json::from_value::<ContentLock>(value);

        assert!(result.is_err());
    }

    #[test]
    fn paykit_payment_verifier_type_uses_canonical_wire_value() {
        assert_eq!(
            serde_json::to_value(VerifierType::PaykitPayment).unwrap(),
            json!("paykit-payment")
        );
        assert_eq!(VerifierType::PaykitPayment.to_string(), "paykit-payment");
        assert_eq!(
            serde_json::from_value::<VerifierType>(json!("paykit-payment")).unwrap(),
            VerifierType::PaykitPayment
        );
    }

    #[test]
    fn stale_paykit_verifier_wire_value_is_rejected() {
        assert!(serde_json::from_value::<VerifierType>(json!("paykit")).is_err());
    }

    #[test]
    fn paykit_payment_params_validate_required_shape() {
        let criterion = Criterion {
            criterion_id: "criterion-1".to_owned(),
            verifier_type: VerifierType::PaykitPayment,
            params: json!({
                "recipient_pubky": test_pubky_identity(),
                "amount": "50000",
                "asset": "BTC",
                "payment_in": 24,
            }),
        };

        assert_eq!(criterion.validate_params(), Ok(()));
        let params = criterion.paykit_payment_params().unwrap().unwrap();
        assert_eq!(params.amount(), "50000");
        assert_eq!(params.asset(), "BTC");
        assert_eq!(params.payment_in(), 24);
        assert_eq!(
            params.recipient_pubky().to_string(),
            criterion.params["recipient_pubky"]
        );
        let _: PaykitPaymentParams = params;
    }

    #[test]
    fn paykit_payment_params_reject_invalid_shapes() {
        let recipient = test_pubky_identity();
        let overflow = serde_json::from_str(&format!(
            r#"{{"recipient_pubky":"{recipient}","amount":"50000","asset":"BTC","payment_in":18446744073709551616}}"#
        ))
        .unwrap();
        for (params, expected) in [
            (json!(null), PaykitPaymentParamsValidationError::NotObject),
            (
                json!({ "recipient_pubky": recipient, "amount": "50000", "asset": "BTC", "payment_in": 24, "memo": "extra" }),
                PaykitPaymentParamsValidationError::UnknownField("memo".to_owned()),
            ),
            (
                json!({ "amount": "50000", "asset": "BTC", "payment_in": 24 }),
                PaykitPaymentParamsValidationError::MissingField("recipient_pubky"),
            ),
            (
                json!({ "recipient_pubky": recipient, "asset": "BTC", "payment_in": 24 }),
                PaykitPaymentParamsValidationError::MissingField("amount"),
            ),
            (
                json!({ "recipient_pubky": recipient, "amount": "50000", "payment_in": 24 }),
                PaykitPaymentParamsValidationError::MissingField("asset"),
            ),
            (
                json!({ "recipient_pubky": recipient, "amount": "50000", "asset": "BTC" }),
                PaykitPaymentParamsValidationError::MissingField("payment_in"),
            ),
            (
                json!({ "recipient_pubky": "not-a-pubky", "amount": "50000", "asset": "BTC", "payment_in": 24 }),
                PaykitPaymentParamsValidationError::InvalidRecipientPubky,
            ),
            (
                json!({ "recipient_pubky": recipient, "amount": "0", "asset": "BTC", "payment_in": 24 }),
                PaykitPaymentParamsValidationError::InvalidAmount,
            ),
            (
                json!({ "recipient_pubky": recipient, "amount": "0.5", "asset": "BTC", "payment_in": 24 }),
                PaykitPaymentParamsValidationError::InvalidAmount,
            ),
            (
                json!({ "recipient_pubky": recipient, "amount": 50000, "asset": "BTC", "payment_in": 24 }),
                PaykitPaymentParamsValidationError::InvalidAmount,
            ),
            (
                json!({ "recipient_pubky": recipient, "amount": "50000", "asset": "", "payment_in": 24 }),
                PaykitPaymentParamsValidationError::InvalidAsset,
            ),
            (
                json!({ "recipient_pubky": recipient, "amount": "50000", "asset": "BTC", "payment_in": 0 }),
                PaykitPaymentParamsValidationError::InvalidPaymentIn,
            ),
            (
                json!({ "recipient_pubky": recipient, "amount": "50000", "asset": "BTC", "payment_in": -1 }),
                PaykitPaymentParamsValidationError::InvalidPaymentIn,
            ),
            (
                json!({ "recipient_pubky": recipient, "amount": "50000", "asset": "BTC", "payment_in": 1.5 }),
                PaykitPaymentParamsValidationError::InvalidPaymentIn,
            ),
            (
                json!({ "recipient_pubky": recipient, "amount": "50000", "asset": "BTC", "payment_in": "24" }),
                PaykitPaymentParamsValidationError::InvalidPaymentIn,
            ),
            (
                overflow,
                PaykitPaymentParamsValidationError::InvalidPaymentIn,
            ),
        ] {
            let criterion = Criterion {
                criterion_id: "criterion-1".to_owned(),
                verifier_type: VerifierType::PaykitPayment,
                params,
            };

            assert_eq!(criterion.validate_params(), Err(expected));
        }
    }

    #[test]
    fn content_lock_rejects_unknown_top_level_fields() {
        let mut value = serde_json::to_value(content_lock_fixture()).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("lock_id".to_owned(), json!("derived-not-serialized"));

        let result = serde_json::from_value::<ContentLock>(value);

        assert!(result.is_err());
    }

    #[test]
    fn nested_objects_do_not_require_version_fields() {
        let value = serde_json::to_value(content_lock_fixture()).unwrap();

        assert!(value.get("version").is_some());
        assert!(value["primary_resource"].get("version").is_none());
        assert!(value["criteria"][0].get("version").is_none());
        assert!(value["lock_logic"].get("version").is_none());
        assert!(value["access_policy"].get("version").is_none());
        assert!(value["lock_server"].get("version").is_none());
    }

    #[test]
    fn content_lock_requires_version_field() {
        let mut value = serde_json::to_value(content_lock_fixture()).unwrap();
        value.as_object_mut().unwrap().remove("version");

        let result = serde_json::from_value::<ContentLock>(value);

        assert!(result.is_err());
    }

    #[test]
    fn content_lock_resource_set_validates_primary_only() {
        let mut content_lock = content_lock_fixture();
        content_lock.secondary_resources.clear();

        assert_eq!(content_lock.validate_resource_set(), Ok(()));
        assert_eq!(content_lock.resource_count(), 1);
        assert_eq!(content_lock.total_resource_size(), Ok(5));
    }

    #[test]
    fn content_lock_resource_set_validates_secondary_only() {
        let mut content_lock = content_lock_fixture();
        content_lock.primary_resource = None;

        assert_eq!(content_lock.validate_resource_set(), Ok(()));
        assert_eq!(content_lock.resource_count(), 1);
        assert_eq!(content_lock.total_resource_size(), Ok(9));
    }

    #[test]
    fn content_lock_resource_set_validates_primary_and_secondary() {
        let content_lock = content_lock_fixture();

        assert_eq!(content_lock.validate_resource_set(), Ok(()));
        assert_eq!(content_lock.resource_count(), 2);
        assert_eq!(content_lock.total_resource_size(), Ok(14));
    }

    #[test]
    fn content_lock_resource_set_rejects_empty_lock() {
        let mut content_lock = content_lock_fixture();
        content_lock.primary_resource = None;
        content_lock.secondary_resources.clear();

        assert_eq!(
            content_lock.validate_resource_set(),
            Err(ContentLockValidationError::EmptyResourceSet)
        );
    }

    #[test]
    fn content_lock_resource_set_rejects_invalid_secondary_path() {
        let mut content_lock = content_lock_fixture();
        content_lock.secondary_resources.clear();
        content_lock.secondary_resources.insert(
            "/pub/locks.app/content/image.png".to_owned(),
            secondary_resource(8, "image/png", 9),
        );

        assert_eq!(
            content_lock.validate_resource_set(),
            Err(ContentLockValidationError::InvalidSecondaryResource {
                path: "/pub/locks.app/content/image.png".to_owned(),
                reason: GuardedResourceValidationError::InvalidPath,
            })
        );
    }

    #[test]
    fn content_lock_resource_set_rejects_invalid_secondary_content_type() {
        let mut content_lock = content_lock_fixture();
        content_lock.secondary_resources.clear();
        content_lock.secondary_resources.insert(
            "/priv/locks.app/content/image.png".to_owned(),
            secondary_resource(8, "not mime", 9),
        );

        assert_eq!(
            content_lock.validate_resource_set(),
            Err(ContentLockValidationError::InvalidSecondaryResource {
                path: "/priv/locks.app/content/image.png".to_owned(),
                reason: GuardedResourceValidationError::InvalidContentType,
            })
        );
    }

    #[test]
    fn content_lock_resource_set_rejects_zero_size_secondary() {
        let mut content_lock = content_lock_fixture();
        content_lock.secondary_resources.clear();
        content_lock.secondary_resources.insert(
            "/priv/locks.app/content/image.png".to_owned(),
            secondary_resource(8, "image/png", 0),
        );

        assert_eq!(
            content_lock.validate_resource_set(),
            Err(ContentLockValidationError::InvalidSecondaryResource {
                path: "/priv/locks.app/content/image.png".to_owned(),
                reason: GuardedResourceValidationError::ZeroSize,
            })
        );
    }

    #[test]
    fn content_lock_resource_set_rejects_primary_secondary_duplicate_path() {
        let mut content_lock = content_lock_fixture();
        let primary = content_lock.primary_resource.as_ref().unwrap();
        content_lock
            .secondary_resources
            .insert(primary.path.clone(), secondary_resource(8, "image/png", 9));

        assert_eq!(
            content_lock.validate_resource_set(),
            Err(ContentLockValidationError::DuplicatePrimarySecondaryPath {
                path: "/priv/locks.app/content/post.json".to_owned(),
            })
        );
    }

    #[test]
    fn content_lock_resource_for_path_finds_primary_and_secondary() {
        let content_lock = content_lock_fixture();

        let primary = content_lock
            .resource_for_path("/priv/locks.app/content/post.json")
            .unwrap();
        assert_eq!(primary.content_type, "application/json");
        assert_eq!(primary.size, 5);

        let secondary = content_lock
            .resource_for_path("/priv/locks.app/content/attachments/image.png")
            .unwrap();
        assert_eq!(
            secondary.path,
            "/priv/locks.app/content/attachments/image.png"
        );
        assert_eq!(secondary.content_type, "image/png");
        assert_eq!(secondary.size, 9);
    }

    #[test]
    fn content_lock_canonical_json_orders_secondary_resource_keys() {
        let mut content_lock = content_lock_fixture();
        content_lock.secondary_resources.clear();
        content_lock.secondary_resources.insert(
            "/priv/locks.app/content/z.txt".to_owned(),
            secondary_resource(9, "text/plain", 1),
        );
        content_lock.secondary_resources.insert(
            "/priv/locks.app/content/a.txt".to_owned(),
            secondary_resource(8, "text/plain", 1),
        );

        let canonical_json = content_lock.canonical_json_string().unwrap();

        assert!(
            canonical_json
                .find("/priv/locks.app/content/a.txt")
                .unwrap()
                < canonical_json
                    .find("/priv/locks.app/content/z.txt")
                    .unwrap()
        );
    }

    #[test]
    fn content_lock_canonical_json_uses_jcs_key_ordering() {
        let canonical_json = content_lock_fixture().canonical_json_string().unwrap();

        assert!(!canonical_json.contains(' '));
        assert!(!canonical_json.contains('\n'));
        assert!(canonical_json.starts_with("{\"access_policy\":"));
        assert!(canonical_json.ends_with(",\"version\":1}"));
        assert!(
            canonical_json.find("\"access_policy\"").unwrap()
                < canonical_json.find("\"created_at\"").unwrap()
        );
        assert!(
            canonical_json.find("\"created_at\"").unwrap()
                < canonical_json.find("\"creator\"").unwrap()
        );
        assert!(
            canonical_json.find("\"creator\"").unwrap()
                < canonical_json.find("\"criteria\"").unwrap()
        );
        assert!(
            canonical_json.find("\"criteria\"").unwrap()
                < canonical_json.find("\"lock_logic\"").unwrap()
        );
        assert!(
            canonical_json.find("\"lock_logic\"").unwrap()
                < canonical_json.find("\"lock_server\"").unwrap()
        );
        assert!(
            canonical_json.find("\"lock_server\"").unwrap()
                < canonical_json.find("\"version\"").unwrap()
        );
    }

    #[test]
    fn content_lock_hash_is_blake3_of_canonical_json() {
        let content_lock = content_lock_fixture();
        let canonical_json = content_lock.canonical_json_bytes().unwrap();

        let expected = blake3::hash(&canonical_json);

        assert_eq!(
            content_lock.lock_hash().unwrap().as_bytes(),
            expected.as_bytes()
        );
    }

    #[test]
    fn content_lock_derives_lock_id_and_content_lock_path_from_hash() {
        let content_lock = content_lock_fixture();
        let lock_id = content_lock.lock_id().unwrap();
        let content_lock_path = content_lock.content_lock_path().unwrap();

        assert_eq!(lock_id.as_str().len(), 52);
        assert_eq!(content_lock_path.lock_id(), &lock_id);
        assert_eq!(
            content_lock_path,
            ContentLockPath::from_str(&format!("/pub/locks.app/{lock_id}.json")).unwrap()
        );
    }

    #[test]
    fn changing_lock_server_override_changes_lock_id() {
        let with_override = content_lock_fixture();
        let mut without_override = content_lock_fixture();
        without_override.lock_server.override_ = None;

        assert_ne!(
            with_override.lock_id().unwrap(),
            without_override.lock_id().unwrap()
        );
    }

    #[test]
    fn changing_paykit_payment_in_changes_lock_id() {
        let mut shorter = content_lock_fixture();
        shorter.criteria = vec![paykit_criterion("payment", &shorter.creator)];
        shorter.lock_logic = LockLogic::All {
            criteria: vec!["payment".to_owned()],
        };
        let mut longer = shorter.clone();
        longer.criteria[0].params["payment_in"] = json!(25);

        assert_ne!(shorter.lock_id().unwrap(), longer.lock_id().unwrap());
    }
}
