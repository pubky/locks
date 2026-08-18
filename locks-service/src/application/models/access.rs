use std::fmt;

use locks_core::{
    ids::{BundleId, CreatorPubky},
    lock_policy::GuardedResource,
};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::application::errors::ApplicationError;

/// Default requested access credential TTL in seconds.
pub const DEFAULT_ACCESS_CREDENTIAL_TTL_SECONDS: u64 = 900;

/// Opaque bearer credential issued by the Lock Server for proxy access.
///
/// The concrete production representation is intentionally deferred. The service
/// layer only treats it as an opaque value that resolves server-side to creator
/// and bundle identity.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct AccessCredential(String);

impl AccessCredential {
    /// Wraps an opaque credential value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the opaque credential value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for AccessCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("AccessCredential")
            .field(&"<redacted>")
            .finish()
    }
}

/// Versioned encrypted bearer envelope persisted for exact final-credential replay.
#[derive(Clone, PartialEq, Eq)]
pub struct EncryptedFinalCredential(String);

impl EncryptedFinalCredential {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for EncryptedFinalCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("EncryptedFinalCredential")
            .field(&"<redacted>")
            .finish()
    }
}

/// Immutable identity bound into final-credential AEAD associated data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalCredentialContext {
    pub deletion_job_id: Uuid,
    pub creator: CreatorPubky,
    pub bundle_id: BundleId,
}

/// Secret-free identity of an eligible deletion snapshot awaiting final credential issuance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalCredentialMaterialization {
    pub creator: CreatorPubky,
    pub bundle_id: BundleId,
}

/// Immutable final-access windows established by the storage serialization winner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FinalAccessWindows {
    pub issuance_started_at: OffsetDateTime,
    pub credential_issuance_deadline: OffsetDateTime,
    pub read_deadline: OffsetDateTime,
}

/// Closed result of claim-fenced final-access window initialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitializeFinalAccessWindowsResult {
    Initialized(FinalAccessWindows),
    ClaimLost,
}

/// A deletion credential returned only after its encrypted bearer is durable.
#[derive(Clone, PartialEq, Eq)]
pub struct IssuedDeletionCredential {
    pub credential: AccessCredential,
    pub expires_at: OffsetDateTime,
}

impl fmt::Debug for IssuedDeletionCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IssuedDeletionCredential")
            .field("credential", &"<redacted>")
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

/// Frozen-manifest authorization prepared before guarded-resource I/O.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletionReadAuthorization {
    pub claim_token: Option<Uuid>,
    pub creator: CreatorPubky,
    pub resource: GuardedResource,
}

/// Non-bearer lookup key derived from an access credential.
///
/// Stores use this BLAKE3 digest instead of raw bearer credential strings.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct AccessCredentialLookupKey([u8; 32]);

impl AccessCredentialLookupKey {
    /// Derives a lookup key from the raw access credential bytes.
    pub fn derive(credential: &AccessCredential) -> Self {
        Self(*blake3::hash(credential.as_str().as_bytes()).as_bytes())
    }

    /// Returns the raw lookup digest bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for AccessCredentialLookupKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("AccessCredentialLookupKey")
            .field(&"<redacted>")
            .finish()
    }
}

/// Policy controlling access credential TTL issuance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccessCredentialPolicy {
    /// Default requested access credential TTL in seconds.
    pub default_ttl_seconds: u64,
    /// Maximum TTL accepted by this Lock Server.
    pub max_ttl_seconds: u64,
}

impl AccessCredentialPolicy {
    /// Creates an access credential policy with the v0 default TTL.
    pub fn new(max_ttl_seconds: u64) -> Self {
        Self {
            default_ttl_seconds: DEFAULT_ACCESS_CREDENTIAL_TTL_SECONDS,
            max_ttl_seconds,
        }
    }

    /// Validates a requested TTL and returns it if supported.
    pub fn validate_requested_ttl_seconds(
        &self,
        requested_seconds: u64,
    ) -> Result<u64, ApplicationError> {
        if requested_seconds == 0 || requested_seconds > self.max_ttl_seconds {
            return Err(ApplicationError::UnsupportedCredentialTtl {
                requested_seconds,
                max_seconds: self.max_ttl_seconds,
            });
        }

        Ok(requested_seconds)
    }
}

/// Server-side state associated with an issued access credential.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessCredentialRecord {
    /// Creator whose entitlement namespace should be checked during validation.
    pub creator: CreatorPubky,
    /// Bundle ID anchoring the entitlement.
    pub bundle_id: BundleId,
    /// Time after which this access credential must not be honored.
    pub expires_at: OffsetDateTime,
}
