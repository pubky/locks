use std::fmt;
use std::str::FromStr;

use base32::Alphabet;
use pubky::PublicKey;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

const LOCK_ID_LEN: usize = 52;
const LOCK_HASH_BYTES: usize = 32;
const BUNDLE_ID_LEN: usize = 26;
const BUNDLE_ID_BYTES: usize = 16;
const CONTENT_LOCK_PREFIX: &str = "/pub/locks.app/";
const CONTENT_LOCK_SUFFIX: &str = ".json";

/// Errors returned when parsing Locks identifier and path value objects.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IdParseError {
    /// A Crockford-base32 identifier had an unexpected encoded length.
    #[error("{kind} must be {expected} Crockford-base32 characters")]
    InvalidCrockfordLength { kind: &'static str, expected: usize },
    /// A Crockford-base32 identifier contained a hyphen/readability separator.
    #[error("{kind} contains a readability separator")]
    ContainsSeparator { kind: &'static str },
    /// A Crockford-base32 identifier could not be decoded by the configured alphabet.
    #[error("{kind} is not valid Crockford base32")]
    InvalidCrockford { kind: &'static str },
    /// A verification task identifier was not a UUID v4.
    #[error("task id must be a UUID v4")]
    InvalidTaskId,
    /// A Pubky identity wrapper could not be parsed as a Pubky public key.
    #[error("Pubky identity must be a valid pubky::PublicKey")]
    InvalidPubkyIdentity,
    /// A content lock path did not match `/pub/locks.app/<lock_id>.json`.
    #[error("content lock path must match /pub/locks.app/<lock_id>.json")]
    InvalidContentLockPath,
    /// A Pubky lock resource did not match `pubky<creator_pubky>/pub/locks.app/<lock_id>.json`.
    #[error("Pubky lock resource must match pubky<creator_pubky>/pub/locks.app/<lock_id>.json")]
    InvalidPubkyLockResource,
}

/// Identifier for a content lock.
///
/// `LockId` is the canonical Crockford-base32 representation of the full
/// 32-byte BLAKE3 lock hash. Parsing delegates Crockford lowercase and
/// ambiguous-character handling to the `base32` crate, then stores the
/// canonical uppercase encoding.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LockId(String);

impl LockId {
    /// Returns the canonical uppercase Crockford-base32 string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Derives a lock identifier from a full 32-byte lock hash.
    pub fn from_hash(lock_hash: LockHash) -> Self {
        Self(base32::encode(Alphabet::Crockford, lock_hash.as_bytes()))
    }
}

impl fmt::Display for LockId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Serialize for LockId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for LockId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_str(&value).map_err(serde::de::Error::custom)
    }
}

impl FromStr for LockId {
    type Err = IdParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        parse_crockford_id("lock id", value, LOCK_ID_LEN, LOCK_HASH_BYTES).map(Self)
    }
}

/// Viewer-generated durable recovery handle for a proof bundle.
///
/// `BundleId` is a 128-bit bearer secret encoded as a fixed-length
/// Crockford-base32 string. Parsing uses the same crate-backed normalization
/// behavior as [`LockId`] and stores the canonical uppercase encoding.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BundleId(String);

impl BundleId {
    /// Returns the canonical uppercase Crockford-base32 string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Constructs a bundle identifier from 16 bytes of caller/viewer entropy.
    pub fn from_bytes(bytes: [u8; BUNDLE_ID_BYTES]) -> Self {
        Self(base32::encode(Alphabet::Crockford, &bytes))
    }

    /// Generates a new random bundle identifier.
    pub fn new_random() -> Self {
        Self::from_bytes(*Uuid::new_v4().as_bytes())
    }
}

impl fmt::Display for BundleId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Serialize for BundleId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for BundleId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_str(&value).map_err(serde::de::Error::custom)
    }
}

impl FromStr for BundleId {
    type Err = IdParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        parse_crockford_id("bundle id", value, BUNDLE_ID_LEN, BUNDLE_ID_BYTES).map(Self)
    }
}

/// BLAKE3 hash of the canonical serialized content lock payload.
///
/// This value is used to derive the [`LockId`] and is not serialized as a
/// field inside the content lock payload itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LockHash([u8; LOCK_HASH_BYTES]);

impl LockHash {
    /// Wraps raw 32-byte BLAKE3 output as a lock hash.
    pub fn from_bytes(bytes: [u8; LOCK_HASH_BYTES]) -> Self {
        Self(bytes)
    }

    /// Returns the raw 32-byte hash value.
    pub fn as_bytes(&self) -> &[u8; LOCK_HASH_BYTES] {
        &self.0
    }
}

/// Hash of a guarded resource payload.
///
/// This identifies the guarded resource version referenced by a content lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GuardedResourceHash([u8; LOCK_HASH_BYTES]);

impl GuardedResourceHash {
    /// Wraps raw 32-byte hash output as a guarded resource hash.
    pub fn from_bytes(bytes: [u8; LOCK_HASH_BYTES]) -> Self {
        Self(bytes)
    }

    /// Returns the raw 32-byte hash value.
    pub fn as_bytes(&self) -> &[u8; LOCK_HASH_BYTES] {
        &self.0
    }
}

impl Serialize for GuardedResourceHash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&base32::encode(Alphabet::Crockford, &self.0))
    }
}

impl<'de> Deserialize<'de> for GuardedResourceHash {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        let decoded = base32::decode(Alphabet::Crockford, &value)
            .filter(|decoded| decoded.len() == LOCK_HASH_BYTES)
            .ok_or_else(|| {
                serde::de::Error::custom("guarded resource hash is not valid Crockford base32")
            })?;
        let mut bytes = [0; LOCK_HASH_BYTES];
        bytes.copy_from_slice(&decoded);
        Ok(Self(bytes))
    }
}

/// Server-generated operational identifier for an asynchronous verification task.
///
/// `TaskId` is a UUID v4. It is not a bearer secret and is not a durable
/// recovery handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskId(Uuid);

impl TaskId {
    /// Returns the underlying UUID value.
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.hyphenated())
    }
}

impl FromStr for TaskId {
    type Err = IdParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let uuid = Uuid::parse_str(value).map_err(|_| IdParseError::InvalidTaskId)?;
        if uuid.get_version_num() != 4 {
            return Err(IdParseError::InvalidTaskId);
        }
        Ok(Self(uuid))
    }
}

/// Opaque Pubky identity for a content creator.
///
/// Parsing delegates Pubky public key validation and canonical rendering to
/// [`pubky::PublicKey`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreatorPubky(String);

impl fmt::Display for CreatorPubky {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Serialize for CreatorPubky {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for CreatorPubky {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_str(&value).map_err(serde::de::Error::custom)
    }
}

impl FromStr for CreatorPubky {
    type Err = IdParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        parse_pubky_identity(value).map(Self)
    }
}

/// Opaque Pubky identity for a Lock Server.
///
/// Parsing delegates Pubky public key validation and canonical rendering to
/// [`pubky::PublicKey`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LockServerPubky(String);

impl fmt::Display for LockServerPubky {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Serialize for LockServerPubky {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for LockServerPubky {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_str(&value).map_err(serde::de::Error::custom)
    }
}

impl FromStr for LockServerPubky {
    type Err = IdParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        parse_pubky_identity(value).map(Self)
    }
}

/// Canonical creator-homeserver-relative path to a content lock file.
///
/// The only accepted shape is `/pub/locks.app/<lock_id>.json`. Display and
/// serialization normalize the embedded [`LockId`] to canonical uppercase form.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ContentLockPath {
    lock_id: LockId,
}

impl ContentLockPath {
    /// Builds the canonical content lock path for a lock identifier.
    pub fn from_lock_id(lock_id: LockId) -> Self {
        Self { lock_id }
    }

    /// Returns the lock identifier embedded in the content lock path.
    pub fn lock_id(&self) -> &LockId {
        &self.lock_id
    }
}

impl fmt::Display for ContentLockPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{CONTENT_LOCK_PREFIX}{}{CONTENT_LOCK_SUFFIX}",
            self.lock_id
        )
    }
}

impl Serialize for ContentLockPath {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for ContentLockPath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_str(&value).map_err(serde::de::Error::custom)
    }
}

impl FromStr for ContentLockPath {
    type Err = IdParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let lock_id = value
            .strip_prefix(CONTENT_LOCK_PREFIX)
            .and_then(|rest| rest.strip_suffix(CONTENT_LOCK_SUFFIX))
            .ok_or(IdParseError::InvalidContentLockPath)?;

        if lock_id.contains('/') || lock_id.contains(':') {
            return Err(IdParseError::InvalidContentLockPath);
        }

        let lock_id =
            LockId::from_str(lock_id).map_err(|_| IdParseError::InvalidContentLockPath)?;
        Ok(Self { lock_id })
    }
}

/// Fully addressed Pubky resource for a public content lock.
///
/// The only accepted serialized shape is
/// `pubky<creator_pubky>/pub/locks.app/<lock_id>.json`, matching the preferred
/// Pubky addressed-resource form from the `pubky` crate. Display and
/// serialization normalize the embedded [`ContentLockPath`] while preserving the
/// creator Pubky identity value.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PubkyLockResource {
    creator: CreatorPubky,
    content_lock_path: ContentLockPath,
}

impl PubkyLockResource {
    /// Builds the canonical addressed lock resource from its parts.
    pub fn new(creator: CreatorPubky, content_lock_path: ContentLockPath) -> Self {
        Self {
            creator,
            content_lock_path,
        }
    }

    /// Returns the creator embedded in the Pubky resource.
    pub fn creator(&self) -> &CreatorPubky {
        &self.creator
    }

    /// Returns the creator-relative content lock path embedded in the Pubky resource.
    pub fn content_lock_path(&self) -> &ContentLockPath {
        &self.content_lock_path
    }

    /// Returns the lock identifier embedded in the content lock path.
    pub fn lock_id(&self) -> &LockId {
        self.content_lock_path.lock_id()
    }
}

impl fmt::Display for PubkyLockResource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", self.creator, self.content_lock_path)
    }
}

impl Serialize for PubkyLockResource {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for PubkyLockResource {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_str(&value).map_err(serde::de::Error::custom)
    }
}

impl FromStr for PubkyLockResource {
    type Err = IdParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.starts_with("pubky://") {
            return Err(IdParseError::InvalidPubkyLockResource);
        }

        let path_start = value
            .find(CONTENT_LOCK_PREFIX)
            .ok_or(IdParseError::InvalidPubkyLockResource)?;
        let (creator, path) = value.split_at(path_start);

        let creator =
            CreatorPubky::from_str(creator).map_err(|_| IdParseError::InvalidPubkyLockResource)?;
        let content_lock_path =
            ContentLockPath::from_str(path).map_err(|_| IdParseError::InvalidPubkyLockResource)?;

        Ok(Self {
            creator,
            content_lock_path,
        })
    }
}

fn parse_crockford_id(
    kind: &'static str,
    value: &str,
    encoded_len: usize,
    decoded_len: usize,
) -> Result<String, IdParseError> {
    if value.len() != encoded_len {
        return Err(IdParseError::InvalidCrockfordLength {
            kind,
            expected: encoded_len,
        });
    }

    if value.contains('-') {
        return Err(IdParseError::ContainsSeparator { kind });
    }

    let decoded = base32::decode(Alphabet::Crockford, value)
        .filter(|decoded| decoded.len() == decoded_len)
        .ok_or(IdParseError::InvalidCrockford { kind })?;

    Ok(base32::encode(Alphabet::Crockford, &decoded))
}

fn parse_pubky_identity(value: &str) -> Result<String, IdParseError> {
    PublicKey::from_str(value)
        .map(|public_key| public_key.to_string())
        .map_err(|_| IdParseError::InvalidPubkyIdentity)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    const LOCK_ID: &str = "000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG";
    const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";

    #[test]
    fn lock_id_accepts_canonical_52_char_crockford() {
        let lock_id = LockId::from_str(LOCK_ID).expect("valid lock id");

        assert_eq!(lock_id.to_string(), LOCK_ID);
    }

    #[test]
    fn lock_id_normalizes_lowercase_and_base32_crockford_ambiguity() {
        let lowercase = LOCK_ID.to_lowercase();
        assert_eq!(LockId::from_str(&lowercase).unwrap().to_string(), LOCK_ID);

        let ambiguous_ones = "I".repeat(52);
        let canonical_ones = "1".repeat(52);
        assert_eq!(
            LockId::from_str(&ambiguous_ones).unwrap().to_string(),
            LockId::from_str(&canonical_ones).unwrap().to_string()
        );

        let ambiguous_zeroes = "o".repeat(52);
        assert_eq!(
            LockId::from_str(&ambiguous_zeroes).unwrap().to_string(),
            "0".repeat(52)
        );
    }

    #[test]
    fn lock_id_rejects_wrong_length_and_hyphens() {
        assert!(LockId::from_str("").is_err());
        assert!(LockId::from_str(&LOCK_ID[..51]).is_err());
        assert!(LockId::from_str(&format!("{}-", &LOCK_ID[..51])).is_err());
    }

    #[test]
    fn bundle_id_accepts_canonical_26_char_crockford() {
        let bundle_id = BundleId::from_str(BUNDLE_ID).expect("valid bundle id");

        assert_eq!(bundle_id.to_string(), BUNDLE_ID);
    }

    #[test]
    fn bundle_id_uses_same_base32_crockford_normalization_as_lock_id() {
        let lowercase = BUNDLE_ID.to_lowercase();
        assert_eq!(
            BundleId::from_str(&lowercase).unwrap().to_string(),
            BUNDLE_ID
        );

        let ambiguous_ones = "l".repeat(26);
        let canonical_ones = "1".repeat(26);
        assert_eq!(
            BundleId::from_str(&ambiguous_ones).unwrap().to_string(),
            BundleId::from_str(&canonical_ones).unwrap().to_string()
        );
    }

    #[test]
    fn bundle_id_rejects_wrong_length_and_hyphens() {
        assert!(BundleId::from_str("").is_err());
        assert!(BundleId::from_str(&BUNDLE_ID[..25]).is_err());
        assert!(BundleId::from_str(&format!("{}-", &BUNDLE_ID[..25])).is_err());
    }

    #[test]
    fn bundle_id_can_be_constructed_from_exact_16_bytes() {
        let bundle_id = BundleId::from_bytes([0; 16]);

        assert_eq!(bundle_id.to_string(), "00000000000000000000000000");
        assert_eq!(BundleId::from_str(bundle_id.as_str()).unwrap(), bundle_id);
    }

    #[test]
    fn bundle_id_new_random_generates_parseable_canonical_id() {
        let bundle_id = BundleId::new_random();

        assert_eq!(bundle_id.as_str().len(), 26);
        assert_eq!(BundleId::from_str(bundle_id.as_str()).unwrap(), bundle_id);
    }

    #[test]
    fn lock_hash_and_guarded_resource_hash_wrap_exact_32_byte_hashes() {
        let lock_bytes = [42; 32];
        let resource_bytes = [7; 32];

        let lock_hash = LockHash::from_bytes(lock_bytes);
        let guarded_resource_hash = GuardedResourceHash::from_bytes(resource_bytes);

        assert_eq!(lock_hash.as_bytes(), &lock_bytes);
        assert_eq!(guarded_resource_hash.as_bytes(), &resource_bytes);
    }

    #[test]
    fn task_id_accepts_uuid_v4_only() {
        let task_id =
            TaskId::from_str("550e8400-e29b-41d4-a716-446655440000").expect("valid v4 task id");

        assert_eq!(task_id.to_string(), "550e8400-e29b-41d4-a716-446655440000");
        assert!(TaskId::from_str("550e8400-e29b-11d4-a716-446655440000").is_err());
    }

    fn test_pubky_identity() -> String {
        pubky::Keypair::random().public_key().to_string()
    }

    #[test]
    fn pubky_wrappers_parse_with_pubky_public_key() {
        let creator = test_pubky_identity();
        let lock_server = test_pubky_identity();

        assert_eq!(
            CreatorPubky::from_str(&creator).unwrap().to_string(),
            creator
        );
        assert_eq!(
            LockServerPubky::from_str(&lock_server).unwrap().to_string(),
            lock_server
        );

        let bare_z32 = pubky::Keypair::random().public_key().z32();
        assert_eq!(
            CreatorPubky::from_str(&bare_z32).unwrap().to_string(),
            format!("pubky{bare_z32}")
        );

        assert!(CreatorPubky::from_str("abc123").is_err());
        assert!(CreatorPubky::from_str("pubky/abc").is_err());
        assert!(CreatorPubky::from_str("pubky abc").is_err());
        assert!(CreatorPubky::from_str("pubky\nabc").is_err());
    }

    #[test]
    fn content_lock_path_accepts_exact_canonical_shape_and_normalizes_embedded_lock_id() {
        let lowercase_path = format!("/pub/locks.app/{}.json", LOCK_ID.to_lowercase());
        let path = ContentLockPath::from_str(&lowercase_path).expect("valid content lock path");

        assert_eq!(path.to_string(), format!("/pub/locks.app/{LOCK_ID}.json"));
        assert_eq!(path.lock_id().to_string(), LOCK_ID);
    }

    #[test]
    fn pubky_lock_resource_accepts_preferred_pubky_resource_shape() {
        let creator = test_pubky_identity();
        let resource = PubkyLockResource::from_str(&format!(
            "{}/pub/locks.app/{}.json",
            creator,
            LOCK_ID.to_lowercase()
        ))
        .expect("valid pubky lock resource");

        assert_eq!(resource.creator().to_string(), creator);
        assert_eq!(resource.lock_id().to_string(), LOCK_ID);
        assert_eq!(
            resource.content_lock_path().to_string(),
            format!("/pub/locks.app/{LOCK_ID}.json")
        );
        assert_eq!(
            resource.to_string(),
            format!("{creator}/pub/locks.app/{LOCK_ID}.json")
        );
    }

    #[test]
    fn pubky_lock_resource_rejects_pubky_url_and_non_lock_paths() {
        assert!(
            PubkyLockResource::from_str(&format!(
                "pubky://creator123/pub/locks.app/{LOCK_ID}.json"
            ))
            .is_err()
        );
        assert!(
            PubkyLockResource::from_str(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/other/file.json"
            )
            .is_err()
        );
        assert!(
            PubkyLockResource::from_str(&format!("creator123/pub/locks.app/{LOCK_ID}.json"))
                .is_err()
        );
    }

    #[test]
    fn content_lock_path_rejects_urls_and_noncanonical_paths() {
        assert!(
            ContentLockPath::from_str(&format!("pubky://creator/pub/locks.app/{LOCK_ID}.json"))
                .is_err()
        );
        assert!(
            ContentLockPath::from_str(&format!("https://example.com/pub/locks.app/{LOCK_ID}.json"))
                .is_err()
        );
        assert!(ContentLockPath::from_str(&format!("/pub/other.app/{LOCK_ID}.json")).is_err());
        assert!(ContentLockPath::from_str(&format!("/pub/locks.app/{LOCK_ID}")).is_err());
    }
}
