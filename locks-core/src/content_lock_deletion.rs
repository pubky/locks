use serde::{Deserialize, Deserializer, Serialize};
use time::{OffsetDateTime, UtcOffset};

use crate::ids::LockId;

/// Supported public content-lock deletion tombstone version.
pub const CONTENT_LOCK_DELETION_TOMBSTONE_VERSION: u16 = 1;
const CONTENT_LOCK_DELETION_TOMBSTONE_TYPE: &str = "content_lock_deletion";

/// Exact public replacement for a content lock while graceful deletion runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentLockDeletionTombstone {
    #[serde(deserialize_with = "deserialize_version")]
    version: u16,
    #[serde(rename = "type", deserialize_with = "deserialize_type")]
    kind: String,
    /// Identifier of the withdrawn canonical content lock.
    pub lock_id: LockId,
    /// Durable proof-admission cutoff, encoded as RFC3339 UTC.
    #[serde(
        serialize_with = "time::serde::rfc3339::serialize",
        deserialize_with = "deserialize_utc_timestamp"
    )]
    pub deletion_started_at: OffsetDateTime,
}

impl ContentLockDeletionTombstone {
    /// Creates the exact supported tombstone payload.
    pub fn new(lock_id: LockId, deletion_started_at: OffsetDateTime) -> Self {
        Self {
            version: CONTENT_LOCK_DELETION_TOMBSTONE_VERSION,
            kind: CONTENT_LOCK_DELETION_TOMBSTONE_TYPE.to_owned(),
            lock_id,
            deletion_started_at: deletion_started_at.to_offset(UtcOffset::UTC),
        }
    }

    /// Returns the supported tombstone version.
    pub fn version(&self) -> u16 {
        self.version
    }
}

fn deserialize_version<'de, D>(deserializer: D) -> Result<u16, D::Error>
where
    D: Deserializer<'de>,
{
    let version = u16::deserialize(deserializer)?;
    if version == CONTENT_LOCK_DELETION_TOMBSTONE_VERSION {
        Ok(version)
    } else {
        Err(serde::de::Error::custom(
            "unsupported content lock deletion tombstone version",
        ))
    }
}

fn deserialize_type<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let kind = String::deserialize(deserializer)?;
    if kind == CONTENT_LOCK_DELETION_TOMBSTONE_TYPE {
        Ok(kind)
    } else {
        Err(serde::de::Error::custom(
            "unsupported content lock deletion tombstone type",
        ))
    }
}

fn deserialize_utc_timestamp<'de, D>(deserializer: D) -> Result<OffsetDateTime, D::Error>
where
    D: Deserializer<'de>,
{
    let timestamp = time::serde::rfc3339::deserialize(deserializer)?;
    if timestamp.offset() == UtcOffset::UTC {
        Ok(timestamp)
    } else {
        Err(serde::de::Error::custom("timestamp must use UTC offset Z"))
    }
}
