use locks_core::ids::{CreatorPubky, LockId};

use crate::application::errors::ApplicationError;

/// Durable lifecycle status for exclusive guarded-path ownership.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentLockOwnershipStatus {
    /// The path is reserved for an intended lock before public publication.
    Reserved,
    /// The intended lock was published successfully.
    Published,
}

impl ContentLockOwnershipStatus {
    /// Returns the stable Postgres representation.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Reserved => "reserved",
            Self::Published => "published",
        }
    }

    /// Parses a status loaded from persistence.
    pub fn from_storage(value: &str) -> Result<Self, ApplicationError> {
        match value {
            "reserved" => Ok(Self::Reserved),
            "published" => Ok(Self::Published),
            _ => Err(ApplicationError::Storage {
                message: format!("invalid content lock ownership status: {value}"),
            }),
        }
    }
}

/// Exclusive ownership of one creator-scoped guarded path by an intended lock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentLockOwnership {
    /// Creator who owns the guarded path.
    pub creator: CreatorPubky,
    /// Full canonical guarded-resource path.
    pub guarded_path: String,
    /// Canonical Lock ID intended to own the path.
    pub lock_id: LockId,
    /// Reservation/publication lifecycle status.
    pub status: ContentLockOwnershipStatus,
}
