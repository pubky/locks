use std::fmt;

use locks_core::ids::CreatorPubky;
use time::OffsetDateTime;

/// One-time code exchanged for a Locks-local frontend session.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct FrontendSessionCode(String);

impl FrontendSessionCode {
    /// Wraps a one-time frontend session code.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Exposes the raw code for hashing/storage adapters.
    pub fn expose_code(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for FrontendSessionCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("FrontendSessionCode")
            .field(&"<redacted>")
            .finish()
    }
}

/// Persisted server-side one-time code state.
#[derive(Clone, PartialEq, Eq)]
pub struct FrontendSessionCodeRecord {
    /// One-time code value.
    pub code: FrontendSessionCode,
    /// Creator identity the resulting frontend session represents.
    pub creator: CreatorPubky,
    /// Expected state supplied by the initiating pubky.app caller.
    pub state: String,
    /// Return target associated with this code.
    pub return_to: String,
    /// Creation timestamp.
    pub created_at: OffsetDateTime,
    /// Expiration timestamp. Codes expire when `now >= expires_at`.
    pub expires_at: OffsetDateTime,
    /// Consumption timestamp for single-use enforcement.
    pub consumed_at: Option<OffsetDateTime>,
}

impl FrontendSessionCodeRecord {
    /// Returns true once this one-time code is expired at `now`.
    pub fn is_expired_at(&self, now: OffsetDateTime) -> bool {
        now >= self.expires_at
    }
}

impl fmt::Debug for FrontendSessionCodeRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FrontendSessionCodeRecord")
            .field("code", &"<redacted>")
            .field("creator", &self.creator)
            .field("state", &self.state)
            .field("return_to", &self.return_to)
            .field("created_at", &self.created_at)
            .field("expires_at", &self.expires_at)
            .field("consumed_at", &self.consumed_at)
            .finish()
    }
}

/// Locks-local browser/frontend session bearer token.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct FrontendSessionToken(String);

impl FrontendSessionToken {
    /// Wraps a Locks-local frontend session bearer token.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Exposes the raw token for hashing/storage adapters.
    pub fn expose_token(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for FrontendSessionToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("FrontendSessionToken")
            .field(&"<redacted>")
            .finish()
    }
}

/// Locks-local frontend session record derived from an exchanged one-time code.
#[derive(Clone, PartialEq, Eq)]
pub struct FrontendSessionRecord {
    /// Bearer token for pubky.app/browser -> Lock Server creator APIs.
    pub token: FrontendSessionToken,
    /// Creator identity represented by this frontend session.
    pub creator: CreatorPubky,
    /// Creation timestamp.
    pub created_at: OffsetDateTime,
    /// Expiration timestamp. Sessions expire when `now >= expires_at`.
    pub expires_at: OffsetDateTime,
}

impl FrontendSessionRecord {
    /// Returns true once this frontend session is expired at `now`.
    pub fn is_expired_at(&self, now: OffsetDateTime) -> bool {
        now >= self.expires_at
    }
}

impl fmt::Debug for FrontendSessionRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FrontendSessionRecord")
            .field("token", &"<redacted>")
            .field("creator", &self.creator)
            .field("created_at", &self.created_at)
            .field("expires_at", &self.expires_at)
            .finish()
    }
}
