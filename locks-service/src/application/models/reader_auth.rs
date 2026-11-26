use std::fmt;

use locks_core::ids::CreatorPubky;
use time::OffsetDateTime;
use uuid::Uuid;

/// Challenge validity window: 2 minutes.
pub const READER_AUTH_CHALLENGE_TTL_SECONDS: i64 = 120;

/// Session idle timeout: 15 minutes.
pub const READER_SESSION_IDLE_TTL_SECONDS: i64 = 900;

/// Session absolute timeout: 60 minutes.
pub const READER_SESSION_ABSOLUTE_TTL_SECONDS: i64 = 3600;

/// Purpose string returned in challenge responses.
pub const READER_SESSION_PURPOSE: &str = "reader-session-v1";

/// Custom PoP `type` field — strict closed schema discriminator.
pub const READER_AUTH_POP_TYPE: &str = "pubky-locks-reader-session-v1";

// ---------------------------------------------------------------------------
// Reader session token
// ---------------------------------------------------------------------------

/// Opaque reader session bearer token. Redacted in Debug output.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ReaderSessionToken(String);

impl ReaderSessionToken {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn expose_token(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ReaderSessionToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ReaderSessionToken")
            .field(&"<redacted>")
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Reader session token hash — blake3 of the raw token
// ---------------------------------------------------------------------------

/// Blake3 hash of a [`ReaderSessionToken`]. Only hashes are persisted.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ReaderSessionTokenHash([u8; 32]);

impl ReaderSessionTokenHash {
    pub fn derive(token: &ReaderSessionToken) -> Self {
        Self(*blake3::hash(token.expose_token().as_bytes()).as_bytes())
    }

    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for ReaderSessionTokenHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ReaderSessionTokenHash")
            .field(&"<redacted>")
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Challenge hash — blake3 of the raw challenge bytes
// ---------------------------------------------------------------------------

/// Blake3 hash of a raw challenge. Only hashes are persisted.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ReaderAuthChallengeHash([u8; 32]);

impl ReaderAuthChallengeHash {
    pub fn derive(challenge_bytes: &[u8]) -> Self {
        Self(*blake3::hash(challenge_bytes).as_bytes())
    }

    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for ReaderAuthChallengeHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ReaderAuthChallengeHash")
            .field(&"<redacted>")
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Challenge record — persisted server-side challenge state
// ---------------------------------------------------------------------------

/// Persisted reader auth challenge. The raw challenge is never stored; only its
/// blake3 hash is kept for later verification.
#[derive(Clone, PartialEq, Eq)]
pub struct ReaderAuthChallengeRecord {
    /// Public identifier for this challenge.
    pub challenge_id: Uuid,
    /// Blake3 hash of the raw challenge bytes.
    pub challenge_hash: ReaderAuthChallengeHash,
    /// Browser origin that requested this challenge.
    pub browser_origin: String,
    /// Creation timestamp.
    pub created_at: OffsetDateTime,
    /// Expiration timestamp. Challenges expire when `now >= expires_at`.
    pub expires_at: OffsetDateTime,
    /// Consumption timestamp for single-use enforcement.
    pub consumed_at: Option<OffsetDateTime>,
}

impl ReaderAuthChallengeRecord {
    /// Returns `true` if this challenge has expired at `now`.
    pub fn is_expired_at(&self, now: OffsetDateTime) -> bool {
        now >= self.expires_at
    }

    /// Returns `true` if this challenge has been consumed.
    pub fn is_consumed(&self) -> bool {
        self.consumed_at.is_some()
    }
}

impl fmt::Debug for ReaderAuthChallengeRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReaderAuthChallengeRecord")
            .field("challenge_id", &self.challenge_id)
            .field("challenge_hash", &"<redacted>")
            .field("browser_origin", &self.browser_origin)
            .field("created_at", &self.created_at)
            .field("expires_at", &self.expires_at)
            .field("consumed_at", &self.consumed_at)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Session record — persisted reader session state
// ---------------------------------------------------------------------------

/// Persisted reader session. The raw session token is never stored; only its
/// blake3 hash is kept for lookup.
#[derive(Clone, PartialEq, Eq)]
pub struct ReaderSessionRecord {
    /// Internal session identifier.
    pub session_id: Uuid,
    /// Reader identity derived from verified grant issuer.
    pub reader: CreatorPubky,
    /// Blake3 hash of the raw session token.
    pub token_hash: ReaderSessionTokenHash,
    /// Challenge that was consumed to create this session.
    pub challenge_id: Uuid,
    /// Root-signed grant identifier retained through grant expiry for replay fencing.
    pub grant_id: String,
    /// Grant client identifier retained as audit context, never as Reader identity.
    pub grant_client_id: String,
    /// Per-proof nonce retained through grant expiry for durable replay prevention.
    pub proof_nonce: String,
    /// Lock Server public key signed into the accepted proof.
    pub lock_server_pubky: String,
    /// Canonical Lock Server origin signed into the accepted proof.
    pub lock_server_origin: String,
    /// Exact browser Origin signed into the accepted proof.
    pub browser_origin: String,
    /// Grant expiration from the verified grant claims.
    pub grant_expires_at: OffsetDateTime,
    /// Idle expiration — refreshed on activity within sessions.
    pub idle_expires_at: OffsetDateTime,
    /// Absolute expiration — the hard ceiling, min(created+60m, grant_exp).
    pub absolute_expires_at: OffsetDateTime,
    /// Creation timestamp.
    pub created_at: OffsetDateTime,
    /// Session this was renewed from, if any.
    pub renewed_from_session_id: Option<Uuid>,
    /// Invalidation timestamp — set by logout or renewal.
    pub invalidated_at: Option<OffsetDateTime>,
}

impl ReaderSessionRecord {
    /// Initializes local session lifetimes from storage-authoritative time.
    /// Returns false when the verified grant is already expired at that boundary.
    pub fn initialize_lifetime_at(&mut self, now: OffsetDateTime) -> bool {
        self.created_at = now;
        self.idle_expires_at = (now + time::Duration::seconds(READER_SESSION_IDLE_TTL_SECONDS))
            .min(self.grant_expires_at);
        self.absolute_expires_at = (now
            + time::Duration::seconds(READER_SESSION_ABSOLUTE_TTL_SECONDS))
        .min(self.grant_expires_at);
        self.is_active_at(now)
    }

    /// Returns `true` if this session is active at `now`.
    pub fn is_active_at(&self, now: OffsetDateTime) -> bool {
        self.invalidated_at.is_none()
            && now < self.idle_expires_at
            && now < self.absolute_expires_at
    }

    /// Returns `true` if this session has been invalidated.
    pub fn is_invalidated(&self) -> bool {
        self.invalidated_at.is_some()
    }

    /// Returns the effective expiration: earliest of idle, absolute.
    pub fn effective_expires_at(&self) -> OffsetDateTime {
        self.idle_expires_at.min(self.absolute_expires_at)
    }
}

impl fmt::Debug for ReaderSessionRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReaderSessionRecord")
            .field("session_id", &self.session_id)
            .field("reader", &self.reader)
            .field("token_hash", &"<redacted>")
            .field("challenge_id", &self.challenge_id)
            .field("grant_id", &self.grant_id)
            .field("grant_client_id", &self.grant_client_id)
            .field("proof_nonce", &"<redacted>")
            .field("lock_server_pubky", &self.lock_server_pubky)
            .field("lock_server_origin", &self.lock_server_origin)
            .field("browser_origin", &self.browser_origin)
            .field("grant_expires_at", &self.grant_expires_at)
            .field("idle_expires_at", &self.idle_expires_at)
            .field("absolute_expires_at", &self.absolute_expires_at)
            .field("created_at", &self.created_at)
            .field("renewed_from_session_id", &self.renewed_from_session_id)
            .field("invalidated_at", &self.invalidated_at)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use time::macros::datetime;

    #[test]
    fn reader_session_token_debug_redacts_value() {
        let token = ReaderSessionToken::new("secret-reader-token");
        let debug = format!("{token:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("secret-reader-token"));
    }

    #[test]
    fn reader_session_token_hash_is_blake3_of_raw_token() {
        let token = ReaderSessionToken::new("secret-reader-token");
        let hash = ReaderSessionTokenHash::derive(&token);
        assert_eq!(
            hash.as_bytes(),
            blake3::hash(b"secret-reader-token").as_bytes()
        );
        assert_eq!(hash, ReaderSessionTokenHash::derive(&token));
        assert_ne!(
            hash,
            ReaderSessionTokenHash::derive(&ReaderSessionToken::new("different"))
        );
    }

    #[test]
    fn reader_session_token_hash_debug_is_redacted() {
        let hash = ReaderSessionTokenHash::derive(&ReaderSessionToken::new("secret-reader-token"));
        let debug = format!("{hash:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("secret-reader-token"));
    }

    #[test]
    fn challenge_hash_is_blake3_of_raw_challenge() {
        let challenge = b"random-challenge-bytes";
        let hash = ReaderAuthChallengeHash::derive(challenge);
        assert_eq!(hash.as_bytes(), blake3::hash(challenge).as_bytes());
    }

    #[test]
    fn challenge_record_expiry_and_consumption() {
        let record = ReaderAuthChallengeRecord {
            challenge_id: Uuid::nil(),
            challenge_hash: ReaderAuthChallengeHash::derive(b"test"),
            browser_origin: "https://app.example.com".to_owned(),
            created_at: datetime!(2026-11-01 12:00:00 UTC),
            expires_at: datetime!(2026-11-01 12:02:00 UTC),
            consumed_at: None,
        };

        assert!(!record.is_expired_at(datetime!(2026-11-01 12:01:59 UTC)));
        assert!(record.is_expired_at(datetime!(2026-11-01 12:02:00 UTC)));
        assert!(!record.is_consumed());
    }

    #[test]
    fn challenge_record_debug_redacts_hash() {
        let record = ReaderAuthChallengeRecord {
            challenge_id: Uuid::nil(),
            challenge_hash: ReaderAuthChallengeHash::derive(b"test"),
            browser_origin: "https://app.example.com".to_owned(),
            created_at: datetime!(2026-11-01 12:00:00 UTC),
            expires_at: datetime!(2026-11-01 12:02:00 UTC),
            consumed_at: None,
        };
        let debug = format!("{record:?}");
        assert!(debug.contains("<redacted>"));
    }

    #[test]
    fn session_record_active_checks() {
        let record = ReaderSessionRecord {
            session_id: Uuid::nil(),
            reader: CreatorPubky::from_str(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            )
            .unwrap(),
            token_hash: ReaderSessionTokenHash::derive(&ReaderSessionToken::new("tok")),
            challenge_id: Uuid::nil(),
            grant_id: "grant-id".to_owned(),
            grant_client_id: "app.example".to_owned(),
            proof_nonce: "proof-nonce".to_owned(),
            lock_server_pubky: "lock-server".to_owned(),
            lock_server_origin: "https://locks.example".to_owned(),
            browser_origin: "https://app.example".to_owned(),
            grant_expires_at: datetime!(2026-11-01 14:00:00 UTC),
            idle_expires_at: datetime!(2026-11-01 12:15:00 UTC),
            absolute_expires_at: datetime!(2026-11-01 13:00:00 UTC),
            created_at: datetime!(2026-11-01 12:00:00 UTC),
            renewed_from_session_id: None,
            invalidated_at: None,
        };

        assert!(record.is_active_at(datetime!(2026-11-01 12:14:59 UTC)));
        assert!(!record.is_active_at(datetime!(2026-11-01 12:15:00 UTC))); // idle expired
        assert!(!record.is_invalidated());
    }

    #[test]
    fn session_record_invalidated_is_not_active() {
        let record = ReaderSessionRecord {
            session_id: Uuid::nil(),
            reader: CreatorPubky::from_str(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            )
            .unwrap(),
            token_hash: ReaderSessionTokenHash::derive(&ReaderSessionToken::new("tok")),
            challenge_id: Uuid::nil(),
            grant_id: "grant-id".to_owned(),
            grant_client_id: "app.example".to_owned(),
            proof_nonce: "proof-nonce".to_owned(),
            lock_server_pubky: "lock-server".to_owned(),
            lock_server_origin: "https://locks.example".to_owned(),
            browser_origin: "https://app.example".to_owned(),
            grant_expires_at: datetime!(2026-11-01 14:00:00 UTC),
            idle_expires_at: datetime!(2026-11-01 12:15:00 UTC),
            absolute_expires_at: datetime!(2026-11-01 13:00:00 UTC),
            created_at: datetime!(2026-11-01 12:00:00 UTC),
            renewed_from_session_id: None,
            invalidated_at: Some(datetime!(2026-11-01 12:05:00 UTC)),
        };

        assert!(!record.is_active_at(datetime!(2026-11-01 12:05:01 UTC)));
        assert!(record.is_invalidated());
    }

    #[test]
    fn session_record_debug_redacts_token_hash() {
        let record = ReaderSessionRecord {
            session_id: Uuid::nil(),
            reader: CreatorPubky::from_str(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            )
            .unwrap(),
            token_hash: ReaderSessionTokenHash::derive(&ReaderSessionToken::new("tok")),
            challenge_id: Uuid::nil(),
            grant_id: "grant-id".to_owned(),
            grant_client_id: "app.example".to_owned(),
            proof_nonce: "proof-nonce".to_owned(),
            lock_server_pubky: "lock-server".to_owned(),
            lock_server_origin: "https://locks.example".to_owned(),
            browser_origin: "https://app.example".to_owned(),
            grant_expires_at: datetime!(2026-11-01 14:00:00 UTC),
            idle_expires_at: datetime!(2026-11-01 12:15:00 UTC),
            absolute_expires_at: datetime!(2026-11-01 13:00:00 UTC),
            created_at: datetime!(2026-11-01 12:00:00 UTC),
            renewed_from_session_id: None,
            invalidated_at: None,
        };
        let debug = format!("{record:?}");
        assert!(debug.contains("<redacted>"));
        assert!(debug.contains("ReaderSessionRecord"));
    }
}
