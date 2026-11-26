use async_trait::async_trait;
use uuid::Uuid;

use crate::application::errors::ApplicationError;
use crate::application::models::{
    ReaderAuthChallengeHash, ReaderAuthChallengeRecord, ReaderSessionRecord, ReaderSessionTokenHash,
};

/// Atomic challenge/session exchange input.
#[derive(Clone)]
pub struct ExchangeReaderSession {
    pub challenge_id: Uuid,
    pub challenge_hash: ReaderAuthChallengeHash,
    pub browser_origin: String,
    pub previous_token_hash: Option<ReaderSessionTokenHash>,
    pub session: ReaderSessionRecord,
}

#[derive(Clone)]
pub struct CreateReaderAuthChallenge {
    pub challenge_id: Uuid,
    pub challenge_hash: ReaderAuthChallengeHash,
    pub browser_origin: String,
    pub ttl_seconds: i64,
}

/// Durable Reader-auth state. Exchange and use operations are atomic so nodes
/// cannot consume a challenge, rotate a predecessor, or accept a stale token
/// without committing the corresponding state transition.
#[async_trait]
pub trait ReaderAuthStore: Send + Sync {
    async fn create_challenge(
        &self,
        request: CreateReaderAuthChallenge,
    ) -> Result<ReaderAuthChallengeRecord, ApplicationError>;

    async fn get_challenge(
        &self,
        challenge_id: Uuid,
    ) -> Result<Option<ReaderAuthChallengeRecord>, ApplicationError>;

    /// Consumes the challenge, reserves `(grant_id, proof_nonce)`, optionally
    /// rotates the predecessor, and inserts the successor in one transaction.
    async fn exchange_session(
        &self,
        exchange: ExchangeReaderSession,
    ) -> Result<ReaderSessionRecord, ApplicationError>;

    /// Atomically validates an active token and advances idle expiry, capped by
    /// absolute/grant expiry. `None` means unavailable, expired, or revoked.
    async fn validate_and_touch(
        &self,
        token_hash: &ReaderSessionTokenHash,
        browser_origin: &str,
        idle_ttl_seconds: i64,
    ) -> Result<Option<ReaderSessionRecord>, ApplicationError>;

    /// Atomically revokes one active token. Exactly one concurrent caller wins.
    async fn invalidate_session(
        &self,
        token_hash: &ReaderSessionTokenHash,
        browser_origin: &str,
    ) -> Result<bool, ApplicationError>;

    async fn purge_expired(&self) -> Result<u64, ApplicationError>;
}

/// Generator for reader session tokens.
pub trait ReaderSessionTokenGenerator: Send + Sync {
    fn generate_reader_session_token(&self) -> crate::application::models::ReaderSessionToken;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::models::{ReaderSessionToken, ReaderSessionTokenHash};
    use time::OffsetDateTime;

    struct CompileOnlyStore;

    #[async_trait]
    impl ReaderAuthStore for CompileOnlyStore {
        async fn create_challenge(
            &self,
            request: CreateReaderAuthChallenge,
        ) -> Result<ReaderAuthChallengeRecord, ApplicationError> {
            Ok(ReaderAuthChallengeRecord {
                challenge_id: request.challenge_id,
                challenge_hash: request.challenge_hash,
                browser_origin: request.browser_origin,
                created_at: OffsetDateTime::UNIX_EPOCH,
                expires_at: OffsetDateTime::UNIX_EPOCH,
                consumed_at: None,
            })
        }

        async fn get_challenge(
            &self,
            _challenge_id: Uuid,
        ) -> Result<Option<ReaderAuthChallengeRecord>, ApplicationError> {
            Ok(None)
        }

        async fn exchange_session(
            &self,
            exchange: ExchangeReaderSession,
        ) -> Result<ReaderSessionRecord, ApplicationError> {
            Ok(exchange.session)
        }

        async fn validate_and_touch(
            &self,
            _token_hash: &ReaderSessionTokenHash,
            _browser_origin: &str,
            _idle_ttl_seconds: i64,
        ) -> Result<Option<ReaderSessionRecord>, ApplicationError> {
            Ok(None)
        }

        async fn invalidate_session(
            &self,
            _token_hash: &ReaderSessionTokenHash,
            _browser_origin: &str,
        ) -> Result<bool, ApplicationError> {
            Ok(false)
        }

        async fn purge_expired(&self) -> Result<u64, ApplicationError> {
            Ok(0)
        }
    }

    struct CompileOnlyGenerator;

    impl ReaderSessionTokenGenerator for CompileOnlyGenerator {
        fn generate_reader_session_token(&self) -> ReaderSessionToken {
            ReaderSessionToken::new("token")
        }
    }

    #[test]
    fn reader_auth_ports_are_object_safe() {
        let store: &dyn ReaderAuthStore = &CompileOnlyStore;
        let generator: &dyn ReaderSessionTokenGenerator = &CompileOnlyGenerator;
        let _ = (store, generator);
    }
}
