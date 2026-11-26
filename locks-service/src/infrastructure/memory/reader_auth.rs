use async_trait::async_trait;
use std::sync::{Arc, RwLock};
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

use crate::application::errors::ApplicationError;
use crate::application::models::{
    ReaderAuthChallengeRecord, ReaderSessionRecord, ReaderSessionTokenHash,
};
use crate::application::ports::{
    Clock, CreateReaderAuthChallenge, ExchangeReaderSession, ReaderAuthStore,
};

#[derive(Default)]
struct State {
    challenges: Vec<ReaderAuthChallengeRecord>,
    sessions: Vec<ReaderSessionRecord>,
    replays: Vec<(String, String, OffsetDateTime)>,
}

/// In-memory Reader-auth store used only by injected development/test runtime.
pub struct InMemoryReaderAuthStore {
    state: RwLock<State>,
    clock: Arc<dyn Clock>,
}

impl Default for InMemoryReaderAuthStore {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryReaderAuthStore {
    pub fn new() -> Self {
        Self::with_clock(Arc::new(SystemClock))
    }

    pub fn with_clock(clock: Arc<dyn Clock>) -> Self {
        Self {
            state: RwLock::new(State::default()),
            clock,
        }
    }
}

struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::now_utc()
    }
}

#[async_trait]
impl ReaderAuthStore for InMemoryReaderAuthStore {
    async fn create_challenge(
        &self,
        request: CreateReaderAuthChallenge,
    ) -> Result<ReaderAuthChallengeRecord, ApplicationError> {
        let mut state = self.state.write().expect("reader auth lock poisoned");
        let now = self.clock.now();
        let record = ReaderAuthChallengeRecord {
            challenge_id: request.challenge_id,
            challenge_hash: request.challenge_hash,
            browser_origin: request.browser_origin,
            created_at: now,
            expires_at: now + Duration::seconds(request.ttl_seconds),
            consumed_at: None,
        };
        if state
            .challenges
            .iter()
            .any(|stored| stored.challenge_id == record.challenge_id)
        {
            return Err(ApplicationError::DuplicateRecord {
                record: "reader_auth_challenge",
            });
        }
        state.challenges.push(record.clone());
        Ok(record)
    }

    async fn get_challenge(
        &self,
        challenge_id: Uuid,
    ) -> Result<Option<ReaderAuthChallengeRecord>, ApplicationError> {
        Ok(self
            .state
            .read()
            .expect("reader auth lock poisoned")
            .challenges
            .iter()
            .find(|record| record.challenge_id == challenge_id)
            .cloned())
    }

    async fn exchange_session(
        &self,
        mut exchange: ExchangeReaderSession,
    ) -> Result<ReaderSessionRecord, ApplicationError> {
        let mut state = self.state.write().expect("reader auth lock poisoned");
        let now = self.clock.now();
        let challenge = state
            .challenges
            .iter()
            .find(|record| record.challenge_id == exchange.challenge_id)
            .cloned()
            .ok_or(ApplicationError::ReaderAuthChallengeUnavailable)?;
        if challenge.is_expired_at(now) {
            return Err(ApplicationError::ReaderAuthChallengeExpired);
        }
        if challenge.is_consumed() {
            return Err(ApplicationError::ReaderAuthChallengeAlreadyConsumed);
        }
        if challenge.challenge_hash != exchange.challenge_hash
            || challenge.browser_origin != exchange.browser_origin
        {
            return Err(ApplicationError::ReaderAuthChallengeUnavailable);
        }
        if state.replays.iter().any(|(grant_id, nonce, expiry)| {
            grant_id == &exchange.session.grant_id
                && nonce == &exchange.session.proof_nonce
                && now < *expiry
        }) {
            return Err(ApplicationError::ReaderAuthChallengeAlreadyConsumed);
        }

        let predecessor_index = if let Some(previous_hash) = &exchange.previous_token_hash {
            let index = state
                .sessions
                .iter()
                .position(|record| &record.token_hash == previous_hash)
                .ok_or(ApplicationError::ReaderSessionRenewalConflict)?;
            let predecessor = &state.sessions[index];
            if !predecessor.is_active_at(now)
                || predecessor.reader != exchange.session.reader
                || predecessor.lock_server_pubky != exchange.session.lock_server_pubky
                || predecessor.lock_server_origin != exchange.session.lock_server_origin
                || predecessor.browser_origin != exchange.session.browser_origin
            {
                return Err(ApplicationError::ReaderSessionRenewalConflict);
            }
            Some(index)
        } else {
            None
        };

        if !exchange.session.initialize_lifetime_at(now) {
            return Err(ApplicationError::ReaderSessionExpired);
        }

        if state.sessions.iter().any(|record| {
            record.session_id == exchange.session.session_id
                || record.token_hash == exchange.session.token_hash
        }) {
            return Err(ApplicationError::DuplicateRecord {
                record: "reader_session",
            });
        }

        let challenge = state
            .challenges
            .iter_mut()
            .find(|record| record.challenge_id == exchange.challenge_id)
            .expect("challenge checked above");
        challenge.consumed_at = Some(now);
        if let Some(index) = predecessor_index {
            exchange.session.renewed_from_session_id = Some(state.sessions[index].session_id);
            state.sessions[index].invalidated_at = Some(now);
        }
        state.replays.push((
            exchange.session.grant_id.clone(),
            exchange.session.proof_nonce.clone(),
            exchange.session.grant_expires_at,
        ));
        state.sessions.push(exchange.session.clone());
        Ok(exchange.session)
    }

    async fn validate_and_touch(
        &self,
        token_hash: &ReaderSessionTokenHash,
        browser_origin: &str,
        idle_ttl_seconds: i64,
    ) -> Result<Option<ReaderSessionRecord>, ApplicationError> {
        let mut state = self.state.write().expect("reader auth lock poisoned");
        let now = self.clock.now();
        let Some(record) = state.sessions.iter_mut().find(|record| {
            &record.token_hash == token_hash && record.browser_origin == browser_origin
        }) else {
            return Ok(None);
        };
        if !record.is_active_at(now) {
            return Ok(None);
        }
        record.idle_expires_at = (now + Duration::seconds(idle_ttl_seconds))
            .min(record.absolute_expires_at)
            .min(record.grant_expires_at);
        Ok(Some(record.clone()))
    }

    async fn invalidate_session(
        &self,
        token_hash: &ReaderSessionTokenHash,
        browser_origin: &str,
    ) -> Result<bool, ApplicationError> {
        let mut state = self.state.write().expect("reader auth lock poisoned");
        let now = self.clock.now();
        let Some(record) = state.sessions.iter_mut().find(|record| {
            &record.token_hash == token_hash && record.browser_origin == browser_origin
        }) else {
            return Ok(false);
        };
        if !record.is_active_at(now) {
            return Ok(false);
        }
        record.invalidated_at = Some(now);
        Ok(true)
    }

    async fn purge_expired(&self) -> Result<u64, ApplicationError> {
        let mut state = self.state.write().expect("reader auth lock poisoned");
        let before = self.clock.now();
        let old = state.challenges.len() + state.sessions.len() + state.replays.len();
        state.challenges.retain(|record| record.expires_at > before);
        state.sessions.retain(|record| {
            record.idle_expires_at > before
                && record.absolute_expires_at > before
                && record.grant_expires_at > before
                && record.invalidated_at.is_none_or(|at| at > before)
        });
        state.replays.retain(|(_, _, expiry)| *expiry > before);
        Ok((old - state.challenges.len() - state.sessions.len() - state.replays.len()) as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::models::{
        ReaderAuthChallengeHash, ReaderSessionToken, ReaderSessionTokenHash,
    };
    use crate::application::ports::{ExchangeReaderSession, ReaderAuthStore};
    use locks_core::ids::CreatorPubky;
    use std::str::FromStr;
    use time::macros::datetime;

    struct FixedClock(OffsetDateTime);

    impl crate::application::ports::Clock for FixedClock {
        fn now(&self) -> OffsetDateTime {
            self.0
        }
    }

    struct AdjustableClock(std::sync::Mutex<OffsetDateTime>);

    impl AdjustableClock {
        fn set(&self, now: OffsetDateTime) {
            *self.0.lock().unwrap() = now;
        }
    }

    impl crate::application::ports::Clock for AdjustableClock {
        fn now(&self) -> OffsetDateTime {
            *self.0.lock().unwrap()
        }
    }

    fn challenge() -> crate::application::ports::CreateReaderAuthChallenge {
        crate::application::ports::CreateReaderAuthChallenge {
            challenge_id: Uuid::from_u128(1),
            challenge_hash: ReaderAuthChallengeHash::derive(b"challenge"),
            browser_origin: "https://app.example".to_owned(),
            ttl_seconds: 120,
        }
    }

    fn session(id: u128, token: &str, nonce: &str) -> ReaderSessionRecord {
        ReaderSessionRecord {
            session_id: Uuid::from_u128(id),
            reader: CreatorPubky::from_str(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            )
            .unwrap(),
            token_hash: ReaderSessionTokenHash::derive(&ReaderSessionToken::new(token)),
            challenge_id: Uuid::from_u128(1),
            grant_id: "grant".to_owned(),
            grant_client_id: "app.example".to_owned(),
            proof_nonce: nonce.to_owned(),
            lock_server_pubky: "server".to_owned(),
            lock_server_origin: "https://locks.example".to_owned(),
            browser_origin: "https://app.example".to_owned(),
            grant_expires_at: datetime!(2026-11-01 14:00:00 UTC),
            idle_expires_at: datetime!(2026-11-01 12:15:00 UTC),
            absolute_expires_at: datetime!(2026-11-01 13:00:00 UTC),
            created_at: datetime!(2026-11-01 12:00:00 UTC),
            renewed_from_session_id: None,
            invalidated_at: None,
        }
    }

    fn exchange(session: ReaderSessionRecord) -> ExchangeReaderSession {
        ExchangeReaderSession {
            challenge_id: Uuid::from_u128(1),
            challenge_hash: ReaderAuthChallengeHash::derive(b"challenge"),
            browser_origin: "https://app.example".to_owned(),
            previous_token_hash: None,
            session,
        }
    }

    #[tokio::test]
    async fn challenge_creation_uses_store_clock_under_the_decisive_lock() {
        let now = datetime!(2026-11-01 12:00:00 UTC);
        let store = InMemoryReaderAuthStore::with_clock(std::sync::Arc::new(FixedClock(now)));

        let created = store
            .create_challenge(crate::application::ports::CreateReaderAuthChallenge {
                challenge_id: Uuid::from_u128(9),
                challenge_hash: ReaderAuthChallengeHash::derive(b"nine"),
                browser_origin: "https://app.example".to_owned(),
                ttl_seconds: 120,
            })
            .await
            .unwrap();

        assert_eq!(created.created_at, now);
        assert_eq!(created.expires_at, now + Duration::seconds(120));
    }

    #[tokio::test]
    async fn purge_removes_challenge_at_expiry_equality() {
        let created_at = datetime!(2026-11-01 12:00:00 UTC);
        let clock = std::sync::Arc::new(AdjustableClock(std::sync::Mutex::new(created_at)));
        let store = InMemoryReaderAuthStore::with_clock(clock.clone());
        let request = challenge();
        let challenge_id = request.challenge_id;
        store.create_challenge(request).await.unwrap();

        clock.set(created_at + Duration::seconds(120));
        store.purge_expired().await.unwrap();

        assert!(store.get_challenge(challenge_id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn purge_removes_session_at_idle_expiry_equality() {
        let created_at = datetime!(2026-11-01 12:00:00 UTC);
        let clock = std::sync::Arc::new(AdjustableClock(std::sync::Mutex::new(created_at)));
        let store = InMemoryReaderAuthStore::with_clock(clock.clone());
        store.create_challenge(challenge()).await.unwrap();
        let created = store
            .exchange_session(exchange(session(2, "idle-expiry", "idle-expiry")))
            .await
            .unwrap();

        clock.set(created.idle_expires_at);
        store.purge_expired().await.unwrap();

        assert!(
            !store
                .state
                .read()
                .unwrap()
                .sessions
                .iter()
                .any(|session| session.token_hash == created.token_hash)
        );
    }

    #[tokio::test]
    async fn exchange_uses_store_authoritative_time_for_session_lifetimes() {
        let store = InMemoryReaderAuthStore::with_clock(std::sync::Arc::new(FixedClock(
            datetime!(2026-11-01 12:01:00 UTC),
        )));
        store.create_challenge(challenge()).await.unwrap();
        let mut request = exchange(session(2, "one", "n1"));
        request.session.created_at = datetime!(2026-11-01 11:00:00 UTC);
        request.session.idle_expires_at = datetime!(2026-11-01 11:15:00 UTC);
        request.session.absolute_expires_at = datetime!(2026-11-01 12:00:00 UTC);

        let created = store.exchange_session(request).await.unwrap();

        assert_eq!(created.created_at, datetime!(2026-11-01 12:01:00 UTC));
        assert_eq!(created.idle_expires_at, datetime!(2026-11-01 12:16:00 UTC));
        assert_eq!(
            created.absolute_expires_at,
            datetime!(2026-11-01 13:01:00 UTC)
        );
    }

    #[tokio::test]
    async fn exchange_is_single_use_and_validation_touches_atomically() {
        let store = InMemoryReaderAuthStore::with_clock(std::sync::Arc::new(FixedClock(
            datetime!(2026-11-01 12:01:00 UTC),
        )));
        store.create_challenge(challenge()).await.unwrap();
        let created = store
            .exchange_session(exchange(session(2, "one", "n1")))
            .await
            .unwrap();
        assert_eq!(created.session_id, Uuid::from_u128(2));
        let replay = store
            .exchange_session(exchange(session(3, "two", "n2")))
            .await;
        assert_eq!(
            replay.unwrap_err(),
            ApplicationError::ReaderAuthChallengeAlreadyConsumed
        );
        let touched = store
            .validate_and_touch(
                &ReaderSessionTokenHash::derive(&ReaderSessionToken::new("one")),
                "https://app.example",
                900,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(touched.idle_expires_at, datetime!(2026-11-01 12:16:00 UTC));
        assert!(
            store
                .validate_and_touch(
                    &ReaderSessionTokenHash::derive(&ReaderSessionToken::new("one")),
                    "https://other.example",
                    900,
                )
                .await
                .unwrap()
                .is_none()
        );
    }
}
