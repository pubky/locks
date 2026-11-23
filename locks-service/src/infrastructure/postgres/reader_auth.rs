use async_trait::async_trait;
use sqlx::{PgPool, Row};
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

use crate::application::errors::ApplicationError;
use crate::application::models::{
    ReaderAuthChallengeHash, ReaderAuthChallengeRecord, ReaderSessionRecord, ReaderSessionTokenHash,
};
use crate::application::ports::{
    CreateReaderAuthChallenge, ExchangeReaderSession, ReaderAuthStore,
};

#[derive(Debug, Clone)]
pub struct PostgresReaderAuthStore {
    pool: PgPool,
}

impl PostgresReaderAuthStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ReaderAuthStore for PostgresReaderAuthStore {
    async fn create_challenge(
        &self,
        request: CreateReaderAuthChallenge,
    ) -> Result<ReaderAuthChallengeRecord, ApplicationError> {
        let row = sqlx::query(
            "WITH authoritative AS (SELECT clock_timestamp() AS now)
             INSERT INTO reader_auth_challenges
             (challenge_id, challenge_hash, browser_origin, created_at, expires_at, consumed_at)
             SELECT $1, $2, $3, now, now + make_interval(secs => $4::double precision), NULL
             FROM authoritative
             ON CONFLICT (challenge_id) DO NOTHING
             RETURNING challenge_id, challenge_hash, browser_origin, created_at, expires_at, consumed_at",
        )
        .bind(request.challenge_id)
        .bind(request.challenge_hash.as_bytes().as_slice())
        .bind(request.browser_origin)
        .bind(request.ttl_seconds as f64)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        row.map(row_to_challenge)
            .transpose()?
            .ok_or(ApplicationError::DuplicateRecord {
                record: "reader_auth_challenge",
            })
    }

    async fn get_challenge(
        &self,
        challenge_id: Uuid,
    ) -> Result<Option<ReaderAuthChallengeRecord>, ApplicationError> {
        let row = sqlx::query(
            "SELECT challenge_id, challenge_hash, browser_origin, created_at, expires_at, consumed_at
             FROM reader_auth_challenges WHERE challenge_id = $1",
        )
        .bind(challenge_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        row.map(row_to_challenge).transpose()
    }

    async fn exchange_session(
        &self,
        mut exchange: ExchangeReaderSession,
    ) -> Result<ReaderSessionRecord, ApplicationError> {
        let mut tx = self.pool.begin().await.map_err(storage_error)?;
        let row = sqlx::query(
            "SELECT challenge_id, challenge_hash, browser_origin, created_at, expires_at, consumed_at
             FROM reader_auth_challenges WHERE challenge_id = $1 FOR UPDATE",
        )
        .bind(exchange.challenge_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage_error)?
        .ok_or(ApplicationError::ReaderAuthChallengeUnavailable)?;
        let challenge = row_to_challenge(row)?;

        let predecessor = if let Some(previous_hash) = &exchange.previous_token_hash {
            let row = sqlx::query(
                "SELECT session_id, reader, token_hash, challenge_id, grant_id, grant_client_id,
                        proof_nonce, lock_server_pubky, lock_server_origin, browser_origin,
                        grant_expires_at, idle_expires_at, absolute_expires_at, created_at,
                        renewed_from_session_id, invalidated_at
                 FROM reader_sessions WHERE token_hash = $1 FOR UPDATE",
            )
            .bind(previous_hash.as_bytes().as_slice())
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage_error)?
            .ok_or(ApplicationError::ReaderSessionRenewalConflict)?;
            Some(row_to_session(row)?)
        } else {
            None
        };

        let now: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *tx)
            .await
            .map_err(storage_error)?;
        if !exchange.session.initialize_lifetime_at(now) {
            return Err(ApplicationError::ReaderSessionExpired);
        }
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

        if let Some(predecessor) = &predecessor
            && (!predecessor.is_active_at(now)
                || predecessor.reader != exchange.session.reader
                || predecessor.lock_server_pubky != exchange.session.lock_server_pubky
                || predecessor.lock_server_origin != exchange.session.lock_server_origin
                || predecessor.browser_origin != exchange.session.browser_origin)
        {
            return Err(ApplicationError::ReaderSessionRenewalConflict);
        }
        if let Some(predecessor) = &predecessor {
            exchange.session.renewed_from_session_id = Some(predecessor.session_id);
        }

        let replay = sqlx::query(
            "INSERT INTO reader_auth_proof_replays (grant_id, proof_nonce, expires_at)
             VALUES ($1, $2, $3) ON CONFLICT (grant_id, proof_nonce) DO NOTHING",
        )
        .bind(&exchange.session.grant_id)
        .bind(&exchange.session.proof_nonce)
        .bind(exchange.session.grant_expires_at)
        .execute(&mut *tx)
        .await
        .map_err(storage_error)?;
        if replay.rows_affected() == 0 {
            return Err(ApplicationError::ReaderAuthChallengeAlreadyConsumed);
        }

        sqlx::query(
            "UPDATE reader_auth_challenges SET consumed_at = $2
             WHERE challenge_id = $1 AND consumed_at IS NULL",
        )
        .bind(exchange.challenge_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(storage_error)?;

        if let (Some(previous_hash), Some(_)) = (&exchange.previous_token_hash, predecessor) {
            let result = sqlx::query(
                "UPDATE reader_sessions SET invalidated_at = $2
                 WHERE token_hash = $1 AND invalidated_at IS NULL
                   AND idle_expires_at > $2 AND absolute_expires_at > $2 AND grant_expires_at > $2",
            )
            .bind(previous_hash.as_bytes().as_slice())
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(storage_error)?;
            if result.rows_affected() != 1 {
                return Err(ApplicationError::ReaderSessionRenewalConflict);
            }
        }

        insert_session(&mut tx, &exchange.session).await?;
        tx.commit().await.map_err(storage_error)?;
        Ok(exchange.session)
    }

    async fn validate_and_touch(
        &self,
        token_hash: &ReaderSessionTokenHash,
        browser_origin: &str,
        idle_ttl_seconds: i64,
    ) -> Result<Option<ReaderSessionRecord>, ApplicationError> {
        let idle_ttl = Duration::seconds(idle_ttl_seconds);
        let mut tx = self.pool.begin().await.map_err(storage_error)?;
        let row = sqlx::query(
            "SELECT session_id, reader, token_hash, challenge_id, grant_id, grant_client_id,
                    proof_nonce, lock_server_pubky, lock_server_origin, browser_origin,
                    grant_expires_at, idle_expires_at, absolute_expires_at, created_at,
                    renewed_from_session_id, invalidated_at
             FROM reader_sessions WHERE token_hash = $1 AND browser_origin = $2 FOR UPDATE",
        )
        .bind(token_hash.as_bytes().as_slice())
        .bind(browser_origin)
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage_error)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let mut session = row_to_session(row)?;
        let now: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *tx)
            .await
            .map_err(storage_error)?;
        if !session.is_active_at(now) {
            return Ok(None);
        }
        session.idle_expires_at = (now + idle_ttl)
            .min(session.absolute_expires_at)
            .min(session.grant_expires_at);
        sqlx::query("UPDATE reader_sessions SET idle_expires_at = $2 WHERE token_hash = $1")
            .bind(token_hash.as_bytes().as_slice())
            .bind(session.idle_expires_at)
            .execute(&mut *tx)
            .await
            .map_err(storage_error)?;
        tx.commit().await.map_err(storage_error)?;
        Ok(Some(session))
    }

    async fn invalidate_session(
        &self,
        token_hash: &ReaderSessionTokenHash,
        browser_origin: &str,
    ) -> Result<bool, ApplicationError> {
        let mut tx = self.pool.begin().await.map_err(storage_error)?;
        let row = sqlx::query(
            "SELECT session_id, reader, token_hash, challenge_id, grant_id, grant_client_id,
                    proof_nonce, lock_server_pubky, lock_server_origin, browser_origin,
                    grant_expires_at, idle_expires_at, absolute_expires_at, created_at,
                    renewed_from_session_id, invalidated_at
             FROM reader_sessions WHERE token_hash = $1 AND browser_origin = $2 FOR UPDATE",
        )
        .bind(token_hash.as_bytes().as_slice())
        .bind(browser_origin)
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage_error)?;
        let Some(row) = row else {
            return Ok(false);
        };
        let session = row_to_session(row)?;
        let now: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *tx)
            .await
            .map_err(storage_error)?;
        if !session.is_active_at(now) {
            return Ok(false);
        }
        sqlx::query("UPDATE reader_sessions SET invalidated_at = $2 WHERE token_hash = $1")
            .bind(token_hash.as_bytes().as_slice())
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(storage_error)?;
        tx.commit().await.map_err(storage_error)?;
        Ok(true)
    }

    async fn purge_expired(&self) -> Result<u64, ApplicationError> {
        let mut tx = self.pool.begin().await.map_err(storage_error)?;
        let before: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *tx)
            .await
            .map_err(storage_error)?;
        let replays = sqlx::query(
            "DELETE FROM reader_auth_proof_replays WHERE ctid IN
             (SELECT ctid FROM reader_auth_proof_replays WHERE expires_at <= $1 LIMIT 1000)",
        )
        .bind(before)
        .execute(&mut *tx)
        .await
        .map_err(storage_error)?
        .rows_affected();
        let sessions = sqlx::query(
            "DELETE FROM reader_sessions WHERE ctid IN
             (SELECT ctid FROM reader_sessions
              WHERE idle_expires_at <= $1 OR absolute_expires_at <= $1
                 OR grant_expires_at <= $1 OR invalidated_at <= $1
              LIMIT 1000)",
        )
        .bind(before)
        .execute(&mut *tx)
        .await
        .map_err(storage_error)?
        .rows_affected();
        let challenges = sqlx::query(
            "DELETE FROM reader_auth_challenges WHERE ctid IN
             (SELECT c.ctid FROM reader_auth_challenges c WHERE c.expires_at <= $1
              AND NOT EXISTS (SELECT 1 FROM reader_sessions s WHERE s.challenge_id = c.challenge_id)
              LIMIT 1000)",
        )
        .bind(before)
        .execute(&mut *tx)
        .await
        .map_err(storage_error)?
        .rows_affected();
        tx.commit().await.map_err(storage_error)?;
        Ok(replays + sessions + challenges)
    }
}

async fn insert_session(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    record: &ReaderSessionRecord,
) -> Result<(), ApplicationError> {
    sqlx::query(
        "INSERT INTO reader_sessions
         (session_id, reader, token_hash, challenge_id, grant_id, grant_client_id,
          proof_nonce, lock_server_pubky, lock_server_origin, browser_origin,
          grant_expires_at, idle_expires_at, absolute_expires_at, created_at,
          renewed_from_session_id, invalidated_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)",
    )
    .bind(record.session_id)
    .bind(record.reader.to_string())
    .bind(record.token_hash.as_bytes().as_slice())
    .bind(record.challenge_id)
    .bind(&record.grant_id)
    .bind(&record.grant_client_id)
    .bind(&record.proof_nonce)
    .bind(&record.lock_server_pubky)
    .bind(&record.lock_server_origin)
    .bind(&record.browser_origin)
    .bind(record.grant_expires_at)
    .bind(record.idle_expires_at)
    .bind(record.absolute_expires_at)
    .bind(record.created_at)
    .bind(record.renewed_from_session_id)
    .bind(record.invalidated_at)
    .execute(&mut **tx)
    .await
    .map_err(storage_error)?;
    Ok(())
}

fn row_to_challenge(
    row: sqlx::postgres::PgRow,
) -> Result<ReaderAuthChallengeRecord, ApplicationError> {
    let bytes: Vec<u8> = row.try_get("challenge_hash").map_err(storage_error)?;
    let bytes: [u8; 32] = bytes.try_into().map_err(|_| ApplicationError::Storage {
        message: "reader challenge hash must be 32 bytes".to_owned(),
    })?;
    Ok(ReaderAuthChallengeRecord {
        challenge_id: row.try_get("challenge_id").map_err(storage_error)?,
        challenge_hash: ReaderAuthChallengeHash::from_bytes(bytes),
        browser_origin: row.try_get("browser_origin").map_err(storage_error)?,
        created_at: row.try_get("created_at").map_err(storage_error)?,
        expires_at: row.try_get("expires_at").map_err(storage_error)?,
        consumed_at: row.try_get("consumed_at").map_err(storage_error)?,
    })
}

fn row_to_session(row: sqlx::postgres::PgRow) -> Result<ReaderSessionRecord, ApplicationError> {
    use std::str::FromStr;
    let reader_text: String = row.try_get("reader").map_err(storage_error)?;
    let reader = locks_core::ids::CreatorPubky::from_str(&reader_text).map_err(|error| {
        ApplicationError::Storage {
            message: format!("invalid reader identity in reader session: {error}"),
        }
    })?;
    let bytes: Vec<u8> = row.try_get("token_hash").map_err(storage_error)?;
    let bytes: [u8; 32] = bytes.try_into().map_err(|_| ApplicationError::Storage {
        message: "reader session token hash must be 32 bytes".to_owned(),
    })?;
    Ok(ReaderSessionRecord {
        session_id: row.try_get("session_id").map_err(storage_error)?,
        reader,
        token_hash: ReaderSessionTokenHash::from_bytes(bytes),
        challenge_id: row.try_get("challenge_id").map_err(storage_error)?,
        grant_id: row.try_get("grant_id").map_err(storage_error)?,
        grant_client_id: row.try_get("grant_client_id").map_err(storage_error)?,
        proof_nonce: row.try_get("proof_nonce").map_err(storage_error)?,
        lock_server_pubky: row.try_get("lock_server_pubky").map_err(storage_error)?,
        lock_server_origin: row.try_get("lock_server_origin").map_err(storage_error)?,
        browser_origin: row.try_get("browser_origin").map_err(storage_error)?,
        grant_expires_at: row.try_get("grant_expires_at").map_err(storage_error)?,
        idle_expires_at: row.try_get("idle_expires_at").map_err(storage_error)?,
        absolute_expires_at: row.try_get("absolute_expires_at").map_err(storage_error)?,
        created_at: row.try_get("created_at").map_err(storage_error)?,
        renewed_from_session_id: row
            .try_get("renewed_from_session_id")
            .map_err(storage_error)?,
        invalidated_at: row.try_get("invalidated_at").map_err(storage_error)?,
    })
}

fn storage_error(error: sqlx::Error) -> ApplicationError {
    ApplicationError::Storage {
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::models::{
        ReaderAuthChallengeHash, ReaderSessionToken, ReaderSessionTokenHash,
    };
    use crate::application::ports::ReaderAuthStore;
    use crate::infrastructure::postgres::testing::TestDatabase;
    use locks_core::ids::CreatorPubky;
    use std::str::FromStr;
    use std::sync::Arc;

    fn challenge(id: u128, _now: OffsetDateTime) -> CreateReaderAuthChallenge {
        CreateReaderAuthChallenge {
            challenge_id: Uuid::from_u128(id),
            challenge_hash: ReaderAuthChallengeHash::derive(format!("challenge-{id}").as_bytes()),
            browser_origin: "https://app.example".to_owned(),
            ttl_seconds: 120,
        }
    }

    fn session(
        id: u128,
        challenge_id: u128,
        token: &str,
        nonce: &str,
        now: OffsetDateTime,
    ) -> ReaderSessionRecord {
        ReaderSessionRecord {
            session_id: Uuid::from_u128(id),
            reader: CreatorPubky::from_str(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            )
            .unwrap(),
            token_hash: ReaderSessionTokenHash::derive(&ReaderSessionToken::new(token)),
            challenge_id: Uuid::from_u128(challenge_id),
            grant_id: "grant".to_owned(),
            grant_client_id: "app.example".to_owned(),
            proof_nonce: nonce.to_owned(),
            lock_server_pubky: "server".to_owned(),
            lock_server_origin: "https://locks.example".to_owned(),
            browser_origin: "https://app.example".to_owned(),
            grant_expires_at: now + Duration::hours(2),
            idle_expires_at: now + Duration::minutes(15),
            absolute_expires_at: now + Duration::hours(1),
            created_at: now,
            renewed_from_session_id: None,
            invalidated_at: None,
        }
    }

    fn exchange(
        challenge_id: u128,
        session: ReaderSessionRecord,
        previous_token_hash: Option<ReaderSessionTokenHash>,
        _node_time: OffsetDateTime,
    ) -> ExchangeReaderSession {
        ExchangeReaderSession {
            challenge_id: Uuid::from_u128(challenge_id),
            challenge_hash: ReaderAuthChallengeHash::derive(
                format!("challenge-{challenge_id}").as_bytes(),
            ),
            browser_origin: "https://app.example".to_owned(),
            previous_token_hash,
            session,
        }
    }

    async fn assert_reader_auth_concurrency_contract(
        store: Arc<dyn ReaderAuthStore>,
        now: OffsetDateTime,
    ) {
        store.create_challenge(challenge(10, now)).await.unwrap();
        let (left, right) = tokio::join!(
            store.exchange_session(exchange(
                10,
                session(10, 10, "challenge-left", "challenge-left", now),
                None,
                now - Duration::days(30),
            )),
            store.exchange_session(exchange(
                10,
                session(11, 10, "challenge-right", "challenge-right", now),
                None,
                now + Duration::days(30),
            )),
        );
        assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
        assert_eq!(
            if left.is_err() { left } else { right }.unwrap_err(),
            ApplicationError::ReaderAuthChallengeAlreadyConsumed
        );

        store.create_challenge(challenge(20, now)).await.unwrap();
        store.create_challenge(challenge(21, now)).await.unwrap();
        let (left, right) = tokio::join!(
            store.exchange_session(exchange(
                20,
                session(20, 20, "replay-left", "same-replay", now),
                None,
                now,
            )),
            store.exchange_session(exchange(
                21,
                session(21, 21, "replay-right", "same-replay", now),
                None,
                now,
            )),
        );
        assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
        assert_eq!(
            if left.is_err() { left } else { right }.unwrap_err(),
            ApplicationError::ReaderAuthChallengeAlreadyConsumed
        );

        store.create_challenge(challenge(30, now)).await.unwrap();
        store
            .exchange_session(exchange(
                30,
                session(30, 30, "renew-old", "renew-old", now),
                None,
                now,
            ))
            .await
            .unwrap();
        store.create_challenge(challenge(31, now)).await.unwrap();
        let old_hash = ReaderSessionTokenHash::derive(&ReaderSessionToken::new("renew-old"));
        let old_hash_for_renewal = old_hash.clone();
        let old_hash_for_logout = old_hash.clone();
        let (renewal, logout) = tokio::join!(
            store.exchange_session(exchange(
                31,
                session(31, 31, "renew-new", "renew-new", now),
                Some(old_hash_for_renewal),
                now,
            )),
            store.invalidate_session(&old_hash_for_logout, "https://app.example"),
        );
        assert_eq!(
            usize::from(renewal.is_ok()) + usize::from(logout.unwrap()),
            1
        );
        assert!(
            store
                .validate_and_touch(&old_hash, "https://app.example", 900)
                .await
                .unwrap()
                .is_none()
        );

        store.create_challenge(challenge(40, now)).await.unwrap();
        store
            .exchange_session(exchange(
                40,
                session(40, 40, "use-logout", "use-logout", now),
                None,
                now,
            ))
            .await
            .unwrap();
        let use_logout_hash =
            ReaderSessionTokenHash::derive(&ReaderSessionToken::new("use-logout"));
        let use_hash = use_logout_hash.clone();
        let logout_hash = use_logout_hash.clone();
        let (used, logged_out) = tokio::join!(
            store.validate_and_touch(&use_hash, "https://app.example", 900),
            store.invalidate_session(&logout_hash, "https://app.example"),
        );
        let _ = used.unwrap();
        assert!(logged_out.unwrap());
        assert!(
            store
                .validate_and_touch(&use_logout_hash, "https://app.example", 900)
                .await
                .unwrap()
                .is_none()
        );

        store.create_challenge(challenge(50, now)).await.unwrap();
        store
            .exchange_session(exchange(
                50,
                session(50, 50, "cleanup-active", "cleanup-active", now),
                None,
                now,
            ))
            .await
            .unwrap();
        let cleanup_hash =
            ReaderSessionTokenHash::derive(&ReaderSessionToken::new("cleanup-active"));
        let touch_hash = cleanup_hash.clone();
        let (purged, touched) = tokio::join!(
            store.purge_expired(),
            store.validate_and_touch(&touch_hash, "https://app.example", 900),
        );
        purged.unwrap();
        assert!(touched.unwrap().is_some());
        assert!(
            store
                .validate_and_touch(&cleanup_hash, "https://app.example", 900)
                .await
                .unwrap()
                .is_some()
        );
    }

    struct ContractClock(OffsetDateTime);

    impl crate::application::ports::Clock for ContractClock {
        fn now(&self) -> OffsetDateTime {
            self.0
        }
    }

    #[tokio::test]
    async fn memory_reader_auth_satisfies_shared_concurrency_contract() {
        let now = OffsetDateTime::now_utc();
        let store: Arc<dyn ReaderAuthStore> = Arc::new(
            crate::infrastructure::memory::reader_auth::InMemoryReaderAuthStore::with_clock(
                Arc::new(ContractClock(now)),
            ),
        );
        assert_reader_auth_concurrency_contract(store, now).await;
    }

    #[tokio::test]
    async fn postgres_reader_auth_satisfies_shared_concurrency_contract_across_connections() {
        let database = TestDatabase::create().await;
        let now: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(database.pool())
            .await
            .unwrap();
        let store: Arc<dyn ReaderAuthStore> =
            Arc::new(PostgresReaderAuthStore::new(database.pool().clone()));

        assert_reader_auth_concurrency_contract(store, now).await;

        database.cleanup().await;
    }

    #[tokio::test]
    async fn postgres_exchange_contention_forces_challenge_and_replay_single_winners() {
        let database = TestDatabase::create().await;
        let now: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(database.pool())
            .await
            .unwrap();
        let node_a_pool = database.independent_pool().await;
        let node_b_pool = database.independent_pool().await;
        let node_a = PostgresReaderAuthStore::new(node_a_pool.clone());
        let node_b = PostgresReaderAuthStore::new(node_b_pool.clone());

        node_a.create_challenge(challenge(70, now)).await.unwrap();
        let mut blocker = database.pool().begin().await.unwrap();
        sqlx::query(
            "SELECT challenge_id FROM reader_auth_challenges WHERE challenge_id = $1 FOR UPDATE",
        )
        .bind(Uuid::from_u128(70))
        .fetch_one(&mut *blocker)
        .await
        .unwrap();
        let left = tokio::spawn({
            let node = node_a.clone();
            async move {
                node.exchange_session(exchange(
                    70,
                    session(70, 70, "challenge-a", "challenge-a", now),
                    None,
                    now,
                ))
                .await
            }
        });
        let right = tokio::spawn({
            let node = node_b.clone();
            async move {
                node.exchange_session(exchange(
                    70,
                    session(71, 70, "challenge-b", "challenge-b", now),
                    None,
                    now,
                ))
                .await
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(!left.is_finished() && !right.is_finished());
        blocker.commit().await.unwrap();
        let left = left.await.unwrap();
        let right = right.await.unwrap();
        assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
        assert_eq!(
            if left.is_err() { left } else { right }.unwrap_err(),
            ApplicationError::ReaderAuthChallengeAlreadyConsumed
        );

        node_a.create_challenge(challenge(72, now)).await.unwrap();
        node_a.create_challenge(challenge(73, now)).await.unwrap();
        let mut blocker = database.pool().begin().await.unwrap();
        sqlx::query(
            "SELECT challenge_id FROM reader_auth_challenges
             WHERE challenge_id IN ($1, $2) ORDER BY challenge_id FOR UPDATE",
        )
        .bind(Uuid::from_u128(72))
        .bind(Uuid::from_u128(73))
        .fetch_all(&mut *blocker)
        .await
        .unwrap();
        let left = tokio::spawn({
            let node = node_a.clone();
            async move {
                node.exchange_session(exchange(
                    72,
                    session(72, 72, "replay-a", "same-forced-replay", now),
                    None,
                    now,
                ))
                .await
            }
        });
        let right = tokio::spawn({
            let node = node_b.clone();
            async move {
                node.exchange_session(exchange(
                    73,
                    session(73, 73, "replay-b", "same-forced-replay", now),
                    None,
                    now,
                ))
                .await
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(!left.is_finished() && !right.is_finished());
        blocker.commit().await.unwrap();
        let left = left.await.unwrap();
        let right = right.await.unwrap();
        assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
        assert_eq!(
            if left.is_err() { left } else { right }.unwrap_err(),
            ApplicationError::ReaderAuthChallengeAlreadyConsumed
        );

        node_a_pool.close().await;
        node_b_pool.close().await;
        database.cleanup().await;
    }

    #[tokio::test]
    async fn postgres_forced_use_logout_and_cleanup_races_do_not_resurrect_sessions() {
        let database = TestDatabase::create().await;
        let now: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(database.pool())
            .await
            .unwrap();
        let store = PostgresReaderAuthStore::new(database.pool().clone());
        store.create_challenge(challenge(80, now)).await.unwrap();
        store
            .exchange_session(exchange(
                80,
                session(80, 80, "use-logout-forced", "use-logout-forced", now),
                None,
                now,
            ))
            .await
            .unwrap();
        let token_hash =
            ReaderSessionTokenHash::derive(&ReaderSessionToken::new("use-logout-forced"));
        let mut blocker = database.pool().begin().await.unwrap();
        sqlx::query("SELECT session_id FROM reader_sessions WHERE token_hash = $1 FOR UPDATE")
            .bind(token_hash.as_bytes().as_slice())
            .fetch_one(&mut *blocker)
            .await
            .unwrap();
        let touch = tokio::spawn({
            let store = store.clone();
            let token_hash = token_hash.clone();
            async move {
                store
                    .validate_and_touch(&token_hash, "https://app.example", 900)
                    .await
            }
        });
        let logout = tokio::spawn({
            let store = store.clone();
            let token_hash = token_hash.clone();
            async move {
                store
                    .invalidate_session(&token_hash, "https://app.example")
                    .await
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(!touch.is_finished() && !logout.is_finished());
        blocker.commit().await.unwrap();
        let _ = touch.await.unwrap().unwrap();
        assert!(logout.await.unwrap().unwrap());
        assert!(
            store
                .validate_and_touch(&token_hash, "https://app.example", 900)
                .await
                .unwrap()
                .is_none()
        );

        store.create_challenge(challenge(81, now)).await.unwrap();
        store
            .exchange_session(exchange(
                81,
                session(81, 81, "cleanup-forced", "cleanup-forced", now),
                None,
                now,
            ))
            .await
            .unwrap();
        let cleanup_hash =
            ReaderSessionTokenHash::derive(&ReaderSessionToken::new("cleanup-forced"));
        sqlx::query(
            "UPDATE reader_sessions SET invalidated_at = clock_timestamp()
             WHERE token_hash = $1",
        )
        .bind(cleanup_hash.as_bytes().as_slice())
        .execute(database.pool())
        .await
        .unwrap();
        let mut blocker = database.pool().begin().await.unwrap();
        sqlx::query("SELECT session_id FROM reader_sessions WHERE token_hash = $1 FOR UPDATE")
            .bind(cleanup_hash.as_bytes().as_slice())
            .fetch_one(&mut *blocker)
            .await
            .unwrap();
        let touch = tokio::spawn({
            let store = store.clone();
            let cleanup_hash = cleanup_hash.clone();
            async move {
                store
                    .validate_and_touch(&cleanup_hash, "https://app.example", 900)
                    .await
            }
        });
        let purge = tokio::spawn({
            let store = store.clone();
            async move { store.purge_expired().await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(!touch.is_finished() && !purge.is_finished());
        blocker.commit().await.unwrap();
        assert!(touch.await.unwrap().unwrap().is_none());
        assert!(purge.await.unwrap().unwrap() >= 1);
        let remaining: i64 =
            sqlx::query_scalar("SELECT count(*) FROM reader_sessions WHERE token_hash = $1")
                .bind(cleanup_hash.as_bytes().as_slice())
                .fetch_one(database.pool())
                .await
                .unwrap();
        assert_eq!(remaining, 0);

        database.cleanup().await;
    }

    #[tokio::test]
    async fn postgres_storage_clock_ignores_skewed_nodes_and_survives_adapter_restart() {
        let database = TestDatabase::create().await;
        let node_a = PostgresReaderAuthStore::new(database.pool().clone());
        let created = node_a
            .create_challenge(challenge(60, OffsetDateTime::UNIX_EPOCH))
            .await
            .unwrap();
        assert_eq!(
            created.expires_at - created.created_at,
            Duration::seconds(120)
        );

        let node_b_pool = database.independent_pool().await;
        let node_b = PostgresReaderAuthStore::new(node_b_pool.clone());
        let stored = node_b
            .get_challenge(created.challenge_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored, created);
        let session = node_b
            .exchange_session(exchange(
                60,
                session(60, 60, "skewed-node", "skewed-node", created.created_at),
                None,
                created.created_at + Duration::days(30),
            ))
            .await
            .unwrap();
        assert!(session.created_at >= created.created_at);
        assert!(session.created_at < created.expires_at);

        node_b_pool.close().await;
        database.cleanup().await;
    }

    #[tokio::test]
    async fn postgres_reader_sessions_survive_restart_and_rotate_with_one_concurrent_winner() {
        let database = TestDatabase::create().await;
        let now: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(database.pool())
            .await
            .unwrap();
        let store = PostgresReaderAuthStore::new(database.pool().clone());
        store.create_challenge(challenge(1, now)).await.unwrap();
        store
            .exchange_session(exchange(1, session(1, 1, "old", "nonce-1", now), None, now))
            .await
            .unwrap();

        let restarted = PostgresReaderAuthStore::new(database.pool().clone());
        let old_hash = ReaderSessionTokenHash::derive(&ReaderSessionToken::new("old"));
        assert!(
            restarted
                .validate_and_touch(&old_hash, "https://other.example", 900)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            restarted
                .validate_and_touch(&old_hash, "https://app.example", 900)
                .await
                .unwrap()
                .is_some()
        );

        restarted.create_challenge(challenge(2, now)).await.unwrap();
        restarted.create_challenge(challenge(3, now)).await.unwrap();
        let left_store = restarted.clone();
        let right_store = restarted.clone();
        let (left, right) = tokio::join!(
            left_store.exchange_session(exchange(
                2,
                session(2, 2, "left", "nonce-2", now),
                Some(old_hash.clone()),
                now,
            )),
            right_store.exchange_session(exchange(
                3,
                session(3, 3, "right", "nonce-3", now),
                Some(old_hash),
                now,
            )),
        );
        assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
        let loser = if left.is_err() { left } else { right };
        assert_eq!(
            loser.unwrap_err(),
            ApplicationError::ReaderSessionRenewalConflict
        );

        database.cleanup().await;
    }

    #[tokio::test]
    async fn postgres_reader_session_renewal_rechecks_expiry_after_waiting_for_lock() {
        let database = TestDatabase::create().await;
        let now: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(database.pool())
            .await
            .unwrap();
        let store = PostgresReaderAuthStore::new(database.pool().clone());
        store.create_challenge(challenge(1, now)).await.unwrap();
        store
            .exchange_session(exchange(1, session(1, 1, "old", "nonce-1", now), None, now))
            .await
            .unwrap();
        let old_hash = ReaderSessionTokenHash::derive(&ReaderSessionToken::new("old"));
        sqlx::query(
            "UPDATE reader_sessions SET idle_expires_at = clock_timestamp() + interval '100 milliseconds'
             WHERE token_hash = $1",
        )
        .bind(old_hash.as_bytes().as_slice())
        .execute(database.pool())
        .await
        .unwrap();
        store.create_challenge(challenge(2, now)).await.unwrap();

        let mut blocker = database.pool().begin().await.unwrap();
        sqlx::query("SELECT session_id FROM reader_sessions WHERE token_hash = $1 FOR UPDATE")
            .bind(old_hash.as_bytes().as_slice())
            .fetch_one(&mut *blocker)
            .await
            .unwrap();
        let renewing_store = store.clone();
        let renewal = tokio::spawn(async move {
            renewing_store
                .exchange_session(exchange(
                    2,
                    session(2, 2, "new", "nonce-2", now),
                    Some(old_hash),
                    now,
                ))
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        blocker.commit().await.unwrap();

        assert_eq!(
            renewal.await.unwrap().unwrap_err(),
            ApplicationError::ReaderSessionRenewalConflict
        );
        database.cleanup().await;
    }

    #[tokio::test]
    async fn postgres_reader_auth_purge_keeps_active_renewed_session() {
        let database = TestDatabase::create().await;
        let now: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(database.pool())
            .await
            .unwrap();
        let store = PostgresReaderAuthStore::new(database.pool().clone());
        store.create_challenge(challenge(1, now)).await.unwrap();
        store
            .exchange_session(exchange(1, session(1, 1, "old", "nonce-1", now), None, now))
            .await
            .unwrap();
        store.create_challenge(challenge(2, now)).await.unwrap();
        let old_hash = ReaderSessionTokenHash::derive(&ReaderSessionToken::new("old"));
        store
            .exchange_session(exchange(
                2,
                session(2, 2, "new", "nonce-2", now),
                Some(old_hash.clone()),
                now,
            ))
            .await
            .unwrap();

        sqlx::query(
            "UPDATE reader_sessions SET invalidated_at = clock_timestamp() - interval '1 second'
             WHERE token_hash = $1",
        )
        .bind(old_hash.as_bytes().as_slice())
        .execute(database.pool())
        .await
        .unwrap();
        store.purge_expired().await.unwrap();

        let new_hash = ReaderSessionTokenHash::derive(&ReaderSessionToken::new("new"));
        assert!(
            store
                .validate_and_touch(&new_hash, "https://app.example", 900)
                .await
                .unwrap()
                .is_some()
        );
        database.cleanup().await;
    }

    #[tokio::test]
    async fn postgres_purge_removes_idle_expired_session_at_equality() {
        let database = TestDatabase::create().await;
        let now: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(database.pool())
            .await
            .unwrap();
        let store = PostgresReaderAuthStore::new(database.pool().clone());
        store.create_challenge(challenge(1, now)).await.unwrap();
        let created = store
            .exchange_session(exchange(
                1,
                session(1, 1, "idle-expiry", "idle-expiry", now),
                None,
                now,
            ))
            .await
            .unwrap();
        sqlx::query(
            "UPDATE reader_sessions SET idle_expires_at = clock_timestamp() WHERE session_id = $1",
        )
        .bind(created.session_id)
        .execute(database.pool())
        .await
        .unwrap();

        store.purge_expired().await.unwrap();

        let remaining: i64 =
            sqlx::query_scalar("SELECT count(*) FROM reader_sessions WHERE session_id = $1")
                .bind(created.session_id)
                .fetch_one(database.pool())
                .await
                .unwrap();
        assert_eq!(remaining, 0);
        database.cleanup().await;
    }

    #[tokio::test]
    async fn postgres_reader_session_touch_rechecks_expiry_after_waiting_for_lock() {
        let database = TestDatabase::create().await;
        let now: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(database.pool())
            .await
            .unwrap();
        let store = PostgresReaderAuthStore::new(database.pool().clone());
        store.create_challenge(challenge(1, now)).await.unwrap();
        store
            .exchange_session(exchange(
                1,
                session(1, 1, "token", "nonce-1", now),
                None,
                now,
            ))
            .await
            .unwrap();
        let token_hash = ReaderSessionTokenHash::derive(&ReaderSessionToken::new("token"));
        sqlx::query(
            "UPDATE reader_sessions SET idle_expires_at = clock_timestamp() + interval '100 milliseconds'
             WHERE token_hash = $1",
        )
        .bind(token_hash.as_bytes().as_slice())
        .execute(database.pool())
        .await
        .unwrap();

        let mut blocker = database.pool().begin().await.unwrap();
        sqlx::query("SELECT session_id FROM reader_sessions WHERE token_hash = $1 FOR UPDATE")
            .bind(token_hash.as_bytes().as_slice())
            .fetch_one(&mut *blocker)
            .await
            .unwrap();
        let touching_store = store.clone();
        let touch = tokio::spawn(async move {
            touching_store
                .validate_and_touch(&token_hash, "https://app.example", 900)
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        blocker.commit().await.unwrap();

        assert!(touch.await.unwrap().unwrap().is_none());
        database.cleanup().await;
    }
}
