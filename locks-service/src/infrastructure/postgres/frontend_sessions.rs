use std::str::FromStr;

use async_trait::async_trait;
use sqlx::{PgPool, Row};

use locks_core::ids::CreatorPubky;

use crate::application::errors::ApplicationError;
use crate::application::models::{
    FrontendSessionCode, FrontendSessionCodeRecord, FrontendSessionRecord, FrontendSessionToken,
};
use crate::application::ports::{FrontendSessionCodeStore, FrontendSessionStore};

/// Postgres-backed store for short-lived frontend session exchange codes.
#[derive(Debug, Clone)]
pub struct PostgresFrontendSessionCodeStore {
    pool: PgPool,
}

impl PostgresFrontendSessionCodeStore {
    /// Creates a store backed by the provided migrated Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

/// Postgres-backed store for Locks-local frontend sessions.
#[derive(Debug, Clone)]
pub struct PostgresFrontendSessionStore {
    pool: PgPool,
}

impl PostgresFrontendSessionStore {
    /// Creates a store backed by the provided migrated Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl FrontendSessionCodeStore for PostgresFrontendSessionCodeStore {
    async fn insert_frontend_session_code(
        &self,
        record: FrontendSessionCodeRecord,
    ) -> Result<(), ApplicationError> {
        let result = sqlx::query(
            "INSERT INTO frontend_session_codes (
                code_hash,
                creator,
                state,
                return_to,
                created_at,
                expires_at,
                consumed_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (code_hash) DO NOTHING",
        )
        .bind(secret_lookup_hash(record.code.expose_code()).as_slice())
        .bind(record.creator.to_string())
        .bind(record.state)
        .bind(record.return_to)
        .bind(record.created_at)
        .bind(record.expires_at)
        .bind(record.consumed_at)
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;

        if result.rows_affected() == 0 {
            return Err(ApplicationError::DuplicateRecord {
                record: "frontend_session_code",
            });
        }

        Ok(())
    }

    async fn consume_frontend_session_code(
        &self,
        code: &FrontendSessionCode,
        now: time::OffsetDateTime,
    ) -> Result<Option<FrontendSessionCodeRecord>, ApplicationError> {
        let row = sqlx::query(
            "WITH candidate AS (
                SELECT creator, state, return_to, created_at, expires_at, consumed_at
                FROM frontend_session_codes
                WHERE code_hash = $1
                  AND consumed_at IS NULL
            ), updated AS (
                UPDATE frontend_session_codes
                SET consumed_at = $2
                WHERE code_hash = $1
                  AND consumed_at IS NULL
                RETURNING 1
            )
            SELECT creator, state, return_to, created_at, expires_at, consumed_at
            FROM candidate
            WHERE EXISTS (SELECT 1 FROM updated)",
        )
        .bind(secret_lookup_hash(code.expose_code()).as_slice())
        .bind(now)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;

        row.map(|row| row_to_code_record(row, code.clone()))
            .transpose()
    }
}

#[async_trait]
impl FrontendSessionStore for PostgresFrontendSessionStore {
    async fn insert_frontend_session(
        &self,
        record: FrontendSessionRecord,
    ) -> Result<(), ApplicationError> {
        let result = sqlx::query(
            "INSERT INTO frontend_sessions (token_hash, creator, created_at, expires_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (token_hash) DO NOTHING",
        )
        .bind(secret_lookup_hash(record.token.expose_token()).as_slice())
        .bind(record.creator.to_string())
        .bind(record.created_at)
        .bind(record.expires_at)
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;

        if result.rows_affected() == 0 {
            return Err(ApplicationError::DuplicateRecord {
                record: "frontend_session",
            });
        }

        Ok(())
    }

    async fn get_frontend_session(
        &self,
        token: &FrontendSessionToken,
    ) -> Result<Option<FrontendSessionRecord>, ApplicationError> {
        let row = sqlx::query(
            "SELECT creator, created_at, expires_at
            FROM frontend_sessions
            WHERE token_hash = $1",
        )
        .bind(secret_lookup_hash(token.expose_token()).as_slice())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;

        row.map(|row| row_to_session_record(row, token.clone()))
            .transpose()
    }

    async fn delete_frontend_session(
        &self,
        token: &FrontendSessionToken,
    ) -> Result<(), ApplicationError> {
        sqlx::query("DELETE FROM frontend_sessions WHERE token_hash = $1")
            .bind(secret_lookup_hash(token.expose_token()).as_slice())
            .execute(&self.pool)
            .await
            .map_err(storage_error)?;
        Ok(())
    }
}

fn row_to_code_record(
    row: sqlx::postgres::PgRow,
    code: FrontendSessionCode,
) -> Result<FrontendSessionCodeRecord, ApplicationError> {
    Ok(FrontendSessionCodeRecord {
        code,
        creator: parse_creator(row.try_get::<String, _>("creator").map_err(storage_error)?)?,
        state: row.try_get("state").map_err(storage_error)?,
        return_to: row.try_get("return_to").map_err(storage_error)?,
        created_at: row.try_get("created_at").map_err(storage_error)?,
        expires_at: row.try_get("expires_at").map_err(storage_error)?,
        consumed_at: row.try_get("consumed_at").map_err(storage_error)?,
    })
}

fn row_to_session_record(
    row: sqlx::postgres::PgRow,
    token: FrontendSessionToken,
) -> Result<FrontendSessionRecord, ApplicationError> {
    Ok(FrontendSessionRecord {
        token,
        creator: parse_creator(row.try_get::<String, _>("creator").map_err(storage_error)?)?,
        created_at: row.try_get("created_at").map_err(storage_error)?,
        expires_at: row.try_get("expires_at").map_err(storage_error)?,
    })
}

fn parse_creator(value: String) -> Result<CreatorPubky, ApplicationError> {
    CreatorPubky::from_str(&value).map_err(|error| ApplicationError::Storage {
        message: format!("invalid frontend session creator stored in Postgres: {error}"),
    })
}

fn secret_lookup_hash(value: &str) -> [u8; 32] {
    *blake3::hash(value.as_bytes()).as_bytes()
}

fn storage_error(error: sqlx::Error) -> ApplicationError {
    ApplicationError::Storage {
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::{PostgresFrontendSessionCodeStore, PostgresFrontendSessionStore};
    use crate::application::models::{
        FrontendSessionCode, FrontendSessionCodeRecord, FrontendSessionRecord, FrontendSessionToken,
    };
    use crate::application::ports::{FrontendSessionCodeStore, FrontendSessionStore};
    use crate::infrastructure::postgres::testing::TestDatabase;
    use locks_core::ids::CreatorPubky;
    use sqlx::Row;
    use time::macros::datetime;

    #[tokio::test]
    async fn frontend_session_code_insert_consume_and_single_use_semantics_match_port_contract() {
        let database = TestDatabase::create().await;
        let store = PostgresFrontendSessionCodeStore::new(database.pool().clone());
        let code = FrontendSessionCode::new("raw-one-time-code");
        let record = frontend_session_code_record(code.clone());
        let consumed_at = datetime!(2026-05-29 12:01:00 UTC);

        assert_eq!(
            store
                .consume_frontend_session_code(&code, consumed_at)
                .await
                .unwrap(),
            None
        );

        store
            .insert_frontend_session_code(record.clone())
            .await
            .unwrap();
        let consumed = store
            .consume_frontend_session_code(&code, consumed_at)
            .await
            .unwrap()
            .expect("code consumed once");
        assert_eq!(
            consumed,
            FrontendSessionCodeRecord {
                consumed_at: None,
                ..record.clone()
            }
        );
        assert_eq!(
            store
                .consume_frontend_session_code(&code, consumed_at)
                .await
                .unwrap(),
            None
        );

        database.cleanup().await;
    }

    #[tokio::test]
    async fn frontend_session_codes_are_hashed_at_rest_without_raw_code() {
        let database = TestDatabase::create().await;
        let store = PostgresFrontendSessionCodeStore::new(database.pool().clone());
        let raw_code = "raw-one-time-code";
        let code = FrontendSessionCode::new(raw_code);

        store
            .insert_frontend_session_code(frontend_session_code_record(code.clone()))
            .await
            .unwrap();

        assert_raw_value_not_stored(
            database.pool(),
            "frontend_session_codes",
            "code_hash",
            raw_code,
        )
        .await;

        database.cleanup().await;
    }

    #[tokio::test]
    async fn frontend_session_insert_get_delete_and_missing_semantics_match_port_contract() {
        let database = TestDatabase::create().await;
        let store = PostgresFrontendSessionStore::new(database.pool().clone());
        let token = FrontendSessionToken::new("raw-session-token");
        let record = frontend_session_record(token.clone());

        assert_eq!(store.get_frontend_session(&token).await.unwrap(), None);

        store.insert_frontend_session(record.clone()).await.unwrap();
        assert_eq!(
            store.get_frontend_session(&token).await.unwrap(),
            Some(record.clone())
        );

        store.delete_frontend_session(&token).await.unwrap();
        store.delete_frontend_session(&token).await.unwrap();
        assert_eq!(store.get_frontend_session(&token).await.unwrap(), None);

        database.cleanup().await;
    }

    #[tokio::test]
    async fn frontend_sessions_are_hashed_at_rest_without_raw_token() {
        let database = TestDatabase::create().await;
        let store = PostgresFrontendSessionStore::new(database.pool().clone());
        let raw_token = "raw-session-token";
        let token = FrontendSessionToken::new(raw_token);

        store
            .insert_frontend_session(frontend_session_record(token.clone()))
            .await
            .unwrap();

        assert_raw_value_not_stored(
            database.pool(),
            "frontend_sessions",
            "token_hash",
            raw_token,
        )
        .await;

        database.cleanup().await;
    }

    fn frontend_session_code_record(code: FrontendSessionCode) -> FrontendSessionCodeRecord {
        FrontendSessionCodeRecord {
            code,
            creator: CreatorPubky::from_str(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            )
            .unwrap(),
            state: "state-123".to_owned(),
            return_to: "https://app.example/locks/callback".to_owned(),
            created_at: datetime!(2026-05-29 12:00:00 UTC),
            expires_at: datetime!(2026-05-29 12:05:00 UTC),
            consumed_at: None,
        }
    }

    fn frontend_session_record(token: FrontendSessionToken) -> FrontendSessionRecord {
        FrontendSessionRecord {
            token,
            creator: CreatorPubky::from_str(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            )
            .unwrap(),
            created_at: datetime!(2026-05-29 12:00:00 UTC),
            expires_at: datetime!(2026-05-29 13:00:00 UTC),
        }
    }

    async fn assert_raw_value_not_stored(
        pool: &sqlx::PgPool,
        table: &str,
        hash_column: &str,
        raw_value: &str,
    ) {
        let query = format!(
            "SELECT EXISTS (
                SELECT 1
                FROM {table}
                WHERE encode({hash_column}, 'escape') = $1
                   OR creator = $1
            )"
        );
        let present = sqlx::query(&query)
            .bind(raw_value)
            .fetch_one(pool)
            .await
            .expect("query raw value absence")
            .try_get::<bool, _>(0)
            .expect("exists result is bool");

        assert!(!present, "raw secret value was stored in {table}");
    }
}
