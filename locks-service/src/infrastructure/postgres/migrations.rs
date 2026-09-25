use sqlx::PgPool;
use sqlx::migrate::Migrator;

use super::PostgresError;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

/// Runs managed Postgres migrations for Lock Server runtime-owned tables.
pub async fn run_migrations(pool: &PgPool) -> Result<(), PostgresError> {
    MIGRATOR.run(pool).await.map_err(PostgresError::from)
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::collections::HashSet;

    use sqlx::Row;
    use sqlx::migrate::Migrator;

    use super::super::testing::TestDatabase;

    #[test]
    fn migration_versions_are_unique() {
        let mut versions = HashSet::new();
        for migration in super::MIGRATOR.iter() {
            assert!(
                versions.insert(migration.version),
                "duplicate migration version {}",
                migration.version
            );
        }
    }

    #[tokio::test]
    async fn run_migrations_creates_runtime_tables_and_is_idempotent() {
        let database = TestDatabase::create().await;

        super::run_migrations(database.pool())
            .await
            .expect("second migration run is idempotent");

        let mut connection = database
            .pool()
            .acquire()
            .await
            .expect("acquire migrated connection");
        assert_table_exists(&mut connection, "verification_tasks").await;
        assert_table_exists(&mut connection, "access_credentials").await;
        assert_table_exists(&mut connection, "creator_authorities").await;
        assert_table_exists(&mut connection, "pending_creator_connect_flows").await;
        assert_table_exists(&mut connection, "frontend_session_codes").await;
        assert_table_exists(&mut connection, "frontend_sessions").await;
        assert_column_exists(&mut connection, "verification_tasks", "creator").await;
        assert_column_exists(&mut connection, "verification_tasks", "bundle_id").await;
        assert_column_exists(&mut connection, "verification_tasks", "next_attempt_at").await;
        assert_column_exists(&mut connection, "verification_tasks", "claim_token").await;
        assert_index_exists(
            &mut connection,
            "verification_tasks",
            "verification_tasks_due_pending_idx",
        )
        .await;
        assert_column_exists(&mut connection, "creator_authorities", "auth_kind").await;
        assert_column_exists(&mut connection, "creator_authorities", "granted_scopes").await;
        assert_column_exists(&mut connection, "creator_authorities", "secret").await;
        assert_column_exists(
            &mut connection,
            "pending_creator_connect_flows",
            "authorization_url",
        )
        .await;
        assert_column_exists(&mut connection, "frontend_session_codes", "code_hash").await;
        assert_column_exists(&mut connection, "frontend_sessions", "token_hash").await;
        assert_unique_constraint_exists(
            &mut connection,
            "verification_tasks",
            "verification_tasks_creator_bundle_unique",
        )
        .await;
        drop(connection);

        database.cleanup().await;
    }

    #[tokio::test]
    async fn reset_only_upgrade_clears_runtime_state_once() {
        let database = TestDatabase::create_unmigrated().await;
        let baseline = Migrator {
            migrations: Cow::Owned(super::MIGRATOR.iter().take(9).cloned().collect()),
            ignore_missing: false,
            locking: false,
            no_tx: false,
        };
        baseline.run(database.pool()).await.unwrap();

        let task_id = uuid::Uuid::new_v4();
        sqlx::query(
            "INSERT INTO verification_tasks (
                 task_id, status, submitted_proof_bundle, submitted_at, creator, bundle_id
             ) VALUES ($1, 'pending', '{}'::jsonb, NOW(), 'creator', 'bundle')",
        )
        .bind(task_id)
        .execute(database.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO access_credentials (
                 lookup_key, creator, bundle_id, expires_at
             ) VALUES ($1, 'creator', 'bundle', NOW() + INTERVAL '1 hour')",
        )
        .bind(b"prototype-credential".as_slice())
        .execute(database.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO creator_authorities (
                 creator, auth_kind, granted_scopes, secret
             ) VALUES ('creator', 'cookie', '[]'::jsonb, 'prototype-secret')",
        )
        .execute(database.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO pending_creator_connect_flows (
                 flow_id, return_to, state, authorization_url, requested_scopes,
                 created_at, expires_at
             ) VALUES (
                 'flow', 'https://example.test/return', 'state',
                 'pubkyauth://signin', '[]'::jsonb, NOW(), NOW() + INTERVAL '1 hour'
             )",
        )
        .execute(database.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO frontend_session_codes (
                 code_hash, creator, state, return_to, created_at, expires_at
             ) VALUES (
                 $1, 'creator', 'state', 'https://example.test/return',
                 NOW(), NOW() + INTERVAL '1 hour'
             )",
        )
        .bind(b"prototype-code".as_slice())
        .execute(database.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO frontend_sessions (token_hash, creator, created_at, expires_at)
             VALUES ($1, 'creator', NOW(), NOW() + INTERVAL '1 hour')",
        )
        .bind(b"prototype-session".as_slice())
        .execute(database.pool())
        .await
        .unwrap();

        super::run_migrations(database.pool()).await.unwrap();

        for table in [
            "verification_tasks",
            "access_credentials",
            "creator_authorities",
            "pending_creator_connect_flows",
            "frontend_session_codes",
            "frontend_sessions",
        ] {
            let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
                .fetch_one(database.pool())
                .await
                .unwrap();
            assert_eq!(count, 0, "prototype rows remain in {table}");
        }

        let applied_versions: Vec<i64> =
            sqlx::query_scalar("SELECT version FROM _sqlx_migrations ORDER BY version")
                .fetch_all(database.pool())
                .await
                .unwrap();
        assert_eq!(applied_versions, (1..=10).collect::<Vec<_>>());

        sqlx::query(
            "INSERT INTO frontend_sessions (token_hash, creator, created_at, expires_at)
             VALUES ($1, 'post-upgrade-creator', NOW(), NOW() + INTERVAL '1 hour')",
        )
        .bind(b"post-upgrade-session".as_slice())
        .execute(database.pool())
        .await
        .unwrap();
        super::run_migrations(database.pool()).await.unwrap();
        let session_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM frontend_sessions")
            .fetch_one(database.pool())
            .await
            .unwrap();
        assert_eq!(session_count, 1, "restart repeated the destructive reset");

        database.cleanup().await;
    }

    async fn assert_table_exists(
        connection: &mut sqlx::pool::PoolConnection<sqlx::Postgres>,
        table_name: &str,
    ) {
        let exists = sqlx::query(
            "SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = current_schema()
                  AND table_name = $1
            )",
        )
        .bind(table_name)
        .fetch_one(connection.as_mut())
        .await
        .expect("query table existence")
        .try_get::<bool, _>(0)
        .expect("table existence is bool");

        assert!(exists, "expected table {table_name} to exist");
    }

    async fn assert_column_exists(
        connection: &mut sqlx::pool::PoolConnection<sqlx::Postgres>,
        table_name: &str,
        column_name: &str,
    ) {
        let exists = sqlx::query(
            "SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = $1
                  AND column_name = $2
            )",
        )
        .bind(table_name)
        .bind(column_name)
        .fetch_one(connection.as_mut())
        .await
        .expect("query column existence")
        .try_get::<bool, _>(0)
        .expect("column existence is bool");

        assert!(
            exists,
            "expected column {table_name}.{column_name} to exist"
        );
    }

    async fn assert_unique_constraint_exists(
        connection: &mut sqlx::pool::PoolConnection<sqlx::Postgres>,
        table_name: &str,
        constraint_name: &str,
    ) {
        let exists = sqlx::query(
            "SELECT EXISTS (
                SELECT 1
                FROM information_schema.table_constraints
                WHERE table_schema = current_schema()
                  AND table_name = $1
                  AND constraint_name = $2
                  AND constraint_type = 'UNIQUE'
            )",
        )
        .bind(table_name)
        .bind(constraint_name)
        .fetch_one(connection.as_mut())
        .await
        .expect("query unique constraint existence")
        .try_get::<bool, _>(0)
        .expect("constraint existence is bool");

        assert!(
            exists,
            "expected unique constraint {constraint_name} on {table_name} to exist"
        );
    }

    async fn assert_index_exists(
        connection: &mut sqlx::pool::PoolConnection<sqlx::Postgres>,
        table_name: &str,
        index_name: &str,
    ) {
        let exists = sqlx::query(
            "SELECT EXISTS (
                SELECT 1
                FROM pg_indexes
                WHERE schemaname = current_schema()
                  AND tablename = $1
                  AND indexname = $2
            )",
        )
        .bind(table_name)
        .bind(index_name)
        .fetch_one(connection.as_mut())
        .await
        .expect("query index existence")
        .try_get::<bool, _>(0)
        .expect("index existence is bool");

        assert!(
            exists,
            "expected index {index_name} on {table_name} to exist"
        );
    }
}
