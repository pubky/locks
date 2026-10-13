use sqlx::PgPool;
use sqlx::migrate::Migrator;

use super::PostgresError;

// Embed ordered migrations so production and isolated test schemas execute identical SQL.
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
        assert_table_exists(&mut connection, "content_lock_ownership").await;
        assert_table_exists(&mut connection, "content_lock_deletion_jobs").await;
        assert_table_exists(&mut connection, "content_lock_force_deletion_receipts").await;
        assert_table_exists(&mut connection, "content_lock_publication_intents").await;
        assert_table_exists(&mut connection, "content_lock_deletion_task_snapshot").await;
        assert_table_exists(&mut connection, "content_lock_access_drain_credentials").await;
        assert_table_exists(&mut connection, "content_lock_access_drain_reads").await;
        assert_column_exists(&mut connection, "verification_tasks", "creator").await;
        assert_column_exists(&mut connection, "verification_tasks", "bundle_id").await;
        assert_column_exists(&mut connection, "verification_tasks", "next_attempt_at").await;
        assert_column_exists(&mut connection, "verification_tasks", "claim_token").await;
        assert_column_exists(&mut connection, "verification_tasks", "terminal_reason").await;
        assert_column_exists(
            &mut connection,
            "verification_tasks",
            "invoice_admission_phase",
        )
        .await;
        assert_column_exists(&mut connection, "verification_tasks", "invoice_created_at").await;
        assert_column_exists(&mut connection, "verification_tasks", "payment_deadline").await;
        assert_column_exists(
            &mut connection,
            "content_lock_deletion_jobs",
            "final_issuance_started_at",
        )
        .await;
        assert_column_exists(
            &mut connection,
            "verification_tasks",
            "invoice_admission_intent",
        )
        .await;
        assert_column_exists(
            &mut connection,
            "content_lock_deletion_jobs",
            "final_credential_issuance_deadline",
        )
        .await;
        assert_column_exists(
            &mut connection,
            "verification_tasks",
            "admission_deadline_at",
        )
        .await;
        assert_column_exists(
            &mut connection,
            "content_lock_deletion_jobs",
            "final_read_deadline",
        )
        .await;
        assert_column_exists(
            &mut connection,
            "verification_tasks",
            "entitlement_to_publish",
        )
        .await;
        assert_column_exists(
            &mut connection,
            "content_lock_deletion_task_snapshot",
            "had_active_credential_at_cutoff",
        )
        .await;
        assert_column_exists(
            &mut connection,
            "content_lock_deletion_task_snapshot",
            "final_credential_eligible_at",
        )
        .await;
        assert_column_exists(&mut connection, "access_credentials", "deletion_job_id").await;
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
        assert_column_exists(&mut connection, "content_lock_ownership", "creator").await;
        assert_column_exists(&mut connection, "content_lock_ownership", "guarded_path").await;
        assert_column_exists(&mut connection, "content_lock_ownership", "lock_id").await;
        assert_column_exists(&mut connection, "content_lock_ownership", "status").await;
        assert_column_exists(
            &mut connection,
            "content_lock_deletion_jobs",
            "frozen_content_lock",
        )
        .await;
        assert_column_exists(&mut connection, "content_lock_deletion_jobs", "claim_token").await;
        assert_column_exists(
            &mut connection,
            "content_lock_deletion_jobs",
            "force_requested_at",
        )
        .await;
        assert_unique_constraint_exists(
            &mut connection,
            "verification_tasks",
            "verification_tasks_creator_bundle_unique",
        )
        .await;
        assert_unique_constraint_exists(
            &mut connection,
            "content_lock_ownership",
            "content_lock_ownership_creator_path_unique",
        )
        .await;
        assert_unique_constraint_exists(
            &mut connection,
            "content_lock_deletion_jobs",
            "content_lock_deletion_jobs_creator_lock_unique",
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
        assert_eq!(
            applied_versions,
            super::MIGRATOR
                .iter()
                .map(|migration| migration.version)
                .collect::<Vec<_>>()
        );

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

    #[tokio::test]
    async fn verification_task_state_constraint_rejects_incomplete_terminal_and_publication_tuples()
    {
        let database = TestDatabase::create().await;

        for (status, extra_columns, extra_values) in [
            ("completed", "", ""),
            ("failed", ", started_at, completed_at", ", NOW(), NOW()"),
            ("expired", ", started_at, completed_at", ", NOW(), NOW()"),
            ("publishing_entitlement", ", started_at", ", NOW()"),
        ] {
            let result = sqlx::query(&format!(
                "INSERT INTO verification_tasks (
                    task_id, status, submitted_proof_bundle, submitted_at, creator, bundle_id
                    {extra_columns}
                 ) VALUES ($1, $2, '{{}}'::jsonb, NOW(), $3, $4 {extra_values})"
            ))
            .bind(uuid::Uuid::new_v4())
            .bind(status)
            .bind(format!("creator-{status}"))
            .bind(format!("bundle-{status}"))
            .execute(database.pool())
            .await;

            assert!(result.is_err(), "incomplete {status} tuple was accepted");
        }

        database.cleanup().await;
    }

    #[tokio::test]
    async fn invoice_admission_migration_marks_existing_accepted_tasks_ready() {
        let database = TestDatabase::create_unmigrated().await;
        let baseline = Migrator {
            migrations: Cow::Owned(super::MIGRATOR.iter().take(12).cloned().collect()),
            ignore_missing: false,
            locking: false,
            no_tx: false,
        };
        baseline.run(database.pool()).await.unwrap();
        let task_id = uuid::Uuid::new_v4();
        sqlx::query(
            "INSERT INTO verification_tasks (
                task_id, status, submitted_proof_bundle, submitted_at, creator, bundle_id
             ) VALUES ($1, 'pending', '{}'::jsonb, clock_timestamp(), 'creator', 'bundle')",
        )
        .bind(task_id)
        .execute(database.pool())
        .await
        .unwrap();

        super::run_migrations(database.pool()).await.unwrap();

        let row: (
            String,
            Option<serde_json::Value>,
            Option<time::OffsetDateTime>,
        ) = sqlx::query_as(
            "SELECT invoice_admission_phase, invoice_admission_intent,
                        admission_deadline_at
                 FROM verification_tasks WHERE task_id = $1",
        )
        .bind(task_id)
        .fetch_one(database.pool())
        .await
        .unwrap();
        assert_eq!(row, ("ready".to_owned(), None, None));

        database.cleanup().await;
    }

    #[tokio::test]
    async fn migration_0021_rejects_every_preexisting_resumable_deletion_state() {
        for (state, failure_code) in [
            ("queued", None),
            ("running", None),
            ("failed", Some("retry_exhausted")),
        ] {
            let database = TestDatabase::create().await;
            sqlx::raw_sql(
                "DROP TABLE content_lock_access_drain_reads;
             DROP TABLE content_lock_access_drain_credentials;
             ALTER TABLE access_credentials DROP COLUMN deletion_job_id;
             ALTER TABLE content_lock_deletion_task_snapshot
                 DROP CONSTRAINT content_lock_deletion_task_snapshot_final_issuance_valid,
                 DROP CONSTRAINT content_lock_deletion_task_snapshot_final_eligibility_valid,
                 DROP COLUMN final_credential_issued_at,
                 DROP COLUMN final_credential_eligible_at,
                 DROP COLUMN had_active_credential_at_cutoff;
             ALTER TABLE content_lock_deletion_jobs
                 DROP CONSTRAINT content_lock_deletion_jobs_final_window_shape,
                 DROP COLUMN final_read_deadline,
                 DROP COLUMN final_credential_issuance_deadline,
                 DROP COLUMN final_issuance_started_at;",
            )
            .execute(database.pool())
            .await
            .unwrap();
            sqlx::query(
                "INSERT INTO content_lock_deletion_jobs
                 (job_id, creator, lock_id, frozen_content_lock, deletion_started_at,
                  state, phase, failure_code, claimed_by, claim_token, claim_expires_at)
             VALUES (
                 $1, 'creator', 'lock', '{}'::jsonb, NOW(),
                 $2, 'withdraw', $3,
                 CASE WHEN $2 = 'running' THEN 'worker' ELSE NULL END,
                 CASE WHEN $2 = 'running' THEN $4::uuid ELSE NULL END,
                 CASE WHEN $2 = 'running' THEN NOW() + INTERVAL '1 minute' ELSE NULL END
             )",
            )
            .bind(uuid::Uuid::new_v4())
            .bind(state)
            .bind(failure_code)
            .bind(uuid::Uuid::new_v4())
            .execute(database.pool())
            .await
            .unwrap();
            let migration = super::MIGRATOR
                .iter()
                .find(|migration| migration.version == 21)
                .expect("migration 0021 exists");

            let error = sqlx::raw_sql(migration.sql.as_ref())
                .execute(database.pool())
                .await
                .expect_err(
                    "0021 must fail closed instead of misclassifying a resumable deletion job",
                );
            assert!(
                error
                    .to_string()
                    .contains("drain or explicitly reset pre-0021 deletion jobs"),
                "unexpected migration error for {state}: {error}"
            );

            database.cleanup().await;
        }
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
