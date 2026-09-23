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
    use std::collections::HashSet;

    use sqlx::Row;

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
        assert_table_exists(&mut connection, "paykit_task_admissions").await;
        assert_column_exists(
            &mut connection,
            "paykit_task_admissions",
            "payment_in_hours",
        )
        .await;
        assert_column_exists(
            &mut connection,
            "paykit_task_admissions",
            "invoice_created_at",
        )
        .await;
        assert_column_exists(
            &mut connection,
            "paykit_task_admissions",
            "payment_deadline",
        )
        .await;
        assert_column_exists(&mut connection, "verification_tasks", "creator").await;
        assert_column_exists(&mut connection, "verification_tasks", "bundle_id").await;
        assert_column_exists(&mut connection, "verification_tasks", "next_attempt_at").await;
        assert_column_exists(&mut connection, "verification_tasks", "claim_token").await;
        assert_column_exists(
            &mut connection,
            "content_lock_deletion_jobs",
            "final_issuance_started_at",
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
            "content_lock_deletion_jobs",
            "final_read_deadline",
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
    async fn migration_0016_rejects_every_preexisting_resumable_deletion_state() {
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
                .find(|migration| migration.version == 16)
                .expect("migration 0016 exists");

            let error = sqlx::raw_sql(migration.sql.as_ref())
                .execute(database.pool())
                .await
                .expect_err(
                    "0016 must fail closed instead of misclassifying a resumable Task 7 job",
                );
            assert!(
                error
                    .to_string()
                    .contains("drain or explicitly reset pre-0016 deletion jobs"),
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
