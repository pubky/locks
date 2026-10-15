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

    const EXACT_PR19_MIGRATION_SHA384: [&str; 23] = [
        "482b82635a50208e887c12d2c9a81828578eea71ad6005281552bae23d74d945095e257d89671bba929b09e95f55a8eb",
        "1bbe43ea9ca0331bac97135625a73c04e7b570631b9603b5a3211b92ea727d30d370f219654fab08b68b796caf99b976",
        "f18e63c7518ade25470087d8d39f91bc214ee8c7882ad4482f995650af82ecbfe0acbf6c5b2242ea6e2e2726a9b84556",
        "af27aceddf6791c09f9356e86e923bf013ad7feb245fecb8e2fb27164f586e1e524df6b1be538f0531403095b8905b0d",
        "b41af00f334586d8411f1c50ea3aa730ad575df125885e56fe0c5ba3b311cd951eb0f605736c314ce8b4c43d799ed33f",
        "663e400c355d04a460f1eb1e34ab42a5b71cc860bb2b9c510dc76d2dc10da20e2519f9bd61f262ed52f40a24d0fd1e03",
        "f6dbc58bcf78ac160574d58a3622e2dabf7db99b739db64ed8448ea0dfcb4b034db41a02e742b845cefdd7c2a7f132fa",
        "83ad8daa6e0245009d0efda398af9b677537e1255a0871ffb57c9eefb5d9b1c85ff213edcb8eb71917424b2ce04b7b99",
        "2cc5858784a43fd9a619b6e647bed251ea037fe553bc5d09af5697ca78d7887dafd1f7ab22217de42a89de4f032e02a8",
        "e4913fa86785be29dcf5b6e5c8d07f1230ff299105ba9cded23c5f3afbb7e3ae47ab632aacaa8618aced4a5f73423cd9",
        "53590b37260f2fcea860f151d99a052a03c23fde60a3c72bb9531fbc65cd8a80ff26c848924f7efbe7fd209e31d17172",
        "fdf7fb3e947bae3d3c0db5c6acf55fb2113483e3c780624b92435114935a3b007c29141b7b50ae041978b67d1846f55b",
        "efa0e2117395f7f496983201b72f10b026d78c74f2c1dffd8ea06f7b858f51a5b78198b8034101debc604a0d2cba1a30",
        "f144f734bf71a985d42d0f9b73fb52c2120a967f4aa5a814595cb3d361976e5f91ef70139c043d15b466350d1362cd91",
        "2e99b695e60afadf7535cb481c678a475469c29c625f85fc61a752be5f6d4c728c1821587b34dea7a7d0e0bec35e7422",
        "5481351f5865c9d634dea6d8f28a16a7b58c6b1979739d15d72f089177c1c20235fdc037abce4d351e5a0f6c235038ee",
        "84ac646f5f1b22edf1325cc5d36a93b3ed53cab514136e15c12d237013e4453423aabdf01689d69187b7e91c4f8eb21a",
        "eb4cec405a6d003cbc811328c5b29f1be4deaf078d5c236e0b42158633e5a8f001f9bb2e8370e139dfa738493c7f77a7",
        "c2abdd3fc95c35e24c76fa77830e761bd44be9a97ab4873f29746975dcc1893fb83556ac98ac26829f39cf599ee01bbc",
        "73aaae14d9a4b66d42e66b48729695539367a5d9ff2364c9adb8456414e4dfc092efdd55eacd670cdbd1a1880dbe8d2f",
        "2fa8062175b72c149d67a603cc864fa98b8d1e713e3f68b902062738c4f9a587afb601a563b70e31130d39a94515d56d",
        "bd6f355ca99d2166f9ecaa55b36f21218260ec82b59f5744b6b05809f4e3edc819795607030300bcb3c9024e696c6284",
        "3247642df5d61ee2dbaceaa74b5f0555fa723c1719fa335b6ae5815176f3d67a823ee3fe47c14ee0e5cd9d20b29cd7a6",
    ];

    fn checksum_hex(checksum: &[u8]) -> String {
        checksum.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn exact_pr19_migrator() -> Migrator {
        let migrations = EXACT_PR19_MIGRATION_SHA384
            .iter()
            .enumerate()
            .map(|(index, expected_checksum)| {
                let version = i64::try_from(index + 1).unwrap();
                let migration = super::MIGRATOR
                    .iter()
                    .find(|migration| migration.version == version)
                    .unwrap_or_else(|| panic!("exact PR19 migration {version:04} is missing"));
                assert_eq!(
                    checksum_hex(migration.checksum.as_ref()),
                    *expected_checksum,
                    "migration {:04} differs from the exact published PR19 artifact",
                    migration.version
                );
                migration.clone()
            })
            .collect();

        Migrator {
            migrations: Cow::Owned(migrations),
            ignore_missing: false,
            locking: false,
            no_tx: false,
        }
    }

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
    async fn migration_0024_rejects_every_preexisting_resumable_deletion_state() {
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
                .find(|migration| migration.version == 24)
                .expect("migration 0024 exists");

            let error = sqlx::raw_sql(migration.sql.as_ref())
                .execute(database.pool())
                .await
                .expect_err(
                    "0024 must fail closed instead of misclassifying a resumable deletion job",
                );
            assert!(
                error
                    .to_string()
                    .contains("drain or explicitly reset pre-0024 deletion jobs"),
                "unexpected migration error for {state}: {error}"
            );

            database.cleanup().await;
        }
    }

    #[tokio::test]
    async fn exact_pr18_migration_prefix_upgrades_through_pr19_without_collision() {
        let database = TestDatabase::create_unmigrated().await;
        let pr18 = Migrator {
            migrations: Cow::Owned(super::MIGRATOR.iter().take(20).cloned().collect()),
            ignore_missing: false,
            locking: false,
            no_tx: false,
        };

        pr18.run(database.pool()).await.unwrap();
        let pr18_versions: Vec<i64> =
            sqlx::query_scalar("SELECT version FROM _sqlx_migrations ORDER BY version")
                .fetch_all(database.pool())
                .await
                .unwrap();
        assert_eq!(pr18_versions, (1..=20).collect::<Vec<_>>());

        super::run_migrations(database.pool()).await.unwrap();
        let upgraded_versions: Vec<i64> =
            sqlx::query_scalar("SELECT version FROM _sqlx_migrations ORDER BY version")
                .fetch_all(database.pool())
                .await
                .unwrap();
        assert_eq!(upgraded_versions, (1..=24).collect::<Vec<_>>());

        database.cleanup().await;
    }

    #[tokio::test]
    async fn exact_pr19_migration_prefix_upgrades_through_pr20_without_collision() {
        let pr19 = exact_pr19_migrator();
        let database = TestDatabase::create_unmigrated().await;
        pr19.run(database.pool()).await.unwrap();
        let before: Vec<(i64, Vec<u8>)> =
            sqlx::query_as("SELECT version, checksum FROM _sqlx_migrations ORDER BY version")
                .fetch_all(database.pool())
                .await
                .unwrap();
        assert_eq!(
            before.iter().map(|row| row.0).collect::<Vec<_>>(),
            (1..=23).collect::<Vec<_>>()
        );
        assert_eq!(
            before
                .iter()
                .map(|row| checksum_hex(&row.1))
                .collect::<Vec<_>>(),
            EXACT_PR19_MIGRATION_SHA384,
            "seeded migration ledger must contain exact published PR19 checksums"
        );

        super::run_migrations(database.pool()).await.unwrap();
        let after: Vec<(i64, Vec<u8>)> =
            sqlx::query_as("SELECT version, checksum FROM _sqlx_migrations ORDER BY version")
                .fetch_all(database.pool())
                .await
                .unwrap();
        assert_eq!(
            after.iter().map(|row| row.0).collect::<Vec<_>>(),
            (1..=24).collect::<Vec<_>>()
        );
        assert_eq!(
            &after[..23],
            before.as_slice(),
            "PR19 migration checksums changed"
        );
        database.cleanup().await;
    }

    #[tokio::test]
    async fn upgrade_changes_deletion_job_task_reference_to_set_null() {
        let database = TestDatabase::create_unmigrated().await;
        let prior_schema = Migrator {
            migrations: Cow::Owned(super::MIGRATOR.iter().take(22).cloned().collect()),
            ignore_missing: false,
            locking: false,
            no_tx: false,
        };
        prior_schema.run(database.pool()).await.unwrap();

        let before: String = sqlx::query_scalar(
            "SELECT confdeltype::text
             FROM pg_constraint
             WHERE conrelid = 'verification_tasks'::regclass
               AND conname = 'verification_tasks_deletion_job_id_fkey'",
        )
        .fetch_one(database.pool())
        .await
        .unwrap();
        assert_eq!(before, "a", "prior schema must retain NO ACTION semantics");

        super::run_migrations(database.pool()).await.unwrap();

        let after: String = sqlx::query_scalar(
            "SELECT confdeltype::text
             FROM pg_constraint
             WHERE conrelid = 'verification_tasks'::regclass
               AND conname = 'verification_tasks_deletion_job_id_fkey'",
        )
        .fetch_one(database.pool())
        .await
        .unwrap();
        assert_eq!(after, "n", "upgrade must install ON DELETE SET NULL");

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
