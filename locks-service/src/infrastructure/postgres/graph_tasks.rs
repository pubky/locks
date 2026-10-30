use std::str::FromStr;

use async_trait::async_trait;
use locks_core::ids::{BundleId, CreatorPubky, TaskId};
use sqlx::{FromRow, PgPool};

use crate::application::errors::ApplicationError;
use crate::application::models::{
    ClaimedGraphTask, GraphRootState, GraphTaskRecord, GraphTaskSnapshot, VerificationTaskStatus,
};
use crate::application::ports::{GraphTaskClaimer, GraphTaskRepository};

use super::proof_admission::lock_proof_admission;
use super::verification_tasks::{reject_deletion_cutoff, status_from_database};

#[derive(Debug, Clone)]
pub struct PostgresGraphTaskRepository {
    pool: PgPool,
}

impl PostgresGraphTaskRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[derive(Debug, FromRow)]
struct GraphTaskRow {
    task_id: String,
    creator: String,
    bundle_id: String,
    status: String,
    submitted_proof_bundle: serde_json::Value,
    submitted_at: time::OffsetDateTime,
    evaluator_snapshot: serde_json::Value,
    evaluator_revision: i64,
}

const GRAPH_TASK_COLUMNS: &str = "task_id::text AS task_id, creator, bundle_id, status, \
    submitted_proof_bundle, submitted_at, evaluator_snapshot, evaluator_revision";

#[async_trait]
impl GraphTaskRepository for PostgresGraphTaskRepository {
    async fn insert_graph_task(
        &self,
        task: GraphTaskRecord,
    ) -> Result<GraphTaskRecord, ApplicationError> {
        validate_new_task(&task)?;
        let snapshot = task.snapshot.as_v1();
        let snapshot_json = task.snapshot.to_json().map_err(snapshot_encode_error)?;
        let submitted = serde_json::json!({
            "version": 2,
            "bundle_id": snapshot.bundle_id,
            "pubky_lock_resource": snapshot.pubky_lock_resource,
        });
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        lock_proof_admission(
            &mut transaction,
            &snapshot.creator,
            snapshot.pubky_lock_resource.lock_id(),
        )
        .await?;

        let existing = sqlx::query_as::<_, GraphTaskRow>(&format!(
            "SELECT {GRAPH_TASK_COLUMNS} FROM verification_tasks
             WHERE creator = $1 AND bundle_id = $2
               AND evaluator_snapshot IS NOT NULL
             FOR UPDATE"
        ))
        .bind(snapshot.creator.to_string())
        .bind(snapshot.bundle_id.to_string())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if let Some(existing) = existing {
            let existing = row_to_task(existing)?;
            if existing.task_id != task.task_id
                || ensure_same_binding(&existing.snapshot, &task.snapshot).is_err()
            {
                return Err(ApplicationError::VerificationTaskConflict);
            }
            transaction.commit().await.map_err(storage_error)?;
            return self
                .get_graph_task_by_handle(&snapshot.creator, &snapshot.bundle_id)
                .await?
                .ok_or(ApplicationError::VerificationTaskConflict);
        }

        reject_deletion_cutoff(
            &mut transaction,
            &snapshot.creator,
            snapshot.pubky_lock_resource.lock_id(),
        )
        .await?;
        let inserted = sqlx::query_as::<_, GraphTaskRow>(&format!(
            "INSERT INTO verification_tasks (
                task_id, creator, bundle_id, status, submitted_proof_bundle,
                submitted_at, invoice_admission_phase, evaluator_snapshot,
                evaluator_revision
             ) VALUES ($1::uuid, $2, $3, 'pending', $4, $5, 'ready', $6, 0)
             ON CONFLICT DO NOTHING
             RETURNING {GRAPH_TASK_COLUMNS}"
        ))
        .bind(task.task_id.to_string())
        .bind(snapshot.creator.to_string())
        .bind(snapshot.bundle_id.to_string())
        .bind(submitted)
        .bind(snapshot.accepted_at)
        .bind(snapshot_json)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let Some(inserted) = inserted else {
            let winner = sqlx::query_as::<_, GraphTaskRow>(&format!(
                "SELECT {GRAPH_TASK_COLUMNS} FROM verification_tasks
                 WHERE creator = $1 AND bundle_id = $2
                   AND evaluator_snapshot IS NOT NULL
                 FOR UPDATE"
            ))
            .bind(snapshot.creator.to_string())
            .bind(snapshot.bundle_id.to_string())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?;
            if let Some(winner) = winner {
                let winner = row_to_task(winner)?;
                if winner.task_id == task.task_id
                    && ensure_same_binding(&winner.snapshot, &task.snapshot).is_ok()
                {
                    transaction.commit().await.map_err(storage_error)?;
                    return Ok(winner);
                }
                return Err(ApplicationError::VerificationTaskConflict);
            }
            let handle_exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(
                    SELECT 1 FROM verification_tasks
                    WHERE creator = $1 AND bundle_id = $2
                )",
            )
            .bind(snapshot.creator.to_string())
            .bind(snapshot.bundle_id.to_string())
            .fetch_one(&mut *transaction)
            .await
            .map_err(storage_error)?;
            if handle_exists {
                return Err(ApplicationError::VerificationTaskConflict);
            }
            return Err(ApplicationError::DuplicateRecord {
                record: "verification_task",
            });
        };
        transaction.commit().await.map_err(storage_error)?;
        row_to_task(inserted)
    }

    async fn get_graph_task_by_handle(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<Option<GraphTaskRecord>, ApplicationError> {
        let row = sqlx::query_as::<_, GraphTaskRow>(&format!(
            "SELECT {GRAPH_TASK_COLUMNS} FROM verification_tasks
             WHERE creator = $1 AND bundle_id = $2 AND evaluator_snapshot IS NOT NULL"
        ))
        .bind(creator.to_string())
        .bind(bundle_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        row.map(row_to_task).transpose()
    }
}

#[async_trait]
impl GraphTaskClaimer for PostgresGraphTaskRepository {
    async fn claim_next_graph_task(
        &self,
        worker_id: &str,
        now: time::OffsetDateTime,
        claim_expires_at: time::OffsetDateTime,
    ) -> Result<Option<ClaimedGraphTask>, ApplicationError> {
        if claim_expires_at <= now {
            return Err(ApplicationError::InvalidVerificationTaskState {
                message: "graph task claim expiry must follow claim time".to_owned(),
            });
        }
        let claim_token = uuid::Uuid::new_v4();
        let row = sqlx::query_as::<_, GraphTaskRow>(
            "WITH candidate AS MATERIALIZED (
                SELECT task_id FROM verification_tasks
                WHERE evaluator_snapshot IS NOT NULL
                  AND deletion_job_id IS NULL
                  AND evaluator_snapshot->'root'->>'status' = 'pending'
                  AND (
                    (status = 'pending' AND (next_attempt_at IS NULL OR next_attempt_at <= clock_timestamp()))
                    OR (status = 'in_progress' AND claim_expires_at <= clock_timestamp())
                  )
                  AND NOT EXISTS (
                    SELECT 1 FROM content_lock_deletion_task_snapshot snapshot
                    WHERE snapshot.verification_task_id = verification_tasks.task_id
                  )
                ORDER BY submitted_at, task_id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
             )
             UPDATE verification_tasks task
             SET status = 'in_progress', claimed_by = $1, claim_token = $2,
                 claim_expires_at = clock_timestamp() + ($3 - $4),
                 started_at = COALESCE(started_at, $4), next_attempt_at = NULL,
                 attempt_count = attempt_count + 1, updated_at = $4
             FROM candidate
             WHERE task.task_id = candidate.task_id
             RETURNING task.task_id::text AS task_id, task.creator, task.bundle_id,
                       task.status, task.submitted_proof_bundle, task.submitted_at,
                       task.evaluator_snapshot, task.evaluator_revision",
        )
        .bind(worker_id)
        .bind(claim_token)
        .bind(claim_expires_at)
        .bind(now)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        row.map(row_to_task)
            .transpose()
            .map(|task| task.map(|task| ClaimedGraphTask { task, claim_token }))
    }

    async fn persist_claimed_graph_snapshot(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        _now: time::OffsetDateTime,
        expected_revision: u64,
        snapshot: GraphTaskSnapshot,
    ) -> Result<Option<GraphTaskRecord>, ApplicationError> {
        let expected_revision = match i64::try_from(expected_revision) {
            Ok(revision) => revision,
            Err(_) => return Ok(None),
        };
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let current = sqlx::query_as::<_, GraphTaskRow>(&format!(
            "SELECT {GRAPH_TASK_COLUMNS} FROM verification_tasks
             WHERE task_id = $1::uuid
               AND status = 'in_progress'
               AND claimed_by = $2 AND claim_token = $3
               AND claim_expires_at > clock_timestamp()
               AND evaluator_revision = $4
               AND deletion_job_id IS NULL
               AND NOT EXISTS (
                 SELECT 1 FROM content_lock_deletion_task_snapshot snapshot
                 WHERE snapshot.verification_task_id = verification_tasks.task_id
               )
             FOR UPDATE"
        ))
        .bind(task_id.to_string())
        .bind(worker_id)
        .bind(claim_token)
        .bind(expected_revision)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let Some(current) = current else {
            transaction.rollback().await.map_err(storage_error)?;
            return Ok(None);
        };
        let current = row_to_task(current)?;
        let database_now: time::OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await
            .map_err(storage_error)?;
        snapshot.as_v1().validate().map_err(invalid_snapshot)?;
        snapshot
            .as_v1()
            .validate_observations_not_after(database_now)
            .map_err(invalid_snapshot)?;
        ensure_same_binding(&current.snapshot, &snapshot)?;
        snapshot
            .as_v1()
            .validate_progress_from(current.snapshot.as_v1())
            .map_err(invalid_snapshot)?;
        if matches!(snapshot.as_v1().root, GraphRootState::Expired(_))
            && database_now < snapshot.as_v1().deadline_at
        {
            return Err(ApplicationError::InvalidVerificationTaskState {
                message: "graph task cannot expire before its deadline".to_owned(),
            });
        }
        expected_revision
            .checked_add(1)
            .ok_or_else(|| ApplicationError::Storage {
                message: "graph evaluator revision overflow".to_owned(),
            })?;
        let snapshot_json = snapshot.to_json().map_err(snapshot_encode_error)?;

        let row = sqlx::query_as::<_, GraphTaskRow>(&format!(
            "UPDATE verification_tasks
             SET evaluator_snapshot = $5,
                 evaluator_revision = evaluator_revision + 1,
                 claimed_by = CASE
                     WHEN $5->'root'->>'status' = 'pending' THEN claimed_by ELSE NULL END,
                 claim_token = CASE
                     WHEN $5->'root'->>'status' = 'pending' THEN claim_token ELSE NULL END,
                 claim_expires_at = CASE
                     WHEN $5->'root'->>'status' = 'pending' THEN claim_expires_at ELSE NULL END,
                 updated_at = $4
             WHERE task_id = $1::uuid
               AND status = 'in_progress'
               AND claimed_by = $2 AND claim_token = $3
               AND claim_expires_at > clock_timestamp()
               AND evaluator_revision = $6
               AND deletion_job_id IS NULL
               AND NOT EXISTS (
                 SELECT 1 FROM content_lock_deletion_task_snapshot snapshot
                 WHERE snapshot.verification_task_id = verification_tasks.task_id
               )
             RETURNING {GRAPH_TASK_COLUMNS}"
        ))
        .bind(task_id.to_string())
        .bind(worker_id)
        .bind(claim_token)
        .bind(database_now)
        .bind(snapshot_json)
        .bind(expected_revision)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;
        row.map(row_to_task).transpose()
    }
}

fn row_to_task(row: GraphTaskRow) -> Result<GraphTaskRecord, ApplicationError> {
    let task_id = TaskId::from_str(&row.task_id).map_err(|error| ApplicationError::Storage {
        message: format!("invalid graph task id stored in Postgres: {error}"),
    })?;
    let snapshot = GraphTaskSnapshot::from_json(row.evaluator_snapshot).map_err(|error| {
        ApplicationError::Storage {
            message: format!("invalid graph task snapshot stored in Postgres: {error}"),
        }
    })?;
    let identity = snapshot.as_v1();
    let submitted_bundle = row
        .submitted_proof_bundle
        .get("bundle_id")
        .and_then(serde_json::Value::as_str);
    let submitted_resource = row
        .submitted_proof_bundle
        .get("pubky_lock_resource")
        .and_then(serde_json::Value::as_str);
    if row.creator != identity.creator.to_string()
        || row.bundle_id != identity.bundle_id.to_string()
        || row.submitted_at != identity.accepted_at
        || submitted_bundle != Some(identity.bundle_id.to_string().as_str())
        || submitted_resource != Some(identity.pubky_lock_resource.to_string().as_str())
    {
        return Err(ApplicationError::Storage {
            message: "graph task snapshot diverges from relational identity or timing".to_owned(),
        });
    }
    let evaluator_revision =
        u64::try_from(row.evaluator_revision).map_err(|_| ApplicationError::Storage {
            message: "negative graph evaluator revision stored in Postgres".to_owned(),
        })?;
    Ok(GraphTaskRecord {
        task_id,
        snapshot,
        evaluator_revision,
        status: status_from_database(&row.status)?,
    })
}

fn validate_new_task(task: &GraphTaskRecord) -> Result<(), ApplicationError> {
    if task.status != VerificationTaskStatus::Pending || task.evaluator_revision != 0 {
        return Err(ApplicationError::InvalidVerificationTaskState {
            message: "new graph task must be pending at evaluator revision zero".to_owned(),
        });
    }
    task.snapshot
        .as_v1()
        .validate_pristine_admission()
        .map_err(invalid_snapshot)
}

fn ensure_same_binding(
    current: &GraphTaskSnapshot,
    next: &GraphTaskSnapshot,
) -> Result<(), ApplicationError> {
    let current = current.as_v1();
    let next = next.as_v1();
    if current.creator != next.creator
        || current.bundle_id != next.bundle_id
        || current.pubky_lock_resource != next.pubky_lock_resource
        || current.lock_hash != next.lock_hash
        || current.reader != next.reader
        || current.accepted_at != next.accepted_at
        || current.deadline_at != next.deadline_at
        || current.operator != next.operator
        || current
            .predicates
            .iter()
            .map(|state| &state.predicate)
            .ne(next.predicates.iter().map(|state| &state.predicate))
    {
        return Err(ApplicationError::InvalidVerificationTaskState {
            message: "graph snapshot replacement changed immutable task binding".to_owned(),
        });
    }
    Ok(())
}

fn invalid_snapshot(message: String) -> ApplicationError {
    ApplicationError::Storage {
        message: format!("invalid graph task snapshot: {message}"),
    }
}

fn snapshot_encode_error(error: serde_json::Error) -> ApplicationError {
    ApplicationError::Storage {
        message: format!("serialize graph task snapshot: {error}"),
    }
}

fn storage_error(error: sqlx::Error) -> ApplicationError {
    ApplicationError::Storage {
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use locks_core::ids::{BundleId, CreatorPubky, LockHash, LockId, PubkyLockResource, TaskId};
    use locks_core::lock_policy::{GraphParticipantV2, GraphPolicyV2, GraphPredicateV2};
    use time::macros::datetime;

    use super::PostgresGraphTaskRepository;
    use crate::application::graph0conf_reducer::reduce_graph_snapshot;
    use crate::application::models::{
        GraphObservationOutcome, GraphTaskRecord, GraphTaskSnapshot, GraphTaskSnapshotV1,
        VerificationTaskStatus,
    };
    use crate::application::ports::{
        GraphTaskClaimer, GraphTaskRepository, VerificationTaskClaimer, VerificationTaskRepository,
    };
    use crate::infrastructure::postgres::testing::TestDatabase;
    use crate::infrastructure::postgres::{
        PostgresVerificationTaskClaimer, PostgresVerificationTaskRepository,
    };

    const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
    const READER: &str = "pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky";
    const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";
    const TASK_ID: &str = "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10";
    const NOW: time::OffsetDateTime = datetime!(2026-10-01 12:00:00 UTC);

    #[tokio::test]
    async fn graph_snapshot_persistence_is_fenced_and_has_no_invoice_intent() {
        let database = TestDatabase::create().await;
        let repository = PostgresGraphTaskRepository::new(database.pool().clone());
        let inserted = repository.insert_graph_task(task()).await.unwrap();
        let invoice_intent: Option<serde_json::Value> = sqlx::query_scalar(
            "SELECT invoice_admission_intent FROM verification_tasks WHERE task_id = $1::uuid",
        )
        .bind(inserted.task_id.to_string())
        .fetch_one(database.pool())
        .await
        .unwrap();
        assert_eq!(invoice_intent, None);
        assert!(
            sqlx::query(
                "UPDATE verification_tasks
                 SET invoice_admission_intent = '{}'::jsonb
                 WHERE task_id = $1::uuid",
            )
            .bind(inserted.task_id.to_string())
            .execute(database.pool())
            .await
            .is_err()
        );

        let ordinary = PostgresVerificationTaskClaimer::new(database.pool().clone());
        assert_eq!(
            ordinary
                .claim_next_verification_task("v1-worker", NOW, NOW + time::Duration::minutes(5),)
                .await
                .unwrap(),
            None
        );
        let ordinary_repository = PostgresVerificationTaskRepository::new(database.pool().clone());
        assert_eq!(
            ordinary_repository
                .get_verification_task_by_handle(&creator(), &bundle_id())
                .await
                .unwrap(),
            None
        );
        ordinary_repository
            .delete_verification_task(&inserted.task_id)
            .await
            .unwrap();
        assert!(
            repository
                .get_graph_task_by_handle(&creator(), &bundle_id())
                .await
                .unwrap()
                .is_some()
        );

        let claim = repository
            .claim_next_graph_task("graph-worker", NOW, NOW + time::Duration::minutes(5))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            repository.insert_graph_task(task()).await.unwrap(),
            claim.task
        );
        let mut next = claim.task.snapshot.clone();
        let GraphTaskSnapshot::V1(snapshot) = &mut next;
        snapshot
            .record_observation(
                0,
                GraphObservationOutcome::False,
                NOW + time::Duration::seconds(1),
            )
            .unwrap();
        let persisted = repository
            .persist_claimed_graph_snapshot(
                &claim.task.task_id,
                "graph-worker",
                &claim.claim_token,
                NOW + time::Duration::seconds(2),
                0,
                next.clone(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(persisted.evaluator_revision, 1);
        assert_eq!(persisted.snapshot, next);
        assert_eq!(
            repository.insert_graph_task(task()).await.unwrap(),
            persisted
        );
        assert_eq!(
            repository
                .persist_claimed_graph_snapshot(
                    &claim.task.task_id,
                    "graph-worker",
                    &claim.claim_token,
                    NOW + time::Duration::seconds(3),
                    0,
                    next,
                )
                .await
                .unwrap(),
            None
        );

        let mut terminal = persisted.snapshot;
        let GraphTaskSnapshot::V1(snapshot) = &mut terminal;
        snapshot
            .record_observation(
                0,
                GraphObservationOutcome::True,
                NOW + time::Duration::seconds(4),
            )
            .unwrap();
        reduce_graph_snapshot(snapshot, NOW + time::Duration::seconds(4)).unwrap();
        repository
            .persist_claimed_graph_snapshot(
                &claim.task.task_id,
                "graph-worker",
                &claim.claim_token,
                NOW + time::Duration::seconds(5),
                1,
                terminal,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            repository
                .claim_next_graph_task(
                    "other-worker",
                    NOW + time::Duration::minutes(5),
                    NOW + time::Duration::minutes(10),
                )
                .await
                .unwrap(),
            None
        );
        database.cleanup().await;
    }

    #[tokio::test]
    async fn corrupt_snapshot_fails_closed_on_read() {
        let database = TestDatabase::create().await;
        let repository = PostgresGraphTaskRepository::new(database.pool().clone());
        let task = repository.insert_graph_task(task()).await.unwrap();
        sqlx::query(
            "UPDATE verification_tasks
             SET evaluator_snapshot = jsonb_set(evaluator_snapshot, '{schema_version}', '99')
             WHERE task_id = $1::uuid",
        )
        .bind(task.task_id.to_string())
        .execute(database.pool())
        .await
        .unwrap();
        assert!(
            repository
                .get_graph_task_by_handle(&creator(), &bundle_id())
                .await
                .is_err()
        );
        database.cleanup().await;
    }

    #[tokio::test]
    async fn stale_postgres_claim_fences_precede_candidate_validation() {
        let database = TestDatabase::create().await;
        let repository = PostgresGraphTaskRepository::new(database.pool().clone());
        repository.insert_graph_task(task()).await.unwrap();
        let claim = repository
            .claim_next_graph_task("worker", NOW, NOW + time::Duration::minutes(5))
            .await
            .unwrap()
            .unwrap();
        let mut invalid = claim.task.snapshot.clone();
        let GraphTaskSnapshot::V1(snapshot) = &mut invalid;
        snapshot.predicates[0].first_true_observation =
            Some(crate::application::models::GraphPredicateObservation {
                outcome: GraphObservationOutcome::True,
                observed_at: NOW,
            });
        assert_eq!(
            repository
                .persist_claimed_graph_snapshot(
                    &claim.task.task_id,
                    "worker",
                    &uuid::Uuid::new_v4(),
                    NOW,
                    0,
                    invalid.clone(),
                )
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            repository
                .persist_claimed_graph_snapshot(
                    &claim.task.task_id,
                    "worker",
                    &claim.claim_token,
                    NOW,
                    1,
                    invalid.clone(),
                )
                .await
                .unwrap(),
            None
        );
        sqlx::query(
            "UPDATE verification_tasks SET claim_expires_at = clock_timestamp()
             WHERE task_id = $1::uuid",
        )
        .bind(claim.task.task_id.to_string())
        .execute(database.pool())
        .await
        .unwrap();
        assert_eq!(
            repository
                .persist_claimed_graph_snapshot(
                    &claim.task.task_id,
                    "worker",
                    &claim.claim_token,
                    NOW,
                    0,
                    invalid,
                )
                .await
                .unwrap(),
            None
        );
        database.cleanup().await;
    }

    #[tokio::test]
    async fn postgres_rejects_true_evidence_later_than_authoritative_database_time() {
        let database = TestDatabase::create().await;
        let repository = PostgresGraphTaskRepository::new(database.pool().clone());
        let database_now: time::OffsetDateTime =
            sqlx::query_scalar("SELECT date_trunc('microseconds', clock_timestamp())")
                .fetch_one(database.pool())
                .await
                .unwrap();
        let mut fresh = task();
        let GraphTaskSnapshot::V1(snapshot) = &mut fresh.snapshot;
        snapshot.accepted_at = database_now;
        snapshot.deadline_at = database_now + time::Duration::seconds(60);
        repository.insert_graph_task(fresh).await.unwrap();
        let claim = repository
            .claim_next_graph_task(
                "worker",
                database_now,
                database_now + time::Duration::minutes(5),
            )
            .await
            .unwrap()
            .unwrap();
        let mut future = claim.task.snapshot.clone();
        let GraphTaskSnapshot::V1(snapshot) = &mut future;
        let future_time = database_now + time::Duration::seconds(30);
        snapshot
            .record_observation(0, GraphObservationOutcome::True, future_time)
            .unwrap();
        reduce_graph_snapshot(snapshot, future_time).unwrap();
        assert!(
            repository
                .persist_claimed_graph_snapshot(
                    &claim.task.task_id,
                    "worker",
                    &claim.claim_token,
                    future_time,
                    0,
                    future,
                )
                .await
                .is_err()
        );
        database.cleanup().await;
    }

    #[tokio::test]
    async fn valid_snapshot_json_must_match_relational_identity_and_timing() {
        let database = TestDatabase::create().await;
        let repository = PostgresGraphTaskRepository::new(database.pool().clone());
        let inserted = repository.insert_graph_task(task()).await.unwrap();
        let replacement_resource = format!(
            "{READER}/pub/app.locks/{}.json",
            LockId::from_hash(LockHash::from_bytes([0; 32]))
        );
        sqlx::query(
            "UPDATE verification_tasks
             SET evaluator_snapshot = jsonb_set(
                    jsonb_set(evaluator_snapshot, '{creator}', to_jsonb($2::text)),
                    '{pubky_lock_resource}', to_jsonb($3::text)
                 )
             WHERE task_id = $1::uuid",
        )
        .bind(inserted.task_id.to_string())
        .bind(READER)
        .bind(replacement_resource)
        .execute(database.pool())
        .await
        .unwrap();
        assert!(
            repository
                .get_graph_task_by_handle(&creator(), &bundle_id())
                .await
                .is_err()
        );

        let original = task().snapshot.to_json().unwrap();
        sqlx::query(
            "UPDATE verification_tasks
             SET evaluator_snapshot = jsonb_set(
                    jsonb_set($2, '{accepted_at}', to_jsonb($3::text)),
                    '{deadline_at}', to_jsonb($4::text)
                 )
             WHERE task_id = $1::uuid",
        )
        .bind(inserted.task_id.to_string())
        .bind(original)
        .bind((NOW + time::Duration::seconds(1)).to_string())
        .bind((NOW + time::Duration::seconds(61)).to_string())
        .execute(database.pool())
        .await
        .unwrap();
        assert!(
            repository
                .get_graph_task_by_handle(&creator(), &bundle_id())
                .await
                .is_err()
        );
        database.cleanup().await;
    }

    #[tokio::test]
    async fn concurrent_changed_binding_reports_conflict_for_loser() {
        let database = TestDatabase::create().await;
        let repository = PostgresGraphTaskRepository::new(database.pool().clone());
        let first = task();
        let mut changed = task();
        changed.task_id = TaskId::from_str("018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d11").unwrap();
        let GraphTaskSnapshot::V1(snapshot) = &mut changed.snapshot;
        snapshot.lock_hash = [1; 32];
        let changed_lock_id = LockId::from_hash(LockHash::from_bytes([1; 32]));
        snapshot.pubky_lock_resource =
            PubkyLockResource::from_str(&format!("{CREATOR}/pub/app.locks/{changed_lock_id}.json"))
                .unwrap();

        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        let left = {
            let repository = repository.clone();
            let barrier = std::sync::Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                repository.insert_graph_task(first).await
            })
        };
        let right = {
            let repository = repository.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                repository.insert_graph_task(changed).await
            })
        };
        let (left, right) = tokio::join!(left, right);
        let results = [left.unwrap(), right.unwrap()];
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert!(results.iter().any(|result| matches!(
            result,
            Err(crate::application::errors::ApplicationError::VerificationTaskConflict)
        )));
        database.cleanup().await;
    }

    fn task() -> GraphTaskRecord {
        GraphTaskRecord {
            task_id: TaskId::from_str(TASK_ID).unwrap(),
            snapshot: GraphTaskSnapshot::V1(
                GraphTaskSnapshotV1::new(
                    creator(),
                    bundle_id(),
                    resource(),
                    [0; 32],
                    CreatorPubky::from_str(READER).unwrap(),
                    NOW,
                    60,
                    GraphPolicyV2 {
                        predicates: vec![GraphPredicateV2::Follows {
                            follower: GraphParticipantV2::Reader,
                            followed: GraphParticipantV2::Creator,
                        }],
                        operator: None,
                    },
                )
                .unwrap(),
            ),
            evaluator_revision: 0,
            status: VerificationTaskStatus::Pending,
        }
    }

    fn creator() -> CreatorPubky {
        CreatorPubky::from_str(CREATOR).unwrap()
    }

    fn bundle_id() -> BundleId {
        BundleId::from_str(BUNDLE_ID).unwrap()
    }

    fn resource() -> PubkyLockResource {
        let lock_id = LockId::from_hash(LockHash::from_bytes([0; 32]));
        PubkyLockResource::from_str(&format!("{CREATOR}/pub/app.locks/{lock_id}.json")).unwrap()
    }
}
