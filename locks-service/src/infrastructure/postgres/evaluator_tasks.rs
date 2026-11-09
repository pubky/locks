use sqlx::{FromRow, PgConnection, PgPool, Postgres, Transaction};

use crate::application::errors::ApplicationError;
use crate::application::ports::{EvaluatorMode, EvaluatorTaskKernel};

#[derive(Debug, Clone, FromRow)]
pub(super) struct EvaluatorTaskRow {
    pub(super) task_id: String,
    pub(super) creator: String,
    pub(super) bundle_id: String,
    pub(super) status: String,
    pub(super) submitted_proof_bundle: serde_json::Value,
    pub(super) submitted_at: time::OffsetDateTime,
    pub(super) evaluator_mode: String,
    pub(super) evaluator_snapshot: serde_json::Value,
    pub(super) evaluator_revision: i64,
    pub(super) completed_at: Option<time::OffsetDateTime>,
    pub(super) terminal_reason: Option<String>,
    pub(super) entitlement_to_publish: Option<serde_json::Value>,
    pub(super) entitlement_publication_claim_token: Option<uuid::Uuid>,
}

pub(super) const EVALUATOR_TASK_COLUMNS: &str = "task_id::text AS task_id, creator, bundle_id, status, \
    submitted_proof_bundle, submitted_at, evaluator_mode, evaluator_snapshot, evaluator_revision, \
    completed_at, terminal_reason, entitlement_to_publish, entitlement_publication_claim_token";

const CLAIMED_EVALUATOR_TASK_COLUMNS: &str = "task.task_id::text AS task_id, task.creator, task.bundle_id, task.status, \
    task.submitted_proof_bundle, task.submitted_at, task.evaluator_mode, task.evaluator_snapshot, \
    task.evaluator_revision, task.completed_at, task.terminal_reason, task.entitlement_to_publish, \
    task.entitlement_publication_claim_token";

fn force_receipt_fence(mode: EvaluatorMode) -> &'static str {
    match mode {
        EvaluatorMode::Graph => "",
        EvaluatorMode::Payment => {
            "AND NOT EXISTS(SELECT 1 FROM content_lock_force_deletion_receipts r \
             WHERE r.creator=verification_tasks.creator \
             AND verification_tasks.submitted_proof_bundle->>'pubky_lock_resource'= \
                 r.creator||'/pub/app.locks/'||r.lock_id||'.json')"
        }
    }
}

fn terminal_root_values(mode: EvaluatorMode) -> &'static str {
    match mode {
        EvaluatorMode::Graph => "'satisfied','expired','failed_invalid_response'",
        EvaluatorMode::Payment => "'satisfied','expired','failed'",
    }
}

pub(super) async fn claim_next(
    pool: &PgPool,
    mode: EvaluatorMode,
    worker_id: &str,
    caller_now: time::OffsetDateTime,
    requested_expiry: time::OffsetDateTime,
) -> Result<Option<(EvaluatorTaskRow, uuid::Uuid)>, ApplicationError> {
    let claim_token = uuid::Uuid::new_v4();
    let sql = format!(
        "WITH candidate AS MATERIALIZED (
            SELECT task_id FROM verification_tasks
            WHERE evaluator_mode = '{mode}' AND evaluator_snapshot IS NOT NULL
              AND deletion_job_id IS NULL
              {force_fence}
              AND (
                (evaluator_snapshot->'root'->>'status' = 'pending'
                 AND ((status = 'pending' AND (next_attempt_at IS NULL OR next_attempt_at <= clock_timestamp()))
                      OR (status = 'in_progress' AND claim_expires_at <= clock_timestamp())))
                OR (evaluator_snapshot->'root'->>'status' = 'satisfied'
                    AND status = 'publishing_entitlement'
                    AND (claim_expires_at IS NULL OR claim_expires_at <= clock_timestamp())
                    AND (next_attempt_at IS NULL OR next_attempt_at <= clock_timestamp()))
                OR (evaluator_snapshot->'root'->>'status' IN ({terminal_roots})
                    AND status = 'in_progress'
                    AND (claim_expires_at IS NULL OR claim_expires_at <= clock_timestamp()))
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
         SET status = CASE WHEN task.status = 'publishing_entitlement' THEN task.status ELSE 'in_progress' END,
             claimed_by = $1, claim_token = $2,
             claim_expires_at = clock_timestamp() + ($3 - $4),
             started_at = COALESCE(started_at, clock_timestamp()), next_attempt_at = NULL,
             attempt_count = attempt_count + 1, updated_at = clock_timestamp()
         FROM candidate
         WHERE task.task_id = candidate.task_id
         RETURNING {columns}",
        mode = mode.as_storage_value(),
        force_fence = force_receipt_fence(mode),
        terminal_roots = terminal_root_values(mode),
        columns = CLAIMED_EVALUATOR_TASK_COLUMNS,
    );
    let row = sqlx::query_as::<_, EvaluatorTaskRow>(&sql)
        .bind(worker_id)
        .bind(claim_token)
        .bind(requested_expiry)
        .bind(caller_now)
        .fetch_optional(pool)
        .await
        .map_err(storage_error)?;
    Ok(row.map(|row| (row, claim_token)))
}

pub(super) async fn release_claim(
    pool: &PgPool,
    mode: EvaluatorMode,
    task_id: &str,
    worker_id: &str,
    claim_token: &uuid::Uuid,
) -> Result<bool, ApplicationError> {
    let (started_at, fences) = match mode {
        // Preserve released graph release semantics exactly.
        EvaluatorMode::Graph => ("", ""),
        EvaluatorMode::Payment => (
            "started_at = CASE WHEN status='in_progress' AND evaluator_snapshot->'root'->>'status'='pending' THEN NULL ELSE started_at END,",
            "AND deletion_job_id IS NULL \
             AND NOT EXISTS(SELECT 1 FROM content_lock_force_deletion_receipts r \
               WHERE r.creator=verification_tasks.creator \
               AND verification_tasks.submitted_proof_bundle->>'pubky_lock_resource'=r.creator||'/pub/app.locks/'||r.lock_id||'.json') \
             AND NOT EXISTS(SELECT 1 FROM content_lock_deletion_task_snapshot s \
               WHERE s.verification_task_id=verification_tasks.task_id)",
        ),
    };
    let sql = format!(
        "UPDATE verification_tasks
         SET status = CASE
               WHEN status = 'in_progress' AND evaluator_snapshot->'root'->>'status' = 'pending'
               THEN 'pending' ELSE status END,
             {started_at}
             claimed_by = NULL, claim_token = NULL, claim_expires_at = NULL,
             updated_at = clock_timestamp()
         WHERE task_id = $1::uuid AND evaluator_mode = '{mode}'
           AND claimed_by = $2 AND claim_token = $3 {fences}",
        started_at = started_at,
        mode = mode.as_storage_value(),
        fences = fences,
    );
    let result = sqlx::query(&sql)
        .bind(task_id)
        .bind(worker_id)
        .bind(claim_token)
        .execute(pool)
        .await
        .map_err(storage_error)?;
    Ok(result.rows_affected() == 1)
}

pub(super) struct EvaluatorSnapshotTransition {
    pub(super) status: &'static str,
    pub(super) terminal_reason: Option<String>,
    pub(super) failure_message: Option<String>,
    pub(super) entitlement: Option<serde_json::Value>,
    pub(super) publication_token: Option<uuid::Uuid>,
    pub(super) retain_claim: bool,
}

fn transition_fences(mode: EvaluatorMode) -> String {
    format!(
        "AND deletion_job_id IS NULL
         {force_fence}
         AND NOT EXISTS (
           SELECT 1 FROM content_lock_deletion_task_snapshot snapshot
           WHERE snapshot.verification_task_id = verification_tasks.task_id
         )",
        force_fence = force_receipt_fence(mode),
    )
}

async fn lock_claimed_task(
    transaction: &mut Transaction<'_, Postgres>,
    mode: EvaluatorMode,
    task_id: &str,
    worker_id: &str,
    claim_token: &uuid::Uuid,
    expected_revision: i64,
) -> Result<Option<EvaluatorTaskRow>, ApplicationError> {
    let sql = format!(
        "SELECT {columns} FROM verification_tasks
         WHERE task_id = $1::uuid AND evaluator_mode = '{mode}'
           AND status = 'in_progress'
           AND claimed_by = $2 AND claim_token = $3
           AND claim_expires_at > clock_timestamp()
           AND evaluator_revision = $4
           {fences}
         FOR UPDATE",
        columns = EVALUATOR_TASK_COLUMNS,
        mode = mode.as_storage_value(),
        fences = transition_fences(mode),
    );
    sqlx::query_as::<_, EvaluatorTaskRow>(&sql)
        .bind(task_id)
        .bind(worker_id)
        .bind(claim_token)
        .bind(expected_revision)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(storage_error)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn persist_claimed_snapshot<F>(
    pool: &PgPool,
    mode: EvaluatorMode,
    task_id: &str,
    worker_id: &str,
    claim_token: &uuid::Uuid,
    expected_revision: i64,
    prepare: F,
) -> Result<Option<EvaluatorTaskRow>, ApplicationError>
where
    F: FnOnce(
        &EvaluatorTaskRow,
        time::OffsetDateTime,
    ) -> Result<(serde_json::Value, EvaluatorSnapshotTransition), ApplicationError>,
{
    let mut transaction = pool.begin().await.map_err(storage_error)?;
    let Some(current) = lock_claimed_task(
        &mut transaction,
        mode,
        task_id,
        worker_id,
        claim_token,
        expected_revision,
    )
    .await?
    else {
        transaction.rollback().await.map_err(storage_error)?;
        return Ok(None);
    };
    let database_now: time::OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage_error)?;
    let (snapshot, transition) = prepare(&current, database_now)?;
    let sql = format!(
        "UPDATE verification_tasks
         SET evaluator_snapshot = $5,
             evaluator_revision = evaluator_revision + 1,
             status = $6, terminal_reason = $7, failure_message = $8,
             completed_at = CASE WHEN $9 THEN NULL ELSE $4 END,
             entitlement_to_publish = $10,
             entitlement_publication_claim_token = $11,
             claimed_by = CASE WHEN $9 THEN claimed_by ELSE NULL END,
             claim_token = CASE WHEN $9 THEN claim_token ELSE NULL END,
             claim_expires_at = CASE WHEN $9 THEN claim_expires_at ELSE NULL END,
             next_attempt_at = NULL, updated_at = $4
         WHERE task_id = $1::uuid AND evaluator_mode = '{mode}'
           AND status = 'in_progress'
           AND claimed_by = $2 AND claim_token = $3
           AND claim_expires_at > clock_timestamp()
           AND evaluator_revision = $12
           {fences}
         RETURNING {columns}",
        mode = mode.as_storage_value(),
        fences = transition_fences(mode),
        columns = EVALUATOR_TASK_COLUMNS,
    );
    let row = sqlx::query_as::<_, EvaluatorTaskRow>(&sql)
        .bind(task_id)
        .bind(worker_id)
        .bind(claim_token)
        .bind(database_now)
        .bind(snapshot)
        .bind(transition.status)
        .bind(transition.terminal_reason)
        .bind(transition.failure_message)
        .bind(transition.retain_claim)
        .bind(transition.entitlement)
        .bind(transition.publication_token)
        .bind(expected_revision)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;
    transaction.commit().await.map_err(storage_error)?;
    Ok(row)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn schedule_claimed_retry<F>(
    pool: &PgPool,
    mode: EvaluatorMode,
    task_id: &str,
    worker_id: &str,
    claim_token: &uuid::Uuid,
    caller_now: time::OffsetDateTime,
    expected_revision: i64,
    requested_due: time::OffsetDateTime,
    prepare: F,
) -> Result<Option<EvaluatorTaskRow>, ApplicationError>
where
    F: FnOnce(
        &EvaluatorTaskRow,
        time::OffsetDateTime,
    ) -> Result<(serde_json::Value, time::OffsetDateTime), ApplicationError>,
{
    let mut transaction = pool.begin().await.map_err(storage_error)?;
    let Some(current) = lock_claimed_task(
        &mut transaction,
        mode,
        task_id,
        worker_id,
        claim_token,
        expected_revision,
    )
    .await?
    else {
        transaction.rollback().await.map_err(storage_error)?;
        return Ok(None);
    };
    let database_now: time::OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage_error)?;
    let (snapshot, deadline) = prepare(&current, database_now)?;
    let due = EvaluatorTaskKernel::retry_due(caller_now, requested_due, database_now, deadline)?;
    let sql = format!(
        "UPDATE verification_tasks
         SET evaluator_snapshot = $5,
             evaluator_revision = evaluator_revision + 1,
             status = 'pending', started_at = NULL,
             claimed_by = NULL, claim_token = NULL, claim_expires_at = NULL,
             next_attempt_at = $6, updated_at = $4
         WHERE task_id = $1::uuid AND evaluator_mode = '{mode}'
           AND status = 'in_progress'
           AND claimed_by = $2 AND claim_token = $3
           AND claim_expires_at > clock_timestamp()
           AND evaluator_revision = $7
           {fences}
         RETURNING {columns}",
        mode = mode.as_storage_value(),
        fences = transition_fences(mode),
        columns = EVALUATOR_TASK_COLUMNS,
    );
    let row = sqlx::query_as::<_, EvaluatorTaskRow>(&sql)
        .bind(task_id)
        .bind(worker_id)
        .bind(claim_token)
        .bind(database_now)
        .bind(snapshot)
        .bind(due)
        .bind(expected_revision)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;
    transaction.commit().await.map_err(storage_error)?;
    Ok(row)
}

fn publication_completion_sql(mode: EvaluatorMode) -> String {
    format!(
        "UPDATE verification_tasks
         SET status = 'completed', completed_at = clock_timestamp(),
             claimed_by = NULL, claim_token = NULL, claim_expires_at = NULL,
             next_attempt_at = NULL, entitlement_publication_claim_token = NULL,
             updated_at = clock_timestamp()
         WHERE task_id = $1::uuid AND evaluator_mode = '{mode}'
           AND status = 'publishing_entitlement'
           AND claimed_by = $2 AND claim_token = $3
           AND claim_expires_at > clock_timestamp()
           AND evaluator_revision = $4
           AND entitlement_to_publish IS NOT NULL
           AND entitlement_publication_claim_token = $5
           AND deletion_job_id IS NULL
           {force_fence}
           AND NOT EXISTS (
             SELECT 1 FROM content_lock_deletion_task_snapshot snapshot
             WHERE snapshot.verification_task_id = verification_tasks.task_id
           )
         RETURNING {columns}",
        mode = mode.as_storage_value(),
        force_fence = force_receipt_fence(mode),
        columns = EVALUATOR_TASK_COLUMNS,
    )
}

pub(super) async fn complete_publication_pool(
    pool: &PgPool,
    mode: EvaluatorMode,
    task_id: &str,
    worker_id: &str,
    claim_token: uuid::Uuid,
    expected_revision: i64,
    publication_token: uuid::Uuid,
) -> Result<Option<EvaluatorTaskRow>, ApplicationError> {
    sqlx::query_as::<_, EvaluatorTaskRow>(&publication_completion_sql(mode))
        .bind(task_id)
        .bind(worker_id)
        .bind(claim_token)
        .bind(expected_revision)
        .bind(publication_token)
        .fetch_optional(pool)
        .await
        .map_err(storage_error)
}

pub(super) async fn complete_publication_connection(
    connection: &mut PgConnection,
    mode: EvaluatorMode,
    task_id: &str,
    worker_id: &str,
    claim_token: uuid::Uuid,
    expected_revision: i64,
    publication_token: uuid::Uuid,
) -> Result<Option<EvaluatorTaskRow>, ApplicationError> {
    sqlx::query_as::<_, EvaluatorTaskRow>(&publication_completion_sql(mode))
        .bind(task_id)
        .bind(worker_id)
        .bind(claim_token)
        .bind(expected_revision)
        .bind(publication_token)
        .fetch_optional(connection)
        .await
        .map_err(storage_error)
}

fn storage_error(error: sqlx::Error) -> ApplicationError {
    ApplicationError::Storage {
        message: format!("evaluator task persistence failed: {error}"),
    }
}
