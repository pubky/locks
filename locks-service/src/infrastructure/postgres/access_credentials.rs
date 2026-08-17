use std::str::FromStr;

use async_trait::async_trait;
use sqlx::{PgPool, Postgres, Row, Transaction};
use time::OffsetDateTime;
use uuid::Uuid;

use locks_core::{
    ids::{BundleId, CreatorPubky, LockId},
    lock_policy::{ContentLock, GuardedResource},
};

use crate::application::errors::ApplicationError;
use crate::application::models::{
    AccessCredential, AccessCredentialLookupKey, AccessCredentialRecord, DeletionReadAuthorization,
    EncryptedFinalCredential, FinalCredentialContext, IssuedDeletionCredential,
};
use crate::application::ports::AccessCredentialStore;
use crate::infrastructure::final_credentials::FinalCredentialCipher;

use super::proof_admission::lock_proof_admission;

/// Postgres-backed store for issued access credential lookup records.
#[derive(Debug, Clone)]
pub struct PostgresAccessCredentialStore {
    pool: PgPool,
    final_credential_cipher: Option<FinalCredentialCipher>,
}

impl PostgresAccessCredentialStore {
    /// Creates a store backed by the provided migrated Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            final_credential_cipher: None,
        }
    }

    pub fn with_final_credential_cipher(
        pool: PgPool,
        final_credential_cipher: FinalCredentialCipher,
    ) -> Self {
        Self {
            pool,
            final_credential_cipher: Some(final_credential_cipher),
        }
    }
}

#[async_trait]
impl AccessCredentialStore for PostgresAccessCredentialStore {
    async fn insert_access_credential(
        &self,
        lock_id: &LockId,
        lookup_key: AccessCredentialLookupKey,
        record: AccessCredentialRecord,
    ) -> Result<(), ApplicationError> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        lock_proof_admission(&mut transaction, &record.creator, lock_id).await?;
        let deletion_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1
                FROM content_lock_deletion_jobs
                WHERE creator = $1 AND lock_id = $2
            )",
        )
        .bind(record.creator.to_string())
        .bind(lock_id.to_string())
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if deletion_exists {
            return Err(ApplicationError::ContentLockDeletionInProgress);
        }

        let result = sqlx::query(
            "INSERT INTO access_credentials (lookup_key, creator, bundle_id, expires_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (lookup_key) DO NOTHING",
        )
        .bind(lookup_key.as_bytes().as_slice())
        .bind(record.creator.to_string())
        .bind(record.bundle_id.to_string())
        .bind(record.expires_at)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;

        if result.rows_affected() == 0 {
            return Err(ApplicationError::DuplicateRecord {
                record: "access_credential",
            });
        }

        transaction.commit().await.map_err(storage_error)?;
        Ok(())
    }

    async fn get_access_credential(
        &self,
        lookup_key: &AccessCredentialLookupKey,
    ) -> Result<Option<AccessCredentialRecord>, ApplicationError> {
        let row = sqlx::query(
            "SELECT creator, bundle_id, expires_at
            FROM access_credentials
            WHERE lookup_key = $1",
        )
        .bind(lookup_key.as_bytes().as_slice())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;

        row.map(row_to_record).transpose()
    }

    async fn delete_access_credential(
        &self,
        lookup_key: &AccessCredentialLookupKey,
    ) -> Result<(), ApplicationError> {
        sqlx::query("DELETE FROM access_credentials WHERE lookup_key = $1")
            .bind(lookup_key.as_bytes().as_slice())
            .execute(&self.pool)
            .await
            .map_err(storage_error)?;
        Ok(())
    }

    async fn initialize_final_access_windows(
        &self,
        deletion_job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        now: OffsetDateTime,
        issuance_deadline: OffsetDateTime,
        read_deadline: OffsetDateTime,
    ) -> Result<bool, ApplicationError> {
        if issuance_deadline <= now || read_deadline <= issuance_deadline {
            return Err(ApplicationError::Storage {
                message: "invalid final access window ordering".to_owned(),
            });
        }
        let updated = sqlx::query(
            "UPDATE content_lock_deletion_jobs
             SET final_issuance_started_at = COALESCE(final_issuance_started_at, $4),
                 final_credential_issuance_deadline =
                     COALESCE(final_credential_issuance_deadline, $5),
                 final_read_deadline = COALESCE(final_read_deadline, $6)
             WHERE job_id = $1 AND claimed_by = $2 AND claim_token = $3
               AND claim_expires_at > $4 AND state = 'running'
               AND force_requested_at IS NULL AND phase = 'issue_final_credentials'",
        )
        .bind(deletion_job_id)
        .bind(worker_id)
        .bind(claim_token)
        .bind(now)
        .bind(issuance_deadline)
        .bind(read_deadline)
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;
        Ok(updated.rows_affected() == 1)
    }

    async fn issue_or_replay_final_credential(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
        now: OffsetDateTime,
        candidate: AccessCredential,
    ) -> Result<Option<IssuedDeletionCredential>, ApplicationError> {
        let cipher = match &self.final_credential_cipher {
            Some(cipher) => cipher,
            None => return Ok(None),
        };
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let job = sqlx::query(
            "SELECT job.job_id, job.phase, job.final_credential_issuance_deadline,
                    job.final_read_deadline, job.frozen_content_lock
             FROM content_lock_deletion_jobs AS job
             WHERE job.creator = $1
               AND job.state IN ('queued', 'running')
               AND job.force_requested_at IS NULL
               AND job.phase IN ('issue_final_credentials', 'drain_final_reads')
               AND job.final_read_deadline > $3
               AND EXISTS (
                   SELECT 1
                   FROM content_lock_deletion_task_snapshot AS snapshot
                   WHERE snapshot.deletion_job_id = job.job_id
                     AND snapshot.bundle_id = $2
                     AND snapshot.resolved_status = 'completed'
                     AND snapshot.final_credential_eligible_at IS NOT NULL
               )
             FOR UPDATE OF job",
        )
        .bind(creator.to_string())
        .bind(bundle_id.to_string())
        .bind(now)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let Some(job) = job else {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(None);
        };
        let deletion_job_id: Uuid = job.try_get("job_id").map_err(storage_error)?;
        let phase: String = job.try_get("phase").map_err(storage_error)?;
        let issuance_deadline: OffsetDateTime = job
            .try_get("final_credential_issuance_deadline")
            .map_err(storage_error)?;
        let expires_at: OffsetDateTime =
            job.try_get("final_read_deadline").map_err(storage_error)?;
        let snapshot_exists = sqlx::query_scalar::<_, bool>(
            "SELECT TRUE
             FROM content_lock_deletion_task_snapshot
             WHERE deletion_job_id = $1 AND bundle_id = $2
               AND resolved_status = 'completed'
               AND final_credential_eligible_at IS NOT NULL
             FOR UPDATE",
        )
        .bind(deletion_job_id)
        .bind(bundle_id.to_string())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?
        .unwrap_or(false);
        if !snapshot_exists {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(None);
        }
        let existing_encrypted: Option<String> = sqlx::query_scalar(
            "SELECT encrypted_bearer
             FROM content_lock_access_drain_credentials
             WHERE deletion_job_id = $1 AND creator = $2 AND bundle_id = $3
               AND credential_kind = 'final'
             FOR UPDATE",
        )
        .bind(deletion_job_id)
        .bind(creator.to_string())
        .bind(bundle_id.to_string())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?
        .flatten();
        let context = FinalCredentialContext {
            deletion_job_id,
            creator: creator.clone(),
            bundle_id: bundle_id.clone(),
        };
        if let Some(encrypted) = existing_encrypted {
            let credential = cipher.decrypt(&context, &EncryptedFinalCredential::new(encrypted))?;
            transaction.commit().await.map_err(storage_error)?;
            return Ok(Some(IssuedDeletionCredential {
                credential,
                expires_at,
            }));
        }
        if phase != "issue_final_credentials" || now >= issuance_deadline {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(None);
        }
        let frozen: serde_json::Value =
            job.try_get("frozen_content_lock").map_err(storage_error)?;
        let frozen: ContentLock =
            serde_json::from_value(frozen).map_err(|error| ApplicationError::Storage {
                message: format!("invalid frozen content lock stored in Postgres: {error}"),
            })?;
        let encrypted = cipher.encrypt(&context, &candidate)?;
        let lookup_key = AccessCredentialLookupKey::derive(&candidate);
        let credential_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO access_credentials (
                lookup_key, creator, bundle_id, expires_at, deletion_job_id
             ) VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(lookup_key.as_bytes().as_slice())
        .bind(creator.to_string())
        .bind(bundle_id.to_string())
        .bind(expires_at)
        .bind(deletion_job_id)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        sqlx::query(
            "INSERT INTO content_lock_access_drain_credentials (
                credential_id, deletion_job_id, lookup_key, creator, bundle_id,
                credential_kind, issued_at, expires_at, encrypted_bearer
             ) VALUES ($1, $2, $3, $4, $5, 'final', $6, $7, $8)",
        )
        .bind(credential_id)
        .bind(deletion_job_id)
        .bind(lookup_key.as_bytes().as_slice())
        .bind(creator.to_string())
        .bind(bundle_id.to_string())
        .bind(now)
        .bind(expires_at)
        .bind(encrypted.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let mut resources: Vec<GuardedResource> = frozen.primary_resource.into_iter().collect();
        resources.extend(
            frozen
                .secondary_resources
                .into_iter()
                .map(|(path, resource)| {
                    GuardedResource::new(path, resource.hash, resource.content_type, resource.size)
                        .expect("persisted frozen manifest was validated at deletion admission")
                }),
        );
        for resource in resources {
            sqlx::query(
                "INSERT INTO content_lock_access_drain_reads (credential_id, guarded_path)
                 VALUES ($1, $2)",
            )
            .bind(credential_id)
            .bind(resource.path)
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        }
        sqlx::query(
            "UPDATE content_lock_deletion_task_snapshot
             SET final_credential_issued_at = $3
             WHERE deletion_job_id = $1 AND bundle_id = $2
               AND final_credential_issued_at IS NULL",
        )
        .bind(deletion_job_id)
        .bind(bundle_id.to_string())
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(Some(IssuedDeletionCredential {
            credential: candidate,
            expires_at,
        }))
    }

    async fn final_credential_available(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
        now: OffsetDateTime,
    ) -> Result<bool, ApplicationError> {
        if self.final_credential_cipher.is_none() {
            return Ok(false);
        }
        sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1
                 FROM content_lock_deletion_jobs AS job
                 JOIN content_lock_deletion_task_snapshot AS snapshot
                   ON snapshot.deletion_job_id = job.job_id
                 WHERE job.creator = $1 AND snapshot.bundle_id = $2
                   AND job.state IN ('queued', 'running')
                   AND job.force_requested_at IS NULL
                   AND job.phase IN ('issue_final_credentials', 'drain_final_reads')
                   AND snapshot.resolved_status = 'completed'
                   AND snapshot.final_credential_eligible_at IS NOT NULL
                   AND job.final_read_deadline > $3
                   AND (
                       EXISTS (
                           SELECT 1
                           FROM content_lock_access_drain_credentials AS credential
                           WHERE credential.deletion_job_id = job.job_id
                             AND credential.creator = $1
                             AND credential.bundle_id = $2
                             AND credential.credential_kind = 'final'
                       )
                       OR (
                           job.phase = 'issue_final_credentials'
                           AND job.final_credential_issuance_deadline > $3
                       )
                   )
             )",
        )
        .bind(creator.to_string())
        .bind(bundle_id.to_string())
        .bind(now)
        .fetch_one(&self.pool)
        .await
        .map_err(storage_error)
    }

    async fn prepare_deletion_read(
        &self,
        lookup_key: &AccessCredentialLookupKey,
        path: &str,
        now: OffsetDateTime,
        _claim_expires_at: OffsetDateTime,
    ) -> Result<Option<DeletionReadAuthorization>, ApplicationError> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let Some(deletion_job_id) = lookup_deletion_job_id(&mut transaction, lookup_key).await?
        else {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(None);
        };
        let Some(job) = lock_active_drain_job(&mut transaction, deletion_job_id).await? else {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(None);
        };
        let credential = sqlx::query(
            "SELECT credential_id, credential_kind, creator, expires_at
             FROM content_lock_access_drain_credentials
             WHERE lookup_key = $1 AND deletion_job_id = $2
             FOR UPDATE",
        )
        .bind(lookup_key.as_bytes().as_slice())
        .bind(deletion_job_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let Some(credential) = credential else {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(None);
        };
        let kind: String = credential
            .try_get("credential_kind")
            .map_err(storage_error)?;
        let credential_expiry: OffsetDateTime =
            credential.try_get("expires_at").map_err(storage_error)?;
        if credential_expiry <= now || !phase_allows_credential_access(&job.phase, &kind) {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(None);
        }
        let frozen: ContentLock =
            serde_json::from_value(job.frozen_content_lock).map_err(|error| {
                ApplicationError::Storage {
                    message: format!("invalid frozen content lock stored in Postgres: {error}"),
                }
            })?;
        let Some(resource) = frozen.resource_for_path(path) else {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(None);
        };
        let creator = CreatorPubky::from_str(
            &credential
                .try_get::<String, _>("creator")
                .map_err(storage_error)?,
        )
        .map_err(|error| ApplicationError::Storage {
            message: format!("invalid drain credential creator stored in Postgres: {error}"),
        })?;
        if kind == "ordinary" {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(Some(DeletionReadAuthorization {
                claim_token: None,
                creator,
                resource,
            }));
        }
        let credential_id: Uuid = credential.try_get("credential_id").map_err(storage_error)?;
        let read = sqlx::query(
            "SELECT claim_token, claim_expires_at, consumed_at
             FROM content_lock_access_drain_reads
             WHERE credential_id = $1 AND guarded_path = $2
             FOR UPDATE",
        )
        .bind(credential_id)
        .bind(path)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let Some(read) = read else {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(None);
        };
        if read
            .try_get::<Option<OffsetDateTime>, _>("consumed_at")
            .map_err(storage_error)?
            .is_some()
        {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(None);
        }
        let existing_claim: Option<Uuid> = read.try_get("claim_token").map_err(storage_error)?;
        let existing_expiry: Option<OffsetDateTime> =
            read.try_get("claim_expires_at").map_err(storage_error)?;
        if existing_claim.is_some() && existing_expiry.is_some_and(|expiry| expiry > now) {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(None);
        }
        let Some(read_deadline) = job.final_read_deadline else {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(None);
        };
        let bounded_expiry = (now + time::Duration::seconds(30))
            .min(credential_expiry)
            .min(read_deadline);
        if read_deadline <= now || bounded_expiry <= now {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(None);
        }
        let claim_token = Uuid::new_v4();
        let updated = sqlx::query(
            "UPDATE content_lock_access_drain_reads
             SET claim_token = $3, claim_expires_at = $4
             WHERE credential_id = $1 AND guarded_path = $2
               AND consumed_at IS NULL
               AND (claim_token IS NULL OR claim_expires_at <= $5)",
        )
        .bind(credential_id)
        .bind(path)
        .bind(claim_token)
        .bind(bounded_expiry)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if updated.rows_affected() != 1 {
            transaction.rollback().await.map_err(storage_error)?;
            return Ok(None);
        }
        transaction.commit().await.map_err(storage_error)?;
        Ok(Some(DeletionReadAuthorization {
            claim_token: Some(claim_token),
            creator,
            resource,
        }))
    }

    async fn deletion_credential_enrolled(
        &self,
        lookup_key: &AccessCredentialLookupKey,
    ) -> Result<bool, ApplicationError> {
        sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1
                 FROM content_lock_access_drain_credentials
                 WHERE lookup_key = $1
             )",
        )
        .bind(lookup_key.as_bytes().as_slice())
        .fetch_one(&self.pool)
        .await
        .map_err(storage_error)
    }

    async fn release_deletion_read(
        &self,
        lookup_key: &AccessCredentialLookupKey,
        path: &str,
        claim_token: Uuid,
        _now: OffsetDateTime,
    ) -> Result<bool, ApplicationError> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let Some(deletion_job_id) = lookup_deletion_job_id(&mut transaction, lookup_key).await?
        else {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(false);
        };
        let Some(job) = lock_active_drain_job(&mut transaction, deletion_job_id).await? else {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(false);
        };
        if !phase_allows_credential_access(&job.phase, "final") {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(false);
        }
        let updated = sqlx::query(
            "UPDATE content_lock_access_drain_reads AS read
             SET claim_token = NULL, claim_expires_at = NULL
             FROM content_lock_access_drain_credentials AS credential
             WHERE read.credential_id = credential.credential_id
               AND credential.deletion_job_id = $4
               AND credential.lookup_key = $1 AND read.guarded_path = $2
               AND read.claim_token = $3 AND read.consumed_at IS NULL",
        )
        .bind(lookup_key.as_bytes().as_slice())
        .bind(path)
        .bind(claim_token)
        .bind(deletion_job_id)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(updated.rows_affected() == 1)
    }

    async fn consume_deletion_read(
        &self,
        lookup_key: &AccessCredentialLookupKey,
        path: &str,
        claim_token: Uuid,
        now: OffsetDateTime,
    ) -> Result<bool, ApplicationError> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let Some(deletion_job_id) = lookup_deletion_job_id(&mut transaction, lookup_key).await?
        else {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(false);
        };
        let Some(job) = lock_active_drain_job(&mut transaction, deletion_job_id).await? else {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(false);
        };
        if !phase_allows_credential_access(&job.phase, "final")
            || job
                .final_read_deadline
                .is_none_or(|deadline| deadline <= now)
        {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(false);
        }
        let updated = sqlx::query(
            "UPDATE content_lock_access_drain_reads AS read
             SET claim_token = NULL, claim_expires_at = NULL, consumed_at = $4
             FROM content_lock_access_drain_credentials AS credential
             WHERE read.credential_id = credential.credential_id
               AND credential.deletion_job_id = $5
               AND credential.lookup_key = $1 AND read.guarded_path = $2
               AND read.claim_token = $3 AND read.claim_expires_at > $4
               AND read.consumed_at IS NULL",
        )
        .bind(lookup_key.as_bytes().as_slice())
        .bind(path)
        .bind(claim_token)
        .bind(now)
        .bind(deletion_job_id)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(updated.rows_affected() == 1)
    }
}

struct LockedDrainJob {
    phase: String,
    final_read_deadline: Option<OffsetDateTime>,
    frozen_content_lock: serde_json::Value,
}

async fn lookup_deletion_job_id(
    transaction: &mut Transaction<'_, Postgres>,
    lookup_key: &AccessCredentialLookupKey,
) -> Result<Option<Uuid>, ApplicationError> {
    sqlx::query_scalar(
        "SELECT deletion_job_id
         FROM content_lock_access_drain_credentials
         WHERE lookup_key = $1",
    )
    .bind(lookup_key.as_bytes().as_slice())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(storage_error)
}

async fn lock_active_drain_job(
    transaction: &mut Transaction<'_, Postgres>,
    deletion_job_id: Uuid,
) -> Result<Option<LockedDrainJob>, ApplicationError> {
    let row = sqlx::query(
        "SELECT phase, final_read_deadline, frozen_content_lock
         FROM content_lock_deletion_jobs
         WHERE job_id = $1 AND state IN ('queued', 'running')
           AND force_requested_at IS NULL
         FOR UPDATE",
    )
    .bind(deletion_job_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(storage_error)?;
    row.map(|row| {
        Ok(LockedDrainJob {
            phase: row.try_get("phase").map_err(storage_error)?,
            final_read_deadline: row.try_get("final_read_deadline").map_err(storage_error)?,
            frozen_content_lock: row.try_get("frozen_content_lock").map_err(storage_error)?,
        })
    })
    .transpose()
}

fn phase_allows_credential_access(phase: &str, credential_kind: &str) -> bool {
    match credential_kind {
        "ordinary" => matches!(
            phase,
            "withdraw"
                | "start_payment_drain"
                | "drain_payments"
                | "drain_existing_credentials"
                | "issue_final_credentials"
                | "drain_final_reads"
        ),
        "final" => matches!(phase, "issue_final_credentials" | "drain_final_reads"),
        _ => false,
    }
}

fn row_to_record(row: sqlx::postgres::PgRow) -> Result<AccessCredentialRecord, ApplicationError> {
    let creator =
        CreatorPubky::from_str(&row.try_get::<String, _>("creator").map_err(storage_error)?)
            .map_err(|error| ApplicationError::Storage {
                message: format!("invalid access credential creator stored in Postgres: {error}"),
            })?;
    let bundle_id = BundleId::from_str(
        &row.try_get::<String, _>("bundle_id")
            .map_err(storage_error)?,
    )
    .map_err(|error| ApplicationError::Storage {
        message: format!("invalid access credential bundle_id stored in Postgres: {error}"),
    })?;

    Ok(AccessCredentialRecord {
        creator,
        bundle_id,
        expires_at: row.try_get("expires_at").map_err(storage_error)?,
    })
}

fn storage_error(error: sqlx::Error) -> ApplicationError {
    ApplicationError::Storage {
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use sqlx::Row;
    use time::macros::datetime;

    use locks_core::ids::{BundleId, CreatorPubky, LockId};

    use super::PostgresAccessCredentialStore;
    use crate::application::errors::ApplicationError;
    use crate::application::models::{
        AccessCredential, AccessCredentialLookupKey, AccessCredentialRecord,
    };
    use crate::application::ports::AccessCredentialStore;
    use crate::infrastructure::postgres::testing::TestDatabase;

    #[tokio::test]
    async fn insert_read_delete_and_duplicate_semantics_match_port_contract() {
        let database = TestDatabase::create().await;
        let store = PostgresAccessCredentialStore::new(database.pool().clone());
        let credential = AccessCredential::new("raw-bearer-credential");
        let lookup_key = AccessCredentialLookupKey::derive(&credential);
        let record = access_credential_record();
        let lock_id =
            LockId::from_str("000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG").unwrap();

        assert_eq!(
            store.get_access_credential(&lookup_key).await.unwrap(),
            None
        );

        store
            .insert_access_credential(&lock_id, lookup_key.clone(), record.clone())
            .await
            .unwrap();
        assert_eq!(
            store.get_access_credential(&lookup_key).await.unwrap(),
            Some(record.clone())
        );
        assert_eq!(
            store
                .insert_access_credential(&lock_id, lookup_key.clone(), record)
                .await,
            Err(ApplicationError::DuplicateRecord {
                record: "access_credential",
            })
        );

        store.delete_access_credential(&lookup_key).await.unwrap();
        store.delete_access_credential(&lookup_key).await.unwrap();
        assert_eq!(
            store.get_access_credential(&lookup_key).await.unwrap(),
            None
        );

        database.cleanup().await;
    }

    #[tokio::test]
    async fn record_survives_store_recreation_and_persists_exact_lookup_key_without_raw_credential()
    {
        let database = TestDatabase::create().await;
        let original_store = PostgresAccessCredentialStore::new(database.pool().clone());
        let recreated_store = PostgresAccessCredentialStore::new(database.pool().clone());
        let raw_credential = "raw-bearer-credential";
        let credential = AccessCredential::new(raw_credential);
        let lookup_key = AccessCredentialLookupKey::derive(&credential);
        let record = access_credential_record();
        let lock_id =
            LockId::from_str("000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG").unwrap();

        original_store
            .insert_access_credential(&lock_id, lookup_key.clone(), record.clone())
            .await
            .unwrap();

        assert_eq!(
            recreated_store
                .get_access_credential(&lookup_key)
                .await
                .unwrap(),
            Some(record)
        );
        assert_stored_lookup_key_is_exact(database.pool(), &lookup_key).await;
        assert_raw_credential_not_stored(database.pool(), raw_credential).await;

        database.cleanup().await;
    }

    #[tokio::test]
    async fn committed_deletion_rejects_ordinary_credential_without_inserting() {
        let database = TestDatabase::create().await;
        let store = PostgresAccessCredentialStore::new(database.pool().clone());
        let lock_id =
            LockId::from_str("000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG").unwrap();
        let record = access_credential_record();
        let lookup_key = AccessCredentialLookupKey::derive(&AccessCredential::new("rejected"));
        sqlx::query(
            "INSERT INTO content_lock_deletion_jobs (
                job_id, creator, lock_id, deletion_started_at, frozen_content_lock
            ) VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(uuid::Uuid::new_v4())
        .bind(record.creator.to_string())
        .bind(lock_id.to_string())
        .bind(datetime!(2026-05-29 12:00:00 UTC))
        .bind(serde_json::json!({"version": "1"}))
        .execute(database.pool())
        .await
        .unwrap();

        assert_eq!(
            store
                .insert_access_credential(&lock_id, lookup_key, record)
                .await,
            Err(ApplicationError::ContentLockDeletionInProgress)
        );
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM access_credentials")
            .fetch_one(database.pool())
            .await
            .unwrap();
        assert_eq!(count, 0);

        database.cleanup().await;
    }

    async fn assert_stored_lookup_key_is_exact(
        pool: &sqlx::PgPool,
        lookup_key: &AccessCredentialLookupKey,
    ) {
        let stored_lookup_key = sqlx::query("SELECT lookup_key FROM access_credentials")
            .fetch_one(pool)
            .await
            .expect("query stored lookup key")
            .try_get::<Vec<u8>, _>("lookup_key")
            .expect("lookup_key is bytes");

        assert_eq!(stored_lookup_key.as_slice(), lookup_key.as_bytes());
    }

    async fn assert_raw_credential_not_stored(pool: &sqlx::PgPool, raw_credential: &str) {
        let raw_credential_present = sqlx::query(
            "SELECT EXISTS (
                SELECT 1
                FROM access_credentials
                WHERE creator = $1 OR bundle_id = $1 OR encode(lookup_key, 'escape') = $1
            )",
        )
        .bind(raw_credential)
        .fetch_one(pool)
        .await
        .expect("query raw credential absence")
        .try_get::<bool, _>(0)
        .expect("exists result is bool");

        assert!(!raw_credential_present);
    }

    fn access_credential_record() -> AccessCredentialRecord {
        AccessCredentialRecord {
            creator: CreatorPubky::from_str(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            )
            .unwrap(),
            bundle_id: BundleId::from_str("000G40R40M30E209185GR38E1W").unwrap(),
            expires_at: datetime!(2026-05-29 12:15:00 UTC),
        }
    }
}
