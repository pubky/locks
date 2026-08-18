use std::str::FromStr;

use async_trait::async_trait;
use sqlx::{PgPool, Postgres, Row, Transaction};
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

use locks_core::{
    ids::{BundleId, CreatorPubky, LockId},
    lock_policy::{ContentLock, GuardedResource},
};

use crate::application::errors::ApplicationError;
use crate::application::models::{
    AccessCredential, AccessCredentialLookupKey, AccessCredentialRecord, DeletionReadAuthorization,
    EncryptedFinalCredential, FinalAccessWindows, FinalCredentialContext,
    FinalCredentialMaterialization, InitializeFinalAccessWindowsResult, IssuedDeletionCredential,
};
use crate::application::ports::{AccessCredentialStore, FinalCredentialWorkerIssueRequest};
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
        issuance_window: Duration,
        read_window: Duration,
    ) -> Result<InitializeFinalAccessWindowsResult, ApplicationError> {
        if issuance_window <= Duration::ZERO || read_window <= Duration::ZERO {
            return Err(ApplicationError::Storage {
                message: "final access window durations must be positive".to_owned(),
            });
        }
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let job = sqlx::query(
            "SELECT state, phase, force_requested_at, claimed_by, claim_token, claim_expires_at,
                    final_issuance_started_at, final_credential_issuance_deadline,
                    final_read_deadline
             FROM content_lock_deletion_jobs
             WHERE job_id = $1
             FOR UPDATE",
        )
        .bind(deletion_job_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let Some(job) = job else {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(InitializeFinalAccessWindowsResult::ClaimLost);
        };
        let now: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await
            .map_err(storage_error)?;
        let owns_live_claim = job.try_get::<String, _>("state").map_err(storage_error)?
            == "running"
            && job.try_get::<String, _>("phase").map_err(storage_error)?
                == "issue_final_credentials"
            && job
                .try_get::<Option<OffsetDateTime>, _>("force_requested_at")
                .map_err(storage_error)?
                .is_none()
            && job
                .try_get::<Option<String>, _>("claimed_by")
                .map_err(storage_error)?
                .as_deref()
                == Some(worker_id)
            && job
                .try_get::<Option<Uuid>, _>("claim_token")
                .map_err(storage_error)?
                == Some(claim_token)
            && job
                .try_get::<Option<OffsetDateTime>, _>("claim_expires_at")
                .map_err(storage_error)?
                .is_some_and(|expires_at| expires_at > now);
        if !owns_live_claim {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(InitializeFinalAccessWindowsResult::ClaimLost);
        }
        let existing = (
            job.try_get::<Option<OffsetDateTime>, _>("final_issuance_started_at")
                .map_err(storage_error)?,
            job.try_get::<Option<OffsetDateTime>, _>("final_credential_issuance_deadline")
                .map_err(storage_error)?,
            job.try_get::<Option<OffsetDateTime>, _>("final_read_deadline")
                .map_err(storage_error)?,
        );
        let windows = match existing {
            (
                Some(issuance_started_at),
                Some(credential_issuance_deadline),
                Some(read_deadline),
            ) => FinalAccessWindows {
                issuance_started_at,
                credential_issuance_deadline,
                read_deadline,
            },
            (None, None, None) => {
                let credential_issuance_deadline =
                    now.checked_add(issuance_window)
                        .ok_or_else(|| ApplicationError::Storage {
                            message: "final credential issuance deadline overflow".to_owned(),
                        })?;
                let read_deadline = credential_issuance_deadline
                    .checked_add(read_window)
                    .ok_or_else(|| ApplicationError::Storage {
                        message: "final read deadline overflow".to_owned(),
                    })?;
                sqlx::query(
                    "UPDATE content_lock_deletion_jobs
                     SET final_issuance_started_at = $2,
                         final_credential_issuance_deadline = $3,
                         final_read_deadline = $4
                     WHERE job_id = $1",
                )
                .bind(deletion_job_id)
                .bind(now)
                .bind(credential_issuance_deadline)
                .bind(read_deadline)
                .execute(&mut *transaction)
                .await
                .map_err(storage_error)?;
                FinalAccessWindows {
                    issuance_started_at: now,
                    credential_issuance_deadline,
                    read_deadline,
                }
            }
            _ => {
                return Err(ApplicationError::Storage {
                    message: "incomplete final access windows in Postgres".to_owned(),
                });
            }
        };
        transaction.commit().await.map_err(storage_error)?;
        Ok(InitializeFinalAccessWindowsResult::Initialized(windows))
    }

    async fn final_credentials_to_materialize(
        &self,
        deletion_job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        limit: usize,
    ) -> Result<Vec<FinalCredentialMaterialization>, ApplicationError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let limit = i64::try_from(limit).map_err(|_| ApplicationError::Storage {
            message: "final credential materialization limit exceeds Postgres BIGINT".to_owned(),
        })?;
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let job = sqlx::query(
            "SELECT creator, state, phase, force_requested_at, claimed_by, claim_token,
                    claim_expires_at, final_credential_issuance_deadline
             FROM content_lock_deletion_jobs
             WHERE job_id = $1
             FOR UPDATE",
        )
        .bind(deletion_job_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let Some(job) = job else {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(Vec::new());
        };
        let now: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await
            .map_err(storage_error)?;
        let owns_live_issue_claim = job.try_get::<String, _>("state").map_err(storage_error)?
            == "running"
            && job.try_get::<String, _>("phase").map_err(storage_error)?
                == "issue_final_credentials"
            && job
                .try_get::<Option<OffsetDateTime>, _>("force_requested_at")
                .map_err(storage_error)?
                .is_none()
            && job
                .try_get::<Option<String>, _>("claimed_by")
                .map_err(storage_error)?
                .as_deref()
                == Some(worker_id)
            && job
                .try_get::<Option<Uuid>, _>("claim_token")
                .map_err(storage_error)?
                == Some(claim_token)
            && job
                .try_get::<Option<OffsetDateTime>, _>("claim_expires_at")
                .map_err(storage_error)?
                .is_some_and(|deadline| now < deadline)
            && job
                .try_get::<Option<OffsetDateTime>, _>("final_credential_issuance_deadline")
                .map_err(storage_error)?
                .is_some_and(|deadline| now < deadline);
        if !owns_live_issue_claim {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(Vec::new());
        }
        let creator: String = job.try_get("creator").map_err(storage_error)?;
        let creator =
            CreatorPubky::from_str(&creator).map_err(|error| ApplicationError::Storage {
                message: format!("invalid deletion job creator stored in Postgres: {error}"),
            })?;
        let bundle_ids: Vec<String> = sqlx::query_scalar(
            "SELECT bundle_id
             FROM content_lock_deletion_task_snapshot
             WHERE deletion_job_id = $1
               AND resolved_status = 'completed'
               AND final_credential_eligible_at IS NOT NULL
               AND final_credential_issued_at IS NULL
             ORDER BY bundle_id ASC
             LIMIT $2",
        )
        .bind(deletion_job_id)
        .bind(limit)
        .fetch_all(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let pending = bundle_ids
            .into_iter()
            .map(|bundle_id| {
                BundleId::from_str(&bundle_id)
                    .map(|bundle_id| FinalCredentialMaterialization {
                        creator: creator.clone(),
                        bundle_id,
                    })
                    .map_err(|error| ApplicationError::Storage {
                        message: format!(
                            "invalid final credential bundle_id stored in Postgres: {error}"
                        ),
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(pending)
    }

    async fn issue_or_replay_final_credential(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
        _caller_now: OffsetDateTime,
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
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let Some(job) = job else {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(None);
        };
        // `transaction_timestamp()` is fixed at BEGIN; sample the wall clock only after the
        // job-row lock has serialized this winner with force/phase/deadline transitions.
        let now: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await
            .map_err(storage_error)?;
        let deletion_job_id: Uuid = job.try_get("job_id").map_err(storage_error)?;
        let phase: String = job.try_get("phase").map_err(storage_error)?;
        let issuance_deadline: OffsetDateTime = job
            .try_get("final_credential_issuance_deadline")
            .map_err(storage_error)?;
        let expires_at: OffsetDateTime =
            job.try_get("final_read_deadline").map_err(storage_error)?;
        if now >= expires_at {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(None);
        }
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

    async fn issue_or_replay_final_credential_for_worker(
        &self,
        request: FinalCredentialWorkerIssueRequest<'_>,
    ) -> Result<Option<IssuedDeletionCredential>, ApplicationError> {
        let FinalCredentialWorkerIssueRequest {
            deletion_job_id,
            worker_id,
            claim_token,
            creator,
            bundle_id,
            now: _caller_now,
            candidate,
        } = request;
        let cipher = match &self.final_credential_cipher {
            Some(cipher) => cipher,
            None => return Ok(None),
        };
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let job = sqlx::query(
            "SELECT job.job_id, job.creator, job.state, job.phase, job.force_requested_at,
                    job.claimed_by, job.claim_token, job.claim_expires_at,
                    job.final_credential_issuance_deadline, job.final_read_deadline,
                    job.frozen_content_lock
             FROM content_lock_deletion_jobs AS job
             WHERE job.job_id = $1
             FOR UPDATE OF job",
        )
        .bind(deletion_job_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let Some(job) = job else {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(None);
        };
        // PostgreSQL transaction time is fixed at BEGIN, so only clock_timestamp() sampled after
        // the row lock is authoritative for a statement that may have waited behind another owner.
        let now: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await
            .map_err(storage_error)?;
        let owns_live_issue_claim = job.try_get::<String, _>("creator").map_err(storage_error)?
            == creator.to_string()
            && job.try_get::<String, _>("state").map_err(storage_error)? == "running"
            && job.try_get::<String, _>("phase").map_err(storage_error)?
                == "issue_final_credentials"
            && job
                .try_get::<Option<OffsetDateTime>, _>("force_requested_at")
                .map_err(storage_error)?
                .is_none()
            && job
                .try_get::<Option<String>, _>("claimed_by")
                .map_err(storage_error)?
                .as_deref()
                == Some(worker_id)
            && job
                .try_get::<Option<Uuid>, _>("claim_token")
                .map_err(storage_error)?
                == Some(claim_token)
            && job
                .try_get::<Option<OffsetDateTime>, _>("claim_expires_at")
                .map_err(storage_error)?
                .is_some_and(|deadline| deadline > now)
            && job
                .try_get::<Option<OffsetDateTime>, _>("final_credential_issuance_deadline")
                .map_err(storage_error)?
                .is_some_and(|deadline| deadline > now)
            && job
                .try_get::<Option<OffsetDateTime>, _>("final_read_deadline")
                .map_err(storage_error)?
                .is_some_and(|deadline| deadline > now);
        if !owns_live_issue_claim {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(None);
        }
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
        claim_duration: Duration,
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
        // Transaction time is fixed at BEGIN. Sample wall-clock time only after the exact job-row
        // fence, so a wait cannot revive an expired credential, deadline, or read claim.
        let now: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await
            .map_err(storage_error)?;
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
        let bounded_expiry = now
            .checked_add(claim_duration)
            .ok_or_else(|| ApplicationError::Storage {
                message: "final read claim expiry overflow".to_owned(),
            })?
            .min(now + time::Duration::seconds(30))
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
        let now: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await
            .map_err(storage_error)?;
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
    use std::{collections::BTreeMap, str::FromStr};

    use sqlx::Row;
    use time::macros::datetime;
    use uuid::Uuid;

    use locks_core::{
        ids::{BundleId, CreatorPubky, GuardedResourceHash, LockId},
        lock_policy::{
            AccessPolicy, CONTENT_LOCK_VERSION, ContentLock, GuardedResource, LockLogic,
            LockServerConfig,
        },
    };

    use super::PostgresAccessCredentialStore;
    use crate::application::errors::ApplicationError;
    use crate::application::models::{
        AccessCredential, AccessCredentialLookupKey, AccessCredentialRecord,
        InitializeFinalAccessWindowsResult,
    };
    use crate::application::ports::{AccessCredentialStore, FinalCredentialWorkerIssueRequest};
    use crate::infrastructure::final_credentials::FinalCredentialCipher;
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

    #[tokio::test]
    async fn final_credentials_to_materialize_returns_eligible_unissued_rows_in_order_with_limit() {
        let database = TestDatabase::create().await;
        let store = PostgresAccessCredentialStore::new(database.pool().clone());
        let now = sqlx::query_scalar::<_, time::OffsetDateTime>("SELECT clock_timestamp()")
            .fetch_one(database.pool())
            .await
            .unwrap();
        let claim_token = Uuid::new_v4();
        let job_id = insert_final_issuance_job(database.pool(), now, claim_token).await;
        insert_final_snapshot(
            database.pool(),
            job_id,
            "000G40R40M30E209185GR38E1W",
            true,
            false,
        )
        .await;
        insert_final_snapshot(
            database.pool(),
            job_id,
            "000G40R40M30E209185GR38E1R",
            true,
            false,
        )
        .await;
        insert_final_snapshot(
            database.pool(),
            job_id,
            "000G40R40M30E209185GR38E1M",
            false,
            false,
        )
        .await;
        insert_final_snapshot(
            database.pool(),
            job_id,
            "000G40R40M30E209185GR38E1G",
            true,
            true,
        )
        .await;

        let bounded = store
            .final_credentials_to_materialize(job_id, "worker", claim_token, 1)
            .await
            .unwrap();
        assert_eq!(bounded.len(), 1);
        assert_eq!(bounded[0].bundle_id.as_str(), "000G40R40M30E209185GR38E1R");
        assert_eq!(bounded[0].creator, creator());

        let all = store
            .final_credentials_to_materialize(job_id, "worker", claim_token, 10)
            .await
            .unwrap();
        assert_eq!(
            all.iter()
                .map(|pending| pending.bundle_id.as_str())
                .collect::<Vec<_>>(),
            vec!["000G40R40M30E209185GR38E1R", "000G40R40M30E209185GR38E1W"]
        );
        assert!(
            store
                .final_credentials_to_materialize(job_id, "worker", claim_token, 0)
                .await
                .unwrap()
                .is_empty()
        );

        database.cleanup().await;
    }

    #[tokio::test]
    async fn final_credentials_to_materialize_revalidates_exact_live_issue_claim_and_deadline() {
        let database = TestDatabase::create().await;
        let store = PostgresAccessCredentialStore::new(database.pool().clone());
        let now = sqlx::query_scalar::<_, time::OffsetDateTime>("SELECT clock_timestamp()")
            .fetch_one(database.pool())
            .await
            .unwrap();
        let claim_token = Uuid::new_v4();
        let job_id = insert_final_issuance_job(database.pool(), now, claim_token).await;
        insert_final_snapshot(
            database.pool(),
            job_id,
            "000G40R40M30E209185GR38E1W",
            true,
            false,
        )
        .await;

        assert!(
            store
                .final_credentials_to_materialize(job_id, "worker", Uuid::new_v4(), 10)
                .await
                .unwrap()
                .is_empty()
        );

        sqlx::query(
            "UPDATE content_lock_deletion_jobs SET force_requested_at = $2 WHERE job_id = $1",
        )
        .bind(job_id)
        .bind(now)
        .execute(database.pool())
        .await
        .unwrap();
        assert!(
            store
                .final_credentials_to_materialize(job_id, "worker", claim_token, 10)
                .await
                .unwrap()
                .is_empty()
        );

        sqlx::query(
            "UPDATE content_lock_deletion_jobs
             SET force_requested_at = NULL, phase = 'drain_final_reads'
             WHERE job_id = $1",
        )
        .bind(job_id)
        .execute(database.pool())
        .await
        .unwrap();
        assert!(
            store
                .final_credentials_to_materialize(job_id, "worker", claim_token, 10)
                .await
                .unwrap()
                .is_empty()
        );

        sqlx::query(
            "UPDATE content_lock_deletion_jobs
             SET phase = 'issue_final_credentials', final_credential_issuance_deadline = $2
             WHERE job_id = $1",
        )
        .bind(job_id)
        .bind(now)
        .execute(database.pool())
        .await
        .unwrap();
        assert!(
            store
                .final_credentials_to_materialize(job_id, "worker", claim_token, 10)
                .await
                .unwrap()
                .is_empty()
        );

        database.cleanup().await;
    }

    #[tokio::test]
    async fn final_credentials_to_materialize_samples_time_after_job_lock_and_rejects_equality() {
        let database = TestDatabase::create().await;
        let store = PostgresAccessCredentialStore::new(database.pool().clone());
        let now = sqlx::query_scalar::<_, time::OffsetDateTime>("SELECT clock_timestamp()")
            .fetch_one(database.pool())
            .await
            .unwrap();
        let claim_token = Uuid::new_v4();
        let job_id = insert_final_issuance_job(database.pool(), now, claim_token).await;
        insert_final_snapshot(
            database.pool(),
            job_id,
            "000G40R40M30E209185GR38E1W",
            true,
            false,
        )
        .await;

        let mut blocker = database.pool().begin().await.unwrap();
        sqlx::query("SELECT job_id FROM content_lock_deletion_jobs WHERE job_id = $1 FOR UPDATE")
            .bind(job_id)
            .fetch_one(&mut *blocker)
            .await
            .unwrap();
        let waiting_store = store.clone();
        let enumeration = tokio::spawn(async move {
            waiting_store
                .final_credentials_to_materialize(job_id, "worker", claim_token, 10)
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        assert!(!enumeration.is_finished());
        sqlx::query(
            "UPDATE content_lock_deletion_jobs
             SET claim_expires_at = clock_timestamp(),
                 final_credential_issuance_deadline = clock_timestamp()
             WHERE job_id = $1",
        )
        .bind(job_id)
        .execute(&mut *blocker)
        .await
        .unwrap();
        blocker.commit().await.unwrap();

        assert!(enumeration.await.unwrap().unwrap().is_empty());
        database.cleanup().await;
    }

    #[tokio::test]
    async fn worker_final_issuance_is_exact_claim_fenced_in_winner_transaction() {
        let database = TestDatabase::create().await;
        let store = PostgresAccessCredentialStore::with_final_credential_cipher(
            database.pool().clone(),
            FinalCredentialCipher::new([41; 32]),
        );
        let now = sqlx::query_scalar::<_, time::OffsetDateTime>("SELECT CURRENT_TIMESTAMP")
            .fetch_one(database.pool())
            .await
            .unwrap();
        let claim_token = Uuid::new_v4();
        let job_id = insert_final_issuance_job(database.pool(), now, claim_token).await;
        let bundle_id = BundleId::from_str("000G40R40M30E209185GR38E1R").unwrap();
        let creator = creator();
        insert_final_snapshot(database.pool(), job_id, bundle_id.as_str(), true, false).await;

        for (job, worker, token, at) in [
            (Uuid::new_v4(), "worker", claim_token, now),
            (job_id, "reclaimer", claim_token, now),
            (job_id, "worker", Uuid::new_v4(), now),
        ] {
            let candidate = AccessCredential::new(format!("denied-{job}-{worker}-{at}"));
            assert!(
                store
                    .issue_or_replay_final_credential_for_worker(
                        FinalCredentialWorkerIssueRequest {
                            deletion_job_id: job,
                            worker_id: worker,
                            claim_token: token,
                            creator: &creator,
                            bundle_id: &bundle_id,
                            now: at,
                            candidate,
                        },
                    )
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM access_credentials")
            .fetch_one(database.pool())
            .await
            .unwrap();
        assert_eq!(count, 0);

        sqlx::query(
            "UPDATE content_lock_deletion_jobs SET force_requested_at = $2 WHERE job_id = $1",
        )
        .bind(job_id)
        .bind(now)
        .execute(database.pool())
        .await
        .unwrap();
        assert!(
            store
                .issue_or_replay_final_credential_for_worker(FinalCredentialWorkerIssueRequest {
                    deletion_job_id: job_id,
                    worker_id: "worker",
                    claim_token,
                    creator: &creator,
                    bundle_id: &bundle_id,
                    now,
                    candidate: AccessCredential::new("force-loser"),
                },)
                .await
                .unwrap()
                .is_none()
        );

        let reclaimed_token = Uuid::new_v4();
        sqlx::query(
            "UPDATE content_lock_deletion_jobs
             SET force_requested_at = NULL, claimed_by = 'reclaimer', claim_token = $2,
                 claim_expires_at = $3
             WHERE job_id = $1",
        )
        .bind(job_id)
        .bind(reclaimed_token)
        .bind(now + time::Duration::minutes(5))
        .execute(database.pool())
        .await
        .unwrap();
        assert!(
            store
                .issue_or_replay_final_credential_for_worker(FinalCredentialWorkerIssueRequest {
                    deletion_job_id: job_id,
                    worker_id: "worker",
                    claim_token,
                    creator: &creator,
                    bundle_id: &bundle_id,
                    now,
                    candidate: AccessCredential::new("stale-reclaimed-loser"),
                },)
                .await
                .unwrap()
                .is_none()
        );

        let winner = AccessCredential::new("postgres-worker-winner");
        let issued = store
            .issue_or_replay_final_credential_for_worker(FinalCredentialWorkerIssueRequest {
                deletion_job_id: job_id,
                worker_id: "reclaimer",
                claim_token: reclaimed_token,
                creator: &creator,
                bundle_id: &bundle_id,
                now,
                candidate: winner.clone(),
            })
            .await
            .unwrap()
            .unwrap();
        let replay = store
            .issue_or_replay_final_credential_for_worker(FinalCredentialWorkerIssueRequest {
                deletion_job_id: job_id,
                worker_id: "reclaimer",
                claim_token: reclaimed_token,
                creator: &creator,
                bundle_id: &bundle_id,
                now,
                candidate: AccessCredential::new("postgres-worker-loser"),
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(issued.credential, winner);
        assert_eq!(replay, issued);
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM access_credentials")
            .fetch_one(database.pool())
            .await
            .unwrap();
        assert_eq!(count, 1);

        database.cleanup().await;
    }

    #[tokio::test]
    async fn public_final_issuance_rechecks_database_time_after_waiting_for_job_lock() {
        let database = TestDatabase::create().await;
        let store = PostgresAccessCredentialStore::with_final_credential_cipher(
            database.pool().clone(),
            FinalCredentialCipher::new([43; 32]),
        );
        let caller_now = datetime!(2026-05-29 12:00:00 UTC);
        let claim_token = Uuid::new_v4();
        let job_id = insert_final_issuance_job(database.pool(), caller_now, claim_token).await;
        let bundle_id = BundleId::from_str("000G40R40M30E209185GR38E1R").unwrap();
        let creator = creator();
        insert_final_snapshot(database.pool(), job_id, bundle_id.as_str(), true, false).await;

        let mut blocker = database.pool().begin().await.unwrap();
        sqlx::query(
            "UPDATE content_lock_deletion_jobs
             SET final_credential_issuance_deadline = clock_timestamp() + interval '500 milliseconds',
                 final_read_deadline = clock_timestamp() + interval '10 minutes'
             WHERE job_id = $1",
        )
        .bind(job_id)
        .execute(&mut *blocker)
        .await
        .unwrap();

        let issuance = tokio::spawn(async move {
            store
                .issue_or_replay_final_credential(
                    &creator,
                    &bundle_id,
                    caller_now,
                    AccessCredential::new("must-not-persist-after-public-lock-wait"),
                )
                .await
        });
        sqlx::query("SELECT pg_sleep(1)")
            .execute(&mut *blocker)
            .await
            .unwrap();
        blocker.commit().await.unwrap();

        assert!(issuance.await.unwrap().unwrap().is_none());
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM access_credentials")
            .fetch_one(database.pool())
            .await
            .unwrap();
        assert_eq!(count, 0);

        database.cleanup().await;
    }

    #[tokio::test]
    async fn worker_final_issuance_rechecks_database_time_after_waiting_for_job_lock() {
        let database = TestDatabase::create().await;
        let store = PostgresAccessCredentialStore::with_final_credential_cipher(
            database.pool().clone(),
            FinalCredentialCipher::new([42; 32]),
        );
        let caller_now = datetime!(2026-05-29 12:00:00 UTC);
        let claim_token = Uuid::new_v4();
        let job_id = insert_final_issuance_job(database.pool(), caller_now, claim_token).await;
        let bundle_id = BundleId::from_str("000G40R40M30E209185GR38E1R").unwrap();
        let creator = creator();
        insert_final_snapshot(database.pool(), job_id, bundle_id.as_str(), true, false).await;

        let mut blocker = database.pool().begin().await.unwrap();
        sqlx::query(
            "UPDATE content_lock_deletion_jobs
             SET claim_expires_at = clock_timestamp() + interval '500 milliseconds',
                 final_credential_issuance_deadline = clock_timestamp() + interval '500 milliseconds',
                 final_read_deadline = clock_timestamp() + interval '10 minutes'
             WHERE job_id = $1",
        )
        .bind(job_id)
        .execute(&mut *blocker)
        .await
        .unwrap();

        let issuance = tokio::spawn(async move {
            store
                .issue_or_replay_final_credential_for_worker(FinalCredentialWorkerIssueRequest {
                    deletion_job_id: job_id,
                    worker_id: "worker",
                    claim_token,
                    creator: &creator,
                    bundle_id: &bundle_id,
                    now: caller_now,
                    candidate: AccessCredential::new("must-not-persist-after-lock-wait"),
                })
                .await
        });
        sqlx::query("SELECT pg_sleep(1)")
            .execute(&mut *blocker)
            .await
            .unwrap();
        blocker.commit().await.unwrap();

        assert!(issuance.await.unwrap().unwrap().is_none());
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM access_credentials")
            .fetch_one(database.pool())
            .await
            .unwrap();
        assert_eq!(count, 0);

        database.cleanup().await;
    }

    #[tokio::test]
    async fn final_access_window_initialization_rechecks_database_time_after_job_lock() {
        let database = TestDatabase::create().await;
        let store = PostgresAccessCredentialStore::new(database.pool().clone());
        let fixture_now: time::OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(database.pool())
            .await
            .unwrap();
        let claim_token = Uuid::new_v4();
        let job_id = insert_final_issuance_job(database.pool(), fixture_now, claim_token).await;

        let mut blocker = database.pool().begin().await.unwrap();
        sqlx::query(
            "UPDATE content_lock_deletion_jobs
             SET claim_expires_at = clock_timestamp() + interval '500 milliseconds',
                 final_issuance_started_at = NULL,
                 final_credential_issuance_deadline = NULL,
                 final_read_deadline = NULL
             WHERE job_id = $1",
        )
        .bind(job_id)
        .execute(&mut *blocker)
        .await
        .unwrap();

        let initialization = tokio::spawn(async move {
            store
                .initialize_final_access_windows(
                    job_id,
                    "worker",
                    claim_token,
                    time::Duration::minutes(15),
                    time::Duration::minutes(15),
                )
                .await
        });
        sqlx::query("SELECT pg_sleep(1)")
            .execute(&mut *blocker)
            .await
            .unwrap();
        blocker.commit().await.unwrap();

        assert_eq!(
            initialization.await.unwrap().unwrap(),
            InitializeFinalAccessWindowsResult::ClaimLost
        );
        let windows: (
            Option<time::OffsetDateTime>,
            Option<time::OffsetDateTime>,
            Option<time::OffsetDateTime>,
        ) = sqlx::query_as(
            "SELECT final_issuance_started_at, final_credential_issuance_deadline,
                        final_read_deadline
                 FROM content_lock_deletion_jobs WHERE job_id = $1",
        )
        .bind(job_id)
        .fetch_one(database.pool())
        .await
        .unwrap();
        assert_eq!(windows, (None, None, None));

        database.cleanup().await;
    }

    async fn insert_final_issuance_job(
        pool: &sqlx::PgPool,
        now: time::OffsetDateTime,
        claim_token: Uuid,
    ) -> Uuid {
        let job_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO content_lock_deletion_jobs (
                job_id, creator, lock_id, frozen_content_lock, deletion_started_at,
                state, phase, claimed_by, claim_token, claim_expires_at,
                final_issuance_started_at, final_credential_issuance_deadline,
                final_read_deadline
             ) VALUES ($1, $2, $3, $4, $5, 'running',
                       'issue_final_credentials', 'worker', $6, $7, $8, $9, $10)",
        )
        .bind(job_id)
        .bind(creator().to_string())
        .bind("000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG")
        .bind(serde_json::to_value(test_content_lock()).unwrap())
        .bind(now)
        .bind(claim_token)
        .bind(now + time::Duration::minutes(5))
        .bind(now - time::Duration::minutes(1))
        .bind(now + time::Duration::minutes(10))
        .bind(now + time::Duration::minutes(20))
        .execute(pool)
        .await
        .unwrap();
        job_id
    }

    async fn insert_final_snapshot(
        pool: &sqlx::PgPool,
        job_id: Uuid,
        bundle_id: &str,
        eligible: bool,
        issued: bool,
    ) {
        let task_id = Uuid::new_v4();
        let now = datetime!(2026-05-29 12:00:00 UTC);
        sqlx::query(
            "INSERT INTO verification_tasks (
                task_id, status, submitted_proof_bundle, submitted_at, creator, bundle_id,
                deletion_job_id
             ) VALUES ($1, 'completed', '{}'::jsonb, $2, $3, $4, $5)",
        )
        .bind(task_id)
        .bind(now)
        .bind(creator().to_string())
        .bind(bundle_id)
        .bind(job_id)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO content_lock_deletion_task_snapshot (
                deletion_job_id, verification_task_id, creator, bundle_id,
                pubky_lock_resource, criterion_id, status_at_cutoff,
                paykit_admission_required, resolved_status, resolved_at,
                final_credential_eligible_at, final_credential_issued_at
             ) VALUES ($1, $2, $3, $4, 'pubkycreator/pub/locks.app/lock.json',
                       'payment', 'completed', TRUE, 'completed', $5,
                       CASE WHEN $6 THEN $5 END,
                       CASE WHEN $7 THEN $5 END)",
        )
        .bind(job_id)
        .bind(task_id)
        .bind(creator().to_string())
        .bind(bundle_id)
        .bind(now)
        .bind(eligible)
        .bind(issued)
        .execute(pool)
        .await
        .unwrap();
    }

    fn creator() -> CreatorPubky {
        CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
    }

    fn test_content_lock() -> ContentLock {
        ContentLock {
            version: CONTENT_LOCK_VERSION,
            creator: creator(),
            primary_resource: Some(
                GuardedResource::new(
                    "/priv/locks.app/content/post.json".to_owned(),
                    GuardedResourceHash::from_bytes([7; 32]),
                    "application/json".to_owned(),
                    42,
                )
                .unwrap(),
            ),
            secondary_resources: BTreeMap::new(),
            criteria: vec![],
            lock_logic: LockLogic::All { criteria: vec![] },
            access_policy: AccessPolicy {
                requested_credential_ttl_seconds: 900,
            },
            lock_server: LockServerConfig { override_: None },
            created_at: datetime!(2026-05-29 11:00:00 UTC),
        }
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
