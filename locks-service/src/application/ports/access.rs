use async_trait::async_trait;
use locks_core::ids::{BundleId, CreatorPubky, LockId};
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

use crate::application::errors::ApplicationError;
use crate::application::models::{
    AccessCredential, AccessCredentialLookupKey, AccessCredentialRecord, DeletionReadAuthorization,
    FinalCredentialMaterialization, InitializeFinalAccessWindowsResult, IssuedDeletionCredential,
};

/// Exact worker ownership and candidate material for one final-credential winner operation.
pub struct FinalCredentialWorkerIssueRequest<'a> {
    pub deletion_job_id: Uuid,
    pub worker_id: &'a str,
    pub claim_token: Uuid,
    pub creator: &'a CreatorPubky,
    pub bundle_id: &'a BundleId,
    pub now: OffsetDateTime,
    pub candidate: AccessCredential,
}

/// Store for issued opaque access credentials.
#[async_trait]
pub trait AccessCredentialStore: Send + Sync {
    /// Inserts an issued access credential record.
    ///
    /// Returns `DuplicateRecord` if the credential lookup key already exists.
    async fn insert_access_credential(
        &self,
        lock_id: &LockId,
        lookup_key: AccessCredentialLookupKey,
        record: AccessCredentialRecord,
    ) -> Result<(), ApplicationError>;

    /// Resolves an access credential lookup key to its server-side state.
    ///
    /// Returns `Ok(None)` when the credential is absent.
    async fn get_access_credential(
        &self,
        lookup_key: &AccessCredentialLookupKey,
    ) -> Result<Option<AccessCredentialRecord>, ApplicationError>;

    /// Ensures an access credential is absent.
    ///
    /// Deleting a missing credential is successful.
    async fn delete_access_credential(
        &self,
        lookup_key: &AccessCredentialLookupKey,
    ) -> Result<(), ApplicationError>;

    async fn initialize_final_access_windows(
        &self,
        _deletion_job_id: Uuid,
        _worker_id: &str,
        _claim_token: Uuid,
        _issuance_window: Duration,
        _read_window: Duration,
    ) -> Result<InitializeFinalAccessWindowsResult, ApplicationError> {
        Ok(InitializeFinalAccessWindowsResult::ClaimLost)
    }

    /// Enumerates eligible snapshots awaiting final credential materialization under an exact
    /// live deletion-worker claim. Implementations return deterministic bundle ordering.
    async fn final_credentials_to_materialize(
        &self,
        _deletion_job_id: Uuid,
        _worker_id: &str,
        _claim_token: Uuid,
        _limit: usize,
    ) -> Result<Vec<FinalCredentialMaterialization>, ApplicationError> {
        Ok(Vec::new())
    }

    async fn issue_or_replay_final_credential(
        &self,
        _creator: &CreatorPubky,
        _bundle_id: &BundleId,
        _now: OffsetDateTime,
        _candidate: AccessCredential,
    ) -> Result<Option<IssuedDeletionCredential>, ApplicationError> {
        Ok(None)
    }

    /// Issues or replays one final credential only while the exact deletion-worker claim remains
    /// live. Implementations revalidate ownership and fresh time in the winner transaction.
    async fn issue_or_replay_final_credential_for_worker(
        &self,
        _request: FinalCredentialWorkerIssueRequest<'_>,
    ) -> Result<Option<IssuedDeletionCredential>, ApplicationError> {
        Ok(None)
    }

    /// Reports whether this deletion Bundle may issue or replay its final credential now.
    async fn final_credential_available(
        &self,
        _creator: &CreatorPubky,
        _bundle_id: &BundleId,
        _now: OffsetDateTime,
    ) -> Result<bool, ApplicationError> {
        Ok(false)
    }

    async fn prepare_deletion_read(
        &self,
        _lookup_key: &AccessCredentialLookupKey,
        _path: &str,
        _claim_duration: Duration,
    ) -> Result<Option<DeletionReadAuthorization>, ApplicationError> {
        Ok(None)
    }

    /// Reports whether a credential was enrolled in deletion, regardless of
    /// whether deletion access is currently usable.
    async fn deletion_credential_enrolled(
        &self,
        _lookup_key: &AccessCredentialLookupKey,
    ) -> Result<bool, ApplicationError> {
        Ok(false)
    }

    async fn release_deletion_read(
        &self,
        _lookup_key: &AccessCredentialLookupKey,
        _path: &str,
        _claim_token: Uuid,
        _now: OffsetDateTime,
    ) -> Result<bool, ApplicationError> {
        Ok(false)
    }

    async fn consume_deletion_read(
        &self,
        _lookup_key: &AccessCredentialLookupKey,
        _path: &str,
        _claim_token: Uuid,
    ) -> Result<bool, ApplicationError> {
        Ok(false)
    }
}

/// Generator for opaque access credentials.
#[async_trait]
pub trait AccessCredentialGenerator: Send + Sync {
    /// Generates a new raw bearer access credential.
    async fn generate_access_credential(&self) -> Result<AccessCredential, ApplicationError>;
}
