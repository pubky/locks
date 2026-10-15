use std::future::Future;
use std::pin::Pin;

use async_trait::async_trait;
use base64::Engine as _;
use locks_core::ids::CreatorPubky;
use locks_core::lock_policy::{
    PRIVATE_PROOF_BUNDLE_PATH_PREFIX, PRIVATE_RESOURCE_CONTENT_PATH_PREFIX,
};

use crate::application::errors::ApplicationError;
use crate::application::models::{CreatorAuthorityAuthKind, CreatorAuthoritySecret};
use crate::application::ports::{
    ContentLockWriteError, CreatorAuthorityManager, CreatorAuthorityStore,
};
use crate::infrastructure::pubky::grant_connect_flow::{
    LockServerGrantPopKeys, restore_grant_session,
};
use crate::infrastructure::pubky::grant_session_cache::GrantSessionCache;
use crate::infrastructure::pubky::legacy_connect_flow::creator_from_pubky_public_key_z32;

/// Bytes fetched from a creator-owned Pubky homeserver resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubkyBytesResource {
    /// Raw resource bytes.
    pub bytes: Vec<u8>,
    /// Optional content type from homeserver response metadata.
    pub content_type: Option<String>,
}

/// Metadata fetched without downloading a creator-owned Pubky resource body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubkyResourceMetadata {
    /// Stored byte length from response metadata.
    pub content_length: Option<u64>,
    /// Stored content type from response metadata.
    pub content_type: Option<String>,
    /// Stored BLAKE3 content hash decoded from Pubky ETag metadata.
    pub content_hash: Option<[u8; 32]>,
}

/// Outcome of an idempotent Pubky homeserver delete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PubkyDeleteOutcome {
    /// Resource was deleted by this request.
    Deleted,
    /// Resource was already absent when deletion reached the homeserver.
    NotFound,
}

impl PubkyResourceMetadata {
    /// Builds metadata for in-process storage adapters which already hold resource bytes.
    pub fn from_bytes(bytes: &[u8], content_type: Option<String>) -> Self {
        Self {
            content_length: u64::try_from(bytes.len()).ok(),
            content_type,
            content_hash: Some(*blake3::hash(bytes).as_bytes()),
        }
    }
}

/// Object-safe seam for creator-scoped Pubky homeserver storage operations.
#[async_trait]
pub trait PubkyHomeserverStorageClient: Send + Sync {
    /// Writes JSON to a creator-owned path.
    async fn put_json_value_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
        body: serde_json::Value,
    ) -> Result<(), ApplicationError>;

    /// Writes JSON while classifying failures that provably precede external dispatch.
    async fn put_json_value_as_creator_classified(
        &self,
        creator: &CreatorPubky,
        path: &str,
        body: serde_json::Value,
    ) -> Result<(), ContentLockWriteError> {
        self.put_json_value_as_creator(creator, path, body)
            .await
            .map_err(ContentLockWriteError::Ambiguous)
    }

    /// Reads JSON from a creator-owned path.
    async fn get_json_value_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<Option<serde_json::Value>, ApplicationError>;

    /// Writes bytes to a creator-owned path.
    async fn put_bytes_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
        bytes: Vec<u8>,
        content_type: &str,
    ) -> Result<(), ApplicationError>;

    /// Reads bytes from a creator-owned path.
    async fn get_bytes_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<Option<PubkyBytesResource>, ApplicationError>;

    /// Reads resource metadata without downloading its body.
    async fn get_metadata_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<Option<PubkyResourceMetadata>, ApplicationError>;

    /// Deletes a creator-owned path.
    async fn delete_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<(), ApplicationError>;

    /// Deletes a creator-owned path while treating a typed homeserver 404 as absence.
    async fn delete_if_present_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<PubkyDeleteOutcome, ApplicationError> {
        self.delete_as_creator(creator, path)
            .await
            .map(|()| PubkyDeleteOutcome::Deleted)
    }
}

#[async_trait]
impl<C> PubkyHomeserverStorageClient for std::sync::Arc<C>
where
    C: PubkyHomeserverStorageClient + ?Sized,
{
    async fn put_json_value_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
        body: serde_json::Value,
    ) -> Result<(), ApplicationError> {
        (**self)
            .put_json_value_as_creator(creator, path, body)
            .await
    }

    async fn put_json_value_as_creator_classified(
        &self,
        creator: &CreatorPubky,
        path: &str,
        body: serde_json::Value,
    ) -> Result<(), ContentLockWriteError> {
        (**self)
            .put_json_value_as_creator_classified(creator, path, body)
            .await
    }

    async fn get_json_value_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<Option<serde_json::Value>, ApplicationError> {
        (**self).get_json_value_as_creator(creator, path).await
    }

    async fn put_bytes_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
        bytes: Vec<u8>,
        content_type: &str,
    ) -> Result<(), ApplicationError> {
        (**self)
            .put_bytes_as_creator(creator, path, bytes, content_type)
            .await
    }

    async fn get_bytes_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<Option<PubkyBytesResource>, ApplicationError> {
        (**self).get_bytes_as_creator(creator, path).await
    }

    async fn get_metadata_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<Option<PubkyResourceMetadata>, ApplicationError> {
        (**self).get_metadata_as_creator(creator, path).await
    }

    async fn delete_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<(), ApplicationError> {
        (**self).delete_as_creator(creator, path).await
    }

    async fn delete_if_present_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<PubkyDeleteOutcome, ApplicationError> {
        (**self).delete_if_present_as_creator(creator, path).await
    }
}

/// Object-safe operations for storage already scoped to one creator's Pubky session.
#[async_trait]
pub trait CreatorScopedPubkyStorage: Send + Sync {
    async fn put_json_value(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> Result<(), ApplicationError>;

    async fn put_json_value_classified(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> Result<(), ContentLockWriteError> {
        self.put_json_value(path, body)
            .await
            .map_err(ContentLockWriteError::Ambiguous)
    }

    async fn get_json_value(
        &self,
        path: &str,
    ) -> Result<Option<serde_json::Value>, ApplicationError>;

    async fn put_bytes(
        &self,
        path: &str,
        bytes: Vec<u8>,
        content_type: &str,
    ) -> Result<(), ApplicationError>;

    async fn get_bytes(&self, path: &str) -> Result<Option<PubkyBytesResource>, ApplicationError>;

    async fn get_metadata(
        &self,
        path: &str,
    ) -> Result<Option<PubkyResourceMetadata>, ApplicationError>;

    async fn delete(&self, path: &str) -> Result<(), ApplicationError>;

    async fn delete_if_present(&self, path: &str) -> Result<PubkyDeleteOutcome, ApplicationError> {
        self.delete(path)
            .await
            .map(|()| PubkyDeleteOutcome::Deleted)
    }
}

/// Creator-scoped storage plus cache generation used to acquire it.
pub struct AcquiredCreatorScopedPubkyStorage {
    storage: Box<dyn CreatorScopedPubkyStorage>,
    generation: Option<u64>,
}

impl AcquiredCreatorScopedPubkyStorage {
    pub fn uncached(storage: Box<dyn CreatorScopedPubkyStorage>) -> Self {
        Self {
            storage,
            generation: None,
        }
    }

    pub fn cached(storage: Box<dyn CreatorScopedPubkyStorage>, generation: u64) -> Self {
        Self {
            storage,
            generation: Some(generation),
        }
    }
}

/// Provides storage scoped to the requested creator's restorable Pubky session.
#[async_trait]
pub trait CreatorScopedPubkyStorageProvider: Send + Sync {
    async fn storage_for_creator(
        &self,
        creator: &CreatorPubky,
    ) -> Result<AcquiredCreatorScopedPubkyStorage, ApplicationError>;

    async fn invalidate_creator_session(&self, _creator: &CreatorPubky, _generation: u64) {}
}

/// Minimal imported Pubky session surface needed by the storage provider.
pub trait ImportedPubkySession: Send + Sync {
    type Storage: CreatorScopedPubkyStorage + 'static;

    fn public_key_z32(&self) -> String;
    fn into_storage(self) -> Self::Storage;
    fn into_grant_storage(self) -> Self::Storage;
}

/// Imports restorable creator authority secrets into Pubky sessions.
#[async_trait]
pub trait PubkySessionImporter: Send + Sync {
    type Session: ImportedPubkySession;

    async fn import_legacy_cookie_session(
        &self,
        secret: &CreatorAuthoritySecret,
    ) -> Result<Self::Session, ApplicationError>;

    async fn import_grant_session(
        &self,
        secret: &CreatorAuthoritySecret,
    ) -> Result<Self::Session, ApplicationError>;
}

/// Creator-scoped storage provider backed by persisted legacy cookie creator authority.
#[derive(Debug)]
pub struct LegacyCookieCreatorScopedPubkyStorageProvider<S, I> {
    store: S,
    importer: I,
    accepted_auth_kind: CreatorAuthorityAuthKind,
}

impl<S, I> LegacyCookieCreatorScopedPubkyStorageProvider<S, I> {
    pub fn new(store: S, importer: I) -> Self {
        Self {
            store,
            importer,
            accepted_auth_kind: CreatorAuthorityAuthKind::LegacyCookie,
        }
    }

    pub fn new_grant(store: S, importer: I) -> Self {
        Self {
            store,
            importer,
            accepted_auth_kind: CreatorAuthorityAuthKind::Grant,
        }
    }

    pub fn importer(&self) -> &I {
        &self.importer
    }
}

#[async_trait]
impl<S, I> CreatorScopedPubkyStorageProvider for LegacyCookieCreatorScopedPubkyStorageProvider<S, I>
where
    S: CreatorAuthorityStore,
    I: PubkySessionImporter,
{
    async fn storage_for_creator(
        &self,
        creator: &CreatorPubky,
    ) -> Result<AcquiredCreatorScopedPubkyStorage, ApplicationError> {
        let record = self
            .store
            .get_creator_authority(creator)
            .await?
            .ok_or(ApplicationError::CreatorAuthorityUnavailable)?;

        if record.auth_kind != self.accepted_auth_kind {
            return Err(ApplicationError::CreatorAuthorityUnavailable);
        }

        let (session, grant_backed) = match record.auth_kind {
            CreatorAuthorityAuthKind::LegacyCookie => (
                self.importer
                    .import_legacy_cookie_session(&record.secret)
                    .await?,
                false,
            ),
            CreatorAuthorityAuthKind::Grant => (
                self.importer.import_grant_session(&record.secret).await?,
                true,
            ),
        };
        let restored_creator = creator_from_pubky_public_key_z32(&session.public_key_z32())?;
        if &restored_creator != creator {
            return Err(ApplicationError::CreatorAuthorityUnavailable);
        }

        let storage = if grant_backed {
            session.into_grant_storage()
        } else {
            session.into_storage()
        };
        Ok(AcquiredCreatorScopedPubkyStorage::uncached(Box::new(
            storage,
        )))
    }
}

#[derive(Clone)]
pub struct GrantCreatorScopedPubkyStorageProvider<S, I>
where
    I: PubkySessionImporter,
{
    cache: std::sync::Arc<GrantSessionCache<S, I>>,
}

impl<S, I> std::fmt::Debug for GrantCreatorScopedPubkyStorageProvider<S, I>
where
    I: PubkySessionImporter,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GrantCreatorScopedPubkyStorageProvider")
            .finish_non_exhaustive()
    }
}

impl<S, I> GrantCreatorScopedPubkyStorageProvider<S, I>
where
    I: PubkySessionImporter,
{
    pub fn new(cache: std::sync::Arc<GrantSessionCache<S, I>>) -> Self {
        Self { cache }
    }
}

#[async_trait]
impl<S, I> CreatorScopedPubkyStorageProvider for GrantCreatorScopedPubkyStorageProvider<S, I>
where
    S: CreatorAuthorityStore,
    I: PubkySessionImporter,
    I::Session: Clone,
{
    async fn storage_for_creator(
        &self,
        creator: &CreatorPubky,
    ) -> Result<AcquiredCreatorScopedPubkyStorage, ApplicationError> {
        let cached = self.cache.session_for_creator(creator).await?;
        Ok(AcquiredCreatorScopedPubkyStorage::cached(
            Box::new(cached.session.into_grant_storage()),
            cached.generation,
        ))
    }

    async fn invalidate_creator_session(&self, creator: &CreatorPubky, generation: u64) {
        self.cache.invalidate_generation(creator, generation).await;
    }
}

/// Real Pubky SDK importer for legacy cookie-session secrets.
#[derive(Debug, Clone)]
pub struct PubkyLegacyCookieSessionImporter {
    client: pubky::PubkyHttpClient,
    grant_pop_keys: Option<LockServerGrantPopKeys>,
    grant_client_id: Option<pubky::ClientId>,
    grant_required_scopes: Vec<String>,
}

impl PubkyLegacyCookieSessionImporter {
    pub fn new(client: pubky::PubkyHttpClient) -> Self {
        Self {
            client,
            grant_pop_keys: None,
            grant_client_id: None,
            grant_required_scopes: Vec::new(),
        }
    }

    pub fn with_grant_policy(
        mut self,
        grant_pop_keys: LockServerGrantPopKeys,
        client_id: pubky::ClientId,
        required_scopes: Vec<String>,
    ) -> Self {
        self.grant_pop_keys = Some(grant_pop_keys);
        self.grant_client_id = Some(client_id);
        self.grant_required_scopes = required_scopes;
        self
    }
}

#[async_trait]
#[allow(
    deprecated,
    reason = "legacy cookie session restore remains current creator authority contract"
)]
impl PubkySessionImporter for PubkyLegacyCookieSessionImporter {
    type Session = PubkyImportedSession;

    async fn import_legacy_cookie_session(
        &self,
        secret: &CreatorAuthoritySecret,
    ) -> Result<Self::Session, ApplicationError> {
        pubky::PubkySession::import_secret(secret.expose_secret(), Some(self.client.clone()))
            .await
            .map(PubkyImportedSession)
            .map_err(|_| ApplicationError::CreatorAuthoritySecret {
                message: "failed to restore legacy creator authority secret".to_owned(),
            })
    }

    async fn import_grant_session(
        &self,
        secret: &CreatorAuthoritySecret,
    ) -> Result<Self::Session, ApplicationError> {
        let keys = self
            .grant_pop_keys
            .as_ref()
            .ok_or(ApplicationError::CreatorAuthorityUnavailable)?;
        let client_id = self
            .grant_client_id
            .as_ref()
            .ok_or(ApplicationError::CreatorAuthorityUnavailable)?;
        restore_grant_session(
            &self.client,
            keys,
            client_id,
            &self.grant_required_scopes,
            secret,
        )
        .await
        .map(PubkyImportedSession)
    }
}

/// Imported real Pubky SDK session.
#[derive(Clone)]
pub struct PubkyImportedSession(pubky::PubkySession);

impl std::fmt::Debug for PubkyImportedSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("PubkyImportedSession")
            .field(&"<redacted>")
            .finish()
    }
}

impl ImportedPubkySession for PubkyImportedSession {
    type Storage = SdkCreatorScopedPubkyStorage;

    fn public_key_z32(&self) -> String {
        self.0.info().public_key().z32()
    }

    fn into_storage(self) -> Self::Storage {
        SdkCreatorScopedPubkyStorage::new(self.0.storage())
    }

    fn into_grant_storage(self) -> Self::Storage {
        SdkCreatorScopedPubkyStorage::new_grant(self.0.storage())
    }
}

/// Creator-scoped storage wrapper around the Pubky SDK session storage API.
#[derive(Debug, Clone)]
pub struct SdkCreatorScopedPubkyStorage {
    storage: pubky::SessionStorage,
    grant_backed: bool,
}

impl SdkCreatorScopedPubkyStorage {
    pub fn new(storage: pubky::SessionStorage) -> Self {
        Self {
            storage,
            grant_backed: false,
        }
    }

    fn new_grant(storage: pubky::SessionStorage) -> Self {
        Self {
            storage,
            grant_backed: true,
        }
    }

    fn map_error(
        &self,
        operation: &'static str,
        path: &str,
        error: pubky::Error,
    ) -> ApplicationError {
        if self.grant_backed {
            pubky_storage_error_for_grant(operation, path, error)
        } else {
            pubky_storage_error(operation, path, error)
        }
    }
}

#[async_trait]
impl CreatorScopedPubkyStorage for SdkCreatorScopedPubkyStorage {
    async fn put_json_value(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> Result<(), ApplicationError> {
        self.storage
            .put_json(path, &body)
            .await
            .map(|_| ())
            .map_err(|error| self.map_error("put", path, error))
    }

    async fn put_json_value_classified(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> Result<(), ContentLockWriteError> {
        self.storage
            .put_json(path, &body)
            .await
            .map(|_| ())
            .map_err(|error| {
                let not_started = pubky_put_failure_is_not_started(&error);
                let error = self.map_error("put", path, error);
                if not_started {
                    ContentLockWriteError::NotStarted(error)
                } else {
                    ContentLockWriteError::Ambiguous(error)
                }
            })
    }

    async fn get_json_value(
        &self,
        path: &str,
    ) -> Result<Option<serde_json::Value>, ApplicationError> {
        if !self
            .storage
            .exists(path)
            .await
            .map_err(|error| self.map_error("head", path, error))?
        {
            return Ok(None);
        }

        self.storage
            .get_json(path)
            .await
            .map(Some)
            .map_err(|error| self.map_error("get", path, error))
    }

    async fn put_bytes(
        &self,
        path: &str,
        bytes: Vec<u8>,
        _content_type: &str,
    ) -> Result<(), ApplicationError> {
        self.storage
            .put(path, bytes)
            .await
            .map(|_| ())
            .map_err(|error| self.map_error("put", path, error))
    }

    async fn get_bytes(&self, path: &str) -> Result<Option<PubkyBytesResource>, ApplicationError> {
        if !self
            .storage
            .exists(path)
            .await
            .map_err(|error| self.map_error("head", path, error))?
        {
            return Ok(None);
        }

        let response = self
            .storage
            .get(path)
            .await
            .map_err(|error| self.map_error("get", path, error))?;
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let bytes = response
            .bytes()
            .await
            .map_err(|error| self.map_error("get", path, error.into()))?
            .to_vec();
        Ok(Some(PubkyBytesResource {
            bytes,
            content_type,
        }))
    }

    async fn get_metadata(
        &self,
        path: &str,
    ) -> Result<Option<PubkyResourceMetadata>, ApplicationError> {
        self.storage
            .stats(path)
            .await
            .map(|stats| {
                stats.map(|stats| PubkyResourceMetadata {
                    content_length: stats.content_length,
                    content_type: stats.content_type,
                    content_hash: stats.etag.as_deref().and_then(decode_pubky_etag),
                })
            })
            .map_err(|error| self.map_error("head", path, error))
    }

    async fn delete(&self, path: &str) -> Result<(), ApplicationError> {
        self.storage
            .delete(path)
            .await
            .map(|_| ())
            .map_err(|error| self.map_error("delete", path, error))
    }

    async fn delete_if_present(&self, path: &str) -> Result<PubkyDeleteOutcome, ApplicationError> {
        match self.storage.delete(path).await {
            Ok(_) => Ok(PubkyDeleteOutcome::Deleted),
            Err(error) if is_pubky_not_found(&error) => Ok(PubkyDeleteOutcome::NotFound),
            Err(error) => Err(self.map_error("delete", path, error)),
        }
    }
}

/// Pubky homeserver storage client backed by a creator-scoped storage provider.
#[derive(Debug)]
pub struct ProviderBackedPubkyHomeserverStorageClient<P> {
    provider: P,
}

impl<P> ProviderBackedPubkyHomeserverStorageClient<P> {
    pub fn new(provider: P) -> Self {
        Self { provider }
    }

    pub fn provider(&self) -> &P {
        &self.provider
    }
}

impl<P> ProviderBackedPubkyHomeserverStorageClient<P>
where
    P: CreatorScopedPubkyStorageProvider,
{
    async fn finish_operation<T>(
        &self,
        creator: &CreatorPubky,
        generation: Option<u64>,
        result: Result<T, ApplicationError>,
    ) -> Result<T, ApplicationError> {
        if matches!(result, Err(ApplicationError::CreatorAuthorityUnavailable))
            && let Some(generation) = generation
        {
            self.provider
                .invalidate_creator_session(creator, generation)
                .await;
        }
        result
    }

    async fn read_with_single_retry<T, F>(
        &self,
        creator: &CreatorPubky,
        path: &str,
        read: F,
    ) -> Result<T, ApplicationError>
    where
        F: for<'a> Fn(
            &'a dyn CreatorScopedPubkyStorage,
            &'a str,
        )
            -> Pin<Box<dyn Future<Output = Result<T, ApplicationError>> + Send + 'a>>,
    {
        let acquired = self.provider.storage_for_creator(creator).await?;
        let generation = acquired.generation;
        let result = read(acquired.storage.as_ref(), path).await;
        let retry = generation.is_some()
            && matches!(result, Err(ApplicationError::CreatorAuthorityUnavailable));
        let result = self.finish_operation(creator, generation, result).await;
        if !retry {
            return result;
        }

        // Read replay is bounded to one reacquisition. Until pubky/pubky#698
        // adds typed provenance, this can also replay a grant-backed storage
        // transport/5xx failure. The second result still invalidates only the
        // generation that produced it.
        let acquired = self.provider.storage_for_creator(creator).await?;
        let result = read(acquired.storage.as_ref(), path).await;
        self.finish_operation(creator, acquired.generation, result)
            .await
    }
}

#[async_trait]
impl<P> PubkyHomeserverStorageClient for ProviderBackedPubkyHomeserverStorageClient<P>
where
    P: CreatorScopedPubkyStorageProvider,
{
    async fn put_json_value_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
        body: serde_json::Value,
    ) -> Result<(), ApplicationError> {
        let acquired = self.provider.storage_for_creator(creator).await?;
        let result = acquired.storage.put_json_value(path, body).await;
        self.finish_operation(creator, acquired.generation, result)
            .await
    }

    async fn put_json_value_as_creator_classified(
        &self,
        creator: &CreatorPubky,
        path: &str,
        body: serde_json::Value,
    ) -> Result<(), ContentLockWriteError> {
        let acquired = self
            .provider
            .storage_for_creator(creator)
            .await
            .map_err(ContentLockWriteError::NotStarted)?;
        let generation = acquired.generation;
        let result = acquired
            .storage
            .put_json_value_classified(path, body.clone())
            .await;
        let retry = generation.is_some()
            && matches!(
                &result,
                Err(ContentLockWriteError::NotStarted(
                    ApplicationError::CreatorAuthorityUnavailable
                ))
            );
        if let Some(generation) = generation
            && content_lock_write_requires_session_invalidation(&result)
        {
            self.provider
                .invalidate_creator_session(creator, generation)
                .await;
        }
        if !retry {
            return result;
        }

        let acquired = self
            .provider
            .storage_for_creator(creator)
            .await
            .map_err(ContentLockWriteError::NotStarted)?;
        let result = acquired.storage.put_json_value_classified(path, body).await;
        if let Some(generation) = acquired.generation
            && content_lock_write_requires_session_invalidation(&result)
        {
            self.provider
                .invalidate_creator_session(creator, generation)
                .await;
        }
        result
    }

    async fn get_json_value_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<Option<serde_json::Value>, ApplicationError> {
        self.read_with_single_retry(creator, path, |storage, path| {
            Box::pin(storage.get_json_value(path))
        })
        .await
    }

    async fn put_bytes_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
        bytes: Vec<u8>,
        content_type: &str,
    ) -> Result<(), ApplicationError> {
        let acquired = self.provider.storage_for_creator(creator).await?;
        let result = acquired.storage.put_bytes(path, bytes, content_type).await;
        self.finish_operation(creator, acquired.generation, result)
            .await
    }

    async fn get_bytes_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<Option<PubkyBytesResource>, ApplicationError> {
        self.read_with_single_retry(creator, path, |storage, path| {
            Box::pin(storage.get_bytes(path))
        })
        .await
    }

    async fn get_metadata_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<Option<PubkyResourceMetadata>, ApplicationError> {
        self.read_with_single_retry(creator, path, |storage, path| {
            Box::pin(storage.get_metadata(path))
        })
        .await
    }

    async fn delete_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<(), ApplicationError> {
        let acquired = self.provider.storage_for_creator(creator).await?;
        let result = acquired.storage.delete(path).await;
        self.finish_operation(creator, acquired.generation, result)
            .await
    }

    async fn delete_if_present_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<PubkyDeleteOutcome, ApplicationError> {
        let acquired = self.provider.storage_for_creator(creator).await?;
        let result = acquired.storage.delete_if_present(path).await;
        self.finish_operation(creator, acquired.generation, result)
            .await
    }
}

/// Storage-client decorator that requires creator authority before every operation.
#[derive(Debug)]
pub struct AuthorizingPubkyHomeserverStorageClient<C, M> {
    inner: C,
    manager: M,
}

impl<C, M> AuthorizingPubkyHomeserverStorageClient<C, M> {
    /// Creates an authorizing storage client from an inner storage client and authority manager.
    pub fn new(inner: C, manager: M) -> Self {
        Self { inner, manager }
    }

    /// Returns the wrapped storage client. Exposed for tests and composition.
    pub fn inner(&self) -> &C {
        &self.inner
    }

    /// Returns the authority manager. Exposed for tests and composition.
    pub fn manager(&self) -> &M {
        &self.manager
    }
}

#[async_trait]
impl<C, M> PubkyHomeserverStorageClient for AuthorizingPubkyHomeserverStorageClient<C, M>
where
    C: PubkyHomeserverStorageClient,
    M: CreatorAuthorityManager,
{
    async fn put_json_value_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
        body: serde_json::Value,
    ) -> Result<(), ApplicationError> {
        self.manager.require_creator_authority(creator).await?;
        self.inner
            .put_json_value_as_creator(creator, path, body)
            .await
    }

    async fn put_json_value_as_creator_classified(
        &self,
        creator: &CreatorPubky,
        path: &str,
        body: serde_json::Value,
    ) -> Result<(), ContentLockWriteError> {
        self.manager
            .require_creator_authority(creator)
            .await
            .map_err(ContentLockWriteError::NotStarted)?;
        self.inner
            .put_json_value_as_creator_classified(creator, path, body)
            .await
    }

    async fn get_json_value_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<Option<serde_json::Value>, ApplicationError> {
        self.manager.require_creator_authority(creator).await?;
        self.inner.get_json_value_as_creator(creator, path).await
    }

    async fn put_bytes_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
        bytes: Vec<u8>,
        content_type: &str,
    ) -> Result<(), ApplicationError> {
        self.manager.require_creator_authority(creator).await?;
        self.inner
            .put_bytes_as_creator(creator, path, bytes, content_type)
            .await
    }

    async fn get_bytes_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<Option<PubkyBytesResource>, ApplicationError> {
        self.manager.require_creator_authority(creator).await?;
        self.inner.get_bytes_as_creator(creator, path).await
    }

    async fn get_metadata_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<Option<PubkyResourceMetadata>, ApplicationError> {
        self.manager.require_creator_authority(creator).await?;
        self.inner.get_metadata_as_creator(creator, path).await
    }

    async fn delete_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<(), ApplicationError> {
        self.manager.require_creator_authority(creator).await?;
        self.inner.delete_as_creator(creator, path).await
    }

    async fn delete_if_present_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<PubkyDeleteOutcome, ApplicationError> {
        self.manager.require_creator_authority(creator).await?;
        self.inner.delete_if_present_as_creator(creator, path).await
    }
}

fn content_lock_write_requires_session_invalidation(
    result: &Result<(), ContentLockWriteError>,
) -> bool {
    matches!(
        result,
        Err(
            ContentLockWriteError::NotStarted(ApplicationError::CreatorAuthorityUnavailable)
                | ContentLockWriteError::Ambiguous(ApplicationError::CreatorAuthorityUnavailable)
        )
    )
}

fn pubky_put_failure_is_not_started(error: &pubky::Error) -> bool {
    matches!(error, pubky::Error::Authentication(_))
        || matches!(
            error,
            pubky::Error::Request(pubky::errors::RequestError::Server { status, .. })
                if matches!(
                    *status,
                    pubky::StatusCode::UNAUTHORIZED | pubky::StatusCode::FORBIDDEN
                )
        )
        || matches!(
            error,
            pubky::Error::Request(pubky::errors::RequestError::Transport(error))
                if error.is_connect()
        )
}

fn is_pubky_not_found(error: &pubky::Error) -> bool {
    matches!(
        error,
        pubky::Error::Request(pubky::errors::RequestError::Server { status, .. })
            if *status == pubky::StatusCode::NOT_FOUND
    )
}

fn decode_pubky_etag(etag: &str) -> Option<[u8; 32]> {
    base64::engine::general_purpose::STANDARD
        .decode(etag)
        .ok()?
        .try_into()
        .ok()
}

/// Maps Pubky storage failures to a secret-safe application storage error.
pub fn pubky_storage_error(
    operation: &'static str,
    path: &str,
    error: impl std::fmt::Display + 'static,
) -> ApplicationError {
    map_pubky_storage_error(operation, path, error, false)
}

fn pubky_storage_error_for_grant(
    operation: &'static str,
    path: &str,
    error: impl std::fmt::Display + 'static,
) -> ApplicationError {
    map_pubky_storage_error(operation, path, error, true)
}

fn map_pubky_storage_error(
    operation: &'static str,
    path: &str,
    error: impl std::fmt::Display + 'static,
    grant_backed: bool,
) -> ApplicationError {
    let error = &error as &dyn std::any::Any;
    let pubky_error = error.downcast_ref::<pubky::Error>();
    let unauthorized = pubky_error.is_some_and(|error| {
        matches!(error, pubky::Error::Authentication(_))
            || matches!(
                error,
                pubky::Error::Request(pubky::errors::RequestError::Server { status, .. })
                    if matches!(
                        *status,
                        pubky::StatusCode::UNAUTHORIZED | pubky::StatusCode::FORBIDDEN
                    )
            )
    });
    if unauthorized {
        tracing::warn!(
            stage = "storage_operation",
            operation,
            error_class = "unauthorized",
            "creator Pubky storage operation failed"
        );
        return ApplicationError::CreatorAuthorityUnavailable;
    }
    let transient_dependency = pubky_error.is_some_and(|error| {
        matches!(
            error,
            pubky::Error::Request(pubky::errors::RequestError::Transport(error))
                if error.is_timeout()
                    || error.is_connect()
                    || error.is_request()
                    || error.is_body()
        ) || matches!(
            error,
            pubky::Error::Request(pubky::errors::RequestError::Server { status, .. })
                if status.is_server_error()
        )
    });
    // Pinned Pubky SDK maps grant-refresh response-body failures, including
    // timeouts after successful headers, to this operation-specific DecodeJson
    // message. Keep the temporary 503 classification limited to that refresh
    // operation rather than widening every JSON decode failure.
    let grant_refresh_response_failure = pubky_error.is_some_and(|error| {
        matches!(
            error,
            pubky::Error::Request(pubky::errors::RequestError::DecodeJson { message })
                if message.starts_with("decoding /auth/grant/session response:")
        )
    });
    if grant_backed && (transient_dependency || grant_refresh_response_failure) {
        tracing::warn!(
            stage = "grant_backed_storage",
            operation,
            error_class = "dependency_unavailable",
            "creator Pubky storage operation failed"
        );
        return ApplicationError::CreatorAuthorityUnavailable;
    }
    tracing::warn!(
        stage = "storage_operation",
        operation,
        error_class = "dependency",
        "creator Pubky storage operation failed"
    );
    let message = if is_private_locks_path(path) {
        format!("Pubky homeserver {operation} failed for private Locks path")
    } else {
        format!("Pubky homeserver {operation} failed")
    };
    ApplicationError::Storage { message }
}

fn is_private_locks_path(path: &str) -> bool {
    path.starts_with(PRIVATE_RESOURCE_CONTENT_PATH_PREFIX)
        || path.starts_with(PRIVATE_PROOF_BUNDLE_PATH_PREFIX)
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use async_trait::async_trait;
    use locks_core::ids::CreatorPubky;
    use serde_json::json;
    use time::macros::datetime;
    use tokio::io::AsyncWriteExt;

    use super::{
        AcquiredCreatorScopedPubkyStorage, AuthorizingPubkyHomeserverStorageClient,
        CreatorScopedPubkyStorage, CreatorScopedPubkyStorageProvider, ImportedPubkySession,
        LegacyCookieCreatorScopedPubkyStorageProvider, ProviderBackedPubkyHomeserverStorageClient,
        PubkyBytesResource, PubkyHomeserverStorageClient, PubkyResourceMetadata,
        PubkySessionImporter, is_pubky_not_found, pubky_put_failure_is_not_started,
        pubky_storage_error, pubky_storage_error_for_grant,
    };
    use crate::application::errors::ApplicationError;
    use crate::application::models::{
        CreatorAuthorityAuthKind, CreatorAuthorityRecord, CreatorAuthoritySecret,
    };
    use crate::application::ports::{
        ContentLockWriteError, CreatorAuthorityManager, CreatorAuthorityStatus,
        CreatorAuthorityStore,
    };

    #[tokio::test]
    async fn get_json_missing_resource_returns_none_after_creator_authority_check() {
        let manager = FakeCreatorAuthorityManager::authorized();
        let inner = FakeStorageClient::new().with_json_read(None);
        let client = AuthorizingPubkyHomeserverStorageClient::new(inner, manager);

        let result = client
            .get_json_value_as_creator(&creator(), "/pub/app.locks/config.json")
            .await
            .unwrap();

        assert_eq!(result, None);
        assert_eq!(client.manager().seen_creators(), vec![creator()]);
        assert_eq!(
            client.inner().operations(),
            vec![format!("get_json {} /pub/app.locks/config.json", creator())]
        );
    }

    #[tokio::test]
    async fn missing_creator_authority_returns_unavailable_before_storage_operation() {
        let manager = FakeCreatorAuthorityManager::unavailable();
        let inner = FakeStorageClient::new().with_json_read(Some(json!({"version": 1})));
        let client = AuthorizingPubkyHomeserverStorageClient::new(inner, manager);

        let result = client
            .get_json_value_as_creator(&creator(), "/pub/app.locks/config.json")
            .await;

        assert_eq!(result, Err(ApplicationError::CreatorAuthorityUnavailable));
        assert_eq!(client.inner().operations(), Vec::<String>::new());
    }

    #[test]
    fn pubky_storage_error_redacts_private_locks_paths() {
        let error = pubky_storage_error(
            "get",
            "/priv/app.locks/content/secret.txt",
            "network timeout",
        );

        let debug = format!("{error:?}");
        assert!(debug.contains("Pubky homeserver get failed for private Locks path"));
        assert!(!debug.contains("secret.txt"));
        assert!(!debug.contains("/priv/app.locks/content/secret.txt"));
        assert!(!debug.contains("network timeout"));
    }

    #[test]
    fn pubky_storage_401_and_403_are_authority_unavailable_without_upstream_body() {
        for status in [
            pubky::StatusCode::UNAUTHORIZED,
            pubky::StatusCode::FORBIDDEN,
        ] {
            let error = pubky_storage_error(
                "get",
                "/priv/app.locks/content/secret.txt",
                pubky::Error::Request(pubky::errors::RequestError::Server {
                    status,
                    message: "raw secret-bearing upstream body".to_owned(),
                }),
            );

            assert_eq!(error, ApplicationError::CreatorAuthorityUnavailable);
            assert!(!format!("{error:?}").contains("raw secret-bearing upstream body"));
        }
    }

    #[test]
    fn delete_not_found_detection_uses_only_typed_404_server_errors() {
        let server_error = |status| {
            pubky::Error::Request(pubky::errors::RequestError::Server {
                status,
                message: "upstream body".to_owned(),
            })
        };

        assert!(is_pubky_not_found(&server_error(
            pubky::StatusCode::NOT_FOUND
        )));
        assert!(!is_pubky_not_found(&server_error(
            pubky::StatusCode::INTERNAL_SERVER_ERROR
        )));
        assert!(!is_pubky_not_found(&pubky::Error::Request(
            pubky::errors::RequestError::DecodeJson {
                message: "not found".to_owned(),
            }
        )));
    }

    #[test]
    fn grant_backed_storage_5xx_is_authority_unavailable_but_legacy_stays_storage_error() {
        let error = || {
            pubky::Error::Request(pubky::errors::RequestError::Server {
                status: pubky::StatusCode::SERVICE_UNAVAILABLE,
                message: "raw secret-bearing upstream body".to_owned(),
            })
        };

        assert_eq!(
            pubky_storage_error_for_grant("get", "/pub/app.locks/config.json", error()),
            ApplicationError::CreatorAuthorityUnavailable
        );
        assert!(matches!(
            pubky_storage_error("get", "/pub/app.locks/config.json", error()),
            ApplicationError::Storage { .. }
        ));

        let not_found = pubky::Error::Request(pubky::errors::RequestError::Server {
            status: pubky::StatusCode::NOT_FOUND,
            message: "not found".to_owned(),
        });
        assert!(matches!(
            pubky_storage_error_for_grant("get", "/pub/app.locks/config.json", not_found),
            ApplicationError::Storage { .. }
        ));
    }

    #[tokio::test]
    async fn grant_backed_storage_timeout_is_authority_unavailable_but_legacy_stays_storage_error()
    {
        async fn timeout_error() -> pubky::Error {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (_socket, _) = listener.accept().await.unwrap();
                std::future::pending::<()>().await;
            });
            let error = reqwest::Client::builder()
                .timeout(Duration::from_millis(10))
                .build()
                .unwrap()
                .get(format!("http://{address}"))
                .send()
                .await
                .unwrap_err();
            server.abort();
            assert!(error.is_timeout());
            pubky::Error::Request(pubky::errors::RequestError::Transport(error))
        }

        assert_eq!(
            pubky_storage_error_for_grant(
                "put",
                "/priv/app.locks/content/example.txt",
                timeout_error().await,
            ),
            ApplicationError::CreatorAuthorityUnavailable
        );
        assert!(matches!(
            pubky_storage_error(
                "put",
                "/priv/app.locks/content/example.txt",
                timeout_error().await,
            ),
            ApplicationError::Storage { .. }
        ));
    }

    #[tokio::test]
    async fn grant_refresh_body_timeout_is_unavailable_but_storage_decode_failure_stays_storage() {
        async fn refresh_body_timeout_message() -> String {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                socket
                    .write_all(
                        b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 64\r\n\r\n{\"token\":",
                    )
                    .await
                    .unwrap();
                std::future::pending::<()>().await;
            });
            let response = reqwest::Client::builder()
                .timeout(Duration::from_millis(100))
                .build()
                .unwrap()
                .get(format!("http://{address}/auth/grant/session"))
                .send()
                .await
                .unwrap();
            let error = response.json::<serde_json::Value>().await.unwrap_err();
            server.abort();
            assert!(error.is_timeout());
            format!("decoding /auth/grant/session response: {error}")
        }

        let timeout_message = refresh_body_timeout_message().await;
        let refresh_timeout = pubky::Error::Request(pubky::errors::RequestError::DecodeJson {
            message: timeout_message.clone(),
        });
        assert_eq!(
            pubky_storage_error_for_grant("get", "/pub/app.locks/config.json", refresh_timeout,),
            ApplicationError::CreatorAuthorityUnavailable
        );
        let legacy_refresh_timeout =
            pubky::Error::Request(pubky::errors::RequestError::DecodeJson {
                message: timeout_message,
            });
        assert!(matches!(
            pubky_storage_error("get", "/pub/app.locks/config.json", legacy_refresh_timeout,),
            ApplicationError::Storage { .. }
        ));

        let storage_decode = pubky::Error::Request(pubky::errors::RequestError::DecodeJson {
            message: "expected value at line 1 column 1".to_owned(),
        });
        assert!(matches!(
            pubky_storage_error_for_grant("get", "/pub/app.locks/config.json", storage_decode,),
            ApplicationError::Storage { .. }
        ));
    }

    #[tokio::test]
    async fn grant_backed_malformed_stored_json_is_storage_error_without_session_eviction() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for body in ["not-json", r#"{"version":1}"#] {
                let (mut socket, _) = listener.accept().await.unwrap();
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        let provider = DecodeThenSuccessProvider::new(format!("http://{address}/stored.json"));
        let client = ProviderBackedPubkyHomeserverStorageClient::new(provider.clone());

        let first = client
            .get_json_value_as_creator(&creator(), "/pub/app.locks/config.json")
            .await;
        assert!(matches!(first, Err(ApplicationError::Storage { .. })));
        assert_eq!(provider.invalidations(), 0);

        assert_eq!(
            client
                .get_json_value_as_creator(&creator(), "/pub/app.locks/config.json")
                .await
                .unwrap(),
            Some(json!({"version": 1}))
        );
        assert_eq!(provider.invalidations(), 0);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn grant_backed_unknown_reqwest_transport_error_fails_closed() {
        let error = reqwest::Client::new()
            .get("://invalid-url")
            .send()
            .await
            .unwrap_err();
        assert!(error.is_builder());
        assert!(!error.is_timeout());
        assert!(!error.is_decode());

        assert!(matches!(
            pubky_storage_error_for_grant(
                "get",
                "/pub/app.locks/config.json",
                pubky::Error::Request(pubky::errors::RequestError::Transport(error)),
            ),
            ApplicationError::Storage { .. }
        ));
    }

    #[tokio::test]
    async fn provider_backed_client_asks_provider_for_creator_scoped_storage_before_each_operation()
    {
        let provider = FakeCreatorScopedPubkyStorageProvider::authorized();
        let client = ProviderBackedPubkyHomeserverStorageClient::new(provider.clone());

        client
            .put_json_value_as_creator(
                &creator(),
                "/pub/app.locks/config.json",
                json!({"version": 1}),
            )
            .await
            .unwrap();
        let bytes = client
            .get_bytes_as_creator(&creator(), "/priv/app.locks/content/example.txt")
            .await
            .unwrap();
        client
            .delete_as_creator(&creator(), "/priv/app.locks/proofs/example.json")
            .await
            .unwrap();

        assert_eq!(
            bytes,
            Some(PubkyBytesResource {
                bytes: b"guarded".to_vec(),
                content_type: Some("text/plain".to_owned())
            })
        );
        assert_eq!(
            provider.seen_creators(),
            vec![creator(), creator(), creator()]
        );
        assert_eq!(
            provider.storage_operations(),
            vec![
                "put_json /pub/app.locks/config.json".to_owned(),
                "get_bytes /priv/app.locks/content/example.txt".to_owned(),
                "delete /priv/app.locks/proofs/example.json".to_owned(),
            ]
        );
    }

    #[tokio::test]
    async fn provider_backed_client_returns_provider_failure_before_storage_operation() {
        let provider = FakeCreatorScopedPubkyStorageProvider::unavailable();
        let client = ProviderBackedPubkyHomeserverStorageClient::new(provider.clone());

        let result = client
            .put_bytes_as_creator(
                &creator(),
                "/priv/app.locks/content/example.txt",
                b"secret".to_vec(),
                "text/plain",
            )
            .await;

        assert_eq!(result, Err(ApplicationError::CreatorAuthorityUnavailable));
        assert_eq!(provider.seen_creators(), vec![creator()]);
        assert_eq!(provider.storage_operations(), Vec::<String>::new());
    }

    #[tokio::test]
    async fn classified_content_lock_write_distinguishes_pre_dispatch_provider_failure() {
        let provider = FakeCreatorScopedPubkyStorageProvider::unavailable();
        let client = ProviderBackedPubkyHomeserverStorageClient::new(provider.clone());

        let result = client
            .put_json_value_as_creator_classified(
                &creator(),
                "/pub/app.locks/example.json",
                json!({"version": 1}),
            )
            .await;

        assert_eq!(
            result,
            Err(ContentLockWriteError::NotStarted(
                ApplicationError::CreatorAuthorityUnavailable
            ))
        );
        assert_eq!(provider.seen_creators(), vec![creator()]);
        assert_eq!(provider.storage_operations(), Vec::<String>::new());
    }

    #[tokio::test]
    async fn classified_content_lock_write_keeps_post_dispatch_failure_ambiguous() {
        let provider = RevokedSessionProvider::default();
        let client = ProviderBackedPubkyHomeserverStorageClient::new(provider.clone());

        let result = client
            .put_json_value_as_creator_classified(
                &creator(),
                "/pub/app.locks/example.json",
                json!({"version": 1}),
            )
            .await;

        assert_eq!(
            result,
            Err(ContentLockWriteError::Ambiguous(
                ApplicationError::CreatorAuthorityUnavailable
            ))
        );
        assert_eq!(*provider.operations.lock().unwrap(), 1);
        assert_eq!(
            *provider.invalidations.lock().unwrap(),
            vec![(creator(), 7)]
        );
    }

    #[tokio::test]
    async fn classified_content_lock_write_retries_stale_cached_session_once() {
        let provider =
            RetryTestProvider::cached([Err(ApplicationError::CreatorAuthorityUnavailable), Ok(())]);
        let client = ProviderBackedPubkyHomeserverStorageClient::new(provider.clone());

        client
            .put_json_value_as_creator_classified(
                &creator(),
                "/pub/app.locks/example.json",
                json!({"version": 1}),
            )
            .await
            .unwrap();

        provider.assert_counts(2, 2, &[1]);
    }

    #[test]
    fn classified_put_only_marks_typed_auth_rejections_as_not_started() {
        let server_error = |status| {
            pubky::Error::Request(pubky::errors::RequestError::Server {
                status,
                message: "upstream body".to_owned(),
            })
        };

        assert!(pubky_put_failure_is_not_started(&server_error(
            pubky::StatusCode::UNAUTHORIZED
        )));
        assert!(pubky_put_failure_is_not_started(&server_error(
            pubky::StatusCode::FORBIDDEN
        )));
        assert!(!pubky_put_failure_is_not_started(&server_error(
            pubky::StatusCode::INTERNAL_SERVER_ERROR
        )));
        assert!(!pubky_put_failure_is_not_started(&server_error(
            pubky::StatusCode::REQUEST_TIMEOUT
        )));
    }

    #[tokio::test]
    async fn classified_put_separates_connect_from_ambiguous_transport_timeout() {
        let unused_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let unused_address = unused_listener.local_addr().unwrap();
        drop(unused_listener);
        let connect = reqwest::Client::new()
            .put(format!("http://{unused_address}/lock"))
            .send()
            .await
            .unwrap_err();
        assert!(connect.is_connect());
        assert!(pubky_put_failure_is_not_started(&pubky::Error::Request(
            pubky::errors::RequestError::Transport(connect)
        )));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        let timeout = reqwest::Client::builder()
            .timeout(Duration::from_millis(20))
            .build()
            .unwrap()
            .put(format!("http://{address}/lock"))
            .body("payload")
            .send()
            .await
            .unwrap_err();
        assert!(timeout.is_timeout());
        assert!(!pubky_put_failure_is_not_started(&pubky::Error::Request(
            pubky::errors::RequestError::Transport(timeout)
        )));
        server.abort();
    }

    #[tokio::test]
    async fn revoked_cached_session_generation_is_invalidated_on_every_storage_path() {
        let provider = RevokedSessionProvider::default();
        let client = ProviderBackedPubkyHomeserverStorageClient::new(provider.clone());

        assert_eq!(
            client
                .put_json_value_as_creator(&creator(), "/pub/app.locks/config.json", json!({}))
                .await,
            Err(ApplicationError::CreatorAuthorityUnavailable)
        );
        assert_eq!(
            client
                .get_json_value_as_creator(&creator(), "/pub/app.locks/config.json")
                .await,
            Err(ApplicationError::CreatorAuthorityUnavailable)
        );
        assert_eq!(
            client
                .put_bytes_as_creator(
                    &creator(),
                    "/priv/app.locks/content/example.txt",
                    Vec::new(),
                    "text/plain",
                )
                .await,
            Err(ApplicationError::CreatorAuthorityUnavailable)
        );
        assert_eq!(
            client
                .get_bytes_as_creator(&creator(), "/priv/app.locks/content/example.txt")
                .await,
            Err(ApplicationError::CreatorAuthorityUnavailable)
        );
        assert_eq!(
            client
                .get_metadata_as_creator(&creator(), "/priv/app.locks/content/example.txt")
                .await,
            Err(ApplicationError::CreatorAuthorityUnavailable)
        );
        assert_eq!(
            client
                .delete_as_creator(&creator(), "/priv/app.locks/content/example.txt")
                .await,
            Err(ApplicationError::CreatorAuthorityUnavailable)
        );
        assert_eq!(*provider.operations.lock().unwrap(), 9);
        assert_eq!(
            *provider.invalidations.lock().unwrap(),
            vec![(creator(), 7); 9]
        );
    }

    #[tokio::test]
    async fn cached_grant_reads_retry_once_with_reacquired_storage() {
        let json_provider =
            RetryTestProvider::cached([Err(ApplicationError::CreatorAuthorityUnavailable), Ok(())]);
        let json_client = ProviderBackedPubkyHomeserverStorageClient::new(json_provider.clone());
        assert_eq!(
            json_client
                .get_json_value_as_creator(&creator(), "/pub/app.locks/config.json")
                .await
                .unwrap(),
            Some(json!({"version": 1}))
        );
        json_provider.assert_counts(2, 2, &[1]);

        let bytes_provider =
            RetryTestProvider::cached([Err(ApplicationError::CreatorAuthorityUnavailable), Ok(())]);
        let bytes_client = ProviderBackedPubkyHomeserverStorageClient::new(bytes_provider.clone());
        assert_eq!(
            bytes_client
                .get_bytes_as_creator(&creator(), "/priv/app.locks/content/example.txt")
                .await
                .unwrap()
                .unwrap()
                .bytes,
            b"restored"
        );
        bytes_provider.assert_counts(2, 2, &[1]);

        let metadata_provider =
            RetryTestProvider::cached([Err(ApplicationError::CreatorAuthorityUnavailable), Ok(())]);
        let metadata_client =
            ProviderBackedPubkyHomeserverStorageClient::new(metadata_provider.clone());
        assert_eq!(
            metadata_client
                .get_metadata_as_creator(&creator(), "/priv/app.locks/content/example.txt")
                .await
                .unwrap()
                .unwrap()
                .content_length,
            Some(8)
        );
        metadata_provider.assert_counts(2, 2, &[1]);
    }

    #[tokio::test]
    async fn cached_grant_read_retries_at_most_once_and_invalidates_each_failed_generation() {
        let provider = RetryTestProvider::cached([
            Err(ApplicationError::CreatorAuthorityUnavailable),
            Err(ApplicationError::CreatorAuthorityUnavailable),
            Ok(()),
        ]);
        let client = ProviderBackedPubkyHomeserverStorageClient::new(provider.clone());

        assert_eq!(
            client
                .get_json_value_as_creator(&creator(), "/pub/app.locks/config.json")
                .await,
            Err(ApplicationError::CreatorAuthorityUnavailable)
        );
        provider.assert_counts(2, 2, &[1, 2]);
    }

    #[tokio::test]
    async fn read_retry_excludes_non_authority_errors_legacy_storage_and_reacquisition_failures() {
        let acquisition_provider = RetryTestProvider::cached([Ok(())]);
        acquisition_provider.fail_acquisition(1);
        let acquisition_client =
            ProviderBackedPubkyHomeserverStorageClient::new(acquisition_provider.clone());
        assert_eq!(
            acquisition_client
                .get_json_value_as_creator(&creator(), "/pub/app.locks/config.json")
                .await,
            Err(ApplicationError::CreatorAuthorityUnavailable)
        );
        acquisition_provider.assert_counts(1, 0, &[]);

        let storage_error_provider = RetryTestProvider::cached([
            Err(ApplicationError::Storage {
                message: "closed test error".to_owned(),
            }),
            Ok(()),
        ]);
        let storage_error_client =
            ProviderBackedPubkyHomeserverStorageClient::new(storage_error_provider.clone());
        assert!(matches!(
            storage_error_client
                .get_json_value_as_creator(&creator(), "/pub/app.locks/config.json")
                .await,
            Err(ApplicationError::Storage { .. })
        ));
        storage_error_provider.assert_counts(1, 1, &[]);

        let missing_provider = RetryTestProvider::cached_missing();
        let missing_client =
            ProviderBackedPubkyHomeserverStorageClient::new(missing_provider.clone());
        assert_eq!(
            missing_client
                .get_json_value_as_creator(&creator(), "/pub/app.locks/missing.json")
                .await
                .unwrap(),
            None
        );
        missing_provider.assert_counts(1, 1, &[]);

        let legacy_provider = RetryTestProvider::uncached([
            Err(ApplicationError::CreatorAuthorityUnavailable),
            Ok(()),
        ]);
        let legacy_client =
            ProviderBackedPubkyHomeserverStorageClient::new(legacy_provider.clone());
        assert_eq!(
            legacy_client
                .get_bytes_as_creator(&creator(), "/priv/app.locks/content/example.txt")
                .await,
            Err(ApplicationError::CreatorAuthorityUnavailable)
        );
        legacy_provider.assert_counts(1, 1, &[]);

        let reacquisition_provider =
            RetryTestProvider::cached([Err(ApplicationError::CreatorAuthorityUnavailable), Ok(())]);
        reacquisition_provider.fail_acquisition(2);
        let reacquisition_client =
            ProviderBackedPubkyHomeserverStorageClient::new(reacquisition_provider.clone());
        assert_eq!(
            reacquisition_client
                .get_metadata_as_creator(&creator(), "/priv/app.locks/content/example.txt")
                .await,
            Err(ApplicationError::CreatorAuthorityUnavailable)
        );
        reacquisition_provider.assert_counts(2, 1, &[1]);
    }

    #[tokio::test]
    async fn stale_read_failure_preserves_replacement_generation_and_retry_singleflight() {
        let provider = StaleGenerationProvider::new();
        let client = Arc::new(ProviderBackedPubkyHomeserverStorageClient::new(
            provider.clone(),
        ));
        let delayed = {
            let client = Arc::clone(&client);
            tokio::spawn(async move {
                client
                    .get_json_value_as_creator(&creator(), "/pub/app.locks/config.json")
                    .await
            })
        };
        provider.first_read_started.notified().await;

        assert_eq!(
            client
                .get_json_value_as_creator(&creator(), "/pub/app.locks/config.json")
                .await
                .unwrap(),
            Some(json!({"generation": 2}))
        );
        provider.release_first_read.notify_one();
        assert_eq!(
            delayed.await.unwrap().unwrap(),
            Some(json!({"generation": 2}))
        );

        assert_eq!(provider.current_generation.load(Ordering::SeqCst), 2);
        assert_eq!(provider.replacements.load(Ordering::SeqCst), 1);
        assert_eq!(provider.acquisitions.load(Ordering::SeqCst), 4);
        assert_eq!(provider.operations.load(Ordering::SeqCst), 4);
        assert_eq!(*provider.invalidations.lock().unwrap(), vec![1, 1]);
    }

    #[tokio::test]
    async fn mutations_invalidate_cached_generation_without_retrying() {
        let json_provider =
            RetryTestProvider::cached([Err(ApplicationError::CreatorAuthorityUnavailable), Ok(())]);
        let json_client = ProviderBackedPubkyHomeserverStorageClient::new(json_provider.clone());
        assert_eq!(
            json_client
                .put_json_value_as_creator(
                    &creator(),
                    "/pub/app.locks/config.json",
                    json!({"version": 1}),
                )
                .await,
            Err(ApplicationError::CreatorAuthorityUnavailable)
        );
        json_provider.assert_counts(1, 1, &[1]);

        let put_provider =
            RetryTestProvider::cached([Err(ApplicationError::CreatorAuthorityUnavailable), Ok(())]);
        let put_client = ProviderBackedPubkyHomeserverStorageClient::new(put_provider.clone());
        assert_eq!(
            put_client
                .put_bytes_as_creator(
                    &creator(),
                    "/priv/app.locks/content/example.txt",
                    b"secret".to_vec(),
                    "text/plain",
                )
                .await,
            Err(ApplicationError::CreatorAuthorityUnavailable)
        );
        put_provider.assert_counts(1, 1, &[1]);

        let delete_provider =
            RetryTestProvider::cached([Err(ApplicationError::CreatorAuthorityUnavailable), Ok(())]);
        let delete_client =
            ProviderBackedPubkyHomeserverStorageClient::new(delete_provider.clone());
        assert_eq!(
            delete_client
                .delete_as_creator(&creator(), "/priv/app.locks/content/example.txt")
                .await,
            Err(ApplicationError::CreatorAuthorityUnavailable)
        );
        delete_provider.assert_counts(1, 1, &[1]);
    }

    #[tokio::test]
    async fn legacy_cookie_provider_missing_authority_returns_unavailable_without_importing() {
        let store = FakeCreatorAuthorityStore::new(None);
        let importer =
            FakePubkySessionImporter::new(Ok(FakeImportedPubkySession::for_creator(creator_z32())));
        let provider = LegacyCookieCreatorScopedPubkyStorageProvider::new(store, importer);

        let result = provider.storage_for_creator(&creator()).await;

        assert_storage_error(result, ApplicationError::CreatorAuthorityUnavailable);
        assert_eq!(provider.importer().seen_secrets(), Vec::<String>::new());
    }

    #[tokio::test]
    async fn legacy_cookie_provider_non_legacy_auth_kind_returns_unavailable_without_importing() {
        let store = FakeCreatorAuthorityStore::new(Some(CreatorAuthorityRecord {
            auth_kind: CreatorAuthorityAuthKind::Grant,
            ..creator_authority_record("grant-secret")
        }));
        let importer =
            FakePubkySessionImporter::new(Ok(FakeImportedPubkySession::for_creator(creator_z32())));
        let provider = LegacyCookieCreatorScopedPubkyStorageProvider::new(store, importer);

        let result = provider.storage_for_creator(&creator()).await;

        assert_storage_error(result, ApplicationError::CreatorAuthorityUnavailable);
        assert_eq!(provider.importer().seen_secrets(), Vec::<String>::new());
    }

    #[tokio::test]
    async fn legacy_cookie_provider_identity_mismatch_returns_secret_free_unavailable_error() {
        let store = FakeCreatorAuthorityStore::new(Some(creator_authority_record(
            "legacy-cookie-session-secret",
        )));
        let importer = FakePubkySessionImporter::new(Ok(FakeImportedPubkySession::for_creator(
            other_creator_z32(),
        )));
        let provider = LegacyCookieCreatorScopedPubkyStorageProvider::new(store, importer);

        let error = storage_error(provider.storage_for_creator(&creator()).await);

        assert_eq!(error, ApplicationError::CreatorAuthorityUnavailable);
        let debug = format!("{error:?}");
        assert!(!debug.contains("legacy-cookie-session-secret"));
        assert_eq!(
            provider.importer().seen_secrets(),
            vec!["legacy-cookie-session-secret".to_owned()]
        );
    }

    #[tokio::test]
    async fn legacy_cookie_provider_imports_matching_session_and_returns_scoped_storage() {
        let store = FakeCreatorAuthorityStore::new(Some(creator_authority_record(
            "legacy-cookie-session-secret",
        )));
        let importer =
            FakePubkySessionImporter::new(Ok(FakeImportedPubkySession::for_creator(creator_z32())));
        let provider = LegacyCookieCreatorScopedPubkyStorageProvider::new(store, importer);

        let storage = provider.storage_for_creator(&creator()).await.unwrap();
        storage
            .storage
            .put_json_value("/pub/app.locks/config.json", json!({"version": 1}))
            .await
            .unwrap();

        assert_eq!(
            provider.importer().seen_secrets(),
            vec!["legacy-cookie-session-secret".to_owned()]
        );
    }

    #[tokio::test]
    async fn grant_provider_accepts_grants_and_rejects_legacy_records() {
        let grant_record = CreatorAuthorityRecord {
            auth_kind: CreatorAuthorityAuthKind::Grant,
            ..creator_authority_record("delegated-grant-state")
        };
        let imported_session = FakeImportedPubkySession::for_creator(creator_z32());
        let provider = LegacyCookieCreatorScopedPubkyStorageProvider::new_grant(
            FakeCreatorAuthorityStore::new(Some(grant_record)),
            FakePubkySessionImporter::new(Ok(imported_session.clone())),
        );

        provider.storage_for_creator(&creator()).await.unwrap();
        assert!(imported_session.grant_storage_selected());
        assert_eq!(
            provider.importer().seen_secrets(),
            vec!["delegated-grant-state".to_owned()]
        );

        let legacy_provider = LegacyCookieCreatorScopedPubkyStorageProvider::new_grant(
            FakeCreatorAuthorityStore::new(Some(creator_authority_record(
                "legacy-cookie-session-secret",
            ))),
            FakePubkySessionImporter::new(Ok(FakeImportedPubkySession::for_creator(creator_z32()))),
        );
        assert_storage_error(
            legacy_provider.storage_for_creator(&creator()).await,
            ApplicationError::CreatorAuthorityUnavailable,
        );
        assert!(legacy_provider.importer().seen_secrets().is_empty());
    }

    #[derive(Debug)]
    struct FakeCreatorAuthorityManager {
        result: Result<CreatorAuthorityStatus, ApplicationError>,
        seen_creators: Mutex<Vec<CreatorPubky>>,
    }

    impl FakeCreatorAuthorityManager {
        fn authorized() -> Self {
            Self {
                result: Ok(CreatorAuthorityStatus {
                    creator: creator(),
                    auth_kind: CreatorAuthorityAuthKind::LegacyCookie,
                    authorized: true,
                    granted_scopes: vec![
                        "/pub/app.locks/:rw".to_owned(),
                        "/priv/app.locks/:rw".to_owned(),
                    ],
                    session_expires_at: None,
                }),
                seen_creators: Mutex::new(Vec::new()),
            }
        }

        fn unavailable() -> Self {
            Self {
                result: Err(ApplicationError::CreatorAuthorityUnavailable),
                seen_creators: Mutex::new(Vec::new()),
            }
        }

        fn seen_creators(&self) -> Vec<CreatorPubky> {
            self.seen_creators.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl CreatorAuthorityManager for FakeCreatorAuthorityManager {
        async fn revalidate_creator_authority(
            &self,
            creator: &CreatorPubky,
        ) -> Result<CreatorAuthorityStatus, ApplicationError> {
            self.seen_creators.lock().unwrap().push(creator.clone());
            self.result.clone()
        }

        async fn require_creator_authority(
            &self,
            creator: &CreatorPubky,
        ) -> Result<CreatorAuthorityStatus, ApplicationError> {
            self.revalidate_creator_authority(creator).await
        }
    }

    #[derive(Debug)]
    struct FakeStorageClient {
        json_read: Mutex<Option<serde_json::Value>>,
        operations: Mutex<Vec<String>>,
    }

    impl FakeStorageClient {
        fn new() -> Self {
            Self {
                json_read: Mutex::new(None),
                operations: Mutex::new(Vec::new()),
            }
        }

        fn with_json_read(self, value: Option<serde_json::Value>) -> Self {
            *self.json_read.lock().unwrap() = value;
            self
        }

        fn operations(&self) -> Vec<String> {
            self.operations.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl PubkyHomeserverStorageClient for FakeStorageClient {
        async fn put_json_value_as_creator(
            &self,
            creator: &CreatorPubky,
            path: &str,
            _body: serde_json::Value,
        ) -> Result<(), ApplicationError> {
            self.operations
                .lock()
                .unwrap()
                .push(format!("put_json {creator} {path}"));
            Ok(())
        }

        async fn get_json_value_as_creator(
            &self,
            creator: &CreatorPubky,
            path: &str,
        ) -> Result<Option<serde_json::Value>, ApplicationError> {
            self.operations
                .lock()
                .unwrap()
                .push(format!("get_json {creator} {path}"));
            Ok(self.json_read.lock().unwrap().clone())
        }

        async fn put_bytes_as_creator(
            &self,
            creator: &CreatorPubky,
            path: &str,
            _bytes: Vec<u8>,
            _content_type: &str,
        ) -> Result<(), ApplicationError> {
            self.operations
                .lock()
                .unwrap()
                .push(format!("put_bytes {creator} {path}"));
            Ok(())
        }

        async fn get_bytes_as_creator(
            &self,
            creator: &CreatorPubky,
            path: &str,
        ) -> Result<Option<PubkyBytesResource>, ApplicationError> {
            self.operations
                .lock()
                .unwrap()
                .push(format!("get_bytes {creator} {path}"));
            Ok(None)
        }

        async fn get_metadata_as_creator(
            &self,
            creator: &CreatorPubky,
            path: &str,
        ) -> Result<Option<PubkyResourceMetadata>, ApplicationError> {
            self.operations
                .lock()
                .unwrap()
                .push(format!("get_metadata {creator} {path}"));
            Ok(None)
        }

        async fn delete_as_creator(
            &self,
            creator: &CreatorPubky,
            path: &str,
        ) -> Result<(), ApplicationError> {
            self.operations
                .lock()
                .unwrap()
                .push(format!("delete {creator} {path}"));
            Ok(())
        }
    }

    #[derive(Debug, Clone, Default)]
    struct RevokedSessionProvider {
        operations: Arc<Mutex<usize>>,
        invalidations: Arc<Mutex<Vec<(CreatorPubky, u64)>>>,
    }

    #[async_trait]
    impl CreatorScopedPubkyStorageProvider for RevokedSessionProvider {
        async fn storage_for_creator(
            &self,
            _creator: &CreatorPubky,
        ) -> Result<AcquiredCreatorScopedPubkyStorage, ApplicationError> {
            Ok(AcquiredCreatorScopedPubkyStorage::cached(
                Box::new(RevokedStorage {
                    operations: Arc::clone(&self.operations),
                }),
                7,
            ))
        }

        async fn invalidate_creator_session(&self, creator: &CreatorPubky, generation: u64) {
            self.invalidations
                .lock()
                .unwrap()
                .push((creator.clone(), generation));
        }
    }

    #[derive(Debug)]
    struct RevokedStorage {
        operations: Arc<Mutex<usize>>,
    }

    impl RevokedStorage {
        fn record_operation(&self) {
            *self.operations.lock().unwrap() += 1;
        }
    }

    #[async_trait]
    impl CreatorScopedPubkyStorage for RevokedStorage {
        async fn put_json_value(
            &self,
            _: &str,
            _: serde_json::Value,
        ) -> Result<(), ApplicationError> {
            self.record_operation();
            Err(ApplicationError::CreatorAuthorityUnavailable)
        }
        async fn get_json_value(
            &self,
            _: &str,
        ) -> Result<Option<serde_json::Value>, ApplicationError> {
            self.record_operation();
            Err(ApplicationError::CreatorAuthorityUnavailable)
        }
        async fn put_bytes(&self, _: &str, _: Vec<u8>, _: &str) -> Result<(), ApplicationError> {
            self.record_operation();
            Err(ApplicationError::CreatorAuthorityUnavailable)
        }
        async fn get_bytes(&self, _: &str) -> Result<Option<PubkyBytesResource>, ApplicationError> {
            self.record_operation();
            Err(ApplicationError::CreatorAuthorityUnavailable)
        }
        async fn get_metadata(
            &self,
            _: &str,
        ) -> Result<Option<PubkyResourceMetadata>, ApplicationError> {
            self.record_operation();
            Err(ApplicationError::CreatorAuthorityUnavailable)
        }
        async fn delete(&self, _: &str) -> Result<(), ApplicationError> {
            self.record_operation();
            Err(ApplicationError::CreatorAuthorityUnavailable)
        }
    }

    #[derive(Debug, Clone)]
    struct StaleGenerationProvider {
        current_generation: Arc<AtomicUsize>,
        replacements: Arc<AtomicUsize>,
        acquisitions: Arc<AtomicUsize>,
        operations: Arc<AtomicUsize>,
        generation_one_reads: Arc<AtomicUsize>,
        invalidations: Arc<Mutex<Vec<u64>>>,
        first_read_started: Arc<tokio::sync::Notify>,
        release_first_read: Arc<tokio::sync::Notify>,
    }

    impl StaleGenerationProvider {
        fn new() -> Self {
            Self {
                current_generation: Arc::new(AtomicUsize::new(1)),
                replacements: Arc::new(AtomicUsize::new(0)),
                acquisitions: Arc::new(AtomicUsize::new(0)),
                operations: Arc::new(AtomicUsize::new(0)),
                generation_one_reads: Arc::new(AtomicUsize::new(0)),
                invalidations: Arc::new(Mutex::new(Vec::new())),
                first_read_started: Arc::new(tokio::sync::Notify::new()),
                release_first_read: Arc::new(tokio::sync::Notify::new()),
            }
        }
    }

    #[async_trait]
    impl CreatorScopedPubkyStorageProvider for StaleGenerationProvider {
        async fn storage_for_creator(
            &self,
            _creator: &CreatorPubky,
        ) -> Result<AcquiredCreatorScopedPubkyStorage, ApplicationError> {
            self.acquisitions.fetch_add(1, Ordering::SeqCst);
            let generation = match self.current_generation.compare_exchange(
                0,
                2,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    self.replacements.fetch_add(1, Ordering::SeqCst);
                    2
                }
                Err(generation) => generation,
            };
            Ok(AcquiredCreatorScopedPubkyStorage::cached(
                Box::new(StaleGenerationStorage {
                    generation,
                    operations: Arc::clone(&self.operations),
                    generation_one_reads: Arc::clone(&self.generation_one_reads),
                    first_read_started: Arc::clone(&self.first_read_started),
                    release_first_read: Arc::clone(&self.release_first_read),
                }),
                generation as u64,
            ))
        }

        async fn invalidate_creator_session(&self, _creator: &CreatorPubky, generation: u64) {
            self.invalidations.lock().unwrap().push(generation);
            let _ = self.current_generation.compare_exchange(
                generation as usize,
                0,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
        }
    }

    #[derive(Debug)]
    struct StaleGenerationStorage {
        generation: usize,
        operations: Arc<AtomicUsize>,
        generation_one_reads: Arc<AtomicUsize>,
        first_read_started: Arc<tokio::sync::Notify>,
        release_first_read: Arc<tokio::sync::Notify>,
    }

    #[async_trait]
    impl CreatorScopedPubkyStorage for StaleGenerationStorage {
        async fn put_json_value(
            &self,
            _: &str,
            _: serde_json::Value,
        ) -> Result<(), ApplicationError> {
            unimplemented!("read-only concurrency test")
        }

        async fn get_json_value(
            &self,
            _: &str,
        ) -> Result<Option<serde_json::Value>, ApplicationError> {
            self.operations.fetch_add(1, Ordering::SeqCst);
            if self.generation == 1 {
                if self.generation_one_reads.fetch_add(1, Ordering::SeqCst) == 0 {
                    self.first_read_started.notify_one();
                    self.release_first_read.notified().await;
                }
                return Err(ApplicationError::CreatorAuthorityUnavailable);
            }
            Ok(Some(json!({"generation": self.generation})))
        }

        async fn put_bytes(&self, _: &str, _: Vec<u8>, _: &str) -> Result<(), ApplicationError> {
            unimplemented!("read-only concurrency test")
        }

        async fn get_bytes(&self, _: &str) -> Result<Option<PubkyBytesResource>, ApplicationError> {
            unimplemented!("read-only concurrency test")
        }

        async fn get_metadata(
            &self,
            _: &str,
        ) -> Result<Option<PubkyResourceMetadata>, ApplicationError> {
            unimplemented!("read-only concurrency test")
        }

        async fn delete(&self, _: &str) -> Result<(), ApplicationError> {
            unimplemented!("read-only concurrency test")
        }
    }

    #[derive(Debug)]
    struct RetryTestState {
        cached: bool,
        missing: bool,
        acquisitions: usize,
        operations: usize,
        invalidations: Vec<u64>,
        fail_acquisition: Option<usize>,
        results: VecDeque<Result<(), ApplicationError>>,
    }

    #[derive(Debug, Clone)]
    struct RetryTestProvider {
        state: Arc<Mutex<RetryTestState>>,
    }

    impl RetryTestProvider {
        fn cached(results: impl IntoIterator<Item = Result<(), ApplicationError>>) -> Self {
            Self::new(true, results)
        }

        fn uncached(results: impl IntoIterator<Item = Result<(), ApplicationError>>) -> Self {
            Self::new(false, results)
        }

        fn cached_missing() -> Self {
            let provider = Self::new(true, [Ok(())]);
            provider.state.lock().unwrap().missing = true;
            provider
        }

        fn new(
            cached: bool,
            results: impl IntoIterator<Item = Result<(), ApplicationError>>,
        ) -> Self {
            Self {
                state: Arc::new(Mutex::new(RetryTestState {
                    cached,
                    missing: false,
                    acquisitions: 0,
                    operations: 0,
                    invalidations: Vec::new(),
                    fail_acquisition: None,
                    results: results.into_iter().collect(),
                })),
            }
        }

        fn fail_acquisition(&self, acquisition: usize) {
            self.state.lock().unwrap().fail_acquisition = Some(acquisition);
        }

        fn assert_counts(&self, acquisitions: usize, operations: usize, invalidations: &[u64]) {
            let state = self.state.lock().unwrap();
            assert_eq!(state.acquisitions, acquisitions);
            assert_eq!(state.operations, operations);
            assert_eq!(state.invalidations, invalidations);
        }
    }

    #[async_trait]
    impl CreatorScopedPubkyStorageProvider for RetryTestProvider {
        async fn storage_for_creator(
            &self,
            _creator: &CreatorPubky,
        ) -> Result<AcquiredCreatorScopedPubkyStorage, ApplicationError> {
            let mut state = self.state.lock().unwrap();
            state.acquisitions += 1;
            let acquisition = state.acquisitions;
            if state.fail_acquisition == Some(acquisition) {
                return Err(ApplicationError::CreatorAuthorityUnavailable);
            }
            let storage = Box::new(RetryTestStorage {
                state: Arc::clone(&self.state),
            });
            Ok(if state.cached {
                AcquiredCreatorScopedPubkyStorage::cached(storage, acquisition as u64)
            } else {
                AcquiredCreatorScopedPubkyStorage::uncached(storage)
            })
        }

        async fn invalidate_creator_session(&self, _creator: &CreatorPubky, generation: u64) {
            self.state.lock().unwrap().invalidations.push(generation);
        }
    }

    #[derive(Debug)]
    struct RetryTestStorage {
        state: Arc<Mutex<RetryTestState>>,
    }

    impl RetryTestStorage {
        fn result(&self) -> Result<(), ApplicationError> {
            let mut state = self.state.lock().unwrap();
            state.operations += 1;
            state.results.pop_front().unwrap_or(Ok(()))
        }
    }

    #[async_trait]
    impl CreatorScopedPubkyStorage for RetryTestStorage {
        async fn put_json_value(
            &self,
            _: &str,
            _: serde_json::Value,
        ) -> Result<(), ApplicationError> {
            self.result()
        }

        async fn put_json_value_classified(
            &self,
            _: &str,
            _: serde_json::Value,
        ) -> Result<(), ContentLockWriteError> {
            self.result().map_err(|error| match error {
                ApplicationError::CreatorAuthorityUnavailable => {
                    ContentLockWriteError::NotStarted(error)
                }
                _ => ContentLockWriteError::Ambiguous(error),
            })
        }

        async fn get_json_value(
            &self,
            _: &str,
        ) -> Result<Option<serde_json::Value>, ApplicationError> {
            let missing = self.state.lock().unwrap().missing;
            self.result()
                .map(|_| (!missing).then(|| json!({"version": 1})))
        }

        async fn put_bytes(&self, _: &str, _: Vec<u8>, _: &str) -> Result<(), ApplicationError> {
            self.result()
        }

        async fn get_bytes(&self, _: &str) -> Result<Option<PubkyBytesResource>, ApplicationError> {
            self.result().map(|_| {
                Some(PubkyBytesResource {
                    bytes: b"restored".to_vec(),
                    content_type: Some("text/plain".to_owned()),
                })
            })
        }

        async fn get_metadata(
            &self,
            _: &str,
        ) -> Result<Option<PubkyResourceMetadata>, ApplicationError> {
            self.result().map(|_| {
                Some(PubkyResourceMetadata {
                    content_length: Some(8),
                    content_type: Some("text/plain".to_owned()),
                    content_hash: Some(*blake3::hash(b"restored").as_bytes()),
                })
            })
        }

        async fn delete(&self, _: &str) -> Result<(), ApplicationError> {
            self.result()
        }
    }

    #[derive(Debug, Clone)]
    struct DecodeThenSuccessProvider {
        endpoint: String,
        invalidated: Arc<AtomicBool>,
        invalidations: Arc<AtomicUsize>,
    }

    impl DecodeThenSuccessProvider {
        fn new(endpoint: String) -> Self {
            Self {
                endpoint,
                invalidated: Arc::new(AtomicBool::new(false)),
                invalidations: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn invalidations(&self) -> usize {
            self.invalidations.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl CreatorScopedPubkyStorageProvider for DecodeThenSuccessProvider {
        async fn storage_for_creator(
            &self,
            _creator: &CreatorPubky,
        ) -> Result<AcquiredCreatorScopedPubkyStorage, ApplicationError> {
            if self.invalidated.load(Ordering::SeqCst) {
                return Err(ApplicationError::CreatorAuthorityUnavailable);
            }
            Ok(AcquiredCreatorScopedPubkyStorage::cached(
                Box::new(DecodeThenSuccessStorage {
                    endpoint: self.endpoint.clone(),
                }),
                7,
            ))
        }

        async fn invalidate_creator_session(&self, _creator: &CreatorPubky, _generation: u64) {
            self.invalidated.store(true, Ordering::SeqCst);
            self.invalidations.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[derive(Debug)]
    struct DecodeThenSuccessStorage {
        endpoint: String,
    }

    #[async_trait]
    impl CreatorScopedPubkyStorage for DecodeThenSuccessStorage {
        async fn put_json_value(
            &self,
            _: &str,
            _: serde_json::Value,
        ) -> Result<(), ApplicationError> {
            unimplemented!("not needed by malformed JSON regression")
        }

        async fn get_json_value(
            &self,
            path: &str,
        ) -> Result<Option<serde_json::Value>, ApplicationError> {
            reqwest::get(&self.endpoint)
                .await
                .unwrap()
                .json::<serde_json::Value>()
                .await
                .map(Some)
                .map_err(|error| {
                    assert!(error.is_decode());
                    assert!(!error.is_timeout());
                    pubky_storage_error_for_grant(
                        "get",
                        path,
                        pubky::Error::Request(pubky::errors::RequestError::Transport(error)),
                    )
                })
        }

        async fn put_bytes(&self, _: &str, _: Vec<u8>, _: &str) -> Result<(), ApplicationError> {
            unimplemented!("not needed by malformed JSON regression")
        }

        async fn get_bytes(&self, _: &str) -> Result<Option<PubkyBytesResource>, ApplicationError> {
            unimplemented!("not needed by malformed JSON regression")
        }

        async fn get_metadata(
            &self,
            _: &str,
        ) -> Result<Option<PubkyResourceMetadata>, ApplicationError> {
            unimplemented!("not needed by malformed JSON regression")
        }

        async fn delete(&self, _: &str) -> Result<(), ApplicationError> {
            unimplemented!("not needed by malformed JSON regression")
        }
    }

    #[derive(Debug, Clone)]
    struct FakeCreatorScopedPubkyStorageProvider {
        result: Result<FakeCreatorScopedPubkyStorage, ApplicationError>,
        seen_creators: Arc<Mutex<Vec<CreatorPubky>>>,
    }

    impl FakeCreatorScopedPubkyStorageProvider {
        fn authorized() -> Self {
            Self {
                result: Ok(FakeCreatorScopedPubkyStorage::default()),
                seen_creators: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn unavailable() -> Self {
            Self {
                result: Err(ApplicationError::CreatorAuthorityUnavailable),
                seen_creators: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn seen_creators(&self) -> Vec<CreatorPubky> {
            self.seen_creators.lock().unwrap().clone()
        }

        fn storage_operations(&self) -> Vec<String> {
            match &self.result {
                Ok(storage) => storage.operations(),
                Err(_) => Vec::new(),
            }
        }
    }

    #[async_trait]
    impl CreatorScopedPubkyStorageProvider for FakeCreatorScopedPubkyStorageProvider {
        async fn storage_for_creator(
            &self,
            creator: &CreatorPubky,
        ) -> Result<AcquiredCreatorScopedPubkyStorage, ApplicationError> {
            self.seen_creators.lock().unwrap().push(creator.clone());
            self.result
                .clone()
                .map(|storage| AcquiredCreatorScopedPubkyStorage::uncached(Box::new(storage)))
        }
    }

    #[derive(Debug, Clone, Default)]
    struct FakeCreatorScopedPubkyStorage {
        operations: Arc<Mutex<Vec<String>>>,
    }

    impl FakeCreatorScopedPubkyStorage {
        fn operations(&self) -> Vec<String> {
            self.operations.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl CreatorScopedPubkyStorage for FakeCreatorScopedPubkyStorage {
        async fn put_json_value(
            &self,
            path: &str,
            _body: serde_json::Value,
        ) -> Result<(), ApplicationError> {
            self.operations
                .lock()
                .unwrap()
                .push(format!("put_json {path}"));
            Ok(())
        }

        async fn get_json_value(
            &self,
            path: &str,
        ) -> Result<Option<serde_json::Value>, ApplicationError> {
            self.operations
                .lock()
                .unwrap()
                .push(format!("get_json {path}"));
            Ok(Some(json!({"version": 1})))
        }

        async fn put_bytes(
            &self,
            path: &str,
            _bytes: Vec<u8>,
            _content_type: &str,
        ) -> Result<(), ApplicationError> {
            self.operations
                .lock()
                .unwrap()
                .push(format!("put_bytes {path}"));
            Ok(())
        }

        async fn get_bytes(
            &self,
            path: &str,
        ) -> Result<Option<PubkyBytesResource>, ApplicationError> {
            self.operations
                .lock()
                .unwrap()
                .push(format!("get_bytes {path}"));
            Ok(Some(PubkyBytesResource {
                bytes: b"guarded".to_vec(),
                content_type: Some("text/plain".to_owned()),
            }))
        }

        async fn get_metadata(
            &self,
            path: &str,
        ) -> Result<Option<PubkyResourceMetadata>, ApplicationError> {
            self.operations
                .lock()
                .unwrap()
                .push(format!("get_metadata {path}"));
            Ok(Some(PubkyResourceMetadata {
                content_length: Some(7),
                content_type: Some("text/plain".to_owned()),
                content_hash: Some(*blake3::hash(b"guarded").as_bytes()),
            }))
        }

        async fn delete(&self, path: &str) -> Result<(), ApplicationError> {
            self.operations
                .lock()
                .unwrap()
                .push(format!("delete {path}"));
            Ok(())
        }
    }

    #[derive(Debug)]
    struct FakeCreatorAuthorityStore {
        record: Option<CreatorAuthorityRecord>,
    }

    impl FakeCreatorAuthorityStore {
        fn new(record: Option<CreatorAuthorityRecord>) -> Self {
            Self { record }
        }
    }

    #[async_trait]
    impl CreatorAuthorityStore for FakeCreatorAuthorityStore {
        async fn upsert_creator_authority(
            &self,
            _authority: CreatorAuthorityRecord,
        ) -> Result<(), ApplicationError> {
            unimplemented!("not needed by storage provider tests")
        }

        async fn get_creator_authority(
            &self,
            creator: &CreatorPubky,
        ) -> Result<Option<CreatorAuthorityRecord>, ApplicationError> {
            Ok(self
                .record
                .as_ref()
                .filter(|record| &record.creator == creator)
                .cloned())
        }

        async fn delete_creator_authority(
            &self,
            _creator: &CreatorPubky,
        ) -> Result<(), ApplicationError> {
            unimplemented!("not needed by storage provider tests")
        }
    }

    #[derive(Debug)]
    struct FakePubkySessionImporter {
        result: Result<FakeImportedPubkySession, ApplicationError>,
        seen_secrets: Mutex<Vec<String>>,
    }

    impl FakePubkySessionImporter {
        fn new(result: Result<FakeImportedPubkySession, ApplicationError>) -> Self {
            Self {
                result,
                seen_secrets: Mutex::new(Vec::new()),
            }
        }

        fn seen_secrets(&self) -> Vec<String> {
            self.seen_secrets.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl PubkySessionImporter for FakePubkySessionImporter {
        type Session = FakeImportedPubkySession;

        async fn import_legacy_cookie_session(
            &self,
            secret: &CreatorAuthoritySecret,
        ) -> Result<Self::Session, ApplicationError> {
            self.seen_secrets
                .lock()
                .unwrap()
                .push(secret.expose_secret().to_owned());
            self.result.clone()
        }

        async fn import_grant_session(
            &self,
            secret: &CreatorAuthoritySecret,
        ) -> Result<Self::Session, ApplicationError> {
            self.seen_secrets
                .lock()
                .unwrap()
                .push(secret.expose_secret().to_owned());
            self.result.clone()
        }
    }

    #[derive(Debug, Clone)]
    struct FakeImportedPubkySession {
        public_key_z32: String,
        storage: FakeCreatorScopedPubkyStorage,
        grant_storage_selected: Arc<AtomicBool>,
    }

    impl FakeImportedPubkySession {
        fn for_creator(public_key_z32: &str) -> Self {
            Self {
                public_key_z32: public_key_z32.to_owned(),
                storage: FakeCreatorScopedPubkyStorage::default(),
                grant_storage_selected: Arc::new(AtomicBool::new(false)),
            }
        }

        fn grant_storage_selected(&self) -> bool {
            self.grant_storage_selected.load(Ordering::SeqCst)
        }
    }

    impl ImportedPubkySession for FakeImportedPubkySession {
        type Storage = FakeCreatorScopedPubkyStorage;

        fn public_key_z32(&self) -> String {
            self.public_key_z32.clone()
        }

        fn into_storage(self) -> Self::Storage {
            self.storage
        }

        fn into_grant_storage(self) -> Self::Storage {
            self.grant_storage_selected.store(true, Ordering::SeqCst);
            self.storage
        }
    }

    fn creator_authority_record(secret: &str) -> CreatorAuthorityRecord {
        CreatorAuthorityRecord {
            creator: creator(),
            auth_kind: CreatorAuthorityAuthKind::LegacyCookie,
            granted_scopes: vec![
                "/pub/app.locks/:rw".to_owned(),
                "/priv/app.locks/:rw".to_owned(),
            ],
            secret: CreatorAuthoritySecret::new(secret),
            session_expires_at: Some(datetime!(2026-05-29 12:15:00 UTC)),
            last_revalidated_at: None,
        }
    }

    fn storage_error(
        result: Result<AcquiredCreatorScopedPubkyStorage, ApplicationError>,
    ) -> ApplicationError {
        match result {
            Ok(_) => panic!("expected storage provider error"),
            Err(error) => error,
        }
    }

    fn assert_storage_error(
        result: Result<AcquiredCreatorScopedPubkyStorage, ApplicationError>,
        expected: ApplicationError,
    ) {
        assert_eq!(storage_error(result), expected);
    }

    fn creator() -> CreatorPubky {
        CreatorPubky::from_str("pubkyo1gg96ewuojmopcjbz8895478wdtxtzzuxnfjjz8o8e77csa1ngo").unwrap()
    }

    fn creator_z32() -> &'static str {
        "o1gg96ewuojmopcjbz8895478wdtxtzzuxnfjjz8o8e77csa1ngo"
    }

    fn other_creator_z32() -> &'static str {
        "pxnu33x7jtpx9ar1ytsi4yxbp6a5o36gwhffs8zoxmbuptici1jy"
    }
}
