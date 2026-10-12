use async_trait::async_trait;
use base64::Engine as _;
use locks_core::ids::CreatorPubky;
use locks_core::lock_policy::{
    PRIVATE_PROOF_BUNDLE_PATH_PREFIX, PRIVATE_RESOURCE_CONTENT_PATH_PREFIX,
};

use crate::application::errors::ApplicationError;
use crate::application::models::{CreatorAuthorityAuthKind, CreatorAuthoritySecret};
use crate::application::ports::{CreatorAuthorityManager, CreatorAuthorityStore};
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
}

/// Object-safe operations for storage already scoped to one creator's Pubky session.
#[async_trait]
pub trait CreatorScopedPubkyStorage: Send + Sync {
    async fn put_json_value(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> Result<(), ApplicationError>;

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

        let session = match record.auth_kind {
            CreatorAuthorityAuthKind::LegacyCookie => {
                self.importer
                    .import_legacy_cookie_session(&record.secret)
                    .await?
            }
            CreatorAuthorityAuthKind::Grant => {
                self.importer.import_grant_session(&record.secret).await?
            }
        };
        let restored_creator = creator_from_pubky_public_key_z32(&session.public_key_z32())?;
        if &restored_creator != creator {
            return Err(ApplicationError::CreatorAuthorityUnavailable);
        }

        Ok(AcquiredCreatorScopedPubkyStorage::uncached(Box::new(
            session.into_storage(),
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
            Box::new(cached.session.into_storage()),
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
}

/// Creator-scoped storage wrapper around the Pubky SDK session storage API.
#[derive(Debug, Clone)]
pub struct SdkCreatorScopedPubkyStorage {
    storage: pubky::SessionStorage,
}

impl SdkCreatorScopedPubkyStorage {
    pub fn new(storage: pubky::SessionStorage) -> Self {
        Self { storage }
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
            .map_err(|error| pubky_storage_error("put", path, error))
    }

    async fn get_json_value(
        &self,
        path: &str,
    ) -> Result<Option<serde_json::Value>, ApplicationError> {
        if !self
            .storage
            .exists(path)
            .await
            .map_err(|error| pubky_storage_error("head", path, error))?
        {
            return Ok(None);
        }

        self.storage
            .get_json(path)
            .await
            .map(Some)
            .map_err(|error| pubky_storage_error("get", path, error))
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
            .map_err(|error| pubky_storage_error("put", path, error))
    }

    async fn get_bytes(&self, path: &str) -> Result<Option<PubkyBytesResource>, ApplicationError> {
        if !self
            .storage
            .exists(path)
            .await
            .map_err(|error| pubky_storage_error("head", path, error))?
        {
            return Ok(None);
        }

        let response = self
            .storage
            .get(path)
            .await
            .map_err(|error| pubky_storage_error("get", path, error))?;
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let bytes = response
            .bytes()
            .await
            .map_err(|error| pubky_storage_error("get", path, error))?
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
            .map_err(|error| pubky_storage_error("head", path, error))
    }

    async fn delete(&self, path: &str) -> Result<(), ApplicationError> {
        self.storage
            .delete(path)
            .await
            .map(|_| ())
            .map_err(|error| pubky_storage_error("delete", path, error))
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

    async fn get_json_value_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<Option<serde_json::Value>, ApplicationError> {
        let acquired = self.provider.storage_for_creator(creator).await?;
        let result = acquired.storage.get_json_value(path).await;
        self.finish_operation(creator, acquired.generation, result)
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
        let acquired = self.provider.storage_for_creator(creator).await?;
        let result = acquired.storage.get_bytes(path).await;
        self.finish_operation(creator, acquired.generation, result)
            .await
    }

    async fn get_metadata_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<Option<PubkyResourceMetadata>, ApplicationError> {
        let acquired = self.provider.storage_for_creator(creator).await?;
        let result = acquired.storage.get_metadata(path).await;
        self.finish_operation(creator, acquired.generation, result)
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
    let error = &error as &dyn std::any::Any;
    let unauthorized = error.downcast_ref::<pubky::Error>().is_some_and(|error| {
        matches!(
            error,
            pubky::Error::Authentication(_)
                | pubky::Error::Request(pubky::errors::RequestError::Server {
                    status: pubky::StatusCode::UNAUTHORIZED | pubky::StatusCode::FORBIDDEN,
                    ..
                })
        )
    });
    if unauthorized {
        tracing::warn!(
            stage = "storage_read",
            operation,
            error_class = "unauthorized",
            "creator Pubky storage operation failed"
        );
        return ApplicationError::CreatorAuthorityUnavailable;
    }
    tracing::warn!(
        stage = "storage_read",
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
    use std::str::FromStr;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use locks_core::ids::CreatorPubky;
    use serde_json::json;
    use time::macros::datetime;

    use super::{
        AcquiredCreatorScopedPubkyStorage, AuthorizingPubkyHomeserverStorageClient,
        CreatorScopedPubkyStorage, CreatorScopedPubkyStorageProvider, ImportedPubkySession,
        LegacyCookieCreatorScopedPubkyStorageProvider, ProviderBackedPubkyHomeserverStorageClient,
        PubkyBytesResource, PubkyHomeserverStorageClient, PubkyResourceMetadata,
        PubkySessionImporter, pubky_storage_error,
    };
    use crate::application::errors::ApplicationError;
    use crate::application::models::{
        CreatorAuthorityAuthKind, CreatorAuthorityRecord, CreatorAuthoritySecret,
    };
    use crate::application::ports::{
        CreatorAuthorityManager, CreatorAuthorityStatus, CreatorAuthorityStore,
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
    fn pubky_storage_unauthorized_is_authority_unavailable_without_upstream_body() {
        let error = pubky_storage_error(
            "get",
            "/priv/app.locks/content/secret.txt",
            pubky::Error::Request(pubky::errors::RequestError::Server {
                status: pubky::StatusCode::UNAUTHORIZED,
                message: "raw secret-bearing upstream body".to_owned(),
            }),
        );

        assert_eq!(error, ApplicationError::CreatorAuthorityUnavailable);
        assert!(!format!("{error:?}").contains("raw secret-bearing upstream body"));
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
        assert_eq!(*provider.operations.lock().unwrap(), 6);
        assert_eq!(
            *provider.invalidations.lock().unwrap(),
            vec![(creator(), 7); 6]
        );
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
        let provider = LegacyCookieCreatorScopedPubkyStorageProvider::new_grant(
            FakeCreatorAuthorityStore::new(Some(grant_record)),
            FakePubkySessionImporter::new(Ok(FakeImportedPubkySession::for_creator(creator_z32()))),
        );

        provider.storage_for_creator(&creator()).await.unwrap();
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
    }

    impl FakeImportedPubkySession {
        fn for_creator(public_key_z32: &str) -> Self {
            Self {
                public_key_z32: public_key_z32.to_owned(),
                storage: FakeCreatorScopedPubkyStorage::default(),
            }
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
