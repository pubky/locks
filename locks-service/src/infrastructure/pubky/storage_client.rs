use async_trait::async_trait;
use locks_core::ids::CreatorPubky;
use locks_core::lock_policy::{
    PRIVATE_PROOF_BUNDLE_PATH_PREFIX, PRIVATE_RESOURCE_CONTENT_PATH_PREFIX,
};

use crate::application::errors::ApplicationError;
use crate::application::models::{CreatorAuthorityAuthKind, CreatorAuthoritySecret};
use crate::application::ports::{CreatorAuthorityManager, CreatorAuthorityStore};
use crate::infrastructure::pubky::legacy_connect_flow::creator_from_pubky_public_key_z32;

/// Bytes fetched from a creator-owned Pubky homeserver resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubkyBytesResource {
    /// Raw resource bytes.
    pub bytes: Vec<u8>,
    /// Optional content type from homeserver response metadata.
    pub content_type: Option<String>,
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

    async fn delete(&self, path: &str) -> Result<(), ApplicationError>;
}

/// Provides storage scoped to the requested creator's restorable Pubky session.
#[async_trait]
pub trait CreatorScopedPubkyStorageProvider: Send + Sync {
    async fn storage_for_creator(
        &self,
        creator: &CreatorPubky,
    ) -> Result<Box<dyn CreatorScopedPubkyStorage>, ApplicationError>;
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
}

/// Creator-scoped storage provider backed by persisted legacy cookie creator authority.
#[derive(Debug)]
pub struct LegacyCookieCreatorScopedPubkyStorageProvider<S, I> {
    store: S,
    importer: I,
}

impl<S, I> LegacyCookieCreatorScopedPubkyStorageProvider<S, I> {
    pub fn new(store: S, importer: I) -> Self {
        Self { store, importer }
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
    ) -> Result<Box<dyn CreatorScopedPubkyStorage>, ApplicationError> {
        let record = self
            .store
            .get_creator_authority(creator)
            .await?
            .ok_or(ApplicationError::CreatorAuthorityUnavailable)?;

        if record.auth_kind != CreatorAuthorityAuthKind::LegacyCookie {
            return Err(ApplicationError::CreatorAuthorityUnavailable);
        }

        let session = self
            .importer
            .import_legacy_cookie_session(&record.secret)
            .await?;
        let restored_creator = creator_from_pubky_public_key_z32(&session.public_key_z32())?;
        if &restored_creator != creator {
            return Err(ApplicationError::CreatorAuthorityUnavailable);
        }

        Ok(Box::new(session.into_storage()))
    }
}

/// Real Pubky SDK importer for legacy cookie-session secrets.
#[derive(Debug, Clone)]
pub struct PubkyLegacyCookieSessionImporter {
    client: pubky::PubkyHttpClient,
}

impl PubkyLegacyCookieSessionImporter {
    pub fn new(client: pubky::PubkyHttpClient) -> Self {
        Self { client }
    }
}

#[async_trait]
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
        self.provider
            .storage_for_creator(creator)
            .await?
            .put_json_value(path, body)
            .await
    }

    async fn get_json_value_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<Option<serde_json::Value>, ApplicationError> {
        self.provider
            .storage_for_creator(creator)
            .await?
            .get_json_value(path)
            .await
    }

    async fn put_bytes_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
        bytes: Vec<u8>,
        content_type: &str,
    ) -> Result<(), ApplicationError> {
        self.provider
            .storage_for_creator(creator)
            .await?
            .put_bytes(path, bytes, content_type)
            .await
    }

    async fn get_bytes_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<Option<PubkyBytesResource>, ApplicationError> {
        self.provider
            .storage_for_creator(creator)
            .await?
            .get_bytes(path)
            .await
    }

    async fn delete_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<(), ApplicationError> {
        self.provider
            .storage_for_creator(creator)
            .await?
            .delete(path)
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

    async fn delete_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<(), ApplicationError> {
        self.manager.require_creator_authority(creator).await?;
        self.inner.delete_as_creator(creator, path).await
    }
}

/// Maps Pubky storage failures to a secret-safe application storage error.
pub fn pubky_storage_error(
    operation: &'static str,
    path: &str,
    error: impl std::fmt::Display,
) -> ApplicationError {
    let message = if is_private_locks_path(path) {
        format!("Pubky homeserver {operation} failed for private Locks path")
    } else {
        format!("Pubky homeserver {operation} failed for {path}: {error}")
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
        AuthorizingPubkyHomeserverStorageClient, CreatorScopedPubkyStorage,
        CreatorScopedPubkyStorageProvider, ImportedPubkySession,
        LegacyCookieCreatorScopedPubkyStorageProvider, ProviderBackedPubkyHomeserverStorageClient,
        PubkyBytesResource, PubkyHomeserverStorageClient, PubkySessionImporter,
        pubky_storage_error,
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
            .get_json_value_as_creator(&creator(), "/pub/locks.app/config.json")
            .await
            .unwrap();

        assert_eq!(result, None);
        assert_eq!(client.manager().seen_creators(), vec![creator()]);
        assert_eq!(
            client.inner().operations(),
            vec![format!("get_json {} /pub/locks.app/config.json", creator())]
        );
    }

    #[tokio::test]
    async fn missing_creator_authority_returns_unavailable_before_storage_operation() {
        let manager = FakeCreatorAuthorityManager::unavailable();
        let inner = FakeStorageClient::new().with_json_read(Some(json!({"version": 1})));
        let client = AuthorizingPubkyHomeserverStorageClient::new(inner, manager);

        let result = client
            .get_json_value_as_creator(&creator(), "/pub/locks.app/config.json")
            .await;

        assert_eq!(result, Err(ApplicationError::CreatorAuthorityUnavailable));
        assert_eq!(client.inner().operations(), Vec::<String>::new());
    }

    #[test]
    fn pubky_storage_error_redacts_private_locks_paths() {
        let error = pubky_storage_error(
            "get",
            "/priv/locks.app/content/secret.txt",
            "network timeout",
        );

        let debug = format!("{error:?}");
        assert!(debug.contains("Pubky homeserver get failed for private Locks path"));
        assert!(!debug.contains("secret.txt"));
        assert!(!debug.contains("/priv/locks.app/content/secret.txt"));
        assert!(!debug.contains("network timeout"));
    }

    #[tokio::test]
    async fn provider_backed_client_asks_provider_for_creator_scoped_storage_before_each_operation()
    {
        let provider = FakeCreatorScopedPubkyStorageProvider::authorized();
        let client = ProviderBackedPubkyHomeserverStorageClient::new(provider.clone());

        client
            .put_json_value_as_creator(
                &creator(),
                "/pub/locks.app/config.json",
                json!({"version": 1}),
            )
            .await
            .unwrap();
        let bytes = client
            .get_bytes_as_creator(&creator(), "/priv/locks.app/content/example.txt")
            .await
            .unwrap();
        client
            .delete_as_creator(&creator(), "/priv/locks.app/proofs/example.json")
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
                "put_json /pub/locks.app/config.json".to_owned(),
                "get_bytes /priv/locks.app/content/example.txt".to_owned(),
                "delete /priv/locks.app/proofs/example.json".to_owned(),
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
                "/priv/locks.app/content/example.txt",
                b"secret".to_vec(),
                "text/plain",
            )
            .await;

        assert_eq!(result, Err(ApplicationError::CreatorAuthorityUnavailable));
        assert_eq!(provider.seen_creators(), vec![creator()]);
        assert_eq!(provider.storage_operations(), Vec::<String>::new());
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
            .put_json_value("/pub/locks.app/config.json", json!({"version": 1}))
            .await
            .unwrap();

        assert_eq!(
            provider.importer().seen_secrets(),
            vec!["legacy-cookie-session-secret".to_owned()]
        );
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
                        "/pub/locks.app/:rw".to_owned(),
                        "/priv/locks.app/:rw".to_owned(),
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
        ) -> Result<Box<dyn CreatorScopedPubkyStorage>, ApplicationError> {
            self.seen_creators.lock().unwrap().push(creator.clone());
            self.result
                .clone()
                .map(|storage| Box::new(storage) as Box<dyn CreatorScopedPubkyStorage>)
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
                "/pub/locks.app/:rw".to_owned(),
                "/priv/locks.app/:rw".to_owned(),
            ],
            secret: CreatorAuthoritySecret::new(secret),
            session_expires_at: Some(datetime!(2026-05-29 12:15:00 UTC)),
            last_revalidated_at: None,
        }
    }

    fn storage_error(
        result: Result<Box<dyn CreatorScopedPubkyStorage>, ApplicationError>,
    ) -> ApplicationError {
        match result {
            Ok(_) => panic!("expected storage provider error"),
            Err(error) => error,
        }
    }

    fn assert_storage_error(
        result: Result<Box<dyn CreatorScopedPubkyStorage>, ApplicationError>,
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
