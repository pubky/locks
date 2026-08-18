use async_trait::async_trait;
use locks_core::ids::{CreatorPubky, GuardedResourceHash};
use locks_core::lock_policy::PRIVATE_RESOURCE_CONTENT_PATH_PREFIX;

use crate::application::errors::ApplicationError;
use crate::application::models::GuardedResourceRecord;
use crate::application::ports::GuardedResourceRepository;
use crate::infrastructure::pubky::storage_client::{
    PubkyBytesResource, PubkyHomeserverStorageClient,
};

const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";

/// Pubky homeserver-backed repository for private guarded resource bytes.
#[derive(Debug)]
pub struct PubkyPrivResourceRepository<C> {
    client: C,
}

impl<C> PubkyPrivResourceRepository<C> {
    /// Creates a repository backed by a Pubky homeserver storage client.
    pub fn new(client: C) -> Self {
        Self { client }
    }

    /// Returns the storage client. Exposed for tests and composition.
    pub fn client(&self) -> &C {
        &self.client
    }
}

#[async_trait]
impl<C> GuardedResourceRepository for PubkyPrivResourceRepository<C>
where
    C: PubkyHomeserverStorageClient,
{
    async fn upsert_guarded_resource(
        &self,
        guarded_resource: GuardedResourceRecord,
    ) -> Result<(), ApplicationError> {
        ensure_private_resource_path(&guarded_resource.path)?;
        self.client
            .put_bytes_as_creator(
                &guarded_resource.creator,
                &guarded_resource.path,
                guarded_resource.bytes,
                &guarded_resource.content_type,
            )
            .await
    }

    async fn get_guarded_resource(
        &self,
        creator: &CreatorPubky,
        path: &str,
        hash: &GuardedResourceHash,
    ) -> Result<Option<GuardedResourceRecord>, ApplicationError> {
        ensure_private_resource_path(path)?;
        let Some(resource) = self.client.get_bytes_as_creator(creator, path).await? else {
            return Ok(None);
        };
        let record = record_from_resource(creator, path, resource)?;
        if record.hash == *hash {
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }

    async fn get_current_guarded_resource(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<Option<GuardedResourceRecord>, ApplicationError> {
        ensure_private_resource_path(path)?;
        self.client
            .get_bytes_as_creator(creator, path)
            .await?
            .map(|resource| record_from_resource(creator, path, resource))
            .transpose()
    }

    async fn delete_guarded_resource(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<bool, ApplicationError> {
        ensure_private_resource_path(path)?;
        let existed = self
            .client
            .get_bytes_as_creator(creator, path)
            .await?
            .is_some();
        if existed {
            self.client.delete_as_creator(creator, path).await?;
        }
        Ok(existed)
    }
}

fn ensure_private_resource_path(path: &str) -> Result<(), ApplicationError> {
    if !path.starts_with(PRIVATE_RESOURCE_CONTENT_PATH_PREFIX)
        || path == PRIVATE_RESOURCE_CONTENT_PATH_PREFIX
        || path.contains("..")
        || path.contains("//")
        || path.contains("://")
    {
        return Err(ApplicationError::InvalidGuardedResource {
            message: "guarded resource path must be under /priv/locks.app/content/".to_owned(),
        });
    }
    Ok(())
}

fn record_from_resource(
    creator: &CreatorPubky,
    path: &str,
    resource: PubkyBytesResource,
) -> Result<GuardedResourceRecord, ApplicationError> {
    let size = u64::try_from(resource.bytes.len()).map_err(|_| {
        ApplicationError::InvalidGuardedResource {
            message: "guarded resource size exceeds u64".to_owned(),
        }
    })?;
    let hash = GuardedResourceHash::from_bytes(*blake3::hash(&resource.bytes).as_bytes());
    Ok(GuardedResourceRecord {
        creator: creator.clone(),
        path: path.to_owned(),
        hash,
        content_type: resource
            .content_type
            .unwrap_or_else(|| DEFAULT_CONTENT_TYPE.to_owned()),
        size,
        bytes: resource.bytes,
    })
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    };

    use async_trait::async_trait;
    use locks_core::ids::{CreatorPubky, GuardedResourceHash};

    use super::PubkyPrivResourceRepository;
    use crate::application::errors::ApplicationError;
    use crate::application::models::GuardedResourceRecord;
    use crate::application::ports::GuardedResourceRepository;
    use crate::infrastructure::pubky::storage_client::{
        PubkyBytesResource, PubkyHomeserverStorageClient,
    };

    #[tokio::test]
    async fn upsert_guarded_resource_writes_bytes_to_exact_private_path() {
        let client = FakeStorageClient::default();
        let repository = PubkyPrivResourceRepository::new(client);
        let record = resource_record(b"guarded bytes".to_vec(), "text/plain");

        repository
            .upsert_guarded_resource(record.clone())
            .await
            .unwrap();

        assert_eq!(
            repository.client().operations(),
            vec![
                "put_bytes pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy /priv/locks.app/content/example.txt text/plain"
                    .to_owned()
            ]
        );
        assert_eq!(repository.client().last_bytes(), record.bytes);
    }

    #[tokio::test]
    async fn upsert_guarded_resource_rejects_paths_outside_private_content_namespace() {
        let repository = PubkyPrivResourceRepository::new(FakeStorageClient::default());
        let mut record = resource_record(b"guarded bytes".to_vec(), "text/plain");
        record.path = "/pub/locks.app/content/example.txt".to_owned();

        let error = repository
            .upsert_guarded_resource(record)
            .await
            .unwrap_err();

        assert_eq!(
            error,
            ApplicationError::InvalidGuardedResource {
                message: "guarded resource path must be under /priv/locks.app/content/".to_owned(),
            }
        );
        assert_eq!(repository.client().operations(), Vec::<String>::new());
    }

    #[tokio::test]
    async fn get_guarded_resource_returns_record_when_fetched_bytes_match_expected_hash() {
        let bytes = b"guarded bytes".to_vec();
        let expected_hash = hash(&bytes);
        let client = FakeStorageClient::default().with_bytes_read(Some(PubkyBytesResource {
            bytes: bytes.clone(),
            content_type: Some("text/plain".to_owned()),
        }));
        let repository = PubkyPrivResourceRepository::new(client);

        let loaded = repository
            .get_guarded_resource(
                &creator(),
                "/priv/locks.app/content/example.txt",
                &expected_hash,
            )
            .await
            .unwrap();

        assert_eq!(
            loaded,
            Some(GuardedResourceRecord {
                creator: creator(),
                path: "/priv/locks.app/content/example.txt".to_owned(),
                hash: expected_hash,
                content_type: "text/plain".to_owned(),
                size: bytes.len() as u64,
                bytes,
            })
        );
    }

    #[tokio::test]
    async fn get_guarded_resource_returns_none_when_fetched_bytes_hash_differs() {
        let client = FakeStorageClient::default().with_bytes_read(Some(PubkyBytesResource {
            bytes: b"tampered bytes".to_vec(),
            content_type: Some("text/plain".to_owned()),
        }));
        let repository = PubkyPrivResourceRepository::new(client);

        let loaded = repository
            .get_guarded_resource(
                &creator(),
                "/priv/locks.app/content/example.txt",
                &hash(b"guarded bytes"),
            )
            .await
            .unwrap();

        assert_eq!(loaded, None);
    }

    #[tokio::test]
    async fn get_guarded_resource_returns_none_when_homeserver_resource_is_missing() {
        let repository = PubkyPrivResourceRepository::new(FakeStorageClient::default());

        let loaded = repository
            .get_guarded_resource(
                &creator(),
                "/priv/locks.app/content/example.txt",
                &hash(b"guarded bytes"),
            )
            .await
            .unwrap();

        assert_eq!(loaded, None);
    }

    #[tokio::test]
    async fn get_current_guarded_resource_derives_hash_size_and_default_content_type_from_fetched_bytes()
     {
        let bytes = b"guarded bytes".to_vec();
        let client = FakeStorageClient::default().with_bytes_read(Some(PubkyBytesResource {
            bytes: bytes.clone(),
            content_type: None,
        }));
        let repository = PubkyPrivResourceRepository::new(client);

        let loaded = repository
            .get_current_guarded_resource(&creator(), "/priv/locks.app/content/example.txt")
            .await
            .unwrap();

        assert_eq!(
            loaded,
            Some(GuardedResourceRecord {
                creator: creator(),
                path: "/priv/locks.app/content/example.txt".to_owned(),
                hash: hash(&bytes),
                content_type: "application/octet-stream".to_owned(),
                size: bytes.len() as u64,
                bytes,
            })
        );
    }

    #[tokio::test]
    async fn delete_guarded_resource_deletes_existing_current_resource_only() {
        let client = FakeStorageClient::default().with_bytes_read(Some(PubkyBytesResource {
            bytes: b"guarded bytes".to_vec(),
            content_type: Some("text/plain".to_owned()),
        }));
        let repository = PubkyPrivResourceRepository::new(client);

        let deleted = repository
            .delete_guarded_resource(&creator(), "/priv/locks.app/content/example.txt")
            .await
            .unwrap();

        assert!(deleted);
        assert_eq!(
            repository.client().operations(),
            vec![
                "get_bytes pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy /priv/locks.app/content/example.txt".to_owned(),
                "delete pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy /priv/locks.app/content/example.txt".to_owned(),
            ]
        );
    }

    #[tokio::test]
    async fn delete_guarded_resource_reports_missing_without_delete_call() {
        let repository = PubkyPrivResourceRepository::new(FakeStorageClient::default());

        let deleted = repository
            .delete_guarded_resource(&creator(), "/priv/locks.app/content/example.txt")
            .await
            .unwrap();

        assert!(!deleted);
        assert_eq!(
            repository.client().operations(),
            vec![
                "get_bytes pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy /priv/locks.app/content/example.txt".to_owned(),
            ]
        );
    }

    #[derive(Debug, Default)]
    struct FakeStorageClient {
        bytes_read: Mutex<Option<PubkyBytesResource>>,
        last_bytes: Mutex<Option<Vec<u8>>>,
        operations: Mutex<Vec<String>>,
        fail_get: AtomicBool,
    }

    impl FakeStorageClient {
        fn with_bytes_read(self, value: Option<PubkyBytesResource>) -> Self {
            *self.bytes_read.lock().unwrap() = value;
            self
        }

        fn operations(&self) -> Vec<String> {
            self.operations.lock().unwrap().clone()
        }

        fn last_bytes(&self) -> Vec<u8> {
            self.last_bytes
                .lock()
                .unwrap()
                .clone()
                .expect("bytes recorded")
        }
    }

    #[async_trait]
    impl PubkyHomeserverStorageClient for FakeStorageClient {
        async fn put_json_value_as_creator(
            &self,
            _creator: &CreatorPubky,
            _path: &str,
            _body: serde_json::Value,
        ) -> Result<(), ApplicationError> {
            unimplemented!("not needed by private resource repository tests")
        }

        async fn get_json_value_as_creator(
            &self,
            _creator: &CreatorPubky,
            _path: &str,
        ) -> Result<Option<serde_json::Value>, ApplicationError> {
            unimplemented!("not needed by private resource repository tests")
        }

        async fn put_bytes_as_creator(
            &self,
            creator: &CreatorPubky,
            path: &str,
            bytes: Vec<u8>,
            content_type: &str,
        ) -> Result<(), ApplicationError> {
            self.operations
                .lock()
                .unwrap()
                .push(format!("put_bytes {creator} {path} {content_type}"));
            *self.last_bytes.lock().unwrap() = Some(bytes);
            Ok(())
        }

        async fn get_bytes_as_creator(
            &self,
            creator: &CreatorPubky,
            path: &str,
        ) -> Result<Option<PubkyBytesResource>, ApplicationError> {
            if self.fail_get.load(Ordering::SeqCst) {
                return Err(ApplicationError::InvalidGuardedResource {
                    message: "storage read failed".to_owned(),
                });
            }
            self.operations
                .lock()
                .unwrap()
                .push(format!("get_bytes {creator} {path}"));
            Ok(self.bytes_read.lock().unwrap().clone())
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
            *self.bytes_read.lock().unwrap() = None;
            Ok(())
        }
    }

    fn resource_record(bytes: Vec<u8>, content_type: &str) -> GuardedResourceRecord {
        GuardedResourceRecord {
            creator: creator(),
            path: "/priv/locks.app/content/example.txt".to_owned(),
            hash: hash(&bytes),
            content_type: content_type.to_owned(),
            size: bytes.len() as u64,
            bytes,
        }
    }

    fn creator() -> CreatorPubky {
        CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
    }

    fn hash(bytes: impl AsRef<[u8]>) -> GuardedResourceHash {
        GuardedResourceHash::from_bytes(*blake3::hash(bytes.as_ref()).as_bytes())
    }
}
