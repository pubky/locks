use async_trait::async_trait;
use locks_core::ids::{ContentLockPath, CreatorPubky};
use locks_core::lock_policy::ContentLock;

use crate::application::errors::ApplicationError;
use crate::application::ports::ContentLockRepository;
use crate::infrastructure::pubky::storage_client::PubkyHomeserverStorageClient;

/// Pubky homeserver-backed repository for public content lock payloads.
#[derive(Debug)]
pub struct PubkyContentLockRepository<C> {
    client: C,
}

impl<C> PubkyContentLockRepository<C> {
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
impl<C> ContentLockRepository for PubkyContentLockRepository<C>
where
    C: PubkyHomeserverStorageClient,
{
    async fn upsert_content_lock(
        &self,
        creator: CreatorPubky,
        content_lock_path: ContentLockPath,
        content_lock: ContentLock,
    ) -> Result<(), ApplicationError> {
        let body =
            serde_json::to_value(content_lock).map_err(|error| ApplicationError::Storage {
                message: format!("failed to serialize content lock for Pubky storage: {error}"),
            })?;
        self.client
            .put_json_value_as_creator(&creator, &content_lock_path.to_string(), body)
            .await
    }

    async fn get_content_lock(
        &self,
        creator: &CreatorPubky,
        content_lock_path: &ContentLockPath,
    ) -> Result<Option<ContentLock>, ApplicationError> {
        self.client
            .get_json_value_as_creator(creator, &content_lock_path.to_string())
            .await?
            .map(|value| {
                serde_json::from_value(value).map_err(|error| ApplicationError::Storage {
                    message: format!(
                        "failed to deserialize content lock from Pubky storage: {error}"
                    ),
                })
            })
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use locks_core::ids::{CreatorPubky, GuardedResourceHash, LockServerPubky};
    use locks_core::lock_policy::{
        AccessPolicy, CONTENT_LOCK_VERSION, ContentLock, Criterion, GuardedResource, LockLogic,
        LockServerConfig, VerifierType,
    };
    use serde_json::json;
    use time::macros::datetime;

    use super::PubkyContentLockRepository;
    use crate::application::errors::ApplicationError;
    use crate::application::ports::ContentLockRepository;
    use crate::infrastructure::pubky::storage_client::{
        PubkyBytesResource, PubkyHomeserverStorageClient,
    };

    #[tokio::test]
    async fn upsert_content_lock_writes_json_to_provided_content_lock_path_exactly() {
        let client = FakeStorageClient::default();
        let repository = PubkyContentLockRepository::new(client);
        let payload = content_lock(900);
        let provided_path = content_lock(901).content_lock_path().unwrap();

        repository
            .upsert_content_lock(creator(), provided_path.clone(), payload.clone())
            .await
            .unwrap();

        assert_ne!(provided_path, payload.content_lock_path().unwrap());
        assert_eq!(
            repository.client().operations(),
            vec![format!(
                "put_json pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy {provided_path}"
            )]
        );
        assert_eq!(
            repository.client().last_json_body(),
            serde_json::to_value(payload).unwrap()
        );
    }

    #[tokio::test]
    async fn get_content_lock_reads_json_from_provided_content_lock_path_exactly() {
        let payload = content_lock(900);
        let requested_path = content_lock(901).content_lock_path().unwrap();
        let client = FakeStorageClient::default()
            .with_json_read(Some(serde_json::to_value(payload.clone()).unwrap()));
        let repository = PubkyContentLockRepository::new(client);

        let loaded = repository
            .get_content_lock(&creator(), &requested_path)
            .await
            .unwrap();

        assert_eq!(loaded, Some(payload));
        assert_eq!(
            repository.client().operations(),
            vec![format!(
                "get_json pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy {requested_path}"
            )]
        );
    }

    #[tokio::test]
    async fn get_content_lock_returns_none_when_homeserver_resource_is_missing() {
        let repository = PubkyContentLockRepository::new(FakeStorageClient::default());
        let requested_path = content_lock(900).content_lock_path().unwrap();

        let loaded = repository
            .get_content_lock(&creator(), &requested_path)
            .await
            .unwrap();

        assert_eq!(loaded, None);
    }

    #[tokio::test]
    async fn storage_errors_are_propagated() {
        let repository = PubkyContentLockRepository::new(FakeStorageClient::default().with_error(
            ApplicationError::Storage {
                message: "pubky unavailable".to_owned(),
            },
        ));
        let requested_path = content_lock(900).content_lock_path().unwrap();

        assert_eq!(
            repository
                .get_content_lock(&creator(), &requested_path)
                .await
                .unwrap_err(),
            ApplicationError::Storage {
                message: "pubky unavailable".to_owned(),
            }
        );
    }

    #[derive(Debug, Default)]
    struct FakeStorageClient {
        json_read: Mutex<Option<serde_json::Value>>,
        last_json_body: Mutex<Option<serde_json::Value>>,
        operations: Mutex<Vec<String>>,
        error: Mutex<Option<ApplicationError>>,
    }

    impl FakeStorageClient {
        fn with_json_read(self, value: Option<serde_json::Value>) -> Self {
            *self.json_read.lock().unwrap() = value;
            self
        }

        fn with_error(self, error: ApplicationError) -> Self {
            *self.error.lock().unwrap() = Some(error);
            self
        }

        fn operations(&self) -> Vec<String> {
            self.operations.lock().unwrap().clone()
        }

        fn last_json_body(&self) -> serde_json::Value {
            self.last_json_body
                .lock()
                .unwrap()
                .clone()
                .expect("json body recorded")
        }

        fn maybe_error(&self) -> Result<(), ApplicationError> {
            if let Some(error) = self.error.lock().unwrap().clone() {
                return Err(error);
            }
            Ok(())
        }
    }

    #[async_trait]
    impl PubkyHomeserverStorageClient for FakeStorageClient {
        async fn put_json_value_as_creator(
            &self,
            creator: &CreatorPubky,
            path: &str,
            body: serde_json::Value,
        ) -> Result<(), ApplicationError> {
            self.maybe_error()?;
            self.operations
                .lock()
                .unwrap()
                .push(format!("put_json {creator} {path}"));
            *self.last_json_body.lock().unwrap() = Some(body);
            Ok(())
        }

        async fn get_json_value_as_creator(
            &self,
            creator: &CreatorPubky,
            path: &str,
        ) -> Result<Option<serde_json::Value>, ApplicationError> {
            self.maybe_error()?;
            self.operations
                .lock()
                .unwrap()
                .push(format!("get_json {creator} {path}"));
            Ok(self.json_read.lock().unwrap().clone())
        }

        async fn put_bytes_as_creator(
            &self,
            _creator: &CreatorPubky,
            _path: &str,
            _bytes: Vec<u8>,
            _content_type: &str,
        ) -> Result<(), ApplicationError> {
            unimplemented!("not needed by content lock repository tests")
        }

        async fn get_bytes_as_creator(
            &self,
            _creator: &CreatorPubky,
            _path: &str,
        ) -> Result<Option<PubkyBytesResource>, ApplicationError> {
            unimplemented!("not needed by content lock repository tests")
        }

        async fn delete_as_creator(
            &self,
            _creator: &CreatorPubky,
            _path: &str,
        ) -> Result<(), ApplicationError> {
            unimplemented!("not needed by content lock repository tests")
        }
    }

    fn creator() -> CreatorPubky {
        CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
    }

    fn server() -> LockServerPubky {
        LockServerPubky::from_str("pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo")
            .unwrap()
    }

    fn content_lock(ttl: u64) -> ContentLock {
        ContentLock {
            version: CONTENT_LOCK_VERSION,
            creator: creator(),
            primary_resource: Some(GuardedResource {
                path: "/priv/locks.app/content/hello.txt".to_owned(),
                hash: GuardedResourceHash::from_bytes([7; 32]),
                content_type: "text/plain".to_owned(),
                size: 13,
            }),
            secondary_resources: Default::default(),
            criteria: vec![Criterion {
                criterion_id: "criterion-1".to_owned(),
                verifier_type: VerifierType::DevStatic,
                params: json!({ "satisfied": true }),
            }],
            lock_logic: LockLogic::All {
                criteria: vec!["criterion-1".to_owned()],
            },
            access_policy: AccessPolicy {
                requested_credential_ttl_seconds: ttl,
            },
            lock_server: LockServerConfig {
                override_: Some(server()),
            },
            created_at: datetime!(2026-05-29 12:00:00 UTC),
        }
    }
}
