use async_trait::async_trait;
use locks_core::ids::CreatorPubky;
use locks_core::lock_service_pointer::{LOCK_SERVICE_POINTER_PATH, LockServicePointer};

use crate::application::errors::ApplicationError;
use crate::application::ports::LockServicePointerRepository;
use crate::infrastructure::pubky::storage_client::PubkyHomeserverStorageClient;

/// Pubky homeserver-backed repository for creator-owned Lock Service Pointers.
#[derive(Debug)]
pub struct PubkyLockServicePointerRepository<C> {
    client: C,
}

impl<C> PubkyLockServicePointerRepository<C> {
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
impl<C> LockServicePointerRepository for PubkyLockServicePointerRepository<C>
where
    C: PubkyHomeserverStorageClient,
{
    async fn upsert_lock_service_pointer(
        &self,
        creator: CreatorPubky,
        pointer: LockServicePointer,
    ) -> Result<(), ApplicationError> {
        let body = serde_json::to_value(pointer).map_err(|error| ApplicationError::Storage {
            message: format!("failed to serialize Lock Service Pointer for Pubky storage: {error}"),
        })?;
        self.client
            .put_json_value_as_creator(&creator, LOCK_SERVICE_POINTER_PATH, body)
            .await
    }

    async fn get_lock_service_pointer(
        &self,
        creator: &CreatorPubky,
    ) -> Result<Option<LockServicePointer>, ApplicationError> {
        self.client
            .get_json_value_as_creator(creator, LOCK_SERVICE_POINTER_PATH)
            .await?
            .map(|value| {
                serde_json::from_value(value).map_err(|error| ApplicationError::Storage {
                    message: format!(
                        "failed to deserialize Lock Service Pointer from Pubky storage: {error}"
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
    use locks_core::ids::{CreatorPubky, LockServerPubky};
    use locks_core::lock_service_pointer::{
        LOCK_SERVICE_POINTER_PATH, LOCK_SERVICE_POINTER_VERSION, LockServicePointer,
    };
    use time::macros::datetime;

    use super::PubkyLockServicePointerRepository;
    use crate::application::errors::ApplicationError;
    use crate::application::ports::LockServicePointerRepository;
    use crate::infrastructure::pubky::storage_client::{
        PubkyBytesResource, PubkyHomeserverStorageClient,
    };

    #[tokio::test]
    async fn upsert_lock_service_pointer_writes_json_to_canonical_pubky_config_path() {
        let client = FakeStorageClient::default();
        let repository = PubkyLockServicePointerRepository::new(client);
        let pointer = pointer();

        repository
            .upsert_lock_service_pointer(creator(), pointer.clone())
            .await
            .unwrap();

        assert_eq!(
            repository.client().operations(),
            vec![format!(
                "put_json pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy {LOCK_SERVICE_POINTER_PATH}"
            )]
        );
        assert_eq!(
            repository.client().last_json_body(),
            serde_json::to_value(pointer).unwrap()
        );
    }

    #[tokio::test]
    async fn get_lock_service_pointer_reads_json_from_canonical_pubky_config_path() {
        let client = FakeStorageClient::default()
            .with_json_read(Some(serde_json::to_value(pointer()).unwrap()));
        let repository = PubkyLockServicePointerRepository::new(client);

        let loaded = repository
            .get_lock_service_pointer(&creator())
            .await
            .unwrap();

        assert_eq!(loaded, Some(pointer()));
        assert_eq!(
            repository.client().operations(),
            vec![format!(
                "get_json pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy {LOCK_SERVICE_POINTER_PATH}"
            )]
        );
    }

    #[tokio::test]
    async fn get_lock_service_pointer_returns_none_when_homeserver_resource_is_missing() {
        let client = FakeStorageClient::default().with_json_read(None);
        let repository = PubkyLockServicePointerRepository::new(client);

        let loaded = repository
            .get_lock_service_pointer(&creator())
            .await
            .unwrap();

        assert_eq!(loaded, None);
    }

    #[tokio::test]
    async fn storage_errors_are_propagated() {
        let client = FakeStorageClient::default().with_error(ApplicationError::Storage {
            message: "pubky unavailable".to_owned(),
        });
        let repository = PubkyLockServicePointerRepository::new(client);

        assert_eq!(
            repository
                .get_lock_service_pointer(&creator())
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
            unimplemented!("not needed by lock service pointer repository tests")
        }

        async fn get_bytes_as_creator(
            &self,
            _creator: &CreatorPubky,
            _path: &str,
        ) -> Result<Option<PubkyBytesResource>, ApplicationError> {
            unimplemented!("not needed by lock service pointer repository tests")
        }

        async fn delete_as_creator(
            &self,
            _creator: &CreatorPubky,
            _path: &str,
        ) -> Result<(), ApplicationError> {
            unimplemented!("not needed by lock service pointer repository tests")
        }
    }

    fn pointer() -> LockServicePointer {
        LockServicePointer {
            version: LOCK_SERVICE_POINTER_VERSION,
            default_lock_server: LockServerPubky::from_str(
                "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
            )
            .unwrap(),
            created_at: datetime!(2026-06-03 00:00:00 UTC),
        }
    }

    fn creator() -> CreatorPubky {
        CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
    }
}
