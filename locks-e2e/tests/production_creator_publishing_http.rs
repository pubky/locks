mod support;

use std::str::FromStr;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::http::StatusCode;
use locks_core::ids::{ContentLockPath, CreatorPubky};
use locks_server::app_state::AppState;
use locks_server::config::RuntimeEnvironment;
use locks_server::testing::TestServerApp;
use locks_service::application::errors::ApplicationError;
use locks_service::application::models::{CreatorAuthorityAuthKind, FrontendSessionToken};
use locks_service::application::ports::{CreatorAuthorityManager, CreatorAuthorityStatus};
use locks_service::infrastructure::pubky::{
    AuthorizingPubkyHomeserverStorageClient, PubkyBytesResource, PubkyContentLockRepository,
    PubkyContentLockTombstoneRepository, PubkyEntitlementRepository, PubkyHomeserverStorageClient,
    PubkyLockServicePointerRepository, PubkyPrivResourceRepository,
};
use serde_json::json;
use support::creator_publishing_client::LocalCreatorPublishingClient;
use time::Duration;

const FRONTEND_SESSION_TOKEN: &str = "frontend-session-secret";
const CREATOR_AUTHORITY_SECRET: &str = "creator-authority-secret";
const GUARDED_BYTES: &[u8] = b"production creator publishing bytes";

#[tokio::test]
async fn production_creator_publishing_http_flow_writes_to_pubky_storage_when_frontend_session_is_authorized()
 {
    let storage = FakePubkyHomeserverStorage::default();
    let manager = FakeCreatorAuthorityManager::authorized();
    let mut config = TestServerApp::default_in_memory_config();
    config.runtime.environment = RuntimeEnvironment::Production;

    let state = AppState::new_empty_in_memory_with_creator_repositories(
        config,
        Arc::new(PubkyContentLockRepository::new(authorizing_storage(
            storage.clone(),
            manager.clone(),
        ))),
        Arc::new(PubkyContentLockTombstoneRepository::new(
            authorizing_storage(storage.clone(), manager.clone()),
        )),
        Arc::new(PubkyPrivResourceRepository::new(authorizing_storage(
            storage.clone(),
            manager.clone(),
        ))),
        Arc::new(PubkyLockServicePointerRepository::new(authorizing_storage(
            storage.clone(),
            manager.clone(),
        ))),
        Arc::new(PubkyEntitlementRepository::new(authorizing_storage(
            storage.clone(),
            manager.clone(),
        ))),
    );
    let test_app = TestServerApp::from_state(state);
    test_app
        .insert_frontend_session_for_test(
            FrontendSessionToken::new(FRONTEND_SESSION_TOKEN),
            creator(),
            time::OffsetDateTime::now_utc() + Duration::hours(12),
        )
        .await
        .unwrap();
    let router = test_app.router();
    let client = LocalCreatorPublishingClient::new(router.clone())
        .with_frontend_session_token(FRONTEND_SESSION_TOKEN);

    let missing_token_error = LocalCreatorPublishingClient::new(router)
        .register_guarded_resource("example.txt", "text/plain", GUARDED_BYTES)
        .await
        .unwrap_err();
    assert_eq!(missing_token_error.status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        missing_token_error.body["error"]["code"],
        "frontend_session_unavailable"
    );
    assert_secret_free(&missing_token_error.body);

    let pointer_json = client
        .set_lock_service_config("pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo")
        .await
        .unwrap();
    assert_eq!(pointer_json["path"], "/pub/locks.app/config.json");
    assert_secret_free(&pointer_json);

    let guarded_json = client
        .register_guarded_resource("example.txt", "text/plain", GUARDED_BYTES)
        .await
        .unwrap();
    let guarded_resource = guarded_json["guarded_resource"].clone();
    assert_eq!(
        guarded_resource["path"],
        "/priv/locks.app/content/example.txt"
    );
    assert_secret_free(&guarded_json);

    let content_lock_json = client
        .create_content_lock(
            guarded_resource.clone(),
            json!([{
                "criterion_id": "criterion-1",
                "verifier_type": "dev-static",
                "params": { "satisfied": true }
            }]),
            json!({ "type": "all", "criteria": ["criterion-1"] }),
            json!({ "requested_credential_ttl_seconds": 900 }),
            json!({ "override": "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo" }),
        )
        .await
        .unwrap();
    let conflict = client
        .create_content_lock(
            guarded_resource,
            json!([{
                "criterion_id": "criterion-1",
                "verifier_type": "dev-static",
                "params": { "satisfied": false }
            }]),
            json!({ "type": "all", "criteria": ["criterion-1"] }),
            json!({ "requested_credential_ttl_seconds": 900 }),
            json!({ "override": "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo" }),
        )
        .await
        .unwrap_err();
    assert_eq!(conflict.status, StatusCode::CONFLICT);
    assert_eq!(conflict.body["error"]["code"], "content_lock_path_conflict");
    assert_secret_free(&conflict.body);
    let content_lock_path = ContentLockPath::from_str(
        content_lock_json["content_lock_path"]
            .as_str()
            .expect("content_lock_path is string"),
    )
    .unwrap();
    assert_secret_free(&content_lock_json);

    let operations = storage.operations();
    assert!(
        operations.iter().any(|operation| {
            operation == "put_json pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy /pub/locks.app/config.json"
        })
    );
    assert!(operations.iter().any(|operation| {
        operation == "put_bytes pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy /priv/locks.app/content/example.txt text/plain"
    }));
    assert!(operations.iter().any(|operation| {
        operation == &format!("put_json pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy {content_lock_path}")
    }));
    assert!(manager.required_creators().contains(&creator().to_string()));
}

#[tokio::test]
async fn production_creator_publishing_http_returns_creator_authority_unavailable_before_pubky_storage_write()
 {
    let storage = FakePubkyHomeserverStorage::default();
    let manager = FakeCreatorAuthorityManager::unavailable();
    let mut config = TestServerApp::default_in_memory_config();
    config.runtime.environment = RuntimeEnvironment::Production;

    let state = AppState::new_empty_in_memory_with_creator_repositories(
        config,
        Arc::new(PubkyContentLockRepository::new(authorizing_storage(
            storage.clone(),
            manager.clone(),
        ))),
        Arc::new(PubkyContentLockTombstoneRepository::new(
            authorizing_storage(storage.clone(), manager.clone()),
        )),
        Arc::new(PubkyPrivResourceRepository::new(authorizing_storage(
            storage.clone(),
            manager.clone(),
        ))),
        Arc::new(PubkyLockServicePointerRepository::new(authorizing_storage(
            storage.clone(),
            manager.clone(),
        ))),
        Arc::new(PubkyEntitlementRepository::new(authorizing_storage(
            storage.clone(),
            manager.clone(),
        ))),
    );
    let test_app = TestServerApp::from_state(state);
    test_app
        .insert_frontend_session_for_test(
            FrontendSessionToken::new(FRONTEND_SESSION_TOKEN),
            creator(),
            time::OffsetDateTime::now_utc() + Duration::hours(12),
        )
        .await
        .unwrap();
    let client = LocalCreatorPublishingClient::new(test_app.router())
        .with_frontend_session_token(FRONTEND_SESSION_TOKEN);

    let error = client
        .register_guarded_resource("example.txt", "text/plain", GUARDED_BYTES)
        .await
        .unwrap_err();

    assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(error.body["error"]["code"], "creator_authority_unavailable");
    assert_secret_free(&error.body);
    assert_eq!(manager.required_creators(), vec![creator().to_string()]);
    assert_eq!(storage.operations(), Vec::<String>::new());
}

fn authorizing_storage(
    storage: FakePubkyHomeserverStorage,
    manager: FakeCreatorAuthorityManager,
) -> AuthorizingPubkyHomeserverStorageClient<FakePubkyHomeserverStorage, FakeCreatorAuthorityManager>
{
    AuthorizingPubkyHomeserverStorageClient::new(storage, manager)
}

fn assert_secret_free(value: &serde_json::Value) {
    let rendered = value.to_string();
    assert!(!rendered.contains(FRONTEND_SESSION_TOKEN));
    assert!(!rendered.contains(CREATOR_AUTHORITY_SECRET));
}

fn creator() -> CreatorPubky {
    CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
}

#[derive(Debug, Clone)]
struct FakeCreatorAuthorityManager {
    inner: Arc<Mutex<FakeCreatorAuthorityManagerInner>>,
}

impl FakeCreatorAuthorityManager {
    fn authorized() -> Self {
        Self {
            inner: Arc::new(Mutex::new(FakeCreatorAuthorityManagerInner {
                authorized: true,
                ..FakeCreatorAuthorityManagerInner::default()
            })),
        }
    }

    fn unavailable() -> Self {
        Self {
            inner: Arc::new(Mutex::new(FakeCreatorAuthorityManagerInner {
                authorized: false,
                ..FakeCreatorAuthorityManagerInner::default()
            })),
        }
    }

    fn required_creators(&self) -> Vec<String> {
        self.inner.lock().unwrap().required_creators.clone()
    }
}

#[derive(Debug, Default)]
struct FakeCreatorAuthorityManagerInner {
    authorized: bool,
    required_creators: Vec<String>,
}

#[async_trait]
impl CreatorAuthorityManager for FakeCreatorAuthorityManager {
    async fn revalidate_creator_authority(
        &self,
        creator: &CreatorPubky,
    ) -> Result<CreatorAuthorityStatus, ApplicationError> {
        self.require_creator_authority(creator).await
    }

    async fn require_creator_authority(
        &self,
        creator: &CreatorPubky,
    ) -> Result<CreatorAuthorityStatus, ApplicationError> {
        let mut inner = self.inner.lock().unwrap();
        inner.required_creators.push(creator.to_string());
        if !inner.authorized {
            return Err(ApplicationError::CreatorAuthorityUnavailable);
        }
        Ok(CreatorAuthorityStatus {
            creator: creator.clone(),
            auth_kind: CreatorAuthorityAuthKind::LegacyCookie,
            authorized: true,
            granted_scopes: vec![
                "/pub/locks.app/:rw".to_owned(),
                "/priv/locks.app/:rw".to_owned(),
            ],
            session_expires_at: None,
        })
    }
}

#[derive(Debug, Clone, Default)]
struct FakePubkyHomeserverStorage {
    inner: Arc<Mutex<FakePubkyHomeserverStorageInner>>,
}

impl FakePubkyHomeserverStorage {
    fn operations(&self) -> Vec<String> {
        self.inner.lock().unwrap().operations.clone()
    }
}

#[derive(Debug, Default)]
struct FakePubkyHomeserverStorageInner {
    operations: Vec<String>,
    json: std::collections::BTreeMap<(String, String), serde_json::Value>,
    bytes: std::collections::BTreeMap<(String, String), PubkyBytesResource>,
}

#[async_trait]
impl PubkyHomeserverStorageClient for FakePubkyHomeserverStorage {
    async fn put_json_value_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
        body: serde_json::Value,
    ) -> Result<(), ApplicationError> {
        let mut inner = self.inner.lock().unwrap();
        inner.operations.push(format!("put_json {creator} {path}"));
        inner
            .json
            .insert((creator.to_string(), path.to_owned()), body);
        Ok(())
    }

    async fn get_json_value_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<Option<serde_json::Value>, ApplicationError> {
        let mut inner = self.inner.lock().unwrap();
        inner.operations.push(format!("get_json {creator} {path}"));
        Ok(inner
            .json
            .get(&(creator.to_string(), path.to_owned()))
            .cloned())
    }

    async fn put_bytes_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
        bytes: Vec<u8>,
        content_type: &str,
    ) -> Result<(), ApplicationError> {
        let mut inner = self.inner.lock().unwrap();
        inner
            .operations
            .push(format!("put_bytes {creator} {path} {content_type}"));
        inner.bytes.insert(
            (creator.to_string(), path.to_owned()),
            PubkyBytesResource {
                bytes,
                content_type: Some(content_type.to_owned()),
            },
        );
        Ok(())
    }

    async fn get_bytes_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<Option<PubkyBytesResource>, ApplicationError> {
        let mut inner = self.inner.lock().unwrap();
        inner.operations.push(format!("get_bytes {creator} {path}"));
        Ok(inner
            .bytes
            .get(&(creator.to_string(), path.to_owned()))
            .cloned())
    }

    async fn delete_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<(), ApplicationError> {
        let mut inner = self.inner.lock().unwrap();
        inner.operations.push(format!("delete {creator} {path}"));
        inner.json.remove(&(creator.to_string(), path.to_owned()));
        inner.bytes.remove(&(creator.to_string(), path.to_owned()));
        Ok(())
    }
}
