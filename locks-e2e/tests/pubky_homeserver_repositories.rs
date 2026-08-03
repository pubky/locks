mod support;

use std::str::FromStr;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::http::{StatusCode, header};
use locks_core::ids::{BundleId, ContentLockPath, CreatorPubky, PubkyLockResource};
use locks_core::verification::{Proof, SUBMITTED_PROOF_BUNDLE_VERSION, SubmittedProofBundle};
use locks_server::app_state::AppState;
use locks_server::testing::TestServerApp;
use locks_service::application::errors::ApplicationError;
use locks_service::application::models::FrontendSessionToken;
use locks_service::infrastructure::pubky::{
    PubkyBytesResource, PubkyContentLockRepository, PubkyEntitlementRepository,
    PubkyHomeserverStorageClient, PubkyLockServicePointerRepository, PubkyPrivResourceRepository,
};
use serde_json::json;
use support::creator_publishing_client::{LocalCreatorPublishingClient, response_bytes};
use time::Duration;

const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";
const FRONTEND_SESSION_TOKEN: &str = "frontend-session-secret";
const GUARDED_BYTES: &[u8] = b"pubky homeserver repository e2e";

#[tokio::test]
async fn pubky_homeserver_repository_flow_writes_to_fake_homeserver_storage() {
    let storage = FakePubkyHomeserverStorage::default();
    let config = TestServerApp::default_in_memory_config();

    let state = AppState::new_empty_in_memory_with_creator_repositories(
        config,
        Arc::new(PubkyContentLockRepository::new(storage.clone())),
        Arc::new(PubkyPrivResourceRepository::new(storage.clone())),
        Arc::new(PubkyLockServicePointerRepository::new(storage.clone())),
        Arc::new(PubkyEntitlementRepository::new(storage.clone())),
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

    let pointer_json = client
        .set_lock_service_config("pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo")
        .await
        .unwrap();
    assert_eq!(pointer_json["path"], "/pub/locks.app/config.json");

    let guarded_json = client
        .register_guarded_resource("pubky-e2e.txt", "text/plain", GUARDED_BYTES)
        .await
        .unwrap();
    let guarded_resource = guarded_json["guarded_resource"].clone();

    let content_lock_json = client
        .create_content_lock(
            guarded_resource,
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
    let content_lock_path = ContentLockPath::from_str(
        content_lock_json["content_lock_path"]
            .as_str()
            .expect("content_lock_path is string"),
    )
    .unwrap();

    let submit_json = client
        .submit_proof_bundle(submitted_proof_bundle_for(content_lock_path))
        .await
        .unwrap();
    assert_eq!(submit_json["status"], "pending");

    let completion_json = client
        .dev_complete_verification(creator(), BUNDLE_ID)
        .await
        .unwrap();
    assert_eq!(completion_json["status"], "completed");

    let credential = client
        .issue_access_credential(creator(), BUNDLE_ID)
        .await
        .unwrap();

    let proxy_response = client
        .proxy_read_guarded_resource(&credential, "pubky-e2e.txt")
        .await
        .unwrap();
    assert_eq!(proxy_response.status(), StatusCode::OK);
    assert_eq!(proxy_response.headers()[header::CONTENT_TYPE], "text/plain");
    assert_eq!(response_bytes(proxy_response).await, GUARDED_BYTES);

    let operations = storage.operations();
    assert!(
        operations.iter().any(|operation| {
            operation == "put_json pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy /pub/locks.app/config.json"
        })
    );
    assert!(operations.iter().any(|operation| {
        operation.starts_with("put_json pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy /pub/locks.app/")
            && operation.ends_with(".json")
            && operation != "put_json pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy /pub/locks.app/config.json"
    }));
    assert!(operations.iter().any(|operation| {
        operation == "put_bytes pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy /priv/locks.app/content/pubky-e2e.txt text/plain"
    }));
    assert!(operations.iter().any(|operation| {
        *operation == format!("put_json pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy /priv/locks.app/proofs/{BUNDLE_ID}.json")
    }));
}

fn submitted_proof_bundle_for(content_lock_path: ContentLockPath) -> SubmittedProofBundle {
    SubmittedProofBundle {
        version: SUBMITTED_PROOF_BUNDLE_VERSION,
        bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
        pubky_lock_resource: PubkyLockResource::new(creator(), content_lock_path),
        reader_public_key: None,
        proofs: vec![Proof {
            criterion_id: "criterion-1".to_owned(),
            verifier_type: locks_core::lock_policy::VerifierType::DevStatic,
            payload: json!({ "satisfied": true }),
        }],
    }
}

fn creator() -> CreatorPubky {
    CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
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
