mod support;

use std::str::FromStr;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::{Body, to_bytes};
use axum::extract::ConnectInfo;
use axum::http::{Request, StatusCode, header};
use locks_core::ids::{ContentLockPath, CreatorPubky};
use locks_server::config::{
    CreatorAuthorityAcquisitionConfig, CreatorAuthorityAcquisitionMethod, RuntimeEnvironment,
};
use locks_server::testing::TestServerApp;
use locks_service::application::errors::ApplicationError;
use locks_service::application::models::{
    CreatorAuthoritySecret, CreatorConnectAuthorizationUrl, LegacyCreatorConnectFlowApproval,
};
use locks_service::application::ports::LegacyCreatorConnectFlowClient;
use locks_service::infrastructure::pubky::{PubkyBytesResource, PubkyHomeserverStorageClient};
use serde_json::{Value, json};
use support::creator_publishing_client::LocalCreatorPublishingClient;
use tower::ServiceExt;

const CREATOR_AUTHORITY_SECRET: &str = "fake-legacy-session-secret";
const GUARDED_BYTES: &[u8] = b"production acquisition bytes";

#[tokio::test]
async fn production_creator_authority_acquisition_enables_pubky_backed_creator_publishing() {
    let storage = FakePubkyHomeserverStorage::default();
    let mut config = TestServerApp::default_in_memory_config();
    config.runtime.environment = RuntimeEnvironment::Production;
    config.creator_authority_acquisition = CreatorAuthorityAcquisitionConfig::default();
    config.creator_authority_acquisition.enabled = true;
    config.creator_authority_acquisition.method = CreatorAuthorityAcquisitionMethod::LegacyConnect;
    config
        .creator_authority_acquisition
        .legacy_connect
        .allowed_return_origins = vec!["https://pubky.app".to_owned()];

    let test_app =
        TestServerApp::new_in_memory_with_pubky_homeserver_storage(config, storage.clone())
            .with_legacy_creator_connect_flow_client(Arc::new(FakeLegacyConnectFlowClient));
    let router = test_app.router();

    let raw_connect_start = router
        .clone()
        .oneshot(json_request(
            "POST",
            "/creator/connect-flows",
            json!({
                "return_to": "https://pubky.app/locks/connected",
                "state": "opaque-state"
            }),
        ))
        .await
        .unwrap();
    assert_eq!(raw_connect_start.status(), StatusCode::NOT_FOUND);

    let shell_response = router
        .clone()
        .oneshot(empty_request(
            "GET",
            "/connect?return_to=https%3A%2F%2Fpubky.app%2Flocks%2Fconnected&state=opaque-state",
        ))
        .await
        .unwrap();
    assert_eq!(shell_response.status(), StatusCode::OK);
    let shell_html = response_text(shell_response).await;
    assert!(shell_html.contains("pubkyauth://fake-secret-flow-url"));
    assert_no_secret_text_material(&shell_html);
    let flow_id = extract_flow_id_from_shell(&shell_html);

    let completion_response = router
        .clone()
        .oneshot(empty_request(
            "POST",
            &format!("/connect/{flow_id}/complete"),
        ))
        .await
        .unwrap();
    assert_eq!(completion_response.status(), StatusCode::SEE_OTHER);
    let location = completion_response
        .headers()
        .get(header::LOCATION)
        .and_then(|value| value.to_str().ok())
        .expect("completion redirect includes Location")
        .to_owned();
    assert!(location.starts_with("https://pubky.app/locks/connected?"));
    assert!(!location.contains("authorization_url"));
    assert!(!location.contains("pubkyauth"));
    assert!(!location.contains(CREATOR_AUTHORITY_SECRET));
    let code = query_pair(&location, "code");
    assert_eq!(query_pair(&location, "state"), "opaque-state");

    let status_before_session = router
        .clone()
        .oneshot(empty_request("GET", "/creator/authority-status"))
        .await
        .unwrap();
    assert_eq!(status_before_session.status(), StatusCode::UNAUTHORIZED);

    let session = response_json(
        router
            .clone()
            .oneshot(json_request(
                "POST",
                "/frontend-sessions",
                json!({"code": code, "state": "opaque-state"}),
            ))
            .await
            .unwrap(),
    )
    .await;
    let frontend_session_token = session["session_token"].as_str().unwrap().to_owned();
    assert_eq!(session["creator"], creator().to_string());
    assert_no_secret_material(&session);

    let status = response_json(
        router
            .clone()
            .oneshot(
                empty_request("GET", "/creator/authority-status")
                    .with_bearer(&frontend_session_token),
            )
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status["creator"], creator().to_string());
    assert_eq!(status["authorized"], true);
    assert_eq!(status["auth_kind"], "legacy_cookie");
    assert_eq!(
        status["granted_scopes"],
        json!(["/pub/locks.app/:rw", "/priv/locks.app/:rw"])
    );
    assert_no_secret_material(&status);

    let client = LocalCreatorPublishingClient::new(router)
        .with_frontend_session_token(&frontend_session_token);
    let guarded_json = client
        .register_guarded_resource("acquired.txt", "text/plain", GUARDED_BYTES)
        .await
        .unwrap();
    let guarded_resource = guarded_json["guarded_resource"].clone();
    assert_eq!(
        guarded_resource["path"],
        "/priv/locks.app/content/acquired.txt"
    );
    assert_no_secret_material(&guarded_json);

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
    assert_no_secret_material(&content_lock_json);

    let operations = storage.operations();
    assert!(operations.iter().any(|operation| {
        operation == "put_bytes pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy /priv/locks.app/content/acquired.txt text/plain"
    }));
    assert!(operations.iter().any(|operation| {
        operation == &format!("put_json pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy {content_lock_path}")
    }));
}

struct FakeLegacyConnectFlowClient;

#[async_trait]
impl LegacyCreatorConnectFlowClient for FakeLegacyConnectFlowClient {
    async fn start_legacy_creator_connect_flow(
        &self,
        requested_scopes: &[String],
    ) -> Result<CreatorConnectAuthorizationUrl, ApplicationError> {
        assert_eq!(
            requested_scopes,
            ["/pub/locks.app/:rw", "/priv/locks.app/:rw"]
        );
        Ok(CreatorConnectAuthorizationUrl::new(
            "pubkyauth://fake-secret-flow-url",
        ))
    }

    async fn await_legacy_creator_connect_flow_approval(
        &self,
        authorization_url: &CreatorConnectAuthorizationUrl,
    ) -> Result<LegacyCreatorConnectFlowApproval, ApplicationError> {
        assert_eq!(
            authorization_url.expose_url(),
            "pubkyauth://fake-secret-flow-url"
        );
        Ok(LegacyCreatorConnectFlowApproval {
            creator: creator(),
            session_secret: CreatorAuthoritySecret::new(CREATOR_AUTHORITY_SECRET),
        })
    }
}

fn creator() -> CreatorPubky {
    CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
}

trait RequestBearerExt {
    fn with_bearer(self, token: &str) -> Self;
}

impl RequestBearerExt for Request<Body> {
    fn with_bearer(mut self, token: &str) -> Self {
        self.headers_mut().insert(
            header::AUTHORIZATION,
            format!("Bearer {token}").parse().unwrap(),
        );
        self
    }
}

fn json_request(method: &str, uri: &str, body: Value) -> Request<Body> {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    request
        .extensions_mut()
        .insert(ConnectInfo(std::net::SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
            12345,
        )));
    request
}

fn empty_request(method: &str, uri: &str) -> Request<Body> {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    request
        .extensions_mut()
        .insert(ConnectInfo(std::net::SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
            12345,
        )));
    request
}

async fn response_json(response: axum::response::Response) -> Value {
    serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap()
}

async fn response_text(response: axum::response::Response) -> String {
    String::from_utf8(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .expect("response body is utf-8")
}

fn extract_flow_id_from_shell(html: &str) -> String {
    let prefix = "action=\"/connect/";
    let start = html
        .find(prefix)
        .expect("connect shell contains completion form action")
        + prefix.len();
    let end = html[start..]
        .find("/complete\"")
        .expect("connect shell completion form action ends with /complete")
        + start;
    html[start..end].to_owned()
}

fn query_pair(location: &str, key: &str) -> String {
    location
        .split_once('?')
        .expect("location contains query")
        .1
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .find_map(|(candidate, value)| (candidate == key).then(|| value.to_owned()))
        .unwrap_or_else(|| panic!("location contains {key} query parameter"))
}

fn assert_no_secret_text_material(rendered: &str) {
    assert!(!rendered.contains(CREATOR_AUTHORITY_SECRET));
    assert!(!rendered.contains("session_secret"));
    assert!(!rendered.contains("creator_authority"));
}

fn assert_no_secret_material(value: &Value) {
    let rendered = value.to_string();
    assert!(!rendered.contains(CREATOR_AUTHORITY_SECRET));
    assert!(!rendered.contains("session_secret"));
    assert!(!rendered.contains("creator_authority"));
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
    json: std::collections::BTreeMap<(String, String), Value>,
    bytes: std::collections::BTreeMap<(String, String), PubkyBytesResource>,
}

#[async_trait]
impl PubkyHomeserverStorageClient for FakePubkyHomeserverStorage {
    async fn put_json_value_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
        value: Value,
    ) -> Result<(), ApplicationError> {
        let mut inner = self.inner.lock().unwrap();
        inner.operations.push(format!("put_json {creator} {path}"));
        inner.operations.push(format!("json_body {value}"));
        inner
            .json
            .insert((creator.to_string(), path.to_owned()), value);
        Ok(())
    }

    async fn get_json_value_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<Option<Value>, ApplicationError> {
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
        self.inner
            .lock()
            .unwrap()
            .operations
            .push(format!("delete {creator} {path}"));
        Ok(())
    }
}
