use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use axum::body::{Body, to_bytes};
use axum::extract::ConnectInfo;
use axum::http::{Request, StatusCode, header};
use locks_core::ids::CreatorPubky;
use locks_server::testing::TestServerApp;
use locks_service::application::errors::ApplicationError;
use locks_service::application::models::{
    CreatorAuthorityAuthKind, CreatorAuthoritySecret, CreatorConnectAuthorizationUrl,
    LegacyCreatorConnectFlowApproval,
};
use locks_service::application::ports::LegacyCreatorConnectFlowClient;
use serde_json::{Value, json};
use tower::ServiceExt;

#[tokio::test]
async fn legacy_connect_shell_http_flow_starts_completes_redirects_and_exchanges_session() {
    let mut config = TestServerApp::default_in_memory_config();
    config.creator_authority_acquisition.enabled = true;
    config
        .creator_authority_acquisition
        .legacy_connect
        .allowed_return_origins = vec!["https://pubky.app".to_owned()];
    let test_app = TestServerApp::new_in_memory(config)
        .with_legacy_creator_connect_flow_client(Arc::new(FakeLegacyConnectFlowClient));
    let router = test_app.router();

    let shell_response = router
        .clone()
        .oneshot(empty_request(
            "GET",
            "/connect?return_to=https%3A%2F%2Fpubky.app%2Flocks%2Fconnected&state=opaque-state",
        ))
        .await
        .unwrap();
    assert_eq!(shell_response.status(), StatusCode::OK);
    assert_eq!(
        shell_response.headers().get(header::CONTENT_TYPE).unwrap(),
        "text/html; charset=utf-8"
    );
    let shell_html = String::from_utf8(response_bytes(shell_response).await).unwrap();
    assert!(shell_html.contains("Enable Locks"));
    assert!(shell_html.contains("pubkyauth://fake-secret-flow-url"));
    assert!(shell_html.contains("data-testid=\"pubky-auth-qr\""));
    assert!(shell_html.contains("<svg"));
    assert!(shell_html.contains("aria-label=\"Pubky authorization QR code\""));
    assert!(!shell_html.contains("fake-legacy-session-secret"));
    assert!(!shell_html.contains("frontend_session_token"));
    assert!(!shell_html.contains("one-time-code"));
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
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    assert!(
        location.starts_with("https://pubky.app/locks/connected?"),
        "unexpected callback location: {location}"
    );
    assert!(location.contains("state=opaque-state"));
    assert!(location.contains("code="));
    assert!(!location.contains("pubkyauth"));
    assert!(!location.contains("authorization_url"));
    assert!(!location.contains("fake-legacy-session-secret"));
    assert!(!location.contains("frontend_session_token"));

    let code = query_param(&location, "code").expect("callback contains one-time code");
    assert!(!code.is_empty());
    let state = query_param(&location, "state").expect("callback contains state");
    assert_eq!(state, "opaque-state");

    let stored_authority = test_app
        .state()
        .creator_authorities()
        .get_creator_authority(&creator())
        .await
        .unwrap()
        .expect("creator authority stored");
    assert_eq!(stored_authority.creator, creator());
    assert_eq!(
        stored_authority.auth_kind,
        CreatorAuthorityAuthKind::LegacyCookie
    );
    assert_eq!(
        stored_authority.secret.expose_secret(),
        "fake-legacy-session-secret"
    );

    let session_response = router
        .clone()
        .oneshot(json_request(
            "POST",
            "/frontend-sessions",
            json!({"code": code, "state": state}),
        ))
        .await
        .unwrap();
    assert_eq!(session_response.status(), StatusCode::OK);
    let session = response_json(session_response).await;
    let session_token = session["session_token"].as_str().unwrap();
    assert!(!session_token.is_empty());
    assert_eq!(
        session["creator"],
        "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy"
    );
    assert_no_keys(
        &session,
        &["code", "state", "authorization_url", "session_secret"],
    );
}

#[tokio::test]
async fn connect_shell_postmessage_mode_returns_json_and_frames_allowed_parent() {
    let mut config = TestServerApp::default_in_memory_config();
    config.creator_authority_acquisition.enabled = true;
    config
        .creator_authority_acquisition
        .legacy_connect
        .allowed_return_origins = vec!["https://pubky.app".to_owned()];
    let test_app = TestServerApp::new_in_memory(config)
        .with_legacy_creator_connect_flow_client(Arc::new(FakeLegacyConnectFlowClient));
    let router = test_app.router();

    let shell_response = router
        .clone()
        .oneshot(empty_request(
            "GET",
            "/connect?return_to=https%3A%2F%2Fpubky.app%2Flocks%2Fconnected&state=opaque-state&delivery=postmessage",
        ))
        .await
        .unwrap();
    assert_eq!(shell_response.status(), StatusCode::OK);
    assert_eq!(
        shell_response
            .headers()
            .get(header::CONTENT_SECURITY_POLICY)
            .unwrap(),
        "frame-ancestors https://pubky.app"
    );
    let shell_html = String::from_utf8(response_bytes(shell_response).await).unwrap();
    // Postmessage shell targets the validated return origin explicitly (never `*`) and publishes
    // the callback message type, and drops the manual approval button.
    assert!(shell_html.contains("locks-auth-callback"));
    assert!(shell_html.contains("TARGET_ORIGIN = \"https://pubky.app\""));
    assert!(shell_html.contains("CALLBACK_STATE = \"opaque-state\""));
    assert!(shell_html.contains("error: \"invalid-response\""));
    assert!(shell_html.contains("error: \"connect-failed\""));
    assert!(!shell_html.contains("connect-failed-\" + res.status"));
    assert!(!shell_html.contains("I approved this connection"));
    assert!(!shell_html.contains("<form"));
    assert!(!shell_html.contains("fake-legacy-session-secret"));
    let flow_id = extract_flow_id_from_postmessage_shell(&shell_html);

    let completion_response = router
        .clone()
        .oneshot(empty_request(
            "POST",
            &format!("/connect/{flow_id}/complete?delivery=postmessage"),
        ))
        .await
        .unwrap();
    assert_eq!(completion_response.status(), StatusCode::OK);
    let body = response_json(completion_response).await;
    assert_eq!(body["state"], "opaque-state");
    let code = body["code"].as_str().expect("json body carries code");
    assert!(!code.is_empty());
    let body_text = body.to_string();
    assert!(!body_text.contains("pubkyauth"));
    assert!(!body_text.contains("authorization_url"));
    assert!(!body_text.contains("fake-legacy-session-secret"));

    // The one-time code still exchanges for a browser session, same as redirect mode.
    let session_response = router
        .clone()
        .oneshot(json_request(
            "POST",
            "/frontend-sessions",
            json!({"code": code, "state": "opaque-state"}),
        ))
        .await
        .unwrap();
    assert_eq!(session_response.status(), StatusCode::OK);
    let session = response_json(session_response).await;
    assert!(!session["session_token"].as_str().unwrap().is_empty());
}

#[tokio::test]
async fn connect_shell_postmessage_mode_rejects_disallowed_return_origin() {
    let mut config = TestServerApp::default_in_memory_config();
    config.creator_authority_acquisition.enabled = true;
    config
        .creator_authority_acquisition
        .legacy_connect
        .allowed_return_origins = vec!["https://pubky.app".to_owned()];
    let test_app = TestServerApp::new_in_memory(config)
        .with_legacy_creator_connect_flow_client(Arc::new(FakeLegacyConnectFlowClient));
    let router = test_app.router();

    let shell_response = router
        .oneshot(empty_request(
            "GET",
            "/connect?return_to=https%3A%2F%2Fevil.example%2Fsteal&state=opaque-state&delivery=postmessage",
        ))
        .await
        .unwrap();
    // Disallowed parent origin never gets a shell (and therefore no code).
    assert_ne!(shell_response.status(), StatusCode::OK);
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
            session_secret: CreatorAuthoritySecret::new("fake-legacy-session-secret"),
        })
    }
}

fn creator() -> CreatorPubky {
    CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
}

fn extract_flow_id_from_shell(html: &str) -> String {
    let prefix = "action=\"/connect/";
    let start = html
        .find(prefix)
        .expect("shell contains completion form action")
        + prefix.len();
    let rest = &html[start..];
    let end = rest
        .find("/complete\"")
        .expect("shell completion form action has complete suffix");
    rest[..end].to_owned()
}

fn extract_flow_id_from_postmessage_shell(html: &str) -> String {
    let prefix = "encodeURIComponent(\"";
    let start = html
        .find(prefix)
        .expect("postmessage shell embeds flow id in completion URL")
        + prefix.len();
    let rest = &html[start..];
    let end = rest
        .find('"')
        .expect("postmessage shell flow id string is terminated");
    rest[..end].to_owned()
}

fn query_param(url: &str, key: &str) -> Option<String> {
    let query = url.split_once('?')?.1;
    query.split('&').find_map(|pair| {
        let (candidate, value) = pair.split_once('=')?;
        if candidate == key {
            Some(value.to_owned())
        } else {
            None
        }
    })
}

fn empty_request(method: &str, uri: &str) -> Request<Body> {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    request.extensions_mut().insert(ConnectInfo(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
        12345,
    )));
    request
}

fn json_request(method: &str, uri: &str, body: Value) -> Request<Body> {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    request.extensions_mut().insert(ConnectInfo(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
        12345,
    )));
    request
}

async fn response_json(response: axum::response::Response) -> Value {
    serde_json::from_slice(&response_bytes(response).await).unwrap()
}

async fn response_bytes(response: axum::response::Response) -> Vec<u8> {
    to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec()
}

fn assert_no_keys(value: &Value, keys: &[&str]) {
    for key in keys {
        assert!(
            value.get(*key).is_none(),
            "unexpected key in response: {key}"
        );
    }
}
