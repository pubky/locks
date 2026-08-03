use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use axum::body::{Body, to_bytes};
use axum::extract::ConnectInfo;
use axum::http::{Request, StatusCode, header};
use locks_core::ids::CreatorPubky;
use locks_core::verification::SubmittedProofBundle;
use serde_json::{Value, json};
use tower::ServiceExt;

#[derive(Debug)]
#[allow(dead_code)]
pub struct HttpTestError {
    pub status: StatusCode,
    pub body: Value,
}

#[derive(Clone)]
pub struct LocalCreatorPublishingClient {
    router: axum::Router,
    frontend_session_token: Option<String>,
}

#[allow(dead_code)]
impl LocalCreatorPublishingClient {
    pub fn new(router: axum::Router) -> Self {
        Self {
            router,
            frontend_session_token: None,
        }
    }

    pub fn with_frontend_session_token(mut self, token: impl Into<String>) -> Self {
        self.frontend_session_token = Some(token.into());
        self
    }

    pub async fn set_lock_service_config(
        &self,
        default_lock_server: &str,
    ) -> Result<Value, HttpTestError> {
        let body = json!({
            "default_lock_server": default_lock_server,
        });

        self.post_json("/creator/lock-service-config", body).await
    }

    pub async fn register_guarded_resource(
        &self,
        path: &str,
        content_type: &str,
        bytes: impl Into<Vec<u8>>,
    ) -> Result<Value, HttpTestError> {
        let response = self
            .router
            .clone()
            .oneshot(self.raw_upload_request(
                &format!("/creator/priv-resources/content/{path}"),
                content_type,
                bytes.into(),
            ))
            .await
            .unwrap();
        json_success_or_error(response).await
    }

    pub async fn delete_guarded_resource(&self, path: &str) -> Result<(), HttpTestError> {
        let response = self
            .router
            .clone()
            .oneshot(self.raw_delete_request(&format!("/creator/priv-resources/content/{path}")))
            .await
            .unwrap();
        if response.status().is_success() {
            Ok(())
        } else {
            Err(error_from_json_response(response).await)
        }
    }

    pub async fn create_content_lock(
        &self,
        primary_resource: Value,
        criteria: Value,
        lock_logic: Value,
        access_policy: Value,
        lock_server: Value,
    ) -> Result<Value, HttpTestError> {
        self.create_content_lock_with_resources(
            Some(primary_resource),
            json!({}),
            criteria,
            lock_logic,
            access_policy,
            lock_server,
        )
        .await
    }

    pub async fn create_content_lock_with_resources(
        &self,
        primary_resource: Option<Value>,
        secondary_resources: Value,
        criteria: Value,
        lock_logic: Value,
        access_policy: Value,
        lock_server: Value,
    ) -> Result<Value, HttpTestError> {
        let mut body = serde_json::Map::new();
        if let Some(primary_resource) = primary_resource {
            body.insert("primary_resource".to_owned(), primary_resource);
        }
        body.insert("secondary_resources".to_owned(), secondary_resources);
        body.insert("criteria".to_owned(), criteria);
        body.insert("lock_logic".to_owned(), lock_logic);
        body.insert("access_policy".to_owned(), access_policy);
        body.insert("lock_server".to_owned(), lock_server);

        self.post_json("/creator/content-locks", Value::Object(body))
            .await
    }

    pub async fn submit_proof_bundle(
        &self,
        submitted: SubmittedProofBundle,
    ) -> Result<Value, HttpTestError> {
        self.post_json(
            "/proof-bundles",
            json!({ "submitted_proof_bundle": submitted }),
        )
        .await
    }

    pub async fn dev_complete_verification(
        &self,
        creator: CreatorPubky,
        bundle_id: &str,
    ) -> Result<Value, HttpTestError> {
        self.post_json(
            "/verification-task-completions",
            public_handle_body(creator, bundle_id),
        )
        .await
    }

    pub async fn issue_access_credential(
        &self,
        creator: CreatorPubky,
        bundle_id: &str,
    ) -> Result<String, HttpTestError> {
        let body = self
            .post_json(
                "/access-credentials",
                public_handle_body(creator, bundle_id),
            )
            .await?;
        Ok(body["credential"]
            .as_str()
            .expect("credential response contains string credential")
            .to_owned())
    }

    pub async fn proxy_read_guarded_resource(
        &self,
        credential: &str,
        path: &str,
    ) -> Result<axum::response::Response, HttpTestError> {
        let response = self
            .router
            .clone()
            .oneshot(auth_request(
                "GET",
                &format!("/priv-resources/content/{path}"),
                &format!("Bearer {credential}"),
            ))
            .await
            .unwrap();

        if response.status().is_success() {
            Ok(response)
        } else {
            Err(error_from_json_response(response).await)
        }
    }

    async fn post_json(&self, uri: &str, body: Value) -> Result<Value, HttpTestError> {
        let response = self
            .router
            .clone()
            .oneshot(self.json_request("POST", uri, body))
            .await
            .unwrap();
        json_success_or_error(response).await
    }

    fn json_request(&self, method: &str, uri: &str, body: Value) -> Request<Body> {
        let mut request = json_request(method, uri, body);
        if let Some(token) = &self.frontend_session_token {
            request.headers_mut().insert(
                header::AUTHORIZATION,
                format!("Bearer {token}").parse().unwrap(),
            );
        }
        request
    }
    fn raw_delete_request(&self, uri: &str) -> Request<Body> {
        let mut request = Request::builder()
            .method("DELETE")
            .uri(uri)
            .body(Body::empty())
            .unwrap();
        if let Some(token) = &self.frontend_session_token {
            request.headers_mut().insert(
                header::AUTHORIZATION,
                format!("Bearer {token}").parse().unwrap(),
            );
        }
        request.extensions_mut().insert(ConnectInfo(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            12345,
        )));
        request
    }

    fn raw_upload_request(&self, uri: &str, content_type: &str, bytes: Vec<u8>) -> Request<Body> {
        let mut request = Request::builder()
            .method("PUT")
            .uri(uri)
            .header(header::CONTENT_TYPE, content_type)
            .body(Body::from(bytes))
            .unwrap();
        if let Some(token) = &self.frontend_session_token {
            request.headers_mut().insert(
                header::AUTHORIZATION,
                format!("Bearer {token}").parse().unwrap(),
            );
        }
        request.extensions_mut().insert(ConnectInfo(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            12345,
        )));
        request
    }
}

async fn json_success_or_error(response: axum::response::Response) -> Result<Value, HttpTestError> {
    let status = response.status();
    let body = response_json(response).await;
    if status.is_success() {
        Ok(body)
    } else {
        Err(HttpTestError { status, body })
    }
}

#[allow(dead_code)]
async fn error_from_json_response(response: axum::response::Response) -> HttpTestError {
    let status = response.status();
    let body = response_json(response).await;
    HttpTestError { status, body }
}

#[allow(dead_code)]
fn public_handle_body(creator: CreatorPubky, bundle_id: &str) -> Value {
    json!({ "creator": creator, "bundle_id": bundle_id })
}

pub fn json_request(method: &str, uri: &str, body: Value) -> Request<Body> {
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

#[allow(dead_code)]
pub fn auth_request(method: &str, uri: &str, authorization: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, authorization)
        .body(Body::empty())
        .unwrap()
}

pub async fn response_json(response: axum::response::Response) -> Value {
    serde_json::from_slice(&response_bytes(response).await).unwrap()
}

pub async fn response_bytes(response: axum::response::Response) -> Vec<u8> {
    to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec()
}
