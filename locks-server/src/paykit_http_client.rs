use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use locks_core::ids::{BundleId, CreatorPubky};
use locks_service::infrastructure::verifiers::paykit_payment::{
    PaykitPaymentStatus, PaykitPaymentStatusClient, PaykitPaymentStatusError,
    PaykitPaymentStatusKind,
};
use pubky_common::crypto::Keypair;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use url::Url;

use crate::config::{
    LockServerCredentialsConfig, LockServerSigningKeyError, PAYKIT_CONNECT_TIMEOUT_SECONDS,
    PAYKIT_REQUEST_TIMEOUT_SECONDS, load_lock_server_signing_keypair,
};

const SIGNATURE_HEADER: &str = "X-Paykit-Signature";

#[derive(Debug, thiserror::Error)]
pub enum PaykitClientError {
    #[error("paykit.server_url must be a valid http(s) URL: {0}")]
    InvalidServerUrl(String),
    #[error("failed to read lock server signing seed: {0}")]
    SigningSeedRead(std::io::Error),
    #[error("lock_server_secret_key must contain keypair-seed:<base64url-no-pad-32-byte-seed>")]
    InvalidSigningSeed,
    #[error("lock_server_public_key does not match lock_server_secret_key signing seed")]
    PublicKeyMismatch,
    #[error("failed to serialize Paykit request body: {0}")]
    Serialize(serde_json::Error),
    #[error("Paykit request failed: {0}")]
    Http(reqwest::Error),
    #[error("Paykit {operation} returned non-success status {status}")]
    NonSuccess {
        operation: &'static str,
        status: StatusCode,
    },
    #[error("Paykit status response was invalid: {0}")]
    InvalidStatusResponse(reqwest::Error),
    #[error("Paykit invoice response was invalid: {0}")]
    InvalidInvoiceResponse(String),
}

#[derive(Debug, Clone)]
pub struct PaykitHttpClient {
    server_url: Url,
    http: reqwest::Client,
    signing_keypair: Keypair,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PaykitInvoiceRequest {
    pub bundle_id: String,
    pub lock_resource: String,
    pub payment_in: u64,
    pub reader: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PaykitInvoiceResponse {
    pub invoice_created_at: OffsetDateTime,
    pub payment_deadline: OffsetDateTime,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PaykitInvoiceResponseBody {
    invoice_created_at: String,
    payment_deadline: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PaykitStatusRequest {
    pub creator: String,
    pub bundle_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PaykitTransactionStatusKind {
    Undetected,
    Detected,
    Confirmed,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PaykitTransactionStatus {
    pub status: PaykitTransactionStatusKind,
    pub confirmations: u32,
    pub amount_matched: bool,
}

impl PaykitHttpClient {
    pub fn new(
        server_url: &str,
        credentials: &LockServerCredentialsConfig,
    ) -> Result<Self, PaykitClientError> {
        Self::from_parts(
            server_url,
            bounded_http_client(
                Duration::from_secs(PAYKIT_CONNECT_TIMEOUT_SECONDS),
                Duration::from_secs(PAYKIT_REQUEST_TIMEOUT_SECONDS),
            )?,
            load_paykit_signing_keypair(credentials)?,
        )
    }

    fn from_parts(
        server_url: &str,
        http: reqwest::Client,
        signing_keypair: Keypair,
    ) -> Result<Self, PaykitClientError> {
        let server_url = parse_server_url(server_url)?;
        Ok(Self {
            server_url,
            http,
            signing_keypair,
        })
    }

    pub async fn create_invoice(
        &self,
        request: &PaykitInvoiceRequest,
    ) -> Result<PaykitInvoiceResponse, PaykitClientError> {
        let response = self.signed_post("invoices", request).await?;

        if !response.status().is_success() {
            return Err(PaykitClientError::NonSuccess {
                operation: "invoice creation",
                status: response.status(),
            });
        }

        let body = response
            .json::<PaykitInvoiceResponseBody>()
            .await
            .map_err(|error| PaykitClientError::InvalidInvoiceResponse(error.to_string()))?;
        let invoice_created_at = OffsetDateTime::parse(&body.invoice_created_at, &Rfc3339)
            .map_err(|error| PaykitClientError::InvalidInvoiceResponse(error.to_string()))?;
        let payment_deadline = OffsetDateTime::parse(&body.payment_deadline, &Rfc3339)
            .map_err(|error| PaykitClientError::InvalidInvoiceResponse(error.to_string()))?;
        if payment_deadline < invoice_created_at {
            return Err(PaykitClientError::InvalidInvoiceResponse(
                "payment_deadline precedes invoice_created_at".to_owned(),
            ));
        }
        Ok(PaykitInvoiceResponse {
            invoice_created_at,
            payment_deadline,
        })
    }

    pub async fn transaction_status(
        &self,
        request: &PaykitStatusRequest,
    ) -> Result<PaykitTransactionStatus, PaykitClientError> {
        let response = self.signed_post("transactions/status", request).await?;

        if !response.status().is_success() {
            return Err(PaykitClientError::NonSuccess {
                operation: "transaction status",
                status: response.status(),
            });
        }

        response
            .json::<PaykitTransactionStatus>()
            .await
            .map_err(PaykitClientError::InvalidStatusResponse)
    }

    fn endpoint(&self, path: &str) -> Url {
        let mut endpoint = self.server_url.clone();
        endpoint.set_query(None);
        endpoint.set_fragment(None);
        {
            let mut segments = endpoint
                .path_segments_mut()
                .expect("validated http(s) server_url supports path segments");
            segments.pop_if_empty();
            for segment in path.split('/') {
                segments.push(segment);
            }
        }
        endpoint
    }

    async fn signed_post<T: Serialize>(
        &self,
        path: &str,
        request: &T,
    ) -> Result<reqwest::Response, PaykitClientError> {
        let body = canonical_body_bytes(request)?;
        self.http
            .post(self.endpoint(path))
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(SIGNATURE_HEADER, sign_body(&self.signing_keypair, &body))
            .body(body)
            .send()
            .await
            .map_err(PaykitClientError::Http)
    }
}

fn bounded_http_client(
    connect_timeout: Duration,
    request_timeout: Duration,
) -> Result<reqwest::Client, PaykitClientError> {
    reqwest::Client::builder()
        .connect_timeout(connect_timeout)
        .timeout(request_timeout)
        .build()
        .map_err(PaykitClientError::Http)
}

#[async_trait]
impl PaykitPaymentStatusClient for PaykitHttpClient {
    async fn transaction_status(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<PaykitPaymentStatus, PaykitPaymentStatusError> {
        let status = PaykitHttpClient::transaction_status(
            self,
            &PaykitStatusRequest {
                creator: creator.to_string(),
                bundle_id: bundle_id.to_string(),
            },
        )
        .await
        .map_err(|_| PaykitPaymentStatusError)?;

        Ok(PaykitPaymentStatus {
            status: match status.status {
                PaykitTransactionStatusKind::Undetected => PaykitPaymentStatusKind::Undetected,
                PaykitTransactionStatusKind::Detected => PaykitPaymentStatusKind::Detected,
                PaykitTransactionStatusKind::Confirmed => PaykitPaymentStatusKind::Confirmed,
            },
            confirmations: status.confirmations,
            amount_matched: status.amount_matched,
        })
    }
}

fn load_paykit_signing_keypair(
    credentials: &LockServerCredentialsConfig,
) -> Result<Keypair, PaykitClientError> {
    load_lock_server_signing_keypair(credentials).map_err(|error| match error {
        LockServerSigningKeyError::Read(source) => PaykitClientError::SigningSeedRead(source),
        LockServerSigningKeyError::InvalidSeed => PaykitClientError::InvalidSigningSeed,
        LockServerSigningKeyError::PublicKeyMismatch => PaykitClientError::PublicKeyMismatch,
    })
}

fn canonical_body_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, PaykitClientError> {
    serde_json_canonicalizer::to_vec(value).map_err(PaykitClientError::Serialize)
}

fn sign_body(keypair: &Keypair, body: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(keypair.sign(body).to_bytes())
}

fn parse_server_url(value: &str) -> Result<Url, PaykitClientError> {
    let parsed =
        Url::parse(value).map_err(|_| PaykitClientError::InvalidServerUrl(value.into()))?;
    match parsed.scheme() {
        "http" | "https" => Ok(parsed),
        _ => Err(PaykitClientError::InvalidServerUrl(value.into())),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use axum::body::Bytes;
    use axum::extract::State;
    use axum::http::HeaderMap;
    use axum::response::IntoResponse;
    use axum::routing::post;
    use axum::{Json, Router};
    use locks_core::ids::LockServerPubky;
    use serde_json::json;
    use tempfile::tempdir;
    use tokio::net::TcpListener;

    use super::*;

    const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";
    const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
    const LOCK_RESOURCE: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/locks.app/1111111111111111111111111111111111111111111111111111.json";
    const READER: &str = "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo";

    #[test]
    fn canonical_invoice_body_serializes_exact_paykit_shape() {
        let body = canonical_body_bytes(&invoice_request()).unwrap();

        assert_eq!(
            String::from_utf8(body).unwrap(),
            format!(
                "{{\"bundle_id\":\"{BUNDLE_ID}\",\"lock_resource\":\"{LOCK_RESOURCE}\",\"payment_in\":24,\"reader\":\"{READER}\"}}"
            )
        );
    }

    #[test]
    fn invoice_signature_is_base64url_no_pad_ed25519_over_canonical_body() {
        let keypair = Keypair::from_secret(&[9_u8; 32]);
        let body = canonical_body_bytes(&invoice_request()).unwrap();

        let signature = sign_body(&keypair, &body);

        assert_eq!(
            signature,
            URL_SAFE_NO_PAD.encode(keypair.sign(&body).to_bytes())
        );
        assert!(!signature.contains('='));
    }

    #[test]
    fn endpoints_append_to_unprefixed_server_url() {
        let client = PaykitHttpClient::from_parts(
            "https://paykit.example",
            reqwest::Client::new(),
            Keypair::from_secret(&[9_u8; 32]),
        )
        .unwrap();

        assert_eq!(
            client.endpoint("invoices").as_str(),
            "https://paykit.example/invoices"
        );
        assert_eq!(
            client.endpoint("transactions/status").as_str(),
            "https://paykit.example/transactions/status"
        );
    }

    #[test]
    fn endpoints_preserve_server_url_path_prefix_with_or_without_trailing_slash() {
        for server_url in [
            "https://paykit.example/services/paykit",
            "https://paykit.example/services/paykit/",
        ] {
            let client = PaykitHttpClient::from_parts(
                server_url,
                reqwest::Client::new(),
                Keypair::from_secret(&[9_u8; 32]),
            )
            .unwrap();

            assert_eq!(
                client.endpoint("invoices").as_str(),
                "https://paykit.example/services/paykit/invoices"
            );
            assert_eq!(
                client.endpoint("transactions/status").as_str(),
                "https://paykit.example/services/paykit/transactions/status"
            );
        }
    }

    #[test]
    fn load_signing_keypair_rejects_mismatched_public_key() {
        let temp_dir = tempdir().unwrap();
        let secret_path = temp_dir.path().join("lock-server.sess");
        std::fs::write(
            &secret_path,
            format!("keypair-seed:{}", URL_SAFE_NO_PAD.encode([9_u8; 32])),
        )
        .unwrap();
        let credentials = LockServerCredentialsConfig {
            lock_server_secret_key: secret_path,
            lock_server_public_key: LockServerPubky::from_str(READER).unwrap(),
            max_ttl_seconds: 900,
        };

        let error = load_paykit_signing_keypair(&credentials).unwrap_err();

        assert!(matches!(error, PaykitClientError::PublicKeyMismatch));
    }

    #[tokio::test]
    async fn create_invoice_posts_canonical_json_with_signature_header() {
        let captured = CapturedRequests::default();
        let server_url = spawn_test_server(captured.clone()).await;
        let keypair = Keypair::from_secret(&[9_u8; 32]);
        let client =
            PaykitHttpClient::from_parts(&server_url, reqwest::Client::new(), keypair.clone())
                .unwrap();
        let expected_body = canonical_body_bytes(&invoice_request()).unwrap();
        let expected_signature = sign_body(&keypair, &expected_body);

        let response = client.create_invoice(&invoice_request()).await.unwrap();

        assert_eq!(
            response.invoice_created_at,
            time::macros::datetime!(2026-08-12 10:00:00 UTC)
        );
        assert_eq!(
            response.payment_deadline,
            time::macros::datetime!(2026-08-13 10:00:00 UTC)
        );

        let request = captured.single();
        assert_eq!(request.path, "/invoices");
        assert_eq!(request.body, expected_body);
        assert_eq!(request.signature, Some(expected_signature));
    }

    #[tokio::test]
    async fn transaction_status_posts_signed_composite_identity_and_parses_status_response() {
        let captured = CapturedRequests::default();
        let server_url = spawn_test_server(captured.clone()).await;
        let keypair = Keypair::from_secret(&[9_u8; 32]);
        let client =
            PaykitHttpClient::from_parts(&server_url, reqwest::Client::new(), keypair.clone())
                .unwrap();
        let status_request = PaykitStatusRequest {
            creator: CREATOR.to_owned(),
            bundle_id: BUNDLE_ID.to_owned(),
        };
        let expected_body = canonical_body_bytes(&status_request).unwrap();
        let expected_signature = sign_body(&keypair, &expected_body);

        let status = client.transaction_status(&status_request).await.unwrap();

        assert_eq!(
            status,
            PaykitTransactionStatus {
                status: PaykitTransactionStatusKind::Detected,
                confirmations: 0,
                amount_matched: true,
            }
        );
        let request = captured.single();
        assert_eq!(request.path, "/transactions/status");
        assert_eq!(
            String::from_utf8(request.body).unwrap(),
            format!("{{\"bundle_id\":\"{BUNDLE_ID}\",\"creator\":\"{CREATOR}\"}}")
        );
        assert_eq!(request.signature, Some(expected_signature));
    }

    #[tokio::test]
    async fn create_invoice_times_out_when_paykit_does_not_respond() {
        let server_url = spawn_hanging_test_server().await;
        let client = PaykitHttpClient::from_parts(
            &server_url,
            bounded_http_client(Duration::from_millis(10), Duration::from_millis(25)).unwrap(),
            Keypair::from_secret(&[9_u8; 32]),
        )
        .unwrap();

        let error = client.create_invoice(&invoice_request()).await.unwrap_err();

        assert!(matches!(error, PaykitClientError::Http(error) if error.is_timeout()));
    }

    #[tokio::test]
    async fn transaction_status_times_out_when_paykit_does_not_respond() {
        let server_url = spawn_hanging_test_server().await;
        let client = PaykitHttpClient::from_parts(
            &server_url,
            bounded_http_client(Duration::from_millis(10), Duration::from_millis(25)).unwrap(),
            Keypair::from_secret(&[9_u8; 32]),
        )
        .unwrap();

        let error = client
            .transaction_status(&PaykitStatusRequest {
                creator: CREATOR.to_owned(),
                bundle_id: BUNDLE_ID.to_owned(),
            })
            .await
            .unwrap_err();

        assert!(matches!(error, PaykitClientError::Http(error) if error.is_timeout()));
    }

    #[tokio::test]
    async fn create_invoice_rejects_malformed_or_inconsistent_success_body() {
        for body in [
            r#"{"invoice_created_at":"2026-08-12T10:00:00Z"}"#,
            r#"{"invoice_created_at":"not-a-time","payment_deadline":"2026-08-13T10:00:00Z"}"#,
            r#"{"invoice_created_at":"2026-08-13T10:00:00Z","payment_deadline":"2026-08-12T10:00:00Z"}"#,
            r#"{"invoice_created_at":"2026-08-12T10:00:00Z","payment_deadline":"2026-08-13T10:00:00Z","extra":true}"#,
        ] {
            let server_url =
                spawn_configured_invoice_server(axum::http::StatusCode::OK, body).await;
            let client = PaykitHttpClient::from_parts(
                &server_url,
                reqwest::Client::new(),
                Keypair::from_secret(&[9_u8; 32]),
            )
            .unwrap();

            assert!(matches!(
                client.create_invoice(&invoice_request()).await,
                Err(PaykitClientError::InvalidInvoiceResponse(_))
            ));
        }
    }

    #[tokio::test]
    async fn status_not_found_and_invalid_success_body_use_retryable_client_error() {
        for (status, body) in [
            (axum::http::StatusCode::NOT_FOUND, "not found"),
            (axum::http::StatusCode::OK, "not-json"),
        ] {
            let server_url = spawn_configured_status_server(status, body).await;
            let client = PaykitHttpClient::from_parts(
                &server_url,
                reqwest::Client::new(),
                Keypair::from_secret(&[9_u8; 32]),
            )
            .unwrap();

            let error = PaykitPaymentStatusClient::transaction_status(
                &client,
                &CreatorPubky::from_str(CREATOR).unwrap(),
                &BundleId::from_str(BUNDLE_ID).unwrap(),
            )
            .await
            .unwrap_err();

            assert_eq!(error, PaykitPaymentStatusError);
        }
    }

    fn invoice_request() -> PaykitInvoiceRequest {
        PaykitInvoiceRequest {
            bundle_id: BUNDLE_ID.to_owned(),
            lock_resource: LOCK_RESOURCE.to_owned(),
            reader: READER.to_owned(),
            payment_in: 24,
        }
    }

    #[derive(Clone, Default)]
    struct CapturedRequests(Arc<Mutex<Vec<CapturedRequest>>>);

    impl CapturedRequests {
        fn push(&self, request: CapturedRequest) {
            self.0.lock().unwrap().push(request);
        }

        fn single(&self) -> CapturedRequest {
            let requests = self.0.lock().unwrap();
            assert_eq!(requests.len(), 1);
            requests[0].clone()
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct CapturedRequest {
        path: String,
        signature: Option<String>,
        body: Vec<u8>,
    }

    async fn spawn_test_server(captured: CapturedRequests) -> String {
        let app = Router::new()
            .route("/invoices", post(capture_invoice))
            .route("/transactions/status", post(capture_status))
            .with_state(captured);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }

    async fn spawn_hanging_test_server() -> String {
        let app = Router::new()
            .route("/invoices", post(hang))
            .route("/transactions/status", post(hang));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }

    async fn spawn_configured_invoice_server(
        status: axum::http::StatusCode,
        body: &'static str,
    ) -> String {
        let app = Router::new()
            .route("/invoices", post(configured_status))
            .with_state(ConfiguredStatusResponse { status, body });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }

    #[derive(Clone)]
    struct ConfiguredStatusResponse {
        status: axum::http::StatusCode,
        body: &'static str,
    }

    async fn spawn_configured_status_server(
        status: axum::http::StatusCode,
        body: &'static str,
    ) -> String {
        let app = Router::new()
            .route("/transactions/status", post(configured_status))
            .with_state(ConfiguredStatusResponse { status, body });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }

    async fn configured_status(
        State(response): State<ConfiguredStatusResponse>,
    ) -> impl IntoResponse {
        (response.status, response.body)
    }

    async fn hang() -> impl IntoResponse {
        std::future::pending::<()>().await;
        axum::http::StatusCode::OK
    }

    async fn capture_invoice(
        State(captured): State<CapturedRequests>,
        headers: HeaderMap,
        body: Bytes,
    ) -> impl IntoResponse {
        captured.push(CapturedRequest {
            path: "/invoices".to_owned(),
            signature: headers
                .get(SIGNATURE_HEADER)
                .map(|value| value.to_str().unwrap().to_owned()),
            body: body.to_vec(),
        });
        Json(json!({
            "invoice_created_at": "2026-08-12T10:00:00Z",
            "payment_deadline": "2026-08-13T10:00:00Z",
        }))
    }

    async fn capture_status(
        State(captured): State<CapturedRequests>,
        headers: HeaderMap,
        body: Bytes,
    ) -> impl IntoResponse {
        captured.push(CapturedRequest {
            path: "/transactions/status".to_owned(),
            signature: headers
                .get(SIGNATURE_HEADER)
                .map(|value| value.to_str().unwrap().to_owned()),
            body: body.to_vec(),
        });
        Json(json!({
            "status": "detected",
            "confirmations": 0,
            "amount_matched": true,
        }))
    }
}
