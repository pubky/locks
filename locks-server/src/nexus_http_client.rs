use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use http_body_util::{BodyExt, Empty};
use hyper::body::Bytes;
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::{Client as HyperClient, connect::HttpConnector};
use hyper_util::rt::TokioExecutor;
use locks_core::ids::CreatorPubky;
use locks_core::lock_policy::{GraphParticipantV2, GraphPredicateV2};
use locks_service::application::ports::Clock;
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use reqwest::StatusCode;
use serde::Deserialize;
use tokio::sync::Semaphore;
use url::Url;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(4);
const BODY_LIMIT: usize = 64 * 1024;
const MAX_RETRY_AFTER_SECONDS: u64 = 60;
const PATH_SEGMENT: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'/')
    .add(b':')
    .add(b'<')
    .add(b'>')
    .add(b'?')
    .add(b'@')
    .add(b'[')
    .add(b'\\')
    .add(b']')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}');
const DOT_PATH_SEGMENT: &AsciiSet = &PATH_SEGMENT.add(b'.');

#[derive(Debug, thiserror::Error)]
pub enum NexusClientConfigError {
    #[error("nexus URL must be an exact HTTPS origin, or loopback HTTP in insecure test mode")]
    InvalidOrigin,

    #[error("failed to load native TLS roots for strict Nexus HTTP client: {0}")]
    TlsRoots(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NexusInvalidResponse {
    Malformed,
    Oversized,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NexusOperationalError {
    Redirect,
    RateLimited,
    NonSuccess,
    Timeout,
    Transport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NexusObservation {
    Valid {
        value: bool,
        origin: String,
        status: u16,
    },
    Invalid {
        kind: NexusInvalidResponse,
        origin: String,
        status: u16,
    },
    Operational {
        kind: NexusOperationalError,
        origin: String,
        status: Option<u16>,
        retry_after_seconds: Option<u64>,
    },
}

#[async_trait]
pub trait NexusGraphVerifier: Send + Sync {
    async fn verify_predicate(
        &self,
        predicate: &GraphPredicateV2,
        creator: &CreatorPubky,
        reader: &CreatorPubky,
        not_after: time::OffsetDateTime,
    ) -> TimedNexusObservation;
}

#[derive(Clone)]
pub struct UnavailableNexusGraphVerifier {
    clock: Arc<dyn Clock>,
}

impl UnavailableNexusGraphVerifier {
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self { clock }
    }
}

#[async_trait]
impl NexusGraphVerifier for UnavailableNexusGraphVerifier {
    async fn verify_predicate(
        &self,
        _predicate: &GraphPredicateV2,
        _creator: &CreatorPubky,
        _reader: &CreatorPubky,
        _not_after: time::OffsetDateTime,
    ) -> TimedNexusObservation {
        TimedNexusObservation {
            observation: NexusObservation::Operational {
                kind: NexusOperationalError::Transport,
                origin: "nexus-unconfigured".to_owned(),
                status: None,
                retry_after_seconds: None,
            },
            observed_at: self.clock.now(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimedNexusObservation {
    pub observation: NexusObservation,
    pub observed_at: time::OffsetDateTime,
}

#[derive(Debug, Clone)]
pub struct NexusEndpoint {
    normalized_origin: String,
}

impl NexusEndpoint {
    pub fn parse(
        server_url: &str,
        allow_insecure_loopback: bool,
    ) -> Result<Self, NexusClientConfigError> {
        let origin = parse_origin(server_url, allow_insecure_loopback)?;
        let normalized_origin = normalized_origin(&origin);
        Ok(Self { normalized_origin })
    }

    pub fn normalized_origin(&self) -> &str {
        &self.normalized_origin
    }
}

#[derive(Debug)]
pub struct LiveNexusEndpoint {
    current: RwLock<NexusEndpoint>,
    allow_insecure_loopback: bool,
}

impl LiveNexusEndpoint {
    pub fn new(initial: NexusEndpoint, allow_insecure_loopback: bool) -> Self {
        Self {
            current: RwLock::new(initial),
            allow_insecure_loopback,
        }
    }

    pub fn current(&self) -> NexusEndpoint {
        self.current
            .read()
            .expect("live Nexus endpoint lock must not be poisoned")
            .clone()
    }

    pub fn update(&self, server_url: &str) -> Result<(), NexusClientConfigError> {
        let endpoint = NexusEndpoint::parse(server_url, self.allow_insecure_loopback)?;
        *self
            .current
            .write()
            .expect("live Nexus endpoint lock must not be poisoned") = endpoint;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
struct UtcClock;

impl Clock for UtcClock {
    fn now(&self) -> time::OffsetDateTime {
        time::OffsetDateTime::now_utc()
    }
}

#[derive(Clone)]
pub struct NexusHttpClient {
    endpoint_resolver: Arc<dyn Fn() -> NexusEndpoint + Send + Sync>,
    raw_http: HyperClient<HttpsConnector<HttpConnector>, Empty<Bytes>>,
    global_permits: Arc<Semaphore>,
    clock: Arc<dyn Clock>,
}

#[derive(Debug, Deserialize)]
struct RelationshipResponse {
    following: bool,
}

#[derive(Debug, Deserialize)]
struct TaggerResponse {
    relationship: bool,
}

impl NexusHttpClient {
    pub fn new(
        server_url: &str,
        allow_insecure_loopback: bool,
    ) -> Result<Self, NexusClientConfigError> {
        Self::with_global_semaphore(
            server_url,
            allow_insecure_loopback,
            Arc::new(Semaphore::new(32)),
        )
    }

    pub fn with_global_semaphore(
        server_url: &str,
        allow_insecure_loopback: bool,
        global_permits: Arc<Semaphore>,
    ) -> Result<Self, NexusClientConfigError> {
        let endpoint = NexusEndpoint::parse(server_url, allow_insecure_loopback)?;
        Self::with_endpoint_resolver_and_clock(
            Arc::new(move || endpoint.clone()),
            global_permits,
            Arc::new(UtcClock),
        )
    }

    pub fn with_endpoint_resolver_and_clock(
        endpoint_resolver: Arc<dyn Fn() -> NexusEndpoint + Send + Sync>,
        global_permits: Arc<Semaphore>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, NexusClientConfigError> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let mut connector = HttpConnector::new();
        connector.enforce_http(false);
        connector.set_connect_timeout(Some(CONNECT_TIMEOUT));
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_native_roots()
            .map_err(|error| NexusClientConfigError::TlsRoots(error.to_string()))?
            .https_or_http()
            .enable_http1()
            .wrap_connector(connector);
        let raw_http = HyperClient::builder(TokioExecutor::new()).build(https);
        Ok(Self {
            endpoint_resolver,
            raw_http,
            global_permits,
            clock,
        })
    }

    pub fn with_endpoint_resolver(
        endpoint_resolver: Arc<dyn Fn() -> NexusEndpoint + Send + Sync>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, NexusClientConfigError> {
        Self::with_endpoint_resolver_and_clock(
            endpoint_resolver,
            Arc::new(Semaphore::new(32)),
            clock,
        )
    }

    pub fn normalized_origin(&self) -> String {
        (self.endpoint_resolver)().normalized_origin
    }

    pub async fn verify(
        &self,
        predicate: &GraphPredicateV2,
        creator: &CreatorPubky,
        reader: &CreatorPubky,
    ) -> NexusObservation {
        let no_task_deadline = self.clock.now() + time::Duration::days(36_500);
        self.verify_before(predicate, creator, reader, no_task_deadline)
            .await
            .observation
    }

    async fn verify_before(
        &self,
        predicate: &GraphPredicateV2,
        creator: &CreatorPubky,
        reader: &CreatorPubky,
        not_after: time::OffsetDateTime,
    ) -> TimedNexusObservation {
        let now = self.clock.now();
        let selected_endpoint = Arc::new(std::sync::Mutex::new(None));
        let observation = if now >= not_after {
            let endpoint = (self.endpoint_resolver)();
            operational(&endpoint, NexusOperationalError::Timeout, None, None)
        } else {
            let remaining = std::time::Duration::try_from(not_after - now)
                .unwrap_or(Duration::ZERO)
                .min(REQUEST_TIMEOUT);
            match tokio::time::timeout(
                remaining,
                self.execute_before(
                    predicate,
                    creator,
                    reader,
                    not_after,
                    Arc::clone(&selected_endpoint),
                ),
            )
            .await
            {
                Ok(observation) => observation,
                Err(_) => {
                    let endpoint = selected_endpoint
                        .lock()
                        .expect("selected Nexus endpoint lock poisoned")
                        .clone()
                        .unwrap_or_else(|| (self.endpoint_resolver)());
                    operational(&endpoint, NexusOperationalError::Timeout, None, None)
                }
            }
        };
        TimedNexusObservation {
            observation,
            observed_at: self.clock.now(),
        }
    }

    async fn execute_before(
        &self,
        predicate: &GraphPredicateV2,
        creator: &CreatorPubky,
        reader: &CreatorPubky,
        not_after: time::OffsetDateTime,
        selected_endpoint: Arc<std::sync::Mutex<Option<NexusEndpoint>>>,
    ) -> NexusObservation {
        let _permit = match self.global_permits.acquire().await {
            Ok(permit) => permit,
            Err(_) => {
                let endpoint = (self.endpoint_resolver)();
                return operational(&endpoint, NexusOperationalError::Transport, None, None);
            }
        };
        let endpoint = (self.endpoint_resolver)();
        *selected_endpoint
            .lock()
            .expect("selected Nexus endpoint lock poisoned") = Some(endpoint.clone());
        if self.clock.now() >= not_after {
            return operational(&endpoint, NexusOperationalError::Timeout, None, None);
        }
        self.execute_raw(&endpoint, predicate, creator, reader)
            .await
    }

    async fn execute_raw(
        &self,
        endpoint: &NexusEndpoint,
        predicate: &GraphPredicateV2,
        creator: &CreatorPubky,
        reader: &CreatorPubky,
    ) -> NexusObservation {
        let (uri, follows) = raw_request_uri(endpoint, predicate, creator, reader);
        let request = match hyper::Request::get(uri).body(Empty::<Bytes>::new()) {
            Ok(request) => request,
            Err(_) => {
                return operational(endpoint, NexusOperationalError::Transport, None, None);
            }
        };
        let response = match self.raw_http.request(request).await {
            Ok(response) => response,
            Err(_) => {
                return operational(endpoint, NexusOperationalError::Transport, None, None);
            }
        };
        let status = response.status();
        if follows && status == StatusCode::NOT_FOUND {
            return NexusObservation::Valid {
                value: false,
                origin: endpoint.normalized_origin.clone(),
                status: status.as_u16(),
            };
        }
        if status.is_redirection() {
            return operational(
                endpoint,
                NexusOperationalError::Redirect,
                Some(status.as_u16()),
                None,
            );
        }
        if status == StatusCode::TOO_MANY_REQUESTS {
            let retry_after = response
                .headers()
                .get(hyper::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                .map(|seconds| seconds.min(MAX_RETRY_AFTER_SECONDS));
            return operational(
                endpoint,
                NexusOperationalError::RateLimited,
                Some(status.as_u16()),
                retry_after,
            );
        }
        if !status.is_success() {
            return operational(
                endpoint,
                NexusOperationalError::NonSuccess,
                Some(status.as_u16()),
                None,
            );
        }
        if response
            .headers()
            .get(hyper::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .is_some_and(|length| length > BODY_LIMIT as u64)
        {
            return NexusObservation::Invalid {
                kind: NexusInvalidResponse::Oversized,
                origin: endpoint.normalized_origin.clone(),
                status: status.as_u16(),
            };
        }
        let mut incoming = response.into_body();
        let mut body = Vec::new();
        while let Some(frame) = incoming.frame().await {
            let frame = match frame {
                Ok(frame) => frame,
                Err(_) => {
                    return operational(
                        endpoint,
                        NexusOperationalError::Transport,
                        Some(status.as_u16()),
                        None,
                    );
                }
            };
            if let Some(data) = frame.data_ref() {
                if body.len().saturating_add(data.len()) > BODY_LIMIT {
                    return NexusObservation::Invalid {
                        kind: NexusInvalidResponse::Oversized,
                        origin: endpoint.normalized_origin.clone(),
                        status: status.as_u16(),
                    };
                }
                body.extend_from_slice(data);
            }
        }
        let value = if follows {
            serde_json::from_slice::<RelationshipResponse>(&body).map(|body| body.following)
        } else {
            serde_json::from_slice::<TaggerResponse>(&body).map(|body| body.relationship)
        };
        match value {
            Ok(value) => NexusObservation::Valid {
                value,
                origin: endpoint.normalized_origin.clone(),
                status: status.as_u16(),
            },
            Err(_) => NexusObservation::Invalid {
                kind: NexusInvalidResponse::Malformed,
                origin: endpoint.normalized_origin.clone(),
                status: status.as_u16(),
            },
        }
    }
}

fn operational(
    endpoint: &NexusEndpoint,
    kind: NexusOperationalError,
    status: Option<u16>,
    retry_after_seconds: Option<u64>,
) -> NexusObservation {
    NexusObservation::Operational {
        kind,
        origin: endpoint.normalized_origin.clone(),
        status,
        retry_after_seconds,
    }
}

fn raw_request_uri(
    endpoint: &NexusEndpoint,
    predicate: &GraphPredicateV2,
    creator: &CreatorPubky,
    reader: &CreatorPubky,
) -> (String, bool) {
    let participant = |role: GraphParticipantV2| match role {
        GraphParticipantV2::Creator => creator.to_string(),
        GraphParticipantV2::Reader => reader.to_string(),
    };
    match predicate {
        GraphPredicateV2::Follows { follower, followed } => {
            let actor = participant(*follower);
            let subject = participant(*followed);
            (
                format!(
                    "{}/v0/user/{}/relationship/{}",
                    endpoint.normalized_origin,
                    utf8_percent_encode(&subject, PATH_SEGMENT),
                    utf8_percent_encode(&actor, PATH_SEGMENT)
                ),
                true,
            )
        }
        GraphPredicateV2::Tagged {
            tagger,
            tagged,
            label,
        } => {
            let actor = participant(*tagger);
            let subject = participant(*tagged);
            let normalized = label.trim().to_lowercase();
            let label_set = if matches!(normalized.as_str(), "." | "..") {
                DOT_PATH_SEGMENT
            } else {
                PATH_SEGMENT
            };
            (
                format!(
                    "{}/v0/user/{}/taggers/{}?viewer_id={}",
                    endpoint.normalized_origin,
                    utf8_percent_encode(&subject, PATH_SEGMENT),
                    utf8_percent_encode(&normalized, label_set),
                    utf8_percent_encode(&actor, PATH_SEGMENT)
                ),
                false,
            )
        }
    }
}

#[async_trait]
impl NexusGraphVerifier for NexusHttpClient {
    async fn verify_predicate(
        &self,
        predicate: &GraphPredicateV2,
        creator: &CreatorPubky,
        reader: &CreatorPubky,
        not_after: time::OffsetDateTime,
    ) -> TimedNexusObservation {
        self.verify_before(predicate, creator, reader, not_after)
            .await
    }
}

fn parse_origin(
    server_url: &str,
    allow_insecure_loopback: bool,
) -> Result<Url, NexusClientConfigError> {
    let url = Url::parse(server_url).map_err(|_| NexusClientConfigError::InvalidOrigin)?;
    let exact_origin = url.username().is_empty()
        && url.password().is_none()
        && url.query().is_none()
        && url.fragment().is_none()
        && (url.path().is_empty() || url.path() == "/")
        && url.host_str().is_some();
    if !exact_origin {
        return Err(NexusClientConfigError::InvalidOrigin);
    }
    let secure = url.scheme() == "https";
    let insecure_loopback = allow_insecure_loopback
        && url.scheme() == "http"
        && matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "::1"));
    if !secure && !insecure_loopback {
        return Err(NexusClientConfigError::InvalidOrigin);
    }
    Ok(url)
}

fn normalized_origin(url: &Url) -> String {
    url.origin().ascii_serialization()
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
    const READER: &str = "pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky";

    #[test]
    fn production_origin_must_be_exact_https_origin() {
        assert!(NexusHttpClient::new("https://nexus.example", false).is_ok());
        assert!(NexusHttpClient::new("http://nexus.example", false).is_err());
        assert!(NexusHttpClient::new("https://user@nexus.example", false).is_err());
        assert!(NexusHttpClient::new("https://nexus.example/path", false).is_err());
        assert!(NexusHttpClient::new("https://nexus.example?x=1", false).is_err());
        assert!(NexusHttpClient::new("https://nexus.example#x", false).is_err());
    }

    #[test]
    fn insecure_mode_is_loopback_only() {
        assert!(NexusHttpClient::new("http://127.0.0.1:8080", true).is_ok());
        assert!(NexusHttpClient::new("http://localhost:8080", true).is_ok());
        assert!(NexusHttpClient::new("http://192.0.2.1:8080", true).is_err());
    }

    #[test]
    fn routes_preserve_asymmetric_roles_and_encode_normalized_label_once() {
        let client = NexusHttpClient::new("http://127.0.0.1:8080", true).unwrap();
        let creator = CreatorPubky::from_str(CREATOR).unwrap();
        let reader = CreatorPubky::from_str(READER).unwrap();
        let endpoint = (client.endpoint_resolver)();
        let (uri, follows) = raw_request_uri(
            &endpoint,
            &GraphPredicateV2::Follows {
                follower: GraphParticipantV2::Reader,
                followed: GraphParticipantV2::Creator,
            },
            &creator,
            &reader,
        );
        assert!(follows);
        assert_eq!(
            uri,
            format!("http://127.0.0.1:8080/v0/user/{CREATOR}/relationship/{READER}")
        );

        let (uri, follows) = raw_request_uri(
            &endpoint,
            &GraphPredicateV2::Tagged {
                tagger: GraphParticipantV2::Creator,
                tagged: GraphParticipantV2::Reader,
                label: "  Ä/Tag?  ".to_owned(),
            },
            &creator,
            &reader,
        );
        assert!(!follows);
        assert_eq!(
            uri,
            format!(
                "http://127.0.0.1:8080/v0/user/{READER}/taggers/%C3%A4%2Ftag%3F?viewer_id={CREATOR}"
            )
        );

        for (label, encoded) in [(".", "%2E"), ("..", "%2E%2E")] {
            let (uri, follows) = raw_request_uri(
                &endpoint,
                &GraphPredicateV2::Tagged {
                    tagger: GraphParticipantV2::Creator,
                    tagged: GraphParticipantV2::Reader,
                    label: label.to_owned(),
                },
                &creator,
                &reader,
            );
            assert!(!follows);
            assert_eq!(
                uri,
                format!(
                    "http://127.0.0.1:8080/v0/user/{READER}/taggers/{encoded}?viewer_id={CREATOR}"
                )
            );
        }
    }

    #[tokio::test]
    async fn global_backpressure_never_starts_io_after_task_deadline() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let permits = Arc::new(Semaphore::new(1));
        let held = permits.clone().acquire_owned().await.unwrap();
        let client =
            NexusHttpClient::with_global_semaphore(&format!("http://{address}"), true, permits)
                .unwrap();
        let creator = CreatorPubky::from_str(CREATOR).unwrap();
        let reader = CreatorPubky::from_str(READER).unwrap();
        let predicate = GraphPredicateV2::Follows {
            follower: GraphParticipantV2::Reader,
            followed: GraphParticipantV2::Creator,
        };
        let verification = tokio::spawn(async move {
            client
                .verify_before(
                    &predicate,
                    &creator,
                    &reader,
                    time::OffsetDateTime::now_utc() + time::Duration::milliseconds(25),
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(held);
        assert!(matches!(
            verification.await.unwrap().observation,
            NexusObservation::Operational {
                kind: NexusOperationalError::Timeout,
                status: None,
                ..
            }
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(50), listener.accept())
                .await
                .is_err()
        );
    }

    #[test]
    fn global_concurrency_bound_is_exactly_32() {
        let client = NexusHttpClient::new("https://nexus.example", false).unwrap();
        assert_eq!(client.global_permits.available_permits(), 32);
    }

    #[tokio::test]
    async fn dot_only_tag_labels_are_sent_as_one_percent_encoded_path_segment() {
        let (endpoint, request) = response_endpoint(br#"{"users":[],"relationship":true}"#).await;
        let resolver_endpoint = endpoint.clone();
        let client = NexusHttpClient::with_endpoint_resolver(
            Arc::new(move || resolver_endpoint.clone()),
            Arc::new(UtcClock),
        )
        .unwrap();
        let creator = CreatorPubky::from_str(CREATOR).unwrap();
        let reader = CreatorPubky::from_str(READER).unwrap();
        let tagged = GraphPredicateV2::Tagged {
            tagger: GraphParticipantV2::Creator,
            tagged: GraphParticipantV2::Reader,
            label: ".".to_owned(),
        };

        assert!(matches!(
            client.verify(&tagged, &creator, &reader).await,
            NexusObservation::Valid { value: true, .. }
        ));
        let request = request.await.unwrap();
        assert!(request.contains(&format!(
            "GET /v0/user/{READER}/taggers/%2E?viewer_id={CREATOR} HTTP/1.1"
        )));
    }

    #[tokio::test]
    async fn url_change_mid_attempt_preserves_origin_used_by_timed_out_request() {
        let (first_endpoint, request_started) = stalling_endpoint().await;
        let second_endpoint = NexusEndpoint::parse("http://127.0.0.1:1", true).unwrap();
        let current = Arc::new(LiveNexusEndpoint::new(first_endpoint.clone(), true));
        let resolver = {
            let current = current.clone();
            Arc::new(move || current.current())
        };
        let client = NexusHttpClient::with_endpoint_resolver(resolver, Arc::new(UtcClock)).unwrap();
        let creator = CreatorPubky::from_str(CREATOR).unwrap();
        let reader = CreatorPubky::from_str(READER).unwrap();
        let tagged = GraphPredicateV2::Tagged {
            tagger: GraphParticipantV2::Reader,
            tagged: GraphParticipantV2::Creator,
            label: "news".to_owned(),
        };
        let verification = tokio::spawn(async move {
            client
                .verify_before(
                    &tagged,
                    &creator,
                    &reader,
                    time::OffsetDateTime::now_utc() + time::Duration::milliseconds(100),
                )
                .await
        });
        request_started.await.unwrap();
        current.update(second_endpoint.normalized_origin()).unwrap();
        let result = verification.await.unwrap();
        assert!(matches!(
            result.observation,
            NexusObservation::Operational {
                kind: NexusOperationalError::Timeout,
                ref origin,
                ..
            } if origin == &first_endpoint.normalized_origin
        ));
    }

    #[tokio::test]
    async fn endpoint_is_resolved_per_request_and_observation_records_actual_origin() {
        let (first_endpoint, first_request) = response_endpoint(br#"{"following":false}"#).await;
        let (second_endpoint, second_request) = response_endpoint(br#"{"following":true}"#).await;
        let current = Arc::new(LiveNexusEndpoint::new(first_endpoint.clone(), true));
        let resolver = {
            let current = current.clone();
            Arc::new(move || current.current())
        };
        let client = NexusHttpClient::with_endpoint_resolver(resolver, Arc::new(UtcClock)).unwrap();
        let creator = CreatorPubky::from_str(CREATOR).unwrap();
        let reader = CreatorPubky::from_str(READER).unwrap();
        let follows = GraphPredicateV2::Follows {
            follower: GraphParticipantV2::Reader,
            followed: GraphParticipantV2::Creator,
        };

        let first = client.verify(&follows, &creator, &reader).await;
        assert!(matches!(
            first,
            NexusObservation::Valid { value: false, ref origin, .. }
                if origin == &first_endpoint.normalized_origin
        ));
        current.update(second_endpoint.normalized_origin()).unwrap();
        let second = client.verify(&follows, &creator, &reader).await;
        assert!(matches!(
            second,
            NexusObservation::Valid { value: true, ref origin, .. }
                if origin == &second_endpoint.normalized_origin
        ));
        assert!(first_request.await.unwrap().contains("/relationship/"));
        assert!(second_request.await.unwrap().contains("/relationship/"));
    }

    #[tokio::test]
    async fn production_shaped_relationship_and_taggers_responses_are_projected() {
        let creator = CreatorPubky::from_str(CREATOR).unwrap();
        let reader = CreatorPubky::from_str(READER).unwrap();
        let follows = GraphPredicateV2::Follows {
            follower: GraphParticipantV2::Reader,
            followed: GraphParticipantV2::Creator,
        };
        let tagged = GraphPredicateV2::Tagged {
            tagger: GraphParticipantV2::Reader,
            tagged: GraphParticipantV2::Creator,
            label: "news".to_owned(),
        };

        let client = one_response_client(
            "200 OK",
            &[],
            br#"{"following":true,"followed_by":false,"muted":false}"#,
        )
        .await;
        assert!(matches!(
            client.verify(&follows, &creator, &reader).await,
            NexusObservation::Valid { value: true, .. }
        ));

        let client = one_response_client(
            "200 OK",
            &[],
            br#"{"users":["pubkyexample"],"relationship":false}"#,
        )
        .await;
        assert!(matches!(
            client.verify(&tagged, &creator, &reader).await,
            NexusObservation::Valid { value: false, .. }
        ));
    }

    #[tokio::test]
    async fn status_and_body_taxonomy_is_closed() {
        let creator = CreatorPubky::from_str(CREATOR).unwrap();
        let reader = CreatorPubky::from_str(READER).unwrap();
        let follows = GraphPredicateV2::Follows {
            follower: GraphParticipantV2::Reader,
            followed: GraphParticipantV2::Creator,
        };
        let tagged = GraphPredicateV2::Tagged {
            tagger: GraphParticipantV2::Reader,
            tagged: GraphParticipantV2::Creator,
            label: "news".to_owned(),
        };

        let client = one_response_client("404 Not Found", &[], b"not-json").await;
        assert!(matches!(
            client.verify(&follows, &creator, &reader).await,
            NexusObservation::Valid {
                value: false,
                status: 404,
                ..
            }
        ));
        let client = one_response_client("302 Found", &[("Location", "/other")], b"").await;
        assert!(matches!(
            client.verify(&follows, &creator, &reader).await,
            NexusObservation::Operational {
                kind: NexusOperationalError::Redirect,
                status: Some(302),
                ..
            }
        ));
        let client =
            one_response_client("429 Too Many Requests", &[("Retry-After", "999")], b"").await;
        assert!(matches!(
            client.verify(&follows, &creator, &reader).await,
            NexusObservation::Operational {
                kind: NexusOperationalError::RateLimited,
                retry_after_seconds: Some(60),
                ..
            }
        ));
        let client =
            one_response_client("429 Too Many Requests", &[("Retry-After", "60")], b"").await;
        assert!(matches!(
            client.verify(&follows, &creator, &reader).await,
            NexusObservation::Operational {
                kind: NexusOperationalError::RateLimited,
                retry_after_seconds: Some(60),
                ..
            }
        ));
        let client = one_response_client("200 OK", &[], br#"{"relationship":false}"#).await;
        assert!(matches!(
            client.verify(&tagged, &creator, &reader).await,
            NexusObservation::Valid {
                value: false,
                status: 200,
                ..
            }
        ));
        let client = one_response_client("200 OK", &[], br#"{"users":[]}"#).await;
        assert!(matches!(
            client.verify(&tagged, &creator, &reader).await,
            NexusObservation::Invalid {
                kind: NexusInvalidResponse::Malformed,
                ..
            }
        ));
        let oversized = vec![b'x'; BODY_LIMIT + 1];
        let client = one_response_client("200 OK", &[], &oversized).await;
        assert!(matches!(
            client.verify(&tagged, &creator, &reader).await,
            NexusObservation::Invalid {
                kind: NexusInvalidResponse::Oversized,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn chunked_overflow_and_stalled_body_are_bounded() {
        let creator = CreatorPubky::from_str(CREATOR).unwrap();
        let reader = CreatorPubky::from_str(READER).unwrap();
        let tagged = GraphPredicateV2::Tagged {
            tagger: GraphParticipantV2::Reader,
            tagged: GraphParticipantV2::Creator,
            label: "news".to_owned(),
        };

        let oversized = vec![b'x'; BODY_LIMIT + 1];
        let chunked = format!(
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{:X}\r\n{}\r\n0\r\n\r\n",
            oversized.len(),
            String::from_utf8_lossy(&oversized)
        );
        let client = raw_response_client(chunked.into_bytes(), false).await;
        assert!(matches!(
            client.verify(&tagged, &creator, &reader).await,
            NexusObservation::Invalid {
                kind: NexusInvalidResponse::Oversized,
                ..
            }
        ));

        let stalled = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1\r\n{".to_vec();
        let client = raw_response_client(stalled, true).await;
        let result = client
            .verify_before(
                &tagged,
                &creator,
                &reader,
                time::OffsetDateTime::now_utc() + time::Duration::milliseconds(50),
            )
            .await;
        assert!(matches!(
            result.observation,
            NexusObservation::Operational {
                kind: NexusOperationalError::Timeout,
                ..
            }
        ));
    }

    async fn stalling_endpoint() -> (NexusEndpoint, tokio::sync::oneshot::Receiver<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0; 8192];
            let _ = socket.read(&mut request).await.unwrap();
            let _ = started_tx.send(());
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1\r\n{")
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });
        (
            NexusEndpoint::parse(&format!("http://{address}"), true).unwrap(),
            started_rx,
        )
    }

    async fn raw_response_client(response: Vec<u8>, hold_open: bool) -> NexusHttpClient {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0; 8192];
            let _ = socket.read(&mut request).await.unwrap();
            socket.write_all(&response).await.unwrap();
            if hold_open {
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });
        NexusHttpClient::new(&format!("http://{address}"), true).unwrap()
    }

    async fn response_endpoint(
        body: &'static [u8],
    ) -> (NexusEndpoint, tokio::sync::oneshot::Receiver<String>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (request_tx, request_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0; 8192];
            let count = socket.read(&mut request).await.unwrap();
            request.truncate(count);
            let _ = request_tx.send(String::from_utf8_lossy(&request).into_owned());
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            socket.write_all(body).await.unwrap();
        });
        (
            NexusEndpoint::parse(&format!("http://{address}"), true).unwrap(),
            request_rx,
        )
    }

    async fn one_response_client(
        status: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> NexusHttpClient {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let status = status.to_owned();
        let headers: Vec<_> = headers
            .iter()
            .map(|(n, v)| ((*n).to_owned(), (*v).to_owned()))
            .collect();
        let body = body.to_vec();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0; 8192];
            let _ = socket.read(&mut request).await.unwrap();
            let mut response = format!(
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n",
                body.len()
            );
            for (name, value) in headers {
                response.push_str(&format!("{name}: {value}\r\n"));
            }
            response.push_str("\r\n");
            socket.write_all(response.as_bytes()).await.unwrap();
            socket.write_all(&body).await.unwrap();
        });
        NexusHttpClient::new(&format!("http://{address}"), true).unwrap()
    }
}
