use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use locks_core::ids::CreatorPubky;
use locks_core::lock_policy::{GraphParticipantV2, GraphPredicateV2};
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use reqwest::{StatusCode, redirect::Policy};
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

#[derive(Debug, thiserror::Error)]
pub enum NexusClientConfigError {
    #[error("nexus URL must be an exact HTTPS origin, or loopback HTTP in insecure test mode")]
    InvalidOrigin,
    #[error("failed to build strict Nexus HTTP client: {0}")]
    Http(#[from] reqwest::Error),
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
    ) -> NexusObservation;
}

#[derive(Debug, Clone)]
pub struct NexusHttpClient {
    origin: Url,
    normalized_origin: String,
    http: reqwest::Client,
    global_permits: Arc<Semaphore>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RelationshipResponse {
    following: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
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
        let origin = parse_origin(server_url, allow_insecure_loopback)?;
        let normalized_origin = normalized_origin(&origin);
        let http = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .redirect(Policy::none())
            .build()?;
        Ok(Self {
            origin,
            normalized_origin,
            http,
            global_permits,
        })
    }

    pub fn normalized_origin(&self) -> &str {
        &self.normalized_origin
    }

    pub async fn verify(
        &self,
        predicate: &GraphPredicateV2,
        creator: &CreatorPubky,
        reader: &CreatorPubky,
    ) -> NexusObservation {
        let no_task_deadline = time::OffsetDateTime::now_utc() + time::Duration::days(36_500);
        self.verify_before(predicate, creator, reader, no_task_deadline)
            .await
    }

    async fn verify_before(
        &self,
        predicate: &GraphPredicateV2,
        creator: &CreatorPubky,
        reader: &CreatorPubky,
        not_after: time::OffsetDateTime,
    ) -> NexusObservation {
        let _permit = match self.global_permits.acquire().await {
            Ok(permit) => permit,
            Err(_) => {
                return self.operational(NexusOperationalError::Transport, None, None);
            }
        };
        if time::OffsetDateTime::now_utc() >= not_after {
            return self.operational(NexusOperationalError::Timeout, None, None);
        }
        let (url, follows) = self.request_url(predicate, creator, reader);
        let response = match self.http.get(url).send().await {
            Ok(response) => response,
            Err(error) => {
                let kind = if error.is_timeout() {
                    NexusOperationalError::Timeout
                } else if error.is_redirect() {
                    NexusOperationalError::Redirect
                } else {
                    NexusOperationalError::Transport
                };
                return self.operational(kind, error.status().map(|status| status.as_u16()), None);
            }
        };
        let status = response.status();
        if follows && status == StatusCode::NOT_FOUND {
            return NexusObservation::Valid {
                value: false,
                origin: self.normalized_origin.clone(),
                status: status.as_u16(),
            };
        }
        if status.is_redirection() {
            return self.operational(NexusOperationalError::Redirect, Some(status.as_u16()), None);
        }
        if status == StatusCode::TOO_MANY_REQUESTS {
            let retry_after = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                .filter(|seconds| *seconds <= MAX_RETRY_AFTER_SECONDS);
            return self.operational(
                NexusOperationalError::RateLimited,
                Some(status.as_u16()),
                retry_after,
            );
        }
        if !status.is_success() {
            return self.operational(
                NexusOperationalError::NonSuccess,
                Some(status.as_u16()),
                None,
            );
        }
        if response
            .content_length()
            .is_some_and(|length| length > BODY_LIMIT as u64)
        {
            return NexusObservation::Invalid {
                kind: NexusInvalidResponse::Oversized,
                origin: self.normalized_origin.clone(),
                status: status.as_u16(),
            };
        }
        let body = match bounded_body(response).await {
            Ok(body) => body,
            Err(BodyError::Oversized) => {
                return NexusObservation::Invalid {
                    kind: NexusInvalidResponse::Oversized,
                    origin: self.normalized_origin.clone(),
                    status: status.as_u16(),
                };
            }
            Err(BodyError::Transport(error)) => {
                return self.operational(
                    if error.is_timeout() {
                        NexusOperationalError::Timeout
                    } else {
                        NexusOperationalError::Transport
                    },
                    Some(status.as_u16()),
                    None,
                );
            }
        };
        let value = if follows {
            serde_json::from_slice::<RelationshipResponse>(&body).map(|body| body.following)
        } else {
            serde_json::from_slice::<TaggerResponse>(&body).map(|body| body.relationship)
        };
        match value {
            Ok(value) => NexusObservation::Valid {
                value,
                origin: self.normalized_origin.clone(),
                status: status.as_u16(),
            },
            Err(_) => NexusObservation::Invalid {
                kind: NexusInvalidResponse::Malformed,
                origin: self.normalized_origin.clone(),
                status: status.as_u16(),
            },
        }
    }

    fn request_url(
        &self,
        predicate: &GraphPredicateV2,
        creator: &CreatorPubky,
        reader: &CreatorPubky,
    ) -> (Url, bool) {
        let participant = |role: GraphParticipantV2| match role {
            GraphParticipantV2::Creator => creator.to_string(),
            GraphParticipantV2::Reader => reader.to_string(),
        };
        match predicate {
            GraphPredicateV2::Follows { follower, followed } => {
                let actor = participant(*follower);
                let subject = participant(*followed);
                let path = format!("/v0/user/{subject}/relationship/{actor}");
                (
                    self.origin.join(&path).expect("validated origin and IDs"),
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
                let encoded = utf8_percent_encode(&normalized, PATH_SEGMENT).to_string();
                let path = format!("/v0/user/{subject}/taggers/{encoded}");
                let mut url = self.origin.join(&path).expect("validated origin and IDs");
                url.query_pairs_mut().append_pair("viewer_id", &actor);
                (url, false)
            }
        }
    }

    fn operational(
        &self,
        kind: NexusOperationalError,
        status: Option<u16>,
        retry_after_seconds: Option<u64>,
    ) -> NexusObservation {
        NexusObservation::Operational {
            kind,
            origin: self.normalized_origin.clone(),
            status,
            retry_after_seconds,
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
    ) -> NexusObservation {
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

enum BodyError {
    Oversized,
    Transport(reqwest::Error),
}

async fn bounded_body(mut response: reqwest::Response) -> Result<Vec<u8>, BodyError> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(BodyError::Transport)? {
        if body.len().saturating_add(chunk.len()) > BODY_LIMIT {
            return Err(BodyError::Oversized);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
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
        let (url, follows) = client.request_url(
            &GraphPredicateV2::Follows {
                follower: GraphParticipantV2::Reader,
                followed: GraphParticipantV2::Creator,
            },
            &creator,
            &reader,
        );
        assert!(follows);
        assert_eq!(
            url.path(),
            format!("/v0/user/{CREATOR}/relationship/{READER}")
        );

        let (url, follows) = client.request_url(
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
            url.path(),
            format!("/v0/user/{READER}/taggers/%C3%A4%2Ftag%3F")
        );
        assert_eq!(url.query(), Some(format!("viewer_id={CREATOR}").as_str()));
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
            verification.await.unwrap(),
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
                retry_after_seconds: None,
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
