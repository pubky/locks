use std::{future::Future, str::FromStr, time::Duration};

use async_trait::async_trait;
use locks_core::ids::CreatorPubky;
#[allow(
    deprecated,
    reason = "legacy cookie auth remains the current creator authority contract"
)]
use pubky::{AuthFlowKind, Capabilities, CookieCredential, PubkyCookieAuthFlow, PubkySession};
use url::Url;

use crate::application::errors::ApplicationError;
use crate::application::models::{
    CreatorAuthoritySecret, CreatorConnectAuthorizationUrl, LegacyCreatorConnectFlowApproval,
};
use crate::application::ports::LegacyCreatorConnectFlowClient;

const MAX_HOMESERVER_RESOLUTION_ATTEMPTS: usize = 3;
const INITIAL_HOMESERVER_RESOLUTION_RETRY_DELAY: Duration = Duration::from_millis(250);

/// Pubky SDK-backed legacy creator connect-flow client.
#[derive(Debug, Clone)]
pub struct PubkyLegacyCreatorConnectFlowClient {
    pubky: pubky::Pubky,
    auth_relay: Option<Url>,
}

impl PubkyLegacyCreatorConnectFlowClient {
    /// Creates a legacy connect-flow adapter backed by the provided Pubky handle.
    pub fn new(pubky: pubky::Pubky) -> Self {
        Self {
            pubky,
            auth_relay: None,
        }
    }

    /// Creates a legacy connect-flow adapter with an explicit Pubky auth relay inbox base.
    pub fn new_with_auth_relay(pubky: pubky::Pubky, auth_relay: Url) -> Self {
        Self {
            pubky,
            auth_relay: Some(auth_relay),
        }
    }
}

#[async_trait]
#[allow(
    deprecated,
    reason = "legacy cookie auth remains the current creator authority contract"
)]
impl LegacyCreatorConnectFlowClient for PubkyLegacyCreatorConnectFlowClient {
    async fn start_legacy_creator_connect_flow(
        &self,
        requested_scopes: &[String],
    ) -> Result<CreatorConnectAuthorizationUrl, ApplicationError> {
        tracing::info!(
            auth_stage = "flow_start",
            "starting legacy creator auth flow"
        );
        let capabilities = requested_scopes_to_capabilities(requested_scopes)?;
        let flow = match &self.auth_relay {
            Some(auth_relay) => PubkyCookieAuthFlow::builder(&capabilities, AuthFlowKind::signin())
                .relay(auth_relay.clone())
                .start(),
            None => self
                .pubky
                .start_cookie_auth_flow(&capabilities, AuthFlowKind::signin()),
        }
        .map_err(|error| {
            log_pubky_stage_failure("flow_start", &error);
            legacy_connect_flow_error("failed to start legacy creator connect flow")
        })?;
        tracing::info!(
            auth_stage = "flow_start",
            "legacy creator auth flow started"
        );
        Ok(CreatorConnectAuthorizationUrl::new(
            flow.authorization_url().to_string(),
        ))
    }

    async fn await_legacy_creator_connect_flow_approval(
        &self,
        authorization_url: &CreatorConnectAuthorizationUrl,
    ) -> Result<LegacyCreatorConnectFlowApproval, ApplicationError> {
        tracing::info!(
            auth_stage = "flow_resume",
            "resuming legacy creator auth flow"
        );
        let flow = self
            .pubky
            .resume_cookie_auth_flow(authorization_url.expose_url())
            .map_err(|error| {
                log_pubky_stage_failure("flow_resume", &error);
                legacy_connect_flow_error("failed to resume legacy creator connect flow")
            })?;
        let target_homeserver = flow.target_homeserver();
        tracing::info!(
            auth_stage = "relay_approval",
            "waiting for legacy creator auth relay approval"
        );
        // The SDK owns relay long-poll retries. `await_token` consumes this resumed flow;
        // any broader retry must construct another flow from the stored authorization URL.
        let token = flow.await_token().await.map_err(|error| {
            log_pubky_stage_failure("relay_approval", &error);
            legacy_connect_flow_error("legacy creator connect flow approval failed or expired")
        })?;
        tracing::info!(
            auth_stage = "relay_approval",
            "legacy creator auth token received and verified"
        );

        let creator_public_key = token.public_key().clone();
        let homeserver = retry_homeserver_resolution(
            || async {
                let homeserver = match target_homeserver.clone() {
                    Some(homeserver) => homeserver,
                    None => self
                        .pubky
                        .get_homeserver_of(&creator_public_key)
                        .await?
                        .unwrap_or_else(|| creator_public_key.clone()),
                };
                self.pubky
                    .client()
                    .pkarr()
                    .resolve(&homeserver, pubky::pkarr::ResolvePolicy::CacheFirst)
                    .await
                    .map_err(pubky::Error::from)?;
                Ok(homeserver)
            },
            INITIAL_HOMESERVER_RESOLUTION_RETRY_DELAY,
        )
        .await
        .map_err(|_| legacy_connect_flow_error("legacy creator homeserver resolution failed"))?;
        tracing::info!(
            auth_stage = "homeserver_resolution",
            "legacy creator homeserver resolved"
        );

        tracing::info!(
            auth_stage = "session_exchange",
            "starting legacy creator POST /session exchange"
        );
        let credential =
            CookieCredential::from_auth_token(&token, self.pubky.client(), Some(homeserver))
                .await
                .map_err(|error| {
                    log_pubky_stage_failure("session_exchange", &error);
                    legacy_connect_flow_error("legacy creator homeserver session exchange failed")
                })?;
        tracing::info!(
            auth_stage = "session_exchange",
            "legacy creator homeserver session established"
        );
        let session = PubkySession::from_cookie_credential(self.pubky.client().clone(), credential);
        let creator = creator_from_pubky_public_key_z32(&session.info().public_key().z32())?;
        let session_secret = session
            .as_cookie()
            .and_then(|cookie| cookie.export_secret())
            .map(CreatorAuthoritySecret::new)
            .ok_or_else(|| {
                tracing::warn!(
                    auth_stage = "credential_export",
                    error_kind = "missing_cookie_secret",
                    "legacy creator auth stage failed"
                );
                legacy_connect_flow_error("legacy creator connect flow returned no cookie secret")
            })?;
        tracing::info!(
            auth_stage = "credential_export",
            "legacy creator credential exported"
        );
        Ok(LegacyCreatorConnectFlowApproval {
            creator,
            session_secret,
        })
    }
}

async fn retry_homeserver_resolution<T, F, Fut>(
    mut operation: F,
    mut retry_delay: Duration,
) -> pubky::Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = pubky::Result<T>>,
{
    for attempt in 1..=MAX_HOMESERVER_RESOLUTION_ATTEMPTS {
        tracing::info!(
            auth_stage = "homeserver_resolution",
            attempt,
            max_attempts = MAX_HOMESERVER_RESOLUTION_ATTEMPTS,
            "starting legacy creator homeserver resolution"
        );
        match operation().await {
            Ok(result) => return Ok(result),
            Err(error)
                if is_retryable_homeserver_resolution_error(&error)
                    && attempt < MAX_HOMESERVER_RESOLUTION_ATTEMPTS =>
            {
                tracing::warn!(
                    auth_stage = "homeserver_resolution",
                    attempt,
                    max_attempts = MAX_HOMESERVER_RESOLUTION_ATTEMPTS,
                    error_kind = pubky_error_kind(&error),
                    retry_delay_ms = retry_delay.as_millis(),
                    "legacy creator homeserver resolution failed; retrying"
                );
                tokio::time::sleep(retry_delay).await;
                retry_delay = retry_delay.saturating_mul(2);
            }
            Err(error) => {
                log_pubky_stage_failure("homeserver_resolution", &error);
                return Err(error);
            }
        }
    }

    unreachable!("bounded homeserver resolution loop always returns")
}

fn is_retryable_homeserver_resolution_error(error: &pubky::Error) -> bool {
    // Resolution failures happen before `/session` dispatch. Never broaden this to request
    // errors: the Homeserver may have consumed the one-shot token before the response was lost.
    matches!(
        error,
        pubky::Error::Pkarr(pubky::errors::PkarrError::Resolve(_))
    )
}

fn log_pubky_stage_failure(auth_stage: &'static str, error: &pubky::Error) {
    tracing::warn!(
        auth_stage,
        error_kind = pubky_error_kind(error),
        http_status = pubky_error_http_status(error),
        "legacy creator auth stage failed"
    );
}

fn pubky_error_kind(error: &pubky::Error) -> &'static str {
    match error {
        pubky::Error::Request(pubky::errors::RequestError::Transport(_)) => "request_transport",
        pubky::Error::Request(pubky::errors::RequestError::Server { .. }) => "request_server",
        pubky::Error::Request(pubky::errors::RequestError::Validation { .. }) => {
            "request_validation"
        }
        pubky::Error::Request(pubky::errors::RequestError::DecodeJson { .. }) => {
            "request_decode_json"
        }
        pubky::Error::Pkarr(pubky::errors::PkarrError::Dns(_)) => "pkarr_dns",
        pubky::Error::Pkarr(pubky::errors::PkarrError::SignPacket(_)) => "pkarr_sign_packet",
        pubky::Error::Pkarr(pubky::errors::PkarrError::Publish(_)) => "pkarr_publish",
        pubky::Error::Pkarr(pubky::errors::PkarrError::Resolve(_)) => "pkarr_resolution",
        pubky::Error::Pkarr(pubky::errors::PkarrError::InvalidRecord(_)) => "pkarr_invalid_record",
        pubky::Error::Parse(_) => "url_parse",
        pubky::Error::Authentication(pubky::errors::AuthError::CookieSessionRecord(_)) => {
            "auth_cookie_record"
        }
        pubky::Error::Authentication(pubky::errors::AuthError::VerificationFailed(_)) => {
            "auth_token_verification"
        }
        pubky::Error::Authentication(pubky::errors::AuthError::DecryptError(_)) => {
            "auth_decryption"
        }
        pubky::Error::Authentication(pubky::errors::AuthError::Validation(_)) => "auth_validation",
        pubky::Error::Authentication(pubky::errors::AuthError::RequestExpired) => "auth_expired",
        pubky::Error::Build(_) => "client_build",
    }
}

fn pubky_error_http_status(error: &pubky::Error) -> Option<u16> {
    match error {
        pubky::Error::Request(pubky::errors::RequestError::Server { status, .. }) => {
            Some(status.as_u16())
        }
        _ => None,
    }
}

/// Default Lock Server Pubky capabilities requested by the legacy creator connect flow.
pub fn legacy_locks_connect_capabilities() -> Capabilities {
    Capabilities::builder()
        .read_write("/priv/locks.app/")
        .expect("static private Locks capability is canonical")
        .read_write("/pub/locks.app/")
        .expect("static public Locks capability is canonical")
        .finish()
}

/// Converts a Pubky SDK z32 public key into the Locks creator identity wrapper.
pub fn creator_from_pubky_public_key_z32(value: &str) -> Result<CreatorPubky, ApplicationError> {
    let public_key =
        pubky::PublicKey::try_from_z32(value).map_err(|error| ApplicationError::Storage {
            message: format!("invalid approved creator Pubky identity: {error}"),
        })?;
    CreatorPubky::from_str(&public_key.to_string()).map_err(|error| ApplicationError::Storage {
        message: format!("invalid approved creator Pubky identity: {error}"),
    })
}

/// Converts a Locks creator identity wrapper into a Pubky SDK z32 public key.
pub fn creator_z32_from_creator_pubky(creator: &CreatorPubky) -> Result<String, ApplicationError> {
    pubky::PublicKey::try_from(creator.to_string())
        .map(|public_key| public_key.z32())
        .map_err(|_| ApplicationError::Storage {
            message: "invalid creator Pubky identity wrapper".to_owned(),
        })
}

fn requested_scopes_to_capabilities(
    requested_scopes: &[String],
) -> Result<Capabilities, ApplicationError> {
    if requested_scopes.is_empty() {
        return Ok(legacy_locks_connect_capabilities());
    }

    let mut builder = Capabilities::builder();
    for scope in requested_scopes {
        builder = builder
            .read_write(scope.trim_end_matches(":rw"))
            .map_err(|_| legacy_connect_flow_error("invalid legacy creator capability scope"))?;
    }
    Ok(builder.finish())
}

fn legacy_connect_flow_error(message: &'static str) -> ApplicationError {
    ApplicationError::CreatorAuthoritySecret {
        message: message.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;

    use async_trait::async_trait;
    use locks_core::ids::CreatorPubky;
    use url::Url;

    use super::{
        PubkyLegacyCreatorConnectFlowClient, creator_from_pubky_public_key_z32,
        creator_z32_from_creator_pubky, legacy_locks_connect_capabilities,
        retry_homeserver_resolution,
    };
    use crate::application::errors::ApplicationError;
    use crate::application::models::{
        CreatorAuthoritySecret, CreatorConnectAuthorizationUrl, LegacyCreatorConnectFlowApproval,
    };
    use crate::application::ports::LegacyCreatorConnectFlowClient;

    const CREATOR_Z32: &str = "o1gg96ewuojmopcjbz8895478wdtxtzzuxnfjjz8o8e77csa1ngo";
    const CREATOR_PUBKY: &str = "pubkyo1gg96ewuojmopcjbz8895478wdtxtzzuxnfjjz8o8e77csa1ngo";

    #[tokio::test]
    async fn legacy_connect_flow_client_port_is_object_safe_and_secret_free() {
        let client: Box<dyn LegacyCreatorConnectFlowClient> = Box::new(FakeLegacyConnectFlowClient);
        let approval = client
            .await_legacy_creator_connect_flow_approval(&CreatorConnectAuthorizationUrl::new(
                "pubkyauth://secret-flow-token",
            ))
            .await
            .unwrap();

        assert_eq!(
            approval.creator,
            CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy")
                .unwrap()
        );
        assert_eq!(
            approval.session_secret.expose_secret(),
            "legacy-cookie-session-secret"
        );
        assert!(!format!("{approval:?}").contains("legacy-cookie-session-secret"));
        assert!(!format!("{approval:?}").contains("pubkyauth://secret-flow-token"));
    }

    #[test]
    fn legacy_locks_capabilities_are_limited_to_public_and_private_locks_namespaces() {
        let capabilities = legacy_locks_connect_capabilities();
        assert_eq!(
            capabilities.to_string(),
            "/priv/locks.app/:rw,/pub/locks.app/:rw"
        );
    }

    #[test]
    fn creator_from_pubky_public_key_z32_adds_pubky_prefix_for_locks_identity() {
        let creator = creator_from_pubky_public_key_z32(CREATOR_Z32).unwrap();
        assert_eq!(creator.to_string(), CREATOR_PUBKY);
    }

    #[test]
    fn creator_z32_from_creator_pubky_removes_pubky_prefix_for_sdk_identity() {
        let creator = CreatorPubky::from_str(CREATOR_PUBKY).unwrap();

        let z32 = creator_z32_from_creator_pubky(&creator).unwrap();

        assert_eq!(z32, CREATOR_Z32);
    }

    #[test]
    fn creator_from_pubky_public_key_z32_rejects_empty_sdk_identity() {
        let error = creator_from_pubky_public_key_z32("").unwrap_err();

        assert_eq!(
            error,
            ApplicationError::Storage {
                message: "invalid approved creator Pubky identity: Invalid PublicKey length, expected 32 bytes but got: 0".to_owned()
            }
        );
    }

    #[test]
    fn sdk_backed_client_can_be_constructed_from_pubky_handle_without_starting_flow() {
        let _client = PubkyLegacyCreatorConnectFlowClient::new;
    }

    #[tokio::test]
    async fn sdk_backed_client_uses_configured_auth_relay_for_started_flow() {
        let client = PubkyLegacyCreatorConnectFlowClient::new_with_auth_relay(
            pubky::Pubky::testnet().unwrap(),
            "http://localhost:15412/inbox/".parse().unwrap(),
        );

        let authorization_url = client.start_legacy_creator_connect_flow(&[]).await.unwrap();

        let parsed = Url::parse(authorization_url.expose_url()).unwrap();
        assert_eq!(
            parsed
                .query_pairs()
                .find_map(|(name, value)| (name == "relay").then(|| value.into_owned())),
            Some("http://localhost:15412/inbox/".to_owned()),
        );
    }

    #[tokio::test]
    async fn sdk_backed_client_rejects_invalid_requested_scope_before_starting_flow() {
        let client = PubkyLegacyCreatorConnectFlowClient::new(pubky::Pubky::testnet().unwrap());

        let error = client
            .start_legacy_creator_connect_flow(&["relative-scope:rw".to_owned()])
            .await
            .unwrap_err();

        assert_eq!(
            error,
            ApplicationError::CreatorAuthoritySecret {
                message: "invalid legacy creator capability scope".to_owned(),
            }
        );
    }

    #[tokio::test]
    async fn homeserver_resolution_retries_transient_pkarr_failures() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed_attempts = Arc::clone(&attempts);

        retry_homeserver_resolution(
            move || {
                let attempt = observed_attempts.fetch_add(1, Ordering::SeqCst);
                async move {
                    if attempt < 2 {
                        return Err(pubky::Error::Pkarr(pubky::errors::PkarrError::Resolve(
                            pubky::pkarr::errors::ResolveError::NoResponses,
                        )));
                    }
                    Ok(())
                }
            },
            Duration::ZERO,
        )
        .await
        .unwrap();

        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn homeserver_resolution_does_not_retry_session_server_failures() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed_attempts = Arc::clone(&attempts);

        let error = retry_homeserver_resolution(
            move || {
                observed_attempts.fetch_add(1, Ordering::SeqCst);
                async {
                    Err::<(), _>(pubky::Error::Request(pubky::errors::RequestError::Server {
                        status: pubky::StatusCode::SERVICE_UNAVAILABLE,
                        message: "session exchange failed".to_owned(),
                    }))
                }
            },
            Duration::ZERO,
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            pubky::Error::Request(pubky::errors::RequestError::Server {
                status: pubky::StatusCode::SERVICE_UNAVAILABLE,
                ..
            })
        ));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    struct FakeLegacyConnectFlowClient;

    #[async_trait]
    impl LegacyCreatorConnectFlowClient for FakeLegacyConnectFlowClient {
        async fn start_legacy_creator_connect_flow(
            &self,
            _requested_scopes: &[String],
        ) -> Result<CreatorConnectAuthorizationUrl, ApplicationError> {
            Ok(CreatorConnectAuthorizationUrl::new(
                "pubkyauth://secret-flow-token",
            ))
        }

        async fn await_legacy_creator_connect_flow_approval(
            &self,
            _authorization_url: &CreatorConnectAuthorizationUrl,
        ) -> Result<LegacyCreatorConnectFlowApproval, ApplicationError> {
            Ok(LegacyCreatorConnectFlowApproval {
                creator: CreatorPubky::from_str(
                    "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
                )
                .unwrap(),
                session_secret: CreatorAuthoritySecret::new("legacy-cookie-session-secret"),
            })
        }
    }
}
