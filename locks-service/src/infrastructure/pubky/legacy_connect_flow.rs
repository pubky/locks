use std::str::FromStr;

use async_trait::async_trait;
use locks_core::ids::CreatorPubky;
use pubky::{AuthFlowKind, Capabilities, PubkyAuthFlow};
use url::Url;

use crate::application::errors::ApplicationError;
use crate::application::models::{
    CreatorAuthoritySecret, CreatorConnectAuthorizationUrl, LegacyCreatorConnectFlowApproval,
};
use crate::application::ports::LegacyCreatorConnectFlowClient;

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
impl LegacyCreatorConnectFlowClient for PubkyLegacyCreatorConnectFlowClient {
    async fn start_legacy_creator_connect_flow(
        &self,
        requested_scopes: &[String],
    ) -> Result<CreatorConnectAuthorizationUrl, ApplicationError> {
        let capabilities = requested_scopes_to_capabilities(requested_scopes);
        let flow = match &self.auth_relay {
            Some(auth_relay) => PubkyAuthFlow::builder(&capabilities, AuthFlowKind::signin())
                .relay(auth_relay.clone())
                .start(),
            None => self
                .pubky
                .start_auth_flow(&capabilities, AuthFlowKind::signin()),
        }
        .map_err(|_| legacy_connect_flow_error("failed to start legacy creator connect flow"))?;
        Ok(CreatorConnectAuthorizationUrl::new(
            flow.authorization_url().to_string(),
        ))
    }

    async fn await_legacy_creator_connect_flow_approval(
        &self,
        authorization_url: &CreatorConnectAuthorizationUrl,
    ) -> Result<LegacyCreatorConnectFlowApproval, ApplicationError> {
        let flow = self
            .pubky
            .resume_auth_flow(authorization_url.expose_url())
            .map_err(|_| {
                legacy_connect_flow_error("failed to resume legacy creator connect flow")
            })?;
        let session = flow.await_approval().await.map_err(|_| {
            legacy_connect_flow_error("legacy creator connect flow approval failed or expired")
        })?;
        let creator = creator_from_pubky_public_key_z32(&session.info().public_key().z32())?;
        let session_secret = CreatorAuthoritySecret::new(session.export_secret());
        Ok(LegacyCreatorConnectFlowApproval {
            creator,
            session_secret,
        })
    }
}

/// Default Lock Server Pubky capabilities requested by the legacy creator connect flow.
pub fn legacy_locks_connect_capabilities() -> Capabilities {
    Capabilities::builder()
        .read_write("/priv/locks.app/")
        .read_write("/pub/locks.app/")
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

fn requested_scopes_to_capabilities(requested_scopes: &[String]) -> Capabilities {
    if requested_scopes.is_empty() {
        return legacy_locks_connect_capabilities();
    }

    let mut builder = Capabilities::builder();
    for scope in requested_scopes {
        builder = builder.read_write(scope.trim_end_matches(":rw"));
    }
    builder.finish()
}

fn legacy_connect_flow_error(message: &'static str) -> ApplicationError {
    ApplicationError::CreatorAuthoritySecret {
        message: message.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use async_trait::async_trait;
    use locks_core::ids::CreatorPubky;

    use super::{
        PubkyLegacyCreatorConnectFlowClient, creator_from_pubky_public_key_z32,
        creator_z32_from_creator_pubky, legacy_locks_connect_capabilities,
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

        assert!(
            authorization_url
                .expose_url()
                .contains("relay=http://localhost:15412/inbox/"),
            "unexpected authorization URL: {}",
            authorization_url.expose_url()
        );
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
