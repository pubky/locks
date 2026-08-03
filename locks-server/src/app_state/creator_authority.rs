use async_trait::async_trait;
use locks_service::{
    application::{
        errors::ApplicationError,
        models::{
            CreatorAuthoritySecret, CreatorConnectAuthorizationUrl,
            LegacyCreatorConnectFlowApproval,
        },
        ports::LegacyCreatorConnectFlowClient,
    },
    infrastructure::pubky::LegacyCookieSessionRevalidator,
};

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct NoopLegacyCookieSessionRevalidator;

#[async_trait]
impl LegacyCookieSessionRevalidator for NoopLegacyCookieSessionRevalidator {
    async fn revalidate_legacy_cookie_secret(
        &self,
        _secret: &CreatorAuthoritySecret,
    ) -> Result<(), ApplicationError> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct DisabledLegacyCreatorConnectFlowClient;

#[async_trait]
impl LegacyCreatorConnectFlowClient for DisabledLegacyCreatorConnectFlowClient {
    async fn start_legacy_creator_connect_flow(
        &self,
        _requested_scopes: &[String],
    ) -> Result<CreatorConnectAuthorizationUrl, ApplicationError> {
        Err(ApplicationError::CreatorAuthorityUnavailable)
    }

    async fn await_legacy_creator_connect_flow_approval(
        &self,
        _authorization_url: &CreatorConnectAuthorizationUrl,
    ) -> Result<LegacyCreatorConnectFlowApproval, ApplicationError> {
        Err(ApplicationError::CreatorAuthorityUnavailable)
    }
}
