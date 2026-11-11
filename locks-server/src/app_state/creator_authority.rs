use async_trait::async_trait;
use locks_core::ids::CreatorPubky;
use locks_service::{
    application::{errors::ApplicationError, models::CreatorAuthoritySecret},
    infrastructure::pubky::GrantCredentialRevalidator,
};

/// Accepts any stored grant record without contacting a homeserver. In-memory and test runtimes
/// only; `GrantCreatorAuthorityManager` still refuses non-grant records before calling it.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct NoopGrantCredentialRevalidator;

#[async_trait]
impl GrantCredentialRevalidator for NoopGrantCredentialRevalidator {
    async fn revalidate_grant_credential(
        &self,
        _creator: &CreatorPubky,
        _secret: &CreatorAuthoritySecret,
    ) -> Result<(), ApplicationError> {
        Ok(())
    }
}

/// Used when hosted acquisition is disabled and no grant-connect table is configured: there is
/// no grant policy to restore a session with, so every creator authority check fails closed.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct UnavailableGrantCredentialRevalidator;

#[async_trait]
impl GrantCredentialRevalidator for UnavailableGrantCredentialRevalidator {
    async fn revalidate_grant_credential(
        &self,
        _creator: &CreatorPubky,
        _secret: &CreatorAuthoritySecret,
    ) -> Result<(), ApplicationError> {
        Err(ApplicationError::CreatorAuthorityUnavailable)
    }
}
