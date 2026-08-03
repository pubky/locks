use locks_core::ids::CreatorPubky;

use crate::application::errors::ApplicationError;
use crate::application::ports::{CreatorAuthorityManager, CreatorAuthorityStatus};

/// Ensures the Lock Server has creator-granted authority before doing Pubky homeserver IO.
///
/// This seam is intentionally tiny so future Pubky-backed content/priv repositories can
/// require creator authority without depending on legacy cookie auth, grant auth, or any
/// concrete infrastructure adapter.
pub async fn require_creator_authority_for_pubky_io(
    manager: &dyn CreatorAuthorityManager,
    creator: &CreatorPubky,
) -> Result<CreatorAuthorityStatus, ApplicationError> {
    manager.require_creator_authority(creator).await
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use async_trait::async_trait;
    use locks_core::ids::CreatorPubky;

    use crate::application::errors::ApplicationError;
    use crate::application::models::CreatorAuthorityAuthKind;
    use crate::application::ports::{CreatorAuthorityManager, CreatorAuthorityStatus};
    use crate::application::use_cases::require_creator_authority_for_pubky_io::require_creator_authority_for_pubky_io;

    #[tokio::test]
    async fn missing_creator_authority_returns_creator_authority_unavailable() {
        let manager =
            FakeCreatorAuthorityManager::new(Err(ApplicationError::CreatorAuthorityUnavailable));

        let error = require_creator_authority_for_pubky_io(&manager, &creator())
            .await
            .unwrap_err();

        assert_eq!(error, ApplicationError::CreatorAuthorityUnavailable);
    }

    #[tokio::test]
    async fn valid_creator_authority_returns_secret_free_status() {
        let status = CreatorAuthorityStatus {
            creator: creator(),
            auth_kind: CreatorAuthorityAuthKind::LegacyCookie,
            authorized: true,
            granted_scopes: vec!["/pub/locks.app/:rw".to_owned()],
            session_expires_at: None,
        };
        let manager = FakeCreatorAuthorityManager::new(Ok(status.clone()));

        let actual = require_creator_authority_for_pubky_io(&manager, &creator())
            .await
            .unwrap();

        assert_eq!(actual, status);
        assert!(!format!("{actual:?}").contains("secret"));
    }

    #[derive(Debug)]
    struct FakeCreatorAuthorityManager {
        result: Result<CreatorAuthorityStatus, ApplicationError>,
    }

    impl FakeCreatorAuthorityManager {
        fn new(result: Result<CreatorAuthorityStatus, ApplicationError>) -> Self {
            Self { result }
        }
    }

    #[async_trait]
    impl CreatorAuthorityManager for FakeCreatorAuthorityManager {
        async fn revalidate_creator_authority(
            &self,
            _creator: &CreatorPubky,
        ) -> Result<CreatorAuthorityStatus, ApplicationError> {
            self.result.clone()
        }

        async fn require_creator_authority(
            &self,
            _creator: &CreatorPubky,
        ) -> Result<CreatorAuthorityStatus, ApplicationError> {
            self.result.clone()
        }
    }

    fn creator() -> CreatorPubky {
        CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
    }
}
