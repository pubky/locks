use async_trait::async_trait;

use locks_core::ids::CreatorPubky;

use crate::application::errors::ApplicationError;
use crate::application::models::{
    CreatorAuthorityAuthKind, CreatorAuthorityRecord, CreatorAuthoritySecret,
};
use crate::application::ports::{
    CreatorAuthorityManager, CreatorAuthorityStatus, CreatorAuthorityStore,
};
use crate::infrastructure::pubky::grant_connect_flow::{
    LockServerGrantPopKeys, restore_grant_session_for_creator,
};
use crate::infrastructure::pubky::grant_session_cache::GrantSessionCache;
use crate::infrastructure::pubky::storage_client::PubkySessionImporter;

#[async_trait]
pub trait GrantCredentialRevalidator: Send + Sync {
    async fn revalidate_grant_credential(
        &self,
        creator: &CreatorPubky,
        secret: &CreatorAuthoritySecret,
    ) -> Result<(), ApplicationError>;
}

#[derive(Debug, Clone)]
pub struct PubkyGrantCredentialRevalidator {
    client: pubky::PubkyHttpClient,
    pop_keys: LockServerGrantPopKeys,
    client_id: pubky::ClientId,
    required_scopes: Vec<String>,
}

impl PubkyGrantCredentialRevalidator {
    pub fn new(
        client: pubky::PubkyHttpClient,
        pop_keys: LockServerGrantPopKeys,
        client_id: pubky::ClientId,
        required_scopes: Vec<String>,
    ) -> Self {
        Self {
            client,
            pop_keys,
            client_id,
            required_scopes,
        }
    }
}

#[async_trait]
impl GrantCredentialRevalidator for PubkyGrantCredentialRevalidator {
    async fn revalidate_grant_credential(
        &self,
        creator: &CreatorPubky,
        secret: &CreatorAuthoritySecret,
    ) -> Result<(), ApplicationError> {
        restore_grant_session_for_creator(
            &self.client,
            &self.pop_keys,
            &self.client_id,
            &self.required_scopes,
            creator,
            secret,
        )
        .await
        .map(|_| ())
    }
}

#[async_trait]
impl<S, I> GrantCredentialRevalidator for std::sync::Arc<GrantSessionCache<S, I>>
where
    S: CreatorAuthorityStore,
    I: PubkySessionImporter,
    I::Session: Clone,
{
    async fn revalidate_grant_credential(
        &self,
        creator: &CreatorPubky,
        secret: &CreatorAuthoritySecret,
    ) -> Result<(), ApplicationError> {
        let cached = self.session_for_creator(creator).await?;
        if cached.record.secret.expose_secret() != secret.expose_secret() {
            tracing::warn!(
                creator = %creator,
                stage = "manager_record_validate",
                error_class = "stale_manager_record",
                "creator grant session restore failed"
            );
            return Err(ApplicationError::CreatorAuthorityUnavailable);
        }
        Ok(())
    }
}

/// Grant-only authority manager. Records written by the removed legacy-connect mode fail closed.
#[derive(Debug)]
pub struct GrantCreatorAuthorityManager<S, G> {
    store: S,
    revalidator: G,
}

impl<S, G> GrantCreatorAuthorityManager<S, G> {
    pub fn new(store: S, revalidator: G) -> Self {
        Self { store, revalidator }
    }
}

#[async_trait]
impl<S, G> CreatorAuthorityManager for GrantCreatorAuthorityManager<S, G>
where
    S: CreatorAuthorityStore,
    G: GrantCredentialRevalidator,
{
    async fn revalidate_creator_authority(
        &self,
        creator: &CreatorPubky,
    ) -> Result<CreatorAuthorityStatus, ApplicationError> {
        let record = self
            .store
            .get_creator_authority(creator)
            .await?
            .ok_or(ApplicationError::CreatorAuthorityUnavailable)?;
        if record.auth_kind != CreatorAuthorityAuthKind::Grant {
            return Err(ApplicationError::CreatorAuthorityUnavailable);
        }
        self.revalidator
            .revalidate_grant_credential(creator, &record.secret)
            .await?;
        Ok(record_to_authorized_status(record))
    }

    async fn require_creator_authority(
        &self,
        creator: &CreatorPubky,
    ) -> Result<CreatorAuthorityStatus, ApplicationError> {
        self.revalidate_creator_authority(creator).await
    }
}

fn record_to_authorized_status(record: CreatorAuthorityRecord) -> CreatorAuthorityStatus {
    CreatorAuthorityStatus {
        creator: record.creator,
        auth_kind: record.auth_kind,
        authorized: true,
        granted_scopes: record.granted_scopes,
        session_expires_at: record.session_expires_at,
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use locks_core::ids::CreatorPubky;
    use time::macros::datetime;

    use super::{
        GrantCreatorAuthorityManager, GrantCredentialRevalidator, PubkyGrantCredentialRevalidator,
    };
    use crate::application::errors::ApplicationError;
    use crate::application::models::{
        CreatorAuthorityAuthKind, CreatorAuthorityRecord, CreatorAuthoritySecret,
    };
    use crate::application::ports::{
        CreatorAuthorityManager, CreatorAuthorityStatus, CreatorAuthorityStore,
    };

    #[tokio::test]
    async fn missing_authority_returns_unavailable_without_revalidating() {
        let manager = GrantCreatorAuthorityManager::new(
            FakeCreatorAuthorityStore::new(None),
            FakeGrantCredentialRevalidator::new(Ok(())),
        );

        assert_eq!(
            manager.require_creator_authority(&creator()).await,
            Err(ApplicationError::CreatorAuthorityUnavailable)
        );
        assert_eq!(manager.revalidator.seen_secrets(), Vec::<String>::new());
    }

    #[tokio::test]
    async fn stored_legacy_cookie_record_is_refused_without_revalidating() {
        let manager = GrantCreatorAuthorityManager::new(
            FakeCreatorAuthorityStore::new(Some(CreatorAuthorityRecord {
                auth_kind: CreatorAuthorityAuthKind::LegacyCookie,
                ..creator_authority_record("legacy-cookie-session-secret")
            })),
            FakeGrantCredentialRevalidator::new(Ok(())),
        );

        assert_eq!(
            manager.require_creator_authority(&creator()).await,
            Err(ApplicationError::CreatorAuthorityUnavailable)
        );
        assert_eq!(
            manager.revalidate_creator_authority(&creator()).await,
            Err(ApplicationError::CreatorAuthorityUnavailable)
        );
        assert_eq!(manager.revalidator.seen_secrets(), Vec::<String>::new());
    }

    #[tokio::test]
    async fn valid_grant_record_revalidates_secret_and_returns_secret_free_status() {
        let manager = GrantCreatorAuthorityManager::new(
            FakeCreatorAuthorityStore::new(Some(creator_authority_record("delegated-grant-state"))),
            FakeGrantCredentialRevalidator::new(Ok(())),
        );

        let status = manager.require_creator_authority(&creator()).await.unwrap();

        assert_eq!(
            status,
            CreatorAuthorityStatus {
                creator: creator(),
                auth_kind: CreatorAuthorityAuthKind::Grant,
                authorized: true,
                granted_scopes: vec![
                    "/pub/app.locks/:rw".to_owned(),
                    "/priv/app.locks/:rw".to_owned(),
                ],
                session_expires_at: Some(datetime!(2026-05-29 12:15:00 UTC)),
            }
        );
        assert_eq!(
            manager.revalidator.seen_secrets(),
            vec!["delegated-grant-state".to_owned()]
        );
        assert!(!format!("{status:?}").contains("delegated-grant-state"));
    }

    #[tokio::test]
    async fn failed_grant_revalidation_returns_unavailable_without_leaking_secret() {
        let manager = GrantCreatorAuthorityManager::new(
            FakeCreatorAuthorityStore::new(Some(creator_authority_record("delegated-grant-state"))),
            FakeGrantCredentialRevalidator::new(Err(ApplicationError::CreatorAuthorityUnavailable)),
        );

        let error = manager
            .require_creator_authority(&creator())
            .await
            .unwrap_err();

        assert_eq!(error, ApplicationError::CreatorAuthorityUnavailable);
        assert!(!format!("{error:?}").contains("delegated-grant-state"));
    }

    #[tokio::test]
    async fn grant_manager_maps_malformed_stored_grant_to_unavailable() {
        let grant_record = creator_authority_record("malformed-delegated-grant-state");
        let revalidator = PubkyGrantCredentialRevalidator::new(
            pubky::PubkyHttpClient::testnet().unwrap(),
            crate::infrastructure::pubky::LockServerGrantPopKeys::from_lock_server_seed([42; 32]),
            pubky::ClientId::new("locks.example").unwrap(),
            vec![
                "/pub/app.locks/:rw".to_owned(),
                "/priv/app.locks/:rw".to_owned(),
            ],
        );
        let manager = GrantCreatorAuthorityManager::new(
            FakeCreatorAuthorityStore::new(Some(grant_record)),
            revalidator,
        );

        assert_eq!(
            manager.revalidate_creator_authority(&creator()).await,
            Err(ApplicationError::CreatorAuthorityUnavailable)
        );
    }

    #[derive(Debug)]
    struct FakeCreatorAuthorityStore {
        record: Option<CreatorAuthorityRecord>,
    }

    impl FakeCreatorAuthorityStore {
        fn new(record: Option<CreatorAuthorityRecord>) -> Self {
            Self { record }
        }
    }

    #[async_trait]
    impl CreatorAuthorityStore for FakeCreatorAuthorityStore {
        async fn upsert_creator_authority(
            &self,
            _authority: CreatorAuthorityRecord,
        ) -> Result<(), ApplicationError> {
            unimplemented!("not needed by manager tests")
        }

        async fn get_creator_authority(
            &self,
            creator: &CreatorPubky,
        ) -> Result<Option<CreatorAuthorityRecord>, ApplicationError> {
            Ok(self
                .record
                .as_ref()
                .filter(|record| &record.creator == creator)
                .cloned())
        }

        async fn delete_creator_authority(
            &self,
            _creator: &CreatorPubky,
        ) -> Result<(), ApplicationError> {
            unimplemented!("not needed by manager tests")
        }
    }

    #[derive(Debug)]
    struct FakeGrantCredentialRevalidator {
        result: Result<(), ApplicationError>,
        seen_secrets: Mutex<Vec<String>>,
    }

    impl FakeGrantCredentialRevalidator {
        fn new(result: Result<(), ApplicationError>) -> Self {
            Self {
                result,
                seen_secrets: Mutex::new(Vec::new()),
            }
        }

        fn seen_secrets(&self) -> Vec<String> {
            self.seen_secrets.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl GrantCredentialRevalidator for FakeGrantCredentialRevalidator {
        async fn revalidate_grant_credential(
            &self,
            _creator: &CreatorPubky,
            secret: &CreatorAuthoritySecret,
        ) -> Result<(), ApplicationError> {
            self.seen_secrets
                .lock()
                .unwrap()
                .push(secret.expose_secret().to_owned());
            self.result.clone()
        }
    }

    fn creator_authority_record(secret: &str) -> CreatorAuthorityRecord {
        CreatorAuthorityRecord {
            creator: creator(),
            auth_kind: CreatorAuthorityAuthKind::Grant,
            granted_scopes: vec![
                "/pub/app.locks/:rw".to_owned(),
                "/priv/app.locks/:rw".to_owned(),
            ],
            secret: CreatorAuthoritySecret::new(secret),
            session_expires_at: Some(datetime!(2026-05-29 12:15:00 UTC)),
            last_revalidated_at: Some(datetime!(2026-05-29 12:00:00 UTC)),
        }
    }

    fn creator() -> CreatorPubky {
        CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
    }
}
