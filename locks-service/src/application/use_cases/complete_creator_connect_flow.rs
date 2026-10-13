use time::{Duration, OffsetDateTime};

use crate::application::errors::ApplicationError;
use crate::application::models::{
    CreatorAuthorityAuthKind, CreatorAuthorityRecord, CreatorConnectFlowId, FrontendSessionCode,
    FrontendSessionCodeRecord, GrantPopKeyId,
};
use crate::application::ports::{
    Clock, CreatorAuthorityStore, CreatorConnectFlowStore, FrontendSessionCodeGenerator,
    FrontendSessionCodeStore, GrantCreatorConnectFlowClient, LegacyCreatorConnectFlowClient,
};

const FRONTEND_SESSION_CODE_TTL: Duration = Duration::minutes(5);

/// Request to complete a pending legacy creator connect flow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompleteCreatorConnectFlowRequest {
    /// Pending flow ID returned from start flow.
    pub flow_id: CreatorConnectFlowId,
}

/// Response containing a one-time frontend session code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompleteCreatorConnectFlowResponse {
    /// Creator approved by Pubky signer.
    pub creator: locks_core::ids::CreatorPubky,
    /// Opaque state from the original pending flow.
    pub state: String,
    /// Return target from the original pending flow.
    pub return_to: String,
    /// One-time code to exchange for a frontend session.
    pub code: FrontendSessionCode,
    /// Expiration timestamp for the one-time code.
    pub code_expires_at: OffsetDateTime,
}

pub enum SelectedCreatorConnectFlowClient<'a> {
    Legacy(&'a dyn LegacyCreatorConnectFlowClient),
    Grant(&'a dyn GrantCreatorConnectFlowClient),
}

/// Completes the selected Pubky creator connect flow and issues a frontend session code.
pub async fn complete_creator_connect_flow(
    flow_store: &dyn CreatorConnectFlowStore,
    authority_store: &dyn CreatorAuthorityStore,
    code_store: &dyn FrontendSessionCodeStore,
    client: SelectedCreatorConnectFlowClient<'_>,
    code_generator: &dyn FrontendSessionCodeGenerator,
    clock: &dyn Clock,
    request: CompleteCreatorConnectFlowRequest,
) -> Result<CompleteCreatorConnectFlowResponse, ApplicationError> {
    let pending = flow_store
        .get_pending_creator_connect_flow(&request.flow_id)
        .await?
        .ok_or(ApplicationError::CreatorConnectFlowUnavailable)?;

    let now = clock.now();
    if pending.is_expired_at(now) {
        flow_store
            .delete_pending_creator_connect_flow(&request.flow_id)
            .await?;
        return Err(ApplicationError::CreatorConnectFlowExpired);
    }

    let selected_auth_kind = match client {
        SelectedCreatorConnectFlowClient::Legacy(_) => CreatorAuthorityAuthKind::LegacyCookie,
        SelectedCreatorConnectFlowClient::Grant(_) => CreatorAuthorityAuthKind::Grant,
    };
    if pending.authorization_url.auth_kind()? != selected_auth_kind {
        return Err(ApplicationError::CreatorAuthorityUnavailable);
    }

    let (creator, authority) = match client {
        SelectedCreatorConnectFlowClient::Grant(grant_client) => {
            let approval = grant_client
                .await_grant_creator_connect_flow_approval(
                    &pending.authorization_url,
                    &GrantPopKeyId::for_connect_flow(&pending.flow_id),
                    &pending.requested_scopes,
                )
                .await?;
            let creator = approval.creator.clone();
            let authority = CreatorAuthorityRecord {
                creator: creator.clone(),
                auth_kind: CreatorAuthorityAuthKind::Grant,
                granted_scopes: approval.granted_scopes,
                secret: approval.grant_state,
                session_expires_at: Some(approval.grant_expires_at),
                last_revalidated_at: Some(now),
            };
            (creator, authority)
        }
        SelectedCreatorConnectFlowClient::Legacy(client) => {
            let approval = client
                .await_legacy_creator_connect_flow_approval(&pending.authorization_url)
                .await?;
            let creator = approval.creator.clone();
            let authority = CreatorAuthorityRecord {
                creator: creator.clone(),
                auth_kind: CreatorAuthorityAuthKind::LegacyCookie,
                granted_scopes: pending.requested_scopes.clone(),
                secret: approval.session_secret,
                session_expires_at: None,
                last_revalidated_at: Some(now),
            };
            (creator, authority)
        }
    };
    let completed_at = clock.now();
    if pending.is_expired_at(completed_at)
        || authority
            .session_expires_at
            .is_some_and(|expires_at| expires_at <= completed_at)
    {
        return Err(ApplicationError::CreatorConnectFlowExpired);
    }
    let consumed = flow_store
        .consume_pending_creator_connect_flow(&request.flow_id, completed_at)
        .await?
        .filter(|consumed| consumed == &pending)
        .ok_or(ApplicationError::CreatorConnectFlowUnavailable)?;
    let committed_at = clock.now();
    if consumed.is_expired_at(committed_at)
        || authority
            .session_expires_at
            .is_some_and(|expires_at| expires_at <= committed_at)
    {
        return Err(ApplicationError::CreatorConnectFlowExpired);
    }
    let authority = CreatorAuthorityRecord {
        last_revalidated_at: Some(committed_at),
        ..authority
    };
    authority_store.upsert_creator_authority(authority).await?;

    let code = code_generator.generate_frontend_session_code();
    let code_expires_at = committed_at + FRONTEND_SESSION_CODE_TTL;
    code_store
        .insert_frontend_session_code(FrontendSessionCodeRecord {
            code: code.clone(),
            creator: creator.clone(),
            state: consumed.state.clone(),
            return_to: consumed.return_to.clone(),
            created_at: committed_at,
            expires_at: code_expires_at,
            consumed_at: None,
        })
        .await?;

    Ok(CompleteCreatorConnectFlowResponse {
        creator,
        state: consumed.state,
        return_to: consumed.return_to,
        code,
        code_expires_at,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::str::FromStr;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use locks_core::ids::CreatorPubky;
    use time::{Duration, OffsetDateTime};
    use tokio::sync::Semaphore;

    use crate::application::errors::ApplicationError;
    use crate::application::models::{
        CreatorAuthorityAuthKind, CreatorAuthorityRecord, CreatorAuthoritySecret,
        CreatorConnectAuthorizationUrl, CreatorConnectFlowId, FrontendSessionCode,
        FrontendSessionCodeRecord, GrantCreatorConnectFlowApproval, GrantPopKeyId,
        LegacyCreatorConnectFlowApproval, PendingCreatorConnectFlowRecord,
    };
    use crate::application::ports::{
        Clock, CreatorAuthorityStore, CreatorConnectFlowStore, FrontendSessionCodeGenerator,
        FrontendSessionCodeStore, GrantCreatorConnectFlowClient, LegacyCreatorConnectFlowClient,
    };
    use crate::application::use_cases::complete_creator_connect_flow::{
        CompleteCreatorConnectFlowRequest, SelectedCreatorConnectFlowClient,
        complete_creator_connect_flow,
    };

    #[tokio::test]
    async fn configured_grant_connect_stores_only_validated_grant_authority() {
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        let flow_store = FlowStore::with_record(grant_pending_flow(now));
        let authority_store = AuthorityStore::default();

        complete_creator_connect_flow(
            &flow_store,
            &authority_store,
            &CodeStore::default(),
            SelectedCreatorConnectFlowClient::Grant(&ApprovedGrantConnectFlowClient(
                now + Duration::days(30),
            )),
            &FixedCodeGenerator,
            &FixedClock(now),
            CompleteCreatorConnectFlowRequest {
                flow_id: CreatorConnectFlowId::new("flow-123"),
            },
        )
        .await
        .unwrap();

        let authority = authority_store.record().unwrap();
        assert_eq!(authority.auth_kind, CreatorAuthorityAuthKind::Grant);
        assert_eq!(authority.secret.expose_secret(), "delegated-grant-state");
        assert_eq!(authority.session_expires_at, Some(now + Duration::days(30)));
    }

    #[tokio::test]
    async fn complete_creator_connect_flow_stores_authority_issues_code_and_deletes_pending_flow() {
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        let flow_store = FlowStore::with_record(pending_flow(now));
        let authority_store = AuthorityStore::default();
        let code_store = CodeStore::default();
        let client = FakeConnectFlowClient;
        let code_generator = FixedCodeGenerator;
        let clock = FixedClock(now);

        let response = complete_creator_connect_flow(
            &flow_store,
            &authority_store,
            &code_store,
            SelectedCreatorConnectFlowClient::Legacy(&client),
            &code_generator,
            &clock,
            CompleteCreatorConnectFlowRequest {
                flow_id: CreatorConnectFlowId::new("flow-123"),
            },
        )
        .await
        .unwrap();

        assert_eq!(response.creator, creator());
        assert_eq!(response.state, "opaque-state");
        assert_eq!(response.return_to, "https://pubky.app/locks/connected");
        assert_eq!(response.code.expose_code(), "one-time-code");
        assert!(response.code_expires_at <= now + Duration::minutes(5));
        assert!(!format!("{response:?}").contains("legacy-cookie-session-secret"));

        assert!(
            flow_store.record().is_none(),
            "pending flow deleted after completion"
        );

        let authority = authority_store.record().expect("creator authority stored");
        assert_eq!(authority.creator, creator());
        assert_eq!(authority.auth_kind, CreatorAuthorityAuthKind::LegacyCookie);
        assert_eq!(
            authority.secret.expose_secret(),
            "legacy-cookie-session-secret"
        );
        assert_eq!(
            authority.granted_scopes,
            vec!["/pub/app.locks/:rw", "/priv/app.locks/:rw"]
        );

        let code = code_store.record().expect("frontend session code stored");
        assert_eq!(code.code.expose_code(), "one-time-code");
        assert_eq!(code.creator, creator());
        assert_eq!(code.state, "opaque-state");
        assert_eq!(code.return_to, "https://pubky.app/locks/connected");
        assert_eq!(code.created_at, now);
        assert_eq!(code.expires_at, response.code_expires_at);
        assert_eq!(code.consumed_at, None);
    }

    #[tokio::test]
    async fn complete_creator_connect_flow_maps_missing_and_expired_flows_to_lifecycle_errors() {
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        let missing = complete_creator_connect_flow(
            &FlowStore::default(),
            &AuthorityStore::default(),
            &CodeStore::default(),
            SelectedCreatorConnectFlowClient::Legacy(&FakeConnectFlowClient),
            &FixedCodeGenerator,
            &FixedClock(now),
            CompleteCreatorConnectFlowRequest {
                flow_id: CreatorConnectFlowId::new("missing"),
            },
        )
        .await
        .unwrap_err();
        assert_eq!(missing, ApplicationError::CreatorConnectFlowUnavailable);

        let expired_flow = PendingCreatorConnectFlowRecord {
            expires_at: now,
            ..pending_flow(now - Duration::minutes(10))
        };
        let expired_store = FlowStore::with_record(expired_flow);
        let expired = complete_creator_connect_flow(
            &expired_store,
            &AuthorityStore::default(),
            &CodeStore::default(),
            SelectedCreatorConnectFlowClient::Legacy(&FakeConnectFlowClient),
            &FixedCodeGenerator,
            &FixedClock(now),
            CompleteCreatorConnectFlowRequest {
                flow_id: CreatorConnectFlowId::new("flow-123"),
            },
        )
        .await
        .unwrap_err();
        assert_eq!(expired, ApplicationError::CreatorConnectFlowExpired);
        assert!(expired_store.record().is_none(), "expired flow cleaned up");
    }

    #[tokio::test]
    async fn concurrent_completion_issues_exactly_one_code_after_approval() {
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        let flow_store = Arc::new(FlowStore::with_record(grant_pending_flow(now)));
        let authority_store = Arc::new(AuthorityStore::default());
        let code_store = Arc::new(CodeStore::default());
        let client = Arc::new(BlockingGrantConnectFlowClient::new(now));

        let first = {
            let flow_store = Arc::clone(&flow_store);
            let authority_store = Arc::clone(&authority_store);
            let code_store = Arc::clone(&code_store);
            let client = Arc::clone(&client);
            tokio::spawn(async move {
                complete_creator_connect_flow(
                    flow_store.as_ref(),
                    authority_store.as_ref(),
                    code_store.as_ref(),
                    SelectedCreatorConnectFlowClient::Grant(client.as_ref()),
                    &FixedCodeGenerator,
                    &FixedClock(now),
                    CompleteCreatorConnectFlowRequest {
                        flow_id: CreatorConnectFlowId::new("flow-123"),
                    },
                )
                .await
            })
        };

        client
            .entered
            .acquire()
            .await
            .expect("approval-entry semaphore remains open")
            .forget();

        let second = {
            let flow_store = Arc::clone(&flow_store);
            let authority_store = Arc::clone(&authority_store);
            let code_store = Arc::clone(&code_store);
            let client = Arc::clone(&client);
            tokio::spawn(async move {
                complete_creator_connect_flow(
                    flow_store.as_ref(),
                    authority_store.as_ref(),
                    code_store.as_ref(),
                    SelectedCreatorConnectFlowClient::Grant(client.as_ref()),
                    &FixedCodeGenerator,
                    &FixedClock(now),
                    CompleteCreatorConnectFlowRequest {
                        flow_id: CreatorConnectFlowId::new("flow-123"),
                    },
                )
                .await
            })
        };
        client
            .entered
            .acquire()
            .await
            .expect("approval-entry semaphore remains open")
            .forget();

        client.release.add_permits(2);
        let first = first.await.expect("first completion task joins");
        let second = second.await.expect("second completion task joins");

        assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
        assert!([first, second].contains(&Err(ApplicationError::CreatorConnectFlowUnavailable)));
    }

    #[tokio::test]
    async fn failed_approval_keeps_pending_flow_retryable() {
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        let flow_store = FlowStore::with_record(grant_pending_flow(now));

        let error = complete_creator_connect_flow(
            &flow_store,
            &AuthorityStore::default(),
            &CodeStore::default(),
            SelectedCreatorConnectFlowClient::Grant(&RejectedGrantConnectFlowClient),
            &FixedCodeGenerator,
            &FixedClock(now),
            CompleteCreatorConnectFlowRequest {
                flow_id: CreatorConnectFlowId::new("flow-123"),
            },
        )
        .await
        .unwrap_err();

        assert_eq!(error, ApplicationError::CreatorAuthorityUnavailable);
        assert_eq!(flow_store.record(), Some(grant_pending_flow(now)));
    }

    #[tokio::test]
    async fn pending_grant_flow_cannot_be_completed_by_legacy_client_after_config_change() {
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        let flow_store = FlowStore::with_record(PendingCreatorConnectFlowRecord {
            authorization_url: CreatorConnectAuthorizationUrl::new(
                "pubkyauth://signin_grant?secret=grant",
            ),
            ..pending_flow(now)
        });

        let error = complete_creator_connect_flow(
            &flow_store,
            &AuthorityStore::default(),
            &CodeStore::default(),
            SelectedCreatorConnectFlowClient::Legacy(&FakeConnectFlowClient),
            &FixedCodeGenerator,
            &FixedClock(now),
            CompleteCreatorConnectFlowRequest {
                flow_id: CreatorConnectFlowId::new("flow-123"),
            },
        )
        .await
        .unwrap_err();

        assert_eq!(error, ApplicationError::CreatorAuthorityUnavailable);
        assert!(flow_store.record().is_some());
    }

    #[tokio::test]
    async fn grant_expiring_at_commit_boundary_issues_no_authority_or_code() {
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        let flow_store = FlowStore::with_record(grant_pending_flow(now));
        let authority_store = AuthorityStore::default();
        let code_store = CodeStore::default();
        let clock = AdvancingClock(Mutex::new(VecDeque::from([
            now,
            now,
            now + Duration::seconds(2),
        ])));

        let error = complete_creator_connect_flow(
            &flow_store,
            &authority_store,
            &code_store,
            SelectedCreatorConnectFlowClient::Grant(&ApprovedGrantConnectFlowClient(
                now + Duration::seconds(1),
            )),
            &FixedCodeGenerator,
            &clock,
            CompleteCreatorConnectFlowRequest {
                flow_id: CreatorConnectFlowId::new("flow-123"),
            },
        )
        .await
        .unwrap_err();

        assert_eq!(error, ApplicationError::CreatorConnectFlowExpired);
        assert!(authority_store.record().is_none());
        assert!(code_store.record().is_none());
    }

    fn pending_flow(now: OffsetDateTime) -> PendingCreatorConnectFlowRecord {
        PendingCreatorConnectFlowRecord {
            flow_id: CreatorConnectFlowId::new("flow-123"),
            return_to: "https://pubky.app/locks/connected".to_owned(),
            state: "opaque-state".to_owned(),
            authorization_url: CreatorConnectAuthorizationUrl::new(
                "pubkyauth://signin?secret=secret-flow-url",
            ),
            requested_scopes: vec![
                "/pub/app.locks/:rw".to_owned(),
                "/priv/app.locks/:rw".to_owned(),
            ],
            created_at: now,
            expires_at: now + Duration::minutes(5),
        }
    }

    fn grant_pending_flow(now: OffsetDateTime) -> PendingCreatorConnectFlowRecord {
        PendingCreatorConnectFlowRecord {
            authorization_url: CreatorConnectAuthorizationUrl::new(
                "pubkyauth://signin_grant?secret=grant",
            ),
            ..pending_flow(now)
        }
    }

    fn creator() -> CreatorPubky {
        CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
    }

    #[derive(Default)]
    struct FlowStore {
        record: Mutex<Option<PendingCreatorConnectFlowRecord>>,
    }

    impl FlowStore {
        fn with_record(record: PendingCreatorConnectFlowRecord) -> Self {
            Self {
                record: Mutex::new(Some(record)),
            }
        }

        fn record(&self) -> Option<PendingCreatorConnectFlowRecord> {
            self.record.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl CreatorConnectFlowStore for FlowStore {
        async fn insert_pending_creator_connect_flow(
            &self,
            record: PendingCreatorConnectFlowRecord,
        ) -> Result<(), ApplicationError> {
            *self.record.lock().unwrap() = Some(record);
            Ok(())
        }

        async fn get_pending_creator_connect_flow(
            &self,
            _flow_id: &CreatorConnectFlowId,
        ) -> Result<Option<PendingCreatorConnectFlowRecord>, ApplicationError> {
            Ok(self.record())
        }

        async fn delete_pending_creator_connect_flow(
            &self,
            _flow_id: &CreatorConnectFlowId,
        ) -> Result<(), ApplicationError> {
            *self.record.lock().unwrap() = None;
            Ok(())
        }

        async fn consume_pending_creator_connect_flow(
            &self,
            _flow_id: &CreatorConnectFlowId,
            now: OffsetDateTime,
        ) -> Result<Option<PendingCreatorConnectFlowRecord>, ApplicationError> {
            let mut record = self.record.lock().unwrap();
            if record
                .as_ref()
                .is_none_or(|record| record.is_expired_at(now))
            {
                return Ok(None);
            }
            Ok(record.take())
        }
    }

    #[derive(Default)]
    struct AuthorityStore {
        record: Mutex<Option<CreatorAuthorityRecord>>,
    }

    impl AuthorityStore {
        fn record(&self) -> Option<CreatorAuthorityRecord> {
            self.record.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl CreatorAuthorityStore for AuthorityStore {
        async fn get_creator_authority(
            &self,
            _creator: &CreatorPubky,
        ) -> Result<Option<CreatorAuthorityRecord>, ApplicationError> {
            Ok(self.record())
        }

        async fn upsert_creator_authority(
            &self,
            record: CreatorAuthorityRecord,
        ) -> Result<(), ApplicationError> {
            *self.record.lock().unwrap() = Some(record);
            Ok(())
        }

        async fn delete_creator_authority(
            &self,
            _creator: &CreatorPubky,
        ) -> Result<(), ApplicationError> {
            *self.record.lock().unwrap() = None;
            Ok(())
        }
    }

    #[derive(Default)]
    struct CodeStore {
        record: Mutex<Option<FrontendSessionCodeRecord>>,
    }

    impl CodeStore {
        fn record(&self) -> Option<FrontendSessionCodeRecord> {
            self.record.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl FrontendSessionCodeStore for CodeStore {
        async fn insert_frontend_session_code(
            &self,
            record: FrontendSessionCodeRecord,
        ) -> Result<(), ApplicationError> {
            *self.record.lock().unwrap() = Some(record);
            Ok(())
        }

        async fn consume_frontend_session_code(
            &self,
            _code: &FrontendSessionCode,
            _now: OffsetDateTime,
        ) -> Result<Option<FrontendSessionCodeRecord>, ApplicationError> {
            Ok(self.record())
        }
    }

    struct FakeConnectFlowClient;

    #[async_trait]
    impl LegacyCreatorConnectFlowClient for FakeConnectFlowClient {
        async fn start_legacy_creator_connect_flow(
            &self,
            _requested_scopes: &[String],
        ) -> Result<CreatorConnectAuthorizationUrl, ApplicationError> {
            unreachable!("complete use case must not start new flow")
        }

        async fn await_legacy_creator_connect_flow_approval(
            &self,
            _authorization_url: &CreatorConnectAuthorizationUrl,
        ) -> Result<LegacyCreatorConnectFlowApproval, ApplicationError> {
            Ok(LegacyCreatorConnectFlowApproval {
                creator: creator(),
                session_secret: CreatorAuthoritySecret::new("legacy-cookie-session-secret"),
            })
        }
    }

    struct ApprovedGrantConnectFlowClient(OffsetDateTime);

    #[async_trait]
    impl GrantCreatorConnectFlowClient for ApprovedGrantConnectFlowClient {
        async fn start_grant_creator_connect_flow(
            &self,
            _requested_scopes: &[String],
            _pop_key_id: &GrantPopKeyId,
        ) -> Result<CreatorConnectAuthorizationUrl, ApplicationError> {
            unreachable!()
        }

        async fn await_grant_creator_connect_flow_approval(
            &self,
            _authorization_url: &CreatorConnectAuthorizationUrl,
            pop_key_id: &GrantPopKeyId,
            _requested_scopes: &[String],
        ) -> Result<GrantCreatorConnectFlowApproval, ApplicationError> {
            assert_eq!(pop_key_id.as_str(), "locks-connect-v1.flow-123");
            Ok(GrantCreatorConnectFlowApproval {
                creator: creator(),
                grant_state: CreatorAuthoritySecret::new("delegated-grant-state"),
                granted_scopes: vec![
                    "/pub/app.locks/:rw".to_owned(),
                    "/priv/app.locks/:rw".to_owned(),
                ],
                grant_expires_at: self.0,
            })
        }
    }

    struct BlockingGrantConnectFlowClient {
        now: OffsetDateTime,
        entered: Semaphore,
        release: Semaphore,
    }

    impl BlockingGrantConnectFlowClient {
        fn new(now: OffsetDateTime) -> Self {
            Self {
                now,
                entered: Semaphore::new(0),
                release: Semaphore::new(0),
            }
        }
    }

    #[async_trait]
    impl GrantCreatorConnectFlowClient for BlockingGrantConnectFlowClient {
        async fn start_grant_creator_connect_flow(
            &self,
            _requested_scopes: &[String],
            _pop_key_id: &GrantPopKeyId,
        ) -> Result<CreatorConnectAuthorizationUrl, ApplicationError> {
            unreachable!()
        }

        async fn await_grant_creator_connect_flow_approval(
            &self,
            authorization_url: &CreatorConnectAuthorizationUrl,
            pop_key_id: &GrantPopKeyId,
            requested_scopes: &[String],
        ) -> Result<GrantCreatorConnectFlowApproval, ApplicationError> {
            self.entered.add_permits(1);
            self.release
                .acquire()
                .await
                .expect("approval-release semaphore remains open")
                .forget();
            ApprovedGrantConnectFlowClient(self.now + Duration::days(30))
                .await_grant_creator_connect_flow_approval(
                    authorization_url,
                    pop_key_id,
                    requested_scopes,
                )
                .await
        }
    }

    struct RejectedGrantConnectFlowClient;

    #[async_trait]
    impl GrantCreatorConnectFlowClient for RejectedGrantConnectFlowClient {
        async fn start_grant_creator_connect_flow(
            &self,
            _requested_scopes: &[String],
            _pop_key_id: &GrantPopKeyId,
        ) -> Result<CreatorConnectAuthorizationUrl, ApplicationError> {
            unreachable!()
        }

        async fn await_grant_creator_connect_flow_approval(
            &self,
            _authorization_url: &CreatorConnectAuthorizationUrl,
            _pop_key_id: &GrantPopKeyId,
            _requested_scopes: &[String],
        ) -> Result<GrantCreatorConnectFlowApproval, ApplicationError> {
            Err(ApplicationError::CreatorAuthorityUnavailable)
        }
    }

    struct FixedCodeGenerator;

    impl FrontendSessionCodeGenerator for FixedCodeGenerator {
        fn generate_frontend_session_code(&self) -> FrontendSessionCode {
            FrontendSessionCode::new("one-time-code")
        }
    }

    struct FixedClock(OffsetDateTime);

    impl Clock for FixedClock {
        fn now(&self) -> OffsetDateTime {
            self.0
        }
    }

    struct AdvancingClock(Mutex<VecDeque<OffsetDateTime>>);

    impl Clock for AdvancingClock {
        fn now(&self) -> OffsetDateTime {
            self.0.lock().unwrap().pop_front().expect("clock sample")
        }
    }
}
