use time::{Duration, OffsetDateTime};

use crate::application::errors::ApplicationError;
use crate::application::models::{
    CreatorAuthorityAuthKind, CreatorAuthorityRecord, CreatorConnectFlowId, FrontendSessionCode,
    FrontendSessionCodeRecord,
};
use crate::application::ports::{
    Clock, CreatorAuthorityStore, CreatorConnectFlowStore, FrontendSessionCodeGenerator,
    FrontendSessionCodeStore, LegacyCreatorConnectFlowClient,
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

/// Completes a legacy Pubky creator connect flow and issues a frontend session code.
pub async fn complete_creator_connect_flow(
    flow_store: &dyn CreatorConnectFlowStore,
    authority_store: &dyn CreatorAuthorityStore,
    code_store: &dyn FrontendSessionCodeStore,
    client: &dyn LegacyCreatorConnectFlowClient,
    code_generator: &dyn FrontendSessionCodeGenerator,
    clock: &dyn Clock,
    request: CompleteCreatorConnectFlowRequest,
) -> Result<CompleteCreatorConnectFlowResponse, ApplicationError> {
    let now = clock.now();
    let pending = flow_store
        .get_pending_creator_connect_flow(&request.flow_id)
        .await?
        .ok_or(ApplicationError::CreatorConnectFlowUnavailable)?;

    if pending.is_expired_at(now) {
        flow_store
            .delete_pending_creator_connect_flow(&request.flow_id)
            .await?;
        return Err(ApplicationError::CreatorConnectFlowExpired);
    }

    let approval = client
        .await_legacy_creator_connect_flow_approval(&pending.authorization_url)
        .await?;
    let authority = CreatorAuthorityRecord {
        creator: approval.creator.clone(),
        auth_kind: CreatorAuthorityAuthKind::LegacyCookie,
        granted_scopes: pending.requested_scopes.clone(),
        secret: approval.session_secret,
        session_expires_at: None,
        last_revalidated_at: Some(now),
    };
    authority_store.upsert_creator_authority(authority).await?;

    let code = code_generator.generate_frontend_session_code();
    let code_expires_at = now + FRONTEND_SESSION_CODE_TTL;
    code_store
        .insert_frontend_session_code(FrontendSessionCodeRecord {
            code: code.clone(),
            creator: approval.creator.clone(),
            state: pending.state.clone(),
            return_to: pending.return_to.clone(),
            created_at: now,
            expires_at: code_expires_at,
            consumed_at: None,
        })
        .await?;

    flow_store
        .delete_pending_creator_connect_flow(&request.flow_id)
        .await?;

    Ok(CompleteCreatorConnectFlowResponse {
        creator: approval.creator,
        state: pending.state,
        return_to: pending.return_to,
        code,
        code_expires_at,
    })
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use locks_core::ids::CreatorPubky;
    use time::{Duration, OffsetDateTime};

    use crate::application::errors::ApplicationError;
    use crate::application::models::{
        CreatorAuthorityAuthKind, CreatorAuthorityRecord, CreatorAuthoritySecret,
        CreatorConnectAuthorizationUrl, CreatorConnectFlowId, FrontendSessionCode,
        FrontendSessionCodeRecord, LegacyCreatorConnectFlowApproval,
        PendingCreatorConnectFlowRecord,
    };
    use crate::application::ports::{
        Clock, CreatorAuthorityStore, CreatorConnectFlowStore, FrontendSessionCodeGenerator,
        FrontendSessionCodeStore, LegacyCreatorConnectFlowClient,
    };
    use crate::application::use_cases::complete_creator_connect_flow::{
        CompleteCreatorConnectFlowRequest, complete_creator_connect_flow,
    };

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
            &client,
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
            vec!["/pub/locks.app/:rw", "/priv/locks.app/:rw"]
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
            &FakeConnectFlowClient,
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
            &FakeConnectFlowClient,
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

    fn pending_flow(now: OffsetDateTime) -> PendingCreatorConnectFlowRecord {
        PendingCreatorConnectFlowRecord {
            flow_id: CreatorConnectFlowId::new("flow-123"),
            return_to: "https://pubky.app/locks/connected".to_owned(),
            state: "opaque-state".to_owned(),
            authorization_url: CreatorConnectAuthorizationUrl::new("pubkyauth://secret-flow-url"),
            requested_scopes: vec![
                "/pub/locks.app/:rw".to_owned(),
                "/priv/locks.app/:rw".to_owned(),
            ],
            created_at: now,
            expires_at: now + Duration::minutes(5),
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
}
