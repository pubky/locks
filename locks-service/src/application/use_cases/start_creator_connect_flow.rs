use time::{Duration, OffsetDateTime};

use crate::application::errors::ApplicationError;
use crate::application::models::{
    CreatorConnectAuthorizationUrl, CreatorConnectFlowId, GrantPopKeyId,
    PendingCreatorConnectFlowRecord,
};
use crate::application::ports::{
    Clock, CreatorConnectFlowIdGenerator, CreatorConnectFlowStore, GrantCreatorConnectFlowClient,
    LegacyCreatorConnectFlowClient,
};

const CREATOR_CONNECT_FLOW_TTL: Duration = Duration::minutes(5);
const DEFAULT_REQUESTED_SCOPES: [&str; 2] = ["/pub/app.locks/:rw", "/priv/app.locks/:rw"];

/// Request to start a legacy Pubky creator connect flow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartCreatorConnectFlowRequest {
    /// Frontend return target carried through the connect flow.
    pub return_to: String,
    /// Opaque frontend state that must later match session-code exchange state.
    pub state: String,
}

/// Secret-bearing response for pubky.app to render as QR/deeplink.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartCreatorConnectFlowResponse {
    /// Server-generated flow ID.
    pub flow_id: CreatorConnectFlowId,
    /// Pubky auth authorization URL. Debug output redacts the embedded secret.
    pub authorization_url: CreatorConnectAuthorizationUrl,
    /// Expiration timestamp for the pending flow.
    pub expires_at: OffsetDateTime,
    /// Echoed opaque frontend state.
    pub state: String,
}

/// Starts and persists a pending legacy creator connect flow.
pub async fn start_creator_connect_flow(
    store: &dyn CreatorConnectFlowStore,
    client: &dyn LegacyCreatorConnectFlowClient,
    grant_client: Option<&dyn GrantCreatorConnectFlowClient>,
    flow_id_generator: &dyn CreatorConnectFlowIdGenerator,
    clock: &dyn Clock,
    request: StartCreatorConnectFlowRequest,
) -> Result<StartCreatorConnectFlowResponse, ApplicationError> {
    validate_start_request(&request)?;
    let now = clock.now();
    let expires_at = now + CREATOR_CONNECT_FLOW_TTL;
    let flow_id = flow_id_generator.generate_creator_connect_flow_id();
    let requested_scopes = default_requested_scopes();
    let authorization_url = match grant_client {
        Some(grant_client) => {
            grant_client
                .start_grant_creator_connect_flow(
                    &requested_scopes,
                    &GrantPopKeyId::for_connect_flow(&flow_id),
                )
                .await?
        }
        None => {
            client
                .start_legacy_creator_connect_flow(&requested_scopes)
                .await?
        }
    };

    store
        .insert_pending_creator_connect_flow(PendingCreatorConnectFlowRecord {
            flow_id: flow_id.clone(),
            return_to: request.return_to,
            state: request.state.clone(),
            authorization_url: authorization_url.clone(),
            requested_scopes,
            created_at: now,
            expires_at,
        })
        .await?;

    Ok(StartCreatorConnectFlowResponse {
        flow_id,
        authorization_url,
        expires_at,
        state: request.state,
    })
}

pub fn default_requested_scopes() -> Vec<String> {
    DEFAULT_REQUESTED_SCOPES
        .iter()
        .map(|scope| (*scope).to_owned())
        .collect()
}

fn validate_start_request(
    request: &StartCreatorConnectFlowRequest,
) -> Result<(), ApplicationError> {
    if request.return_to.trim().is_empty() {
        return Err(ApplicationError::Storage {
            message: "creator connect return_to is required".to_owned(),
        });
    }
    if request.state.trim().is_empty() {
        return Err(ApplicationError::Storage {
            message: "creator connect state is required".to_owned(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use time::{Duration, OffsetDateTime};

    use super::{StartCreatorConnectFlowRequest, start_creator_connect_flow};
    use crate::application::errors::ApplicationError;
    use crate::application::models::{
        CreatorConnectAuthorizationUrl, CreatorConnectFlowId, GrantCreatorConnectFlowApproval,
        GrantPopKeyId, LegacyCreatorConnectFlowApproval, PendingCreatorConnectFlowRecord,
    };
    use crate::application::ports::{
        Clock, CreatorConnectFlowIdGenerator, CreatorConnectFlowStore,
        GrantCreatorConnectFlowClient, LegacyCreatorConnectFlowClient,
    };

    #[tokio::test]
    async fn configured_grant_connect_starts_only_grant_flow() {
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        let store = RecordingFlowStore::default();
        let grant = RecordingGrantConnectFlowClient::default();

        let response = start_creator_connect_flow(
            &store,
            &PanicLegacyConnectFlowClient,
            Some(&grant),
            &FixedFlowIdGenerator,
            &FixedClock(now),
            StartCreatorConnectFlowRequest {
                return_to: "https://pubky.app/locks/connected".to_owned(),
                state: "opaque-state".to_owned(),
            },
        )
        .await
        .unwrap();

        assert_eq!(
            response.authorization_url.expose_url(),
            "pubkyauth://signin_grant?secret=grant"
        );
        assert_eq!(grant.started.lock().unwrap().len(), 1);
        assert_eq!(
            store.record().unwrap().authorization_url,
            response.authorization_url
        );
    }

    #[tokio::test]
    async fn start_creator_connect_flow_stores_pending_flow_and_returns_safe_response() {
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        let store = RecordingFlowStore::default();
        let client = FakeConnectFlowClient;
        let generator = FixedFlowIdGenerator;
        let clock = FixedClock(now);

        let response = start_creator_connect_flow(
            &store,
            &client,
            None,
            &generator,
            &clock,
            StartCreatorConnectFlowRequest {
                return_to: "https://pubky.app/locks/connected".to_owned(),
                state: "opaque-state".to_owned(),
            },
        )
        .await
        .unwrap();

        assert_eq!(response.flow_id, CreatorConnectFlowId::new("flow-123"));
        assert_eq!(
            response.authorization_url.expose_url(),
            "pubkyauth://signin?secret=secret-flow-url"
        );
        assert_eq!(response.state, "opaque-state");
        assert!(response.expires_at <= now + Duration::minutes(5));
        assert!(!format!("{response:?}").contains("secret-flow-url"));

        let stored = store.record().expect("pending flow stored");
        assert_eq!(stored.flow_id, response.flow_id);
        assert_eq!(stored.return_to, "https://pubky.app/locks/connected");
        assert_eq!(stored.state, "opaque-state");
        assert_eq!(
            stored.authorization_url.expose_url(),
            "pubkyauth://signin?secret=secret-flow-url"
        );
        assert_eq!(stored.created_at, now);
        assert_eq!(stored.expires_at, response.expires_at);
        assert_eq!(
            stored.requested_scopes,
            vec!["/pub/app.locks/:rw", "/priv/app.locks/:rw"]
        );
    }

    #[tokio::test]
    async fn start_creator_connect_flow_rejects_empty_state_and_return_to_before_starting_sdk_flow()
    {
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        let store = RecordingFlowStore::default();
        let client = FakeConnectFlowClient;
        let generator = FixedFlowIdGenerator;
        let clock = FixedClock(now);

        for request in [
            StartCreatorConnectFlowRequest {
                return_to: "".to_owned(),
                state: "state".to_owned(),
            },
            StartCreatorConnectFlowRequest {
                return_to: "https://pubky.app/locks/connected".to_owned(),
                state: "".to_owned(),
            },
        ] {
            let error =
                start_creator_connect_flow(&store, &client, None, &generator, &clock, request)
                    .await
                    .unwrap_err();
            assert!(matches!(error, ApplicationError::Storage { .. }));
        }

        assert!(store.record().is_none());
    }

    #[derive(Default)]
    struct RecordingFlowStore {
        record: std::sync::Mutex<Option<PendingCreatorConnectFlowRecord>>,
    }

    impl RecordingFlowStore {
        fn record(&self) -> Option<PendingCreatorConnectFlowRecord> {
            self.record.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl CreatorConnectFlowStore for RecordingFlowStore {
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

    struct FixedFlowIdGenerator;

    impl CreatorConnectFlowIdGenerator for FixedFlowIdGenerator {
        fn generate_creator_connect_flow_id(&self) -> CreatorConnectFlowId {
            CreatorConnectFlowId::new("flow-123")
        }
    }

    struct FixedClock(OffsetDateTime);

    impl Clock for FixedClock {
        fn now(&self) -> OffsetDateTime {
            self.0
        }
    }

    struct FakeConnectFlowClient;

    #[async_trait]
    impl LegacyCreatorConnectFlowClient for FakeConnectFlowClient {
        async fn start_legacy_creator_connect_flow(
            &self,
            _requested_scopes: &[String],
        ) -> Result<CreatorConnectAuthorizationUrl, ApplicationError> {
            Ok(CreatorConnectAuthorizationUrl::new(
                "pubkyauth://signin?secret=secret-flow-url",
            ))
        }

        async fn await_legacy_creator_connect_flow_approval(
            &self,
            _authorization_url: &CreatorConnectAuthorizationUrl,
        ) -> Result<LegacyCreatorConnectFlowApproval, ApplicationError> {
            unreachable!("start use case must not await approval")
        }
    }

    struct PanicLegacyConnectFlowClient;

    #[async_trait]
    impl LegacyCreatorConnectFlowClient for PanicLegacyConnectFlowClient {
        async fn start_legacy_creator_connect_flow(
            &self,
            _requested_scopes: &[String],
        ) -> Result<CreatorConnectAuthorizationUrl, ApplicationError> {
            panic!("grant-only mode must not start legacy cookie flow")
        }

        async fn await_legacy_creator_connect_flow_approval(
            &self,
            _authorization_url: &CreatorConnectAuthorizationUrl,
        ) -> Result<LegacyCreatorConnectFlowApproval, ApplicationError> {
            unreachable!()
        }
    }

    #[derive(Default)]
    struct RecordingGrantConnectFlowClient {
        started: std::sync::Mutex<Vec<GrantPopKeyId>>,
    }

    #[async_trait]
    impl GrantCreatorConnectFlowClient for RecordingGrantConnectFlowClient {
        async fn start_grant_creator_connect_flow(
            &self,
            _requested_scopes: &[String],
            pop_key_id: &GrantPopKeyId,
        ) -> Result<CreatorConnectAuthorizationUrl, ApplicationError> {
            self.started.lock().unwrap().push(pop_key_id.clone());
            Ok(CreatorConnectAuthorizationUrl::new(
                "pubkyauth://signin_grant?secret=grant",
            ))
        }

        async fn await_grant_creator_connect_flow_approval(
            &self,
            _authorization_url: &CreatorConnectAuthorizationUrl,
            _pop_key_id: &GrantPopKeyId,
            _requested_scopes: &[String],
        ) -> Result<GrantCreatorConnectFlowApproval, ApplicationError> {
            unreachable!()
        }
    }
}
