use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use pubky::{
    AuthFlowKind, Capabilities, ClientId, DelegatedGrantAuthFlowState, GrantClaims,
    PubkyGrantAuthFlow, PubkyHttpClient, PublicKey,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use url::Url;

use crate::application::errors::ApplicationError;
use crate::application::models::{FoldedAuthFlowSecret, GrantPopKeyId};
use crate::application::ports::{
    FoldedAuthApprovalPoll, FoldedAuthApprovalPollError, FoldedAuthApprovalPoller,
    FoldedAuthPollCall,
};
use crate::infrastructure::pubky::grant_connect_flow::LockServerGrantPopKeys;
use crate::infrastructure::pubky::legacy_connect_flow::creator_from_pubky_public_key_z32;

const FLOW_STATE_VERSION: u16 = 1;

#[derive(Clone)]
pub struct PubkyFoldedAuthApprovalPoller {
    client: PubkyHttpClient,
    pop_keys: LockServerGrantPopKeys,
    flows: Arc<Mutex<HashMap<[u8; 32], CachedFlow>>>,
}

const MAX_CACHED_FLOWS: usize = 1_024;
const CACHED_FLOW_IDLE_TTL: std::time::Duration = std::time::Duration::from_secs(10 * 60);

struct CachedFlow {
    flow: Arc<Mutex<PubkyGrantAuthFlow>>,
    last_polled: std::time::Instant,
}

impl PubkyFoldedAuthApprovalPoller {
    pub fn new(client: PubkyHttpClient, pop_keys: LockServerGrantPopKeys) -> Self {
        Self {
            client,
            pop_keys,
            flows: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredDelegatedFlowState {
    version: u16,
    authorization_url: String,
    key_id: String,
    client_pk: String,
    approval_key_secret: Option<String>,
}

pub fn start_folded_auth_flow(
    client: PubkyHttpClient,
    relay: Option<Url>,
    client_id: ClientId,
    pop_keys: &LockServerGrantPopKeys,
    key_id: &GrantPopKeyId,
) -> Result<(String, FoldedAuthFlowSecret), ApplicationError> {
    let mut builder =
        PubkyGrantAuthFlow::builder(&Capabilities::default(), AuthFlowKind::signin(), client_id)
            .client(client)
            .delegated_client_signer(
                key_id.as_str().to_owned(),
                pop_keys.public_key(key_id),
                pop_keys.signer(key_id),
            );
    if let Some(relay) = relay {
        builder = builder.relay(relay);
    }
    let flow = builder.start().map_err(|_| flow_error("failed to start"))?;
    let authorization_url = flow.authorization_url().to_string();
    let state = flow
        .save_delegated()
        .ok_or_else(|| flow_error("missing delegated restore state"))?;
    Ok((authorization_url, encode_flow_state(&state)?))
}

pub fn encode_flow_state(
    state: &DelegatedGrantAuthFlowState,
) -> Result<FoldedAuthFlowSecret, ApplicationError> {
    let encoded = serde_json::to_vec(&StoredDelegatedFlowState {
        version: FLOW_STATE_VERSION,
        authorization_url: state.authorization_url.clone(),
        key_id: state.key_id.clone(),
        client_pk: state.client_pk.z32(),
        approval_key_secret: state
            .approval_key_secret
            .map(|secret| URL_SAFE_NO_PAD.encode(secret)),
    })
    .map_err(|_| flow_error("failed to encode"))?;
    Ok(FoldedAuthFlowSecret::new(encoded))
}

pub fn decode_flow_state(
    secret: &FoldedAuthFlowSecret,
) -> Result<DelegatedGrantAuthFlowState, ApplicationError> {
    let stored: StoredDelegatedFlowState = serde_json::from_slice(secret.expose_bytes())
        .map_err(|_| flow_error("invalid stored state"))?;
    if stored.version != FLOW_STATE_VERSION
        || stored.authorization_url.is_empty()
        || stored.key_id.is_empty()
    {
        return Err(flow_error("invalid stored state"));
    }
    let client_pk = PublicKey::try_from_z32(&stored.client_pk)
        .map_err(|_| flow_error("invalid stored state"))?;
    let approval_key_secret = stored
        .approval_key_secret
        .map(|value| {
            URL_SAFE_NO_PAD
                .decode(value)
                .map_err(|_| flow_error("invalid stored state"))?
                .try_into()
                .map_err(|_| flow_error("invalid stored state"))
        })
        .transpose()?;
    Ok(DelegatedGrantAuthFlowState {
        authorization_url: stored.authorization_url,
        key_id: stored.key_id,
        client_pk,
        approval_key_secret,
    })
}

#[async_trait]
impl FoldedAuthApprovalPoller for PubkyFoldedAuthApprovalPoller {
    async fn poll_approval(&self, flow_secret: &FoldedAuthFlowSecret) -> FoldedAuthPollCall {
        let state = match decode_flow_state(flow_secret) {
            Ok(state) => state,
            Err(error) => return FoldedAuthPollCall::NotAttempted(error),
        };
        let key_id = GrantPopKeyId::new(state.key_id.clone());
        if state.client_pk != self.pop_keys.public_key(&key_id) {
            return FoldedAuthPollCall::NotAttempted(flow_error("invalid delegated key binding"));
        }
        let flow_key = *blake3::hash(flow_secret.expose_bytes()).as_bytes();
        let flow = {
            let mut flows = self.flows.lock().await;
            let now = std::time::Instant::now();
            flows.retain(|_, cached| now.duration_since(cached.last_polled) < CACHED_FLOW_IDLE_TTL);
            if let Some(cached) = flows.get_mut(&flow_key) {
                cached.last_polled = now;
                Arc::clone(&cached.flow)
            } else {
                let flow = match PubkyGrantAuthFlow::restore_delegated(
                    state,
                    self.client.clone(),
                    self.pop_keys.signer(&key_id),
                ) {
                    Ok(flow) => Arc::new(Mutex::new(flow)),
                    Err(_) => {
                        return FoldedAuthPollCall::NotAttempted(flow_error("failed to restore"));
                    }
                };
                if flows.len() >= MAX_CACHED_FLOWS
                    && let Some(oldest) = flows
                        .iter()
                        .min_by_key(|(_, cached)| cached.last_polled)
                        .map(|(key, _)| *key)
                {
                    flows.remove(&oldest);
                }
                flows.insert(
                    flow_key,
                    CachedFlow {
                        flow: Arc::clone(&flow),
                        last_polled: now,
                    },
                );
                flow
            }
        };
        tokio::task::yield_now().await;
        let attempted = flow.lock().await.try_poll_credential_once().await;
        let Some(credential) = (match attempted {
            Ok(credential) => credential,
            Err(_) => {
                self.flows.lock().await.remove(&flow_key);
                return FoldedAuthPollCall::Attempted(Err(FoldedAuthApprovalPollError));
            }
        }) else {
            return FoldedAuthPollCall::Attempted(Ok(FoldedAuthApprovalPoll::Pending));
        };
        self.flows.lock().await.remove(&flow_key);
        let Some(state) = credential.export_delegated_restore_state().await else {
            return FoldedAuthPollCall::Attempted(Err(FoldedAuthApprovalPollError));
        };
        let Ok(claims) = GrantClaims::decode(&state.grant_jws) else {
            return FoldedAuthPollCall::Attempted(Err(FoldedAuthApprovalPollError));
        };
        let Ok(reader) = creator_from_pubky_public_key_z32(&claims.iss.z32()) else {
            return FoldedAuthPollCall::Attempted(Err(FoldedAuthApprovalPollError));
        };
        FoldedAuthPollCall::Attempted(Ok(FoldedAuthApprovalPoll::Approved { reader }))
    }
}

fn flow_error(class: &'static str) -> ApplicationError {
    ApplicationError::FoldedAuthSecret {
        message: format!("folded auth flow {class}"),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use axum::Router;
    use axum::body::Bytes;
    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::routing::get;
    use pubky::{ClientId, DelegatedGrantAuthFlowState, Keypair, Pubky, PubkyHttpClient};
    use tokio::sync::{Mutex, Notify};
    use url::Url;

    use super::{
        PubkyFoldedAuthApprovalPoller, decode_flow_state, encode_flow_state, start_folded_auth_flow,
    };
    use crate::application::models::{FoldedAuthFlowSecret, GrantPopKeyId};
    use crate::application::ports::{FoldedAuthApprovalPoller, FoldedAuthPollCall};
    use crate::infrastructure::pubky::grant_connect_flow::LockServerGrantPopKeys;

    #[test]
    fn delegated_pubky_flow_state_round_trips_every_restore_field() {
        let key_id = GrantPopKeyId::new("folded-flow-7".to_owned());
        let keys = LockServerGrantPopKeys::from_lock_server_seed([9; 32]);
        let state = DelegatedGrantAuthFlowState {
            authorization_url: "pubkyauth://signin?relay=https%3A%2F%2Frelay.example".to_owned(),
            key_id: key_id.as_str().to_owned(),
            client_pk: keys.public_key(&key_id),
            approval_key_secret: Some([4; 32]),
        };

        let decoded = decode_flow_state(&encode_flow_state(&state).unwrap()).unwrap();
        assert_eq!(decoded.authorization_url, state.authorization_url);
        assert_eq!(decoded.key_id, state.key_id);
        assert_eq!(decoded.client_pk, state.client_pk);
        assert_eq!(decoded.approval_key_secret, state.approval_key_secret);
    }

    #[test]
    fn delegated_pubky_flow_state_rejects_unknown_and_malformed_versions() {
        for bytes in [
            br#"{"version":2,"authorization_url":"x","key_id":"x","client_pk":"x","approval_key_secret":null}"#.as_slice(),
            br#"{"version":1,"authorization_url":"x","key_id":"x","client_pk":"x","approval_key_secret":null,"extra":true}"#.as_slice(),
            b"not-json".as_slice(),
        ] {
            let error = decode_flow_state(&FoldedAuthFlowSecret::new(bytes.to_vec())).unwrap_err();
            assert_eq!(error.to_string(), "folded auth secret error: folded auth flow invalid stored state");
            assert!(!format!("{error:?}").contains("approval_key_secret"));
        }
    }

    #[tokio::test]
    async fn real_pubky_ack_then_exchange_failure_is_classified_as_attempted() {
        let relay = RelayState::default();
        let app = Router::new()
            .route(
                "/{*path}",
                get(relay_get).post(relay_post).delete(relay_delete),
            )
            .with_state(relay.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let relay_url =
            Url::parse(&format!("http://{}/inbox", listener.local_addr().unwrap())).unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let mut client_builder = PubkyHttpClient::builder();
        client_builder.pkarr(|builder| {
            builder
                .no_default_network()
                .relays(&["http://127.0.0.1:1"])
                .unwrap()
                .relay_request_timeout(std::time::Duration::from_millis(50))
        });
        let client = client_builder.build().unwrap();
        let keys = LockServerGrantPopKeys::from_lock_server_seed([7; 32]);
        let key_id = GrantPopKeyId::new("folded-real-sdk".to_owned());
        let (authorization_url, secret) = start_folded_auth_flow(
            client.clone(),
            Some(relay_url),
            ClientId::new("locks.test").unwrap(),
            &keys,
            &key_id,
        )
        .unwrap();
        Pubky::with_client(client.clone())
            .signer(Keypair::random())
            .approve_auth(authorization_url)
            .await
            .unwrap();

        let poller = PubkyFoldedAuthApprovalPoller::new(client, keys);
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                match poller.poll_approval(&secret).await {
                    FoldedAuthPollCall::Attempted(Ok(_)) => tokio::task::yield_now().await,
                    result => break result,
                }
            }
        })
        .await
        .expect("real Pubky poll must finish");
        assert!(matches!(result, FoldedAuthPollCall::Attempted(Err(_))));
        assert!(relay.acked.load(Ordering::SeqCst));
    }

    #[derive(Clone, Default)]
    struct RelayState {
        body: Arc<Mutex<Option<Bytes>>>,
        ready: Arc<Notify>,
        acked: Arc<AtomicBool>,
    }

    async fn relay_post(State(state): State<RelayState>, body: Bytes) -> StatusCode {
        *state.body.lock().await = Some(body);
        state.ready.notify_waiters();
        StatusCode::OK
    }

    async fn relay_get(State(state): State<RelayState>) -> Bytes {
        loop {
            if let Some(body) = state.body.lock().await.clone() {
                return body;
            }
            state.ready.notified().await;
        }
    }

    async fn relay_delete(State(state): State<RelayState>) -> StatusCode {
        state.acked.store(true, Ordering::SeqCst);
        StatusCode::OK
    }
}
