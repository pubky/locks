use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use locks_core::ids::CreatorPubky;
use pubky::{
    AuthFlowKind, Capability, ClientId, DelegatedGrantAuthFlowState, DelegatedGrantCredentialState,
    DelegatedSignFn, GrantClaims, GrantCredential, Keypair, POP_JWS_TYP, PopProofClaims,
    PubkyGrantAuthFlow, PubkyHttpClient, PubkySession, PublicKey, delegated_sign_callback,
};
use serde_json::{Map, Value, json};
use time::OffsetDateTime;
use url::Url;

use crate::application::errors::ApplicationError;
use crate::application::models::{
    CreatorAuthoritySecret, CreatorConnectAuthorizationUrl, GrantCreatorConnectFlowApproval,
    GrantPopKeyId,
};
use crate::application::ports::GrantCreatorConnectFlowClient;
use crate::infrastructure::pubky::legacy_connect_flow::{
    creator_from_pubky_public_key_z32, requested_scopes_to_capabilities,
};

/// Changing this context or Lock Server seed makes existing delegated grants unrestorable.
const GRANT_POP_KEY_CONTEXT: &str = "pubky-locks 2026-09-23 creator grant PoP key v1";
const GRANT_STATE_VERSION: u64 = 1;
const GRANT_STATE_FIELDS: [&str; 5] = ["client_pk", "grant_jws", "homeserver", "key_id", "v"];

/// Deterministic per-flow PoP keys derived from Lock Server signing seed.
#[derive(Clone)]
pub struct LockServerGrantPopKeys {
    seed: Arc<[u8; 32]>,
}

impl fmt::Debug for LockServerGrantPopKeys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("LockServerGrantPopKeys")
            .field(&"<redacted>")
            .finish()
    }
}

impl LockServerGrantPopKeys {
    pub fn from_lock_server_seed(seed: [u8; 32]) -> Self {
        Self {
            seed: Arc::new(seed),
        }
    }

    fn keypair(&self, key_id: &GrantPopKeyId) -> Keypair {
        let mut hasher = blake3::Hasher::new_derive_key(GRANT_POP_KEY_CONTEXT);
        hasher.update(self.seed.as_ref());
        hasher.update(key_id.as_str().as_bytes());
        Keypair::from_secret(hasher.finalize().as_bytes())
    }

    pub fn public_key(&self, key_id: &GrantPopKeyId) -> PublicKey {
        self.keypair(key_id).public_key()
    }

    pub fn signer(&self, key_id: &GrantPopKeyId) -> DelegatedSignFn {
        let keypair = self.keypair(key_id);
        delegated_sign_callback(move |signing_input: String| {
            let signature = sign_pop_signing_input(&keypair, &signing_input);
            async move { signature }
        })
    }
}

fn sign_pop_signing_input(keypair: &Keypair, signing_input: &str) -> pubky::Result<Vec<u8>> {
    if !is_pop_signing_input(signing_input) {
        return Err(pubky::errors::AuthError::Validation(
            "Lock Server grant key only signs pubky-pop proofs".to_owned(),
        )
        .into());
    }
    Ok(keypair.sign(signing_input.as_bytes()).to_bytes().to_vec())
}

fn is_pop_signing_input(signing_input: &str) -> bool {
    let Some((header, claims)) = signing_input.split_once('.') else {
        return false;
    };
    if claims.is_empty() || claims.contains('.') {
        return false;
    }
    let Ok(header) = URL_SAFE_NO_PAD.decode(header) else {
        return false;
    };
    let Ok(header) = serde_json::from_slice::<Map<String, Value>>(&header) else {
        return false;
    };
    let mut header_fields: Vec<&str> = header.keys().map(String::as_str).collect();
    header_fields.sort_unstable();
    if header_fields != ["alg", "typ"] {
        return false;
    }
    if header.get("alg").and_then(Value::as_str) != Some("EdDSA")
        || header.get("typ").and_then(Value::as_str) != Some(POP_JWS_TYP)
    {
        return false;
    }
    let Ok(claims) = URL_SAFE_NO_PAD.decode(claims) else {
        return false;
    };
    let Ok(object) = serde_json::from_slice::<Map<String, Value>>(&claims) else {
        return false;
    };
    let mut fields: Vec<&str> = object.keys().map(String::as_str).collect();
    fields.sort_unstable();
    fields == ["aud", "gid", "iat", "nonce"]
        && serde_json::from_slice::<PopProofClaims>(&claims).is_ok()
}

/// Pubky SDK delegated-grant connect adapter.
#[derive(Debug, Clone)]
pub struct PubkyGrantCreatorConnectFlowClient {
    client: PubkyHttpClient,
    auth_relay: Option<Url>,
    client_id: ClientId,
    pop_keys: LockServerGrantPopKeys,
}

impl PubkyGrantCreatorConnectFlowClient {
    pub fn new(
        client: PubkyHttpClient,
        auth_relay: Option<Url>,
        client_id: ClientId,
        pop_keys: LockServerGrantPopKeys,
    ) -> Self {
        Self {
            client,
            auth_relay,
            client_id,
            pop_keys,
        }
    }
}

#[async_trait]
impl GrantCreatorConnectFlowClient for PubkyGrantCreatorConnectFlowClient {
    async fn start_grant_creator_connect_flow(
        &self,
        requested_scopes: &[String],
        pop_key_id: &GrantPopKeyId,
    ) -> Result<CreatorConnectAuthorizationUrl, ApplicationError> {
        let capabilities = requested_scopes_to_capabilities(requested_scopes)?;
        let mut builder = PubkyGrantAuthFlow::builder(
            &capabilities,
            AuthFlowKind::signin(),
            self.client_id.clone(),
        )
        .client(self.client.clone())
        .delegated_client_signer(
            pop_key_id.as_str().to_owned(),
            self.pop_keys.public_key(pop_key_id),
            self.pop_keys.signer(pop_key_id),
        );
        if let Some(auth_relay) = &self.auth_relay {
            builder = builder.relay(auth_relay.clone());
        }
        let flow = builder
            .start()
            .map_err(|_| grant_error("failed to start grant creator connect flow"))?;
        Ok(CreatorConnectAuthorizationUrl::new(
            flow.authorization_url().to_string(),
        ))
    }

    async fn await_grant_creator_connect_flow_approval(
        &self,
        authorization_url: &CreatorConnectAuthorizationUrl,
        pop_key_id: &GrantPopKeyId,
        requested_scopes: &[String],
    ) -> Result<GrantCreatorConnectFlowApproval, ApplicationError> {
        let expected_client_pk = self.pop_keys.public_key(pop_key_id);
        let flow = PubkyGrantAuthFlow::restore_delegated(
            DelegatedGrantAuthFlowState {
                authorization_url: authorization_url.expose_url().to_owned(),
                key_id: pop_key_id.as_str().to_owned(),
                client_pk: expected_client_pk.clone(),
            },
            self.client.clone(),
            self.pop_keys.signer(pop_key_id),
        )
        .map_err(|_| grant_error("failed to resume grant creator connect flow"))?;
        let credential = flow
            .await_credential()
            .await
            .map_err(|_| grant_error("grant creator connect flow approval failed or expired"))?;
        let state = credential
            .export_delegated_restore_state()
            .await
            .ok_or_else(|| grant_error("grant creator connect flow did not use a delegated key"))?;
        let session_pubky =
            PubkySession::from_grant_credential(self.client.clone(), credential).public_key();
        grant_approval_from_parts(
            state,
            &session_pubky,
            &self.client_id,
            &expected_client_pk,
            requested_scopes,
        )
    }
}

fn grant_approval_from_parts(
    state: DelegatedGrantCredentialState,
    session_pubky: &PublicKey,
    expected_client_id: &ClientId,
    expected_client_pk: &PublicKey,
    requested_scopes: &[String],
) -> Result<GrantCreatorConnectFlowApproval, ApplicationError> {
    let claims = GrantClaims::decode(&state.grant_jws)
        .map_err(|_| grant_error("grant creator connect flow returned no grant"))?;
    if &claims.iss != session_pubky {
        return Err(grant_error(
            "grant issuer does not match the homeserver session",
        ));
    }
    if &claims.client_id != expected_client_id {
        return Err(grant_error("grant names a different client id"));
    }
    if &claims.cnf != expected_client_pk || &state.client_pk != expected_client_pk {
        return Err(grant_error("grant is not bound to the Lock Server key"));
    }
    let requested = requested_scopes_to_capabilities(requested_scopes)?;
    if !requested
        .iter()
        .all(|required| grant_covers(&claims.caps, required))
    {
        return Err(grant_error("grant does not cover the requested scopes"));
    }
    let creator = creator_from_pubky_public_key_z32(&claims.iss.z32())?;
    let grant_expires_at = i64::try_from(claims.exp)
        .ok()
        .and_then(|exp| OffsetDateTime::from_unix_timestamp(exp).ok())
        .ok_or_else(|| grant_error("grant expiry is out of range"))?;
    if grant_expires_at <= OffsetDateTime::now_utc() {
        return Err(grant_error("grant is expired"));
    }

    Ok(GrantCreatorConnectFlowApproval {
        creator,
        grant_state: encode_grant_state(&state),
        granted_scopes: claims.caps.iter().map(ToString::to_string).collect(),
        grant_expires_at,
    })
}

fn grant_covers(granted: &[Capability], required: &Capability) -> bool {
    granted.iter().any(|capability| {
        capability.scope_covers_path(required.scope())
            && required
                .actions()
                .iter()
                .all(|action| capability.actions().contains(action))
    })
}

pub fn encode_grant_state(state: &DelegatedGrantCredentialState) -> CreatorAuthoritySecret {
    CreatorAuthoritySecret::new(
        json!({
            "v": GRANT_STATE_VERSION,
            "grant_jws": state.grant_jws,
            "homeserver": state.homeserver_pk.z32(),
            "key_id": state.key_id,
            "client_pk": state.client_pk.z32(),
        })
        .to_string(),
    )
}

pub fn decode_grant_state(
    secret: &CreatorAuthoritySecret,
) -> Result<DelegatedGrantCredentialState, ApplicationError> {
    let object: Map<String, Value> =
        serde_json::from_str(secret.expose_secret()).map_err(|_| invalid_grant_state())?;
    let mut fields: Vec<&str> = object.keys().map(String::as_str).collect();
    fields.sort_unstable();
    if fields != GRANT_STATE_FIELDS
        || object.get("v").and_then(Value::as_u64) != Some(GRANT_STATE_VERSION)
    {
        return Err(invalid_grant_state());
    }
    let text = |name: &str| {
        object
            .get(name)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(invalid_grant_state)
    };
    let public_key =
        |name: &str| PublicKey::try_from_z32(text(name)?).map_err(|_| invalid_grant_state());
    Ok(DelegatedGrantCredentialState {
        grant_jws: text("grant_jws")?.to_owned(),
        homeserver_pk: public_key("homeserver")?,
        key_id: text("key_id")?.to_owned(),
        client_pk: public_key("client_pk")?,
    })
}

pub async fn restore_grant_session(
    client: &PubkyHttpClient,
    pop_keys: &LockServerGrantPopKeys,
    expected_client_id: &ClientId,
    required_scopes: &[String],
    secret: &CreatorAuthoritySecret,
) -> Result<PubkySession, ApplicationError> {
    let state = decode_grant_state(secret)?;
    let key_id = GrantPopKeyId::new(state.key_id.clone());
    let expected_client_pk = pop_keys.public_key(&key_id);
    let claims = GrantClaims::decode(&state.grant_jws).map_err(|_| grant_restore_error())?;
    let required =
        requested_scopes_to_capabilities(required_scopes).map_err(|_| grant_restore_error())?;
    if state.client_pk != expected_client_pk
        || claims.cnf != expected_client_pk
        || &claims.client_id != expected_client_id
        || !required
            .iter()
            .all(|required| grant_covers(&claims.caps, required))
        || i64::try_from(claims.exp)
            .ok()
            .and_then(|exp| OffsetDateTime::from_unix_timestamp(exp).ok())
            .is_none_or(|expires_at| expires_at <= OffsetDateTime::now_utc())
    {
        return Err(grant_restore_error());
    }
    let credential =
        GrantCredential::import_delegated_state(state, client, pop_keys.signer(&key_id))
            .await
            .map_err(|_| grant_restore_error())?;
    Ok(PubkySession::from_grant_credential(
        client.clone(),
        credential,
    ))
}

pub async fn restore_grant_session_for_creator(
    client: &PubkyHttpClient,
    pop_keys: &LockServerGrantPopKeys,
    expected_client_id: &ClientId,
    required_scopes: &[String],
    creator: &CreatorPubky,
    secret: &CreatorAuthoritySecret,
) -> Result<PubkySession, ApplicationError> {
    let session = restore_grant_session(
        client,
        pop_keys,
        expected_client_id,
        required_scopes,
        secret,
    )
    .await?;
    let restored = creator_from_pubky_public_key_z32(&session.public_key().z32())?;
    if &restored != creator {
        return Err(ApplicationError::CreatorAuthorityUnavailable);
    }
    Ok(session)
}

fn grant_error(message: &'static str) -> ApplicationError {
    ApplicationError::CreatorAuthoritySecret {
        message: message.to_owned(),
    }
}

fn invalid_grant_state() -> ApplicationError {
    grant_error("invalid stored grant creator authority")
}

fn grant_restore_error() -> ApplicationError {
    grant_error("failed to restore grant creator authority")
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use pubky::{
        Capability, ClientId, DelegatedGrantCredentialState, GRANT_JWS_TYP, GrantClaims, GrantId,
        Keypair, POP_JWS_TYP, PopNonce, PopProofClaims, PublicKey,
    };
    use pubky_common::auth::jws::jws_signing_input;
    use url::Url;

    use super::{
        LockServerGrantPopKeys, PubkyGrantCreatorConnectFlowClient, decode_grant_state,
        encode_grant_state, grant_approval_from_parts, restore_grant_session,
        sign_pop_signing_input,
    };
    use crate::application::errors::ApplicationError;
    use crate::application::models::{CreatorAuthoritySecret, CreatorConnectFlowId, GrantPopKeyId};
    use crate::application::ports::{
        GrantCreatorConnectFlowClient, LegacyCreatorConnectFlowClient,
    };
    use crate::infrastructure::pubky::PubkyLegacyCreatorConnectFlowClient;

    const SEED: [u8; 32] = [42; 32];
    const CLIENT_ID: &str = "locks.example";

    fn keys() -> LockServerGrantPopKeys {
        LockServerGrantPopKeys::from_lock_server_seed(SEED)
    }

    fn key_id() -> GrantPopKeyId {
        GrantPopKeyId::for_connect_flow(&CreatorConnectFlowId::new("flow-123"))
    }

    fn signed_state(
        issuer: &Keypair,
        client_id: &str,
        cnf: PublicKey,
        caps: &str,
    ) -> DelegatedGrantCredentialState {
        let claims = GrantClaims {
            iss: issuer.public_key(),
            client_id: ClientId::new(client_id).unwrap(),
            caps: caps
                .split(',')
                .map(|cap| Capability::from_str(cap).unwrap())
                .collect(),
            cnf: cnf.clone(),
            jti: GrantId::generate(),
            iat: 1_790_000_000,
            exp: 4_102_444_800,
        };
        DelegatedGrantCredentialState {
            grant_jws: claims.sign(issuer, GRANT_JWS_TYP),
            homeserver_pk: Keypair::random().public_key(),
            key_id: key_id().as_str().to_owned(),
            client_pk: cnf,
        }
    }

    fn approval(
        state: DelegatedGrantCredentialState,
        session_pubky: &PublicKey,
    ) -> Result<crate::application::models::GrantCreatorConnectFlowApproval, ApplicationError> {
        grant_approval_from_parts(
            state,
            session_pubky,
            &ClientId::new(CLIENT_ID).unwrap(),
            &keys().public_key(&key_id()),
            &[
                "/pub/locks.app/:rw".to_owned(),
                "/priv/locks.app/:rw".to_owned(),
            ],
        )
    }

    #[test]
    fn grant_pop_keys_are_stable_per_flow_and_distinct_from_identity_key() {
        let flow_a = key_id();
        let flow_b = GrantPopKeyId::for_connect_flow(&CreatorConnectFlowId::new("flow-456"));

        assert_eq!(keys().public_key(&flow_a), keys().public_key(&flow_a));
        assert_ne!(keys().public_key(&flow_a), keys().public_key(&flow_b));
        assert_ne!(
            keys().public_key(&flow_a),
            LockServerGrantPopKeys::from_lock_server_seed([7; 32]).public_key(&flow_a)
        );
        assert_ne!(
            keys().public_key(&flow_a),
            Keypair::from_secret(&SEED).public_key()
        );
        assert!(!format!("{:?}", keys()).contains("42"));
    }

    #[test]
    fn grant_pop_key_signs_only_pubky_pop_proofs() {
        let keypair = keys().keypair(&key_id());
        let pop = jws_signing_input(
            POP_JWS_TYP,
            &PopProofClaims {
                aud: Keypair::random().public_key(),
                gid: GrantId::generate(),
                nonce: PopNonce::generate(),
                iat: 1_790_000_000,
            },
        );
        assert_eq!(
            sign_pop_signing_input(&keypair, &pop).unwrap(),
            keypair.sign(pop.as_bytes()).to_bytes().to_vec()
        );

        let grant = jws_signing_input(GRANT_JWS_TYP, &serde_json::json!({"caps": "/:rw"}));
        let unsigned_header = format!(
            "{}.e30",
            URL_SAFE_NO_PAD.encode(br#"{"alg":"none","typ":"pubky-pop"}"#)
        );
        let arbitrary_pop_payload = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(br#"{"alg":"EdDSA","typ":"pubky-pop"}"#),
            URL_SAFE_NO_PAD.encode(br#"{"foo":"bar"}"#)
        );
        for rejected in [
            grant.as_str(),
            unsigned_header.as_str(),
            arbitrary_pop_payload.as_str(),
            "not-a-jws",
            "e30.e30.e30",
            "",
        ] {
            assert!(sign_pop_signing_input(&keypair, rejected).is_err());
        }
    }

    #[test]
    fn stored_grant_state_contains_restore_metadata_without_private_keys() {
        let pop_keypair = keys().keypair(&key_id());
        let state = signed_state(
            &Keypair::random(),
            CLIENT_ID,
            pop_keypair.public_key(),
            "/priv/locks.app/:rw,/pub/locks.app/:rw",
        );

        let stored = encode_grant_state(&state);
        let text = stored.expose_secret();
        let object: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(text).unwrap();
        let mut fields: Vec<&str> = object.keys().map(String::as_str).collect();
        fields.sort_unstable();
        assert_eq!(
            fields,
            ["client_pk", "grant_jws", "homeserver", "key_id", "v"]
        );
        for secret in [pop_keypair.secret(), SEED] {
            assert!(!text.contains(&URL_SAFE_NO_PAD.encode(secret)));
            assert!(!text.contains(&base64::engine::general_purpose::STANDARD.encode(secret)));
        }
        assert!(!format!("{stored:?}").contains(&state.grant_jws));
        assert_eq!(decode_grant_state(&stored).unwrap(), state);
    }

    #[test]
    fn stored_grant_state_rejects_unknown_missing_and_legacy_formats() {
        let state = signed_state(
            &Keypair::random(),
            CLIENT_ID,
            keys().public_key(&key_id()),
            "/priv/locks.app/:rw,/pub/locks.app/:rw",
        );
        let stored = encode_grant_state(&state);
        let mut object: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(stored.expose_secret()).unwrap();
        let mut with_secret = object.clone();
        with_secret.insert("client_key_secret".to_owned(), "AAAA".into());
        let mut wrong_version = object.clone();
        wrong_version.insert("v".to_owned(), 2.into());
        object.remove("key_id");

        for rejected in [
            serde_json::Value::Object(with_secret).to_string(),
            serde_json::Value::Object(wrong_version).to_string(),
            serde_json::Value::Object(object).to_string(),
            "legacy-cookie-session-secret".to_owned(),
        ] {
            assert!(decode_grant_state(&CreatorAuthoritySecret::new(rejected)).is_err());
        }
    }

    #[test]
    fn grant_creator_must_equal_grant_issuer() {
        let issuer = Keypair::random();
        let state = signed_state(
            &issuer,
            CLIENT_ID,
            keys().public_key(&key_id()),
            "/priv/locks.app/:rw,/pub/locks.app/:rw",
        );

        assert!(approval(state.clone(), &Keypair::random().public_key()).is_err());
        let approved = approval(state, &issuer.public_key()).unwrap();
        assert_eq!(
            approved.creator.to_string(),
            format!("pubky{}", issuer.public_key().z32())
        );
    }

    #[test]
    fn grant_approval_rejects_foreign_client_id_key_and_narrow_caps() {
        let issuer = Keypair::random();
        let pop = keys().public_key(&key_id());
        let full = "/priv/locks.app/:rw,/pub/locks.app/:rw";
        for (state, message) in [
            (
                signed_state(&issuer, "evil.example", pop.clone(), full),
                "grant names a different client id",
            ),
            (
                signed_state(&issuer, CLIENT_ID, Keypair::random().public_key(), full),
                "grant is not bound to the Lock Server key",
            ),
            (
                signed_state(&issuer, CLIENT_ID, pop.clone(), "/pub/locks.app/:rw"),
                "grant does not cover the requested scopes",
            ),
            (
                signed_state(
                    &issuer,
                    CLIENT_ID,
                    pop.clone(),
                    "/priv/locks.app/:r,/pub/locks.app/:rw",
                ),
                "grant does not cover the requested scopes",
            ),
            (
                signed_state(
                    &issuer,
                    CLIENT_ID,
                    pop.clone(),
                    "/priv/locks.app-evil/:rw,/pub/locks.app/:rw",
                ),
                "grant does not cover the requested scopes",
            ),
        ] {
            assert_eq!(
                approval(state, &issuer.public_key()).unwrap_err(),
                ApplicationError::CreatorAuthoritySecret {
                    message: message.to_owned()
                }
            );
        }
    }

    #[test]
    fn grant_approval_rejects_expired_grant() {
        let issuer = Keypair::random();
        let claims = GrantClaims {
            iss: issuer.public_key(),
            client_id: ClientId::new(CLIENT_ID).unwrap(),
            caps: ["/priv/locks.app/:rw", "/pub/locks.app/:rw"]
                .into_iter()
                .map(|cap| Capability::from_str(cap).unwrap())
                .collect(),
            cnf: keys().public_key(&key_id()),
            jti: GrantId::generate(),
            iat: 1,
            exp: 2,
        };
        let state = DelegatedGrantCredentialState {
            grant_jws: claims.sign(&issuer, GRANT_JWS_TYP),
            homeserver_pk: Keypair::random().public_key(),
            key_id: key_id().as_str().to_owned(),
            client_pk: keys().public_key(&key_id()),
        };
        assert_eq!(
            approval(state, &issuer.public_key()).unwrap_err(),
            ApplicationError::CreatorAuthoritySecret {
                message: "grant is expired".to_owned()
            }
        );
    }

    #[tokio::test]
    async fn grant_and_legacy_connect_request_identical_capabilities() {
        let relay: Url = "http://localhost:15412/inbox/".parse().unwrap();
        let legacy = PubkyLegacyCreatorConnectFlowClient::new_with_auth_relay(
            pubky::Pubky::testnet().unwrap(),
            relay.clone(),
        );
        let grant = PubkyGrantCreatorConnectFlowClient::new(
            pubky::PubkyHttpClient::testnet().unwrap(),
            Some(relay),
            ClientId::new(CLIENT_ID).unwrap(),
            keys(),
        );
        let scopes = vec![
            "/pub/locks.app/:rw".to_owned(),
            "/priv/locks.app/:rw".to_owned(),
        ];

        let legacy_url = Url::parse(
            legacy
                .start_legacy_creator_connect_flow(&scopes)
                .await
                .unwrap()
                .expose_url(),
        )
        .unwrap();
        let grant_url = Url::parse(
            grant
                .start_grant_creator_connect_flow(&scopes, &key_id())
                .await
                .unwrap()
                .expose_url(),
        )
        .unwrap();
        let param = |url: &Url, name: &str| {
            url.query_pairs()
                .find_map(|(key, value)| (key == name).then(|| value.into_owned()))
        };

        assert_eq!(grant_url.scheme(), "pubkyauth");
        assert_eq!(grant_url.host_str(), Some("signin_grant"));
        assert_eq!(param(&grant_url, "caps"), param(&legacy_url, "caps"));
        assert_eq!(param(&grant_url, "relay"), param(&legacy_url, "relay"));
        assert_eq!(param(&grant_url, "cid").as_deref(), Some(CLIENT_ID));
        assert_eq!(
            param(&grant_url, "cpk"),
            Some(keys().public_key(&key_id()).z32())
        );
    }

    #[tokio::test]
    async fn restore_refuses_state_bound_to_another_key_before_network() {
        let state = signed_state(
            &Keypair::random(),
            CLIENT_ID,
            Keypair::random().public_key(),
            "/priv/locks.app/:rw,/pub/locks.app/:rw",
        );

        let error = restore_grant_session(
            &pubky::PubkyHttpClient::testnet().unwrap(),
            &keys(),
            &ClientId::new(CLIENT_ID).unwrap(),
            &[
                "/priv/locks.app/:rw".to_owned(),
                "/pub/locks.app/:rw".to_owned(),
            ],
            &encode_grant_state(&state),
        )
        .await
        .unwrap_err();

        assert_eq!(
            error,
            ApplicationError::CreatorAuthoritySecret {
                message: "failed to restore grant creator authority".to_owned()
            }
        );
    }

    #[tokio::test]
    async fn restore_refuses_foreign_client_and_narrow_scopes_before_network() {
        let issuer = Keypair::random();
        let expected_scopes = [
            "/priv/locks.app/:rw".to_owned(),
            "/pub/locks.app/:rw".to_owned(),
        ];

        for state in [
            signed_state(
                &issuer,
                "evil.example",
                keys().public_key(&key_id()),
                "/priv/locks.app/:rw,/pub/locks.app/:rw",
            ),
            signed_state(
                &issuer,
                CLIENT_ID,
                keys().public_key(&key_id()),
                "/pub/locks.app/:rw",
            ),
        ] {
            let error = restore_grant_session(
                &pubky::PubkyHttpClient::testnet().unwrap(),
                &keys(),
                &ClientId::new(CLIENT_ID).unwrap(),
                &expected_scopes,
                &encode_grant_state(&state),
            )
            .await
            .unwrap_err();

            assert_eq!(
                error,
                ApplicationError::CreatorAuthoritySecret {
                    message: "failed to restore grant creator authority".to_owned()
                }
            );
        }
    }
}
