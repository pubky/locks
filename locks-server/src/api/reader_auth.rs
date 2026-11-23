use axum::Json;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, header};
use locks_core::ids::CreatorPubky;
use locks_service::application::models::{
    READER_AUTH_CHALLENGE_TTL_SECONDS, READER_AUTH_POP_TYPE, READER_SESSION_IDLE_TTL_SECONDS,
    READER_SESSION_PURPOSE, ReaderAuthChallengeHash, ReaderSessionRecord, ReaderSessionToken,
    ReaderSessionTokenHash,
};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::SocketAddr;
use std::str::FromStr;
use time::OffsetDateTime;
use uuid::Uuid;

use locks_service::application::ports::{
    CreateReaderAuthChallenge, ExchangeReaderSession, ReaderSessionTokenGenerator,
};

use crate::api::errors::{ApiError, ApiErrorCode};
use crate::app_state::AppState;
use crate::rate_limit::{ReaderAuthRateLimitKey, client_ip};

// ---------------------------------------------------------------------------
// DTOs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateChallengeHttpRequest {}

#[derive(Clone, PartialEq, Eq, Serialize)]
pub(crate) struct CreateChallengeHttpResponse {
    pub challenge_id: String,
    pub challenge: String,
    pub lock_server_pubky: String,
    pub lock_server_origin: String,
    pub purpose: &'static str,
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: OffsetDateTime,
}

impl fmt::Debug for CreateChallengeHttpResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreateChallengeHttpResponse")
            .field("challenge_id", &self.challenge_id)
            .field("challenge", &"<redacted>")
            .field("lock_server_pubky", &self.lock_server_pubky)
            .field("lock_server_origin", &self.lock_server_origin)
            .field("purpose", &self.purpose)
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateSessionHttpRequest {
    pub challenge_id: String,
    pub credentials: CustomPopCredentials,
}

impl fmt::Debug for CreateSessionHttpRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreateSessionHttpRequest")
            .field("challenge_id", &self.challenge_id)
            .field("credentials", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CustomPopCredentials {
    pub grant: String,
    pub pop: String,
}

#[derive(Clone, PartialEq, Eq, Serialize)]
pub(crate) struct CreateSessionHttpResponse {
    pub reader: CreatorPubky,
    pub session_token: String,
    #[serde(with = "time::serde::rfc3339")]
    pub idle_expires_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: OffsetDateTime,
}

impl fmt::Debug for CreateSessionHttpResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreateSessionHttpResponse")
            .field("reader", &self.reader)
            .field("session_token", &"<redacted>")
            .field("idle_expires_at", &self.idle_expires_at)
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// POST /reader-auth/challenges — strict `{}` body, browser-only exact Origin.
pub(super) async fn create_reader_auth_challenge(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(_body): Json<CreateChallengeHttpRequest>,
) -> Result<Json<CreateChallengeHttpResponse>, ApiError> {
    let lock_server_origin = state.canonical_lock_server_origin().ok_or_else(|| {
        ApiError::new(
            ApiErrorCode::InvalidRequest,
            "reader auth not configured on this lock server",
        )
    })?;

    let browser_origin = extract_allowed_reader_origin(&state, &headers)?;
    enforce_reader_auth_rate_limit(&state, peer, &headers, &browser_origin)?;

    let challenge_id = Uuid::new_v4();
    let challenge_bytes: [u8; 32] = rand::random();
    let challenge_b64 = base64_url_encode(&challenge_bytes);
    let challenge_hash = ReaderAuthChallengeHash::derive(&challenge_bytes);
    let reader_auth = state.reader_auth().ok_or_else(|| {
        ApiError::new(
            ApiErrorCode::InvalidRequest,
            "reader auth not configured on this lock server",
        )
    })?;
    reader_auth.purge_expired().await.map_err(ApiError::from)?;
    let record = reader_auth
        .create_challenge(CreateReaderAuthChallenge {
            challenge_id,
            challenge_hash,
            browser_origin: browser_origin.clone(),
            ttl_seconds: READER_AUTH_CHALLENGE_TTL_SECONDS,
        })
        .await
        .map_err(ApiError::from)?;

    Ok(Json(CreateChallengeHttpResponse {
        challenge_id: challenge_id.to_string(),
        challenge: challenge_b64,
        lock_server_pubky: state
            .config()
            .credentials
            .lock_server_public_key
            .to_string(),
        lock_server_origin: lock_server_origin.to_owned(),
        purpose: READER_SESSION_PURPOSE,
        expires_at: record.expires_at,
    }))
}

/// POST /reader-sessions — exchange challenge + custom PoP for session token.
pub(super) async fn create_reader_session(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(body): Json<CreateSessionHttpRequest>,
) -> Result<Json<CreateSessionHttpResponse>, ApiError> {
    let lock_server_origin = state.canonical_lock_server_origin().ok_or_else(|| {
        ApiError::new(
            ApiErrorCode::InvalidRequest,
            "reader auth not configured on this lock server",
        )
    })?;

    let browser_origin = extract_allowed_reader_origin(&state, &headers)?;
    enforce_reader_auth_rate_limit(&state, peer, &headers, &browser_origin)?;
    let _permit = state
        .reader_auth_semaphore()
        .try_acquire()
        .map_err(|_| ApiError::new(ApiErrorCode::RateLimited, "rate limit exceeded"))?;

    if body.challenge_id.len() > 64
        || body.credentials.grant.len() > 8 * 1024
        || body.credentials.pop.len() > 8 * 1024
    {
        return Err(ApiError::new(
            ApiErrorCode::InvalidRequest,
            "invalid request",
        ));
    }

    // Parse challenge_id.
    let challenge_id = Uuid::from_str(&body.challenge_id)
        .map_err(|_| ApiError::new(ApiErrorCode::InvalidRequest, "invalid challenge_id format"))?;

    // Verify custom PoP.
    let custom_pop = pubky::custom_pop::CustomPop {
        grant: body.credentials.grant.clone(),
        pop: body.credentials.pop.clone(),
    };

    let verified = pubky::custom_pop::verify_custom_grant_pop(
        &custom_pop,
        pubky::custom_pop::DEFAULT_CUSTOM_POP_CLOCK_SKEW,
    )
    .map_err(|_| {
        ApiError::new(
            ApiErrorCode::ReaderAuthPopVerificationFailed,
            "reader authentication proof verification failed",
        )
    })?;

    // Validate strict closed PoP data schema.
    let pop_data = verified.data();
    validate_pop_data(
        pop_data,
        &state
            .config()
            .credentials
            .lock_server_public_key
            .to_string(),
        lock_server_origin,
        &body.challenge_id,
        &browser_origin,
    )?;

    // Extract challenge from PoP data to verify against stored hash.
    let pop_challenge_b64 = pop_data
        .get("challenge")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            ApiError::new(
                ApiErrorCode::InvalidRequest,
                "missing challenge in PoP data",
            )
        })?;
    let challenge_bytes = base64_url_decode(pop_challenge_b64).map_err(|_| {
        ApiError::new(
            ApiErrorCode::InvalidRequest,
            "invalid challenge encoding in PoP data",
        )
    })?;
    let challenge_hash = ReaderAuthChallengeHash::derive(&challenge_bytes);

    // Check every signed/stored boundary before the atomic consume+insert.
    let reader_auth = state.reader_auth().ok_or_else(|| {
        ApiError::new(
            ApiErrorCode::InvalidRequest,
            "reader auth not configured on this lock server",
        )
    })?;
    let challenge = reader_auth
        .get_challenge(challenge_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| {
            ApiError::new(
                ApiErrorCode::ReaderAuthChallengeUnavailable,
                "reader auth challenge unavailable",
            )
        })?;
    if challenge.browser_origin != browser_origin || challenge.challenge_hash != challenge_hash {
        return Err(ApiError::new(
            ApiErrorCode::ReaderAuthPopVerificationFailed,
            "reader authentication proof verification failed",
        ));
    }
    let proof_iat = i64::try_from(verified.iat()).map_err(|_| {
        ApiError::new(
            ApiErrorCode::ReaderAuthPopVerificationFailed,
            "reader authentication proof verification failed",
        )
    })?;
    if proof_iat < challenge.created_at.unix_timestamp() - 30
        || proof_iat > challenge.expires_at.unix_timestamp() + 30
    {
        return Err(ApiError::new(
            ApiErrorCode::ReaderAuthPopVerificationFailed,
            "reader authentication proof verification failed",
        ));
    }

    // Derive reader from verified grant issuer.
    let reader_pubkey = verified.identity();
    let reader = CreatorPubky::from_str(&reader_pubkey.to_string()).map_err(|_| {
        ApiError::new(
            ApiErrorCode::InvalidRequest,
            "reader identity not a valid pubky",
        )
    })?;

    // An optional predecessor is validated and rotated inside the atomic exchange.
    let previous_token_hash = parse_optional_reader_bearer(&headers)?;

    // Compute session times.
    let grant_expires_at = {
        let exp = verified.grant_claims().exp;
        let exp = i64::try_from(exp).map_err(|_| {
            ApiError::new(
                ApiErrorCode::InvalidRequest,
                "invalid grant expiration timestamp",
            )
        })?;
        OffsetDateTime::from_unix_timestamp(exp).map_err(|_| {
            ApiError::new(
                ApiErrorCode::InvalidRequest,
                "invalid grant expiration timestamp",
            )
        })?
    };

    // Generate session token.
    let session_token = state
        .reader_session_token_generator()
        .generate_reader_session_token();
    let token_hash = ReaderSessionTokenHash::derive(&session_token);

    let session_id = Uuid::new_v4();
    let record = ReaderSessionRecord {
        session_id,
        reader: reader.clone(),
        token_hash,
        challenge_id,
        grant_id: verified.grant_claims().jti.to_string(),
        grant_client_id: verified.grant_claims().client_id.to_string(),
        proof_nonce: verified.nonce().to_string(),
        lock_server_pubky: state
            .config()
            .credentials
            .lock_server_public_key
            .to_string(),
        lock_server_origin: lock_server_origin.to_owned(),
        browser_origin: browser_origin.clone(),
        grant_expires_at,
        // Storage initializes these atomically from its authoritative clock.
        idle_expires_at: OffsetDateTime::UNIX_EPOCH,
        absolute_expires_at: OffsetDateTime::UNIX_EPOCH,
        created_at: OffsetDateTime::UNIX_EPOCH,
        renewed_from_session_id: None,
        invalidated_at: None,
    };

    let stored_session = reader_auth
        .exchange_session(ExchangeReaderSession {
            challenge_id,
            challenge_hash,
            browser_origin,
            previous_token_hash,
            session: record,
        })
        .await
        .map_err(ApiError::from)?;

    Ok(Json(CreateSessionHttpResponse {
        reader,
        session_token: session_token.expose_token().to_owned(),
        idle_expires_at: stored_session.idle_expires_at,
        expires_at: stored_session.absolute_expires_at,
    }))
}

/// DELETE /reader-sessions/current — invalidate the current reader session.
pub(super) async fn delete_reader_session(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<axum::http::StatusCode, ApiError> {
    let browser_origin = extract_allowed_reader_origin(&state, &headers)?;
    let token = parse_reader_bearer_token(&headers)?;
    let token_hash = ReaderSessionTokenHash::derive(&token);

    let session_store = state.reader_auth().ok_or_else(|| {
        ApiError::new(
            ApiErrorCode::ReaderSessionUnavailable,
            "reader session unavailable",
        )
    })?;

    let invalidated = session_store
        .invalidate_session(&token_hash, &browser_origin)
        .await
        .map_err(ApiError::from)?;

    if !invalidated {
        return Err(ApiError::new(
            ApiErrorCode::ReaderSessionUnavailable,
            "reader session unavailable",
        ));
    }

    Ok(axum::http::StatusCode::NO_CONTENT)
}

// ---------------------------------------------------------------------------
// Reader bearer extractor (for protecting routes)
// ---------------------------------------------------------------------------

/// Extracts and validates a reader session from the Authorization header.
/// Returns `Ok(None)` when no bearer token is present.
/// Returns `Err` for invalid/expired/invalidated tokens.
pub(crate) async fn extract_optional_reader_session(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<Option<ReaderSessionRecord>, ApiError> {
    let token = match try_parse_reader_bearer_token(headers) {
        Some(token) => token,
        None if headers.contains_key(header::AUTHORIZATION) => {
            return Err(ApiError::new(
                ApiErrorCode::ReaderSessionUnavailable,
                "reader session unavailable",
            ));
        }
        None => return Ok(None),
    };
    let token_hash = ReaderSessionTokenHash::derive(&token);
    let browser_origin = extract_allowed_reader_origin(state, headers)?;

    let session_store = match state.reader_auth() {
        Some(store) => store,
        None => {
            // Reader auth not configured, but a bearer was presented → never downgrade.
            return Err(ApiError::new(
                ApiErrorCode::ReaderSessionUnavailable,
                "reader session unavailable",
            ));
        }
    };

    let session = session_store
        .validate_and_touch(
            &token_hash,
            &browser_origin,
            READER_SESSION_IDLE_TTL_SECONDS,
        )
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| {
            ApiError::new(
                ApiErrorCode::ReaderSessionUnavailable,
                "reader session unavailable",
            )
        })?;

    Ok(Some(session))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn extract_single_origin(headers: &HeaderMap) -> Result<String, ApiError> {
    let invalid = || {
        ApiError::new(
            ApiErrorCode::InvalidRequest,
            "missing or invalid Origin header",
        )
    };
    let mut origins = headers.get_all(header::ORIGIN).iter();
    let origin = origins.next().ok_or_else(invalid)?;
    if origins.next().is_some() {
        return Err(invalid()); // duplicate
    }
    let origin = origin.to_str().map_err(|_| invalid())?;
    if origin.is_empty() || origin == "null" {
        return Err(invalid());
    }
    Ok(origin.to_owned())
}

pub(crate) fn extract_allowed_reader_origin(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<String, ApiError> {
    let origin = extract_single_origin(headers)?;
    let allowed = crate::config::canonical_grant_origin(&origin).as_deref() == Some(&origin)
        && state
            .config()
            .reader_auth
            .allowed_origins
            .iter()
            .any(|allowed| allowed == &origin);
    if !allowed {
        return Err(ApiError::new(
            ApiErrorCode::ReaderSessionUnavailable,
            "reader session unavailable",
        ));
    }
    Ok(origin)
}

fn parse_reader_bearer_token(headers: &HeaderMap) -> Result<ReaderSessionToken, ApiError> {
    try_parse_reader_bearer_token(headers).ok_or_else(|| {
        ApiError::new(
            ApiErrorCode::ReaderSessionUnavailable,
            "reader session unavailable",
        )
    })
}

fn try_parse_reader_bearer_token(headers: &HeaderMap) -> Option<ReaderSessionToken> {
    let mut values = headers.get_all(header::AUTHORIZATION).iter();
    let value = values.next()?;
    if values.next().is_some() {
        return None; // duplicate
    }
    let value = value.to_str().ok()?;
    let mut parts = value.split(' ');
    let scheme = parts.next()?;
    let token = parts.next()?;
    if parts.next().is_some() || !scheme.eq_ignore_ascii_case("Bearer") || token.is_empty() {
        return None;
    }
    Some(ReaderSessionToken::new(token.to_owned()))
}

/// Parses an optional Reader predecessor token for atomic renewal.
fn parse_optional_reader_bearer(
    headers: &HeaderMap,
) -> Result<Option<ReaderSessionTokenHash>, ApiError> {
    let token = match try_parse_reader_bearer_token(headers) {
        Some(token) => token,
        None if headers.contains_key(header::AUTHORIZATION) => {
            return Err(ApiError::new(
                ApiErrorCode::ReaderSessionUnavailable,
                "reader session unavailable",
            ));
        }
        None => return Ok(None),
    };
    Ok(Some(ReaderSessionTokenHash::derive(&token)))
}

fn validate_pop_data(
    data: &serde_json::Value,
    lock_server_pubky: &str,
    lock_server_origin: &str,
    challenge_id: &str,
    browser_origin: &str,
) -> Result<(), ApiError> {
    let obj = data.as_object().ok_or_else(|| {
        ApiError::new(
            ApiErrorCode::ReaderAuthPopVerificationFailed,
            "PoP data must be a JSON object",
        )
    })?;

    // Strict closed schema: exactly these 6 keys.
    let expected_keys: &[&str] = &[
        "browser_origin",
        "challenge",
        "challenge_id",
        "lock_server_origin",
        "lock_server_pubky",
        "type",
    ];
    let mut actual_keys: Vec<&str> = obj.keys().map(String::as_str).collect();
    actual_keys.sort();
    if actual_keys != expected_keys {
        return Err(ApiError::new(
            ApiErrorCode::ReaderAuthPopVerificationFailed,
            "PoP data has invalid schema: unexpected or missing fields",
        ));
    }

    let check = |key: &str, expected: &str| -> Result<(), ApiError> {
        let val = obj.get(key).and_then(|v| v.as_str()).ok_or_else(|| {
            ApiError::new(
                ApiErrorCode::ReaderAuthPopVerificationFailed,
                format!("PoP data missing or non-string field: {key}"),
            )
        })?;
        if val != expected {
            return Err(ApiError::new(
                ApiErrorCode::ReaderAuthPopVerificationFailed,
                format!("PoP data field {key} mismatch"),
            ));
        }
        Ok(())
    };

    check("type", READER_AUTH_POP_TYPE)?;
    check("lock_server_pubky", lock_server_pubky)?;
    check("lock_server_origin", lock_server_origin)?;
    check("challenge_id", challenge_id)?;
    check("browser_origin", browser_origin)?;
    // "challenge" is validated later against stored hash.

    Ok(())
}

fn base64_url_encode(data: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(data)
}

fn enforce_reader_auth_rate_limit(
    state: &AppState,
    peer: SocketAddr,
    headers: &HeaderMap,
    origin: &str,
) -> Result<(), ApiError> {
    let key = ReaderAuthRateLimitKey {
        client_ip: client_ip(peer, headers, state.config().rate_limits.trusted_proxy_hops),
        origin: origin.to_owned(),
    };
    if state
        .reader_auth_rate_limiter()
        .check(&key, state.clock().now())
        .allowed
    {
        Ok(())
    } else {
        Err(ApiError::new(
            ApiErrorCode::RateLimited,
            "rate limit exceeded",
        ))
    }
}

fn base64_url_decode(data: &str) -> Result<Vec<u8>, base64::DecodeError> {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use time::macros::datetime;

    #[test]
    fn create_session_response_debug_redacts_session_token() {
        let response = CreateSessionHttpResponse {
            reader: CreatorPubky::from_str(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            )
            .unwrap(),
            session_token: "live-reader-session-secret".to_owned(),
            idle_expires_at: datetime!(2026-11-01 12:15:00 UTC),
            expires_at: datetime!(2026-11-01 13:00:00 UTC),
        };

        let debug = format!("{response:?}");

        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("live-reader-session-secret"));
    }
}
