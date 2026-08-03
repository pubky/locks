use time::{Duration, OffsetDateTime};

use crate::application::errors::ApplicationError;
use crate::application::models::{
    FrontendSessionCode, FrontendSessionRecord, FrontendSessionToken,
};
use crate::application::ports::{
    Clock, FrontendSessionCodeStore, FrontendSessionStore, FrontendSessionTokenGenerator,
};

const FRONTEND_SESSION_TTL: Duration = Duration::hours(24);

/// Request to exchange a one-time code for a frontend session token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExchangeFrontendSessionCodeRequest {
    /// One-time code returned by the completed creator connect flow.
    pub code: FrontendSessionCode,
    /// Opaque state expected to match the original connect flow.
    pub state: String,
}

/// Response containing the raw frontend session bearer token exactly once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExchangeFrontendSessionCodeResponse {
    /// Raw frontend session bearer token. Debug output redacts the token.
    pub session_token: FrontendSessionToken,
    /// Creator represented by the frontend session.
    pub creator: locks_core::ids::CreatorPubky,
    /// Expiration timestamp for the frontend session.
    pub expires_at: OffsetDateTime,
}

/// Exchanges a one-time frontend session code for a Locks-local frontend session token.
pub async fn exchange_frontend_session_code(
    code_store: &dyn FrontendSessionCodeStore,
    session_store: &dyn FrontendSessionStore,
    token_generator: &dyn FrontendSessionTokenGenerator,
    clock: &dyn Clock,
    request: ExchangeFrontendSessionCodeRequest,
) -> Result<ExchangeFrontendSessionCodeResponse, ApplicationError> {
    let now = clock.now();
    let code_record = code_store
        .consume_frontend_session_code(&request.code, now)
        .await?
        .ok_or(ApplicationError::FrontendSessionCodeUnavailable)?;

    if code_record.consumed_at.is_some() {
        return Err(ApplicationError::FrontendSessionCodeAlreadyConsumed);
    }
    if code_record.is_expired_at(now) {
        return Err(ApplicationError::FrontendSessionCodeExpired);
    }
    if code_record.state != request.state {
        return Err(ApplicationError::FrontendSessionStateMismatch);
    }

    let session_token = token_generator.generate_frontend_session_token();
    let expires_at = now + FRONTEND_SESSION_TTL;
    session_store
        .insert_frontend_session(FrontendSessionRecord {
            token: session_token.clone(),
            creator: code_record.creator.clone(),
            created_at: now,
            expires_at,
        })
        .await?;

    Ok(ExchangeFrontendSessionCodeResponse {
        session_token,
        creator: code_record.creator,
        expires_at,
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
        FrontendSessionCode, FrontendSessionCodeRecord, FrontendSessionRecord, FrontendSessionToken,
    };
    use crate::application::ports::{
        Clock, FrontendSessionCodeStore, FrontendSessionStore, FrontendSessionTokenGenerator,
    };
    use crate::application::use_cases::exchange_frontend_session_code::{
        ExchangeFrontendSessionCodeRequest, exchange_frontend_session_code,
    };

    #[tokio::test]
    async fn exchange_frontend_session_code_consumes_code_creates_session_and_returns_raw_token_once()
     {
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        let code_store = CodeStore::with_record(code_record(now));
        let session_store = SessionStore::default();
        let token_generator = FixedTokenGenerator;
        let clock = FixedClock(now);

        let response = exchange_frontend_session_code(
            &code_store,
            &session_store,
            &token_generator,
            &clock,
            ExchangeFrontendSessionCodeRequest {
                code: FrontendSessionCode::new("one-time-code"),
                state: "opaque-state".to_owned(),
            },
        )
        .await
        .unwrap();

        assert_eq!(
            response.session_token.expose_token(),
            "frontend-session-token"
        );
        assert_eq!(response.creator, creator());
        assert!(response.expires_at > now);
        assert!(!format!("{response:?}").contains("frontend-session-token"));

        let stored = session_store.record().expect("frontend session stored");
        assert_eq!(stored.token.expose_token(), "frontend-session-token");
        assert_eq!(stored.creator, creator());
        assert_eq!(stored.created_at, now);
        assert_eq!(stored.expires_at, response.expires_at);
        assert!(stored.expires_at > now);
    }

    #[tokio::test]
    async fn exchange_frontend_session_code_rejects_missing_expired_consumed_and_state_mismatch() {
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();

        let missing = exchange_frontend_session_code(
            &CodeStore::default(),
            &SessionStore::default(),
            &FixedTokenGenerator,
            &FixedClock(now),
            request("one-time-code", "opaque-state"),
        )
        .await
        .unwrap_err();
        assert_eq!(missing, ApplicationError::FrontendSessionCodeUnavailable);

        let expired = exchange_frontend_session_code(
            &CodeStore::with_record(FrontendSessionCodeRecord {
                expires_at: now,
                ..code_record(now - Duration::minutes(10))
            }),
            &SessionStore::default(),
            &FixedTokenGenerator,
            &FixedClock(now),
            request("one-time-code", "opaque-state"),
        )
        .await
        .unwrap_err();
        assert_eq!(expired, ApplicationError::FrontendSessionCodeExpired);

        let consumed = exchange_frontend_session_code(
            &CodeStore::with_record(FrontendSessionCodeRecord {
                consumed_at: Some(now),
                ..code_record(now)
            }),
            &SessionStore::default(),
            &FixedTokenGenerator,
            &FixedClock(now),
            request("one-time-code", "opaque-state"),
        )
        .await
        .unwrap_err();
        assert_eq!(
            consumed,
            ApplicationError::FrontendSessionCodeAlreadyConsumed
        );

        let mismatch = exchange_frontend_session_code(
            &CodeStore::with_record(code_record(now)),
            &SessionStore::default(),
            &FixedTokenGenerator,
            &FixedClock(now),
            request("one-time-code", "wrong-state"),
        )
        .await
        .unwrap_err();
        assert_eq!(mismatch, ApplicationError::FrontendSessionStateMismatch);
    }

    fn request(code: &str, state: &str) -> ExchangeFrontendSessionCodeRequest {
        ExchangeFrontendSessionCodeRequest {
            code: FrontendSessionCode::new(code),
            state: state.to_owned(),
        }
    }

    fn code_record(now: OffsetDateTime) -> FrontendSessionCodeRecord {
        FrontendSessionCodeRecord {
            code: FrontendSessionCode::new("one-time-code"),
            creator: creator(),
            state: "opaque-state".to_owned(),
            return_to: "https://pubky.app/locks/connected".to_owned(),
            created_at: now,
            expires_at: now + Duration::minutes(5),
            consumed_at: None,
        }
    }

    fn creator() -> CreatorPubky {
        CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
    }

    #[derive(Default)]
    struct CodeStore {
        record: Mutex<Option<FrontendSessionCodeRecord>>,
    }

    impl CodeStore {
        fn with_record(record: FrontendSessionCodeRecord) -> Self {
            Self {
                record: Mutex::new(Some(record)),
            }
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
            now: OffsetDateTime,
        ) -> Result<Option<FrontendSessionCodeRecord>, ApplicationError> {
            let mut guard = self.record.lock().unwrap();
            let record = guard.clone();
            if let Some(stored) = guard.as_mut() {
                stored.consumed_at = Some(now);
            }
            Ok(record)
        }
    }

    #[derive(Default)]
    struct SessionStore {
        record: Mutex<Option<FrontendSessionRecord>>,
    }

    impl SessionStore {
        fn record(&self) -> Option<FrontendSessionRecord> {
            self.record.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl FrontendSessionStore for SessionStore {
        async fn insert_frontend_session(
            &self,
            record: FrontendSessionRecord,
        ) -> Result<(), ApplicationError> {
            *self.record.lock().unwrap() = Some(record);
            Ok(())
        }

        async fn get_frontend_session(
            &self,
            _token: &FrontendSessionToken,
        ) -> Result<Option<FrontendSessionRecord>, ApplicationError> {
            Ok(self.record())
        }

        async fn delete_frontend_session(
            &self,
            _token: &FrontendSessionToken,
        ) -> Result<(), ApplicationError> {
            *self.record.lock().unwrap() = None;
            Ok(())
        }
    }

    struct FixedTokenGenerator;

    impl FrontendSessionTokenGenerator for FixedTokenGenerator {
        fn generate_frontend_session_token(&self) -> FrontendSessionToken {
            FrontendSessionToken::new("frontend-session-token")
        }
    }

    struct FixedClock(OffsetDateTime);

    impl Clock for FixedClock {
        fn now(&self) -> OffsetDateTime {
            self.0
        }
    }
}
