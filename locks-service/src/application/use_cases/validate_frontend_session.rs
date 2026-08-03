use time::OffsetDateTime;

use locks_core::ids::CreatorPubky;

use crate::application::errors::ApplicationError;
use crate::application::models::FrontendSessionToken;
use crate::application::ports::{Clock, FrontendSessionStore};

/// Request to validate a Locks-local frontend session token for creator APIs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidateFrontendSessionRequest {
    /// Raw frontend session bearer token supplied by pubky.app/browser code.
    pub session_token: FrontendSessionToken,
}

/// Secret-free validated frontend session context for creator APIs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidateFrontendSessionResponse {
    /// Creator identity represented by the frontend session.
    pub creator: CreatorPubky,
    /// Session expiration timestamp.
    pub expires_at: OffsetDateTime,
}

/// Validates a Locks-local frontend session token and derives the creator context.
pub async fn validate_frontend_session(
    store: &dyn FrontendSessionStore,
    clock: &dyn Clock,
    request: ValidateFrontendSessionRequest,
) -> Result<ValidateFrontendSessionResponse, ApplicationError> {
    let now = clock.now();
    let Some(record) = store.get_frontend_session(&request.session_token).await? else {
        return Err(ApplicationError::FrontendSessionUnavailable);
    };

    if record.is_expired_at(now) {
        return Err(ApplicationError::FrontendSessionExpired);
    }

    Ok(ValidateFrontendSessionResponse {
        creator: record.creator,
        expires_at: record.expires_at,
    })
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use locks_core::ids::CreatorPubky;
    use time::{Duration, OffsetDateTime};

    use super::{ValidateFrontendSessionRequest, validate_frontend_session};
    use crate::application::errors::ApplicationError;
    use crate::application::models::{FrontendSessionRecord, FrontendSessionToken};
    use crate::application::ports::{Clock, FrontendSessionStore};

    #[tokio::test]
    async fn validate_frontend_session_returns_creator_without_exposing_token() {
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        let store = SessionStore::with_record(session_record(now));
        let token = FrontendSessionToken::new("frontend-session-token");

        let response = validate_frontend_session(
            &store,
            &FixedClock(now),
            ValidateFrontendSessionRequest {
                session_token: token,
            },
        )
        .await
        .unwrap();

        assert_eq!(response.creator, creator());
        assert_eq!(response.expires_at, now + Duration::hours(12));
        assert!(!format!("{response:?}").contains("frontend-session-token"));
    }

    #[tokio::test]
    async fn validate_frontend_session_rejects_missing_and_expired_tokens() {
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();

        let missing = validate_frontend_session(
            &SessionStore::default(),
            &FixedClock(now),
            ValidateFrontendSessionRequest {
                session_token: FrontendSessionToken::new("missing-token"),
            },
        )
        .await
        .unwrap_err();
        assert_eq!(missing, ApplicationError::FrontendSessionUnavailable);

        let expired = validate_frontend_session(
            &SessionStore::with_record(FrontendSessionRecord {
                expires_at: now,
                ..session_record(now - Duration::hours(12))
            }),
            &FixedClock(now),
            ValidateFrontendSessionRequest {
                session_token: FrontendSessionToken::new("expired-token"),
            },
        )
        .await
        .unwrap_err();
        assert_eq!(expired, ApplicationError::FrontendSessionExpired);
    }

    fn session_record(now: OffsetDateTime) -> FrontendSessionRecord {
        FrontendSessionRecord {
            token: FrontendSessionToken::new("frontend-session-token"),
            creator: creator(),
            created_at: now,
            expires_at: now + Duration::hours(12),
        }
    }

    fn creator() -> CreatorPubky {
        CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
    }

    #[derive(Debug, Default)]
    struct SessionStore {
        record: Mutex<Option<FrontendSessionRecord>>,
    }

    impl SessionStore {
        fn with_record(record: FrontendSessionRecord) -> Self {
            Self {
                record: Mutex::new(Some(record)),
            }
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
            Ok(self.record.lock().unwrap().clone())
        }

        async fn delete_frontend_session(
            &self,
            _token: &FrontendSessionToken,
        ) -> Result<(), ApplicationError> {
            *self.record.lock().unwrap() = None;
            Ok(())
        }
    }

    #[derive(Debug, Clone, Copy)]
    struct FixedClock(OffsetDateTime);

    impl Clock for FixedClock {
        fn now(&self) -> OffsetDateTime {
            self.0
        }
    }
}
