use crate::creator::CreatorLocks;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocksSession {
    secret: String,
}

impl LocksSession {
    pub fn new(secret: impl Into<String>) -> Self {
        Self {
            secret: secret.into(),
        }
    }

    pub fn export_secret(&self) -> &str {
        &self.secret
    }

    pub(crate) fn authorization_header_value(&self) -> String {
        format!("Bearer {}", self.secret)
    }

    pub fn creator(&self) -> CreatorLocks {
        CreatorLocks::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use locks_core::ids::LockServerPubky;

    use crate::client::LocksClient;

    use super::*;

    #[test]
    fn session_export_secret_returns_original_bearer_equivalent_secret() {
        let session = LocksSession::new("frontend-session-secret");

        assert_eq!(session.export_secret(), "frontend-session-secret");
    }

    #[test]
    fn client_restore_session_creates_session_for_original_secret() {
        let client = LocksClient::for_server(lock_server());

        let session = client.restore_session("frontend-session-secret");

        assert_eq!(session.export_secret(), "frontend-session-secret");
    }

    #[test]
    fn restored_session_builds_bearer_authorization_header_internally() {
        let client = LocksClient::for_server(lock_server());
        let session = client.restore_session("frontend-session-secret");

        assert_eq!(
            session.authorization_header_value(),
            "Bearer frontend-session-secret"
        );
    }

    fn lock_server() -> LockServerPubky {
        LockServerPubky::from_str("pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo")
            .unwrap()
    }
}
