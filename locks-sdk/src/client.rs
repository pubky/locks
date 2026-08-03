use locks_core::ids::LockServerPubky;
use url::Url;

use crate::discovery::CreatorLockServicePointer;
use crate::error::{LocksSdkError, Result};
use crate::session::LocksSession;

#[derive(Debug, Clone)]
pub struct LocksClient {
    lock_server: LockServerPubky,
}

impl LocksClient {
    pub fn for_server(lock_server: LockServerPubky) -> Self {
        Self { lock_server }
    }

    pub fn for_creator_pointer(pointer: CreatorLockServicePointer) -> Self {
        Self::for_server(pointer.into_inner().default_lock_server)
    }

    pub fn lock_server(&self) -> &LockServerPubky {
        &self.lock_server
    }

    pub fn transport_url(&self, path_and_query: &str) -> Result<Url> {
        if !path_and_query.starts_with('/') {
            return Err(LocksSdkError::InvalidTransportUrl);
        }

        Url::parse(&format!(
            "https://_pubky.{}{}",
            raw_pubky_z32(&self.lock_server),
            path_and_query
        ))
        .map_err(|_| LocksSdkError::InvalidTransportUrl)
    }

    pub fn restore_session(&self, secret: impl Into<String>) -> LocksSession {
        LocksSession::new(secret)
    }
}

fn raw_pubky_z32(lock_server: &LockServerPubky) -> String {
    let value = lock_server.to_string();
    value.strip_prefix("pubky").unwrap_or(&value).to_owned()
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn locks_client_can_be_constructed_for_explicit_lock_server() {
        let lock_server = lock_server();

        let client = LocksClient::for_server(lock_server.clone());

        assert_eq!(client.lock_server(), &lock_server);
    }

    #[test]
    fn locks_client_can_be_constructed_from_creator_lock_service_pointer() {
        let pointer = CreatorLockServicePointer::validate_value(serde_json::json!({
            "version": 1,
            "default_lock_server": lock_server(),
            "created_at": "2026-06-03T00:00:00Z"
        }))
        .unwrap();

        let client = LocksClient::for_creator_pointer(pointer);

        assert_eq!(client.lock_server(), &lock_server());
    }

    #[test]
    fn transport_url_uses_direct_pubky_host_for_lock_server() {
        let client = LocksClient::for_server(lock_server());

        let url = client.transport_url("/.well-known/locks-server").unwrap();

        assert_eq!(
            url.as_str(),
            "https://_pubky.7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo/.well-known/locks-server"
        );
    }

    #[test]
    fn transport_url_preserves_path_and_query() {
        let client = LocksClient::for_server(lock_server());

        let url = client
            .transport_url("/connect?return_to=https%3A%2F%2Fpubky.app%2Fcallback&state=opaque")
            .unwrap();

        assert_eq!(url.path(), "/connect");
        assert_eq!(
            url.query(),
            Some("return_to=https%3A%2F%2Fpubky.app%2Fcallback&state=opaque")
        );
    }

    fn lock_server() -> LockServerPubky {
        LockServerPubky::from_str("pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo")
            .unwrap()
    }
}
