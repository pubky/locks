use async_trait::async_trait;
use locks_core::ids::CreatorPubky;

use crate::application::errors::ApplicationError;
use crate::application::models::{
    CreatorAuthorityAuthKind, CreatorAuthorityRecord, CreatorConnectAuthorizationUrl,
    CreatorConnectFlowId, FrontendSessionCode, FrontendSessionCodeRecord, FrontendSessionRecord,
    FrontendSessionToken, LegacyCreatorConnectFlowApproval, PendingCreatorConnectFlowRecord,
};

/// Secret-free status view for creator-granted homeserver authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatorAuthorityStatus {
    /// Creator whose homeserver authority was checked.
    pub creator: CreatorPubky,
    /// Auth mechanism backing the authority.
    pub auth_kind: CreatorAuthorityAuthKind,
    /// Whether the authority is currently usable.
    pub authorized: bool,
    /// Scopes granted to the Lock Server.
    pub granted_scopes: Vec<String>,
    /// Optional session expiration reported by the underlying auth mechanism.
    pub session_expires_at: Option<time::OffsetDateTime>,
}

/// Store for secret-bearing creator-granted homeserver authority.
#[async_trait]
pub trait CreatorAuthorityStore: Send + Sync {
    /// Creates or replaces the creator authority record for a creator.
    async fn upsert_creator_authority(
        &self,
        authority: CreatorAuthorityRecord,
    ) -> Result<(), ApplicationError>;

    /// Loads the current creator authority record.
    ///
    /// Returns `Ok(None)` when the creator has no stored authority on this Lock Server.
    async fn get_creator_authority(
        &self,
        creator: &CreatorPubky,
    ) -> Result<Option<CreatorAuthorityRecord>, ApplicationError>;

    /// Ensures the creator authority record is absent.
    async fn delete_creator_authority(
        &self,
        creator: &CreatorPubky,
    ) -> Result<(), ApplicationError>;
}

/// Runtime boundary for checking creator-granted homeserver authority.
#[async_trait]
pub trait CreatorAuthorityManager: Send + Sync {
    /// Revalidates creator authority and returns a secret-free status view.
    async fn revalidate_creator_authority(
        &self,
        creator: &CreatorPubky,
    ) -> Result<CreatorAuthorityStatus, ApplicationError>;

    /// Requires usable creator authority for Pubky homeserver I/O.
    async fn require_creator_authority(
        &self,
        creator: &CreatorPubky,
    ) -> Result<CreatorAuthorityStatus, ApplicationError>;
}

/// Store for short-lived, secret-bearing pending creator connect flows.
#[async_trait]
pub trait CreatorConnectFlowStore: Send + Sync {
    /// Inserts a pending connect flow.
    async fn insert_pending_creator_connect_flow(
        &self,
        record: PendingCreatorConnectFlowRecord,
    ) -> Result<(), ApplicationError>;

    /// Loads a pending connect flow by server-generated flow ID.
    ///
    /// Returns `Ok(None)` when the flow is absent.
    async fn get_pending_creator_connect_flow(
        &self,
        flow_id: &CreatorConnectFlowId,
    ) -> Result<Option<PendingCreatorConnectFlowRecord>, ApplicationError>;

    /// Ensures the pending connect flow is absent.
    async fn delete_pending_creator_connect_flow(
        &self,
        flow_id: &CreatorConnectFlowId,
    ) -> Result<(), ApplicationError>;
}

/// Store for short-lived one-time frontend session exchange codes.
#[async_trait]
pub trait FrontendSessionCodeStore: Send + Sync {
    /// Inserts a newly issued frontend session exchange code.
    async fn insert_frontend_session_code(
        &self,
        record: FrontendSessionCodeRecord,
    ) -> Result<(), ApplicationError>;

    /// Consumes a frontend session exchange code exactly once.
    ///
    /// Returns `Ok(None)` when the code is absent. Implementations should mark a
    /// present code as consumed atomically with returning it.
    async fn consume_frontend_session_code(
        &self,
        code: &FrontendSessionCode,
        now: time::OffsetDateTime,
    ) -> Result<Option<FrontendSessionCodeRecord>, ApplicationError>;
}

/// Store for Locks-local frontend sessions.
#[async_trait]
pub trait FrontendSessionStore: Send + Sync {
    /// Inserts a newly issued frontend session.
    async fn insert_frontend_session(
        &self,
        record: FrontendSessionRecord,
    ) -> Result<(), ApplicationError>;

    /// Loads a frontend session by raw token wrapper.
    ///
    /// Returns `Ok(None)` when the session is absent.
    async fn get_frontend_session(
        &self,
        token: &FrontendSessionToken,
    ) -> Result<Option<FrontendSessionRecord>, ApplicationError>;

    /// Ensures the frontend session is absent.
    async fn delete_frontend_session(
        &self,
        token: &FrontendSessionToken,
    ) -> Result<(), ApplicationError>;
}

/// Generator for server-owned creator connect flow IDs.
pub trait CreatorConnectFlowIdGenerator: Send + Sync {
    /// Generates a new opaque flow ID.
    fn generate_creator_connect_flow_id(&self) -> CreatorConnectFlowId;
}

/// Generator for one-time frontend session exchange codes.
pub trait FrontendSessionCodeGenerator: Send + Sync {
    /// Generates a new raw one-time code.
    fn generate_frontend_session_code(&self) -> FrontendSessionCode;
}

/// Generator for Locks-local frontend session tokens.
pub trait FrontendSessionTokenGenerator: Send + Sync {
    /// Generates a new raw frontend session token.
    fn generate_frontend_session_token(&self) -> FrontendSessionToken;
}

/// Object-safe seam for starting and completing legacy Pubky creator connect flows.
#[async_trait]
pub trait LegacyCreatorConnectFlowClient: Send + Sync {
    /// Starts a legacy Pubky auth flow and returns the secret-bearing authorization URL.
    async fn start_legacy_creator_connect_flow(
        &self,
        requested_scopes: &[String],
    ) -> Result<CreatorConnectAuthorizationUrl, ApplicationError>;

    /// Resumes a pending flow from its authorization URL and awaits signer approval.
    async fn await_legacy_creator_connect_flow_approval(
        &self,
        authorization_url: &CreatorConnectAuthorizationUrl,
    ) -> Result<LegacyCreatorConnectFlowApproval, ApplicationError>;
}

#[cfg(test)]
mod legacy_connect_flow_port_tests {
    use super::*;
    use crate::application::models::{
        CreatorConnectFlowId, FrontendSessionCode, FrontendSessionCodeRecord,
        FrontendSessionRecord, FrontendSessionToken, PendingCreatorConnectFlowRecord,
    };
    use time::OffsetDateTime;

    struct CompileOnlyConnectFlowStore;

    #[async_trait]
    impl CreatorConnectFlowStore for CompileOnlyConnectFlowStore {
        async fn insert_pending_creator_connect_flow(
            &self,
            _record: PendingCreatorConnectFlowRecord,
        ) -> Result<(), ApplicationError> {
            Ok(())
        }

        async fn get_pending_creator_connect_flow(
            &self,
            _flow_id: &CreatorConnectFlowId,
        ) -> Result<Option<PendingCreatorConnectFlowRecord>, ApplicationError> {
            Ok(None)
        }

        async fn delete_pending_creator_connect_flow(
            &self,
            _flow_id: &CreatorConnectFlowId,
        ) -> Result<(), ApplicationError> {
            Ok(())
        }
    }

    struct CompileOnlyFrontendSessionCodeStore;

    #[async_trait]
    impl FrontendSessionCodeStore for CompileOnlyFrontendSessionCodeStore {
        async fn insert_frontend_session_code(
            &self,
            _record: FrontendSessionCodeRecord,
        ) -> Result<(), ApplicationError> {
            Ok(())
        }

        async fn consume_frontend_session_code(
            &self,
            _code: &FrontendSessionCode,
            _now: OffsetDateTime,
        ) -> Result<Option<FrontendSessionCodeRecord>, ApplicationError> {
            Ok(None)
        }
    }

    struct CompileOnlyFrontendSessionStore;

    #[async_trait]
    impl FrontendSessionStore for CompileOnlyFrontendSessionStore {
        async fn insert_frontend_session(
            &self,
            _record: FrontendSessionRecord,
        ) -> Result<(), ApplicationError> {
            Ok(())
        }

        async fn get_frontend_session(
            &self,
            _token: &FrontendSessionToken,
        ) -> Result<Option<FrontendSessionRecord>, ApplicationError> {
            Ok(None)
        }

        async fn delete_frontend_session(
            &self,
            _token: &FrontendSessionToken,
        ) -> Result<(), ApplicationError> {
            Ok(())
        }
    }

    struct CompileOnlyGenerators;

    impl CreatorConnectFlowIdGenerator for CompileOnlyGenerators {
        fn generate_creator_connect_flow_id(&self) -> CreatorConnectFlowId {
            CreatorConnectFlowId::new("flow-id")
        }
    }

    impl FrontendSessionCodeGenerator for CompileOnlyGenerators {
        fn generate_frontend_session_code(&self) -> FrontendSessionCode {
            FrontendSessionCode::new("code")
        }
    }

    impl FrontendSessionTokenGenerator for CompileOnlyGenerators {
        fn generate_frontend_session_token(&self) -> FrontendSessionToken {
            FrontendSessionToken::new("token")
        }
    }

    #[test]
    fn legacy_connect_flow_ports_are_object_safe() {
        let connect_flow_store: &dyn CreatorConnectFlowStore = &CompileOnlyConnectFlowStore;
        let code_store: &dyn FrontendSessionCodeStore = &CompileOnlyFrontendSessionCodeStore;
        let session_store: &dyn FrontendSessionStore = &CompileOnlyFrontendSessionStore;
        let flow_id_generator: &dyn CreatorConnectFlowIdGenerator = &CompileOnlyGenerators;
        let code_generator: &dyn FrontendSessionCodeGenerator = &CompileOnlyGenerators;
        let token_generator: &dyn FrontendSessionTokenGenerator = &CompileOnlyGenerators;

        let _ = (
            connect_flow_store,
            code_store,
            session_store,
            flow_id_generator,
            code_generator,
            token_generator,
        );
    }
}
