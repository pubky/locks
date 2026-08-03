use std::fmt;

use locks_core::ids::CreatorPubky;
use time::OffsetDateTime;

use crate::application::errors::ApplicationError;

/// Auth mechanism used for a creator-granted Lock Server homeserver authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreatorAuthorityAuthKind {
    /// Interim legacy cookie/session auth flow.
    LegacyCookie,
    /// Future grant-based auth flow.
    Grant,
}

impl CreatorAuthorityAuthKind {
    /// Stable storage representation.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LegacyCookie => "legacy_cookie",
            Self::Grant => "grant",
        }
    }
}

impl std::str::FromStr for CreatorAuthorityAuthKind {
    type Err = ApplicationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "legacy_cookie" => Ok(Self::LegacyCookie),
            "grant" => Ok(Self::Grant),
            _ => Err(ApplicationError::InvalidCreatorAuthorityAuthKind {
                auth_kind: value.to_owned(),
            }),
        }
    }
}

/// Secret material for restoring creator-granted homeserver authority.
#[derive(Clone, PartialEq, Eq)]
pub struct CreatorAuthoritySecret(String);

impl CreatorAuthoritySecret {
    /// Wraps creator authority secret material.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Exposes the raw secret for infrastructure adapters that restore sessions.
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for CreatorAuthoritySecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("CreatorAuthoritySecret")
            .field(&"<redacted>")
            .finish()
    }
}

/// Private runtime record for one creator-granted Lock Server authority.
#[derive(Clone, PartialEq, Eq)]
pub struct CreatorAuthorityRecord {
    /// Creator whose homeserver authority is represented.
    pub creator: CreatorPubky,
    /// Auth mechanism used by the stored secret.
    pub auth_kind: CreatorAuthorityAuthKind,
    /// Scopes granted to the Lock Server.
    pub granted_scopes: Vec<String>,
    /// Secret-bearing session/credential material.
    pub secret: CreatorAuthoritySecret,
    /// Optional session expiration reported by the underlying auth mechanism.
    pub session_expires_at: Option<OffsetDateTime>,
    /// Last time this authority was revalidated.
    pub last_revalidated_at: Option<OffsetDateTime>,
}

impl fmt::Debug for CreatorAuthorityRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreatorAuthorityRecord")
            .field("creator", &self.creator)
            .field("auth_kind", &self.auth_kind)
            .field("granted_scopes", &self.granted_scopes)
            .field("secret", &"<redacted>")
            .field("session_expires_at", &self.session_expires_at)
            .field("last_revalidated_at", &self.last_revalidated_at)
            .finish()
    }
}

/// Approved legacy Pubky auth-flow material, converted to Locks creator-authority state.
#[derive(Clone, PartialEq, Eq)]
pub struct LegacyCreatorConnectFlowApproval {
    /// Approved creator identity.
    pub creator: CreatorPubky,
    /// Restorable legacy Pubky session secret.
    pub session_secret: CreatorAuthoritySecret,
}

impl fmt::Debug for LegacyCreatorConnectFlowApproval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LegacyCreatorConnectFlowApproval")
            .field("creator", &self.creator)
            .field("session_secret", &"<redacted>")
            .finish()
    }
}

/// Server-generated identifier for a pending creator connect flow.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreatorConnectFlowId(String);

impl CreatorConnectFlowId {
    /// Wraps an opaque connect flow identifier.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the opaque identifier value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Secret-bearing Pubky authorization URL for a pending Lock-Server-owned flow.
#[derive(Clone, PartialEq, Eq)]
pub struct CreatorConnectAuthorizationUrl(String);

impl CreatorConnectAuthorizationUrl {
    /// Wraps an authorization URL containing Pubky relay/client secret material.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Exposes the raw URL for infrastructure code that resumes the flow.
    pub fn expose_url(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for CreatorConnectAuthorizationUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("CreatorConnectAuthorizationUrl")
            .field(&"<redacted>")
            .finish()
    }
}

/// Secret-bearing pending Lock-Server-owned Pubky auth flow state.
#[derive(Clone, PartialEq, Eq)]
pub struct PendingCreatorConnectFlowRecord {
    /// Server-generated pending flow ID.
    pub flow_id: CreatorConnectFlowId,
    /// Caller-provided redirect/callback target returned after completion.
    pub return_to: String,
    /// Caller-provided CSRF/correlation state echoed into frontend-session exchange.
    pub state: String,
    /// Secret-bearing authorization URL needed to resume/await Pubky approval.
    pub authorization_url: CreatorConnectAuthorizationUrl,
    /// Scopes requested for the Lock Server's creator-granted homeserver session.
    pub requested_scopes: Vec<String>,
    /// Creation timestamp.
    pub created_at: OffsetDateTime,
    /// Expiration timestamp. Pending flows expire when `now >= expires_at`.
    pub expires_at: OffsetDateTime,
}

impl PendingCreatorConnectFlowRecord {
    /// Returns true once this pending flow is expired at `now`.
    pub fn is_expired_at(&self, now: OffsetDateTime) -> bool {
        now >= self.expires_at
    }
}

impl fmt::Debug for PendingCreatorConnectFlowRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PendingCreatorConnectFlowRecord")
            .field("flow_id", &self.flow_id)
            .field("return_to", &self.return_to)
            .field("state", &self.state)
            .field("authorization_url", &"<redacted>")
            .field("requested_scopes", &self.requested_scopes)
            .field("created_at", &self.created_at)
            .field("expires_at", &self.expires_at)
            .finish()
    }
}
