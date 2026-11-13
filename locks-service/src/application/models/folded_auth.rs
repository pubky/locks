use std::fmt;

use locks_core::ids::{BundleId, CreatorPubky, PubkyLockResource, TaskId};
use locks_core::lock_policy::{GraphPolicyV2, GraphPredicateV2, PaymentPolicyV2};
use locks_core::verification::SubmittedProofBundleV2;
use time::{Duration, OffsetDateTime};

use super::{
    GraphTaskRecord, GraphTaskSnapshot, GraphTaskSnapshotV1, MixedTaskRecord, MixedTaskSnapshot,
    MixedTaskSnapshotV1, VerificationTaskStatus,
};

pub const FOLDED_AUTH_TTL: Duration = Duration::minutes(10);
pub const FOLDED_AUTH_TOMBSTONE_TTL: Duration = Duration::hours(24);
/// A dispatched poll lease is abandoned when it has not been renewed for this long.
pub const FOLDED_AUTH_POLL_ABANDONED_AFTER: Duration = Duration::minutes(1);
const STATE_HASH_DOMAIN: &[u8] = b"pubky-locks/folded-auth-state/v2\0";

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct FoldedAuthStateHash([u8; 32]);

impl FoldedAuthStateHash {
    pub fn derive(opaque_state: &[u8]) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(STATE_HASH_DOMAIN);
        hasher.update(opaque_state);
        Self(*hasher.finalize().as_bytes())
    }

    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for FoldedAuthStateHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("FoldedAuthStateHash(<redacted>)")
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct FoldedAuthFlowSecret(Vec<u8>);

impl FoldedAuthFlowSecret {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    pub fn expose_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for FoldedAuthFlowSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("FoldedAuthFlowSecret(<redacted>)")
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct EncryptedFoldedAuthFlowSecret(String);

impl EncryptedFoldedAuthFlowSecret {
    pub fn new(value: String) -> Self {
        Self(value)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for EncryptedFoldedAuthFlowSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("EncryptedFoldedAuthFlowSecret(<redacted>)")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FoldedAuthStatus {
    Pending,
    Accepted,
    Expired,
    RestartRequired,
}

impl FoldedAuthStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Accepted => "accepted",
            Self::Expired => "expired",
            Self::RestartRequired => "restart_required",
        }
    }

    pub fn from_storage_value(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "accepted" => Some(Self::Accepted),
            "expired" => Some(Self::Expired),
            "restart_required" => Some(Self::RestartRequired),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FoldedAuthEvaluatorMode {
    Graph,
    Mixed,
}

impl FoldedAuthEvaluatorMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Graph => "graph",
            Self::Mixed => "mixed",
        }
    }

    pub fn from_storage_value(value: &str) -> Option<Self> {
        match value {
            "graph" => Some(Self::Graph),
            "mixed" => Some(Self::Mixed),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoldedAuthAttempt {
    pub creator: CreatorPubky,
    pub bundle_id: BundleId,
    pub generation: u64,
    pub state_hash: FoldedAuthStateHash,
    pub status: FoldedAuthStatus,
    pub submitted_proof_bundle: SubmittedProofBundleV2,
    pub pubky_lock_resource: PubkyLockResource,
    pub lock_hash: [u8; 32],
    pub verification_window_seconds: u64,
    pub graph: GraphPolicyV2,
    pub evaluator_mode: FoldedAuthEvaluatorMode,
    pub payment: Option<PaymentPolicyV2>,
    pub created_at: OffsetDateTime,
    pub expires_at: OffsetDateTime,
    /// Durable fact that approval polling may have consumed one-shot authority.
    pub poll_dispatched: bool,
    pub terminal_at: Option<OffsetDateTime>,
    pub purge_after: Option<OffsetDateTime>,
    pub task_id: Option<TaskId>,
}

impl FoldedAuthAttempt {
    pub fn exact_request_matches(&self, request: &StartFoldedAuthAttempt) -> bool {
        self.submitted_proof_bundle == request.submitted_proof_bundle
            && self.pubky_lock_resource == request.pubky_lock_resource
            && self.lock_hash == request.lock_hash
            && self.verification_window_seconds == request.verification_window_seconds
            && self.graph == request.graph
            && self.evaluator_mode == request.evaluator_mode
            && self.payment == request.payment
    }

    pub fn context(&self) -> FoldedAuthSecretContext {
        FoldedAuthSecretContext {
            creator: self.creator.clone(),
            bundle_id: self.bundle_id.clone(),
            generation: self.generation,
            pubky_lock_resource: self.pubky_lock_resource.clone(),
            lock_hash: self.lock_hash,
        }
    }

    pub fn accepted_graph_task(
        &self,
        task_id: TaskId,
        reader: CreatorPubky,
        accepted_at: OffsetDateTime,
    ) -> Result<GraphTaskRecord, String> {
        Ok(GraphTaskRecord {
            task_id,
            snapshot: GraphTaskSnapshot::V1(GraphTaskSnapshotV1::new(
                self.creator.clone(),
                self.bundle_id.clone(),
                self.pubky_lock_resource.clone(),
                self.lock_hash,
                reader,
                accepted_at,
                self.verification_window_seconds,
                self.graph.clone(),
            )?),
            evaluator_revision: 0,
            status: VerificationTaskStatus::Pending,
            entitlement_to_publish: None,
            entitlement_publication_token: None,
            completed_at: None,
        })
    }

    pub fn accepted_mixed_task(
        &self,
        task_id: TaskId,
        reader: CreatorPubky,
        accepted_at: OffsetDateTime,
    ) -> Result<MixedTaskRecord, String> {
        if self.evaluator_mode != FoldedAuthEvaluatorMode::Mixed {
            return Err("folded auth attempt is not mixed".to_owned());
        }
        let payment = self
            .payment
            .clone()
            .ok_or_else(|| "mixed folded auth attempt lacks payment policy".to_owned())?;
        Ok(MixedTaskRecord {
            task_id,
            snapshot: MixedTaskSnapshot::V1(MixedTaskSnapshotV1::new(
                self.submitted_proof_bundle.clone(),
                self.lock_hash,
                reader,
                self.graph.clone(),
                payment,
                accepted_at,
                self.verification_window_seconds,
            )?),
            evaluator_revision: 0,
            status: VerificationTaskStatus::Pending,
            terminal_reason: None,
            entitlement_to_publish: None,
            entitlement_publication_token: None,
            completed_at: None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoredFoldedAuthAttempt {
    pub attempt: FoldedAuthAttempt,
    pub flow_secret: FoldedAuthFlowSecret,
    pub pending_response: Option<FoldedAuthPendingResponse>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct FoldedAuthPendingResponse {
    pub auth_url: String,
    pub state: String,
}

impl std::fmt::Debug for FoldedAuthPendingResponse {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FoldedAuthPendingResponse")
            .field("auth_url", &"<redacted>")
            .field("state", &"<redacted>")
            .finish()
    }
}

#[cfg(test)]
mod redaction_tests {
    use super::FoldedAuthPendingResponse;

    #[test]
    fn pending_response_debug_redacts_auth_url_and_state() {
        let pending = FoldedAuthPendingResponse {
            auth_url: "pubkyauth://secret-bearing-url".to_owned(),
            state: "secret-bearing-state".to_owned(),
        };

        let debug = format!("{pending:?}");
        assert!(!debug.contains("secret-bearing-url"));
        assert!(!debug.contains("secret-bearing-state"));
        assert_eq!(debug.matches("<redacted>").count(), 2);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartFoldedAuthAttempt {
    pub submitted_proof_bundle: SubmittedProofBundleV2,
    pub pubky_lock_resource: PubkyLockResource,
    pub lock_hash: [u8; 32],
    pub verification_window_seconds: u64,
    pub graph: GraphPolicyV2,
    pub evaluator_mode: FoldedAuthEvaluatorMode,
    pub payment: Option<PaymentPolicyV2>,
    pub state_hash: FoldedAuthStateHash,
    pub flow_secret: FoldedAuthFlowSecret,
    pub pending_response: FoldedAuthPendingResponse,
    pub created_at: OffsetDateTime,
}

impl StartFoldedAuthAttempt {
    pub fn creator(&self) -> &CreatorPubky {
        self.submitted_proof_bundle.pubky_lock_resource.creator()
    }

    pub fn bundle_id(&self) -> &BundleId {
        &self.submitted_proof_bundle.bundle_id
    }

    pub fn expires_at(&self) -> Option<OffsetDateTime> {
        self.created_at.checked_add(FOLDED_AUTH_TTL)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.flow_secret.expose_bytes().is_empty()
            || self.pending_response.auth_url.is_empty()
            || self.pending_response.state.is_empty()
        {
            return Err("folded auth flow secret or pending response is invalid".to_owned());
        }
        if self.submitted_proof_bundle.version != 2
            || self.submitted_proof_bundle.reader_pubky.is_some()
            || self.submitted_proof_bundle.pubky_lock_resource != self.pubky_lock_resource
            || self.pubky_lock_resource.creator() != self.creator()
        {
            return Err("folded auth request binding is invalid".to_owned());
        }
        if !(60..=604_800).contains(&self.verification_window_seconds) {
            return Err("folded auth verification window is invalid".to_owned());
        }
        if !(1..=8).contains(&self.graph.predicates.len())
            || (self.graph.predicates.len() == 1) != self.graph.operator.is_none()
        {
            return Err("folded auth graph policy is invalid".to_owned());
        }
        let unique = self
            .graph
            .predicates
            .iter()
            .collect::<std::collections::HashSet<&GraphPredicateV2>>();
        if unique.len() != self.graph.predicates.len() {
            return Err("folded auth graph policy contains duplicates".to_owned());
        }
        match (self.evaluator_mode, &self.payment) {
            (FoldedAuthEvaluatorMode::Graph, None) | (FoldedAuthEvaluatorMode::Mixed, Some(_)) => {}
            _ => return Err("folded auth evaluator policy shape is invalid".to_owned()),
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FoldedAuthStartDisposition {
    Started,
    Rotated,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoldedAuthStartResult {
    pub attempt: FoldedAuthAttempt,
    pub disposition: FoldedAuthStartDisposition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoldedAuthSecretContext {
    pub creator: CreatorPubky,
    pub bundle_id: BundleId,
    pub generation: u64,
    pub pubky_lock_resource: PubkyLockResource,
    pub lock_hash: [u8; 32],
}
