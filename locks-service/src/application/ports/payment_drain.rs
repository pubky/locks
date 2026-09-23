use std::fmt;

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use locks_core::ids::{BundleId, CreatorPubky, PubkyLockResource};
use time::OffsetDateTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaymentDrainStatus {
    Active,
    Completed,
}

impl PaymentDrainStatus {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "active" => Some(Self::Active),
            "completed" => Some(Self::Completed),
            _ => None,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct PaymentDrainCleanupToken(String);

impl PaymentDrainCleanupToken {
    pub fn parse(value: &str) -> Option<Self> {
        if value.len() != 43 || value.contains('=') {
            return None;
        }
        let decoded: [u8; 32] = URL_SAFE_NO_PAD.decode(value).ok()?.try_into().ok()?;
        (URL_SAFE_NO_PAD.encode(decoded) == value).then(|| Self(value.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for PaymentDrainCleanupToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PaymentDrainCleanupToken(<redacted>)")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaymentDrainSummary {
    pub status: PaymentDrainStatus,
    pub accepted_count: u64,
    pub terminal_count: u64,
    pub cancellation_enqueued_count: u64,
    pub cleanup_token: PaymentDrainCleanupToken,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaymentRequestState {
    Proposed,
    ProposalExpired,
    Accepted,
    Rejected,
    Canceled,
    ProofSubmitted,
    ActiveRecurring,
    RecoveryRequired,
    InvalidConflict,
}

impl PaymentRequestState {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "proposed" => Some(Self::Proposed),
            "proposal_expired" => Some(Self::ProposalExpired),
            "accepted" => Some(Self::Accepted),
            "rejected" => Some(Self::Rejected),
            "canceled" => Some(Self::Canceled),
            "proof_submitted" => Some(Self::ProofSubmitted),
            "active_recurring" => Some(Self::ActiveRecurring),
            "recovery_required" => Some(Self::RecoveryRequired),
            "invalid_conflict" => Some(Self::InvalidConflict),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaymentState {
    Undetected,
    Detected,
    Confirmed,
    Expired,
}

impl PaymentState {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "undetected" => Some(Self::Undetected),
            "detected" => Some(Self::Detected),
            "confirmed" => Some(Self::Confirmed),
            "expired" => Some(Self::Expired),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PaymentRequestStatus {
    pub request_state: PaymentRequestState,
    pub payment_state: PaymentState,
    pub invoice_created_at: OffsetDateTime,
    pub payment_deadline: OffsetDateTime,
    pub confirmations: u32,
    pub amount_matched: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaymentDrainClientError {
    NotFound,
    Conflict,
    MalformedSuccess,
    Transport,
    Server,
}

#[async_trait]
pub trait PaymentDrainClient: Send + Sync {
    async fn start_payment_drain(
        &self,
        lock_resource: &PubkyLockResource,
    ) -> Result<PaymentDrainSummary, PaymentDrainClientError>;

    async fn lookup_payment_drain(
        &self,
        lock_resource: &PubkyLockResource,
    ) -> Result<Option<PaymentDrainSummary>, PaymentDrainClientError>;

    async fn payment_request_status(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<Option<PaymentRequestStatus>, PaymentDrainClientError>;
}
