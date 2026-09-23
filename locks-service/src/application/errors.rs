use locks_core::ids::LockId;
use locks_core::lock_policy::VerifierType;

use crate::application::models::VerificationTaskStatus;

/// Errors raised by the Lock Server application layer.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ApplicationError {
    /// An insert-only operation found an existing record.
    #[error("duplicate {record} record")]
    DuplicateRecord {
        /// Stable record kind for diagnostics.
        record: &'static str,
    },
    /// A guarded path already has an in-flight or published ownership record.
    #[error("content lock path conflict")]
    ContentLockPathConflict {
        /// Full creator-scoped guarded path for structured internal handling.
        guarded_path: String,
    },
    /// A graceful deletion cutoff already blocks new proof Bundle IDs for the lock.
    #[error("content lock deletion in progress")]
    ContentLockDeletionInProgress,
    /// Persisted content-lock deletion state violates its internal invariants.
    #[error("invalid content lock deletion state: {message}")]
    InvalidContentLockDeletionState {
        /// Secret-free invariant failure detail.
        message: String,
    },
    /// An update-only operation targeted a missing record.
    #[error("missing {record} record")]
    MissingRecord {
        /// Stable record kind for diagnostics.
        record: &'static str,
    },
    /// Repository/store adapter failure.
    #[error("storage error: {message}")]
    Storage {
        /// Human-readable storage failure detail.
        message: String,
    },
    /// Verifier adapter failure.
    #[error("verifier error: {message}")]
    Verifier {
        /// Human-readable verifier failure detail.
        message: String,
    },
    /// Criterion verifier is not terminal yet and should be retried later.
    #[error("verification pending")]
    VerificationPending,
    /// A verification provider is transiently unavailable and should be retried later.
    #[error("verification dependency unavailable")]
    VerificationDependencyUnavailable,
    /// Submitted payment proof does not match its canonical content lock criterion.
    #[error("invalid paykit payment submission")]
    InvalidPaykitPaymentSubmission,
    /// Attempted verification task transition is not allowed.
    #[error("invalid verification task transition from {from:?} to {to:?}")]
    InvalidVerificationTaskTransition {
        /// Current task status.
        from: VerificationTaskStatus,
        /// Requested next task status.
        to: VerificationTaskStatus,
    },
    /// Existing verification task record violates lifecycle invariants.
    #[error("invalid verification task state: {message}")]
    InvalidVerificationTaskState {
        /// Human-readable invariant failure detail.
        message: String,
    },
    /// Failure message was missing, empty, or present for a non-failed transition.
    #[error("invalid verification task failure message")]
    InvalidVerificationTaskFailureMessage,
    /// Existing verification attempt was submitted again with different proof material.
    #[error("verification task conflict")]
    VerificationTaskConflict,
    /// Worker lease was lost before a claim-fenced lifecycle transition could be persisted.
    #[error("verification task claim lost")]
    VerificationTaskClaimLost,
    /// Public proof bundle submission exceeded the configured admission limit.
    #[error("rate limit exceeded")]
    RateLimited,
    /// Requested access credential TTL is unsupported by this Lock Server.
    #[error("unsupported credential TTL: requested {requested_seconds}s, max {max_seconds}s")]
    UnsupportedCredentialTtl {
        /// Requested TTL in seconds.
        requested_seconds: u64,
        /// Maximum supported TTL in seconds.
        max_seconds: u64,
    },
    /// Access credential generation failed.
    #[error("access credential generation error: {message}")]
    CredentialGeneration {
        /// Human-readable credential generation failure detail.
        message: String,
    },
    /// Final deletion credential envelope could not be encrypted or decrypted.
    #[error("final credential secret error: {message}")]
    FinalCredentialSecret {
        /// Stable secret-free failure detail.
        message: String,
    },
    /// Creator-granted homeserver authority is missing, expired, revoked, or unusable.
    #[error("creator authority unavailable")]
    CreatorAuthorityUnavailable,
    /// Creator authority secret material could not be imported, restored, or used.
    #[error("creator authority secret error: {message}")]
    CreatorAuthoritySecret {
        /// Secret-free creator authority failure detail.
        message: String,
    },
    /// Creator authority record contains an unsupported auth kind.
    #[error("invalid creator authority auth kind: {auth_kind}")]
    InvalidCreatorAuthorityAuthKind {
        /// Invalid auth kind string read from storage or input.
        auth_kind: String,
    },
    /// Pending creator connect flow is missing or no longer usable.
    #[error("creator connect flow unavailable")]
    CreatorConnectFlowUnavailable,
    /// Pending creator connect flow has expired.
    #[error("creator connect flow expired")]
    CreatorConnectFlowExpired,
    /// One-time frontend session code is missing or no longer usable.
    #[error("frontend session code unavailable")]
    FrontendSessionCodeUnavailable,
    /// One-time frontend session code has expired.
    #[error("frontend session code expired")]
    FrontendSessionCodeExpired,
    /// One-time frontend session code was already consumed.
    #[error("frontend session code already consumed")]
    FrontendSessionCodeAlreadyConsumed,
    /// Frontend session is missing or no longer usable.
    #[error("frontend session unavailable")]
    FrontendSessionUnavailable,
    /// Frontend session has expired.
    #[error("frontend session expired")]
    FrontendSessionExpired,
    /// Frontend session exchange state does not match the stored state.
    #[error("frontend session state mismatch")]
    FrontendSessionStateMismatch,
    /// Content lock has no criteria and cannot authorize access.
    #[error("content lock has no criteria")]
    EmptyContentLockCriteria,
    /// Content lock contains a duplicate criterion ID.
    #[error("duplicate content lock criterion: {criterion_id}")]
    DuplicateContentLockCriterion {
        /// Duplicate criterion identifier.
        criterion_id: String,
    },
    /// Verification result contains a duplicate criterion ID.
    #[error("duplicate verification result criterion: {criterion_id}")]
    DuplicateVerificationResultCriterion {
        /// Duplicate criterion identifier.
        criterion_id: String,
    },
    /// Verification result references a criterion missing from the content lock.
    #[error("unknown verification result criterion: {criterion_id}")]
    UnknownVerificationResultCriterion {
        /// Unknown criterion identifier.
        criterion_id: String,
    },
    /// No verified entitlement exists for the requested credential operation.
    #[error("entitlement not found")]
    EntitlementNotFound,
    /// Referenced content lock cannot be loaded for entitlement validation.
    #[error("content lock unavailable")]
    ContentLockUnavailable,
    /// Fetched content lock does not hash to the Lock ID embedded in its path.
    #[error("content lock hash mismatch: expected {expected}, actual {actual}")]
    ContentLockHashMismatch {
        /// Lock ID embedded in the content lock path.
        expected: LockId,
        /// Lock ID derived from the fetched content lock.
        actual: LockId,
    },
    /// Entitlement evidence is valid but insufficient for the content lock.
    #[error("entitlement not satisfied")]
    EntitlementNotSatisfied,
    /// Content lock canonicalization/hash derivation failed.
    #[error("content lock canonicalization error: {message}")]
    ContentLockCanonicalization {
        /// Human-readable canonicalization failure detail.
        message: String,
    },
    /// Verifier type is known to the protocol but not supported by this Lock Server.
    #[error("unsupported verifier type: {verifier_type}")]
    UnsupportedVerifierType {
        /// Unsupported verifier type.
        verifier_type: VerifierType,
    },
    /// Referenced guarded resource cannot be loaded for proxy read.
    #[error("guarded resource unavailable")]
    GuardedResourceUnavailable,
    /// Guarded resource descriptor or bytes failed validation.
    #[error("invalid guarded resource: {message}")]
    InvalidGuardedResource {
        /// Human-readable guarded resource validation failure detail.
        message: String,
    },
    /// Presented access credential is unknown or no longer recognized.
    #[error("invalid access credential")]
    InvalidAccessCredential,
    /// Presented access credential existed but has expired.
    #[error("expired access credential")]
    ExpiredAccessCredential,
}
