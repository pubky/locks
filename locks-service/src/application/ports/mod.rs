pub mod semantics {}

mod access;
mod content_lock_deletion;
pub mod content_lock_deletion_action_ownership;
mod content_lock_ownership;
pub mod content_lock_tombstone;
mod creator_authority;
mod entitlement;
mod guarded_resources;
mod lock_policy;
mod payment_drain;
mod payment_drain_repository;
mod runtime;
mod verification;

pub use access::*;
pub use content_lock_deletion::*;
pub use content_lock_deletion_action_ownership::*;
pub use content_lock_ownership::*;
pub use content_lock_tombstone::*;
pub use creator_authority::*;
pub use entitlement::*;
pub use guarded_resources::*;
pub use lock_policy::*;
pub use payment_drain::*;
pub use payment_drain_repository::*;
pub use runtime::*;
pub use verification::*;

#[cfg(test)]
mod payment_drain_contract_tests {
    use super::{
        PaymentDrainCleanupToken, PaymentDrainClient, PaymentDrainClientError, PaymentDrainStatus,
        PaymentRequestState, PaymentState,
    };

    fn assert_object_safe(_: &dyn PaymentDrainClient) {}

    #[test]
    fn payment_drain_port_is_object_safe_and_closed_values_parse_strictly() {
        let _ = assert_object_safe;
        assert_eq!(
            PaymentDrainStatus::parse("active"),
            Some(PaymentDrainStatus::Active)
        );
        assert_eq!(PaymentDrainStatus::parse("complete"), None);
        assert_eq!(
            PaymentRequestState::parse("proposal_expired"),
            Some(PaymentRequestState::ProposalExpired)
        );
        assert_eq!(PaymentRequestState::parse("unknown"), None);
        assert_eq!(PaymentState::parse("expired"), Some(PaymentState::Expired));
        assert_eq!(PaymentState::parse("late"), None);
        let token =
            PaymentDrainCleanupToken::parse("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA").unwrap();
        assert_eq!(
            token.as_str(),
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        );
        assert!(PaymentDrainCleanupToken::parse("short").is_none());
        assert_eq!(format!("{token:?}"), "PaymentDrainCleanupToken(<redacted>)");
        let _ = PaymentDrainClientError::NotFound;
        let _ = PaymentDrainClientError::Conflict;
        let _ = PaymentDrainClientError::MalformedSuccess;
        let _ = PaymentDrainClientError::Transport;
        let _ = PaymentDrainClientError::Server;
    }
}
