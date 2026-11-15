//! Postgres infrastructure adapters for Lock Server private runtime state.
//!
//! This module owns Postgres-backed repositories, worker coordination adapters,
//! and migration helpers for Lock-Server-private runtime state. Pubky-owned
//! domain resources such as content locks, guarded resources, and verified proof
//! bundles intentionally remain behind their existing ports until Pubky-backed
//! adapters or explicit production indexes are designed.

pub mod access_credentials;
pub mod content_lock_deletion_action_ownership;
pub mod content_lock_deletions;
pub mod content_lock_ownership;
pub mod creator_authority;
pub mod creator_connect_flows;
pub mod errors;
mod evaluator_tasks;
pub mod folded_auth;
pub mod frontend_sessions;
pub mod graph_tasks;
pub mod migrations;
pub mod mixed_tasks;
pub mod payment_drains;
pub mod payment_tasks;
mod proof_admission;
#[cfg(test)]
pub(crate) mod testing;
pub mod verification_task_claims;
pub mod verification_tasks;

pub use access_credentials::PostgresAccessCredentialStore;
pub use content_lock_deletion_action_ownership::PostgresContentLockDeletionActionOwnership;
pub use content_lock_deletions::PostgresContentLockDeletionRepository;
pub use content_lock_ownership::PostgresContentLockOwnershipRepository;
pub use creator_authority::{CreatorAuthoritySecretCipher, PostgresCreatorAuthorityStore};
pub use creator_connect_flows::PostgresCreatorConnectFlowStore;
pub use errors::PostgresError;
pub use folded_auth::PostgresFoldedAuthRepository;
pub use frontend_sessions::{PostgresFrontendSessionCodeStore, PostgresFrontendSessionStore};
pub use graph_tasks::PostgresGraphTaskRepository;
pub use migrations::run_migrations;
pub use mixed_tasks::PostgresMixedTaskRepository;
pub use payment_drains::PostgresPaymentDrainRepository;
pub use payment_tasks::PostgresPaymentTaskRepository;
pub use verification_task_claims::PostgresVerificationTaskClaimer;
pub use verification_tasks::PostgresVerificationTaskRepository;
