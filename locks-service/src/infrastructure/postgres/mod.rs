//! Postgres infrastructure adapters for Lock Server private runtime state.
//!
//! This module owns Postgres-backed repositories, worker coordination adapters,
//! and migration helpers for Lock-Server-private runtime state. Pubky-owned
//! domain resources such as content locks, guarded resources, and verified proof
//! bundles intentionally remain behind their existing ports until Pubky-backed
//! adapters or explicit production indexes are designed.

pub mod access_credentials;
pub mod creator_authority;
pub mod creator_connect_flows;
pub mod errors;
pub mod frontend_sessions;
pub mod migrations;
#[cfg(test)]
pub(crate) mod testing;
pub mod verification_task_claims;
pub mod verification_tasks;

pub use access_credentials::PostgresAccessCredentialStore;
pub use creator_authority::{CreatorAuthoritySecretCipher, PostgresCreatorAuthorityStore};
pub use creator_connect_flows::PostgresCreatorConnectFlowStore;
pub use errors::PostgresError;
pub use frontend_sessions::{PostgresFrontendSessionCodeStore, PostgresFrontendSessionStore};
pub use migrations::run_migrations;
pub use verification_task_claims::PostgresVerificationTaskClaimer;
pub use verification_tasks::PostgresVerificationTaskRepository;
