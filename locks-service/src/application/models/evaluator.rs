use locks_core::ids::{BundleId, CreatorPubky, PubkyLockResource};
use time::OffsetDateTime;

/// Serialization-neutral identity shared by every evaluator snapshot.
///
/// This value is intentionally not `Serialize`/`Deserialize`: released graph and
/// payment snapshots keep their exact flat JSON layouts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvaluatorTaskIdentityV1 {
    pub creator: CreatorPubky,
    pub bundle_id: BundleId,
    pub pubky_lock_resource: PubkyLockResource,
    pub lock_hash: [u8; 32],
    pub reader: CreatorPubky,
    pub accepted_at: OffsetDateTime,
    pub deadline_at: OffsetDateTime,
}

/// Common identity access for version-one evaluator snapshots.
pub trait EvaluatorTaskSnapshotV1 {
    fn identity(&self) -> EvaluatorTaskIdentityV1;
}
