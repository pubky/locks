use std::str::FromStr;

use locks_core::ids::{BundleId, CreatorPubky, LockHash, LockId, PubkyLockResource, TaskId};
use locks_core::lock_policy::{GraphParticipantV2, GraphPolicyV2, GraphPredicateV2};
use locks_service::application::models::{
    GraphTaskRecord, GraphTaskSnapshot, GraphTaskSnapshotV1, VerificationTaskStatus,
};
use time::macros::datetime;

const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
const READER: &str = "pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky";
const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";

pub fn graph_task() -> GraphTaskRecord {
    GraphTaskRecord {
        task_id: TaskId::from_str("018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10").unwrap(),
        snapshot: GraphTaskSnapshot::V1(
            GraphTaskSnapshotV1::new(
                creator(),
                bundle_id(),
                resource(),
                [0; 32],
                CreatorPubky::from_str(READER).unwrap(),
                datetime!(2026-10-01 12:00:00 UTC),
                60,
                GraphPolicyV2 {
                    predicates: vec![GraphPredicateV2::Follows {
                        follower: GraphParticipantV2::Reader,
                        followed: GraphParticipantV2::Creator,
                    }],
                    operator: None,
                },
            )
            .unwrap(),
        ),
        evaluator_revision: 0,
        status: VerificationTaskStatus::Pending,
    }
}

pub fn creator() -> CreatorPubky {
    CreatorPubky::from_str(CREATOR).unwrap()
}

pub fn bundle_id() -> BundleId {
    BundleId::from_str(BUNDLE_ID).unwrap()
}

fn resource() -> PubkyLockResource {
    let lock_id = LockId::from_hash(LockHash::from_bytes([0; 32]));
    PubkyLockResource::from_str(&format!("{CREATOR}/pub/app.locks/{lock_id}.json")).unwrap()
}
