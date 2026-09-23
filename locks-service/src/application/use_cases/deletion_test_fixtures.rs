use std::{collections::BTreeMap, str::FromStr};

use locks_core::{
    ids::{CreatorPubky, GuardedResourceHash},
    lock_policy::{
        AccessPolicy, CONTENT_LOCK_VERSION, ContentLock, GuardedResource, LockLogic,
        LockServerConfig, SecondaryGuardedResource,
    },
};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::application::models::{
    ClaimedContentLockDeletionJob, ContentLockDeletionJob, ContentLockDeletionPhase,
};

pub(super) fn claimed_deletion_job(
    now: OffsetDateTime,
    phase: ContentLockDeletionPhase,
    force: bool,
) -> ClaimedContentLockDeletionJob {
    let mut secondary_resources = BTreeMap::new();
    for path in [
        "/priv/locks.app/content/z",
        "/priv/locks.app/content/a",
        "/priv/locks.app/content/m",
    ] {
        secondary_resources.insert(
            path.to_owned(),
            SecondaryGuardedResource {
                hash: GuardedResourceHash::from_bytes([7; 32]),
                content_type: "text/plain".to_owned(),
                size: 1,
            },
        );
    }
    let lock = ContentLock {
        version: CONTENT_LOCK_VERSION,
        creator: CreatorPubky::from_str(
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
        )
        .unwrap(),
        primary_resource: Some(
            GuardedResource::new(
                "/priv/locks.app/content/m",
                GuardedResourceHash::from_bytes([8; 32]),
                "text/plain",
                1,
            )
            .unwrap(),
        ),
        secondary_resources,
        criteria: vec![],
        lock_logic: LockLogic::All { criteria: vec![] },
        access_policy: AccessPolicy {
            requested_credential_ttl_seconds: 900,
        },
        lock_server: LockServerConfig { override_: None },
        created_at: now,
    };
    let mut job = ContentLockDeletionJob::new(Uuid::from_u128(1), lock, now).unwrap();
    job.phase = phase;
    job.force_requested_at = force.then_some(now);
    ClaimedContentLockDeletionJob {
        job,
        claim_token: Uuid::from_u128(2),
    }
}
