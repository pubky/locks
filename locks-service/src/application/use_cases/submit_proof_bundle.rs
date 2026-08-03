use locks_core::verification::SubmittedProofBundle;

use crate::application::errors::ApplicationError;
use crate::application::models::{VerificationTaskRecord, VerificationTaskStatus};
use crate::application::ports::{Clock, VerificationTaskIdGenerator, VerificationTaskRepository};
use crate::application::use_cases::get_verification_task::VerificationTaskLifecycleView;

/// Request to create a server-owned verification task from submitted proof material.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmitProofBundleRequest {
    /// Viewer-submitted proof material for the content lock.
    pub submitted_proof_bundle: SubmittedProofBundle,
}

/// Result returned after creating or finding a verification task.
pub type SubmittedVerificationTask = VerificationTaskLifecycleView;

/// Creates pending verification tasks from submitted proof bundles.
pub struct SubmitProofBundleUseCase<'a> {
    task_ids: &'a dyn VerificationTaskIdGenerator,
    tasks: &'a dyn VerificationTaskRepository,
    clock: &'a dyn Clock,
}

impl<'a> SubmitProofBundleUseCase<'a> {
    /// Creates a submit-proof use case from its application ports.
    pub fn new(
        task_ids: &'a dyn VerificationTaskIdGenerator,
        tasks: &'a dyn VerificationTaskRepository,
        clock: &'a dyn Clock,
    ) -> Self {
        Self {
            task_ids,
            tasks,
            clock,
        }
    }

    /// Returns an existing lifecycle for an exact submission replay.
    ///
    /// A changed submission under the same public handle conflicts. `None` means the handle is
    /// available for invoice creation and task insertion.
    pub async fn find_existing(
        &self,
        submitted_proof_bundle: &SubmittedProofBundle,
    ) -> Result<Option<SubmittedVerificationTask>, ApplicationError> {
        let creator = submitted_proof_bundle.pubky_lock_resource.creator();
        let bundle_id = &submitted_proof_bundle.bundle_id;
        let Some(existing) = self
            .tasks
            .get_verification_task_by_handle(creator, bundle_id)
            .await?
        else {
            return Ok(None);
        };

        if existing.submitted_proof_bundle == *submitted_proof_bundle {
            Ok(Some(VerificationTaskLifecycleView::from(existing)))
        } else {
            Err(ApplicationError::VerificationTaskConflict)
        }
    }

    /// Creates or finds a pending verification task and returns public lifecycle state.
    pub async fn execute(
        &self,
        request: SubmitProofBundleRequest,
    ) -> Result<SubmittedVerificationTask, ApplicationError> {
        let submitted_proof_bundle = request.submitted_proof_bundle;
        if let Some(existing) = self.find_existing(&submitted_proof_bundle).await? {
            return Ok(existing);
        }
        let creator = submitted_proof_bundle.pubky_lock_resource.creator().clone();

        let task_id = self.task_ids.generate_task_id().await?;
        let submitted_at = self.clock.now();
        let task = VerificationTaskRecord {
            task_id,
            creator,
            submitted_proof_bundle,
            status: VerificationTaskStatus::Pending,
            submitted_at,
            started_at: None,
            completed_at: None,
            failure_message: None,
        };

        match self.tasks.insert_verification_task(task.clone()).await {
            Ok(()) => Ok(VerificationTaskLifecycleView::from(task)),
            Err(ApplicationError::DuplicateRecord {
                record: "verification_task",
            }) => {
                let existing = self
                    .tasks
                    .get_verification_task_by_handle(
                        &task.creator,
                        &task.submitted_proof_bundle.bundle_id,
                    )
                    .await?
                    .ok_or(ApplicationError::DuplicateRecord {
                        record: "verification_task",
                    })?;
                if existing.submitted_proof_bundle == task.submitted_proof_bundle {
                    Ok(VerificationTaskLifecycleView::from(existing))
                } else {
                    Err(ApplicationError::VerificationTaskConflict)
                }
            }
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use serde_json::json;
    use time::OffsetDateTime;
    use time::macros::datetime;

    use locks_core::ids::{BundleId, CreatorPubky, PubkyLockResource, TaskId};
    use locks_core::lock_policy::VerifierType;
    use locks_core::verification::{Proof, SUBMITTED_PROOF_BUNDLE_VERSION, SubmittedProofBundle};

    use super::*;

    const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";
    const LOCK_ID: &str = "000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG";
    const TASK_ID: &str = "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10";

    #[tokio::test]
    async fn submit_proof_bundle_creates_pending_lifecycle_when_handle_is_new() {
        let task_ids = FixedTaskIdGenerator::new(TaskId::from_str(TASK_ID).unwrap());
        let tasks = CapturingTaskRepository::default();
        let clock = FixedClock::new(datetime!(2026-05-29 12:00:00 UTC));
        let use_case = SubmitProofBundleUseCase::new(&task_ids, &tasks, &clock);
        let submitted_proof_bundle = submitted_proof_bundle();

        let result = use_case
            .execute(SubmitProofBundleRequest {
                submitted_proof_bundle: submitted_proof_bundle.clone(),
            })
            .await
            .unwrap();

        assert_eq!(result.creator, creator());
        assert_eq!(result.bundle_id, BundleId::from_str(BUNDLE_ID).unwrap());
        assert_eq!(result.status, VerificationTaskStatus::Pending);
        assert_eq!(result.submitted_at, datetime!(2026-05-29 12:00:00 UTC));
        assert_eq!(result.started_at, None);
        assert_eq!(result.completed_at, None);
        assert_eq!(result.failure_message, None);
        assert_eq!(task_ids.generate_count(), 1);
        let inserted = tasks.inserted_task();
        assert_eq!(inserted.task_id, TaskId::from_str(TASK_ID).unwrap());
        assert_eq!(inserted.creator, creator());
        assert_eq!(inserted.submitted_proof_bundle, submitted_proof_bundle);
        assert_eq!(inserted.status, VerificationTaskStatus::Pending);
        assert_eq!(inserted.submitted_at, result.submitted_at);
        assert_eq!(inserted.started_at, None);
        assert_eq!(inserted.completed_at, None);
        assert_eq!(inserted.failure_message, None);
    }

    #[tokio::test]
    async fn submit_proof_bundle_returns_existing_lifecycle_for_identical_resubmission() {
        let existing = verification_task_with_bundle(submitted_proof_bundle())
            .transition_to(
                VerificationTaskStatus::InProgress,
                datetime!(2026-05-29 12:01:00 UTC),
                None,
            )
            .unwrap();
        let task_ids = FixedTaskIdGenerator::new(TaskId::from_str(TASK_ID).unwrap());
        let tasks = CapturingTaskRepository::with_existing(existing);
        let clock = FixedClock::new(datetime!(2026-05-29 12:05:00 UTC));
        let use_case = SubmitProofBundleUseCase::new(&task_ids, &tasks, &clock);

        let result = use_case
            .execute(SubmitProofBundleRequest {
                submitted_proof_bundle: submitted_proof_bundle(),
            })
            .await
            .unwrap();

        assert_eq!(result.creator, creator());
        assert_eq!(result.bundle_id, BundleId::from_str(BUNDLE_ID).unwrap());
        assert_eq!(result.status, VerificationTaskStatus::InProgress);
        assert_eq!(result.submitted_at, datetime!(2026-05-29 12:00:00 UTC));
        assert_eq!(result.started_at, Some(datetime!(2026-05-29 12:01:00 UTC)));
        assert_eq!(result.completed_at, None);
        assert_eq!(result.failure_message, None);
        assert_eq!(task_ids.generate_count(), 0);
        assert_eq!(tasks.insert_count(), 0);
        assert_eq!(tasks.update_count(), 0);
    }

    #[tokio::test]
    async fn submit_proof_bundle_conflicts_when_existing_handle_has_different_proof_bundle() {
        let existing = verification_task_with_bundle(submitted_proof_bundle());
        let task_ids = FixedTaskIdGenerator::new(TaskId::from_str(TASK_ID).unwrap());
        let tasks = CapturingTaskRepository::with_existing(existing);
        let clock = FixedClock::new(datetime!(2026-05-29 12:05:00 UTC));
        let use_case = SubmitProofBundleUseCase::new(&task_ids, &tasks, &clock);

        let result = use_case
            .execute(SubmitProofBundleRequest {
                submitted_proof_bundle: submitted_proof_bundle_with_payload(json!({
                    "satisfied": false
                })),
            })
            .await;

        assert_eq!(result, Err(ApplicationError::VerificationTaskConflict));
        assert_eq!(task_ids.generate_count(), 0);
        assert_eq!(tasks.insert_count(), 0);
        assert_eq!(tasks.update_count(), 0);
    }

    #[tokio::test]
    async fn submit_proof_bundle_returns_existing_lifecycle_when_concurrent_identical_insert_wins()
    {
        let existing = verification_task_with_bundle(submitted_proof_bundle())
            .transition_to(
                VerificationTaskStatus::InProgress,
                datetime!(2026-05-29 12:01:00 UTC),
                None,
            )
            .unwrap();
        let task_ids = FixedTaskIdGenerator::new(TaskId::from_str(TASK_ID).unwrap());
        let tasks = CapturingTaskRepository::with_insert_duplicate_race(existing);
        let clock = FixedClock::new(datetime!(2026-05-29 12:05:00 UTC));
        let use_case = SubmitProofBundleUseCase::new(&task_ids, &tasks, &clock);

        let result = use_case
            .execute(SubmitProofBundleRequest {
                submitted_proof_bundle: submitted_proof_bundle(),
            })
            .await
            .unwrap();

        assert_eq!(result.status, VerificationTaskStatus::InProgress);
        assert_eq!(result.started_at, Some(datetime!(2026-05-29 12:01:00 UTC)));
        assert_eq!(task_ids.generate_count(), 1);
        assert_eq!(tasks.insert_count(), 1);
        assert_eq!(tasks.update_count(), 0);
    }

    #[tokio::test]
    async fn submit_proof_bundle_conflicts_when_concurrent_insert_has_different_proof_bundle() {
        let existing = verification_task_with_bundle(submitted_proof_bundle_with_payload(json!({
            "satisfied": false
        })));
        let task_ids = FixedTaskIdGenerator::new(TaskId::from_str(TASK_ID).unwrap());
        let tasks = CapturingTaskRepository::with_insert_duplicate_race(existing);
        let clock = FixedClock::new(datetime!(2026-05-29 12:05:00 UTC));
        let use_case = SubmitProofBundleUseCase::new(&task_ids, &tasks, &clock);

        let result = use_case
            .execute(SubmitProofBundleRequest {
                submitted_proof_bundle: submitted_proof_bundle(),
            })
            .await;

        assert_eq!(result, Err(ApplicationError::VerificationTaskConflict));
        assert_eq!(task_ids.generate_count(), 1);
        assert_eq!(tasks.insert_count(), 1);
        assert_eq!(tasks.update_count(), 0);
    }

    fn creator() -> CreatorPubky {
        CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
    }

    fn submitted_proof_bundle() -> SubmittedProofBundle {
        submitted_proof_bundle_with_payload(json!({}))
    }

    fn submitted_proof_bundle_with_payload(payload: serde_json::Value) -> SubmittedProofBundle {
        SubmittedProofBundle {
            version: SUBMITTED_PROOF_BUNDLE_VERSION,
            bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
            pubky_lock_resource: PubkyLockResource::from_str(&format!(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/locks.app/{LOCK_ID}.json"
            ))
            .unwrap(),
            reader_public_key: None,
            proofs: vec![Proof {
                criterion_id: "criterion-1".to_owned(),
                verifier_type: VerifierType::DevStatic,
                payload,
            }],
        }
    }

    fn verification_task_with_bundle(
        submitted_proof_bundle: SubmittedProofBundle,
    ) -> VerificationTaskRecord {
        VerificationTaskRecord {
            task_id: TaskId::from_str(TASK_ID).unwrap(),
            creator: creator(),
            submitted_proof_bundle,
            status: VerificationTaskStatus::Pending,
            submitted_at: datetime!(2026-05-29 12:00:00 UTC),
            started_at: None,
            completed_at: None,
            failure_message: None,
        }
    }

    struct FixedTaskIdGenerator {
        task_id: TaskId,
        generate_count: Mutex<usize>,
    }

    impl FixedTaskIdGenerator {
        fn new(task_id: TaskId) -> Self {
            Self {
                task_id,
                generate_count: Mutex::new(0),
            }
        }

        fn generate_count(&self) -> usize {
            *self.generate_count.lock().unwrap()
        }
    }

    #[async_trait]
    impl VerificationTaskIdGenerator for FixedTaskIdGenerator {
        async fn generate_task_id(&self) -> Result<TaskId, ApplicationError> {
            *self.generate_count.lock().unwrap() += 1;
            Ok(self.task_id)
        }
    }

    #[derive(Default)]
    struct CapturingTaskRepository {
        existing: Mutex<Option<VerificationTaskRecord>>,
        inserted: Mutex<Option<VerificationTaskRecord>>,
        insert_count: Mutex<usize>,
        update_count: Mutex<usize>,
        insert_duplicate_race: Mutex<Option<VerificationTaskRecord>>,
    }

    impl CapturingTaskRepository {
        fn with_existing(existing: VerificationTaskRecord) -> Self {
            Self {
                existing: Mutex::new(Some(existing)),
                inserted: Mutex::new(None),
                insert_count: Mutex::new(0),
                update_count: Mutex::new(0),
                insert_duplicate_race: Mutex::new(None),
            }
        }

        fn with_insert_duplicate_race(existing: VerificationTaskRecord) -> Self {
            Self {
                existing: Mutex::new(None),
                inserted: Mutex::new(None),
                insert_count: Mutex::new(0),
                update_count: Mutex::new(0),
                insert_duplicate_race: Mutex::new(Some(existing)),
            }
        }

        fn inserted_task(&self) -> VerificationTaskRecord {
            self.inserted.lock().unwrap().clone().unwrap()
        }

        fn insert_count(&self) -> usize {
            *self.insert_count.lock().unwrap()
        }

        fn update_count(&self) -> usize {
            *self.update_count.lock().unwrap()
        }
    }

    #[async_trait]
    impl VerificationTaskRepository for CapturingTaskRepository {
        async fn insert_verification_task(
            &self,
            task: VerificationTaskRecord,
        ) -> Result<(), ApplicationError> {
            *self.insert_count.lock().unwrap() += 1;
            if let Some(existing) = self.insert_duplicate_race.lock().unwrap().take() {
                *self.existing.lock().unwrap() = Some(existing);
                return Err(ApplicationError::DuplicateRecord {
                    record: "verification_task",
                });
            }
            *self.inserted.lock().unwrap() = Some(task.clone());
            *self.existing.lock().unwrap() = Some(task);
            Ok(())
        }

        async fn update_verification_task(
            &self,
            _task: VerificationTaskRecord,
        ) -> Result<(), ApplicationError> {
            *self.update_count.lock().unwrap() += 1;
            Ok(())
        }

        async fn get_verification_task(
            &self,
            _task_id: &TaskId,
        ) -> Result<Option<VerificationTaskRecord>, ApplicationError> {
            unreachable!()
        }

        async fn get_verification_task_by_handle(
            &self,
            creator: &CreatorPubky,
            bundle_id: &BundleId,
        ) -> Result<Option<VerificationTaskRecord>, ApplicationError> {
            Ok(self.existing.lock().unwrap().clone().filter(|task| {
                &task.creator == creator && &task.submitted_proof_bundle.bundle_id == bundle_id
            }))
        }

        async fn delete_verification_task(
            &self,
            _task_id: &TaskId,
        ) -> Result<(), ApplicationError> {
            unreachable!()
        }
    }

    struct FixedClock {
        now: OffsetDateTime,
    }

    impl FixedClock {
        fn new(now: OffsetDateTime) -> Self {
            Self { now }
        }
    }

    impl Clock for FixedClock {
        fn now(&self) -> OffsetDateTime {
            self.now
        }
    }
}
