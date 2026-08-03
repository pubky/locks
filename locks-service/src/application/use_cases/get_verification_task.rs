use time::OffsetDateTime;

use locks_core::ids::{BundleId, CreatorPubky, TaskId};

use crate::application::errors::ApplicationError;
use crate::application::models::{VerificationTaskRecord, VerificationTaskStatus};
use crate::application::ports::VerificationTaskRepository;

/// Request to read verification task state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GetVerificationTaskRequest {
    /// Server-generated operational task ID.
    pub task_id: TaskId,
}

/// Public verification attempt handle used by content viewers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerificationTaskHandle {
    /// Creator whose content lock is being verified.
    pub creator: CreatorPubky,
    /// Viewer-generated durable bundle identifier for this verification attempt.
    pub bundle_id: BundleId,
}

/// Request to read public verification task lifecycle state by handle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetVerificationTaskByHandleRequest {
    /// Creator whose content lock is being verified.
    pub creator: CreatorPubky,
    /// Viewer-generated durable bundle identifier for this verification attempt.
    pub bundle_id: BundleId,
}

/// Public lifecycle view of verification task state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerificationTaskLifecycleView {
    /// Creator whose content lock is being verified.
    pub creator: CreatorPubky,
    /// Viewer-generated durable bundle identifier for this verification attempt.
    pub bundle_id: BundleId,
    /// Current lifecycle status.
    pub status: VerificationTaskStatus,
    /// Timestamp when the task was accepted.
    pub submitted_at: OffsetDateTime,
    /// Timestamp when the task was dispatched to verifier work.
    pub started_at: Option<OffsetDateTime>,
    /// Timestamp when the task reached a terminal successful/failed/expired state.
    pub completed_at: Option<OffsetDateTime>,
    /// Viewer-safe failure detail for failed tasks.
    pub failure_message: Option<String>,
}

impl From<VerificationTaskRecord> for VerificationTaskLifecycleView {
    fn from(task: VerificationTaskRecord) -> Self {
        Self {
            creator: task.creator,
            bundle_id: task.submitted_proof_bundle.bundle_id,
            status: task.status,
            submitted_at: task.submitted_at,
            started_at: task.started_at,
            completed_at: task.completed_at,
            failure_message: task.failure_message,
        }
    }
}

/// Read-only view of verification task state for internal callers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerificationTaskView {
    /// Server-generated operational task ID.
    pub task_id: TaskId,
    /// Current lifecycle status.
    pub status: VerificationTaskStatus,
    /// Timestamp when the task was accepted.
    pub submitted_at: OffsetDateTime,
    /// Timestamp when the task was dispatched to verifier work.
    pub started_at: Option<OffsetDateTime>,
    /// Timestamp when the task reached a terminal successful/failed/expired state.
    pub completed_at: Option<OffsetDateTime>,
    /// Persisted failure detail for failed tasks.
    pub failure_message: Option<String>,
}

/// Read-only verification task polling use case.
pub struct GetVerificationTaskUseCase<'a> {
    tasks: &'a dyn VerificationTaskRepository,
}

impl<'a> GetVerificationTaskUseCase<'a> {
    /// Creates a get-task use case from its repository port.
    pub fn new(tasks: &'a dyn VerificationTaskRepository) -> Self {
        Self { tasks }
    }

    /// Loads a verification task by ID and returns a side-effect-free view.
    pub async fn execute(
        &self,
        request: GetVerificationTaskRequest,
    ) -> Result<VerificationTaskView, ApplicationError> {
        let task = self
            .tasks
            .get_verification_task(&request.task_id)
            .await?
            .ok_or(ApplicationError::MissingRecord {
                record: "verification_task",
            })?;

        Ok(VerificationTaskView {
            task_id: task.task_id,
            status: task.status,
            submitted_at: task.submitted_at,
            started_at: task.started_at,
            completed_at: task.completed_at,
            failure_message: task.failure_message,
        })
    }
}

/// Read-only verification task polling use case by public handle.
pub struct GetVerificationTaskByHandleUseCase<'a> {
    tasks: &'a dyn VerificationTaskRepository,
}

impl<'a> GetVerificationTaskByHandleUseCase<'a> {
    /// Creates a get-task-by-handle use case from its repository port.
    pub fn new(tasks: &'a dyn VerificationTaskRepository) -> Self {
        Self { tasks }
    }

    /// Loads a verification task by public handle and returns a side-effect-free lifecycle view.
    pub async fn execute(
        &self,
        request: GetVerificationTaskByHandleRequest,
    ) -> Result<VerificationTaskLifecycleView, ApplicationError> {
        let task = self
            .tasks
            .get_verification_task_by_handle(&request.creator, &request.bundle_id)
            .await?
            .ok_or(ApplicationError::MissingRecord {
                record: "verification_task",
            })?;

        Ok(VerificationTaskLifecycleView::from(task))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use serde_json::json;
    use time::macros::datetime;

    use locks_core::ids::{BundleId, CreatorPubky, PubkyLockResource, TaskId};
    use locks_core::lock_policy::VerifierType;
    use locks_core::verification::{Proof, SUBMITTED_PROOF_BUNDLE_VERSION, SubmittedProofBundle};

    use super::*;
    use crate::application::models::VerificationTaskRecord;

    const TASK_ID: &str = "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10";
    const LOCK_ID: &str = "000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG";

    #[tokio::test]
    async fn get_verification_task_returns_read_only_view() {
        let task = verification_task()
            .transition_to(
                VerificationTaskStatus::InProgress,
                datetime!(2026-05-29 12:01:00 UTC),
                None,
            )
            .unwrap();
        let repo = FakeTaskRepository::new(Some(task));
        let use_case = GetVerificationTaskUseCase::new(&repo);

        let view = use_case
            .execute(GetVerificationTaskRequest {
                task_id: TaskId::from_str(TASK_ID).unwrap(),
            })
            .await
            .unwrap();

        assert_eq!(view.task_id, TaskId::from_str(TASK_ID).unwrap());
        assert_eq!(view.status, VerificationTaskStatus::InProgress);
        assert_eq!(view.submitted_at, datetime!(2026-05-29 12:00:00 UTC));
        assert_eq!(view.started_at, Some(datetime!(2026-05-29 12:01:00 UTC)));
        assert_eq!(view.completed_at, None);
        assert_eq!(view.failure_message, None);
        assert_eq!(repo.update_count(), 0);
        assert_eq!(repo.delete_count(), 0);
    }

    #[tokio::test]
    async fn get_verification_task_returns_missing_record_when_absent() {
        let repo = FakeTaskRepository::new(None);
        let use_case = GetVerificationTaskUseCase::new(&repo);

        let result = use_case
            .execute(GetVerificationTaskRequest {
                task_id: TaskId::from_str(TASK_ID).unwrap(),
            })
            .await;

        assert_eq!(
            result,
            Err(ApplicationError::MissingRecord {
                record: "verification_task",
            })
        );
        assert_eq!(repo.update_count(), 0);
        assert_eq!(repo.delete_count(), 0);
    }

    #[test]
    fn public_lifecycle_view_replaces_task_id_with_creator_and_bundle_id() {
        let task = verification_task()
            .transition_to(
                VerificationTaskStatus::InProgress,
                datetime!(2026-05-29 12:01:00 UTC),
                None,
            )
            .unwrap();

        let view = VerificationTaskLifecycleView::from(task);

        assert_eq!(
            view.creator,
            CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy")
                .unwrap()
        );
        assert_eq!(
            view.bundle_id,
            BundleId::from_str("000G40R40M30E209185GR38E1W").unwrap()
        );
        assert_eq!(view.status, VerificationTaskStatus::InProgress);
        assert_eq!(view.submitted_at, datetime!(2026-05-29 12:00:00 UTC));
        assert_eq!(view.started_at, Some(datetime!(2026-05-29 12:01:00 UTC)));
        assert_eq!(view.completed_at, None);
        assert_eq!(view.failure_message, None);
        let debug = format!("{view:?}");
        assert!(!debug.contains(TASK_ID));
        assert!(!debug.contains("submitted_proof_bundle"));
        assert!(!debug.contains("satisfied"));
    }

    #[tokio::test]
    async fn get_verification_task_by_handle_returns_public_lifecycle_view() {
        let task = verification_task()
            .transition_to(
                VerificationTaskStatus::InProgress,
                datetime!(2026-05-29 12:01:00 UTC),
                None,
            )
            .unwrap();
        let repo = FakeTaskRepository::new(Some(task));
        let use_case = GetVerificationTaskByHandleUseCase::new(&repo);

        let view = use_case
            .execute(GetVerificationTaskByHandleRequest {
                creator: CreatorPubky::from_str(
                    "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
                )
                .unwrap(),
                bundle_id: BundleId::from_str("000G40R40M30E209185GR38E1W").unwrap(),
            })
            .await
            .unwrap();

        assert_eq!(
            view.creator,
            CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy")
                .unwrap()
        );
        assert_eq!(
            view.bundle_id,
            BundleId::from_str("000G40R40M30E209185GR38E1W").unwrap()
        );
        assert_eq!(view.status, VerificationTaskStatus::InProgress);
        assert_eq!(view.submitted_at, datetime!(2026-05-29 12:00:00 UTC));
        assert_eq!(view.started_at, Some(datetime!(2026-05-29 12:01:00 UTC)));
        assert_eq!(view.completed_at, None);
        assert_eq!(view.failure_message, None);
        assert_eq!(repo.update_count(), 0);
        assert_eq!(repo.delete_count(), 0);
    }

    #[tokio::test]
    async fn get_verification_task_by_handle_returns_missing_record_when_absent() {
        let repo = FakeTaskRepository::new(None);
        let use_case = GetVerificationTaskByHandleUseCase::new(&repo);

        let result = use_case
            .execute(GetVerificationTaskByHandleRequest {
                creator: CreatorPubky::from_str(
                    "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
                )
                .unwrap(),
                bundle_id: BundleId::from_str("000G40R40M30E209185GR38E1W").unwrap(),
            })
            .await;

        assert_eq!(
            result,
            Err(ApplicationError::MissingRecord {
                record: "verification_task",
            })
        );
        assert_eq!(repo.update_count(), 0);
        assert_eq!(repo.delete_count(), 0);
    }

    fn verification_task() -> VerificationTaskRecord {
        VerificationTaskRecord {
            task_id: TaskId::from_str(TASK_ID).unwrap(),
            creator: CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap(),
            submitted_proof_bundle: SubmittedProofBundle {
                version: SUBMITTED_PROOF_BUNDLE_VERSION,
                bundle_id: BundleId::from_str("000G40R40M30E209185GR38E1W").unwrap(),
                pubky_lock_resource: PubkyLockResource::from_str(&format!(
                    "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/locks.app/{LOCK_ID}.json"
                ))
                .unwrap(),
                reader_public_key: None,
                proofs: vec![Proof {
                    criterion_id: "criterion-1".to_owned(),
                    verifier_type: VerifierType::DevStatic,
                    payload: json!({ "satisfied": true }),
                }],
            },
            status: VerificationTaskStatus::Pending,
            submitted_at: datetime!(2026-05-29 12:00:00 UTC),
            started_at: None,
            completed_at: None,
            failure_message: None,
        }
    }

    #[derive(Default)]
    struct FakeTaskRepository {
        task: Option<VerificationTaskRecord>,
        update_count: Mutex<u32>,
        delete_count: Mutex<u32>,
    }

    impl FakeTaskRepository {
        fn new(task: Option<VerificationTaskRecord>) -> Self {
            Self {
                task,
                update_count: Mutex::new(0),
                delete_count: Mutex::new(0),
            }
        }

        fn update_count(&self) -> u32 {
            *self.update_count.lock().unwrap()
        }

        fn delete_count(&self) -> u32 {
            *self.delete_count.lock().unwrap()
        }
    }

    #[async_trait]
    impl VerificationTaskRepository for FakeTaskRepository {
        async fn insert_verification_task(
            &self,
            _task: VerificationTaskRecord,
        ) -> Result<(), ApplicationError> {
            unreachable!("get use case must not insert tasks")
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
            task_id: &TaskId,
        ) -> Result<Option<VerificationTaskRecord>, ApplicationError> {
            Ok(self.task.clone().filter(|task| &task.task_id == task_id))
        }

        async fn get_verification_task_by_handle(
            &self,
            creator: &CreatorPubky,
            bundle_id: &BundleId,
        ) -> Result<Option<VerificationTaskRecord>, ApplicationError> {
            Ok(self.task.clone().filter(|task| {
                &task.creator == creator && &task.submitted_proof_bundle.bundle_id == bundle_id
            }))
        }

        async fn delete_verification_task(
            &self,
            _task_id: &TaskId,
        ) -> Result<(), ApplicationError> {
            *self.delete_count.lock().unwrap() += 1;
            Ok(())
        }
    }
}
