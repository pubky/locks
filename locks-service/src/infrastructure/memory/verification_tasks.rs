use std::collections::HashMap;
use std::sync::{RwLock as SyncRwLock, Weak};

use async_trait::async_trait;
use tokio::sync::RwLock;

use locks_core::ids::{BundleId, CreatorPubky, TaskId};

use crate::application::errors::ApplicationError;
use crate::application::models::{
    ClaimedInvoiceAdmission, INVOICE_ADMISSION_INTENT_VERSION, InvoiceAdmissionIntentV1,
    InvoiceAdmissionPhase, InvoiceAdmissionRecord, InvoiceAdmissionRetryReason,
    VerificationTaskRecord, VerificationTaskStatus,
};
use crate::application::ports::{InvoiceAdmissionRepository, VerificationTaskRepository};
use crate::infrastructure::memory::verification_task_claims::InMemoryVerificationTaskClaimer;

/// In-memory verification task repository.
#[derive(Debug, Default)]
pub struct InMemoryVerificationTaskRepository {
    records: RwLock<HashMap<TaskId, VerificationTaskRecord>>,
    invoice_admissions: RwLock<HashMap<TaskId, MemoryInvoiceAdmission>>,
    verification_task_claimer: SyncRwLock<Option<Weak<InMemoryVerificationTaskClaimer>>>,
}

#[derive(Debug, Clone)]
struct MemoryInvoiceAdmission {
    record: InvoiceAdmissionRecord,
    claimed_by: Option<String>,
    claim_token: Option<uuid::Uuid>,
    claim_expires_at: Option<time::OffsetDateTime>,
}

impl InMemoryVerificationTaskRepository {
    /// Creates an empty repository.
    pub fn new() -> Self {
        Self::default()
    }

    /// Connects tasks created here to the paired in-memory verification claimer.
    pub fn attach_verification_task_claimer(&self, claimer: Weak<InMemoryVerificationTaskClaimer>) {
        *self.verification_task_claimer.write().unwrap() = Some(claimer);
    }

    fn verification_task_claimer(&self) -> Option<std::sync::Arc<InMemoryVerificationTaskClaimer>> {
        self.verification_task_claimer
            .read()
            .unwrap()
            .as_ref()
            .and_then(Weak::upgrade)
    }
}

#[async_trait]
impl VerificationTaskRepository for InMemoryVerificationTaskRepository {
    async fn insert_verification_task(
        &self,
        task: VerificationTaskRecord,
    ) -> Result<(), ApplicationError> {
        let mut records = self.records.write().await;
        if records.contains_key(&task.task_id)
            || records.values().any(|existing| {
                existing.creator == task.creator
                    && existing.submitted_proof_bundle.bundle_id
                        == task.submitted_proof_bundle.bundle_id
            })
        {
            return Err(ApplicationError::DuplicateRecord {
                record: "verification_task",
            });
        }
        records.insert(task.task_id, task.clone());
        drop(records);
        if let Some(claimer) = self.verification_task_claimer() {
            claimer
                .register_task(task, InvoiceAdmissionPhase::Ready)
                .await;
        }
        Ok(())
    }

    async fn update_verification_task(
        &self,
        task: VerificationTaskRecord,
    ) -> Result<(), ApplicationError> {
        let mut records = self.records.write().await;
        if !records.contains_key(&task.task_id) {
            return Err(ApplicationError::MissingRecord {
                record: "verification_task",
            });
        }
        records.insert(task.task_id, task);
        Ok(())
    }

    async fn get_verification_task(
        &self,
        task_id: &TaskId,
    ) -> Result<Option<VerificationTaskRecord>, ApplicationError> {
        Ok(self.records.read().await.get(task_id).cloned())
    }

    async fn get_verification_task_by_handle(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<Option<VerificationTaskRecord>, ApplicationError> {
        Ok(self
            .records
            .read()
            .await
            .values()
            .find(|task| {
                &task.creator == creator && &task.submitted_proof_bundle.bundle_id == bundle_id
            })
            .cloned())
    }

    async fn delete_verification_task(&self, task_id: &TaskId) -> Result<(), ApplicationError> {
        self.records.write().await.remove(task_id);
        self.invoice_admissions.write().await.remove(task_id);
        if let Some(claimer) = self.verification_task_claimer() {
            claimer.remove_task(task_id).await;
        }
        Ok(())
    }
}

#[async_trait]
impl InvoiceAdmissionRepository for InMemoryVerificationTaskRepository {
    async fn insert_invoice_pending_task(
        &self,
        task: VerificationTaskRecord,
        intent: InvoiceAdmissionIntentV1,
    ) -> Result<InvoiceAdmissionRecord, ApplicationError> {
        validate_invoice_admission_inputs(&task, &intent)?;
        let mut records = self.records.write().await;
        if let Some(existing) = records.values().find(|existing| {
            existing.creator == task.creator
                && existing.submitted_proof_bundle.bundle_id
                    == task.submitted_proof_bundle.bundle_id
        }) {
            let admissions = self.invoice_admissions.read().await;
            return match admissions.get(&existing.task_id).map(|state| &state.record) {
                Some(admission)
                    if existing.submitted_proof_bundle == task.submitted_proof_bundle
                        && admission.intent == intent =>
                {
                    Ok(admission.clone())
                }
                _ => Err(ApplicationError::VerificationTaskConflict),
            };
        }
        if records.contains_key(&task.task_id) {
            return Err(ApplicationError::DuplicateRecord {
                record: "verification_task",
            });
        }

        let admission = InvoiceAdmissionRecord {
            task: task.clone(),
            phase: InvoiceAdmissionPhase::InvoicePending,
            intent,
            admission_deadline_at: task.submitted_at + time::Duration::minutes(10),
            next_attempt_at: Some(task.submitted_at),
            attempt_count: 0,
            retry_reason: None,
        };
        self.invoice_admissions.write().await.insert(
            task.task_id,
            MemoryInvoiceAdmission {
                record: admission.clone(),
                claimed_by: None,
                claim_token: None,
                claim_expires_at: None,
            },
        );
        records.insert(task.task_id, task.clone());
        drop(records);
        if let Some(claimer) = self.verification_task_claimer() {
            claimer
                .register_task(task, InvoiceAdmissionPhase::InvoicePending)
                .await;
        }
        Ok(admission)
    }

    async fn get_invoice_admission(
        &self,
        task_id: &TaskId,
    ) -> Result<Option<InvoiceAdmissionRecord>, ApplicationError> {
        Ok(self
            .invoice_admissions
            .read()
            .await
            .get(task_id)
            .map(|state| state.record.clone()))
    }

    async fn claim_next_invoice_admission(
        &self,
        worker_id: &str,
        now: time::OffsetDateTime,
        claim_ttl: time::Duration,
    ) -> Result<Option<ClaimedInvoiceAdmission>, ApplicationError> {
        let mut admissions = self.invoice_admissions.write().await;
        let Some((_, state)) = admissions
            .iter_mut()
            .filter(|(_, state)| {
                state.record.phase == InvoiceAdmissionPhase::InvoicePending
                    && state
                        .record
                        .next_attempt_at
                        .or(state.claim_expires_at)
                        .is_some_and(|claimable_at| claimable_at <= now)
                    && state
                        .claim_expires_at
                        .is_none_or(|claim_expires_at| claim_expires_at <= now)
            })
            .min_by_key(|(_, state)| {
                (
                    state.record.next_attempt_at.or(state.claim_expires_at),
                    state.record.task.submitted_at,
                    state.record.task.task_id.to_string(),
                )
            })
        else {
            return Ok(None);
        };
        let claim_token = uuid::Uuid::new_v4();
        state.claimed_by = Some(worker_id.to_owned());
        state.claim_token = Some(claim_token);
        state.claim_expires_at = Some(now + claim_ttl);
        state.record.next_attempt_at = None;
        state.record.attempt_count = state.record.attempt_count.saturating_add(1);
        Ok(Some(ClaimedInvoiceAdmission {
            admission: state.record.clone(),
            claim_token,
            deadline_expired: now >= state.record.admission_deadline_at,
        }))
    }

    async fn mark_invoice_admission_ready(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        now: time::OffsetDateTime,
    ) -> Result<Option<InvoiceAdmissionRecord>, ApplicationError> {
        let mut admissions = self.invoice_admissions.write().await;
        let Some(state) = admissions.get_mut(task_id).filter(|state| {
            state.record.phase == InvoiceAdmissionPhase::InvoicePending
                && state.record.admission_deadline_at > now
                && state.claimed_by.as_deref() == Some(worker_id)
                && state.claim_token.as_ref() == Some(claim_token)
                && state
                    .claim_expires_at
                    .is_some_and(|claim_expires_at| claim_expires_at > now)
        }) else {
            return Ok(None);
        };
        state.record.phase = InvoiceAdmissionPhase::Ready;
        state.record.next_attempt_at = None;
        state.record.retry_reason = None;
        state.claimed_by = None;
        state.claim_token = None;
        state.claim_expires_at = None;
        let record = state.record.clone();
        drop(admissions);
        if let Some(claimer) = self.verification_task_claimer() {
            claimer.mark_invoice_admission_ready(task_id).await;
        }
        Ok(Some(record))
    }

    async fn schedule_invoice_admission_retry(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        now: time::OffsetDateTime,
        retry_after: time::Duration,
        retry_reason: Option<InvoiceAdmissionRetryReason>,
    ) -> Result<Option<InvoiceAdmissionRecord>, ApplicationError> {
        let mut admissions = self.invoice_admissions.write().await;
        let Some(state) = admissions.get_mut(task_id).filter(|state| {
            state.record.phase == InvoiceAdmissionPhase::InvoicePending
                && state.record.admission_deadline_at > now
                && state.claimed_by.as_deref() == Some(worker_id)
                && state.claim_token.as_ref() == Some(claim_token)
                && state.claim_expires_at.is_some_and(|expires| expires > now)
        }) else {
            return Ok(None);
        };
        state.record.next_attempt_at =
            Some((now + retry_after).min(state.record.admission_deadline_at));
        if retry_reason.is_some() {
            state.record.retry_reason = retry_reason;
        }
        state.claimed_by = None;
        state.claim_token = None;
        state.claim_expires_at = None;
        Ok(Some(state.record.clone()))
    }

    async fn mark_invoice_admission_failed(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        now: time::OffsetDateTime,
        failure_message: &str,
    ) -> Result<Option<InvoiceAdmissionRecord>, ApplicationError> {
        let message = failure_message.trim();
        if message.is_empty() {
            return Err(ApplicationError::InvalidVerificationTaskFailureMessage);
        }
        let mut records = self.records.write().await;
        let mut admissions = self.invoice_admissions.write().await;
        let Some(state) = admissions.get_mut(task_id).filter(|state| {
            state.record.phase == InvoiceAdmissionPhase::InvoicePending
                && state.claimed_by.as_deref() == Some(worker_id)
                && state.claim_token.as_ref() == Some(claim_token)
                && state.claim_expires_at.is_some_and(|expires| expires > now)
        }) else {
            return Ok(None);
        };
        state.record.phase = InvoiceAdmissionPhase::Failed;
        state.record.task.status = VerificationTaskStatus::Failed;
        state.record.task.started_at = Some(now);
        state.record.task.completed_at = Some(now);
        state.record.task.failure_message = Some(message.to_owned());
        state.record.next_attempt_at = None;
        state.record.retry_reason = None;
        state.claimed_by = None;
        state.claim_token = None;
        state.claim_expires_at = None;
        records.insert(*task_id, state.record.task.clone());
        Ok(Some(state.record.clone()))
    }
}

fn validate_invoice_admission_inputs(
    task: &VerificationTaskRecord,
    intent: &InvoiceAdmissionIntentV1,
) -> Result<(), ApplicationError> {
    if intent.version != INVOICE_ADMISSION_INTENT_VERSION
        || task.status != VerificationTaskStatus::Pending
        || task.creator != intent.creator
        || task.submitted_proof_bundle.bundle_id != intent.bundle_id
        || task.submitted_proof_bundle.pubky_lock_resource != intent.lock_resource
        || task.submitted_proof_bundle.reader_public_key.as_ref() != Some(&intent.reader)
    {
        return Err(ApplicationError::InvalidVerificationTaskState {
            message: "invoice admission intent diverges from pending verification task".to_owned(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use serde_json::json;
    use time::macros::datetime;

    use locks_core::ids::{BundleId, CreatorPubky, PubkyLockResource};
    use locks_core::lock_policy::VerifierType;
    use locks_core::verification::{Proof, SUBMITTED_PROOF_BUNDLE_VERSION, SubmittedProofBundle};

    use super::*;
    use crate::application::models::{
        INVOICE_ADMISSION_INTENT_VERSION, InvoiceAdmissionIntentV1, InvoiceAdmissionPhase,
        VerificationTaskStatus,
    };
    use crate::application::ports::InvoiceAdmissionRepository;

    const TASK_ID: &str = "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10";
    const OTHER_TASK_ID: &str = "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d11";
    const LOCK_ID: &str = "000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG";
    const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";

    #[tokio::test]
    async fn insert_update_read_and_delete_use_explicit_semantics() {
        let repo = InMemoryVerificationTaskRepository::new();
        let task_id = TaskId::from_str(TASK_ID).unwrap();
        let pending = task(VerificationTaskStatus::Pending);
        let in_progress = pending
            .transition_to(
                VerificationTaskStatus::InProgress,
                datetime!(2026-05-29 12:01:00 UTC),
                None,
            )
            .unwrap();

        assert_eq!(repo.get_verification_task(&task_id).await.unwrap(), None);
        assert_eq!(
            repo.update_verification_task(pending.clone()).await,
            Err(ApplicationError::MissingRecord {
                record: "verification_task",
            })
        );

        repo.insert_verification_task(pending.clone())
            .await
            .unwrap();
        assert_eq!(
            repo.insert_verification_task(pending).await,
            Err(ApplicationError::DuplicateRecord {
                record: "verification_task",
            })
        );

        repo.update_verification_task(in_progress.clone())
            .await
            .unwrap();
        assert_eq!(
            repo.get_verification_task(&task_id).await.unwrap(),
            Some(in_progress)
        );

        repo.delete_verification_task(&task_id).await.unwrap();
        repo.delete_verification_task(&task_id).await.unwrap();
        assert_eq!(repo.get_verification_task(&task_id).await.unwrap(), None);
    }

    #[tokio::test]
    async fn lookup_by_handle_matches_creator_and_bundle_id() {
        let repo = InMemoryVerificationTaskRepository::new();
        let creator =
            CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy")
                .unwrap();
        let other_creator =
            CreatorPubky::from_str("pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky")
                .unwrap();
        let bundle_id = BundleId::from_str(BUNDLE_ID).unwrap();
        let task = task_with(
            TASK_ID,
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            BUNDLE_ID,
            VerificationTaskStatus::Pending,
        );
        let other_creator_task = task_with(
            OTHER_TASK_ID,
            "pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky",
            BUNDLE_ID,
            VerificationTaskStatus::Pending,
        );

        repo.insert_verification_task(other_creator_task.clone())
            .await
            .unwrap();
        repo.insert_verification_task(task.clone()).await.unwrap();

        assert_eq!(
            repo.get_verification_task_by_handle(&creator, &bundle_id)
                .await
                .unwrap(),
            Some(task)
        );
        assert_eq!(
            repo.get_verification_task_by_handle(&other_creator, &bundle_id)
                .await
                .unwrap(),
            Some(other_creator_task)
        );
        assert_eq!(
            repo.get_verification_task_by_handle(
                &CreatorPubky::from_str(
                    "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo"
                )
                .unwrap(),
                &bundle_id,
            )
            .await
            .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn insert_rejects_duplicate_public_handle_even_with_distinct_task_id() {
        let repo = InMemoryVerificationTaskRepository::new();
        let original = task_with(
            TASK_ID,
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            BUNDLE_ID,
            VerificationTaskStatus::Pending,
        );
        let duplicate_handle = task_with(
            OTHER_TASK_ID,
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            BUNDLE_ID,
            VerificationTaskStatus::Pending,
        );

        repo.insert_verification_task(original.clone())
            .await
            .unwrap();

        assert_eq!(
            repo.insert_verification_task(duplicate_handle).await,
            Err(ApplicationError::DuplicateRecord {
                record: "verification_task",
            })
        );
        assert_eq!(
            repo.get_verification_task(&TaskId::from_str(TASK_ID).unwrap())
                .await
                .unwrap(),
            Some(original)
        );
        assert_eq!(
            repo.get_verification_task(&TaskId::from_str(OTHER_TASK_ID).unwrap())
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn invoice_admission_insert_is_exactly_replayable_and_sets_ten_minute_deadline() {
        let repo = InMemoryVerificationTaskRepository::new();
        let mut task = task(VerificationTaskStatus::Pending);
        task.submitted_proof_bundle.reader_public_key = Some(
            CreatorPubky::from_str("pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky")
                .unwrap(),
        );
        let intent = invoice_intent(&task);

        let inserted = repo
            .insert_invoice_pending_task(task.clone(), intent.clone())
            .await
            .unwrap();
        let replayed = repo
            .insert_invoice_pending_task(task.clone(), intent.clone())
            .await
            .unwrap();

        assert_eq!(inserted, replayed);
        assert_eq!(inserted.task, task);
        assert_eq!(inserted.phase, InvoiceAdmissionPhase::InvoicePending);
        assert_eq!(inserted.intent, intent);
        assert_eq!(
            inserted.admission_deadline_at,
            task.submitted_at + time::Duration::minutes(10)
        );
        assert_eq!(inserted.next_attempt_at, Some(task.submitted_at));
        assert_eq!(inserted.attempt_count, 0);

        let mut changed = intent;
        changed.reader =
            CreatorPubky::from_str("pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo")
                .unwrap();
        let mut changed_task = task;
        changed_task.submitted_proof_bundle.reader_public_key = Some(changed.reader.clone());
        assert_eq!(
            repo.insert_invoice_pending_task(changed_task, changed)
                .await,
            Err(ApplicationError::VerificationTaskConflict)
        );
    }

    #[tokio::test]
    async fn invoice_admission_insert_rejects_unsupported_intent_version() {
        let repo = InMemoryVerificationTaskRepository::new();
        let mut task = task(VerificationTaskStatus::Pending);
        task.submitted_proof_bundle.reader_public_key = Some(
            CreatorPubky::from_str("pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky")
                .unwrap(),
        );
        let mut intent = invoice_intent(&task);
        intent.version = INVOICE_ADMISSION_INTENT_VERSION + 1;

        assert!(matches!(
            repo.insert_invoice_pending_task(task, intent).await,
            Err(ApplicationError::InvalidVerificationTaskState { message })
                if message.contains("intent diverges")
        ));
    }

    #[tokio::test]
    async fn invoice_admission_ready_transition_rejects_stale_claim_token() {
        let repo = InMemoryVerificationTaskRepository::new();
        let mut task = task(VerificationTaskStatus::Pending);
        task.submitted_proof_bundle.reader_public_key = Some(
            CreatorPubky::from_str("pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky")
                .unwrap(),
        );
        repo.insert_invoice_pending_task(task.clone(), invoice_intent(&task))
            .await
            .unwrap();
        let claim = repo
            .claim_next_invoice_admission("worker-a", task.submitted_at, time::Duration::minutes(1))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            repo.mark_invoice_admission_ready(
                &task.task_id,
                "worker-a",
                &uuid::Uuid::new_v4(),
                task.submitted_at,
            )
            .await
            .unwrap(),
            None
        );
        assert_eq!(
            repo.schedule_invoice_admission_retry(
                &task.task_id,
                "worker-a",
                &uuid::Uuid::new_v4(),
                task.submitted_at,
                time::Duration::seconds(5),
                None,
            )
            .await
            .unwrap(),
            None
        );
        assert_eq!(
            repo.mark_invoice_admission_failed(
                &task.task_id,
                "worker-a",
                &uuid::Uuid::new_v4(),
                task.submitted_at,
                "invoice admission failed",
            )
            .await
            .unwrap(),
            None
        );
        let ready = repo
            .mark_invoice_admission_ready(
                &task.task_id,
                "worker-a",
                &claim.claim_token,
                task.submitted_at,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ready.phase, InvoiceAdmissionPhase::Ready);
        assert_eq!(ready.task.status, VerificationTaskStatus::Pending);
        assert_eq!(ready.next_attempt_at, None);
    }

    #[tokio::test]
    async fn invoice_admission_reclaims_expired_lease_with_fresh_token() {
        let repo = InMemoryVerificationTaskRepository::new();
        let mut task = task(VerificationTaskStatus::Pending);
        task.submitted_proof_bundle.reader_public_key = Some(
            CreatorPubky::from_str("pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky")
                .unwrap(),
        );
        repo.insert_invoice_pending_task(task.clone(), invoice_intent(&task))
            .await
            .unwrap();
        let lease = time::Duration::seconds(5);
        let first = repo
            .claim_next_invoice_admission("worker-a", task.submitted_at, lease)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            repo.claim_next_invoice_admission(
                "worker-b",
                task.submitted_at + lease - time::Duration::nanoseconds(1),
                lease,
            )
            .await
            .unwrap(),
            None
        );
        let second = repo
            .claim_next_invoice_admission("worker-b", task.submitted_at + lease, lease)
            .await
            .unwrap()
            .unwrap();

        assert_ne!(second.claim_token, first.claim_token);
        assert_eq!(second.admission.attempt_count, 2);
    }

    #[tokio::test]
    async fn invoice_admission_retry_is_due_gated_and_failure_is_fenced() {
        let repo = InMemoryVerificationTaskRepository::new();
        let mut task = task(VerificationTaskStatus::Pending);
        task.submitted_proof_bundle.reader_public_key = Some(
            CreatorPubky::from_str("pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky")
                .unwrap(),
        );
        repo.insert_invoice_pending_task(task.clone(), invoice_intent(&task))
            .await
            .unwrap();
        let first = repo
            .claim_next_invoice_admission("worker-a", task.submitted_at, time::Duration::minutes(1))
            .await
            .unwrap()
            .unwrap();
        let due_at = task.submitted_at + time::Duration::seconds(5);
        let retry = repo
            .schedule_invoice_admission_retry(
                &task.task_id,
                "worker-a",
                &first.claim_token,
                task.submitted_at,
                time::Duration::seconds(5),
                None,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retry.next_attempt_at, Some(due_at));
        assert_eq!(retry.attempt_count, 1);
        assert_eq!(
            repo.claim_next_invoice_admission(
                "worker-b",
                due_at - time::Duration::nanoseconds(1),
                time::Duration::minutes(1),
            )
            .await
            .unwrap(),
            None
        );
        let second = repo
            .claim_next_invoice_admission("worker-b", due_at, time::Duration::minutes(1))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second.admission.attempt_count, 2);

        let failed = repo
            .mark_invoice_admission_failed(
                &task.task_id,
                "worker-b",
                &second.claim_token,
                due_at,
                "invoice admission failed",
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(failed.phase, InvoiceAdmissionPhase::Failed);
        assert_eq!(failed.task.status, VerificationTaskStatus::Failed);
        assert_eq!(
            failed.task.failure_message.as_deref(),
            Some("invoice admission failed")
        );
        assert_eq!(failed.task.entitlement_to_publish, None);
        assert_eq!(failed.next_attempt_at, None);
    }

    #[tokio::test]
    async fn invoice_admission_retry_keeps_reader_setup_reason_across_other_transient_errors() {
        let repo = InMemoryVerificationTaskRepository::new();
        let mut task = task(VerificationTaskStatus::Pending);
        task.submitted_proof_bundle.reader_public_key = Some(
            CreatorPubky::from_str("pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky")
                .unwrap(),
        );
        repo.insert_invoice_pending_task(task.clone(), invoice_intent(&task))
            .await
            .unwrap();
        let first = repo
            .claim_next_invoice_admission("worker-a", task.submitted_at, time::Duration::minutes(1))
            .await
            .unwrap()
            .unwrap();
        repo.schedule_invoice_admission_retry(
            &task.task_id,
            "worker-a",
            &first.claim_token,
            task.submitted_at,
            time::Duration::ZERO,
            Some(InvoiceAdmissionRetryReason::ReaderWalletSetupNeeded),
        )
        .await
        .unwrap()
        .unwrap();
        let second = repo
            .claim_next_invoice_admission("worker-b", task.submitted_at, time::Duration::minutes(1))
            .await
            .unwrap()
            .unwrap();
        let retry = repo
            .schedule_invoice_admission_retry(
                &task.task_id,
                "worker-b",
                &second.claim_token,
                task.submitted_at,
                time::Duration::seconds(1),
                None,
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            retry.retry_reason,
            Some(InvoiceAdmissionRetryReason::ReaderWalletSetupNeeded)
        );
    }

    #[tokio::test]
    async fn invoice_admission_fractional_claim_and_retry_durations_keep_precision() {
        let repo = InMemoryVerificationTaskRepository::new();
        let mut task = task(VerificationTaskStatus::Pending);
        task.submitted_proof_bundle.reader_public_key = Some(
            CreatorPubky::from_str("pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky")
                .unwrap(),
        );
        repo.insert_invoice_pending_task(task.clone(), invoice_intent(&task))
            .await
            .unwrap();

        let claim_ttl = time::Duration::milliseconds(750);
        let claim = repo
            .claim_next_invoice_admission("worker-a", task.submitted_at, claim_ttl)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            repo.invoice_admissions
                .read()
                .await
                .get(&task.task_id)
                .unwrap()
                .claim_expires_at,
            Some(task.submitted_at + claim_ttl)
        );

        let retry_started_at = task.submitted_at + time::Duration::milliseconds(250);
        let retry_after = time::Duration::milliseconds(1_250);
        let retry = repo
            .schedule_invoice_admission_retry(
                &task.task_id,
                "worker-a",
                &claim.claim_token,
                retry_started_at,
                retry_after,
                None,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retry.next_attempt_at, Some(retry_started_at + retry_after));
    }

    #[tokio::test]
    async fn expired_invoice_admission_remains_claimable_for_terminal_failure() {
        let repo = InMemoryVerificationTaskRepository::new();
        let mut task = task(VerificationTaskStatus::Pending);
        task.submitted_proof_bundle.reader_public_key = Some(
            CreatorPubky::from_str("pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky")
                .unwrap(),
        );
        let inserted = repo
            .insert_invoice_pending_task(task.clone(), invoice_intent(&task))
            .await
            .unwrap();

        let claim = repo
            .claim_next_invoice_admission(
                "worker-a",
                inserted.admission_deadline_at,
                time::Duration::minutes(1),
            )
            .await
            .unwrap()
            .expect("deadline-expired admission remains eligible for terminalization");
        assert_eq!(claim.admission.task.task_id, task.task_id);
        assert!(claim.deadline_expired);
        assert_eq!(
            repo.mark_invoice_admission_ready(
                &task.task_id,
                "worker-a",
                &claim.claim_token,
                inserted.admission_deadline_at,
            )
            .await
            .unwrap(),
            None
        );
        assert!(
            repo.mark_invoice_admission_failed(
                &task.task_id,
                "worker-a",
                &claim.claim_token,
                inserted.admission_deadline_at,
                "invoice admission deadline expired",
            )
            .await
            .unwrap()
            .is_some()
        );
    }

    fn task(status: VerificationTaskStatus) -> VerificationTaskRecord {
        task_with(
            TASK_ID,
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            BUNDLE_ID,
            status,
        )
    }

    fn task_with(
        task_id: &str,
        creator: &str,
        bundle_id: &str,
        status: VerificationTaskStatus,
    ) -> VerificationTaskRecord {
        VerificationTaskRecord {
            task_id: TaskId::from_str(task_id).unwrap(),
            creator: CreatorPubky::from_str(creator).unwrap(),
            submitted_proof_bundle: SubmittedProofBundle {
                version: SUBMITTED_PROOF_BUNDLE_VERSION,
                bundle_id: BundleId::from_str(bundle_id).unwrap(),
                pubky_lock_resource: PubkyLockResource::from_str(&format!(
                    "{creator}/pub/app.locks/{LOCK_ID}.json"
                ))
                .unwrap(),
                reader_public_key: None,
                proofs: vec![Proof {
                    criterion_id: "criterion-1".to_owned(),
                    verifier_type: VerifierType::DevStatic,
                    payload: json!({}),
                }],
            },
            status,
            submitted_at: datetime!(2026-05-29 12:00:00 UTC),
            started_at: None,
            completed_at: None,
            failure_message: None,
            terminal_reason: None,
            entitlement_to_publish: None,
        }
    }

    fn invoice_intent(task: &VerificationTaskRecord) -> InvoiceAdmissionIntentV1 {
        InvoiceAdmissionIntentV1 {
            version: INVOICE_ADMISSION_INTENT_VERSION,
            creator: task.creator.clone(),
            bundle_id: task.submitted_proof_bundle.bundle_id.clone(),
            lock_resource: task.submitted_proof_bundle.pubky_lock_resource.clone(),
            reader: CreatorPubky::from_str(
                "pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky",
            )
            .unwrap(),
        }
    }
}
