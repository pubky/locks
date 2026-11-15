use async_trait::async_trait;
use locks_core::ids::{BundleId, CreatorPubky, PubkyLockResource, TaskId};
use locks_core::verification::VerifiedProofBundle;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::application::errors::ApplicationError;
use crate::application::models::{
    MixedRootState, MixedTaskSnapshot, PaymentProviderStatus, PaymentTaskSnapshot,
    VerificationTaskStatus, VerificationTerminalReason,
};

use super::{
    PaykitAuthority, PaymentDrainCleanupPreparation, PaymentDrainSummary,
    PaymentProviderCreateResponse,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PaymentDrainObligationKind {
    PaykitV1 {
        criterion_id: String,
        invoice_created_at: OffsetDateTime,
        payment_deadline: OffsetDateTime,
    },
    ProviderV2 {
        evaluator_revision: u64,
        snapshot: Box<ProviderV2EvaluatorSnapshot>,
        reconciliation_terminal: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderV2EvaluatorSnapshot {
    Payment(PaymentTaskSnapshot),
    Mixed(MixedTaskSnapshot),
}

impl ProviderV2EvaluatorSnapshot {
    pub fn payment_snapshot(&self) -> Result<PaymentTaskSnapshot, ApplicationError> {
        match self {
            Self::Payment(snapshot) => Ok(snapshot.clone()),
            Self::Mixed(snapshot) => snapshot
                .as_v1()
                .payment_snapshot()
                .map(PaymentTaskSnapshot::V1)
                .map_err(|message| ApplicationError::InvalidContentLockDeletionState { message }),
        }
    }

    pub fn with_payment_snapshot(
        &self,
        payment: PaymentTaskSnapshot,
    ) -> Result<Self, ApplicationError> {
        match self {
            Self::Payment(_) => Ok(Self::Payment(payment)),
            Self::Mixed(snapshot) => {
                if snapshot.as_v1().root != MixedRootState::Pending {
                    return Ok(Self::Mixed(snapshot.clone()));
                }
                let mut mixed = snapshot.clone();
                let frozen_payment_root = mixed.as_v1().payment.root;
                mixed
                    .as_v1_mut()
                    .replace_payment_snapshot(payment.as_v1().clone());
                mixed.as_v1_mut().payment.root = frozen_payment_root;
                mixed.as_v1().validate().map_err(|message| {
                    ApplicationError::InvalidContentLockDeletionState { message }
                })?;
                Ok(Self::Mixed(mixed))
            }
        }
    }

    pub fn to_json(&self) -> Result<serde_json::Value, serde_json::Error> {
        match self {
            Self::Payment(snapshot) => snapshot.to_json(),
            Self::Mixed(snapshot) => snapshot.to_json(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaymentDrainObligation {
    pub task_id: TaskId,
    pub creator: CreatorPubky,
    pub bundle_id: BundleId,
    pub lock_resource: PubkyLockResource,
    pub kind: PaymentDrainObligationKind,
    pub status: VerificationTaskStatus,
    pub entitlement_to_publish: Option<VerifiedProofBundle>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderV2DeletionReconciliation {
    pub create_conflict: bool,
    pub create_response: Option<PaymentProviderCreateResponse>,
    pub status: Option<PaymentProviderStatus>,
    pub terminal_snapshot: ProviderV2EvaluatorSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PaymentDrainTerminalTransition {
    pub status: VerificationTaskStatus,
    pub entitlement_publication_token: Option<Uuid>,
    pub terminal_reason: Option<VerificationTerminalReason>,
}

#[async_trait]
pub trait PaymentDrainRepository: Send + Sync {
    async fn store_payment_drain(
        &self,
        deletion_job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        summary: &PaymentDrainSummary,
    ) -> Result<bool, ApplicationError>;

    async fn get_payment_drain(
        &self,
        deletion_job_id: Uuid,
    ) -> Result<Option<PaymentDrainSummary>, ApplicationError>;

    async fn reconcile_payment_drain(
        &self,
        deletion_job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        summary: &PaymentDrainSummary,
    ) -> Result<bool, ApplicationError>;

    async fn store_provider_v2_fence(
        &self,
        deletion_job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        lock_resource: &PubkyLockResource,
        authority: &PaykitAuthority,
    ) -> Result<bool, ApplicationError>;

    async fn prepare_provider_v2_fence(
        &self,
        deletion_job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        lock_resource: &PubkyLockResource,
        authority: &PaykitAuthority,
    ) -> Result<bool, ApplicationError>;

    async fn list_obligations(
        &self,
        deletion_job_id: Uuid,
    ) -> Result<Vec<PaymentDrainObligation>, ApplicationError>;

    async fn begin_entitlement_publication(
        &self,
        deletion_job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        task_id: &TaskId,
        entitlement: &VerifiedProofBundle,
    ) -> Result<Option<Uuid>, ApplicationError>;

    async fn persist_terminal_obligation(
        &self,
        deletion_job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        task_id: &TaskId,
        transition: PaymentDrainTerminalTransition,
    ) -> Result<bool, ApplicationError>;

    async fn persist_provider_v2_terminal(
        &self,
        deletion_job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        task_id: &TaskId,
        reconciliation: &ProviderV2DeletionReconciliation,
    ) -> Result<bool, ApplicationError>;

    async fn persist_provider_v2_binding(
        &self,
        deletion_job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        task_id: &TaskId,
        create_conflict: bool,
        response: Option<PaymentProviderCreateResponse>,
    ) -> Result<bool, ApplicationError> {
        let _ = (
            deletion_job_id,
            worker_id,
            claim_token,
            task_id,
            create_conflict,
            response,
        );
        Err(ApplicationError::InvalidContentLockDeletionState {
            message: "provider-v2 binding checkpoints are not supported by this repository"
                .to_owned(),
        })
    }

    async fn persist_provider_v2_status(
        &self,
        deletion_job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        task_id: &TaskId,
        status: Option<&PaymentProviderStatus>,
    ) -> Result<bool, ApplicationError> {
        let _ = (deletion_job_id, worker_id, claim_token, task_id, status);
        Err(ApplicationError::InvalidContentLockDeletionState {
            message: "provider-v2 status checkpoints are not supported by this repository"
                .to_owned(),
        })
    }

    async fn all_obligations_terminal(
        &self,
        deletion_job_id: Uuid,
    ) -> Result<bool, ApplicationError>;

    async fn prepare_payment_drain_cleanup(
        &self,
        deletion_job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        lock_resource: &PubkyLockResource,
        authority: &PaykitAuthority,
    ) -> Result<PaymentDrainCleanupPreparation, ApplicationError> {
        let _ = (
            deletion_job_id,
            worker_id,
            claim_token,
            lock_resource,
            authority,
        );
        Err(ApplicationError::InvalidContentLockDeletionState {
            message: "payment drain cleanup is not supported by this repository".to_owned(),
        })
    }

    async fn persist_payment_drain_cleanup(
        &self,
        deletion_job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        cleanup_token: &super::PaymentDrainCleanupToken,
    ) -> Result<bool, ApplicationError> {
        let _ = (deletion_job_id, worker_id, claim_token, cleanup_token);
        Err(ApplicationError::InvalidContentLockDeletionState {
            message: "payment drain cleanup is not supported by this repository".to_owned(),
        })
    }
}
