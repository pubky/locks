use async_trait::async_trait;
use locks_core::ids::{BundleId, CreatorPubky, PubkyLockResource, TaskId};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::application::errors::ApplicationError;
use crate::application::models::VerificationTaskStatus;

use super::PaymentDrainSummary;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaymentDrainObligation {
    pub task_id: TaskId,
    pub creator: CreatorPubky,
    pub bundle_id: BundleId,
    pub lock_resource: PubkyLockResource,
    pub criterion_id: String,
    pub invoice_created_at: OffsetDateTime,
    pub payment_deadline: OffsetDateTime,
    pub status: VerificationTaskStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PaymentDrainTerminalTransition {
    pub status: VerificationTaskStatus,
    pub entitlement_publication_token: Option<Uuid>,
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
    ) -> Result<Option<Uuid>, ApplicationError>;

    async fn persist_terminal_obligation(
        &self,
        deletion_job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        task_id: &TaskId,
        transition: PaymentDrainTerminalTransition,
    ) -> Result<bool, ApplicationError>;

    async fn all_obligations_terminal(
        &self,
        deletion_job_id: Uuid,
    ) -> Result<bool, ApplicationError>;
}
