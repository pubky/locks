use async_trait::async_trait;
use locks_core::ids::{BundleId, CreatorPubky, TaskId};
use locks_core::lock_policy::PaymentPolicyV2;
use locks_core::verification::SubmittedProofBundleV2;

use crate::application::errors::ApplicationError;
use crate::application::models::{
    ClaimedPaymentTask, PaymentProviderCreateRequest, PaymentProviderStatus, PaymentTaskRecord,
    PaymentTaskSnapshot,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PaymentProviderCreateResponse {
    pub invoice_created_at: time::OffsetDateTime,
    pub proposal_expires_at: time::OffsetDateTime,
    pub deadline: time::OffsetDateTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaymentProviderErrorKind {
    InvalidResponse,
    Conflict,
    Retryable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PaymentProviderError {
    pub kind: PaymentProviderErrorKind,
    pub retry_after_seconds: Option<u64>,
}

#[async_trait]
pub trait PaymentProviderClient: Send + Sync {
    async fn create_invoice(
        &self,
        request: &PaymentProviderCreateRequest,
    ) -> Result<PaymentProviderCreateResponse, PaymentProviderError>;

    async fn invoice_status(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<PaymentProviderStatus, PaymentProviderError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmitPaymentTask {
    pub task_id: TaskId,
    pub submitted_proof_bundle: SubmittedProofBundleV2,
    pub lock_hash: [u8; 32],
    pub payment: PaymentPolicyV2,
    pub verification_window_seconds: u64,
}

#[async_trait]
pub trait PaymentPublicationActionGuard: Send {
    async fn complete(&mut self) -> Result<Option<PaymentTaskRecord>, ApplicationError>;
    async fn release(self: Box<Self>) -> Result<(), ApplicationError>;
}

#[async_trait]
pub trait PaymentTaskRepository: Send + Sync {
    async fn admit_payment_task(
        &self,
        request: AdmitPaymentTask,
    ) -> Result<PaymentTaskRecord, ApplicationError>;
    async fn get_payment_task_by_handle(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<Option<PaymentTaskRecord>, ApplicationError>;
}

#[async_trait]
pub trait PaymentTaskClaimer: Send + Sync {
    async fn claim_next_payment_task(
        &self,
        worker_id: &str,
        now: time::OffsetDateTime,
        claim_expires_at: time::OffsetDateTime,
    ) -> Result<Option<ClaimedPaymentTask>, ApplicationError>;
    #[allow(clippy::too_many_arguments)]
    async fn persist_claimed_payment_snapshot(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        now: time::OffsetDateTime,
        expected_revision: u64,
        snapshot: PaymentTaskSnapshot,
    ) -> Result<Option<PaymentTaskRecord>, ApplicationError>;
    #[allow(clippy::too_many_arguments)]
    async fn schedule_payment_task_retry(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        now: time::OffsetDateTime,
        expected_revision: u64,
        snapshot: PaymentTaskSnapshot,
        next_attempt_at: time::OffsetDateTime,
    ) -> Result<Option<PaymentTaskRecord>, ApplicationError>;
    async fn release_claimed_payment_task(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
    ) -> Result<bool, ApplicationError>;
    async fn acquire_payment_publication_action(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        expected_revision: u64,
        publication_token: uuid::Uuid,
    ) -> Result<Option<Box<dyn PaymentPublicationActionGuard>>, ApplicationError>;
    #[allow(clippy::too_many_arguments)]
    async fn complete_claimed_payment_publication(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        now: time::OffsetDateTime,
        expected_revision: u64,
        publication_token: uuid::Uuid,
    ) -> Result<Option<PaymentTaskRecord>, ApplicationError>;
}
