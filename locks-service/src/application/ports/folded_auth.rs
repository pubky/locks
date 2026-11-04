use async_trait::async_trait;
use locks_core::ids::{CreatorPubky, TaskId};
use time::OffsetDateTime;

use crate::application::errors::ApplicationError;
use crate::application::models::{
    FoldedAuthAttempt, FoldedAuthFlowSecret, FoldedAuthStartResult, FoldedAuthStateHash,
    GrantPopKeyId, GraphTaskRecord, RestoredFoldedAuthAttempt, StartFoldedAuthAttempt,
};

pub trait FoldedAuthFlowStarter: Send + Sync {
    fn start(
        &self,
        key_id: &GrantPopKeyId,
    ) -> Result<(String, FoldedAuthFlowSecret), ApplicationError>;
}

#[async_trait]
pub trait FoldedAuthRepository: Send + Sync {
    async fn get_latest_by_handle(
        &self,
        creator: &CreatorPubky,
        bundle_id: &locks_core::ids::BundleId,
    ) -> Result<Option<FoldedAuthAttempt>, ApplicationError>;

    async fn start_or_rotate(
        &self,
        request: StartFoldedAuthAttempt,
    ) -> Result<FoldedAuthStartResult, ApplicationError>;

    async fn get_by_state_hash(
        &self,
        state_hash: &FoldedAuthStateHash,
    ) -> Result<Option<FoldedAuthAttempt>, ApplicationError>;

    async fn restore_pending(
        &self,
        state_hash: &FoldedAuthStateHash,
    ) -> Result<Option<RestoredFoldedAuthAttempt>, ApplicationError>;

    async fn mark_poll_dispatched(
        &self,
        state_hash: &FoldedAuthStateHash,
        expected_generation: u64,
        now: OffsetDateTime,
    ) -> Result<Option<FoldedAuthAttempt>, ApplicationError>;

    /// Renews ownership while the exact generation's dependency poll remains live.
    async fn renew_poll_dispatch(
        &self,
        state_hash: &FoldedAuthStateHash,
        expected_generation: u64,
        now: OffsetDateTime,
    ) -> Result<Option<FoldedAuthAttempt>, ApplicationError>;

    async fn confirm_not_attempted(
        &self,
        state_hash: &FoldedAuthStateHash,
        expected_generation: u64,
        now: OffsetDateTime,
    ) -> Result<Option<FoldedAuthAttempt>, ApplicationError>;

    async fn confirm_pending_generation(
        &self,
        state_hash: &FoldedAuthStateHash,
        expected_generation: u64,
        now: OffsetDateTime,
    ) -> Result<Option<FoldedAuthAttempt>, ApplicationError>;

    async fn require_restart(
        &self,
        state_hash: &FoldedAuthStateHash,
        expected_generation: u64,
        now: OffsetDateTime,
    ) -> Result<Option<FoldedAuthAttempt>, ApplicationError>;

    async fn expire(
        &self,
        state_hash: &FoldedAuthStateHash,
        expected_generation: u64,
        now: OffsetDateTime,
    ) -> Result<Option<FoldedAuthAttempt>, ApplicationError>;

    async fn accept_and_insert_graph_task(
        &self,
        state_hash: &FoldedAuthStateHash,
        expected_generation: u64,
        task_id: TaskId,
        reader: CreatorPubky,
        now: OffsetDateTime,
    ) -> Result<Option<GraphTaskRecord>, ApplicationError>;

    async fn purge_terminal_before(&self, now: OffsetDateTime) -> Result<u64, ApplicationError>;

    async fn accepted_task_id(
        &self,
        state_hash: &FoldedAuthStateHash,
    ) -> Result<Option<TaskId>, ApplicationError> {
        Ok(self
            .get_by_state_hash(state_hash)
            .await?
            .and_then(|attempt| attempt.task_id))
    }

    async fn accepted_graph_task(
        &self,
        state_hash: &FoldedAuthStateHash,
    ) -> Result<Option<GraphTaskRecord>, ApplicationError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FoldedAuthApprovalPoll {
    Pending,
    Approved { reader: CreatorPubky },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("folded auth approval poll failed after dispatch")]
pub struct FoldedAuthApprovalPollError;

#[derive(Debug)]
pub enum FoldedAuthPollCall {
    TransientBeforePoll(ApplicationError),
    UnrecoverableBeforePoll,
    Attempted(Result<FoldedAuthApprovalPoll, FoldedAuthApprovalPollError>),
}

#[async_trait]
pub trait FoldedAuthApprovalPoller: Send + Sync {
    /// Adapter must mark whether Pubky approval polling was actually attempted.
    async fn poll_approval(
        &self,
        flow_secret: &crate::application::models::FoldedAuthFlowSecret,
    ) -> FoldedAuthPollCall;
}
