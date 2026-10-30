use std::str::FromStr;
use std::sync::Arc;

use locks_core::ids::TaskId;

use crate::application::errors::ApplicationError;
use crate::application::models::{
    FoldedAuthAttempt, FoldedAuthStateHash, FoldedAuthStatus, GraphTaskRecord,
};
use crate::application::ports::{
    Clock, FoldedAuthApprovalPoll, FoldedAuthApprovalPoller, FoldedAuthPollCall,
    FoldedAuthRepository,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PollFoldedAuthOutcome {
    Pending(FoldedAuthAttempt),
    Accepted(GraphTaskRecord),
    RestartRequired(FoldedAuthAttempt),
    Expired(FoldedAuthAttempt),
    Stale,
}

pub struct PollFoldedAuthAttempt<R: ?Sized, P: ?Sized> {
    repository: Arc<R>,
    poller: Arc<P>,
    clock: Arc<dyn Clock>,
}

impl<R, P> PollFoldedAuthAttempt<R, P>
where
    R: FoldedAuthRepository + ?Sized,
    P: FoldedAuthApprovalPoller + ?Sized,
{
    pub fn new(repository: Arc<R>, poller: Arc<P>, clock: Arc<dyn Clock>) -> Self {
        Self {
            repository,
            poller,
            clock,
        }
    }

    pub async fn execute(
        &self,
        state_hash: &FoldedAuthStateHash,
    ) -> Result<PollFoldedAuthOutcome, ApplicationError> {
        let before_poll = self.clock.now();
        let Some(attempt) = self.repository.get_by_state_hash(state_hash).await? else {
            return Ok(PollFoldedAuthOutcome::Stale);
        };
        match attempt.status {
            FoldedAuthStatus::Expired => return Ok(PollFoldedAuthOutcome::Expired(attempt)),
            FoldedAuthStatus::RestartRequired => {
                return Ok(PollFoldedAuthOutcome::RestartRequired(attempt));
            }
            FoldedAuthStatus::Accepted => {
                return Ok(
                    match self.repository.accepted_graph_task(state_hash).await? {
                        Some(task) => PollFoldedAuthOutcome::Accepted(task),
                        None => PollFoldedAuthOutcome::Stale,
                    },
                );
            }
            FoldedAuthStatus::Pending => {}
        }
        let generation = attempt.generation;
        if attempt.poll_dispatched {
            return Ok(
                match self
                    .repository
                    .require_restart(state_hash, generation, before_poll)
                    .await?
                {
                    Some(attempt) if attempt.status == FoldedAuthStatus::Expired => {
                        PollFoldedAuthOutcome::Expired(attempt)
                    }
                    Some(attempt) if attempt.status == FoldedAuthStatus::RestartRequired => {
                        PollFoldedAuthOutcome::RestartRequired(attempt)
                    }
                    _ => PollFoldedAuthOutcome::Stale,
                },
            );
        }
        if attempt.expires_at <= before_poll {
            return Ok(
                match self
                    .repository
                    .expire(state_hash, generation, before_poll)
                    .await?
                {
                    Some(attempt) if attempt.status == FoldedAuthStatus::Expired => {
                        PollFoldedAuthOutcome::Expired(attempt)
                    }
                    _ => PollFoldedAuthOutcome::Stale,
                },
            );
        }
        let Some(restored) = self.repository.restore_pending(state_hash).await? else {
            return Ok(PollFoldedAuthOutcome::Stale);
        };
        if restored.attempt.generation != generation {
            return Ok(PollFoldedAuthOutcome::Stale);
        }
        if self
            .repository
            .mark_poll_dispatched(state_hash, generation, before_poll)
            .await?
            .is_none()
        {
            return Ok(PollFoldedAuthOutcome::Stale);
        }

        // No repository transaction or row lock is held across this awaited SDK/relay call.
        let polled = self.poller.poll_approval(&restored.flow_secret).await;
        let result_time = self.clock.now();
        match polled {
            FoldedAuthPollCall::NotAttempted(error) => {
                if self
                    .repository
                    .confirm_not_attempted(state_hash, generation, result_time)
                    .await?
                    .is_none()
                {
                    return Ok(PollFoldedAuthOutcome::Stale);
                }
                Err(error)
            }
            FoldedAuthPollCall::Attempted(Ok(FoldedAuthApprovalPoll::Pending)) => Ok(
                match self
                    .repository
                    .confirm_pending_generation(state_hash, generation, result_time)
                    .await?
                {
                    Some(attempt) if attempt.status == FoldedAuthStatus::Expired => {
                        PollFoldedAuthOutcome::Expired(attempt)
                    }
                    Some(attempt) if attempt.status == FoldedAuthStatus::Pending => {
                        PollFoldedAuthOutcome::Pending(attempt)
                    }
                    _ => PollFoldedAuthOutcome::Stale,
                },
            ),
            FoldedAuthPollCall::Attempted(Err(_)) => Ok(
                match self
                    .repository
                    .require_restart(state_hash, generation, result_time)
                    .await?
                {
                    Some(attempt) if attempt.status == FoldedAuthStatus::Expired => {
                        PollFoldedAuthOutcome::Expired(attempt)
                    }
                    Some(attempt) if attempt.status == FoldedAuthStatus::RestartRequired => {
                        PollFoldedAuthOutcome::RestartRequired(attempt)
                    }
                    _ => PollFoldedAuthOutcome::Stale,
                },
            ),
            FoldedAuthPollCall::Attempted(Ok(FoldedAuthApprovalPoll::Approved { reader })) => {
                let task_id = TaskId::from_str(&uuid::Uuid::new_v4().to_string())
                    .expect("UUID v4 is a valid task id");
                Ok(
                    match self
                        .repository
                        .accept_and_insert_graph_task(
                            state_hash,
                            generation,
                            task_id,
                            reader,
                            result_time,
                        )
                        .await?
                    {
                        Some(task) => PollFoldedAuthOutcome::Accepted(task),
                        None => match self.repository.get_by_state_hash(state_hash).await? {
                            Some(attempt) if attempt.status == FoldedAuthStatus::Expired => {
                                PollFoldedAuthOutcome::Expired(attempt)
                            }
                            _ => PollFoldedAuthOutcome::Stale,
                        },
                    },
                )
            }
        }
    }
}
