use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use locks_core::ids::TaskId;

use crate::application::errors::ApplicationError;
use crate::application::models::{
    FoldedAuthAttempt, FoldedAuthEvaluatorMode, FoldedAuthStateHash, FoldedAuthStatus,
    GraphTaskRecord, MixedTaskRecord,
};
use crate::application::ports::{
    Clock, FoldedAuthApprovalPoll, FoldedAuthApprovalPoller, FoldedAuthPollCall,
    FoldedAuthRepository,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PollFoldedAuthOutcome {
    Pending(FoldedAuthAttempt),
    Accepted(GraphTaskRecord),
    AcceptedMixed(Box<MixedTaskRecord>),
    RestartRequired(FoldedAuthAttempt),
    Expired(FoldedAuthAttempt),
    Stale,
}

pub struct PollFoldedAuthAttempt<R: ?Sized, P: ?Sized> {
    repository: Arc<R>,
    poller: Arc<P>,
    clock: Arc<dyn Clock>,
    poll_lease_renewal_interval: Duration,
}

const POLL_LEASE_RENEWAL_INTERVAL: Duration = Duration::from_secs(20);

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
            poll_lease_renewal_interval: POLL_LEASE_RENEWAL_INTERVAL,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_poll_lease_renewal_interval(
        repository: Arc<R>,
        poller: Arc<P>,
        clock: Arc<dyn Clock>,
        poll_lease_renewal_interval: Duration,
    ) -> Self {
        assert!(!poll_lease_renewal_interval.is_zero());
        Self {
            repository,
            poller,
            clock,
            poll_lease_renewal_interval,
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
                return Ok(match attempt.evaluator_mode {
                    FoldedAuthEvaluatorMode::Graph => {
                        match self.repository.accepted_graph_task(state_hash).await? {
                            Some(task) => PollFoldedAuthOutcome::Accepted(task),
                            None => PollFoldedAuthOutcome::Stale,
                        }
                    }
                    FoldedAuthEvaluatorMode::Mixed => {
                        match self.repository.accepted_mixed_task(state_hash).await? {
                            Some(task) => PollFoldedAuthOutcome::AcceptedMixed(Box::new(task)),
                            None => PollFoldedAuthOutcome::Stale,
                        }
                    }
                });
            }
            FoldedAuthStatus::Pending => {}
        }
        let generation = attempt.generation;
        if attempt.poll_dispatched {
            return Ok(PollFoldedAuthOutcome::Pending(attempt));
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
        // Renewing durable ownership keeps cleanup from abandoning a still-live poll.
        let polled = {
            let poll = self.poller.poll_approval(&restored.flow_secret);
            tokio::pin!(poll);
            let renewal_delay = tokio::time::sleep(self.poll_lease_renewal_interval);
            tokio::pin!(renewal_delay);
            'polling: loop {
                tokio::select! {
                    result = &mut poll => break 'polling result,
                    _ = &mut renewal_delay => {
                        let renewal = self.repository.renew_poll_dispatch(
                            state_hash,
                            generation,
                            self.clock.now(),
                        );
                        tokio::pin!(renewal);
                        tokio::select! {
                            // Keep polling the potentially one-shot dependency call while
                            // a storage renewal waits. Every completion remains fenced by
                            // state hash and generation if ownership was already lost.
                            result = &mut poll => break 'polling result,
                            _ = &mut renewal => {
                                renewal_delay.as_mut().reset(
                                    tokio::time::Instant::now()
                                        + self.poll_lease_renewal_interval,
                                );
                            }
                        }
                    }
                }
            }
        };
        let result_time = self.clock.now();
        match polled {
            FoldedAuthPollCall::TransientBeforePoll(error) => {
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
            FoldedAuthPollCall::UnrecoverableBeforePoll => Ok(
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
                let accepted = match attempt.evaluator_mode {
                    FoldedAuthEvaluatorMode::Graph => self
                        .repository
                        .accept_and_insert_graph_task(
                            state_hash,
                            generation,
                            task_id,
                            reader,
                            result_time,
                        )
                        .await?
                        .map(PollFoldedAuthOutcome::Accepted),
                    FoldedAuthEvaluatorMode::Mixed => self
                        .repository
                        .accept_and_insert_mixed_task(
                            state_hash,
                            generation,
                            task_id,
                            reader,
                            result_time,
                        )
                        .await?
                        .map(Box::new)
                        .map(PollFoldedAuthOutcome::AcceptedMixed),
                };
                Ok(match accepted {
                    Some(outcome) => outcome,
                    None => match self.repository.get_by_state_hash(state_hash).await? {
                        Some(attempt) if attempt.status == FoldedAuthStatus::Expired => {
                            PollFoldedAuthOutcome::Expired(attempt)
                        }
                        _ => PollFoldedAuthOutcome::Stale,
                    },
                })
            }
        }
    }
}
