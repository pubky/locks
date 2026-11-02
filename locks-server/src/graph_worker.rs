use std::sync::Arc;
use std::time::Duration;

use locks_core::ids::TaskId;
use locks_service::application::errors::ApplicationError;
use locks_service::application::graph0conf_reducer::reduce_graph_snapshot;
use locks_service::application::models::{
    GraphObservationOutcome, GraphRootState, GraphTaskSnapshot,
};
use locks_service::application::ports::{Clock, GraphTaskClaimer};
use tokio::sync::Semaphore;

use crate::nexus_http_client::{NexusGraphVerifier, NexusObservation};

const FALSE_REPOLL_SECONDS: u64 = 5;
const OPERATIONAL_BACKOFF_SECONDS: [u64; 6] = [1, 2, 4, 8, 16, 30];
const PER_TASK_CONCURRENCY: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphWorkerTick {
    Idle,
    Persisted(TaskId),
    RetryScheduled(TaskId),
    Terminal(TaskId),
    ClaimLost,
}

pub trait GraphRetryJitter: Send + Sync {
    fn jitter(&self, base: Duration) -> Duration;
}

#[derive(Debug, Default)]
pub struct OsGraphRetryJitter;

impl GraphRetryJitter for OsGraphRetryJitter {
    fn jitter(&self, base: Duration) -> Duration {
        let percent = 80 + u64::from(rand::random::<u8>() % 41);
        Duration::from_millis(
            u64::try_from(base.as_millis())
                .unwrap_or(u64::MAX)
                .saturating_mul(percent)
                / 100,
        )
    }
}

pub struct GraphVerificationWorker {
    claimer: Arc<dyn GraphTaskClaimer>,
    verifier: Arc<dyn NexusGraphVerifier>,
    clock: Arc<dyn Clock>,
    jitter: Arc<dyn GraphRetryJitter>,
    worker_id: String,
    claim_timeout: time::Duration,
    per_task_permits: Arc<Semaphore>,
}

impl GraphVerificationWorker {
    pub fn new(
        claimer: Arc<dyn GraphTaskClaimer>,
        verifier: Arc<dyn NexusGraphVerifier>,
        clock: Arc<dyn Clock>,
        jitter: Arc<dyn GraphRetryJitter>,
        worker_id: String,
        claim_timeout: time::Duration,
    ) -> Self {
        Self {
            claimer,
            verifier,
            clock,
            jitter,
            worker_id,
            claim_timeout,
            per_task_permits: Arc::new(Semaphore::new(PER_TASK_CONCURRENCY)),
        }
    }

    pub async fn run_once(&self) -> Result<GraphWorkerTick, ApplicationError> {
        let claim_now = self.clock.now();
        let Some(claim) = self
            .claimer
            .claim_next_graph_task(&self.worker_id, claim_now, claim_now + self.claim_timeout)
            .await?
        else {
            return Ok(GraphWorkerTick::Idle);
        };
        let task_id = claim.task.task_id;
        let expected_revision = claim.task.evaluator_revision;
        let claim_token = claim.claim_token;
        let mut snapshot = claim.task.snapshot.clone();
        let deadline = snapshot.as_v1().deadline_at;
        let dispatch_now = self.clock.now();

        if dispatch_now >= deadline {
            reduce_graph_snapshot(snapshot_v1_mut(&mut snapshot), dispatch_now)
                .map_err(invalid_snapshot)?;
            return self
                .persist_terminal_or_pending(
                    task_id,
                    claim_token,
                    expected_revision,
                    snapshot,
                    dispatch_now,
                )
                .await;
        }

        let due: Vec<_> = snapshot
            .as_v1()
            .predicates
            .iter()
            .enumerate()
            .filter(|(_, state)| {
                state.first_true_observation.is_none()
                    && !state.terminal_invalid
                    && state.next_due_at.is_none_or(|due| due <= dispatch_now)
            })
            .map(|(index, state)| (index, state.predicate.clone()))
            .take(PER_TASK_CONCURRENCY)
            .collect();

        if due.is_empty() {
            let next = next_due(snapshot.as_v1(), dispatch_now);
            return self
                .schedule_retry(
                    task_id,
                    claim_token,
                    expected_revision,
                    snapshot,
                    dispatch_now,
                    next,
                )
                .await;
        }

        let creator = snapshot.as_v1().creator.clone();
        let reader = snapshot.as_v1().reader.clone();
        let mut set = tokio::task::JoinSet::new();
        for (index, predicate) in due {
            // Recheck after backpressure. Waiting for a permit must never turn into late I/O.
            let permit = self
                .per_task_permits
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| ApplicationError::Storage {
                    message: "graph per-task semaphore closed".to_owned(),
                })?;
            if self.clock.now() >= deadline {
                drop(permit);
                break;
            }
            let verifier = Arc::clone(&self.verifier);
            let creator = creator.clone();
            let reader = reader.clone();
            set.spawn(async move {
                let _permit = permit;
                let observation = verifier
                    .verify_predicate(&predicate, &creator, &reader, deadline)
                    .await;
                (index, observation)
            });
        }

        while let Some(result) = set.join_next().await {
            let (index, observation) = result.map_err(|error| ApplicationError::Storage {
                message: format!("graph verifier task failed: {error}"),
            })?;
            let observed_at = self.clock.now();
            if observed_at > deadline {
                continue;
            }
            apply_observation(
                snapshot_v1_mut(&mut snapshot),
                index,
                observation,
                observed_at,
                deadline,
                self.jitter.as_ref(),
            )?;
        }

        let reduced_at = self.clock.now();
        reduce_graph_snapshot(snapshot_v1_mut(&mut snapshot), reduced_at)
            .map_err(invalid_snapshot)?;
        self.persist_terminal_or_pending(
            task_id,
            claim_token,
            expected_revision,
            snapshot,
            reduced_at,
        )
        .await
    }

    pub async fn run_until_shutdown(
        &self,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
        poll_interval: Duration,
    ) -> Result<(), ApplicationError> {
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            let tick = self.run_once().await?;
            if tick == GraphWorkerTick::Idle {
                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            return Ok(());
                        }
                    }
                    _ = tokio::time::sleep(poll_interval) => {}
                }
            }
        }
    }

    async fn persist_terminal_or_pending(
        &self,
        task_id: TaskId,
        claim_token: uuid::Uuid,
        expected_revision: u64,
        snapshot: GraphTaskSnapshot,
        now: time::OffsetDateTime,
    ) -> Result<GraphWorkerTick, ApplicationError> {
        if snapshot.as_v1().root != GraphRootState::Pending {
            let persisted = self
                .claimer
                .persist_claimed_graph_snapshot(
                    &task_id,
                    &self.worker_id,
                    &claim_token,
                    now,
                    expected_revision,
                    snapshot,
                )
                .await?;
            return Ok(if persisted.is_some() {
                GraphWorkerTick::Terminal(task_id)
            } else {
                GraphWorkerTick::ClaimLost
            });
        }
        let due = next_due(snapshot.as_v1(), now);
        self.schedule_retry(task_id, claim_token, expected_revision, snapshot, now, due)
            .await
    }

    async fn schedule_retry(
        &self,
        task_id: TaskId,
        claim_token: uuid::Uuid,
        expected_revision: u64,
        snapshot: GraphTaskSnapshot,
        now: time::OffsetDateTime,
        next_attempt_at: time::OffsetDateTime,
    ) -> Result<GraphWorkerTick, ApplicationError> {
        let persisted = self
            .claimer
            .schedule_graph_task_retry(
                &task_id,
                &self.worker_id,
                &claim_token,
                now,
                expected_revision,
                snapshot,
                next_attempt_at,
            )
            .await?;
        Ok(if persisted.is_some() {
            GraphWorkerTick::RetryScheduled(task_id)
        } else {
            GraphWorkerTick::ClaimLost
        })
    }
}

fn apply_observation(
    snapshot: &mut locks_service::application::models::GraphTaskSnapshotV1,
    index: usize,
    observation: NexusObservation,
    observed_at: time::OffsetDateTime,
    deadline: time::OffsetDateTime,
    jitter: &dyn GraphRetryJitter,
) -> Result<(), ApplicationError> {
    match observation {
        NexusObservation::Valid {
            value,
            origin,
            status,
        } => {
            let next_due = if value {
                None
            } else {
                Some(clamp_due(
                    observed_at,
                    jitter.jitter(Duration::from_secs(FALSE_REPOLL_SECONDS)),
                    deadline,
                ))
            };
            snapshot
                .record_nexus_observation(
                    index,
                    if value {
                        GraphObservationOutcome::True
                    } else {
                        GraphObservationOutcome::False
                    },
                    observed_at,
                    origin,
                    Some(status),
                    next_due,
                    0,
                )
                .map_err(invalid_snapshot)
        }
        NexusObservation::Invalid { origin, status, .. } => snapshot
            .terminalize_invalid_nexus_response(index, observed_at, origin, Some(status))
            .map_err(invalid_snapshot),
        NexusObservation::Operational {
            origin,
            status,
            retry_after_seconds,
            ..
        } => {
            let failures = snapshot.predicates[index]
                .operational_failure_count
                .saturating_add(1);
            let honored_retry_after = retry_after_seconds.filter(|seconds| {
                time::Duration::seconds(*seconds as i64) <= deadline - observed_at
            });
            let base = honored_retry_after.unwrap_or_else(|| {
                OPERATIONAL_BACKOFF_SECONDS[usize::try_from(failures.saturating_sub(1))
                    .unwrap_or(usize::MAX)
                    .min(OPERATIONAL_BACKOFF_SECONDS.len() - 1)]
            });
            let delay = if honored_retry_after.is_some() {
                Duration::from_secs(base.min(60))
            } else {
                jitter.jitter(Duration::from_secs(base))
            };
            let next_due = clamp_due(observed_at, delay, deadline);
            snapshot
                .record_nexus_observation(
                    index,
                    GraphObservationOutcome::DependencyUnavailable,
                    observed_at,
                    origin,
                    status,
                    Some(next_due),
                    failures,
                )
                .map_err(invalid_snapshot)
        }
    }
}

fn next_due(
    snapshot: &locks_service::application::models::GraphTaskSnapshotV1,
    now: time::OffsetDateTime,
) -> time::OffsetDateTime {
    snapshot
        .predicates
        .iter()
        .filter(|state| state.first_true_observation.is_none() && !state.terminal_invalid)
        .filter_map(|state| state.next_due_at)
        .min()
        .unwrap_or(snapshot.deadline_at)
        .max(now + time::Duration::milliseconds(1))
        .min(snapshot.deadline_at)
}

fn clamp_due(
    now: time::OffsetDateTime,
    delay: Duration,
    deadline: time::OffsetDateTime,
) -> time::OffsetDateTime {
    let milliseconds = i64::try_from(delay.as_millis()).unwrap_or(i64::MAX);
    (now + time::Duration::milliseconds(milliseconds)).min(deadline)
}

fn snapshot_v1_mut(
    snapshot: &mut GraphTaskSnapshot,
) -> &mut locks_service::application::models::GraphTaskSnapshotV1 {
    match snapshot {
        GraphTaskSnapshot::V1(snapshot) => snapshot,
    }
}

fn invalid_snapshot(message: String) -> ApplicationError {
    ApplicationError::Storage {
        message: format!("invalid graph worker snapshot: {message}"),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::str::FromStr;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use locks_core::ids::{BundleId, CreatorPubky, LockHash, LockId, PubkyLockResource, TaskId};
    use locks_core::lock_policy::{GraphParticipantV2, GraphPolicyV2, GraphPredicateV2};
    use locks_service::application::models::{
        GraphTaskRecord, GraphTaskSnapshotV1, VerificationTaskStatus,
    };
    use locks_service::application::ports::GraphTaskRepository;
    use locks_service::infrastructure::memory::graph_tasks::InMemoryGraphTaskRepository;
    use time::macros::datetime;

    use super::*;

    const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
    const READER: &str = "pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky";
    const NOW: time::OffsetDateTime = datetime!(2026-10-01 12:00:00 UTC);

    #[derive(Debug)]
    struct FixedJitter;

    impl GraphRetryJitter for FixedJitter {
        fn jitter(&self, base: Duration) -> Duration {
            base
        }
    }

    #[derive(Debug)]
    struct MutableClock(Mutex<time::OffsetDateTime>);

    impl MutableClock {
        fn set(&self, now: time::OffsetDateTime) {
            *self.0.lock().unwrap() = now;
        }
    }

    impl Clock for MutableClock {
        fn now(&self) -> time::OffsetDateTime {
            *self.0.lock().unwrap()
        }
    }

    struct FakeNexus {
        responses: Mutex<VecDeque<NexusObservation>>,
        calls: std::sync::atomic::AtomicUsize,
    }

    #[async_trait]
    impl NexusGraphVerifier for FakeNexus {
        async fn verify_predicate(
            &self,
            _predicate: &GraphPredicateV2,
            _creator: &CreatorPubky,
            _reader: &CreatorPubky,
            _not_after: time::OffsetDateTime,
        ) -> NexusObservation {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.responses.lock().unwrap().pop_front().unwrap()
        }
    }

    #[test]
    fn retry_backoff_and_jitter_contract_is_bounded() {
        for base in OPERATIONAL_BACKOFF_SECONDS {
            let low = Duration::from_millis(base * 800);
            let high = Duration::from_millis(base * 1_200);
            for _ in 0..100 {
                let value = OsGraphRetryJitter.jitter(Duration::from_secs(base));
                assert!(value >= low && value <= high);
            }
        }
        assert_eq!(
            FixedJitter.jitter(Duration::from_secs(5)),
            Duration::from_secs(5)
        );
    }

    #[tokio::test]
    async fn false_repolls_then_true_is_sticky_and_persisted() {
        let repository = Arc::new(InMemoryGraphTaskRepository::new());
        repository.insert_graph_task(task()).await.unwrap();
        let clock = Arc::new(MutableClock(Mutex::new(NOW)));
        let verifier = Arc::new(FakeNexus {
            responses: Mutex::new(VecDeque::from([
                NexusObservation::Valid {
                    value: false,
                    origin: "https://nexus.example".to_owned(),
                    status: 200,
                },
                NexusObservation::Valid {
                    value: true,
                    origin: "https://nexus.example".to_owned(),
                    status: 200,
                },
            ])),
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let worker = GraphVerificationWorker::new(
            repository.clone(),
            verifier.clone(),
            clock.clone(),
            Arc::new(FixedJitter),
            "graph-worker".to_owned(),
            time::Duration::seconds(30),
        );

        assert!(matches!(
            worker.run_once().await.unwrap(),
            GraphWorkerTick::RetryScheduled(_)
        ));
        assert_eq!(worker.run_once().await.unwrap(), GraphWorkerTick::Idle);
        clock.set(NOW + time::Duration::seconds(5));
        assert!(matches!(
            worker.run_once().await.unwrap(),
            GraphWorkerTick::Terminal(_)
        ));
        assert_eq!(verifier.calls.load(std::sync::atomic::Ordering::SeqCst), 2);

        let stored = repository
            .get_graph_task_by_handle(&creator(), &bundle_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.snapshot.as_v1().root, GraphRootState::Satisfied);
        let evidence = stored.snapshot.as_v1().predicates[0]
            .first_true_observation
            .as_ref()
            .unwrap();
        assert_eq!(evidence.origin.as_deref(), Some("https://nexus.example"));
    }

    #[tokio::test]
    async fn deadline_reached_dispatches_no_nexus_io() {
        let repository = Arc::new(InMemoryGraphTaskRepository::new());
        repository.insert_graph_task(task()).await.unwrap();
        let clock = Arc::new(MutableClock(Mutex::new(NOW + time::Duration::seconds(60))));
        let verifier = Arc::new(FakeNexus {
            responses: Mutex::new(VecDeque::new()),
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let worker = GraphVerificationWorker::new(
            repository,
            verifier.clone(),
            clock,
            Arc::new(FixedJitter),
            "graph-worker".to_owned(),
            time::Duration::seconds(30),
        );

        assert!(matches!(
            worker.run_once().await.unwrap(),
            GraphWorkerTick::Terminal(_)
        ));
        assert_eq!(verifier.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    fn task() -> GraphTaskRecord {
        let lock_id = LockId::from_hash(LockHash::from_bytes([0; 32]));
        GraphTaskRecord {
            task_id: TaskId::from_str("018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10").unwrap(),
            snapshot: GraphTaskSnapshot::V1(
                GraphTaskSnapshotV1::new(
                    creator(),
                    bundle_id(),
                    PubkyLockResource::from_str(&format!("{CREATOR}/pub/app.locks/{lock_id}.json"))
                        .unwrap(),
                    [0; 32],
                    CreatorPubky::from_str(READER).unwrap(),
                    NOW,
                    60,
                    GraphPolicyV2 {
                        predicates: vec![GraphPredicateV2::Follows {
                            follower: GraphParticipantV2::Reader,
                            followed: GraphParticipantV2::Creator,
                        }],
                        operator: None,
                    },
                )
                .unwrap(),
            ),
            evaluator_revision: 0,
            status: VerificationTaskStatus::Pending,
        }
    }

    fn creator() -> CreatorPubky {
        CreatorPubky::from_str(CREATOR).unwrap()
    }

    fn bundle_id() -> BundleId {
        BundleId::from_str("000G40R40M30E209185GR38E1W").unwrap()
    }
}
