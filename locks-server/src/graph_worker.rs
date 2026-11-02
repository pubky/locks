use std::sync::Arc;
use std::time::Duration;

use locks_core::ids::TaskId;
use locks_service::application::errors::ApplicationError;
use locks_service::application::graph0conf_reducer::reduce_graph_snapshot;
use locks_service::application::models::{
    GraphObservationOutcome, GraphRootState, GraphTaskSnapshot,
};
use locks_service::application::ports::{Clock, GraphTaskClaimer};

use crate::nexus_http_client::{NexusGraphVerifier, NexusObservation, TimedNexusObservation};

const FALSE_REPOLL_SECONDS: u64 = 5;
const OPERATIONAL_BACKOFF_SECONDS: [u64; 6] = [1, 2, 4, 8, 16, 30];
const PER_TASK_CONCURRENCY: usize = 8;
const CONCURRENT_TASK_WAVES: usize = 4;

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
            if self.clock.now() >= deadline {
                break;
            }
            let verifier = Arc::clone(&self.verifier);
            let creator = creator.clone();
            let reader = reader.clone();
            set.spawn(async move {
                let observation = verifier
                    .verify_predicate(&predicate, &creator, &reader, deadline)
                    .await;
                (index, observation)
            });
        }

        while let Some(result) = set.join_next().await {
            let (index, timed) = result.map_err(|error| ApplicationError::Storage {
                message: format!("graph verifier task failed: {error}"),
            })?;
            let TimedNexusObservation {
                observation,
                observed_at,
            } = timed;
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

    async fn run_wave(&self) -> Result<[GraphWorkerTick; CONCURRENT_TASK_WAVES], ApplicationError> {
        let (first, second, third, fourth) = tokio::try_join!(
            self.run_once(),
            self.run_once(),
            self.run_once(),
            self.run_once()
        )?;
        Ok([first, second, third, fourth])
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
            let ticks = self.run_wave().await?;
            if ticks.iter().all(|tick| *tick == GraphWorkerTick::Idle) {
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
            let retry_after = retry_after_seconds.map(|seconds| seconds.min(60));
            let base = retry_after.unwrap_or_else(|| {
                OPERATIONAL_BACKOFF_SECONDS[usize::try_from(failures.saturating_sub(1))
                    .unwrap_or(usize::MAX)
                    .min(OPERATIONAL_BACKOFF_SECONDS.len() - 1)]
            });
            let delay = if retry_after.is_some() {
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
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    use locks_core::ids::{
        BundleId, CreatorPubky, LockHash, LockId, LockServerPubky, PubkyLockResource, TaskId,
    };
    use locks_core::lock_policy::{GraphParticipantV2, GraphPolicyV2, GraphPredicateV2};
    use locks_service::application::models::{
        GraphTaskRecord, GraphTaskSnapshotV1, VerificationTaskStatus,
    };
    use locks_service::application::ports::GraphTaskRepository;
    use locks_service::infrastructure::memory::graph_tasks::InMemoryGraphTaskRepository;
    use pubky_common::crypto::Keypair;
    use time::macros::datetime;
    use tokio::sync::Semaphore;

    use super::*;
    use crate::config::{NexusConfig, PaykitConfig, RuntimeEnvironment};
    use crate::testing::TestServerApp;
    use crate::worker::{
        InvoiceAdmissionWorker, InvoiceRetryJitter, InvoiceWorkerTick, VerificationWorker,
        WorkerTick,
    };

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

    impl InvoiceRetryJitter for FixedJitter {
        fn delay(&self, _attempt_count: u32) -> time::Duration {
            time::Duration::seconds(1)
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
        responses: Mutex<VecDeque<TimedNexusObservation>>,
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
        ) -> TimedNexusObservation {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.responses.lock().unwrap().pop_front().unwrap()
        }
    }

    struct CompletingNexus {
        clock: Arc<MutableClock>,
    }

    #[async_trait]
    impl NexusGraphVerifier for CompletingNexus {
        async fn verify_predicate(
            &self,
            _predicate: &GraphPredicateV2,
            _creator: &CreatorPubky,
            _reader: &CreatorPubky,
            not_after: time::OffsetDateTime,
        ) -> TimedNexusObservation {
            let completed = TimedNexusObservation {
                observation: NexusObservation::Valid {
                    value: true,
                    origin: "https://nexus.example".to_owned(),
                    status: 200,
                },
                observed_at: not_after,
            };
            self.clock.set(not_after + time::Duration::milliseconds(1));
            completed
        }
    }

    struct BlockingNexus {
        calls: std::sync::atomic::AtomicUsize,
        gate: Arc<Semaphore>,
    }

    #[async_trait]
    impl NexusGraphVerifier for BlockingNexus {
        async fn verify_predicate(
            &self,
            _predicate: &GraphPredicateV2,
            _creator: &CreatorPubky,
            _reader: &CreatorPubky,
            _not_after: time::OffsetDateTime,
        ) -> TimedNexusObservation {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.gate.acquire().await.unwrap().forget();
            TimedNexusObservation {
                observation: NexusObservation::Valid {
                    value: false,
                    origin: "https://nexus.example".to_owned(),
                    status: 200,
                },
                observed_at: NOW,
            }
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

    #[test]
    fn retry_after_is_clamped_to_remaining_window_without_backoff_fallback() {
        let mut snapshot = task().snapshot.as_v1().clone();
        let deadline = snapshot.deadline_at;
        let observed_at = deadline - time::Duration::seconds(10);
        apply_observation(
            &mut snapshot,
            0,
            NexusObservation::Operational {
                kind: crate::nexus_http_client::NexusOperationalError::RateLimited,
                origin: "https://nexus.example".to_owned(),
                status: Some(429),
                retry_after_seconds: Some(60),
            },
            observed_at,
            deadline,
            &FixedJitter,
        )
        .unwrap();
        assert_eq!(
            snapshot.predicates[0].next_due_at,
            Some(snapshot.deadline_at)
        );
    }

    #[tokio::test]
    async fn false_repolls_then_true_is_sticky_and_persisted() {
        let repository = Arc::new(InMemoryGraphTaskRepository::new());
        repository.insert_graph_task(task()).await.unwrap();
        let clock = Arc::new(MutableClock(Mutex::new(NOW)));
        let verifier = Arc::new(FakeNexus {
            responses: Mutex::new(VecDeque::from([
                TimedNexusObservation {
                    observation: NexusObservation::Valid {
                        value: false,
                        origin: "https://nexus.example".to_owned(),
                        status: 200,
                    },
                    observed_at: NOW,
                },
                TimedNexusObservation {
                    observation: NexusObservation::Valid {
                        value: true,
                        origin: "https://nexus.example".to_owned(),
                        status: 200,
                    },
                    observed_at: NOW + time::Duration::seconds(5),
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
    async fn completion_timestamp_at_deadline_is_timely_even_if_join_happens_later() {
        let repository = Arc::new(InMemoryGraphTaskRepository::new());
        repository.insert_graph_task(task()).await.unwrap();
        let clock = Arc::new(MutableClock(Mutex::new(NOW)));
        let worker = GraphVerificationWorker::new(
            repository.clone(),
            Arc::new(CompletingNexus {
                clock: clock.clone(),
            }),
            clock,
            Arc::new(FixedJitter),
            "graph-worker".to_owned(),
            time::Duration::seconds(120),
        );

        assert!(matches!(
            worker.run_once().await.unwrap(),
            GraphWorkerTick::Terminal(_)
        ));
        let stored = repository
            .get_graph_task_by_handle(&creator(), &bundle_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.snapshot.as_v1().root, GraphRootState::Satisfied);
        assert_eq!(
            stored.snapshot.as_v1().predicates[0]
                .first_true_observation
                .as_ref()
                .unwrap()
                .observed_at,
            stored.snapshot.as_v1().deadline_at
        );
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

    #[tokio::test]
    async fn four_task_waves_dispatch_eight_each_and_reach_thirty_two_concurrent_calls() {
        let repository = Arc::new(InMemoryGraphTaskRepository::new());
        for index in 0..4 {
            repository
                .insert_graph_task(eight_predicate_task(index))
                .await
                .unwrap();
        }
        let verifier = Arc::new(BlockingNexus {
            calls: std::sync::atomic::AtomicUsize::new(0),
            gate: Arc::new(Semaphore::new(0)),
        });
        let worker = Arc::new(GraphVerificationWorker::new(
            repository,
            verifier.clone(),
            Arc::new(MutableClock(Mutex::new(NOW))),
            Arc::new(FixedJitter),
            "graph-worker".to_owned(),
            time::Duration::seconds(30),
        ));
        let running = {
            let worker = worker.clone();
            tokio::spawn(async move { worker.run_wave().await })
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while verifier.calls.load(std::sync::atomic::Ordering::SeqCst) < 32 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all four task waves should dispatch concurrently");
        assert_eq!(verifier.calls.load(std::sync::atomic::Ordering::SeqCst), 32);
        verifier.gate.add_permits(32);
        assert_eq!(running.await.unwrap().unwrap().len(), 4);
    }

    #[tokio::test]
    async fn graph_only_task_in_production_worker_composition_makes_zero_paykit_calls() {
        let paykit_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let secret_dir = tempfile::tempdir().unwrap();
        let secret_path = secret_dir.path().join("lock-server.sess");
        std::fs::write(
            &secret_path,
            format!("keypair-seed:{}", URL_SAFE_NO_PAD.encode([9_u8; 32])),
        )
        .unwrap();
        let mut config = TestServerApp::default_in_memory_config();
        config.runtime.environment = RuntimeEnvironment::Production;
        config.credentials.lock_server_secret_key = secret_path;
        config.credentials.lock_server_public_key =
            LockServerPubky::from_str(&Keypair::from_secret(&[9_u8; 32]).public_key().to_string())
                .unwrap();
        config.paykit = Some(PaykitConfig {
            server_url: format!("http://{}", paykit_listener.local_addr().unwrap()),
            minimum_confirmations: 0,
        });
        config.nexus = Some(NexusConfig {
            server_url: "https://nexus.example".to_owned(),
            allow_insecure_loopback: false,
        });
        let clock = Arc::new(MutableClock(Mutex::new(NOW)));
        let state =
            crate::app_state::AppState::new_empty_in_memory(config).with_clock(clock.clone());
        state.graph_tasks().insert_graph_task(task()).await.unwrap();
        let nexus = Arc::new(FakeNexus {
            responses: Mutex::new(VecDeque::from([TimedNexusObservation {
                observation: NexusObservation::Valid {
                    value: true,
                    origin: "https://nexus.example".to_owned(),
                    status: 200,
                },
                observed_at: NOW,
            }])),
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let graph = GraphVerificationWorker::new(
            state.graph_task_claimer().clone(),
            nexus,
            clock,
            Arc::new(FixedJitter),
            "graph-worker".to_owned(),
            time::Duration::seconds(30),
        );
        let paykit = state.paykit_http_client().unwrap();
        let invoice = InvoiceAdmissionWorker::from_state(&state, paykit.as_ref(), &FixedJitter);
        let verification = VerificationWorker::from_state(&state);

        let (verification_tick, invoice_tick, graph_tick) = tokio::join!(
            verification.run_once(),
            invoice.run_once(),
            graph.run_once()
        );

        assert_eq!(verification_tick.unwrap(), WorkerTick::Idle);
        assert_eq!(invoice_tick.unwrap(), InvoiceWorkerTick::Idle);
        assert!(matches!(graph_tick.unwrap(), GraphWorkerTick::Terminal(_)));
        assert!(
            tokio::time::timeout(Duration::from_millis(50), paykit_listener.accept())
                .await
                .is_err(),
            "graph-only execution must not open any Paykit connection"
        );
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

    fn eight_predicate_task(index: u8) -> GraphTaskRecord {
        let lock_hash = [index; 32];
        let lock_id = LockId::from_hash(LockHash::from_bytes(lock_hash));
        GraphTaskRecord {
            task_id: TaskId::from_str(&format!("018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d{index:02}"))
                .unwrap(),
            snapshot: GraphTaskSnapshot::V1(
                GraphTaskSnapshotV1::new(
                    creator(),
                    BundleId::new_random(),
                    PubkyLockResource::from_str(&format!("{CREATOR}/pub/app.locks/{lock_id}.json"))
                        .unwrap(),
                    lock_hash,
                    CreatorPubky::from_str(READER).unwrap(),
                    NOW,
                    60,
                    GraphPolicyV2 {
                        predicates: (0..8)
                            .map(|predicate| GraphPredicateV2::Tagged {
                                tagger: GraphParticipantV2::Reader,
                                tagged: GraphParticipantV2::Creator,
                                label: format!("trusted-{index}-{predicate}"),
                            })
                            .collect(),
                        operator: Some(locks_core::lock_policy::GraphOperatorV2::And),
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
