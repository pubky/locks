use std::{
    collections::HashMap, future::Future, net::SocketAddr, path::PathBuf, pin::Pin, time::Duration,
};

use tokio::{
    net::TcpListener,
    sync::watch,
    task::{Id, JoinError, JoinSet},
};

use crate::{
    app_state::{WorkerKind, WorkerReadiness, WorkerReadinessEvidence},
    config::ConfigError,
};

pub type RuntimeTaskFuture = Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'static>>;
pub type ShutdownFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
pub type HttpTaskFactory =
    Box<dyn FnOnce(watch::Receiver<bool>) -> RuntimeTaskFuture + Send + 'static>;
pub type LifecycleTaskFactory =
    Box<dyn FnOnce(watch::Receiver<bool>) -> RuntimeTaskFuture + Send + 'static>;
pub type WorkerTaskFactory =
    Box<dyn FnOnce(WorkerTaskContext) -> RuntimeTaskFuture + Send + 'static>;

#[derive(Debug)]
pub enum InitialStartupOutcome<T> {
    Ready { listener: TcpListener, initial: T },
    ShutdownRequested,
}

#[derive(Debug, thiserror::Error)]
pub enum InitialStartupError {
    #[error("failed to bind HTTP listener: {0}")]
    Bind(#[source] std::io::Error),
    #[error("initial startup task failed: {0}")]
    Initial(#[source] anyhow::Error),
    #[error("initial startup exceeded lifecycle timeout {0:?}")]
    TimedOut(Duration),
}

pub async fn bind_listener_then_run_initial<S, F, Fut, T>(
    bind_addr: SocketAddr,
    lifecycle_timeout: Duration,
    shutdown: &mut Pin<Box<S>>,
    initial: F,
) -> Result<InitialStartupOutcome<T>, InitialStartupError>
where
    S: Future<Output = ()> + ?Sized,
    F: FnOnce() -> Fut,
    Fut: Future<Output = anyhow::Result<T>>,
{
    let listener = tokio::select! {
        biased;
        _ = shutdown.as_mut() => return Ok(InitialStartupOutcome::ShutdownRequested),
        result = TcpListener::bind(bind_addr) => result.map_err(InitialStartupError::Bind)?,
    };

    let initial = tokio::time::timeout(lifecycle_timeout, initial());
    tokio::pin!(initial);
    tokio::select! {
        biased;
        _ = shutdown.as_mut() => Ok(InitialStartupOutcome::ShutdownRequested),
        result = &mut initial => match result {
            Ok(Ok(initial)) => Ok(InitialStartupOutcome::Ready { listener, initial }),
            Ok(Err(error)) => Err(InitialStartupError::Initial(error)),
            Err(_) => Err(InitialStartupError::TimedOut(lifecycle_timeout)),
        },
    }
}

#[derive(Clone)]
pub struct WorkerTaskContext {
    shutdown: watch::Receiver<bool>,
    readiness: WorkerReadiness,
    worker: WorkerKind,
}

impl WorkerTaskContext {
    pub fn shutdown(&self) -> watch::Receiver<bool> {
        self.shutdown.clone()
    }

    pub fn record(&self, evidence: WorkerReadinessEvidence) {
        self.readiness.record(self.worker, evidence);
    }
}

#[derive(Default)]
pub struct RuntimeTasks {
    shutdown_timeout: Duration,
    http: Option<HttpTaskFactory>,
    pkarr_republisher: Option<LifecycleTaskFactory>,
    verification_worker: Option<WorkerTaskFactory>,
    deletion_worker: Option<WorkerTaskFactory>,
}

impl RuntimeTasks {
    pub fn new(shutdown_timeout: Duration) -> Self {
        Self {
            shutdown_timeout,
            ..Self::default()
        }
    }

    pub fn with_http(mut self, factory: HttpTaskFactory) -> Self {
        self.http = Some(factory);
        self
    }

    pub fn with_pkarr_republisher(mut self, factory: LifecycleTaskFactory) -> Self {
        self.pkarr_republisher = Some(factory);
        self
    }

    pub fn with_verification_worker(mut self, factory: WorkerTaskFactory) -> Self {
        self.verification_worker = Some(factory);
        self
    }

    pub fn with_deletion_worker(mut self, factory: WorkerTaskFactory) -> Self {
        self.deletion_worker = Some(factory);
        self
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("{0} exited unexpectedly")]
    UnexpectedExit(&'static str),
    #[error("{task} failed: {error}")]
    TaskFailed {
        task: &'static str,
        error: anyhow::Error,
    },
    #[error("{0} panicked")]
    TaskPanicked(&'static str),
    #[error("runtime shutdown exceeded {0:?}")]
    ShutdownTimedOut(Duration),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OwnedTask {
    Http,
    PkarrRepublisher,
    Verification,
    Deletion,
    Unknown,
}

impl OwnedTask {
    fn label(self) -> &'static str {
        match self {
            Self::Http => "HTTP server",
            Self::PkarrRepublisher => "PKARR republisher",
            Self::Verification => "verification worker",
            Self::Deletion => "deletion worker",
            Self::Unknown => "owned runtime task",
        }
    }

    fn worker(self) -> Option<WorkerKind> {
        match self {
            Self::Http | Self::PkarrRepublisher | Self::Unknown => None,
            Self::Verification => Some(WorkerKind::Verification),
            Self::Deletion => Some(WorkerKind::Deletion),
        }
    }
}

pub async fn supervise(
    readiness: WorkerReadiness,
    mut shutdown: ShutdownFuture,
    tasks: RuntimeTasks,
) -> Result<(), RuntimeError> {
    let shutdown_timeout = tasks.shutdown_timeout;
    let (worker_shutdown_tx, worker_shutdown_rx) = watch::channel(false);
    let (service_shutdown_tx, service_shutdown_rx) = watch::channel(false);
    let verification_enabled = tasks.verification_worker.is_some();
    let deletion_enabled = tasks.deletion_worker.is_some();
    let mut workers_remaining = usize::from(verification_enabled) + usize::from(deletion_enabled);
    let mut roots = JoinSet::new();
    let mut root_tasks = HashMap::new();

    if let Some(factory) = tasks.http {
        spawn_root(
            &mut roots,
            &mut root_tasks,
            OwnedTask::Http,
            factory(service_shutdown_rx),
        );
    }
    if let Some(factory) = tasks.pkarr_republisher {
        let shutdown = service_shutdown_tx.subscribe();
        spawn_root(
            &mut roots,
            &mut root_tasks,
            OwnedTask::PkarrRepublisher,
            factory(shutdown),
        );
    }
    if let Some(factory) = tasks.verification_worker {
        let context = WorkerTaskContext {
            shutdown: worker_shutdown_rx.clone(),
            readiness: readiness.clone(),
            worker: WorkerKind::Verification,
        };
        spawn_root(
            &mut roots,
            &mut root_tasks,
            OwnedTask::Verification,
            factory(context),
        );
    }
    if let Some(factory) = tasks.deletion_worker {
        let context = WorkerTaskContext {
            shutdown: worker_shutdown_rx,
            readiness: readiness.clone(),
            worker: WorkerKind::Deletion,
        };
        spawn_root(
            &mut roots,
            &mut root_tasks,
            OwnedTask::Deletion,
            factory(context),
        );
    }

    let failure = tokio::select! {
        biased;
        _ = &mut shutdown => None,
        completed = join_next_owned(&mut roots, &mut root_tasks) => {
            if let Some((task, _)) = &completed
                && task.worker().is_some()
            {
                workers_remaining = workers_remaining.saturating_sub(1);
            }
            Some(classify_premature(completed, &readiness))
        },
    };

    begin_worker_shutdown(
        &readiness,
        verification_enabled,
        deletion_enabled,
        &worker_shutdown_tx,
    );

    let mut failure = failure;
    let drain_result = tokio::time::timeout(
        shutdown_timeout,
        stop_workers_then_drain_services(
            &mut roots,
            &mut root_tasks,
            workers_remaining,
            &service_shutdown_tx,
            &mut failure,
        ),
    )
    .await;
    match drain_result {
        Ok(()) => match failure {
            Some(error) => Err(error),
            None => Ok(()),
        },
        Err(_) => {
            roots.abort_all();
            while roots.join_next().await.is_some() {}
            Err(failure.unwrap_or(RuntimeError::ShutdownTimedOut(shutdown_timeout)))
        }
    }
}

fn spawn_root(
    roots: &mut JoinSet<(OwnedTask, anyhow::Result<()>)>,
    root_tasks: &mut HashMap<Id, OwnedTask>,
    task: OwnedTask,
    future: RuntimeTaskFuture,
) {
    let handle = roots.spawn(async move { (task, future.await) });
    root_tasks.insert(handle.id(), task);
}

type OwnedCompletion = (OwnedTask, Result<anyhow::Result<()>, JoinError>);

async fn join_next_owned(
    roots: &mut JoinSet<(OwnedTask, anyhow::Result<()>)>,
    root_tasks: &mut HashMap<Id, OwnedTask>,
) -> Option<OwnedCompletion> {
    match roots.join_next_with_id().await {
        Some(Ok((id, (task, result)))) => {
            root_tasks.remove(&id);
            Some((task, Ok(result)))
        }
        Some(Err(error)) => {
            let task = root_tasks.remove(&error.id()).unwrap_or(OwnedTask::Unknown);
            Some((task, Err(error)))
        }
        None => None,
    }
}

fn begin_worker_shutdown(
    readiness: &WorkerReadiness,
    verification_enabled: bool,
    deletion_enabled: bool,
    worker_shutdown_tx: &watch::Sender<bool>,
) {
    if verification_enabled {
        readiness.record(WorkerKind::Verification, WorkerReadinessEvidence::Stopping);
    }
    if deletion_enabled {
        readiness.record(WorkerKind::Deletion, WorkerReadinessEvidence::Stopping);
    }
    worker_shutdown_tx.send_replace(true);
}

async fn stop_workers_then_drain_services(
    roots: &mut JoinSet<(OwnedTask, anyhow::Result<()>)>,
    root_tasks: &mut HashMap<Id, OwnedTask>,
    mut workers_remaining: usize,
    service_shutdown_tx: &watch::Sender<bool>,
    failure: &mut Option<RuntimeError>,
) {
    while workers_remaining > 0 {
        match join_next_owned(roots, root_tasks).await {
            Some((task, result)) if task.worker().is_some() => {
                workers_remaining -= 1;
                if let Some(error) = classify_shutdown_completion(task, result, false) {
                    failure.get_or_insert(error);
                }
            }
            Some((task, result)) => {
                if let Some(error) = classify_shutdown_completion(task, result, true) {
                    failure.get_or_insert(error);
                }
            }
            None => {
                failure.get_or_insert(RuntimeError::UnexpectedExit("runtime task set"));
                break;
            }
        }
    }

    // Worker root completion is explicit evidence that no future queue claim can begin.
    // Only now may HTTP graceful draining and other service-root cancellation start.
    service_shutdown_tx.send_replace(true);
    drain(roots, root_tasks, failure).await;
}

fn classify_premature(
    completed: Option<OwnedCompletion>,
    readiness: &WorkerReadiness,
) -> RuntimeError {
    match completed {
        Some((task, Ok(result))) => {
            if let Some(worker) = task.worker() {
                readiness.record(worker, WorkerReadinessEvidence::UnexpectedExit);
            }
            match result {
                Ok(()) => RuntimeError::UnexpectedExit(task.label()),
                Err(error) => RuntimeError::TaskFailed {
                    task: task.label(),
                    error,
                },
            }
        }
        Some((task, Err(error))) => {
            if let Some(worker) = task.worker() {
                readiness.record(worker, WorkerReadinessEvidence::UnexpectedExit);
            }
            classify_join_error(task, error)
        }
        None => RuntimeError::UnexpectedExit("runtime task set"),
    }
}

fn classify_shutdown_completion(
    task: OwnedTask,
    result: Result<anyhow::Result<()>, JoinError>,
    success_is_unexpected: bool,
) -> Option<RuntimeError> {
    match result {
        Ok(Ok(())) if success_is_unexpected => Some(RuntimeError::UnexpectedExit(task.label())),
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(RuntimeError::TaskFailed {
            task: task.label(),
            error,
        }),
        Err(error) => Some(classify_join_error(task, error)),
    }
}

fn classify_join_error(task: OwnedTask, error: JoinError) -> RuntimeError {
    if error.is_panic() {
        RuntimeError::TaskPanicked(task.label())
    } else {
        RuntimeError::UnexpectedExit(task.label())
    }
}

async fn drain(
    roots: &mut JoinSet<(OwnedTask, anyhow::Result<()>)>,
    root_tasks: &mut HashMap<Id, OwnedTask>,
    failure: &mut Option<RuntimeError>,
) {
    while let Some((task, result)) = join_next_owned(roots, root_tasks).await {
        if let Some(error) = classify_shutdown_completion(task, result, false) {
            failure.get_or_insert(error);
        }
    }
}

pub async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    while shutdown.changed().await.is_ok() {
        if *shutdown.borrow() {
            return;
        }
    }
}

pub fn parse_config_arg<I>(args: I) -> Result<Option<PathBuf>, ConfigError>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let _program = args.next();
    let mut config_path = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                if config_path.is_some() {
                    return Err(ConfigError::InvalidArgs(
                        "--config may only be provided once".to_owned(),
                    ));
                }
                let value = args.next().ok_or_else(|| {
                    ConfigError::InvalidArgs("--config requires a path".to_owned())
                })?;
                config_path = Some(PathBuf::from(value));
            }
            "--help" | "-h" => {
                return Err(ConfigError::InvalidArgs(
                    "usage: locks-server [--config <path>]".to_owned(),
                ));
            }
            other => {
                return Err(ConfigError::InvalidArgs(format!(
                    "unknown argument: {other}"
                )));
            }
        }
    }
    Ok(config_path)
}

pub fn home_dir_from_env() -> Result<PathBuf, ConfigError> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or(ConfigError::MissingHome)
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    };

    use tokio::sync::{Notify, oneshot};

    use crate::app_state::{
        WorkerKind, WorkerReadiness, WorkerReadinessEvidence, WorkerReadinessState,
    };

    use super::{RuntimeTasks, parse_config_arg, supervise, wait_for_shutdown};

    #[tokio::test]
    async fn premature_worker_success_fails_runtime_and_marks_worker_unexpected() {
        let readiness = WorkerReadiness::new(true, false);
        let observed = readiness.clone();
        let tasks = RuntimeTasks::new(Duration::from_millis(100))
            .with_verification_worker(Box::new(|_context| Box::pin(async { Ok(()) })));

        let error = supervise(readiness, Box::pin(std::future::pending()), tasks)
            .await
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("verification worker exited unexpectedly")
        );
        assert_eq!(
            observed.worker_state(WorkerKind::Verification),
            WorkerReadinessState::NotReady
        );
    }

    #[tokio::test]
    async fn worker_error_and_panic_fail_runtime() {
        for tasks in [
            RuntimeTasks::new(Duration::from_millis(100)).with_verification_worker(Box::new(
                |_context| Box::pin(async { anyhow::bail!("dependency broke") }),
            )),
            RuntimeTasks::new(Duration::from_millis(100))
                .with_verification_worker(Box::new(|_context| Box::pin(async { panic!("boom") }))),
        ] {
            let error = supervise(
                WorkerReadiness::new(true, false),
                Box::pin(std::future::pending()),
                tasks,
            )
            .await
            .unwrap_err();
            assert!(error.to_string().contains("failed") || error.to_string().contains("panicked"));
        }
    }

    #[tokio::test]
    async fn worker_panic_starts_service_drain_promptly_and_preserves_panic_verdict() {
        let http_started = Arc::new(Notify::new());
        let http_draining = Arc::new(Notify::new());
        let (panic_tx, panic_rx) = oneshot::channel();
        let tasks = RuntimeTasks::new(Duration::from_secs(5))
            .with_http(Box::new({
                let http_started = Arc::clone(&http_started);
                let http_draining = Arc::clone(&http_draining);
                move |shutdown| {
                    Box::pin(async move {
                        http_started.notify_one();
                        wait_for_shutdown(shutdown).await;
                        http_draining.notify_one();
                        Ok(())
                    })
                }
            }))
            .with_verification_worker(Box::new(move |_context| {
                Box::pin(async move {
                    let _ = panic_rx.await;
                    panic!("boom");
                })
            }));
        let supervisor = tokio::spawn(supervise(
            WorkerReadiness::new(true, false),
            Box::pin(std::future::pending()),
            tasks,
        ));

        http_started.notified().await;
        panic_tx.send(()).unwrap();
        let error = tokio::time::timeout(Duration::from_millis(200), supervisor)
            .await
            .expect("worker panic should not wait for the shutdown deadline")
            .unwrap()
            .unwrap_err();

        tokio::time::timeout(Duration::from_millis(100), http_draining.notified())
            .await
            .expect("worker panic should signal service draining");
        assert!(matches!(
            error,
            super::RuntimeError::TaskPanicked("verification worker")
        ));
    }

    #[tokio::test]
    async fn pkarr_republisher_failure_fails_runtime() {
        let tasks = RuntimeTasks::new(Duration::from_millis(100)).with_pkarr_republisher(Box::new(
            |_shutdown| Box::pin(async { anyhow::bail!("publish failed") }),
        ));

        let error = supervise(
            WorkerReadiness::new(false, false),
            Box::pin(std::future::pending()),
            tasks,
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("PKARR republisher failed"));
    }

    #[tokio::test]
    async fn readiness_is_not_ready_before_workers_observe_sticky_cancellation() {
        let readiness = WorkerReadiness::new(true, true);
        let observed = readiness.clone();
        let observations = Arc::new(Mutex::new(Vec::new()));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let tasks = RuntimeTasks::new(Duration::from_millis(100))
            .with_verification_worker(observing_worker(
                observed.clone(),
                Arc::clone(&observations),
                WorkerKind::Verification,
            ))
            .with_deletion_worker(observing_worker(
                observed,
                Arc::clone(&observations),
                WorkerKind::Deletion,
            ));
        shutdown_tx.send(()).unwrap();

        supervise(
            readiness,
            Box::pin(async {
                let _ = shutdown_rx.await;
            }),
            tasks,
        )
        .await
        .unwrap();

        assert_eq!(
            *observations.lock().unwrap(),
            vec![
                WorkerReadinessState::NotReady,
                WorkerReadinessState::NotReady
            ]
        );
    }

    fn observing_worker(
        readiness: WorkerReadiness,
        observations: Arc<Mutex<Vec<WorkerReadinessState>>>,
        kind: WorkerKind,
    ) -> super::WorkerTaskFactory {
        Box::new(move |context| {
            Box::pin(async move {
                wait_for_shutdown(context.shutdown()).await;
                observations
                    .lock()
                    .unwrap()
                    .push(readiness.worker_state(kind));
                Ok(())
            })
        })
    }

    #[tokio::test]
    async fn http_drain_waits_for_explicit_worker_claim_stop() {
        let worker_stopping = Arc::new(Notify::new());
        let allow_worker_stop = Arc::new(Notify::new());
        let http_draining = Arc::new(Notify::new());
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let tasks = RuntimeTasks::new(Duration::from_secs(1))
            .with_http(Box::new({
                let http_draining = Arc::clone(&http_draining);
                move |shutdown| {
                    Box::pin(async move {
                        wait_for_shutdown(shutdown).await;
                        http_draining.notify_one();
                        Ok(())
                    })
                }
            }))
            .with_verification_worker(Box::new({
                let worker_stopping = Arc::clone(&worker_stopping);
                let allow_worker_stop = Arc::clone(&allow_worker_stop);
                move |context| {
                    Box::pin(async move {
                        wait_for_shutdown(context.shutdown()).await;
                        worker_stopping.notify_one();
                        allow_worker_stop.notified().await;
                        Ok(())
                    })
                }
            }));
        let supervisor = tokio::spawn(supervise(
            WorkerReadiness::new(true, false),
            Box::pin(async {
                let _ = shutdown_rx.await;
            }),
            tasks,
        ));

        shutdown_tx.send(()).unwrap();
        worker_stopping.notified().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(20), http_draining.notified())
                .await
                .is_err(),
            "HTTP draining started before the verification worker stopped claiming"
        );
        allow_worker_stop.notify_one();
        supervisor.await.unwrap().unwrap();
        tokio::time::timeout(Duration::from_millis(100), http_draining.notified())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn graceful_shutdown_joins_all_roots_and_late_subscriber_exits() {
        let joined = Arc::new(Mutex::new(Vec::new()));
        let late_subscription_exited = Arc::new(Notify::new());
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let tasks = RuntimeTasks::new(Duration::from_secs(1))
            .with_http(joining_task(Arc::clone(&joined), "http"))
            .with_verification_worker(Box::new({
                let joined = Arc::clone(&joined);
                let late_subscription_exited = Arc::clone(&late_subscription_exited);
                move |context| {
                    Box::pin(async move {
                        wait_for_shutdown(context.shutdown()).await;
                        wait_for_shutdown(context.shutdown()).await;
                        late_subscription_exited.notify_one();
                        joined.lock().unwrap().push("verification");
                        Ok(())
                    })
                }
            }))
            .with_deletion_worker(Box::new({
                let joined = Arc::clone(&joined);
                move |context| {
                    Box::pin(async move {
                        wait_for_shutdown(context.shutdown()).await;
                        joined.lock().unwrap().push("deletion");
                        Ok(())
                    })
                }
            }));
        shutdown_tx.send(()).unwrap();

        supervise(
            WorkerReadiness::new(true, true),
            Box::pin(async {
                let _ = shutdown_rx.await;
            }),
            tasks,
        )
        .await
        .unwrap();
        tokio::time::timeout(
            Duration::from_millis(100),
            late_subscription_exited.notified(),
        )
        .await
        .unwrap();
        let mut roots = joined.lock().unwrap().clone();
        roots.sort_unstable();
        assert_eq!(roots, vec!["deletion", "http", "verification"]);
    }

    fn joining_task(
        joined: Arc<Mutex<Vec<&'static str>>>,
        name: &'static str,
    ) -> super::HttpTaskFactory {
        Box::new(move |shutdown| {
            Box::pin(async move {
                wait_for_shutdown(shutdown).await;
                joined.lock().unwrap().push(name);
                Ok(())
            })
        })
    }

    #[tokio::test]
    async fn blocked_root_is_aborted_and_drained_at_deadline() {
        let dropped = Arc::new(Notify::new());
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let tasks =
            RuntimeTasks::new(Duration::from_millis(20)).with_verification_worker(Box::new({
                let dropped = Arc::clone(&dropped);
                move |_context| {
                    Box::pin(async move {
                        let _guard = DropSignal(dropped);
                        std::future::pending::<()>().await;
                        Ok(())
                    })
                }
            }));
        shutdown_tx.send(()).unwrap();
        let started = Instant::now();

        let error = supervise(
            WorkerReadiness::new(true, false),
            Box::pin(async {
                let _ = shutdown_rx.await;
            }),
            tasks,
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("shutdown exceeded"));
        assert!(started.elapsed() < Duration::from_millis(250));
        tokio::time::timeout(Duration::from_millis(100), dropped.notified())
            .await
            .unwrap();
    }

    struct DropSignal(Arc<Notify>);
    impl Drop for DropSignal {
        fn drop(&mut self) {
            self.0.notify_one();
        }
    }

    #[test]
    fn readiness_evidence_can_be_recorded_through_worker_context_contract() {
        let readiness = WorkerReadiness::new(true, false);
        readiness.record(WorkerKind::Verification, WorkerReadinessEvidence::Ready);
        assert_eq!(
            readiness.worker_state(WorkerKind::Verification),
            WorkerReadinessState::Ready
        );
    }

    #[test]
    fn parse_config_arg_accepts_absent_or_explicit_config() {
        assert_eq!(parse_config_arg(["locks-server".to_owned()]).unwrap(), None);
        assert_eq!(
            parse_config_arg([
                "locks-server".to_owned(),
                "--config".to_owned(),
                "/tmp/locks.toml".to_owned(),
            ])
            .unwrap()
            .unwrap()
            .to_str(),
            Some("/tmp/locks.toml")
        );
    }

    #[test]
    fn parse_config_arg_rejects_unknown_or_incomplete_arguments() {
        let missing =
            parse_config_arg(["locks-server".to_owned(), "--config".to_owned()]).unwrap_err();
        assert!(missing.to_string().contains("requires a path"));

        let unknown =
            parse_config_arg(["locks-server".to_owned(), "--port".to_owned()]).unwrap_err();
        assert!(unknown.to_string().contains("unknown argument"));
    }
}
