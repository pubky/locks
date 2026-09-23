use std::{env, error::Error, time::Duration};

use locks_server::api::routes::router;
use locks_server::config::{FilesystemLockServerIdentityProvider, load_or_initialize_config};
use locks_server::deletion_worker::{
    DeletionWorker, DeletionWorkerConfig, RandomFullJitter, RuntimeClaimedDeletionExecutor,
};
use locks_server::pkdns::LockServerKeyRepublisher;
use locks_server::runtime::{
    InitialStartupOutcome, RuntimeTasks, ShutdownFuture, bind_listener_then_run_initial,
    home_dir_from_env, parse_config_arg, supervise, wait_for_shutdown,
};
use locks_server::storage::build_runtime_state;
use locks_server::worker::VerificationWorker;
use tower_http::trace::TraceLayer;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config_path = parse_config_arg(env::args())?;
    let home_dir = home_dir_from_env()?;
    let identity_provider = FilesystemLockServerIdentityProvider;
    let config = load_or_initialize_config(config_path, &home_dir, &identity_provider)?;
    init_tracing(&config.logging.level);
    let bind_addr = config.bind_addr;
    let state = build_runtime_state(config).await?;
    let shutdown_timeout =
        Duration::from_secs(state.config().deletion_worker.shutdown_timeout_seconds);
    // Validate publication state before network startup, but do not advertise the service until
    // secret validation, database startup, migrations, and listener binding have all succeeded.
    let key_republisher = LockServerKeyRepublisher::build_if_required(state.config())?;
    let mut shutdown: ShutdownFuture = Box::pin(shutdown_signal());
    let startup = bind_listener_then_run_initial(
        bind_addr,
        shutdown_timeout,
        &mut shutdown,
        move || async move {
            if let Some(republisher) = &key_republisher {
                republisher
                    .publish_initial()
                    .await
                    .map_err(anyhow::Error::from)?;
            }
            Ok(key_republisher)
        },
    )
    .await?;
    let InitialStartupOutcome::Ready {
        listener,
        initial: key_republisher,
    } = startup
    else {
        return Ok(());
    };
    let app = router(state.clone()).layer(TraceLayer::new_for_http());

    let mut tasks = RuntimeTasks::new(shutdown_timeout).with_http(Box::new(move |shutdown| {
        Box::pin(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .with_graceful_shutdown(wait_for_shutdown(shutdown))
            .await
            .map_err(anyhow::Error::from)
        })
    }));

    if let Some(key_republisher) = key_republisher {
        tasks = tasks.with_pkarr_republisher(Box::new(move |shutdown| {
            Box::pin(async move {
                key_republisher
                    .run_until_shutdown(shutdown)
                    .await
                    .map_err(anyhow::Error::from)
            })
        }));
    }

    if state.config().worker.enabled {
        let worker_state = state.clone();
        tasks = tasks.with_verification_worker(Box::new(move |context| {
            Box::pin(async move {
                VerificationWorker::from_state(&worker_state)
                    .run_until_shutdown_with_readiness(
                        context.shutdown(),
                        worker_state.worker_readiness(),
                    )
                    .await
                    .map_err(anyhow::Error::from)
            })
        }));
    }

    if state.config().deletion_worker.enabled {
        let worker_state = state.clone();
        tasks = tasks.with_deletion_worker(Box::new(move |context| {
            Box::pin(async move {
                let runtime = &worker_state.config().deletion_worker;
                let retry = &worker_state.config().deletion;
                let executor = RuntimeClaimedDeletionExecutor::new(worker_state.clone());
                let jitter = RandomFullJitter;
                let config = DeletionWorkerConfig {
                    worker_id: runtime.worker_id.clone(),
                    poll_interval: Duration::from_millis(runtime.poll_interval_ms),
                    claim_timeout: Duration::from_secs(runtime.claim_timeout_seconds),
                    retry_max_attempts: retry.retry_max_attempts,
                    retry_initial_backoff: Duration::from_secs(retry.retry_initial_backoff_seconds),
                    retry_max_backoff: Duration::from_secs(retry.retry_max_backoff_seconds),
                };
                DeletionWorker::new(
                    worker_state.content_lock_deletions().as_ref(),
                    worker_state.clock().as_ref(),
                    &executor,
                    &jitter,
                    config,
                )
                .run_until_shutdown_with_readiness(
                    context.shutdown(),
                    worker_state.worker_readiness(),
                )
                .await
                .map_err(anyhow::Error::from)
            })
        }));
    }

    info!(%bind_addr, "starting locks-server");
    supervise(state.worker_readiness().clone(), shutdown, tasks).await?;
    Ok(())
}

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        error!(%error, "failed to listen for shutdown signal");
        std::future::pending::<()>().await;
    }
}

fn init_tracing(configured_level: &str) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(configured_level));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}
