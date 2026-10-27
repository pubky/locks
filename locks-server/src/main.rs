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
use locks_server::worker::{InvoiceAdmissionWorker, OsFullJitter, VerificationWorker};
use tower_http::trace::TraceLayer;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

const SENSITIVE_PUBKY_LOG_DIRECTIVES: &[&str] = &[
    "pkarr=off",
    "pubky::actors::auth::relay::auth_relay_listener=off",
    "pubky::actors::auth::relay::http_relay_inbox_channel=off",
    "pubky::actors::auth::relay::http_relay_link_channel=off",
    "pubky::actors::auth::cookie::credential=off",
    "pubky::actors::pkdns=off",
    "pubky::client::http_targets::native=off",
    "tower_http::trace=off",
];

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
                let verification = VerificationWorker::from_state(&worker_state);
                if let Some(paykit) = worker_state.paykit_http_client().cloned() {
                    let jitter = OsFullJitter;
                    let invoice =
                        InvoiceAdmissionWorker::from_state(&worker_state, paykit.as_ref(), &jitter);
                    tokio::try_join!(
                        verification.run_until_shutdown_with_readiness(
                            context.shutdown(),
                            worker_state.worker_readiness(),
                        ),
                        invoice.run_until_shutdown(context.shutdown()),
                    )
                    .map(|_| ())
                    .map_err(anyhow::Error::from)
                } else {
                    verification
                        .run_until_shutdown_with_readiness(
                            context.shutdown(),
                            worker_state.worker_readiness(),
                        )
                        .await
                        .map_err(anyhow::Error::from)
                }
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
    let filter = with_sensitive_pubky_logs_disabled(filter);
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

fn with_sensitive_pubky_logs_disabled(filter: EnvFilter) -> EnvFilter {
    SENSITIVE_PUBKY_LOG_DIRECTIVES
        .iter()
        .fold(filter, |filter, directive| {
            filter.add_directive(
                directive
                    .parse()
                    .expect("hard-coded Pubky log directive must be valid"),
            )
        })
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use locks_server::config::LoggingConfig;

    use super::{SENSITIVE_PUBKY_LOG_DIRECTIVES, with_sensitive_pubky_logs_disabled};
    use tracing::{Event, Subscriber};
    use tracing_subscriber::layer::{Context, SubscriberExt};
    use tracing_subscriber::{EnvFilter, Layer, Registry};

    #[test]
    fn default_filter_emits_info_but_disables_sensitive_pubky_targets() {
        let filter =
            with_sensitive_pubky_logs_disabled(EnvFilter::new(LoggingConfig::default().level));
        let directives = filter.to_string();

        for directive in SENSITIVE_PUBKY_LOG_DIRECTIVES {
            assert!(directives.contains(directive));
        }

        let targets = RecordedTargets::default();
        let subscriber = Registry::default().with(filter).with(targets.clone());
        let _guard = tracing::subscriber::set_default(subscriber);
        tracing::info!(target: "locks_server::timing_filter_test", "visible control event");
        tracing::info!(target: "pkarr", creator = "identifier-must-not-appear", "suppressed event");

        assert_eq!(
            targets.0.lock().unwrap().as_slice(),
            ["locks_server::timing_filter_test"]
        );
    }

    #[derive(Clone, Default)]
    struct RecordedTargets(Arc<Mutex<Vec<&'static str>>>);

    impl<S> Layer<S> for RecordedTargets
    where
        S: Subscriber,
    {
        fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
            self.0.lock().unwrap().push(event.metadata().target());
        }
    }
}
