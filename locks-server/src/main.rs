use std::env;
use std::error::Error;

use locks_server::api::routes::router;
use locks_server::config::{FilesystemLockServerIdentityProvider, load_or_initialize_config};
use locks_server::pkdns::LockServerKeyRepublisher;
use locks_server::runtime::{home_dir_from_env, parse_config_arg};
use locks_server::storage::build_runtime_state;
use locks_server::worker::VerificationWorker;
use tokio::net::TcpListener;
use tokio::sync::watch;
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
    let _key_republisher = LockServerKeyRepublisher::start_if_required(&config).await?;
    let state = build_runtime_state(config).await?;
    let worker_enabled = state.config().worker.enabled;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let worker_handle = if worker_enabled {
        let worker_state = state.clone();
        Some(tokio::spawn(async move {
            let worker = VerificationWorker::from_state(&worker_state);
            if let Err(error) = worker.run_until_shutdown(shutdown_rx).await {
                error!(%error, "verification worker stopped with error");
            }
        }))
    } else {
        None
    };
    let app = router(state).layer(TraceLayer::new_for_http());
    let listener = TcpListener::bind(bind_addr).await?;

    info!(%bind_addr, "starting locks-server");
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;
    let _ = shutdown_tx.send(true);
    if let Some(worker_handle) = worker_handle {
        worker_handle.await?;
    }
    Ok(())
}

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        error!(%error, "failed to listen for shutdown signal");
    }
}

fn init_tracing(configured_level: &str) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(configured_level));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}
