use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use locks_server::runtime::{InitialStartupOutcome, bind_listener_then_run_initial};
use tokio::sync::{Notify, oneshot};

#[tokio::test]
async fn listener_bind_failure_does_not_attempt_initial_publication() {
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bind_addr = occupied.local_addr().unwrap();
    let publication_calls = Arc::new(AtomicUsize::new(0));
    let observed_calls = Arc::clone(&publication_calls);
    let mut shutdown = Box::pin(std::future::pending());

    let error = bind_listener_then_run_initial(
        bind_addr,
        Duration::from_millis(100),
        &mut shutdown,
        move || async move {
            observed_calls.fetch_add(1, Ordering::SeqCst);
            Ok::<_, anyhow::Error>(())
        },
    )
    .await
    .unwrap_err();

    assert!(error.to_string().contains("bind"));
    assert_eq!(publication_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn blocked_initial_publication_stops_when_shutdown_is_requested() {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let mut shutdown = Box::pin(async move {
        let _ = shutdown_rx.await;
    });
    let publication_started = Arc::new(Notify::new());
    let observed_start = Arc::clone(&publication_started);
    let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let startup = bind_listener_then_run_initial(
        bind_addr,
        Duration::from_secs(1),
        &mut shutdown,
        move || async move {
            observed_start.notify_one();
            std::future::pending::<anyhow::Result<()>>().await
        },
    );
    let request_shutdown = async {
        publication_started.notified().await;
        shutdown_tx.send(()).unwrap();
    };
    let (result, ()) = tokio::join!(startup, request_shutdown);

    assert!(matches!(
        result.unwrap(),
        InitialStartupOutcome::ShutdownRequested
    ));
}

#[tokio::test]
async fn blocked_initial_publication_is_bounded_by_lifecycle_timeout() {
    let mut shutdown = Box::pin(std::future::pending());
    let started = Instant::now();

    let error = bind_listener_then_run_initial(
        "127.0.0.1:0".parse().unwrap(),
        Duration::from_millis(20),
        &mut shutdown,
        || async { std::future::pending::<anyhow::Result<()>>().await },
    )
    .await
    .unwrap_err();

    assert!(error.to_string().contains("initial startup exceeded"));
    assert!(started.elapsed() < Duration::from_millis(250));
}
