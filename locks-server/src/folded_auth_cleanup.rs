use std::sync::Arc;
use std::time::Duration;

use locks_service::application::errors::ApplicationError;
use locks_service::application::ports::{Clock, FoldedAuthRepository};
use tokio::sync::watch;

pub const FOLDED_AUTH_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

pub struct FoldedAuthCleanupWorker {
    repository: Arc<dyn FoldedAuthRepository>,
    clock: Arc<dyn Clock>,
    interval: Duration,
}

impl FoldedAuthCleanupWorker {
    pub fn new(
        repository: Arc<dyn FoldedAuthRepository>,
        clock: Arc<dyn Clock>,
        interval: Duration,
    ) -> Self {
        Self {
            repository,
            clock,
            interval,
        }
    }

    pub async fn run_once(&self) -> Result<u64, ApplicationError> {
        self.repository
            .purge_terminal_before(self.clock.now())
            .await
    }

    pub async fn run_until_shutdown(
        &self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), ApplicationError> {
        self.run_once().await?;
        let mut interval = tokio::time::interval(self.interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await;
        loop {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                _ = interval.tick() => {
                    self.run_once().await?;
                }
            }
        }
    }
}
