use std::sync::{Arc, RwLock};

/// Runtime composition state for the Lock Server process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeStorageKind {
    InMemory,
    Postgres,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerKind {
    Verification,
    Deletion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerReadinessState {
    Disabled,
    Starting,
    Ready,
    Degraded,
    NotReady,
    Stopped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerReadinessEvidence {
    Starting,
    Ready,
    DependencySucceeded,
    TransientDependencyFailure,
    PendingWork,
    LockContention,
    TerminalBusinessFailure,
    Stopping,
    Stopped,
    UnexpectedExit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadinessStatus {
    Ready,
    Degraded,
    NotReady,
}

#[derive(Debug, Clone)]
pub struct WorkerReadiness {
    states: Arc<RwLock<[WorkerReadinessState; 2]>>,
}

impl WorkerReadiness {
    pub fn new(verification_enabled: bool, deletion_enabled: bool) -> Self {
        let initial = |enabled| {
            if enabled {
                WorkerReadinessState::Starting
            } else {
                WorkerReadinessState::Disabled
            }
        };
        Self {
            states: Arc::new(RwLock::new([
                initial(verification_enabled),
                initial(deletion_enabled),
            ])),
        }
    }

    pub fn status(&self) -> ReadinessStatus {
        let states = self.states.read().expect("worker readiness lock poisoned");
        if states.iter().any(|state| {
            matches!(
                state,
                WorkerReadinessState::Starting
                    | WorkerReadinessState::NotReady
                    | WorkerReadinessState::Stopped
            )
        }) {
            ReadinessStatus::NotReady
        } else if states.contains(&WorkerReadinessState::Degraded) {
            ReadinessStatus::Degraded
        } else {
            ReadinessStatus::Ready
        }
    }

    pub fn worker_state(&self, worker: WorkerKind) -> WorkerReadinessState {
        self.states.read().expect("worker readiness lock poisoned")[worker.index()]
    }

    pub fn record(&self, worker: WorkerKind, evidence: WorkerReadinessEvidence) {
        let state = match evidence {
            WorkerReadinessEvidence::Starting => Some(WorkerReadinessState::Starting),
            WorkerReadinessEvidence::Ready | WorkerReadinessEvidence::DependencySucceeded => {
                Some(WorkerReadinessState::Ready)
            }
            WorkerReadinessEvidence::TransientDependencyFailure => {
                Some(WorkerReadinessState::Degraded)
            }
            WorkerReadinessEvidence::Stopping | WorkerReadinessEvidence::UnexpectedExit => {
                Some(WorkerReadinessState::NotReady)
            }
            WorkerReadinessEvidence::Stopped => Some(WorkerReadinessState::Stopped),
            WorkerReadinessEvidence::PendingWork
            | WorkerReadinessEvidence::LockContention
            | WorkerReadinessEvidence::TerminalBusinessFailure => None,
        };
        if let Some(state) = state {
            self.states.write().expect("worker readiness lock poisoned")[worker.index()] = state;
        }
    }
}

impl WorkerKind {
    fn index(self) -> usize {
        match self {
            Self::Verification => 0,
            Self::Deletion => 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enabled_workers_start_not_ready_and_require_independent_ready_evidence() {
        let readiness = WorkerReadiness::new(true, true);

        assert_eq!(readiness.status(), ReadinessStatus::NotReady);
        assert_eq!(
            readiness.worker_state(WorkerKind::Verification),
            WorkerReadinessState::Starting
        );
        assert_eq!(
            readiness.worker_state(WorkerKind::Deletion),
            WorkerReadinessState::Starting
        );

        readiness.record(WorkerKind::Verification, WorkerReadinessEvidence::Ready);
        assert_eq!(readiness.status(), ReadinessStatus::NotReady);

        readiness.record(WorkerKind::Deletion, WorkerReadinessEvidence::Ready);
        assert_eq!(readiness.status(), ReadinessStatus::Ready);
    }

    #[test]
    fn deletion_dependency_failure_degrades_until_success_without_business_outcome_noise() {
        let readiness = WorkerReadiness::new(false, true);
        readiness.record(WorkerKind::Deletion, WorkerReadinessEvidence::Ready);

        readiness.record(
            WorkerKind::Deletion,
            WorkerReadinessEvidence::TransientDependencyFailure,
        );
        assert_eq!(readiness.status(), ReadinessStatus::Degraded);
        assert_eq!(
            readiness.worker_state(WorkerKind::Deletion),
            WorkerReadinessState::Degraded
        );

        for evidence in [
            WorkerReadinessEvidence::PendingWork,
            WorkerReadinessEvidence::LockContention,
            WorkerReadinessEvidence::TerminalBusinessFailure,
        ] {
            readiness.record(WorkerKind::Deletion, evidence);
            assert_eq!(readiness.status(), ReadinessStatus::Degraded);
        }

        readiness.record(
            WorkerKind::Deletion,
            WorkerReadinessEvidence::DependencySucceeded,
        );
        assert_eq!(readiness.status(), ReadinessStatus::Ready);
    }

    #[test]
    fn stopping_stopped_and_unexpected_exit_are_not_ready() {
        for evidence in [
            WorkerReadinessEvidence::Stopping,
            WorkerReadinessEvidence::Stopped,
            WorkerReadinessEvidence::UnexpectedExit,
        ] {
            let readiness = WorkerReadiness::new(false, true);
            readiness.record(WorkerKind::Deletion, WorkerReadinessEvidence::Ready);
            readiness.record(WorkerKind::Deletion, evidence);
            assert_eq!(readiness.status(), ReadinessStatus::NotReady);
        }
    }
}
