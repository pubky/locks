use locks_service::application::use_cases::execute_content_lock_deletion_phase::{
    ContentLockDeletionPhaseExecutor, DeletionPhaseExecutionOutcome,
};

#[test]
fn deletion_phase_executor_exposes_closed_outcomes() {
    let _ = std::mem::size_of::<ContentLockDeletionPhaseExecutor<'static>>();
    let outcomes = [
        DeletionPhaseExecutionOutcome::Progressed,
        DeletionPhaseExecutionOutcome::Deferred,
        DeletionPhaseExecutionOutcome::ClaimLost,
        DeletionPhaseExecutionOutcome::TerminalFailed,
        DeletionPhaseExecutionOutcome::TransientDependencyFailure,
        DeletionPhaseExecutionOutcome::FatalFailure,
    ];
    assert_eq!(outcomes.len(), 6);
}
