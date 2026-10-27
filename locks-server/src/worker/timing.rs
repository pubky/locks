use std::time::Instant;

#[derive(Clone, Copy)]
pub(super) enum TimingWorker {
    Verification,
    InvoiceAdmission,
}

impl TimingWorker {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Verification => "verification",
            Self::InvoiceAdmission => "invoice_admission",
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum TimingStage {
    Claim,
    Verification,
    PaykitStatus,
    EntitlementPublication,
    PaykitInvoice,
    AdmissionPersistence,
    RetrySchedule,
    Task,
}

impl TimingStage {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Claim => "claim",
            Self::Verification => "verification",
            Self::PaykitStatus => "paykit_status",
            Self::EntitlementPublication => "entitlement_publication",
            Self::PaykitInvoice => "paykit_invoice",
            Self::AdmissionPersistence => "admission_persistence",
            Self::RetrySchedule => "retry_schedule",
            Self::Task => "task",
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum TimingOutcome {
    Claimed,
    Completed,
    Expired,
    RetryRequired,
    RetryScheduled,
    Failed,
    ClaimLost,
    Error,
}

impl TimingOutcome {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Claimed => "claimed",
            Self::Completed => "completed",
            Self::Expired => "expired",
            Self::RetryRequired => "retry_required",
            Self::RetryScheduled => "retry_scheduled",
            Self::Failed => "failed",
            Self::ClaimLost => "claim_lost",
            Self::Error => "error",
        }
    }
}

pub(super) fn record_timing(
    worker: TimingWorker,
    stage: TimingStage,
    outcome: TimingOutcome,
    started: Instant,
) {
    tracing::info!(
        worker = worker.as_str(),
        stage = stage.as_str(),
        outcome = outcome.as_str(),
        elapsed_ms = elapsed_millis(started),
        "worker task timing"
    );
}

fn elapsed_millis(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}
