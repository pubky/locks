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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fmt;
    use std::sync::{Arc, Mutex};
    use std::time::Instant;

    use tracing::{Event, Level, Subscriber};
    use tracing_subscriber::layer::{Context, SubscriberExt};
    use tracing_subscriber::{EnvFilter, Layer, Registry};

    use super::{TimingOutcome, TimingStage, TimingWorker, record_timing};
    use crate::config::LoggingConfig;

    #[test]
    fn timing_event_is_visible_at_default_info_without_identifier_fields() {
        record_timing(
            TimingWorker::Verification,
            TimingStage::PaykitStatus,
            TimingOutcome::Completed,
            Instant::now(),
        );
        let events = RecordedEvents::default();
        let filter = EnvFilter::new(LoggingConfig::default().level);
        let subscriber = Registry::default().with(filter).with(events.clone());
        let _guard = tracing::subscriber::set_default(subscriber);
        tracing::callsite::rebuild_interest_cache();

        record_timing(
            TimingWorker::Verification,
            TimingStage::PaykitStatus,
            TimingOutcome::Completed,
            Instant::now(),
        );

        let events = events.0.lock().unwrap();
        assert_eq!(
            events.len(),
            1,
            "default info filter must emit timing event"
        );
        let event = &events[0];
        assert_eq!(event.level, Level::INFO);
        assert_eq!(event.target, "locks_server::worker::timing");
        assert_eq!(event.fields.get("worker").unwrap(), "verification");
        assert_eq!(event.fields.get("stage").unwrap(), "paykit_status");
        assert_eq!(event.fields.get("outcome").unwrap(), "completed");
        assert!(event.fields.contains_key("elapsed_ms"));
        let mut field_names = event.fields.keys().map(String::as_str).collect::<Vec<_>>();
        field_names.sort_unstable();
        assert_eq!(
            field_names,
            ["elapsed_ms", "message", "outcome", "stage", "worker"]
        );
    }

    #[derive(Clone, Default)]
    struct RecordedEvents(Arc<Mutex<Vec<RecordedEvent>>>);

    struct RecordedEvent {
        level: Level,
        target: &'static str,
        fields: HashMap<String, String>,
    }

    impl<S> Layer<S> for RecordedEvents
    where
        S: Subscriber,
    {
        fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
            let mut fields = HashMap::new();
            event.record(&mut FieldVisitor(&mut fields));
            self.0.lock().unwrap().push(RecordedEvent {
                level: *event.metadata().level(),
                target: event.metadata().target(),
                fields,
            });
        }
    }

    struct FieldVisitor<'a>(&'a mut HashMap<String, String>);

    impl tracing::field::Visit for FieldVisitor<'_> {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
            self.0.insert(field.name().to_owned(), format!("{value:?}"));
        }

        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            self.0.insert(field.name().to_owned(), value.to_owned());
        }

        fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
            self.0.insert(field.name().to_owned(), value.to_string());
        }
    }
}
