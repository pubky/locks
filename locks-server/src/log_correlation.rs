use locks_core::ids::{BundleId, CreatorPubky};
use tracing::{debug, error, info, warn};

const PUBKY_PREFIX: &str = "pubky";
const PUBKY_PAYLOAD_LEN: usize = 52;
const REF_EDGE_LEN: usize = 4;
const INVALID_REF: &str = "invalid";
const MISSING_REF: &str = "none";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CorrelationRefs {
    pub(crate) creator_ref: String,
    pub(crate) reader_ref: String,
    pub(crate) operation_ref: String,
}

impl CorrelationRefs {
    pub(crate) fn new(
        creator: &CreatorPubky,
        reader: Option<&CreatorPubky>,
        bundle_id: &BundleId,
    ) -> Self {
        Self {
            creator_ref: pubky_ref(&creator.to_string()),
            reader_ref: reader
                .map(|reader| pubky_ref(&reader.to_string()))
                .unwrap_or_else(|| MISSING_REF.to_owned()),
            operation_ref: operation_ref(bundle_id),
        }
    }
}

pub(crate) fn pubky_ref(value: &str) -> String {
    let Some(payload) = value.strip_prefix(PUBKY_PREFIX) else {
        return INVALID_REF.to_owned();
    };
    if payload.len() != PUBKY_PAYLOAD_LEN || !payload.is_ascii() {
        return INVALID_REF.to_owned();
    }
    bounded_ref(payload)
}

pub(crate) fn operation_ref(bundle_id: &BundleId) -> String {
    bounded_ref(bundle_id.as_str())
}

fn bounded_ref(value: &str) -> String {
    if value.len() < REF_EDGE_LEN * 2 || !value.is_ascii() {
        return INVALID_REF.to_owned();
    }
    format!(
        "{}...{}",
        &value[..REF_EDGE_LEN],
        &value[value.len() - REF_EDGE_LEN..]
    )
}

pub(crate) fn lifecycle_event(refs: &CorrelationRefs, stage: &'static str, outcome: &'static str) {
    info!(
        creator_ref = %refs.creator_ref,
        reader_ref = %refs.reader_ref,
        operation_ref = %refs.operation_ref,
        stage,
        outcome,
        "locks operation lifecycle"
    );
}

pub(crate) fn retry_event(refs: &CorrelationRefs, stage: &'static str, cause: &'static str) {
    debug!(
        creator_ref = %refs.creator_ref,
        reader_ref = %refs.reader_ref,
        operation_ref = %refs.operation_ref,
        stage,
        outcome = "retry_scheduled",
        cause,
        "locks operation lifecycle"
    );
}

pub(crate) fn rejection_event(refs: &CorrelationRefs, stage: &'static str, cause: &'static str) {
    warn!(
        creator_ref = %refs.creator_ref,
        reader_ref = %refs.reader_ref,
        operation_ref = %refs.operation_ref,
        stage,
        outcome = "rejected",
        cause,
        "locks operation lifecycle"
    );
}

pub(crate) fn failure_event(refs: &CorrelationRefs, stage: &'static str, cause: &'static str) {
    error!(
        creator_ref = %refs.creator_ref,
        reader_ref = %refs.reader_ref,
        operation_ref = %refs.operation_ref,
        stage,
        outcome = "failed",
        cause,
        "locks operation lifecycle"
    );
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::str::FromStr;
    use std::sync::{Arc, Mutex};

    use locks_core::ids::{BundleId, CreatorPubky};
    use tracing::subscriber::with_default;

    use super::{CorrelationRefs, failure_event, lifecycle_event, operation_ref, pubky_ref};

    const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
    const READER: &str = "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo";
    const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";

    #[test]
    fn pubky_refs_strip_shared_prefix_and_do_not_expose_full_keys() {
        let creator_ref = pubky_ref(CREATOR);
        let reader_ref = pubky_ref(READER);

        assert_eq!(creator_ref, "tkrq...p7qy");
        assert_eq!(reader_ref, "7ir1...g9eo");
        assert_ne!(creator_ref, reader_ref);
        assert!(!creator_ref.starts_with("pubk"));
        assert!(!reader_ref.starts_with("pubk"));
        assert!(!creator_ref.contains(CREATOR));
        assert!(!reader_ref.contains(READER));
    }

    #[test]
    fn pubky_refs_fail_closed_for_bad_prefix_length_or_non_ascii_input() {
        for value in [
            "tkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            "pubkyshort",
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxpéy",
        ] {
            assert_eq!(pubky_ref(value), "invalid");
        }
    }

    #[test]
    fn operation_refs_are_stable_and_separate_different_bundles() {
        let first = BundleId::from_str(BUNDLE_ID).unwrap();
        let first_after_restart = BundleId::from_str(BUNDLE_ID).unwrap();
        let second = BundleId::from_bytes([2; 16]);

        assert_eq!(operation_ref(&first), operation_ref(&first_after_restart));
        assert_ne!(operation_ref(&first), operation_ref(&second));
        assert_eq!(operation_ref(&first), "000G...8E1W");
        assert!(!operation_ref(&first).contains(BUNDLE_ID));
    }

    #[test]
    fn lifecycle_trace_has_exact_correlation_fields_without_full_values() {
        let refs = correlation_refs();
        let output = capture_trace(|| lifecycle_event(&refs, "proof_submission", "accepted"));

        for expected in [
            "creator_ref=tkrq...p7qy",
            "reader_ref=7ir1...g9eo",
            "operation_ref=000G...8E1W",
            "stage=\"proof_submission\"",
            "outcome=\"accepted\"",
        ] {
            assert!(
                output.contains(expected),
                "missing {expected:?} in {output:?}"
            );
        }
        for forbidden in [CREATOR, READER, BUNDLE_ID, "creator_ref=pubk"] {
            assert!(
                !output.contains(forbidden),
                "found forbidden value {forbidden:?} in {output:?}"
            );
        }
    }

    #[test]
    fn failure_trace_has_bounded_refs_stage_outcome_and_cause() {
        let refs = correlation_refs();
        let output = capture_trace(|| failure_event(&refs, "connection_state_lookup", "timeout"));

        for expected in [
            "creator_ref=tkrq...p7qy",
            "reader_ref=7ir1...g9eo",
            "operation_ref=000G...8E1W",
            "stage=\"connection_state_lookup\"",
            "outcome=\"failed\"",
            "cause=\"timeout\"",
        ] {
            assert!(
                output.contains(expected),
                "missing {expected:?} in {output:?}"
            );
        }
        for forbidden in [CREATOR, READER, BUNDLE_ID] {
            assert!(!output.contains(forbidden));
        }
    }

    #[test]
    fn correlation_refs_include_only_bounded_values() {
        let refs = correlation_refs();

        assert_eq!(refs.creator_ref, "tkrq...p7qy");
        assert_eq!(refs.reader_ref, "7ir1...g9eo");
        assert_eq!(refs.operation_ref, "000G...8E1W");
        let debug = format!("{refs:?}");
        assert!(!debug.contains(CREATOR));
        assert!(!debug.contains(READER));
        assert!(!debug.contains(BUNDLE_ID));
    }

    fn correlation_refs() -> CorrelationRefs {
        CorrelationRefs::new(
            &CreatorPubky::from_str(CREATOR).unwrap(),
            Some(&CreatorPubky::from_str(READER).unwrap()),
            &BundleId::from_str(BUNDLE_ID).unwrap(),
        )
    }

    fn capture_trace(emit: impl FnOnce()) -> String {
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer({
                let bytes = Arc::clone(&bytes);
                move || BufferWriter(Arc::clone(&bytes))
            })
            .finish();
        with_default(subscriber, emit);
        let output = bytes.lock().unwrap().clone();
        String::from_utf8(output).unwrap()
    }

    struct BufferWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for BufferWriter {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
}
