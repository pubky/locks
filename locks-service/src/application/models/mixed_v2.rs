use locks_core::ids::{BundleId, CreatorPubky, LockHash, LockId, PubkyLockResource, TaskId};
use locks_core::lock_policy::{GraphOperatorV2, GraphPolicyV2, PaymentPolicyV2};
use locks_core::verification::{SatisfiedByV2, SubmittedProofBundleV2, VerifiedProofBundleV2};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use super::{
    EvaluatorTaskIdentityV1, EvaluatorTaskSnapshotV1, GraphPredicateState, GraphRootState,
    GraphTaskSnapshotV1, PaymentInvoiceState, PaymentOperationalErrorCategory,
    PaymentProviderStatus, PaymentRequestStateV2, PaymentRootState, PaymentStateV2,
    PaymentTaskSnapshotV1, PaymentTermsV2, VerificationTaskStatus, VerificationTerminalReason,
};

pub const MIXED_TASK_SNAPSHOT_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MixedTaskRecord {
    pub task_id: TaskId,
    pub snapshot: MixedTaskSnapshot,
    pub evaluator_revision: u64,
    pub status: VerificationTaskStatus,
    pub terminal_reason: Option<VerificationTerminalReason>,
    pub entitlement_to_publish: Option<VerifiedProofBundleV2>,
    pub entitlement_publication_token: Option<uuid::Uuid>,
    pub completed_at: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedMixedTask {
    pub task: MixedTaskRecord,
    pub claim_token: uuid::Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MixedTaskSnapshot {
    V1(MixedTaskSnapshotV1),
}

impl MixedTaskSnapshot {
    pub fn to_json(&self) -> Result<serde_json::Value, serde_json::Error> {
        match self {
            Self::V1(value) => serde_json::to_value(value),
        }
    }

    pub fn from_json(value: serde_json::Value) -> Result<Self, serde_json::Error> {
        let version = value
            .get("schema_version")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| json_error("missing mixed task snapshot schema_version"))?;
        if version != u64::from(MIXED_TASK_SNAPSHOT_VERSION) {
            return Err(json_error("unsupported mixed task snapshot schema_version"));
        }
        let snapshot: MixedTaskSnapshotV1 = serde_json::from_value(value)?;
        snapshot.validate().map_err(json_error)?;
        Ok(Self::V1(snapshot))
    }

    pub fn as_v1(&self) -> &MixedTaskSnapshotV1 {
        match self {
            Self::V1(value) => value,
        }
    }

    pub fn as_v1_mut(&mut self) -> &mut MixedTaskSnapshotV1 {
        match self {
            Self::V1(value) => value,
        }
    }
}

impl EvaluatorTaskSnapshotV1 for MixedTaskSnapshot {
    fn identity(&self) -> EvaluatorTaskIdentityV1 {
        self.as_v1().identity()
    }
}

fn json_error(message: impl Into<String>) -> serde_json::Error {
    serde_json::Error::io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message.into(),
    ))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct MixedGraphStateV1 {
    pub operator: Option<GraphOperatorV2>,
    pub predicates: Vec<GraphPredicateState>,
    pub root: GraphRootState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct MixedPaymentStateV1 {
    pub payment: PaymentTermsV2,
    pub invoice: PaymentInvoiceState,
    pub provider_status: Option<PaymentProviderStatus>,
    pub operational_failure_count: u32,
    pub operational_failure_total: u32,
    #[serde(default, with = "time::serde::rfc3339::option")]
    pub operational_first_failure_at: Option<OffsetDateTime>,
    #[serde(default, with = "time::serde::rfc3339::option")]
    pub operational_last_failure_at: Option<OffsetDateTime>,
    pub operational_last_category: Option<PaymentOperationalErrorCategory>,
    pub operational_last_http_status: Option<u16>,
    #[serde(default, with = "time::serde::rfc3339::option")]
    pub next_due_at: Option<OffsetDateTime>,
    pub root: PaymentRootState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct MixedTaskSnapshotV1 {
    pub schema_version: u16,
    pub creator: CreatorPubky,
    pub bundle_id: BundleId,
    pub submitted_proof_bundle: SubmittedProofBundleV2,
    pub pubky_lock_resource: PubkyLockResource,
    pub lock_hash: [u8; 32],
    pub reader: CreatorPubky,
    #[serde(with = "time::serde::rfc3339")]
    pub accepted_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub deadline_at: OffsetDateTime,
    pub graph: MixedGraphStateV1,
    pub payment: MixedPaymentStateV1,
    pub root: MixedRootState,
    pub satisfied_by: Vec<SatisfiedByV2>,
}

impl MixedTaskSnapshotV1 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        submitted: SubmittedProofBundleV2,
        lock_hash: [u8; 32],
        reader: CreatorPubky,
        graph: GraphPolicyV2,
        payment: PaymentPolicyV2,
        accepted_at: OffsetDateTime,
        verification_window_seconds: u64,
    ) -> Result<Self, String> {
        if submitted.reader_pubky.is_some() {
            return Err("mixed task wire bundle must omit reader".to_owned());
        }
        let creator = submitted.pubky_lock_resource.creator().clone();
        let graph_snapshot = GraphTaskSnapshotV1::new(
            creator.clone(),
            submitted.bundle_id.clone(),
            submitted.pubky_lock_resource.clone(),
            lock_hash,
            reader.clone(),
            accepted_at,
            verification_window_seconds,
            graph,
        )?;
        let mut payment_submission = submitted.clone();
        payment_submission.reader_pubky = Some(reader.clone());
        let payment_snapshot = PaymentTaskSnapshotV1::new(
            payment_submission,
            lock_hash,
            payment,
            accepted_at,
            verification_window_seconds,
        )?;
        let snapshot = Self {
            schema_version: MIXED_TASK_SNAPSHOT_VERSION,
            creator,
            bundle_id: submitted.bundle_id.clone(),
            submitted_proof_bundle: submitted,
            pubky_lock_resource: graph_snapshot.pubky_lock_resource.clone(),
            lock_hash,
            reader,
            accepted_at,
            deadline_at: graph_snapshot.deadline_at,
            graph: MixedGraphStateV1 {
                operator: graph_snapshot.operator,
                predicates: graph_snapshot.predicates,
                root: graph_snapshot.root,
            },
            payment: MixedPaymentStateV1::from_payment_snapshot(payment_snapshot),
            root: MixedRootState::Pending,
            satisfied_by: Vec::new(),
        };
        snapshot.validate_pristine_admission()?;
        Ok(snapshot)
    }

    pub fn identity(&self) -> EvaluatorTaskIdentityV1 {
        EvaluatorTaskIdentityV1 {
            creator: self.creator.clone(),
            bundle_id: self.bundle_id.clone(),
            pubky_lock_resource: self.pubky_lock_resource.clone(),
            lock_hash: self.lock_hash,
            reader: self.reader.clone(),
            accepted_at: self.accepted_at,
            deadline_at: self.deadline_at,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != MIXED_TASK_SNAPSHOT_VERSION {
            return Err("unsupported mixed task snapshot schema_version".to_owned());
        }
        if self.submitted_proof_bundle.reader_pubky.is_some()
            || self.creator != *self.pubky_lock_resource.creator()
            || self.submitted_proof_bundle.pubky_lock_resource != self.pubky_lock_resource
            || self.submitted_proof_bundle.bundle_id != self.bundle_id
        {
            return Err("mixed task identity diverges".to_owned());
        }
        let expected_lock_id = LockId::from_hash(LockHash::from_bytes(self.lock_hash));
        if self.pubky_lock_resource.lock_id() != &expected_lock_id {
            return Err("mixed task lock hash diverges from resource".to_owned());
        }
        self.graph_snapshot()?.validate()?;
        self.payment_snapshot()?.validate()?;
        match self.root {
            MixedRootState::Pending if !self.satisfied_by.is_empty() => {
                return Err("pending mixed task has satisfaction path".to_owned());
            }
            MixedRootState::Satisfied if !is_canonical_satisfaction(&self.satisfied_by) => {
                return Err("satisfied mixed task lacks canonical satisfaction path".to_owned());
            }
            MixedRootState::Expired(_) | MixedRootState::Failed(_)
                if !self.satisfied_by.is_empty() =>
            {
                return Err("terminal unsuccessful mixed task has satisfaction path".to_owned());
            }
            _ => {}
        }
        if self.root == MixedRootState::Satisfied {
            let status = self
                .payment
                .provider_status
                .as_ref()
                .ok_or_else(|| "satisfied mixed task lacks provider status".to_owned())?;
            let payment_live = matches!(
                status.request_state,
                PaymentRequestStateV2::Accepted
                    | PaymentRequestStateV2::ProofSubmitted
                    | PaymentRequestStateV2::ActiveRecurring
            ) && status.payment_state != PaymentStateV2::Expired;
            let payment_satisfied = status.first_threshold_satisfied.is_some();
            let graph_satisfied = self.graph.root == GraphRootState::Satisfied;
            let path_has_payment = self.satisfied_by.contains(&SatisfiedByV2::Payment);
            let path_has_graph = self.satisfied_by.contains(&SatisfiedByV2::ZeroConfGraph);
            let zero_conf_satisfied = graph_satisfied && status.is_zero_conf_eligible();
            if !payment_live
                || payment_satisfied != path_has_payment
                || zero_conf_satisfied != path_has_graph
                || (self.payment.root == PaymentRootState::Satisfied) != payment_satisfied
            {
                return Err("mixed satisfaction path diverges from evidence".to_owned());
            }
        }
        Ok(())
    }

    pub fn validate_pristine_admission(&self) -> Result<(), String> {
        self.validate()?;
        if self.root != MixedRootState::Pending
            || !self.satisfied_by.is_empty()
            || self.graph.root != GraphRootState::Pending
            || self.payment.root != PaymentRootState::Pending
        {
            return Err("new mixed task must be pristine".to_owned());
        }
        self.graph_snapshot()?.validate_pristine_admission()?;
        self.payment_snapshot()?.validate_pristine_admission()
    }

    pub fn validate_progress_from(&self, current: &Self) -> Result<(), String> {
        current.validate()?;
        self.validate()?;
        if current.root != MixedRootState::Pending
            && (self.root != current.root || self.satisfied_by != current.satisfied_by)
        {
            return Err("terminal mixed root cannot change".to_owned());
        }
        if self.schema_version != current.schema_version
            || self.creator != current.creator
            || self.bundle_id != current.bundle_id
            || self.submitted_proof_bundle != current.submitted_proof_bundle
            || self.pubky_lock_resource != current.pubky_lock_resource
            || self.lock_hash != current.lock_hash
            || self.reader != current.reader
            || self.accepted_at != current.accepted_at
            || self.deadline_at != current.deadline_at
        {
            return Err("mixed snapshot immutable binding changed".to_owned());
        }
        self.graph_snapshot()?
            .validate_progress_from(&current.graph_snapshot()?)?;
        self.payment_snapshot()?
            .validate_progress_from(&current.payment_snapshot()?)
    }

    pub fn apply_create_response(
        &mut self,
        invoice_created_at: OffsetDateTime,
        proposal_expires_at: OffsetDateTime,
        deadline: OffsetDateTime,
    ) -> Result<(), String> {
        let current = self.clone();
        let mut payment = self.payment_snapshot()?;
        payment.apply_create_response(invoice_created_at, proposal_expires_at, deadline)?;
        self.payment = MixedPaymentStateV1::from_payment_snapshot(payment);
        self.validate_progress_from(&current)
    }

    pub fn apply_payment_status(&mut self, status: PaymentProviderStatus) -> Result<(), String> {
        let current = self.clone();
        let mut payment = self.payment_snapshot()?;
        payment.apply_status(status)?;
        self.payment = MixedPaymentStateV1::from_payment_snapshot(payment);
        self.validate_progress_from(&current)
    }

    pub fn record_graph_observation(
        &mut self,
        predicate_index: usize,
        outcome: super::GraphObservationOutcome,
        observed_at: OffsetDateTime,
    ) -> Result<(), String> {
        let current = self.clone();
        let mut graph = self.graph_snapshot()?;
        graph.record_observation(predicate_index, outcome, observed_at)?;
        self.graph = MixedGraphStateV1 {
            operator: graph.operator,
            predicates: graph.predicates,
            root: graph.root,
        };
        self.validate_progress_from(&current)
    }

    pub fn graph_snapshot(&self) -> Result<GraphTaskSnapshotV1, String> {
        let snapshot = GraphTaskSnapshotV1 {
            schema_version: super::GRAPH_TASK_SNAPSHOT_VERSION,
            creator: self.creator.clone(),
            bundle_id: self.bundle_id.clone(),
            pubky_lock_resource: self.pubky_lock_resource.clone(),
            lock_hash: self.lock_hash,
            reader: self.reader.clone(),
            accepted_at: self.accepted_at,
            deadline_at: self.deadline_at,
            operator: self.graph.operator,
            predicates: self.graph.predicates.clone(),
            root: self.graph.root,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    pub fn payment_snapshot(&self) -> Result<PaymentTaskSnapshotV1, String> {
        let mut submitted = self.submitted_proof_bundle.clone();
        submitted.reader_pubky = Some(self.reader.clone());
        let snapshot = PaymentTaskSnapshotV1 {
            schema_version: super::PAYMENT_TASK_SNAPSHOT_VERSION,
            creator: self.creator.clone(),
            bundle_id: self.bundle_id.clone(),
            submitted_proof_bundle: submitted,
            pubky_lock_resource: self.pubky_lock_resource.clone(),
            lock_hash: self.lock_hash,
            reader: self.reader.clone(),
            payment: self.payment.payment.clone(),
            accepted_at: self.accepted_at,
            deadline_at: self.deadline_at,
            invoice: self.payment.invoice.clone(),
            provider_status: self.payment.provider_status.clone(),
            operational_failure_count: self.payment.operational_failure_count,
            operational_failure_total: self.payment.operational_failure_total,
            operational_first_failure_at: self.payment.operational_first_failure_at,
            operational_last_failure_at: self.payment.operational_last_failure_at,
            operational_last_category: self.payment.operational_last_category,
            operational_last_http_status: self.payment.operational_last_http_status,
            next_due_at: self.payment.next_due_at,
            root: self.payment.root,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    pub fn replace_graph_snapshot(&mut self, graph: GraphTaskSnapshotV1) {
        self.graph = MixedGraphStateV1 {
            operator: graph.operator,
            predicates: graph.predicates,
            root: graph.root,
        };
    }

    pub fn replace_payment_snapshot(&mut self, payment: PaymentTaskSnapshotV1) {
        self.payment = MixedPaymentStateV1::from_payment_snapshot(payment);
    }
}

impl MixedPaymentStateV1 {
    fn from_payment_snapshot(snapshot: PaymentTaskSnapshotV1) -> Self {
        Self {
            payment: snapshot.payment,
            invoice: snapshot.invoice,
            provider_status: snapshot.provider_status,
            operational_failure_count: snapshot.operational_failure_count,
            operational_failure_total: snapshot.operational_failure_total,
            operational_first_failure_at: snapshot.operational_first_failure_at,
            operational_last_failure_at: snapshot.operational_last_failure_at,
            operational_last_category: snapshot.operational_last_category,
            operational_last_http_status: snapshot.operational_last_http_status,
            next_due_at: snapshot.next_due_at,
            root: snapshot.root,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status", content = "reason")]
pub enum MixedRootState {
    Pending,
    Satisfied,
    Expired(VerificationTerminalReason),
    Failed(VerificationTerminalReason),
}

impl MixedRootState {
    pub const fn terminal_status(self) -> Option<VerificationTaskStatus> {
        match self {
            Self::Pending => None,
            Self::Satisfied => Some(VerificationTaskStatus::PublishingEntitlement),
            Self::Expired(_) => Some(VerificationTaskStatus::Expired),
            Self::Failed(_) => Some(VerificationTaskStatus::Failed),
        }
    }

    pub const fn terminal_reason(self) -> Option<VerificationTerminalReason> {
        match self {
            Self::Pending | Self::Satisfied => None,
            Self::Expired(reason) | Self::Failed(reason) => Some(reason),
        }
    }
}

fn is_canonical_satisfaction(values: &[SatisfiedByV2]) -> bool {
    matches!(
        values,
        [SatisfiedByV2::Payment]
            | [SatisfiedByV2::ZeroConfGraph]
            | [SatisfiedByV2::Payment, SatisfiedByV2::ZeroConfGraph]
    )
}
