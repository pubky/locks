use locks_core::ids::{BundleId, CreatorPubky, LockHash, LockId, PubkyLockResource, TaskId};
use locks_core::lock_policy::PaymentPolicyV2;
use locks_core::verification::{SubmittedProofBundleV2, VerifiedProofBundleV2};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use super::{
    EvaluatorTaskIdentityV1, EvaluatorTaskSnapshotV1, VerificationTaskStatus,
    VerificationTerminalReason,
};

pub const PAYMENT_TASK_SNAPSHOT_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaymentProviderCreateRequest {
    pub bundle_id: BundleId,
    pub lock_resource: PubkyLockResource,
    pub reader: CreatorPubky,
    pub payment: PaymentTermsV2,
    pub deadline: OffsetDateTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaymentTaskRecord {
    pub task_id: TaskId,
    pub snapshot: PaymentTaskSnapshot,
    pub evaluator_revision: u64,
    pub status: VerificationTaskStatus,
    pub entitlement_to_publish: Option<VerifiedProofBundleV2>,
    pub entitlement_publication_token: Option<uuid::Uuid>,
    pub completed_at: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedPaymentTask {
    pub task: PaymentTaskRecord,
    pub claim_token: uuid::Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PaymentTaskSnapshot {
    V1(PaymentTaskSnapshotV1),
}

impl PaymentTaskSnapshot {
    pub fn to_json(&self) -> Result<serde_json::Value, serde_json::Error> {
        match self {
            Self::V1(value) => serde_json::to_value(value),
        }
    }

    pub fn from_json(value: serde_json::Value) -> Result<Self, serde_json::Error> {
        let version = value
            .get("schema_version")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| json_error("missing payment task snapshot schema_version"))?;
        if version != u64::from(PAYMENT_TASK_SNAPSHOT_VERSION) {
            return Err(json_error(
                "unsupported payment task snapshot schema_version",
            ));
        }
        let snapshot: PaymentTaskSnapshotV1 = serde_json::from_value(value)?;
        snapshot.validate().map_err(json_error)?;
        Ok(Self::V1(snapshot))
    }

    pub fn as_v1(&self) -> &PaymentTaskSnapshotV1 {
        match self {
            Self::V1(value) => value,
        }
    }
    pub fn as_v1_mut(&mut self) -> &mut PaymentTaskSnapshotV1 {
        match self {
            Self::V1(value) => value,
        }
    }
}

impl EvaluatorTaskSnapshotV1 for PaymentTaskSnapshot {
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
pub struct PaymentTaskSnapshotV1 {
    pub schema_version: u16,
    pub creator: CreatorPubky,
    pub bundle_id: BundleId,
    pub submitted_proof_bundle: SubmittedProofBundleV2,
    pub pubky_lock_resource: PubkyLockResource,
    pub lock_hash: [u8; 32],
    pub reader: CreatorPubky,
    pub payment: PaymentTermsV2,
    #[serde(with = "time::serde::rfc3339")]
    pub accepted_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub deadline_at: OffsetDateTime,
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

#[derive(Debug, Clone, Copy)]
pub struct PaymentEvaluationV1<'a> {
    pub submitted_proof_bundle: &'a SubmittedProofBundleV2,
    pub payment: &'a PaymentTermsV2,
    pub invoice: &'a PaymentInvoiceState,
    pub provider_status: &'a Option<PaymentProviderStatus>,
    pub operational_failure_count: u32,
    pub operational_failure_total: u32,
    pub operational_first_failure_at: Option<OffsetDateTime>,
    pub operational_last_failure_at: Option<OffsetDateTime>,
    pub operational_last_category: Option<PaymentOperationalErrorCategory>,
    pub operational_last_http_status: Option<u16>,
    pub next_due_at: Option<OffsetDateTime>,
    pub root: &'a PaymentRootState,
}

impl PaymentTaskSnapshotV1 {
    pub fn evaluation(&self) -> PaymentEvaluationV1<'_> {
        PaymentEvaluationV1 {
            submitted_proof_bundle: &self.submitted_proof_bundle,
            payment: &self.payment,
            invoice: &self.invoice,
            provider_status: &self.provider_status,
            operational_failure_count: self.operational_failure_count,
            operational_failure_total: self.operational_failure_total,
            operational_first_failure_at: self.operational_first_failure_at,
            operational_last_failure_at: self.operational_last_failure_at,
            operational_last_category: self.operational_last_category,
            operational_last_http_status: self.operational_last_http_status,
            next_due_at: self.next_due_at,
            root: &self.root,
        }
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

    pub fn new(
        submitted: SubmittedProofBundleV2,
        lock_hash: [u8; 32],
        payment: PaymentPolicyV2,
        accepted_at: OffsetDateTime,
        verification_window_seconds: u64,
    ) -> Result<Self, String> {
        let reader = submitted
            .reader_pubky
            .clone()
            .ok_or_else(|| "payment task requires reader".to_owned())?;
        let creator = submitted.pubky_lock_resource.creator().clone();
        if payment.recipient_pubky != creator
            || payment.asset != "BTC"
            || payment.minimum_confirmations > 6
        {
            return Err("invalid payment task policy".to_owned());
        }
        let amount_sats = payment
            .amount
            .parse::<u64>()
            .map_err(|_| "invalid payment task amount".to_owned())?;
        if amount_sats == 0 || !payment.amount.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err("invalid payment task amount".to_owned());
        }
        let seconds = i64::try_from(verification_window_seconds)
            .map_err(|_| "verification window exceeds supported duration".to_owned())?;
        if !(60..=604_800).contains(&seconds) {
            return Err("invalid verification window".to_owned());
        }
        let deadline_at = accepted_at
            .checked_add(time::Duration::seconds(seconds))
            .ok_or_else(|| "verification deadline overflow".to_owned())?;
        let snapshot = Self {
            schema_version: PAYMENT_TASK_SNAPSHOT_VERSION,
            creator,
            bundle_id: submitted.bundle_id.clone(),
            pubky_lock_resource: submitted.pubky_lock_resource.clone(),
            submitted_proof_bundle: submitted,
            lock_hash,
            reader,
            payment: PaymentTermsV2 {
                recipient_pubky: payment.recipient_pubky,
                amount: payment.amount,
                amount_sats,
                asset: PaymentAssetV2::Btc,
                minimum_confirmations: payment.minimum_confirmations,
            },
            accepted_at,
            deadline_at,
            invoice: PaymentInvoiceState::Pending,
            provider_status: None,
            operational_failure_count: 0,
            operational_failure_total: 0,
            operational_first_failure_at: None,
            operational_last_failure_at: None,
            operational_last_category: None,
            operational_last_http_status: None,
            next_due_at: None,
            root: PaymentRootState::Pending,
        };
        snapshot.validate_pristine_admission()?;
        Ok(snapshot)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != PAYMENT_TASK_SNAPSHOT_VERSION {
            return Err("unsupported payment task snapshot schema_version".to_owned());
        }
        if !self.accepted_at.nanosecond().is_multiple_of(1_000)
            || !self.deadline_at.nanosecond().is_multiple_of(1_000)
        {
            return Err("payment task timestamps must use microsecond precision".to_owned());
        }
        let window = self.deadline_at - self.accepted_at;
        if !(60..=604_800).contains(&window.whole_seconds())
            || window != time::Duration::seconds(window.whole_seconds())
        {
            return Err("invalid payment task verification window".to_owned());
        }
        if self.pubky_lock_resource.creator() != &self.creator
            || self.submitted_proof_bundle.pubky_lock_resource != self.pubky_lock_resource
            || self.submitted_proof_bundle.bundle_id != self.bundle_id
            || self.submitted_proof_bundle.reader_pubky.as_ref() != Some(&self.reader)
        {
            return Err("payment task identity diverges".to_owned());
        }
        let expected_lock_id = LockId::from_hash(LockHash::from_bytes(self.lock_hash));
        if self.pubky_lock_resource.lock_id() != &expected_lock_id {
            return Err("payment task lock hash diverges from resource".to_owned());
        }
        self.payment.validate(&self.creator)?;
        if let PaymentInvoiceState::Created {
            invoice_created_at,
            proposal_expires_at,
        } = self.invoice
        {
            validate_timestamp(invoice_created_at)?;
            validate_timestamp(proposal_expires_at)?;
            if invoice_created_at < self.accepted_at
                || invoice_created_at > proposal_expires_at
                || proposal_expires_at >= self.deadline_at
            {
                return Err("invalid provider create timestamps".to_owned());
            }
        }
        if let Some(status) = &self.provider_status {
            if !matches!(self.invoice, PaymentInvoiceState::Created { .. }) {
                return Err("provider status exists before invoice creation".to_owned());
            }
            status.validate_against(self)?;
        }
        if self.operational_failure_total == 0 {
            if self.operational_failure_count != 0
                || self.operational_first_failure_at.is_some()
                || self.operational_last_failure_at.is_some()
                || self.operational_last_category.is_some()
                || self.operational_last_http_status.is_some()
            {
                return Err("payment operational aggregate is inconsistent".to_owned());
            }
        } else {
            let (Some(first), Some(last), Some(_)) = (
                self.operational_first_failure_at,
                self.operational_last_failure_at,
                self.operational_last_category,
            ) else {
                return Err("payment operational aggregate is incomplete".to_owned());
            };
            if self.operational_failure_count > self.operational_failure_total
                || first < self.accepted_at
                || first > last
                || last > self.deadline_at
            {
                return Err("payment operational aggregate is invalid".to_owned());
            }
        }
        if self.next_due_at.is_some_and(|due| due > self.deadline_at) {
            return Err("payment retry exceeds deadline".to_owned());
        }
        Ok(())
    }

    pub fn validate_pristine_admission(&self) -> Result<(), String> {
        self.validate()?;
        if self.invoice != PaymentInvoiceState::Pending
            || self.provider_status.is_some()
            || self.root != PaymentRootState::Pending
            || self.next_due_at.is_some()
            || self.operational_failure_total != 0
        {
            return Err("new payment task must be pristine".to_owned());
        }
        Ok(())
    }

    pub fn has_same_admission_binding(&self, other: &Self) -> bool {
        self.schema_version == other.schema_version
            && self.creator == other.creator
            && self.bundle_id == other.bundle_id
            && self.submitted_proof_bundle == other.submitted_proof_bundle
            && self.pubky_lock_resource == other.pubky_lock_resource
            && self.lock_hash == other.lock_hash
            && self.reader == other.reader
            && self.payment == other.payment
            && self.accepted_at == other.accepted_at
            && self.deadline_at == other.deadline_at
    }

    pub fn validate_progress_from(&self, current: &Self) -> Result<(), String> {
        current.validate()?;
        self.validate()?;
        if current.root != PaymentRootState::Pending && self.root != current.root {
            return Err("terminal payment root cannot change".to_owned());
        }
        if self.schema_version != current.schema_version
            || self.creator != current.creator
            || self.bundle_id != current.bundle_id
            || self.submitted_proof_bundle != current.submitted_proof_bundle
            || self.pubky_lock_resource != current.pubky_lock_resource
            || self.lock_hash != current.lock_hash
            || self.reader != current.reader
            || self.payment != current.payment
            || self.accepted_at != current.accepted_at
            || self.deadline_at != current.deadline_at
        {
            return Err("payment snapshot immutable binding changed".to_owned());
        }
        if matches!(current.invoice, PaymentInvoiceState::Created { .. })
            && self.invoice != current.invoice
        {
            return Err("provider create response changed".to_owned());
        }
        let old_first = current
            .provider_status
            .as_ref()
            .and_then(|s| s.first_threshold_satisfied.as_ref());
        let new_first = self
            .provider_status
            .as_ref()
            .and_then(|s| s.first_threshold_satisfied.as_ref());
        if old_first.is_some() && old_first != new_first {
            return Err("sticky payment threshold evidence changed".to_owned());
        }
        if self.operational_failure_total < current.operational_failure_total {
            return Err("payment operational total regressed".to_owned());
        }
        Ok(())
    }

    pub fn apply_create_response(
        &mut self,
        invoice_created_at: OffsetDateTime,
        proposal_expires_at: OffsetDateTime,
        deadline: OffsetDateTime,
    ) -> Result<(), String> {
        if self.invoice != PaymentInvoiceState::Pending || deadline != self.deadline_at {
            return Err("provider create response conflicts with payment task".to_owned());
        }
        let mut next = self.clone();
        next.invoice = PaymentInvoiceState::Created {
            invoice_created_at,
            proposal_expires_at,
        };
        next.validate_progress_from(self)?;
        *self = next;
        Ok(())
    }

    pub fn apply_status(&mut self, status: PaymentProviderStatus) -> Result<(), String> {
        status.validate_against(self)?;
        let mut next = self.clone();
        next.provider_status = Some(status);
        next.operational_failure_count = 0;
        next.next_due_at = None;
        next.validate_progress_from(self)?;
        *self = next;
        Ok(())
    }
}

fn validate_timestamp(value: OffsetDateTime) -> Result<(), String> {
    if value.nanosecond().is_multiple_of(1_000) {
        Ok(())
    } else {
        Err("provider timestamp exceeds microsecond precision".to_owned())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct PaymentTermsV2 {
    pub recipient_pubky: CreatorPubky,
    pub amount: String,
    pub amount_sats: u64,
    pub asset: PaymentAssetV2,
    pub minimum_confirmations: u8,
}

impl PaymentTermsV2 {
    fn validate(&self, creator: &CreatorPubky) -> Result<(), String> {
        if &self.recipient_pubky != creator
            || self.amount_sats == 0
            || self.amount.parse::<u64>().ok() != Some(self.amount_sats)
            || !self.amount.bytes().all(|byte| byte.is_ascii_digit())
            || self.asset != PaymentAssetV2::Btc
            || self.minimum_confirmations > 6
        {
            return Err("invalid persisted payment terms".to_owned());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PaymentAssetV2 {
    #[serde(rename = "BTC")]
    Btc,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status", deny_unknown_fields)]
pub enum PaymentInvoiceState {
    Pending,
    Created {
        #[serde(with = "time::serde::rfc3339")]
        invoice_created_at: OffsetDateTime,
        #[serde(with = "time::serde::rfc3339")]
        proposal_expires_at: OffsetDateTime,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaymentRequestStateV2 {
    Proposed,
    ProposalExpired,
    Accepted,
    Rejected,
    Canceled,
    ProofSubmitted,
    ActiveRecurring,
    RecoveryRequired,
    InvalidConflict,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaymentStateV2 {
    Undetected,
    Detected,
    Confirmed,
    Expired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaymentSampleStatusV2 {
    Undetected,
    Detected,
    Confirmed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct BitcoinOutpointV2 {
    pub txid: String,
    pub vout: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct PaymentSampleV2 {
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
    pub status: PaymentSampleStatusV2,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outpoint: Option<BitcoinOutpointV2>,
    pub confirmations: u8,
    pub amount_matched: bool,
}

impl PaymentSampleV2 {
    pub fn validate(&self) -> Result<(), String> {
        validate_timestamp(self.observed_at)?;
        if self.confirmations > 6 {
            return Err("provider confirmations exceed six".to_owned());
        }
        if let Some(outpoint) = &self.outpoint
            && (outpoint.txid.len() != 64
                || !outpoint
                    .txid
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()))
        {
            return Err("invalid bitcoin outpoint".to_owned());
        }
        let valid = match self.status {
            PaymentSampleStatusV2::Undetected => {
                self.outpoint.is_none() && self.confirmations == 0 && !self.amount_matched
            }
            PaymentSampleStatusV2::Detected => self.outpoint.is_some() && self.confirmations == 0,
            PaymentSampleStatusV2::Confirmed => self.outpoint.is_some() && self.confirmations > 0,
        };
        if valid {
            Ok(())
        } else {
            Err("impossible provider payment sample".to_owned())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct PaymentProviderStatus {
    pub creator: CreatorPubky,
    pub bundle_id: BundleId,
    pub request_state: PaymentRequestStateV2,
    pub payment_state: PaymentStateV2,
    #[serde(with = "time::serde::rfc3339")]
    pub invoice_created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub proposal_expires_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub deadline: OffsetDateTime,
    pub asset: PaymentAssetV2,
    pub minimum_confirmations: u8,
    pub current_sample: Option<PaymentSampleV2>,
    pub first_threshold_satisfied: Option<PaymentSampleV2>,
}

impl PaymentProviderStatus {
    pub fn validate(&self) -> Result<(), String> {
        for value in [
            self.invoice_created_at,
            self.proposal_expires_at,
            self.deadline,
        ] {
            validate_timestamp(value)?;
        }
        if self.invoice_created_at >= self.proposal_expires_at
            || self.proposal_expires_at >= self.deadline
            || self.minimum_confirmations > 6
            || self.asset != PaymentAssetV2::Btc
        {
            return Err("invalid provider status envelope".to_owned());
        }
        if let Some(current) = &self.current_sample {
            current.validate()?;
            if current.observed_at < self.invoice_created_at {
                return Err("current payment evidence predates invoice".to_owned());
            }
        }
        if let Some(first) = &self.first_threshold_satisfied {
            first.validate()?;
            if first.observed_at < self.invoice_created_at
                || first.outpoint.is_none()
                || !first.amount_matched
                || first.confirmations < self.minimum_confirmations
                || first.observed_at > self.deadline
                || first.status == PaymentSampleStatusV2::Undetected
            {
                return Err("invalid first threshold evidence".to_owned());
            }
        }
        Ok(())
    }

    fn validate_against(&self, snapshot: &PaymentTaskSnapshotV1) -> Result<(), String> {
        self.validate()?;
        if self.creator != snapshot.creator
            || self.bundle_id != snapshot.bundle_id
            || self.deadline != snapshot.deadline_at
            || self.asset != PaymentAssetV2::Btc
            || self.minimum_confirmations != snapshot.payment.minimum_confirmations
        {
            return Err("provider status binding mismatch".to_owned());
        }
        let PaymentInvoiceState::Created {
            invoice_created_at,
            proposal_expires_at,
        } = snapshot.invoice
        else {
            return Err("provider status precedes invoice creation".to_owned());
        };
        if self.invoice_created_at != invoice_created_at
            || self.proposal_expires_at != proposal_expires_at
        {
            return Err("provider status create timestamps changed".to_owned());
        }
        if let Some(current) = &self.current_sample {
            current.validate()?;
        }
        if let Some(first) = &self.first_threshold_satisfied {
            first.validate()?;
            if first.outpoint.is_none()
                || !first.amount_matched
                || first.confirmations < self.minimum_confirmations
                || first.observed_at > self.deadline
                || first.status == PaymentSampleStatusV2::Undetected
            {
                return Err("invalid first threshold evidence".to_owned());
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaymentOperationalErrorCategory {
    RateLimited,
    NotFound,
    Server,
    Timeout,
    Transport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status", content = "reason")]
pub enum PaymentRootState {
    Pending,
    Satisfied,
    Expired(VerificationTerminalReason),
    Failed(VerificationTerminalReason),
}

impl PaymentRootState {
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
