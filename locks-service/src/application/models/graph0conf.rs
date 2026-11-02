use std::collections::HashSet;

use locks_core::ids::{BundleId, CreatorPubky, LockHash, LockId, PubkyLockResource, TaskId};
use locks_core::lock_policy::{GraphOperatorV2, GraphPolicyV2, GraphPredicateV2};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

pub const GRAPH_TASK_SNAPSHOT_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphTaskRecord {
    pub task_id: TaskId,
    pub snapshot: GraphTaskSnapshot,
    pub evaluator_revision: u64,
    pub status: super::VerificationTaskStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedGraphTask {
    pub task: GraphTaskRecord,
    pub claim_token: uuid::Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphTaskSnapshot {
    V1(GraphTaskSnapshotV1),
}

impl GraphTaskSnapshot {
    pub fn to_json(&self) -> Result<serde_json::Value, serde_json::Error> {
        match self {
            Self::V1(snapshot) => serde_json::to_value(snapshot),
        }
    }

    pub fn from_json(value: serde_json::Value) -> Result<Self, serde_json::Error> {
        let version = value
            .get("schema_version")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| json_error("missing graph task snapshot schema_version"))?;
        if version != u64::from(GRAPH_TASK_SNAPSHOT_VERSION) {
            return Err(json_error("unsupported graph task snapshot schema_version"));
        }
        let snapshot: GraphTaskSnapshotV1 = serde_json::from_value(value)?;
        snapshot.validate().map_err(json_error)?;
        Ok(Self::V1(snapshot))
    }

    pub fn as_v1(&self) -> &GraphTaskSnapshotV1 {
        match self {
            Self::V1(snapshot) => snapshot,
        }
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
pub struct GraphTaskSnapshotV1 {
    pub schema_version: u16,
    pub creator: CreatorPubky,
    pub bundle_id: BundleId,
    pub pubky_lock_resource: PubkyLockResource,
    pub lock_hash: [u8; 32],
    pub reader: CreatorPubky,
    #[serde(with = "time::serde::rfc3339")]
    pub accepted_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub deadline_at: OffsetDateTime,
    pub operator: Option<GraphOperatorV2>,
    pub predicates: Vec<GraphPredicateState>,
    pub root: GraphRootState,
}

impl GraphTaskSnapshotV1 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        creator: CreatorPubky,
        bundle_id: BundleId,
        pubky_lock_resource: PubkyLockResource,
        lock_hash: [u8; 32],
        reader: CreatorPubky,
        accepted_at: OffsetDateTime,
        verification_window_seconds: u64,
        graph: GraphPolicyV2,
    ) -> Result<Self, String> {
        if !(60..=604_800).contains(&verification_window_seconds) {
            return Err("verification window must be in 60..=604800 seconds".to_owned());
        }
        let seconds = i64::try_from(verification_window_seconds)
            .map_err(|_| "verification window exceeds supported duration".to_owned())?;
        let deadline_at = accepted_at
            .checked_add(time::Duration::seconds(seconds))
            .ok_or_else(|| "verification deadline overflow".to_owned())?;
        let snapshot = Self {
            schema_version: GRAPH_TASK_SNAPSHOT_VERSION,
            creator,
            bundle_id,
            pubky_lock_resource,
            lock_hash,
            reader,
            accepted_at,
            deadline_at,
            operator: graph.operator,
            predicates: graph
                .predicates
                .into_iter()
                .map(GraphPredicateState::new)
                .collect(),
            root: GraphRootState::Pending,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    pub fn record_observation(
        &mut self,
        predicate_index: usize,
        outcome: GraphObservationOutcome,
        observed_at: OffsetDateTime,
    ) -> Result<(), String> {
        self.validate()?;
        if self.root != GraphRootState::Pending {
            return Err("terminal graph task snapshot cannot accept observations".to_owned());
        }
        if observed_at < self.accepted_at || observed_at > self.deadline_at {
            return Err("graph observation falls outside accepted task window".to_owned());
        }
        let mut next = self.clone();
        let predicate = next
            .predicates
            .get_mut(predicate_index)
            .ok_or_else(|| "graph predicate index is out of bounds".to_owned())?;
        if predicate
            .latest_observation
            .as_ref()
            .is_some_and(|latest| observed_at < latest.observed_at)
        {
            return Err("graph observation precedes latest observation".to_owned());
        }
        let observation = GraphPredicateObservation {
            outcome,
            observed_at,
            origin: None,
            http_status: None,
        };
        predicate.latest_observation = Some(observation.clone());
        if outcome == GraphObservationOutcome::True && predicate.first_true_observation.is_none() {
            predicate.first_true_observation = Some(observation);
        }
        next.validate()?;
        *self = next;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_nexus_observation(
        &mut self,
        predicate_index: usize,
        outcome: GraphObservationOutcome,
        observed_at: OffsetDateTime,
        origin: String,
        http_status: Option<u16>,
        next_due_at: Option<OffsetDateTime>,
        operational_failure_count: u32,
    ) -> Result<(), String> {
        let mut next = self.clone();
        next.record_observation(predicate_index, outcome, observed_at)?;
        let predicate = next
            .predicates
            .get_mut(predicate_index)
            .ok_or_else(|| "graph predicate index is out of bounds".to_owned())?;
        let latest = predicate
            .latest_observation
            .as_mut()
            .ok_or_else(|| "graph predicate observation was not recorded".to_owned())?;
        latest.origin = Some(origin);
        latest.http_status = http_status;
        if outcome == GraphObservationOutcome::True {
            predicate.first_true_observation = Some(latest.clone());
        }
        predicate.next_due_at = next_due_at;
        predicate.operational_failure_count = operational_failure_count;
        next.validate()?;
        *self = next;
        Ok(())
    }

    pub fn terminalize_invalid_nexus_response(
        &mut self,
        predicate_index: usize,
        observed_at: OffsetDateTime,
        origin: String,
        http_status: Option<u16>,
    ) -> Result<(), String> {
        let mut next = self.clone();
        next.record_nexus_observation(
            predicate_index,
            GraphObservationOutcome::DependencyUnavailable,
            observed_at,
            origin,
            http_status,
            None,
            0,
        )?;
        next.predicates[predicate_index].terminal_invalid = true;
        next.validate()?;
        *self = next;
        Ok(())
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != GRAPH_TASK_SNAPSHOT_VERSION {
            return Err("unsupported graph task snapshot schema_version".to_owned());
        }
        if self.deadline_at < self.accepted_at {
            return Err("graph task deadline precedes acceptance".to_owned());
        }
        if !self.accepted_at.nanosecond().is_multiple_of(1_000)
            || !self.deadline_at.nanosecond().is_multiple_of(1_000)
        {
            return Err(
                "graph task acceptance and deadline must use microsecond precision".to_owned(),
            );
        }
        let verification_window = self.deadline_at - self.accepted_at;
        let verification_window_seconds = verification_window.whole_seconds();
        if !(60..=604_800).contains(&verification_window_seconds)
            || verification_window != time::Duration::seconds(verification_window_seconds)
        {
            return Err(
                "graph task verification window must be whole seconds in 60..=604800".to_owned(),
            );
        }
        if self.pubky_lock_resource.creator() != &self.creator {
            return Err("graph task creator diverges from lock resource".to_owned());
        }
        let expected_lock_id = LockId::from_hash(LockHash::from_bytes(self.lock_hash));
        if self.pubky_lock_resource.lock_id() != &expected_lock_id {
            return Err("graph task lock hash diverges from lock resource".to_owned());
        }
        if !(1..=8).contains(&self.predicates.len()) {
            return Err("graph task must contain 1..=8 predicates".to_owned());
        }
        if (self.predicates.len() == 1) != self.operator.is_none() {
            return Err("graph task operator does not match predicate cardinality".to_owned());
        }
        let unique: HashSet<_> = self
            .predicates
            .iter()
            .map(|state| &state.predicate)
            .collect();
        if unique.len() != self.predicates.len() {
            return Err("graph task contains duplicate predicates".to_owned());
        }
        for state in &self.predicates {
            state.validate(self.accepted_at, self.deadline_at)?;
        }
        if self.root != GraphRootState::Pending
            && self.root != self.terminal_root_from_observations()
        {
            return Err("graph task terminal root diverges from observations".to_owned());
        }
        Ok(())
    }

    pub fn validate_progress_from(&self, current: &Self) -> Result<(), String> {
        current.validate()?;
        self.validate()?;
        if current.root != GraphRootState::Pending {
            return Err("terminal graph task snapshot cannot be replaced".to_owned());
        }
        if current.predicates.len() != self.predicates.len()
            || current
                .predicates
                .iter()
                .zip(&self.predicates)
                .any(|(current, next)| current.predicate != next.predicate)
        {
            return Err("graph snapshot replacement changed predicate shape".to_owned());
        }
        for (current, next) in current.predicates.iter().zip(&self.predicates) {
            if current.terminal_invalid && !next.terminal_invalid {
                return Err("graph snapshot replacement cleared terminal-invalid state".to_owned());
            }
            if current.first_true_observation.is_some()
                && current.first_true_observation != next.first_true_observation
            {
                return Err("graph snapshot replacement changed sticky true evidence".to_owned());
            }
            match (&current.latest_observation, &next.latest_observation) {
                (Some(_), None) => {
                    return Err("graph snapshot replacement erased latest observation".to_owned());
                }
                (Some(current), Some(next)) if next.observed_at < current.observed_at => {
                    return Err("graph snapshot replacement regressed observation time".to_owned());
                }
                (Some(current), Some(next))
                    if next.observed_at == current.observed_at && next != current =>
                {
                    return Err(
                        "graph snapshot replacement changed equal-time observation".to_owned()
                    );
                }
                _ => {}
            }
            if current.first_true_observation.is_none()
                && next.first_true_observation.is_some()
                && next.first_true_observation != next.latest_observation
            {
                return Err(
                    "sticky true evidence must originate from the latest observation transition"
                        .to_owned(),
                );
            }
        }
        Ok(())
    }

    pub fn validate_pristine_admission(&self) -> Result<(), String> {
        self.validate()?;
        if self.root != GraphRootState::Pending
            || self.predicates.iter().any(|state| {
                state.latest_observation.is_some()
                    || state.first_true_observation.is_some()
                    || state.next_due_at.is_some()
                    || state.operational_failure_count != 0
                    || state.terminal_invalid
            })
        {
            return Err("new graph task snapshot must be pristine and pending".to_owned());
        }
        Ok(())
    }

    pub fn validate_observations_not_after(
        &self,
        evaluation_time: OffsetDateTime,
    ) -> Result<(), String> {
        if self.predicates.iter().any(|state| {
            [
                state.latest_observation.as_ref(),
                state.first_true_observation.as_ref(),
            ]
            .into_iter()
            .flatten()
            .any(|observation| observation.observed_at > evaluation_time)
        }) {
            return Err("graph observation is later than evaluation time".to_owned());
        }
        Ok(())
    }

    fn terminal_root_from_observations(&self) -> GraphRootState {
        let values: Vec<_> = self
            .predicates
            .iter()
            .map(|state| {
                if state.first_true_observation.is_some() {
                    TerminalPredicateTruth::True
                } else if state.terminal_invalid {
                    TerminalPredicateTruth::Invalid
                } else {
                    match state
                        .latest_observation
                        .as_ref()
                        .map(|observation| observation.outcome)
                    {
                        Some(GraphObservationOutcome::True) => TerminalPredicateTruth::True,
                        Some(GraphObservationOutcome::DependencyUnavailable) => {
                            TerminalPredicateTruth::Unavailable
                        }
                        Some(GraphObservationOutcome::False) | None => {
                            TerminalPredicateTruth::False
                        }
                    }
                }
            })
            .collect();
        let truth = match self.operator {
            None => values[0],
            Some(GraphOperatorV2::And) => {
                if values.contains(&TerminalPredicateTruth::Invalid) {
                    TerminalPredicateTruth::Invalid
                } else if values.contains(&TerminalPredicateTruth::False) {
                    TerminalPredicateTruth::False
                } else if values.contains(&TerminalPredicateTruth::Unavailable) {
                    TerminalPredicateTruth::Unavailable
                } else {
                    TerminalPredicateTruth::True
                }
            }
            Some(GraphOperatorV2::Or) => {
                if values.contains(&TerminalPredicateTruth::True) {
                    TerminalPredicateTruth::True
                } else if values
                    .iter()
                    .all(|value| *value == TerminalPredicateTruth::Invalid)
                {
                    TerminalPredicateTruth::Invalid
                } else if values.contains(&TerminalPredicateTruth::Unavailable) {
                    TerminalPredicateTruth::Unavailable
                } else {
                    TerminalPredicateTruth::False
                }
            }
        };
        match truth {
            TerminalPredicateTruth::True => GraphRootState::Satisfied,
            TerminalPredicateTruth::False => GraphRootState::Expired(GraphDeadlineResult::False),
            TerminalPredicateTruth::Unavailable => {
                GraphRootState::Expired(GraphDeadlineResult::DependencyUnavailable)
            }
            TerminalPredicateTruth::Invalid => GraphRootState::FailedInvalidResponse,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TerminalPredicateTruth {
    True,
    False,
    Unavailable,
    Invalid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct GraphPredicateState {
    pub predicate: GraphPredicateV2,
    pub latest_observation: Option<GraphPredicateObservation>,
    pub first_true_observation: Option<GraphPredicateObservation>,
    #[serde(default, with = "time::serde::rfc3339::option")]
    pub next_due_at: Option<OffsetDateTime>,
    #[serde(default)]
    pub operational_failure_count: u32,
    #[serde(default)]
    pub terminal_invalid: bool,
}

impl GraphPredicateState {
    fn new(predicate: GraphPredicateV2) -> Self {
        Self {
            predicate,
            latest_observation: None,
            first_true_observation: None,
            next_due_at: None,
            operational_failure_count: 0,
            terminal_invalid: false,
        }
    }

    fn validate(
        &self,
        accepted_at: OffsetDateTime,
        deadline_at: OffsetDateTime,
    ) -> Result<(), String> {
        for observation in [
            self.latest_observation.as_ref(),
            self.first_true_observation.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            if observation.observed_at < accepted_at || observation.observed_at > deadline_at {
                return Err("stored graph observation falls outside task window".to_owned());
            }
            if observation
                .origin
                .as_ref()
                .is_some_and(|origin| origin.trim().is_empty())
            {
                return Err("stored Nexus observation origin is empty".to_owned());
            }
            if observation.http_status.is_some() && observation.origin.is_none() {
                return Err("stored Nexus HTTP status lacks origin".to_owned());
            }
        }
        if self.terminal_invalid {
            let valid_terminal_evidence = self.latest_observation.as_ref().is_some_and(|latest| {
                latest.outcome == GraphObservationOutcome::DependencyUnavailable
                    && latest
                        .origin
                        .as_ref()
                        .is_some_and(|origin| !origin.trim().is_empty())
                    && latest.http_status.is_some()
            });
            if !valid_terminal_evidence
                || self.first_true_observation.is_some()
                || self.next_due_at.is_some()
                || self.operational_failure_count != 0
            {
                return Err(
                    "terminal-invalid graph predicate lacks matching unavailable Nexus evidence"
                        .to_owned(),
                );
            }
        }
        if self
            .first_true_observation
            .as_ref()
            .is_some_and(|observation| observation.outcome != GraphObservationOutcome::True)
        {
            return Err("sticky graph observation is not true".to_owned());
        }
        if self.latest_observation.as_ref().is_some_and(|observation| {
            observation.outcome == GraphObservationOutcome::True
                && self.first_true_observation.is_none()
        }) {
            return Err("latest true graph observation lacks sticky evidence".to_owned());
        }
        if let Some(first_true) = &self.first_true_observation {
            let Some(latest) = &self.latest_observation else {
                return Err("sticky true graph observation lacks latest evidence".to_owned());
            };
            if first_true.observed_at > latest.observed_at
                || (first_true.observed_at == latest.observed_at
                    && first_true.outcome != latest.outcome)
            {
                return Err(
                    "sticky true graph observation diverges from latest evidence".to_owned(),
                );
            }
        }
        if self
            .next_due_at
            .is_some_and(|next_due_at| next_due_at > deadline_at)
        {
            return Err("graph predicate next due time exceeds task deadline".to_owned());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct GraphPredicateObservation {
    pub outcome: GraphObservationOutcome,
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_status: Option<u16>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphObservationOutcome {
    True,
    False,
    DependencyUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status", content = "result")]
pub enum GraphRootState {
    Pending,
    Satisfied,
    Expired(GraphDeadlineResult),
    FailedInvalidResponse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphDeadlineResult {
    False,
    DependencyUnavailable,
}
