use locks_core::lock_policy::GraphOperatorV2;

use crate::application::models::{
    GraphDeadlineResult, GraphObservationOutcome, GraphRootState, GraphTaskSnapshotV1,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PredicateTruth {
    True,
    False,
    Unavailable,
    Unknown,
    Invalid,
}

pub fn reduce_graph_snapshot(
    snapshot: &mut GraphTaskSnapshotV1,
    now: time::OffsetDateTime,
) -> Result<(), String> {
    snapshot.validate()?;
    snapshot.validate_observations_not_after(now)?;
    if snapshot.root != GraphRootState::Pending {
        return Ok(());
    }

    let deadline_reached = now >= snapshot.deadline_at;
    let values: Vec<_> = snapshot
        .predicates
        .iter()
        .map(|state| {
            if state.first_true_observation.is_some() {
                PredicateTruth::True
            } else if state.terminal_invalid {
                PredicateTruth::Invalid
            } else {
                match state
                    .latest_observation
                    .as_ref()
                    .map(|observation| observation.outcome)
                {
                    Some(GraphObservationOutcome::True) => PredicateTruth::True,
                    Some(GraphObservationOutcome::False) => {
                        if deadline_reached {
                            PredicateTruth::False
                        } else {
                            PredicateTruth::Unknown
                        }
                    }
                    Some(GraphObservationOutcome::DependencyUnavailable) => {
                        if deadline_reached {
                            PredicateTruth::Unavailable
                        } else {
                            PredicateTruth::Unknown
                        }
                    }
                    None if deadline_reached => PredicateTruth::False,
                    None => PredicateTruth::Unknown,
                }
            }
        })
        .collect();

    let truth = match snapshot.operator {
        None => values[0],
        Some(GraphOperatorV2::And) => {
            if values.contains(&PredicateTruth::False) {
                PredicateTruth::False
            } else if values.contains(&PredicateTruth::Unknown) {
                PredicateTruth::Unknown
            } else if values.contains(&PredicateTruth::Invalid) {
                PredicateTruth::Invalid
            } else if values.contains(&PredicateTruth::Unavailable) {
                PredicateTruth::Unavailable
            } else {
                PredicateTruth::True
            }
        }
        Some(GraphOperatorV2::Or) => {
            if values.contains(&PredicateTruth::True) {
                PredicateTruth::True
            } else if values.contains(&PredicateTruth::Unknown) {
                PredicateTruth::Unknown
            } else if values.contains(&PredicateTruth::Invalid) {
                PredicateTruth::Invalid
            } else if values.contains(&PredicateTruth::Unavailable) && deadline_reached {
                PredicateTruth::Unavailable
            } else {
                PredicateTruth::False
            }
        }
    };

    let next_root = match truth {
        PredicateTruth::True => GraphRootState::Satisfied,
        PredicateTruth::False if deadline_reached => {
            GraphRootState::Expired(GraphDeadlineResult::False)
        }
        PredicateTruth::Unavailable if deadline_reached => {
            GraphRootState::Expired(GraphDeadlineResult::DependencyUnavailable)
        }
        PredicateTruth::Invalid => GraphRootState::FailedInvalidResponse,
        PredicateTruth::False | PredicateTruth::Unavailable | PredicateTruth::Unknown => {
            GraphRootState::Pending
        }
    };

    // Compute and validate complete next state before mutating caller-owned snapshot.
    let mut next = snapshot.clone();
    next.root = next_root;
    next.validate()?;
    *snapshot = next;
    Ok(())
}
