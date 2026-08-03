use std::collections::{HashMap, HashSet};

use locks_core::lock_policy::{ContentLock, LockLogic};
use locks_core::verification::VerificationResult;

use crate::application::errors::ApplicationError;

/// Evaluates whether stored verification evidence satisfies a content lock.
///
/// Returns `Ok(false)` for valid but insufficient evidence and `Err` for
/// malformed/corrupt content locks or verification results.
pub fn evaluate_entitlement(
    content_lock: &ContentLock,
    verification_result: &VerificationResult,
) -> Result<bool, ApplicationError> {
    let criterion_ids = content_lock_criterion_ids(content_lock)?;
    let result_map = verification_result_map(verification_result, &criterion_ids)?;

    match &content_lock.lock_logic {
        LockLogic::All { .. } => Ok(criterion_ids
            .iter()
            .all(|criterion_id| result_map.get(criterion_id).copied().unwrap_or(false))),
        LockLogic::Any { .. } => Ok(criterion_ids
            .iter()
            .any(|criterion_id| result_map.get(criterion_id).copied().unwrap_or(false))),
    }
}

fn content_lock_criterion_ids(
    content_lock: &ContentLock,
) -> Result<HashSet<String>, ApplicationError> {
    if content_lock.criteria.is_empty() {
        return Err(ApplicationError::EmptyContentLockCriteria);
    }

    let mut criterion_ids = HashSet::with_capacity(content_lock.criteria.len());
    for criterion in &content_lock.criteria {
        if !criterion_ids.insert(criterion.criterion_id.clone()) {
            return Err(ApplicationError::DuplicateContentLockCriterion {
                criterion_id: criterion.criterion_id.clone(),
            });
        }
    }

    Ok(criterion_ids)
}

fn verification_result_map(
    verification_result: &VerificationResult,
    content_lock_criterion_ids: &HashSet<String>,
) -> Result<HashMap<String, bool>, ApplicationError> {
    let mut result_map = HashMap::with_capacity(verification_result.criteria.len());

    for result in &verification_result.criteria {
        if !content_lock_criterion_ids.contains(&result.criterion_id) {
            return Err(ApplicationError::UnknownVerificationResultCriterion {
                criterion_id: result.criterion_id.clone(),
            });
        }

        if result_map
            .insert(result.criterion_id.clone(), result.satisfied)
            .is_some()
        {
            return Err(ApplicationError::DuplicateVerificationResultCriterion {
                criterion_id: result.criterion_id.clone(),
            });
        }
    }

    Ok(result_map)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use serde_json::json;
    use time::macros::datetime;

    use locks_core::ids::{CreatorPubky, GuardedResourceHash, LockServerPubky};
    use locks_core::lock_policy::{
        AccessPolicy, CONTENT_LOCK_VERSION, ContentLock, Criterion, GuardedResource, LockLogic,
        LockServerConfig, VerifierType,
    };
    use locks_core::verification::{CriterionVerificationResult, VerificationResult};

    use crate::application::entitlement_evaluator::evaluate_entitlement;
    use crate::application::errors::ApplicationError;

    #[test]
    fn all_logic_returns_true_when_every_content_lock_criterion_is_satisfied() {
        let content_lock = content_lock_fixture(
            LockLogic::All {
                criteria: vec!["criterion-1".to_owned(), "criterion-2".to_owned()],
            },
            vec![criterion("criterion-1"), criterion("criterion-2")],
        );
        let verification_result = verification_result(vec![
            result("criterion-1", true),
            result("criterion-2", true),
        ]);

        assert_eq!(
            evaluate_entitlement(&content_lock, &verification_result),
            Ok(true)
        );
    }

    #[test]
    fn all_logic_returns_false_when_any_content_lock_criterion_is_missing_or_unsatisfied() {
        let content_lock = content_lock_fixture(
            LockLogic::All {
                criteria: vec!["criterion-1".to_owned(), "criterion-2".to_owned()],
            },
            vec![criterion("criterion-1"), criterion("criterion-2")],
        );

        assert_eq!(
            evaluate_entitlement(
                &content_lock,
                &verification_result(vec![result("criterion-1", true)])
            ),
            Ok(false)
        );
        assert_eq!(
            evaluate_entitlement(
                &content_lock,
                &verification_result(vec![
                    result("criterion-1", true),
                    result("criterion-2", false)
                ]),
            ),
            Ok(false)
        );
    }

    #[test]
    fn any_logic_returns_true_when_at_least_one_known_criterion_is_satisfied() {
        let content_lock = content_lock_fixture(
            LockLogic::Any {
                criteria: vec!["criterion-1".to_owned(), "criterion-2".to_owned()],
            },
            vec![criterion("criterion-1"), criterion("criterion-2")],
        );
        let verification_result = verification_result(vec![
            result("criterion-1", false),
            result("criterion-2", true),
        ]);

        assert_eq!(
            evaluate_entitlement(&content_lock, &verification_result),
            Ok(true)
        );
    }

    #[test]
    fn any_logic_returns_false_when_no_known_criterion_is_satisfied() {
        let content_lock = content_lock_fixture(
            LockLogic::Any {
                criteria: vec!["criterion-1".to_owned(), "criterion-2".to_owned()],
            },
            vec![criterion("criterion-1"), criterion("criterion-2")],
        );

        assert_eq!(
            evaluate_entitlement(
                &content_lock,
                &verification_result(vec![result("criterion-1", false)])
            ),
            Ok(false)
        );
    }

    #[test]
    fn evaluator_rejects_empty_content_lock_criteria() {
        let content_lock = content_lock_fixture(LockLogic::All { criteria: vec![] }, vec![]);

        assert_eq!(
            evaluate_entitlement(&content_lock, &verification_result(vec![])),
            Err(ApplicationError::EmptyContentLockCriteria)
        );
    }

    #[test]
    fn evaluator_rejects_duplicate_content_lock_criterion_ids() {
        let content_lock = content_lock_fixture(
            LockLogic::All {
                criteria: vec!["criterion-1".to_owned()],
            },
            vec![criterion("criterion-1"), criterion("criterion-1")],
        );

        assert_eq!(
            evaluate_entitlement(
                &content_lock,
                &verification_result(vec![result("criterion-1", true)])
            ),
            Err(ApplicationError::DuplicateContentLockCriterion {
                criterion_id: "criterion-1".to_owned(),
            })
        );
    }

    #[test]
    fn evaluator_rejects_duplicate_verification_result_criterion_ids() {
        let content_lock = content_lock_fixture(
            LockLogic::All {
                criteria: vec!["criterion-1".to_owned()],
            },
            vec![criterion("criterion-1")],
        );

        assert_eq!(
            evaluate_entitlement(
                &content_lock,
                &verification_result(vec![
                    result("criterion-1", true),
                    result("criterion-1", false)
                ]),
            ),
            Err(ApplicationError::DuplicateVerificationResultCriterion {
                criterion_id: "criterion-1".to_owned(),
            })
        );
    }

    #[test]
    fn evaluator_rejects_unknown_verification_result_criterion_ids() {
        let content_lock = content_lock_fixture(
            LockLogic::Any {
                criteria: vec!["criterion-1".to_owned()],
            },
            vec![criterion("criterion-1")],
        );

        assert_eq!(
            evaluate_entitlement(
                &content_lock,
                &verification_result(vec![result("criterion-1", true), result("unknown", true)]),
            ),
            Err(ApplicationError::UnknownVerificationResultCriterion {
                criterion_id: "unknown".to_owned(),
            })
        );
    }

    fn content_lock_fixture(lock_logic: LockLogic, criteria: Vec<Criterion>) -> ContentLock {
        ContentLock {
            version: CONTENT_LOCK_VERSION,
            creator: CreatorPubky::from_str(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            )
            .unwrap(),
            primary_resource: Some(GuardedResource {
                path: "/priv/locks.app/content/hello.txt".to_owned(),
                hash: GuardedResourceHash::from_bytes([7; 32]),
                content_type: "text/plain".to_owned(),
                size: 13,
            }),
            secondary_resources: Default::default(),
            criteria,
            lock_logic,
            access_policy: AccessPolicy {
                requested_credential_ttl_seconds: 900,
            },
            lock_server: LockServerConfig {
                override_: Some(
                    LockServerPubky::from_str(
                        "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
                    )
                    .unwrap(),
                ),
            },
            created_at: datetime!(2026-05-29 12:00:00 UTC),
        }
    }

    fn criterion(criterion_id: &str) -> Criterion {
        Criterion {
            criterion_id: criterion_id.to_owned(),
            verifier_type: VerifierType::DevStatic,
            params: json!({ "satisfied": true }),
        }
    }

    fn verification_result(criteria: Vec<CriterionVerificationResult>) -> VerificationResult {
        VerificationResult { criteria }
    }

    fn result(criterion_id: &str, satisfied: bool) -> CriterionVerificationResult {
        CriterionVerificationResult {
            criterion_id: criterion_id.to_owned(),
            satisfied,
            verified_at: datetime!(2026-05-29 12:30:00 UTC),
            verified_by: LockServerPubky::from_str(
                "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
            )
            .unwrap(),
            verifier_type: VerifierType::DevStatic,
        }
    }
}
