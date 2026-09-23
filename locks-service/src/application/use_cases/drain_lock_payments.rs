use locks_core::ids::{ContentLockPath, LockServerPubky, PubkyLockResource};
use locks_core::lock_policy::VerifierType;
use locks_core::verification::{
    CriterionVerificationResult, EntitlementLifetime, VERIFIED_PROOF_BUNDLE_VERSION,
    VerificationResult, VerifiedProofBundle,
};

use crate::application::errors::ApplicationError;
use crate::application::models::{
    AdvanceContentLockDeletionPhaseResult, ClaimedContentLockDeletionJob, ContentLockDeletionPhase,
    VerificationTaskStatus,
};
use crate::application::ports::{
    Clock, ContentLockDeletionRepository, EntitlementRepository, PaymentDrainClient,
    PaymentDrainClientError, PaymentDrainRepository, PaymentDrainStatus, PaymentDrainSummary,
    PaymentDrainTerminalTransition, PaymentRequestState, PaymentRequestStatus, PaymentState,
    same_entitlement_decision,
};

use super::execute_content_lock_deletion_phase::{
    DeletionDependencyEvidence, DeletionDependencySource, DeletionExecutionErrorClass,
    DeletionPhaseExecution, DeletionPhaseExecutionOutcome, classify_deletion_execution_error,
};

pub struct DrainLockPaymentsUseCase<'a> {
    deletions: &'a dyn ContentLockDeletionRepository,
    drains: &'a dyn PaymentDrainRepository,
    paykit: &'a dyn PaymentDrainClient,
    entitlements: &'a dyn EntitlementRepository,
    clock: &'a dyn Clock,
    verified_by: LockServerPubky,
    minimum_confirmations: u32,
}

impl<'a> DrainLockPaymentsUseCase<'a> {
    pub fn new(
        deletions: &'a dyn ContentLockDeletionRepository,
        drains: &'a dyn PaymentDrainRepository,
        paykit: &'a dyn PaymentDrainClient,
        entitlements: &'a dyn EntitlementRepository,
        clock: &'a dyn Clock,
        verified_by: LockServerPubky,
        minimum_confirmations: u32,
    ) -> Self {
        Self {
            deletions,
            drains,
            paykit,
            entitlements,
            clock,
            verified_by,
            minimum_confirmations,
        }
    }

    pub async fn execute_claimed(
        &self,
        claim: ClaimedContentLockDeletionJob,
        worker_id: &str,
    ) -> Result<bool, ApplicationError> {
        self.execute_claimed_observed(claim, worker_id, &mut DeletionDependencyEvidence::none())
            .await
    }

    pub async fn execute_claimed_with_evidence(
        &self,
        claim: ClaimedContentLockDeletionJob,
        worker_id: &str,
    ) -> DeletionPhaseExecution {
        let mut evidence = DeletionDependencyEvidence::none();
        match self
            .execute_claimed_observed(claim, worker_id, &mut evidence)
            .await
        {
            Ok(true) => DeletionPhaseExecution::new(DeletionPhaseExecutionOutcome::Progressed)
                .with_evidence(evidence),
            Ok(false) => DeletionPhaseExecution::new(DeletionPhaseExecutionOutcome::Deferred)
                .with_evidence(evidence),
            Err(error) => {
                let outcome = match classify_deletion_execution_error(&error) {
                    DeletionExecutionErrorClass::TransientDependency => {
                        DeletionPhaseExecutionOutcome::TransientDependencyFailure
                    }
                    DeletionExecutionErrorClass::Fatal => {
                        DeletionPhaseExecutionOutcome::FatalFailure
                    }
                };
                DeletionPhaseExecution::new(outcome).with_evidence(evidence)
            }
        }
    }

    async fn execute_claimed_observed(
        &self,
        claim: ClaimedContentLockDeletionJob,
        worker_id: &str,
        evidence: &mut DeletionDependencyEvidence,
    ) -> Result<bool, ApplicationError> {
        match claim.job.phase {
            ContentLockDeletionPhase::StartPaymentDrain => {
                self.start(claim, worker_id, evidence).await
            }
            ContentLockDeletionPhase::DrainPayments => self.drain(claim, worker_id, evidence).await,
            _ => Err(ApplicationError::InvalidContentLockDeletionState {
                message: "payment drain use case requires a payment drain phase".to_owned(),
            }),
        }
    }

    async fn start(
        &self,
        claim: ClaimedContentLockDeletionJob,
        worker_id: &str,
        evidence: &mut DeletionDependencyEvidence,
    ) -> Result<bool, ApplicationError> {
        let lock_resource = lock_resource(&claim);
        let summary = match self.paykit.start_payment_drain(&lock_resource).await {
            Ok(summary) => {
                observe_healthy(evidence, DeletionDependencySource::PaymentProvider);
                summary
            }
            Err(PaymentDrainClientError::Conflict) => {
                observe_healthy(evidence, DeletionDependencySource::PaymentProvider);
                match self.paykit.lookup_payment_drain(&lock_resource).await {
                    Ok(Some(summary)) => {
                        observe_healthy(evidence, DeletionDependencySource::PaymentProvider);
                        summary
                    }
                    Ok(None) => {
                        observe_healthy(evidence, DeletionDependencySource::PaymentProvider);
                        return Err(ApplicationError::Verifier {
                            message: "Paykit payment drain conflict could not be reconciled"
                                .to_owned(),
                        });
                    }
                    Err(error) => {
                        observe_unavailable(evidence, DeletionDependencySource::PaymentProvider);
                        return Err(map_client_error(error));
                    }
                }
            }
            Err(error) => {
                observe_unavailable(evidence, DeletionDependencySource::PaymentProvider);
                return Err(map_client_error(error));
            }
        };
        let _now = self.clock.now();
        match self
            .drains
            .store_payment_drain(claim.job.job_id, worker_id, claim.claim_token, &summary)
            .await
        {
            Ok(true) => observe_healthy(evidence, DeletionDependencySource::PaymentDrainRepository),
            Ok(false) => return Ok(false),
            Err(error) => {
                observe_unavailable(evidence, DeletionDependencySource::PaymentDrainRepository);
                return Err(error);
            }
        }
        match self
            .deletions
            .advance_phase(
                claim.job.job_id,
                worker_id,
                claim.claim_token,
                ContentLockDeletionPhase::DrainPayments,
            )
            .await
        {
            Ok(AdvanceContentLockDeletionPhaseResult::Advanced(_)) => {
                observe_healthy(evidence, DeletionDependencySource::RepositoryPhaseMutation);
                Ok(true)
            }
            Ok(AdvanceContentLockDeletionPhaseResult::ClaimLost) => Ok(false),
            Ok(_) => {
                observe_healthy(evidence, DeletionDependencySource::RepositoryPhaseMutation);
                Ok(false)
            }
            Err(error) => {
                observe_unavailable(evidence, DeletionDependencySource::RepositoryPhaseMutation);
                Err(error)
            }
        }
    }

    async fn drain(
        &self,
        claim: ClaimedContentLockDeletionJob,
        worker_id: &str,
        evidence: &mut DeletionDependencyEvidence,
    ) -> Result<bool, ApplicationError> {
        let lock_resource = lock_resource(&claim);
        let summary = match self.paykit.lookup_payment_drain(&lock_resource).await {
            Ok(Some(summary)) => {
                observe_healthy(evidence, DeletionDependencySource::PaymentProvider);
                summary
            }
            Ok(None) => {
                observe_healthy(evidence, DeletionDependencySource::PaymentProvider);
                return Err(ApplicationError::Verifier {
                    message: "Paykit payment drain not found".to_owned(),
                });
            }
            Err(error) => {
                observe_unavailable(evidence, DeletionDependencySource::PaymentProvider);
                return Err(map_client_error(error));
            }
        };
        let persisted = match self.drains.get_payment_drain(claim.job.job_id).await {
            Ok(Some(persisted)) => {
                observe_healthy(evidence, DeletionDependencySource::PaymentDrainRepository);
                persisted
            }
            Ok(None) => {
                observe_healthy(evidence, DeletionDependencySource::PaymentDrainRepository);
                return Err(ApplicationError::InvalidContentLockDeletionState {
                    message: "payment drain cleanup token is missing".to_owned(),
                });
            }
            Err(error) => {
                observe_unavailable(evidence, DeletionDependencySource::PaymentDrainRepository);
                return Err(error);
            }
        };
        validate_aggregate_progress(&persisted, &summary)?;
        let _now = self.clock.now();
        match self
            .drains
            .reconcile_payment_drain(claim.job.job_id, worker_id, claim.claim_token, &summary)
            .await
        {
            Ok(true) => observe_healthy(evidence, DeletionDependencySource::PaymentDrainRepository),
            Ok(false) => return Ok(false),
            Err(error) => {
                observe_unavailable(evidence, DeletionDependencySource::PaymentDrainRepository);
                return Err(error);
            }
        }

        let obligations = match self.drains.list_obligations(claim.job.job_id).await {
            Ok(obligations) => {
                observe_healthy(evidence, DeletionDependencySource::PaymentDrainRepository);
                obligations
            }
            Err(error) => {
                observe_unavailable(evidence, DeletionDependencySource::PaymentDrainRepository);
                return Err(error);
            }
        };
        for obligation in obligations {
            if matches!(
                obligation.status,
                VerificationTaskStatus::Completed
                    | VerificationTaskStatus::Expired
                    | VerificationTaskStatus::Failed
            ) {
                continue;
            }
            let status = match self
                .paykit
                .payment_request_status(&obligation.creator, &obligation.bundle_id)
                .await
            {
                Ok(Some(status)) => {
                    observe_healthy(evidence, DeletionDependencySource::PaymentProvider);
                    status
                }
                Ok(None) => {
                    observe_healthy(evidence, DeletionDependencySource::PaymentProvider);
                    return Err(ApplicationError::Verifier {
                        message: "Paykit payment request not found".to_owned(),
                    });
                }
                Err(error) => {
                    observe_unavailable(evidence, DeletionDependencySource::PaymentProvider);
                    return Err(map_client_error(error));
                }
            };
            if status.invoice_created_at != obligation.invoice_created_at
                || status.payment_deadline != obligation.payment_deadline
            {
                return Err(ApplicationError::InvalidVerificationTaskState {
                    message: "Paykit payment window changed during deletion".to_owned(),
                });
            }
            let Some(next_status) = classify_payment_task(status, self.minimum_confirmations)?
            else {
                continue;
            };
            let now = self.clock.now();
            let mut entitlement_publication_token = None;
            if next_status == VerificationTaskStatus::Completed {
                let publication_token = match self
                    .drains
                    .begin_entitlement_publication(
                        claim.job.job_id,
                        worker_id,
                        claim.claim_token,
                        &obligation.task_id,
                    )
                    .await
                {
                    Ok(Some(token)) => {
                        observe_healthy(evidence, DeletionDependencySource::PaymentDrainRepository);
                        token
                    }
                    Ok(None) => return Ok(false),
                    Err(error) => {
                        observe_unavailable(
                            evidence,
                            DeletionDependencySource::PaymentDrainRepository,
                        );
                        return Err(error);
                    }
                };
                let entitlement = VerifiedProofBundle {
                    version: VERIFIED_PROOF_BUNDLE_VERSION,
                    bundle_id: obligation.bundle_id.clone(),
                    pubky_lock_resource: obligation.lock_resource.clone(),
                    verification_result: VerificationResult {
                        criteria: vec![CriterionVerificationResult {
                            criterion_id: obligation.criterion_id.clone(),
                            satisfied: true,
                            verified_at: now,
                            verified_by: self.verified_by.clone(),
                            verifier_type: VerifierType::PaykitPayment,
                        }],
                    },
                    entitlement_lifetime: EntitlementLifetime::Unbounded,
                };
                persist_entitlement(self.entitlements, entitlement, evidence).await?;
                entitlement_publication_token = Some(publication_token);
            }
            match self
                .drains
                .persist_terminal_obligation(
                    claim.job.job_id,
                    worker_id,
                    claim.claim_token,
                    &obligation.task_id,
                    PaymentDrainTerminalTransition {
                        status: next_status,
                        entitlement_publication_token,
                    },
                )
                .await
            {
                Ok(true) => {
                    observe_healthy(evidence, DeletionDependencySource::PaymentDrainRepository)
                }
                Ok(false) => return Ok(false),
                Err(error) => {
                    observe_unavailable(evidence, DeletionDependencySource::PaymentDrainRepository);
                    return Err(error);
                }
            }
        }

        if summary.status != PaymentDrainStatus::Completed {
            return Ok(false);
        }
        match self.drains.all_obligations_terminal(claim.job.job_id).await {
            Ok(true) => observe_healthy(evidence, DeletionDependencySource::PaymentDrainRepository),
            Ok(false) => {
                observe_healthy(evidence, DeletionDependencySource::PaymentDrainRepository);
                return Ok(false);
            }
            Err(error) => {
                observe_unavailable(evidence, DeletionDependencySource::PaymentDrainRepository);
                return Err(error);
            }
        }
        match self
            .deletions
            .advance_phase(
                claim.job.job_id,
                worker_id,
                claim.claim_token,
                ContentLockDeletionPhase::DrainExistingCredentials,
            )
            .await
        {
            Ok(AdvanceContentLockDeletionPhaseResult::Advanced(_)) => {
                observe_healthy(evidence, DeletionDependencySource::RepositoryPhaseMutation);
                Ok(true)
            }
            Ok(AdvanceContentLockDeletionPhaseResult::ClaimLost) => Ok(false),
            Ok(_) => {
                observe_healthy(evidence, DeletionDependencySource::RepositoryPhaseMutation);
                Ok(false)
            }
            Err(error) => {
                observe_unavailable(evidence, DeletionDependencySource::RepositoryPhaseMutation);
                Err(error)
            }
        }
    }
}

fn lock_resource(claim: &ClaimedContentLockDeletionJob) -> PubkyLockResource {
    PubkyLockResource::new(
        claim.job.creator.clone(),
        ContentLockPath::from_lock_id(claim.job.lock_id.clone()),
    )
}

async fn persist_entitlement(
    entitlements: &dyn EntitlementRepository,
    entitlement: VerifiedProofBundle,
    evidence: &mut DeletionDependencyEvidence,
) -> Result<(), ApplicationError> {
    if let Err(error) = entitlements
        .insert_verified_proof_bundle(entitlement.clone())
        .await
    {
        let expected_duplicate = matches!(
            &error,
            ApplicationError::DuplicateRecord { record }
                if *record == "verified_proof_bundle"
        );
        if !expected_duplicate {
            observe_unavailable(evidence, DeletionDependencySource::EntitlementRepository);
        }
        let existing = match entitlements
            .get_verified_proof_bundle(
                entitlement.pubky_lock_resource.creator(),
                &entitlement.bundle_id,
            )
            .await
        {
            Ok(existing) => {
                observe_healthy(evidence, DeletionDependencySource::EntitlementRepository);
                existing
            }
            Err(read_error) => {
                observe_unavailable(evidence, DeletionDependencySource::EntitlementRepository);
                return Err(read_error);
            }
        };
        if !existing
            .as_ref()
            .is_some_and(|existing| same_entitlement_decision(existing, &entitlement))
        {
            return Err(error);
        }
    } else {
        observe_healthy(evidence, DeletionDependencySource::EntitlementRepository);
    }
    Ok(())
}

fn observe_healthy(evidence: &mut DeletionDependencyEvidence, source: DeletionDependencySource) {
    *evidence = evidence.merge(DeletionDependencyEvidence::healthy(source));
}

fn observe_unavailable(
    evidence: &mut DeletionDependencyEvidence,
    source: DeletionDependencySource,
) {
    *evidence = evidence.merge(DeletionDependencyEvidence::unavailable(source));
}

fn map_client_error(error: PaymentDrainClientError) -> ApplicationError {
    ApplicationError::Verifier {
        message: match error {
            PaymentDrainClientError::NotFound => "Paykit payment drain not found",
            PaymentDrainClientError::Conflict => "Paykit payment drain conflict",
            PaymentDrainClientError::MalformedSuccess => {
                "Paykit payment drain returned malformed success"
            }
            PaymentDrainClientError::Transport => "Paykit payment drain transport failure",
            PaymentDrainClientError::Server => "Paykit payment drain server failure",
        }
        .to_owned(),
    }
}

fn validate_aggregate_progress(
    persisted: &PaymentDrainSummary,
    current: &PaymentDrainSummary,
) -> Result<(), ApplicationError> {
    let accepted_delta = persisted.accepted_count.checked_sub(current.accepted_count);
    let terminal_delta = current.terminal_count.checked_sub(persisted.terminal_count);
    if persisted.cleanup_token != current.cleanup_token
        || persisted.cancellation_enqueued_count != current.cancellation_enqueued_count
        || accepted_delta.is_none()
        || terminal_delta.is_none()
        || accepted_delta != terminal_delta
        || (current.status == PaymentDrainStatus::Completed && current.accepted_count != 0)
        || (current.status == PaymentDrainStatus::Active && current.accepted_count == 0)
        || (persisted.status == PaymentDrainStatus::Completed
            && current.status != PaymentDrainStatus::Completed)
    {
        return Err(ApplicationError::InvalidContentLockDeletionState {
            message: "Paykit payment drain aggregate changed incompatibly for deletion job"
                .to_owned(),
        });
    }
    Ok(())
}

pub(crate) fn classify_payment_task(
    status: PaymentRequestStatus,
    minimum_confirmations: u32,
) -> Result<Option<VerificationTaskStatus>, ApplicationError> {
    if matches!(
        status.request_state,
        PaymentRequestState::RecoveryRequired
            | PaymentRequestState::InvalidConflict
            | PaymentRequestState::ProofSubmitted
            | PaymentRequestState::ActiveRecurring
    ) {
        return Err(ApplicationError::InvalidVerificationTaskState {
            message: "Paykit payment request entered an unsupported failure state".to_owned(),
        });
    }
    if matches!(
        status.request_state,
        PaymentRequestState::Rejected
            | PaymentRequestState::Canceled
            | PaymentRequestState::ProposalExpired
    ) || (status.request_state == PaymentRequestState::Accepted
        && status.payment_state == PaymentState::Expired)
    {
        return Ok(Some(VerificationTaskStatus::Expired));
    }

    if status.request_state != PaymentRequestState::Accepted || !status.amount_matched {
        return Ok(None);
    }

    if minimum_confirmations == 0
        && matches!(
            status.payment_state,
            PaymentState::Detected | PaymentState::Confirmed
        )
    {
        return Ok(Some(VerificationTaskStatus::Completed));
    }

    Ok((status.payment_state == PaymentState::Confirmed
        && status.confirmations >= minimum_confirmations)
        .then_some(VerificationTaskStatus::Completed))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use async_trait::async_trait;
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use time::macros::datetime;

    use locks_core::ids::{BundleId, ContentLockPath, CreatorPubky, PubkyLockResource};
    use locks_core::verification::{
        EntitlementLifetime, VERIFIED_PROOF_BUNDLE_VERSION, VerificationResult, VerifiedProofBundle,
    };

    use crate::application::errors::ApplicationError;
    use crate::application::models::VerificationTaskStatus;
    use crate::application::ports::{
        EntitlementRepository, PaymentDrainCleanupToken, PaymentDrainStatus, PaymentDrainSummary,
        PaymentRequestState, PaymentRequestStatus, PaymentState,
    };

    use crate::application::use_cases::execute_content_lock_deletion_phase::DeletionDependencyStatus;

    use super::{
        DeletionDependencyEvidence, DeletionDependencySource, classify_payment_task,
        persist_entitlement, validate_aggregate_progress,
    };

    #[tokio::test]
    async fn exact_duplicate_entitlement_replay_finishes_with_healthy_evidence() {
        let entitlement = entitlement();
        let repository = DuplicateEntitlementRepository(entitlement.clone());
        let mut evidence = DeletionDependencyEvidence::none();

        persist_entitlement(&repository, entitlement, &mut evidence)
            .await
            .unwrap();

        assert_eq!(
            evidence.status(DeletionDependencySource::EntitlementRepository),
            Some(DeletionDependencyStatus::Healthy)
        );
    }

    struct DuplicateEntitlementRepository(VerifiedProofBundle);

    #[async_trait]
    impl EntitlementRepository for DuplicateEntitlementRepository {
        async fn insert_verified_proof_bundle(
            &self,
            _: VerifiedProofBundle,
        ) -> Result<(), ApplicationError> {
            Err(ApplicationError::DuplicateRecord {
                record: "verified_proof_bundle",
            })
        }

        async fn get_verified_proof_bundle(
            &self,
            _: &CreatorPubky,
            _: &BundleId,
        ) -> Result<Option<VerifiedProofBundle>, ApplicationError> {
            Ok(Some(self.0.clone()))
        }

        async fn delete_verified_proof_bundle(
            &self,
            _: &CreatorPubky,
            _: &BundleId,
        ) -> Result<(), ApplicationError> {
            unreachable!()
        }
    }

    fn entitlement() -> VerifiedProofBundle {
        let creator =
            CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy")
                .unwrap();
        let path = ContentLockPath::from_str(
            "/pub/locks.app/000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG.json",
        )
        .unwrap();
        VerifiedProofBundle {
            version: VERIFIED_PROOF_BUNDLE_VERSION,
            bundle_id: BundleId::from_str("000G40R40M30E209185GR38E1W").unwrap(),
            pubky_lock_resource: PubkyLockResource::new(creator, path),
            verification_result: VerificationResult { criteria: vec![] },
            entitlement_lifetime: EntitlementLifetime::Unbounded,
        }
    }

    #[test]
    fn aggregate_progress_accepts_only_equal_monotonic_transfer() {
        let initial = summary(PaymentDrainStatus::Active, 2, 3, 1, 1);
        assert!(
            validate_aggregate_progress(&initial, &summary(PaymentDrainStatus::Active, 1, 4, 1, 1))
                .is_ok()
        );
        assert!(
            validate_aggregate_progress(
                &initial,
                &summary(PaymentDrainStatus::Completed, 0, 5, 1, 1)
            )
            .is_ok()
        );

        for invalid in [
            summary(PaymentDrainStatus::Active, 3, 2, 1, 1),
            summary(PaymentDrainStatus::Active, 1, 3, 1, 1),
            summary(PaymentDrainStatus::Active, 1, 5, 1, 1),
            summary(PaymentDrainStatus::Active, 1, 4, 2, 1),
            summary(PaymentDrainStatus::Active, 1, 4, 1, 2),
            summary(PaymentDrainStatus::Completed, 1, 4, 1, 1),
        ] {
            assert!(validate_aggregate_progress(&initial, &invalid).is_err());
        }
        let completed = summary(PaymentDrainStatus::Completed, 0, 5, 1, 1);
        assert!(
            validate_aggregate_progress(
                &completed,
                &summary(PaymentDrainStatus::Active, 0, 5, 1, 1)
            )
            .is_err()
        );
    }

    fn summary(
        status: PaymentDrainStatus,
        accepted_count: u64,
        terminal_count: u64,
        cancellation_enqueued_count: u64,
        token_byte: u8,
    ) -> PaymentDrainSummary {
        PaymentDrainSummary {
            status,
            accepted_count,
            terminal_count,
            cancellation_enqueued_count,
            cleanup_token: PaymentDrainCleanupToken::parse(
                &URL_SAFE_NO_PAD.encode([token_byte; 32]),
            )
            .unwrap(),
        }
    }

    #[test]
    fn terminal_unpaid_states_expire_without_failure() {
        for request_state in [
            PaymentRequestState::Rejected,
            PaymentRequestState::Canceled,
            PaymentRequestState::ProposalExpired,
        ] {
            assert_eq!(
                classify_payment_task(status(request_state, PaymentState::Undetected, 0, false), 6),
                Ok(Some(VerificationTaskStatus::Expired))
            );
        }
        assert_eq!(
            classify_payment_task(
                status(
                    PaymentRequestState::Accepted,
                    PaymentState::Expired,
                    0,
                    false
                ),
                6,
            ),
            Ok(Some(VerificationTaskStatus::Expired))
        );
    }

    #[test]
    fn confirmations_are_applied_only_by_locks() {
        assert_eq!(
            classify_payment_task(
                status(
                    PaymentRequestState::Accepted,
                    PaymentState::Detected,
                    0,
                    true
                ),
                0,
            ),
            Ok(Some(VerificationTaskStatus::Completed))
        );
        assert_eq!(
            classify_payment_task(
                status(
                    PaymentRequestState::Accepted,
                    PaymentState::Detected,
                    0,
                    true
                ),
                1,
            ),
            Ok(None)
        );
        assert_eq!(
            classify_payment_task(
                status(
                    PaymentRequestState::Accepted,
                    PaymentState::Confirmed,
                    5,
                    true
                ),
                6,
            ),
            Ok(None)
        );
        assert_eq!(
            classify_payment_task(
                status(
                    PaymentRequestState::Accepted,
                    PaymentState::Confirmed,
                    6,
                    true
                ),
                6,
            ),
            Ok(Some(VerificationTaskStatus::Completed))
        );
    }

    #[test]
    fn timely_matched_payment_stays_pending_after_deadline_until_confirmed() {
        let mut value = status(
            PaymentRequestState::Accepted,
            PaymentState::Detected,
            0,
            true,
        );
        value.payment_deadline = datetime!(2026-08-11 10:00:00 UTC);
        assert_eq!(classify_payment_task(value, 6), Ok(None));
    }

    #[test]
    fn classification_failure_states_fail_closed() {
        for request_state in [
            PaymentRequestState::RecoveryRequired,
            PaymentRequestState::InvalidConflict,
            PaymentRequestState::ProofSubmitted,
            PaymentRequestState::ActiveRecurring,
        ] {
            assert!(
                classify_payment_task(
                    status(request_state, PaymentState::Undetected, 0, false),
                    6,
                )
                .is_err()
            );
        }
    }

    fn status(
        request_state: PaymentRequestState,
        payment_state: PaymentState,
        confirmations: u32,
        amount_matched: bool,
    ) -> PaymentRequestStatus {
        PaymentRequestStatus {
            request_state,
            payment_state,
            invoice_created_at: datetime!(2026-08-12 10:00:00 UTC),
            payment_deadline: datetime!(2026-08-13 10:00:00 UTC),
            confirmations,
            amount_matched,
        }
    }
}
