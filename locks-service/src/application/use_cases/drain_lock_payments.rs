use locks_core::ids::{ContentLockPath, LockServerPubky, PubkyLockResource};
use locks_core::lock_policy::VerifierType;
use locks_core::verification::{
    CriterionVerificationResult, EntitlementLifetime, VERIFIED_PROOF_BUNDLE_VERSION,
    VerificationResult, VerifiedProofBundle,
};

use crate::application::errors::ApplicationError;
use crate::application::models::{
    ClaimedContentLockDeletionJob, ContentLockDeletionPhase, VerificationTaskStatus,
};
use crate::application::ports::{
    Clock, ContentLockDeletionRepository, EntitlementRepository, PaymentDrainClient,
    PaymentDrainClientError, PaymentDrainRepository, PaymentDrainStatus, PaymentDrainSummary,
    PaymentDrainTerminalTransition, PaymentRequestState, PaymentRequestStatus, PaymentState,
    same_entitlement_decision,
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
        match claim.job.phase {
            ContentLockDeletionPhase::StartPaymentDrain => self.start(claim, worker_id).await,
            ContentLockDeletionPhase::DrainPayments => self.drain(claim, worker_id).await,
            _ => Err(ApplicationError::InvalidContentLockDeletionState {
                message: "payment drain use case requires a payment drain phase".to_owned(),
            }),
        }
    }

    async fn start(
        &self,
        claim: ClaimedContentLockDeletionJob,
        worker_id: &str,
    ) -> Result<bool, ApplicationError> {
        let lock_resource = lock_resource(&claim);
        let summary = match self.paykit.start_payment_drain(&lock_resource).await {
            Ok(summary) => summary,
            Err(PaymentDrainClientError::Conflict) => self
                .paykit
                .lookup_payment_drain(&lock_resource)
                .await
                .map_err(map_client_error)?
                .ok_or_else(|| ApplicationError::Verifier {
                    message: "Paykit payment drain conflict could not be reconciled".to_owned(),
                })?,
            Err(error) => return Err(map_client_error(error)),
        };
        let now = self.clock.now();
        if !self
            .drains
            .store_payment_drain(
                claim.job.job_id,
                worker_id,
                claim.claim_token,
                now,
                &summary,
            )
            .await?
        {
            return Ok(false);
        }
        Ok(self
            .deletions
            .advance_phase(
                claim.job.job_id,
                worker_id,
                claim.claim_token,
                now,
                ContentLockDeletionPhase::DrainPayments,
            )
            .await?
            .is_some())
    }

    async fn drain(
        &self,
        claim: ClaimedContentLockDeletionJob,
        worker_id: &str,
    ) -> Result<bool, ApplicationError> {
        let lock_resource = lock_resource(&claim);
        let summary = self
            .paykit
            .lookup_payment_drain(&lock_resource)
            .await
            .map_err(map_client_error)?
            .ok_or_else(|| ApplicationError::Verifier {
                message: "Paykit payment drain not found".to_owned(),
            })?;
        let persisted = self
            .drains
            .get_payment_drain(claim.job.job_id)
            .await?
            .ok_or_else(|| ApplicationError::InvalidContentLockDeletionState {
                message: "payment drain cleanup token is missing".to_owned(),
            })?;
        validate_aggregate_progress(&persisted, &summary)?;
        let now = self.clock.now();
        if !self
            .drains
            .reconcile_payment_drain(
                claim.job.job_id,
                worker_id,
                claim.claim_token,
                now,
                &summary,
            )
            .await?
        {
            return Ok(false);
        }

        for obligation in self.drains.list_obligations(claim.job.job_id).await? {
            if matches!(
                obligation.status,
                VerificationTaskStatus::Completed
                    | VerificationTaskStatus::Expired
                    | VerificationTaskStatus::Failed
            ) {
                continue;
            }
            let status = self
                .paykit
                .payment_request_status(&obligation.creator, &obligation.bundle_id)
                .await
                .map_err(map_client_error)?
                .ok_or_else(|| ApplicationError::Verifier {
                    message: "Paykit payment request not found".to_owned(),
                })?;
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
                let Some(publication_token) = self
                    .drains
                    .begin_entitlement_publication(
                        claim.job.job_id,
                        worker_id,
                        claim.claim_token,
                        now,
                        &obligation.task_id,
                    )
                    .await?
                else {
                    return Ok(false);
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
                persist_entitlement(self.entitlements, entitlement).await?;
                entitlement_publication_token = Some(publication_token);
            }
            if !self
                .drains
                .persist_terminal_obligation(
                    claim.job.job_id,
                    worker_id,
                    claim.claim_token,
                    now,
                    &obligation.task_id,
                    PaymentDrainTerminalTransition {
                        status: next_status,
                        entitlement_publication_token,
                    },
                )
                .await?
            {
                return Ok(false);
            }
        }

        if summary.status != PaymentDrainStatus::Completed
            || !self
                .drains
                .all_obligations_terminal(claim.job.job_id)
                .await?
        {
            return Ok(false);
        }
        Ok(self
            .deletions
            .advance_phase(
                claim.job.job_id,
                worker_id,
                claim.claim_token,
                self.clock.now(),
                ContentLockDeletionPhase::DrainExistingCredentials,
            )
            .await?
            .is_some())
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
) -> Result<(), ApplicationError> {
    if let Err(error) = entitlements
        .insert_verified_proof_bundle(entitlement.clone())
        .await
    {
        let existing = entitlements
            .get_verified_proof_bundle(
                entitlement.pubky_lock_resource.creator(),
                &entitlement.bundle_id,
            )
            .await?;
        if !existing
            .as_ref()
            .is_some_and(|existing| same_entitlement_decision(existing, &entitlement))
        {
            return Err(error);
        }
    }
    Ok(())
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
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use time::macros::datetime;

    use crate::application::models::VerificationTaskStatus;
    use crate::application::ports::{
        PaymentDrainCleanupToken, PaymentDrainStatus, PaymentDrainSummary, PaymentRequestState,
        PaymentRequestStatus, PaymentState,
    };

    use super::{classify_payment_task, validate_aggregate_progress};

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
