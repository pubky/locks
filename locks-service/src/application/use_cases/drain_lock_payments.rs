use locks_core::ids::{ContentLockPath, LockServerPubky, PubkyLockResource};
use locks_core::lock_policy::VerifierType;
use locks_core::verification::{
    CriterionVerificationResult, EntitlementLifetime, VERIFIED_PROOF_BUNDLE_VERSION,
    VerificationResult, VerifiedProofBundle,
};

use crate::application::errors::ApplicationError;
use crate::application::models::{
    AdvanceContentLockDeletionPhaseResult, ClaimedContentLockDeletionJob, ContentLockDeletionPhase,
    PaymentInvoiceState, PaymentProviderCreateRequest, PaymentRequestStateV2, PaymentRootState,
    PaymentStateV2, PaymentTaskSnapshot, VerificationTaskStatus, VerificationTerminalReason,
};
use crate::application::ports::{
    Clock, ContentLockDeletionRepository, EntitlementRepository, PaymentDrainCleanupPreparation,
    PaymentDrainClient, PaymentDrainClientError, PaymentDrainObligation,
    PaymentDrainObligationKind, PaymentDrainRepository, PaymentDrainStatus, PaymentDrainSummary,
    PaymentDrainTerminalTransition, PaymentRequestState, PaymentRequestStatus, PaymentState,
    ProviderV2DeletionReconciliation, same_entitlement_decision,
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
        let job_id = claim.job.job_id;
        let claim_token = claim.claim_token;
        let mut evidence = DeletionDependencyEvidence::none();
        match self
            .execute_claimed_observed(claim, worker_id, &mut evidence)
            .await
        {
            Ok(true) => DeletionPhaseExecution::new(DeletionPhaseExecutionOutcome::Progressed)
                .with_evidence(evidence),
            Ok(false) => DeletionPhaseExecution::new(DeletionPhaseExecutionOutcome::Deferred)
                .with_evidence(evidence),
            Err(
                ApplicationError::InvalidVerificationTaskState { .. }
                | ApplicationError::InvalidContentLockDeletionState { .. },
            ) => match self
                .deletions
                .finish(
                    job_id,
                    worker_id,
                    claim_token,
                    Some(crate::application::models::ContentLockDeletionFailureCode::StateCorrupt),
                )
                .await
            {
                Ok(Some(_)) => {
                    DeletionPhaseExecution::new(DeletionPhaseExecutionOutcome::TerminalFailed)
                        .with_evidence(evidence)
                        .with_evidence(DeletionDependencyEvidence::healthy(
                            DeletionDependencySource::RepositoryTerminalMutation,
                        ))
                }
                Ok(None) => DeletionPhaseExecution::new(DeletionPhaseExecutionOutcome::ClaimLost)
                    .with_evidence(evidence),
                Err(finish_error) => {
                    let outcome = match classify_deletion_execution_error(&finish_error) {
                        DeletionExecutionErrorClass::TransientDependency => {
                            DeletionPhaseExecutionOutcome::TransientDependencyFailure
                        }
                        DeletionExecutionErrorClass::Fatal => {
                            DeletionPhaseExecutionOutcome::FatalFailure
                        }
                    };
                    DeletionPhaseExecution::new(outcome)
                        .with_evidence(evidence)
                        .with_evidence(DeletionDependencyEvidence::unavailable(
                            DeletionDependencySource::RepositoryTerminalMutation,
                        ))
                }
            },
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

    pub async fn cleanup_claimed_with_evidence(
        &self,
        claim: ClaimedContentLockDeletionJob,
        worker_id: &str,
    ) -> DeletionPhaseExecution {
        if claim.job.phase != ContentLockDeletionPhase::PurgeOperationalState {
            return DeletionPhaseExecution::new(DeletionPhaseExecutionOutcome::FatalFailure);
        }
        let authority = match self.paykit.authority() {
            Some(authority) => authority,
            None => {
                return DeletionPhaseExecution::new(DeletionPhaseExecutionOutcome::FatalFailure);
            }
        };
        let lock_resource = lock_resource(&claim);
        let preparation = match self
            .drains
            .prepare_payment_drain_cleanup(
                claim.job.job_id,
                worker_id,
                claim.claim_token,
                &lock_resource,
                &authority,
            )
            .await
        {
            Ok(preparation) => preparation,
            Err(error) => {
                return deletion_error_execution(
                    &error,
                    DeletionDependencySource::PaymentDrainRepository,
                );
            }
        };
        let mut evidence =
            DeletionDependencyEvidence::healthy(DeletionDependencySource::PaymentDrainRepository);
        match preparation {
            PaymentDrainCleanupPreparation::ClaimLost => {
                return DeletionPhaseExecution::new(DeletionPhaseExecutionOutcome::ClaimLost);
            }
            PaymentDrainCleanupPreparation::NotRequired
            | PaymentDrainCleanupPreparation::Completed => {}
            PaymentDrainCleanupPreparation::Pending(cleanup_token) => {
                if let Err(error) = self
                    .paykit
                    .cleanup_payment_drain(&lock_resource, &cleanup_token)
                    .await
                {
                    return DeletionPhaseExecution::new(match error {
                        PaymentDrainClientError::Transport | PaymentDrainClientError::Server => {
                            DeletionPhaseExecutionOutcome::TransientDependencyFailure
                        }
                        _ => DeletionPhaseExecutionOutcome::FatalFailure,
                    })
                    .with_evidence(DeletionDependencyEvidence::unavailable(
                        DeletionDependencySource::PaymentProvider,
                    ));
                }
                evidence = evidence.merge(DeletionDependencyEvidence::healthy(
                    DeletionDependencySource::PaymentProvider,
                ));
                match self
                    .drains
                    .persist_payment_drain_cleanup(
                        claim.job.job_id,
                        worker_id,
                        claim.claim_token,
                        &cleanup_token,
                    )
                    .await
                {
                    Ok(true) => {}
                    Ok(false) => {
                        return DeletionPhaseExecution::new(
                            DeletionPhaseExecutionOutcome::ClaimLost,
                        )
                        .with_evidence(evidence);
                    }
                    Err(error) => {
                        return deletion_error_execution(
                            &error,
                            DeletionDependencySource::PaymentDrainRepository,
                        )
                        .with_evidence(evidence);
                    }
                }
            }
        }
        match self
            .deletions
            .finish(claim.job.job_id, worker_id, claim.claim_token, None)
            .await
        {
            Ok(Some(_)) => DeletionPhaseExecution::new(DeletionPhaseExecutionOutcome::Progressed)
                .with_evidence(evidence)
                .with_evidence(DeletionDependencyEvidence::healthy(
                    DeletionDependencySource::RepositoryTerminalMutation,
                )),
            Ok(None) => DeletionPhaseExecution::new(DeletionPhaseExecutionOutcome::ClaimLost)
                .with_evidence(evidence),
            Err(error) => deletion_error_execution(
                &error,
                DeletionDependencySource::RepositoryTerminalMutation,
            )
            .with_evidence(evidence),
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
        if !self
            .deletions
            .expire_unresolved_non_paykit_tasks(claim.job.job_id, worker_id, claim.claim_token)
            .await?
        {
            return Ok(false);
        }
        observe_healthy(evidence, DeletionDependencySource::PaymentDrainRepository);
        let has_paykit_snapshots = self
            .deletions
            .has_paykit_snapshots(claim.job.job_id)
            .await?;
        observe_healthy(evidence, DeletionDependencySource::PaymentDrainRepository);
        if !has_paykit_snapshots {
            return self
                .advance_to(
                    &claim,
                    worker_id,
                    ContentLockDeletionPhase::DrainPayments,
                    evidence,
                )
                .await;
        }
        let lock_resource = lock_resource(&claim);
        let obligations = self.drains.list_obligations(claim.job.job_id).await?;
        let has_provider_v2 = obligations.iter().any(|obligation| {
            matches!(
                obligation.kind,
                PaymentDrainObligationKind::ProviderV2 { .. }
            )
        });
        if has_provider_v2 {
            let authority = self.paykit.authority().ok_or_else(|| {
                ApplicationError::InvalidContentLockDeletionState {
                    message: "provider-v2 deletion requires a normalized Paykit authority"
                        .to_owned(),
                }
            })?;
            if !self
                .drains
                .prepare_provider_v2_fence(
                    claim.job.job_id,
                    worker_id,
                    claim.claim_token,
                    &lock_resource,
                    &authority,
                )
                .await?
            {
                return Ok(false);
            }
            observe_healthy(evidence, DeletionDependencySource::PaymentDrainRepository);
        }
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
        if has_provider_v2 {
            let authority = self.paykit.authority().ok_or_else(|| {
                ApplicationError::InvalidContentLockDeletionState {
                    message: "provider-v2 deletion requires a normalized Paykit authority"
                        .to_owned(),
                }
            })?;
            if !self
                .drains
                .store_provider_v2_fence(
                    claim.job.job_id,
                    worker_id,
                    claim.claim_token,
                    &lock_resource,
                    &authority,
                )
                .await?
            {
                return Ok(false);
            }
        }
        self.advance_to(
            &claim,
            worker_id,
            ContentLockDeletionPhase::DrainPayments,
            evidence,
        )
        .await
    }

    async fn drain(
        &self,
        claim: ClaimedContentLockDeletionJob,
        worker_id: &str,
        evidence: &mut DeletionDependencyEvidence,
    ) -> Result<bool, ApplicationError> {
        if !self
            .deletions
            .expire_unresolved_non_paykit_tasks(claim.job.job_id, worker_id, claim.claim_token)
            .await?
        {
            return Ok(false);
        }
        observe_healthy(evidence, DeletionDependencySource::PaymentDrainRepository);
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
        let has_provider_v2 = obligations.iter().any(|obligation| {
            matches!(
                obligation.kind,
                PaymentDrainObligationKind::ProviderV2 { .. }
            )
        });
        if has_provider_v2 {
            let authority = self.paykit.authority().ok_or_else(|| {
                ApplicationError::InvalidContentLockDeletionState {
                    message: "provider-v2 deletion requires the frozen Paykit authority".to_owned(),
                }
            })?;
            let lock_resource = lock_resource(&claim);
            if !self
                .drains
                .store_provider_v2_fence(
                    claim.job.job_id,
                    worker_id,
                    claim.claim_token,
                    &lock_resource,
                    &authority,
                )
                .await?
            {
                return Err(ApplicationError::InvalidContentLockDeletionState {
                    message: "provider-v2 Paykit authority or drain fence changed after restart"
                        .to_owned(),
                });
            }
        }
        if obligations.is_empty() {
            let persisted = match self.drains.get_payment_drain(claim.job.job_id).await {
                Ok(Some(persisted)) => {
                    observe_healthy(evidence, DeletionDependencySource::PaymentDrainRepository);
                    persisted
                }
                Ok(None) => {
                    observe_healthy(evidence, DeletionDependencySource::PaymentDrainRepository);
                    return self
                        .advance_completed_drain(claim, worker_id, evidence)
                        .await;
                }
                Err(error) => {
                    observe_unavailable(evidence, DeletionDependencySource::PaymentDrainRepository);
                    return Err(error);
                }
            };
            if persisted.status == PaymentDrainStatus::Completed {
                return self
                    .advance_completed_drain(claim, worker_id, evidence)
                    .await;
            }

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
            validate_aggregate_progress(&persisted, &summary)?;
            match self
                .drains
                .reconcile_payment_drain(claim.job.job_id, worker_id, claim.claim_token, &summary)
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
            if summary.status != PaymentDrainStatus::Completed {
                return Ok(false);
            }
            return self
                .advance_completed_drain(claim, worker_id, evidence)
                .await;
        }
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
        let mut replayed_publication = false;
        for obligation in obligations.iter().filter(|obligation| {
            obligation.status == VerificationTaskStatus::PublishingEntitlement
                && matches!(obligation.kind, PaymentDrainObligationKind::PaykitV1 { .. })
        }) {
            replayed_publication = true;
            let entitlement = obligation.entitlement_to_publish.clone().ok_or_else(|| {
                ApplicationError::InvalidVerificationTaskState {
                    message: "publishing payment task is missing entitlement payload".to_owned(),
                }
            })?;
            let publication_token = match self
                .drains
                .begin_entitlement_publication(
                    claim.job.job_id,
                    worker_id,
                    claim.claim_token,
                    &obligation.task_id,
                    &entitlement,
                )
                .await
            {
                Ok(Some(token)) => {
                    observe_healthy(evidence, DeletionDependencySource::PaymentDrainRepository);
                    token
                }
                Ok(None) => return Ok(false),
                Err(error) => {
                    observe_unavailable(evidence, DeletionDependencySource::PaymentDrainRepository);
                    return Err(error);
                }
            };
            persist_entitlement(self.entitlements, entitlement, evidence).await?;
            match self
                .drains
                .persist_terminal_obligation(
                    claim.job.job_id,
                    worker_id,
                    claim.claim_token,
                    &obligation.task_id,
                    PaymentDrainTerminalTransition {
                        status: VerificationTaskStatus::Completed,
                        entitlement_publication_token: Some(publication_token),
                        terminal_reason: None,
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
        if persisted.status == PaymentDrainStatus::Completed
            && self
                .all_obligations_terminal(claim.job.job_id, evidence)
                .await?
        {
            return self
                .advance_completed_drain(claim, worker_id, evidence)
                .await;
        }
        if replayed_publication {
            return Ok(false);
        }

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

        for obligation in obligations {
            let (criterion_id, invoice_created_at, payment_deadline) = match &obligation.kind {
                PaymentDrainObligationKind::ProviderV2 {
                    reconciliation_terminal,
                    ..
                } => {
                    if *reconciliation_terminal {
                        continue;
                    }
                    if summary.status != PaymentDrainStatus::Completed {
                        continue;
                    }
                    self.reconcile_provider_v2(&claim, worker_id, &obligation, evidence)
                        .await?;
                    continue;
                }
                PaymentDrainObligationKind::PaykitV1 {
                    criterion_id,
                    invoice_created_at,
                    payment_deadline,
                } => (criterion_id, *invoice_created_at, *payment_deadline),
            };
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
            if status.invoice_created_at != invoice_created_at
                || status.payment_deadline != payment_deadline
            {
                return Err(ApplicationError::InvalidVerificationTaskState {
                    message: "Paykit payment window changed during deletion".to_owned(),
                });
            }
            let Some((next_status, terminal_reason)) =
                classify_payment_task(status, self.minimum_confirmations)?
            else {
                continue;
            };
            let now = self.clock.now();
            let mut entitlement_publication_token = None;
            if next_status == VerificationTaskStatus::Completed {
                let entitlement =
                    obligation
                        .entitlement_to_publish
                        .unwrap_or_else(|| VerifiedProofBundle {
                            version: VERIFIED_PROOF_BUNDLE_VERSION,
                            bundle_id: obligation.bundle_id.clone(),
                            pubky_lock_resource: obligation.lock_resource.clone(),
                            verification_result: VerificationResult {
                                criteria: vec![CriterionVerificationResult {
                                    criterion_id: criterion_id.clone(),
                                    satisfied: true,
                                    verified_at: now,
                                    verified_by: self.verified_by.clone(),
                                    verifier_type: VerifierType::PaykitPayment,
                                }],
                            },
                            entitlement_lifetime: EntitlementLifetime::Unbounded,
                        });
                let publication_token = match self
                    .drains
                    .begin_entitlement_publication(
                        claim.job.job_id,
                        worker_id,
                        claim.claim_token,
                        &obligation.task_id,
                        &entitlement,
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
                        terminal_reason,
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
        self.finish_completed_drain(claim, worker_id, evidence)
            .await
    }

    async fn reconcile_provider_v2(
        &self,
        claim: &ClaimedContentLockDeletionJob,
        worker_id: &str,
        obligation: &PaymentDrainObligation,
        evidence: &mut DeletionDependencyEvidence,
    ) -> Result<bool, ApplicationError> {
        let PaymentDrainObligationKind::ProviderV2 { snapshot, .. } = &obligation.kind else {
            unreachable!("provider-v2 reconciliation requires a provider-v2 obligation");
        };
        let frozen = snapshot.as_v1();
        let request = PaymentProviderCreateRequest {
            bundle_id: frozen.bundle_id.clone(),
            lock_resource: frozen.pubky_lock_resource.clone(),
            reader: frozen.reader.clone(),
            payment: frozen.payment.clone(),
            deadline: frozen.deadline_at,
        };
        let reconciliation = match self.paykit.replay_provider_v2_invoice(&request).await {
            Ok(response) => {
                observe_healthy(evidence, DeletionDependencySource::PaymentProvider);
                let mut expected = frozen.clone();
                match expected.invoice {
                    PaymentInvoiceState::Pending => expected
                        .apply_create_response(
                            response.invoice_created_at,
                            response.proposal_expires_at,
                            response.deadline,
                        )
                        .map_err(
                            |message| ApplicationError::InvalidContentLockDeletionState { message },
                        )?,
                    PaymentInvoiceState::Created {
                        invoice_created_at,
                        proposal_expires_at,
                    } if invoice_created_at == response.invoice_created_at
                        && proposal_expires_at == response.proposal_expires_at
                        && frozen.deadline_at == response.deadline => {}
                    PaymentInvoiceState::Created { .. } => {
                        return Err(ApplicationError::InvalidContentLockDeletionState {
                            message: "provider-v2 replay changed immutable create timestamps"
                                .to_owned(),
                        });
                    }
                }
                if !self
                    .drains
                    .persist_provider_v2_binding(
                        claim.job.job_id,
                        worker_id,
                        claim.claim_token,
                        &obligation.task_id,
                        false,
                        Some(response),
                    )
                    .await?
                {
                    return Err(ApplicationError::InvalidContentLockDeletionState {
                        message: "provider-v2 binding checkpoint lost its frozen task fence"
                            .to_owned(),
                    });
                }
                observe_healthy(evidence, DeletionDependencySource::PaymentDrainRepository);
                let status = self
                    .paykit
                    .provider_v2_invoice_status(&obligation.creator, &obligation.bundle_id)
                    .await
                    .map_err(|error| {
                        observe_unavailable(evidence, DeletionDependencySource::PaymentProvider);
                        map_client_error(error)
                    })?
                    .ok_or_else(|| ApplicationError::InvalidContentLockDeletionState {
                        message: "provider-v2 exact replay was followed by invoice absence"
                            .to_owned(),
                    })?;
                status.validate_against(&expected).map_err(|message| {
                    ApplicationError::InvalidContentLockDeletionState { message }
                })?;
                if !self
                    .drains
                    .persist_provider_v2_status(
                        claim.job.job_id,
                        worker_id,
                        claim.claim_token,
                        &obligation.task_id,
                        Some(&status),
                    )
                    .await?
                {
                    return Err(ApplicationError::InvalidContentLockDeletionState {
                        message: "provider-v2 status checkpoint lost its frozen task fence"
                            .to_owned(),
                    });
                }
                if !provider_v2_status_is_terminal(&status)? {
                    return Ok(false);
                }
                expected.apply_status(status.clone()).map_err(|message| {
                    ApplicationError::InvalidContentLockDeletionState { message }
                })?;
                expected.root =
                    PaymentRootState::Expired(VerificationTerminalReason::ContentLockDeleted);
                ProviderV2DeletionReconciliation {
                    create_conflict: false,
                    create_response: Some(response),
                    status: Some(status),
                    terminal_snapshot: PaymentTaskSnapshot::V1(expected),
                }
            }
            Err(PaymentDrainClientError::Conflict) => {
                observe_healthy(evidence, DeletionDependencySource::PaymentProvider);
                if !self
                    .drains
                    .persist_provider_v2_binding(
                        claim.job.job_id,
                        worker_id,
                        claim.claim_token,
                        &obligation.task_id,
                        true,
                        None,
                    )
                    .await?
                {
                    return Err(ApplicationError::InvalidContentLockDeletionState {
                        message: "provider-v2 conflict checkpoint lost its frozen task fence"
                            .to_owned(),
                    });
                }
                match self
                    .paykit
                    .provider_v2_invoice_status(&obligation.creator, &obligation.bundle_id)
                    .await
                {
                    Ok(None) => {
                        if !self
                            .drains
                            .persist_provider_v2_status(
                                claim.job.job_id,
                                worker_id,
                                claim.claim_token,
                                &obligation.task_id,
                                None,
                            )
                            .await?
                        {
                            return Err(ApplicationError::InvalidContentLockDeletionState {
                                message:
                                    "provider-v2 absence checkpoint lost its frozen task fence"
                                        .to_owned(),
                            });
                        }
                        let mut terminal_snapshot = (**snapshot).clone();
                        terminal_snapshot.as_v1_mut().root = PaymentRootState::Expired(
                            VerificationTerminalReason::ContentLockDeleted,
                        );
                        ProviderV2DeletionReconciliation {
                            create_conflict: true,
                            create_response: None,
                            status: None,
                            terminal_snapshot,
                        }
                    }
                    Ok(Some(status)) => {
                        return Err(ApplicationError::InvalidContentLockDeletionState {
                            message: format!(
                                "provider-v2 replay conflict resolved to an existing {:?} invoice",
                                status.request_state
                            ),
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
        if !self
            .drains
            .persist_provider_v2_terminal(
                claim.job.job_id,
                worker_id,
                claim.claim_token,
                &obligation.task_id,
                &reconciliation,
            )
            .await?
        {
            return Err(ApplicationError::InvalidContentLockDeletionState {
                message: "provider-v2 reconciliation lost its frozen task fence".to_owned(),
            });
        }
        observe_healthy(evidence, DeletionDependencySource::PaymentDrainRepository);
        Ok(true)
    }

    async fn finish_completed_drain(
        &self,
        claim: ClaimedContentLockDeletionJob,
        worker_id: &str,
        evidence: &mut DeletionDependencyEvidence,
    ) -> Result<bool, ApplicationError> {
        if !self
            .all_obligations_terminal(claim.job.job_id, evidence)
            .await?
        {
            return Ok(false);
        }
        self.advance_completed_drain(claim, worker_id, evidence)
            .await
    }

    async fn all_obligations_terminal(
        &self,
        job_id: uuid::Uuid,
        evidence: &mut DeletionDependencyEvidence,
    ) -> Result<bool, ApplicationError> {
        match self.drains.all_obligations_terminal(job_id).await {
            Ok(terminal) => {
                observe_healthy(evidence, DeletionDependencySource::PaymentDrainRepository);
                Ok(terminal)
            }
            Err(error) => {
                observe_unavailable(evidence, DeletionDependencySource::PaymentDrainRepository);
                Err(error)
            }
        }
    }

    async fn advance_completed_drain(
        &self,
        claim: ClaimedContentLockDeletionJob,
        worker_id: &str,
        evidence: &mut DeletionDependencyEvidence,
    ) -> Result<bool, ApplicationError> {
        self.advance_to(
            &claim,
            worker_id,
            ContentLockDeletionPhase::DrainExistingCredentials,
            evidence,
        )
        .await
    }

    async fn advance_to(
        &self,
        claim: &ClaimedContentLockDeletionJob,
        worker_id: &str,
        next_phase: ContentLockDeletionPhase,
        evidence: &mut DeletionDependencyEvidence,
    ) -> Result<bool, ApplicationError> {
        match self
            .deletions
            .advance_phase(claim.job.job_id, worker_id, claim.claim_token, next_phase)
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
    let write_error = entitlements
        .insert_verified_proof_bundle(entitlement.clone())
        .await
        .err();
    if write_error.as_ref().is_some_and(|error| {
        let expected_duplicate = matches!(
            error,
            ApplicationError::DuplicateRecord { record }
                if *record == "verified_proof_bundle"
        );
        !expected_duplicate
    }) {
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
        return Err(write_error.unwrap_or_else(|| ApplicationError::Storage {
            message: "published entitlement was not equivalent on read-back".to_owned(),
        }));
    }
    Ok(())
}

fn observe_healthy(evidence: &mut DeletionDependencyEvidence, source: DeletionDependencySource) {
    *evidence = evidence.merge(DeletionDependencyEvidence::healthy(source));
}

fn deletion_error_execution(
    error: &ApplicationError,
    source: DeletionDependencySource,
) -> DeletionPhaseExecution {
    let outcome = match classify_deletion_execution_error(error) {
        DeletionExecutionErrorClass::TransientDependency => {
            DeletionPhaseExecutionOutcome::TransientDependencyFailure
        }
        DeletionExecutionErrorClass::Fatal => DeletionPhaseExecutionOutcome::FatalFailure,
    };
    DeletionPhaseExecution::new(outcome)
        .with_evidence(DeletionDependencyEvidence::unavailable(source))
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

fn provider_v2_status_is_terminal(
    status: &crate::application::models::PaymentProviderStatus,
) -> Result<bool, ApplicationError> {
    if matches!(
        status.request_state,
        PaymentRequestStateV2::InvalidConflict | PaymentRequestStateV2::RecoveryRequired
    ) {
        return Err(ApplicationError::InvalidContentLockDeletionState {
            message: "provider-v2 invoice entered a conflicting recovery state during deletion"
                .to_owned(),
        });
    }
    Ok(matches!(
        status.request_state,
        PaymentRequestStateV2::ProposalExpired
            | PaymentRequestStateV2::Rejected
            | PaymentRequestStateV2::Canceled
    ) || status.payment_state == PaymentStateV2::Expired)
}

pub(crate) fn classify_payment_task(
    status: PaymentRequestStatus,
    minimum_confirmations: u32,
) -> Result<Option<(VerificationTaskStatus, Option<VerificationTerminalReason>)>, ApplicationError>
{
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
    let terminal_reason = match (status.request_state, status.payment_state) {
        (PaymentRequestState::Rejected, _) => {
            Some(VerificationTerminalReason::PaymentRequestRejected)
        }
        (PaymentRequestState::Canceled, _) => {
            Some(VerificationTerminalReason::PaymentRequestCanceled)
        }
        (PaymentRequestState::ProposalExpired, _) => {
            Some(VerificationTerminalReason::ProposalExpired)
        }
        (PaymentRequestState::Accepted, PaymentState::Expired) => {
            Some(VerificationTerminalReason::PaymentDeadlineExpired)
        }
        _ => None,
    };
    if let Some(reason) = terminal_reason {
        return Ok(Some((VerificationTaskStatus::Expired, Some(reason))));
    }

    if status.request_state != PaymentRequestState::Accepted {
        return Ok(None);
    }
    Ok(crate::application::ports::payment_qualifies(
        status.payment_state,
        status.bitcoin,
        status.usdt_arbitrum,
        minimum_confirmations,
    )
    .then_some((VerificationTaskStatus::Completed, None)))
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
    use crate::application::models::{
        PaymentAssetV2, PaymentProviderStatus, PaymentRequestStateV2, PaymentStateV2,
        VerificationTaskStatus, VerificationTerminalReason,
    };
    use crate::application::ports::{
        EntitlementRepository, PaymentDrainCleanupToken, PaymentDrainStatus, PaymentDrainSummary,
        PaymentRequestState, PaymentRequestStatus, PaymentState,
    };

    use crate::application::use_cases::execute_content_lock_deletion_phase::DeletionDependencyStatus;

    use super::{
        DeletionDependencyEvidence, DeletionDependencySource, classify_payment_task,
        persist_entitlement, provider_v2_status_is_terminal, validate_aggregate_progress,
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
            "/pub/app.locks/000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG.json",
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
        for (request_state, reason) in [
            (
                PaymentRequestState::Rejected,
                VerificationTerminalReason::PaymentRequestRejected,
            ),
            (
                PaymentRequestState::Canceled,
                VerificationTerminalReason::PaymentRequestCanceled,
            ),
            (
                PaymentRequestState::ProposalExpired,
                VerificationTerminalReason::ProposalExpired,
            ),
        ] {
            assert_eq!(
                classify_payment_task(status(request_state, PaymentState::Undetected, 0, false), 6),
                Ok(Some((VerificationTaskStatus::Expired, Some(reason))))
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
            Ok(Some((
                VerificationTaskStatus::Expired,
                Some(VerificationTerminalReason::PaymentDeadlineExpired)
            )))
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
            Ok(Some((VerificationTaskStatus::Completed, None)))
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
            Ok(Some((VerificationTaskStatus::Completed, None)))
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

    #[test]
    fn provider_v2_reconciliation_waits_for_closed_provider_lifecycle() {
        for (request_state, payment_state, terminal) in [
            (
                PaymentRequestStateV2::Proposed,
                PaymentStateV2::Undetected,
                false,
            ),
            (
                PaymentRequestStateV2::Accepted,
                PaymentStateV2::Detected,
                false,
            ),
            (
                PaymentRequestStateV2::Rejected,
                PaymentStateV2::Undetected,
                true,
            ),
            (
                PaymentRequestStateV2::Canceled,
                PaymentStateV2::Undetected,
                true,
            ),
            (
                PaymentRequestStateV2::ProposalExpired,
                PaymentStateV2::Undetected,
                true,
            ),
            (
                PaymentRequestStateV2::Accepted,
                PaymentStateV2::Expired,
                true,
            ),
        ] {
            let value = PaymentProviderStatus {
                creator: CreatorPubky::from_str(
                    "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
                )
                .unwrap(),
                bundle_id: BundleId::from_str("000G40R40M30E209185GR38E1W").unwrap(),
                request_state,
                payment_state,
                invoice_created_at: datetime!(2026-08-12 10:00:00 UTC),
                proposal_expires_at: datetime!(2026-08-12 11:00:00 UTC),
                deadline: datetime!(2026-08-13 10:00:00 UTC),
                asset: PaymentAssetV2::Btc,
                minimum_confirmations: 0,
                current_sample: None,
                first_threshold_satisfied: None,
            };
            assert_eq!(provider_v2_status_is_terminal(&value), Ok(terminal));
        }
    }

    #[test]
    fn provider_v2_reconciliation_rejects_conflict_and_recovery_even_when_payment_expired() {
        for request_state in [
            PaymentRequestStateV2::InvalidConflict,
            PaymentRequestStateV2::RecoveryRequired,
        ] {
            let value = PaymentProviderStatus {
                creator: CreatorPubky::from_str(
                    "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
                )
                .unwrap(),
                bundle_id: BundleId::from_str("000G40R40M30E209185GR38E1W").unwrap(),
                request_state,
                payment_state: PaymentStateV2::Expired,
                invoice_created_at: datetime!(2026-08-12 10:00:00 UTC),
                proposal_expires_at: datetime!(2026-08-12 11:00:00 UTC),
                deadline: datetime!(2026-08-13 10:00:00 UTC),
                asset: PaymentAssetV2::Btc,
                minimum_confirmations: 0,
                current_sample: None,
                first_threshold_satisfied: None,
            };
            assert!(provider_v2_status_is_terminal(&value).is_err());
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
            bitcoin: Some(crate::application::ports::PaymentObservation {
                confirmations,
                amount_matched,
                paid_on_time: true,
            }),
            usdt_arbitrum: None,
        }
    }
}
