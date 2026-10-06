mod access;
mod creator_authority;
mod frontend_session;
mod guarded_resource;
mod verification;

pub use access::*;
pub use creator_authority::*;
pub use frontend_session::*;
pub use guarded_resource::*;
pub use verification::*;

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use serde_json::json;
    use time::macros::datetime;

    use locks_core::ids::{BundleId, CreatorPubky, LockServerPubky, PubkyLockResource, TaskId};
    use locks_core::lock_policy::VerifierType;
    use locks_core::verification::{
        CriterionVerificationResult, EntitlementLifetime, Proof, SUBMITTED_PROOF_BUNDLE_VERSION,
        SubmittedProofBundle, VERIFIED_PROOF_BUNDLE_VERSION, VerificationResult,
        VerifiedProofBundle,
    };

    use super::{
        AccessCredential, AccessCredentialLookupKey, AccessCredentialPolicy,
        CreatorAuthorityAuthKind, CreatorAuthorityRecord, CreatorAuthoritySecret,
        CreatorConnectAuthorizationUrl, CreatorConnectFlowId,
        DEFAULT_ACCESS_CREDENTIAL_TTL_SECONDS, FrontendSessionCode, FrontendSessionCodeRecord,
        FrontendSessionRecord, FrontendSessionToken, INVOICE_ADMISSION_INTENT_VERSION,
        InvoiceAdmissionIntentV1, InvoiceAdmissionPhase, PendingCreatorConnectFlowRecord,
        VerificationTaskRecord, VerificationTaskStatus, VerificationTerminalReason,
    };
    use crate::application::errors::ApplicationError;

    const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";
    const LOCK_ID: &str = "000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG";
    const TASK_ID: &str = "550e8400-e29b-41d4-a716-446655440000";

    #[test]
    fn invoice_admission_intent_v1_has_closed_canonical_json_shape() {
        let intent = InvoiceAdmissionIntentV1 {
            version: INVOICE_ADMISSION_INTENT_VERSION,
            creator: CreatorPubky::from_str(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            )
            .unwrap(),
            bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
            lock_resource: PubkyLockResource::from_str(&format!(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/app.locks/{LOCK_ID}.json"
            ))
            .unwrap(),
            reader: CreatorPubky::from_str(
                "pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky",
            )
            .unwrap(),
        };

        assert_eq!(
            serde_json::to_value(&intent).unwrap(),
            json!({
                "version": 1,
                "creator": "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
                "bundle_id": BUNDLE_ID,
                "lock_resource": format!(
                    "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/app.locks/{LOCK_ID}.json"
                ),
                "reader": "pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky"
            })
        );
        assert_eq!(
            serde_json::from_value::<InvoiceAdmissionIntentV1>(
                serde_json::to_value(&intent).unwrap()
            )
            .unwrap(),
            intent
        );
        assert!(serde_json::from_value::<InvoiceAdmissionIntentV1>(json!({
            "version": 2,
            "creator": "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            "bundle_id": BUNDLE_ID,
            "lock_resource": format!(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/app.locks/{LOCK_ID}.json"
            ),
            "reader": "pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky"
        })).is_err());
        assert_eq!(
            InvoiceAdmissionPhase::InvoicePending.as_str(),
            "invoice_pending"
        );
        assert_eq!(InvoiceAdmissionPhase::Ready.as_str(), "ready");
        assert_eq!(InvoiceAdmissionPhase::Failed.as_str(), "failed");
    }

    #[test]
    fn access_credential_debug_output_redacts_bearer_value() {
        let credential = AccessCredential::new("secret-bearer-value");

        let debug = format!("{credential:?}");

        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("secret-bearer-value"));
    }

    #[test]
    fn access_credential_lookup_key_is_blake3_of_raw_credential_bytes() {
        let credential = AccessCredential::new("secret-bearer-value");

        let lookup_key = AccessCredentialLookupKey::derive(&credential);

        assert_eq!(
            lookup_key.as_bytes(),
            blake3::hash("secret-bearer-value".as_bytes()).as_bytes()
        );
        assert_eq!(lookup_key, AccessCredentialLookupKey::derive(&credential));
        assert_ne!(
            lookup_key,
            AccessCredentialLookupKey::derive(&AccessCredential::new("different-bearer-value"))
        );
    }

    #[test]
    fn access_credential_lookup_key_debug_output_is_redacted() {
        let credential = AccessCredential::new("secret-bearer-value");
        let lookup_key = AccessCredentialLookupKey::derive(&credential);

        let debug = format!("{lookup_key:?}");

        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("secret-bearer-value"));
    }

    #[test]
    fn creator_authority_secret_debug_redacts_secret_value() {
        let secret = CreatorAuthoritySecret::new("legacy-cookie-session-secret");

        let debug = format!("{secret:?}");

        assert!(debug.contains("CreatorAuthoritySecret"));
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("legacy-cookie-session-secret"));
    }

    #[test]
    fn creator_authority_record_debug_redacts_secret_material() {
        let record = CreatorAuthorityRecord {
            creator: CreatorPubky::from_str(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            )
            .unwrap(),
            auth_kind: CreatorAuthorityAuthKind::LegacyCookie,
            granted_scopes: vec![
                "/pub/app.locks/:rw".to_owned(),
                "/priv/app.locks/:rw".to_owned(),
            ],
            secret: CreatorAuthoritySecret::new("legacy-cookie-session-secret"),
            session_expires_at: None,
            last_revalidated_at: None,
        };

        let debug = format!("{record:?}");

        assert!(debug.contains("CreatorAuthorityRecord"));
        assert!(debug.contains("LegacyCookie"));
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("legacy-cookie-session-secret"));
    }

    #[test]
    fn creator_authority_auth_kind_parses_storage_values() {
        assert_eq!(
            CreatorAuthorityAuthKind::from_str("legacy_cookie"),
            Ok(CreatorAuthorityAuthKind::LegacyCookie)
        );
        assert_eq!(
            CreatorAuthorityAuthKind::from_str("grant"),
            Ok(CreatorAuthorityAuthKind::Grant)
        );
        assert_eq!(
            CreatorAuthorityAuthKind::LegacyCookie.as_str(),
            "legacy_cookie"
        );
        assert_eq!(CreatorAuthorityAuthKind::Grant.as_str(), "grant");
        assert_eq!(
            CreatorAuthorityAuthKind::from_str("cookie"),
            Err(ApplicationError::InvalidCreatorAuthorityAuthKind {
                auth_kind: "cookie".to_owned(),
            })
        );
    }

    #[test]
    fn creator_connect_flow_debug_output_redacts_authorization_url() {
        let record = PendingCreatorConnectFlowRecord {
            flow_id: CreatorConnectFlowId::new("flow-123"),
            return_to: "https://pubky.app/locks/callback".to_owned(),
            state: "frontend-state".to_owned(),
            authorization_url: CreatorConnectAuthorizationUrl::new(
                "pubkyauth://relay.example/connect?client_secret=super-secret-client-secret",
            ),
            requested_scopes: vec![
                "/pub/app.locks/:rw".to_owned(),
                "/priv/app.locks/:rw".to_owned(),
            ],
            created_at: datetime!(2026-06-17 12:00:00 UTC),
            expires_at: datetime!(2026-06-17 12:05:00 UTC),
        };

        let debug = format!("{record:?}");

        assert!(debug.contains("PendingCreatorConnectFlowRecord"));
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("super-secret-client-secret"));
        assert!(!debug.contains("pubkyauth://relay.example"));
        assert!(!record.is_expired_at(datetime!(2026-06-17 12:04:59 UTC)));
        assert!(record.is_expired_at(datetime!(2026-06-17 12:05:00 UTC)));
    }

    #[test]
    fn frontend_session_code_debug_output_redacts_code_and_detects_expiry() {
        let code = FrontendSessionCode::new("one-time-code-secret");
        let record = FrontendSessionCodeRecord {
            code: code.clone(),
            creator: CreatorPubky::from_str(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            )
            .unwrap(),
            state: "frontend-state".to_owned(),
            return_to: "https://pubky.app/locks/callback".to_owned(),
            created_at: datetime!(2026-06-17 12:00:00 UTC),
            expires_at: datetime!(2026-06-17 12:02:00 UTC),
            consumed_at: None,
        };

        let code_debug = format!("{code:?}");
        let record_debug = format!("{record:?}");

        assert!(code_debug.contains("<redacted>"));
        assert!(!code_debug.contains("one-time-code-secret"));
        assert!(record_debug.contains("<redacted>"));
        assert!(!record_debug.contains("one-time-code-secret"));
        assert!(!record.is_expired_at(datetime!(2026-06-17 12:01:59 UTC)));
        assert!(record.is_expired_at(datetime!(2026-06-17 12:02:00 UTC)));
    }

    #[test]
    fn frontend_session_token_debug_output_redacts_token_and_detects_expiry() {
        let token = FrontendSessionToken::new("frontend-session-token-secret");
        let record = FrontendSessionRecord {
            token: token.clone(),
            creator: CreatorPubky::from_str(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            )
            .unwrap(),
            created_at: datetime!(2026-06-17 12:00:00 UTC),
            expires_at: datetime!(2026-06-18 12:00:00 UTC),
        };

        let token_debug = format!("{token:?}");
        let record_debug = format!("{record:?}");

        assert!(token_debug.contains("<redacted>"));
        assert!(!token_debug.contains("frontend-session-token-secret"));
        assert!(record_debug.contains("<redacted>"));
        assert!(!record_debug.contains("frontend-session-token-secret"));
        assert!(!record.is_expired_at(datetime!(2026-06-18 11:59:59 UTC)));
        assert!(record.is_expired_at(datetime!(2026-06-18 12:00:00 UTC)));
    }

    #[test]
    fn access_credential_policy_uses_default_ttl_and_rejects_unsupported_values() {
        let policy = AccessCredentialPolicy::new(3600);

        assert_eq!(
            policy.default_ttl_seconds,
            DEFAULT_ACCESS_CREDENTIAL_TTL_SECONDS
        );
        assert_eq!(policy.validate_requested_ttl_seconds(900), Ok(900));
        assert_eq!(
            policy.validate_requested_ttl_seconds(0),
            Err(ApplicationError::UnsupportedCredentialTtl {
                requested_seconds: 0,
                max_seconds: 3600,
            })
        );
        assert_eq!(
            policy.validate_requested_ttl_seconds(3601),
            Err(ApplicationError::UnsupportedCredentialTtl {
                requested_seconds: 3601,
                max_seconds: 3600,
            })
        );
    }

    #[test]
    fn pending_task_transitions_to_in_progress_and_sets_started_at() {
        let task = pending_task();
        let started_at = datetime!(2026-05-29 12:01:00 UTC);

        let transitioned = task
            .transition_to(VerificationTaskStatus::InProgress, started_at, None)
            .unwrap();

        assert_eq!(transitioned.status, VerificationTaskStatus::InProgress);
        assert_eq!(transitioned.started_at, Some(started_at));
        assert_eq!(transitioned.completed_at, None);
        assert_eq!(transitioned.failure_message, None);
    }

    #[test]
    fn entitlement_publication_intent_precedes_completion_and_preserves_exact_payload() {
        let task = in_progress_task();
        let completed_at = datetime!(2026-05-29 12:03:00 UTC);
        let entitlement = entitlement();

        let publishing = task
            .begin_entitlement_publication(entitlement.clone())
            .unwrap();
        assert_eq!(
            publishing.status,
            VerificationTaskStatus::PublishingEntitlement
        );
        assert_eq!(publishing.entitlement_to_publish, Some(entitlement));
        assert_eq!(publishing.completed_at, None);

        let transitioned = publishing
            .complete_entitlement_publication(completed_at)
            .unwrap();

        assert_eq!(transitioned.status, VerificationTaskStatus::Completed);
        assert_eq!(transitioned.started_at, task.started_at);
        assert_eq!(transitioned.completed_at, Some(completed_at));
        assert_eq!(transitioned.failure_message, None);
        assert!(transitioned.entitlement_to_publish.is_some());
    }

    #[test]
    fn in_progress_task_transitions_to_failed_with_non_empty_failure_message() {
        let task = in_progress_task();
        let completed_at = datetime!(2026-05-29 12:03:00 UTC);

        let transitioned = task
            .transition_to(
                VerificationTaskStatus::Failed,
                completed_at,
                Some(" verifier rejected proof ".to_owned()),
            )
            .unwrap();

        assert_eq!(transitioned.status, VerificationTaskStatus::Failed);
        assert_eq!(transitioned.completed_at, Some(completed_at));
        assert_eq!(
            transitioned.failure_message.as_deref(),
            Some("verifier rejected proof")
        );
    }

    #[test]
    fn in_progress_task_expires_with_typed_reason_without_failure_message() {
        let expired_at = datetime!(2026-05-29 12:04:00 UTC);
        let from_in_progress = in_progress_task()
            .expire(
                VerificationTerminalReason::PaymentRequestRejected,
                expired_at,
            )
            .unwrap();

        assert_eq!(from_in_progress.status, VerificationTaskStatus::Expired);
        assert!(from_in_progress.started_at.is_some());
        assert_eq!(from_in_progress.completed_at, Some(expired_at));
        assert_eq!(from_in_progress.failure_message, None);
        assert_eq!(
            from_in_progress.terminal_reason,
            Some(VerificationTerminalReason::PaymentRequestRejected)
        );
    }

    #[test]
    fn terminal_task_states_cannot_transition() {
        let completed = completed_task();

        let error = completed
            .transition_to(
                VerificationTaskStatus::Failed,
                datetime!(2026-05-29 12:05:00 UTC),
                Some("too late".to_owned()),
            )
            .unwrap_err();

        assert_eq!(
            error,
            ApplicationError::InvalidVerificationTaskTransition {
                from: VerificationTaskStatus::Completed,
                to: VerificationTaskStatus::Failed,
            }
        );
    }

    #[test]
    fn failed_transition_requires_non_empty_failure_message() {
        let task = in_progress_task();

        let error = task
            .transition_to(
                VerificationTaskStatus::Failed,
                datetime!(2026-05-29 12:03:00 UTC),
                Some("   ".to_owned()),
            )
            .unwrap_err();

        assert_eq!(
            error,
            ApplicationError::InvalidVerificationTaskFailureMessage
        );
    }

    #[test]
    fn non_failed_transitions_reject_failure_message() {
        let task = in_progress_task()
            .begin_entitlement_publication(entitlement())
            .unwrap();

        let error = task
            .transition_to(
                VerificationTaskStatus::Completed,
                datetime!(2026-05-29 12:03:00 UTC),
                Some("should not be here".to_owned()),
            )
            .unwrap_err();

        assert_eq!(
            error,
            ApplicationError::InvalidVerificationTaskFailureMessage
        );
    }

    #[test]
    fn transition_rejects_malformed_current_task_state() {
        let mut task = pending_task();
        task.started_at = Some(datetime!(2026-05-29 12:00:30 UTC));

        let error = task
            .transition_to(
                VerificationTaskStatus::InProgress,
                datetime!(2026-05-29 12:01:00 UTC),
                None,
            )
            .unwrap_err();

        assert!(matches!(
            error,
            ApplicationError::InvalidVerificationTaskState { .. }
        ));
    }

    fn pending_task() -> VerificationTaskRecord {
        VerificationTaskRecord {
            task_id: TaskId::from_str(TASK_ID).unwrap(),
            creator: CreatorPubky::from_str(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            )
            .unwrap(),
            submitted_proof_bundle: submitted_proof_bundle(),
            status: VerificationTaskStatus::Pending,
            submitted_at: datetime!(2026-05-29 12:00:00 UTC),
            started_at: None,
            completed_at: None,
            failure_message: None,
            terminal_reason: None,
            entitlement_to_publish: None,
        }
    }

    fn in_progress_task() -> VerificationTaskRecord {
        VerificationTaskRecord {
            status: VerificationTaskStatus::InProgress,
            started_at: Some(datetime!(2026-05-29 12:01:00 UTC)),
            ..pending_task()
        }
    }

    fn completed_task() -> VerificationTaskRecord {
        VerificationTaskRecord {
            status: VerificationTaskStatus::Completed,
            started_at: Some(datetime!(2026-05-29 12:01:00 UTC)),
            completed_at: Some(datetime!(2026-05-29 12:03:00 UTC)),
            entitlement_to_publish: Some(entitlement()),
            ..pending_task()
        }
    }

    fn submitted_proof_bundle() -> SubmittedProofBundle {
        SubmittedProofBundle {
            version: SUBMITTED_PROOF_BUNDLE_VERSION,
            bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
            pubky_lock_resource: PubkyLockResource::from_str(&format!(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/app.locks/{LOCK_ID}.json"
            ))
            .unwrap(),
            reader_public_key: None,
            proofs: vec![Proof {
                criterion_id: "criterion-1".to_owned(),
                verifier_type: VerifierType::DevStatic,
                payload: json!({}),
            }],
        }
    }

    fn entitlement() -> VerifiedProofBundle {
        let submitted = submitted_proof_bundle();
        VerifiedProofBundle {
            version: VERIFIED_PROOF_BUNDLE_VERSION,
            bundle_id: submitted.bundle_id,
            pubky_lock_resource: submitted.pubky_lock_resource,
            verification_result: VerificationResult {
                criteria: vec![CriterionVerificationResult {
                    criterion_id: "criterion-1".to_owned(),
                    satisfied: true,
                    verified_at: datetime!(2026-05-29 12:02:00 UTC),
                    verified_by: LockServerPubky::from_str(
                        "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
                    )
                    .unwrap(),
                    verifier_type: VerifierType::DevStatic,
                }],
            },
            entitlement_lifetime: EntitlementLifetime::Unbounded,
        }
    }
}
