use time::{Duration, OffsetDateTime};

use locks_core::ids::{BundleId, CreatorPubky};

use crate::application::errors::ApplicationError;
use crate::application::models::{
    AccessCredential, AccessCredentialLookupKey, AccessCredentialPolicy, AccessCredentialRecord,
};
use crate::application::ports::{
    AccessCredentialGenerator, AccessCredentialStore, Clock, ContentLockRepository,
    EntitlementRepository,
};
use crate::application::use_cases::entitlement_check::load_valid_entitlement;

/// Request to issue a short-lived access credential from an existing entitlement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssueAccessCredentialRequest {
    /// Creator whose entitlement namespace should be resolved.
    pub creator: CreatorPubky,
    /// Viewer-generated bundle ID anchoring the entitlement.
    pub bundle_id: BundleId,
}

/// Raw issued credential plus the time after which it must not be honored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssuedAccessCredential {
    /// Raw bearer credential returned to the caller once.
    pub credential: AccessCredential,
    /// Time after which the credential is invalid.
    pub expires_at: OffsetDateTime,
}

/// Issues short-lived access credentials after re-checking entitlement state.
pub struct IssueAccessCredentialUseCase<'a> {
    entitlements: &'a dyn EntitlementRepository,
    content_locks: &'a dyn ContentLockRepository,
    credential_store: &'a dyn AccessCredentialStore,
    credential_generator: &'a dyn AccessCredentialGenerator,
    clock: &'a dyn Clock,
    policy: AccessCredentialPolicy,
}

impl<'a> IssueAccessCredentialUseCase<'a> {
    /// Creates a credential issuance use case from its application ports.
    pub fn new(
        entitlements: &'a dyn EntitlementRepository,
        content_locks: &'a dyn ContentLockRepository,
        credential_store: &'a dyn AccessCredentialStore,
        credential_generator: &'a dyn AccessCredentialGenerator,
        clock: &'a dyn Clock,
        policy: AccessCredentialPolicy,
    ) -> Self {
        Self {
            entitlements,
            content_locks,
            credential_store,
            credential_generator,
            clock,
            policy,
        }
    }

    /// Issues a new access credential if the current entitlement remains valid.
    pub async fn execute(
        &self,
        request: IssueAccessCredentialRequest,
    ) -> Result<IssuedAccessCredential, ApplicationError> {
        let now = self.clock.now();
        if self
            .credential_store
            .final_credential_available(&request.creator, &request.bundle_id, now)
            .await?
        {
            let candidate = self
                .credential_generator
                .generate_access_credential()
                .await?;
            let final_issue_now = self.clock.now();
            if let Some(final_credential) = self
                .credential_store
                .issue_or_replay_final_credential(
                    &request.creator,
                    &request.bundle_id,
                    final_issue_now,
                    candidate,
                )
                .await?
            {
                return Ok(IssuedAccessCredential {
                    credential: final_credential.credential,
                    expires_at: final_credential.expires_at,
                });
            }
        }
        let valid_entitlement = load_valid_entitlement(
            self.entitlements,
            self.content_locks,
            &request.creator,
            &request.bundle_id,
        )
        .await?;

        let requested_ttl_seconds = self.policy.validate_requested_ttl_seconds(
            valid_entitlement
                .content_lock
                .access_policy
                .requested_credential_ttl_seconds,
        )?;
        let expires_at = now + Duration::seconds(requested_ttl_seconds as i64);
        let credential = self
            .credential_generator
            .generate_access_credential()
            .await?;
        let lookup_key = AccessCredentialLookupKey::derive(&credential);

        self.credential_store
            .insert_access_credential(
                &valid_entitlement.lock_id,
                lookup_key,
                AccessCredentialRecord {
                    creator: request.creator,
                    bundle_id: request.bundle_id,
                    expires_at,
                },
            )
            .await?;

        Ok(IssuedAccessCredential {
            credential,
            expires_at,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use time::macros::datetime;

    use locks_core::ids::{BundleId, CreatorPubky};

    use super::{IssueAccessCredentialRequest, IssuedAccessCredential};
    use crate::application::models::AccessCredential;

    #[test]
    fn issue_access_credential_request_uses_creator_and_bundle_id_only() {
        let request = IssueAccessCredentialRequest {
            creator: CreatorPubky::from_str(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            )
            .unwrap(),
            bundle_id: BundleId::from_str("000G40R40M30E209185GR38E1W").unwrap(),
        };

        assert_eq!(
            request.creator.to_string(),
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy"
        );
        assert_eq!(request.bundle_id.as_str(), "000G40R40M30E209185GR38E1W");
    }

    #[test]
    fn issued_access_credential_returns_raw_credential_and_expiry() {
        let issued = IssuedAccessCredential {
            credential: AccessCredential::new("raw-bearer-credential"),
            expires_at: datetime!(2026-05-29 12:15:00 UTC),
        };

        assert_eq!(issued.credential.as_str(), "raw-bearer-credential");
        assert_eq!(issued.expires_at, datetime!(2026-05-29 12:15:00 UTC));
    }
}
