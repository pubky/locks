use time::OffsetDateTime;

use locks_core::ids::{BundleId, CreatorPubky};

use crate::application::errors::ApplicationError;
use crate::application::models::{AccessCredential, AccessCredentialLookupKey};
use crate::application::ports::{
    AccessCredentialStore, Clock, ContentLockRepository, EntitlementRepository,
};
use crate::application::use_cases::entitlement_check::load_valid_entitlement;

/// Request to validate a presented raw access credential.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidateAccessCredentialRequest {
    /// Raw bearer credential presented by the caller.
    pub credential: AccessCredential,
}

/// Successful validation result used by later proxy-read orchestration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedAccessCredential {
    /// Creator whose entitlement namespace remains valid.
    pub creator: CreatorPubky,
    /// Bundle ID anchoring the still-valid entitlement.
    pub bundle_id: BundleId,
    /// Time after which the credential must not be honored.
    pub expires_at: OffsetDateTime,
}

/// Validates presented access credentials and re-checks entitlement state.
pub struct ValidateAccessCredentialUseCase<'a> {
    credential_store: &'a dyn AccessCredentialStore,
    entitlements: &'a dyn EntitlementRepository,
    content_locks: &'a dyn ContentLockRepository,
    clock: &'a dyn Clock,
}

impl<'a> ValidateAccessCredentialUseCase<'a> {
    /// Creates a credential validation use case from its application ports.
    pub fn new(
        credential_store: &'a dyn AccessCredentialStore,
        entitlements: &'a dyn EntitlementRepository,
        content_locks: &'a dyn ContentLockRepository,
        clock: &'a dyn Clock,
    ) -> Self {
        Self {
            credential_store,
            entitlements,
            content_locks,
            clock,
        }
    }

    /// Validates a presented credential, deleting it if expired or entitlement is revoked.
    pub async fn execute(
        &self,
        request: ValidateAccessCredentialRequest,
    ) -> Result<ValidatedAccessCredential, ApplicationError> {
        let lookup_key = AccessCredentialLookupKey::derive(&request.credential);
        let record = self
            .credential_store
            .get_access_credential(&lookup_key)
            .await?
            .ok_or(ApplicationError::InvalidAccessCredential)?;

        if record.expires_at <= self.clock.now() {
            self.credential_store
                .delete_access_credential(&lookup_key)
                .await?;
            return Err(ApplicationError::ExpiredAccessCredential);
        }

        if let Err(error) = load_valid_entitlement(
            self.entitlements,
            self.content_locks,
            &record.creator,
            &record.bundle_id,
        )
        .await
        {
            self.credential_store
                .delete_access_credential(&lookup_key)
                .await?;
            return Err(error);
        }

        Ok(ValidatedAccessCredential {
            creator: record.creator,
            bundle_id: record.bundle_id,
            expires_at: record.expires_at,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use time::macros::datetime;

    use locks_core::ids::{BundleId, CreatorPubky};

    use super::{ValidateAccessCredentialRequest, ValidatedAccessCredential};
    use crate::application::models::AccessCredential;

    #[test]
    fn validate_access_credential_request_carries_raw_credential_only() {
        let request = ValidateAccessCredentialRequest {
            credential: AccessCredential::new("raw-bearer-credential"),
        };

        assert_eq!(request.credential.as_str(), "raw-bearer-credential");
    }

    #[test]
    fn validated_access_credential_returns_authorized_context_without_storage_record() {
        let validated = ValidatedAccessCredential {
            creator: CreatorPubky::from_str(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            )
            .unwrap(),
            bundle_id: BundleId::from_str("000G40R40M30E209185GR38E1W").unwrap(),
            expires_at: datetime!(2026-05-29 12:15:00 UTC),
        };

        assert_eq!(
            validated.creator.to_string(),
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy"
        );
        assert_eq!(validated.bundle_id.as_str(), "000G40R40M30E209185GR38E1W");
        assert_eq!(validated.expires_at, datetime!(2026-05-29 12:15:00 UTC));
    }
}
