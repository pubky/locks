use std::str::FromStr;

use locks_core::ids::CreatorPubky;
use pubky::Capabilities;

use crate::application::errors::ApplicationError;

/// Default Lock Server Pubky capabilities requested by the creator connect flow.
pub fn locks_connect_capabilities() -> Capabilities {
    Capabilities::builder()
        .read_write("/priv/app.locks/")
        .expect("static private Locks capability is canonical")
        .read_write("/pub/app.locks/")
        .expect("static public Locks capability is canonical")
        .finish()
}

/// Converts a Pubky SDK z32 public key into the Locks creator identity wrapper.
pub fn creator_from_pubky_public_key_z32(value: &str) -> Result<CreatorPubky, ApplicationError> {
    let public_key =
        pubky::PublicKey::try_from_z32(value).map_err(|error| ApplicationError::Storage {
            message: format!("invalid approved creator Pubky identity: {error}"),
        })?;
    CreatorPubky::from_str(&public_key.to_string()).map_err(|error| ApplicationError::Storage {
        message: format!("invalid approved creator Pubky identity: {error}"),
    })
}

/// Converts a Locks creator identity wrapper into a Pubky SDK z32 public key.
pub fn creator_z32_from_creator_pubky(creator: &CreatorPubky) -> Result<String, ApplicationError> {
    pubky::PublicKey::try_from(creator.to_string())
        .map(|public_key| public_key.z32())
        .map_err(|_| ApplicationError::Storage {
            message: "invalid creator Pubky identity wrapper".to_owned(),
        })
}

pub(crate) fn requested_scopes_to_capabilities(
    requested_scopes: &[String],
) -> Result<Capabilities, ApplicationError> {
    if requested_scopes.is_empty() {
        return Ok(locks_connect_capabilities());
    }

    let mut builder = Capabilities::builder();
    for scope in requested_scopes {
        builder = builder
            .read_write(scope.trim_end_matches(":rw"))
            .map_err(|_| ApplicationError::CreatorAuthoritySecret {
                message: "invalid creator capability scope".to_owned(),
            })?;
    }
    Ok(builder.finish())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use locks_core::ids::CreatorPubky;

    use super::{
        creator_from_pubky_public_key_z32, creator_z32_from_creator_pubky,
        locks_connect_capabilities, requested_scopes_to_capabilities,
    };
    use crate::application::errors::ApplicationError;

    const CREATOR_Z32: &str = "o1gg96ewuojmopcjbz8895478wdtxtzzuxnfjjz8o8e77csa1ngo";
    const CREATOR_PUBKY: &str = "pubkyo1gg96ewuojmopcjbz8895478wdtxtzzuxnfjjz8o8e77csa1ngo";

    #[test]
    fn locks_capabilities_are_limited_to_public_and_private_locks_namespaces() {
        let capabilities = locks_connect_capabilities();
        assert_eq!(
            capabilities.to_string(),
            "/priv/app.locks/:rw,/pub/app.locks/:rw"
        );
    }

    #[test]
    fn empty_requested_scopes_default_to_locks_capabilities() {
        assert_eq!(
            requested_scopes_to_capabilities(&[]).unwrap().to_string(),
            locks_connect_capabilities().to_string()
        );
    }

    #[test]
    fn invalid_requested_scope_is_rejected() {
        let error =
            requested_scopes_to_capabilities(&["relative-scope:rw".to_owned()]).unwrap_err();

        assert_eq!(
            error,
            ApplicationError::CreatorAuthoritySecret {
                message: "invalid creator capability scope".to_owned(),
            }
        );
    }

    #[test]
    fn creator_from_pubky_public_key_z32_adds_pubky_prefix_for_locks_identity() {
        let creator = creator_from_pubky_public_key_z32(CREATOR_Z32).unwrap();
        assert_eq!(creator.to_string(), CREATOR_PUBKY);
    }

    #[test]
    fn creator_z32_from_creator_pubky_removes_pubky_prefix_for_sdk_identity() {
        let creator = CreatorPubky::from_str(CREATOR_PUBKY).unwrap();

        let z32 = creator_z32_from_creator_pubky(&creator).unwrap();

        assert_eq!(z32, CREATOR_Z32);
    }

    #[test]
    fn creator_from_pubky_public_key_z32_rejects_empty_sdk_identity() {
        let error = creator_from_pubky_public_key_z32("").unwrap_err();

        assert_eq!(
            error,
            ApplicationError::Storage {
                message: "invalid approved creator Pubky identity: Invalid PublicKey length, expected 32 bytes but got: 0".to_owned()
            }
        );
    }
}
