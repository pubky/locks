use std::fmt;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

const CREATOR_AUTHORITY_KEY_CONTEXT: &str =
    "pubky-locks v1 runtime master key: creator authority secrets";
const FINAL_CREDENTIAL_KEY_CONTEXT: &str =
    "pubky-locks v1 runtime master key: final deletion credentials";

/// Root key for deriving independent runtime encryption keys.
#[derive(Clone)]
pub struct RuntimeMasterKey {
    bytes: [u8; 32],
}

impl fmt::Debug for RuntimeMasterKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("RuntimeMasterKey")
            .field(&"<redacted>")
            .finish()
    }
}

#[derive(Debug, thiserror::Error)]
#[error("runtime master key must be an unpadded base64url-encoded 32-byte key")]
pub struct InvalidRuntimeMasterKey;

impl RuntimeMasterKey {
    pub fn from_base64url(value: &str) -> Result<Self, InvalidRuntimeMasterKey> {
        let bytes = URL_SAFE_NO_PAD
            .decode(value)
            .map_err(|_| InvalidRuntimeMasterKey)?;
        let bytes = bytes.try_into().map_err(|_| InvalidRuntimeMasterKey)?;
        Ok(Self { bytes })
    }

    pub fn creator_authority_key(&self) -> [u8; 32] {
        blake3::derive_key(CREATOR_AUTHORITY_KEY_CONTEXT, &self.bytes)
    }

    pub fn final_credential_key(&self) -> [u8; 32] {
        blake3::derive_key(FINAL_CREDENTIAL_KEY_CONTEXT, &self.bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_stable_distinct_keys_for_closed_runtime_domains() {
        let encoded = URL_SAFE_NO_PAD.encode([7u8; 32]);
        let first = RuntimeMasterKey::from_base64url(&encoded).unwrap();
        let second = RuntimeMasterKey::from_base64url(&encoded).unwrap();

        assert_eq!(
            first.creator_authority_key(),
            second.creator_authority_key()
        );
        assert_eq!(first.final_credential_key(), second.final_credential_key());
        assert_ne!(first.creator_authority_key(), first.final_credential_key());
        assert_ne!(first.creator_authority_key(), [7u8; 32]);
        assert_ne!(first.final_credential_key(), [7u8; 32]);
    }

    #[test]
    fn rejects_invalid_or_wrong_length_values_without_exposing_input() {
        for value in ["not-a-key***", &URL_SAFE_NO_PAD.encode([7u8; 31])] {
            let error = RuntimeMasterKey::from_base64url(value).unwrap_err();
            let debug = format!("{error:?}");
            assert!(!debug.contains(value));
        }
    }

    #[test]
    fn debug_output_redacts_root_key() {
        let encoded = URL_SAFE_NO_PAD.encode([9u8; 32]);
        let key = RuntimeMasterKey::from_base64url(&encoded).unwrap();
        let debug = format!("{key:?}");

        assert_eq!(debug, "RuntimeMasterKey(\"<redacted>\")");
        assert!(!debug.contains(&encoded));
    }
}
