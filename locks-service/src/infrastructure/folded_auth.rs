use std::fmt;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};
use rand::RngCore;
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};

use crate::application::errors::ApplicationError;
use crate::application::models::{
    EncryptedFoldedAuthFlowSecret, FoldedAuthFlowSecret, FoldedAuthSecretContext,
};
use crate::infrastructure::runtime_master_key::RuntimeMasterKey;

const ENVELOPE_PREFIX: &str = "v1.xchacha20poly1305:";
const AAD_DOMAIN: &[u8] = b"pubky-locks-folded-auth-flow-aad";
const PLAINTEXT_VERSION: u16 = 1;

#[derive(Clone)]
pub struct FoldedAuthSecretCipher {
    key: [u8; 32],
}

impl fmt::Debug for FoldedAuthSecretCipher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("FoldedAuthSecretCipher")
            .field(&"<redacted>")
            .finish()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PlaintextEnvelope {
    version: u16,
    flow_state: String,
}

impl FoldedAuthSecretCipher {
    pub fn from_runtime_master_key(master_key: &RuntimeMasterKey) -> Self {
        Self::new(master_key.folded_auth_key())
    }

    fn new(key: [u8; 32]) -> Self {
        Self { key }
    }

    pub fn encrypt(
        &self,
        context: &FoldedAuthSecretContext,
        secret: &FoldedAuthFlowSecret,
    ) -> Result<EncryptedFoldedAuthFlowSecret, ApplicationError> {
        let plaintext = serde_json::to_vec(&PlaintextEnvelope {
            version: PLAINTEXT_VERSION,
            flow_state: URL_SAFE_NO_PAD.encode(secret.expose_bytes()),
        })
        .map_err(|_| encrypt_error())?;
        let cipher = XChaCha20Poly1305::new(Key::from_slice(&self.key));
        let mut nonce = [0_u8; 24];
        OsRng.fill_bytes(&mut nonce);
        let ciphertext = cipher
            .encrypt(
                XNonce::from_slice(&nonce),
                Payload {
                    msg: &plaintext,
                    aad: &associated_data(context),
                },
            )
            .map_err(|_| encrypt_error())?;
        Ok(EncryptedFoldedAuthFlowSecret::new(format!(
            "{ENVELOPE_PREFIX}{}:{}",
            URL_SAFE_NO_PAD.encode(nonce),
            URL_SAFE_NO_PAD.encode(ciphertext)
        )))
    }

    pub fn decrypt(
        &self,
        context: &FoldedAuthSecretContext,
        encrypted: &EncryptedFoldedAuthFlowSecret,
    ) -> Result<FoldedAuthFlowSecret, ApplicationError> {
        let rest = encrypted
            .as_str()
            .strip_prefix(ENVELOPE_PREFIX)
            .ok_or_else(decrypt_error)?;
        let (nonce, ciphertext) = rest.split_once(':').ok_or_else(decrypt_error)?;
        let nonce: [u8; 24] = URL_SAFE_NO_PAD
            .decode(nonce)
            .map_err(|_| decrypt_error())?
            .try_into()
            .map_err(|_| decrypt_error())?;
        let ciphertext = URL_SAFE_NO_PAD
            .decode(ciphertext)
            .map_err(|_| decrypt_error())?;
        let cipher = XChaCha20Poly1305::new(Key::from_slice(&self.key));
        let plaintext = cipher
            .decrypt(
                XNonce::from_slice(&nonce),
                Payload {
                    msg: &ciphertext,
                    aad: &associated_data(context),
                },
            )
            .map_err(|_| decrypt_error())?;
        let decoded: PlaintextEnvelope =
            serde_json::from_slice(&plaintext).map_err(|_| decrypt_error())?;
        if decoded.version != PLAINTEXT_VERSION {
            return Err(decrypt_error());
        }
        let bytes = URL_SAFE_NO_PAD
            .decode(decoded.flow_state)
            .map_err(|_| decrypt_error())?;
        if bytes.is_empty() {
            return Err(decrypt_error());
        }
        Ok(FoldedAuthFlowSecret::new(bytes))
    }
}

fn associated_data(context: &FoldedAuthSecretContext) -> Vec<u8> {
    let creator = context.creator.to_string();
    let bundle = context.bundle_id.to_string();
    let resource = context.pubky_lock_resource.to_string();
    let mut output = Vec::with_capacity(256);
    append_field(&mut output, AAD_DOMAIN);
    append_field(&mut output, &[1]);
    append_field(&mut output, creator.as_bytes());
    append_field(&mut output, bundle.as_bytes());
    append_field(&mut output, &context.generation.to_be_bytes());
    append_field(&mut output, resource.as_bytes());
    append_field(&mut output, &context.lock_hash);
    output
}

fn append_field(output: &mut Vec<u8>, field: &[u8]) {
    let length = u32::try_from(field.len()).expect("folded auth AAD field length fits u32");
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(field);
}

fn encrypt_error() -> ApplicationError {
    ApplicationError::FoldedAuthSecret {
        message: "failed to encrypt folded auth flow".to_owned(),
    }
}

fn decrypt_error() -> ApplicationError {
    ApplicationError::FoldedAuthSecret {
        message: "invalid folded auth flow envelope".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use locks_core::ids::{BundleId, ContentLockPath, CreatorPubky, PubkyLockResource};

    use super::FoldedAuthSecretCipher;
    use crate::application::models::{FoldedAuthFlowSecret, FoldedAuthSecretContext};

    #[test]
    fn ciphertext_is_randomized_context_bound_and_redacted() {
        let cipher = FoldedAuthSecretCipher::new([7; 32]);
        let encryption_context = context(1);
        let secret = FoldedAuthFlowSecret::new(b"sensitive-flow-state".to_vec());
        let first = cipher.encrypt(&encryption_context, &secret).unwrap();
        let second = cipher.encrypt(&encryption_context, &secret).unwrap();

        assert_ne!(first.as_str(), second.as_str());
        assert!(!first.as_str().contains("sensitive"));
        assert!(!format!("{first:?} {secret:?} {cipher:?}").contains("sensitive"));
        assert_eq!(
            cipher
                .decrypt(&encryption_context, &first)
                .unwrap()
                .expose_bytes(),
            secret.expose_bytes()
        );
        assert!(cipher.decrypt(&context(2), &first).is_err());
        assert!(
            FoldedAuthSecretCipher::new([8; 32])
                .decrypt(&encryption_context, &first)
                .is_err()
        );
    }

    fn context(generation: u64) -> FoldedAuthSecretContext {
        let creator =
            CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy")
                .unwrap();
        FoldedAuthSecretContext {
            creator: creator.clone(),
            bundle_id: BundleId::from_str("000G40R40M30E209185GR38E1W").unwrap(),
            generation,
            pubky_lock_resource: PubkyLockResource::new(
                creator,
                ContentLockPath::from_str(
                    "/pub/app.locks/000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG.json",
                )
                .unwrap(),
            ),
            lock_hash: [4; 32],
        }
    }
}
