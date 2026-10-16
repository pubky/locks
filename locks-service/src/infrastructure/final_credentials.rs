use std::fmt;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};
use rand::RngCore;
use rand::rngs::OsRng;

use crate::application::errors::ApplicationError;
use crate::application::models::{
    AccessCredential, EncryptedFinalCredential, FinalCredentialContext,
};

const ENVELOPE_PREFIX: &str = "v1.xchacha20poly1305:";
const AAD_DOMAIN: &[u8] = b"pubky-locks-final-credential-aad";

#[derive(Clone)]
pub struct FinalCredentialCipher {
    key: [u8; 32],
}

impl fmt::Debug for FinalCredentialCipher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("FinalCredentialCipher")
            .field(&"<redacted>")
            .finish()
    }
}

impl FinalCredentialCipher {
    pub fn new(key: [u8; 32]) -> Self {
        Self { key }
    }

    pub fn encrypt(
        &self,
        context: &FinalCredentialContext,
        credential: &AccessCredential,
    ) -> Result<EncryptedFinalCredential, ApplicationError> {
        let cipher = XChaCha20Poly1305::new(Key::from_slice(&self.key));
        let mut nonce_bytes = [0u8; 24];
        OsRng.fill_bytes(&mut nonce_bytes);
        let ciphertext = cipher
            .encrypt(
                XNonce::from_slice(&nonce_bytes),
                Payload {
                    msg: credential.as_str().as_bytes(),
                    aad: &associated_data(context),
                },
            )
            .map_err(|_| encrypt_error())?;
        Ok(EncryptedFinalCredential::new(format!(
            "{ENVELOPE_PREFIX}{}:{}",
            URL_SAFE_NO_PAD.encode(nonce_bytes),
            URL_SAFE_NO_PAD.encode(ciphertext)
        )))
    }

    pub fn decrypt(
        &self,
        context: &FinalCredentialContext,
        envelope: &EncryptedFinalCredential,
    ) -> Result<AccessCredential, ApplicationError> {
        let rest = envelope
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
        String::from_utf8(plaintext)
            .map(AccessCredential::new)
            .map_err(|_| decrypt_error())
    }
}

fn associated_data(context: &FinalCredentialContext) -> Vec<u8> {
    let creator = context.creator.to_string();
    let bundle_id = context.bundle_id.to_string();
    let mut aad = Vec::with_capacity(AAD_DOMAIN.len() + creator.len() + bundle_id.len() + 32);
    append_field(&mut aad, AAD_DOMAIN);
    append_field(&mut aad, &[1]);
    append_field(&mut aad, context.deletion_job_id.as_bytes());
    append_field(&mut aad, creator.as_bytes());
    append_field(&mut aad, bundle_id.as_bytes());
    aad
}

fn append_field(output: &mut Vec<u8>, field: &[u8]) {
    let length = u32::try_from(field.len()).expect("credential AAD fields fit in u32");
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(field);
}

fn encrypt_error() -> ApplicationError {
    ApplicationError::FinalCredentialSecret {
        message: "failed to encrypt final credential".to_owned(),
    }
}

fn decrypt_error() -> ApplicationError {
    ApplicationError::FinalCredentialSecret {
        message: "invalid final credential envelope".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use locks_core::ids::{BundleId, CreatorPubky};
    use uuid::Uuid;

    use super::*;

    #[test]
    fn encrypted_final_credential_round_trips_under_exact_context() {
        let cipher = FinalCredentialCipher::new([7; 32]);
        let context = context();
        let credential = AccessCredential::new("secret-final-bearer");

        let envelope = cipher.encrypt(&context, &credential).unwrap();
        let decrypted = cipher.decrypt(&context, &envelope).unwrap();

        assert_eq!(decrypted, credential);
        assert!(!envelope.as_str().contains(credential.as_str()));
        assert!(!format!("{envelope:?}").contains(credential.as_str()));
        assert!(!format!("{cipher:?}").contains('7'));
    }

    #[test]
    fn wrong_key_or_any_context_change_fails_closed() {
        let cipher = FinalCredentialCipher::new([7; 32]);
        let context = context();
        let envelope = cipher
            .encrypt(&context, &AccessCredential::new("secret-final-bearer"))
            .unwrap();

        assert!(
            FinalCredentialCipher::new([8; 32])
                .decrypt(&context, &envelope)
                .is_err()
        );
        for (field, changed) in [
            (
                "job",
                FinalCredentialContext {
                    deletion_job_id: Uuid::new_v4(),
                    ..context.clone()
                },
            ),
            (
                "creator",
                FinalCredentialContext {
                    creator: CreatorPubky::from_str(
                        &pubky_common::crypto::Keypair::from_secret(&[2; 32])
                            .public_key()
                            .to_string(),
                    )
                    .unwrap(),
                    ..context.clone()
                },
            ),
            (
                "bundle",
                FinalCredentialContext {
                    bundle_id: BundleId::from_str("000G40R40M30E209185GR38E1V").unwrap(),
                    ..context.clone()
                },
            ),
        ] {
            assert_ne!(changed, context, "{field} mutation must change context");
            assert!(
                cipher.decrypt(&changed, &envelope).is_err(),
                "altered {field} authenticated successfully"
            );
        }
    }

    #[test]
    fn wrong_version_and_corrupt_envelopes_fail_without_secret_output() {
        let cipher = FinalCredentialCipher::new([7; 32]);
        let context = context();
        let bearer = "secret-final-bearer";
        let valid = cipher
            .encrypt(&context, &AccessCredential::new(bearer))
            .unwrap();
        let corrupt = [
            EncryptedFinalCredential::new(valid.as_str().replacen("v1.", "v2.", 1)),
            EncryptedFinalCredential::new("v1.xchacha20poly1305:not-base64:not-base64"),
            EncryptedFinalCredential::new("v1.xchacha20poly1305:"),
        ];

        for envelope in corrupt {
            let error = cipher.decrypt(&context, &envelope).unwrap_err();
            assert_eq!(
                error,
                ApplicationError::FinalCredentialSecret {
                    message: "invalid final credential envelope".to_owned()
                }
            );
            assert!(!format!("{error:?}").contains(bearer));
            assert!(!error.to_string().contains(envelope.as_str()));
        }
    }

    fn context() -> FinalCredentialContext {
        FinalCredentialContext {
            deletion_job_id: Uuid::from_u128(1),
            creator: CreatorPubky::from_str(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            )
            .unwrap(),
            bundle_id: BundleId::from_str("000G40R40M30E209185GR38E1W").unwrap(),
        }
    }
}
