use pubky::{PubkyResource, PublicKey, PublicStorage, StatusCode, errors::RequestError};

const PAYKIT_PATH_PREFIX: &str = env!("LOCKS_PAYKIT_PATH_PREFIX");

/// Return whether a user has any public child under the current Paykit v0 namespace.
///
/// This reports data presence only. It does not prove that a Paykit receiver is valid,
/// current, capable, or ready.
pub async fn has_paykit_data(storage: &PublicStorage, user: &PublicKey) -> pubky::Result<bool> {
    let listing = storage
        .list(paykit_directory(user))?
        .shallow(true)
        .limit(1)
        .send()
        .await;
    classify_listing(listing)
}

fn paykit_directory(user: &PublicKey) -> String {
    format!("pubky://{}{PAYKIT_PATH_PREFIX}/", user.z32())
}

fn classify_listing(listing: pubky::Result<Vec<PubkyResource>>) -> pubky::Result<bool> {
    match listing {
        Ok(entries) => Ok(!entries.is_empty()),
        Err(pubky::Error::Request(RequestError::Server { status, .. }))
            if status == StatusCode::NOT_FOUND =>
        {
            Ok(false)
        }
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use pubky::{Keypair, PubkyResource, StatusCode, errors::RequestError};

    use super::{PAYKIT_PATH_PREFIX, classify_listing, paykit_directory};

    fn user() -> pubky::PublicKey {
        Keypair::from_secret(&[7; 32]).public_key()
    }

    fn resource(path: &str) -> PubkyResource {
        format!("pubky://{}{path}", user().z32()).parse().unwrap()
    }

    #[test]
    fn paykit_directory_uses_canonical_v0_prefix() {
        assert_eq!(PAYKIT_PATH_PREFIX, "/pub/paykit/v0");
        assert_eq!(
            paykit_directory(&user()),
            format!("pubky://{}{PAYKIT_PATH_PREFIX}/", user().z32())
        );
    }

    #[test]
    fn empty_listing_means_no_paykit_data() {
        assert!(!classify_listing(Ok(Vec::new())).unwrap());
    }

    #[test]
    fn any_valid_child_means_paykit_data() {
        assert!(
            classify_listing(Ok(vec![resource(
                "/pub/paykit/v0/unknown/future-record.bin"
            )]))
            .unwrap()
        );
    }

    #[test]
    fn absent_namespace_means_no_paykit_data() {
        assert!(
            !classify_listing(Err(pubky::Error::Request(RequestError::Server {
                status: StatusCode::NOT_FOUND,
                message: "not found".to_owned(),
            })))
            .unwrap()
        );
    }

    #[test]
    fn non_absence_failures_remain_errors() {
        let error = classify_listing(Err(pubky::Error::Request(RequestError::Server {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: "unavailable".to_owned(),
        })))
        .unwrap_err();

        assert!(matches!(
            error,
            pubky::Error::Request(RequestError::Server {
                status: StatusCode::SERVICE_UNAVAILABLE,
                ..
            })
        ));
    }

    #[test]
    fn malformed_listing_remains_error() {
        let error = classify_listing(Err(pubky::Error::Request(RequestError::Validation {
            message: "malformed listing entry".to_owned(),
        })))
        .unwrap_err();

        assert!(matches!(
            error,
            pubky::Error::Request(RequestError::Validation { .. })
        ));
    }
}
