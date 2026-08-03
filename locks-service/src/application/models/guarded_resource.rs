use locks_core::ids::{CreatorPubky, GuardedResourceHash};

/// Local representation of the current guarded resource bytes and metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GuardedResourceRecord {
    /// Creator who owns the guarded resource.
    pub creator: CreatorPubky,
    /// Creator-homeserver-relative guarded resource path.
    pub path: String,
    /// Hash expected by the content lock.
    pub hash: GuardedResourceHash,
    /// MIME content type for serving the guarded resource.
    pub content_type: String,
    /// Exact guarded resource byte length.
    pub size: u64,
    /// Bytes returned by the proxy-read use case.
    pub bytes: Vec<u8>,
}
