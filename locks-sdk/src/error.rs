pub type Result<T> = std::result::Result<T, LocksSdkError>;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LocksSdkError {
    #[error("invalid lock server transport URL")]
    InvalidTransportUrl,
    #[error("invalid locks server discovery response")]
    InvalidDiscoveryResponse,
    #[error("unexpected locks server service: {0}")]
    UnexpectedDiscoveryService(String),
    #[error("unsupported locks server API version: {0}")]
    UnsupportedApiVersion(String),
    #[error("discovery lock server identity does not match expected server")]
    LockServerMismatch,
    #[error("unsupported Locks service pointer version: {0}")]
    UnsupportedLockServicePointerVersion(u16),
    #[error("PKARR record did not contain a browser-usable domain endpoint")]
    MissingBrowserDomainEndpoint,
    #[error("browser endpoint is missing its domain")]
    MissingBrowserEndpointDomain,
    #[error("browser testnet endpoint is missing required HTTP_PORT parameter")]
    MissingHttpPortParam,
    #[error("invalid Lock Server response: {0}")]
    InvalidResponse(String),
    #[error("content lock creator does not match requested resource")]
    ContentLockCreatorMismatch,
    #[error("content lock path does not match requested resource")]
    ContentLockPathMismatch,
    #[error(
        "content lock has no Lock Server override and no creator Lock Service Pointer was provided"
    )]
    MissingCreatorLockServicePointer,
}
