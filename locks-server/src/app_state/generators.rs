use std::str::FromStr;

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use locks_core::ids::TaskId;
use locks_service::application::{
    errors::ApplicationError,
    models::{AccessCredential, CreatorConnectFlowId, FrontendSessionCode, FrontendSessionToken},
    ports::{
        AccessCredentialGenerator, Clock, CreatorConnectFlowIdGenerator,
        FrontendSessionCodeGenerator, FrontendSessionTokenGenerator, VerificationTaskIdGenerator,
    },
};
use rand::RngCore;
use rand::rngs::OsRng;
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Debug, Default)]
pub struct OsRandomTaskIdGenerator;

#[async_trait]
impl VerificationTaskIdGenerator for OsRandomTaskIdGenerator {
    async fn generate_task_id(&self) -> Result<TaskId, ApplicationError> {
        let task_id = Uuid::new_v4().to_string();
        TaskId::from_str(&task_id).map_err(|error| ApplicationError::Storage {
            message: format!("generated invalid task ID: {error}"),
        })
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct OsRandomCreatorConnectFlowIdGenerator;

impl CreatorConnectFlowIdGenerator for OsRandomCreatorConnectFlowIdGenerator {
    fn generate_creator_connect_flow_id(&self) -> CreatorConnectFlowId {
        CreatorConnectFlowId::new(Uuid::new_v4().to_string())
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct OsRandomFrontendSessionCodeGenerator;

impl FrontendSessionCodeGenerator for OsRandomFrontendSessionCodeGenerator {
    fn generate_frontend_session_code(&self) -> FrontendSessionCode {
        let mut bytes = [0_u8; 32];
        OsRng.fill_bytes(&mut bytes);
        FrontendSessionCode::new(URL_SAFE_NO_PAD.encode(bytes))
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct OsRandomFrontendSessionTokenGenerator;

impl FrontendSessionTokenGenerator for OsRandomFrontendSessionTokenGenerator {
    fn generate_frontend_session_token(&self) -> FrontendSessionToken {
        let mut bytes = [0_u8; 32];
        OsRng.fill_bytes(&mut bytes);
        FrontendSessionToken::new(URL_SAFE_NO_PAD.encode(bytes))
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct OsRandomAccessCredentialGenerator;

#[async_trait]
impl AccessCredentialGenerator for OsRandomAccessCredentialGenerator {
    async fn generate_access_credential(&self) -> Result<AccessCredential, ApplicationError> {
        let mut bytes = [0_u8; 32];
        OsRng.fill_bytes(&mut bytes);
        Ok(AccessCredential::new(URL_SAFE_NO_PAD.encode(bytes)))
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::now_utc()
    }
}
