use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use locks_core::ids::CreatorPubky;
use locks_service::application::{
    errors::ApplicationError,
    models::{
        CreatorAuthorityRecord, CreatorConnectFlowId, FrontendSessionCode,
        FrontendSessionCodeRecord, FrontendSessionRecord, FrontendSessionToken,
        PendingCreatorConnectFlowRecord,
    },
    ports::{
        AccessCredentialStore, CreatorAuthorityManager, CreatorAuthorityStore,
        CreatorConnectFlowStore, FrontendSessionCodeStore, FrontendSessionStore,
        LegacyCreatorConnectFlowClient, VerificationTaskClaimer, VerificationTaskRepository,
    },
};
use time::OffsetDateTime;
use tokio::sync::RwLock;

#[derive(Clone)]
pub(super) struct PrivateRuntimeAdapters {
    pub(super) verification_tasks: Arc<dyn VerificationTaskRepository>,
    pub(super) verification_task_claimer: Arc<dyn VerificationTaskClaimer>,
    pub(super) access_credentials: Arc<dyn AccessCredentialStore>,
    pub(super) creator_authorities: Arc<dyn CreatorAuthorityStore>,
    pub(super) creator_connect_flows: Arc<dyn CreatorConnectFlowStore>,
    pub(super) frontend_session_codes: Arc<dyn FrontendSessionCodeStore>,
    pub(super) frontend_sessions: Arc<dyn FrontendSessionStore>,
    pub(super) creator_authority_manager: Arc<dyn CreatorAuthorityManager>,
    pub(super) legacy_creator_connect_flow_client: Arc<dyn LegacyCreatorConnectFlowClient>,
}

#[derive(Debug, Clone, Default)]
pub(super) struct InMemoryCreatorAuthorityStore {
    records: Arc<RwLock<HashMap<CreatorPubky, CreatorAuthorityRecord>>>,
}

impl InMemoryCreatorAuthorityStore {
    pub(super) fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl CreatorAuthorityStore for InMemoryCreatorAuthorityStore {
    async fn upsert_creator_authority(
        &self,
        authority: CreatorAuthorityRecord,
    ) -> Result<(), ApplicationError> {
        self.records
            .write()
            .await
            .insert(authority.creator.clone(), authority);
        Ok(())
    }

    async fn get_creator_authority(
        &self,
        creator: &CreatorPubky,
    ) -> Result<Option<CreatorAuthorityRecord>, ApplicationError> {
        Ok(self.records.read().await.get(creator).cloned())
    }

    async fn delete_creator_authority(
        &self,
        creator: &CreatorPubky,
    ) -> Result<(), ApplicationError> {
        self.records.write().await.remove(creator);
        Ok(())
    }
}

#[derive(Debug, Default)]
pub(super) struct InMemoryCreatorConnectFlowStore {
    records: RwLock<HashMap<CreatorConnectFlowId, PendingCreatorConnectFlowRecord>>,
}

impl InMemoryCreatorConnectFlowStore {
    pub(super) fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl CreatorConnectFlowStore for InMemoryCreatorConnectFlowStore {
    async fn insert_pending_creator_connect_flow(
        &self,
        record: PendingCreatorConnectFlowRecord,
    ) -> Result<(), ApplicationError> {
        self.records
            .write()
            .await
            .insert(record.flow_id.clone(), record);
        Ok(())
    }

    async fn get_pending_creator_connect_flow(
        &self,
        flow_id: &CreatorConnectFlowId,
    ) -> Result<Option<PendingCreatorConnectFlowRecord>, ApplicationError> {
        Ok(self.records.read().await.get(flow_id).cloned())
    }

    async fn delete_pending_creator_connect_flow(
        &self,
        flow_id: &CreatorConnectFlowId,
    ) -> Result<(), ApplicationError> {
        self.records.write().await.remove(flow_id);
        Ok(())
    }
}

#[derive(Debug, Default)]
pub(super) struct InMemoryFrontendSessionCodeStore {
    records: RwLock<HashMap<FrontendSessionCode, FrontendSessionCodeRecord>>,
}

impl InMemoryFrontendSessionCodeStore {
    pub(super) fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl FrontendSessionCodeStore for InMemoryFrontendSessionCodeStore {
    async fn insert_frontend_session_code(
        &self,
        record: FrontendSessionCodeRecord,
    ) -> Result<(), ApplicationError> {
        self.records
            .write()
            .await
            .insert(record.code.clone(), record);
        Ok(())
    }

    async fn consume_frontend_session_code(
        &self,
        code: &FrontendSessionCode,
        now: OffsetDateTime,
    ) -> Result<Option<FrontendSessionCodeRecord>, ApplicationError> {
        let mut records = self.records.write().await;
        let Some(record) = records.get_mut(code) else {
            return Ok(None);
        };
        let previous = record.clone();
        record.consumed_at = Some(now);
        Ok(Some(previous))
    }
}

#[derive(Debug, Default)]
pub(super) struct InMemoryFrontendSessionStore {
    records: RwLock<HashMap<FrontendSessionToken, FrontendSessionRecord>>,
}

impl InMemoryFrontendSessionStore {
    pub(super) fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl FrontendSessionStore for InMemoryFrontendSessionStore {
    async fn insert_frontend_session(
        &self,
        record: FrontendSessionRecord,
    ) -> Result<(), ApplicationError> {
        self.records
            .write()
            .await
            .insert(record.token.clone(), record);
        Ok(())
    }

    async fn get_frontend_session(
        &self,
        token: &FrontendSessionToken,
    ) -> Result<Option<FrontendSessionRecord>, ApplicationError> {
        Ok(self.records.read().await.get(token).cloned())
    }

    async fn delete_frontend_session(
        &self,
        token: &FrontendSessionToken,
    ) -> Result<(), ApplicationError> {
        self.records.write().await.remove(token);
        Ok(())
    }
}
