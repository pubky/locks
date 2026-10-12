use std::collections::HashMap;
use std::sync::{Arc, Weak};

use locks_core::ids::CreatorPubky;
use time::OffsetDateTime;
use tokio::sync::{Mutex, watch};

use crate::application::errors::ApplicationError;
use crate::application::models::{CreatorAuthorityAuthKind, CreatorAuthorityRecord};
use crate::application::ports::CreatorAuthorityStore;
use crate::infrastructure::pubky::legacy_connect_flow::creator_from_pubky_public_key_z32;
use crate::infrastructure::pubky::storage_client::{ImportedPubkySession, PubkySessionImporter};

type RestoreResult<T> = Result<T, ApplicationError>;

#[derive(Debug, Clone)]
pub struct CachedGrantSession<T> {
    pub record: CreatorAuthorityRecord,
    pub session: T,
    pub(crate) generation: u64,
}

#[derive(Debug)]
enum CacheValue<T> {
    Ready(T),
    InFlight(watch::Receiver<Option<RestoreResult<T>>>),
}

#[derive(Debug)]
struct CacheEntry<T> {
    fingerprint: [u8; 32],
    generation: u64,
    observed_at: u64,
    last_used: u64,
    value: CacheValue<T>,
}

#[derive(Debug)]
struct CacheState<T> {
    entries: HashMap<CreatorPubky, CacheEntry<T>>,
    next_generation: u64,
    next_observation: u64,
    access_counter: u64,
}

impl<T> Default for CacheState<T> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            next_generation: 0,
            next_observation: 0,
            access_counter: 0,
        }
    }
}

#[derive(Debug)]
pub struct GrantSessionCache<S, I>
where
    I: PubkySessionImporter,
{
    store: S,
    importer: I,
    capacity: usize,
    state: Arc<Mutex<CacheState<I::Session>>>,
    authority_load_gates: Arc<Mutex<HashMap<CreatorPubky, Weak<Mutex<()>>>>>,
}

impl<S, I> Clone for GrantSessionCache<S, I>
where
    S: Clone,
    I: PubkySessionImporter + Clone,
{
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            importer: self.importer.clone(),
            capacity: self.capacity,
            state: Arc::clone(&self.state),
            authority_load_gates: Arc::clone(&self.authority_load_gates),
        }
    }
}

impl<S, I> GrantSessionCache<S, I>
where
    S: CreatorAuthorityStore,
    I: PubkySessionImporter,
    I::Session: Clone,
{
    pub fn new(store: S, importer: I, capacity: usize) -> Self {
        assert!(
            capacity > 0,
            "grant session cache capacity must be positive"
        );
        Self {
            store,
            importer,
            capacity,
            state: Arc::new(Mutex::new(CacheState::default())),
            authority_load_gates: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn session_for_creator(
        &self,
        creator: &CreatorPubky,
    ) -> Result<CachedGrantSession<I::Session>, ApplicationError> {
        let authority_load_gate = self.authority_load_gate(creator).await;
        let authority_load_guard = authority_load_gate.lock().await;
        let observation = {
            let mut state = self.state.lock().await;
            state.next_observation = state.next_observation.wrapping_add(1);
            state.next_observation
        };
        let record = match self.load_current_grant(creator).await {
            Ok(record) => record,
            Err(error) => {
                let mut state = self.state.lock().await;
                let superseded = state
                    .entries
                    .get(creator)
                    .is_some_and(|entry| observation_is_newer(entry.observed_at, observation));
                if !superseded {
                    state.entries.remove(creator);
                }
                return Err(error);
            }
        };
        let fingerprint = authority_fingerprint(&record);

        enum Action<T> {
            Ready {
                generation: u64,
                session: T,
            },
            Wait {
                generation: u64,
                receiver: watch::Receiver<Option<RestoreResult<T>>>,
            },
            Restore {
                generation: u64,
                sender: watch::Sender<Option<RestoreResult<T>>>,
            },
            Retry,
        }

        let action = {
            let mut state = self.state.lock().await;
            state.entries.retain(|_, entry| match &entry.value {
                CacheValue::Ready(_) => true,
                CacheValue::InFlight(receiver) => receiver.has_changed().is_ok(),
            });
            state.access_counter = state.access_counter.wrapping_add(1);
            let access = state.access_counter;
            let same_authority = state
                .entries
                .get(creator)
                .is_some_and(|entry| entry.fingerprint == fingerprint);
            if same_authority {
                let entry = state.entries.get_mut(creator).expect("entry was present");
                if observation_is_newer(observation, entry.observed_at) {
                    entry.observed_at = observation;
                }
                entry.last_used = access;
                match &entry.value {
                    CacheValue::Ready(session) => Action::Ready {
                        generation: entry.generation,
                        session: session.clone(),
                    },
                    CacheValue::InFlight(receiver) => Action::Wait {
                        generation: entry.generation,
                        receiver: receiver.clone(),
                    },
                }
            } else if state
                .entries
                .get(creator)
                .is_some_and(|entry| observation_is_newer(entry.observed_at, observation))
            {
                Action::Retry
            } else {
                if !state.entries.contains_key(creator) && state.entries.len() >= self.capacity {
                    let evict = state
                        .entries
                        .iter()
                        .filter(|(_, entry)| matches!(entry.value, CacheValue::Ready(_)))
                        .min_by_key(|(_, entry)| entry.last_used)
                        .map(|(creator, _)| creator.clone());
                    if let Some(evict) = evict {
                        state.entries.remove(&evict);
                    } else {
                        tracing::warn!(
                            creator = %creator,
                            stage = "cache_admission",
                            error_class = "capacity_in_flight",
                            "creator grant session restore failed"
                        );
                        return Err(ApplicationError::CreatorAuthorityUnavailable);
                    }
                }
                state.next_generation = state.next_generation.wrapping_add(1);
                let generation = state.next_generation;
                let (sender, receiver) = watch::channel(None);
                state.entries.insert(
                    creator.clone(),
                    CacheEntry {
                        fingerprint,
                        generation,
                        observed_at: observation,
                        last_used: access,
                        value: CacheValue::InFlight(receiver),
                    },
                );
                Action::Restore { generation, sender }
            }
        };
        drop(authority_load_guard);
        drop(authority_load_gate);

        match action {
            Action::Retry => Box::pin(self.session_for_creator(creator)).await,
            Action::Ready {
                generation,
                session,
            } => Ok(CachedGrantSession {
                record,
                session,
                generation,
            }),
            Action::Wait {
                generation,
                mut receiver,
            } => loop {
                if let Some(result) = receiver.borrow().clone() {
                    return result.map(|session| CachedGrantSession {
                        record: record.clone(),
                        session,
                        generation,
                    });
                }
                if receiver.changed().await.is_err() {
                    self.invalidate_generation(creator, generation).await;
                    return Box::pin(self.session_for_creator(creator)).await;
                }
            },
            Action::Restore { generation, sender } => {
                let result = self.restore_current(creator, &record, fingerprint).await;
                let mut state = self.state.lock().await;
                let is_current = state.entries.get(creator).is_some_and(|entry| {
                    entry.generation == generation && entry.fingerprint == fingerprint
                });
                let result = if is_current {
                    if let Ok(session) = &result {
                        state.access_counter = state.access_counter.wrapping_add(1);
                        let access = state.access_counter;
                        if let Some(entry) = state.entries.get_mut(creator) {
                            entry.last_used = access;
                            entry.value = CacheValue::Ready(session.clone());
                        }
                    } else {
                        state.entries.remove(creator);
                    }
                    result
                } else {
                    tracing::warn!(
                        creator = %creator,
                        stage = "cache_install",
                        error_class = "superseded_generation",
                        "creator grant session restore failed"
                    );
                    Err(ApplicationError::CreatorAuthorityUnavailable)
                };
                let _ = sender.send(Some(result.clone()));
                result.map(|session| CachedGrantSession {
                    record,
                    session,
                    generation,
                })
            }
        }
    }

    pub async fn invalidate(&self, creator: &CreatorPubky) {
        self.state.lock().await.entries.remove(creator);
    }

    pub(crate) async fn invalidate_generation(&self, creator: &CreatorPubky, generation: u64) {
        let mut state = self.state.lock().await;
        if state
            .entries
            .get(creator)
            .is_some_and(|entry| entry.generation == generation)
        {
            state.entries.remove(creator);
        }
    }

    #[cfg(test)]
    async fn cached_creator_count(&self) -> usize {
        self.state.lock().await.entries.len()
    }

    async fn load_current_grant(
        &self,
        creator: &CreatorPubky,
    ) -> Result<CreatorAuthorityRecord, ApplicationError> {
        let record = self
            .store
            .get_creator_authority(creator)
            .await
            .inspect_err(|error| {
                tracing::warn!(
                    creator = %creator,
                    stage = "authority_lookup",
                    error_class = application_error_class(error),
                    "creator grant session restore failed"
                );
            })?
            .ok_or_else(|| {
                tracing::warn!(
                    creator = %creator,
                    stage = "authority_lookup",
                    error_class = "missing",
                    "creator grant session restore failed"
                );
                ApplicationError::CreatorAuthorityUnavailable
            })?;
        if record.auth_kind != CreatorAuthorityAuthKind::Grant
            || record.creator != *creator
            || record
                .session_expires_at
                .is_none_or(|expiry| expiry <= OffsetDateTime::now_utc())
        {
            tracing::warn!(
                creator = %creator,
                stage = "grant_validate",
                error_class = "invalid_or_expired",
                "creator grant session restore failed"
            );
            return Err(ApplicationError::CreatorAuthorityUnavailable);
        }
        Ok(record)
    }

    async fn authority_load_gate(&self, creator: &CreatorPubky) -> Arc<Mutex<()>> {
        let mut gates = self.authority_load_gates.lock().await;
        gates.retain(|_, gate| gate.strong_count() > 0);
        if let Some(gate) = gates.get(creator).and_then(Weak::upgrade) {
            return gate;
        }
        let gate = Arc::new(Mutex::new(()));
        gates.insert(creator.clone(), Arc::downgrade(&gate));
        gate
    }

    async fn restore_current(
        &self,
        creator: &CreatorPubky,
        record: &CreatorAuthorityRecord,
        fingerprint: [u8; 32],
    ) -> RestoreResult<I::Session> {
        let session = self
            .importer
            .import_grant_session(&record.secret)
            .await
            .inspect_err(|error| {
                tracing::warn!(
                    creator = %creator,
                    stage = "grant_exchange",
                    error_class = application_error_class(error),
                    "creator grant session restore failed"
                );
            })?;
        let restored =
            creator_from_pubky_public_key_z32(&session.public_key_z32()).map_err(|_| {
                tracing::warn!(
                    creator = %creator,
                    stage = "identity_validate",
                    error_class = "invalid_identity",
                    "creator grant session restore failed"
                );
                ApplicationError::CreatorAuthorityUnavailable
            })?;
        if restored != *creator {
            tracing::warn!(
                creator = %creator,
                stage = "identity_validate",
                error_class = "identity_mismatch",
                "creator grant session restore failed"
            );
            return Err(ApplicationError::CreatorAuthorityUnavailable);
        }
        let current = self.load_current_grant(creator).await?;
        if authority_fingerprint(&current) != fingerprint {
            tracing::warn!(
                creator = %creator,
                stage = "authority_rotation",
                error_class = "stale_exchange",
                "creator grant session restore failed"
            );
            return Err(ApplicationError::CreatorAuthorityUnavailable);
        }
        Ok(session)
    }
}

fn authority_fingerprint(record: &CreatorAuthorityRecord) -> [u8; 32] {
    let mut scopes = record.granted_scopes.clone();
    scopes.sort_unstable();
    let mut hasher = blake3::Hasher::new_derive_key("pubky-locks creator grant cache v1");
    hasher.update(record.creator.to_string().as_bytes());
    hasher.update(record.auth_kind.as_str().as_bytes());
    for scope in scopes {
        hasher.update(scope.as_bytes());
        hasher.update(&[0]);
    }
    hasher.update(record.secret.expose_secret().as_bytes());
    if let Some(expiry) = record.session_expires_at {
        hasher.update(&expiry.unix_timestamp_nanos().to_le_bytes());
    }
    *hasher.finalize().as_bytes()
}

fn observation_is_newer(candidate: u64, reference: u64) -> bool {
    let distance = candidate.wrapping_sub(reference);
    distance != 0 && distance < (1_u64 << 63)
}

fn application_error_class(error: &ApplicationError) -> &'static str {
    match error {
        ApplicationError::CreatorAuthorityUnavailable => "authority_unavailable",
        ApplicationError::CreatorAuthoritySecret { .. } => "authority_secret",
        ApplicationError::Storage { .. } => "storage",
        _ => "application",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::fmt;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use async_trait::async_trait;
    use locks_core::ids::CreatorPubky;
    use time::{Duration as TimeDuration, OffsetDateTime};
    use tokio::sync::{Notify, RwLock};
    use tracing::{Event, Subscriber};
    use tracing_subscriber::layer::{Context, SubscriberExt};
    use tracing_subscriber::{Layer, Registry};

    use super::GrantSessionCache;
    use crate::application::errors::ApplicationError;
    use crate::application::models::{
        CreatorAuthorityAuthKind, CreatorAuthorityRecord, CreatorAuthoritySecret,
    };
    use crate::application::ports::CreatorAuthorityManager;
    use crate::application::ports::CreatorAuthorityStore;
    use crate::infrastructure::pubky::legacy_creator_authority::{
        GrantCreatorAuthorityManager, GrantCredentialRevalidator,
    };
    use crate::infrastructure::pubky::storage_client::{
        CreatorScopedPubkyStorage, CreatorScopedPubkyStorageProvider,
        GrantCreatorScopedPubkyStorageProvider, ImportedPubkySession, PubkyBytesResource,
        PubkyResourceMetadata, PubkySessionImporter,
    };

    #[tokio::test]
    async fn concurrent_and_sequential_reads_share_one_grant_exchange() {
        let store = MutableAuthorityStore::with_records([record(creator_a(), "grant-a")]);
        let importer = RecordingImporter::successful(Duration::from_millis(30));
        let cache = Arc::new(GrantSessionCache::new(store, importer.clone(), 8));

        let mut tasks = Vec::new();
        for _ in 0..8 {
            let cache = Arc::clone(&cache);
            tasks.push(tokio::spawn(async move {
                cache.session_for_creator(&creator_a()).await.unwrap();
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        cache.session_for_creator(&creator_a()).await.unwrap();

        assert_eq!(importer.calls(), 1);
    }

    #[tokio::test]
    async fn authority_and_storage_share_one_cached_grant_session() {
        let store = MutableAuthorityStore::with_records([record(creator_a(), "grant-a")]);
        let importer = RecordingImporter::successful(Duration::from_millis(30));
        let cache = Arc::new(GrantSessionCache::new(store.clone(), importer.clone(), 8));
        let manager = GrantCreatorAuthorityManager::new(store, Arc::clone(&cache));
        let provider = GrantCreatorScopedPubkyStorageProvider::new(cache);
        let creator = creator_a();

        let (authority, storage) = tokio::join!(
            manager.require_creator_authority(&creator),
            provider.storage_for_creator(&creator),
        );

        authority.unwrap();
        storage.unwrap();
        assert_eq!(importer.calls(), 1);
    }

    #[tokio::test]
    async fn different_creators_exchange_without_blocking_each_other() {
        let store = MutableAuthorityStore::with_records([
            record(creator_a(), "grant-a"),
            record(creator_b(), "grant-b"),
        ]);
        let importer = RecordingImporter::successful(Duration::from_millis(40));
        let cache = Arc::new(GrantSessionCache::new(store, importer.clone(), 8));

        let creator_a = creator_a();
        let creator_b = creator_b();
        let (a, b) = tokio::join!(
            cache.session_for_creator(&creator_a),
            cache.session_for_creator(&creator_b)
        );

        a.unwrap();
        b.unwrap();
        assert_eq!(importer.calls(), 2);
        assert!(importer.max_in_flight() >= 2);
    }

    #[tokio::test]
    async fn authority_changes_invalidate_cached_session() {
        let store = MutableAuthorityStore::with_records([record(creator_a(), "grant-a")]);
        let importer = RecordingImporter::successful(Duration::ZERO);
        let cache = GrantSessionCache::new(store.clone(), importer.clone(), 8);

        cache.session_for_creator(&creator_a()).await.unwrap();
        let mut changed = record(creator_a(), "grant-a-rotated");
        changed.granted_scopes.push("/pub/other/:r".to_owned());
        store.put(changed).await;
        cache.session_for_creator(&creator_a()).await.unwrap();

        assert_eq!(importer.calls(), 2);
    }

    #[tokio::test]
    async fn invalidation_only_removes_the_acquired_session_generation() {
        let store = MutableAuthorityStore::with_records([record(creator_a(), "grant-a")]);
        let importer = RecordingImporter::successful(Duration::ZERO);
        let cache = GrantSessionCache::new(store, importer.clone(), 8);

        let old = cache.session_for_creator(&creator_a()).await.unwrap();
        cache.invalidate(&creator_a()).await;
        let replacement = cache.session_for_creator(&creator_a()).await.unwrap();

        cache
            .invalidate_generation(&creator_a(), old.generation)
            .await;
        cache.session_for_creator(&creator_a()).await.unwrap();
        assert_eq!(importer.calls(), 2, "old failure must preserve replacement");

        cache
            .invalidate_generation(&creator_a(), replacement.generation)
            .await;
        cache.session_for_creator(&creator_a()).await.unwrap();
        assert_eq!(
            importer.calls(),
            3,
            "current failure must evict its session"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn revalidation_rejects_authority_rotated_after_manager_lookup() {
        let events = RecordedEvents::default();
        let _guard = tracing::subscriber::set_default(Registry::default().with(events.clone()));
        let store = MutableAuthorityStore::with_records([record(creator_a(), "grant-a-rotated")]);
        let importer = RecordingImporter::successful(Duration::ZERO);
        let cache = Arc::new(GrantSessionCache::new(store, importer, 8));
        let stale_secret = CreatorAuthoritySecret::new("grant-a");

        assert_eq!(
            cache
                .revalidate_grant_credential(&creator_a(), &stale_secret)
                .await,
            Err(ApplicationError::CreatorAuthorityUnavailable)
        );
        events.assert_secret_safe_diagnostic(
            "manager_record_validate",
            "stale_manager_record",
            &["grant-a", "grant-a-rotated"],
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn late_exchange_cannot_install_rotated_authority() {
        let events = RecordedEvents::default();
        let _guard = tracing::subscriber::set_default(Registry::default().with(events.clone()));
        let store = MutableAuthorityStore::with_records([record(creator_a(), "grant-a")]);
        let importer = RecordingImporter::successful(Duration::from_millis(60));
        let cache = Arc::new(GrantSessionCache::new(store.clone(), importer.clone(), 8));

        let old = {
            let cache = Arc::clone(&cache);
            tokio::spawn(async move { cache.session_for_creator(&creator_a()).await })
        };
        while importer.calls() == 0 {
            tokio::task::yield_now().await;
        }
        store.put(record(creator_a(), "grant-a-rotated")).await;
        let current = cache.session_for_creator(&creator_a()).await;

        assert_eq!(
            old.await.unwrap().unwrap_err(),
            ApplicationError::CreatorAuthorityUnavailable
        );
        current.unwrap();
        cache.session_for_creator(&creator_a()).await.unwrap();
        assert_eq!(importer.calls(), 2);
        events.assert_secret_safe_diagnostic(
            "cache_install",
            "superseded_generation",
            &["grant-a", "grant-a-rotated"],
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stale_authority_lookup_cannot_replace_newer_grant_restore() {
        let store = DelayedFirstLookupStore::new(record(creator_a(), "grant-a"));
        let importer = GrantAwareImporter::default();
        let cache = Arc::new(GrantSessionCache::new(store.clone(), importer.clone(), 8));

        let stale = {
            let cache = Arc::clone(&cache);
            tokio::spawn(async move { cache.session_for_creator(&creator_a()).await })
        };
        store.first_lookup_captured.notified().await;

        store.put(record(creator_a(), "grant-a-rotated")).await;
        let replacement = {
            let cache = Arc::clone(&cache);
            tokio::spawn(async move { cache.session_for_creator(&creator_a()).await })
        };
        store.release_first_lookup.notify_one();
        importer.replacement_started.notified().await;
        let waiter = {
            let cache = Arc::clone(&cache);
            tokio::spawn(async move { cache.session_for_creator(&creator_a()).await })
        };

        importer.release_replacement.notify_one();

        assert_eq!(
            stale.await.unwrap().unwrap_err(),
            ApplicationError::CreatorAuthorityUnavailable
        );
        replacement.await.unwrap().unwrap();
        waiter.await.unwrap().unwrap();
        cache.session_for_creator(&creator_a()).await.unwrap();
        assert_eq!(
            importer.secrets(),
            vec!["grant-a".to_owned(), "grant-a-rotated".to_owned()],
            "stale restore must fail while the replacement becomes current"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn later_request_with_stale_snapshot_cannot_replace_newer_grant_restore() {
        let store = AdversarialSnapshotStore::new(record(creator_a(), "grant-a"));
        let importer = GrantAwareImporter::default();
        let cache = Arc::new(GrantSessionCache::new(store.clone(), importer.clone(), 8));

        let earlier = {
            let cache = Arc::clone(&cache);
            tokio::spawn(async move { cache.session_for_creator(&creator_a()).await })
        };
        store.first_lookup_waiting.notified().await;

        let later = {
            let cache = Arc::clone(&cache);
            tokio::spawn(async move { cache.session_for_creator(&creator_a()).await })
        };
        tokio::task::yield_now().await;

        store.put(record(creator_a(), "grant-a-rotated")).await;
        store.release_first_lookup.notify_one();
        importer.replacement_started.notified().await;

        store.release_second_lookup.notify_one();
        tokio::task::yield_now().await;
        importer.release_replacement.notify_one();

        earlier.await.unwrap().unwrap();
        later.await.unwrap().unwrap();
        cache.session_for_creator(&creator_a()).await.unwrap();
        assert_eq!(
            importer.secrets(),
            vec!["grant-a-rotated".to_owned()],
            "a later request must not capture and install a stale authority snapshot"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cache_saturation_reports_secret_safe_diagnostic() {
        let events = RecordedEvents::default();
        let _guard = tracing::subscriber::set_default(Registry::default().with(events.clone()));
        let store = MutableAuthorityStore::with_records([
            record(creator_a(), "grant-a"),
            record(creator_b(), "grant-b"),
        ]);
        let importer = RecordingImporter::successful(Duration::from_secs(10));
        let cache = Arc::new(GrantSessionCache::new(store, importer.clone(), 1));
        let in_flight = {
            let cache = Arc::clone(&cache);
            tokio::spawn(async move { cache.session_for_creator(&creator_a()).await })
        };
        while importer.calls() == 0 {
            tokio::task::yield_now().await;
        }

        assert_eq!(
            cache.session_for_creator(&creator_b()).await.unwrap_err(),
            ApplicationError::CreatorAuthorityUnavailable
        );
        in_flight.abort();
        in_flight.await.unwrap_err();
        events.assert_secret_safe_diagnostic(
            "cache_admission",
            "capacity_in_flight",
            &["grant-a", "grant-b"],
        );
    }

    #[tokio::test]
    async fn failed_and_expired_grants_are_not_cached() {
        let store = MutableAuthorityStore::with_records([record(creator_a(), "grant-a")]);
        let importer = RecordingImporter::with_results([
            Err(ApplicationError::CreatorAuthorityUnavailable),
            Ok(FakeImportedSession::for_creator(&creator_a())),
        ]);
        let cache = GrantSessionCache::new(store.clone(), importer.clone(), 8);

        assert_eq!(
            cache.session_for_creator(&creator_a()).await.unwrap_err(),
            ApplicationError::CreatorAuthorityUnavailable
        );
        cache.session_for_creator(&creator_a()).await.unwrap();
        assert_eq!(importer.calls(), 2);

        let mut expired = record(creator_a(), "expired");
        expired.session_expires_at = Some(OffsetDateTime::now_utc() - TimeDuration::SECOND);
        store.put(expired).await;
        assert_eq!(
            cache.session_for_creator(&creator_a()).await.unwrap_err(),
            ApplicationError::CreatorAuthorityUnavailable
        );
        assert_eq!(importer.calls(), 2);
    }

    #[tokio::test]
    async fn cancelled_exchange_does_not_leave_creator_permanently_in_flight() {
        let store = MutableAuthorityStore::with_records([record(creator_a(), "grant-a")]);
        let importer = RecordingImporter::successful(Duration::from_millis(20));
        let cache = Arc::new(GrantSessionCache::new(store, importer.clone(), 8));

        let exchange = {
            let cache = Arc::clone(&cache);
            tokio::spawn(async move { cache.session_for_creator(&creator_a()).await })
        };
        while importer.calls() == 0 {
            tokio::task::yield_now().await;
        }
        exchange.abort();
        exchange.await.unwrap_err();

        tokio::time::timeout(
            Duration::from_millis(100),
            cache.session_for_creator(&creator_a()),
        )
        .await
        .expect("a cancelled exchange must release the creator single-flight slot")
        .expect("a new exchange must restore the creator session");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn waiter_retries_after_the_restore_owner_is_cancelled() {
        let store = MutableAuthorityStore::with_records([record(creator_a(), "grant-a")]);
        let importer = RecordingImporter::successful(Duration::from_millis(20));
        let cache = Arc::new(GrantSessionCache::new(store, importer.clone(), 8));

        let owner = {
            let cache = Arc::clone(&cache);
            tokio::spawn(async move { cache.session_for_creator(&creator_a()).await })
        };
        while importer.calls() == 0 {
            tokio::task::yield_now().await;
        }
        let waiter = {
            let cache = Arc::clone(&cache);
            tokio::spawn(async move { cache.session_for_creator(&creator_a()).await })
        };
        tokio::task::yield_now().await;
        owner.abort();
        owner.await.unwrap_err();

        tokio::time::timeout(Duration::from_millis(100), waiter)
            .await
            .expect("waiter must retry after owner cancellation")
            .expect("waiter task must complete")
            .expect("waiter retry must restore the session");
        assert_eq!(importer.calls(), 2);
    }

    #[tokio::test]
    async fn cancelled_exchange_releases_capacity_for_another_creator() {
        let store = MutableAuthorityStore::with_records([
            record(creator_a(), "grant-a"),
            record(creator_b(), "grant-b"),
        ]);
        let importer = RecordingImporter::successful(Duration::from_millis(20));
        let cache = Arc::new(GrantSessionCache::new(store, importer.clone(), 1));

        let exchange = {
            let cache = Arc::clone(&cache);
            tokio::spawn(async move { cache.session_for_creator(&creator_a()).await })
        };
        while importer.calls() == 0 {
            tokio::task::yield_now().await;
        }
        exchange.abort();
        exchange.await.unwrap_err();

        tokio::time::timeout(
            Duration::from_millis(100),
            cache.session_for_creator(&creator_b()),
        )
        .await
        .expect("a cancelled exchange must release cache capacity")
        .expect("another creator must restore after cancelled capacity owner");
        assert_eq!(importer.calls(), 2);
        assert_eq!(cache.cached_creator_count().await, 1);
    }

    #[tokio::test]
    async fn cache_capacity_is_bounded_by_lru_eviction() {
        let store = MutableAuthorityStore::with_records([
            record(creator_a(), "grant-a"),
            record(creator_b(), "grant-b"),
        ]);
        let importer = RecordingImporter::successful(Duration::ZERO);
        let cache = GrantSessionCache::new(store, importer.clone(), 1);

        cache.session_for_creator(&creator_a()).await.unwrap();
        cache.session_for_creator(&creator_b()).await.unwrap();
        cache.session_for_creator(&creator_a()).await.unwrap();

        assert_eq!(importer.calls(), 3);
        assert!(cache.cached_creator_count().await <= 1);
    }

    #[derive(Clone, Default)]
    struct MutableAuthorityStore {
        records: Arc<RwLock<HashMap<CreatorPubky, CreatorAuthorityRecord>>>,
    }

    impl MutableAuthorityStore {
        fn with_records(records: impl IntoIterator<Item = CreatorAuthorityRecord>) -> Self {
            Self {
                records: Arc::new(RwLock::new(
                    records
                        .into_iter()
                        .map(|record| (record.creator.clone(), record))
                        .collect(),
                )),
            }
        }

        async fn put(&self, record: CreatorAuthorityRecord) {
            self.records
                .write()
                .await
                .insert(record.creator.clone(), record);
        }
    }

    #[async_trait]
    impl CreatorAuthorityStore for MutableAuthorityStore {
        async fn upsert_creator_authority(
            &self,
            authority: CreatorAuthorityRecord,
        ) -> Result<(), ApplicationError> {
            self.put(authority).await;
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

    #[derive(Clone)]
    struct DelayedFirstLookupStore {
        record: Arc<RwLock<CreatorAuthorityRecord>>,
        calls: Arc<AtomicUsize>,
        first_lookup_captured: Arc<Notify>,
        release_first_lookup: Arc<Notify>,
    }

    impl DelayedFirstLookupStore {
        fn new(record: CreatorAuthorityRecord) -> Self {
            Self {
                record: Arc::new(RwLock::new(record)),
                calls: Arc::new(AtomicUsize::new(0)),
                first_lookup_captured: Arc::new(Notify::new()),
                release_first_lookup: Arc::new(Notify::new()),
            }
        }

        async fn put(&self, record: CreatorAuthorityRecord) {
            *self.record.write().await = record;
        }
    }

    #[async_trait]
    impl CreatorAuthorityStore for DelayedFirstLookupStore {
        async fn upsert_creator_authority(
            &self,
            authority: CreatorAuthorityRecord,
        ) -> Result<(), ApplicationError> {
            self.put(authority).await;
            Ok(())
        }

        async fn get_creator_authority(
            &self,
            creator: &CreatorPubky,
        ) -> Result<Option<CreatorAuthorityRecord>, ApplicationError> {
            let record = self.record.read().await.clone();
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                self.first_lookup_captured.notify_one();
                self.release_first_lookup.notified().await;
            }
            Ok((record.creator == *creator).then_some(record))
        }

        async fn delete_creator_authority(
            &self,
            _creator: &CreatorPubky,
        ) -> Result<(), ApplicationError> {
            unimplemented!("not needed by delayed lookup test")
        }
    }

    #[derive(Clone)]
    struct AdversarialSnapshotStore {
        record: Arc<RwLock<CreatorAuthorityRecord>>,
        calls: Arc<AtomicUsize>,
        first_lookup_waiting: Arc<Notify>,
        release_first_lookup: Arc<Notify>,
        release_second_lookup: Arc<Notify>,
    }

    impl AdversarialSnapshotStore {
        fn new(record: CreatorAuthorityRecord) -> Self {
            Self {
                record: Arc::new(RwLock::new(record)),
                calls: Arc::new(AtomicUsize::new(0)),
                first_lookup_waiting: Arc::new(Notify::new()),
                release_first_lookup: Arc::new(Notify::new()),
                release_second_lookup: Arc::new(Notify::new()),
            }
        }

        async fn put(&self, record: CreatorAuthorityRecord) {
            *self.record.write().await = record;
        }
    }

    #[async_trait]
    impl CreatorAuthorityStore for AdversarialSnapshotStore {
        async fn upsert_creator_authority(
            &self,
            authority: CreatorAuthorityRecord,
        ) -> Result<(), ApplicationError> {
            self.put(authority).await;
            Ok(())
        }

        async fn get_creator_authority(
            &self,
            creator: &CreatorPubky,
        ) -> Result<Option<CreatorAuthorityRecord>, ApplicationError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                self.first_lookup_waiting.notify_one();
                self.release_first_lookup.notified().await;
            }
            let record = self.record.read().await.clone();
            if call == 1 {
                self.release_second_lookup.notified().await;
            }
            Ok((record.creator == *creator).then_some(record))
        }

        async fn delete_creator_authority(
            &self,
            _creator: &CreatorPubky,
        ) -> Result<(), ApplicationError> {
            unimplemented!("not needed by adversarial snapshot test")
        }
    }

    #[derive(Clone, Default)]
    struct GrantAwareImporter {
        secrets: Arc<Mutex<Vec<String>>>,
        replacement_started: Arc<Notify>,
        release_replacement: Arc<Notify>,
    }

    impl GrantAwareImporter {
        fn secrets(&self) -> Vec<String> {
            self.secrets.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl PubkySessionImporter for GrantAwareImporter {
        type Session = FakeImportedSession;

        async fn import_legacy_cookie_session(
            &self,
            _secret: &CreatorAuthoritySecret,
        ) -> Result<Self::Session, ApplicationError> {
            unreachable!("grant cache never imports legacy cookie sessions")
        }

        async fn import_grant_session(
            &self,
            secret: &CreatorAuthoritySecret,
        ) -> Result<Self::Session, ApplicationError> {
            let secret = secret.expose_secret().to_owned();
            self.secrets.lock().unwrap().push(secret.clone());
            if secret == "grant-a-rotated" {
                self.replacement_started.notify_one();
                self.release_replacement.notified().await;
            }
            Ok(FakeImportedSession::for_creator(&creator_a()))
        }
    }

    #[derive(Clone)]
    struct RecordingImporter {
        results: Arc<Mutex<VecDeque<Result<FakeImportedSession, ApplicationError>>>>,
        calls: Arc<AtomicUsize>,
        in_flight: Arc<AtomicUsize>,
        max_in_flight: Arc<AtomicUsize>,
        delay: Duration,
    }

    impl RecordingImporter {
        fn successful(delay: Duration) -> Self {
            Self {
                results: Arc::new(Mutex::new(VecDeque::new())),
                calls: Arc::new(AtomicUsize::new(0)),
                in_flight: Arc::new(AtomicUsize::new(0)),
                max_in_flight: Arc::new(AtomicUsize::new(0)),
                delay,
            }
        }

        fn with_results(
            results: impl IntoIterator<Item = Result<FakeImportedSession, ApplicationError>>,
        ) -> Self {
            Self {
                results: Arc::new(Mutex::new(results.into_iter().collect())),
                calls: Arc::new(AtomicUsize::new(0)),
                in_flight: Arc::new(AtomicUsize::new(0)),
                max_in_flight: Arc::new(AtomicUsize::new(0)),
                delay: Duration::ZERO,
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }

        fn max_in_flight(&self) -> usize {
            self.max_in_flight.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl PubkySessionImporter for RecordingImporter {
        type Session = FakeImportedSession;

        async fn import_legacy_cookie_session(
            &self,
            _secret: &CreatorAuthoritySecret,
        ) -> Result<Self::Session, ApplicationError> {
            unreachable!("grant cache never imports legacy cookie sessions")
        }

        async fn import_grant_session(
            &self,
            secret: &CreatorAuthoritySecret,
        ) -> Result<Self::Session, ApplicationError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let active = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(active, Ordering::SeqCst);
            tokio::time::sleep(self.delay).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            if let Some(result) = self.results.lock().unwrap().pop_front() {
                return result;
            }
            let creator = if secret.expose_secret().ends_with('b') {
                creator_b()
            } else {
                creator_a()
            };
            Ok(FakeImportedSession::for_creator(&creator))
        }
    }

    #[derive(Debug, Clone)]
    struct FakeImportedSession {
        creator_z32: String,
    }

    impl FakeImportedSession {
        fn for_creator(creator: &CreatorPubky) -> Self {
            Self {
                creator_z32: creator.to_string().trim_start_matches("pubky").to_owned(),
            }
        }
    }

    impl ImportedPubkySession for FakeImportedSession {
        type Storage = FakeStorage;

        fn public_key_z32(&self) -> String {
            self.creator_z32.clone()
        }

        fn into_storage(self) -> Self::Storage {
            FakeStorage
        }

        fn into_grant_storage(self) -> Self::Storage {
            FakeStorage
        }
    }

    #[derive(Debug, Clone)]
    struct FakeStorage;

    #[async_trait]
    impl CreatorScopedPubkyStorage for FakeStorage {
        async fn put_json_value(
            &self,
            _path: &str,
            _body: serde_json::Value,
        ) -> Result<(), ApplicationError> {
            Ok(())
        }

        async fn get_json_value(
            &self,
            _path: &str,
        ) -> Result<Option<serde_json::Value>, ApplicationError> {
            Ok(None)
        }

        async fn put_bytes(
            &self,
            _path: &str,
            _bytes: Vec<u8>,
            _content_type: &str,
        ) -> Result<(), ApplicationError> {
            Ok(())
        }

        async fn get_bytes(
            &self,
            _path: &str,
        ) -> Result<Option<PubkyBytesResource>, ApplicationError> {
            Ok(None)
        }

        async fn get_metadata(
            &self,
            _path: &str,
        ) -> Result<Option<PubkyResourceMetadata>, ApplicationError> {
            Ok(None)
        }

        async fn delete(&self, _path: &str) -> Result<(), ApplicationError> {
            Ok(())
        }
    }

    #[derive(Clone, Default)]
    struct RecordedEvents(Arc<Mutex<Vec<HashMap<String, String>>>>);

    impl RecordedEvents {
        fn assert_secret_safe_diagnostic(
            &self,
            stage: &str,
            error_class: &str,
            forbidden_values: &[&str],
        ) {
            let events = self.0.lock().unwrap();
            let event = events
                .iter()
                .find(|event| {
                    event.get("stage").is_some_and(|value| value == stage)
                        && event
                            .get("error_class")
                            .is_some_and(|value| value == error_class)
                })
                .unwrap_or_else(|| {
                    panic!(
                        "missing diagnostic stage={stage} error_class={error_class}; events={events:?}"
                    )
                });
            const SENSITIVE_FIELD_TERMS: [&str; 5] = ["grant", "secret", "session", "path", "body"];
            assert!(
                event
                    .keys()
                    .all(|key| { SENSITIVE_FIELD_TERMS.iter().all(|term| !key.contains(term)) }),
                "diagnostic contains sensitive field: {event:?}"
            );
            assert!(
                forbidden_values
                    .iter()
                    .all(|value| event.values().all(|field| !field.contains(value))),
                "diagnostic contains sensitive value: {event:?}"
            );
        }
    }

    impl<S> Layer<S> for RecordedEvents
    where
        S: Subscriber,
    {
        fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
            let mut fields = HashMap::new();
            event.record(&mut FieldVisitor(&mut fields));
            self.0.lock().unwrap().push(fields);
        }
    }

    struct FieldVisitor<'a>(&'a mut HashMap<String, String>);

    impl tracing::field::Visit for FieldVisitor<'_> {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
            self.0.insert(field.name().to_owned(), format!("{value:?}"));
        }

        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            self.0.insert(field.name().to_owned(), value.to_owned());
        }
    }

    fn record(creator: CreatorPubky, secret: &str) -> CreatorAuthorityRecord {
        CreatorAuthorityRecord {
            creator,
            auth_kind: CreatorAuthorityAuthKind::Grant,
            granted_scopes: vec![
                "/pub/app.locks/:rw".to_owned(),
                "/priv/app.locks/:rw".to_owned(),
            ],
            secret: CreatorAuthoritySecret::new(secret),
            session_expires_at: Some(OffsetDateTime::now_utc() + TimeDuration::HOUR),
            last_revalidated_at: None,
        }
    }

    fn creator_a() -> CreatorPubky {
        CreatorPubky::from_str("pubkyo1gg96ewuojmopcjbz8895478wdtxtzzuxnfjjz8o8e77csa1ngo").unwrap()
    }

    fn creator_b() -> CreatorPubky {
        CreatorPubky::from_str("pubkypxnu33x7jtpx9ar1ytsi4yxbp6a5o36gwhffs8zoxmbuptici1jy").unwrap()
    }
}
