use locks_core::ids::{CreatorPubky, LockServerPubky};
use locks_core::lock_service_pointer::{
    LOCK_SERVICE_POINTER_PATH, LOCK_SERVICE_POINTER_VERSION, LockServicePointer,
};

use crate::application::errors::ApplicationError;
use crate::application::ports::{Clock, LockServicePointerRepository};

pub struct SetLockServicePointerRequest {
    pub creator: CreatorPubky,
    pub default_lock_server: LockServerPubky,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetLockServicePointerResponse {
    pub creator: CreatorPubky,
    pub path: &'static str,
    pub lock_service_pointer: LockServicePointer,
}

pub struct SetLockServicePointerUseCase<'a> {
    repository: &'a dyn LockServicePointerRepository,
    clock: &'a dyn Clock,
}

impl<'a> SetLockServicePointerUseCase<'a> {
    pub fn new(repository: &'a dyn LockServicePointerRepository, clock: &'a dyn Clock) -> Self {
        Self { repository, clock }
    }

    pub async fn execute(
        &self,
        request: SetLockServicePointerRequest,
    ) -> Result<SetLockServicePointerResponse, ApplicationError> {
        let pointer = LockServicePointer {
            version: LOCK_SERVICE_POINTER_VERSION,
            default_lock_server: request.default_lock_server,
            created_at: self.clock.now(),
        };
        self.repository
            .upsert_lock_service_pointer(request.creator.clone(), pointer.clone())
            .await?;

        Ok(SetLockServicePointerResponse {
            creator: request.creator,
            path: LOCK_SERVICE_POINTER_PATH,
            lock_service_pointer: pointer,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use locks_core::ids::{CreatorPubky, LockServerPubky};
    use locks_core::lock_service_pointer::LOCK_SERVICE_POINTER_PATH;
    use time::OffsetDateTime;
    use time::macros::datetime;

    use crate::application::ports::{Clock, LockServicePointerRepository};
    use crate::application::use_cases::set_lock_service_pointer::{
        SetLockServicePointerRequest, SetLockServicePointerUseCase,
    };
    use crate::infrastructure::memory::lock_service_pointers::InMemoryLockServicePointerRepository;

    #[tokio::test]
    async fn set_lock_service_pointer_stores_current_pointer_by_creator() {
        let repo = InMemoryLockServicePointerRepository::new();
        let clock = FixedClock(datetime!(2026-06-03 00:00:00 UTC));
        let use_case = SetLockServicePointerUseCase::new(&repo, &clock);
        let creator = creator();
        let lock_server =
            LockServerPubky::from_str("pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo")
                .unwrap();

        let response = use_case
            .execute(SetLockServicePointerRequest {
                creator: creator.clone(),
                default_lock_server: lock_server.clone(),
            })
            .await
            .unwrap();

        assert_eq!(response.creator, creator);
        assert_eq!(response.path, LOCK_SERVICE_POINTER_PATH);
        assert_eq!(response.lock_service_pointer.version, 1);
        assert_eq!(
            response.lock_service_pointer.default_lock_server,
            lock_server
        );
        assert_eq!(response.lock_service_pointer.created_at, clock.now());

        let stored = repo
            .get_lock_service_pointer(&creator)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored, response.lock_service_pointer);
    }

    #[tokio::test]
    async fn set_lock_service_pointer_replaces_current_pointer_by_creator() {
        let repo = InMemoryLockServicePointerRepository::new();
        let first_clock = FixedClock(datetime!(2026-06-03 00:00:00 UTC));
        let second_clock = FixedClock(datetime!(2026-06-03 01:00:00 UTC));
        let creator = creator();

        SetLockServicePointerUseCase::new(&repo, &first_clock)
            .execute(SetLockServicePointerRequest {
                creator: creator.clone(),
                default_lock_server: LockServerPubky::from_str(
                    "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
                )
                .unwrap(),
            })
            .await
            .unwrap();

        let replacement = SetLockServicePointerUseCase::new(&repo, &second_clock)
            .execute(SetLockServicePointerRequest {
                creator: creator.clone(),
                default_lock_server: LockServerPubky::from_str(
                    "pubky3kj4afafdba8diu5oxd96dz6orrqt5nfgbmi473go6ju8s64z36y",
                )
                .unwrap(),
            })
            .await
            .unwrap();

        let stored = repo
            .get_lock_service_pointer(&creator)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored, replacement.lock_service_pointer);
        assert_eq!(
            stored.default_lock_server,
            LockServerPubky::from_str("pubky3kj4afafdba8diu5oxd96dz6orrqt5nfgbmi473go6ju8s64z36y")
                .unwrap()
        );
        assert_eq!(stored.created_at, second_clock.now());
    }

    fn creator() -> CreatorPubky {
        CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
    }

    struct FixedClock(OffsetDateTime);

    impl Clock for FixedClock {
        fn now(&self) -> OffsetDateTime {
            self.0
        }
    }
}
