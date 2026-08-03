use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Mutex;

use locks_core::ids::CreatorPubky;
use time::OffsetDateTime;

use crate::config::VerificationSubmissionRateLimitConfig;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct VerificationSubmissionRateLimitKey {
    pub client_address: IpAddr,
    pub creator: CreatorPubky,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateLimitDecision {
    pub allowed: bool,
    pub retry_after_seconds: Option<u64>,
}

#[derive(Debug)]
pub struct InMemoryVerificationSubmissionRateLimiter {
    config: VerificationSubmissionRateLimitConfig,
    windows: Mutex<HashMap<VerificationSubmissionRateLimitKey, WindowCounter>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WindowCounter {
    started_at: OffsetDateTime,
    count: u32,
}

impl InMemoryVerificationSubmissionRateLimiter {
    pub fn new(config: VerificationSubmissionRateLimitConfig) -> Self {
        Self {
            config,
            windows: Mutex::new(HashMap::new()),
        }
    }

    pub fn check(
        &self,
        key: &VerificationSubmissionRateLimitKey,
        now: OffsetDateTime,
    ) -> RateLimitDecision {
        if !self.config.enabled {
            return RateLimitDecision::allowed();
        }

        let mut windows = self.windows.lock().expect("rate limiter mutex poisoned");
        let window = windows.entry(key.clone()).or_insert(WindowCounter {
            started_at: now,
            count: 0,
        });

        if window_has_expired(window.started_at, now, self.config.window_seconds) {
            window.started_at = now;
            window.count = 0;
        }

        if window.count < self.config.max_requests {
            window.count += 1;
            return RateLimitDecision::allowed();
        }

        RateLimitDecision::rejected(retry_after_seconds(
            window.started_at,
            now,
            self.config.window_seconds,
        ))
    }
}

impl RateLimitDecision {
    fn allowed() -> Self {
        Self {
            allowed: true,
            retry_after_seconds: None,
        }
    }

    fn rejected(retry_after_seconds: u64) -> Self {
        Self {
            allowed: false,
            retry_after_seconds: Some(retry_after_seconds),
        }
    }
}

fn window_has_expired(
    started_at: OffsetDateTime,
    now: OffsetDateTime,
    window_seconds: u64,
) -> bool {
    retry_after_seconds(started_at, now, window_seconds) == 0
}

fn retry_after_seconds(
    started_at: OffsetDateTime,
    now: OffsetDateTime,
    window_seconds: u64,
) -> u64 {
    let elapsed = (now - started_at).whole_seconds();
    if elapsed < 0 {
        return window_seconds;
    }
    window_seconds.saturating_sub(elapsed as u64)
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};
    use std::str::FromStr;

    use locks_core::ids::CreatorPubky;
    use time::macros::datetime;

    use super::{InMemoryVerificationSubmissionRateLimiter, VerificationSubmissionRateLimitKey};
    use crate::config::VerificationSubmissionRateLimitConfig;

    #[test]
    fn allows_requests_under_limit() {
        let limiter = limiter(2, 60);
        let key = key(
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            [127, 0, 0, 1],
        );
        let now = datetime!(2026-06-03 12:00:00 UTC);

        let first = limiter.check(&key, now);
        let second = limiter.check(&key, now);

        assert!(first.allowed);
        assert_eq!(first.retry_after_seconds, None);
        assert!(second.allowed);
        assert_eq!(second.retry_after_seconds, None);
    }

    #[test]
    fn rejects_request_after_limit_until_window_resets() {
        let limiter = limiter(2, 60);
        let key = key(
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            [127, 0, 0, 1],
        );
        let now = datetime!(2026-06-03 12:00:00 UTC);

        assert!(limiter.check(&key, now).allowed);
        assert!(limiter.check(&key, now).allowed);
        let rejected = limiter.check(&key, now);

        assert!(!rejected.allowed);
        assert_eq!(rejected.retry_after_seconds, Some(60));

        let reset = limiter.check(&key, now + time::Duration::seconds(60));

        assert!(reset.allowed);
        assert_eq!(reset.retry_after_seconds, None);
    }

    #[test]
    fn disabled_rate_limiter_always_allows() {
        let limiter =
            InMemoryVerificationSubmissionRateLimiter::new(VerificationSubmissionRateLimitConfig {
                enabled: false,
                max_requests: 0,
                window_seconds: 0,
            });
        let key = key(
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            [127, 0, 0, 1],
        );
        let now = datetime!(2026-06-03 12:00:00 UTC);

        for _ in 0..10 {
            let decision = limiter.check(&key, now);
            assert!(decision.allowed);
            assert_eq!(decision.retry_after_seconds, None);
        }
    }

    #[test]
    fn separate_creators_have_separate_windows() {
        let limiter = limiter(1, 60);
        let first_creator = key(
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            [127, 0, 0, 1],
        );
        let second_creator = key(
            "pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky",
            [127, 0, 0, 1],
        );
        let now = datetime!(2026-06-03 12:00:00 UTC);

        assert!(limiter.check(&first_creator, now).allowed);
        assert!(!limiter.check(&first_creator, now).allowed);
        assert!(limiter.check(&second_creator, now).allowed);
    }

    #[test]
    fn separate_client_addresses_have_separate_windows() {
        let limiter = limiter(1, 60);
        let first_client = key(
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            [127, 0, 0, 1],
        );
        let second_client = key(
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            [127, 0, 0, 2],
        );
        let now = datetime!(2026-06-03 12:00:00 UTC);

        assert!(limiter.check(&first_client, now).allowed);
        assert!(!limiter.check(&first_client, now).allowed);
        assert!(limiter.check(&second_client, now).allowed);
    }

    fn limiter(
        max_requests: u32,
        window_seconds: u64,
    ) -> InMemoryVerificationSubmissionRateLimiter {
        InMemoryVerificationSubmissionRateLimiter::new(VerificationSubmissionRateLimitConfig {
            enabled: true,
            max_requests,
            window_seconds,
        })
    }

    fn key(creator: &str, ip_octets: [u8; 4]) -> VerificationSubmissionRateLimitKey {
        VerificationSubmissionRateLimitKey {
            client_address: IpAddr::V4(Ipv4Addr::from(ip_octets)),
            creator: CreatorPubky::from_str(creator).unwrap(),
        }
    }
}
