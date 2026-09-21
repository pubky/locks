use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::Instant;

use locks_core::ids::{BundleId, CreatorPubky};
use time::OffsetDateTime;

use crate::config::{
    PaykitConnectionStateLookupRateLimitConfig, VerificationSubmissionRateLimitConfig,
};

#[derive(Debug)]
struct TokenBucketState {
    tokens: u64,
    remainder: u128,
    last: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PaykitConnectionStateLookupRateLimitKey {
    pub client_address: IpAddr,
    pub creator: CreatorPubky,
    pub bundle_id: BundleId,
}

#[derive(Debug)]
pub struct InMemoryPaykitConnectionStateLookupRateLimiter {
    config: PaykitConnectionStateLookupRateLimitConfig,
    state: Mutex<PaykitConnectionStateLookupRateLimitState>,
}

#[derive(Debug)]
struct PaykitConnectionStateLookupRateLimitState {
    windows: HashMap<PaykitConnectionStateLookupRateLimitKey, WindowCounter>,
    global: TokenBucketState,
}

impl InMemoryPaykitConnectionStateLookupRateLimiter {
    pub fn new(config: PaykitConnectionStateLookupRateLimitConfig) -> Self {
        Self::new_at(config, Instant::now())
    }

    fn new_at(config: PaykitConnectionStateLookupRateLimitConfig, now: Instant) -> Self {
        Self {
            state: Mutex::new(PaykitConnectionStateLookupRateLimitState {
                windows: HashMap::new(),
                global: TokenBucketState {
                    tokens: config.global_burst,
                    remainder: 0,
                    last: now,
                },
            }),
            config,
        }
    }

    pub fn check(
        &self,
        key: &PaykitConnectionStateLookupRateLimitKey,
        wall_now: OffsetDateTime,
        monotonic_now: Instant,
    ) -> RateLimitDecision {
        let mut state = self.state.lock().expect("rate limiter mutex poisoned");
        if !state.windows.contains_key(key) && state.windows.len() >= self.config.max_entries {
            state.windows.retain(|_, window| {
                !window_has_expired(window.started_at, wall_now, self.config.window_seconds)
            });
            if state.windows.len() >= self.config.max_entries {
                let retry_after = state
                    .windows
                    .values()
                    .map(|window| {
                        retry_after_seconds(window.started_at, wall_now, self.config.window_seconds)
                    })
                    .min()
                    .unwrap_or(self.config.window_seconds);
                return RateLimitDecision::rejected(retry_after);
            }
        }

        match state.windows.get(key) {
            Some(window)
                if !window_has_expired(window.started_at, wall_now, self.config.window_seconds)
                    && window.count >= self.config.max_requests =>
            {
                return RateLimitDecision::rejected(retry_after_seconds(
                    window.started_at,
                    wall_now,
                    self.config.window_seconds,
                ));
            }
            _ => {}
        }

        refill_token_bucket(
            &mut state.global,
            monotonic_now,
            self.config.global_requests_per_second,
            self.config.global_burst,
        );
        if state.global.tokens == 0 {
            return RateLimitDecision::rejected(1);
        }

        state.global.tokens -= 1;
        let window = state.windows.entry(key.clone()).or_insert(WindowCounter {
            started_at: wall_now,
            count: 0,
        });
        if window_has_expired(window.started_at, wall_now, self.config.window_seconds) {
            window.started_at = wall_now;
            window.count = 0;
        }
        window.count += 1;
        RateLimitDecision::allowed()
    }
}

fn refill_token_bucket(
    state: &mut TokenBucketState,
    now: Instant,
    rate_per_second: u64,
    burst: u64,
) {
    let elapsed_nanos = now.saturating_duration_since(state.last).as_nanos();
    state.last = now;
    if state.tokens == burst {
        state.remainder = 0;
        return;
    }

    let accrued = elapsed_nanos
        .saturating_mul(u128::from(rate_per_second))
        .saturating_add(state.remainder);
    let added = accrued / 1_000_000_000;
    state.remainder = accrued % 1_000_000_000;
    state.tokens = state
        .tokens
        .saturating_add(u64::try_from(added).unwrap_or(u64::MAX))
        .min(burst);
    if state.tokens == burst {
        state.remainder = 0;
    }
}

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
    use std::time::{Duration, Instant};

    use locks_core::ids::{BundleId, CreatorPubky};
    use time::macros::datetime;

    use super::{
        InMemoryPaykitConnectionStateLookupRateLimiter, InMemoryVerificationSubmissionRateLimiter,
        PaykitConnectionStateLookupRateLimitKey, VerificationSubmissionRateLimitKey,
    };
    use crate::config::{
        PaykitConnectionStateLookupRateLimitConfig, VerificationSubmissionRateLimitConfig,
    };

    #[test]
    fn global_rejection_does_not_consume_fixed_window_budget() {
        let monotonic_start = Instant::now();
        let limiter = InMemoryPaykitConnectionStateLookupRateLimiter::new_at(
            PaykitConnectionStateLookupRateLimitConfig {
                max_requests: 1,
                window_seconds: 60,
                max_in_flight: 16,
                max_entries: 10_000,
                global_requests_per_second: 1,
                global_burst: 1,
            },
            monotonic_start,
        );
        let wall_now = datetime!(2026-06-03 12:00:00 UTC);
        let first = paykit_key(
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            "000G40R40M30E209185GR38E1W",
            [127, 0, 0, 1],
        );
        let second = paykit_key(
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            "000G50R40M30E209185GR38E1W",
            [127, 0, 0, 1],
        );

        assert!(limiter.check(&first, wall_now, monotonic_start).allowed);
        assert_eq!(
            limiter
                .check(&second, wall_now, monotonic_start)
                .retry_after_seconds,
            Some(1)
        );
        assert!(
            limiter
                .check(
                    &second,
                    wall_now + time::Duration::seconds(1),
                    monotonic_start + Duration::from_secs(1),
                )
                .allowed
        );
    }

    #[test]
    fn paykit_lookup_limiter_is_independent_per_ip_creator_and_bundle() {
        let monotonic_now = Instant::now();
        let limiter = InMemoryPaykitConnectionStateLookupRateLimiter::new(
            PaykitConnectionStateLookupRateLimitConfig {
                max_requests: 1,
                window_seconds: 60,
                max_in_flight: 16,
                max_entries: 10_000,
                global_requests_per_second: 50,
                global_burst: 50,
            },
        );
        let now = datetime!(2026-06-03 12:00:00 UTC);
        let first = paykit_key(
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            "000G40R40M30E209185GR38E1W",
            [127, 0, 0, 1],
        );
        let other_bundle = paykit_key(
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            "000G50R40M30E209185GR38E1W",
            [127, 0, 0, 1],
        );

        assert!(limiter.check(&first, now, monotonic_now).allowed);
        let rejected = limiter.check(&first, now, monotonic_now);
        assert!(!rejected.allowed);
        assert_eq!(rejected.retry_after_seconds, Some(60));
        assert!(limiter.check(&other_bundle, now, monotonic_now).allowed);
    }

    #[test]
    fn paykit_lookup_limiter_rejects_new_keys_at_entry_cap() {
        let monotonic_now = Instant::now();
        let limiter = InMemoryPaykitConnectionStateLookupRateLimiter::new(
            PaykitConnectionStateLookupRateLimitConfig {
                max_requests: 60,
                window_seconds: 60,
                max_in_flight: 16,
                max_entries: 1,
                global_requests_per_second: 50,
                global_burst: 50,
            },
        );
        let now = datetime!(2026-06-03 12:00:00 UTC);
        let first = paykit_key(
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            "000G40R40M30E209185GR38E1W",
            [127, 0, 0, 1],
        );
        let second = paykit_key(
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            "000G50R40M30E209185GR38E1W",
            [127, 0, 0, 1],
        );

        assert!(limiter.check(&first, now, monotonic_now).allowed);
        let rejected = limiter.check(&second, now, monotonic_now);
        assert!(!rejected.allowed);
        assert_eq!(rejected.retry_after_seconds, Some(60));
    }

    #[test]
    fn paykit_lookup_limiter_evicts_expired_keys_before_applying_entry_cap() {
        let monotonic_now = Instant::now();
        let limiter = InMemoryPaykitConnectionStateLookupRateLimiter::new(
            PaykitConnectionStateLookupRateLimitConfig {
                max_requests: 60,
                window_seconds: 60,
                max_in_flight: 16,
                max_entries: 1,
                global_requests_per_second: 50,
                global_burst: 50,
            },
        );
        let now = datetime!(2026-06-03 12:00:00 UTC);
        let expired = paykit_key(
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            "000G40R40M30E209185GR38E1W",
            [127, 0, 0, 1],
        );
        let replacement = paykit_key(
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            "000G50R40M30E209185GR38E1W",
            [127, 0, 0, 1],
        );

        assert!(limiter.check(&expired, now, monotonic_now).allowed);
        assert!(
            limiter
                .check(
                    &replacement,
                    now + time::Duration::seconds(60),
                    monotonic_now + Duration::from_secs(60),
                )
                .allowed
        );
    }

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

    fn paykit_key(
        creator: &str,
        bundle_id: &str,
        ip_octets: [u8; 4],
    ) -> PaykitConnectionStateLookupRateLimitKey {
        PaykitConnectionStateLookupRateLimitKey {
            client_address: IpAddr::V4(Ipv4Addr::from(ip_octets)),
            creator: CreatorPubky::from_str(creator).unwrap(),
            bundle_id: BundleId::from_str(bundle_id).unwrap(),
        }
    }
}
