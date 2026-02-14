//! Per-account rate limiter (token bucket).
//!
//! Kotlin版の SimplePreTradeRisk / TokenBucket と同じ仕様:
//! - RISK_RATE_PER_SEC と RISK_RATE_BURST を利用
//! - account_id 単位のトークンバケット
//! - burst が許容される

use dashmap::DashMap;
use std::sync::Mutex;
use std::time::Instant;

#[derive(Debug, Clone, Copy)]
pub struct RateLimitConfig {
    pub per_sec: u64,
    pub burst: u64,
}

impl RateLimitConfig {
    pub fn from_env() -> Self {
        let per_sec = std::env::var("RISK_RATE_PER_SEC")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        let burst = std::env::var("RISK_RATE_BURST")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(per_sec);
        Self {
            per_sec: if per_sec > 0 { per_sec } else { 0 },
            burst: if burst > 0 { burst } else { 0 },
        }
    }

    pub fn enabled(&self) -> bool {
        self.per_sec > 0 && self.burst > 0
    }
}

pub struct AccountRateLimiter {
    config: RateLimitConfig,
    buckets: DashMap<String, TokenBucket>,
}

impl AccountRateLimiter {
    pub fn from_env() -> Option<Self> {
        let config = RateLimitConfig::from_env();
        if !config.enabled() {
            return None;
        }
        Some(Self {
            config,
            buckets: DashMap::new(),
        })
    }

    pub fn try_acquire(&self, account_id: &str) -> bool {
        if !self.config.enabled() {
            return true;
        }
        let mut entry = self
            .buckets
            .entry(account_id.to_string())
            .or_insert_with(|| TokenBucket::new(self.config.per_sec, self.config.burst));
        entry.try_acquire()
    }
}

struct TokenBucket {
    rate_per_sec: u64,
    burst: u64,
    state: Mutex<BucketState>,
}

struct BucketState {
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(rate_per_sec: u64, burst: u64) -> Self {
        Self {
            rate_per_sec,
            burst,
            state: Mutex::new(BucketState {
                tokens: burst as f64,
                last_refill: Instant::now(),
            }),
        }
    }

    fn try_acquire(&self) -> bool {
        let mut state = self.state.lock().unwrap();
        self.refill_locked(&mut state);
        if state.tokens >= 1.0 {
            state.tokens -= 1.0;
            return true;
        }
        false
    }

    fn refill_locked(&self, state: &mut BucketState) {
        let now = Instant::now();
        let elapsed = now.duration_since(state.last_refill).as_secs_f64();
        if elapsed <= 0.0 {
            return;
        }
        let add = elapsed * self.rate_per_sec as f64;
        state.tokens = (state.tokens + add).min(self.burst as f64);
        state.last_refill = now;
    }
}
