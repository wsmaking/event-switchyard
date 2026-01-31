//! Backpressure 判定
//!
//! 入口で早期に制限をかけるための簡易判定ロジック。

#[derive(Debug, Clone, Copy)]
pub struct BackpressureConfig {
    pub inflight_max: Option<u64>,
    pub soft_wal_age_ms_max: Option<u64>,
    pub wal_bytes_max: Option<u64>,
    pub wal_age_ms_max: Option<u64>,
    pub disk_free_pct_min: Option<f64>,
}

impl BackpressureConfig {
    pub fn from_env() -> Self {
        Self {
            inflight_max: parse_u64_env("BACKPRESSURE_INFLIGHT_MAX"),
            soft_wal_age_ms_max: parse_u64_env("BACKPRESSURE_SOFT_WAL_AGE_MS_MAX"),
            wal_bytes_max: parse_u64_env("BACKPRESSURE_WAL_BYTES_MAX"),
            wal_age_ms_max: parse_u64_env("BACKPRESSURE_WAL_AGE_MS_MAX"),
            disk_free_pct_min: parse_f64_env("BACKPRESSURE_DISK_FREE_PCT_MIN"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BackpressureMetrics {
    pub inflight: u64,
    pub wal_bytes: u64,
    pub wal_age_ms: u64,
    pub disk_free_pct: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureReason {
    Inflight,
    SoftWalAge,
    WalBytes,
    WalAge,
    DiskFree,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureLevel {
    Soft,
    Hard,
}

#[derive(Debug, Clone, Copy)]
pub struct BackpressureDecision {
    pub reason: BackpressureReason,
    pub level: BackpressureLevel,
}

pub fn evaluate(
    config: &BackpressureConfig,
    metrics: &BackpressureMetrics,
) -> Option<BackpressureDecision> {
    if let Some(max) = config.inflight_max {
        if metrics.inflight > max {
            return Some(BackpressureDecision {
                reason: BackpressureReason::Inflight,
                level: BackpressureLevel::Hard,
            });
        }
    }
    if let Some(max) = config.wal_bytes_max {
        if metrics.wal_bytes > max {
            return Some(BackpressureDecision {
                reason: BackpressureReason::WalBytes,
                level: BackpressureLevel::Hard,
            });
        }
    }
    if let Some(max) = config.wal_age_ms_max {
        if metrics.wal_age_ms > max {
            return Some(BackpressureDecision {
                reason: BackpressureReason::WalAge,
                level: BackpressureLevel::Hard,
            });
        }
    }
    if let Some(min_pct) = config.disk_free_pct_min {
        if let Some(pct) = metrics.disk_free_pct {
            if pct < min_pct {
                return Some(BackpressureDecision {
                    reason: BackpressureReason::DiskFree,
                    level: BackpressureLevel::Hard,
                });
            }
        }
    }
    if let Some(max) = config.soft_wal_age_ms_max {
        if metrics.wal_age_ms > max {
            return Some(BackpressureDecision {
                reason: BackpressureReason::SoftWalAge,
                level: BackpressureLevel::Soft,
            });
        }
    }
    None
}

fn parse_u64_env(key: &str) -> Option<u64> {
    std::env::var(key).ok().and_then(|v| v.parse::<u64>().ok())
}

fn parse_f64_env(key: &str) -> Option<f64> {
    std::env::var(key).ok().and_then(|v| v.parse::<f64>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backpressure_inflight() {
        let cfg = BackpressureConfig {
            inflight_max: Some(10),
            soft_wal_age_ms_max: None,
            wal_bytes_max: None,
            wal_age_ms_max: None,
            disk_free_pct_min: None,
        };
        let metrics = BackpressureMetrics {
            inflight: 11,
            wal_bytes: 0,
            wal_age_ms: 0,
            disk_free_pct: None,
        };
        let decision = evaluate(&cfg, &metrics).unwrap();
        assert_eq!(decision.reason, BackpressureReason::Inflight);
    }

    #[test]
    fn backpressure_wal_bytes() {
        let cfg = BackpressureConfig {
            inflight_max: None,
            soft_wal_age_ms_max: None,
            wal_bytes_max: Some(100),
            wal_age_ms_max: None,
            disk_free_pct_min: None,
        };
        let metrics = BackpressureMetrics {
            inflight: 0,
            wal_bytes: 101,
            wal_age_ms: 0,
            disk_free_pct: None,
        };
        let decision = evaluate(&cfg, &metrics).unwrap();
        assert_eq!(decision.reason, BackpressureReason::WalBytes);
    }
}
