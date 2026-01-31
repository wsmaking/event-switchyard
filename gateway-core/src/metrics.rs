//! Latency measurement utilities.
//!
//! Uses an HDR histogram for accurate p50/p95/p99/p999 without coarse buckets.

use hdrhistogram::Histogram;
use parking_lot::Mutex;
use std::time::Instant;

/// Current time in nanoseconds (monotonic since process start).
#[inline]
pub fn now_nanos() -> u64 {
    static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    let start = START.get_or_init(Instant::now);
    start.elapsed().as_nanos() as u64
}

/// Latency histogram backed by HDR histogram.
#[derive(Debug)]
pub struct LatencyHistogram {
    hist: Mutex<Histogram<u64>>,
}

impl LatencyHistogram {
    pub fn new() -> Self {
        let hist = Histogram::new_with_bounds(1, 60_000_000_000, 3)
            .unwrap_or_else(|_| Histogram::new(3).expect("hdr histogram"));
        Self {
            hist: Mutex::new(hist),
        }
    }

    /// Record a latency value in nanoseconds.
    #[inline]
    pub fn record(&self, latency_nanos: u64) {
        let value = latency_nanos.max(1);
        let mut hist = self.hist.lock();
        let _ = hist.record(value);
    }

    /// Snapshot current stats.
    pub fn snapshot(&self) -> LatencyStats {
        let hist = self.hist.lock();
        let count = hist.len();
        if count == 0 {
            return LatencyStats {
                count: 0,
                min_nanos: 0,
                max_nanos: 0,
                mean_nanos: 0,
                p50_nanos: 0,
                p95_nanos: 0,
                p99_nanos: 0,
                p999_nanos: 0,
            };
        }
        LatencyStats {
            count,
            min_nanos: hist.min(),
            max_nanos: hist.max(),
            mean_nanos: hist.mean().round() as u64,
            p50_nanos: hist.value_at_quantile(0.50),
            p95_nanos: hist.value_at_quantile(0.95),
            p99_nanos: hist.value_at_quantile(0.99),
            p999_nanos: hist.value_at_quantile(0.999),
        }
    }

    /// Reset all samples.
    pub fn reset(&self) {
        let mut hist = self.hist.lock();
        hist.reset();
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of latency stats.
#[derive(Debug, Clone)]
pub struct LatencyStats {
    pub count: u64,
    pub min_nanos: u64,
    pub max_nanos: u64,
    pub mean_nanos: u64,
    pub p50_nanos: u64,
    pub p95_nanos: u64,
    pub p99_nanos: u64,
    pub p999_nanos: u64,
}

impl LatencyStats {
    /// Percentile lookup for common p50/p95/p99/p999 values.
    pub fn percentile(&self, p: f64) -> u64 {
        if self.count == 0 {
            return 0;
        }
        if (p - 50.0).abs() < 0.0001 {
            return self.p50_nanos;
        }
        if (p - 95.0).abs() < 0.0001 {
            return self.p95_nanos;
        }
        if (p - 99.0).abs() < 0.0001 {
            return self.p99_nanos;
        }
        if (p - 99.9).abs() < 0.0001 {
            return self.p999_nanos;
        }
        self.p99_nanos
    }

    /// Human-readable summary.
    pub fn summary(&self) -> String {
        format!(
            "count={}, min={}ns, mean={}ns, max={}ns, p50={}ns, p99={}ns",
            self.count,
            self.min_nanos,
            self.mean_nanos,
            self.max_nanos,
            self.p50_nanos,
            self.p99_nanos
        )
    }
}

/// RAII guard for latency measurement.
pub struct LatencyGuard<'a> {
    start: u64,
    histogram: &'a LatencyHistogram,
}

impl<'a> LatencyGuard<'a> {
    pub fn new(histogram: &'a LatencyHistogram) -> Self {
        Self {
            start: now_nanos(),
            histogram,
        }
    }
}

impl<'a> Drop for LatencyGuard<'a> {
    fn drop(&mut self) {
        let elapsed = now_nanos().saturating_sub(self.start);
        self.histogram.record(elapsed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_records() {
        let hist = LatencyHistogram::new();
        hist.record(100);
        hist.record(200);
        let stats = hist.snapshot();
        assert_eq!(stats.count, 2);
        assert!(stats.p50_nanos >= 100);
    }
}
