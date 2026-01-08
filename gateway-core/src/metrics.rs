//! Ultra-low overhead latency measurement
//! Uses rdtsc/rdtscp on x86_64, mach_absolute_time on Apple Silicon

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Get current timestamp in nanoseconds
/// Uses the highest resolution timer available on the platform
#[inline]
pub fn now_nanos() -> u64 {
    // On most platforms, Instant uses the best available high-resolution timer
    // This is a baseline implementation - can be optimized per-platform
    static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    let start = START.get_or_init(Instant::now);
    start.elapsed().as_nanos() as u64
}

/// Histogram bucket for latency distribution
/// Uses fixed buckets: 0-100ns, 100-500ns, 500ns-1μs, 1-5μs, 5-10μs, 10-50μs, 50-100μs, 100μs+
#[derive(Debug)]
pub struct LatencyHistogram {
    buckets: [AtomicU64; 8],
    sum_nanos: AtomicU64,
    count: AtomicU64,
    min_nanos: AtomicU64,
    max_nanos: AtomicU64,
}

impl LatencyHistogram {
    pub const BUCKET_BOUNDS: [u64; 8] = [
        100,      // 0-100ns
        500,      // 100-500ns
        1_000,    // 500ns-1μs
        5_000,    // 1-5μs
        10_000,   // 5-10μs
        50_000,   // 10-50μs
        100_000,  // 50-100μs
        u64::MAX, // 100μs+
    ];

    pub fn new() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            sum_nanos: AtomicU64::new(0),
            count: AtomicU64::new(0),
            min_nanos: AtomicU64::new(u64::MAX),
            max_nanos: AtomicU64::new(0),
        }
    }

    /// Record a latency measurement
    #[inline]
    pub fn record(&self, latency_nanos: u64) {
        // Find bucket
        let bucket_idx = Self::BUCKET_BOUNDS
            .iter()
            .position(|&bound| latency_nanos <= bound)
            .unwrap_or(7);

        self.buckets[bucket_idx].fetch_add(1, Ordering::Relaxed);
        self.sum_nanos.fetch_add(latency_nanos, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);

        // Update min (CAS loop)
        let mut current_min = self.min_nanos.load(Ordering::Relaxed);
        while latency_nanos < current_min {
            match self.min_nanos.compare_exchange_weak(
                current_min,
                latency_nanos,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => current_min = x,
            }
        }

        // Update max (CAS loop)
        let mut current_max = self.max_nanos.load(Ordering::Relaxed);
        while latency_nanos > current_max {
            match self.max_nanos.compare_exchange_weak(
                current_max,
                latency_nanos,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => current_max = x,
            }
        }
    }

    /// Get statistics snapshot
    pub fn snapshot(&self) -> LatencyStats {
        let count = self.count.load(Ordering::Relaxed);
        let sum = self.sum_nanos.load(Ordering::Relaxed);
        let min = self.min_nanos.load(Ordering::Relaxed);
        let max = self.max_nanos.load(Ordering::Relaxed);

        let buckets: [u64; 8] = std::array::from_fn(|i| self.buckets[i].load(Ordering::Relaxed));

        LatencyStats {
            count,
            sum_nanos: sum,
            min_nanos: if min == u64::MAX { 0 } else { min },
            max_nanos: max,
            mean_nanos: if count > 0 { sum / count } else { 0 },
            buckets,
        }
    }

    /// Reset all counters
    pub fn reset(&self) {
        for bucket in &self.buckets {
            bucket.store(0, Ordering::Relaxed);
        }
        self.sum_nanos.store(0, Ordering::Relaxed);
        self.count.store(0, Ordering::Relaxed);
        self.min_nanos.store(u64::MAX, Ordering::Relaxed);
        self.max_nanos.store(0, Ordering::Relaxed);
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of latency statistics
#[derive(Debug, Clone)]
pub struct LatencyStats {
    pub count: u64,
    pub sum_nanos: u64,
    pub min_nanos: u64,
    pub max_nanos: u64,
    pub mean_nanos: u64,
    pub buckets: [u64; 8],
}

impl LatencyStats {
    /// Estimate percentile from histogram buckets
    /// Returns the upper bound of the bucket containing the percentile
    pub fn percentile(&self, p: f64) -> u64 {
        if self.count == 0 {
            return 0;
        }

        let target = (self.count as f64 * p / 100.0).ceil() as u64;
        let mut cumulative = 0u64;

        for (i, &bucket_count) in self.buckets.iter().enumerate() {
            cumulative += bucket_count;
            if cumulative >= target {
                return LatencyHistogram::BUCKET_BOUNDS[i];
            }
        }

        LatencyHistogram::BUCKET_BOUNDS[7]
    }

    /// Format as human-readable string
    pub fn summary(&self) -> String {
        format!(
            "count={}, min={}ns, mean={}ns, max={}ns, p50={}ns, p99={}ns",
            self.count,
            self.min_nanos,
            self.mean_nanos,
            self.max_nanos,
            self.percentile(50.0),
            self.percentile(99.0)
        )
    }
}

/// RAII guard for measuring elapsed time
pub struct LatencyGuard<'a> {
    histogram: &'a LatencyHistogram,
    start: u64,
}

impl<'a> LatencyGuard<'a> {
    #[inline]
    pub fn new(histogram: &'a LatencyHistogram) -> Self {
        Self {
            histogram,
            start: now_nanos(),
        }
    }
}

impl Drop for LatencyGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        let elapsed = now_nanos() - self.start;
        self.histogram.record(elapsed);
    }
}

/// Macro for timing a block of code
#[macro_export]
macro_rules! time_block {
    ($histogram:expr, $block:expr) => {{
        let _guard = $crate::metrics::LatencyGuard::new($histogram);
        $block
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_histogram_basic() {
        let hist = LatencyHistogram::new();

        hist.record(50);   // bucket 0 (0-100ns)
        hist.record(200);  // bucket 1 (100-500ns)
        hist.record(800);  // bucket 2 (500ns-1μs)
        hist.record(3000); // bucket 3 (1-5μs)

        let stats = hist.snapshot();
        assert_eq!(stats.count, 4);
        assert_eq!(stats.min_nanos, 50);
        assert_eq!(stats.max_nanos, 3000);
    }

    #[test]
    fn test_percentile() {
        let hist = LatencyHistogram::new();

        // Add 100 samples in first bucket
        for _ in 0..99 {
            hist.record(50);
        }
        // Add 1 sample in last bucket
        hist.record(200_000);

        let stats = hist.snapshot();
        assert_eq!(stats.percentile(50.0), 100);  // p50 in first bucket
        assert_eq!(stats.percentile(99.0), 100);  // p99 in first bucket
        assert_eq!(stats.percentile(100.0), u64::MAX); // p100 in last bucket
    }

    #[test]
    fn test_now_nanos_monotonic() {
        let t1 = now_nanos();
        let t2 = now_nanos();
        assert!(t2 >= t1);
    }

    #[test]
    fn test_latency_guard() {
        let hist = LatencyHistogram::new();
        {
            let _guard = LatencyGuard::new(&hist);
            // Do some work
            std::hint::black_box(42);
        }
        let stats = hist.snapshot();
        assert_eq!(stats.count, 1);
        assert!(stats.min_nanos > 0);
    }
}
