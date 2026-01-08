//! Gateway Core - Ultra-low latency order processing in Rust
//!
//! This library provides the hot-path components for order processing:
//! - Lock-free ring buffer queue (queue.rs)
//! - O(1) risk checks (risk.rs)
//! - Nanosecond precision latency measurement (metrics.rs)
//!
//! Target: p99 < 1μs for queue + risk check combined

pub mod metrics;
pub mod queue;
pub mod risk;

pub use metrics::{now_nanos, LatencyGuard, LatencyHistogram, LatencyStats};
pub use queue::{FastPathQueue, Order};
pub use risk::{AccountPosition, RiskChecker, RiskResult, SymbolLimits};

/// Process an order through the fast path
/// Returns the risk check result and elapsed nanoseconds
#[inline]
pub fn process_order(
    order: &Order,
    checker: &RiskChecker,
    histogram: &LatencyHistogram,
) -> RiskResult {
    let start = now_nanos();
    let result = checker.check_simple(order);
    let elapsed = now_nanos() - start;
    histogram.record(elapsed);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fast_path_integration() {
        let queue = FastPathQueue::new(1024);
        let checker = RiskChecker::new();
        let histogram = LatencyHistogram::new();

        // Create and enqueue order
        let order = Order::new(1, 100, *b"AAPL\0\0\0\0", 1, 100, 15000, now_nanos());
        queue.push(order).unwrap();

        // Dequeue and process
        let dequeued = queue.pop().unwrap();
        let result = process_order(&dequeued, &checker, &histogram);

        assert_eq!(result, RiskResult::Accepted);

        let stats = histogram.snapshot();
        assert_eq!(stats.count, 1);
        println!("Fast path latency: {}", stats.summary());
    }

    #[test]
    fn test_high_throughput() {
        let queue = FastPathQueue::new(65536);
        let checker = RiskChecker::new();
        let histogram = LatencyHistogram::new();

        // Process 100k orders
        for i in 0..100_000u64 {
            let order = Order::new(i, 100, *b"AAPL\0\0\0\0", 1, 100, 15000, now_nanos());
            queue.push(order).unwrap();

            let dequeued = queue.pop().unwrap();
            process_order(&dequeued, &checker, &histogram);
        }

        let stats = histogram.snapshot();
        assert_eq!(stats.count, 100_000);
        println!(
            "100k orders processed - p50: {}ns, p99: {}ns, max: {}ns",
            stats.percentile(50.0),
            stats.percentile(99.0),
            stats.max_nanos
        );
    }
}
