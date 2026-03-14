//! Latency measurement utilities.
//!
//! Uses an HDR histogram for accurate p50/p95/p99/p999 without coarse buckets.

use hdrhistogram::Histogram;
use parking_lot::Mutex;
use std::time::{Duration, Instant};

/// Current time in nanoseconds (monotonic since process start).
#[inline]
pub fn now_nanos() -> u64 {
    static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    let start = START.get_or_init(Instant::now);
    start.elapsed().as_nanos() as u64
}

/// rdtscp stamp (cycle counter + CPU core hint).
#[derive(Debug, Clone, Copy)]
pub struct RdtscpStamp {
    pub cycles: u64,
    pub aux: u32,
}

/// rdtscp-based monotonic clock converter.
#[derive(Debug, Clone)]
pub struct RdtscpClock {
    hz: u64,
    invariant_tsc: bool,
}

impl RdtscpClock {
    /// Detect rdtscp availability and estimate TSC frequency.
    pub fn detect() -> Option<Self> {
        if !rdtscp_supported_internal() {
            return None;
        }
        let hz = detect_tsc_hz().filter(|v| *v > 0)?;
        Some(Self {
            hz,
            invariant_tsc: invariant_tsc_supported_internal(),
        })
    }

    #[inline]
    pub fn hz(&self) -> u64 {
        self.hz
    }

    #[inline]
    pub fn invariant_tsc(&self) -> bool {
        self.invariant_tsc
    }

    #[inline]
    pub fn now(&self) -> Option<RdtscpStamp> {
        rdtscp_read_internal()
    }

    #[inline]
    pub fn same_core(&self, a: RdtscpStamp, b: RdtscpStamp) -> bool {
        a.aux == b.aux
    }

    #[inline]
    pub fn elapsed_ns(&self, start: RdtscpStamp, end: RdtscpStamp) -> Option<u64> {
        if end.cycles < start.cycles || self.hz == 0 {
            return None;
        }
        Some(cycles_to_ns(end.cycles - start.cycles, self.hz))
    }
}

/// Whether rdtscp is available on this machine.
#[inline]
pub fn rdtscp_supported() -> bool {
    rdtscp_supported_internal()
}

#[inline]
fn cycles_to_ns(delta_cycles: u64, hz: u64) -> u64 {
    (((delta_cycles as u128).saturating_mul(1_000_000_000u128)) / hz as u128) as u64
}

fn detect_tsc_hz() -> Option<u64> {
    tsc_hz_from_cpuid().or_else(calibrate_tsc_hz)
}

#[cfg(target_arch = "x86_64")]
fn rdtscp_supported_internal() -> bool {
    use std::arch::x86_64::__cpuid;
    // 0x80000001 EDX bit 27 => RDTSCP
    let ext = unsafe { __cpuid(0x8000_0000) };
    if ext.eax < 0x8000_0001 {
        return false;
    }
    let leaf = unsafe { __cpuid(0x8000_0001) };
    (leaf.edx & (1 << 27)) != 0
}

#[cfg(not(target_arch = "x86_64"))]
fn rdtscp_supported_internal() -> bool {
    false
}

#[cfg(target_arch = "x86_64")]
fn invariant_tsc_supported_internal() -> bool {
    use std::arch::x86_64::__cpuid;
    // 0x80000007 EDX bit 8 => Invariant TSC
    let ext = unsafe { __cpuid(0x8000_0000) };
    if ext.eax < 0x8000_0007 {
        return false;
    }
    let leaf = unsafe { __cpuid(0x8000_0007) };
    (leaf.edx & (1 << 8)) != 0
}

#[cfg(not(target_arch = "x86_64"))]
fn invariant_tsc_supported_internal() -> bool {
    false
}

#[cfg(target_arch = "x86_64")]
fn rdtscp_read_internal() -> Option<RdtscpStamp> {
    if !rdtscp_supported_internal() {
        return None;
    }
    use std::arch::x86_64::_rdtscp;
    let mut aux = 0u32;
    let cycles = unsafe { _rdtscp(&mut aux) };
    Some(RdtscpStamp {
        cycles: cycles as u64,
        aux,
    })
}

#[cfg(not(target_arch = "x86_64"))]
fn rdtscp_read_internal() -> Option<RdtscpStamp> {
    None
}

#[cfg(target_arch = "x86_64")]
fn tsc_hz_from_cpuid() -> Option<u64> {
    use std::arch::x86_64::__cpuid;
    let max_basic = unsafe { __cpuid(0) }.eax;
    if max_basic >= 0x15 {
        let leaf = unsafe { __cpuid(0x15) };
        if leaf.eax > 0 && leaf.ebx > 0 && leaf.ecx > 0 {
            let numer = (leaf.ebx as u128).saturating_mul(leaf.ecx as u128);
            let denom = leaf.eax as u128;
            let hz = (numer / denom) as u64;
            if hz > 0 {
                return Some(hz);
            }
        }
    }
    if max_basic >= 0x16 {
        let leaf = unsafe { __cpuid(0x16) };
        if leaf.eax > 0 {
            return Some((leaf.eax as u64).saturating_mul(1_000_000));
        }
    }
    None
}

#[cfg(not(target_arch = "x86_64"))]
fn tsc_hz_from_cpuid() -> Option<u64> {
    None
}

fn calibrate_tsc_hz() -> Option<u64> {
    let start = rdtscp_read_internal()?;
    let t0 = Instant::now();
    std::thread::sleep(Duration::from_millis(25));
    let end = rdtscp_read_internal()?;
    if end.cycles <= start.cycles {
        return None;
    }
    let elapsed_ns = t0.elapsed().as_nanos() as u64;
    if elapsed_ns == 0 {
        return None;
    }
    let delta_cycles = end.cycles - start.cycles;
    let hz = ((delta_cycles as u128).saturating_mul(1_000_000_000u128) / elapsed_ns as u128) as u64;
    (hz > 0).then_some(hz)
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
