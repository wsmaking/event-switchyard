use super::feedback::FeedbackEvent;
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedbackDropPolicy {
    DropNewest,
    DropOldest,
}

impl FeedbackDropPolicy {
    fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "drop_oldest" | "oldest" => Self::DropOldest,
            _ => Self::DropNewest,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FeedbackExportConfig {
    pub enabled: bool,
    pub path: PathBuf,
    pub queue_capacity: usize,
    pub drop_policy: FeedbackDropPolicy,
}

impl FeedbackExportConfig {
    pub fn from_env() -> Self {
        let enabled = std::env::var("QUANT_FEEDBACK_ENABLE")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let path = std::env::var("QUANT_FEEDBACK_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("var/gateway/quant_feedback.jsonl"));
        let queue_capacity = std::env::var("QUANT_FEEDBACK_QUEUE_CAPACITY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(4096);
        let drop_policy = std::env::var("QUANT_FEEDBACK_DROP_POLICY")
            .map(|v| FeedbackDropPolicy::parse(&v))
            .unwrap_or(FeedbackDropPolicy::DropNewest);
        Self {
            enabled,
            path,
            queue_capacity,
            drop_policy,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FeedbackExportMetricsSnapshot {
    pub submitted_total: u64,
    pub written_total: u64,
    pub dropped_total: u64,
    pub dropped_newest_total: u64,
    pub dropped_oldest_total: u64,
    pub write_error_total: u64,
    pub queue_depth: u64,
    pub queue_depth_peak: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedbackSubmitOutcome {
    Disabled,
    Enqueued,
    DroppedNewest,
}

#[derive(Default)]
struct FeedbackExportMetrics {
    submitted_total: AtomicU64,
    written_total: AtomicU64,
    dropped_total: AtomicU64,
    dropped_newest_total: AtomicU64,
    dropped_oldest_total: AtomicU64,
    write_error_total: AtomicU64,
    queue_depth: AtomicU64,
    queue_depth_peak: AtomicU64,
}

impl FeedbackExportMetrics {
    fn snapshot(&self) -> FeedbackExportMetricsSnapshot {
        FeedbackExportMetricsSnapshot {
            submitted_total: self.submitted_total.load(Ordering::Relaxed),
            written_total: self.written_total.load(Ordering::Relaxed),
            dropped_total: self.dropped_total.load(Ordering::Relaxed),
            dropped_newest_total: self.dropped_newest_total.load(Ordering::Relaxed),
            dropped_oldest_total: self.dropped_oldest_total.load(Ordering::Relaxed),
            write_error_total: self.write_error_total.load(Ordering::Relaxed),
            queue_depth: self.queue_depth.load(Ordering::Relaxed),
            queue_depth_peak: self.queue_depth_peak.load(Ordering::Relaxed),
        }
    }

    fn set_queue_depth(&self, depth: u64) {
        self.queue_depth.store(depth, Ordering::Relaxed);
        loop {
            let peak = self.queue_depth_peak.load(Ordering::Relaxed);
            if depth <= peak {
                break;
            }
            if self
                .queue_depth_peak
                .compare_exchange(peak, depth, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }
}

struct FeedbackQueueState {
    queue: VecDeque<FeedbackEvent>,
    closed: bool,
}

struct FeedbackQueue {
    state: Mutex<FeedbackQueueState>,
    cv: Condvar,
    capacity: usize,
    drop_policy: FeedbackDropPolicy,
    metrics: Arc<FeedbackExportMetrics>,
}

impl FeedbackQueue {
    fn new(
        capacity: usize,
        drop_policy: FeedbackDropPolicy,
        metrics: Arc<FeedbackExportMetrics>,
    ) -> Self {
        Self {
            state: Mutex::new(FeedbackQueueState {
                queue: VecDeque::with_capacity(capacity.min(1024)),
                closed: false,
            }),
            cv: Condvar::new(),
            capacity,
            drop_policy,
            metrics,
        }
    }

    fn submit(&self, event: FeedbackEvent) -> FeedbackSubmitOutcome {
        self.metrics.submitted_total.fetch_add(1, Ordering::Relaxed);
        let mut guard = self.state.lock().expect("feedback queue lock poisoned");
        if guard.closed {
            self.metrics.dropped_total.fetch_add(1, Ordering::Relaxed);
            self.metrics
                .dropped_newest_total
                .fetch_add(1, Ordering::Relaxed);
            return FeedbackSubmitOutcome::DroppedNewest;
        }
        if guard.queue.len() >= self.capacity {
            match self.drop_policy {
                FeedbackDropPolicy::DropNewest => {
                    self.metrics.dropped_total.fetch_add(1, Ordering::Relaxed);
                    self.metrics
                        .dropped_newest_total
                        .fetch_add(1, Ordering::Relaxed);
                    self.metrics.set_queue_depth(guard.queue.len() as u64);
                    return FeedbackSubmitOutcome::DroppedNewest;
                }
                FeedbackDropPolicy::DropOldest => {
                    let _ = guard.queue.pop_front();
                    self.metrics.dropped_total.fetch_add(1, Ordering::Relaxed);
                    self.metrics
                        .dropped_oldest_total
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        guard.queue.push_back(event);
        self.metrics.set_queue_depth(guard.queue.len() as u64);
        self.cv.notify_one();
        FeedbackSubmitOutcome::Enqueued
    }

    fn recv_blocking(&self) -> Option<FeedbackEvent> {
        let mut guard = self.state.lock().expect("feedback queue lock poisoned");
        loop {
            if let Some(event) = guard.queue.pop_front() {
                self.metrics.set_queue_depth(guard.queue.len() as u64);
                return Some(event);
            }
            if guard.closed {
                self.metrics.set_queue_depth(0);
                return None;
            }
            guard = self.cv.wait(guard).expect("feedback queue wait poisoned");
        }
    }

    fn close(&self) {
        let mut guard = self.state.lock().expect("feedback queue lock poisoned");
        guard.closed = true;
        self.cv.notify_all();
    }
}

pub struct FeedbackExporter {
    queue: Option<Arc<FeedbackQueue>>,
    metrics: Arc<FeedbackExportMetrics>,
    worker: Option<JoinHandle<()>>,
}

impl FeedbackExporter {
    pub fn from_env() -> Self {
        Self::new(FeedbackExportConfig::from_env())
    }

    pub fn disabled() -> Self {
        Self::new(FeedbackExportConfig {
            enabled: false,
            path: PathBuf::new(),
            queue_capacity: 1,
            drop_policy: FeedbackDropPolicy::DropNewest,
        })
    }

    pub fn new(config: FeedbackExportConfig) -> Self {
        let metrics = Arc::new(FeedbackExportMetrics::default());
        if !config.enabled {
            return Self {
                queue: None,
                metrics,
                worker: None,
            };
        }

        let queue = Arc::new(FeedbackQueue::new(
            config.queue_capacity,
            config.drop_policy,
            Arc::clone(&metrics),
        ));
        let worker_queue = Arc::clone(&queue);
        let worker_metrics = Arc::clone(&metrics);
        let worker_path = config.path.clone();
        let worker = thread::Builder::new()
            .name("quant-feedback-export".to_string())
            .spawn(move || writer_loop(worker_queue, worker_metrics, &worker_path))
            .expect("spawn quant feedback worker");

        Self {
            queue: Some(queue),
            metrics,
            worker: Some(worker),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.queue.is_some()
    }

    pub fn submit(&self, event: FeedbackEvent) -> FeedbackSubmitOutcome {
        match &self.queue {
            Some(queue) => queue.submit(event),
            None => FeedbackSubmitOutcome::Disabled,
        }
    }

    pub fn metrics(&self) -> FeedbackExportMetricsSnapshot {
        self.metrics.snapshot()
    }
}

impl Drop for FeedbackExporter {
    fn drop(&mut self) {
        if let Some(queue) = &self.queue {
            queue.close();
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn writer_loop(queue: Arc<FeedbackQueue>, metrics: Arc<FeedbackExportMetrics>, path: &Path) {
    let mut writer: Option<BufWriter<File>> = None;
    while let Some(event) = queue.recv_blocking() {
        let line = match event.to_json_line() {
            Ok(line) => line,
            Err(_) => {
                metrics.write_error_total.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };

        if writer.is_none() {
            match open_append_writer(path) {
                Ok(file) => writer = Some(BufWriter::new(file)),
                Err(_) => {
                    metrics.write_error_total.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            }
        }

        let mut write_ok = false;
        if let Some(active_writer) = writer.as_mut() {
            if active_writer.write_all(&line).is_ok() && active_writer.flush().is_ok() {
                metrics.written_total.fetch_add(1, Ordering::Relaxed);
                write_ok = true;
            }
        }
        if !write_ok {
            metrics.write_error_total.fetch_add(1, Ordering::Relaxed);
            writer = None;
        }
    }
}

fn open_append_writer(path: &Path) -> std::io::Result<File> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    OpenOptions::new().create(true).append(true).open(path)
}

#[cfg(test)]
mod tests {
    use super::{
        FeedbackDropPolicy, FeedbackExportConfig, FeedbackExporter, FeedbackQueue,
        FeedbackSubmitOutcome,
    };
    use crate::strategy::feedback::FeedbackEvent;
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn feedback_fixture(seq: u64) -> FeedbackEvent {
        FeedbackEvent::accepted("sess", seq, "acc", "AAPL", seq)
    }

    fn wait_until(predicate: impl Fn() -> bool) {
        for _ in 0..50 {
            if predicate() {
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!("condition not met");
    }

    #[test]
    fn queue_drop_newest_keeps_existing_events() {
        let metrics = Arc::default();
        let queue = FeedbackQueue::new(2, FeedbackDropPolicy::DropNewest, metrics);

        assert_eq!(
            queue.submit(feedback_fixture(1)),
            FeedbackSubmitOutcome::Enqueued
        );
        assert_eq!(
            queue.submit(feedback_fixture(2)),
            FeedbackSubmitOutcome::Enqueued
        );
        assert_eq!(
            queue.submit(feedback_fixture(3)),
            FeedbackSubmitOutcome::DroppedNewest
        );

        assert_eq!(queue.recv_blocking().expect("first").session_seq, 1);
        assert_eq!(queue.recv_blocking().expect("second").session_seq, 2);
        queue.close();
        assert_eq!(queue.recv_blocking(), None);
    }

    #[test]
    fn queue_drop_oldest_replaces_head() {
        let metrics = Arc::default();
        let queue = FeedbackQueue::new(2, FeedbackDropPolicy::DropOldest, metrics);

        assert_eq!(
            queue.submit(feedback_fixture(1)),
            FeedbackSubmitOutcome::Enqueued
        );
        assert_eq!(
            queue.submit(feedback_fixture(2)),
            FeedbackSubmitOutcome::Enqueued
        );
        assert_eq!(
            queue.submit(feedback_fixture(3)),
            FeedbackSubmitOutcome::Enqueued
        );

        assert_eq!(queue.recv_blocking().expect("first").session_seq, 2);
        assert_eq!(queue.recv_blocking().expect("second").session_seq, 3);
        queue.close();
        assert_eq!(queue.recv_blocking(), None);
    }

    #[test]
    fn exporter_writes_json_lines_and_updates_metrics() {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("quant_feedback_{stamp}.jsonl"));
        let exporter = FeedbackExporter::new(FeedbackExportConfig {
            enabled: true,
            path: path.clone(),
            queue_capacity: 8,
            drop_policy: FeedbackDropPolicy::DropNewest,
        });

        assert_eq!(
            exporter.submit(feedback_fixture(10)),
            FeedbackSubmitOutcome::Enqueued
        );
        assert_eq!(
            exporter.submit(feedback_fixture(11)),
            FeedbackSubmitOutcome::Enqueued
        );

        wait_until(|| exporter.metrics().written_total >= 2);
        let contents = std::fs::read_to_string(&path).expect("read feedback file");
        let lines = contents.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("\"sessionSeq\":10"));
        assert!(lines[1].contains("\"sessionSeq\":11"));

        let metrics = exporter.metrics();
        assert_eq!(metrics.submitted_total, 2);
        assert_eq!(metrics.written_total, 2);
        assert_eq!(metrics.dropped_total, 0);

        let _ = std::fs::remove_file(path);
    }
}
