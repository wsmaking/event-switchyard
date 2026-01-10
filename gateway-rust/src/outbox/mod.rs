//! Outbox worker (tail audit log and publish to bus).
//!
//! Minimal at-least-once delivery for bus events.

use crate::audit::AuditEvent;
use crate::bus::{BusEvent, BusPublisher};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

static OUTBOX_LINES_READ: AtomicU64 = AtomicU64::new(0);
static OUTBOX_EVENTS_PUBLISHED: AtomicU64 = AtomicU64::new(0);
static OUTBOX_EVENTS_SKIPPED: AtomicU64 = AtomicU64::new(0);
static OUTBOX_READ_ERRORS: AtomicU64 = AtomicU64::new(0);
static OUTBOX_PUBLISH_ERRORS: AtomicU64 = AtomicU64::new(0);
static OUTBOX_OFFSET_RESETS: AtomicU64 = AtomicU64::new(0);

pub struct OutboxMetrics {
    pub lines_read: u64,
    pub events_published: u64,
    pub events_skipped: u64,
    pub read_errors: u64,
    pub publish_errors: u64,
    pub offset_resets: u64,
}

pub fn metrics() -> OutboxMetrics {
    OutboxMetrics {
        lines_read: OUTBOX_LINES_READ.load(Ordering::Relaxed),
        events_published: OUTBOX_EVENTS_PUBLISHED.load(Ordering::Relaxed),
        events_skipped: OUTBOX_EVENTS_SKIPPED.load(Ordering::Relaxed),
        read_errors: OUTBOX_READ_ERRORS.load(Ordering::Relaxed),
        publish_errors: OUTBOX_PUBLISH_ERRORS.load(Ordering::Relaxed),
        offset_resets: OUTBOX_OFFSET_RESETS.load(Ordering::Relaxed),
    }
}

pub struct OutboxWorker {
    offset_path: PathBuf,
    audit_path: PathBuf,
    bus: Arc<BusPublisher>,
}

impl OutboxWorker {
    pub fn new(audit_path: impl AsRef<Path>, bus: Arc<BusPublisher>, offset_path: impl AsRef<Path>) -> Self {
        Self {
            offset_path: offset_path.as_ref().to_path_buf(),
            audit_path: audit_path.as_ref().to_path_buf(),
            bus,
        }
    }

    pub fn start(self) {
        thread::spawn(move || self.run());
    }

    fn run(self) {
        let mut offset = read_offset(&self.offset_path);
        let base_sleep_ms = 200u64;
        let max_sleep_ms = 5000u64;
        let mut backoff_ms = base_sleep_ms;
        loop {
            let file = match File::open(&self.audit_path) {
                Ok(f) => f,
                Err(_) => {
                    backoff_ms = (backoff_ms * 2).min(max_sleep_ms);
                    thread::sleep(Duration::from_millis(backoff_ms));
                    continue;
                }
            };
            if let Ok(meta) = file.metadata() {
                if offset > meta.len() {
                    eprintln!(
                        "outbox: offset {} exceeds audit size {}, resetting to 0",
                        offset,
                        meta.len()
                    );
                    offset = 0;
                    OUTBOX_OFFSET_RESETS.fetch_add(1, Ordering::Relaxed);
                }
            }
            let mut reader = BufReader::new(file);
            if reader.seek(SeekFrom::Start(offset)).is_err() {
                offset = 0;
                let _ = reader.seek(SeekFrom::Start(0));
                OUTBOX_OFFSET_RESETS.fetch_add(1, Ordering::Relaxed);
            }

            let mut progressed = false;
            let mut had_error = false;
            let mut line = String::new();
            loop {
                line.clear();
                let prev_offset = offset;
                match reader.read_line(&mut line) {
                    Ok(0) => break,
                    Ok(n) => {
                        OUTBOX_LINES_READ.fetch_add(1, Ordering::Relaxed);
                        let next_offset = prev_offset + n as u64;
                        if let Ok(event) = serde_json::from_str::<AuditEvent>(line.trim()) {
                            if let Some(bus_event) = to_bus_event(event) {
                                if self.bus.publish_blocking(bus_event) {
                                    offset = next_offset;
                                    progressed = true;
                                    OUTBOX_EVENTS_PUBLISHED.fetch_add(1, Ordering::Relaxed);
                                } else {
                                    OUTBOX_PUBLISH_ERRORS.fetch_add(1, Ordering::Relaxed);
                                    had_error = true;
                                    break;
                                }
                            } else {
                                offset = next_offset;
                                progressed = true;
                                OUTBOX_EVENTS_SKIPPED.fetch_add(1, Ordering::Relaxed);
                            }
                        } else {
                            offset = next_offset;
                            progressed = true;
                            OUTBOX_EVENTS_SKIPPED.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(_) => {
                        OUTBOX_READ_ERRORS.fetch_add(1, Ordering::Relaxed);
                        had_error = true;
                        break;
                    }
                }
            }

            if progressed {
                write_offset(&self.offset_path, offset);
            }
            if progressed && !had_error {
                backoff_ms = base_sleep_ms;
            } else {
                backoff_ms = (backoff_ms * 2).min(max_sleep_ms);
            }
            thread::sleep(Duration::from_millis(backoff_ms));
        }
    }
}

fn to_bus_event(event: AuditEvent) -> Option<BusEvent> {
    match event.event_type.as_str() {
        "OrderAccepted" | "CancelRequested" => Some(BusEvent {
            event_type: event.event_type,
            at: event.at,
            account_id: event.account_id,
            order_id: event.order_id,
            data: event.data,
        }),
        _ => None,
    }
}

fn read_offset(path: &Path) -> u64 {
    let content = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(_) => return 0,
    };
    let trimmed = content.trim();
    if trimmed.is_empty() {
        return 0;
    }
    match trimmed.parse::<u64>() {
        Ok(v) => v,
        Err(_) => {
            let _ = backup_corrupt_offset(path);
            0
        }
    }
}

fn write_offset(path: &Path, offset: u64) {
    let parent = path.parent();
    if let Some(dir) = parent {
        let _ = std::fs::create_dir_all(dir);
    }
    let tmp_path = temp_offset_path(path);
    if let Ok(mut f) = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp_path)
    {
        let _ = writeln!(f, "{offset}");
        let _ = f.flush();
        let _ = f.sync_all();
        let _ = std::fs::rename(&tmp_path, path);
        if let Some(dir) = parent {
            if let Ok(dir) = std::fs::File::open(dir) {
                let _ = dir.sync_all();
            }
        }
    }
}

fn backup_corrupt_offset(path: &Path) -> std::io::Result<()> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("outbox.offset");
    let backup_name = format!("{file_name}.bad.{millis}");
    let backup_path = path
        .parent()
        .map(|p| p.join(&backup_name))
        .unwrap_or_else(|| PathBuf::from(backup_name));
    eprintln!(
        "outbox: offset file invalid, backing up to {} and resetting to 0",
        backup_path.display()
    );
    std::fs::rename(path, backup_path)
}

fn temp_offset_path(path: &Path) -> PathBuf {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("outbox.offset");
    let tmp_name = format!("{file_name}.tmp.{millis}");
    path.parent()
        .map(|p| p.join(&tmp_name))
        .unwrap_or_else(|| PathBuf::from(tmp_name))
}
