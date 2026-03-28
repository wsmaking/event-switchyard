//! Outbox worker (tail audit log and publish to bus).
//!
//! Minimal at-least-once delivery for bus events.

use crate::audit::AuditEvent;
use crate::bus::{BusEvent, BusPublisher};
use serde::Serialize;
use serde_json::Value;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

static OUTBOX_LINES_READ: AtomicU64 = AtomicU64::new(0);
static OUTBOX_EVENTS_PUBLISHED: AtomicU64 = AtomicU64::new(0);
static OUTBOX_EVENTS_SKIPPED: AtomicU64 = AtomicU64::new(0);
static OUTBOX_READ_ERRORS: AtomicU64 = AtomicU64::new(0);
static OUTBOX_PUBLISH_ERRORS: AtomicU64 = AtomicU64::new(0);
static OUTBOX_OFFSET_RESETS: AtomicU64 = AtomicU64::new(0);
static OUTBOX_BACKOFF_BASE_MS: AtomicU64 = AtomicU64::new(0);
static OUTBOX_BACKOFF_MAX_MS: AtomicU64 = AtomicU64::new(0);
static OUTBOX_BACKOFF_CURRENT_MS: AtomicU64 = AtomicU64::new(0);

pub struct OutboxMetrics {
    pub lines_read: u64,
    pub events_published: u64,
    pub events_skipped: u64,
    pub read_errors: u64,
    pub publish_errors: u64,
    pub offset_resets: u64,
    pub backoff_base_ms: u64,
    pub backoff_max_ms: u64,
    pub backoff_current_ms: u64,
}

pub fn metrics() -> OutboxMetrics {
    OutboxMetrics {
        lines_read: OUTBOX_LINES_READ.load(Ordering::Relaxed),
        events_published: OUTBOX_EVENTS_PUBLISHED.load(Ordering::Relaxed),
        events_skipped: OUTBOX_EVENTS_SKIPPED.load(Ordering::Relaxed),
        read_errors: OUTBOX_READ_ERRORS.load(Ordering::Relaxed),
        publish_errors: OUTBOX_PUBLISH_ERRORS.load(Ordering::Relaxed),
        offset_resets: OUTBOX_OFFSET_RESETS.load(Ordering::Relaxed),
        backoff_base_ms: OUTBOX_BACKOFF_BASE_MS.load(Ordering::Relaxed),
        backoff_max_ms: OUTBOX_BACKOFF_MAX_MS.load(Ordering::Relaxed),
        backoff_current_ms: OUTBOX_BACKOFF_CURRENT_MS.load(Ordering::Relaxed),
    }
}

pub struct OutboxWorker {
    offset_path: PathBuf,
    audit_path: PathBuf,
    bus: Arc<BusPublisher>,
}

impl OutboxWorker {
    pub fn new(
        audit_path: impl AsRef<Path>,
        bus: Arc<BusPublisher>,
        offset_path: impl AsRef<Path>,
    ) -> Self {
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
        let base_sleep_ms = env_u64("OUTBOX_BACKOFF_BASE_MS", 200);
        let max_sleep_ms = env_u64("OUTBOX_BACKOFF_MAX_MS", 5000);
        OUTBOX_BACKOFF_BASE_MS.store(base_sleep_ms, Ordering::Relaxed);
        OUTBOX_BACKOFF_MAX_MS.store(max_sleep_ms, Ordering::Relaxed);
        let mut backoff_ms = base_sleep_ms;
        loop {
            let file = match File::open(&self.audit_path) {
                Ok(f) => f,
                Err(_) => {
                    backoff_ms = (backoff_ms * 2).min(max_sleep_ms);
                    OUTBOX_BACKOFF_CURRENT_MS.store(backoff_ms, Ordering::Relaxed);
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
                            if let Some(bus_record) = to_bus_record(event, next_offset) {
                                if publish_bus_record(&self.bus, bus_record) {
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
            OUTBOX_BACKOFF_CURRENT_MS.store(backoff_ms, Ordering::Relaxed);
            thread::sleep(Duration::from_millis(backoff_ms));
        }
    }
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

fn to_bus_record(event: AuditEvent, next_offset: u64) -> Option<BusRecord> {
    if bus_schema_version() >= 2 {
        return to_bus_event_v2(event, next_offset);
    }
    to_bus_event_v1(event)
}

fn publish_bus_record(bus: &BusPublisher, record: BusRecord) -> bool {
    match record {
        BusRecord::V1(event) => bus.publish_blocking(event),
        BusRecord::Raw { key, payload } => bus.publish_raw_blocking(&key, payload),
    }
}

fn to_bus_event_v1(event: AuditEvent) -> Option<BusRecord> {
    match event.event_type.as_str() {
        "OrderAccepted" | "AmendRequested" | "CancelRequested" | "ExecutionReport"
        | "OrderUpdated" => Some(BusRecord::V1(BusEvent {
            event_type: event.event_type,
            at: crate::bus::format_event_time(event.at),
            account_id: event.account_id,
            order_id: event.order_id,
            data: event.data,
        })),
        _ => None,
    }
}

fn to_bus_event_v2(event: AuditEvent, next_offset: u64) -> Option<BusRecord> {
    match event.event_type.as_str() {
        "OrderAccepted" | "AmendRequested" | "CancelRequested" | "ExecutionReport"
        | "OrderUpdated" => {
            let aggregate_id = event
                .order_id
                .clone()
                .unwrap_or_else(|| event.account_id.clone());
            let payload = BusEventV2 {
                event_id: format!("evt-{}-{}", aggregate_id, next_offset),
                event_type: event.event_type,
                schema_version: 2,
                source_system: "gateway-rust".to_string(),
                aggregate_id,
                aggregate_seq: next_offset,
                occurred_at: crate::bus::format_event_time(event.at),
                ingested_at: crate::bus::format_event_time(now_epoch_ms()),
                account_id: event.account_id.clone(),
                order_id: event.order_id.clone(),
                venue_order_id: venue_order_id(&event.data),
                correlation_id: correlation_id(&event.data),
                causation_id: None,
                data: event.data,
            };
            let key = payload
                .order_id
                .clone()
                .unwrap_or_else(|| payload.account_id.clone());
            let payload = match serde_json::to_vec(&payload) {
                Ok(bytes) => bytes,
                Err(_) => return None,
            };
            Some(BusRecord::Raw { key, payload })
        }
        _ => None,
    }
}

fn bus_schema_version() -> u64 {
    std::env::var("BUS_SCHEMA_VERSION")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(2)
}

fn now_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn venue_order_id(data: &Value) -> Option<String> {
    ["venueOrderId", "exchangeOrderId", "externalOrderId"]
        .iter()
        .find_map(|field| {
            data.get(field)
                .and_then(|value| value.as_str())
                .map(str::to_string)
        })
}

fn correlation_id(data: &Value) -> Option<String> {
    ["correlationId", "clientOrderId", "idempotencyKey"]
        .iter()
        .find_map(|field| {
            data.get(field)
                .and_then(|value| value.as_str())
                .map(str::to_string)
        })
}

enum BusRecord {
    V1(BusEvent),
    Raw { key: String, payload: Vec<u8> },
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct BusEventV2 {
    event_id: String,
    event_type: String,
    schema_version: u32,
    source_system: String,
    aggregate_id: String,
    aggregate_seq: u64,
    occurred_at: String,
    ingested_at: String,
    account_id: String,
    order_id: Option<String>,
    venue_order_id: Option<String>,
    correlation_id: Option<String>,
    causation_id: Option<String>,
    data: Value,
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_to_bus_event_filters() {
        let accepted = AuditEvent {
            event_type: "OrderAccepted".to_string(),
            at: 1,
            account_id: "acct-1".to_string(),
            order_id: Some("ord-1".to_string()),
            data: json!({"status":"ACCEPTED"}),
        };
        let amend = AuditEvent {
            event_type: "AmendRequested".to_string(),
            at: 2,
            account_id: "acct-1".to_string(),
            order_id: Some("ord-1".to_string()),
            data: json!({"status":"AMEND_REQUESTED"}),
        };
        let report = AuditEvent {
            event_type: "ExecutionReport".to_string(),
            at: 3,
            account_id: "acct-1".to_string(),
            order_id: Some("ord-1".to_string()),
            data: json!({"status":"FILLED"}),
        };

        assert!(to_bus_record(accepted, 10).is_some());
        assert!(to_bus_record(amend, 20).is_some());
        assert!(to_bus_record(report, 30).is_some());
    }

    #[test]
    fn test_to_bus_event_v2_contains_contract_fields() {
        unsafe {
            std::env::set_var("BUS_SCHEMA_VERSION", "2");
        }
        let accepted = AuditEvent {
            event_type: "OrderAccepted".to_string(),
            at: 1_714_680_000_000,
            account_id: "acct-1".to_string(),
            order_id: Some("ord-1".to_string()),
            data: json!({"symbol":"7203","side":"BUY","qty":100,"price":2800,"clientOrderId":"cli-1"}),
        };

        let record = to_bus_record(accepted, 123).expect("record");
        match record {
            BusRecord::Raw { key, payload } => {
                let parsed: serde_json::Value = serde_json::from_slice(&payload).expect("json");
                assert_eq!(key, "ord-1");
                assert_eq!(parsed["schemaVersion"], 2);
                assert_eq!(parsed["eventType"], "OrderAccepted");
                assert_eq!(parsed["aggregateId"], "ord-1");
                assert_eq!(parsed["aggregateSeq"], 123);
                assert_eq!(parsed["correlationId"], "cli-1");
            }
            BusRecord::V1(_) => panic!("expected v2 raw payload"),
        }
        unsafe {
            std::env::remove_var("BUS_SCHEMA_VERSION");
        }
    }

    #[test]
    fn test_read_offset_invalid_backs_up() {
        let dir = std::env::temp_dir().join(format!(
            "outbox_test_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("outbox.offset");
        std::fs::write(&path, "not-a-number").unwrap();

        let value = read_offset(&path);

        assert_eq!(0, value);
        assert!(!path.exists());
        let backup_found = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(Result::ok)
            .any(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("outbox.offset.bad.")
            });
        assert!(backup_found);
    }
}
