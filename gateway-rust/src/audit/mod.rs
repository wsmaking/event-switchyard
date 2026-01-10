//! Audit log (append-only JSONL) + reader
//!
//! Minimal implementation for order/account event history.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    pub at: u64,
    pub account_id: String,
    pub order_id: Option<String>,
    pub data: Value,
}

pub struct AuditLog {
    path: PathBuf,
    writer: Mutex<BufWriter<File>>,
}

impl AuditLog {
    pub fn new(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .write(true)
            .open(&path)?;
        Ok(Self {
            path,
            writer: Mutex::new(BufWriter::new(file)),
        })
    }

    pub fn append(&self, event: AuditEvent) {
        if let Ok(mut writer) = self.writer.lock() {
            if let Ok(line) = serde_json::to_string(&event) {
                let _ = writer.write_all(line.as_bytes());
                let _ = writer.write_all(b"\n");
                let _ = writer.flush();
            }
        }
    }

    pub fn read_events(
        &self,
        account_id: &str,
        order_id: Option<&str>,
        limit: usize,
        since_ms: Option<u64>,
        after_ms: Option<u64>,
    ) -> Vec<AuditEvent> {
        let limit = limit.clamp(1, 1000);
        let file = match File::open(&self.path) {
            Ok(f) => f,
            Err(_) => return Vec::new(),
        };
        let reader = BufReader::new(file);
        let mut bucket: VecDeque<AuditEvent> = VecDeque::with_capacity(limit);
        for line in reader.lines().flatten() {
            if line.trim().is_empty() {
                continue;
            }
            let ev: AuditEvent = match serde_json::from_str(&line) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if ev.account_id != account_id {
                continue;
            }
            if let Some(order_id) = order_id {
                if ev.order_id.as_deref() != Some(order_id) {
                    continue;
                }
            }
            if let Some(since) = since_ms {
                if ev.at < since {
                    continue;
                }
            }
            if let Some(after) = after_ms {
                if ev.at <= after {
                    continue;
                }
            }
            bucket.push_back(ev);
            while bucket.len() > limit {
                bucket.pop_front();
            }
        }
        bucket.into_iter().collect()
    }
}

pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub fn parse_time_param(raw: &str) -> Option<u64> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.chars().all(|c| c.is_ascii_digit()) {
        let value = trimmed.parse::<u64>().ok()?;
        if trimmed.len() <= 10 {
            return Some(value * 1000);
        }
        return Some(value);
    }
    None
}
