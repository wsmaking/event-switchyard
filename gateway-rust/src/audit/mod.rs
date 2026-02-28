//! Audit log (append-only JSONL) + reader
//!
//! Minimal implementation for order/account event history.

use aws_sdk_kms::Client as KmsClient;
use aws_sdk_kms::primitives::Blob;
use base64::Engine;
use gateway_core::now_nanos;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha256;
use std::collections::{HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc::UnboundedSender, oneshot};

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
    hash_path: Option<PathBuf>,
    hash_writer: Option<Mutex<BufWriter<File>>>,
    anchor_path: Option<PathBuf>,
    anchor_export_dir: Option<PathBuf>,
    hmac_keys: HashMap<String, Vec<u8>>,
    active_key_id: Option<String>,
    prev_hash: Mutex<Vec<u8>>,
    seq: Mutex<u64>,
    last_key_id: Mutex<Option<String>>,
    last_anchor_day: Mutex<Option<u64>>,
    wal_bytes: AtomicU64,
    wal_last_append_ms: AtomicU64,
    wal_pending_enqueue_ms: Mutex<VecDeque<u64>>,
    fdatasync_enabled: bool,
    async_enabled: bool,
    fdatasync_max_wait_us: u64,
    fdatasync_max_batch: usize,
    fdatasync_adaptive: bool,
    fdatasync_adaptive_min_wait_us: u64,
    fdatasync_adaptive_max_wait_us: u64,
    fdatasync_adaptive_min_batch: usize,
    fdatasync_adaptive_max_batch: usize,
    fdatasync_adaptive_target_sync_us: u64,
    fdatasync_coalesce_us: u64,
    append_tx: Mutex<Option<Sender<AuditAppendRequest>>>,
}

#[derive(Debug, Clone, Copy)]
pub struct AuditAppendTimings {
    pub enqueue_done_ns: u64,
    pub durable_done_ns: u64,
    pub fdatasync_ns: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct AuditDurableReceipt {
    pub durable_done_ns: u64,
    pub fdatasync_ns: u64,
}

pub struct AuditAppendWithReceipt {
    pub timings: AuditAppendTimings,
    pub durable_rx: Option<oneshot::Receiver<AuditDurableReceipt>>,
}

#[derive(Debug, Clone)]
pub struct AuditDurableNotification {
    pub event_type: String,
    pub account_id: String,
    pub order_id: Option<String>,
    pub event_at: u64,
    pub request_start_ns: u64,
    pub enqueue_done_ns: u64,
    pub durable_done_ns: u64,
    pub fdatasync_ns: u64,
}

struct AuditAppendRequest {
    event: AuditEvent,
    request_start_ns: u64,
    enqueue_done_ns: u64,
    durable_reply: Option<oneshot::Sender<AuditDurableReceipt>>,
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
        let (wal_bytes, wal_last_append_ms) = match file.metadata() {
            Ok(meta) => {
                let bytes = meta.len();
                let last_ms = meta
                    .modified()
                    .ok()
                    .and_then(|m| m.duration_since(UNIX_EPOCH).ok())
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                (bytes, last_ms)
            }
            Err(_) => (0, 0),
        };
        let fdatasync_enabled = std::env::var("AUDIT_FDATASYNC")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(true);
        let async_enabled = std::env::var("AUDIT_ASYNC_WAL")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(true);
        let fdatasync_max_wait_us = std::env::var("AUDIT_FDATASYNC_MAX_WAIT_US")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(200);
        let fdatasync_max_batch = std::env::var("AUDIT_FDATASYNC_MAX_BATCH")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(64);
        let fdatasync_adaptive = std::env::var("AUDIT_FDATASYNC_ADAPTIVE")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let fdatasync_adaptive_max_wait_us = std::env::var("AUDIT_FDATASYNC_ADAPTIVE_MAX_WAIT_US")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(fdatasync_max_wait_us);
        let fdatasync_adaptive_min_wait_us = std::env::var("AUDIT_FDATASYNC_ADAPTIVE_MIN_WAIT_US")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or((fdatasync_max_wait_us / 2).max(50));
        let fdatasync_adaptive_max_batch = std::env::var("AUDIT_FDATASYNC_ADAPTIVE_MAX_BATCH")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(fdatasync_max_batch);
        let fdatasync_adaptive_min_batch = std::env::var("AUDIT_FDATASYNC_ADAPTIVE_MIN_BATCH")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or((fdatasync_max_batch / 2).max(1));
        let fdatasync_adaptive_target_sync_us =
            std::env::var("AUDIT_FDATASYNC_ADAPTIVE_TARGET_SYNC_US")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(4_000);
        let fdatasync_coalesce_us = std::env::var("AUDIT_FDATASYNC_COALESCE_US")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        let (hash_path, hash_writer, hmac_keys, active_key_id, prev_hash, seq, last_key_id) =
            init_hash_chain(&path)?;
        let anchor_path = std::env::var("AUDIT_ANCHOR_PATH")
            .ok()
            .map(PathBuf::from)
            .or_else(|| {
                let mut s = path.to_string_lossy().to_string();
                s.push_str(".anchor");
                Some(PathBuf::from(s))
            });
        let anchor_export_dir = std::env::var("AUDIT_ANCHOR_EXPORT_DIR")
            .ok()
            .map(PathBuf::from);
        Ok(Self {
            path,
            writer: Mutex::new(BufWriter::new(file)),
            hash_path,
            hash_writer,
            anchor_path,
            anchor_export_dir,
            hmac_keys,
            active_key_id,
            prev_hash: Mutex::new(prev_hash),
            seq: Mutex::new(seq),
            last_key_id: Mutex::new(last_key_id),
            last_anchor_day: Mutex::new(None),
            wal_bytes: AtomicU64::new(wal_bytes),
            wal_last_append_ms: AtomicU64::new(wal_last_append_ms),
            wal_pending_enqueue_ms: Mutex::new(VecDeque::new()),
            fdatasync_enabled,
            async_enabled,
            fdatasync_max_wait_us,
            fdatasync_max_batch,
            fdatasync_adaptive,
            fdatasync_adaptive_min_wait_us,
            fdatasync_adaptive_max_wait_us,
            fdatasync_adaptive_min_batch,
            fdatasync_adaptive_max_batch,
            fdatasync_adaptive_target_sync_us,
            fdatasync_coalesce_us,
            append_tx: Mutex::new(None),
        })
    }

    pub fn append(&self, event: AuditEvent) {
        let _ = self.append_with_timings(event, now_nanos());
    }

    pub fn append_with_timings(
        &self,
        event: AuditEvent,
        request_start_ns: u64,
    ) -> AuditAppendTimings {
        if self.async_enabled {
            if let Ok(guard) = self.append_tx.lock() {
                if let Some(tx) = guard.as_ref() {
                    let send_start_ns = now_nanos();
                    let request = AuditAppendRequest {
                        event: event.clone(),
                        request_start_ns,
                        enqueue_done_ns: send_start_ns,
                        durable_reply: None,
                    };
                    if tx.send(request).is_ok() {
                        let enqueue_done_ns = now_nanos();
                        if let Ok(mut pending) = self.wal_pending_enqueue_ms.lock() {
                            pending.push_back(now_millis());
                        }
                        return AuditAppendTimings {
                            enqueue_done_ns,
                            durable_done_ns: 0,
                            fdatasync_ns: 0,
                        };
                    }
                }
            }
        }
        self.append_sync_with_timings(event)
    }

    /// `/v2` 契約向け: PendingAccepted の durable 完了を待つための receipt を返す。
    /// 非同期WAL有効時は専用writerキューに必ず積み、HTTPスレッドでの同期書き込みへはフォールバックしない。
    pub fn append_with_durable_receipt(
        &self,
        event: AuditEvent,
        request_start_ns: u64,
    ) -> AuditAppendWithReceipt {
        if self.async_enabled {
            if let Ok(guard) = self.append_tx.lock() {
                if let Some(tx) = guard.as_ref() {
                    let send_start_ns = now_nanos();
                    let (reply_tx, reply_rx) = oneshot::channel();
                    let request = AuditAppendRequest {
                        event,
                        request_start_ns,
                        enqueue_done_ns: send_start_ns,
                        durable_reply: Some(reply_tx),
                    };
                    if tx.send(request).is_ok() {
                        let enqueue_done_ns = now_nanos();
                        if let Ok(mut pending) = self.wal_pending_enqueue_ms.lock() {
                            pending.push_back(now_millis());
                        }
                        return AuditAppendWithReceipt {
                            timings: AuditAppendTimings {
                                enqueue_done_ns,
                                durable_done_ns: 0,
                                fdatasync_ns: 0,
                            },
                            durable_rx: Some(reply_rx),
                        };
                    }
                }
            }
            return AuditAppendWithReceipt {
                timings: AuditAppendTimings {
                    enqueue_done_ns: 0,
                    durable_done_ns: 0,
                    fdatasync_ns: 0,
                },
                durable_rx: None,
            };
        }

        AuditAppendWithReceipt {
            timings: self.append_sync_with_timings(event),
            durable_rx: None,
        }
    }

    #[cfg(test)]
    pub fn poison_writer_for_test(&self) {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = self.writer.lock().expect("writer lock");
            panic!("poison writer mutex for test");
        }));
    }

    pub fn start_async_writer(
        self: std::sync::Arc<Self>,
        durable_tx: Option<UnboundedSender<AuditDurableNotification>>,
    ) {
        if !self.async_enabled {
            return;
        }
        let mut guard = match self.append_tx.lock() {
            Ok(v) => v,
            Err(poisoned) => poisoned.into_inner(),
        };
        if guard.is_some() {
            return;
        }
        let (tx, rx) = mpsc::channel::<AuditAppendRequest>();
        *guard = Some(tx);
        drop(guard);

        let fdatasync_enabled = self.fdatasync_enabled;
        let max_wait_us = self.fdatasync_max_wait_us.max(1);
        let max_batch = self.fdatasync_max_batch.max(1);
        let fdatasync_coalesce_us = self.fdatasync_coalesce_us;
        let target_batch_bytes = std::env::var("AUDIT_FDATASYNC_TARGET_BATCH_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(64 * 1024)
            .max(1_024);
        let min_target_batch_bytes = std::env::var("AUDIT_FDATASYNC_TARGET_BATCH_BYTES_MIN")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or((target_batch_bytes / 4).max(4 * 1024))
            .max(1_024);
        let max_target_batch_bytes = std::env::var("AUDIT_FDATASYNC_TARGET_BATCH_BYTES_MAX")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or((target_batch_bytes.saturating_mul(8)).max(target_batch_bytes))
            .max(min_target_batch_bytes);
        let coalesce_min_us = std::env::var("AUDIT_FDATASYNC_COALESCE_MIN_US")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or((fdatasync_coalesce_us / 4).max(50));
        let coalesce_max_us = std::env::var("AUDIT_FDATASYNC_COALESCE_MAX_US")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(
                fdatasync_coalesce_us
                    .max(max_wait_us.saturating_mul(4))
                    .max(coalesce_min_us),
            )
            .max(coalesce_min_us);
        let adaptive_cfg = if self.fdatasync_adaptive {
            let min_wait_us = self
                .fdatasync_adaptive_min_wait_us
                .max(1)
                .min(self.fdatasync_adaptive_max_wait_us.max(1));
            let max_wait_us = self.fdatasync_adaptive_max_wait_us.max(min_wait_us);
            let min_batch = self
                .fdatasync_adaptive_min_batch
                .max(1)
                .min(self.fdatasync_adaptive_max_batch.max(1));
            let max_batch = self.fdatasync_adaptive_max_batch.max(min_batch);
            Some(FdatasyncAdaptiveConfig {
                min_wait_us,
                max_wait_us,
                min_batch,
                max_batch,
                target_sync_us: self.fdatasync_adaptive_target_sync_us.max(100),
            })
        } else {
            None
        };
        std::thread::spawn(move || {
            run_async_writer(
                self,
                rx,
                durable_tx,
                fdatasync_enabled,
                max_wait_us,
                max_batch,
                fdatasync_coalesce_us,
                coalesce_min_us,
                coalesce_max_us,
                target_batch_bytes,
                min_target_batch_bytes,
                max_target_batch_bytes,
                adaptive_cfg,
            );
        });
    }

    fn append_sync_with_timings(&self, event: AuditEvent) -> AuditAppendTimings {
        let mut enqueue_done_ns = 0;
        let mut durable_done_ns = 0;
        let mut fdatasync_ns = 0;
        if let Ok(mut writer) = self.writer.lock() {
            if let Ok(line) = serde_json::to_string(&event) {
                let mut line_bytes = line.into_bytes();
                line_bytes.push(b'\n');
                if writer.write_all(&line_bytes).is_err() {
                    return AuditAppendTimings {
                        enqueue_done_ns: 0,
                        durable_done_ns: 0,
                        fdatasync_ns: 0,
                    };
                }
                if writer.flush().is_err() {
                    return AuditAppendTimings {
                        enqueue_done_ns: 0,
                        durable_done_ns: 0,
                        fdatasync_ns: 0,
                    };
                }
                self.append_hash_chain(&line_bytes);
                enqueue_done_ns = now_nanos();

                self.wal_bytes
                    .fetch_add(line_bytes.len() as u64, Ordering::Relaxed);
                self.wal_last_append_ms
                    .store(now_millis(), Ordering::Relaxed);

                if self.fdatasync_enabled {
                    let sync_start = now_nanos();
                    if writer.get_ref().sync_data().is_err() {
                        return AuditAppendTimings {
                            enqueue_done_ns,
                            durable_done_ns: 0,
                            fdatasync_ns: 0,
                        };
                    }
                    fdatasync_ns = now_nanos() - sync_start;
                }
                durable_done_ns = now_nanos();
            }
        }
        AuditAppendTimings {
            enqueue_done_ns,
            durable_done_ns,
            fdatasync_ns,
        }
    }

    pub fn wal_bytes(&self) -> u64 {
        self.wal_bytes.load(Ordering::Relaxed)
    }

    pub fn async_enabled(&self) -> bool {
        self.async_enabled
    }

    pub fn wal_age_ms(&self) -> u64 {
        if let Ok(pending) = self.wal_pending_enqueue_ms.lock() {
            if let Some(oldest) = pending.front() {
                return now_millis().saturating_sub(*oldest);
            }
        }
        0
    }

    pub fn disk_free_pct(&self) -> Option<f64> {
        disk_free_pct(&self.path)
    }

    pub fn read_events(
        &self,
        account_id: &str,
        order_id: Option<&str>,
        limit: usize,
        since_ms: Option<u64>,
        after_ms: Option<u64>,
    ) -> Vec<AuditEvent> {
        read_events_from_path(&self.path, account_id, order_id, limit, since_ms, after_ms)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn anchor_snapshot(&self) -> Option<AuditAnchor> {
        let prev_hash = match self.prev_hash.lock() {
            Ok(v) => v,
            Err(poisoned) => poisoned.into_inner(),
        };
        if prev_hash.is_empty() {
            return None;
        }
        let seq = match self.seq.lock() {
            Ok(v) => v,
            Err(poisoned) => poisoned.into_inner(),
        };
        let key_id = match self.last_key_id.lock() {
            Ok(v) => v,
            Err(poisoned) => poisoned.into_inner(),
        };
        Some(AuditAnchor {
            seq: *seq,
            at: now_millis(),
            key_id: key_id.clone().unwrap_or_else(|| "unknown".into()),
            hash: base64::engine::general_purpose::STANDARD.encode(&*prev_hash),
        })
    }

    pub fn verify(&self, from_seq: u64, limit: usize) -> AuditVerifyResult {
        if self.hmac_keys.is_empty() {
            return AuditVerifyResult {
                ok: false,
                checked: 0,
                last_seq: 0,
                error: Some("AUDIT_HMAC_KEY not configured".into()),
            };
        }
        let hash_path = match &self.hash_path {
            Some(p) => p,
            None => {
                return AuditVerifyResult {
                    ok: false,
                    checked: 0,
                    last_seq: 0,
                    error: Some("audit hash file not configured".into()),
                };
            }
        };
        let limit = limit.clamp(1, 10_000);

        let audit_file = match File::open(&self.path) {
            Ok(f) => f,
            Err(err) => {
                return AuditVerifyResult {
                    ok: false,
                    checked: 0,
                    last_seq: 0,
                    error: Some(format!("audit log open failed: {err}")),
                };
            }
        };
        let hash_file = match File::open(hash_path) {
            Ok(f) => f,
            Err(err) => {
                return AuditVerifyResult {
                    ok: false,
                    checked: 0,
                    last_seq: 0,
                    error: Some(format!("audit hash open failed: {err}")),
                };
            }
        };

        let mut audit_reader = BufReader::new(audit_file);
        let mut hash_reader = BufReader::new(hash_file);
        let mut line = String::new();
        let mut hash_line = String::new();
        let mut prev_hash: Vec<u8> = Vec::new();
        let mut checked = 0u64;
        let mut last_seq = 0u64;

        loop {
            line.clear();
            hash_line.clear();
            let read_log = audit_reader.read_line(&mut line).ok().unwrap_or(0);
            let read_hash = hash_reader.read_line(&mut hash_line).ok().unwrap_or(0);
            if read_log == 0 && read_hash == 0 {
                break;
            }
            if read_log == 0 || read_hash == 0 {
                return AuditVerifyResult {
                    ok: false,
                    checked,
                    last_seq,
                    error: Some("audit log and hash length mismatch".into()),
                };
            }

            let entry: AuditHashEntry = match serde_json::from_str(hash_line.trim()) {
                Ok(v) => v,
                Err(_) => {
                    return AuditVerifyResult {
                        ok: false,
                        checked,
                        last_seq,
                        error: Some("hash entry parse failed".into()),
                    };
                }
            };

            if entry.seq < from_seq {
                prev_hash = decode_hash(&entry.hash).unwrap_or_default();
                last_seq = entry.seq;
                continue;
            }

            let mut line_bytes = line.as_bytes().to_vec();
            if line_bytes.last() != Some(&b'\n') {
                line_bytes.push(b'\n');
            }
            let key = match self.hmac_keys.get(&entry.key_id) {
                Some(k) => k,
                None => {
                    return AuditVerifyResult {
                        ok: false,
                        checked,
                        last_seq: entry.seq,
                        error: Some(format!("unknown audit key id: {}", entry.key_id)),
                    };
                }
            };
            let expected = compute_hmac(key, &prev_hash, &line_bytes);
            let expected_b64 = base64::engine::general_purpose::STANDARD.encode(expected);
            if expected_b64 != entry.hash {
                return AuditVerifyResult {
                    ok: false,
                    checked,
                    last_seq: entry.seq,
                    error: Some("hash mismatch detected".into()),
                };
            }
            prev_hash = decode_hash(&entry.hash).unwrap_or_default();
            checked += 1;
            last_seq = entry.seq;
            if checked >= limit as u64 {
                break;
            }
        }

        AuditVerifyResult {
            ok: true,
            checked,
            last_seq,
            error: None,
        }
    }

    fn append_hash_chain(&self, line_bytes: &[u8]) {
        let (key, writer, key_id) = match (&self.active_key_id, &self.hash_writer) {
            (Some(id), Some(w)) => match self.hmac_keys.get(id) {
                Some(k) => (k, w, id),
                None => return,
            },
            _ => return,
        };
        let mut prev_hash = match self.prev_hash.lock() {
            Ok(v) => v,
            Err(poisoned) => poisoned.into_inner(),
        };
        let mut seq = match self.seq.lock() {
            Ok(v) => v,
            Err(poisoned) => poisoned.into_inner(),
        };
        let hash_bytes = compute_hmac(key, &prev_hash, line_bytes);
        let hash_b64 = base64::engine::general_purpose::STANDARD.encode(&hash_bytes);
        let prev_b64 = if prev_hash.is_empty() {
            None
        } else {
            Some(base64::engine::general_purpose::STANDARD.encode(&*prev_hash))
        };

        *seq += 1;
        let entry = AuditHashEntry {
            seq: *seq,
            at: now_millis(),
            key_id: key_id.clone(),
            prev_hash: prev_b64,
            hash: hash_b64.clone(),
        };
        if let Ok(mut writer) = writer.lock() {
            if let Ok(line) = serde_json::to_string(&entry) {
                let _ = writer.write_all(line.as_bytes());
                let _ = writer.write_all(b"\n");
                let _ = writer.flush();
            }
        }
        if let Ok(mut last_key_id) = self.last_key_id.lock() {
            *last_key_id = Some(key_id.clone());
        }
        if let Some(anchor_path) = &self.anchor_path {
            let anchor = AuditAnchor {
                seq: *seq,
                at: entry.at,
                key_id: key_id.clone(),
                hash: hash_b64.clone(),
            };
            let _ = write_anchor(anchor_path, &anchor);
        }
        self.export_anchor_if_needed(*seq, entry.at, key_id, hash_b64);
        *prev_hash = hash_bytes;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AuditHashEntry {
    pub seq: u64,
    pub at: u64,
    pub key_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev_hash: Option<String>,
    pub hash: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditVerifyResult {
    pub ok: bool,
    pub checked: u64,
    pub last_seq: u64,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditAnchor {
    pub seq: u64,
    pub at: u64,
    pub key_id: String,
    pub hash: String,
}

fn init_hash_chain(
    path: &Path,
) -> std::io::Result<(
    Option<PathBuf>,
    Option<Mutex<BufWriter<File>>>,
    HashMap<String, Vec<u8>>,
    Option<String>,
    Vec<u8>,
    u64,
    Option<String>,
)> {
    let (keys, active_id) = load_audit_keys();
    if keys.is_empty() || active_id.is_none() {
        return Ok((None, None, HashMap::new(), None, Vec::new(), 0, None));
    }
    let hash_path = std::env::var("AUDIT_HASH_PATH")
        .ok()
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let mut s = path.to_string_lossy().to_string();
            s.push_str(".hash");
            PathBuf::from(s)
        });
    if let Some(parent) = hash_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let hash_file = OpenOptions::new()
        .create(true)
        .append(true)
        .write(true)
        .open(&hash_path)?;
    let (prev_hash, seq, last_key_id) = match load_last_hash(&hash_path) {
        Ok(v) => v,
        Err(msg) => {
            let _ = backup_corrupt_hash(&hash_path);
            eprintln!("audit hash invalid: {} (resetting chain)", msg);
            (Vec::new(), 0, None)
        }
    };
    Ok((
        Some(hash_path),
        Some(Mutex::new(BufWriter::new(hash_file))),
        keys,
        active_id,
        prev_hash,
        seq,
        last_key_id,
    ))
}

fn load_last_hash(path: &Path) -> Result<(Vec<u8>, u64, Option<String>), String> {
    let mut file = File::open(path).map_err(|e| e.to_string())?;
    let len = file.metadata().map_err(|e| e.to_string())?.len();
    if len == 0 {
        return Ok((Vec::new(), 0, None));
    }
    let read_len = len.min(8192);
    file.seek(SeekFrom::End(-(read_len as i64)))
        .map_err(|e| e.to_string())?;
    let mut buf = vec![0u8; read_len as usize];
    file.read_exact(&mut buf).map_err(|e| e.to_string())?;
    let text = String::from_utf8_lossy(&buf);
    let line = match text.lines().rev().find(|l| !l.trim().is_empty()) {
        Some(l) => l,
        None => return Ok((Vec::new(), 0, None)),
    };
    let entry: AuditHashEntry =
        serde_json::from_str(line).map_err(|_| "hash entry parse failed".to_string())?;
    let hash_bytes = decode_hash(&entry.hash).unwrap_or_default();
    Ok((hash_bytes, entry.seq, Some(entry.key_id)))
}

fn decode_hash(value: &str) -> Option<Vec<u8>> {
    base64::engine::general_purpose::STANDARD
        .decode(value.trim())
        .ok()
}

fn backup_corrupt_hash(path: &Path) -> std::io::Result<()> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("audit.hash");
    let backup_name = format!("{file_name}.bad.{millis}");
    let backup_path = path
        .parent()
        .map(|p| p.join(&backup_name))
        .unwrap_or_else(|| PathBuf::from(backup_name));
    std::fs::rename(path, backup_path)
}

fn compute_hmac(key: &[u8], prev_hash: &[u8], line_bytes: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC key invalid");
    mac.update(prev_hash);
    mac.update(line_bytes);
    mac.finalize().into_bytes().to_vec()
}

fn write_anchor(path: &Path, anchor: &AuditAnchor) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp_path = temp_anchor_path(path);
    let mut f = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp_path)?;
    let line = serde_json::to_string(anchor)?;
    f.write_all(line.as_bytes())?;
    f.write_all(b"\n")?;
    f.flush()?;
    f.sync_all()?;
    std::fs::rename(&tmp_path, path)?;
    if let Some(parent) = path.parent() {
        if let Ok(dir) = File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    Ok(())
}

fn days_since_epoch(ms: u64) -> u64 {
    ms / 1000 / 86400
}

fn anchor_filename(day: u64) -> String {
    format!("audit.anchor.{day}")
}

impl AuditLog {
    fn export_anchor_if_needed(&self, seq: u64, at_ms: u64, key_id: &str, hash: String) {
        let export_dir = match &self.anchor_export_dir {
            Some(d) => d,
            None => return,
        };
        let day = days_since_epoch(at_ms);
        let mut last_day = match self.last_anchor_day.lock() {
            Ok(v) => v,
            Err(poisoned) => poisoned.into_inner(),
        };
        if last_day.is_some_and(|d| d == day) {
            return;
        }
        let anchor = AuditAnchor {
            seq,
            at: at_ms,
            key_id: key_id.to_string(),
            hash,
        };
        if let Ok(()) = write_anchor_export(export_dir, day, &anchor) {
            *last_day = Some(day);
        }
    }
}

fn write_anchor_export(dir: &Path, day: u64, anchor: &AuditAnchor) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let path = dir.join(anchor_filename(day));
    let tmp_path = temp_anchor_path(&path);
    let mut f = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp_path)?;
    let line = serde_json::to_string(anchor)?;
    f.write_all(line.as_bytes())?;
    f.write_all(b"\n")?;
    f.flush()?;
    f.sync_all()?;
    std::fs::rename(&tmp_path, &path)?;
    if let Ok(dir) = File::open(dir) {
        let _ = dir.sync_all();
    }
    Ok(())
}

fn temp_anchor_path(path: &Path) -> PathBuf {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("audit.anchor");
    let tmp_name = format!("{file_name}.tmp.{millis}");
    path.parent()
        .map(|p| p.join(&tmp_name))
        .unwrap_or_else(|| PathBuf::from(tmp_name))
}

fn load_audit_keys() -> (HashMap<String, Vec<u8>>, Option<String>) {
    let mut keys = HashMap::new();
    if let Some((key_id, key_bytes)) = load_kms_key() {
        keys.insert(key_id, key_bytes);
    }
    if let Ok(list) = std::env::var("AUDIT_HMAC_KEYS") {
        for part in list.split(',') {
            let trimmed = part.trim();
            if trimmed.is_empty() {
                continue;
            }
            let mut iter = trimmed.splitn(2, ':');
            let key_id = match iter.next() {
                Some(v) if !v.is_empty() => v.trim().to_string(),
                _ => continue,
            };
            let raw = match iter.next() {
                Some(v) if !v.is_empty() => v.trim(),
                _ => continue,
            };
            if let Some(key_bytes) = parse_key_value(raw) {
                keys.insert(key_id, key_bytes);
            }
        }
    }
    if keys.is_empty() {
        if let Ok(value) = std::env::var("AUDIT_HMAC_KEY") {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                keys.insert("key-1".to_string(), trimmed.as_bytes().to_vec());
            }
        }
    }
    let active_id = std::env::var("AUDIT_KEY_ID")
        .ok()
        .filter(|v| !v.trim().is_empty());
    let active_id = active_id.or_else(|| keys.keys().next().cloned());
    (keys, active_id)
}

fn parse_key_value(raw: &str) -> Option<Vec<u8>> {
    let trimmed = raw.trim();
    if let Some(value) = trimmed.strip_prefix("base64:") {
        return base64::engine::general_purpose::STANDARD
            .decode(value.trim())
            .ok();
    }
    Some(trimmed.as_bytes().to_vec())
}

fn load_kms_key() -> Option<(String, Vec<u8>)> {
    let ciphertext_b64 = std::env::var("AUDIT_KMS_CIPHERTEXT_B64").ok()?;
    let ciphertext = base64::engine::general_purpose::STANDARD
        .decode(ciphertext_b64.trim())
        .ok()?;
    let key_id = std::env::var("AUDIT_KMS_KEY_ID").unwrap_or_else(|_| "kms-1".into());
    let plaintext = decrypt_kms_blocking(ciphertext)?;
    Some((key_id, plaintext))
}

fn decrypt_kms_blocking(ciphertext: Vec<u8>) -> Option<Vec<u8>> {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        return handle.block_on(decrypt_kms(ciphertext));
    }
    let runtime = tokio::runtime::Runtime::new().ok()?;
    runtime.block_on(decrypt_kms(ciphertext))
}

async fn decrypt_kms(ciphertext: Vec<u8>) -> Option<Vec<u8>> {
    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let client = KmsClient::new(&config);
    let response = client
        .decrypt()
        .ciphertext_blob(Blob::new(ciphertext))
        .send()
        .await
        .ok()?;
    let plaintext = response.plaintext()?;
    Some(plaintext.as_ref().to_vec())
}

fn run_async_writer(
    audit: std::sync::Arc<AuditLog>,
    rx: Receiver<AuditAppendRequest>,
    durable_tx: Option<UnboundedSender<AuditDurableNotification>>,
    fdatasync_enabled: bool,
    max_wait_us: u64,
    max_batch: usize,
    fdatasync_coalesce_us: u64,
    coalesce_min_us: u64,
    coalesce_max_us: u64,
    target_batch_bytes: usize,
    min_target_batch_bytes: usize,
    max_target_batch_bytes: usize,
    adaptive_cfg: Option<FdatasyncAdaptiveConfig>,
) {
    fn notify_durable_batch(
        batch: &mut Vec<AuditAppendRequest>,
        durable_tx: &Option<UnboundedSender<AuditDurableNotification>>,
        durable_done_ns: u64,
        fdatasync_ns: u64,
        success: bool,
    ) {
        if success {
            if let Some(tx) = durable_tx.as_ref() {
                for req in batch.iter() {
                    let _ = tx.send(AuditDurableNotification {
                        event_type: req.event.event_type.clone(),
                        account_id: req.event.account_id.clone(),
                        order_id: req.event.order_id.clone(),
                        event_at: req.event.at,
                        request_start_ns: req.request_start_ns,
                        enqueue_done_ns: req.enqueue_done_ns,
                        durable_done_ns,
                        fdatasync_ns,
                    });
                }
            }
        }
        for req in batch.iter_mut() {
            if let Some(reply) = req.durable_reply.take() {
                let _ = reply.send(AuditDurableReceipt {
                    durable_done_ns: if success { durable_done_ns } else { 0 },
                    fdatasync_ns: if success { fdatasync_ns } else { 0 },
                });
            }
        }
        batch.clear();
    }

    let mut cur_wait_us = adaptive_cfg
        .as_ref()
        .map(|c| c.min_wait_us)
        .unwrap_or(max_wait_us)
        .max(1);
    let mut cur_batch = adaptive_cfg
        .as_ref()
        .map(|c| c.min_batch)
        .unwrap_or(max_batch)
        .max(1);
    let mut batch: Vec<AuditAppendRequest> = Vec::with_capacity(
        max_batch.max(
            adaptive_cfg
                .as_ref()
                .map(|c| c.max_batch)
                .unwrap_or(max_batch),
        ),
    );
    let mut pending_durable: Vec<AuditAppendRequest> = Vec::new();
    let mut cur_coalesce_us = fdatasync_coalesce_us;
    let mut cur_target_batch_bytes = target_batch_bytes.max(min_target_batch_bytes);
    let mut pending_bytes_since_sync = 0usize;
    let mut last_sync_ns = now_nanos();
    loop {
        let first = match rx.recv() {
            Ok(v) => v,
            Err(_) => {
                // channel close直前に保留した通知がある場合は最後にdurable化して通知する。
                if !pending_durable.is_empty() {
                    let mut fdatasync_ns = 0;
                    let mut sync_ok = true;
                    if fdatasync_enabled {
                        if let Ok(writer) = audit.writer.lock() {
                            let sync_start = now_nanos();
                            sync_ok = writer.get_ref().sync_data().is_ok();
                            fdatasync_ns = now_nanos().saturating_sub(sync_start);
                        } else {
                            sync_ok = false;
                        }
                    }
                    let durable_done_ns = if sync_ok { now_nanos() } else { 0 };
                    notify_durable_batch(
                        &mut pending_durable,
                        &durable_tx,
                        durable_done_ns,
                        fdatasync_ns,
                        sync_ok,
                    );
                }
                break;
            }
        };
        batch.push(first);
        let started = Instant::now();
        let max_wait = Duration::from_micros(cur_wait_us);
        while batch.len() < cur_batch {
            let elapsed = started.elapsed();
            let remaining = max_wait
                .checked_sub(elapsed)
                .unwrap_or(Duration::from_micros(0));
            if remaining.is_zero() {
                break;
            }
            match rx.recv_timeout(remaining) {
                Ok(v) => batch.push(v),
                Err(mpsc::RecvTimeoutError::Timeout) => break,
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }

        // 同期直前にキューを軽くドレインして、fdatasync回数を抑える。
        // 追加待機はせず try_recv のみ使うため、遅延を増やさずにバッチ効率だけ上げる。
        while batch.len() < cur_batch {
            match rx.try_recv() {
                Ok(v) => batch.push(v),
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => break,
            }
        }

        let now_ns = now_nanos();
        let within_coalesce_window = fdatasync_enabled
            && cur_coalesce_us > 0
            && now_ns.saturating_sub(last_sync_ns) < cur_coalesce_us.saturating_mul(1_000);
        let mut total_bytes = 0usize;
        let processed_len = batch.len();
        let mut fdatasync_ns = 0;
        let mut write_ok = true;
        if let Ok(mut writer) = audit.writer.lock() {
            for req in &batch {
                match serde_json::to_string(&req.event) {
                    Ok(line) => {
                        let mut line_bytes = line.into_bytes();
                        line_bytes.push(b'\n');
                        if writer.write_all(&line_bytes).is_err() {
                            write_ok = false;
                            break;
                        }
                        audit.append_hash_chain(&line_bytes);
                        total_bytes += line_bytes.len();
                    }
                    Err(_) => {
                        write_ok = false;
                        break;
                    }
                }
            }
            if write_ok && writer.flush().is_err() {
                write_ok = false;
            }
            if write_ok {
                audit
                    .wal_bytes
                    .fetch_add(total_bytes as u64, Ordering::Relaxed);
                audit
                    .wal_last_append_ms
                    .store(now_millis(), Ordering::Relaxed);
            }
        } else {
            write_ok = false;
        }

        if !write_ok {
            pending_durable.append(&mut batch);
            notify_durable_batch(&mut pending_durable, &durable_tx, 0, 0, false);
            if let Ok(mut pending) = audit.wal_pending_enqueue_ms.lock() {
                let drain = processed_len.min(pending.len());
                for _ in 0..drain {
                    let _ = pending.pop_front();
                }
            }
            continue;
        }

        let candidate_pending_bytes = pending_bytes_since_sync.saturating_add(total_bytes);
        let should_defer_sync =
            within_coalesce_window && candidate_pending_bytes < cur_target_batch_bytes;
        if fdatasync_enabled && !should_defer_sync {
            if let Ok(writer) = audit.writer.lock() {
                let sync_start = now_nanos();
                if writer.get_ref().sync_data().is_err() {
                    write_ok = false;
                }
                fdatasync_ns = now_nanos().saturating_sub(sync_start);
            } else {
                write_ok = false;
            }
        }

        if !write_ok {
            pending_durable.append(&mut batch);
            notify_durable_batch(&mut pending_durable, &durable_tx, 0, 0, false);
            if let Ok(mut pending) = audit.wal_pending_enqueue_ms.lock() {
                let drain = processed_len.min(pending.len());
                for _ in 0..drain {
                    let _ = pending.pop_front();
                }
            }
            continue;
        }

        if should_defer_sync {
            pending_bytes_since_sync = candidate_pending_bytes;
            pending_durable.append(&mut batch);
        } else {
            let durable_done_ns = now_nanos();
            if fdatasync_enabled {
                last_sync_ns = durable_done_ns;
            }
            pending_bytes_since_sync = 0;
            pending_durable.append(&mut batch);
            notify_durable_batch(
                &mut pending_durable,
                &durable_tx,
                durable_done_ns,
                fdatasync_ns,
                true,
            );
        }

        if !should_defer_sync {
            if let Some(cfg) = adaptive_cfg.as_ref() {
                let sync_us = fdatasync_ns / 1_000;
                let used_batch = processed_len.max(1);
                let high_fill = used_batch >= (cur_batch.saturating_mul(9) / 10).max(1);
                let low_fill = used_batch <= (cur_batch / 2).max(1);

                if sync_us > cfg.target_sync_us || high_fill {
                    cur_wait_us = (cur_wait_us + (cur_wait_us / 4).max(10)).min(cfg.max_wait_us);
                    cur_batch = (cur_batch + (cur_batch / 8).max(1)).min(cfg.max_batch);
                    cur_target_batch_bytes = (cur_target_batch_bytes
                        + (cur_target_batch_bytes / 5).max(1024))
                    .min(max_target_batch_bytes);
                    if cur_coalesce_us > 0 {
                        cur_coalesce_us =
                            (cur_coalesce_us + (cur_coalesce_us / 6).max(10)).min(coalesce_max_us);
                    }
                } else if sync_us < (cfg.target_sync_us / 2) && low_fill {
                    cur_wait_us = cur_wait_us
                        .saturating_sub((cur_wait_us / 5).max(10))
                        .max(cfg.min_wait_us);
                    cur_batch = cur_batch
                        .saturating_sub((cur_batch / 10).max(1))
                        .max(cfg.min_batch);
                    cur_target_batch_bytes = cur_target_batch_bytes
                        .saturating_sub((cur_target_batch_bytes / 6).max(1024))
                        .max(min_target_batch_bytes);
                    if cur_coalesce_us > 0 {
                        cur_coalesce_us = cur_coalesce_us
                            .saturating_sub((cur_coalesce_us / 6).max(10))
                            .max(coalesce_min_us);
                    }
                }
            }
        }

        if cur_target_batch_bytes < min_target_batch_bytes {
            cur_target_batch_bytes = min_target_batch_bytes;
        } else if cur_target_batch_bytes > max_target_batch_bytes {
            cur_target_batch_bytes = max_target_batch_bytes;
        }
        if cur_coalesce_us > 0 {
            cur_coalesce_us = cur_coalesce_us.clamp(coalesce_min_us, coalesce_max_us);
        }

        if let Ok(mut pending) = audit.wal_pending_enqueue_ms.lock() {
            let drain = processed_len.min(pending.len());
            for _ in 0..drain {
                let _ = pending.pop_front();
            }
        }
        batch.clear();
    }
}

#[derive(Debug, Clone, Copy)]
struct FdatasyncAdaptiveConfig {
    min_wait_us: u64,
    max_wait_us: u64,
    min_batch: usize,
    max_batch: usize,
    target_sync_us: u64,
}

pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(unix)]
fn disk_free_pct(path: &Path) -> Option<f64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) };
    if rc != 0 {
        return None;
    }
    let total = stat.f_blocks as f64 * stat.f_frsize as f64;
    let free = stat.f_bavail as f64 * stat.f_frsize as f64;
    if total <= 0.0 {
        return None;
    }
    Some((free / total) * 100.0)
}

#[cfg(not(unix))]
fn disk_free_pct(_path: &Path) -> Option<f64> {
    None
}

pub fn read_events_from_path(
    path: impl AsRef<Path>,
    account_id: &str,
    order_id: Option<&str>,
    limit: usize,
    since_ms: Option<u64>,
    after_ms: Option<u64>,
) -> Vec<AuditEvent> {
    let limit = limit.clamp(1, 1000);
    let file = match File::open(path) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn set_env(key: &str, value: &str) -> Option<String> {
        let prev = std::env::var(key).ok();
        unsafe {
            std::env::set_var(key, value);
        }
        prev
    }

    fn restore_env(key: &str, value: Option<String>) {
        if let Some(v) = value {
            unsafe {
                std::env::set_var(key, v);
            }
        } else {
            unsafe {
                std::env::remove_var(key);
            }
        }
    }

    #[test]
    fn test_append_and_read_events() {
        let prev_async = set_env("AUDIT_ASYNC_WAL", "0");
        let prev_fsync = set_env("AUDIT_FDATASYNC", "0");

        let dir = std::env::temp_dir().join(format!(
            "audit_test_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("audit.log");

        let log = AuditLog::new(&path).unwrap();
        let event = AuditEvent {
            event_type: "OrderAccepted".to_string(),
            at: now_millis(),
            account_id: "acct-1".to_string(),
            order_id: Some("ord-1".to_string()),
            data: json!({"status":"ACCEPTED"}),
        };
        log.append(event.clone());

        let events = log.read_events("acct-1", Some("ord-1"), 10, None, None);
        assert_eq!(1, events.len());
        assert_eq!(event.event_type, events[0].event_type);

        restore_env("AUDIT_ASYNC_WAL", prev_async);
        restore_env("AUDIT_FDATASYNC", prev_fsync);
    }
}
