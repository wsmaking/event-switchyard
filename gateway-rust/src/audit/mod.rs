//! Audit log (append-only JSONL) + reader
//!
//! Minimal implementation for order/account event history.

use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha256;
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
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
    hash_path: Option<PathBuf>,
    hash_writer: Option<Mutex<BufWriter<File>>>,
    hmac_key: Option<Vec<u8>>,
    key_id: Option<String>,
    prev_hash: Mutex<Vec<u8>>,
    seq: Mutex<u64>,
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
        let (hash_path, hash_writer, hmac_key, key_id, prev_hash, seq) = init_hash_chain(&path)?;
        Ok(Self {
            path,
            writer: Mutex::new(BufWriter::new(file)),
            hash_path,
            hash_writer,
            hmac_key,
            key_id,
            prev_hash: Mutex::new(prev_hash),
            seq: Mutex::new(seq),
        })
    }

    pub fn append(&self, event: AuditEvent) {
        if let Ok(mut writer) = self.writer.lock() {
            if let Ok(line) = serde_json::to_string(&event) {
                let mut line_bytes = line.into_bytes();
                line_bytes.push(b'\n');
                let _ = writer.write_all(&line_bytes);
                let _ = writer.flush();
                self.append_hash_chain(&line_bytes);
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

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn verify(&self, from_seq: u64, limit: usize) -> AuditVerifyResult {
        let key = match &self.hmac_key {
            Some(k) => k,
            None => {
                return AuditVerifyResult {
                    ok: false,
                    checked: 0,
                    last_seq: 0,
                    error: Some("AUDIT_HMAC_KEY not configured".into()),
                };
            }
        };
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
        let (key, writer, key_id) = match (&self.hmac_key, &self.hash_writer, &self.key_id) {
            (Some(k), Some(w), Some(id)) => (k, w, id),
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

fn init_hash_chain(path: &Path) -> std::io::Result<(Option<PathBuf>, Option<Mutex<BufWriter<File>>>, Option<Vec<u8>>, Option<String>, Vec<u8>, u64)> {
    let key = match std::env::var("AUDIT_HMAC_KEY") {
        Ok(v) if !v.trim().is_empty() => v.into_bytes(),
        _ => {
            return Ok((None, None, None, None, Vec::new(), 0));
        }
    };
    let key_id = std::env::var("AUDIT_KEY_ID").unwrap_or_else(|_| "key-1".into());
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
    let (prev_hash, seq) = match load_last_hash(&hash_path) {
        Ok(v) => v,
        Err(msg) => {
            let _ = backup_corrupt_hash(&hash_path);
            eprintln!("audit hash invalid: {} (resetting chain)", msg);
            (Vec::new(), 0)
        }
    };
    Ok((
        Some(hash_path),
        Some(Mutex::new(BufWriter::new(hash_file))),
        Some(key),
        Some(key_id),
        prev_hash,
        seq,
    ))
}

fn load_last_hash(path: &Path) -> Result<(Vec<u8>, u64), String> {
    let mut file = File::open(path).map_err(|e| e.to_string())?;
    let len = file.metadata().map_err(|e| e.to_string())?.len();
    if len == 0 {
        return Ok((Vec::new(), 0));
    }
    let read_len = len.min(8192);
    file.seek(SeekFrom::End(-(read_len as i64)))
        .map_err(|e| e.to_string())?;
    let mut buf = vec![0u8; read_len as usize];
    file.read_exact(&mut buf).map_err(|e| e.to_string())?;
    let text = String::from_utf8_lossy(&buf);
    let line = match text.lines().rev().find(|l| !l.trim().is_empty()) {
        Some(l) => l,
        None => return Ok((Vec::new(), 0)),
    };
    let entry: AuditHashEntry = serde_json::from_str(line).map_err(|_| "hash entry parse failed".to_string())?;
    let hash_bytes = decode_hash(&entry.hash).unwrap_or_default();
    Ok((hash_bytes, entry.seq))
}

fn decode_hash(value: &str) -> Option<Vec<u8>> {
    base64::engine::general_purpose::STANDARD.decode(value.trim()).ok()
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
