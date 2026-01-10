//! Audit log (append-only JSONL) + reader
//!
//! Minimal implementation for order/account event history.

use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha256;
use aws_sdk_kms::primitives::Blob;
use aws_sdk_kms::Client as KmsClient;
use std::collections::{HashMap, VecDeque};
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
    anchor_path: Option<PathBuf>,
    anchor_export_dir: Option<PathBuf>,
    hmac_keys: HashMap<String, Vec<u8>>,
    active_key_id: Option<String>,
    prev_hash: Mutex<Vec<u8>>,
    seq: Mutex<u64>,
    last_key_id: Mutex<Option<String>>,
    last_anchor_day: Mutex<Option<u64>>,
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
        let (hash_path, hash_writer, hmac_keys, active_key_id, prev_hash, seq, last_key_id) = init_hash_chain(&path)?;
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
                hash: hash_b64,
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
    path: &Path
) -> std::io::Result<(Option<PathBuf>, Option<Mutex<BufWriter<File>>>, HashMap<String, Vec<u8>>, Option<String>, Vec<u8>, u64, Option<String>)> {
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
    let entry: AuditHashEntry = serde_json::from_str(line).map_err(|_| "hash entry parse failed".to_string())?;
    let hash_bytes = decode_hash(&entry.hash).unwrap_or_default();
    Ok((hash_bytes, entry.seq, Some(entry.key_id)))
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
    let active_id = std::env::var("AUDIT_KEY_ID").ok().filter(|v| !v.trim().is_empty());
    let active_id = active_id.or_else(|| keys.keys().next().cloned());
    (keys, active_id)
}

fn parse_key_value(raw: &str) -> Option<Vec<u8>> {
    let trimmed = raw.trim();
    if let Some(value) = trimmed.strip_prefix("base64:") {
        return base64::engine::general_purpose::STANDARD.decode(value.trim()).ok();
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
