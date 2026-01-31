//! Audit mirror worker (tail WAL audit log and append to audit mirror).
//!
//! Purpose:
//! - Keep WAL minimal and sync-friendly.
//! - Generate a separate audit log asynchronously, replayable from WAL.

use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct AuditMirrorWorker {
    wal_path: PathBuf,
    mirror_path: PathBuf,
    offset_path: PathBuf,
    hash_path: PathBuf,
    anchor_path: PathBuf,
    anchor_export_dir: Option<PathBuf>,
    hmac_keys: HashMap<String, Vec<u8>>,
    active_key_id: Option<String>,
}

impl AuditMirrorWorker {
    pub fn new(
        wal_path: impl AsRef<Path>,
        mirror_path: impl AsRef<Path>,
        offset_path: impl AsRef<Path>,
    ) -> Self {
        let mirror_path = mirror_path.as_ref().to_path_buf();
        let hash_path = std::env::var("AUDIT_MIRROR_HASH_PATH")
            .ok()
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                let mut s = mirror_path.to_string_lossy().to_string();
                s.push_str(".hash");
                PathBuf::from(s)
            });
        let anchor_path = std::env::var("AUDIT_MIRROR_ANCHOR_PATH")
            .ok()
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                let mut s = mirror_path.to_string_lossy().to_string();
                s.push_str(".anchor");
                PathBuf::from(s)
            });
        let anchor_export_dir = std::env::var("AUDIT_MIRROR_ANCHOR_EXPORT_DIR")
            .ok()
            .map(PathBuf::from);
        let (hmac_keys, active_key_id) = load_audit_keys();
        Self {
            wal_path: wal_path.as_ref().to_path_buf(),
            mirror_path,
            offset_path: offset_path.as_ref().to_path_buf(),
            hash_path,
            anchor_path,
            anchor_export_dir,
            hmac_keys,
            active_key_id,
        }
    }

    pub fn start(self) {
        thread::spawn(move || self.run());
    }

    fn run(self) {
        let mut offset = read_offset(&self.offset_path);
        let base_sleep_ms = env_u64("AUDIT_MIRROR_BACKOFF_BASE_MS", 200);
        let max_sleep_ms = env_u64("AUDIT_MIRROR_BACKOFF_MAX_MS", 5000);
        let mut backoff_ms = base_sleep_ms;
        let (mut seq, mut prev_hash, mut last_key_id, mut last_anchor_day) =
            read_hash_state(&self.hash_path, &self.anchor_path);

        loop {
            let file = match File::open(&self.wal_path) {
                Ok(f) => f,
                Err(_) => {
                    backoff_ms = (backoff_ms * 2).min(max_sleep_ms);
                    thread::sleep(Duration::from_millis(backoff_ms));
                    continue;
                }
            };

            if let Ok(meta) = file.metadata() {
                if offset > meta.len() {
                    offset = 0;
                    truncate_file(&self.mirror_path);
                    truncate_file(&self.hash_path);
                    truncate_file(&self.anchor_path);
                    seq = 0;
                    prev_hash = Vec::new();
                    last_key_id = None;
                    last_anchor_day = None;
                }
            }

            let mut reader = BufReader::new(file);
            if reader.seek(SeekFrom::Start(offset)).is_err() {
                offset = 0;
                let _ = reader.seek(SeekFrom::Start(0));
                truncate_file(&self.mirror_path);
            }

            let mut mirror = match OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.mirror_path)
            {
                Ok(f) => f,
                Err(_) => {
                    backoff_ms = (backoff_ms * 2).min(max_sleep_ms);
                    thread::sleep(Duration::from_millis(backoff_ms));
                    continue;
                }
            };
            let mut hash_writer = match OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.hash_path)
            {
                Ok(f) => Some(f),
                Err(_) => None,
            };

            let mut progressed = false;
            let mut had_error = false;
            let mut line = String::new();

            loop {
                line.clear();
                let prev_offset = offset;
                match reader.read_line(&mut line) {
                    Ok(0) => break,
                    Ok(n) => {
                        let next_offset = prev_offset + n as u64;
                        let line_bytes = line.as_bytes();
                        if mirror.write_all(line_bytes).is_ok() {
                            if let Some(writer) = hash_writer.as_mut() {
                                if let Some((entry_json, hash_bytes, key_id, at_ms)) =
                                    build_hash_entry(
                                        &self.hmac_keys,
                                        &self.active_key_id,
                                        &prev_hash,
                                        line_bytes,
                                        seq + 1,
                                    )
                                {
                                    if writer.write_all(entry_json.as_bytes()).is_ok() {
                                        prev_hash = hash_bytes;
                                        seq += 1;
                                        last_key_id = Some(key_id);
                                        if let Some(anchor_day) = write_anchor_if_needed(
                                            &self.anchor_path,
                                            self.anchor_export_dir.as_deref(),
                                            seq,
                                            at_ms,
                                            &prev_hash,
                                            last_key_id.as_deref(),
                                            last_anchor_day,
                                        ) {
                                            last_anchor_day = Some(anchor_day);
                                        }
                                    }
                                }
                            }
                            offset = next_offset;
                            progressed = true;
                        } else {
                            had_error = true;
                            break;
                        }
                    }
                    Err(_) => {
                        had_error = true;
                        break;
                    }
                }
            }

            if progressed {
                let _ = mirror.flush();
                if let Some(writer) = hash_writer.as_mut() {
                    let _ = writer.flush();
                }
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

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

fn load_audit_keys() -> (HashMap<String, Vec<u8>>, Option<String>) {
    let mut keys = HashMap::new();
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
        return base64::engine::general_purpose::STANDARD.decode(value.trim()).ok();
    }
    Some(trimmed.as_bytes().to_vec())
}

fn truncate_file(path: &Path) {
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Ok(mut f) = OpenOptions::new().create(true).write(true).truncate(true).open(path) {
        let _ = f.flush();
        let _ = f.sync_all();
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
    trimmed.parse::<u64>().unwrap_or(0)
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

fn temp_offset_path(path: &Path) -> PathBuf {
    let mut tmp = path.to_path_buf();
    if let Some(name) = path.file_name() {
        let mut tmp_name = name.to_os_string();
        tmp_name.push(".tmp");
        tmp.set_file_name(tmp_name);
    }
    tmp
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct AuditHashEntry {
    seq: u64,
    key_id: String,
    prev_hash: String,
    hash: String,
}

fn compute_hmac(key: &[u8], prev_hash: &[u8], line_bytes: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC key invalid");
    mac.update(prev_hash);
    mac.update(line_bytes);
    mac.finalize().into_bytes().to_vec()
}

fn build_hash_entry(
    keys: &HashMap<String, Vec<u8>>,
    active_key_id: &Option<String>,
    prev_hash: &[u8],
    line_bytes: &[u8],
    seq: u64,
) -> Option<(String, Vec<u8>, String, u64)> {
    let key_id = active_key_id.as_ref()?;
    let key = keys.get(key_id)?;
    let hash = compute_hmac(key, prev_hash, line_bytes);
    let entry = AuditHashEntry {
        seq,
        key_id: key_id.clone(),
        prev_hash: base64::engine::general_purpose::STANDARD.encode(prev_hash),
        hash: base64::engine::general_purpose::STANDARD.encode(&hash),
    };
    let mut line = serde_json::to_string(&entry).ok()?;
    line.push('\n');
    Some((line, hash, key_id.clone(), now_millis()))
}

fn read_hash_state(hash_path: &Path, anchor_path: &Path) -> (u64, Vec<u8>, Option<String>, Option<u64>) {
    let file = match File::open(hash_path) {
        Ok(f) => f,
        Err(_) => return (0, Vec::new(), None, None),
    };
    let reader = BufReader::new(file);
    let mut last_entry: Option<AuditHashEntry> = None;
    for line in reader.lines().flatten() {
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(entry) = serde_json::from_str::<AuditHashEntry>(&line) {
            last_entry = Some(entry);
        }
    }
    if let Some(entry) = last_entry {
        let prev_hash = base64::engine::general_purpose::STANDARD
            .decode(entry.hash)
            .unwrap_or_default();
        let now_ms = now_millis();
        let anchor_day = days_since_epoch(now_ms);
        let anchor = AuditAnchor {
            seq: entry.seq,
            at: now_ms,
            key_id: entry.key_id.clone(),
            hash: base64::engine::general_purpose::STANDARD.encode(&prev_hash),
        };
        let _ = write_anchor(anchor_path, &anchor);
        return (entry.seq, prev_hash, Some(entry.key_id), Some(anchor_day));
    }
    (0, Vec::new(), None, None)
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct AuditAnchor {
    seq: u64,
    at: u64,
    key_id: String,
    hash: String,
}

fn write_anchor_if_needed(
    anchor_path: &Path,
    export_dir: Option<&Path>,
    seq: u64,
    at_ms: u64,
    hash: &[u8],
    key_id: Option<&str>,
    last_anchor_day: Option<u64>,
) -> Option<u64> {
    let key_id = key_id.unwrap_or("unknown");
    let day = days_since_epoch(at_ms);
    if last_anchor_day.is_some_and(|d| d == day) {
        return None;
    }
    let anchor = AuditAnchor {
        seq,
        at: at_ms,
        key_id: key_id.to_string(),
        hash: base64::engine::general_purpose::STANDARD.encode(hash),
    };
    let _ = write_anchor(anchor_path, &anchor);
    if let Some(dir) = export_dir {
        let _ = write_anchor_export(dir, day, &anchor);
    }
    Some(day)
}

fn write_anchor(path: &Path, anchor: &AuditAnchor) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp_path = temp_offset_path(path);
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

fn write_anchor_export(dir: &Path, day: u64, anchor: &AuditAnchor) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let path = dir.join(anchor_filename(day));
    let tmp_path = temp_offset_path(&path);
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

fn days_since_epoch(ms: u64) -> u64 {
    ms / 1000 / 86400
}

fn anchor_filename(day: u64) -> String {
    format!("audit.anchor.{day}")
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
