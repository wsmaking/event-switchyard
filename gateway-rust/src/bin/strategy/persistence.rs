use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CursorState {
    pub scope_kind: String,
    pub scope_id: String,
    pub next_cursor: u64,
}

pub fn read_json_file<T: DeserializeOwned>(path: &str) -> Result<T, String> {
    let raw = fs::read_to_string(path).map_err(|err| format!("read {path} failed: {err}"))?;
    serde_json::from_str(&raw).map_err(|err| format!("parse {path} failed: {err}"))
}

pub fn write_json_file<T: Serialize>(path: &str, value: &T) -> Result<(), String> {
    let raw = serde_json::to_string_pretty(value).map_err(|err| err.to_string())?;
    if let Some(parent) = Path::new(path).parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .map_err(|err| format!("create parent dir for {path} failed: {err}"))?;
        }
    }
    fs::write(path, raw + "\n").map_err(|err| format!("write {path} failed: {err}"))
}

pub fn load_cursor_state(
    path: &str,
    expected_scope_kind: &str,
    expected_scope_id: &str,
) -> Result<CursorState, String> {
    let state = read_json_file::<CursorState>(path)?;
    if state.scope_kind != expected_scope_kind || state.scope_id != expected_scope_id {
        return Err("cursor file scope mismatch".to_string());
    }
    Ok(state)
}

pub fn persist_cursor_state(
    path: &str,
    scope_kind: &str,
    scope_id: &str,
    next_cursor: u64,
) -> Result<(), String> {
    write_json_file(
        path,
        &CursorState {
            scope_kind: scope_kind.to_string(),
            scope_id: scope_id.to_string(),
            next_cursor,
        },
    )
}
