use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, put},
};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[allow(dead_code)]
#[path = "../strategy/market_input.rs"]
mod strategy_market_input;

use strategy_market_input::StrategyMarketInput;

const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:18082";
const DEFAULT_STATE_DIR: &str = "var/gateway/strategy-market-input";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScopeKind {
    ExecutionRunId,
    IntentId,
}

impl ScopeKind {
    fn label(self) -> &'static str {
        match self {
            Self::ExecutionRunId => "execution",
            Self::IntentId => "intent",
        }
    }

    fn from_label(label: &str) -> Option<Self> {
        match label {
            "execution" => Some(Self::ExecutionRunId),
            "intent" => Some(Self::IntentId),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct StoredMarketInput {
    scope_kind: String,
    scope_id: String,
    updated_at_ns: u64,
    input: StrategyMarketInput,
}

#[derive(Debug, Clone)]
struct ServerConfig {
    listen_addr: SocketAddr,
    state_dir: PathBuf,
}

#[derive(Clone)]
struct AppState {
    state_dir: Arc<PathBuf>,
    store: Arc<DashMap<String, StoredMarketInput>>,
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let config = parse_args()?;
    let store = load_store(&config.state_dir)?;
    let state = AppState {
        state_dir: Arc::new(config.state_dir.clone()),
        store: Arc::new(store),
    };

    let app = Router::new()
        .route("/health", get(handle_health))
        .route(
            "/alpha-input/execution/{scope_id}",
            get(handle_get_execution),
        )
        .route(
            "/alpha-input/execution/{scope_id}",
            put(handle_put_execution),
        )
        .route(
            "/alpha-input/execution/{scope_id}",
            delete(handle_delete_execution),
        )
        .route("/alpha-input/intent/{scope_id}", get(handle_get_intent))
        .route("/alpha-input/intent/{scope_id}", put(handle_put_intent))
        .route(
            "/alpha-input/intent/{scope_id}",
            delete(handle_delete_intent),
        )
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(config.listen_addr)
        .await
        .map_err(|err| format!("bind {} failed: {err}", config.listen_addr))?;
    println!(
        "strategy_market_input_server listening on {}",
        config.listen_addr
    );
    axum::serve(listener, app)
        .await
        .map_err(|err| format!("server failed: {err}"))
}

async fn handle_health() -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "OK"
    }))
}

async fn handle_get_execution(
    State(state): State<AppState>,
    Path(scope_id): Path<String>,
) -> Result<Json<StrategyMarketInput>, (StatusCode, String)> {
    get_input(state, ScopeKind::ExecutionRunId, scope_id)
}

async fn handle_put_execution(
    State(state): State<AppState>,
    Path(scope_id): Path<String>,
    Json(input): Json<StrategyMarketInput>,
) -> Result<Json<StoredMarketInput>, (StatusCode, String)> {
    put_input(state, ScopeKind::ExecutionRunId, scope_id, input)
}

async fn handle_delete_execution(
    State(state): State<AppState>,
    Path(scope_id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    delete_input(state, ScopeKind::ExecutionRunId, scope_id)
}

async fn handle_get_intent(
    State(state): State<AppState>,
    Path(scope_id): Path<String>,
) -> Result<Json<StrategyMarketInput>, (StatusCode, String)> {
    get_input(state, ScopeKind::IntentId, scope_id)
}

async fn handle_put_intent(
    State(state): State<AppState>,
    Path(scope_id): Path<String>,
    Json(input): Json<StrategyMarketInput>,
) -> Result<Json<StoredMarketInput>, (StatusCode, String)> {
    put_input(state, ScopeKind::IntentId, scope_id, input)
}

async fn handle_delete_intent(
    State(state): State<AppState>,
    Path(scope_id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    delete_input(state, ScopeKind::IntentId, scope_id)
}

fn get_input(
    state: AppState,
    kind: ScopeKind,
    scope_id: String,
) -> Result<Json<StrategyMarketInput>, (StatusCode, String)> {
    let key = market_input_key(kind, &scope_id);
    let Some(entry) = state.store.get(&key) else {
        return Err((StatusCode::NOT_FOUND, "MARKET_INPUT_NOT_FOUND".to_string()));
    };
    Ok(Json(entry.input.clone()))
}

fn put_input(
    state: AppState,
    kind: ScopeKind,
    scope_id: String,
    mut input: StrategyMarketInput,
) -> Result<Json<StoredMarketInput>, (StatusCode, String)> {
    input
        .validate()
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
    if input.observed_at_ns.is_none() {
        input.observed_at_ns = Some(now_epoch_ns());
    }
    let stored = StoredMarketInput {
        scope_kind: kind.label().to_string(),
        scope_id,
        updated_at_ns: now_epoch_ns(),
        input,
    };
    persist_input(&state.state_dir, &stored)
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err))?;
    let key = market_input_key(kind, &stored.scope_id);
    state.store.insert(key, stored.clone());
    Ok(Json(stored))
}

fn delete_input(
    state: AppState,
    kind: ScopeKind,
    scope_id: String,
) -> Result<StatusCode, (StatusCode, String)> {
    let key = market_input_key(kind, &scope_id);
    state.store.remove(&key);
    let path = market_input_path(&state.state_dir, kind, &scope_id);
    if path.exists() {
        fs::remove_file(&path).map_err(|err| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("delete {} failed: {err}", path.display()),
            )
        })?;
    }
    Ok(StatusCode::NO_CONTENT)
}

fn parse_args() -> Result<ServerConfig, String> {
    let mut listen_addr = DEFAULT_LISTEN_ADDR.to_string();
    let mut state_dir = DEFAULT_STATE_DIR.to_string();

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            "--listen" => listen_addr = next_arg(&mut args, "--listen")?,
            "--state-dir" => state_dir = next_arg(&mut args, "--state-dir")?,
            other => return Err(format!("unknown arg: {other}")),
        }
    }

    Ok(ServerConfig {
        listen_addr: listen_addr
            .parse::<SocketAddr>()
            .map_err(|err| format!("invalid --listen: {err}"))?,
        state_dir: PathBuf::from(state_dir),
    })
}

fn next_arg(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("missing value for {flag}"))
}

fn print_usage() {
    println!("strategy_market_input_server");
    println!("usage:");
    println!(
        "  cargo run --manifest-path gateway-rust/Cargo.toml --bin strategy_market_input_server -- \\"
    );
    println!("    [--listen 127.0.0.1:18082] [--state-dir var/gateway/strategy-market-input]");
}

fn load_store(state_dir: &FsPath) -> Result<DashMap<String, StoredMarketInput>, String> {
    let store = DashMap::new();
    if !state_dir.exists() {
        return Ok(store);
    }
    let entries = fs::read_dir(state_dir)
        .map_err(|err| format!("read state dir {} failed: {err}", state_dir.display()))?;
    for entry in entries {
        let entry = entry.map_err(|err| format!("read dir entry failed: {err}"))?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let raw = fs::read_to_string(&path)
            .map_err(|err| format!("read {} failed: {err}", path.display()))?;
        let stored: StoredMarketInput = serde_json::from_str(&raw)
            .map_err(|err| format!("parse {} failed: {err}", path.display()))?;
        if ScopeKind::from_label(&stored.scope_kind).is_none() {
            return Err(format!(
                "unsupported scope kind {} in {}",
                stored.scope_kind,
                path.display()
            ));
        }
        let kind = ScopeKind::from_label(&stored.scope_kind).expect("checked");
        store.insert(market_input_key(kind, &stored.scope_id), stored);
    }
    Ok(store)
}

fn persist_input(state_dir: &FsPath, stored: &StoredMarketInput) -> Result<(), String> {
    fs::create_dir_all(state_dir)
        .map_err(|err| format!("create state dir {} failed: {err}", state_dir.display()))?;
    let kind = ScopeKind::from_label(&stored.scope_kind)
        .ok_or_else(|| format!("unsupported scope kind {}", stored.scope_kind))?;
    let path = market_input_path(state_dir, kind, &stored.scope_id);
    let raw = serde_json::to_string_pretty(stored).map_err(|err| err.to_string())?;
    fs::write(&path, raw + "\n").map_err(|err| format!("write {} failed: {err}", path.display()))
}

fn market_input_path(state_dir: &FsPath, kind: ScopeKind, scope_id: &str) -> PathBuf {
    state_dir.join(format!(
        "{}_{}.json",
        kind.label(),
        sanitize_file_component(scope_id)
    ))
}

fn market_input_key(kind: ScopeKind, scope_id: &str) -> String {
    format!("{}:{scope_id}", kind.label())
}

fn sanitize_file_component(raw: &str) -> String {
    let sanitized = raw
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    if sanitized.is_empty() {
        "scope".to_string()
    } else {
        sanitized
    }
}

fn now_epoch_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::{
        ScopeKind, StoredMarketInput, load_store, market_input_key, market_input_path,
        persist_input, sanitize_file_component,
    };
    use crate::strategy_market_input::StrategyMarketInput;
    use std::fs;

    #[test]
    fn sanitize_file_component_replaces_scope_separators() {
        assert_eq!(sanitize_file_component("run::1/child"), "run__1_child");
    }

    #[test]
    fn market_input_key_includes_scope_kind() {
        assert_eq!(
            market_input_key(ScopeKind::ExecutionRunId, "run-1"),
            "execution:run-1"
        );
    }

    #[test]
    fn persist_and_load_store_round_trip() {
        let dir = std::env::temp_dir().join(format!(
            "strategy_market_input_server_{}",
            std::process::id()
        ));
        let stored = StoredMarketInput {
            scope_kind: "execution".to_string(),
            scope_id: "run-1".to_string(),
            updated_at_ns: 10,
            input: StrategyMarketInput {
                desired_signed_qty: 12,
                observed_at_ns: Some(9),
                max_decision_age_ns: Some(100),
                market_snapshot_id: Some("snap-1".to_string()),
                signal_id: Some("sig-1".to_string()),
            },
        };

        persist_input(&dir, &stored).expect("persist");
        let loaded = load_store(&dir).expect("load");
        let key = market_input_key(ScopeKind::ExecutionRunId, "run-1");
        let entry = loaded.get(&key).expect("entry");
        assert_eq!(entry.input.desired_signed_qty, 12);

        let path = market_input_path(&dir, ScopeKind::ExecutionRunId, "run-1");
        let _ = fs::remove_file(path);
        let _ = fs::remove_dir_all(dir);
    }
}
