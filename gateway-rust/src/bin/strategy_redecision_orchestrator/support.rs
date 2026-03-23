use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::env;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::strategy::intent::StrategyIntent;
use crate::strategy::market_input::StrategyMarketInput;
use crate::strategy::persistence::{
    CursorState as ReaderCursorState, read_json_file as read_shared_json_file,
    write_json_file as write_shared_json_file,
};
use crate::strategy::redecision_support::load_template_intent as load_shared_template_intent;

const DEFAULT_BASE_URL: &str = "http://127.0.0.1:8081";
const DEFAULT_POLL_INTERVAL_MS: u64 = 1_000;
const DEFAULT_LIMIT: usize = 500;
const DEFAULT_STATE_DIR: &str = "var/gateway/strategy-redecision-orchestrator";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(super) enum OrchestratorScopeKind {
    ExecutionRunId,
    IntentId,
}

impl OrchestratorScopeKind {
    pub(super) fn endpoint_path(self) -> &'static str {
        match self {
            Self::ExecutionRunId => "/strategy/catchup/execution",
            Self::IntentId => "/strategy/catchup/intent",
        }
    }

    pub(super) fn label(self) -> &'static str {
        match self {
            Self::ExecutionRunId => "executionRunId",
            Self::IntentId => "intentId",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(super) enum ExecutionMode {
    DryRun,
    AdaptOnly,
    Submit,
}

impl Default for ExecutionMode {
    fn default() -> Self {
        Self::DryRun
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct OrchestratorConfig {
    #[serde(default = "default_base_url")]
    pub(super) base_url: String,
    #[serde(default)]
    pub(super) market_input_base_url: Option<String>,
    #[serde(default = "default_poll_interval_ms")]
    pub(super) poll_interval_ms: u64,
    #[serde(default = "default_limit")]
    pub(super) limit: usize,
    #[serde(default)]
    pub(super) max_pages: Option<usize>,
    #[serde(default = "default_state_dir")]
    pub(super) state_dir: String,
    pub(super) runs: Vec<OrchestratorRunConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct OrchestratorRunConfig {
    pub(super) name: String,
    pub(super) scope_kind: OrchestratorScopeKind,
    pub(super) scope_id: String,
    pub(super) template_intent_path: String,
    #[serde(default)]
    pub(super) market_input_path: Option<String>,
    #[serde(default)]
    pub(super) market_input_url: Option<String>,
    #[serde(default = "default_true")]
    pub(super) enabled: bool,
    #[serde(default)]
    pub(super) target_signed_qty: Option<i64>,
    #[serde(default)]
    pub(super) limit: Option<usize>,
    #[serde(default)]
    pub(super) max_pages: Option<usize>,
    #[serde(default)]
    pub(super) execution_mode: ExecutionMode,
    #[serde(default)]
    pub(super) cursor_path: Option<String>,
    #[serde(default)]
    pub(super) state_path: Option<String>,
    #[serde(default)]
    pub(super) status_path: Option<String>,
    #[serde(default)]
    pub(super) next_intent_id: Option<String>,
    #[serde(default)]
    pub(super) next_decision_key: Option<String>,
    #[serde(default)]
    pub(super) next_created_at_ns: Option<u64>,
    #[serde(default)]
    pub(super) next_expires_at_ns: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(super) struct OrchestratorPersistentRunState {
    pub(super) scope_kind: String,
    pub(super) scope_id: String,
    pub(super) next_cursor: u64,
    #[serde(default)]
    pub(super) last_successful_intent_id: Option<String>,
    #[serde(default)]
    pub(super) last_successful_decision_key: Option<String>,
    #[serde(default)]
    pub(super) last_execution_mode: Option<ExecutionMode>,
    #[serde(default)]
    pub(super) last_successful_at_ns: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(super) struct OrchestratorExecutionResult {
    pub(super) mode: ExecutionMode,
    pub(super) executed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) adapt_http_status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) adapt_ok: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) adapt_response: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) submit_http_status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) submit_ok: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) submit_response: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) skipped_reason: Option<String>,
}

impl OrchestratorExecutionResult {
    pub(super) fn skipped(mode: ExecutionMode, reason: impl Into<String>) -> Self {
        Self {
            mode,
            executed: false,
            adapt_http_status: None,
            adapt_ok: None,
            adapt_response: None,
            submit_http_status: None,
            submit_ok: None,
            submit_response: None,
            skipped_reason: Some(reason.into()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(super) struct OrchestratorRunOutput {
    pub(super) name: String,
    pub(super) tick_at_ns: u64,
    pub(super) scope_kind: String,
    pub(super) scope_id: String,
    pub(super) next_cursor: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) market_input: Option<StrategyMarketInput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) snapshot: Option<crate::strategy::catchup::StrategyExecutionCatchupLoopSnapshot>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) recovery_context: Option<crate::strategy::alpha_redecision::AlphaRecoveryContext>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) redecision_input: Option<crate::strategy::alpha_redecision::AlphaReDecisionInput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) redecision: Option<crate::strategy::alpha_redecision::AlphaReDecision>,
    pub(super) execution_result: OrchestratorExecutionResult,
    pub(super) persisted_state: OrchestratorPersistentRunState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(super) struct OrchestratorTickOutput {
    pub(super) iteration: usize,
    pub(super) tick_at_ns: u64,
    pub(super) runs: Vec<OrchestratorRunOutput>,
}

#[derive(Debug, Clone)]
pub(super) struct OrchestratorCliConfig {
    pub(super) config_path: String,
    pub(super) run_filter: Option<String>,
    pub(super) once: bool,
    pub(super) iterations: Option<usize>,
}

#[derive(Debug, Clone)]
pub(super) struct RunPaths {
    pub(super) cursor_path: String,
    pub(super) state_path: String,
    pub(super) status_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RunScope {
    pub(super) kind: OrchestratorScopeKind,
    pub(super) id: String,
}

impl RunScope {
    pub(super) fn new(kind: OrchestratorScopeKind, id: String) -> Self {
        Self { kind, id }
    }
}

pub(super) fn selected_runs(
    config: &OrchestratorConfig,
    run_filter: Option<&str>,
) -> Result<Vec<OrchestratorRunConfig>, String> {
    let runs = config
        .runs
        .iter()
        .filter(|run| run.enabled)
        .filter(|run| run_filter.is_none_or(|filter| run.name == filter))
        .cloned()
        .collect::<Vec<_>>();
    if runs.is_empty() {
        return Err("no enabled orchestrator runs selected".to_string());
    }
    Ok(runs)
}

pub(super) fn load_config(path: &str) -> Result<OrchestratorConfig, String> {
    let config = load_json_file::<OrchestratorConfig>(path)?;
    if config.runs.is_empty() {
        return Err("runs must not be empty".to_string());
    }
    if config.poll_interval_ms == 0 {
        return Err("pollIntervalMs must be greater than zero".to_string());
    }
    for run in &config.runs {
        if run.market_input_path.is_none()
            && run.market_input_url.is_none()
            && config.market_input_base_url.is_none()
        {
            return Err(format!(
                "run {} must set marketInputPath, marketInputUrl, or config.marketInputBaseUrl",
                run.name
            ));
        }
    }
    Ok(config)
}

pub(super) fn load_template_intent(path: &str) -> Result<StrategyIntent, String> {
    load_shared_template_intent(path)
}

pub(super) fn load_json_file<T: DeserializeOwned>(path: &str) -> Result<T, String> {
    read_shared_json_file(path)
}

pub(super) fn market_input_url(
    config: &OrchestratorConfig,
    run: &OrchestratorRunConfig,
    scope: &RunScope,
) -> Result<Option<String>, String> {
    if let Some(url) = run.market_input_url.as_ref() {
        return Ok(Some(url.clone()));
    }
    let Some(base) = config.market_input_base_url.as_ref() else {
        return Ok(None);
    };
    let base = base.trim_end_matches('/');
    let scope_segment = match scope.kind {
        OrchestratorScopeKind::ExecutionRunId => "execution",
        OrchestratorScopeKind::IntentId => "intent",
    };
    Ok(Some(format!(
        "{base}/alpha-input/{scope_segment}/{}",
        encode_path_segment(&scope.id)
    )))
}

pub(super) fn resolve_run_paths(config: &OrchestratorConfig, run: &OrchestratorRunConfig) -> RunPaths {
    let state_dir = PathBuf::from(&config.state_dir);
    let stem = sanitize_file_component(&run.name);
    RunPaths {
        cursor_path: run.cursor_path.clone().unwrap_or_else(|| {
            state_dir
                .join(format!("{stem}.cursor.json"))
                .to_string_lossy()
                .to_string()
        }),
        state_path: run.state_path.clone().unwrap_or_else(|| {
            state_dir
                .join(format!("{stem}.state.json"))
                .to_string_lossy()
                .to_string()
        }),
        status_path: run.status_path.clone().unwrap_or_else(|| {
            state_dir
                .join(format!("{stem}.status.json"))
                .to_string_lossy()
                .to_string()
        }),
    }
}

pub(super) fn sanitize_file_component(raw: &str) -> String {
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
        "run".to_string()
    } else {
        sanitized
    }
}

pub(super) fn encode_path_segment(raw: &str) -> String {
    let mut encoded = String::new();
    for byte in raw.bytes() {
        let ch = byte as char;
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | '~') {
            encoded.push(ch);
        } else {
            encoded.push('%');
            encoded.push_str(&format!("{byte:02X}"));
        }
    }
    encoded
}

pub(super) fn load_persistent_state(
    path: &str,
    scope: &RunScope,
) -> Result<OrchestratorPersistentRunState, String> {
    let state = load_json_file::<OrchestratorPersistentRunState>(path)?;
    if state.scope_kind != scope.kind.label() || state.scope_id != scope.id {
        return Err("persistent state scope mismatch".to_string());
    }
    Ok(state)
}

pub(super) fn default_persistent_state(scope: &RunScope) -> OrchestratorPersistentRunState {
    OrchestratorPersistentRunState {
        scope_kind: scope.kind.label().to_string(),
        scope_id: scope.id.clone(),
        next_cursor: 0,
        last_successful_intent_id: None,
        last_successful_decision_key: None,
        last_execution_mode: None,
        last_successful_at_ns: None,
    }
}

pub(super) fn persist_artifacts(
    paths: &RunPaths,
    scope: &RunScope,
    state: &OrchestratorPersistentRunState,
    output: &OrchestratorRunOutput,
) -> Result<(), String> {
    persist_json_file(&paths.state_path, state)?;
    persist_json_file(
        &paths.cursor_path,
        &ReaderCursorState {
            scope_kind: scope.kind.label().to_string(),
            scope_id: scope.id.clone(),
            next_cursor: state.next_cursor,
        },
    )?;
    persist_json_file(&paths.status_path, output)
}

pub(super) fn persist_json_file<T: Serialize>(path: &str, value: &T) -> Result<(), String> {
    write_shared_json_file(path, value)
}

pub(super) fn parse_args() -> Result<OrchestratorCliConfig, String> {
    let mut config_path = None;
    let mut run_filter = None;
    let mut once = false;
    let mut iterations = None;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            "--config" => config_path = Some(next_arg(&mut args, "--config")?),
            "--run" => run_filter = Some(next_arg(&mut args, "--run")?),
            "--once" => once = true,
            "--iterations" => iterations = Some(parse_usize_arg(&mut args, "--iterations")?),
            other => return Err(format!("unknown arg: {other}")),
        }
    }

    let Some(config_path) = config_path else {
        return Err("--config is required".to_string());
    };

    Ok(OrchestratorCliConfig {
        config_path,
        run_filter,
        once,
        iterations,
    })
}

fn next_arg(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("missing value for {flag}"))
}

fn parse_usize_arg(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<usize, String> {
    next_arg(args, flag)?
        .parse::<usize>()
        .map_err(|err| format!("invalid value for {flag}: {err}"))
}

fn print_usage() {
    println!("strategy_redecision_orchestrator");
    println!("usage:");
    println!(
        "  cargo run --manifest-path gateway-rust/Cargo.toml --bin strategy_redecision_orchestrator -- \\"
    );
    println!("    --config contracts/fixtures/strategy_redecision_orchestrator_v1.json \\");
    println!("    [--run alpha-run-1] [--once] [--iterations 10]");
}

pub(super) fn now_epoch_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

fn default_base_url() -> String {
    DEFAULT_BASE_URL.to_string()
}

fn default_poll_interval_ms() -> u64 {
    DEFAULT_POLL_INTERVAL_MS
}

fn default_limit() -> usize {
    DEFAULT_LIMIT
}

fn default_state_dir() -> String {
    DEFAULT_STATE_DIR.to_string()
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::{
        ExecutionMode, OrchestratorConfig, OrchestratorPersistentRunState, OrchestratorRunConfig,
        OrchestratorScopeKind, RunScope, encode_path_segment, load_config, load_persistent_state,
        market_input_url, persist_json_file, resolve_run_paths, sanitize_file_component,
    };
    use crate::strategy::http_client::parse_http_base_url;
    use crate::strategy::market_input::StrategyMarketInput;
    use serde_json::json;
    use std::fs;

    #[test]
    fn sanitize_file_component_replaces_slashes() {
        assert_eq!(sanitize_file_component("alpha/run:1"), "alpha_run_1");
    }

    #[test]
    fn resolve_run_paths_defaults_under_state_dir() {
        let config = OrchestratorConfig {
            base_url: "http://127.0.0.1:8081".to_string(),
            market_input_base_url: None,
            poll_interval_ms: 1_000,
            limit: 500,
            max_pages: None,
            state_dir: "var/test/orchestrator".to_string(),
            runs: vec![],
        };
        let run = OrchestratorRunConfig {
            name: "alpha/run-1".to_string(),
            scope_kind: OrchestratorScopeKind::ExecutionRunId,
            scope_id: "run-1".to_string(),
            template_intent_path: "intent.json".to_string(),
            market_input_path: Some("market.json".to_string()),
            market_input_url: None,
            enabled: true,
            target_signed_qty: None,
            limit: None,
            max_pages: None,
            execution_mode: ExecutionMode::DryRun,
            cursor_path: None,
            state_path: None,
            status_path: None,
            next_intent_id: None,
            next_decision_key: None,
            next_created_at_ns: None,
            next_expires_at_ns: None,
        };

        let paths = resolve_run_paths(&config, &run);
        assert!(paths.cursor_path.ends_with("alpha_run-1.cursor.json"));
        assert!(paths.state_path.ends_with("alpha_run-1.state.json"));
        assert!(paths.status_path.ends_with("alpha_run-1.status.json"));
    }

    #[test]
    fn load_persistent_state_rejects_scope_mismatch() {
        let path = std::env::temp_dir().join(format!(
            "strategy_redecision_orchestrator_state_{}.json",
            std::process::id()
        ));
        let path_string = path.to_string_lossy().to_string();
        persist_json_file(
            &path_string,
            &OrchestratorPersistentRunState {
                scope_kind: "intentId".to_string(),
                scope_id: "intent-1".to_string(),
                next_cursor: 7,
                last_successful_intent_id: None,
                last_successful_decision_key: None,
                last_execution_mode: None,
                last_successful_at_ns: None,
            },
        )
        .expect("persist state");

        let err = load_persistent_state(
            &path_string,
            &RunScope::new(OrchestratorScopeKind::ExecutionRunId, "run-1".to_string()),
        )
        .expect_err("scope mismatch must fail");
        assert_eq!(err, "persistent state scope mismatch");
        let _ = fs::remove_file(path);
    }

    #[test]
    fn market_input_round_trips_json() {
        let raw = json!({
            "desiredSignedQty": 60,
            "observedAtNs": 1000,
            "maxDecisionAgeNs": 100,
            "marketSnapshotId": "snap-1",
            "signalId": "signal-1"
        });
        let parsed: StrategyMarketInput =
            serde_json::from_value(raw).expect("market input should deserialize");
        assert_eq!(parsed.desired_signed_qty, 60);
        assert_eq!(parsed.observed_at_ns, Some(1000));
    }

    #[test]
    fn encode_path_segment_percent_encodes_separators() {
        assert_eq!(encode_path_segment("run::1/a"), "run%3A%3A1%2Fa");
    }

    #[test]
    fn market_input_url_uses_config_base_url_and_scope() {
        let config = OrchestratorConfig {
            base_url: "http://127.0.0.1:8081".to_string(),
            market_input_base_url: Some("http://127.0.0.1:18082".to_string()),
            poll_interval_ms: 1_000,
            limit: 500,
            max_pages: None,
            state_dir: "var/test/orchestrator".to_string(),
            runs: vec![],
        };
        let run = OrchestratorRunConfig {
            name: "alpha".to_string(),
            scope_kind: OrchestratorScopeKind::ExecutionRunId,
            scope_id: "run::1".to_string(),
            template_intent_path: "intent.json".to_string(),
            market_input_path: None,
            market_input_url: None,
            enabled: true,
            target_signed_qty: None,
            limit: None,
            max_pages: None,
            execution_mode: ExecutionMode::DryRun,
            cursor_path: None,
            state_path: None,
            status_path: None,
            next_intent_id: None,
            next_decision_key: None,
            next_created_at_ns: None,
            next_expires_at_ns: None,
        };

        let url = market_input_url(
            &config,
            &run,
            &RunScope::new(OrchestratorScopeKind::ExecutionRunId, "run::1".to_string()),
        )
        .expect("url")
        .expect("must be present");
        assert_eq!(
            url,
            "http://127.0.0.1:18082/alpha-input/execution/run%3A%3A1"
        );
    }

    #[test]
    fn load_config_rejects_missing_market_input_source() {
        let path = std::env::temp_dir().join(format!(
            "strategy_redecision_orchestrator_config_{}.json",
            std::process::id()
        ));
        let raw = json!({
            "baseUrl": "http://127.0.0.1:8081",
            "pollIntervalMs": 1000,
            "limit": 500,
            "stateDir": "var/test/orchestrator",
            "runs": [{
                "name": "alpha",
                "scopeKind": "EXECUTION_RUN_ID",
                "scopeId": "run-1",
                "templateIntentPath": "intent.json"
            }]
        });
        fs::write(&path, serde_json::to_string(&raw).expect("json")).expect("write config");
        let path_string = path.to_string_lossy().to_string();
        let err = load_config(&path_string).expect_err("config must fail");
        assert!(err.contains("marketInputPath, marketInputUrl, or config.marketInputBaseUrl"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn parse_http_base_url_accepts_market_input_url_shape() {
        let parsed = parse_http_base_url("http://127.0.0.1:18082/alpha-input/execution/run-1")
            .expect("url should parse");
        assert_eq!(parsed.host, "127.0.0.1");
        assert_eq!(parsed.port, 18082);
        assert_eq!(parsed.base_path, "/alpha-input/execution/run-1");
    }
}
