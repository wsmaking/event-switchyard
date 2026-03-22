use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, sleep};

#[allow(dead_code)]
#[path = "../order/mod.rs"]
mod order;
mod strategy;

use strategy::alpha_redecision::{
    AlphaMarketContext, AlphaNextIntentParams, AlphaReDecision, AlphaReDecisionAction,
    AlphaReDecisionInput, AlphaRecoveryContext,
};
use strategy::catchup::{
    StrategyExecutionCatchupLoop, StrategyExecutionCatchupLoopSnapshot,
    target_signed_qty_for_intent,
};
use strategy::intent::StrategyIntent;
use strategy::replay::StrategyExecutionCatchupInput;

const DEFAULT_BASE_URL: &str = "http://127.0.0.1:8081";
const DEFAULT_POLL_INTERVAL_MS: u64 = 1_000;
const DEFAULT_LIMIT: usize = 500;
const DEFAULT_STATE_DIR: &str = "var/gateway/strategy-redecision-orchestrator";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum OrchestratorScopeKind {
    ExecutionRunId,
    IntentId,
}

impl OrchestratorScopeKind {
    fn endpoint_path(self) -> &'static str {
        match self {
            Self::ExecutionRunId => "/strategy/catchup/execution",
            Self::IntentId => "/strategy/catchup/intent",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::ExecutionRunId => "executionRunId",
            Self::IntentId => "intentId",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum ExecutionMode {
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
struct OrchestratorConfig {
    #[serde(default = "default_base_url")]
    base_url: String,
    #[serde(default = "default_poll_interval_ms")]
    poll_interval_ms: u64,
    #[serde(default = "default_limit")]
    limit: usize,
    #[serde(default)]
    max_pages: Option<usize>,
    #[serde(default = "default_state_dir")]
    state_dir: String,
    runs: Vec<OrchestratorRunConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrchestratorRunConfig {
    name: String,
    scope_kind: OrchestratorScopeKind,
    scope_id: String,
    template_intent_path: String,
    market_input_path: String,
    #[serde(default = "default_true")]
    enabled: bool,
    #[serde(default)]
    target_signed_qty: Option<i64>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    max_pages: Option<usize>,
    #[serde(default)]
    execution_mode: ExecutionMode,
    #[serde(default)]
    cursor_path: Option<String>,
    #[serde(default)]
    state_path: Option<String>,
    #[serde(default)]
    status_path: Option<String>,
    #[serde(default)]
    next_intent_id: Option<String>,
    #[serde(default)]
    next_decision_key: Option<String>,
    #[serde(default)]
    next_created_at_ns: Option<u64>,
    #[serde(default)]
    next_expires_at_ns: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct ReaderCursorState {
    scope_kind: String,
    scope_id: String,
    next_cursor: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct OrchestratorPersistentRunState {
    scope_kind: String,
    scope_id: String,
    next_cursor: u64,
    #[serde(default)]
    last_successful_intent_id: Option<String>,
    #[serde(default)]
    last_successful_decision_key: Option<String>,
    #[serde(default)]
    last_execution_mode: Option<ExecutionMode>,
    #[serde(default)]
    last_successful_at_ns: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct MarketInputFile {
    desired_signed_qty: i64,
    #[serde(default)]
    observed_at_ns: Option<u64>,
    #[serde(default)]
    max_decision_age_ns: Option<u64>,
    #[serde(default)]
    market_snapshot_id: Option<String>,
    #[serde(default)]
    signal_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct OrchestratorExecutionResult {
    mode: ExecutionMode,
    executed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    adapt_http_status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    adapt_ok: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    adapt_response: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    submit_http_status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    submit_ok: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    submit_response: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    skipped_reason: Option<String>,
}

impl OrchestratorExecutionResult {
    fn skipped(mode: ExecutionMode, reason: impl Into<String>) -> Self {
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
struct OrchestratorRunOutput {
    name: String,
    tick_at_ns: u64,
    scope_kind: String,
    scope_id: String,
    next_cursor: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    market_input: Option<MarketInputFile>,
    #[serde(skip_serializing_if = "Option::is_none")]
    snapshot: Option<StrategyExecutionCatchupLoopSnapshot>,
    #[serde(skip_serializing_if = "Option::is_none")]
    recovery_context: Option<AlphaRecoveryContext>,
    #[serde(skip_serializing_if = "Option::is_none")]
    redecision_input: Option<AlphaReDecisionInput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    redecision: Option<AlphaReDecision>,
    execution_result: OrchestratorExecutionResult,
    persisted_state: OrchestratorPersistentRunState,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct OrchestratorTickOutput {
    iteration: usize,
    tick_at_ns: u64,
    runs: Vec<OrchestratorRunOutput>,
}

#[derive(Debug, Clone)]
struct OrchestratorCliConfig {
    config_path: String,
    run_filter: Option<String>,
    once: bool,
    iterations: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedHttpBaseUrl {
    host: String,
    port: u16,
    base_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SimpleHttpResponse {
    status_code: u16,
    body: Vec<u8>,
}

#[derive(Debug, Clone)]
struct RunPaths {
    cursor_path: String,
    state_path: String,
    status_path: String,
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let cli = parse_args()?;
    let config = load_config(&cli.config_path)?;
    let runs = selected_runs(&config, cli.run_filter.as_deref())?;

    if cli.once {
        let output = run_tick(&config, &runs, 1).await?;
        let raw = serde_json::to_string_pretty(&output).map_err(|err| err.to_string())?;
        println!("{raw}");
        return Ok(());
    }

    let mut iteration = 0usize;
    loop {
        if let Some(limit) = cli.iterations {
            if iteration >= limit {
                break;
            }
        }
        iteration = iteration.saturating_add(1);
        let output = run_tick(&config, &runs, iteration).await?;
        let raw = serde_json::to_string(&output).map_err(|err| err.to_string())?;
        println!("{raw}");

        if let Some(limit) = cli.iterations {
            if iteration >= limit {
                break;
            }
        }
        sleep(Duration::from_millis(config.poll_interval_ms.max(1))).await;
    }

    Ok(())
}

async fn run_tick(
    config: &OrchestratorConfig,
    runs: &[OrchestratorRunConfig],
    iteration: usize,
) -> Result<OrchestratorTickOutput, String> {
    let tick_at_ns = now_epoch_ns();
    let mut outputs = Vec::with_capacity(runs.len());
    for run in runs {
        outputs.push(process_run(config, run, tick_at_ns).await?);
    }
    Ok(OrchestratorTickOutput {
        iteration,
        tick_at_ns,
        runs: outputs,
    })
}

async fn process_run(
    config: &OrchestratorConfig,
    run: &OrchestratorRunConfig,
    tick_at_ns: u64,
) -> Result<OrchestratorRunOutput, String> {
    let scope = RunScope::new(run.scope_kind, run.scope_id.clone());
    let paths = resolve_run_paths(config, run);
    let mut state = load_persistent_state(&paths.state_path, &scope)
        .unwrap_or_else(|_| default_persistent_state(&scope));
    let mut output = OrchestratorRunOutput {
        name: run.name.clone(),
        tick_at_ns,
        scope_kind: scope.kind.label().to_string(),
        scope_id: scope.id.clone(),
        next_cursor: state.next_cursor,
        market_input: None,
        snapshot: None,
        recovery_context: None,
        redecision_input: None,
        redecision: None,
        execution_result: OrchestratorExecutionResult::skipped(run.execution_mode, "NOT_EVALUATED"),
        persisted_state: state.clone(),
        error: None,
    };

    let template_intent = match load_template_intent(&run.template_intent_path) {
        Ok(intent) => intent,
        Err(err) => {
            output.error = Some(err);
            persist_artifacts(&paths, &scope, &state, &output)?;
            return Ok(output);
        }
    };
    if scope.kind == OrchestratorScopeKind::ExecutionRunId {
        if let Some(run_id) = template_intent.execution_run_id.as_deref() {
            if run_id != scope.id {
                output.error = Some("TEMPLATE_EXECUTION_RUN_ID_SCOPE_MISMATCH".to_string());
                persist_artifacts(&paths, &scope, &state, &output)?;
                return Ok(output);
            }
        }
    }

    let limit = run.limit.unwrap_or(config.limit).max(1);
    let max_pages = run.max_pages.or(config.max_pages);
    let fetch = fetch_catchup_snapshot(
        &config.base_url,
        &scope,
        state.next_cursor,
        limit,
        max_pages,
    )
    .await;
    let (snapshot, next_cursor, incomplete_reason) = match fetch {
        Ok(value) => value,
        Err(err) => {
            output.error = Some(err);
            persist_artifacts(&paths, &scope, &state, &output)?;
            return Ok(output);
        }
    };
    state.next_cursor = next_cursor;
    output.next_cursor = next_cursor;
    output.snapshot = Some(snapshot.clone());

    let target_signed_qty = match run.target_signed_qty {
        Some(value) => value,
        None => match target_signed_qty_for_intent(&template_intent) {
            Ok(value) => value,
            Err(err) => {
                output.error = Some(err);
                output.persisted_state = state.clone();
                persist_artifacts(&paths, &scope, &state, &output)?;
                return Ok(output);
            }
        },
    };
    let recovery_context = AlphaRecoveryContext::from_snapshot(snapshot.clone(), target_signed_qty);
    output.recovery_context = Some(recovery_context.clone());

    if let Some(reason) = incomplete_reason {
        output.execution_result =
            OrchestratorExecutionResult::skipped(run.execution_mode, "CATCHUP_INCOMPLETE");
        output.error = Some(reason);
        output.persisted_state = state.clone();
        persist_artifacts(&paths, &scope, &state, &output)?;
        return Ok(output);
    }

    let market_input = match load_json_file::<MarketInputFile>(&run.market_input_path) {
        Ok(value) => value,
        Err(err) => {
            output.execution_result = OrchestratorExecutionResult::skipped(
                run.execution_mode,
                "MARKET_INPUT_NOT_AVAILABLE",
            );
            output.error = Some(format!("market input unavailable: {err}"));
            output.persisted_state = state.clone();
            persist_artifacts(&paths, &scope, &state, &output)?;
            return Ok(output);
        }
    };
    output.market_input = Some(market_input.clone());

    let redecision_input = match build_redecision_input(
        &template_intent,
        &recovery_context,
        run,
        &market_input,
        tick_at_ns,
    ) {
        Ok(input) => input,
        Err(err) => {
            output.execution_result = OrchestratorExecutionResult::skipped(
                run.execution_mode,
                "INVALID_REDECISION_INPUT",
            );
            output.error = Some(err);
            output.persisted_state = state.clone();
            persist_artifacts(&paths, &scope, &state, &output)?;
            return Ok(output);
        }
    };
    output.redecision_input = Some(redecision_input.clone());

    let redecision = AlphaReDecision::evaluate(redecision_input, tick_at_ns);
    output.redecision = Some(redecision.clone());

    let execution_result =
        execute_redecision_flow(&config.base_url, &redecision, run.execution_mode, &state).await?;
    if execution_result.executed {
        if let Some(intent) = redecision.proposed_intent.as_ref() {
            state.last_successful_intent_id = Some(intent.intent_id.clone());
            state.last_successful_decision_key = intent.decision_key.clone();
            state.last_execution_mode = Some(run.execution_mode);
            state.last_successful_at_ns = Some(tick_at_ns);
        }
    }
    output.execution_result = execution_result;
    output.persisted_state = state.clone();
    persist_artifacts(&paths, &scope, &state, &output)?;
    Ok(output)
}

async fn fetch_catchup_snapshot(
    base_url: &str,
    scope: &RunScope,
    start_cursor: u64,
    limit: usize,
    max_pages: Option<usize>,
) -> Result<(StrategyExecutionCatchupLoopSnapshot, u64, Option<String>), String> {
    let mut loop_state = StrategyExecutionCatchupLoop::new();
    let mut next_cursor = start_cursor;
    let mut pages_read = 0usize;
    let mut incomplete_reason = None;

    loop {
        if let Some(max_pages) = max_pages {
            if pages_read >= max_pages {
                if loop_state.has_more() {
                    incomplete_reason = Some("CATCHUP_MAX_PAGES_REACHED".to_string());
                }
                break;
            }
        }

        let page = fetch_catchup_page(base_url, scope, next_cursor, limit).await?;
        loop_state
            .apply_page(&page)
            .map_err(|err| format!("{err:?}"))?;
        next_cursor = loop_state.next_cursor();
        pages_read = pages_read.saturating_add(1);

        if !page.has_more || !loop_state.has_more() || page.fact_count == 0 {
            break;
        }
    }

    let snapshot = loop_state.snapshot();
    Ok((snapshot, next_cursor, incomplete_reason))
}

fn build_redecision_input(
    template: &StrategyIntent,
    recovery: &AlphaRecoveryContext,
    run: &OrchestratorRunConfig,
    market_input: &MarketInputFile,
    now_ns: u64,
) -> Result<AlphaReDecisionInput, String> {
    let market = build_market_context(template, market_input, now_ns)?;
    let next_intent = build_next_intent_params(template, recovery, run, now_ns)?;
    Ok(AlphaReDecisionInput {
        template_intent: template.clone(),
        recovery: recovery.clone(),
        market,
        next_intent,
    })
}

fn build_market_context(
    template: &StrategyIntent,
    market_input: &MarketInputFile,
    now_ns: u64,
) -> Result<AlphaMarketContext, String> {
    let max_decision_age_ns = market_input
        .max_decision_age_ns
        .or(template.max_decision_age_ns)
        .ok_or_else(|| "MARKET_MAX_DECISION_AGE_NS_REQUIRED".to_string())?;
    let market = AlphaMarketContext {
        observed_at_ns: market_input.observed_at_ns.unwrap_or(now_ns),
        desired_signed_qty: market_input.desired_signed_qty,
        max_decision_age_ns,
        market_snapshot_id: market_input
            .market_snapshot_id
            .clone()
            .or_else(|| template.market_snapshot_id.clone()),
        signal_id: market_input
            .signal_id
            .clone()
            .or_else(|| template.signal_id.clone()),
    };
    market.validate().map_err(|err| err.to_string())?;
    Ok(market)
}

fn build_next_intent_params(
    template: &StrategyIntent,
    recovery: &AlphaRecoveryContext,
    run: &OrchestratorRunConfig,
    now_ns: u64,
) -> Result<AlphaNextIntentParams, String> {
    let suffix = recovery.next_cursor.max(1);
    let params = AlphaNextIntentParams {
        intent_id: run
            .next_intent_id
            .clone()
            .unwrap_or_else(|| format!("{}::redecision::{suffix}", template.intent_id)),
        decision_key: run
            .next_decision_key
            .clone()
            .unwrap_or_else(|| format!("redecision-{suffix}")),
        decision_attempt_seq: 1,
        created_at_ns: run.next_created_at_ns.unwrap_or(now_ns),
        expires_at_ns: run.next_expires_at_ns,
    };
    params.validate().map_err(|err| err.to_string())?;
    Ok(params)
}

async fn execute_redecision_flow(
    base_url: &str,
    redecision: &AlphaReDecision,
    mode: ExecutionMode,
    state: &OrchestratorPersistentRunState,
) -> Result<OrchestratorExecutionResult, String> {
    if redecision.action != AlphaReDecisionAction::SubmitFreshIntent {
        return Ok(OrchestratorExecutionResult::skipped(
            mode,
            redecision_skip_reason(redecision),
        ));
    }
    let Some(intent) = redecision.proposed_intent.as_ref() else {
        return Ok(OrchestratorExecutionResult::skipped(
            mode,
            "PROPOSED_INTENT_MISSING",
        ));
    };
    if proposal_already_executed(state, intent) {
        return Ok(OrchestratorExecutionResult::skipped(
            mode,
            "PROPOSAL_ALREADY_EXECUTED",
        ));
    }
    if matches!(mode, ExecutionMode::DryRun) {
        return Ok(OrchestratorExecutionResult::skipped(mode, "DRY_RUN"));
    }

    let parsed = parse_http_base_url(base_url)?;
    let adapt_response = post_strategy_intent_adapt(&parsed, intent).await?;
    let adapt_ok = (200..300).contains(&adapt_response.status_code);
    let adapt_body = decode_json_or_string(&adapt_response.body);

    if matches!(mode, ExecutionMode::AdaptOnly) {
        return Ok(OrchestratorExecutionResult {
            mode,
            executed: adapt_ok,
            adapt_http_status: Some(adapt_response.status_code),
            adapt_ok: Some(adapt_ok),
            adapt_response: Some(adapt_body),
            submit_http_status: None,
            submit_ok: None,
            submit_response: None,
            skipped_reason: None,
        });
    }

    if !adapt_ok {
        return Ok(OrchestratorExecutionResult {
            mode,
            executed: false,
            adapt_http_status: Some(adapt_response.status_code),
            adapt_ok: Some(adapt_ok),
            adapt_response: Some(adapt_body),
            submit_http_status: None,
            submit_ok: None,
            submit_response: None,
            skipped_reason: Some("ADAPT_NOT_SUCCESS".to_string()),
        });
    }

    let submit_response = post_strategy_intent_submit(&parsed, intent).await?;
    let submit_ok = (200..300).contains(&submit_response.status_code);
    let submit_body = decode_json_or_string(&submit_response.body);

    Ok(OrchestratorExecutionResult {
        mode,
        executed: submit_ok,
        adapt_http_status: Some(adapt_response.status_code),
        adapt_ok: Some(adapt_ok),
        adapt_response: Some(adapt_body),
        submit_http_status: Some(submit_response.status_code),
        submit_ok: Some(submit_ok),
        submit_response: Some(submit_body),
        skipped_reason: None,
    })
}

fn proposal_already_executed(
    state: &OrchestratorPersistentRunState,
    intent: &StrategyIntent,
) -> bool {
    state.last_successful_intent_id.as_deref() == Some(intent.intent_id.as_str())
        && state.last_successful_decision_key.as_deref() == intent.decision_key.as_deref()
}

fn redecision_skip_reason(redecision: &AlphaReDecision) -> String {
    redecision.reason.clone().unwrap_or_else(|| {
        match redecision.action {
            AlphaReDecisionAction::InvalidInput => "INVALID_INPUT",
            AlphaReDecisionAction::AbortAlphaStale => "STRATEGY_INTENT_ALPHA_STALE",
            AlphaReDecisionAction::HoldUnknownExposure => "UNKNOWN_EXPOSURE_PRESENT",
            AlphaReDecisionAction::AbortSideFlip => "SIDE_FLIP_REQUIRES_NEW_RUN",
            AlphaReDecisionAction::NoopNoSignal => "NO_DESIRED_QTY",
            AlphaReDecisionAction::NoopAlreadySatisfied => {
                "LIVE_COMMITTED_QTY_ALREADY_SATISFIES_DESIRED_QTY"
            }
            AlphaReDecisionAction::SubmitFreshIntent => "SUBMIT_FRESH_INTENT",
        }
        .to_string()
    })
}

fn selected_runs<'a>(
    config: &'a OrchestratorConfig,
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

fn load_config(path: &str) -> Result<OrchestratorConfig, String> {
    let config = load_json_file::<OrchestratorConfig>(path)?;
    if config.runs.is_empty() {
        return Err("runs must not be empty".to_string());
    }
    if config.poll_interval_ms == 0 {
        return Err("pollIntervalMs must be greater than zero".to_string());
    }
    Ok(config)
}

fn load_template_intent(path: &str) -> Result<StrategyIntent, String> {
    let intent = load_json_file::<StrategyIntent>(path)?;
    intent
        .validate()
        .map_err(|err| format!("invalid template intent: {err}"))?;
    Ok(intent)
}

fn load_json_file<T: DeserializeOwned>(path: &str) -> Result<T, String> {
    let raw = fs::read_to_string(path).map_err(|err| format!("read {path} failed: {err}"))?;
    serde_json::from_str(&raw).map_err(|err| format!("parse {path} failed: {err}"))
}

fn resolve_run_paths(config: &OrchestratorConfig, run: &OrchestratorRunConfig) -> RunPaths {
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
        "run".to_string()
    } else {
        sanitized
    }
}

fn load_persistent_state(
    path: &str,
    scope: &RunScope,
) -> Result<OrchestratorPersistentRunState, String> {
    let state = load_json_file::<OrchestratorPersistentRunState>(path)?;
    if state.scope_kind != scope.kind.label() || state.scope_id != scope.id {
        return Err("persistent state scope mismatch".to_string());
    }
    Ok(state)
}

fn default_persistent_state(scope: &RunScope) -> OrchestratorPersistentRunState {
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

fn persist_artifacts(
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

fn persist_json_file<T: Serialize>(path: &str, value: &T) -> Result<(), String> {
    let raw = serde_json::to_string_pretty(value).map_err(|err| err.to_string())?;
    if let Some(parent) = Path::new(path).parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .map_err(|err| format!("create parent dir for {path} failed: {err}"))?;
        }
    }
    fs::write(path, raw + "\n").map_err(|err| format!("write {path} failed: {err}"))
}

fn parse_args() -> Result<OrchestratorCliConfig, String> {
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

fn now_epoch_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RunScope {
    kind: OrchestratorScopeKind,
    id: String,
}

impl RunScope {
    fn new(kind: OrchestratorScopeKind, id: String) -> Self {
        Self { kind, id }
    }
}

async fn fetch_catchup_page(
    base_url: &str,
    scope: &RunScope,
    after_cursor: u64,
    limit: usize,
) -> Result<StrategyExecutionCatchupInput, String> {
    let parsed = parse_http_base_url(base_url)?;
    let path = format!(
        "{}{}/{id}?afterCursor={after_cursor}&limit={limit}",
        parsed.base_path,
        scope.kind.endpoint_path(),
        id = scope.id
    );
    let raw = http_get_body(&parsed, &path).await?;
    serde_json::from_slice::<StrategyExecutionCatchupInput>(&raw)
        .map_err(|err| format!("decode catch-up page failed: {err}"))
}

async fn post_strategy_intent_adapt(
    base: &ParsedHttpBaseUrl,
    intent: &StrategyIntent,
) -> Result<SimpleHttpResponse, String> {
    let path = format!("{}{}", base.base_path, "/strategy/intent/adapt");
    let body =
        serde_json::to_vec(intent).map_err(|err| format!("encode adapt request failed: {err}"))?;
    http_request(base, "POST", &path, Some(&body)).await
}

async fn post_strategy_intent_submit(
    base: &ParsedHttpBaseUrl,
    intent: &StrategyIntent,
) -> Result<SimpleHttpResponse, String> {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct SubmitRequest<'a> {
        intent: &'a StrategyIntent,
    }

    let path = format!("{}{}", base.base_path, "/strategy/intent/submit");
    let body = serde_json::to_vec(&SubmitRequest { intent })
        .map_err(|err| format!("encode submit request failed: {err}"))?;
    http_request(base, "POST", &path, Some(&body)).await
}

fn parse_http_base_url(raw: &str) -> Result<ParsedHttpBaseUrl, String> {
    let trimmed = raw.trim();
    let rest = trimmed
        .strip_prefix("http://")
        .ok_or_else(|| "only http:// base URLs are supported".to_string())?;
    if rest.is_empty() {
        return Err("base URL host is required".to_string());
    }

    let (host_port, base_path) = match rest.split_once('/') {
        Some((left, right)) => (left, format!("/{}", right.trim_matches('/'))),
        None => (rest, String::new()),
    };
    if host_port.is_empty() {
        return Err("base URL host is required".to_string());
    }
    let (host, port) = match host_port.rsplit_once(':') {
        Some((host, port)) if !host.is_empty() && !port.is_empty() => {
            let port = port
                .parse::<u16>()
                .map_err(|err| format!("invalid port: {err}"))?;
            (host.to_string(), port)
        }
        _ => (host_port.to_string(), 80),
    };

    Ok(ParsedHttpBaseUrl {
        host,
        port,
        base_path: if base_path == "/" {
            String::new()
        } else {
            base_path
        },
    })
}

async fn http_get_body(base: &ParsedHttpBaseUrl, path: &str) -> Result<Vec<u8>, String> {
    let response = http_request(base, "GET", path, None).await?;
    if response.status_code != 200 {
        let message = String::from_utf8_lossy(&response.body);
        return Err(format!("HTTP {}: {}", response.status_code, message));
    }
    Ok(response.body)
}

async fn http_request(
    base: &ParsedHttpBaseUrl,
    method: &str,
    path: &str,
    body: Option<&[u8]>,
) -> Result<SimpleHttpResponse, String> {
    let mut stream = TcpStream::connect((base.host.as_str(), base.port))
        .await
        .map_err(|err| format!("connect failed: {err}"))?;
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {host}\r\nAccept: application/json\r\nConnection: close\r\n",
        host = base.host
    );
    if let Some(body) = body {
        request.push_str("Content-Type: application/json\r\n");
        request.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    request.push_str("\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|err| format!("write request failed: {err}"))?;
    if let Some(body) = body {
        stream
            .write_all(body)
            .await
            .map_err(|err| format!("write request body failed: {err}"))?;
    }
    let mut raw = Vec::new();
    stream
        .read_to_end(&mut raw)
        .await
        .map_err(|err| format!("read response failed: {err}"))?;
    parse_http_response(&raw)
}

fn parse_http_response(raw: &[u8]) -> Result<SimpleHttpResponse, String> {
    let split = raw
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| "invalid HTTP response".to_string())?;
    let (head, body) = raw.split_at(split + 4);
    let head = std::str::from_utf8(head).map_err(|err| format!("invalid HTTP headers: {err}"))?;
    let mut lines = head.split("\r\n").filter(|line| !line.is_empty());
    let status_line = lines
        .next()
        .ok_or_else(|| "missing HTTP status line".to_string())?;
    let status_code = status_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| "missing HTTP status code".to_string())?
        .parse::<u16>()
        .map_err(|err| format!("invalid HTTP status code: {err}"))?;

    let mut headers = HashMap::<String, String>::new();
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
        }
    }

    let body = if headers
        .get("transfer-encoding")
        .map(|value| value.eq_ignore_ascii_case("chunked"))
        .unwrap_or(false)
    {
        decode_chunked_body(body)?
    } else {
        body.to_vec()
    };

    Ok(SimpleHttpResponse { status_code, body })
}

fn decode_json_or_string(body: &[u8]) -> Value {
    if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(body)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(body).to_string()))
    }
}

fn decode_chunked_body(raw: &[u8]) -> Result<Vec<u8>, String> {
    let mut cursor = 0usize;
    let mut out = Vec::new();
    loop {
        let line_end = raw[cursor..]
            .windows(2)
            .position(|window| window == b"\r\n")
            .map(|offset| cursor + offset)
            .ok_or_else(|| "invalid chunk header".to_string())?;
        let size_raw = std::str::from_utf8(&raw[cursor..line_end])
            .map_err(|err| format!("invalid chunk size: {err}"))?;
        let size_hex = size_raw
            .split(';')
            .next()
            .ok_or_else(|| "invalid chunk size".to_string())?;
        let size = usize::from_str_radix(size_hex.trim(), 16)
            .map_err(|err| format!("invalid chunk size: {err}"))?;
        cursor = line_end + 2;
        if size == 0 {
            return Ok(out);
        }
        let end = cursor
            .checked_add(size)
            .ok_or_else(|| "chunk body overflow".to_string())?;
        if end + 2 > raw.len() {
            return Err("truncated chunk body".to_string());
        }
        out.extend_from_slice(&raw[cursor..end]);
        if &raw[end..end + 2] != b"\r\n" {
            return Err("invalid chunk terminator".to_string());
        }
        cursor = end + 2;
    }
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
        ExecutionMode, MarketInputFile, OrchestratorConfig, OrchestratorPersistentRunState,
        OrchestratorRunConfig, OrchestratorScopeKind, RunScope, decode_chunked_body,
        load_persistent_state, parse_http_base_url, parse_http_response, persist_json_file,
        proposal_already_executed, resolve_run_paths, sanitize_file_component,
    };
    use crate::order::{OrderType, TimeInForce};
    use crate::strategy::intent::{
        ExecutionPolicyKind, IntentUrgency, STRATEGY_INTENT_SCHEMA_VERSION, StrategyIntent,
    };
    use serde_json::json;
    use std::fs;

    #[test]
    fn parse_http_base_url_supports_port_and_prefix() {
        let parsed = parse_http_base_url("http://127.0.0.1:8081/api").expect("parse");
        assert_eq!(parsed.host, "127.0.0.1");
        assert_eq!(parsed.port, 8081);
        assert_eq!(parsed.base_path, "/api");
    }

    #[test]
    fn decode_chunked_body_round_trips() {
        let raw = b"4\r\nWiki\r\n5\r\npedia\r\n0\r\n\r\n";
        let body = decode_chunked_body(raw).expect("decode chunked");
        assert_eq!(body, b"Wikipedia");
    }

    #[test]
    fn parse_http_response_handles_chunked_json() {
        let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\n{\"ok\"\r\n3\r\n:1}\r\n0\r\n\r\n";
        let response = parse_http_response(raw).expect("parse response");
        assert_eq!(response.status_code, 200);
        assert_eq!(response.body, br#"{"ok":1}"#);
    }

    #[test]
    fn sanitize_file_component_replaces_slashes() {
        assert_eq!(sanitize_file_component("alpha/run:1"), "alpha_run_1");
    }

    #[test]
    fn resolve_run_paths_defaults_under_state_dir() {
        let config = OrchestratorConfig {
            base_url: "http://127.0.0.1:8081".to_string(),
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
            market_input_path: "market.json".to_string(),
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
    fn proposal_already_executed_matches_successful_intent_and_decision() {
        let state = OrchestratorPersistentRunState {
            scope_kind: "executionRunId".to_string(),
            scope_id: "run-1".to_string(),
            next_cursor: 10,
            last_successful_intent_id: Some("intent-1".to_string()),
            last_successful_decision_key: Some("decision-1".to_string()),
            last_execution_mode: Some(ExecutionMode::Submit),
            last_successful_at_ns: Some(10),
        };
        let intent = StrategyIntent {
            schema_version: STRATEGY_INTENT_SCHEMA_VERSION,
            intent_id: "intent-1".to_string(),
            account_id: "acc-1".to_string(),
            session_id: "sess-1".to_string(),
            symbol: "AAPL".to_string(),
            side: "BUY".to_string(),
            order_type: OrderType::Limit,
            qty: 10,
            limit_price: Some(100),
            time_in_force: TimeInForce::Ioc,
            urgency: IntentUrgency::High,
            execution_policy: ExecutionPolicyKind::Aggressive,
            risk_budget_ref: None,
            model_id: None,
            execution_run_id: Some("run-1".to_string()),
            decision_key: Some("decision-1".to_string()),
            decision_attempt_seq: Some(1),
            decision_basis_at_ns: Some(10),
            max_decision_age_ns: Some(100),
            market_snapshot_id: Some("market-1".to_string()),
            signal_id: Some("signal-1".to_string()),
            recovery_policy: None,
            algo: None,
            created_at_ns: 10,
            expires_at_ns: 200,
        };
        assert!(proposal_already_executed(&state, &intent));
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
        let parsed: MarketInputFile =
            serde_json::from_value(raw).expect("market input should deserialize");
        assert_eq!(parsed.desired_signed_qty, 60);
        assert_eq!(parsed.observed_at_ns, Some(1000));
    }
}
