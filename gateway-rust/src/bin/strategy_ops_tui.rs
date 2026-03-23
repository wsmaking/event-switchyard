use serde::Deserialize;
use std::collections::BTreeMap;
use std::env;
use tokio::time::{Duration, interval};

#[allow(dead_code)]
#[path = "../order/mod.rs"]
mod order;
mod strategy;
#[path = "strategy_ops_tui/tui_view.rs"]
mod tui_view;

use strategy::alpha_redecision::{
    AlphaReDecision, AlphaReDecisionInput, AlphaRecoveryContext,
};
use strategy::catchup::{StrategyExecutionCatchupLoop, target_signed_qty_for_intent};
use strategy::http_client::{
    ACCEPT_JSON_OR_TEXT, fetch_catchup_page as fetch_strategy_catchup_page,
    http_get_body_with_accept, parse_http_base_url,
};
use strategy::intent::StrategyIntent;
use strategy::redecision_support::{
    AlphaMarketOverrides, AlphaNextIntentOverrides,
    build_optional_redecision_input as build_shared_optional_redecision_input,
    load_template_intent as load_shared_template_intent,
};
use strategy::replay::StrategyExecutionCatchupInput;
use strategy::scope::StrategyTargetScope as DashboardScope;
use tui_view::{
    DashboardUiState, KeyHandling, TerminalUiGuard, handle_key, now_epoch_ns,
    parse_selected_metrics, render_dashboard, spawn_key_reader,
};

const DEFAULT_BASE_URL: &str = "http://127.0.0.1:8081";
const DEFAULT_LIMIT: usize = 500;
const DEFAULT_POLL_INTERVAL_MS: u64 = 1_000;
const DEFAULT_MAX_DECISIONS: usize = 12;
const MAX_CATCHUP_PAGES_PER_REFRESH: usize = 8;
const METRIC_NAMES: &[&str] = &[
    "gateway_v3_confirm_store_size",
    "gateway_v3_loss_suspect_total",
    "gateway_v3_durable_queue_utilization_pct_max",
    "gateway_v3_durable_confirm_p99_us",
    "gateway_v3_durable_backlog_growth_per_sec",
    "gateway_v3_durable_receipt_inflight",
    "gateway_v3_durable_admission_level",
    "gateway_strategy_runtime_parent_count",
    "gateway_strategy_runtime_active_parent_count",
    "gateway_strategy_runtime_paused_parent_count",
    "gateway_quant_feedback_queue_depth",
];

#[derive(Debug, Clone)]
struct TuiConfig {
    base_url: String,
    scope: Option<DashboardScope>,
    parent_intent_id: Option<String>,
    limit: usize,
    poll_interval_ms: u64,
    max_decisions: usize,
    target_signed_qty: Option<i64>,
    template_intent_path: Option<String>,
    market_desired_signed_qty: Option<i64>,
    market_observed_at_ns: Option<u64>,
    market_max_decision_age_ns: Option<u64>,
    market_snapshot_id: Option<String>,
    market_signal_id: Option<String>,
    next_intent_id: Option<String>,
    next_decision_key: Option<String>,
    next_created_at_ns: Option<u64>,
    next_expires_at_ns: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AlgoRuntimeView {
    parent_intent_id: String,
    policy: String,
    #[serde(alias = "recoveryPolicy")]
    runtime_mode: String,
    total_qty: u64,
    child_count: u32,
    status: String,
    #[serde(default)]
    accepted_at_ns: Option<u64>,
    #[serde(default)]
    completed_at_ns: Option<u64>,
    #[serde(default)]
    final_reason: Option<String>,
    last_updated_at_ns: u64,
    slices: Vec<AlgoRuntimeSliceView>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AlgoRuntimeSliceView {
    sequence: u32,
    qty: u64,
    status: String,
    #[serde(default)]
    session_seq: Option<u64>,
    #[serde(default)]
    reject_reason: Option<String>,
}

#[derive(Debug, Clone)]
struct CatchupView {
    snapshot: strategy::catchup::StrategyExecutionCatchupLoopSnapshot,
    recovery_context: Option<AlphaRecoveryContext>,
    redecision: Option<AlphaReDecision>,
}

#[derive(Debug, Clone, Default)]
struct DashboardState {
    catchup: Option<CatchupView>,
    runtime: Option<AlgoRuntimeView>,
    metrics: BTreeMap<String, String>,
    errors: Vec<String>,
    last_refresh_ns: Option<u64>,
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
    let _guard = TerminalUiGuard::enter();
    let mut ui = DashboardUiState::new(config.max_decisions);
    if config.scope.is_none() {
        ui.show_catchup = false;
    }
    if config.parent_intent_id.is_none() {
        ui.show_runtime = false;
    }
    let mut state = DashboardState::default();

    if let Err(err) = refresh_dashboard(&config, &mut state).await {
        state.errors = vec![err];
        state.last_refresh_ns = Some(now_epoch_ns());
    }
    render_dashboard(&config, &ui, &state)?;

    let mut refresh_tick = interval(Duration::from_millis(config.poll_interval_ms.max(100)));
    refresh_tick.tick().await;
    let mut key_events = spawn_key_reader();

    loop {
        tokio::select! {
            _ = refresh_tick.tick(), if !ui.paused => {
                if let Err(err) = refresh_dashboard(&config, &mut state).await {
                    state.errors = vec![err];
                    state.last_refresh_ns = Some(now_epoch_ns());
                }
                render_dashboard(&config, &ui, &state)?;
            }
            Some(key) = key_events.recv() => {
                match handle_key(key, &mut ui) {
                    KeyHandling::Quit => break,
                    KeyHandling::RefreshNow => {
                        if let Err(err) = refresh_dashboard(&config, &mut state).await {
                            state.errors = vec![err];
                            state.last_refresh_ns = Some(now_epoch_ns());
                        }
                    }
                    KeyHandling::RenderOnly => {}
                }
                render_dashboard(&config, &ui, &state)?;
            }
            _ = tokio::signal::ctrl_c() => break,
        }
    }

    Ok(())
}

async fn refresh_dashboard(config: &TuiConfig, state: &mut DashboardState) -> Result<(), String> {
    let mut errors = Vec::new();
    if let Some(scope) = config.scope.as_ref() {
        match fetch_catchup_view(config, scope).await {
            Ok(view) => state.catchup = Some(view),
            Err(err) => errors.push(format!("catchup: {err}")),
        }
    }
    if let Some(parent_intent_id) = config.parent_intent_id.as_deref() {
        match fetch_runtime_view(&config.base_url, parent_intent_id).await {
            Ok(runtime) => state.runtime = Some(runtime),
            Err(err) => errors.push(format!("runtime: {err}")),
        }
    }
    match fetch_metrics_view(&config.base_url).await {
        Ok(metrics) => state.metrics = metrics,
        Err(err) => errors.push(format!("metrics: {err}")),
    }
    state.errors = errors;
    state.last_refresh_ns = Some(now_epoch_ns());
    Ok(())
}

async fn fetch_catchup_view(
    config: &TuiConfig,
    scope: &DashboardScope,
) -> Result<CatchupView, String> {
    let mut loop_state = StrategyExecutionCatchupLoop::new();
    let mut after_cursor = 0u64;
    let mut pages_read = 0usize;
    loop {
        if pages_read >= MAX_CATCHUP_PAGES_PER_REFRESH {
            break;
        }
        let page = fetch_catchup_page(&config.base_url, scope, after_cursor, config.limit).await?;
        loop_state
            .apply_page(&page)
            .map_err(|err| format!("{err:?}"))?;
        pages_read = pages_read.saturating_add(1);
        after_cursor = loop_state.next_cursor();
        if !page.has_more || page.fact_count == 0 {
            break;
        }
    }

    let snapshot = loop_state.snapshot();
    let template_intent = config
        .template_intent_path
        .as_deref()
        .map(load_template_intent)
        .transpose()?;
    let target_signed_qty = match (config.target_signed_qty, template_intent.as_ref()) {
        (Some(value), _) => Some(value),
        (None, Some(intent)) => Some(target_signed_qty_for_intent(intent)?),
        (None, None) => None,
    };
    let recovery_context = target_signed_qty
        .map(|signed_qty| AlphaRecoveryContext::from_snapshot(snapshot.clone(), signed_qty));
    let redecision = match (template_intent.as_ref(), recovery_context.as_ref()) {
        (Some(template), Some(recovery)) => {
            let now_ns = now_epoch_ns();
            build_redecision_input(template, recovery, config, now_ns)?
                .map(|input| AlphaReDecision::evaluate(input, now_ns))
        }
        _ => None,
    };

    Ok(CatchupView {
        snapshot,
        recovery_context,
        redecision,
    })
}

async fn fetch_runtime_view(
    base_url: &str,
    parent_intent_id: &str,
) -> Result<AlgoRuntimeView, String> {
    let parsed = parse_http_base_url(base_url)?;
    let path = format!(
        "{}{}{}",
        parsed.base_path, "/strategy/runtime/", parent_intent_id
    );
    let raw = strategy::http_client::http_get_body(&parsed, &path).await?;
    serde_json::from_slice(&raw).map_err(|err| format!("decode runtime failed: {err}"))
}

async fn fetch_metrics_view(base_url: &str) -> Result<BTreeMap<String, String>, String> {
    let parsed = parse_http_base_url(base_url)?;
    let path = format!("{}{}", parsed.base_path, "/metrics");
    let raw = http_get_body_with_accept(&parsed, &path, ACCEPT_JSON_OR_TEXT).await?;
    let text = String::from_utf8(raw).map_err(|err| format!("decode metrics failed: {err}"))?;
    Ok(parse_selected_metrics(&text))
}

fn parse_args() -> Result<TuiConfig, String> {
    let mut base_url = DEFAULT_BASE_URL.to_string();
    let mut execution_run_id = None;
    let mut intent_id = None;
    let mut parent_intent_id = None;
    let mut limit = DEFAULT_LIMIT;
    let mut poll_interval_ms = DEFAULT_POLL_INTERVAL_MS;
    let mut max_decisions = DEFAULT_MAX_DECISIONS;
    let mut target_signed_qty = None;
    let mut template_intent_path = None;
    let mut market_desired_signed_qty = None;
    let mut market_observed_at_ns = None;
    let mut market_max_decision_age_ns = None;
    let mut market_snapshot_id = None;
    let mut market_signal_id = None;
    let mut next_intent_id = None;
    let mut next_decision_key = None;
    let mut next_created_at_ns = None;
    let mut next_expires_at_ns = None;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            "--base-url" => base_url = next_arg(&mut args, "--base-url")?,
            "--execution-run-id" => {
                execution_run_id = Some(next_arg(&mut args, "--execution-run-id")?)
            }
            "--intent-id" => intent_id = Some(next_arg(&mut args, "--intent-id")?),
            "--parent-intent-id" => {
                parent_intent_id = Some(next_arg(&mut args, "--parent-intent-id")?)
            }
            "--limit" => limit = parse_usize_arg(&mut args, "--limit")?,
            "--poll-interval-ms" => {
                poll_interval_ms = parse_u64_arg(&mut args, "--poll-interval-ms")?
            }
            "--max-decisions" => max_decisions = parse_usize_arg(&mut args, "--max-decisions")?,
            "--target-signed-qty" => {
                target_signed_qty = Some(parse_i64_arg(&mut args, "--target-signed-qty")?)
            }
            "--template-intent" => {
                template_intent_path = Some(next_arg(&mut args, "--template-intent")?)
            }
            "--market-desired-signed-qty" => {
                market_desired_signed_qty =
                    Some(parse_i64_arg(&mut args, "--market-desired-signed-qty")?)
            }
            "--market-observed-at-ns" => {
                market_observed_at_ns = Some(parse_u64_arg(&mut args, "--market-observed-at-ns")?)
            }
            "--market-max-decision-age-ns" => {
                market_max_decision_age_ns =
                    Some(parse_u64_arg(&mut args, "--market-max-decision-age-ns")?)
            }
            "--market-snapshot-id" => {
                market_snapshot_id = Some(next_arg(&mut args, "--market-snapshot-id")?)
            }
            "--market-signal-id" => {
                market_signal_id = Some(next_arg(&mut args, "--market-signal-id")?)
            }
            "--next-intent-id" => next_intent_id = Some(next_arg(&mut args, "--next-intent-id")?),
            "--next-decision-key" => {
                next_decision_key = Some(next_arg(&mut args, "--next-decision-key")?)
            }
            "--next-created-at-ns" => {
                next_created_at_ns = Some(parse_u64_arg(&mut args, "--next-created-at-ns")?)
            }
            "--next-expires-at-ns" => {
                next_expires_at_ns = Some(parse_u64_arg(&mut args, "--next-expires-at-ns")?)
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }

    let scope = match (execution_run_id, intent_id) {
        (Some(value), None) => Some(DashboardScope::ExecutionRunId(value)),
        (None, Some(value)) => Some(DashboardScope::IntentId(value)),
        (None, None) => None,
        _ => return Err("choose either --execution-run-id or --intent-id".to_string()),
    };
    Ok(TuiConfig {
        base_url,
        scope,
        parent_intent_id,
        limit: limit.max(1),
        poll_interval_ms: poll_interval_ms.max(100),
        max_decisions: max_decisions.max(1),
        target_signed_qty,
        template_intent_path,
        market_desired_signed_qty,
        market_observed_at_ns,
        market_max_decision_age_ns,
        market_snapshot_id,
        market_signal_id,
        next_intent_id,
        next_decision_key,
        next_created_at_ns,
        next_expires_at_ns,
    })
}

fn print_usage() {
    println!("strategy_ops_tui");
    println!("  Monitoring TUI for strategy catch-up, re-decision, runtime, and metrics.");
    println!("  cargo run --manifest-path gateway-rust/Cargo.toml --bin strategy_ops_tui -- \\");
    println!("    [--base-url {}] \\", DEFAULT_BASE_URL);
    println!(
        "    [--execution-run-id run-1 | --intent-id intent-1] [--parent-intent-id parent-1] \\"
    );
    println!(
        "    [--limit {}] [--poll-interval-ms {}] [--max-decisions {}] \\",
        DEFAULT_LIMIT, DEFAULT_POLL_INTERVAL_MS, DEFAULT_MAX_DECISIONS
    );
    println!(
        "    [--target-signed-qty 100] [--template-intent contracts/fixtures/strategy_intent_v2.json] \\"
    );
    println!(
        "    [--market-desired-signed-qty 60] [--market-observed-at-ns NOW_NS] [--market-max-decision-age-ns 1000000] \\"
    );
    println!("    [--market-snapshot-id snap-1] [--market-signal-id signal-1]");
    println!("  no scope args => metrics-only mode");
    println!("  keys: q quit, h help, p pause, space refresh, c/r/m/e toggle panels, +/- rows");
}

fn next_arg(args: &mut impl Iterator<Item = String>, name: &str) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("missing value for {name}"))
}

fn parse_u64_arg(args: &mut impl Iterator<Item = String>, name: &str) -> Result<u64, String> {
    next_arg(args, name)?
        .parse::<u64>()
        .map_err(|err| format!("invalid {name}: {err}"))
}

fn parse_i64_arg(args: &mut impl Iterator<Item = String>, name: &str) -> Result<i64, String> {
    next_arg(args, name)?
        .parse::<i64>()
        .map_err(|err| format!("invalid {name}: {err}"))
}

fn parse_usize_arg(args: &mut impl Iterator<Item = String>, name: &str) -> Result<usize, String> {
    next_arg(args, name)?
        .parse::<usize>()
        .map_err(|err| format!("invalid {name}: {err}"))
}

fn load_template_intent(path: &str) -> Result<StrategyIntent, String> {
    load_shared_template_intent(path)
}

fn build_redecision_input(
    template: &StrategyIntent,
    recovery: &AlphaRecoveryContext,
    config: &TuiConfig,
    now_ns: u64,
) -> Result<Option<AlphaReDecisionInput>, String> {
    build_shared_optional_redecision_input(
        template,
        recovery,
        AlphaMarketOverrides {
            desired_signed_qty: config.market_desired_signed_qty,
            observed_at_ns: config.market_observed_at_ns,
            max_decision_age_ns: config.market_max_decision_age_ns,
            market_snapshot_id: config.market_snapshot_id.clone(),
            signal_id: config.market_signal_id.clone(),
        },
        AlphaNextIntentOverrides {
            intent_id: config.next_intent_id.clone(),
            decision_key: config.next_decision_key.clone(),
            created_at_ns: config.next_created_at_ns,
            expires_at_ns: config.next_expires_at_ns,
        },
        now_ns,
    )
}

async fn fetch_catchup_page(
    base_url: &str,
    scope: &DashboardScope,
    after_cursor: u64,
    limit: usize,
) -> Result<StrategyExecutionCatchupInput, String> {
    fetch_strategy_catchup_page(
        base_url,
        scope.endpoint_path(),
        scope.id(),
        after_cursor,
        limit,
    )
    .await
}
