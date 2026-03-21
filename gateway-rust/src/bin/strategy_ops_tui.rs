use chrono::{DateTime, Local, Utc};
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};
use std::env;
use std::fmt::Write as _;
use std::fs;
use std::io::{self, Write};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, sleep};

#[allow(dead_code)]
#[path = "../order/mod.rs"]
mod order;
mod strategy;

use strategy::alpha_redecision::{
    AlphaMarketContext, AlphaNextIntentParams, AlphaReDecision, AlphaRecoveryContext,
};
use strategy::catchup::{StrategyExecutionCatchupLoop, target_signed_qty_for_intent};
use strategy::intent::StrategyIntent;
use strategy::replay::{StrategyExecutionCatchupInput, StrategyExecutionFactStatus};

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

#[derive(Debug, Clone, PartialEq, Eq)]
enum DashboardScope {
    ExecutionRunId(String),
    IntentId(String),
}

impl DashboardScope {
    fn endpoint_path(&self) -> &'static str {
        match self {
            Self::ExecutionRunId(_) => "/strategy/catchup/execution",
            Self::IntentId(_) => "/strategy/catchup/intent",
        }
    }

    fn id(&self) -> &str {
        match self {
            Self::ExecutionRunId(value) | Self::IntentId(value) => value,
        }
    }

    fn label(&self) -> &'static str {
        match self {
            Self::ExecutionRunId(_) => "executionRunId",
            Self::IntentId(_) => "intentId",
        }
    }
}

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

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AlgoRuntimeView {
    parent_intent_id: String,
    policy: String,
    recovery_policy: String,
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

struct TerminalUiGuard;

impl TerminalUiGuard {
    fn enter() -> Self {
        print!("\x1b[?25l");
        let _ = io::stdout().flush();
        Self
    }
}

impl Drop for TerminalUiGuard {
    fn drop(&mut self) {
        print!("\x1b[0m\x1b[?25h\n");
        let _ = io::stdout().flush();
    }
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
    let mut state = DashboardState::default();

    loop {
        tokio::select! {
            result = refresh_dashboard(&config, &mut state) => {
                if let Err(err) = result {
                    state.errors = vec![err];
                    state.last_refresh_ns = Some(now_epoch_ns());
                }
                render_dashboard(&config, &state)?;
                sleep(Duration::from_millis(config.poll_interval_ms.max(1))).await;
            }
            _ = tokio::signal::ctrl_c() => {
                break;
            }
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
    let raw = http_get_body(&parsed, &path).await?;
    serde_json::from_slice(&raw).map_err(|err| format!("decode runtime failed: {err}"))
}

async fn fetch_metrics_view(base_url: &str) -> Result<BTreeMap<String, String>, String> {
    let parsed = parse_http_base_url(base_url)?;
    let path = format!("{}{}", parsed.base_path, "/metrics");
    let raw = http_get_body(&parsed, &path).await?;
    let text = String::from_utf8(raw).map_err(|err| format!("decode metrics failed: {err}"))?;
    Ok(parse_selected_metrics(&text))
}

fn render_dashboard(config: &TuiConfig, state: &DashboardState) -> Result<(), String> {
    let mut out = String::new();
    let now_ns = now_epoch_ns();
    let refreshed_at = state
        .last_refresh_ns
        .map(format_timestamp_ns)
        .unwrap_or_else(|| "-".to_string());

    write!(
        out,
        "\x1b[2J\x1b[HStrategy Ops TUI  refreshed={}  poll={}ms  ctrl-c=exit\n",
        refreshed_at, config.poll_interval_ms
    )
    .map_err(|err| err.to_string())?;
    write!(out, "Base: {}\n", config.base_url).map_err(|err| err.to_string())?;
    match config.scope.as_ref() {
        Some(scope) => write!(out, "Scope: {}={}\n", scope.label(), scope.id())
            .map_err(|err| err.to_string())?,
        None => out.push_str("Scope: -\n"),
    }
    if let Some(parent_intent_id) = config.parent_intent_id.as_deref() {
        write!(out, "Parent runtime: {}\n", parent_intent_id).map_err(|err| err.to_string())?;
    }
    out.push('\n');

    if let Some(catchup) = state.catchup.as_ref() {
        render_catchup_panel(&mut out, catchup, config.max_decisions, now_ns)?;
    } else {
        out.push_str("Catch-up\n  not configured\n\n");
    }

    if let Some(runtime) = state.runtime.as_ref() {
        render_runtime_panel(&mut out, runtime, now_ns)?;
    } else {
        out.push_str("Algo Runtime\n  not configured\n\n");
    }

    render_metrics_panel(&mut out, &state.metrics)?;
    render_error_panel(&mut out, &state.errors)?;

    print!("{out}");
    io::stdout().flush().map_err(|err| err.to_string())
}

fn render_catchup_panel(
    out: &mut String,
    catchup: &CatchupView,
    max_decisions: usize,
    now_ns: u64,
) -> Result<(), String> {
    writeln!(out, "Catch-up").map_err(|err| err.to_string())?;
    writeln!(
        out,
        "  facts={} decisions={} nextCursor={} hasMore={} statusTotals[rej={} unconf={} durAcc={} durRej={} loss={}]",
        catchup.snapshot.total_fact_count,
        catchup.snapshot.decisions.len(),
        catchup.snapshot.next_cursor,
        catchup.snapshot.has_more,
        catchup.snapshot.latest_status_totals.rejected,
        catchup.snapshot.latest_status_totals.unconfirmed,
        catchup.snapshot.latest_status_totals.durable_accepted,
        catchup.snapshot.latest_status_totals.durable_rejected,
        catchup.snapshot.latest_status_totals.loss_suspect,
    )
    .map_err(|err| err.to_string())?;

    if let Some(recovery) = catchup.recovery_context.as_ref() {
        writeln!(
            out,
            "  recovery target={} committed={} filled={} open={} failed={} unknown={} unsent={}",
            format_signed(recovery.target_signed_qty),
            format_signed(recovery.committed_signed_qty()),
            format_signed(recovery.filled_signed_qty),
            format_signed(recovery.open_signed_qty),
            format_signed(recovery.failed_signed_qty),
            format_signed(recovery.unknown_signed_qty),
            format_signed(recovery.unsent_signed_qty),
        )
        .map_err(|err| err.to_string())?;
        writeln!(
            out,
            "  operator {} {}",
            format_debug(&recovery.operator_status),
            recovery.operator_reason.as_deref().unwrap_or("-")
        )
        .map_err(|err| err.to_string())?;
        writeln!(
            out,
            "  unknown breakdown unconfirmed={} lossSuspect={} residual={}",
            format_signed(recovery.unknown_exposure_breakdown.unconfirmed_signed_qty),
            format_signed(recovery.unknown_exposure_breakdown.loss_suspect_signed_qty),
            format_signed(
                recovery
                    .unknown_exposure_breakdown
                    .residual_unknown_signed_qty
            ),
        )
        .map_err(|err| err.to_string())?;
    } else {
        writeln!(out, "  recovery target=-").map_err(|err| err.to_string())?;
    }

    if let Some(redecision) = catchup.redecision.as_ref() {
        writeln!(
            out,
            "  redecision {} desired={} effective={} residual={} reason={}",
            format_debug(&redecision.action),
            format_signed(redecision.desired_signed_qty),
            format_signed(redecision.effective_desired_signed_qty),
            format_signed(redecision.residual_signed_qty),
            redecision.reason.as_deref().unwrap_or("-"),
        )
        .map_err(|err| err.to_string())?;
        if let Some(intent) = redecision.proposed_intent.as_ref() {
            writeln!(
                out,
                "  proposed intent={} decisionKey={} qty={} staleAt={}",
                intent.intent_id,
                intent.decision_key.as_deref().unwrap_or("-"),
                intent.qty,
                format_age_ns(now_ns, intent.decision_basis_at_ns.unwrap_or(now_ns)),
            )
            .map_err(|err| err.to_string())?;
        }
    } else {
        writeln!(out, "  redecision -").map_err(|err| err.to_string())?;
    }

    writeln!(
        out,
        "  {:<22} {:>8} {:<18} {:<18} {:>7} {:>7} {:<18}",
        "decision", "qty", "latest", "live", "filled", "remain", "reason"
    )
    .map_err(|err| err.to_string())?;

    let mut rows = if let Some(recovery) = catchup.recovery_context.as_ref() {
        recovery.decisions.clone()
    } else {
        catchup
            .snapshot
            .decisions
            .iter()
            .cloned()
            .map(
                |decision| strategy::alpha_redecision::AlphaRecoveryDecision {
                    decision,
                    class: strategy::alpha_redecision::AlphaDecisionClass::Unknown,
                    filled_signed_qty: 0,
                    open_signed_qty: 0,
                    failed_signed_qty: 0,
                    unknown_signed_qty: 0,
                    unknown_reason: None,
                },
            )
            .collect::<Vec<_>>()
    };
    rows.sort_by(|left, right| {
        right
            .decision
            .latest_event_at_ns
            .cmp(&left.decision.latest_event_at_ns)
            .then_with(|| right.decision.cursor.cmp(&left.decision.cursor))
    });
    for row in rows.into_iter().take(max_decisions) {
        let live_status = row
            .decision
            .live_order
            .as_ref()
            .map(|order| order.status.as_str())
            .unwrap_or("-");
        let live_filled = row
            .decision
            .live_order
            .as_ref()
            .map(|order| order.filled_qty)
            .unwrap_or(0);
        let live_remaining = row
            .decision
            .live_order
            .as_ref()
            .map(|order| order.remaining_qty)
            .unwrap_or(0);
        writeln!(
            out,
            "  {:<22} {:>8} {:<18} {:<18} {:>7} {:>7} {:<18}",
            decision_label(&row.decision),
            row.decision.position_delta_qty.unwrap_or_default(),
            format_fact_status(row.decision.latest_status),
            trim_to_width(live_status, 18),
            live_filled,
            live_remaining,
            trim_to_width(
                row.decision
                    .reason
                    .as_deref()
                    .or_else(|| row.unknown_reason.map(unknown_reason_label))
                    .unwrap_or("-"),
                18
            ),
        )
        .map_err(|err| err.to_string())?;
    }
    out.push('\n');
    Ok(())
}

fn render_runtime_panel(
    out: &mut String,
    runtime: &AlgoRuntimeView,
    now_ns: u64,
) -> Result<(), String> {
    writeln!(out, "Algo Runtime").map_err(|err| err.to_string())?;
    writeln!(
        out,
        "  parent={} status={} policy={} recovery={} totalQty={} children={} updated={}",
        runtime.parent_intent_id,
        runtime.status,
        runtime.policy,
        runtime.recovery_policy,
        runtime.total_qty,
        runtime.child_count,
        format_age_ns(now_ns, runtime.last_updated_at_ns),
    )
    .map_err(|err| err.to_string())?;
    writeln!(
        out,
        "  acceptedAt={} completedAt={} finalReason={}",
        runtime
            .accepted_at_ns
            .map(format_timestamp_ns)
            .unwrap_or_else(|| "-".to_string()),
        runtime
            .completed_at_ns
            .map(format_timestamp_ns)
            .unwrap_or_else(|| "-".to_string()),
        runtime.final_reason.as_deref().unwrap_or("-"),
    )
    .map_err(|err| err.to_string())?;

    let mut counts = BTreeMap::<String, u64>::new();
    for slice in &runtime.slices {
        *counts.entry(slice.status.clone()).or_default() += 1;
    }
    writeln!(
        out,
        "  child counts scheduled={} dispatching={} volatileAccepted={} durableAccepted={} rejected={} skipped={}",
        counts.get("SCHEDULED").copied().unwrap_or(0),
        counts.get("DISPATCHING").copied().unwrap_or(0),
        counts.get("VOLATILE_ACCEPTED").copied().unwrap_or(0),
        counts.get("DURABLE_ACCEPTED").copied().unwrap_or(0),
        counts.get("REJECTED").copied().unwrap_or(0),
        counts.get("SKIPPED").copied().unwrap_or(0),
    )
    .map_err(|err| err.to_string())?;
    writeln!(
        out,
        "  {:>3} {:>8} {:<18} {:>8} {:<18}",
        "seq", "qty", "status", "sessSeq", "reason"
    )
    .map_err(|err| err.to_string())?;
    for slice in runtime.slices.iter().take(12) {
        writeln!(
            out,
            "  {:>3} {:>8} {:<18} {:>8} {:<18}",
            slice.sequence,
            slice.qty,
            slice.status,
            slice
                .session_seq
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            trim_to_width(slice.reject_reason.as_deref().unwrap_or("-"), 18),
        )
        .map_err(|err| err.to_string())?;
    }
    out.push('\n');
    Ok(())
}

fn render_metrics_panel(
    out: &mut String,
    metrics: &BTreeMap<String, String>,
) -> Result<(), String> {
    writeln!(out, "Metrics").map_err(|err| err.to_string())?;
    for name in METRIC_NAMES {
        writeln!(
            out,
            "  {:<42} {}",
            name,
            metrics.get(*name).map(String::as_str).unwrap_or("-")
        )
        .map_err(|err| err.to_string())?;
    }
    out.push('\n');
    Ok(())
}

fn render_error_panel(out: &mut String, errors: &[String]) -> Result<(), String> {
    writeln!(out, "Errors").map_err(|err| err.to_string())?;
    if errors.is_empty() {
        writeln!(out, "  -").map_err(|err| err.to_string())?;
        return Ok(());
    }
    for error in errors {
        writeln!(out, "  {}", error).map_err(|err| err.to_string())?;
    }
    Ok(())
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
    if scope.is_none() && parent_intent_id.is_none() {
        return Err(
            "one of --execution-run-id, --intent-id, or --parent-intent-id is required".to_string(),
        );
    }

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
        "    [--target-signed-qty 100] [--template-intent contracts/fixtures/strategy_intent_v1.json] \\"
    );
    println!(
        "    [--market-desired-signed-qty 60] [--market-observed-at-ns NOW_NS] [--market-max-decision-age-ns 1000000] \\"
    );
    println!("    [--market-snapshot-id snap-1] [--market-signal-id signal-1]");
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
    let raw =
        fs::read_to_string(path).map_err(|err| format!("read template intent failed: {err}"))?;
    let intent: StrategyIntent =
        serde_json::from_str(&raw).map_err(|err| format!("parse template intent failed: {err}"))?;
    intent
        .validate()
        .map_err(|err| format!("invalid template intent: {err}"))?;
    Ok(intent)
}

fn build_redecision_input(
    template: &StrategyIntent,
    recovery: &AlphaRecoveryContext,
    config: &TuiConfig,
    now_ns: u64,
) -> Result<Option<strategy::alpha_redecision::AlphaReDecisionInput>, String> {
    let Some(market_desired_signed_qty) = config.market_desired_signed_qty else {
        return Ok(None);
    };
    let market = build_market_context(template, config, market_desired_signed_qty, now_ns)?;
    let next_intent = build_next_intent_params(template, recovery, config, now_ns)?;
    Ok(Some(strategy::alpha_redecision::AlphaReDecisionInput {
        template_intent: template.clone(),
        recovery: recovery.clone(),
        market,
        next_intent,
    }))
}

fn build_market_context(
    template: &StrategyIntent,
    config: &TuiConfig,
    desired_signed_qty: i64,
    now_ns: u64,
) -> Result<AlphaMarketContext, String> {
    let max_decision_age_ns = config
        .market_max_decision_age_ns
        .or(template.max_decision_age_ns)
        .ok_or_else(|| "MARKET_MAX_DECISION_AGE_NS_REQUIRED".to_string())?;
    let market = AlphaMarketContext {
        observed_at_ns: config.market_observed_at_ns.unwrap_or(now_ns),
        desired_signed_qty,
        max_decision_age_ns,
        market_snapshot_id: config
            .market_snapshot_id
            .clone()
            .or_else(|| template.market_snapshot_id.clone()),
        signal_id: config
            .market_signal_id
            .clone()
            .or_else(|| template.signal_id.clone()),
    };
    market.validate().map_err(|err| err.to_string())?;
    Ok(market)
}

fn build_next_intent_params(
    template: &StrategyIntent,
    recovery: &AlphaRecoveryContext,
    config: &TuiConfig,
    now_ns: u64,
) -> Result<AlphaNextIntentParams, String> {
    let suffix = recovery.next_cursor.max(1);
    let params = AlphaNextIntentParams {
        intent_id: config
            .next_intent_id
            .clone()
            .unwrap_or_else(|| format!("{}::redecision::{suffix}", template.intent_id)),
        decision_key: config
            .next_decision_key
            .clone()
            .unwrap_or_else(|| format!("redecision-{suffix}")),
        decision_attempt_seq: 1,
        created_at_ns: config.next_created_at_ns.unwrap_or(now_ns),
        expires_at_ns: config.next_expires_at_ns,
    };
    params.validate().map_err(|err| err.to_string())?;
    Ok(params)
}

async fn fetch_catchup_page(
    base_url: &str,
    scope: &DashboardScope,
    after_cursor: u64,
    limit: usize,
) -> Result<StrategyExecutionCatchupInput, String> {
    let parsed = parse_http_base_url(base_url)?;
    let path = format!(
        "{}{}/{id}?afterCursor={after_cursor}&limit={limit}",
        parsed.base_path,
        scope.endpoint_path(),
        id = scope.id()
    );
    let raw = http_get_body(&parsed, &path).await?;
    serde_json::from_slice::<StrategyExecutionCatchupInput>(&raw)
        .map_err(|err| format!("decode catch-up page failed: {err}"))
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
        "{method} {path} HTTP/1.1\r\nHost: {host}\r\nAccept: application/json, text/plain\r\nConnection: close\r\n",
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
            .map_err(|err| format!("invalid chunk header utf8: {err}"))?;
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

fn parse_selected_metrics(raw: &str) -> BTreeMap<String, String> {
    let mut parsed = BTreeMap::new();
    for line in raw.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let mut parts = trimmed.split_whitespace();
        let Some(name) = parts.next() else {
            continue;
        };
        let Some(value) = parts.next() else {
            continue;
        };
        if METRIC_NAMES.contains(&name) {
            parsed.insert(name.to_string(), value.to_string());
        }
    }
    parsed
}

fn decision_label(decision: &strategy::catchup::StrategyExecutionDecisionState) -> String {
    if let Some(decision_key) = decision.decision_key.as_deref() {
        return trim_to_width(decision_key, 22);
    }
    if let Some(session_seq) = decision.session_seq {
        return trim_to_width(&format!("{}/{}", decision.session_id, session_seq), 22);
    }
    trim_to_width(&decision.session_id, 22)
}

fn trim_to_width(value: &str, width: usize) -> String {
    if value.chars().count() <= width {
        return value.to_string();
    }
    if width <= 1 {
        return "…".to_string();
    }
    let prefix = value.chars().take(width - 1).collect::<String>();
    format!("{prefix}…")
}

fn unknown_reason_label(
    reason: strategy::alpha_redecision::AlphaUnknownExposureReason,
) -> &'static str {
    match reason {
        strategy::alpha_redecision::AlphaUnknownExposureReason::Unconfirmed => "UNCONFIRMED",
        strategy::alpha_redecision::AlphaUnknownExposureReason::LossSuspect => "LOSS_SUSPECT",
        strategy::alpha_redecision::AlphaUnknownExposureReason::ResidualUnknown => {
            "RESIDUAL_UNKNOWN"
        }
        strategy::alpha_redecision::AlphaUnknownExposureReason::None => "-",
    }
}

fn format_fact_status(status: StrategyExecutionFactStatus) -> &'static str {
    match status {
        StrategyExecutionFactStatus::Rejected => "REJECTED",
        StrategyExecutionFactStatus::Unconfirmed => "UNCONFIRMED",
        StrategyExecutionFactStatus::DurableAccepted => "DURABLE_ACCEPTED",
        StrategyExecutionFactStatus::DurableRejected => "DURABLE_REJECTED",
        StrategyExecutionFactStatus::LossSuspect => "LOSS_SUSPECT",
    }
}

fn format_signed(value: i64) -> String {
    if value > 0 {
        format!("+{value}")
    } else {
        value.to_string()
    }
}

fn format_debug<T: std::fmt::Debug>(value: &T) -> String {
    format!("{value:?}")
}

fn format_age_ns(now_ns: u64, at_ns: u64) -> String {
    if at_ns == 0 {
        return "-".to_string();
    }
    let age_ns = now_ns.saturating_sub(at_ns);
    if age_ns < 1_000 {
        return format!("{age_ns}ns ago");
    }
    if age_ns < 1_000_000 {
        return format!("{}us ago", age_ns / 1_000);
    }
    if age_ns < 1_000_000_000 {
        return format!("{}ms ago", age_ns / 1_000_000);
    }
    format!("{:.2}s ago", age_ns as f64 / 1_000_000_000.0)
}

fn format_timestamp_ns(at_ns: u64) -> String {
    if at_ns == 0 {
        return "-".to_string();
    }
    let secs = (at_ns / 1_000_000_000) as i64;
    let nanos = (at_ns % 1_000_000_000) as u32;
    let Some(datetime) = DateTime::<Utc>::from_timestamp(secs, nanos) else {
        return at_ns.to_string();
    };
    datetime
        .with_timezone(&Local)
        .format("%Y-%m-%d %H:%M:%S%.3f %Z")
        .to_string()
}

fn now_epoch_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::{METRIC_NAMES, parse_selected_metrics, trim_to_width};

    #[test]
    fn parse_selected_metrics_keeps_only_requested_names() {
        let raw = "\
# HELP gateway_v3_confirm_store_size foo\n\
gateway_v3_confirm_store_size 42\n\
gateway_quant_feedback_queue_depth 7\n\
gateway_other_metric 999\n";
        let parsed = parse_selected_metrics(raw);
        assert_eq!(
            parsed
                .get("gateway_v3_confirm_store_size")
                .map(String::as_str),
            Some("42")
        );
        assert_eq!(
            parsed
                .get("gateway_quant_feedback_queue_depth")
                .map(String::as_str),
            Some("7")
        );
        assert!(!parsed.contains_key("gateway_other_metric"));
        assert!(METRIC_NAMES.contains(&"gateway_v3_confirm_store_size"));
    }

    #[test]
    fn trim_to_width_adds_ellipsis() {
        assert_eq!(trim_to_width("abcdef", 4), "abc…");
        assert_eq!(trim_to_width("abc", 4), "abc");
    }
}
