use chrono::{DateTime, Local, Utc};
use libc::{self, termios as Termios};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::env;
use std::fmt::Write as _;
use std::fs;
use std::io::{self, IsTerminal, Read, Write};
use std::os::fd::AsRawFd;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::time::{Duration, interval};

#[allow(dead_code)]
#[path = "../order/mod.rs"]
mod order;
mod strategy;

use strategy::alpha_redecision::{
    AlphaDecisionClass, AlphaMarketContext, AlphaNextIntentParams, AlphaReDecision,
    AlphaReDecisionAction, AlphaReDecisionInput, AlphaRecoveryContext, AlphaRecoveryDecision,
    AlphaRecoveryOperatorStatus, AlphaUnknownExposureReason,
};
use strategy::catchup::{StrategyExecutionCatchupLoop, target_signed_qty_for_intent};
use strategy::http_client::{
    ACCEPT_JSON_OR_TEXT, fetch_catchup_page as fetch_strategy_catchup_page,
    http_get_body_with_accept, parse_http_base_url,
};
use strategy::intent::StrategyIntent;
use strategy::replay::{StrategyExecutionCatchupInput, StrategyExecutionFactStatus};

const DEFAULT_BASE_URL: &str = "http://127.0.0.1:8081";
const DEFAULT_LIMIT: usize = 500;
const DEFAULT_POLL_INTERVAL_MS: u64 = 1_000;
const DEFAULT_MAX_DECISIONS: usize = 12;
const MAX_CATCHUP_PAGES_PER_REFRESH: usize = 8;
const ANSI_RESET: &str = "\x1b[0m";
const ANSI_BOLD: &str = "1";
const ANSI_DIM: &str = "2";
const ANSI_RED: &str = "31";
const ANSI_GREEN: &str = "32";
const ANSI_YELLOW: &str = "33";
const ANSI_BLUE: &str = "34";
const ANSI_MAGENTA: &str = "35";
const ANSI_CYAN: &str = "36";
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

#[derive(Debug, Clone)]
struct DashboardUiState {
    show_help: bool,
    show_catchup: bool,
    show_runtime: bool,
    show_metrics: bool,
    show_errors: bool,
    paused: bool,
    colors_enabled: bool,
    visible_decisions: usize,
}

impl DashboardUiState {
    fn new(max_decisions: usize) -> Self {
        Self {
            show_help: false,
            show_catchup: true,
            show_runtime: true,
            show_metrics: true,
            show_errors: true,
            paused: false,
            colors_enabled: io::stdout().is_terminal(),
            visible_decisions: max_decisions.max(1),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UiKey {
    Quit,
    ToggleHelp,
    ToggleCatchup,
    ToggleRuntime,
    ToggleMetrics,
    ToggleErrors,
    TogglePause,
    RefreshNow,
    IncreaseRows,
    DecreaseRows,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyHandling {
    Quit,
    RefreshNow,
    RenderOnly,
}

#[derive(Debug, Clone, Default)]
struct DashboardState {
    catchup: Option<CatchupView>,
    runtime: Option<AlgoRuntimeView>,
    metrics: BTreeMap<String, String>,
    errors: Vec<String>,
    last_refresh_ns: Option<u64>,
}

struct TerminalUiGuard {
    original_termios: Option<Termios>,
}

impl TerminalUiGuard {
    fn enter() -> Self {
        let original_termios = configure_raw_mode();
        print!("\x1b[?25l");
        let _ = io::stdout().flush();
        Self { original_termios }
    }
}

impl Drop for TerminalUiGuard {
    fn drop(&mut self) {
        restore_raw_mode(self.original_termios.as_ref());
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

fn handle_key(key: UiKey, ui: &mut DashboardUiState) -> KeyHandling {
    match key {
        UiKey::Quit => KeyHandling::Quit,
        UiKey::ToggleHelp => {
            ui.show_help = !ui.show_help;
            KeyHandling::RenderOnly
        }
        UiKey::ToggleCatchup => {
            ui.show_catchup = !ui.show_catchup;
            KeyHandling::RenderOnly
        }
        UiKey::ToggleRuntime => {
            ui.show_runtime = !ui.show_runtime;
            KeyHandling::RenderOnly
        }
        UiKey::ToggleMetrics => {
            ui.show_metrics = !ui.show_metrics;
            KeyHandling::RenderOnly
        }
        UiKey::ToggleErrors => {
            ui.show_errors = !ui.show_errors;
            KeyHandling::RenderOnly
        }
        UiKey::TogglePause => {
            ui.paused = !ui.paused;
            KeyHandling::RenderOnly
        }
        UiKey::RefreshNow => KeyHandling::RefreshNow,
        UiKey::IncreaseRows => {
            ui.visible_decisions = ui.visible_decisions.saturating_add(1);
            KeyHandling::RenderOnly
        }
        UiKey::DecreaseRows => {
            ui.visible_decisions = ui.visible_decisions.saturating_sub(1).max(1);
            KeyHandling::RenderOnly
        }
    }
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

fn render_dashboard(
    config: &TuiConfig,
    ui: &DashboardUiState,
    state: &DashboardState,
) -> Result<(), String> {
    let mut out = String::new();
    let now_ns = now_epoch_ns();
    let refreshed_at = state
        .last_refresh_ns
        .map(format_timestamp_ns)
        .unwrap_or_else(|| "-".to_string());
    let refresh_mode = if ui.paused {
        paint(ui.colors_enabled, ANSI_YELLOW, "paused")
    } else {
        paint(ui.colors_enabled, ANSI_GREEN, "live")
    };

    write!(
        out,
        "\x1b[2J\x1b[H{}  refreshed={}  poll={}ms  refresh={}  ctrl-c=exit\n",
        paint(ui.colors_enabled, ANSI_BOLD, "Strategy Ops TUI"),
        refreshed_at,
        config.poll_interval_ms,
        refresh_mode,
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
    writeln!(out, "Keys: {}", render_key_summary(ui.colors_enabled))
        .map_err(|err| err.to_string())?;
    writeln!(
        out,
        "Panels: catchup={} runtime={} metrics={} errors={} rows={}",
        on_off(ui.show_catchup),
        on_off(ui.show_runtime),
        on_off(ui.show_metrics),
        on_off(ui.show_errors),
        ui.visible_decisions,
    )
    .map_err(|err| err.to_string())?;
    out.push('\n');

    if ui.show_help {
        render_help_panel(&mut out, ui.colors_enabled)?;
    }
    if ui.show_catchup {
        if let Some(catchup) = state.catchup.as_ref() {
            render_catchup_panel(
                &mut out,
                catchup,
                ui.visible_decisions,
                now_ns,
                ui.colors_enabled,
            )?;
        } else {
            out.push_str("Catch-up\n  not configured\n\n");
        }
    }
    if ui.show_runtime {
        if let Some(runtime) = state.runtime.as_ref() {
            render_runtime_panel(&mut out, runtime, now_ns, ui.colors_enabled)?;
        } else {
            out.push_str("Algo Runtime\n  not configured\n\n");
        }
    }
    if ui.show_metrics {
        render_metrics_panel(&mut out, &state.metrics, ui.colors_enabled)?;
    }
    if ui.show_errors {
        render_error_panel(&mut out, &state.errors, ui.colors_enabled)?;
    }

    print!("{out}");
    io::stdout().flush().map_err(|err| err.to_string())
}

fn render_help_panel(out: &mut String, colors_enabled: bool) -> Result<(), String> {
    writeln!(out, "{}", paint(colors_enabled, ANSI_CYAN, "Help")).map_err(|err| err.to_string())?;
    writeln!(out, "  q      quit").map_err(|err| err.to_string())?;
    writeln!(out, "  h, ?   toggle help").map_err(|err| err.to_string())?;
    writeln!(out, "  p      pause/resume auto refresh").map_err(|err| err.to_string())?;
    writeln!(out, "  space  refresh now").map_err(|err| err.to_string())?;
    writeln!(out, "  c      toggle catch-up panel").map_err(|err| err.to_string())?;
    writeln!(out, "  r      toggle runtime panel").map_err(|err| err.to_string())?;
    writeln!(out, "  m      toggle metrics panel").map_err(|err| err.to_string())?;
    writeln!(out, "  e      toggle errors panel").map_err(|err| err.to_string())?;
    writeln!(out, "  + / -  increase/decrease visible decision rows")
        .map_err(|err| err.to_string())?;
    out.push('\n');
    Ok(())
}

fn render_catchup_panel(
    out: &mut String,
    catchup: &CatchupView,
    max_decisions: usize,
    now_ns: u64,
    colors_enabled: bool,
) -> Result<(), String> {
    writeln!(out, "{}", paint(colors_enabled, ANSI_CYAN, "Catch-up"))
        .map_err(|err| err.to_string())?;
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
        let unknown_text = if recovery.unknown_signed_qty != 0 {
            paint(
                colors_enabled,
                ANSI_YELLOW,
                format_signed(recovery.unknown_signed_qty),
            )
        } else {
            format_signed(recovery.unknown_signed_qty)
        };
        writeln!(
            out,
            "  recovery target={} committed={} filled={} open={} failed={} unknown={} unsent={}",
            format_signed(recovery.target_signed_qty),
            format_signed(recovery.committed_signed_qty()),
            format_signed(recovery.filled_signed_qty),
            format_signed(recovery.open_signed_qty),
            format_signed(recovery.failed_signed_qty),
            unknown_text,
            format_signed(recovery.unsent_signed_qty),
        )
        .map_err(|err| err.to_string())?;
        writeln!(
            out,
            "  operator {} {}",
            paint_operator_status(
                colors_enabled,
                &recovery.operator_status,
                format_debug(&recovery.operator_status),
            ),
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
            paint_redecision_action(colors_enabled, redecision),
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
        "  {} {} {} {} {} {} {}",
        fit_cell("decision", 22),
        fit_cell_right("qty", 8),
        fit_cell("latest", 18),
        fit_cell("live", 18),
        fit_cell_right("filled", 7),
        fit_cell_right("remain", 7),
        fit_cell("reason", 18),
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
            .map(|decision| AlphaRecoveryDecision {
                decision,
                class: AlphaDecisionClass::Unknown,
                filled_signed_qty: 0,
                open_signed_qty: 0,
                failed_signed_qty: 0,
                unknown_signed_qty: 0,
                unknown_reason: None,
            })
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
        let latest_cell = paint_fact_status(
            colors_enabled,
            row.decision.latest_status,
            fit_cell(format_fact_status(row.decision.latest_status), 18),
        );
        let live_cell = paint_live_status(
            colors_enabled,
            live_status,
            fit_cell(trim_to_width(live_status, 18), 18),
        );
        let reason_raw = row
            .decision
            .reason
            .as_deref()
            .or_else(|| row.unknown_reason.map(unknown_reason_label))
            .unwrap_or("-");
        let reason_cell = paint_reason_cell(
            colors_enabled,
            &row,
            fit_cell(trim_to_width(reason_raw, 18), 18),
        );
        writeln!(
            out,
            "  {} {} {} {} {} {} {}",
            fit_cell(decision_label(&row.decision), 22),
            fit_cell_right(
                row.decision
                    .position_delta_qty
                    .unwrap_or_default()
                    .to_string(),
                8
            ),
            latest_cell,
            live_cell,
            fit_cell_right(live_filled.to_string(), 7),
            fit_cell_right(live_remaining.to_string(), 7),
            reason_cell,
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
    colors_enabled: bool,
) -> Result<(), String> {
    writeln!(out, "{}", paint(colors_enabled, ANSI_CYAN, "Algo Runtime"))
        .map_err(|err| err.to_string())?;
    writeln!(
        out,
        "  parent={} status={} policy={} recovery={} totalQty={} children={} updated={}",
        runtime.parent_intent_id,
        paint_runtime_status(colors_enabled, &runtime.status, runtime.status.clone()),
        runtime.policy,
        runtime.runtime_mode,
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
        "  {} {} {} {} {}",
        fit_cell_right("seq", 3),
        fit_cell_right("qty", 8),
        fit_cell("status", 18),
        fit_cell_right("sessSeq", 8),
        fit_cell("reason", 18),
    )
    .map_err(|err| err.to_string())?;
    for slice in runtime.slices.iter().take(12) {
        writeln!(
            out,
            "  {} {} {} {} {}",
            fit_cell_right(slice.sequence.to_string(), 3),
            fit_cell_right(slice.qty.to_string(), 8),
            paint_runtime_status(colors_enabled, &slice.status, fit_cell(&slice.status, 18)),
            fit_cell_right(
                slice
                    .session_seq
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                8,
            ),
            paint_reason_text(
                colors_enabled,
                slice.reject_reason.as_deref(),
                fit_cell(
                    trim_to_width(slice.reject_reason.as_deref().unwrap_or("-"), 18),
                    18
                ),
            ),
        )
        .map_err(|err| err.to_string())?;
    }
    out.push('\n');
    Ok(())
}

fn render_metrics_panel(
    out: &mut String,
    metrics: &BTreeMap<String, String>,
    colors_enabled: bool,
) -> Result<(), String> {
    writeln!(out, "{}", paint(colors_enabled, ANSI_CYAN, "Metrics"))
        .map_err(|err| err.to_string())?;
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

fn render_error_panel(
    out: &mut String,
    errors: &[String],
    colors_enabled: bool,
) -> Result<(), String> {
    writeln!(out, "{}", paint(colors_enabled, ANSI_CYAN, "Errors"))
        .map_err(|err| err.to_string())?;
    if errors.is_empty() {
        writeln!(out, "  -").map_err(|err| err.to_string())?;
        return Ok(());
    }
    for error in errors {
        writeln!(out, "  {}", paint(colors_enabled, ANSI_RED, error))
            .map_err(|err| err.to_string())?;
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
) -> Result<Option<AlphaReDecisionInput>, String> {
    let Some(market_desired_signed_qty) = config.market_desired_signed_qty else {
        return Ok(None);
    };
    let market = build_market_context(template, config, market_desired_signed_qty, now_ns)?;
    let next_intent = build_next_intent_params(template, recovery, config, now_ns)?;
    Ok(Some(AlphaReDecisionInput {
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
    fetch_strategy_catchup_page(
        base_url,
        scope.endpoint_path(),
        scope.id(),
        after_cursor,
        limit,
    )
    .await
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

fn render_key_summary(colors_enabled: bool) -> String {
    [
        ("q", "quit"),
        ("h", "help"),
        ("p", "pause"),
        ("space", "refresh"),
        ("c/r/m/e", "toggle panels"),
        ("+/-", "rows"),
    ]
    .into_iter()
    .map(|(key, label)| format!("{} {}", paint(colors_enabled, ANSI_BOLD, key), label))
    .collect::<Vec<_>>()
    .join("  ")
}

fn on_off(enabled: bool) -> &'static str {
    if enabled { "on" } else { "off" }
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

fn fit_cell(value: impl AsRef<str>, width: usize) -> String {
    format!(
        "{:<width$}",
        trim_to_width(value.as_ref(), width),
        width = width
    )
}

fn fit_cell_right(value: impl AsRef<str>, width: usize) -> String {
    format!(
        "{:>width$}",
        trim_to_width(value.as_ref(), width),
        width = width
    )
}

fn unknown_reason_label(reason: AlphaUnknownExposureReason) -> &'static str {
    match reason {
        AlphaUnknownExposureReason::Unconfirmed => "UNCONFIRMED",
        AlphaUnknownExposureReason::LossSuspect => "LOSS_SUSPECT",
        AlphaUnknownExposureReason::ResidualUnknown => "RESIDUAL_UNKNOWN",
        AlphaUnknownExposureReason::None => "-",
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

fn paint(colors_enabled: bool, code: &str, text: impl AsRef<str>) -> String {
    let text = text.as_ref();
    if !colors_enabled {
        return text.to_string();
    }
    format!("\x1b[{code}m{text}{ANSI_RESET}")
}

fn paint_fact_status(
    colors_enabled: bool,
    status: StrategyExecutionFactStatus,
    text: String,
) -> String {
    let code = match status {
        StrategyExecutionFactStatus::DurableAccepted => ANSI_GREEN,
        StrategyExecutionFactStatus::Rejected | StrategyExecutionFactStatus::DurableRejected => {
            ANSI_RED
        }
        StrategyExecutionFactStatus::Unconfirmed | StrategyExecutionFactStatus::LossSuspect => {
            ANSI_YELLOW
        }
    };
    paint(colors_enabled, code, text)
}

fn paint_live_status(colors_enabled: bool, raw_status: &str, text: String) -> String {
    let upper = raw_status.trim().to_ascii_uppercase();
    let code = if raw_status == "-" {
        ANSI_DIM
    } else if upper.contains("ACTIVE") || upper.contains("OPEN") {
        ANSI_GREEN
    } else if upper.contains("REJECT") || upper.contains("CANCEL") || upper.contains("FAIL") {
        ANSI_RED
    } else {
        ANSI_BLUE
    };
    paint(colors_enabled, code, text)
}

fn paint_runtime_status(colors_enabled: bool, raw_status: &str, text: String) -> String {
    let code = match raw_status {
        "RUNNING" | "DURABLE_ACCEPTED" | "COMPLETED" => ANSI_GREEN,
        "VOLATILE_ACCEPTED" => ANSI_BLUE,
        "DISPATCHING" => ANSI_YELLOW,
        "REJECTED" => ANSI_RED,
        "SKIPPED" | "SCHEDULED" | "PAUSED" => ANSI_DIM,
        _ => ANSI_MAGENTA,
    };
    paint(colors_enabled, code, text)
}

fn paint_reason_text(colors_enabled: bool, reason: Option<&str>, text: String) -> String {
    if reason.is_some() {
        return paint(colors_enabled, ANSI_RED, text);
    }
    text
}

fn paint_reason_cell(colors_enabled: bool, row: &AlphaRecoveryDecision, text: String) -> String {
    if row.unknown_reason.is_some() {
        return paint(colors_enabled, ANSI_YELLOW, text);
    }
    if row.decision.reason.is_some() {
        return paint(colors_enabled, ANSI_RED, text);
    }
    text
}

fn paint_operator_status(
    colors_enabled: bool,
    status: &AlphaRecoveryOperatorStatus,
    text: String,
) -> String {
    let code = match status {
        AlphaRecoveryOperatorStatus::ReadyForReDecision => ANSI_GREEN,
        AlphaRecoveryOperatorStatus::HoldUnconfirmedExposure
        | AlphaRecoveryOperatorStatus::HoldResidualUnknownExposure => ANSI_YELLOW,
        AlphaRecoveryOperatorStatus::HoldLossSuspectExposure
        | AlphaRecoveryOperatorStatus::HoldMixedUnknownExposure => ANSI_RED,
    };
    paint(colors_enabled, code, text)
}

fn paint_redecision_action(colors_enabled: bool, redecision: &AlphaReDecision) -> String {
    let code = match redecision.action {
        AlphaReDecisionAction::SubmitFreshIntent => ANSI_GREEN,
        AlphaReDecisionAction::HoldUnknownExposure => ANSI_YELLOW,
        AlphaReDecisionAction::AbortAlphaStale
        | AlphaReDecisionAction::AbortSideFlip
        | AlphaReDecisionAction::InvalidInput => ANSI_RED,
        AlphaReDecisionAction::NoopNoSignal | AlphaReDecisionAction::NoopAlreadySatisfied => {
            ANSI_DIM
        }
    };
    paint(colors_enabled, code, format_debug(&redecision.action))
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

fn configure_raw_mode() -> Option<Termios> {
    if !io::stdin().is_terminal() {
        return None;
    }
    let fd = io::stdin().as_raw_fd();
    let mut original = unsafe { std::mem::zeroed::<Termios>() };
    if unsafe { libc::tcgetattr(fd, &mut original) } != 0 {
        return None;
    }
    let mut raw = original;
    raw.c_lflag &= !(libc::ICANON | libc::ECHO);
    raw.c_cc[libc::VMIN] = 1;
    raw.c_cc[libc::VTIME] = 0;
    if unsafe { libc::tcsetattr(fd, libc::TCSANOW, &raw) } != 0 {
        return None;
    }
    Some(original)
}

fn restore_raw_mode(original: Option<&Termios>) {
    let Some(original) = original else {
        return;
    };
    let fd = io::stdin().as_raw_fd();
    let _ = unsafe { libc::tcsetattr(fd, libc::TCSANOW, original) };
}

fn spawn_key_reader() -> mpsc::UnboundedReceiver<UiKey> {
    let (tx, rx) = mpsc::unbounded_channel();
    if !io::stdin().is_terminal() {
        return rx;
    }
    thread::spawn(move || {
        let stdin = io::stdin();
        let mut locked = stdin.lock();
        let mut buf = [0u8; 1];
        loop {
            match locked.read(&mut buf) {
                Ok(0) => continue,
                Ok(_) => {
                    if let Some(key) = parse_ui_key(buf[0]) {
                        if tx.send(key).is_err() {
                            break;
                        }
                    }
                }
                Err(_) => break,
            }
        }
    });
    rx
}

fn parse_ui_key(byte: u8) -> Option<UiKey> {
    match byte {
        b'q' | b'Q' => Some(UiKey::Quit),
        b'h' | b'H' | b'?' => Some(UiKey::ToggleHelp),
        b'c' | b'C' => Some(UiKey::ToggleCatchup),
        b'r' | b'R' => Some(UiKey::ToggleRuntime),
        b'm' | b'M' => Some(UiKey::ToggleMetrics),
        b'e' | b'E' => Some(UiKey::ToggleErrors),
        b'p' | b'P' => Some(UiKey::TogglePause),
        b' ' | b'\n' | b'\r' => Some(UiKey::RefreshNow),
        b'+' | b'=' => Some(UiKey::IncreaseRows),
        b'-' | b'_' => Some(UiKey::DecreaseRows),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{METRIC_NAMES, UiKey, parse_selected_metrics, parse_ui_key, trim_to_width};

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

    #[test]
    fn parse_ui_key_maps_expected_shortcuts() {
        assert_eq!(parse_ui_key(b'q'), Some(UiKey::Quit));
        assert_eq!(parse_ui_key(b'h'), Some(UiKey::ToggleHelp));
        assert_eq!(parse_ui_key(b' '), Some(UiKey::RefreshNow));
        assert_eq!(parse_ui_key(b'+'), Some(UiKey::IncreaseRows));
        assert_eq!(parse_ui_key(b'x'), None);
    }
}
