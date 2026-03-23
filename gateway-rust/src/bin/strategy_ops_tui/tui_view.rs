use super::*;
use chrono::{DateTime, Local, Utc};
use libc::{self, termios as Termios};
use crate::strategy::alpha_redecision::{
    AlphaDecisionClass, AlphaReDecisionAction, AlphaRecoveryDecision,
    AlphaRecoveryOperatorStatus, AlphaUnknownExposureReason,
};
use crate::strategy::replay::StrategyExecutionFactStatus;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io::{self, IsTerminal, Read, Write};
use std::os::fd::AsRawFd;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

const ANSI_RESET: &str = "\x1b[0m";
const ANSI_BOLD: &str = "1";
const ANSI_DIM: &str = "2";
const ANSI_RED: &str = "31";
const ANSI_GREEN: &str = "32";
const ANSI_YELLOW: &str = "33";
const ANSI_BLUE: &str = "34";
const ANSI_MAGENTA: &str = "35";
const ANSI_CYAN: &str = "36";

#[derive(Debug, Clone)]
pub(super) struct DashboardUiState {
    pub(super) show_help: bool,
    pub(super) show_catchup: bool,
    pub(super) show_runtime: bool,
    pub(super) show_metrics: bool,
    pub(super) show_errors: bool,
    pub(super) paused: bool,
    pub(super) colors_enabled: bool,
    pub(super) visible_decisions: usize,
}

impl DashboardUiState {
    pub(super) fn new(max_decisions: usize) -> Self {
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
pub(super) enum UiKey {
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
pub(super) enum KeyHandling {
    Quit,
    RefreshNow,
    RenderOnly,
}

pub(super) struct TerminalUiGuard {
    original_termios: Option<Termios>,
}

impl TerminalUiGuard {
    pub(super) fn enter() -> Self {
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

pub(super) fn handle_key(key: UiKey, ui: &mut DashboardUiState) -> KeyHandling {
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

pub(super) fn render_dashboard(
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

pub(super) fn parse_selected_metrics(raw: &str) -> BTreeMap<String, String> {
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

pub(super) fn now_epoch_ns() -> u64 {
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

pub(super) fn spawn_key_reader() -> mpsc::UnboundedReceiver<UiKey> {
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
