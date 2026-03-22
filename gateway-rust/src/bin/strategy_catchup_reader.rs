use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::env;
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
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
use strategy::http_client::{
    decode_json_or_string, fetch_catchup_page as fetch_strategy_catchup_page, parse_http_base_url,
};
use strategy::intent::StrategyIntent;
use strategy::replay::StrategyExecutionCatchupInput;

#[derive(Debug, Clone, PartialEq, Eq)]
enum ReaderScope {
    ExecutionRunId(String),
    IntentId(String),
}

impl ReaderScope {
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
}

#[derive(Debug, Clone)]
struct ReaderConfig {
    base_url: String,
    scope: ReaderScope,
    after_cursor: Option<u64>,
    cursor_path: Option<String>,
    limit: usize,
    max_pages: Option<usize>,
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
    loop_interval_ms: Option<u64>,
    loop_iterations: Option<usize>,
    adapt_proposal: bool,
    submit_proposal: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct ReaderCursorState {
    scope_kind: String,
    scope_id: String,
    next_cursor: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct ReaderOutput {
    scope_kind: String,
    scope_id: String,
    snapshot: StrategyExecutionCatchupLoopSnapshot,
    #[serde(skip_serializing_if = "Option::is_none")]
    recovery_context: Option<AlphaRecoveryContext>,
    #[serde(skip_serializing_if = "Option::is_none")]
    redecision_input: Option<AlphaReDecisionInput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    redecision: Option<AlphaReDecision>,
    #[serde(skip_serializing_if = "Option::is_none")]
    execution_result: Option<ReaderExecutionResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct ReaderLoopTickOutput {
    iteration: usize,
    polled_at_ns: u64,
    output: ReaderOutput,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct ReaderExecutionResult {
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

impl ReaderExecutionResult {
    fn skipped(reason: impl Into<String>) -> Self {
        Self {
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

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let config = parse_args()?;
    let mut next_cursor = if let Some(after_cursor) = config.after_cursor {
        after_cursor
    } else if let Some(cursor_path) = config.cursor_path.as_deref() {
        load_cursor_state(cursor_path, &config.scope)?.next_cursor
    } else {
        0
    };

    if let Some(loop_interval_ms) = config.loop_interval_ms {
        let mut iteration = 0usize;
        loop {
            if let Some(loop_iterations) = config.loop_iterations {
                if iteration >= loop_iterations {
                    break;
                }
            }

            let (output, updated_cursor) = run_single_pass(&config, next_cursor).await?;
            next_cursor = updated_cursor;
            let tick = ReaderLoopTickOutput {
                iteration: iteration.saturating_add(1),
                polled_at_ns: now_epoch_ns(),
                output,
            };
            let raw = serde_json::to_string(&tick).map_err(|err| err.to_string())?;
            println!("{raw}");
            iteration = iteration.saturating_add(1);

            if let Some(loop_iterations) = config.loop_iterations {
                if iteration >= loop_iterations {
                    break;
                }
            }
            sleep(Duration::from_millis(loop_interval_ms.max(1))).await;
        }
        return Ok(());
    }

    let (output, _) = run_single_pass(&config, next_cursor).await?;
    let raw = serde_json::to_string_pretty(&output).map_err(|err| err.to_string())?;
    println!("{raw}");
    Ok(())
}

async fn run_single_pass(
    config: &ReaderConfig,
    start_cursor: u64,
) -> Result<(ReaderOutput, u64), String> {
    let mut loop_state = StrategyExecutionCatchupLoop::new();
    let mut next_cursor = start_cursor;
    let mut pages_read = 0usize;
    loop {
        if let Some(max_pages) = config.max_pages {
            if pages_read >= max_pages {
                break;
            }
        }

        let page =
            fetch_catchup_page(&config.base_url, &config.scope, next_cursor, config.limit).await?;
        loop_state
            .apply_page(&page)
            .map_err(|err| format!("{err:?}"))?;
        next_cursor = loop_state.next_cursor();
        pages_read = pages_read.saturating_add(1);

        if let Some(cursor_path) = config.cursor_path.as_deref() {
            persist_cursor_state(cursor_path, &config.scope, next_cursor)?;
        }

        if !page.has_more || !loop_state.has_more() || page.fact_count == 0 {
            break;
        }
    }

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

    let now_ns = now_epoch_ns();
    let snapshot = loop_state.snapshot();
    let recovery_context = target_signed_qty
        .map(|signed_qty| AlphaRecoveryContext::from_snapshot(snapshot.clone(), signed_qty));
    let redecision_input = match (template_intent.as_ref(), recovery_context.as_ref()) {
        (Some(template), Some(recovery)) => {
            build_redecision_input(template, recovery, config, now_ns)?
        }
        _ => None,
    };
    let redecision = redecision_input
        .clone()
        .map(|input| AlphaReDecision::evaluate(input, now_ns));
    let execution_result = if config.adapt_proposal {
        Some(
            execute_redecision_flow(
                &config.base_url,
                redecision.as_ref(),
                config.submit_proposal,
            )
            .await?,
        )
    } else {
        None
    };

    Ok((
        ReaderOutput {
            scope_kind: match &config.scope {
                ReaderScope::ExecutionRunId(_) => "executionRunId".to_string(),
                ReaderScope::IntentId(_) => "intentId".to_string(),
            },
            scope_id: config.scope.id().to_string(),
            snapshot,
            recovery_context,
            redecision_input,
            redecision,
            execution_result,
        },
        next_cursor,
    ))
}

fn parse_args() -> Result<ReaderConfig, String> {
    let mut base_url = "http://127.0.0.1:8081".to_string();
    let mut execution_run_id = None;
    let mut intent_id = None;
    let mut after_cursor = None;
    let mut cursor_path = None;
    let mut limit = 500usize;
    let mut max_pages = None;
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
    let mut loop_interval_ms = None;
    let mut loop_iterations = None;
    let mut adapt_proposal = false;
    let mut submit_proposal = false;

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
            "--after-cursor" => {
                after_cursor = Some(parse_u64_arg(&mut args, "--after-cursor")?);
            }
            "--cursor-path" => cursor_path = Some(next_arg(&mut args, "--cursor-path")?),
            "--limit" => limit = parse_usize_arg(&mut args, "--limit")?,
            "--max-pages" => max_pages = Some(parse_usize_arg(&mut args, "--max-pages")?),
            "--target-signed-qty" => {
                target_signed_qty = Some(parse_i64_arg(&mut args, "--target-signed-qty")?);
            }
            "--template-intent" => {
                template_intent_path = Some(next_arg(&mut args, "--template-intent")?);
            }
            "--market-desired-signed-qty" => {
                market_desired_signed_qty =
                    Some(parse_i64_arg(&mut args, "--market-desired-signed-qty")?);
            }
            "--market-observed-at-ns" => {
                market_observed_at_ns = Some(parse_u64_arg(&mut args, "--market-observed-at-ns")?);
            }
            "--market-max-decision-age-ns" => {
                market_max_decision_age_ns =
                    Some(parse_u64_arg(&mut args, "--market-max-decision-age-ns")?);
            }
            "--market-snapshot-id" => {
                market_snapshot_id = Some(next_arg(&mut args, "--market-snapshot-id")?);
            }
            "--market-signal-id" => {
                market_signal_id = Some(next_arg(&mut args, "--market-signal-id")?);
            }
            "--next-intent-id" | "--proposal-intent-id" => {
                next_intent_id = Some(next_arg(&mut args, arg.as_str())?);
            }
            "--next-decision-key" | "--proposal-decision-key" => {
                next_decision_key = Some(next_arg(&mut args, arg.as_str())?);
            }
            "--next-created-at-ns" | "--proposal-created-at-ns" => {
                next_created_at_ns = Some(parse_u64_arg(&mut args, arg.as_str())?);
            }
            "--next-expires-at-ns" | "--proposal-expires-at-ns" => {
                next_expires_at_ns = Some(parse_u64_arg(&mut args, arg.as_str())?);
            }
            "--loop-interval-ms" => {
                loop_interval_ms = Some(parse_u64_arg(&mut args, "--loop-interval-ms")?);
            }
            "--loop-iterations" => {
                loop_iterations = Some(parse_usize_arg(&mut args, "--loop-iterations")?);
            }
            "--adapt-proposal" => adapt_proposal = true,
            "--submit-proposal" => {
                submit_proposal = true;
                adapt_proposal = true;
            }
            "--unconfirmed-policy" | "--loss-suspect-policy" => {
                let _ = next_arg(&mut args, arg.as_str())?;
            }
            other => return Err(format!("unknown arg: {other}")),
        }
    }

    let scope = match (execution_run_id, intent_id) {
        (Some(_), Some(_)) => {
            return Err("choose either --execution-run-id or --intent-id".to_string());
        }
        (Some(value), None) => ReaderScope::ExecutionRunId(value),
        (None, Some(value)) => ReaderScope::IntentId(value),
        (None, None) => {
            return Err("one of --execution-run-id or --intent-id is required".to_string());
        }
    };

    if adapt_proposal {
        if template_intent_path.is_none() {
            return Err("--adapt-proposal requires --template-intent".to_string());
        }
        if market_desired_signed_qty.is_none() {
            return Err("--adapt-proposal requires --market-desired-signed-qty".to_string());
        }
    }
    if submit_proposal && !adapt_proposal {
        return Err("--submit-proposal requires --adapt-proposal".to_string());
    }

    Ok(ReaderConfig {
        base_url,
        scope,
        after_cursor,
        cursor_path,
        limit: limit.max(1),
        max_pages,
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
        loop_interval_ms,
        loop_iterations,
        adapt_proposal,
        submit_proposal,
    })
}

fn next_arg(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("missing value for {flag}"))
}

fn parse_u64_arg(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<u64, String> {
    next_arg(args, flag)?
        .parse::<u64>()
        .map_err(|err| format!("invalid value for {flag}: {err}"))
}

fn parse_usize_arg(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<usize, String> {
    next_arg(args, flag)?
        .parse::<usize>()
        .map_err(|err| format!("invalid value for {flag}: {err}"))
}

fn parse_i64_arg(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<i64, String> {
    next_arg(args, flag)?
        .parse::<i64>()
        .map_err(|err| format!("invalid value for {flag}: {err}"))
}

fn print_usage() {
    println!("strategy_catchup_reader");
    println!("usage:");
    println!(
        "  cargo run --manifest-path gateway-rust/Cargo.toml --bin strategy_catchup_reader -- \\"
    );
    println!("    (--execution-run-id RUN_ID | --intent-id INTENT_ID) \\");
    println!("    [--base-url http://127.0.0.1:8081] [--after-cursor 0] \\");
    println!(
        "    [--cursor-path var/gateway/catchup.cursor.json] [--limit 500] [--max-pages 100] \\"
    );
    println!("    [--target-signed-qty 100] [--template-intent /tmp/intent.json] \\");
    println!(
        "    [--market-desired-signed-qty 60] [--market-observed-at-ns NOW_NS] [--market-max-decision-age-ns 1000000] \\"
    );
    println!("    [--market-snapshot-id snap-1] [--market-signal-id signal-1] \\");
    println!("    [--next-intent-id fresh-intent-1] [--next-decision-key fresh-decision-1] \\");
    println!("    [--next-created-at-ns 1000] [--next-expires-at-ns 2000] \\");
    println!("    [--loop-interval-ms 1000] [--loop-iterations 10] \\");
    println!("    [--adapt-proposal] [--submit-proposal]");
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
    config: &ReaderConfig,
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
    config: &ReaderConfig,
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
    config: &ReaderConfig,
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

fn now_epoch_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

fn load_cursor_state(path: &str, scope: &ReaderScope) -> Result<ReaderCursorState, String> {
    let raw = fs::read_to_string(path).map_err(|err| format!("read cursor file failed: {err}"))?;
    let state: ReaderCursorState =
        serde_json::from_str(&raw).map_err(|err| format!("parse cursor file failed: {err}"))?;
    let expected_kind = match scope {
        ReaderScope::ExecutionRunId(_) => "executionRunId",
        ReaderScope::IntentId(_) => "intentId",
    };
    if state.scope_kind != expected_kind || state.scope_id != scope.id() {
        return Err("cursor file scope mismatch".to_string());
    }
    Ok(state)
}

fn persist_cursor_state(path: &str, scope: &ReaderScope, next_cursor: u64) -> Result<(), String> {
    let state = ReaderCursorState {
        scope_kind: match scope {
            ReaderScope::ExecutionRunId(_) => "executionRunId".to_string(),
            ReaderScope::IntentId(_) => "intentId".to_string(),
        },
        scope_id: scope.id().to_string(),
        next_cursor,
    };
    let raw = serde_json::to_string_pretty(&state).map_err(|err| err.to_string())?;
    if let Some(parent) = Path::new(path).parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).map_err(|err| err.to_string())?;
        }
    }
    fs::write(path, raw + "\n").map_err(|err| format!("write cursor file failed: {err}"))
}

async fn fetch_catchup_page(
    base_url: &str,
    scope: &ReaderScope,
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

async fn execute_redecision_flow(
    base_url: &str,
    redecision: Option<&AlphaReDecision>,
    submit_proposal: bool,
) -> Result<ReaderExecutionResult, String> {
    let Some(redecision) = redecision else {
        return Ok(ReaderExecutionResult::skipped("REDECISION_NOT_REQUESTED"));
    };
    if redecision.action != AlphaReDecisionAction::SubmitFreshIntent {
        return Ok(ReaderExecutionResult::skipped(redecision_skip_reason(
            redecision,
        )));
    }
    let Some(intent) = redecision.proposed_intent.as_ref() else {
        return Ok(ReaderExecutionResult::skipped("PROPOSED_INTENT_MISSING"));
    };

    let parsed = parse_http_base_url(base_url)?;
    let adapt_response = strategy::http_client::post_strategy_intent_adapt(&parsed, intent).await?;
    let adapt_ok = (200..300).contains(&adapt_response.status_code);
    let adapt_body = decode_json_or_string(&adapt_response.body);

    if !submit_proposal {
        return Ok(ReaderExecutionResult {
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
        return Ok(ReaderExecutionResult {
            adapt_http_status: Some(adapt_response.status_code),
            adapt_ok: Some(adapt_ok),
            adapt_response: Some(adapt_body),
            submit_http_status: None,
            submit_ok: None,
            submit_response: None,
            skipped_reason: Some("ADAPT_NOT_SUCCESS".to_string()),
        });
    }

    let submit_response =
        strategy::http_client::post_strategy_intent_submit(&parsed, intent).await?;
    let submit_ok = (200..300).contains(&submit_response.status_code);
    let submit_body = decode_json_or_string(&submit_response.body);

    Ok(ReaderExecutionResult {
        adapt_http_status: Some(adapt_response.status_code),
        adapt_ok: Some(adapt_ok),
        adapt_response: Some(adapt_body),
        submit_http_status: Some(submit_response.status_code),
        submit_ok: Some(submit_ok),
        submit_response: Some(submit_body),
        skipped_reason: None,
    })
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

#[cfg(test)]
mod tests {
    use super::{
        ReaderConfig, ReaderCursorState, ReaderScope, build_redecision_input, persist_cursor_state,
    };
    use crate::order::{OrderType, TimeInForce};
    use crate::strategy::alpha_redecision::{
        AlphaRecoveryContext, AlphaRecoveryOperatorStatus, AlphaUnknownExposureBreakdown,
    };
    use crate::strategy::http_client::decode_json_or_string;
    use crate::strategy::intent::{
        ExecutionPolicyKind, IntentUrgency, STRATEGY_INTENT_SCHEMA_VERSION, StrategyIntent,
    };
    use serde_json::Value;
    use std::fs;

    fn template_intent() -> StrategyIntent {
        StrategyIntent {
            schema_version: STRATEGY_INTENT_SCHEMA_VERSION,
            intent_id: "template-intent-1".to_string(),
            account_id: "acc-1".to_string(),
            session_id: "sess-1".to_string(),
            symbol: "AAPL".to_string(),
            side: "BUY".to_string(),
            order_type: OrderType::Limit,
            qty: 100,
            limit_price: Some(100),
            time_in_force: TimeInForce::Ioc,
            urgency: IntentUrgency::High,
            execution_policy: ExecutionPolicyKind::Aggressive,
            risk_budget_ref: None,
            model_id: Some("model-1".to_string()),
            execution_run_id: Some("run-1".to_string()),
            decision_key: Some("decision-template-1".to_string()),
            decision_attempt_seq: Some(1),
            decision_basis_at_ns: Some(100),
            max_decision_age_ns: Some(1_000),
            market_snapshot_id: Some("market-template-1".to_string()),
            signal_id: Some("signal-template-1".to_string()),
            recovery_policy: None,
            algo: None,
            created_at_ns: 100,
            expires_at_ns: 1_100,
        }
    }

    fn reader_config() -> ReaderConfig {
        ReaderConfig {
            base_url: "http://127.0.0.1:8081".to_string(),
            scope: ReaderScope::ExecutionRunId("run-1".to_string()),
            after_cursor: None,
            cursor_path: None,
            limit: 500,
            max_pages: None,
            target_signed_qty: None,
            template_intent_path: None,
            market_desired_signed_qty: Some(60),
            market_observed_at_ns: Some(1_000),
            market_max_decision_age_ns: None,
            market_snapshot_id: None,
            market_signal_id: None,
            next_intent_id: None,
            next_decision_key: None,
            next_created_at_ns: Some(1_005),
            next_expires_at_ns: None,
            loop_interval_ms: None,
            loop_iterations: None,
            adapt_proposal: false,
            submit_proposal: false,
        }
    }

    #[test]
    fn persist_cursor_state_writes_scope_guarded_json() {
        let path = std::env::temp_dir().join(format!(
            "strategy_catchup_reader_cursor_{}.json",
            std::process::id()
        ));
        let path_string = path.to_string_lossy().to_string();
        persist_cursor_state(
            &path_string,
            &ReaderScope::ExecutionRunId("run-1".to_string()),
            77,
        )
        .expect("persist cursor");
        let raw = fs::read_to_string(&path).expect("read cursor");
        let parsed: ReaderCursorState = serde_json::from_str(&raw).expect("parse cursor");
        assert_eq!(parsed.scope_kind, "executionRunId");
        assert_eq!(parsed.scope_id, "run-1");
        assert_eq!(parsed.next_cursor, 77);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn build_redecision_input_uses_deterministic_defaults() {
        let recovery = AlphaRecoveryContext {
            execution_run_id: Some("run-1".to_string()),
            intent_id: Some("intent-1".to_string()),
            target_signed_qty: 100,
            filled_signed_qty: 40,
            open_signed_qty: 0,
            failed_signed_qty: 20,
            unknown_signed_qty: 0,
            unknown_exposure_breakdown: AlphaUnknownExposureBreakdown::default(),
            unsent_signed_qty: 40,
            requires_manual_intervention: false,
            operator_status: AlphaRecoveryOperatorStatus::ReadyForReDecision,
            operator_reason: None,
            next_cursor: 77,
            has_more: false,
            latest_status_totals: Default::default(),
            decisions: vec![],
        };

        let input = build_redecision_input(&template_intent(), &recovery, &reader_config(), 1_500)
            .expect("build input")
            .expect("redecision input");

        assert_eq!(input.market.desired_signed_qty, 60);
        assert_eq!(input.market.observed_at_ns, 1_000);
        assert_eq!(input.market.max_decision_age_ns, 1_000);
        assert_eq!(
            input.market.market_snapshot_id.as_deref(),
            Some("market-template-1")
        );
        assert_eq!(input.market.signal_id.as_deref(), Some("signal-template-1"));
        assert_eq!(
            input.next_intent.intent_id,
            "template-intent-1::redecision::77"
        );
        assert_eq!(input.next_intent.decision_key, "redecision-77");
        assert_eq!(input.next_intent.decision_attempt_seq, 1);
        assert_eq!(input.next_intent.created_at_ns, 1_005);
        assert_eq!(input.next_intent.expires_at_ns, None);
    }

    #[test]
    fn build_redecision_input_returns_none_without_market_qty() {
        let mut config = reader_config();
        config.market_desired_signed_qty = None;
        let recovery = AlphaRecoveryContext {
            execution_run_id: Some("run-1".to_string()),
            intent_id: Some("intent-1".to_string()),
            target_signed_qty: 100,
            filled_signed_qty: 40,
            open_signed_qty: 0,
            failed_signed_qty: 20,
            unknown_signed_qty: 0,
            unknown_exposure_breakdown: AlphaUnknownExposureBreakdown::default(),
            unsent_signed_qty: 40,
            requires_manual_intervention: false,
            operator_status: AlphaRecoveryOperatorStatus::ReadyForReDecision,
            operator_reason: None,
            next_cursor: 77,
            has_more: false,
            latest_status_totals: Default::default(),
            decisions: vec![],
        };

        let input =
            build_redecision_input(&template_intent(), &recovery, &config, 1_500).expect("build");
        assert!(input.is_none());
    }

    #[test]
    fn decode_json_or_string_falls_back_to_plain_text() {
        let value = decode_json_or_string(b"not-json");
        assert_eq!(value, Value::String("not-json".to_string()));
    }
}
