use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::strategy::alpha_redecision::{
    AlphaReDecision, AlphaReDecisionInput, AlphaRecoveryContext,
};
use crate::strategy::catchup::StrategyExecutionCatchupLoopSnapshot;
use crate::strategy::intent::StrategyIntent;
use crate::strategy::persistence::{
    CursorState as ReaderCursorState, load_cursor_state as load_shared_cursor_state,
    persist_cursor_state as persist_shared_cursor_state,
};
use crate::strategy::redecision_support::load_template_intent as load_shared_template_intent;
use crate::strategy::scope::StrategyTargetScope;

pub(super) type ReaderScope = StrategyTargetScope;

#[derive(Debug, Clone)]
pub(super) struct ReaderConfig {
    pub(super) base_url: String,
    pub(super) scope: ReaderScope,
    pub(super) after_cursor: Option<u64>,
    pub(super) cursor_path: Option<String>,
    pub(super) limit: usize,
    pub(super) max_pages: Option<usize>,
    pub(super) target_signed_qty: Option<i64>,
    pub(super) template_intent_path: Option<String>,
    pub(super) market_desired_signed_qty: Option<i64>,
    pub(super) market_observed_at_ns: Option<u64>,
    pub(super) market_max_decision_age_ns: Option<u64>,
    pub(super) market_snapshot_id: Option<String>,
    pub(super) market_signal_id: Option<String>,
    pub(super) next_intent_id: Option<String>,
    pub(super) next_decision_key: Option<String>,
    pub(super) next_created_at_ns: Option<u64>,
    pub(super) next_expires_at_ns: Option<u64>,
    pub(super) loop_interval_ms: Option<u64>,
    pub(super) loop_iterations: Option<usize>,
    pub(super) adapt_proposal: bool,
    pub(super) submit_proposal: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(super) struct ReaderOutput {
    pub(super) scope_kind: String,
    pub(super) scope_id: String,
    pub(super) snapshot: StrategyExecutionCatchupLoopSnapshot,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) recovery_context: Option<AlphaRecoveryContext>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) redecision_input: Option<AlphaReDecisionInput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) redecision: Option<AlphaReDecision>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) execution_result: Option<ReaderExecutionResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(super) struct ReaderLoopTickOutput {
    pub(super) iteration: usize,
    pub(super) polled_at_ns: u64,
    pub(super) output: ReaderOutput,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(super) struct ReaderExecutionResult {
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

impl ReaderExecutionResult {
    pub(super) fn skipped(reason: impl Into<String>) -> Self {
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

pub(super) fn parse_args() -> Result<ReaderConfig, String> {
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

pub(super) fn load_template_intent(path: &str) -> Result<StrategyIntent, String> {
    load_shared_template_intent(path)
}

pub(super) fn now_epoch_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

pub(super) fn load_cursor_state(path: &str, scope: &ReaderScope) -> Result<ReaderCursorState, String> {
    load_shared_cursor_state(path, scope.label(), scope.id())
}

pub(super) fn persist_cursor_state(
    path: &str,
    scope: &ReaderScope,
    next_cursor: u64,
) -> Result<(), String> {
    persist_shared_cursor_state(path, scope.label(), scope.id(), next_cursor)
}

#[cfg(test)]
mod tests {
    use super::{ReaderCursorState, ReaderScope, persist_cursor_state};
    use std::fs;

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
}
