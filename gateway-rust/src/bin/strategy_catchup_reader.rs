use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[allow(dead_code)]
#[path = "../order/mod.rs"]
mod order;
mod strategy;

use strategy::catchup::{
    StrategyExecutionCatchupLoop, StrategyExecutionFreshIntentParams,
    StrategyExecutionFreshIntentProposal, StrategyExecutionRecoveryPlan,
    StrategyExecutionRecoveryPolicy, StrategyExecutionUnknownQtyPolicy,
    target_signed_qty_for_intent,
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
    recovery_policy: StrategyExecutionRecoveryPolicy,
    template_intent_path: Option<String>,
    proposal_intent_id: Option<String>,
    proposal_decision_key: Option<String>,
    proposal_created_at_ns: Option<u64>,
    proposal_expires_at_ns: Option<u64>,
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
    snapshot: strategy::catchup::StrategyExecutionCatchupLoopSnapshot,
    #[serde(skip_serializing_if = "Option::is_none")]
    recovery_plan: Option<StrategyExecutionRecoveryPlan>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fresh_intent_proposal: Option<StrategyExecutionFreshIntentProposal>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedHttpBaseUrl {
    host: String,
    port: u16,
    base_path: String,
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
    let mut loop_state = StrategyExecutionCatchupLoop::new();

    let mut next_cursor = if let Some(after_cursor) = config.after_cursor {
        after_cursor
    } else if let Some(cursor_path) = config.cursor_path.as_deref() {
        load_cursor_state(cursor_path, &config.scope)?.next_cursor
    } else {
        0
    };

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

    let snapshot = loop_state.snapshot();
    let recovery_plan = target_signed_qty.map(|target_signed_qty| {
        loop_state.build_recovery_plan(target_signed_qty, config.recovery_policy)
    });
    let fresh_intent_proposal = match (template_intent.as_ref(), recovery_plan.as_ref()) {
        (Some(template), Some(plan)) => Some(plan.propose_fresh_intent(
            template,
            build_fresh_intent_params(
                template,
                plan,
                config.proposal_intent_id.as_deref(),
                config.proposal_decision_key.as_deref(),
                config.proposal_created_at_ns,
                config.proposal_expires_at_ns,
            )?,
        )),
        _ => None,
    };
    let output = ReaderOutput {
        scope_kind: match &config.scope {
            ReaderScope::ExecutionRunId(_) => "executionRunId".to_string(),
            ReaderScope::IntentId(_) => "intentId".to_string(),
        },
        scope_id: config.scope.id().to_string(),
        snapshot,
        recovery_plan,
        fresh_intent_proposal,
    };
    let raw = serde_json::to_string_pretty(&output).map_err(|err| err.to_string())?;
    println!("{raw}");
    Ok(())
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
    let mut unconfirmed = StrategyExecutionUnknownQtyPolicy::Hold;
    let mut loss_suspect = StrategyExecutionUnknownQtyPolicy::Hold;
    let mut template_intent_path = None;
    let mut proposal_intent_id = None;
    let mut proposal_decision_key = None;
    let mut proposal_created_at_ns = None;
    let mut proposal_expires_at_ns = None;

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
            "--limit" => {
                limit = parse_usize_arg(&mut args, "--limit")?;
            }
            "--max-pages" => {
                max_pages = Some(parse_usize_arg(&mut args, "--max-pages")?);
            }
            "--target-signed-qty" => {
                target_signed_qty = Some(parse_i64_arg(&mut args, "--target-signed-qty")?);
            }
            "--template-intent" => {
                template_intent_path = Some(next_arg(&mut args, "--template-intent")?);
            }
            "--proposal-intent-id" => {
                proposal_intent_id = Some(next_arg(&mut args, "--proposal-intent-id")?);
            }
            "--proposal-decision-key" => {
                proposal_decision_key = Some(next_arg(&mut args, "--proposal-decision-key")?);
            }
            "--proposal-created-at-ns" => {
                proposal_created_at_ns =
                    Some(parse_u64_arg(&mut args, "--proposal-created-at-ns")?);
            }
            "--proposal-expires-at-ns" => {
                proposal_expires_at_ns =
                    Some(parse_u64_arg(&mut args, "--proposal-expires-at-ns")?);
            }
            "--unconfirmed-policy" => {
                unconfirmed =
                    parse_unknown_qty_policy(&next_arg(&mut args, "--unconfirmed-policy")?)?;
            }
            "--loss-suspect-policy" => {
                loss_suspect =
                    parse_unknown_qty_policy(&next_arg(&mut args, "--loss-suspect-policy")?)?;
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

    Ok(ReaderConfig {
        base_url,
        scope,
        after_cursor,
        cursor_path,
        limit: limit.max(1),
        max_pages,
        target_signed_qty,
        recovery_policy: StrategyExecutionRecoveryPolicy {
            unconfirmed,
            loss_suspect,
        },
        template_intent_path,
        proposal_intent_id,
        proposal_decision_key,
        proposal_created_at_ns,
        proposal_expires_at_ns,
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

fn parse_unknown_qty_policy(raw: &str) -> Result<StrategyExecutionUnknownQtyPolicy, String> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "hold" => Ok(StrategyExecutionUnknownQtyPolicy::Hold),
        "retry" => Ok(StrategyExecutionUnknownQtyPolicy::Retry),
        "ignore" => Ok(StrategyExecutionUnknownQtyPolicy::Ignore),
        _ => Err(format!(
            "invalid policy: {raw} (expected hold|retry|ignore)"
        )),
    }
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
    println!(
        "    [--target-signed-qty 100] [--template-intent /tmp/intent.json] [--unconfirmed-policy hold|retry|ignore] \\"
    );
    println!(
        "    [--proposal-intent-id fresh-intent-1] [--proposal-decision-key fresh-decision-1] \\"
    );
    println!("    [--proposal-created-at-ns 1000] [--proposal-expires-at-ns 2000] \\");
    println!("    [--loss-suspect-policy hold|retry|ignore]");
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

fn build_fresh_intent_params(
    template: &StrategyIntent,
    plan: &StrategyExecutionRecoveryPlan,
    proposal_intent_id: Option<&str>,
    proposal_decision_key: Option<&str>,
    proposal_created_at_ns: Option<u64>,
    proposal_expires_at_ns: Option<u64>,
) -> Result<StrategyExecutionFreshIntentParams, String> {
    let created_at_ns = proposal_created_at_ns.unwrap_or_else(now_epoch_ns);
    let default_lifetime_ns = template
        .expires_at_ns
        .saturating_sub(template.created_at_ns)
        .max(1);
    let expires_at_ns =
        proposal_expires_at_ns.unwrap_or_else(|| created_at_ns.saturating_add(default_lifetime_ns));
    let suffix = plan.next_cursor.max(1);
    let params = StrategyExecutionFreshIntentParams {
        intent_id: proposal_intent_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("{}::fresh::{suffix}", template.intent_id)),
        decision_key: proposal_decision_key
            .map(str::to_string)
            .unwrap_or_else(|| format!("fresh-{suffix}")),
        decision_attempt_seq: 1,
        created_at_ns,
        expires_at_ns,
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
    let mut stream = TcpStream::connect((base.host.as_str(), base.port))
        .await
        .map_err(|err| format!("connect failed: {err}"))?;
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nAccept: application/json\r\nConnection: close\r\n\r\n",
        host = base.host
    );
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|err| format!("write request failed: {err}"))?;
    let mut raw = Vec::new();
    stream
        .read_to_end(&mut raw)
        .await
        .map_err(|err| format!("read response failed: {err}"))?;
    parse_http_response(&raw)
}

fn parse_http_response(raw: &[u8]) -> Result<Vec<u8>, String> {
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

    if status_code != 200 {
        let message = String::from_utf8_lossy(&body);
        return Err(format!("HTTP {status_code}: {message}"));
    }

    Ok(body)
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

#[cfg(test)]
mod tests {
    use super::{
        ReaderCursorState, ReaderScope, build_fresh_intent_params, decode_chunked_body,
        parse_http_base_url, parse_http_response, persist_cursor_state,
    };
    use crate::order::{OrderType, TimeInForce};
    use crate::strategy::catchup::StrategyExecutionRecoveryPlan;
    use crate::strategy::intent::{
        ExecutionPolicyKind, IntentUrgency, STRATEGY_INTENT_SCHEMA_VERSION, StrategyIntent,
        StrategyRecoveryPolicy,
    };
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
            recovery_policy: Some(StrategyRecoveryPolicy::NoAutoResume),
            algo: None,
            created_at_ns: 100,
            expires_at_ns: 1_100,
        }
    }

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
        let body = parse_http_response(raw).expect("parse response");
        assert_eq!(body, br#"{"ok":1}"#);
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
    fn build_fresh_intent_params_uses_deterministic_defaults() {
        let plan = StrategyExecutionRecoveryPlan {
            execution_run_id: Some("run-1".to_string()),
            intent_id: Some("intent-1".to_string()),
            target_signed_qty: 100,
            confirmed_signed_qty: 40,
            retryable_signed_qty: 20,
            blocked_signed_qty: 0,
            ignored_signed_qty: 0,
            fresh_signed_qty_capacity: 60,
            requires_manual_intervention: false,
            next_cursor: 77,
            has_more: false,
            latest_status_totals: Default::default(),
            decisions: vec![],
        };

        let params =
            build_fresh_intent_params(&template_intent(), &plan, None, None, Some(1_000), None)
                .expect("build params");

        assert_eq!(params.intent_id, "template-intent-1::fresh::77");
        assert_eq!(params.decision_key, "fresh-77");
        assert_eq!(params.decision_attempt_seq, 1);
        assert_eq!(params.created_at_ns, 1_000);
        assert_eq!(params.expires_at_ns, 2_000);
    }
}
