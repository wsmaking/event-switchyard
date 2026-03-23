use tokio::time::{Duration, sleep};

#[allow(dead_code)]
#[path = "../order/mod.rs"]
mod order;
mod strategy;
#[path = "strategy_catchup_reader/support.rs"]
mod support;

use strategy::alpha_redecision::{
    AlphaReDecision, AlphaReDecisionAction, AlphaReDecisionInput, AlphaRecoveryContext,
};
use strategy::catchup::{
    StrategyExecutionCatchupLoop, target_signed_qty_for_intent,
};
use strategy::http_client::{
    decode_json_or_string, fetch_catchup_page as fetch_strategy_catchup_page, parse_http_base_url,
};
use strategy::intent::StrategyIntent;
use strategy::redecision_support::{
    AlphaMarketOverrides, AlphaNextIntentOverrides,
    build_optional_redecision_input as build_shared_optional_redecision_input,
    redecision_skip_reason as shared_redecision_skip_reason,
};
use strategy::replay::StrategyExecutionCatchupInput;
use support::{
    ReaderConfig, ReaderExecutionResult, ReaderLoopTickOutput, ReaderOutput, ReaderScope,
    load_cursor_state, load_template_intent, now_epoch_ns, parse_args, persist_cursor_state,
};

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
            scope_kind: config.scope.label().to_string(),
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

fn build_redecision_input(
    template: &StrategyIntent,
    recovery: &AlphaRecoveryContext,
    config: &ReaderConfig,
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
    shared_redecision_skip_reason(redecision)
}

#[cfg(test)]
mod tests {
    use super::{ReaderConfig, ReaderScope, build_redecision_input};
    use crate::order::{OrderType, TimeInForce};
    use crate::strategy::alpha_redecision::{
        AlphaRecoveryContext, AlphaRecoveryOperatorStatus, AlphaUnknownExposureBreakdown,
    };
    use crate::strategy::http_client::decode_json_or_string;
    use crate::strategy::intent::{
        ExecutionPolicyKind, IntentUrgency, STRATEGY_INTENT_SCHEMA_VERSION, StrategyIntent,
    };
    use serde_json::Value;
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
