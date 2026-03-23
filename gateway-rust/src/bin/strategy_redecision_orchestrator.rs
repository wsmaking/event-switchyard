use tokio::time::{Duration, sleep};

#[allow(dead_code)]
#[path = "../order/mod.rs"]
mod order;
mod strategy;
#[path = "strategy_redecision_orchestrator/input.rs"]
mod input;
#[path = "strategy_redecision_orchestrator/support.rs"]
mod support;

use strategy::alpha_redecision::{
    AlphaReDecision, AlphaReDecisionAction, AlphaReDecisionInput, AlphaRecoveryContext,
};
use strategy::catchup::target_signed_qty_for_intent;
use strategy::http_client::{
    decode_json_or_string, parse_http_base_url, post_strategy_intent_adapt,
    post_strategy_intent_submit,
};
use strategy::intent::StrategyIntent;
use strategy::market_input::StrategyMarketInput;
use strategy::redecision_support::{
    AlphaMarketOverrides, AlphaNextIntentOverrides,
    build_redecision_input as build_shared_redecision_input,
    redecision_skip_reason as shared_redecision_skip_reason,
};
use input::{fetch_catchup_snapshot, load_market_input};
use support::{
    ExecutionMode, OrchestratorConfig, OrchestratorExecutionResult, OrchestratorPersistentRunState,
    OrchestratorRunConfig, OrchestratorRunOutput, OrchestratorScopeKind,
    OrchestratorTickOutput, RunScope, default_persistent_state, load_config,
    load_persistent_state, load_template_intent, now_epoch_ns, parse_args,
    persist_artifacts, resolve_run_paths, selected_runs,
};

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

    let market_input = match load_market_input(config, run, &scope).await {
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

fn build_redecision_input(
    template: &StrategyIntent,
    recovery: &AlphaRecoveryContext,
    run: &OrchestratorRunConfig,
    market_input: &StrategyMarketInput,
    now_ns: u64,
) -> Result<AlphaReDecisionInput, String> {
    build_shared_redecision_input(
        template,
        recovery,
        AlphaMarketOverrides {
            desired_signed_qty: Some(market_input.desired_signed_qty),
            observed_at_ns: market_input.observed_at_ns,
            max_decision_age_ns: market_input.max_decision_age_ns,
            market_snapshot_id: market_input.market_snapshot_id.clone(),
            signal_id: market_input.signal_id.clone(),
        },
        AlphaNextIntentOverrides {
            intent_id: run.next_intent_id.clone(),
            decision_key: run.next_decision_key.clone(),
            created_at_ns: run.next_created_at_ns,
            expires_at_ns: run.next_expires_at_ns,
        },
        now_ns,
    )
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
    shared_redecision_skip_reason(redecision)
}

#[cfg(test)]
mod tests {
    use super::{ExecutionMode, OrchestratorPersistentRunState, proposal_already_executed};
    use crate::order::{OrderType, TimeInForce};
    use crate::strategy::intent::{
        ExecutionPolicyKind, IntentUrgency, STRATEGY_INTENT_SCHEMA_VERSION, StrategyIntent,
    };

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
}
