use super::support::{
    append_algo_runtime_snapshot_durable, apply_execution_policy_to_order_request,
    build_algo_parent_execution, resolve_effective_policy, resolve_effective_risk_budget_ref,
    snapshot_error, upsert_shadow_record_for_intent, validate_intent_against_snapshot,
    validate_intent_freshness, validate_legacy_recovery_policy,
};
use super::*;

pub(crate) async fn handle_post_strategy_intent_adapt(
    State(state): State<AppState>,
    Json(intent): Json<StrategyIntent>,
) -> Result<Json<StrategyIntentAdaptResponse>, (StatusCode, Json<StrategyConfigErrorResponse>)> {
    intent.validate().map_err(|reason| {
        let snapshot = state.strategy_snapshot_store.snapshot();
        snapshot_error(&state, &snapshot, StatusCode::UNPROCESSABLE_ENTITY, reason)
    })?;

    let snapshot = state.strategy_snapshot_store.snapshot();
    let now_ns = gateway_core::now_nanos();
    validate_intent_freshness(&intent, now_ns).map_err(|reason| {
        snapshot_error(&state, &snapshot, StatusCode::UNPROCESSABLE_ENTITY, reason)
    })?;
    validate_intent_against_snapshot(&snapshot, &intent, now_ns).map_err(|reason| {
        let status = if reason == "STRATEGY_SNAPSHOT_STALE" {
            StatusCode::SERVICE_UNAVAILABLE
        } else {
            StatusCode::UNPROCESSABLE_ENTITY
        };
        snapshot_error(&state, &snapshot, status, reason)
    })?;

    let effective_policy = resolve_effective_policy(&snapshot, &intent);
    let effective_risk_budget_ref = resolve_effective_risk_budget_ref(&snapshot, &intent);
    let applied_policy = apply_execution_policy_to_order_request(&intent, &effective_policy)
        .map_err(|reason| {
            snapshot_error(&state, &snapshot, StatusCode::UNPROCESSABLE_ENTITY, reason)
        })?;
    let algo_plan =
        build_algo_execution_plan(&intent, effective_policy.execution_policy.as_ref(), now_ns)
            .map_err(|reason| {
                snapshot_error(&state, &snapshot, StatusCode::UNPROCESSABLE_ENTITY, reason)
            })?;
    validate_legacy_recovery_policy(&intent).map_err(|reason| {
        snapshot_error(&state, &snapshot, StatusCode::UNPROCESSABLE_ENTITY, reason)
    })?;

    let policy_adjustments = applied_policy.adjustment_strings();
    let order_request = applied_policy.order_request;

    Ok(Json(StrategyIntentAdaptResponse {
        snapshot_id: snapshot.snapshot_id.clone(),
        version: snapshot.version,
        adapted_at_ns: now_ns,
        order_request,
        algo_plan,
        effective_policy,
        effective_risk_budget_ref,
        policy_adjustments,
        shadow_enabled: snapshot.shadow_enabled,
    }))
}

pub(crate) async fn handle_post_strategy_intent_submit(
    State(state): State<AppState>,
    Json(request): Json<StrategyIntentSubmitRequest>,
) -> Result<
    (StatusCode, Json<StrategyIntentSubmitResponse>),
    (StatusCode, Json<StrategyConfigErrorResponse>),
> {
    if state.v3_startup_rebuild_in_progress() {
        let snapshot = state.strategy_snapshot_store.snapshot();
        return Err(snapshot_error(
            &state,
            &snapshot,
            StatusCode::SERVICE_UNAVAILABLE,
            "STRATEGY_STARTUP_REBUILD_IN_PROGRESS",
        ));
    }
    request.intent.validate().map_err(|reason| {
        let snapshot = state.strategy_snapshot_store.snapshot();
        snapshot_error(&state, &snapshot, StatusCode::UNPROCESSABLE_ENTITY, reason)
    })?;

    let snapshot = state.strategy_snapshot_store.snapshot();
    let now_ns = gateway_core::now_nanos();
    validate_intent_freshness(&request.intent, now_ns).map_err(|reason| {
        snapshot_error(&state, &snapshot, StatusCode::UNPROCESSABLE_ENTITY, reason)
    })?;
    validate_intent_against_snapshot(&snapshot, &request.intent, now_ns).map_err(|reason| {
        let status = if reason == "STRATEGY_SNAPSHOT_STALE" {
            StatusCode::SERVICE_UNAVAILABLE
        } else {
            StatusCode::UNPROCESSABLE_ENTITY
        };
        snapshot_error(&state, &snapshot, status, reason)
    })?;

    let effective_policy = resolve_effective_policy(&snapshot, &request.intent);
    let effective_risk_budget_ref = resolve_effective_risk_budget_ref(&snapshot, &request.intent);
    let applied_policy =
        apply_execution_policy_to_order_request(&request.intent, &effective_policy).map_err(
            |reason| snapshot_error(&state, &snapshot, StatusCode::UNPROCESSABLE_ENTITY, reason),
        )?;
    let algo_plan = build_algo_execution_plan(
        &request.intent,
        effective_policy.execution_policy.as_ref(),
        now_ns,
    )
    .map_err(|reason| {
        snapshot_error(&state, &snapshot, StatusCode::UNPROCESSABLE_ENTITY, reason)
    })?;
    validate_legacy_recovery_policy(&request.intent).map_err(|reason| {
        snapshot_error(&state, &snapshot, StatusCode::UNPROCESSABLE_ENTITY, reason)
    })?;
    let order_request = applied_policy.order_request.clone();
    let policy_adjustments = applied_policy.adjustment_strings();
    let shadow_seeded = if let Some(shadow_run_id) = request.shadow_run_id.as_deref() {
        let replaced = upsert_shadow_record_for_intent(
            &state,
            &snapshot,
            shadow_run_id,
            &request.intent,
            request.predicted_policy.clone(),
            request.predicted_outcome.clone(),
            now_ns,
        )?;
        let _ = replaced;
        true
    } else {
        false
    };
    if let Some(algo_plan) = algo_plan.clone() {
        let algo_runtime = build_algo_parent_execution(
            &request,
            order_request.clone(),
            &effective_policy,
            effective_risk_budget_ref.clone(),
            &algo_plan,
            now_ns,
        );
        append_algo_runtime_snapshot_durable(&state, &algo_runtime, now_ns)
            .await
            .map_err(|reason| {
                snapshot_error(&state, &snapshot, StatusCode::SERVICE_UNAVAILABLE, reason)
            })?;
        state
            .strategy_runtime_store
            .insert(algo_runtime.clone())
            .map_err(|reason| {
                snapshot_error(&state, &snapshot, StatusCode::UNPROCESSABLE_ENTITY, reason)
            })?;
        spawn_algo_runtime(state.clone(), &algo_runtime);

        return Ok((
            StatusCode::ACCEPTED,
            Json(StrategyIntentSubmitResponse {
                snapshot_id: snapshot.snapshot_id.clone(),
                version: snapshot.version,
                submitted_at_ns: now_ns,
                order_request,
                algo_plan: Some(algo_plan),
                algo_runtime: Some(algo_runtime),
                effective_policy,
                effective_risk_budget_ref,
                policy_adjustments,
                shadow_enabled: snapshot.shadow_enabled,
                shadow_run_id: request.shadow_run_id,
                shadow_seeded,
                volatile_order: VolatileOrderResponse::algo_runtime_scheduled(
                    &request.intent.session_id,
                    now_ns,
                ),
            }),
        ));
    }
    let (status, volatile_order) =
        super::super::orders::process_order_v3_hot_path_with_strategy_context(
            &state,
            &request.intent.account_id,
            &request.intent.session_id,
            order_request.clone(),
            Some(Arc::new(effective_policy.clone())),
            effective_risk_budget_ref.clone().map(Arc::<str>::from),
            now_ns,
        );

    Ok((
        status,
        Json(StrategyIntentSubmitResponse {
            snapshot_id: snapshot.snapshot_id.clone(),
            version: snapshot.version,
            submitted_at_ns: now_ns,
            order_request,
            algo_plan: None,
            algo_runtime: None,
            effective_policy,
            effective_risk_budget_ref,
            policy_adjustments,
            shadow_enabled: snapshot.shadow_enabled,
            shadow_run_id: request.shadow_run_id,
            shadow_seeded,
            volatile_order,
        }),
    ))
}

pub(crate) async fn handle_post_strategy_intent_shadow(
    State(state): State<AppState>,
    Json(request): Json<StrategyIntentShadowSeedRequest>,
) -> Result<Json<ShadowRecordUpsertResponse>, (StatusCode, Json<StrategyConfigErrorResponse>)> {
    request.intent.validate().map_err(|reason| {
        let snapshot = state.strategy_snapshot_store.snapshot();
        snapshot_error(&state, &snapshot, StatusCode::UNPROCESSABLE_ENTITY, reason)
    })?;

    let snapshot = state.strategy_snapshot_store.snapshot();
    let now_ns = gateway_core::now_nanos();
    validate_intent_against_snapshot(&snapshot, &request.intent, now_ns).map_err(|reason| {
        let status = if reason == "STRATEGY_SNAPSHOT_STALE" {
            StatusCode::SERVICE_UNAVAILABLE
        } else {
            StatusCode::UNPROCESSABLE_ENTITY
        };
        snapshot_error(&state, &snapshot, status, reason)
    })?;
    let effective_policy = resolve_effective_policy(&snapshot, &request.intent);
    let _algo_plan = build_algo_execution_plan(
        &request.intent,
        effective_policy.execution_policy.as_ref(),
        now_ns,
    )
    .map_err(|reason| {
        snapshot_error(&state, &snapshot, StatusCode::UNPROCESSABLE_ENTITY, reason)
    })?;
    validate_legacy_recovery_policy(&request.intent).map_err(|reason| {
        snapshot_error(&state, &snapshot, StatusCode::UNPROCESSABLE_ENTITY, reason)
    })?;

    let replaced = upsert_shadow_record_for_intent(
        &state,
        &snapshot,
        &request.shadow_run_id,
        &request.intent,
        request.predicted_policy,
        request.predicted_outcome,
        now_ns,
    )?;

    Ok(Json(ShadowRecordUpsertResponse {
        shadow_run_id: request.shadow_run_id,
        intent_id: request.intent.intent_id,
        replaced,
        record_count: state.strategy_shadow_store.size(),
        upsert_total: state.strategy_shadow_store.upsert_total(),
    }))
}

pub(crate) async fn handle_get_strategy_config(
    State(state): State<AppState>,
) -> Json<ExecutionConfigSnapshot> {
    Json(state.strategy_snapshot_store.snapshot().as_ref().clone())
}

pub(crate) async fn handle_put_strategy_config(
    State(state): State<AppState>,
    Json(mut snapshot): Json<ExecutionConfigSnapshot>,
) -> Result<Json<StrategyConfigUpdateResponse>, (StatusCode, Json<StrategyConfigErrorResponse>)> {
    let current = state.strategy_snapshot_store.snapshot();
    if snapshot.version < current.version {
        return Err((
            StatusCode::CONFLICT,
            Json(StrategyConfigErrorResponse {
                reason: "SNAPSHOT_VERSION_STALE".to_string(),
                current_snapshot_id: current.snapshot_id.clone(),
                current_version: current.version,
                applied_total: state.strategy_snapshot_store.applied_total(),
                current_applied_at_ns: current.applied_at_ns,
            }),
        ));
    }

    if snapshot.applied_at_ns == 0 {
        snapshot.applied_at_ns = gateway_core::now_nanos();
    }

    let previous = state
        .strategy_snapshot_store
        .replace(snapshot.clone())
        .map_err(|reason| {
            (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(StrategyConfigErrorResponse {
                    reason: reason.to_string(),
                    current_snapshot_id: current.snapshot_id.clone(),
                    current_version: current.version,
                    applied_total: state.strategy_snapshot_store.applied_total(),
                    current_applied_at_ns: current.applied_at_ns,
                }),
            )
        })?;

    Ok(Json(StrategyConfigUpdateResponse {
        snapshot_id: snapshot.snapshot_id,
        version: snapshot.version,
        previous_version: previous.version,
        applied_total: state.strategy_snapshot_store.applied_total(),
        applied_at_ns: snapshot.applied_at_ns,
    }))
}

pub(crate) async fn handle_post_shadow_record(
    State(state): State<AppState>,
    Json(record): Json<ShadowRecord>,
) -> Result<Json<ShadowRecordUpsertResponse>, (StatusCode, Json<StrategyConfigErrorResponse>)> {
    let current_snapshot = state.strategy_snapshot_store.snapshot();
    let replaced = state
        .strategy_shadow_store
        .upsert(record.clone())
        .map_err(|reason| {
            (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(StrategyConfigErrorResponse {
                    reason: reason.to_string(),
                    current_snapshot_id: current_snapshot.snapshot_id.clone(),
                    current_version: current_snapshot.version,
                    applied_total: state.strategy_snapshot_store.applied_total(),
                    current_applied_at_ns: current_snapshot.applied_at_ns,
                }),
            )
        })?
        .is_some();

    Ok(Json(ShadowRecordUpsertResponse {
        shadow_run_id: record.shadow_run_id,
        intent_id: record.intent_id,
        replaced,
        record_count: state.strategy_shadow_store.size(),
        upsert_total: state.strategy_shadow_store.upsert_total(),
    }))
}

pub(crate) async fn handle_get_shadow_record(
    State(state): State<AppState>,
    Path((shadow_run_id, intent_id)): Path<(String, String)>,
) -> Result<Json<ShadowRecord>, StatusCode> {
    state
        .strategy_shadow_store
        .get(&shadow_run_id, &intent_id)
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

pub(crate) async fn handle_get_shadow_run_summary(
    State(state): State<AppState>,
    Path(shadow_run_id): Path<String>,
) -> Result<Json<ShadowRunSummaryResponse>, StatusCode> {
    state
        .strategy_shadow_store
        .summary_for_run(&shadow_run_id)
        .map(|summary| {
            Json(ShadowRunSummaryResponse {
                shadow_run_id: summary.shadow_run_id,
                record_count: summary.record_count,
                pending_count: summary.pending_count,
                matched_count: summary.matched_count,
                skipped_count: summary.skipped_count,
                timed_out_count: summary.timed_out_count,
                negative_score_count: summary.negative_score_count,
                zero_score_count: summary.zero_score_count,
                positive_score_count: summary.positive_score_count,
                total_score_bps: summary.total_score_bps,
                average_score_bps: summary.average_score_bps,
                min_score_bps: summary.min_score_bps,
                max_score_bps: summary.max_score_bps,
                last_evaluated_at_ns: summary.last_evaluated_at_ns,
                top_positive: summary.top_positive,
                top_negative: summary.top_negative,
            })
        })
        .ok_or(StatusCode::NOT_FOUND)
}
