use super::alpha_redecision::{
    AlphaMarketContext, AlphaNextIntentParams, AlphaReDecision, AlphaReDecisionAction,
    AlphaReDecisionInput, AlphaRecoveryContext,
};
use super::intent::StrategyIntent;
use std::fs;

#[derive(Debug, Clone, Default)]
pub struct AlphaMarketOverrides {
    pub desired_signed_qty: Option<i64>,
    pub observed_at_ns: Option<u64>,
    pub max_decision_age_ns: Option<u64>,
    pub market_snapshot_id: Option<String>,
    pub signal_id: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct AlphaNextIntentOverrides {
    pub intent_id: Option<String>,
    pub decision_key: Option<String>,
    pub created_at_ns: Option<u64>,
    pub expires_at_ns: Option<u64>,
}

pub fn load_template_intent(path: &str) -> Result<StrategyIntent, String> {
    let raw =
        fs::read_to_string(path).map_err(|err| format!("read template intent failed: {err}"))?;
    let intent: StrategyIntent =
        serde_json::from_str(&raw).map_err(|err| format!("parse template intent failed: {err}"))?;
    intent
        .validate()
        .map_err(|err| format!("invalid template intent: {err}"))?;
    Ok(intent)
}

pub fn build_optional_redecision_input(
    template: &StrategyIntent,
    recovery: &AlphaRecoveryContext,
    market: AlphaMarketOverrides,
    next_intent: AlphaNextIntentOverrides,
    now_ns: u64,
) -> Result<Option<AlphaReDecisionInput>, String> {
    if market.desired_signed_qty.is_none() {
        return Ok(None);
    }
    build_redecision_input(template, recovery, market, next_intent, now_ns).map(Some)
}

pub fn build_redecision_input(
    template: &StrategyIntent,
    recovery: &AlphaRecoveryContext,
    market: AlphaMarketOverrides,
    next_intent: AlphaNextIntentOverrides,
    now_ns: u64,
) -> Result<AlphaReDecisionInput, String> {
    let market = build_market_context(template, market, now_ns)?;
    let next_intent = build_next_intent_params(template, recovery, next_intent, now_ns)?;
    Ok(AlphaReDecisionInput {
        template_intent: template.clone(),
        recovery: recovery.clone(),
        market,
        next_intent,
    })
}

pub fn redecision_skip_reason(redecision: &AlphaReDecision) -> String {
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

fn build_market_context(
    template: &StrategyIntent,
    market: AlphaMarketOverrides,
    now_ns: u64,
) -> Result<AlphaMarketContext, String> {
    let desired_signed_qty = market
        .desired_signed_qty
        .ok_or_else(|| "MARKET_DESIRED_SIGNED_QTY_REQUIRED".to_string())?;
    let max_decision_age_ns = market
        .max_decision_age_ns
        .or(template.max_decision_age_ns)
        .ok_or_else(|| "MARKET_MAX_DECISION_AGE_NS_REQUIRED".to_string())?;
    let market = AlphaMarketContext {
        observed_at_ns: market.observed_at_ns.unwrap_or(now_ns),
        desired_signed_qty,
        max_decision_age_ns,
        market_snapshot_id: market
            .market_snapshot_id
            .or_else(|| template.market_snapshot_id.clone()),
        signal_id: market.signal_id.or_else(|| template.signal_id.clone()),
    };
    market.validate().map_err(|err| err.to_string())?;
    Ok(market)
}

fn build_next_intent_params(
    template: &StrategyIntent,
    recovery: &AlphaRecoveryContext,
    next_intent: AlphaNextIntentOverrides,
    now_ns: u64,
) -> Result<AlphaNextIntentParams, String> {
    let suffix = recovery.next_cursor.max(1);
    let params = AlphaNextIntentParams {
        intent_id: next_intent
            .intent_id
            .unwrap_or_else(|| format!("{}::redecision::{suffix}", template.intent_id)),
        decision_key: next_intent
            .decision_key
            .unwrap_or_else(|| format!("redecision-{suffix}")),
        decision_attempt_seq: 1,
        created_at_ns: next_intent.created_at_ns.unwrap_or(now_ns),
        expires_at_ns: next_intent.expires_at_ns,
    };
    params.validate().map_err(|err| err.to_string())?;
    Ok(params)
}
