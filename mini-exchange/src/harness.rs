use crate::{
    BookMetrics, BookSnapshot, MatchingEngine, OrderInput, OrderType, QueuePosition, Side,
    SlippageStats, SubmitOutcome,
};
use serde::{Deserialize, Serialize};

pub const QUANT_STRATEGY_INTENT_SCHEMA_VERSION: u16 = 2;
pub const QUANT_STRATEGY_INTENT_LEGACY_SCHEMA_VERSION: u16 = 1;
pub const DEFAULT_SLICE_COUNT: usize = 4;
pub const DEFAULT_PARTICIPATION_BPS: u64 = 2_500;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExecutionPolicy {
    Passive,
    Aggressive,
    Twap,
    Vwap,
    Pov,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IntentUrgency {
    Low,
    #[default]
    Normal,
    High,
    Critical,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct RiskBudgetRef {
    pub budget_id: String,
    pub version: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TimeInForce {
    #[default]
    Gtc,
    Ioc,
    Fok,
    Gtd,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuantStrategyIntent {
    pub schema_version: u16,
    pub intent_id: String,
    pub account_id: String,
    pub session_id: String,
    pub symbol: String,
    pub side: String,
    #[serde(rename = "type")]
    pub order_type: OrderType,
    pub qty: u64,
    pub limit_price: Option<u64>,
    #[serde(default)]
    pub time_in_force: TimeInForce,
    #[serde(default)]
    pub urgency: IntentUrgency,
    #[serde(default)]
    pub execution_policy: Option<ExecutionPolicy>,
    #[serde(default)]
    pub risk_budget_ref: Option<RiskBudgetRef>,
    #[serde(default)]
    pub model_id: Option<String>,
    pub created_at_ns: u64,
    pub expires_at_ns: u64,
    #[serde(default)]
    pub slice_count: Option<usize>,
    #[serde(default)]
    pub vwap_weights: Vec<u64>,
    #[serde(default)]
    pub participation_bps: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum QuantCleanExecutionPolicyKind {
    #[default]
    Default,
    Passive,
    Aggressive,
    Twap,
    Vwap,
    Pov,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuantCleanStrategyIntent {
    pub schema_version: u16,
    pub intent_id: String,
    pub account_id: String,
    pub session_id: String,
    pub symbol: String,
    pub side: String,
    #[serde(rename = "type")]
    pub order_type: OrderType,
    pub qty: u64,
    pub limit_price: Option<u64>,
    #[serde(default)]
    pub time_in_force: TimeInForce,
    #[serde(default)]
    pub urgency: IntentUrgency,
    #[serde(default)]
    pub execution_policy: QuantCleanExecutionPolicyKind,
    #[serde(default)]
    pub risk_budget_ref: Option<RiskBudgetRef>,
    #[serde(default)]
    pub model_id: Option<String>,
    pub created_at_ns: u64,
    pub expires_at_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StrategyIntent {
    pub intent_id: String,
    pub side: Side,
    pub qty: u64,
    pub limit_price: Option<u64>,
    pub ts: u64,
    pub slice_count: usize,
    pub vwap_weights: Vec<u64>,
    pub participation_bps: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PolicyMaterialization {
    pub order_id: String,
    pub side: Side,
    pub order_type: OrderType,
    pub price: Option<u64>,
    pub qty: u64,
    pub ts: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChildExecutionReport {
    pub child_index: usize,
    pub order: PolicyMaterialization,
    pub submit: SubmitOutcome,
    pub queue_position: Option<QueuePosition>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PolicyEvaluationReport {
    pub policy: ExecutionPolicy,
    pub child_reports: Vec<ChildExecutionReport>,
    pub book_after: BookSnapshot,
    pub metrics_after: BookMetrics,
    pub slippage_after: SlippageStats,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PolicyScenarioReport {
    pub scenario_id: String,
    pub input_intent: StrategyIntent,
    pub reports: Vec<PolicyEvaluationReport>,
}

impl PolicyMaterialization {
    pub fn to_order_input(&self) -> OrderInput {
        OrderInput {
            order_id: self.order_id.clone(),
            side: self.side,
            order_type: self.order_type,
            price: self.price,
            qty: self.qty,
            ts: self.ts,
        }
    }
}

impl QuantStrategyIntent {
    pub fn validate(&self) -> Result<(), &'static str> {
        if !is_supported_quant_strategy_intent_schema_version(self.schema_version) {
            return Err("UNSUPPORTED_SCHEMA_VERSION");
        }
        if self.intent_id.trim().is_empty() {
            return Err("INTENT_ID_REQUIRED");
        }
        if self.account_id.trim().is_empty() {
            return Err("ACCOUNT_ID_REQUIRED");
        }
        if self.session_id.trim().is_empty() {
            return Err("SESSION_ID_REQUIRED");
        }
        if self.symbol.trim().is_empty() {
            return Err("SYMBOL_REQUIRED");
        }
        if self.qty == 0 {
            return Err("QTY_REQUIRED");
        }
        if !matches!(
            self.side.trim().to_ascii_uppercase().as_str(),
            "BUY" | "SELL"
        ) {
            return Err("INVALID_SIDE");
        }
        if self.expires_at_ns <= self.created_at_ns {
            return Err("INVALID_EXPIRY");
        }
        if self.order_type == OrderType::Limit && self.limit_price.unwrap_or(0) == 0 {
            return Err("LIMIT_PRICE_REQUIRED");
        }
        Ok(())
    }
}

impl QuantCleanStrategyIntent {
    pub fn validate(&self) -> Result<(), &'static str> {
        if !is_supported_quant_strategy_intent_schema_version(self.schema_version) {
            return Err("UNSUPPORTED_SCHEMA_VERSION");
        }
        if self.intent_id.trim().is_empty() {
            return Err("INTENT_ID_REQUIRED");
        }
        if self.account_id.trim().is_empty() {
            return Err("ACCOUNT_ID_REQUIRED");
        }
        if self.session_id.trim().is_empty() {
            return Err("SESSION_ID_REQUIRED");
        }
        if self.symbol.trim().is_empty() {
            return Err("SYMBOL_REQUIRED");
        }
        if self.qty == 0 {
            return Err("QTY_REQUIRED");
        }
        if !matches!(
            self.side.trim().to_ascii_uppercase().as_str(),
            "BUY" | "SELL"
        ) {
            return Err("INVALID_SIDE");
        }
        if self.expires_at_ns <= self.created_at_ns {
            return Err("INVALID_EXPIRY");
        }
        if self.order_type == OrderType::Limit && self.limit_price.unwrap_or(0) == 0 {
            return Err("LIMIT_PRICE_REQUIRED");
        }
        Ok(())
    }
}

fn is_supported_quant_strategy_intent_schema_version(schema_version: u16) -> bool {
    matches!(
        schema_version,
        QUANT_STRATEGY_INTENT_SCHEMA_VERSION | QUANT_STRATEGY_INTENT_LEGACY_SCHEMA_VERSION
    )
}

impl TryFrom<&QuantStrategyIntent> for StrategyIntent {
    type Error = &'static str;

    fn try_from(value: &QuantStrategyIntent) -> Result<Self, Self::Error> {
        value.validate()?;
        let side = match value.side.trim().to_ascii_uppercase().as_str() {
            "BUY" => Side::Buy,
            "SELL" => Side::Sell,
            _ => return Err("INVALID_SIDE"),
        };

        Ok(Self {
            intent_id: value.intent_id.clone(),
            side,
            qty: value.qty,
            limit_price: value.limit_price,
            ts: value.created_at_ns,
            slice_count: value.slice_count.unwrap_or(DEFAULT_SLICE_COUNT),
            vwap_weights: if value.vwap_weights.is_empty() {
                vec![1; value.slice_count.unwrap_or(DEFAULT_SLICE_COUNT)]
            } else {
                value.vwap_weights.clone()
            },
            participation_bps: value.participation_bps.unwrap_or(DEFAULT_PARTICIPATION_BPS),
        })
    }
}

impl TryFrom<QuantStrategyIntent> for StrategyIntent {
    type Error = &'static str;

    fn try_from(value: QuantStrategyIntent) -> Result<Self, Self::Error> {
        StrategyIntent::try_from(&value)
    }
}

impl From<QuantCleanExecutionPolicyKind> for Option<ExecutionPolicy> {
    fn from(value: QuantCleanExecutionPolicyKind) -> Self {
        match value {
            QuantCleanExecutionPolicyKind::Default => None,
            QuantCleanExecutionPolicyKind::Passive => Some(ExecutionPolicy::Passive),
            QuantCleanExecutionPolicyKind::Aggressive => Some(ExecutionPolicy::Aggressive),
            QuantCleanExecutionPolicyKind::Twap => Some(ExecutionPolicy::Twap),
            QuantCleanExecutionPolicyKind::Vwap => Some(ExecutionPolicy::Vwap),
            QuantCleanExecutionPolicyKind::Pov => Some(ExecutionPolicy::Pov),
        }
    }
}

impl TryFrom<&QuantCleanStrategyIntent> for QuantStrategyIntent {
    type Error = &'static str;

    fn try_from(value: &QuantCleanStrategyIntent) -> Result<Self, Self::Error> {
        value.validate()?;
        Ok(Self {
            schema_version: value.schema_version,
            intent_id: value.intent_id.clone(),
            account_id: value.account_id.clone(),
            session_id: value.session_id.clone(),
            symbol: value.symbol.clone(),
            side: value.side.clone(),
            order_type: value.order_type,
            qty: value.qty,
            limit_price: value.limit_price,
            time_in_force: value.time_in_force,
            urgency: value.urgency,
            execution_policy: value.execution_policy.into(),
            risk_budget_ref: value.risk_budget_ref.clone(),
            model_id: value.model_id.clone(),
            created_at_ns: value.created_at_ns,
            expires_at_ns: value.expires_at_ns,
            slice_count: None,
            vwap_weights: Vec::new(),
            participation_bps: None,
        })
    }
}

impl TryFrom<QuantCleanStrategyIntent> for QuantStrategyIntent {
    type Error = &'static str;

    fn try_from(value: QuantCleanStrategyIntent) -> Result<Self, Self::Error> {
        QuantStrategyIntent::try_from(&value)
    }
}

impl PolicyEvaluationReport {
    pub fn executed_qty(&self) -> u64 {
        self.child_reports
            .iter()
            .flat_map(|child| child.submit.trades.iter())
            .map(|trade| trade.qty)
            .sum()
    }

    pub fn remaining_qty(&self) -> u64 {
        let submitted_qty: u64 = self.child_reports.iter().map(|child| child.order.qty).sum();
        submitted_qty.saturating_sub(self.executed_qty())
    }

    pub fn resting_qty(&self) -> u64 {
        self.child_reports
            .iter()
            .filter(|child| child.submit.resting)
            .map(|child| child.submit.remaining_qty)
            .sum()
    }

    pub fn render(&self) -> String {
        let mut lines = Vec::new();
        lines.push(format!("POLICY {:?}", self.policy));
        lines.push(format!("CHILD_COUNT {}", self.child_reports.len()));
        lines.push(format!("EXECUTED_QTY {}", self.executed_qty()));
        lines.push(format!("RESTING_QTY {}", self.resting_qty()));

        for child in &self.child_reports {
            lines.push(format!("CHILD {}", child.child_index));
            lines.push(format!("ORDER_ID {}", child.order.order_id));
            lines.push(format!("ORDER_TYPE {:?}", child.order.order_type));
            lines.push(format!(
                "ORDER_PRICE {}",
                child
                    .order
                    .price
                    .map(|price| price.to_string())
                    .unwrap_or_else(|| "MARKET".to_string())
            ));
            lines.push(format!("ORDER_QTY {}", child.order.qty));
            lines.push(format!("ACCEPTED {}", child.submit.accepted));
            lines.push(format!(
                "REJECT_REASON {}",
                child.submit.reject_reason.unwrap_or("NONE")
            ));
            lines.push(format!(
                "CHILD_EXECUTED_QTY {}",
                child
                    .submit
                    .trades
                    .iter()
                    .map(|trade| trade.qty)
                    .sum::<u64>()
            ));
            lines.push(format!(
                "CHILD_REMAINING_QTY {}",
                child.submit.remaining_qty
            ));
            lines.push(format!("CHILD_RESTING {}", child.submit.resting));
            if let Some(position) = &child.queue_position {
                lines.push(position.render_line());
            }
        }

        lines.push(self.metrics_after.render());
        lines.push(self.slippage_after.render());
        lines.push(self.book_after.render());
        lines.join("\n")
    }
}

impl PolicyScenarioReport {
    pub fn from_reports(
        input_intent: StrategyIntent,
        reports: Vec<PolicyEvaluationReport>,
    ) -> Self {
        Self {
            scenario_id: input_intent.intent_id.clone(),
            input_intent,
            reports,
        }
    }

    pub fn render_text(&self) -> String {
        let mut sections = Vec::new();
        sections.push(format!("SCENARIO {}", self.scenario_id));
        for report in &self.reports {
            sections.push(report.render());
        }
        sections.join("\n\n---\n\n")
    }

    pub fn render_json_pretty(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    pub fn render_csv_summary(&self) -> String {
        let mut lines = Vec::new();
        lines.push("scenario_id,policy,child_count,executed_qty,resting_qty,slippage_events,total_notional_slippage,avg_slippage_ticks_milli,spread,best_bid_depth,best_ask_depth,last_price".to_string());
        for report in &self.reports {
            lines.push(format!(
                "{},{:?},{},{},{},{},{},{},{},{},{},{}",
                self.scenario_id,
                report.policy,
                report.child_reports.len(),
                report.executed_qty(),
                report.resting_qty(),
                report.slippage_after.event_count,
                report.slippage_after.total_notional_slippage,
                report.slippage_after.average_slippage_ticks_milli(),
                report
                    .metrics_after
                    .spread
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "None".to_string()),
                report.metrics_after.best_bid_depth,
                report.metrics_after.best_ask_depth,
                report
                    .book_after
                    .last_price
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "None".to_string()),
            ));
        }
        lines.join("\n")
    }
}

pub fn evaluate_policies(
    seed_orders: &[OrderInput],
    intent: &StrategyIntent,
    policies: &[ExecutionPolicy],
) -> Result<Vec<PolicyEvaluationReport>, &'static str> {
    let mut base_engine = MatchingEngine::new();
    for order in seed_orders {
        let outcome = base_engine.submit(order.clone());
        if !outcome.accepted {
            return Err(outcome.reject_reason.unwrap_or("SEED_ORDER_REJECTED"));
        }
    }

    let mut reports = Vec::new();
    for policy in policies {
        let mut engine = base_engine.clone();
        let child_reports = simulate_policy(&mut engine, *policy, intent)?;
        reports.push(PolicyEvaluationReport {
            policy: *policy,
            child_reports,
            book_after: engine.snapshot(),
            metrics_after: engine.book_metrics(),
            slippage_after: engine.slippage_stats().clone(),
        });
    }

    Ok(reports)
}

pub fn evaluate_quant_intent(
    seed_orders: &[OrderInput],
    quant_intent: &QuantStrategyIntent,
    policies: &[ExecutionPolicy],
) -> Result<PolicyScenarioReport, &'static str> {
    let input_intent = StrategyIntent::try_from(quant_intent)?;
    let reports = evaluate_policies(seed_orders, &input_intent, policies)?;
    Ok(PolicyScenarioReport::from_reports(input_intent, reports))
}

pub fn policies_for_quant_clean_intent(intent: &QuantCleanStrategyIntent) -> Vec<ExecutionPolicy> {
    match intent.execution_policy {
        QuantCleanExecutionPolicyKind::Default => vec![
            ExecutionPolicy::Passive,
            ExecutionPolicy::Aggressive,
            ExecutionPolicy::Twap,
            ExecutionPolicy::Vwap,
            ExecutionPolicy::Pov,
        ],
        QuantCleanExecutionPolicyKind::Passive => vec![ExecutionPolicy::Passive],
        QuantCleanExecutionPolicyKind::Aggressive => vec![ExecutionPolicy::Aggressive],
        QuantCleanExecutionPolicyKind::Twap => vec![ExecutionPolicy::Twap],
        QuantCleanExecutionPolicyKind::Vwap => vec![ExecutionPolicy::Vwap],
        QuantCleanExecutionPolicyKind::Pov => vec![ExecutionPolicy::Pov],
    }
}

pub fn evaluate_quant_clean_intent(
    seed_orders: &[OrderInput],
    quant_clean_intent: &QuantCleanStrategyIntent,
    policies: &[ExecutionPolicy],
) -> Result<PolicyScenarioReport, &'static str> {
    let quant_intent = QuantStrategyIntent::try_from(quant_clean_intent)?;
    evaluate_quant_intent(seed_orders, &quant_intent, policies)
}

pub fn materialize_order(
    engine: &MatchingEngine,
    policy: ExecutionPolicy,
    intent: &StrategyIntent,
) -> Result<PolicyMaterialization, &'static str> {
    if intent.qty == 0 {
        return Err("INTENT_QTY_REQUIRED");
    }

    let snapshot = engine.snapshot();
    let best_bid = snapshot.bids.first().map(|level| level.price);
    let best_ask = snapshot.asks.first().map(|level| level.price);

    let (order_type, price) = match policy {
        ExecutionPolicy::Passive => {
            let limit_price = intent.limit_price.ok_or("PASSIVE_LIMIT_REQUIRED")?;
            let passive_price = match intent.side {
                Side::Buy => best_bid
                    .map(|best| best.min(limit_price))
                    .unwrap_or(limit_price),
                Side::Sell => best_ask
                    .map(|best| best.max(limit_price))
                    .unwrap_or(limit_price),
            };
            (OrderType::Limit, Some(passive_price))
        }
        ExecutionPolicy::Aggressive => aggressive_order_shape(intent.limit_price),
        ExecutionPolicy::Twap | ExecutionPolicy::Vwap | ExecutionPolicy::Pov => {
            return Err("POLICY_REQUIRES_SIMULATION");
        }
    };

    Ok(PolicyMaterialization {
        order_id: format!("{}::{:?}", intent.intent_id, policy),
        side: intent.side,
        order_type,
        price,
        qty: intent.qty,
        ts: intent.ts,
    })
}

fn simulate_policy(
    engine: &mut MatchingEngine,
    policy: ExecutionPolicy,
    intent: &StrategyIntent,
) -> Result<Vec<ChildExecutionReport>, &'static str> {
    match policy {
        ExecutionPolicy::Passive | ExecutionPolicy::Aggressive => {
            let materialized = materialize_order(engine, policy, intent)?;
            let submit = engine.submit(materialized.to_order_input());
            let queue_position = engine.queue_position(&materialized.order_id);
            Ok(vec![ChildExecutionReport {
                child_index: 0,
                order: materialized,
                submit,
                queue_position,
            }])
        }
        ExecutionPolicy::Twap => simulate_twap(engine, intent),
        ExecutionPolicy::Vwap => simulate_vwap(engine, intent),
        ExecutionPolicy::Pov => simulate_pov(engine, intent),
    }
}

fn simulate_twap(
    engine: &mut MatchingEngine,
    intent: &StrategyIntent,
) -> Result<Vec<ChildExecutionReport>, &'static str> {
    let slice_count = validated_slice_count(intent.slice_count)?;
    let slices = split_evenly(intent.qty, slice_count);
    simulate_child_aggressive_orders(engine, intent, slices, ExecutionPolicy::Twap)
}

fn simulate_vwap(
    engine: &mut MatchingEngine,
    intent: &StrategyIntent,
) -> Result<Vec<ChildExecutionReport>, &'static str> {
    if intent.vwap_weights.is_empty() {
        return Err("VWAP_WEIGHTS_REQUIRED");
    }
    let slices = split_weighted(intent.qty, &intent.vwap_weights)?;
    simulate_child_aggressive_orders(engine, intent, slices, ExecutionPolicy::Vwap)
}

fn simulate_pov(
    engine: &mut MatchingEngine,
    intent: &StrategyIntent,
) -> Result<Vec<ChildExecutionReport>, &'static str> {
    let slice_count = validated_slice_count(intent.slice_count)?;
    if intent.participation_bps == 0 || intent.participation_bps > 10_000 {
        return Err("INVALID_PARTICIPATION_BPS");
    }

    let mut remaining = intent.qty;
    let mut child_reports = Vec::new();
    for index in 0..slice_count {
        if remaining == 0 {
            break;
        }

        let snapshot = engine.snapshot();
        let opposite_depth = top3_opposite_depth(&snapshot, intent.side);
        if opposite_depth == 0 {
            break;
        }

        let target_qty =
            ((u128::from(opposite_depth) * u128::from(intent.participation_bps)) / 10_000) as u64;
        let child_qty = target_qty.max(1).min(remaining);
        remaining -= child_qty;

        let materialized = materialize_aggressive_child(
            intent,
            child_qty,
            intent.ts + index as u64,
            format!("{}::Pov::{}", intent.intent_id, index),
        );
        let submit = engine.submit(materialized.to_order_input());
        let queue_position = engine.queue_position(&materialized.order_id);
        child_reports.push(ChildExecutionReport {
            child_index: index,
            order: materialized,
            submit,
            queue_position,
        });
    }

    Ok(child_reports)
}

fn simulate_child_aggressive_orders(
    engine: &mut MatchingEngine,
    intent: &StrategyIntent,
    slices: Vec<u64>,
    policy: ExecutionPolicy,
) -> Result<Vec<ChildExecutionReport>, &'static str> {
    let mut child_reports = Vec::new();
    for (index, qty) in slices.into_iter().enumerate() {
        if qty == 0 {
            continue;
        }
        let materialized = materialize_aggressive_child(
            intent,
            qty,
            intent.ts + index as u64,
            format!("{}::{:?}::{}", intent.intent_id, policy, index),
        );
        let submit = engine.submit(materialized.to_order_input());
        let queue_position = engine.queue_position(&materialized.order_id);
        child_reports.push(ChildExecutionReport {
            child_index: index,
            order: materialized,
            submit,
            queue_position,
        });
    }
    Ok(child_reports)
}

fn materialize_aggressive_child(
    intent: &StrategyIntent,
    qty: u64,
    ts: u64,
    order_id: String,
) -> PolicyMaterialization {
    let (order_type, price) = aggressive_order_shape(intent.limit_price);
    PolicyMaterialization {
        order_id,
        side: intent.side,
        order_type,
        price,
        qty,
        ts,
    }
}

fn aggressive_order_shape(limit_price: Option<u64>) -> (OrderType, Option<u64>) {
    match limit_price {
        Some(limit_price) => (OrderType::Limit, Some(limit_price)),
        None => (OrderType::Market, None),
    }
}

fn validated_slice_count(slice_count: usize) -> Result<usize, &'static str> {
    if slice_count == 0 {
        Err("SLICE_COUNT_REQUIRED")
    } else {
        Ok(slice_count)
    }
}

fn split_evenly(total_qty: u64, slice_count: usize) -> Vec<u64> {
    let base = total_qty / slice_count as u64;
    let remainder = total_qty % slice_count as u64;
    (0..slice_count)
        .map(|index| base + u64::from((index as u64) < remainder))
        .collect()
}

fn split_weighted(total_qty: u64, weights: &[u64]) -> Result<Vec<u64>, &'static str> {
    if weights.is_empty() {
        return Err("VWAP_WEIGHTS_REQUIRED");
    }
    let total_weight: u64 = weights.iter().sum();
    if total_weight == 0 {
        return Err("VWAP_WEIGHTS_INVALID");
    }

    let mut allocated = 0u64;
    let mut slices = Vec::with_capacity(weights.len());
    for (index, weight) in weights.iter().copied().enumerate() {
        let mut qty =
            ((u128::from(total_qty) * u128::from(weight)) / u128::from(total_weight)) as u64;
        if index == weights.len() - 1 {
            qty = total_qty.saturating_sub(allocated);
        }
        allocated += qty;
        slices.push(qty);
    }

    Ok(slices)
}

fn top3_opposite_depth(snapshot: &BookSnapshot, side: Side) -> u64 {
    match side {
        Side::Buy => snapshot
            .asks
            .iter()
            .take(3)
            .map(|level| level.total_qty)
            .sum(),
        Side::Sell => snapshot
            .bids
            .iter()
            .take(3)
            .map(|level| level.total_qty)
            .sum(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_PARTICIPATION_BPS, DEFAULT_SLICE_COUNT, ExecutionPolicy, PolicyScenarioReport,
        QUANT_STRATEGY_INTENT_LEGACY_SCHEMA_VERSION, QUANT_STRATEGY_INTENT_SCHEMA_VERSION,
        QuantCleanExecutionPolicyKind, QuantCleanStrategyIntent, QuantStrategyIntent,
        StrategyIntent, evaluate_policies, evaluate_quant_clean_intent, evaluate_quant_intent,
        materialize_order, policies_for_quant_clean_intent,
    };
    use crate::{OrderInput, OrderType, Side};

    fn limit(order_id: &str, side: Side, price: u64, qty: u64, ts: u64) -> OrderInput {
        OrderInput {
            order_id: order_id.to_string(),
            side,
            order_type: OrderType::Limit,
            price: Some(price),
            qty,
            ts,
        }
    }

    fn base_intent() -> StrategyIntent {
        StrategyIntent {
            intent_id: "intent-1".to_string(),
            side: Side::Buy,
            qty: 100,
            limit_price: Some(101),
            ts: 4,
            slice_count: 4,
            vwap_weights: vec![1, 3],
            participation_bps: 2_500,
        }
    }

    fn quant_intent_fixture() -> QuantStrategyIntent {
        QuantStrategyIntent {
            schema_version: QUANT_STRATEGY_INTENT_SCHEMA_VERSION,
            intent_id: "quant-intent-1".to_string(),
            account_id: "acc-1".to_string(),
            session_id: "sess-1".to_string(),
            symbol: "AAPL".to_string(),
            side: "BUY".to_string(),
            order_type: OrderType::Limit,
            qty: 100,
            limit_price: Some(102),
            time_in_force: super::TimeInForce::Ioc,
            urgency: super::IntentUrgency::High,
            execution_policy: Some(ExecutionPolicy::Aggressive),
            risk_budget_ref: Some(super::RiskBudgetRef {
                budget_id: "budget-1".to_string(),
                version: 1,
            }),
            model_id: Some("model-1".to_string()),
            created_at_ns: 5,
            expires_at_ns: 100,
            slice_count: Some(4),
            vwap_weights: vec![1, 2, 3, 4],
            participation_bps: Some(2_500),
        }
    }

    fn quant_clean_intent_fixture() -> QuantCleanStrategyIntent {
        QuantCleanStrategyIntent {
            schema_version: QUANT_STRATEGY_INTENT_SCHEMA_VERSION,
            intent_id: "gateway-intent-1".to_string(),
            account_id: "acc-1".to_string(),
            session_id: "sess-1".to_string(),
            symbol: "AAPL".to_string(),
            side: "BUY".to_string(),
            order_type: OrderType::Limit,
            qty: 100,
            limit_price: Some(102),
            time_in_force: super::TimeInForce::Ioc,
            urgency: super::IntentUrgency::High,
            execution_policy: QuantCleanExecutionPolicyKind::Passive,
            risk_budget_ref: Some(super::RiskBudgetRef {
                budget_id: "budget-2".to_string(),
                version: 7,
            }),
            model_id: Some("model-2".to_string()),
            created_at_ns: 9,
            expires_at_ns: 99,
        }
    }

    #[test]
    fn quant_clean_intent_accepts_legacy_schema_version() {
        let mut intent = quant_clean_intent_fixture();
        intent.schema_version = QUANT_STRATEGY_INTENT_LEGACY_SCHEMA_VERSION;

        assert_eq!(intent.validate(), Ok(()));
    }

    #[test]
    fn quant_clean_intent_rejects_unknown_schema_version() {
        let mut intent = quant_clean_intent_fixture();
        intent.schema_version = 99;

        assert_eq!(intent.validate(), Err("UNSUPPORTED_SCHEMA_VERSION"));
    }

    #[test]
    fn passive_buy_posts_on_bid_while_aggressive_crosses() {
        let seed = vec![
            limit("B0", Side::Buy, 99, 100, 1),
            limit("S0", Side::Sell, 100, 50, 2),
            limit("S1", Side::Sell, 101, 80, 3),
        ];
        let intent = base_intent();

        let reports = evaluate_policies(
            &seed,
            &intent,
            &[ExecutionPolicy::Passive, ExecutionPolicy::Aggressive],
        )
        .expect("reports");

        let passive = &reports[0];
        assert_eq!(passive.child_reports[0].order.order_type, OrderType::Limit);
        assert_eq!(passive.child_reports[0].order.price, Some(99));
        assert!(passive.child_reports[0].submit.trades.is_empty());
        assert!(passive.child_reports[0].submit.resting);
        assert_eq!(
            passive.child_reports[0]
                .queue_position
                .as_ref()
                .map(|position| position.position),
            Some(2)
        );

        let aggressive = &reports[1];
        assert_eq!(aggressive.child_reports[0].order.price, Some(101));
        assert_eq!(aggressive.child_reports[0].submit.trades.len(), 2);
        assert_eq!(aggressive.child_reports[0].submit.trades[0].price, 100);
        assert_eq!(aggressive.child_reports[0].submit.trades[1].price, 101);
        assert_eq!(aggressive.executed_qty(), 100);
        assert_eq!(aggressive.slippage_after.event_count, 1);
    }

    #[test]
    fn passive_requires_limit_price() {
        let engine = crate::MatchingEngine::new();
        let mut intent = base_intent();
        intent.limit_price = None;

        let result = materialize_order(&engine, ExecutionPolicy::Passive, &intent);

        assert_eq!(result, Err("PASSIVE_LIMIT_REQUIRED"));
    }

    #[test]
    fn aggressive_without_limit_uses_market() {
        let engine = crate::MatchingEngine::new();
        let intent = StrategyIntent {
            intent_id: "intent-3".to_string(),
            side: Side::Sell,
            qty: 25,
            limit_price: None,
            ts: 1,
            slice_count: 2,
            vwap_weights: vec![1, 1],
            participation_bps: 2_000,
        };

        let order =
            materialize_order(&engine, ExecutionPolicy::Aggressive, &intent).expect("materialize");

        assert_eq!(order.order_type, OrderType::Market);
        assert_eq!(order.price, None);
    }

    #[test]
    fn twap_splits_evenly_across_children() {
        let seed = vec![
            limit("S0", Side::Sell, 100, 25, 1),
            limit("S1", Side::Sell, 101, 25, 2),
            limit("S2", Side::Sell, 102, 25, 3),
            limit("S3", Side::Sell, 103, 25, 4),
        ];
        let intent = StrategyIntent {
            qty: 100,
            slice_count: 4,
            limit_price: Some(103),
            ..base_intent()
        };

        let reports = evaluate_policies(&seed, &intent, &[ExecutionPolicy::Twap]).expect("reports");
        let twap = &reports[0];

        assert_eq!(twap.child_reports.len(), 4);
        assert_eq!(twap.child_reports[0].order.qty, 25);
        assert_eq!(twap.child_reports[1].order.qty, 25);
        assert_eq!(twap.executed_qty(), 100);
    }

    #[test]
    fn vwap_uses_weighted_child_sizes() {
        let seed = vec![
            limit("S0", Side::Sell, 100, 25, 1),
            limit("S1", Side::Sell, 101, 75, 2),
        ];
        let intent = StrategyIntent {
            qty: 100,
            slice_count: 2,
            vwap_weights: vec![1, 3],
            ..base_intent()
        };

        let reports = evaluate_policies(&seed, &intent, &[ExecutionPolicy::Vwap]).expect("reports");
        let vwap = &reports[0];

        assert_eq!(vwap.child_reports.len(), 2);
        assert_eq!(vwap.child_reports[0].order.qty, 25);
        assert_eq!(vwap.child_reports[1].order.qty, 75);
        assert_eq!(vwap.executed_qty(), 100);
    }

    #[test]
    fn pov_targets_fraction_of_visible_opposite_depth() {
        let seed = vec![
            limit("S0", Side::Sell, 100, 40, 1),
            limit("S1", Side::Sell, 101, 60, 2),
        ];
        let intent = StrategyIntent {
            qty: 100,
            slice_count: 2,
            participation_bps: 2_500,
            ..base_intent()
        };

        let reports = evaluate_policies(&seed, &intent, &[ExecutionPolicy::Pov]).expect("reports");
        let pov = &reports[0];

        assert_eq!(pov.child_reports.len(), 2);
        assert_eq!(pov.child_reports[0].order.qty, 25);
        assert_eq!(pov.child_reports[1].order.qty, 18);
        assert_eq!(pov.executed_qty(), 43);
    }

    #[test]
    fn quant_intent_adapter_maps_to_harness_defaults() {
        let mut fixture = quant_intent_fixture();
        fixture.slice_count = None;
        fixture.vwap_weights = Vec::new();
        fixture.participation_bps = None;

        let adapted = StrategyIntent::try_from(&fixture).expect("adapt");

        assert_eq!(adapted.intent_id, "quant-intent-1");
        assert_eq!(adapted.side, Side::Buy);
        assert_eq!(adapted.slice_count, DEFAULT_SLICE_COUNT);
        assert_eq!(adapted.vwap_weights, vec![1; DEFAULT_SLICE_COUNT]);
        assert_eq!(adapted.participation_bps, DEFAULT_PARTICIPATION_BPS);
    }

    #[test]
    fn quant_clean_adapter_matches_gateway_shape() {
        let adapted = QuantStrategyIntent::try_from(&quant_clean_intent_fixture()).expect("adapt");

        assert_eq!(adapted.intent_id, "gateway-intent-1");
        assert_eq!(adapted.execution_policy, Some(ExecutionPolicy::Passive));
        assert_eq!(adapted.slice_count, None);
        assert!(adapted.vwap_weights.is_empty());
        assert_eq!(adapted.participation_bps, None);
    }

    #[test]
    fn quant_clean_default_policy_expands_to_full_compare_set() {
        let mut fixture = quant_clean_intent_fixture();
        fixture.execution_policy = QuantCleanExecutionPolicyKind::Default;

        let policies = policies_for_quant_clean_intent(&fixture);

        assert_eq!(
            policies,
            vec![
                ExecutionPolicy::Passive,
                ExecutionPolicy::Aggressive,
                ExecutionPolicy::Twap,
                ExecutionPolicy::Vwap,
                ExecutionPolicy::Pov,
            ]
        );
    }

    #[test]
    fn quant_clean_intent_evaluation_uses_gateway_schema_input() {
        let seed = vec![
            limit("B0", Side::Buy, 99, 100, 1),
            limit("S0", Side::Sell, 100, 50, 2),
            limit("S1", Side::Sell, 101, 80, 3),
        ];

        let scenario = evaluate_quant_clean_intent(
            &seed,
            &quant_clean_intent_fixture(),
            &[ExecutionPolicy::Passive, ExecutionPolicy::Aggressive],
        )
        .expect("scenario");

        assert_eq!(scenario.scenario_id, "gateway-intent-1");
        assert_eq!(scenario.reports.len(), 2);
    }

    #[test]
    fn quant_intent_evaluation_renders_json_and_csv() {
        let seed = vec![
            limit("B0", Side::Buy, 99, 100, 1),
            limit("S0", Side::Sell, 100, 50, 2),
            limit("S1", Side::Sell, 101, 80, 3),
        ];
        let scenario = evaluate_quant_intent(
            &seed,
            &quant_intent_fixture(),
            &[ExecutionPolicy::Passive, ExecutionPolicy::Aggressive],
        )
        .expect("scenario");

        let json = scenario.render_json_pretty().expect("json");
        let csv = scenario.render_csv_summary();

        assert!(json.contains("\"scenarioId\": \"quant-intent-1\""));
        assert!(
            json.contains("\"policy\": \"Passive\"") || json.contains("\"policy\": \"PASSIVE\"")
        );
        assert!(csv.contains("scenario_id,policy,child_count"));
        assert!(csv.contains("quant-intent-1,Aggressive"));
    }

    #[test]
    fn scenario_text_render_includes_all_reports() {
        let scenario = PolicyScenarioReport::from_reports(
            base_intent(),
            evaluate_policies(
                &[
                    limit("B0", Side::Buy, 99, 100, 1),
                    limit("S0", Side::Sell, 100, 50, 2),
                ],
                &base_intent(),
                &[ExecutionPolicy::Passive, ExecutionPolicy::Aggressive],
            )
            .expect("reports"),
        );

        let rendered = scenario.render_text();

        assert!(rendered.contains("SCENARIO intent-1"));
        assert!(rendered.contains("POLICY Passive"));
        assert!(rendered.contains("POLICY Aggressive"));
    }
}
