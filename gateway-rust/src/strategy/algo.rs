use serde::{Deserialize, Serialize};

use crate::strategy::config::ExecutionPolicyConfig;
use crate::strategy::intent::{ExecutionPolicyKind, StrategyIntent};

pub const STRATEGY_ALGO_PLAN_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AlgoExecutionSlice {
    pub child_intent_id: String,
    pub sequence: u32,
    pub qty: u64,
    pub send_at_ns: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub weight_bps: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_market_volume: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub participation_target_bps: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AlgoExecutionPlan {
    pub schema_version: u16,
    pub parent_intent_id: String,
    pub policy: ExecutionPolicyKind,
    pub total_qty: u64,
    pub child_count: u32,
    pub start_at_ns: u64,
    pub slice_interval_ns: u64,
    pub slices: Vec<AlgoExecutionSlice>,
}

fn build_child_intent_id(parent_intent_id: &str, sequence: u32) -> String {
    format!("{parent_intent_id}::child-{sequence:02}")
}

fn split_equal(total_qty: u64, count: usize) -> Vec<u64> {
    let base = total_qty / count as u64;
    let remainder = total_qty % count as u64;
    (0..count)
        .map(|idx| base + u64::from((idx as u64) < remainder))
        .collect()
}

fn split_weighted(total_qty: u64, weights: &[u32]) -> Result<Vec<u64>, &'static str> {
    let total_weight = weights.iter().map(|value| *value as u64).sum::<u64>();
    if total_weight == 0 {
        return Err("STRATEGY_POLICY_VWAP_REQUIRES_NONZERO_WEIGHTS");
    }
    let mut assigned = 0u64;
    let mut out = Vec::with_capacity(weights.len());
    for (idx, weight) in weights.iter().enumerate() {
        let qty = if idx + 1 == weights.len() {
            total_qty.saturating_sub(assigned)
        } else {
            ((total_qty as u128) * (*weight as u128) / (total_weight as u128)) as u64
        };
        assigned = assigned.saturating_add(qty);
        out.push(qty);
    }
    Ok(out)
}

fn split_pov(
    total_qty: u64,
    expected_market_volume: &[u64],
    participation_target_bps: u32,
) -> Result<Vec<u64>, &'static str> {
    if participation_target_bps == 0 || participation_target_bps > 10_000 {
        return Err("STRATEGY_POLICY_POV_REQUIRES_VALID_PARTICIPATION");
    }
    let capacities = expected_market_volume
        .iter()
        .map(|value| ((*value as u128) * (participation_target_bps as u128) / 10_000u128) as u64)
        .collect::<Vec<_>>();
    let total_capacity = capacities.iter().sum::<u64>();
    if total_capacity < total_qty {
        return Err("STRATEGY_POLICY_POV_CAPACITY_TOO_SMALL");
    }
    let mut remaining = total_qty;
    let mut out = Vec::with_capacity(capacities.len());
    for capacity in capacities {
        let qty = remaining.min(capacity);
        out.push(qty);
        remaining = remaining.saturating_sub(qty);
    }
    Ok(out)
}

pub fn build_algo_execution_plan(
    intent: &StrategyIntent,
    effective_policy: Option<&ExecutionPolicyConfig>,
    now_ns: u64,
) -> Result<Option<AlgoExecutionPlan>, &'static str> {
    let policy_kind = effective_policy
        .map(|policy| policy.policy)
        .unwrap_or(intent.execution_policy);

    let is_algo = matches!(
        policy_kind,
        ExecutionPolicyKind::Twap | ExecutionPolicyKind::Vwap | ExecutionPolicyKind::Pov
    );
    if !is_algo {
        return Ok(None);
    }

    let algo = intent.algo.as_ref().ok_or("STRATEGY_ALGO_SPEC_REQUIRED")?;
    let start_at_ns = algo.start_at_ns.unwrap_or(now_ns);
    let slice_interval_ns = algo
        .slice_interval_ns
        .filter(|value| *value > 0)
        .ok_or("STRATEGY_ALGO_SLICE_INTERVAL_REQUIRED")?;

    let (quantities, weights, expected_market_volume, participation_target_bps) = match policy_kind
    {
        ExecutionPolicyKind::Twap => {
            let slice_count =
                algo.slice_count
                    .filter(|value| *value >= 2)
                    .ok_or("STRATEGY_POLICY_TWAP_REQUIRES_SLICE_COUNT")? as usize;
            (
                split_equal(intent.qty, slice_count),
                vec![None; slice_count],
                vec![None; slice_count],
                vec![None; slice_count],
            )
        }
        ExecutionPolicyKind::Vwap => {
            if algo.volume_curve_bps.len() < 2 {
                return Err("STRATEGY_POLICY_VWAP_REQUIRES_VOLUME_CURVE");
            }
            let quantities = split_weighted(intent.qty, &algo.volume_curve_bps)?;
            let slice_count = quantities.len();
            (
                quantities,
                algo.volume_curve_bps.iter().copied().map(Some).collect(),
                vec![None; slice_count],
                vec![None; slice_count],
            )
        }
        ExecutionPolicyKind::Pov => {
            if algo.expected_market_volume.is_empty() {
                return Err("STRATEGY_POLICY_POV_REQUIRES_EXPECTED_VOLUME");
            }
            let participation = algo
                .participation_target_bps
                .or_else(|| effective_policy.and_then(|policy| policy.participation_rate_bps))
                .ok_or("STRATEGY_POLICY_POV_REQUIRES_PARTICIPATION")?;
            let quantities = split_pov(intent.qty, &algo.expected_market_volume, participation)?;
            let slice_count = quantities.len();
            (
                quantities,
                vec![None; slice_count],
                algo.expected_market_volume
                    .iter()
                    .copied()
                    .map(Some)
                    .collect(),
                vec![Some(participation); slice_count],
            )
        }
        _ => unreachable!(),
    };

    let slices = quantities
        .into_iter()
        .enumerate()
        .filter(|(_, qty)| *qty > 0)
        .map(|(idx, qty)| AlgoExecutionSlice {
            child_intent_id: build_child_intent_id(&intent.intent_id, idx as u32 + 1),
            sequence: idx as u32 + 1,
            qty,
            send_at_ns: start_at_ns.saturating_add(slice_interval_ns.saturating_mul(idx as u64)),
            weight_bps: weights[idx],
            expected_market_volume: expected_market_volume[idx],
            participation_target_bps: participation_target_bps[idx],
        })
        .collect::<Vec<_>>();
    if slices.is_empty() {
        return Err("STRATEGY_ALGO_EMPTY_PLAN");
    }

    Ok(Some(AlgoExecutionPlan {
        schema_version: STRATEGY_ALGO_PLAN_SCHEMA_VERSION,
        parent_intent_id: intent.intent_id.clone(),
        policy: policy_kind,
        total_qty: intent.qty,
        child_count: slices.len() as u32,
        start_at_ns,
        slice_interval_ns,
        slices,
    }))
}

#[cfg(test)]
mod tests {
    use super::build_algo_execution_plan;
    use crate::order::{OrderType, TimeInForce};
    use crate::strategy::config::ExecutionPolicyConfig;
    use crate::strategy::intent::{
        AlgoExecutionSpec, ExecutionPolicyKind, IntentUrgency, StrategyIntent,
        StrategyRecoveryPolicy,
    };

    fn intent_fixture(policy: ExecutionPolicyKind) -> StrategyIntent {
        StrategyIntent {
            schema_version: crate::strategy::intent::STRATEGY_INTENT_SCHEMA_VERSION,
            intent_id: "intent-algo-1".to_string(),
            account_id: "acc-1".to_string(),
            session_id: "sess-1".to_string(),
            symbol: "AAPL".to_string(),
            side: "BUY".to_string(),
            order_type: OrderType::Limit,
            qty: 100,
            limit_price: Some(101),
            time_in_force: TimeInForce::Gtc,
            urgency: IntentUrgency::Normal,
            execution_policy: policy,
            risk_budget_ref: None,
            model_id: Some("model-1".to_string()),
            execution_run_id: Some("run-1".to_string()),
            decision_key: Some("decision-1".to_string()),
            decision_attempt_seq: Some(1),
            decision_basis_at_ns: None,
            max_decision_age_ns: None,
            market_snapshot_id: None,
            signal_id: None,
            recovery_policy: Some(StrategyRecoveryPolicy::NoAutoResume),
            algo: None,
            created_at_ns: 10,
            expires_at_ns: 1_000_000,
        }
    }

    #[test]
    fn twap_plan_splits_evenly() {
        let mut intent = intent_fixture(ExecutionPolicyKind::Twap);
        intent.algo = Some(AlgoExecutionSpec {
            slice_count: Some(3),
            slice_interval_ns: Some(100),
            start_at_ns: Some(1_000),
            ..AlgoExecutionSpec::default()
        });

        let plan = build_algo_execution_plan(
            &intent,
            Some(&ExecutionPolicyConfig {
                policy: ExecutionPolicyKind::Twap,
                ..ExecutionPolicyConfig::default()
            }),
            0,
        )
        .expect("plan")
        .expect("algo plan");

        assert_eq!(plan.child_count, 3);
        assert_eq!(
            plan.slices
                .iter()
                .map(|slice| slice.qty)
                .collect::<Vec<_>>(),
            vec![34, 33, 33]
        );
        assert_eq!(plan.slices[0].send_at_ns, 1_000);
        assert_eq!(plan.slices[2].send_at_ns, 1_200);
    }

    #[test]
    fn vwap_plan_uses_curve_weights() {
        let mut intent = intent_fixture(ExecutionPolicyKind::Vwap);
        intent.algo = Some(AlgoExecutionSpec {
            slice_interval_ns: Some(50),
            volume_curve_bps: vec![1000, 3000, 6000],
            ..AlgoExecutionSpec::default()
        });

        let plan = build_algo_execution_plan(
            &intent,
            Some(&ExecutionPolicyConfig {
                policy: ExecutionPolicyKind::Vwap,
                ..ExecutionPolicyConfig::default()
            }),
            5,
        )
        .expect("plan")
        .expect("algo plan");

        assert_eq!(plan.child_count, 3);
        assert_eq!(
            plan.slices
                .iter()
                .map(|slice| slice.qty)
                .collect::<Vec<_>>(),
            vec![10, 30, 60]
        );
        assert_eq!(plan.slices[1].weight_bps, Some(3000));
    }

    #[test]
    fn pov_plan_requires_capacity() {
        let mut intent = intent_fixture(ExecutionPolicyKind::Pov);
        intent.algo = Some(AlgoExecutionSpec {
            slice_interval_ns: Some(10),
            expected_market_volume: vec![100, 100],
            participation_target_bps: Some(1000),
            ..AlgoExecutionSpec::default()
        });

        let err = build_algo_execution_plan(
            &intent,
            Some(&ExecutionPolicyConfig {
                policy: ExecutionPolicyKind::Pov,
                participation_rate_bps: Some(1000),
                ..ExecutionPolicyConfig::default()
            }),
            0,
        )
        .expect_err("capacity should be too small");

        assert_eq!(err, "STRATEGY_POLICY_POV_CAPACITY_TOO_SMALL");
    }
}
