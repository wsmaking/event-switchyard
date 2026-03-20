use std::env;
use std::fs;
use std::path::Path;

#[allow(dead_code)]
#[path = "../order/mod.rs"]
mod order;
#[allow(dead_code)]
#[path = "../strategy/intent.rs"]
mod strategy_intent;

use order::{OrderType, TimeInForce};
use strategy_intent::{
    ExecutionPolicyKind, IntentUrgency, RiskBudgetRef, STRATEGY_INTENT_SCHEMA_VERSION,
    StrategyIntent, StrategyRecoveryPolicy,
};

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut output_path = "contracts/fixtures/strategy_intent_v1.json".to_string();
    let mut profile = "aggressive_buy".to_string();
    let mut pretty = true;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                print_usage();
                return Ok(());
            }
            "--output" => output_path = next_arg(&mut args, "--output")?,
            "--profile" => profile = next_arg(&mut args, "--profile")?,
            "--compact" => pretty = false,
            other => return Err(format!("unknown arg: {other}")),
        }
    }

    let intent = fixture_for_profile(&profile)?;
    intent.validate().map_err(|err| err.to_string())?;

    let raw = if pretty {
        serde_json::to_string_pretty(&intent).map_err(|err| err.to_string())?
    } else {
        serde_json::to_string(&intent).map_err(|err| err.to_string())?
    };

    if let Some(parent) = Path::new(&output_path).parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).map_err(|err| err.to_string())?;
        }
    }
    fs::write(&output_path, raw + "\n").map_err(|err| err.to_string())?;

    println!("WROTE {output_path}");
    Ok(())
}

fn next_arg(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("missing value for {flag}"))
}

fn fixture_for_profile(profile: &str) -> Result<StrategyIntent, String> {
    match profile {
        "aggressive_buy" => Ok(StrategyIntent {
            schema_version: STRATEGY_INTENT_SCHEMA_VERSION,
            intent_id: "export-aggressive-buy-1".to_string(),
            account_id: "acc-export-1".to_string(),
            session_id: "sess-export-1".to_string(),
            symbol: "AAPL".to_string(),
            side: "BUY".to_string(),
            order_type: OrderType::Limit,
            qty: 100,
            limit_price: Some(101),
            time_in_force: TimeInForce::Ioc,
            urgency: IntentUrgency::High,
            execution_policy: ExecutionPolicyKind::Aggressive,
            risk_budget_ref: Some(RiskBudgetRef {
                budget_id: "budget-export-1".to_string(),
                version: 3,
            }),
            model_id: Some("model-export-1".to_string()),
            execution_run_id: Some("run-export-1".to_string()),
            decision_key: Some("decision-export-1".to_string()),
            decision_attempt_seq: Some(1),
            recovery_policy: Some(StrategyRecoveryPolicy::NoAutoResume),
            algo: None,
            created_at_ns: 100,
            expires_at_ns: 1_000,
        }),
        "passive_sell" => Ok(StrategyIntent {
            schema_version: STRATEGY_INTENT_SCHEMA_VERSION,
            intent_id: "export-passive-sell-1".to_string(),
            account_id: "acc-export-2".to_string(),
            session_id: "sess-export-2".to_string(),
            symbol: "AAPL".to_string(),
            side: "SELL".to_string(),
            order_type: OrderType::Limit,
            qty: 120,
            limit_price: Some(100),
            time_in_force: TimeInForce::Gtc,
            urgency: IntentUrgency::Normal,
            execution_policy: ExecutionPolicyKind::Passive,
            risk_budget_ref: Some(RiskBudgetRef {
                budget_id: "budget-export-2".to_string(),
                version: 1,
            }),
            model_id: Some("model-export-2".to_string()),
            execution_run_id: Some("run-export-2".to_string()),
            decision_key: Some("decision-export-2".to_string()),
            decision_attempt_seq: Some(1),
            recovery_policy: Some(StrategyRecoveryPolicy::NoAutoResume),
            algo: None,
            created_at_ns: 200,
            expires_at_ns: 1_200,
        }),
        "default_compare" => Ok(StrategyIntent {
            schema_version: STRATEGY_INTENT_SCHEMA_VERSION,
            intent_id: "export-default-compare-1".to_string(),
            account_id: "acc-export-3".to_string(),
            session_id: "sess-export-3".to_string(),
            symbol: "AAPL".to_string(),
            side: "BUY".to_string(),
            order_type: OrderType::Limit,
            qty: 90,
            limit_price: Some(100),
            time_in_force: TimeInForce::Ioc,
            urgency: IntentUrgency::Normal,
            execution_policy: ExecutionPolicyKind::Default,
            risk_budget_ref: Some(RiskBudgetRef {
                budget_id: "budget-export-3".to_string(),
                version: 2,
            }),
            model_id: Some("model-export-3".to_string()),
            execution_run_id: Some("run-export-3".to_string()),
            decision_key: Some("decision-export-3".to_string()),
            decision_attempt_seq: Some(1),
            recovery_policy: Some(StrategyRecoveryPolicy::NoAutoResume),
            algo: None,
            created_at_ns: 300,
            expires_at_ns: 1_300,
        }),
        _ => Err(format!("unsupported profile: {profile}")),
    }
}

fn print_usage() {
    println!("export-strategy-intent");
    println!("usage:");
    println!(
        "  cargo run --manifest-path gateway-rust/Cargo.toml --bin export_strategy_intent -- \\"
    );
    println!("    [--output contracts/fixtures/strategy_intent_v1.json] \\");
    println!("    [--profile aggressive_buy|passive_sell|default_compare] [--compact]");
}

#[cfg(test)]
mod tests {
    use super::{fixture_for_profile, strategy_intent::ExecutionPolicyKind};

    #[test]
    fn aggressive_buy_fixture_is_valid() {
        let intent = fixture_for_profile("aggressive_buy").expect("fixture");

        assert_eq!(intent.validate(), Ok(()));
        assert_eq!(intent.execution_policy, ExecutionPolicyKind::Aggressive);
        assert_eq!(intent.side, "BUY");
    }

    #[test]
    fn default_compare_fixture_round_trips_json() {
        let intent = fixture_for_profile("default_compare").expect("fixture");

        let raw = serde_json::to_string(&intent).expect("serialize");
        let parsed: super::strategy_intent::StrategyIntent =
            serde_json::from_str(&raw).expect("deserialize");

        assert_eq!(parsed.intent_id, "export-default-compare-1");
        assert_eq!(parsed.execution_policy, ExecutionPolicyKind::Default);
    }
}
