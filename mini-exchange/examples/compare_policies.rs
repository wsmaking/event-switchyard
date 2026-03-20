use mini_exchange::harness::{
    ExecutionPolicy, PolicyScenarioReport, StrategyIntent, evaluate_policies,
};
use mini_exchange::{OrderInput, OrderType, Side};

fn main() {
    let seed_orders = vec![
        limit("B0", Side::Buy, 99, 100, 1),
        limit("S0", Side::Sell, 100, 50, 2),
        limit("S1", Side::Sell, 101, 80, 3),
        limit("S2", Side::Sell, 102, 120, 4),
    ];

    let intent = StrategyIntent {
        intent_id: "demo-intent-buy-1".to_string(),
        side: Side::Buy,
        qty: 100,
        limit_price: Some(102),
        ts: 5,
        slice_count: 4,
        vwap_weights: vec![1, 2, 3, 4],
        participation_bps: 2_500,
    };

    let reports = evaluate_policies(
        &seed_orders,
        &intent,
        &[
            ExecutionPolicy::Passive,
            ExecutionPolicy::Aggressive,
            ExecutionPolicy::Twap,
            ExecutionPolicy::Vwap,
            ExecutionPolicy::Pov,
        ],
    )
    .expect("policy reports");

    let scenario = PolicyScenarioReport::from_reports(intent, reports);
    println!("{}", scenario.render_text());
}

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
