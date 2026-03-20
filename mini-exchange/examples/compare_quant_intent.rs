use std::env;
use std::fs;

use mini_exchange::harness::{
    ExecutionPolicy, PolicyScenarioReport, QuantStrategyIntent, evaluate_quant_intent,
};
use mini_exchange::{OrderInput, OrderType, Side};

fn main() {
    let mut format = "text".to_string();
    let mut intent_path = "mini-exchange/examples/quant_intent_buy.json".to_string();

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--format" => {
                format = args.next().unwrap_or_else(|| "text".to_string());
            }
            "--intent" => {
                intent_path = args
                    .next()
                    .unwrap_or_else(|| "mini-exchange/examples/quant_intent_buy.json".to_string());
            }
            _ => {}
        }
    }

    let raw = fs::read_to_string(&intent_path).expect("read intent fixture");
    let quant_intent: QuantStrategyIntent = serde_json::from_str(&raw).expect("parse intent");
    let seed_orders = vec![
        limit("B0", Side::Buy, 99, 100, 1),
        limit("S0", Side::Sell, 100, 50, 2),
        limit("S1", Side::Sell, 101, 80, 3),
        limit("S2", Side::Sell, 102, 120, 4),
    ];

    let scenario = evaluate_quant_intent(
        &seed_orders,
        &quant_intent,
        &[
            ExecutionPolicy::Passive,
            ExecutionPolicy::Aggressive,
            ExecutionPolicy::Twap,
            ExecutionPolicy::Vwap,
            ExecutionPolicy::Pov,
        ],
    )
    .expect("scenario");

    print_scenario(&scenario, &format);
}

fn print_scenario(scenario: &PolicyScenarioReport, format: &str) {
    match format {
        "json" => println!("{}", scenario.render_json_pretty().expect("json")),
        "csv" => println!("{}", scenario.render_csv_summary()),
        _ => println!("{}", scenario.render_text()),
    }
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
