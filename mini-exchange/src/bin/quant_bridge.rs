use std::env;
use std::fs;
use std::path::Path;

use mini_exchange::OrderInput;
use mini_exchange::harness::{
    ExecutionPolicy, QuantCleanStrategyIntent, evaluate_quant_clean_intent,
    policies_for_quant_clean_intent,
};

const DEFAULT_SEED_PATH: &str = "mini-exchange/examples/seed_book.json";

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut intent_path: Option<String> = None;
    let mut seed_path = DEFAULT_SEED_PATH.to_string();
    let mut format = "text".to_string();
    let mut policy_mode = "intent".to_string();
    let mut output_path: Option<String> = None;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                print_usage();
                return Ok(());
            }
            "--intent" => intent_path = Some(next_arg(&mut args, "--intent")?),
            "--seed" => seed_path = next_arg(&mut args, "--seed")?,
            "--format" => format = next_arg(&mut args, "--format")?,
            "--policies" => policy_mode = next_arg(&mut args, "--policies")?,
            "--output" => output_path = Some(next_arg(&mut args, "--output")?),
            other => return Err(format!("unknown arg: {other}")),
        }
    }

    let intent_path = intent_path.ok_or_else(|| "--intent is required".to_string())?;
    let seed_orders = read_seed_orders(&seed_path)?;
    let quant_intent = read_quant_clean_intent(&intent_path)?;
    let policies = resolve_policies(&quant_intent, &policy_mode)?;
    let scenario = evaluate_quant_clean_intent(&seed_orders, &quant_intent, &policies)
        .map_err(|err| err.to_string())?;
    let rendered = render_report(&scenario, &format)?;

    if let Some(output_path) = output_path {
        if let Some(parent) = Path::new(&output_path).parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).map_err(|err| err.to_string())?;
            }
        }
        fs::write(&output_path, rendered).map_err(|err| err.to_string())?;
        println!("WROTE {output_path}");
    } else {
        println!("{rendered}");
    }

    Ok(())
}

fn next_arg(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("missing value for {flag}"))
}

fn read_seed_orders(path: &str) -> Result<Vec<OrderInput>, String> {
    let raw = fs::read_to_string(path)
        .map_err(|err| format!("failed to read seed file {path}: {err}"))?;
    serde_json::from_str(&raw).map_err(|err| format!("failed to parse seed file {path}: {err}"))
}

fn read_quant_clean_intent(path: &str) -> Result<QuantCleanStrategyIntent, String> {
    let raw = fs::read_to_string(path)
        .map_err(|err| format!("failed to read intent file {path}: {err}"))?;
    serde_json::from_str(&raw).map_err(|err| format!("failed to parse intent file {path}: {err}"))
}

fn resolve_policies(
    intent: &QuantCleanStrategyIntent,
    policy_mode: &str,
) -> Result<Vec<ExecutionPolicy>, String> {
    match policy_mode.to_ascii_lowercase().as_str() {
        "intent" => Ok(policies_for_quant_clean_intent(intent)),
        "all" => Ok(vec![
            ExecutionPolicy::Passive,
            ExecutionPolicy::Aggressive,
            ExecutionPolicy::Twap,
            ExecutionPolicy::Vwap,
            ExecutionPolicy::Pov,
        ]),
        custom => {
            let mut parsed = Vec::new();
            for token in custom.split(',').filter(|token| !token.trim().is_empty()) {
                parsed.push(parse_policy(token.trim())?);
            }
            if parsed.is_empty() {
                Err("no policies resolved".to_string())
            } else {
                Ok(parsed)
            }
        }
    }
}

fn parse_policy(token: &str) -> Result<ExecutionPolicy, String> {
    match token.to_ascii_uppercase().as_str() {
        "PASSIVE" => Ok(ExecutionPolicy::Passive),
        "AGGRESSIVE" => Ok(ExecutionPolicy::Aggressive),
        "TWAP" => Ok(ExecutionPolicy::Twap),
        "VWAP" => Ok(ExecutionPolicy::Vwap),
        "POV" => Ok(ExecutionPolicy::Pov),
        _ => Err(format!("unsupported policy: {token}")),
    }
}

fn render_report(
    scenario: &mini_exchange::harness::PolicyScenarioReport,
    format: &str,
) -> Result<String, String> {
    match format.to_ascii_lowercase().as_str() {
        "text" => Ok(scenario.render_text()),
        "json" => scenario.render_json_pretty().map_err(|err| err.to_string()),
        "csv" => Ok(scenario.render_csv_summary()),
        _ => Err(format!("unsupported format: {format}")),
    }
}

fn print_usage() {
    println!("quant-bridge");
    println!("usage:");
    println!("  cargo run --manifest-path mini-exchange/Cargo.toml --bin quant_bridge -- \\");
    println!(
        "    --intent /path/to/strategy_intent.json [--seed mini-exchange/examples/seed_book.json] \\"
    );
    println!(
        "    [--format text|json|csv] [--policies intent|all|PASSIVE,AGGRESSIVE] [--output path]"
    );
}
