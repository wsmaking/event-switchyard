use std::env;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

use mini_exchange::OrderInput;
use mini_exchange::harness::{
    ExecutionPolicy, PolicyScenarioReport, QuantCleanStrategyIntent, evaluate_quant_clean_intent,
    policies_for_quant_clean_intent,
};

const DEFAULT_SEED_PATH: &str = "mini-exchange/examples/seed_book.json";
const DEFAULT_FORMATS: &str = "text,json,csv";

fn main() {
    match run() {
        Ok(has_failures) => {
            if has_failures {
                std::process::exit(1);
            }
        }
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    }
}

fn run() -> Result<bool, String> {
    let mut input_dir: Option<String> = None;
    let mut output_dir: Option<String> = None;
    let mut seed_path = DEFAULT_SEED_PATH.to_string();
    let mut formats = DEFAULT_FORMATS.to_string();
    let mut policy_mode = "intent".to_string();

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                print_usage();
                return Ok(false);
            }
            "--input-dir" => input_dir = Some(next_arg(&mut args, "--input-dir")?),
            "--output-dir" => output_dir = Some(next_arg(&mut args, "--output-dir")?),
            "--seed" => seed_path = next_arg(&mut args, "--seed")?,
            "--formats" => formats = next_arg(&mut args, "--formats")?,
            "--policies" => policy_mode = next_arg(&mut args, "--policies")?,
            other => return Err(format!("unknown arg: {other}")),
        }
    }

    let input_dir = input_dir.ok_or_else(|| "--input-dir is required".to_string())?;
    let output_dir = output_dir.ok_or_else(|| "--output-dir is required".to_string())?;
    let seed_orders = read_seed_orders(&seed_path)?;
    let formats = parse_formats(&formats)?;

    fs::create_dir_all(&output_dir)
        .map_err(|err| format!("failed to create output dir {output_dir}: {err}"))?;

    let mut entries = list_json_files(&input_dir)?;
    entries.sort();

    let mut batch_summary =
        vec!["input_file,intent_id,status,policy_count,output_base,error".to_string()];
    let mut policy_summary: Vec<String> = Vec::new();
    let mut failures = false;

    for path in entries {
        let input_file = path
            .file_name()
            .and_then(OsStr::to_str)
            .ok_or_else(|| format!("invalid file name: {}", path.display()))?
            .to_string();
        let output_base =
            sanitize_stem(path.file_stem().and_then(OsStr::to_str).unwrap_or("intent"));

        match process_one(
            &path,
            &seed_orders,
            &policy_mode,
            &formats,
            Path::new(&output_dir),
            &output_base,
        ) {
            Ok((intent_id, policy_csv_lines)) => {
                batch_summary.push(format!(
                    "{},{},OK,{},{},",
                    csv_escape(&input_file),
                    csv_escape(&intent_id),
                    policy_csv_lines.len(),
                    csv_escape(&output_base),
                ));
                if policy_summary.is_empty() {
                    policy_summary.push("scenario_id,policy,child_count,executed_qty,resting_qty,slippage_events,total_notional_slippage,avg_slippage_ticks_milli,spread,best_bid_depth,best_ask_depth,last_price".to_string());
                }
                policy_summary.extend(policy_csv_lines);
            }
            Err(err) => {
                failures = true;
                batch_summary.push(format!(
                    "{},{},ERROR,0,{},{}",
                    csv_escape(&input_file),
                    "",
                    csv_escape(&output_base),
                    csv_escape(&err),
                ));
            }
        }
    }

    fs::write(
        Path::new(&output_dir).join("batch_summary.csv"),
        batch_summary.join("\n") + "\n",
    )
    .map_err(|err| format!("failed to write batch summary: {err}"))?;

    if !policy_summary.is_empty() {
        fs::write(
            Path::new(&output_dir).join("policy_summary.csv"),
            policy_summary.join("\n") + "\n",
        )
        .map_err(|err| format!("failed to write policy summary: {err}"))?;
    }

    println!("WROTE {}/batch_summary.csv", output_dir);
    if !policy_summary.is_empty() {
        println!("WROTE {}/policy_summary.csv", output_dir);
    }

    Ok(failures)
}

fn process_one(
    path: &Path,
    seed_orders: &[OrderInput],
    policy_mode: &str,
    formats: &[OutputFormat],
    output_dir: &Path,
    output_base: &str,
) -> Result<(String, Vec<String>), String> {
    let quant_intent = read_quant_clean_intent(path)?;
    let policies = resolve_policies(&quant_intent, policy_mode)?;
    let scenario = evaluate_quant_clean_intent(seed_orders, &quant_intent, &policies)
        .map_err(|err| err.to_string())?;

    for format in formats {
        let rendered = render_report(&scenario, *format)?;
        let extension = format.extension();
        let output_path = output_dir.join(format!("{}.{}", output_base, extension));
        fs::write(&output_path, rendered)
            .map_err(|err| format!("failed to write {}: {err}", output_path.display()))?;
    }

    let csv_lines = scenario
        .render_csv_summary()
        .lines()
        .skip(1)
        .map(|line| line.to_string())
        .collect::<Vec<_>>();

    Ok((scenario.scenario_id.clone(), csv_lines))
}

fn list_json_files(dir: &str) -> Result<Vec<PathBuf>, String> {
    let mut files = Vec::new();
    for entry in
        fs::read_dir(dir).map_err(|err| format!("failed to read input dir {dir}: {err}"))?
    {
        let entry = entry.map_err(|err| format!("failed to read dir entry in {dir}: {err}"))?;
        let path = entry.path();
        if path.extension().and_then(OsStr::to_str) == Some("json") {
            files.push(path);
        }
    }
    Ok(files)
}

fn read_seed_orders(path: &str) -> Result<Vec<OrderInput>, String> {
    let raw = fs::read_to_string(path)
        .map_err(|err| format!("failed to read seed file {path}: {err}"))?;
    serde_json::from_str(&raw).map_err(|err| format!("failed to parse seed file {path}: {err}"))
}

fn read_quant_clean_intent(path: &Path) -> Result<QuantCleanStrategyIntent, String> {
    let raw = fs::read_to_string(path)
        .map_err(|err| format!("failed to read intent file {}: {err}", path.display()))?;
    serde_json::from_str(&raw)
        .map_err(|err| format!("failed to parse intent file {}: {err}", path.display()))
}

fn next_arg(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("missing value for {flag}"))
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputFormat {
    Text,
    Json,
    Csv,
}

impl OutputFormat {
    fn extension(self) -> &'static str {
        match self {
            Self::Text => "txt",
            Self::Json => "json",
            Self::Csv => "csv",
        }
    }
}

fn parse_formats(spec: &str) -> Result<Vec<OutputFormat>, String> {
    let mut formats = Vec::new();
    for token in spec.split(',').filter(|token| !token.trim().is_empty()) {
        let format = match token.trim().to_ascii_lowercase().as_str() {
            "text" | "txt" => OutputFormat::Text,
            "json" => OutputFormat::Json,
            "csv" => OutputFormat::Csv,
            _ => return Err(format!("unsupported format: {}", token.trim())),
        };
        if !formats.contains(&format) {
            formats.push(format);
        }
    }
    if formats.is_empty() {
        Err("no output formats resolved".to_string())
    } else {
        Ok(formats)
    }
}

fn render_report(scenario: &PolicyScenarioReport, format: OutputFormat) -> Result<String, String> {
    match format {
        OutputFormat::Text => Ok(scenario.render_text()),
        OutputFormat::Json => scenario.render_json_pretty().map_err(|err| err.to_string()),
        OutputFormat::Csv => Ok(scenario.render_csv_summary()),
    }
}

fn sanitize_stem(raw: &str) -> String {
    let mut output = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            output.push(ch);
        } else {
            output.push('_');
        }
    }
    if output.is_empty() {
        "intent".to_string()
    } else {
        output
    }
}

fn csv_escape(raw: &str) -> String {
    if raw.contains(',') || raw.contains('"') || raw.contains('\n') {
        format!("\"{}\"", raw.replace('"', "\"\""))
    } else {
        raw.to_string()
    }
}

fn print_usage() {
    println!("quant-batch");
    println!("usage:");
    println!("  cargo run --manifest-path mini-exchange/Cargo.toml --bin quant_batch -- \\");
    println!("    --input-dir /path/to/intents --output-dir /path/to/reports \\");
    println!("    [--seed mini-exchange/examples/seed_book.json] [--formats text,json,csv] \\");
    println!("    [--policies intent|all|PASSIVE,AGGRESSIVE]");
}
