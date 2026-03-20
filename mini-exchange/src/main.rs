use std::env;
use std::fs;
use std::io::{self, Read};

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() > 1 && (args[1] == "--help" || args[1] == "-h") {
        print_usage();
        return;
    }

    let input = if args.len() > 2 && args[1] == "--file" {
        match fs::read_to_string(&args[2]) {
            Ok(contents) => contents,
            Err(err) => {
                eprintln!("failed to read file {}: {}", args[2], err);
                std::process::exit(1);
            }
        }
    } else {
        let mut stdin_input = String::new();
        if let Err(err) = io::stdin().read_to_string(&mut stdin_input) {
            eprintln!("failed to read stdin: {}", err);
            std::process::exit(1);
        }
        stdin_input
    };

    match mini_exchange::cli::run_script(&input) {
        Ok(output) => {
            if !output.is_empty() {
                println!("{}", output);
            }
        }
        Err(err) => {
            eprintln!("{}", err);
            std::process::exit(1);
        }
    }
}

fn print_usage() {
    println!("mini-exchange v0.2");
    println!("usage:");
    println!("  cargo run --manifest-path mini-exchange/Cargo.toml -- --file path/to/script.txt");
    println!("  cat script.txt | cargo run --manifest-path mini-exchange/Cargo.toml");
    println!();
    println!("commands:");
    println!("  SUBMIT <order_id> <BUY|SELL> <LIMIT|MARKET> <price|-> <qty> <ts>");
    println!("  CANCEL <order_id> [ts]");
    println!("  ADVANCE <ts>");
    println!("  BOOK");
    println!("  BOOK_DELAYED <ts>");
    println!("  TRADES");
    println!("  LAST");
    println!("  QUEUE <order_id>");
    println!("  METRICS");
    println!("  SLIPPAGE");
    println!("  SET_CANCEL_LATENCY <ticks>");
    println!("  SET_MD_DELAY <ticks>");
    println!("  SET_RISK <MAX_ORDER_QTY|MAX_MARKET_QTY|MAX_NOTIONAL> <value|NONE>");
}
