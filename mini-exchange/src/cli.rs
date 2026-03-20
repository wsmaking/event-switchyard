use crate::{MatchingEngine, OrderInput, OrderType, Side};

enum Command {
    Submit(OrderInput),
    Cancel { order_id: String, ts: Option<u64> },
    Advance(u64),
    Book,
    BookDelayed(u64),
    Trades,
    Last,
    Queue(String),
    Metrics,
    Slippage,
    SetCancelLatency(u64),
    SetMarketDataDelay(u64),
    SetRisk { key: String, value: Option<u64> },
}

pub fn run_script(input: &str) -> Result<String, String> {
    let mut engine = MatchingEngine::new();
    let mut output = Vec::new();

    for (index, raw_line) in input.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        match parse_command(line).map_err(|reason| format!("line {}: {}", index + 1, reason))? {
            Command::Submit(order) => {
                let due = engine
                    .advance_to(order.ts)
                    .map_err(|reason| format!("line {}: {}", index + 1, reason))?;
                output.extend(due.into_iter().map(|event| event.render_line()));
                let status = engine.submit(order.clone());
                output.push(status.status_line(&order.order_id));
                output.extend(status.trade_lines());
            }
            Command::Cancel { order_id, ts } => {
                if let Some(ts) = ts {
                    let due = engine
                        .advance_to(ts)
                        .map_err(|reason| format!("line {}: {}", index + 1, reason))?;
                    output.extend(due.into_iter().map(|event| event.render_line()));
                }
                let outcome = engine.request_cancel(&order_id);
                output.push(outcome.render_line(&order_id));
            }
            Command::Advance(ts) => {
                let due = engine
                    .advance_to(ts)
                    .map_err(|reason| format!("line {}: {}", index + 1, reason))?;
                output.extend(due.into_iter().map(|event| event.render_line()));
                output.push(format!("TIME {}", ts));
            }
            Command::Book => output.push(engine.snapshot().render()),
            Command::BookDelayed(ts) => {
                let due = engine
                    .advance_to(ts)
                    .map_err(|reason| format!("line {}: {}", index + 1, reason))?;
                output.extend(due.into_iter().map(|event| event.render_line()));
                output.push(engine.delayed_snapshot_at(ts).render());
            }
            Command::Trades => {
                output.extend(engine.trades().iter().map(|trade| trade.render_line()));
            }
            Command::Last => match engine.snapshot().last_price {
                Some(price) => output.push(format!("LAST_PRICE: {}", price)),
                None => output.push("LAST_PRICE: None".to_string()),
            },
            Command::Queue(order_id) => match engine.queue_position(&order_id) {
                Some(position) => output.push(position.render_line()),
                None => output.push(format!("QUEUE {} NOT_ON_BOOK", order_id)),
            },
            Command::Metrics => output.push(engine.book_metrics().render()),
            Command::Slippage => output.push(engine.slippage_stats().render()),
            Command::SetCancelLatency(latency) => {
                engine.set_cancel_latency(latency);
                output.push(format!("CANCEL_LATENCY {}", engine.cancel_latency()));
            }
            Command::SetMarketDataDelay(delay) => {
                engine.set_market_data_delay(delay);
                output.push(format!("MARKET_DATA_DELAY {}", engine.market_data_delay()));
            }
            Command::SetRisk { key, value } => {
                engine
                    .set_risk_limit(&key, value)
                    .map_err(|reason| format!("line {}: {}", index + 1, reason))?;
                output.push(format!(
                    "RISK {} {}",
                    key,
                    value
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "NONE".to_string())
                ));
            }
        }
    }

    Ok(output.join("\n"))
}

fn parse_command(line: &str) -> Result<Command, String> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    let Some(name) = parts.first().copied() else {
        return Err("EMPTY_COMMAND".to_string());
    };

    match name {
        "SUBMIT" => parse_submit(&parts),
        "CANCEL" => parse_cancel(&parts),
        "ADVANCE" => parse_advance(&parts),
        "BOOK" if parts.len() == 1 => Ok(Command::Book),
        "BOOK_DELAYED" => parse_book_delayed(&parts),
        "TRADES" if parts.len() == 1 => Ok(Command::Trades),
        "LAST" if parts.len() == 1 => Ok(Command::Last),
        "QUEUE" => parse_queue(&parts),
        "METRICS" if parts.len() == 1 => Ok(Command::Metrics),
        "SLIPPAGE" if parts.len() == 1 => Ok(Command::Slippage),
        "SET_CANCEL_LATENCY" => parse_set_cancel_latency(&parts),
        "SET_MD_DELAY" => parse_set_market_data_delay(&parts),
        "SET_RISK" => parse_set_risk(&parts),
        "BOOK" | "TRADES" | "LAST" | "METRICS" | "SLIPPAGE" => {
            Err(format!("{} takes no arguments", name))
        }
        other => Err(format!("UNKNOWN_COMMAND {}", other)),
    }
}

fn parse_submit(parts: &[&str]) -> Result<Command, String> {
    if parts.len() != 7 {
        return Err("SUBMIT requires 6 arguments".to_string());
    }

    let order_id = parts[1].to_string();
    let side = parse_side(parts[2])?;
    let order_type = parse_order_type(parts[3])?;
    let price = parse_price(parts[4])?;
    let qty = parts[5]
        .parse::<u64>()
        .map_err(|_| "INVALID_QTY_LITERAL".to_string())?;
    let ts = parts[6]
        .parse::<u64>()
        .map_err(|_| "INVALID_TS_LITERAL".to_string())?;

    Ok(Command::Submit(OrderInput {
        order_id,
        side,
        order_type,
        price,
        qty,
        ts,
    }))
}

fn parse_cancel(parts: &[&str]) -> Result<Command, String> {
    if !(parts.len() == 2 || parts.len() == 3) {
        return Err("CANCEL requires 1 or 2 arguments".to_string());
    }
    let ts = if parts.len() == 3 {
        Some(
            parts[2]
                .parse::<u64>()
                .map_err(|_| "INVALID_TS_LITERAL".to_string())?,
        )
    } else {
        None
    };
    Ok(Command::Cancel {
        order_id: parts[1].to_string(),
        ts,
    })
}

fn parse_advance(parts: &[&str]) -> Result<Command, String> {
    if parts.len() != 2 {
        return Err("ADVANCE requires 1 argument".to_string());
    }
    Ok(Command::Advance(
        parts[1]
            .parse::<u64>()
            .map_err(|_| "INVALID_TS_LITERAL".to_string())?,
    ))
}

fn parse_book_delayed(parts: &[&str]) -> Result<Command, String> {
    if parts.len() != 2 {
        return Err("BOOK_DELAYED requires 1 argument".to_string());
    }
    Ok(Command::BookDelayed(
        parts[1]
            .parse::<u64>()
            .map_err(|_| "INVALID_TS_LITERAL".to_string())?,
    ))
}

fn parse_queue(parts: &[&str]) -> Result<Command, String> {
    if parts.len() != 2 {
        return Err("QUEUE requires 1 argument".to_string());
    }
    Ok(Command::Queue(parts[1].to_string()))
}

fn parse_set_cancel_latency(parts: &[&str]) -> Result<Command, String> {
    if parts.len() != 2 {
        return Err("SET_CANCEL_LATENCY requires 1 argument".to_string());
    }
    Ok(Command::SetCancelLatency(
        parts[1]
            .parse::<u64>()
            .map_err(|_| "INVALID_LATENCY_LITERAL".to_string())?,
    ))
}

fn parse_set_market_data_delay(parts: &[&str]) -> Result<Command, String> {
    if parts.len() != 2 {
        return Err("SET_MD_DELAY requires 1 argument".to_string());
    }
    Ok(Command::SetMarketDataDelay(
        parts[1]
            .parse::<u64>()
            .map_err(|_| "INVALID_DELAY_LITERAL".to_string())?,
    ))
}

fn parse_set_risk(parts: &[&str]) -> Result<Command, String> {
    if parts.len() != 3 {
        return Err("SET_RISK requires 2 arguments".to_string());
    }
    let value = match parts[2] {
        "NONE" => None,
        raw => Some(
            raw.parse::<u64>()
                .map_err(|_| "INVALID_RISK_VALUE".to_string())?,
        ),
    };
    Ok(Command::SetRisk {
        key: parts[1].to_string(),
        value,
    })
}

fn parse_side(raw: &str) -> Result<Side, String> {
    match raw {
        "BUY" => Ok(Side::Buy),
        "SELL" => Ok(Side::Sell),
        _ => Err(format!("INVALID_SIDE {}", raw)),
    }
}

fn parse_order_type(raw: &str) -> Result<OrderType, String> {
    match raw {
        "LIMIT" => Ok(OrderType::Limit),
        "MARKET" => Ok(OrderType::Market),
        _ => Err(format!("INVALID_TYPE {}", raw)),
    }
}

fn parse_price(raw: &str) -> Result<Option<u64>, String> {
    match raw {
        "-" | "NONE" => Ok(None),
        _ => raw
            .parse::<u64>()
            .map(Some)
            .map_err(|_| format!("INVALID_PRICE {}", raw)),
    }
}

#[cfg(test)]
mod tests {
    use super::run_script;

    #[test]
    fn script_renders_accept_trade_and_book() {
        let script = "\
SUBMIT S1 SELL LIMIT 100 50 1
SUBMIT S2 SELL LIMIT 101 80 2
SUBMIT B1 BUY MARKET - 100 3
BOOK
";

        let output = run_script(script).expect("script should run");

        assert!(output.contains("ACCEPT S1"));
        assert!(output.contains("ACCEPT S2"));
        assert!(output.contains("ACCEPT B1"));
        assert!(output.contains("TRADE B1 S1 100 50"));
        assert!(output.contains("TRADE B1 S2 101 50"));
        assert!(output.contains("ASKS"));
        assert!(output.contains("101: 30"));
        assert!(output.contains("LAST_PRICE: 101"));
    }

    #[test]
    fn script_renders_cancel_and_trade_log() {
        let script = "\
SUBMIT O1 BUY LIMIT 99 100 1
CANCEL O1
TRADES
LAST
";

        let output = run_script(script).expect("script should run");

        assert!(output.contains("ACCEPT O1"));
        assert!(output.contains("CANCELLED O1"));
        assert!(output.contains("LAST_PRICE: None"));
    }

    #[test]
    fn script_ignores_comments_and_can_replay_trades() {
        let script = "\
# comment
SUBMIT B1 BUY LIMIT 99 100 1
SUBMIT S1 SELL LIMIT 99 60 2
TRADES
";

        let output = run_script(script).expect("script should run");

        assert!(output.contains("ACCEPT B1"));
        assert!(output.contains("ACCEPT S1"));
        assert!(output.contains("TRADE B1 S1 99 60"));
    }

    #[test]
    fn script_rejects_invalid_command_shape() {
        let output = run_script("SUBMIT only four args\n");

        assert_eq!(output.unwrap_err(), "line 1: SUBMIT requires 6 arguments");
    }

    #[test]
    fn script_reports_queue_position_metrics_and_slippage() {
        let script = "\
SUBMIT A BUY LIMIT 99 100 1
SUBMIT B BUY LIMIT 99 50 2
QUEUE B
METRICS
SUBMIT S SELL MARKET - 120 3
SLIPPAGE
";

        let output = run_script(script).expect("script should run");

        assert!(
            output.contains("QUEUE B side=Buy price=99 position=2 ahead_qty=100 level_qty=150")
        );
        assert!(output.contains("THIN_ASK true"));
        assert!(output.contains("SLIPPAGE_EVENT_COUNT 1"));
    }

    #[test]
    fn script_supports_cancel_latency_and_delayed_book() {
        let script = "\
SET_CANCEL_LATENCY 3
SET_MD_DELAY 2
SUBMIT O1 BUY LIMIT 99 100 1
CANCEL O1 2
BOOK_DELAYED 3
ADVANCE 5
BOOK
";

        let output = run_script(script).expect("script should run");

        assert!(output.contains("CANCEL_PENDING O1 5"));
        assert!(output.contains("BIDS\n99: 100"));
        assert!(output.contains("CANCELLED O1"));
        assert!(output.ends_with("BIDS\n\nASKS\n\nLAST_PRICE: None"));
    }

    #[test]
    fn script_applies_risk_limits() {
        let script = "\
SET_RISK MAX_ORDER_QTY 10
SUBMIT O1 BUY LIMIT 99 11 1
SET_RISK MAX_MARKET_QTY 5
SUBMIT O2 BUY MARKET - 6 2
";

        let output = run_script(script).expect("script should run");

        assert!(output.contains("RISK MAX_ORDER_QTY 10"));
        assert!(output.contains("REJECT O1 RISK_MAX_ORDER_QTY"));
        assert!(output.contains("REJECT O2 RISK_MAX_MARKET_QTY"));
    }
}
