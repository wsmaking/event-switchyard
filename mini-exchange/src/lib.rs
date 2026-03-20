use std::collections::{BTreeMap, HashMap, VecDeque};

use serde::{Deserialize, Serialize};

pub mod cli;
pub mod harness;

const THIN_BOOK_TOP_DEPTH_THRESHOLD: u64 = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderType {
    Limit,
    Market,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderInput {
    pub order_id: String,
    pub side: Side,
    pub order_type: OrderType,
    pub price: Option<u64>,
    pub qty: u64,
    pub ts: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Trade {
    pub buy_order_id: String,
    pub sell_order_id: String,
    pub price: u64,
    pub qty: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BookLevel {
    pub price: u64,
    pub total_qty: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BookSnapshot {
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
    pub last_price: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubmitOutcome {
    pub accepted: bool,
    pub reject_reason: Option<&'static str>,
    pub trades: Vec<Trade>,
    pub resting: bool,
    pub remaining_qty: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelOutcome {
    pub cancelled: bool,
    pub reject_reason: Option<&'static str>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelLifecycleEvent {
    pub order_id: String,
    pub apply_at_ts: u64,
    pub outcome: CancelOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum CancelRequestOutcome {
    Immediate(CancelOutcome),
    Pending { order_id: String, apply_at_ts: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueuePosition {
    pub order_id: String,
    pub side: Side,
    pub price: u64,
    pub position: usize,
    pub ahead_qty: u64,
    pub level_total_qty: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RiskLimits {
    pub max_order_qty: Option<u64>,
    pub max_market_qty: Option<u64>,
    pub max_notional: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BookMetrics {
    pub spread: Option<u64>,
    pub best_bid_depth: u64,
    pub best_ask_depth: u64,
    pub top3_bid_depth: u64,
    pub top3_ask_depth: u64,
    pub bid_levels: usize,
    pub ask_levels: usize,
    pub thin_bid: bool,
    pub thin_ask: bool,
    pub imbalance_bps: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SlippageEvent {
    pub order_id: String,
    pub side: Side,
    pub reference_price: u64,
    pub executed_qty: u64,
    pub executed_notional: u128,
    pub worst_price: u64,
    pub notional_slippage: i128,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SlippageStats {
    pub event_count: usize,
    pub total_executed_qty: u64,
    pub total_notional_slippage: i128,
    pub max_worst_slippage_ticks: u64,
    pub last_event: Option<SlippageEvent>,
}

impl Trade {
    pub fn render_line(&self) -> String {
        format!(
            "TRADE {} {} {} {}",
            self.buy_order_id, self.sell_order_id, self.price, self.qty
        )
    }
}

impl BookSnapshot {
    pub fn render(&self) -> String {
        fn render_side(label: &str, levels: &[BookLevel]) -> String {
            let mut lines = vec![label.to_string()];
            for level in levels {
                lines.push(format!("{}: {}", level.price, level.total_qty));
            }
            lines.join("\n")
        }

        let sections = vec![
            render_side("BIDS", &self.bids),
            render_side("ASKS", &self.asks),
            match self.last_price {
                Some(price) => format!("LAST_PRICE: {}", price),
                None => "LAST_PRICE: None".to_string(),
            },
        ];
        sections.join("\n\n")
    }
}

impl SubmitOutcome {
    pub fn status_line(&self, order_id: &str) -> String {
        match self.reject_reason {
            Some(reason) => format!("REJECT {} {}", order_id, reason),
            None => format!("ACCEPT {}", order_id),
        }
    }

    pub fn trade_lines(&self) -> Vec<String> {
        self.trades.iter().map(Trade::render_line).collect()
    }
}

impl CancelOutcome {
    pub fn status_line(&self, order_id: &str) -> String {
        match self.reject_reason {
            Some(reason) => format!("REJECT {} {}", order_id, reason),
            None => format!("CANCELLED {}", order_id),
        }
    }
}

impl CancelLifecycleEvent {
    pub fn render_line(&self) -> String {
        self.outcome.status_line(&self.order_id)
    }
}

impl CancelRequestOutcome {
    pub fn render_line(&self, order_id: &str) -> String {
        match self {
            Self::Immediate(outcome) => outcome.status_line(order_id),
            Self::Pending { apply_at_ts, .. } => {
                format!("CANCEL_PENDING {} {}", order_id, apply_at_ts)
            }
        }
    }
}

impl QueuePosition {
    pub fn render_line(&self) -> String {
        format!(
            "QUEUE {} side={:?} price={} position={} ahead_qty={} level_qty={}",
            self.order_id,
            self.side,
            self.price,
            self.position,
            self.ahead_qty,
            self.level_total_qty
        )
    }
}

impl BookMetrics {
    pub fn render(&self) -> String {
        let mut lines = Vec::new();
        lines.push(match self.spread {
            Some(spread) => format!("SPREAD {}", spread),
            None => "SPREAD None".to_string(),
        });
        lines.push(format!("BEST_BID_DEPTH {}", self.best_bid_depth));
        lines.push(format!("BEST_ASK_DEPTH {}", self.best_ask_depth));
        lines.push(format!("TOP3_BID_DEPTH {}", self.top3_bid_depth));
        lines.push(format!("TOP3_ASK_DEPTH {}", self.top3_ask_depth));
        lines.push(format!("BID_LEVELS {}", self.bid_levels));
        lines.push(format!("ASK_LEVELS {}", self.ask_levels));
        lines.push(format!("THIN_BID {}", self.thin_bid));
        lines.push(format!("THIN_ASK {}", self.thin_ask));
        lines.push(match self.imbalance_bps {
            Some(value) => format!("IMBALANCE_BPS {}", value),
            None => "IMBALANCE_BPS None".to_string(),
        });
        lines.join("\n")
    }
}

impl SlippageEvent {
    pub fn average_slippage_ticks_milli(&self) -> i128 {
        if self.executed_qty == 0 {
            0
        } else {
            self.notional_slippage * 1000 / i128::from(self.executed_qty)
        }
    }
}

impl SlippageStats {
    pub fn average_slippage_ticks_milli(&self) -> i128 {
        if self.total_executed_qty == 0 {
            0
        } else {
            self.total_notional_slippage * 1000 / i128::from(self.total_executed_qty)
        }
    }

    pub fn render(&self) -> String {
        let mut lines = Vec::new();
        lines.push(format!("SLIPPAGE_EVENT_COUNT {}", self.event_count));
        lines.push(format!(
            "SLIPPAGE_TOTAL_EXECUTED_QTY {}",
            self.total_executed_qty
        ));
        lines.push(format!(
            "SLIPPAGE_TOTAL_NOTIONAL_TICKS {}",
            self.total_notional_slippage
        ));
        lines.push(format!(
            "SLIPPAGE_AVG_TICKS_MILLI {}",
            self.average_slippage_ticks_milli()
        ));
        lines.push(format!(
            "SLIPPAGE_MAX_WORST_TICKS {}",
            self.max_worst_slippage_ticks
        ));
        if let Some(last) = &self.last_event {
            lines.push(format!("SLIPPAGE_LAST_ORDER {}", last.order_id));
            lines.push(format!(
                "SLIPPAGE_LAST_AVG_TICKS_MILLI {}",
                last.average_slippage_ticks_milli()
            ));
        }
        lines.join("\n")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OrderState {
    order_id: String,
    side: Side,
    price: Option<u64>,
    remaining_qty: u64,
    active: bool,
    resting: bool,
    ts: u64,
}

impl OrderState {
    fn from_input(input: &OrderInput) -> Self {
        Self {
            order_id: input.order_id.clone(),
            side: input.side,
            price: input.price,
            remaining_qty: input.qty,
            active: true,
            resting: false,
            ts: input.ts,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MatchingEngine {
    bids: BTreeMap<u64, VecDeque<String>>,
    asks: BTreeMap<u64, VecDeque<String>>,
    orders: HashMap<String, OrderState>,
    trades: Vec<Trade>,
    last_price: Option<u64>,
    current_ts: u64,
    cancel_latency: u64,
    market_data_delay: u64,
    pending_cancels: BTreeMap<u64, Vec<String>>,
    snapshot_history: Vec<(u64, BookSnapshot)>,
    risk_limits: RiskLimits,
    slippage_stats: SlippageStats,
}

impl Default for MatchingEngine {
    fn default() -> Self {
        let mut engine = Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            orders: HashMap::new(),
            trades: Vec::new(),
            last_price: None,
            current_ts: 0,
            cancel_latency: 0,
            market_data_delay: 0,
            pending_cancels: BTreeMap::new(),
            snapshot_history: Vec::new(),
            risk_limits: RiskLimits::default(),
            slippage_stats: SlippageStats::default(),
        };
        engine.record_snapshot(0);
        engine
    }
}

impl MatchingEngine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn submit(&mut self, input: OrderInput) -> SubmitOutcome {
        if let Err(reason) = self.advance_to(input.ts) {
            return SubmitOutcome {
                accepted: false,
                reject_reason: Some(reason),
                trades: Vec::new(),
                resting: false,
                remaining_qty: 0,
            };
        }

        if let Err(reason) = validate_input(
            &input,
            &self.orders,
            &self.risk_limits,
            self.market_reference_price(input.side),
        ) {
            return SubmitOutcome {
                accepted: false,
                reject_reason: Some(reason),
                trades: Vec::new(),
                resting: false,
                remaining_qty: 0,
            };
        }

        let mut incoming = OrderState::from_input(&input);
        let mut trades = Vec::new();
        let reference_price = self.next_match_price(incoming.side);

        while incoming.remaining_qty > 0 {
            let maybe_price = self.next_match_price(incoming.side);
            let Some(price) = maybe_price else {
                break;
            };
            if !self.can_cross(&incoming, price) {
                break;
            }

            let Some(resting_id) = self.front_resting_id(incoming.side, price).cloned() else {
                self.remove_empty_level(incoming.side.opposite(), price);
                continue;
            };

            let (trade, resting_filled) = {
                let resting = self
                    .orders
                    .get_mut(&resting_id)
                    .expect("resting order must exist");
                let trade_qty = incoming.remaining_qty.min(resting.remaining_qty);
                incoming.remaining_qty -= trade_qty;
                resting.remaining_qty -= trade_qty;
                let resting_filled = resting.remaining_qty == 0;
                if resting_filled {
                    resting.active = false;
                    resting.resting = false;
                }
                (
                    Trade {
                        buy_order_id: if incoming.side == Side::Buy {
                            incoming.order_id.clone()
                        } else {
                            resting.order_id.clone()
                        },
                        sell_order_id: if incoming.side == Side::Sell {
                            incoming.order_id.clone()
                        } else {
                            resting.order_id.clone()
                        },
                        price,
                        qty: trade_qty,
                    },
                    resting_filled,
                )
            };

            self.last_price = Some(trade.price);
            trades.push(trade.clone());
            self.trades.push(trade);

            if resting_filled {
                self.pop_front_resting_id(incoming.side, price, &resting_id);
            }
        }

        let mut resting = false;
        if incoming.remaining_qty > 0 && input.order_type == OrderType::Limit {
            let book = self.book_for_side_mut(input.side);
            book.entry(input.price.expect("validated limit price"))
                .or_default()
                .push_back(input.order_id.clone());
            incoming.resting = true;
            resting = true;
        } else {
            incoming.active = false;
        }

        let remaining_qty = incoming.remaining_qty;
        self.orders.insert(input.order_id.clone(), incoming);
        self.record_snapshot(self.current_ts);

        if let Some(reference_price) = reference_price {
            self.record_slippage(&input, reference_price, &trades);
        }

        SubmitOutcome {
            accepted: true,
            reject_reason: None,
            trades,
            resting,
            remaining_qty,
        }
    }

    pub fn cancel(&mut self, order_id: &str) -> CancelOutcome {
        let outcome = self.cancel_immediate(order_id);
        if outcome.cancelled {
            self.record_snapshot(self.current_ts);
        }
        outcome
    }

    pub fn advance_to(&mut self, ts: u64) -> Result<Vec<CancelLifecycleEvent>, &'static str> {
        if ts < self.current_ts {
            return Err("NON_MONOTONIC_TS");
        }
        self.current_ts = ts;

        let due_keys: Vec<u64> = self
            .pending_cancels
            .keys()
            .copied()
            .filter(|apply_at| *apply_at <= ts)
            .collect();

        let mut events = Vec::new();
        for apply_at in due_keys {
            let Some(order_ids) = self.pending_cancels.remove(&apply_at) else {
                continue;
            };
            for order_id in order_ids {
                let outcome = self.cancel_immediate(&order_id);
                if outcome.cancelled {
                    self.record_snapshot(apply_at);
                }
                events.push(CancelLifecycleEvent {
                    order_id,
                    apply_at_ts: apply_at,
                    outcome,
                });
            }
        }

        Ok(events)
    }

    pub fn request_cancel(&mut self, order_id: &str) -> CancelRequestOutcome {
        if self.cancel_latency == 0 {
            return CancelRequestOutcome::Immediate(self.cancel(order_id));
        }

        if !self.is_cancellable(order_id) {
            return CancelRequestOutcome::Immediate(CancelOutcome {
                cancelled: false,
                reject_reason: Some(if self.orders.contains_key(order_id) {
                    "NOT_CANCELLABLE"
                } else {
                    "NOT_FOUND"
                }),
            });
        }

        let apply_at_ts = self.current_ts.saturating_add(self.cancel_latency);
        self.pending_cancels
            .entry(apply_at_ts)
            .or_default()
            .push(order_id.to_string());

        CancelRequestOutcome::Pending {
            order_id: order_id.to_string(),
            apply_at_ts,
        }
    }

    pub fn set_cancel_latency(&mut self, latency: u64) {
        self.cancel_latency = latency;
    }

    pub fn cancel_latency(&self) -> u64 {
        self.cancel_latency
    }

    pub fn set_market_data_delay(&mut self, delay: u64) {
        self.market_data_delay = delay;
    }

    pub fn market_data_delay(&self) -> u64 {
        self.market_data_delay
    }

    pub fn set_risk_limit(&mut self, key: &str, value: Option<u64>) -> Result<(), &'static str> {
        match key {
            "MAX_ORDER_QTY" => self.risk_limits.max_order_qty = value,
            "MAX_MARKET_QTY" => self.risk_limits.max_market_qty = value,
            "MAX_NOTIONAL" => self.risk_limits.max_notional = value,
            _ => return Err("UNKNOWN_RISK_LIMIT"),
        }
        Ok(())
    }

    pub fn risk_limits(&self) -> &RiskLimits {
        &self.risk_limits
    }

    pub fn delayed_snapshot_at(&self, at_ts: u64) -> BookSnapshot {
        let delayed_ts = at_ts.saturating_sub(self.market_data_delay);
        self.snapshot_history
            .iter()
            .rev()
            .find(|(snapshot_ts, _)| *snapshot_ts <= delayed_ts)
            .map(|(_, snapshot)| snapshot.clone())
            .unwrap_or_else(|| BookSnapshot {
                bids: Vec::new(),
                asks: Vec::new(),
                last_price: None,
            })
    }

    pub fn queue_position(&self, order_id: &str) -> Option<QueuePosition> {
        let order = self.orders.get(order_id)?;
        if !order.active || !order.resting || order.remaining_qty == 0 {
            return None;
        }

        let price = order.price?;
        let queue = match order.side {
            Side::Buy => self.bids.get(&price)?,
            Side::Sell => self.asks.get(&price)?,
        };

        let mut position = 0usize;
        let mut ahead_qty = 0u64;
        let mut level_total_qty = 0u64;
        for queued_id in queue {
            let queued = self.orders.get(queued_id)?;
            if !queued.active || !queued.resting {
                continue;
            }
            level_total_qty += queued.remaining_qty;
            position += 1;
            if queued.order_id == order_id {
                return Some(QueuePosition {
                    order_id: order_id.to_string(),
                    side: order.side,
                    price,
                    position,
                    ahead_qty,
                    level_total_qty,
                });
            }
            ahead_qty += queued.remaining_qty;
        }
        None
    }

    pub fn book_metrics(&self) -> BookMetrics {
        let snapshot = self.snapshot();
        let spread = match (snapshot.bids.first(), snapshot.asks.first()) {
            (Some(best_bid), Some(best_ask)) => Some(best_ask.price.saturating_sub(best_bid.price)),
            _ => None,
        };
        let best_bid_depth = snapshot
            .bids
            .first()
            .map(|level| level.total_qty)
            .unwrap_or(0);
        let best_ask_depth = snapshot
            .asks
            .first()
            .map(|level| level.total_qty)
            .unwrap_or(0);
        let top3_bid_depth = snapshot
            .bids
            .iter()
            .take(3)
            .map(|level| level.total_qty)
            .sum();
        let top3_ask_depth = snapshot
            .asks
            .iter()
            .take(3)
            .map(|level| level.total_qty)
            .sum();
        let imbalance_bps = {
            let total = i128::from(top3_bid_depth) + i128::from(top3_ask_depth);
            if total == 0 {
                None
            } else {
                Some(
                    (((i128::from(top3_bid_depth) - i128::from(top3_ask_depth)) * 10_000) / total)
                        as i64,
                )
            }
        };

        BookMetrics {
            spread,
            best_bid_depth,
            best_ask_depth,
            top3_bid_depth,
            top3_ask_depth,
            bid_levels: snapshot.bids.len(),
            ask_levels: snapshot.asks.len(),
            thin_bid: snapshot.bids.is_empty() || best_bid_depth < THIN_BOOK_TOP_DEPTH_THRESHOLD,
            thin_ask: snapshot.asks.is_empty() || best_ask_depth < THIN_BOOK_TOP_DEPTH_THRESHOLD,
            imbalance_bps,
        }
    }

    pub fn slippage_stats(&self) -> &SlippageStats {
        &self.slippage_stats
    }

    pub fn current_ts(&self) -> u64 {
        self.current_ts
    }

    pub fn snapshot(&self) -> BookSnapshot {
        BookSnapshot {
            bids: aggregate_levels_desc(&self.bids, &self.orders),
            asks: aggregate_levels_asc(&self.asks, &self.orders),
            last_price: self.last_price,
        }
    }

    pub fn trades(&self) -> &[Trade] {
        &self.trades
    }

    fn cancel_immediate(&mut self, order_id: &str) -> CancelOutcome {
        let Some(order) = self.orders.get_mut(order_id) else {
            return CancelOutcome {
                cancelled: false,
                reject_reason: Some("NOT_FOUND"),
            };
        };

        if !order.active || !order.resting || order.remaining_qty == 0 {
            return CancelOutcome {
                cancelled: false,
                reject_reason: Some("NOT_CANCELLABLE"),
            };
        }

        let price = order.price.expect("resting order price");
        let side = order.side;
        order.active = false;
        order.resting = false;
        order.remaining_qty = 0;
        let _ = order;

        let book = self.book_for_side_mut(side);
        if let Some(level) = book.get_mut(&price) {
            level.retain(|existing| existing != order_id);
            if level.is_empty() {
                book.remove(&price);
            }
        }

        CancelOutcome {
            cancelled: true,
            reject_reason: None,
        }
    }

    fn is_cancellable(&self, order_id: &str) -> bool {
        self.orders
            .get(order_id)
            .map(|order| order.active && order.resting && order.remaining_qty > 0)
            .unwrap_or(false)
    }

    fn market_reference_price(&self, side: Side) -> Option<u64> {
        self.next_match_price(side)
    }

    fn record_snapshot(&mut self, ts: u64) {
        let snapshot = self.snapshot();
        self.snapshot_history.push((ts, snapshot));
    }

    fn record_slippage(&mut self, input: &OrderInput, reference_price: u64, trades: &[Trade]) {
        if trades.is_empty() {
            return;
        }

        let executed_qty: u64 = trades.iter().map(|trade| trade.qty).sum();
        if executed_qty == 0 {
            return;
        }

        let executed_notional: u128 = trades
            .iter()
            .map(|trade| u128::from(trade.price) * u128::from(trade.qty))
            .sum();
        let worst_price = match input.side {
            Side::Buy => trades
                .iter()
                .map(|trade| trade.price)
                .max()
                .unwrap_or(reference_price),
            Side::Sell => trades
                .iter()
                .map(|trade| trade.price)
                .min()
                .unwrap_or(reference_price),
        };

        let notional_slippage: i128 = trades
            .iter()
            .map(|trade| match input.side {
                Side::Buy => {
                    i128::from(trade.price.saturating_sub(reference_price)) * i128::from(trade.qty)
                }
                Side::Sell => {
                    i128::from(reference_price.saturating_sub(trade.price)) * i128::from(trade.qty)
                }
            })
            .sum();

        let event = SlippageEvent {
            order_id: input.order_id.clone(),
            side: input.side,
            reference_price,
            executed_qty,
            executed_notional,
            worst_price,
            notional_slippage,
        };

        let worst_slippage_ticks = match input.side {
            Side::Buy => worst_price.saturating_sub(reference_price),
            Side::Sell => reference_price.saturating_sub(worst_price),
        };

        self.slippage_stats.event_count += 1;
        self.slippage_stats.total_executed_qty += executed_qty;
        self.slippage_stats.total_notional_slippage += notional_slippage;
        self.slippage_stats.max_worst_slippage_ticks = self
            .slippage_stats
            .max_worst_slippage_ticks
            .max(worst_slippage_ticks);
        self.slippage_stats.last_event = Some(event);
    }
}

impl Side {
    fn opposite(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }
}

fn validate_input(
    input: &OrderInput,
    existing_orders: &HashMap<String, OrderState>,
    risk_limits: &RiskLimits,
    market_reference_price: Option<u64>,
) -> Result<(), &'static str> {
    match input.order_type {
        OrderType::Limit if input.price.is_none() => Err("LIMIT_PRICE_REQUIRED"),
        OrderType::Market if input.price.is_some() => Err("MARKET_PRICE_FORBIDDEN"),
        _ if input.qty == 0 => Err("INVALID_QTY"),
        _ if existing_orders.contains_key(&input.order_id) => Err("DUPLICATE_ORDER_ID"),
        _ if matches!(risk_limits.max_order_qty, Some(limit) if input.qty > limit) => {
            Err("RISK_MAX_ORDER_QTY")
        }
        _ if input.order_type == OrderType::Market
            && matches!(risk_limits.max_market_qty, Some(limit) if input.qty > limit) =>
        {
            Err("RISK_MAX_MARKET_QTY")
        }
        _ if matches!(risk_limits.max_notional, Some(limit) if estimated_notional(input, market_reference_price).is_some_and(|notional| notional > u128::from(limit))) => {
            Err("RISK_MAX_NOTIONAL")
        }
        _ if input.order_type == OrderType::Market
            && risk_limits.max_notional.is_some()
            && market_reference_price.is_none() =>
        {
            Err("RISK_NOTIONAL_REFERENCE_UNAVAILABLE")
        }
        _ => Ok(()),
    }
}

fn estimated_notional(input: &OrderInput, market_reference_price: Option<u64>) -> Option<u128> {
    match input.order_type {
        OrderType::Limit => input
            .price
            .map(|price| u128::from(price) * u128::from(input.qty)),
        OrderType::Market => {
            market_reference_price.map(|price| u128::from(price) * u128::from(input.qty))
        }
    }
}

fn aggregate_levels_desc(
    levels: &BTreeMap<u64, VecDeque<String>>,
    orders: &HashMap<String, OrderState>,
) -> Vec<BookLevel> {
    levels
        .iter()
        .rev()
        .filter_map(|(price, queue)| {
            let total_qty: u64 = queue
                .iter()
                .filter_map(|order_id| orders.get(order_id))
                .filter(|order| order.active && order.resting)
                .map(|order| order.remaining_qty)
                .sum();
            (total_qty > 0).then_some(BookLevel {
                price: *price,
                total_qty,
            })
        })
        .collect()
}

fn aggregate_levels_asc(
    levels: &BTreeMap<u64, VecDeque<String>>,
    orders: &HashMap<String, OrderState>,
) -> Vec<BookLevel> {
    levels
        .iter()
        .filter_map(|(price, queue)| {
            let total_qty: u64 = queue
                .iter()
                .filter_map(|order_id| orders.get(order_id))
                .filter(|order| order.active && order.resting)
                .map(|order| order.remaining_qty)
                .sum();
            (total_qty > 0).then_some(BookLevel {
                price: *price,
                total_qty,
            })
        })
        .collect()
}

impl MatchingEngine {
    fn can_cross(&self, incoming: &OrderState, resting_price: u64) -> bool {
        match incoming.side {
            Side::Buy => match incoming.price {
                Some(limit_price) => resting_price <= limit_price,
                None => true,
            },
            Side::Sell => match incoming.price {
                Some(limit_price) => resting_price >= limit_price,
                None => true,
            },
        }
    }

    fn next_match_price(&self, incoming_side: Side) -> Option<u64> {
        match incoming_side {
            Side::Buy => self.asks.keys().next().copied(),
            Side::Sell => self.bids.keys().next_back().copied(),
        }
    }

    fn front_resting_id(&self, incoming_side: Side, price: u64) -> Option<&String> {
        match incoming_side {
            Side::Buy => self.asks.get(&price)?.front(),
            Side::Sell => self.bids.get(&price)?.front(),
        }
    }

    fn pop_front_resting_id(&mut self, incoming_side: Side, price: u64, expected_id: &str) {
        let book = match incoming_side {
            Side::Buy => &mut self.asks,
            Side::Sell => &mut self.bids,
        };
        if let Some(level) = book.get_mut(&price) {
            let front = level.pop_front();
            debug_assert_eq!(front.as_deref(), Some(expected_id));
            if level.is_empty() {
                book.remove(&price);
            }
        }
    }

    fn remove_empty_level(&mut self, side: Side, price: u64) {
        let book = self.book_for_side_mut(side);
        if matches!(book.get(&price), Some(level) if level.is_empty()) {
            book.remove(&price);
        }
    }

    fn book_for_side_mut(&mut self, side: Side) -> &mut BTreeMap<u64, VecDeque<String>> {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CancelRequestOutcome, MatchingEngine, OrderInput, OrderType, Side};

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

    fn market(order_id: &str, side: Side, qty: u64, ts: u64) -> OrderInput {
        OrderInput {
            order_id: order_id.to_string(),
            side,
            order_type: OrderType::Market,
            price: None,
            qty,
            ts,
        }
    }

    #[test]
    fn case_1_limit_order_rests_on_book() {
        let mut engine = MatchingEngine::new();

        let outcome = engine.submit(limit("B1", Side::Buy, 99, 100, 1));

        assert!(outcome.accepted);
        assert!(outcome.trades.is_empty());
        let snapshot = engine.snapshot();
        assert_eq!(snapshot.last_price, None);
        assert_eq!(snapshot.bids.len(), 1);
        assert_eq!(snapshot.bids[0].price, 99);
        assert_eq!(snapshot.bids[0].total_qty, 100);
        assert!(snapshot.asks.is_empty());
    }

    #[test]
    fn case_2_market_buy_consumes_best_asks() {
        let mut engine = MatchingEngine::new();
        engine.submit(limit("S1", Side::Sell, 100, 50, 1));
        engine.submit(limit("S2", Side::Sell, 101, 80, 2));

        let outcome = engine.submit(market("B1", Side::Buy, 100, 3));

        assert!(outcome.accepted);
        assert_eq!(outcome.trades.len(), 2);
        assert_eq!(outcome.trades[0].price, 100);
        assert_eq!(outcome.trades[0].qty, 50);
        assert_eq!(outcome.trades[1].price, 101);
        assert_eq!(outcome.trades[1].qty, 50);

        let snapshot = engine.snapshot();
        assert_eq!(snapshot.last_price, Some(101));
        assert!(snapshot.bids.is_empty());
        assert_eq!(snapshot.asks.len(), 1);
        assert_eq!(snapshot.asks[0].price, 101);
        assert_eq!(snapshot.asks[0].total_qty, 30);
    }

    #[test]
    fn case_3_same_price_uses_fifo() {
        let mut engine = MatchingEngine::new();
        engine.submit(limit("A", Side::Buy, 99, 100, 1));
        engine.submit(limit("B", Side::Buy, 99, 100, 2));

        let outcome = engine.submit(market("S1", Side::Sell, 150, 3));

        assert!(outcome.accepted);
        assert_eq!(outcome.trades.len(), 2);
        assert_eq!(outcome.trades[0].buy_order_id, "A");
        assert_eq!(outcome.trades[0].qty, 100);
        assert_eq!(outcome.trades[1].buy_order_id, "B");
        assert_eq!(outcome.trades[1].qty, 50);

        let snapshot = engine.snapshot();
        assert_eq!(snapshot.bids.len(), 1);
        assert_eq!(snapshot.bids[0].price, 99);
        assert_eq!(snapshot.bids[0].total_qty, 50);
    }

    #[test]
    fn case_4_partial_fill_leaves_resting_qty() {
        let mut engine = MatchingEngine::new();
        engine.submit(limit("S1", Side::Sell, 100, 200, 1));

        let outcome = engine.submit(limit("B1", Side::Buy, 100, 50, 2));

        assert!(outcome.accepted);
        assert_eq!(outcome.trades.len(), 1);
        assert_eq!(outcome.trades[0].price, 100);
        assert_eq!(outcome.trades[0].qty, 50);

        let snapshot = engine.snapshot();
        assert_eq!(snapshot.asks.len(), 1);
        assert_eq!(snapshot.asks[0].price, 100);
        assert_eq!(snapshot.asks[0].total_qty, 150);
    }

    #[test]
    fn case_5_cancel_removes_resting_order() {
        let mut engine = MatchingEngine::new();
        engine.submit(limit("O1", Side::Buy, 99, 100, 1));

        let outcome = engine.cancel("O1");

        assert!(outcome.cancelled);
        let snapshot = engine.snapshot();
        assert!(snapshot.bids.is_empty());
        assert!(snapshot.asks.is_empty());
    }

    #[test]
    fn case_6_cancel_missing_order_is_rejected() {
        let mut engine = MatchingEngine::new();

        let outcome = engine.cancel("NO_SUCH_ORDER");

        assert!(!outcome.cancelled);
        assert_eq!(outcome.reject_reason, Some("NOT_FOUND"));
    }

    #[test]
    fn case_7_crossing_limit_order_trades_against_resting_book() {
        let mut engine = MatchingEngine::new();
        engine.submit(limit("S1", Side::Sell, 100, 100, 1));

        let outcome = engine.submit(limit("B1", Side::Buy, 101, 60, 2));

        assert!(outcome.accepted);
        assert_eq!(outcome.trades.len(), 1);
        assert_eq!(outcome.trades[0].price, 100);
        assert_eq!(outcome.trades[0].qty, 60);

        let snapshot = engine.snapshot();
        assert_eq!(snapshot.asks.len(), 1);
        assert_eq!(snapshot.asks[0].price, 100);
        assert_eq!(snapshot.asks[0].total_qty, 40);
    }

    #[test]
    fn rejects_invalid_order_shapes() {
        let mut engine = MatchingEngine::new();

        let missing_limit_price = engine.submit(OrderInput {
            order_id: "bad-limit".to_string(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: None,
            qty: 10,
            ts: 1,
        });
        assert_eq!(
            missing_limit_price.reject_reason,
            Some("LIMIT_PRICE_REQUIRED")
        );

        let market_with_price = engine.submit(OrderInput {
            order_id: "bad-market".to_string(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: Some(100),
            qty: 10,
            ts: 2,
        });
        assert_eq!(
            market_with_price.reject_reason,
            Some("MARKET_PRICE_FORBIDDEN")
        );

        let duplicate = engine.submit(limit("dup", Side::Buy, 99, 10, 3));
        assert!(duplicate.accepted);
        let duplicate_again = engine.submit(limit("dup", Side::Buy, 99, 10, 4));
        assert_eq!(duplicate_again.reject_reason, Some("DUPLICATE_ORDER_ID"));
    }

    #[test]
    fn market_residual_expires_without_resting() {
        let mut engine = MatchingEngine::new();
        engine.submit(limit("S1", Side::Sell, 100, 20, 1));

        let outcome = engine.submit(market("B1", Side::Buy, 50, 2));

        assert!(outcome.accepted);
        assert_eq!(outcome.trades.len(), 1);
        assert_eq!(outcome.trades[0].qty, 20);
        assert!(!outcome.resting);
        assert_eq!(outcome.remaining_qty, 30);
        let snapshot = engine.snapshot();
        assert!(snapshot.asks.is_empty());
        assert!(snapshot.bids.is_empty());
        assert_eq!(snapshot.last_price, Some(100));
    }

    #[test]
    fn filled_order_cannot_be_cancelled() {
        let mut engine = MatchingEngine::new();
        engine.submit(limit("B1", Side::Buy, 100, 40, 1));
        engine.submit(market("S1", Side::Sell, 40, 2));

        let outcome = engine.cancel("B1");

        assert!(!outcome.cancelled);
        assert_eq!(outcome.reject_reason, Some("NOT_CANCELLABLE"));
    }

    #[test]
    fn queue_position_reports_ahead_qty() {
        let mut engine = MatchingEngine::new();
        engine.submit(limit("A", Side::Buy, 99, 100, 1));
        engine.submit(limit("B", Side::Buy, 99, 50, 2));

        let position = engine.queue_position("B").expect("queue position");

        assert_eq!(position.position, 2);
        assert_eq!(position.ahead_qty, 100);
        assert_eq!(position.level_total_qty, 150);
    }

    #[test]
    fn market_data_delay_returns_older_snapshot() {
        let mut engine = MatchingEngine::new();
        engine.set_market_data_delay(2);
        engine.submit(limit("B1", Side::Buy, 99, 100, 1));
        engine.submit(limit("S1", Side::Sell, 101, 100, 3));

        let delayed = engine.delayed_snapshot_at(3);

        assert_eq!(delayed.bids.len(), 1);
        assert_eq!(delayed.bids[0].price, 99);
        assert!(delayed.asks.is_empty());
    }

    #[test]
    fn slippage_stats_capture_market_sweep() {
        let mut engine = MatchingEngine::new();
        engine.submit(limit("S1", Side::Sell, 100, 50, 1));
        engine.submit(limit("S2", Side::Sell, 101, 80, 2));

        let outcome = engine.submit(market("B1", Side::Buy, 100, 3));

        assert!(outcome.accepted);
        let stats = engine.slippage_stats();
        assert_eq!(stats.event_count, 1);
        assert_eq!(stats.total_executed_qty, 100);
        assert_eq!(stats.max_worst_slippage_ticks, 1);
        assert_eq!(
            stats
                .last_event
                .as_ref()
                .map(|event| event.order_id.as_str()),
            Some("B1")
        );
    }

    #[test]
    fn risk_limits_reject_large_order() {
        let mut engine = MatchingEngine::new();
        engine
            .set_risk_limit("MAX_ORDER_QTY", Some(10))
            .expect("set risk");

        let outcome = engine.submit(limit("B1", Side::Buy, 99, 11, 1));

        assert!(!outcome.accepted);
        assert_eq!(outcome.reject_reason, Some("RISK_MAX_ORDER_QTY"));
    }

    #[test]
    fn cancel_latency_defers_removal_until_advance() {
        let mut engine = MatchingEngine::new();
        engine.set_cancel_latency(3);
        engine.submit(limit("B1", Side::Buy, 99, 100, 1));
        engine.advance_to(2).expect("advance");

        let request = engine.request_cancel("B1");
        assert_eq!(
            request,
            CancelRequestOutcome::Pending {
                order_id: "B1".to_string(),
                apply_at_ts: 5
            }
        );
        assert!(engine.queue_position("B1").is_some());

        let events = engine.advance_to(5).expect("advance");

        assert_eq!(events.len(), 1);
        assert!(events[0].outcome.cancelled);
        assert!(engine.queue_position("B1").is_none());
    }

    #[test]
    fn renders_trade_status_cancel_and_snapshot_output() {
        let mut engine = MatchingEngine::new();
        let accepted = engine.submit(limit("B1", Side::Buy, 99, 100, 1));
        assert_eq!(accepted.status_line("B1"), "ACCEPT B1");

        engine.submit(limit("S1", Side::Sell, 100, 50, 2));
        engine.submit(limit("S2", Side::Sell, 101, 80, 3));
        let market_buy = engine.submit(market("B2", Side::Buy, 100, 4));
        assert_eq!(market_buy.trade_lines()[0], "TRADE B2 S1 100 50");
        assert_eq!(market_buy.trade_lines()[1], "TRADE B2 S2 101 50");

        let cancelled = engine.cancel("B1");
        assert_eq!(cancelled.status_line("B1"), "CANCELLED B1");
        let missing_cancel = engine.cancel("NO_SUCH");
        assert_eq!(
            missing_cancel.status_line("NO_SUCH"),
            "REJECT NO_SUCH NOT_FOUND"
        );

        let snapshot = engine.snapshot().render();
        assert!(snapshot.contains("BIDS"));
        assert!(snapshot.contains("ASKS"));
        assert!(snapshot.contains("101: 30"));
        assert!(snapshot.contains("LAST_PRICE: 101"));
    }
}
