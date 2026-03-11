//! Exchange 送信ワーカー
//!
//! キューから注文を取り出し、Exchange に送信する。

use chrono::DateTime;
use serde_json::json;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tracing::{error, info, warn};

use gateway_core::Order;

use crate::audit::{self, AuditEvent, AuditLog};
use crate::bus::{BusEvent, BusPublisher, format_event_time};
use crate::exchange::{
    ExchangeClient, ExecutionReport, OrderSide, OrderStatus as ExchangeOrderStatus,
};
use crate::sse::SseHub;
use crate::store::{ExecReport, OrderIdMap, OrderStatus as StoreOrderStatus, ShardedOrderStore};

#[derive(Clone)]
pub struct ExecutionReportContext {
    sharded_store: Arc<ShardedOrderStore>,
    order_id_map: Arc<OrderIdMap>,
    sse_hub: Arc<SseHub>,
    audit_log: Arc<AuditLog>,
    bus_publisher: Arc<BusPublisher>,
    bus_mode_outbox: bool,
}

impl ExecutionReportContext {
    pub fn new(
        sharded_store: Arc<ShardedOrderStore>,
        order_id_map: Arc<OrderIdMap>,
        sse_hub: Arc<SseHub>,
        audit_log: Arc<AuditLog>,
        bus_publisher: Arc<BusPublisher>,
        bus_mode_outbox: bool,
    ) -> Self {
        Self {
            sharded_store,
            order_id_map,
            sse_hub,
            audit_log,
            bus_publisher,
            bus_mode_outbox,
        }
    }
}

/// キューから注文を取り出して Exchange に送信
pub fn start_worker(
    queue: Arc<gateway_core::FastPathQueue>,
    exchange_host: String,
    exchange_port: u16,
    report_ctx: ExecutionReportContext,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        // Exchange に接続
        let client = match ExchangeClient::connect(&exchange_host, exchange_port) {
            Ok(c) => Arc::new(c),
            Err(e) => {
                error!("failed to connect to exchange: {}", e);
                return;
            }
        };
        info!(
            "connected to exchange at {}:{}",
            exchange_host, exchange_port
        );

        loop {
            // キューから注文を取り出す
            if let Some(order) = queue.pop() {
                if let Err(e) = send_order(&client, &order, report_ctx.clone()) {
                    error!("failed to send order {}: {}", order.order_id, e);
                }
            } else {
                // キューが空なら少し待つ
                thread::sleep(Duration::from_micros(100));
            }
        }
    })
}

fn send_order(
    client: &ExchangeClient,
    order: &Order,
    report_ctx: ExecutionReportContext,
) -> std::io::Result<()> {
    let order_id = order.order_id.to_string();
    let symbol = String::from_utf8_lossy(&order.symbol)
        .trim_end_matches('\0')
        .to_string();
    let side = if order.side == 1 {
        OrderSide::Buy
    } else {
        OrderSide::Sell
    };

    client.send_new_order(
        &order_id,
        &symbol,
        side,
        order.qty as i64,
        order.price as i64,
        Box::new(move |report: ExecutionReport| apply_execution_report(report, &report_ctx)),
    )
}

fn map_exchange_status(status: ExchangeOrderStatus) -> StoreOrderStatus {
    match status {
        // Exchange の PENDING は送信中相当として扱う。
        ExchangeOrderStatus::Pending => StoreOrderStatus::Sent,
        ExchangeOrderStatus::Accepted => StoreOrderStatus::Accepted,
        ExchangeOrderStatus::Sent => StoreOrderStatus::Sent,
        ExchangeOrderStatus::PartiallyFilled => StoreOrderStatus::PartiallyFilled,
        ExchangeOrderStatus::Filled => StoreOrderStatus::Filled,
        ExchangeOrderStatus::Canceled => StoreOrderStatus::Canceled,
        ExchangeOrderStatus::Rejected => StoreOrderStatus::Rejected,
    }
}

fn as_non_negative_u64(value: i64) -> u64 {
    if value <= 0 { 0 } else { value as u64 }
}

fn parse_report_at_ms(raw: &str) -> u64 {
    DateTime::parse_from_rfc3339(raw)
        .ok()
        .and_then(|dt| {
            let ms = dt.timestamp_millis();
            if ms < 0 { None } else { Some(ms as u64) }
        })
        .unwrap_or_else(audit::now_millis)
}

fn publish_bus_if_direct(ctx: &ExecutionReportContext, event: BusEvent) {
    if !ctx.bus_mode_outbox {
        ctx.bus_publisher.publish(event);
    }
}

fn apply_execution_report(report: ExecutionReport, ctx: &ExecutionReportContext) {
    let internal_order_id = match report.order_id.parse::<u64>() {
        Ok(id) => id,
        Err(_) => {
            warn!(order_id = %report.order_id, "execution report with non-internal order id");
            return;
        }
    };
    let external_order_id = match ctx.order_id_map.to_external(internal_order_id) {
        Some(v) => v,
        None => {
            warn!(
                internal_order_id,
                "missing order id mapping for execution report"
            );
            return;
        }
    };
    let account_id = match ctx.order_id_map.get_account_id(internal_order_id) {
        Some(v) => v,
        None => {
            warn!(
                internal_order_id,
                "missing account mapping for execution report"
            );
            return;
        }
    };

    if ctx
        .sharded_store
        .find_by_id_with_account(&external_order_id, &account_id)
        .is_none()
    {
        warn!(
            order_id = %external_order_id,
            account_id = %account_id,
            "execution report dropped: order snapshot missing"
        );
        return;
    }

    let report_at_ms = parse_report_at_ms(&report.at);
    let normalized_status = map_exchange_status(report.status);
    let filled_qty_delta = as_non_negative_u64(report.filled_qty_delta);
    let filled_qty_total = as_non_negative_u64(report.filled_qty_total);

    let execution_report_data = json!({
        "status": normalized_status.as_str(),
        "filledQtyDelta": filled_qty_delta,
        "filledQtyTotal": filled_qty_total,
        "price": report.price,
    });

    ctx.audit_log.append(AuditEvent {
        event_type: "ExecutionReport".into(),
        at: report_at_ms,
        account_id: account_id.clone(),
        order_id: Some(external_order_id.clone()),
        data: execution_report_data.clone(),
    });
    publish_bus_if_direct(
        ctx,
        BusEvent {
            event_type: "ExecutionReport".into(),
            at: format_event_time(report_at_ms),
            account_id: account_id.clone(),
            order_id: Some(external_order_id.clone()),
            data: execution_report_data,
        },
    );

    let execution_report_sse = json!({
        "orderId": external_order_id.clone(),
        "status": normalized_status.as_str(),
        "filledQtyDelta": filled_qty_delta,
        "filledQtyTotal": filled_qty_total,
        "price": report.price,
        "at": report.at,
    })
    .to_string();
    ctx.sse_hub.publish_order(
        &external_order_id,
        "execution_report",
        &execution_report_sse,
    );
    ctx.sse_hub
        .publish_account(&account_id, "execution_report", &execution_report_sse);

    let exec_report = ExecReport {
        order_id: external_order_id.clone(),
        status: normalized_status,
        filled_qty_delta,
        filled_qty_total,
        at: report_at_ms,
    };
    let updated = ctx
        .sharded_store
        .apply_execution_report(&exec_report, &account_id);

    if let Some(snapshot) = updated {
        let updated_data = json!({
            "status": snapshot.status.as_str(),
            "filledQty": snapshot.filled_qty,
        });
        ctx.audit_log.append(AuditEvent {
            event_type: "OrderUpdated".into(),
            at: report_at_ms,
            account_id: account_id.clone(),
            order_id: Some(snapshot.order_id.clone()),
            data: updated_data.clone(),
        });
        publish_bus_if_direct(
            ctx,
            BusEvent {
                event_type: "OrderUpdated".into(),
                at: format_event_time(report_at_ms),
                account_id: account_id.clone(),
                order_id: Some(snapshot.order_id.clone()),
                data: updated_data,
            },
        );
        let snapshot_sse = json!({
            "orderId": snapshot.order_id,
            "accountId": snapshot.account_id,
            "status": snapshot.status.as_str(),
            "filledQty": snapshot.filled_qty,
            "lastUpdateAt": snapshot.last_update_at,
        })
        .to_string();
        ctx.sse_hub
            .publish_order(&exec_report.order_id, "order_update", &snapshot_sse);
        ctx.sse_hub
            .publish_account(&account_id, "order_update", &snapshot_sse);
    } else {
        warn!(
            order_id = %exec_report.order_id,
            account_id = %account_id,
            "execution report applied no update"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::BusPublisher;
    use crate::order::{OrderType, TimeInForce};
    use crate::store::OrderSnapshot;
    use gateway_core::now_nanos;

    #[test]
    fn map_exchange_pending_to_sent() {
        assert_eq!(
            map_exchange_status(ExchangeOrderStatus::Pending),
            StoreOrderStatus::Sent
        );
    }

    #[tokio::test]
    async fn execution_report_updates_store_and_fanout() {
        let wal_path =
            std::env::temp_dir().join(format!("gateway-rust-exec-report-test-{}.log", now_nanos()));
        let ctx = ExecutionReportContext::new(
            Arc::new(ShardedOrderStore::new_with_ttl_and_shards(86_400_000, 8)),
            Arc::new(OrderIdMap::new()),
            Arc::new(SseHub::new()),
            Arc::new(AuditLog::new(wal_path).expect("create audit log")),
            Arc::new(BusPublisher::disabled_for_test()),
            false,
        );

        let mut order_rx = ctx.sse_hub.subscribe_order("ord_exec_1");
        let mut account_rx = ctx.sse_hub.subscribe_account("acc_exec_1");

        let order = OrderSnapshot::new(
            "ord_exec_1".into(),
            "acc_exec_1".into(),
            "AAPL".into(),
            "BUY".into(),
            OrderType::Limit,
            100,
            Some(15000),
            TimeInForce::Gtc,
            None,
            None,
        );
        ctx.sharded_store.put(order, None);
        ctx.order_id_map
            .register_with_internal(7, "ord_exec_1".into(), "acc_exec_1".into());

        apply_execution_report(
            ExecutionReport {
                order_id: "7".into(),
                status: ExchangeOrderStatus::PartiallyFilled,
                filled_qty_delta: 30,
                filled_qty_total: 30,
                price: Some(15000),
                at: "2025-01-01T00:00:00Z".into(),
            },
            &ctx,
        );

        let updated = ctx
            .sharded_store
            .find_by_id_with_account("ord_exec_1", "acc_exec_1")
            .expect("updated snapshot");
        assert_eq!(updated.status, StoreOrderStatus::PartiallyFilled);
        assert_eq!(updated.filled_qty, 30);

        let order_e1 = tokio::time::timeout(Duration::from_secs(1), order_rx.recv())
            .await
            .expect("order execution report timeout")
            .expect("order execution report recv");
        let order_e2 = tokio::time::timeout(Duration::from_secs(1), order_rx.recv())
            .await
            .expect("order update timeout")
            .expect("order update recv");
        assert_eq!(order_e1.event_type, "execution_report");
        assert_eq!(order_e2.event_type, "order_update");

        let account_e1 = tokio::time::timeout(Duration::from_secs(1), account_rx.recv())
            .await
            .expect("account execution report timeout")
            .expect("account execution report recv");
        let account_e2 = tokio::time::timeout(Duration::from_secs(1), account_rx.recv())
            .await
            .expect("account update timeout")
            .expect("account update recv");
        assert_eq!(account_e1.event_type, "execution_report");
        assert_eq!(account_e2.event_type, "order_update");

        let mut seen_execution_report = false;
        let mut seen_order_updated = false;
        for _ in 0..50 {
            let events =
                ctx.audit_log
                    .read_events("acc_exec_1", Some("ord_exec_1"), 100, None, None);
            seen_execution_report = events.iter().any(|e| e.event_type == "ExecutionReport");
            seen_order_updated = events.iter().any(|e| e.event_type == "OrderUpdated");
            if seen_execution_report && seen_order_updated {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(seen_execution_report);
        assert!(seen_order_updated);

        let bus_metrics = ctx.bus_publisher.metrics();
        assert!(bus_metrics.publish_dropped >= 2);
    }
}
