//! Exchange 送信ワーカー
//!
//! キューから注文を取り出し、Exchange に送信する。

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use gateway_core::Order;

use crate::exchange::{ExchangeClient, ExecutionReport, OrderSide};

/// キューから注文を取り出して Exchange に送信
pub fn start_worker(
    queue: Arc<gateway_core::FastPathQueue>,
    exchange_host: String,
    exchange_port: u16,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        // Exchange に接続
        let client = match ExchangeClient::connect(&exchange_host, exchange_port) {
            Ok(c) => Arc::new(c),
            Err(e) => {
                eprintln!("Failed to connect to exchange: {}", e);
                return;
            }
        };
        eprintln!(
            "Connected to exchange at {}:{}",
            exchange_host, exchange_port
        );

        loop {
            // キューから注文を取り出す
            if let Some(order) = queue.pop() {
                if let Err(e) = send_order(&client, &order) {
                    eprintln!("Failed to send order {}: {}", order.order_id, e);
                }
            } else {
                // キューが空なら少し待つ
                thread::sleep(Duration::from_micros(100));
            }
        }
    })
}

fn send_order(client: &ExchangeClient, order: &Order) -> std::io::Result<()> {
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
        Box::new(move |report: ExecutionReport| {
            // Execution Report 受信時のコールバック
            eprintln!(
                "ExecutionReport: order_id={}, status={:?}, filled={}",
                report.order_id, report.status, report.filled_qty_total
            );
        }),
    )
}
