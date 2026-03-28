use std::io;
use std::sync::Mutex;

use super::client::{ExchangeClient, ReportCallback};
use super::protocol::OrderSide;

pub trait VenueOrderControl: Send + Sync {
    fn send_new_order(
        &self,
        order_id: &str,
        symbol: &str,
        side: OrderSide,
        qty: i64,
        price: i64,
        on_report: ReportCallback,
    ) -> io::Result<()>;

    fn send_cancel(&self, order_id: &str, on_report: ReportCallback) -> io::Result<()>;
}

pub struct TcpVenueOrderControl {
    client: Mutex<ExchangeClient>,
}

impl TcpVenueOrderControl {
    pub fn connect(host: &str, port: u16) -> io::Result<Self> {
        Ok(Self {
            client: Mutex::new(ExchangeClient::connect(host, port)?),
        })
    }
}

impl VenueOrderControl for TcpVenueOrderControl {
    fn send_new_order(
        &self,
        order_id: &str,
        symbol: &str,
        side: OrderSide,
        qty: i64,
        price: i64,
        on_report: ReportCallback,
    ) -> io::Result<()> {
        self.client
            .lock()
            .unwrap()
            .send_new_order(order_id, symbol, side, qty, price, on_report)
    }

    fn send_cancel(&self, order_id: &str, on_report: ReportCallback) -> io::Result<()> {
        self.client.lock().unwrap().send_cancel(order_id, on_report)
    }
}
