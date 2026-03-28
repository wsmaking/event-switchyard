//! Exchange クライアント
//!
//! Kotlin Gateway と同じ JSONL プロトコルで取引所と通信。

mod client;
mod control;
mod protocol;

pub use client::ExchangeClient;
pub use control::{TcpVenueOrderControl, VenueOrderControl};
pub use protocol::{ExecutionReport, OrderSide, OrderStatus};
