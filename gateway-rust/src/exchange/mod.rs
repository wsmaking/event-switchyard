//! Exchange クライアント
//!
//! Kotlin Gateway と同じ JSONL プロトコルで取引所と通信。

mod client;
mod protocol;

pub use client::ExchangeClient;
pub use protocol::{ExecutionReport, OrderSide, OrderStatus, TcpExchangeRequest, TcpExchangeRequestType};
