//! Exchange TCP クライアント
//!
//! JSONL プロトコルで取引所と通信。

use std::collections::HashMap;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::thread;

use super::protocol::{ExecutionReport, OrderSide, TcpExchangeRequest, TcpExchangeRequestType};

pub type ReportCallback = Box<dyn Fn(ExecutionReport) + Send + Sync>;

/// Exchange TCP クライアント
pub struct ExchangeClient {
    writer: Arc<Mutex<BufWriter<TcpStream>>>,
    callbacks: Arc<Mutex<HashMap<String, ReportCallback>>>,
    _reader_thread: thread::JoinHandle<()>,
}

impl ExchangeClient {
    /// 接続
    pub fn connect(host: &str, port: u16) -> std::io::Result<Self> {
        let stream = TcpStream::connect((host, port))?;
        stream.set_nodelay(true)?;

        let read_stream = stream.try_clone()?;
        let writer = Arc::new(Mutex::new(BufWriter::new(stream)));
        let callbacks: Arc<Mutex<HashMap<String, ReportCallback>>> = Arc::new(Mutex::new(HashMap::new()));

        let callbacks_clone = Arc::clone(&callbacks);
        let reader_thread = thread::spawn(move || {
            Self::read_loop(read_stream, callbacks_clone);
        });

        Ok(Self {
            writer,
            callbacks,
            _reader_thread: reader_thread,
        })
    }

    /// 新規注文送信
    pub fn send_new_order(
        &self,
        order_id: &str,
        symbol: &str,
        side: OrderSide,
        qty: i64,
        price: i64,
        on_report: ReportCallback,
    ) -> std::io::Result<()> {
        {
            let mut cbs = self.callbacks.lock().unwrap();
            cbs.insert(order_id.to_string(), on_report);
        }

        let request = TcpExchangeRequest {
            request_type: TcpExchangeRequestType::New,
            order_id: order_id.to_string(),
            symbol: Some(symbol.to_string()),
            side: Some(side),
            qty: Some(qty),
            price: Some(price),
        };

        self.send(&request)
    }

    /// キャンセル送信
    pub fn send_cancel(&self, order_id: &str, on_report: ReportCallback) -> std::io::Result<()> {
        {
            let mut cbs = self.callbacks.lock().unwrap();
            cbs.insert(order_id.to_string(), on_report);
        }

        let request = TcpExchangeRequest {
            request_type: TcpExchangeRequestType::Cancel,
            order_id: order_id.to_string(),
            symbol: None,
            side: None,
            qty: None,
            price: None,
        };

        self.send(&request)
    }

    fn send(&self, request: &TcpExchangeRequest) -> std::io::Result<()> {
        let line = serde_json::to_string(request)?;
        let mut writer = self.writer.lock().unwrap();
        writeln!(writer, "{}", line)?;
        writer.flush()?;
        Ok(())
    }

    fn read_loop(stream: TcpStream, callbacks: Arc<Mutex<HashMap<String, ReportCallback>>>) {
        let reader = BufReader::new(stream);
        for line in reader.lines() {
            let line = match line {
                Ok(l) => l,
                Err(_) => break,
            };

            let report: ExecutionReport = match serde_json::from_str(&line) {
                Ok(r) => r,
                Err(_) => continue,
            };

            let callback = {
                let cbs = callbacks.lock().unwrap();
                cbs.get(&report.order_id).map(|c| c as *const _)
            };

            if let Some(callback_ptr) = callback {
                // Safe: callback is still in the map
                let callback: &ReportCallback = unsafe { &*callback_ptr };
                callback(report.clone());

                if report.status.is_terminal() {
                    let mut cbs = callbacks.lock().unwrap();
                    cbs.remove(&report.order_id);
                }
            }
        }
    }
}
