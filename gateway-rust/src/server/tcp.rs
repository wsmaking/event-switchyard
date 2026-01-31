//! TCP サーバー
//!
//! バイナリプロトコルで最小レイテンシを実現。
//! HTTP層のオーバーヘッドを排除。

use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::engine::{FastPathEngine, ProcessResult};
use crate::protocol::{self, REQUEST_SIZE, RESPONSE_SIZE};

/// TCPサーバーを起動
pub async fn run(port: u16, engine: FastPathEngine) -> anyhow::Result<()> {
    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).await?;
    info!("TCP server listening on {}", addr);

    let engine = Arc::new(engine);

    loop {
        let (socket, peer) = listener.accept().await?;
        let engine = Arc::clone(&engine);

        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, engine).await {
                warn!("Connection error from {}: {}", peer, e);
            }
        });
    }
}

/// 接続を処理
async fn handle_connection(
    mut socket: tokio::net::TcpStream,
    engine: Arc<FastPathEngine>,
) -> anyhow::Result<()> {
    // TCP_NODELAY を設定（Nagleアルゴリズム無効化）
    socket.set_nodelay(true)?;

    let mut req_buf = [0u8; REQUEST_SIZE];
    let mut resp_buf = [0u8; RESPONSE_SIZE];

    loop {
        // リクエスト読み取り
        match socket.read_exact(&mut req_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // クライアントが切断
                break;
            }
            Err(e) => return Err(e.into()),
        }

        let start = gateway_core::now_nanos();

        // リクエストパース
        let (order_id, account_id, symbol, side, qty, price, _timestamp) =
            protocol::parse_request(&req_buf);

        // 注文処理
        let result = engine.process_order(order_id, account_id, symbol, side, qty, price);

        let elapsed = gateway_core::now_nanos() - start;

        // ステータスコード変換
        let status = match result {
            ProcessResult::Accepted => protocol::status::ACCEPTED,
            ProcessResult::RejectedMaxQty => protocol::status::REJECTED_MAX_QTY,
            ProcessResult::RejectedMaxNotional => protocol::status::REJECTED_MAX_NOTIONAL,
            ProcessResult::RejectedDailyLimit => protocol::status::REJECTED_DAILY_LIMIT,
            ProcessResult::RejectedUnknownSymbol => protocol::status::REJECTED_UNKNOWN_SYMBOL,
            ProcessResult::ErrorQueueFull => protocol::status::ERROR_QUEUE_FULL,
        };

        // レスポンス構築・送信
        protocol::build_response(&mut resp_buf, order_id, status, elapsed);
        socket.write_all(&resp_buf).await?;
    }

    Ok(())
}
