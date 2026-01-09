//! monoio TCP サーバー
//!
//! Linux: io_uring, macOS: kqueue を使用。
//! tokio より低レベルな非同期I/Oでsyscallオーバーヘッドを削減。
//!
//! ## 起動方法
//! ```bash
//! cargo run --release --features iouring --bin gateway-iouring
//! ```

use std::env;

use monoio::io::{AsyncReadRentExt, AsyncWriteRentExt};
use monoio::net::{TcpListener, TcpStream};

// プロトコル定数
const REQUEST_SIZE: usize = 48;
const RESPONSE_SIZE: usize = 24;

mod status {
    pub const ACCEPTED: u8 = 0;
}

#[monoio::main]
async fn main() {
    let port: u16 = env::var("GATEWAY_TCP_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(9001);

    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).expect("Failed to bind");
    eprintln!("io_uring TCP server listening on {}", addr);

    loop {
        match listener.accept().await {
            Ok((stream, _peer)) => {
                monoio::spawn(handle_connection(stream));
            }
            Err(e) => {
                eprintln!("Accept error: {}", e);
            }
        }
    }
}

async fn handle_connection(mut stream: TcpStream) {
    stream.set_nodelay(true).ok();

    let mut req_buf = vec![0u8; REQUEST_SIZE];
    let mut resp_buf = vec![0u8; RESPONSE_SIZE];

    loop {
        // リクエスト読み取り (所有権ベースI/O)
        let (result, buf) = stream.read_exact(req_buf).await;
        req_buf = buf;

        match result {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(_) => break,
        }

        let start = std::time::Instant::now();

        // order_id を取得
        let order_id = u64::from_le_bytes(req_buf[0..8].try_into().unwrap());

        // シンプルな処理（フルエンジンなしでレイテンシ測定）
        let elapsed_ns = start.elapsed().as_nanos() as u64;

        // レスポンス構築
        resp_buf[0..8].copy_from_slice(&order_id.to_le_bytes());
        resp_buf[8] = status::ACCEPTED;
        resp_buf[9..16].fill(0);
        resp_buf[16..24].copy_from_slice(&elapsed_ns.to_le_bytes());

        // レスポンス送信 (所有権ベースI/O)
        let (result, buf) = stream.write_all(resp_buf).await;
        resp_buf = buf;

        if result.is_err() {
            break;
        }
    }
}
