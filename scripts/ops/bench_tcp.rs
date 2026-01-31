//! TCPベンチマーククライアント
//!
//! 使用方法:
//! ```bash
//! rustc -O scripts/ops/bench_tcp.rs -o /tmp/bench_tcp
//! /tmp/bench_tcp localhost 9001 10000
//! ```

use std::convert::TryInto;
use std::env;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Instant;

const REQUEST_SIZE: usize = 48;
const RESPONSE_SIZE: usize = 24;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 4 {
        eprintln!("Usage: {} <host> <port> <requests>", args[0]);
        std::process::exit(1);
    }

    let host = &args[1];
    let port: u16 = args[2].parse().expect("Invalid port");
    let requests: usize = args[3].parse().expect("Invalid requests");
    let warmup = requests / 10;

    println!("=== TCP Benchmark ===");
    println!("Target: {}:{}", host, port);
    println!("Requests: {} (warmup: {})", requests, warmup);
    println!();

    // 接続
    let addr = format!("{}:{}", host, port);
    let mut stream = TcpStream::connect(&addr).expect("Failed to connect");
    stream.set_nodelay(true).expect("Failed to set TCP_NODELAY");

    let mut req_buf = [0u8; REQUEST_SIZE];
    let mut resp_buf = [0u8; RESPONSE_SIZE];
    let mut latencies = Vec::with_capacity(requests);

    // ウォームアップ
    print!("Warming up...");
    for i in 0..warmup {
        build_request(&mut req_buf, i as u64);
        stream.write_all(&req_buf).expect("Write failed");
        stream.read_exact(&mut resp_buf).expect("Read failed");
    }
    println!(" done");

    // ベンチマーク
    print!("Running benchmark...");
    for i in 0..requests {
        build_request(&mut req_buf, (warmup + i) as u64);

        let start = Instant::now();
        stream.write_all(&req_buf).expect("Write failed");
        stream.read_exact(&mut resp_buf).expect("Read failed");
        let elapsed = start.elapsed().as_nanos() as u64;

        latencies.push(elapsed);

        if (i + 1) % (requests / 10) == 0 {
            print!(".");
            std::io::stdout().flush().unwrap();
        }
    }
    println!(" done");
    println!();

    // 統計計算
    latencies.sort();
    let count = latencies.len();
    let p50 = latencies[count * 50 / 100];
    let p90 = latencies[count * 90 / 100];
    let p99 = latencies[count * 99 / 100];
    let p999 = latencies[count * 999 / 1000];
    let min = latencies[0];
    let max = latencies[count - 1];
    let sum: u64 = latencies.iter().sum();
    let avg = sum / count as u64;

    println!("=== Results ===");
    println!("Count: {}", count);
    println!("Min:   {} ns", min);
    println!("Avg:   {} ns", avg);
    println!("P50:   {} ns", p50);
    println!("P90:   {} ns", p90);
    println!("P99:   {} ns", p99);
    println!("P99.9: {} ns", p999);
    println!("Max:   {} ns", max);
    println!();

    // サーバー側レイテンシ（最後のレスポンスから）
    let server_latency = u64::from_le_bytes(resp_buf[16..24].try_into().unwrap());
    println!("Server-side latency (last): {} ns", server_latency);
}

fn build_request(buf: &mut [u8; REQUEST_SIZE], order_id: u64) {
    buf.fill(0);
    buf[0..8].copy_from_slice(&order_id.to_le_bytes()); // order_id
    buf[8..16].copy_from_slice(&1u64.to_le_bytes()); // account_id
    buf[16..24].copy_from_slice(b"AAPL\0\0\0\0"); // symbol
    buf[24] = 1; // side (BUY)
    buf[28..32].copy_from_slice(&100u32.to_le_bytes()); // qty
    buf[32..40].copy_from_slice(&15000u64.to_le_bytes()); // price
    buf[40..48].copy_from_slice(&0u64.to_le_bytes()); // timestamp
}
