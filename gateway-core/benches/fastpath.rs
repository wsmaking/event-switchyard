//! Criterion ベンチマーク
//!
//! ## 実行方法
//! ```bash
//! cargo bench
//! ```
//!
//! ## 結果の見方
//! - time: 1回あたりの処理時間（小さいほど良い）
//! - thrpt: スループット（大きいほど良い）
//!
//! ## 結果の保存先
//! target/criterion/ にHTMLレポートが生成される

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use gateway_core::{
    now_nanos, process_order, FastPathQueue, LatencyHistogram, Order, RiskChecker, RiskResult,
};

/// キュー操作のベンチマーク
///
/// 計測内容: push 1回 + pop 1回
/// 期待値: ~30ns（ロックフリーなので非常に高速）
fn bench_queue_push_pop(c: &mut Criterion) {
    let queue = FastPathQueue::new(65536);
    let order = Order::new(1, 100, *b"AAPL\0\0\0\0", 1, 100, 15000, 0);

    c.bench_function("queue_push_pop", |b| {
        b.iter(|| {
            // black_box: コンパイラ最適化で消されないようにする
            queue.push(black_box(order)).unwrap();
            black_box(queue.pop().unwrap())
        })
    });
}

/// リスクチェック単体のベンチマーク
///
/// 計測内容: check_simple 1回（口座チェックなし）
/// 期待値: ~6ns（単純な比較演算のみ）
fn bench_risk_check(c: &mut Criterion) {
    let checker = RiskChecker::new();
    let order = Order::new(1, 100, *b"AAPL\0\0\0\0", 1, 100, 15000, 0);

    c.bench_function("risk_check_simple", |b| {
        b.iter(|| black_box(checker.check_simple(black_box(&order))))
    });
}

/// フルパスのベンチマーク
///
/// 計測内容: 注文作成 → push → pop → リスクチェック → 計測記録
/// 期待値: ~110ns（実際のホットパス全体）
fn bench_full_fast_path(c: &mut Criterion) {
    let queue = FastPathQueue::new(65536);
    let checker = RiskChecker::new();
    let histogram = LatencyHistogram::new();

    c.bench_function("fast_path_full", |b| {
        b.iter(|| {
            let order = Order::new(1, 100, *b"AAPL\0\0\0\0", 1, 100, 15000, now_nanos());
            queue.push(order).unwrap();
            let dequeued = queue.pop().unwrap();
            black_box(process_order(black_box(&dequeued), &checker, &histogram))
        })
    });
}

/// スループット測定
///
/// バッチサイズ別に ops/sec を計測
/// 期待値: ~9M ops/sec
fn bench_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput");

    for batch_size in [100, 1000, 10000] {
        // Throughput::Elements でバッチサイズを指定
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_function(format!("batch_{}", batch_size), |b| {
            let queue = FastPathQueue::new(65536);
            let checker = RiskChecker::new();
            let histogram = LatencyHistogram::new();

            b.iter(|| {
                for i in 0..batch_size {
                    let order =
                        Order::new(i as u64, 100, *b"AAPL\0\0\0\0", 1, 100, 15000, now_nanos());
                    queue.push(order).unwrap();
                    let dequeued = queue.pop().unwrap();
                    let result = process_order(&dequeued, &checker, &histogram);
                    assert_eq!(result, RiskResult::Accepted);
                }
            })
        });
    }

    group.finish();
}

/// Order 構造体作成のベンチマーク
///
/// 計測内容: Order::new 1回
/// 期待値: ~40ns（メモリ初期化コスト）
fn bench_order_creation(c: &mut Criterion) {
    c.bench_function("order_new", |b| {
        b.iter(|| {
            black_box(Order::new(
                black_box(1),
                black_box(100),
                black_box(*b"AAPL\0\0\0\0"),
                black_box(1),
                black_box(100),
                black_box(15000),
                black_box(now_nanos()),
            ))
        })
    });
}

/// 時刻取得のベンチマーク
///
/// 計測内容: now_nanos 1回
/// 期待値: ~30ns（システムコールではなくユーザー空間で完結）
fn bench_now_nanos(c: &mut Criterion) {
    c.bench_function("now_nanos", |b| b.iter(|| black_box(now_nanos())));
}

/// ヒストグラム記録のベンチマーク
///
/// 計測内容: histogram.record 1回
/// 期待値: ~3ns（atomic 操作のみ）
fn bench_histogram_record(c: &mut Criterion) {
    let histogram = LatencyHistogram::new();

    c.bench_function("histogram_record", |b| {
        b.iter(|| {
            histogram.record(black_box(500));
        })
    });
}

// ベンチマーク群を登録
criterion_group!(
    benches,
    bench_queue_push_pop,
    bench_risk_check,
    bench_full_fast_path,
    bench_throughput,
    bench_order_creation,
    bench_now_nanos,
    bench_histogram_record,
);

criterion_main!(benches);
