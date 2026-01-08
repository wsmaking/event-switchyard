//! Criterion benchmarks for fast path latency
//!
//! Run with: cargo bench
//! Results saved to target/criterion/

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use gateway_core::{
    now_nanos, process_order, FastPathQueue, LatencyHistogram, Order, RiskChecker, RiskResult,
};

fn bench_queue_push_pop(c: &mut Criterion) {
    let queue = FastPathQueue::new(65536);
    let order = Order::new(1, 100, *b"AAPL\0\0\0\0", 1, 100, 15000, 0);

    c.bench_function("queue_push_pop", |b| {
        b.iter(|| {
            queue.push(black_box(order)).unwrap();
            black_box(queue.pop().unwrap())
        })
    });
}

fn bench_risk_check(c: &mut Criterion) {
    let checker = RiskChecker::new();
    let order = Order::new(1, 100, *b"AAPL\0\0\0\0", 1, 100, 15000, 0);

    c.bench_function("risk_check_simple", |b| {
        b.iter(|| black_box(checker.check_simple(black_box(&order))))
    });
}

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

fn bench_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput");

    for batch_size in [100, 1000, 10000] {
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

fn bench_now_nanos(c: &mut Criterion) {
    c.bench_function("now_nanos", |b| b.iter(|| black_box(now_nanos())));
}

fn bench_histogram_record(c: &mut Criterion) {
    let histogram = LatencyHistogram::new();

    c.bench_function("histogram_record", |b| {
        b.iter(|| {
            histogram.record(black_box(500));
        })
    });
}

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
