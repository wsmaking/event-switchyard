#!/bin/bash
# Kotlin vs Rust FFI ベンチマーク実行スクリプト

set -e

cd "$(dirname "$0")/.."

echo "=== Rustライブラリをビルド ==="
export PATH="$HOME/.cargo/bin:$PATH"
(cd gateway-core && cargo build --release)

echo ""
echo "=== ベンチマーク実行 ==="
export GATEWAY_CORE_LIB_PATH="$(pwd)/gateway-core/target/release/libgateway_core.dylib"

./gradlew :gateway:test --tests "gateway.native.LatencyBenchmark" \
  -Dtest.single=LatencyBenchmark \
  --rerun \
  2>&1 | tee /tmp/benchmark_output.txt

echo ""
echo "=== 結果抜粋 ==="
grep -E "(===|---|\s+p50|\s+p99|改善|サンプル|Kotlin|Rust|平均|最大)" /tmp/benchmark_output.txt || true
