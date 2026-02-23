.PHONY: help run run-gateway run-backoffice dev-bench metrics stats health dashboard grafana grafana-up grafana-down clean stop build test all bench gate check check-lite check-full bless compose-gateway compose-gateway-down backoffice-recovery gateway-backoffice-e2e perf-gate-rust perf-gate-rust-full preset-switch-gate pure-hft-absolute-gate pure-hft-absolute-gate-loop fdatasync-ab fdatasync-adaptive-sweep adaptive-profile-search rust-check rust-test rust-run gateway-run gateway-check gateway-test backoffice-run backoffice-check backoffice-test app-run app-check app-test

# デフォルトターゲット: ヘルプ表示
.DEFAULT_GOAL := help

# 既定値（make 時に上書き可能： make bench RUNS=5 など）
OUT  ?= var/results/pr.json
MODE ?= quick
RUNS ?= 1
CASE ?= match_engine_hot

help:
	@echo "========================================="
	@echo "  HFT Fast Path 開発コマンド"
	@echo "========================================="
	@echo ""
	@echo "基本コマンド:"
	@echo "  make run         - 旧アプリケーション起動 (/events)"
	@echo "  make run-gateway - 新Gateway起動 (/orders)"
	@echo "  make run-backoffice - BackOffice(consumer)起動 (/positions)"
	@echo "  make backoffice-recovery - BackOffice復旧チェック"
	@echo "  make gateway-backoffice-e2e - Gateway→BackOfficeの疎通確認"
	@echo "  make dev-bench   - 開発用ベンチマーク (/api/orders 10,000 runs)"
	@echo "  make dashboard   - リアルタイムダッシュボード表示"
	@echo ""
	@echo "メトリクス確認:"
	@echo "  make metrics     - Prometheusメトリクス表示"
	@echo "  make stats       - 統計情報表示 (JSON)"
	@echo "  make health      - ヘルスチェック"
	@echo ""
	@echo "グラフ可視化 (Grafana):"
	@echo "  make grafana     - Grafana起動 (http://localhost:3000)"
	@echo "  make grafana-down- Grafana停止"
	@echo ""
	@echo "開発ツール:"
	@echo "  make build       - ビルド"
	@echo "  make test        - テスト実行"
	@echo "  make clean       - クリーンビルド"
	@echo "  make stop        - 実行中プロセス停止"
	@echo ""
	@echo "ワンコマンド実行:"
	@echo "  make all         - ビルド→起動→ベンチ→メトリクス表示"
	@echo ""
	@echo "CI/CD用 (既存):"
	@echo "  make bench       - CI/CDベンチマーク"
	@echo "  make gate        - 性能ゲート検証"
	@echo "  make check       - gate1 (bench + gate + change risk)"
	@echo "  make check-lite  - 軽量ゲート (unit/contract/fault/slo)"
	@echo "  make check-full  - 重量ゲート (check + perf-gate-rust-full)"
	@echo "  make bless       - ベースライン更新"
	@echo "  make perf-gate-rust - Rust GatewayのPerf Gate"
	@echo "  make perf-gate-rust-full - Perf Gate + OSサンプル付き"
	@echo "  make perf-gate-rust-ci - PR向け実測ゲート (strict + regression)"
	@echo "  make perf-gate-rust-nightly - 夜間向け実測ゲート (full report)"
	@echo "  make perf-gate-rust-bless - Rust Perf Gateのベースライン更新"
	@echo "  make preset-switch-gate - Normal/Degraded切替シナリオのPASS/FAIL判定"
	@echo "  make pure-hft-absolute-gate - /v3 absolute gateをLinux条件で1回実行"
	@echo "  make pure-hft-absolute-gate-loop - /v3 absolute gateを複数回定常実行"
	@echo "  make fdatasync-ab - fixed/adaptive の durability A/B比較"
	@echo "  make fdatasync-adaptive-sweep - adaptiveパラメータの小規模スイープ"
	@echo "  make adaptive-profile-search - durability+switch安定性を組み合わせた探索"
	@echo ""
	@echo "Rust Gateway (rootから実行):"
	@echo "  make rust-check  - gateway-rust の cargo check"
	@echo "  make rust-test   - gateway-rust の cargo test"
	@echo "  make rust-run    - gateway-rust を起動 (release)"
	@echo ""
	@echo "Kotlin/Java (rootから実行):"
	@echo "  make app-run     - app を起動"
	@echo "  make app-check   - app の check"
	@echo "  make app-test    - app の test"
	@echo "  make gateway-run - gateway を起動"
	@echo "  make gateway-check - gateway の check"
	@echo "  make gateway-test  - gateway の test"
	@echo "  make backoffice-run - backoffice を起動"
	@echo "  make backoffice-check - backoffice の check"
	@echo "  make backoffice-test  - backoffice の test"
	@echo ""
	@echo "========================================="

# アプリケーション起動
run:
	@echo "==> Starting HFT Fast Path..."
	FAST_PATH_ENABLE=1 FAST_PATH_METRICS=1 ./gradlew :app:run

# Gateway起動
run-gateway:
	@echo "==> Starting Gateway..."
	./gradlew :gateway:run

# BackOffice起動
run-backoffice:
	@echo "==> Starting BackOffice..."
	./gradlew :backoffice:run

# Kotlin/Java module aliases (rootから実行)
app-run: run
gateway-run: run-gateway
backoffice-run: run-backoffice

app-check:
	@echo "==> Checking app..."
	@./gradlew :app:check

gateway-check:
	@echo "==> Checking gateway..."
	@./gradlew :gateway:check

backoffice-check:
	@echo "==> Checking backoffice..."
	@./gradlew :backoffice:check

app-test:
	@echo "==> Testing app..."
	@./gradlew :app:test

gateway-test:
	@echo "==> Testing gateway..."
	@./gradlew :gateway:test

backoffice-test:
	@echo "==> Testing backoffice..."
	@./gradlew :backoffice:test

# 開発用ベンチマーク実行
dev-bench:
	@echo "==> Running development benchmark (/api/orders, 10,000 requests)..."
	@python3 scripts/bench_orders.py --runs 10000

# Prometheusメトリクス表示（整形済み）
metrics:
	@echo "==> Fast Path Metrics (Prometheus format)"
	@curl -s http://localhost:8080/metrics | grep -E "^(fast_path|orderbook|jvm_memory|jvm_gc)" | sort

# 統計情報表示（JSON整形）
stats:
	@echo "==> Fast Path Statistics"
	@curl -s http://localhost:8080/stats | python3 -m json.tool

# ヘルスチェック
health:
	@echo "==> Health Check"
	@curl -s http://localhost:8080/health | python3 -m json.tool

# リアルタイムダッシュボード
dashboard:
	@echo "==> Starting Real-time Dashboard (Ctrl+C to exit)"
	@./scripts/tools/dashboard.sh

# Grafana起動 (Prometheus + Grafana)
grafana:
	@echo "==> Starting Grafana + Prometheus..."
	@docker-compose --profile monitoring up -d
	@echo ""
	@echo "✓ Grafana is starting..."
	@echo "  URL:      http://localhost:3000"
	@echo "  User:     admin"
	@echo "  Password: admin"
	@echo ""
	@echo "  Dashboard: HFT Fast Path - Real-time Performance"
	@echo ""
	@echo "  Prometheus: http://localhost:9090"
	@echo ""
	@echo "==> Waiting for services to be ready (10s)..."
	@sleep 10
	@echo "✓ Services are ready!"
	@echo ""
	@echo "==> Use 'make grafana-down' to stop Grafana"

# Grafana起動 (エイリアス)
grafana-up: grafana

# Grafana停止
grafana-down:
	@echo "==> Stopping Grafana + Prometheus..."
	@docker-compose --profile monitoring down
	@echo "✓ Grafana stopped"

# Gateway E2E (Gateway + Kafka + BackOffice)
compose-gateway:
	@echo "==> Starting Gateway stack (gateway + kafka + backoffice)..."
	@docker-compose --profile gateway up -d --build
	@echo "✓ Gateway:    http://localhost:8081"
	@echo "✓ BackOffice: http://localhost:8082/positions"

compose-gateway-down:
	@echo "==> Stopping Gateway stack..."
	@docker-compose --profile gateway down

# BackOffice復旧チェック
backoffice-recovery:
	@scripts/ops/backoffice_recovery_check.sh

# Gateway→BackOffice E2E
gateway-backoffice-e2e:
	@scripts/ops/gateway_backoffice_e2e.sh

# Rust Gateway Perf Gate
perf-gate-rust:
	@scripts/ops/perf_gate_rust.sh

# Rust Gateway Perf Gate (full report)
perf-gate-rust-full:
	@scripts/ops/perf_gate_rust_full.sh

# Rust Perf Gate (PR CI向け)
perf-gate-rust-ci:
	@STRICT=1 BASELINE_REGRESSION=0.03 scripts/ops/perf_gate_rust.sh

# Rust Perf Gate (Nightly向け)
perf-gate-rust-nightly:
	@STRICT=1 BASELINE_REGRESSION=0.03 scripts/ops/perf_gate_rust_full.sh

# Rust Perf Gate baseline update (明示的に実行)
perf-gate-rust-bless:
	@UPDATE_BASELINE=1 scripts/ops/perf_gate_rust_full.sh

# Inflight preset switch gate (Normal -> Degraded -> Normal)
preset-switch-gate:
	@scripts/ops/check_preset_switch_gate.sh

pure-hft-absolute-gate:
	@scripts/ops/check_pure_hft_absolute_gate.sh

pure-hft-absolute-gate-loop:
	@scripts/ops/run_pure_hft_absolute_gate_loop.sh

fdatasync-ab:
	@scripts/ops/run_fdatasync_ab_compare.sh

fdatasync-adaptive-sweep:
	@scripts/ops/run_fdatasync_adaptive_sweep.sh

adaptive-profile-search:
	@scripts/ops/run_adaptive_profile_search.sh

# Rust Gateway (rootから実行)
rust-check:
	@echo "==> Rust Gateway check..."
	@cd gateway-rust && cargo check

rust-test:
	@echo "==> Rust Gateway test..."
	@cd gateway-rust && cargo test

rust-run:
	@echo "==> Starting Rust Gateway..."
	@cd gateway-rust && cargo run --release

# ビルド
build:
	@echo "==> Building..."
	./gradlew build

# テスト実行
test:
	@echo "==> Running tests..."
	./gradlew test

# クリーンビルド
clean:
	@echo "==> Clean build..."
	./gradlew clean build

# 実行中プロセス停止
stop:
	@echo "==> Stopping running processes..."
	@pkill -f "gradlew :app:run" || true
	@pkill -f "gradlew :gateway:run" || true
	@echo "✓ Stop requested"

# ワンコマンド実行: ビルド→起動→待機→ベンチ→メトリクス
all: build
	@echo "==> Starting application in background..."
	@FAST_PATH_ENABLE=1 FAST_PATH_METRICS=1 ./gradlew :app:run > /dev/null 2>&1 &
	@echo "==> Waiting for application startup (30s)..."
	@sleep 30
	@echo "==> Running benchmark..."
	@python3 scripts/bench_orderbook.py --runs 10000
	@echo ""
	@echo "==> Fast Path Metrics Summary:"
	@curl -s http://localhost:8080/stats | python3 -c "import sys,json; d=json.load(sys.stdin); fp=d['fast_path']; print(f'  p99 Latency: {fp[\"p99_us\"]:.2f} μs'); print(f'  Processed:   {fp[\"count\"]}'); print(f'  Errors:      {fp[\"error_count\"]}'); print(f'  Throughput:  {fp.get(\"throughput_rps\", 0):.0f} req/s')"
	@echo ""
	@echo "==> Use 'make stop' to stop the application"

# CI/CD用ベンチマーク (既存)
bench:
	OUT=$(OUT) MODE=$(MODE) RUNS=$(RUNS) CASE=$(CASE) tools/bench/run.sh

# 性能ゲート検証 (既存)
gate:
	tools/gate/run.sh $(OUT)

# bench + gate (既存)
check:
	@scripts/ops/gate1_check.sh

check-lite:
	@echo "==> Running check-lite..."
	@./gradlew :gateway:test :backoffice:test :app:test
	@scripts/ops/check_lite.sh

check-full: check
	@echo "==> Running check-full..."
	@$(MAKE) perf-gate-rust-full
	@scripts/ops/gate2_e2e.sh

# 良い結果をベースラインに昇格 (既存)
bless:
	BLESS=1 tools/gate/run.sh $(OUT)
