.PHONY: help run dev-bench metrics stats health dashboard grafana grafana-up grafana-down clean stop build test all bench gate check bless

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
	@echo "  make run         - アプリケーション起動"
	@echo "  make dev-bench   - 開発用ベンチマーク (10,000 runs)"
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
	@echo "  make check       - bench + gate"
	@echo "  make bless       - ベースライン更新"
	@echo ""
	@echo "========================================="

# アプリケーション起動
run:
	@echo "==> Starting HFT Fast Path..."
	FAST_PATH_ENABLE=1 FAST_PATH_METRICS=1 ./gradlew run

# 開発用ベンチマーク実行
dev-bench:
	@echo "==> Running development benchmark (10,000 requests)..."
	@python3 scripts/bench_orderbook.py --runs 10000

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
	@./scripts/dashboard.sh

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
	@pkill -f "gradlew run" || echo "No processes to stop"

# ワンコマンド実行: ビルド→起動→待機→ベンチ→メトリクス
all: build
	@echo "==> Starting application in background..."
	@FAST_PATH_ENABLE=1 FAST_PATH_METRICS=1 ./gradlew run > /dev/null 2>&1 &
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
check: bench gate

# 良い結果をベースラインに昇格 (既存)
bless:
	BLESS=1 tools/gate/run.sh $(OUT)
