#!/bin/bash
# リアルタイムダッシュボード - Fast Path メトリクス表示

# 色定義
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color
BOLD='\033[1m'

# ヘッダー表示
show_header() {
    clear
    echo -e "${BOLD}${CYAN}=========================================${NC}"
    echo -e "${BOLD}${CYAN}  HFT Fast Path - Real-time Dashboard${NC}"
    echo -e "${BOLD}${CYAN}=========================================${NC}"
    echo ""
}

# メトリクス表示
show_metrics() {
    # /stats エンドポイントからJSON取得
    local stats=$(curl -s http://localhost:8080/stats 2>/dev/null)

    if [ -z "$stats" ]; then
        echo -e "${RED}Error: Cannot connect to http://localhost:8080${NC}"
        echo "Make sure the application is running (make run)"
        exit 1
    fi

    # JSONパース
    local fp_p99=$(echo "$stats" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['fast_path']['p99_us'])" 2>/dev/null || echo "N/A")
    local fp_p50=$(echo "$stats" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['fast_path']['p50_us'])" 2>/dev/null || echo "N/A")
    local fp_count=$(echo "$stats" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['fast_path']['count'])" 2>/dev/null || echo "0")
    local fp_errors=$(echo "$stats" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['fast_path']['error_count'])" 2>/dev/null || echo "0")
    local fp_drops=$(echo "$stats" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['fast_path']['drop_count'])" 2>/dev/null || echo "0")

    local slow_count=$(echo "$stats" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('slow_path', {}).get('count', 0))" 2>/dev/null || echo "0")

    local jvm_heap=$(echo "$stats" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f\"{d['jvm']['memory']['heap_used_mb']:.0f}/{d['jvm']['memory']['heap_max_mb']:.0f}\")" 2>/dev/null || echo "N/A")
    local jvm_gc=$(echo "$stats" | python3 -c "import sys,json; d=json.load(sys.stdin); gcs=d['jvm']['gc']; total=sum(gc['collection_count'] for gc in gcs); print(total)" 2>/dev/null || echo "0")

    # 色付きで表示
    echo -e "${BOLD}Fast Path Performance:${NC}"
    echo -e "  p50 Latency:    ${GREEN}${fp_p50} μs${NC}"

    # p99が100μs以下なら緑、それ以上なら黄色
    if (( $(echo "$fp_p99 < 100" | bc -l 2>/dev/null || echo 0) )); then
        echo -e "  p99 Latency:    ${GREEN}${fp_p99} μs${NC} ✓ (Target: <100μs)"
    else
        echo -e "  p99 Latency:    ${YELLOW}${fp_p99} μs${NC} ⚠ (Target: <100μs)"
    fi

    echo ""
    echo -e "${BOLD}Event Counts:${NC}"
    echo -e "  Processed:      ${CYAN}${fp_count}${NC}"
    echo -e "  Fast Path:      ${GREEN}${fp_count}${NC}"
    echo -e "  Slow Path:      ${BLUE}${slow_count}${NC}"

    # エラー/ドロップがあれば赤で表示
    if [ "$fp_errors" -gt 0 ]; then
        echo -e "  Errors:         ${RED}${fp_errors}${NC} ✗"
    else
        echo -e "  Errors:         ${GREEN}${fp_errors}${NC} ✓"
    fi

    if [ "$fp_drops" -gt 0 ]; then
        echo -e "  Drops:          ${RED}${fp_drops}${NC} ✗"
    else
        echo -e "  Drops:          ${GREEN}${fp_drops}${NC} ✓"
    fi

    echo ""
    echo -e "${BOLD}JVM Status:${NC}"
    echo -e "  Heap Memory:    ${CYAN}${jvm_heap} MB${NC}"
    echo -e "  GC Count:       ${BLUE}${jvm_gc}${NC}"

    echo ""
    echo -e "${BOLD}Timestamp:${NC} $(date '+%Y-%m-%d %H:%M:%S')"
}

# メイン処理
main() {
    # Ctrl+C で終了
    trap 'echo ""; echo "Dashboard stopped."; exit 0' INT

    # 初回表示
    show_header
    show_metrics

    # 1秒ごとに更新
    while true; do
        sleep 1
        show_header
        show_metrics
    done
}

# 実行
main
