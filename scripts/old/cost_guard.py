"""
コストガード (Cost Guard)

目的:
  ベンチマーク・API呼び出しのコストを監視し、予算超過時にCIを失敗させる。

使用例:
  # ベンチマーク結果からコスト計算
  python scripts/cost_guard.py \
    --in var/results/canary.json \
    --budget-yen-per-1k 12.0 \
    --report var/results/cost_report.json

  # CI統合
  python scripts/cost_guard.py --in var/results/canary.json --budget-yen-per-1k 12.0 --fail-on-exceed
"""

import json
import sys
import argparse
from pathlib import Path
from dataclasses import dataclass
from typing import Dict


@dataclass
class CostMetrics:
    """コストメトリクス"""
    total_events: int
    total_requests: int
    estimated_cost_yen: float
    cost_per_1k_events: float


class CostGuard:
    """コストガード"""

    # コスト計算定数 (例: OpenAI API、Kafka、Chronicle Queueストレージ)
    # 実際の料金体系に応じて調整
    COST_PER_1K_EVENTS_YEN = {
        "fast_path": 0.0,  # メモリ処理、コスト無し
        "chronicle_write": 0.001,  # ストレージ書き込み (1000イベントあたり0.001円と仮定)
        "kafka_send": 0.01,  # Kafka送信 (1000イベントあたり0.01円と仮定)
        "openai_embedding": 2.0,  # OpenAI Embeddings API (実際: $0.00002/token, 約1000イベントで2円)
    }

    def __init__(self, budget_yen_per_1k: float):
        self.budget_yen_per_1k = budget_yen_per_1k

    def calculate_cost(self, results: Dict) -> CostMetrics:
        """ベンチマーク結果からコスト計算"""
        metrics = results.get("metrics", {})
        fast_path = metrics.get("fast_path", {})
        chronicle_queue = metrics.get("chronicle_queue", {})

        total_events = fast_path.get("count", 0)
        chronicle_writes = chronicle_queue.get("write_count", 0) if chronicle_queue else 0

        # コスト計算
        # - Fast Path処理: 無料 (メモリ処理)
        # - Chronicle Queue書き込み: 0.001円/1000イベント
        cost_chronicle = (chronicle_writes / 1000.0) * self.COST_PER_1K_EVENTS_YEN["chronicle_write"]

        total_cost_yen = cost_chronicle
        cost_per_1k = (total_cost_yen / (total_events / 1000.0)) if total_events > 0 else 0.0

        return CostMetrics(
            total_events=total_events,
            total_requests=total_events,
            estimated_cost_yen=total_cost_yen,
            cost_per_1k_events=cost_per_1k
        )

    def check_budget(self, cost_metrics: CostMetrics) -> bool:
        """予算チェック"""
        return cost_metrics.cost_per_1k_events <= self.budget_yen_per_1k


def main():
    parser = argparse.ArgumentParser(description="コストガード: ベンチマーク・API呼び出しコスト監視")
    parser.add_argument("--in", dest="input", required=True, help="ベンチマーク結果JSON")
    parser.add_argument("--budget-yen-per-1k", type=float, required=True, help="予算 (円/1000イベント)")
    parser.add_argument("--report", help="コストレポート出力 (JSON)")
    parser.add_argument("--fail-on-exceed", action="store_true", help="予算超過時にエラー終了")

    args = parser.parse_args()

    # ベンチマーク結果読み込み
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"❌ エラー: ベンチマーク結果が見つかりません: {input_path}", file=sys.stderr)
        sys.exit(1)

    with open(input_path, 'r') as f:
        results = json.load(f)

    # コスト計算
    guard = CostGuard(args.budget_yen_per_1k)
    cost_metrics = guard.calculate_cost(results)

    # コンソール出力
    print("💰 コストガード結果:")
    print(f"   総イベント数: {cost_metrics.total_events:,}")
    print(f"   推定コスト: ¥{cost_metrics.estimated_cost_yen:.4f}")
    print(f"   コスト/1000イベント: ¥{cost_metrics.cost_per_1k_events:.4f}")
    print(f"   予算/1000イベント: ¥{args.budget_yen_per_1k:.4f}")
    print()

    # 予算チェック
    within_budget = guard.check_budget(cost_metrics)

    if within_budget:
        print("✅ 予算内に収まっています")
        status = "PASS"
        exit_code = 0
    else:
        overage_pct = ((cost_metrics.cost_per_1k_events - args.budget_yen_per_1k) / args.budget_yen_per_1k) * 100
        print(f"❌ 予算超過: ¥{cost_metrics.cost_per_1k_events:.4f} > ¥{args.budget_yen_per_1k:.4f} (+{overage_pct:.1f}%)")
        status = "FAIL"
        exit_code = 1 if args.fail_on_exceed else 0

    # レポート出力
    if args.report:
        report_path = Path(args.report)
        report_path.parent.mkdir(parents=True, exist_ok=True)

        report = {
            "status": status,
            "budget_yen_per_1k": args.budget_yen_per_1k,
            "actual_cost_yen_per_1k": cost_metrics.cost_per_1k_events,
            "total_events": cost_metrics.total_events,
            "estimated_cost_yen": cost_metrics.estimated_cost_yen,
            "within_budget": within_budget
        }

        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)

        print(f"💾 コストレポート保存: {report_path}")

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
