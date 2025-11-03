"""
背圧オートチューナ (Backpressure Auto-Tuner)

目的:
  /metricsからメトリクスを読み取り、consumer lag、fetch latency、retry rate、
  drop countなどを分析して、Kafkaプロデューサー/コンシューマーの最適パラメータを推奨する。

出力:
  - CSV形式の推奨パラメータ (out/backpressure_tuner.csv)
  - PRコメント用マークダウン (オプション: --pr-comment)

使用例:
  # ベンチマーク結果から推奨を生成
  python scripts/backpressure_tuner.py --metrics var/results/canary.json --out var/results/tuner.csv

  # PRコメント生成
  python scripts/backpressure_tuner.py --metrics var/results/canary.json --pr-comment var/results/tuner_comment.md
"""

import json
import sys
import argparse
import csv
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass


@dataclass
class MetricsInput:
    """入力メトリクス"""
    fast_path_count: int
    drop_count: int
    process_p99_us: float
    persistence_queue_lag: int
    persistence_queue_error_count: int
    persistence_queue_write_p99_us: float
    throughput_events_per_sec: float


@dataclass
class TunerRecommendation:
    """チューニング推奨"""
    parameter: str
    current_value: Optional[str]
    recommended_value: str
    reason: str
    priority: str  # high, medium, low


class BackpressureTuner:
    """背圧オートチューナ"""

    # デフォルト値 (Kafka/Fast Path)
    DEFAULT_LINGER_MS = 10
    DEFAULT_BATCH_SIZE = 16384
    DEFAULT_MAX_IN_FLIGHT = 5
    DEFAULT_BUFFER_SIZE = 65536
    DEFAULT_MAX_POLL_INTERVAL_MS = 300000

    # SLO閾値 (docs/specs/slo.mdから)
    SLO_P99_US = 100.0
    SLO_DROP_COUNT = 0
    SLO_THROUGHPUT_MIN = 10000
    SLO_PQ_LAG_MAX = 1000
    SLO_ERROR_RATE_MAX = 0.01

    def __init__(self, metrics: MetricsInput):
        self.metrics = metrics
        self.recommendations: List[TunerRecommendation] = []

    def analyze_and_recommend(self) -> List[TunerRecommendation]:
        """メトリクスを分析し、推奨パラメータを生成"""
        self._check_drop_count()
        self._check_persistence_queue_lag()
        self._check_latency()
        self._check_error_rate()
        self._check_throughput()

        return self.recommendations

    def _check_drop_count(self):
        """ドロップ数チェック: Fast Pathバッファ溢れ"""
        if self.metrics.drop_count > 0:
            # Fast Pathリングバッファサイズ拡大を推奨
            self.recommendations.append(TunerRecommendation(
                parameter="FAST_PATH_BUFFER_SIZE",
                current_value=str(self.DEFAULT_BUFFER_SIZE),
                recommended_value=str(self.DEFAULT_BUFFER_SIZE * 2),
                reason=f"ドロップ検出: {self.metrics.drop_count}件。バッファサイズ2倍化でバースト吸収",
                priority="high"
            ))

            # Persistence Queue処理能力向上 (Batch size拡大)
            self.recommendations.append(TunerRecommendation(
                parameter="CHRONICLE_BATCH_SIZE",
                current_value="1",
                recommended_value="10",
                reason="Persistence Queue処理遅延。バッチ書き込みで書き込みスループット向上",
                priority="high"
            ))

    def _check_persistence_queue_lag(self):
        """Persistence Queueラグチェック"""
        if self.metrics.persistence_queue_lag > self.SLO_PQ_LAG_MAX:
            # Chronicle Queue書き込み並列度向上
            self.recommendations.append(TunerRecommendation(
                parameter="PERSISTENCE_QUEUE_THREADS",
                current_value="1",
                recommended_value="2",
                reason=f"Persistence Queueラグ高騰: {self.metrics.persistence_queue_lag}件 > {self.SLO_PQ_LAG_MAX}件。並列書き込みで改善",
                priority="high"
            ))

    def _check_latency(self):
        """レイテンシチェック: p99がSLO超過"""
        if self.metrics.process_p99_us > self.SLO_P99_US:
            overage_pct = ((self.metrics.process_p99_us - self.SLO_P99_US) / self.SLO_P99_US) * 100

            # GC調整推奨
            self.recommendations.append(TunerRecommendation(
                parameter="JVM_GC_PAUSE_TARGET",
                current_value="1ms",
                recommended_value="0.5ms",
                reason=f"p99超過: {self.metrics.process_p99_us:.1f}μs > {self.SLO_P99_US}μs (+{overage_pct:.1f}%)。GC pause target引き下げ",
                priority="medium"
            ))

            # YieldingWaitStrategy -> BusySpinWaitStrategy (超低レイテンシ優先)
            self.recommendations.append(TunerRecommendation(
                parameter="FAST_PATH_WAIT_STRATEGY",
                current_value="YieldingWaitStrategy",
                recommended_value="BusySpinWaitStrategy",
                reason="レイテンシSLO未達。BusySpinで待機オーバーヘッド削減 (CPU使用率は上昇)",
                priority="low"
            ))

    def _check_error_rate(self):
        """エラー率チェック"""
        if self.metrics.fast_path_count > 0:
            error_rate = (self.metrics.persistence_queue_error_count / self.metrics.fast_path_count) * 100
        else:
            error_rate = 0.0

        if error_rate > self.SLO_ERROR_RATE_MAX:
            # Chronicle Queue書き込みエラー: ディスク容量/権限チェック推奨
            self.recommendations.append(TunerRecommendation(
                parameter="CHRONICLE_QUEUE_PATH",
                current_value="var/chronicle",
                recommended_value="検証が必要",
                reason=f"Chronicle書き込みエラー率: {error_rate:.2f}% > {self.SLO_ERROR_RATE_MAX}%。ディスク容量/権限を確認",
                priority="high"
            ))

    def _check_throughput(self):
        """スループットチェック"""
        if self.metrics.throughput_events_per_sec < self.SLO_THROUGHPUT_MIN:
            shortfall_pct = ((self.SLO_THROUGHPUT_MIN - self.metrics.throughput_events_per_sec) / self.SLO_THROUGHPUT_MIN) * 100

            # Kafkaプロデューサーlinger.ms削減 (レイテンシ優先)
            self.recommendations.append(TunerRecommendation(
                parameter="KAFKA_LINGER_MS",
                current_value=str(self.DEFAULT_LINGER_MS),
                recommended_value="1",
                reason=f"スループット不足: {self.metrics.throughput_events_per_sec:.0f} < {self.SLO_THROUGHPUT_MIN} events/s (-{shortfall_pct:.1f}%)。linger.ms削減で送信遅延短縮",
                priority="medium"
            ))


def load_metrics_from_file(file_path: Path) -> MetricsInput:
    """ベンチマーク結果JSONからメトリクス抽出"""
    with open(file_path, 'r') as f:
        data = json.load(f)

    metrics = data.get("metrics", {})
    fast_path = metrics.get("fast_path", {})
    process_latency = fast_path.get("process_latency_us", {})
    persistence_queue = metrics.get("persistence_queue", {})
    summary = metrics.get("summary", {})

    return MetricsInput(
        fast_path_count=fast_path.get("count", 0),
        drop_count=fast_path.get("drop_count", 0),
        process_p99_us=process_latency.get("p99", 0.0),
        persistence_queue_lag=persistence_queue.get("lag", 0),
        persistence_queue_error_count=persistence_queue.get("error_count", 0),
        persistence_queue_write_p99_us=persistence_queue.get("write_latency_us", {}).get("p99", 0.0),
        throughput_events_per_sec=summary.get("throughput_events_per_sec", 0.0)
    )


def write_csv(recommendations: List[TunerRecommendation], output_path: Path):
    """CSV形式で推奨パラメータ出力"""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w', newline='') as csvfile:
        fieldnames = ['priority', 'parameter', 'current_value', 'recommended_value', 'reason']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for rec in sorted(recommendations, key=lambda r: {'high': 0, 'medium': 1, 'low': 2}[r.priority]):
            writer.writerow({
                'priority': rec.priority,
                'parameter': rec.parameter,
                'current_value': rec.current_value or "N/A",
                'recommended_value': rec.recommended_value,
                'reason': rec.reason
            })


def generate_pr_comment(recommendations: List[TunerRecommendation]) -> str:
    """PRコメント用マークダウン生成"""
    if not recommendations:
        return "✅ **背圧オートチューナ**: すべてのメトリクスが正常範囲内です。推奨事項はありません。"

    lines = [
        "## 背圧オートチューナ推奨事項",
        "",
        "以下のパラメータ調整を推奨します:",
        "",
        "| 優先度 | パラメータ | 現在値 | 推奨値 | 理由 |",
        "|--------|-----------|--------|--------|------|"
    ]

    priority_emoji = {
        'high': '🔴',
        'medium': '🟡',
        'low': '⚪'
    }

    for rec in sorted(recommendations, key=lambda r: {'high': 0, 'medium': 1, 'low': 2}[r.priority]):
        emoji = priority_emoji.get(rec.priority, '')
        lines.append(f"| {emoji} {rec.priority.upper()} | `{rec.parameter}` | {rec.current_value or 'N/A'} | **{rec.recommended_value}** | {rec.reason} |")

    lines.append("")
    lines.append("### 適用方法")
    lines.append("")
    lines.append("```bash")
    lines.append("# 環境変数として設定")
    for rec in recommendations:
        if rec.parameter.startswith("FAST_PATH_") or rec.parameter.startswith("KAFKA_") or rec.parameter.startswith("CHRONICLE_"):
            lines.append(f"export {rec.parameter}={rec.recommended_value}")
    lines.append("```")
    lines.append("")
    lines.append("**注意**: 本推奨は自動生成です。適用前に必ずステージング環境で検証してください。")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="背圧オートチューナ: Kafkaパラメータ推奨生成")
    parser.add_argument("--metrics", required=True, help="入力メトリクスJSON (ベンチマーク結果)")
    parser.add_argument("--out", help="出力CSV (例: var/results/tuner.csv)")
    parser.add_argument("--pr-comment", help="PRコメント用マークダウン出力 (例: var/results/tuner_comment.md)")
    parser.add_argument("--verbose", action="store_true", help="詳細出力")

    args = parser.parse_args()

    # メトリクス読み込み
    metrics_path = Path(args.metrics)
    if not metrics_path.exists():
        print(f"❌ エラー: メトリクスファイルが見つかりません: {metrics_path}", file=sys.stderr)
        sys.exit(1)

    metrics = load_metrics_from_file(metrics_path)

    if args.verbose:
        print("📊 入力メトリクス:")
        print(f"   Fast Path処理数: {metrics.fast_path_count}")
        print(f"   ドロップ数: {metrics.drop_count}")
        print(f"   p99レイテンシ: {metrics.process_p99_us:.1f}μs")
        print(f"   Persistence Queue Lag: {metrics.persistence_queue_lag}")
        print(f"   スループット: {metrics.throughput_events_per_sec:.0f} events/s")
        print()

    # チューニング推奨生成
    tuner = BackpressureTuner(metrics)
    recommendations = tuner.analyze_and_recommend()

    if not recommendations:
        print("✅ すべてのメトリクスが正常範囲内です。推奨事項はありません。")
        # 空CSVを出力 (CI向け)
        if args.out:
            write_csv([], Path(args.out))
        sys.exit(0)

    # CSV出力
    if args.out:
        output_path = Path(args.out)
        write_csv(recommendations, output_path)
        print(f"💾 推奨パラメータCSV出力: {output_path}")

    # PRコメント生成
    if args.pr_comment:
        comment_path = Path(args.pr_comment)
        comment_path.parent.mkdir(parents=True, exist_ok=True)
        comment = generate_pr_comment(recommendations)
        with open(comment_path, 'w') as f:
            f.write(comment)
        print(f"💬 PRコメント生成: {comment_path}")

    # コンソール出力
    print()
    print("🔧 背圧オートチューナ推奨:")
    for rec in sorted(recommendations, key=lambda r: {'high': 0, 'medium': 1, 'low': 2}[r.priority]):
        priority_color = {
            'high': '\033[91m',     # 赤
            'medium': '\033[93m',   # 黄
            'low': '\033[90m'       # グレー
        }
        color = priority_color.get(rec.priority, '')
        reset = '\033[0m'
        print(f"  {color}[{rec.priority.upper()}]{reset} {rec.parameter}: {rec.current_value} → {rec.recommended_value}")
        print(f"       理由: {rec.reason}")

    print()
    print(f"✅ 合計 {len(recommendations)} 件の推奨事項を生成")


if __name__ == "__main__":
    main()
