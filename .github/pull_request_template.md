# タイトル（例）
W2: p99/GC gate導入 + baseline seed（match_engine_hot）

## 概要
- 何を、なぜ、どの範囲で変更したか（1–3行）

## 背景 / 目的（Issue/REQ）
- Issue: #123（あれば）
- 要件: REQ-PERF-001（docs/specs/perf.md への相対リンク）

## 変更点
- `rv/tools/gate.py` 追加（p99 ≤ 1000µs、baseline比 +5%）
- `.github/workflows/review.yml` 追加（bench → gate → コメント） ほか

## 再現手順
```bash
bench/bench.sh --case match_engine_hot --runs=5 --out results/pr.json
python rv/tools/gate.py --bench results/pr.json --req docs/specs/perf.md --baseline baseline/match_engine_hot.json
