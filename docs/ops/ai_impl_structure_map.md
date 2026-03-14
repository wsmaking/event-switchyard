# AI実装 構造マップ（時系列/因果判定対応）

最終更新: 2026-03-14

## 1. ソース依存関係（モジュール間）

```mermaid
flowchart LR
  subgraph EntryPoints["Entry Scripts"]
    EP_A["scripts/ops/ai_rag_index.py"]
    EP_B["scripts/ops/ai_rag_query.py"]
    EP_C["scripts/ops/ai_incident_agent.py"]
    EP_D["scripts/ops/analyze_slo_with_ai.py"]
    EP_E["scripts/ops/run_ai_perf_probe.py"]
    EP_F["scripts/ops/run_v3_open_loop_probe.sh"]
    EP_G["scripts/ops/collect_metrics_timeseries.py"]
  end

  T["scripts/ops/ai_tools.py"]
  U["scripts/ops/ai_model_adapter.py"]

  EP_A -->|"SQLite FTS5 index build"| IDX["var/ai_index/docs.sqlite"]
  EP_B -->|"import retrieve_evidence"| T
  EP_C -->|"import detect/retrieve/compare"| T
  EP_C -->|"import create_model_adapter"| U
  EP_E -->|"import run_agent_with_llm"| EP_C
  EP_F -->|"spawn"| EP_G
  EP_F -->|"write"| S1["var/results/{run}.summary.txt"]
  EP_F -->|"write"| S2["var/results/{run}.metrics.prom"]
  EP_G -->|"write"| S3["var/results/{run}.timeseries.jsonl"]

  T -->|"read"| S1
  T -->|"read (optional)"| S2
  T -->|"read (optional)"| S3
  T -->|"read (optional)"| S4["var/results/{run}.perf.json"]
  T -->|"query"| IDX

  EP_E -->|"collect counters"| K["perf stat / powermetrics"]
  EP_E -->|"write"| S4

  EP_D -->|"invoke unified agent"| EP_C
  EP_D -->|"format legacy 3-section stdout"| N["Legacy stdout report"]
  EP_C -->|"provider=mock"| M["Mock LLM path"]
  EP_C -. "provider=openai" .-> L["OpenAI API"]
  EP_C -. "provider=claude/anthropic" .-> A["Anthropic API"]
```

## 2. 処理順序（時系列 + 因果判定）

```mermaid
flowchart TD
  A["1. run_v3_open_loop_probe.sh 起動"] --> B["2. gateway 起動"]
  B --> C["3. collect_metrics_timeseries.py 開始"]
  C --> D["4. load実行中に /metrics を周期収集"]
  D --> E["5. timeseries.jsonl 保存"]
  D --> F["6. run終了時 metrics.prom / summary.txt 保存"]
  F --> G["7. ai_incident_agent.py"]
  E --> G
  G --> H["8. ai_tools.build_causal_signals\n(圧力→backpressure→rate低下順序判定)"]
  H --> I["9. ModelAdapter分析\n(mock/openai/claude)"]
  I --> J["10. {run}.triage.json + stdout"]
```

## 3. 責務分離

| ファイル | 役割 | 外部API依存 |
|---|---|---|
| `ai_rag_index.py` | `docs/ops/*.md` + `summary/metrics/perf` のチャンク化とSQLite索引構築 | なし |
| `ai_rag_query.py` | 索引への検索確認CLI | なし |
| `ai_tools.py` | SLO判定、run入力/時系列/perf読込、RAG検索、因果シグナル生成 | なし |
| `ai_model_adapter.py` | `ModelAdapter`抽象 + `MockModelAdapter/OpenAIAdapter/AnthropicAdapter` | OpenAI/Claude利用時のみあり |
| `ai_incident_agent.py` | Detect→Retrieve→Analyze→Reportのオーケストレーション | OpenAI/Claude利用時のみあり |
| `run_ai_perf_probe.py` | 負荷実行 + CPUカウンタ収集 + perf JSON生成 + triage連携 | OpenAI/Claude利用時のみあり |
| `collect_metrics_timeseries.py` | `/metrics` の周期サンプリング（JSONL時系列保存） | なし |
| `analyze_slo_with_ai.py` | legacy CLI互換ラッパー（内部は `ai_incident_agent.py` を呼び出し） | OpenAI/Claude利用時のみあり |

## 4. データ境界

```mermaid
flowchart LR
  A["docs/ops/*.md"] --> B["RAG Index DB (SQLite FTS5)"]
  C["var/results/{run}.summary.txt"] --> D["Agent Context"]
  E["var/results/{run}.metrics.prom"] --> D
  F["var/results/{run}.perf.json"] --> D
  G["var/results/{run}.timeseries.jsonl"] --> D
  B --> D
  D --> H["causal_signals生成"]
  H --> I["ModelAdapter"]
  I --> J["stdout JSON report"]
```

## 5. 現在のモード

- `mock` が既定（API疎通なし）
- `--provider openai` で OpenAI API 呼び出しに切替
- `--provider claude`（または `anthropic`）で Claude API 呼び出しに切替
- モデル既定: `gpt-5-nano`（openai）/ `claude-sonnet-4-20250514`（claude）
- 主因判定ヒント: `causal_signals.timeline.order` を出力
