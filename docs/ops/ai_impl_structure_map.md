# AI実装 構造マップ（Mock既定 / OpenAI切替可）

最終更新: 2026-03-14

## 1. ソース依存関係（モジュール間）

```mermaid
flowchart LR
  subgraph EntryPoints["Entry Scripts"]
    A["scripts/ops/ai_rag_index.py"]
    B["scripts/ops/ai_rag_query.py"]
    C["scripts/ops/ai_incident_agent.py"]
    D["scripts/ops/analyze_slo_with_ai.py"]
    E["scripts/ops/run_ai_perf_probe.py"]
  end

  T["scripts/ops/ai_tools.py"]
  U["scripts/ops/ai_model_adapter.py"]

  A -->|"SQLite FTS5 index build"| G["var/ai_index/docs.sqlite"]
  B -->|"import retrieve_evidence"| T
  C -->|"import detect/retrieve/compare"| T
  C -->|"import create_model_adapter"| U
  E -->|"import run_agent_with_llm"| C

  T -->|"read"| H["var/results/*.summary.txt"]
  T -->|"read (optional)"| I["var/results/*.metrics.prom"]
  T -->|"read (optional)"| J["var/results/*.perf.json"]
  T -->|"query"| G

  E -->|"collect counters"| K["perf stat / powermetrics"]
  E -->|"write"| J

  D -->|"internal SLO parser + prompt builder"| N["Single-run stdout report"]
  D -->|"default"| M["Mock LLM path"]
  D -. "opt-in: --provider openai" .-> L["OpenAI API"]
  C -->|"provider=mock"| M
  C -. "provider=openai" .-> L
```

## 2. 処理順序（CPU計測 + RAG Agent）

```mermaid
flowchart TD
  A["1. probe command実行"] --> B["2. summary.txt生成"]
  A --> C["3. counter収集\nLinux: perf stat\nmacOS: powermetrics"]
  B --> D["4. run_name解決"]
  C --> E["5. {run}.perf.json書き込み"]
  D --> F["6. ai_incident_agent.py 呼び出し（任意）"]
  E --> F
  F --> G["7. violations検知 + RAG検索 + recent比較"]
  G --> H["8. ModelAdapterで分析\n(mock / openai)"]
  H --> I["9. {run}.triage.json + stdout"]
```

## 3. 責務分離

| ファイル | 役割 | 外部API依存 |
|---|---|---|
| `ai_rag_index.py` | `docs/ops/*.md` + `summary/metrics/perf` のチャンク化とSQLite索引構築 | なし |
| `ai_rag_query.py` | 索引への検索確認CLI | なし |
| `ai_tools.py` | SLO判定、run入力読込、perf profile読込、RAG検索、run比較ユーティリティ | なし |
| `ai_model_adapter.py` | `ModelAdapter`抽象 + `MockModelAdapter/OpenAIAdapter` | OpenAI利用時のみあり |
| `ai_incident_agent.py` | Detect→Retrieve→Analyze→Reportのオーケストレーション | OpenAI利用時のみあり |
| `run_ai_perf_probe.py` | 負荷実行 + CPUカウンタ収集 + perf JSON生成 + triage連携 | OpenAI利用時のみあり |
| `analyze_slo_with_ai.py` | 単発解析スクリプト（legacy互換） | OpenAI利用時のみあり |

## 4. データ境界

```mermaid
flowchart LR
  A["docs/ops/*.md"] --> B["RAG Index DB (SQLite FTS5)"]
  C["var/results/{run}.summary.txt"] --> D["Agent Context"]
  E["var/results/{run}.metrics.prom"] --> D
  F["var/results/{run}.perf.json"] --> D
  B --> D
  D --> G["ModelAdapter"]
  G --> H["stdout JSON report"]
```

## 5. 現在のモード

- `mock` が既定（API疎通なし）
- `--provider openai` で OpenAI API 呼び出しに切替
- モデル既定: `gpt-5-nano`（openai選択時）
