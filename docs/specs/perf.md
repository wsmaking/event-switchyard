- REQ-PERF-001: `latency_us.p99 ≤ 1000µs` は **本番系ケース（例: match_engine_*）にのみ適用**。
  `blackbox_*` ケースは **smoke** とし、SLOチェックは **SKIP**。baseline比較も case/env 不一致時は **SKIP**。

- REQ-PERF-002: GC pause p99 ≤ 5ms
