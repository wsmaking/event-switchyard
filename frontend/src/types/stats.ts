// /stats APIのレスポンス型定義 (一部フィールドはオプショナル)
export interface StatsResponse {
  fast_path_count: number;
  slow_path_count: number;
  fallback_count: number;
  fast_path_ratio: number;
  fast_path_avg_publish_us?: number;
  fast_path_avg_process_us?: number;
  fast_path_drop_count?: number;
  fast_path_publish_p50_us?: number;
  fast_path_publish_p99_us?: number;
  fast_path_publish_p999_us?: number;
  fast_path_process_p50_us?: number;
  fast_path_process_p99_us?: number;
  fast_path_process_p999_us?: number;
}

// ダッシュボード表示用のメトリクス
export interface DashboardMetrics {
  // レイテンシ (マイクロ秒)
  latency: {
    p50: number;
    p99: number;
    p999: number;
  };
  // スループット
  throughput: {
    total: number;
    fastPath: number;
    slowPath: number;
    ratio: number; // Fast Path使用率 (0-100%)
  };
  // エラー・ドロップ
  errors: {
    dropCount: number;
  };
  // Tail Ratio (p99 / p50)
  tailRatio: number;
}

// StatsResponseをDashboardMetricsに変換
export function toDashboardMetrics(stats: StatsResponse): DashboardMetrics {
  const totalCount = stats.fast_path_count + stats.slow_path_count;

  // デフォルト値を設定 (undefinedの場合は0)
  const p50 = stats.fast_path_process_p50_us ?? 0;
  const p99 = stats.fast_path_process_p99_us ?? 0;
  const p999 = stats.fast_path_process_p999_us ?? 0;
  const dropCount = stats.fast_path_drop_count ?? 0;

  return {
    latency: {
      p50,
      p99,
      p999,
    },
    throughput: {
      total: totalCount,
      fastPath: stats.fast_path_count,
      slowPath: stats.slow_path_count,
      ratio: totalCount > 0 ? (stats.fast_path_count / totalCount) * 100 : 0,
    },
    errors: {
      dropCount,
    },
    tailRatio: p50 > 0 ? p99 / p50 : 0,
  };
}
