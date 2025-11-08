import { useQuery } from '@tanstack/react-query';
import type { StatsResponse, DashboardMetrics } from '../types/stats';
import { toDashboardMetrics } from '../types/stats';

// 本番環境では同一オリジン、開発時はlocalhost:8080
const API_BASE_URL = import.meta.env.DEV ? 'http://localhost:8080' : '';

// /stats APIからデータ取得
async function fetchStats(): Promise<StatsResponse> {
  const response = await fetch(`${API_BASE_URL}/stats`);

  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }

  return response.json();
}

// カスタムフック: リアルタイムでstatsを取得
export function useStats(refetchInterval = 1000) {
  const query = useQuery({
    queryKey: ['stats'],
    queryFn: fetchStats,
    refetchInterval, // 1秒ごとに自動リフェッチ
    staleTime: 500,  // 500ms以内は古いデータとみなさない
    retry: 3,        // 失敗時3回リトライ
  });

  // DashboardMetrics形式に変換
  const metrics: DashboardMetrics | undefined = query.data
    ? toDashboardMetrics(query.data)
    : undefined;

  return {
    ...query,
    metrics,
    rawStats: query.data,
  };
}
