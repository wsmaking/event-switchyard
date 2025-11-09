import { useQuery } from '@tanstack/react-query';
import type { StockInfo, Position } from '../types/trading';

const API_BASE_URL = import.meta.env.DEV ? 'http://localhost:8080' : '';

// 銘柄情報取得
async function fetchStockInfo(symbol: string): Promise<StockInfo> {
  const response = await fetch(`${API_BASE_URL}/api/market/${symbol}`);

  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }

  return response.json();
}

// 保有銘柄取得
async function fetchPositions(): Promise<Position[]> {
  const response = await fetch(`${API_BASE_URL}/api/positions`);

  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }

  return response.json();
}

export function useStockInfo(symbol: string) {
  return useQuery({
    queryKey: ['stockInfo', symbol],
    queryFn: () => fetchStockInfo(symbol),
    enabled: symbol.length > 0,
    refetchInterval: 3000, // 3秒ごとに更新
  });
}

export function usePositions() {
  return useQuery({
    queryKey: ['positions'],
    queryFn: fetchPositions,
    refetchInterval: 5000, // 5秒ごとに更新
  });
}
