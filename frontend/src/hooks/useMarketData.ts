import { useQuery } from '@tanstack/react-query';
import type { StockInfo, Position, PricePoint } from '../types/trading';

const API_BASE_URL = import.meta.env.DEV ? 'http://localhost:8080' : '';

// 銘柄情報取得
async function fetchStockInfo(symbol: string): Promise<StockInfo> {
  const response = await fetch(`${API_BASE_URL}/api/market/${symbol}`);

  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }

  return response.json();
}

// 価格履歴取得
async function fetchPriceHistory(symbol: string): Promise<PricePoint[]> {
  const response = await fetch(`${API_BASE_URL}/api/market/${symbol}/history?limit=120`);

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
    refetchInterval: 5000, // 5秒ごとに更新
  });
}

export function usePositions() {
  return useQuery({
    queryKey: ['positions'],
    queryFn: fetchPositions,
    refetchInterval: 5000, // 5秒ごとに更新
  });
}

export function usePriceHistory(symbol: string) {
  return useQuery({
    queryKey: ['priceHistory', symbol],
    queryFn: () => fetchPriceHistory(symbol),
    enabled: symbol.length > 0,
    refetchInterval: 10000,
  });
}
