import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import type { Order, OrderFinalOut, OrderRequest } from '../types/trading';

const API_BASE_URL = import.meta.env.DEV ? 'http://localhost:8080' : '';

// 注文履歴取得
async function fetchOrders(): Promise<Order[]> {
  const response = await fetch(`${API_BASE_URL}/api/orders`);

  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }

  return response.json();
}

// 新規注文送信
async function submitOrder(request: OrderRequest): Promise<Order> {
  const response = await fetch(`${API_BASE_URL}/api/orders`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(request),
  });

  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }

  return response.json();
}

async function fetchOrderFinalOut(orderId: string): Promise<OrderFinalOut> {
  const response = await fetch(`${API_BASE_URL}/api/orders/${orderId}/final-out`);

  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }

  return response.json();
}

async function resetDemo(): Promise<{ status: string }> {
  const response = await fetch(`${API_BASE_URL}/api/demo/reset`, {
    method: 'POST',
  });

  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }

  return response.json();
}

async function runReplayScenario(input: { scenario: string; request: OrderRequest }): Promise<Order> {
  const response = await fetch(`${API_BASE_URL}/api/demo/scenarios/${encodeURIComponent(input.scenario)}/run`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(input.request),
  });

  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }

  return response.json();
}

export function useOrders() {
  return useQuery({
    queryKey: ['orders'],
    queryFn: fetchOrders,
    refetchInterval: 2000, // 2秒ごとに更新
  });
}

export function useSubmitOrder() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: submitOrder,
    onSuccess: () => {
      // 注文成功後、履歴を再取得
      queryClient.invalidateQueries({ queryKey: ['orders'] });
    },
  });
}

export function useOrderFinalOut(orderId: string | null) {
  return useQuery({
    queryKey: ['orderFinalOut', orderId],
    queryFn: () => fetchOrderFinalOut(orderId as string),
    enabled: !!orderId,
    refetchInterval: 2000,
  });
}

export function useResetDemo() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: resetDemo,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['orders'] });
      queryClient.invalidateQueries({ queryKey: ['positions'] });
      queryClient.invalidateQueries({ queryKey: ['orderFinalOut'] });
    },
  });
}

export function useRunReplayScenario() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: runReplayScenario,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['orders'] });
      queryClient.invalidateQueries({ queryKey: ['positions'] });
      queryClient.invalidateQueries({ queryKey: ['orderFinalOut'] });
    },
  });
}
