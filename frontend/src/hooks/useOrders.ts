import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import type { Order, OrderRequest } from '../types/trading';

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
