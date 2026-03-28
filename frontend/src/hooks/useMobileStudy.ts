import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useEffect, useState } from 'react';
import { isLocalBundleRuntime, isMobileRuntime } from '../appBase';
import { getOfflineOpsOverview, getOfflineOrderFinalOut, getOfflineOrders, runOfflineReplayScenario } from '../offline/mobileOfflineStore';
import type { OpsOverview, Order, OrderFinalOut, OrderRequest } from '../types/trading';

const API_BASE_URL = import.meta.env.DEV ? 'http://localhost:8080' : '';

export type MobileOrderStreamState = 'idle' | 'connecting' | 'open' | 'error' | 'offline';

async function fetchJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, init);
  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }
  return response.json();
}

async function fetchJsonWithOfflineFallback<T>(path: string, fallback: () => T | Promise<T>, init?: RequestInit): Promise<T> {
  try {
    return await fetchJson<T>(path, init);
  } catch (error) {
    if (!isMobileLearningRoute()) {
      throw error;
    }
    return fallback();
  }
}

function isMobileLearningRoute() {
  return isMobileRuntime();
}

export function useMobileOrders() {
  return useQuery({
    queryKey: ['mobileOrders'],
    queryFn: () => fetchJsonWithOfflineFallback<Order[]>('/api/orders', () => getOfflineOrders()),
    refetchInterval: 2000,
  });
}

export function useMobileOrderFinalOut(orderId: string | null) {
  return useQuery({
    queryKey: ['mobileOrderFinalOut', orderId],
    queryFn: () =>
      fetchJsonWithOfflineFallback<OrderFinalOut>(
        `/api/orders/${orderId}/final-out`,
        () => getOfflineOrderFinalOut(orderId as string)
      ),
    enabled: !!orderId,
    refetchInterval: 2000,
  });
}

export function useMobileOpsOverview(orderId: string | null) {
  return useQuery({
    queryKey: ['mobileOpsOverview', orderId],
    queryFn: () => {
      const query = orderId ? `?orderId=${encodeURIComponent(orderId)}` : '';
      return fetchJsonWithOfflineFallback<OpsOverview>(`/api/ops/overview${query}`, () => getOfflineOpsOverview(orderId));
    },
    refetchInterval: 3000,
  });
}

export function useRunMobileReplayScenario() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (input: { scenario: string; request: OrderRequest }) =>
      fetchJsonWithOfflineFallback<Order>(
        `/api/demo/scenarios/${encodeURIComponent(input.scenario)}/run`,
        () => runOfflineReplayScenario(input),
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify(input.request),
        }
      ),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['mobileOrders'] });
      queryClient.invalidateQueries({ queryKey: ['mobileOrderFinalOut'] });
      queryClient.invalidateQueries({ queryKey: ['mobileOpsOverview'] });
      queryClient.invalidateQueries({ queryKey: ['mobileHome'] });
    },
  });
}

export function useMobileOrderStream(orderId: string | null) {
  const queryClient = useQueryClient();
  const [connectionState, setConnectionState] = useState<MobileOrderStreamState>(
    orderId ? 'connecting' : 'idle'
  );

  useEffect(() => {
    if (!orderId) {
      setConnectionState('idle');
      return;
    }
    if (isLocalBundleRuntime()) {
      setConnectionState('offline');
      return;
    }
    if (isMobileLearningRoute() && typeof navigator !== 'undefined' && navigator.onLine === false) {
      setConnectionState('offline');
      return;
    }

    setConnectionState('connecting');
    const eventSource = new EventSource(`${API_BASE_URL}/api/order-stream?orderId=${encodeURIComponent(orderId)}`);
    const refresh = () => {
      queryClient.invalidateQueries({ queryKey: ['mobileOrders'] });
      queryClient.invalidateQueries({ queryKey: ['mobileOrderFinalOut'] });
      queryClient.invalidateQueries({ queryKey: ['mobileOpsOverview'] });
      queryClient.invalidateQueries({ queryKey: ['mobileHome'] });
    };

    let settled = false;
    eventSource.addEventListener('ready', () => {
      settled = true;
      setConnectionState('open');
      refresh();
    });
    eventSource.addEventListener('update', () => {
      settled = true;
      setConnectionState('open');
      refresh();
    });
    eventSource.onerror = () => {
      if (isMobileLearningRoute() && !navigator.onLine) {
        setConnectionState('offline');
        eventSource.close();
        return;
      }
      if (!settled && isMobileLearningRoute()) {
        setConnectionState('offline');
        eventSource.close();
        return;
      }
      setConnectionState('error');
    };

    return () => {
      eventSource.close();
      setConnectionState('idle');
    };
  }, [orderId, queryClient]);

  return connectionState;
}
