import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import type { StrategyConfig, StrategyConfigUpdate } from '../types/trading';

const API_BASE_URL = import.meta.env.DEV ? 'http://localhost:8080' : '';
const ADMIN_TOKEN = import.meta.env.VITE_STRATEGY_ADMIN_TOKEN as string | undefined;

async function fetchStrategyConfig(): Promise<StrategyConfig> {
  const response = await fetch(`${API_BASE_URL}/api/strategy`);
  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }
  return response.json();
}

async function updateStrategyConfig(config: StrategyConfigUpdate): Promise<StrategyConfig> {
  const authHeaders = ADMIN_TOKEN ? { Authorization: `Bearer ${ADMIN_TOKEN}` } : {};
  const response = await fetch(`${API_BASE_URL}/api/strategy`, {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
      ...authHeaders,
    },
    body: JSON.stringify(config),
  });
  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }
  return response.json();
}

export function useStrategyConfig() {
  return useQuery({
    queryKey: ['strategy-config'],
    queryFn: fetchStrategyConfig,
    refetchInterval: 5000,
  });
}

export function useUpdateStrategyConfig() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: updateStrategyConfig,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['strategy-config'] });
    },
  });
}
