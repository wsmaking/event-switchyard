import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import type { StrategyConfig, StrategyConfigUpdate } from '../types/trading';

const API_BASE_URL = import.meta.env.DEV ? 'http://localhost:8080' : '';

async function fetchStrategyConfig(): Promise<StrategyConfig> {
  const response = await fetch(`${API_BASE_URL}/api/strategy`);
  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }
  return response.json();
}

async function updateStrategyConfig(config: StrategyConfigUpdate): Promise<StrategyConfig> {
  const response = await fetch(`${API_BASE_URL}/api/strategy`, {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
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
