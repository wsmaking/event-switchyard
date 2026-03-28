import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import type {
  MobileCardDetail,
  MobileCardSummary,
  MobileDrillAttemptRequest,
  MobileDrillDetail,
  MobileDrillProgressResponse,
  MobileDrillSummary,
  MobileHome,
  MobileOptionEvaluateRequest,
  MobileOptionEvaluation,
  MobileProgressResponse,
  MobileProgressUpdateRequest,
  MobileRiskEvaluateRequest,
  MobileRiskEvaluation,
  MobileRiskScenario,
} from '../types/mobile';

const API_BASE_URL = import.meta.env.DEV ? 'http://localhost:8080' : '';

async function fetchJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, init);
  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }
  return response.json();
}

export function useMobileHome() {
  return useQuery({
    queryKey: ['mobileHome'],
    queryFn: () => fetchJson<MobileHome>('/api/mobile/home'),
    refetchInterval: 5000,
  });
}

export function useMobileCards() {
  return useQuery({
    queryKey: ['mobileCards'],
    queryFn: () => fetchJson<MobileCardSummary[]>('/api/mobile/cards'),
    refetchInterval: 30000,
  });
}

export function useMobileCard(cardId: string | null) {
  return useQuery({
    queryKey: ['mobileCard', cardId],
    queryFn: () => fetchJson<MobileCardDetail>(`/api/mobile/cards/${cardId}`),
    enabled: !!cardId,
  });
}

export function useMobileProgress() {
  return useQuery({
    queryKey: ['mobileProgress'],
    queryFn: () => fetchJson<MobileProgressResponse>('/api/mobile/progress'),
    refetchInterval: 10000,
  });
}

export function useMobileDrills() {
  return useQuery({
    queryKey: ['mobileDrills'],
    queryFn: () => fetchJson<MobileDrillSummary[]>('/api/mobile/drills'),
    refetchInterval: 10000,
  });
}

export function useMobileDrill(drillId: string | null) {
  return useQuery({
    queryKey: ['mobileDrill', drillId],
    queryFn: () => fetchJson<MobileDrillDetail>(`/api/mobile/drills/${drillId}`),
    enabled: !!drillId,
  });
}

export function useUpdateMobileProgress() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (request: MobileProgressUpdateRequest) =>
      fetchJson<MobileProgressResponse>('/api/mobile/progress', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(request),
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['mobileHome'] });
      queryClient.invalidateQueries({ queryKey: ['mobileCards'] });
      queryClient.invalidateQueries({ queryKey: ['mobileCard'] });
      queryClient.invalidateQueries({ queryKey: ['mobileProgress'] });
    },
  });
}

export function useSubmitMobileDrillAttempt() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (request: MobileDrillAttemptRequest) =>
      fetchJson<MobileDrillProgressResponse>('/api/mobile/drills/attempt', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(request),
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['mobileHome'] });
      queryClient.invalidateQueries({ queryKey: ['mobileProgress'] });
      queryClient.invalidateQueries({ queryKey: ['mobileDrills'] });
      queryClient.invalidateQueries({ queryKey: ['mobileDrill'] });
    },
  });
}

export function useMobileRiskScenarios() {
  return useQuery({
    queryKey: ['mobileRiskScenarios'],
    queryFn: () => fetchJson<MobileRiskScenario[]>('/api/mobile/risk/scenarios'),
  });
}

export function useEvaluateMobileRisk() {
  return useMutation({
    mutationFn: (request: MobileRiskEvaluateRequest) =>
      fetchJson<MobileRiskEvaluation>('/api/mobile/risk/evaluate', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(request),
      }),
  });
}

export function useEvaluateMobileOption() {
  return useMutation({
    mutationFn: (request: MobileOptionEvaluateRequest) =>
      fetchJson<MobileOptionEvaluation>('/api/mobile/risk/options/evaluate', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(request),
      }),
  });
}
