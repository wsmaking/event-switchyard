import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { isMobileRuntime } from '../appBase';
import type {
  MobileCardDetail,
  MobileCardSummary,
  MobileDrillAttemptRequest,
  MobileDrillDetail,
  MobileDrillProgressResponse,
  MobileDrillSummary,
  MobileInstitutionalFlow,
  MobileHome,
  MobileOptionEvaluateRequest,
  MobileOptionEvaluation,
  MobileOperationsGuide,
  MobilePostTradeGuide,
  MobileProgressResponse,
  MobileProgressUpdateRequest,
  MobileRiskDeepDive,
  MobileRiskEvaluateRequest,
  MobileRiskEvaluation,
  MobileRiskScenario,
  MobileAssetClassGuide,
} from '../types/mobile';
import {
  applyOfflineMobileDrillAttempt,
  applyOfflineMobileProgress,
  evaluateOfflineMobileOption,
  evaluateOfflineMobileRisk,
  getOfflineMobileCard,
  getOfflineMobileCards,
  getOfflineMobileDrill,
  getOfflineMobileDrills,
  getOfflineMobileHome,
  getOfflineMobileProgress,
  getOfflineMobileRiskScenarios,
} from '../offline/mobileOfflineStore';
import {
  getOfflineMobileAssetClassGuide,
  getOfflineMobileInstitutionalFlow,
  getOfflineMobileOperationsGuide,
  getOfflineMobilePostTradeGuide,
  getOfflineMobileRiskDeepDive,
} from '../offline/mobileRoadmapOffline';

const API_BASE_URL = import.meta.env.DEV ? 'http://localhost:8080' : '';

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

export function useMobileHome() {
  return useQuery({
    queryKey: ['mobileHome'],
    queryFn: () => fetchJsonWithOfflineFallback<MobileHome>('/api/mobile/home', () => getOfflineMobileHome()),
    refetchInterval: 5000,
  });
}

export function useMobileCards() {
  return useQuery({
    queryKey: ['mobileCards'],
    queryFn: () => fetchJsonWithOfflineFallback<MobileCardSummary[]>('/api/mobile/cards', () => getOfflineMobileCards()),
    refetchInterval: 30000,
  });
}

export function useMobileCard(cardId: string | null) {
  return useQuery({
    queryKey: ['mobileCard', cardId],
    queryFn: () =>
      fetchJsonWithOfflineFallback<MobileCardDetail>(`/api/mobile/cards/${cardId}`, () => getOfflineMobileCard(cardId as string)),
    enabled: !!cardId,
  });
}

export function useMobileProgress() {
  return useQuery({
    queryKey: ['mobileProgress'],
    queryFn: () => fetchJsonWithOfflineFallback<MobileProgressResponse>('/api/mobile/progress', () => getOfflineMobileProgress()),
    refetchInterval: 10000,
  });
}

export function useMobileDrills() {
  return useQuery({
    queryKey: ['mobileDrills'],
    queryFn: () => fetchJsonWithOfflineFallback<MobileDrillSummary[]>('/api/mobile/drills', () => getOfflineMobileDrills()),
    refetchInterval: 10000,
  });
}

export function useMobileInstitutionalFlow() {
  return useQuery({
    queryKey: ['mobileInstitutionalFlow'],
    queryFn: () =>
      fetchJsonWithOfflineFallback<MobileInstitutionalFlow>(
        '/api/mobile/institutional-flow',
        () => getOfflineMobileInstitutionalFlow()
      ),
    refetchInterval: 15000,
  });
}

export function useMobilePostTradeGuide() {
  return useQuery({
    queryKey: ['mobilePostTradeGuide'],
    queryFn: () =>
      fetchJsonWithOfflineFallback<MobilePostTradeGuide>(
        '/api/mobile/post-trade',
        () => getOfflineMobilePostTradeGuide()
      ),
    refetchInterval: 15000,
  });
}

export function useMobileRiskDeepDive() {
  return useQuery({
    queryKey: ['mobileRiskDeepDive'],
    queryFn: () =>
      fetchJsonWithOfflineFallback<MobileRiskDeepDive>(
        '/api/mobile/risk/deep-dive',
        () => getOfflineMobileRiskDeepDive()
      ),
    refetchInterval: 15000,
  });
}

export function useMobileAssetClassGuide() {
  return useQuery({
    queryKey: ['mobileAssetClassGuide'],
    queryFn: () =>
      fetchJsonWithOfflineFallback<MobileAssetClassGuide>(
        '/api/mobile/asset-classes',
        () => getOfflineMobileAssetClassGuide()
      ),
    refetchInterval: 30000,
  });
}

export function useMobileOperationsGuide() {
  return useQuery({
    queryKey: ['mobileOperationsGuide'],
    queryFn: () =>
      fetchJsonWithOfflineFallback<MobileOperationsGuide>(
        '/api/mobile/operations',
        () => getOfflineMobileOperationsGuide()
      ),
    refetchInterval: 10000,
  });
}

export function useMobileDrill(drillId: string | null) {
  return useQuery({
    queryKey: ['mobileDrill', drillId],
    queryFn: () =>
      fetchJsonWithOfflineFallback<MobileDrillDetail>(`/api/mobile/drills/${drillId}`, () => getOfflineMobileDrill(drillId as string)),
    enabled: !!drillId,
  });
}

export function useUpdateMobileProgress() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (request: MobileProgressUpdateRequest) =>
      fetchJsonWithOfflineFallback<MobileProgressResponse>(
        '/api/mobile/progress',
        () => applyOfflineMobileProgress(request),
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify(request),
        }
      ),
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
      fetchJsonWithOfflineFallback<MobileDrillProgressResponse>(
        '/api/mobile/drills/attempt',
        () => applyOfflineMobileDrillAttempt(request),
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify(request),
        }
      ),
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
    queryFn: () =>
      fetchJsonWithOfflineFallback<MobileRiskScenario[]>('/api/mobile/risk/scenarios', () => getOfflineMobileRiskScenarios()),
  });
}

export function useEvaluateMobileRisk() {
  return useMutation({
    mutationFn: (request: MobileRiskEvaluateRequest) =>
      fetchJsonWithOfflineFallback<MobileRiskEvaluation>(
        '/api/mobile/risk/evaluate',
        () => evaluateOfflineMobileRisk(request),
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify(request),
        }
      ),
  });
}

export function useEvaluateMobileOption() {
  return useMutation({
    mutationFn: (request: MobileOptionEvaluateRequest) =>
      fetchJsonWithOfflineFallback<MobileOptionEvaluation>(
        '/api/mobile/risk/options/evaluate',
        () => evaluateOfflineMobileOption(request),
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify(request),
        }
      ),
  });
}
