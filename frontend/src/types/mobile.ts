import type { Position } from './trading';

export interface MobileProgressCard {
  cardId: string;
  bookmarked: boolean;
  completed: boolean;
  masteryLevel: number;
  correctCount: number;
  incorrectCount: number;
  lastReviewedAt: number;
  nextReviewAt: number;
}

export interface MobileCardSummary {
  id: string;
  title: string;
  category: string;
  difficulty: string;
  bookmarked: boolean;
  due: boolean;
  progress: MobileProgressCard;
  route: string;
}

export interface MobileLearningAnchor {
  route: string;
  orderId: string | null;
  cardId: string | null;
  updatedAt: number;
}

export interface MobileHome {
  accountId: string;
  generatedAt: number;
  deliveryMode?: 'LIVE' | 'ON_DEVICE';
  continueLearning: {
    route: string;
    orderId: string | null;
    cardId: string | null;
    title: string;
    detail: string;
  };
  todaySuggestion: {
    label: string;
    title: string;
    detail: string;
    route: string;
  };
  mainlineStatus: {
    healthy: boolean;
    summary: string;
    omsState: string;
    backOfficeState: string;
    sequenceGapCount: number;
    pendingOrphanCount: number;
    deadLetterCount: number;
    focusNotes: string[];
  };
  recentOrders: Array<{
    orderId: string;
    symbol: string;
    symbolName: string;
    status: string;
    submittedAt: number;
    filledQuantity: number;
    remainingQuantity: number;
    learningFocus: string;
  }>;
  dueCards: MobileCardSummary[];
  bookmarks: MobileCardSummary[];
  quickActions: Array<{
    label: string;
    route: string;
    tone: string;
  }>;
  progress: {
    anchor: MobileLearningAnchor | null;
    dueCount: number;
    dueDrillCount: number;
    bookmarkedCount: number;
    completedCount: number;
  };
}

export interface MobileCardDetail {
  card: {
    id: string;
    title: string;
    category: string;
    difficulty: string;
    question: string;
    shortAnswer: string;
    longAnswer: string;
    businessContext?: string;
    decisionRule?: string;
    eventFlow?: string[];
    tradeoffs?: string[];
    operatorChecks?: string[];
    routes: string[];
    codeReferences: string[];
    keywords: string[];
    implementationAnchors?: MobileImplementationAnchor[];
  };
  progress: MobileProgressCard;
}

export interface MobileImplementationAnchor {
  title: string;
  path: string;
  focus: string;
  excerpt?: string | null;
}

export interface MobileProgressResponse {
  accountId: string;
  updatedAt: number;
  anchor: MobileLearningAnchor | null;
  dueCount: number;
  dueDrillCount: number;
  bookmarkedCount: number;
  completedCount: number;
  cards: MobileCardSummary[];
}

export interface MobileDrillProgress {
  drillId: string;
  attemptCount: number;
  lastAttemptAt: number;
  lastClarityScore: number;
  nextReviewAt: number;
  lastNote: string | null;
  audioDataUrl: string | null;
}

export interface MobileDrillSummary {
  id: string;
  title: string;
  category: string;
  due: boolean;
  progress: MobileDrillProgress;
}

export interface MobileDrillDetail {
  drill: {
    id: string;
    title: string;
    category: string;
    prompt: string;
    routes: string[];
    keywords: string[];
  };
  progress: MobileDrillProgress;
}

export interface MobileDrillAttemptRequest {
  drillId: string;
  clarityScore: number;
  note?: string | null;
  audioDataUrl?: string | null;
}

export interface MobileDrillProgressResponse {
  accountId: string;
  updatedAt: number;
  dueCount: number;
  drills: MobileDrillSummary[];
}

export interface MobileRiskScenario {
  id: string;
  title: string;
  description: string;
  targetSymbol: string | null;
  shockPercent: number;
  scope: string;
  assumptions: string[];
}

export interface MobileRiskEvaluation {
  accountId: string;
  scenarioId: string;
  title: string;
  description: string;
  evaluatedAt: number;
  portfolio: {
    currentMarketValue: number;
    shockedMarketValue: number;
    pnlDelta: number;
    cashBalance: number;
    realizedPnl: number;
  };
  positions: Array<{
    symbol: string;
    symbolName: string;
    netQty: number;
    avgPrice: number;
    currentPrice: number;
    shockedPrice: number;
    currentValue: number;
    shockedValue: number;
    pnlDelta: number;
    shockPercent: number;
  }>;
  historicalVar: {
    confidenceLevel: number;
    observationCount: number;
    varLoss: number;
    expectedShortfall: number;
    holdingPeriod: string;
    methodology: string;
  };
  hedgeComparison: {
    hedgeSymbol: string | null;
    hedgeRatio: number;
    unhedgedPnlDelta: number;
    hedgedPnlDelta: number;
    protectionAmount: number;
    note: string;
  };
  assumptions: string[];
}

export interface MobileProgressUpdateRequest {
  type: 'anchor' | 'bookmark' | 'review';
  route?: string;
  orderId?: string | null;
  cardId?: string | null;
  bookmarked?: boolean;
  correct?: boolean;
}

export interface MobileRiskEvaluateRequest {
  scenarioId?: string | null;
  targetSymbol?: string | null;
  customShockPercent?: number | null;
}

export interface MobileRouteState {
  section: 'home' | 'orders' | 'ledger' | 'architecture' | 'cards' | 'drills' | 'risk';
  orderId: string | null;
  cardId: string | null;
  drillId: string | null;
}

export interface MobilePortfolioSnapshot {
  positions: Position[];
}

export interface MobileOptionEvaluateRequest {
  symbol?: string | null;
  optionType?: 'CALL' | 'PUT';
  spotPrice?: number | null;
  strikePrice?: number | null;
  volatilityPercent?: number | null;
  ratePercent?: number | null;
  maturityDays?: number | null;
  contracts?: number | null;
}

export interface MobileOptionEvaluation {
  symbol: string;
  symbolName: string;
  optionType: 'CALL' | 'PUT';
  spotPrice: number;
  strikePrice: number;
  volatilityPercent: number;
  ratePercent: number;
  maturityDays: number;
  contracts: number;
  optionPrice: number;
  totalPremium: number;
  greeks: {
    delta: number;
    gamma: number;
    vega: number;
    theta: number;
  };
  payoffCurve: Array<{
    underlyingPrice: number;
    payoff: number;
  }>;
  assumptions: string[];
}
