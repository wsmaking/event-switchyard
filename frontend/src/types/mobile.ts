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
  section:
    | 'home'
    | 'orders'
    | 'market'
    | 'institutional'
    | 'posttrade'
    | 'ledger'
    | 'architecture'
    | 'assets'
    | 'operations'
    | 'cards'
    | 'drills'
    | 'risk';
  orderId: string | null;
  symbol: string | null;
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

export interface MobileInstitutionalFlow {
  generatedAt: number;
  anchorOrderId: string | null;
  symbol: string;
  symbolName: string;
  clientIntent: string;
  executionStyles: Array<{
    name: string;
    useCase: string;
    businessRule: string;
    systemImplication: string;
    tradeoffs: string[];
  }>;
  parentOrderPlan: {
    side: string;
    totalQuantity: number;
    arrivalMidPrice: number;
    targetParticipationPercent: number;
    scheduleWindowMinutes: number;
    chosenStyle: string;
    whyNotOtherChoices: string[];
  };
  childOrders: Array<{
    id: string;
    venueIntent: string;
    plannedQuantity: number;
    benchmarkPrice: number;
    expectedFillPrice: number;
    expectedSlippageBps: number;
    timeBucketLabel: string;
    learningPoint: string;
  }>;
  allocationPlan: {
    blockBook: string;
    averagePrice: number;
    totalQuantity: number;
    allocations: Array<{
      targetBook: string;
      quantity: number;
      ratioPercent: number;
      note: string;
    }>;
    settlementNote: string;
    controlChecks: string[];
  };
  parentExecutionState: {
    parentStatus: string;
    executionStyle: string;
    targetQuantity: number;
    executedQuantity: number;
    remainingQuantity: number;
    participationTargetPercent: number;
    participationActualPercent: number;
    scheduleWindowLabel: string;
    childStates: Array<{
      childId: string;
      state: string;
      venueIntent: string;
      plannedQuantity: number;
      executedQuantity: number;
      remainingQuantity: number;
      benchmarkPrice: number;
      averageFillPrice: number;
      slippageBps: number;
      nextAction: string;
    }>;
    operatorAlerts: string[];
  } | null;
  allocationState: {
    allocationStatus: string;
    allocatedQuantity: number;
    pendingQuantity: number;
    allocationAveragePrice: number;
    books: Array<{
      book: string;
      targetQuantity: number;
      allocatedQuantity: number;
      status: string;
      rationale: string;
    }>;
    controls: string[];
  } | null;
  operatorChecks: string[];
  implementationAnchors: MobileImplementationAnchor[];
}

export interface MobilePostTradeGuide {
  generatedAt: number;
  orderId: string | null;
  symbol: string | null;
  orderStatus: string;
  stages: Array<{
    name: string;
    owner: string;
    purpose: string;
    currentView: string;
    whyItMatters: string;
  }>;
  feeBreakdown: {
    grossNotional: number;
    commission: number;
    exchangeFee: number;
    taxes: number;
    netCashMovement: number;
    assumptions: string[];
  };
  statementPreview: {
    accountId: string;
    symbol: string;
    symbolName: string;
    settledQuantity: number;
    averagePrice: number;
    settlementDateLabel: string;
    netCashMovementLabel: string;
    notes: string[];
  };
  settlementChecks: Array<{
    title: string;
    rule: string;
    currentValue: string;
  }>;
  corporateActionHooks: Array<{
    name: string;
    businessImpact: string;
    systemImpact: string;
  }>;
  settlementProjection: {
    settlementStatus: string;
    tradeDateLabel: string;
    settlementDateLabel: string;
    grossNotional: number;
    netCashMovement: number;
    settledQuantity: number;
    cashLegStatus: string;
    securitiesLegStatus: string;
    exceptionFlags: string[];
    nextAction: string;
  } | null;
  statementProjection: {
    statementStatus: string;
    confirmReference: string;
    statementReference: string;
    customerFacingSummary: string;
    lines: Array<{
      label: string;
      value: string;
      note: string;
    }>;
    controls: string[];
  } | null;
  implementationAnchors: MobileImplementationAnchor[];
}

export interface MobileRiskDeepDive {
  generatedAt: number;
  accountId: string;
  marketValue: number;
  cashBalance: number;
  concentration: Array<{
    symbol: string;
    symbolName: string;
    exposure: number;
    weightPercent: number;
    note: string;
  }>;
  liquidity: Array<{
    symbol: string;
    symbolName: string;
    positionQuantity: number;
    visibleTopOfBookQuantity: number;
    participationPercent: number;
    estimatedDaysToExit: number;
    note: string;
  }>;
  scenarioLibrary: Array<{
    id: string;
    title: string;
    category: string;
    shock: string;
    rationale: string;
    focus: string;
  }>;
  backtesting: {
    observationCount: number;
    breachRatePercent: number;
    averageTailLoss: number;
    note: string;
    samples: Array<{
      label: string;
      pnl: number;
      breached: boolean;
    }>;
  };
  modelBoundaries: Array<{
    title: string;
    whyItMatters: string;
    whatIncluded: string;
    whatExcluded: string;
  }>;
  implementationAnchors: MobileImplementationAnchor[];
}

export interface MobileAssetClassGuide {
  generatedAt: number;
  assetClasses: Array<{
    assetClass: string;
    lifecycle: string;
    valuationDriver: string;
    settlementModel: string;
    riskDriver: string;
    operatorWatchpoints: string[];
    whatStaysCommon: string[];
    whatMustSpecialize: string[];
  }>;
  boundaryPrinciples: string[];
  implementationAnchors: MobileImplementationAnchor[];
}

export interface MobileOperationsGuide {
  generatedAt: number;
  liveState: {
    gatewayState: string;
    omsState: string;
    backOfficeState: string;
    marketDataState: string;
    schemaState: string;
    capacityState: string;
    sequenceGapCount: number;
    pendingOrphanCount: number;
    deadLetterCount: number;
    activeIncidentCount: number;
    activeIncidents: string[];
    reconcileNotes: string[];
  };
  sessionMonitors: Array<{
    name: string;
    state: string;
    whyItMatters: string;
    currentValue: string;
    checkpoints: string[];
    operatorActions: string[];
  }>;
  incidentDrills: Array<{
    name: string;
    trigger: string;
    firstQuestions: string[];
    actions: string[];
    severity: string;
    active: boolean;
    recoverySignal: string;
  }>;
  schemaControls: Array<{
    title: string;
    rule: string;
    failureMode: string;
    currentState: string;
    operatorChecks: string[];
  }>;
  capacityControls: Array<{
    title: string;
    metric: string;
    threshold: string;
    whyItMatters: string;
    currentValue: string;
    status: string;
    operatorActions: string[];
  }>;
  operatorSequence: string[];
  implementationAnchors: MobileImplementationAnchor[];
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
