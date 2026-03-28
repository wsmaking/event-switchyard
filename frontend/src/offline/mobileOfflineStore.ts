import type {
  MobileCardDetail,
  MobileCardSummary,
  MobileDrillAttemptRequest,
  MobileDrillDetail,
  MobileDrillProgress,
  MobileDrillProgressResponse,
  MobileDrillSummary,
  MobileHome,
  MobileOptionEvaluateRequest,
  MobileOptionEvaluation,
  MobileProgressCard,
  MobileProgressResponse,
  MobileProgressUpdateRequest,
  MobileRiskEvaluateRequest,
  MobileRiskEvaluation,
  MobileRiskScenario,
} from '../types/mobile';
import { OrderSide, OrderStatus, OrderType, TimeInForce, type AccountOverview, type BalanceEffect, type BusStats, type Fill, type LedgerEntry, type OpsOverview, type Order, type OrderFinalOut, type OrderRequest, type OrderTimelineEntry, type OmsStats, type OmsReconcile, type BackOfficeStats, type BackOfficeReconcile, type PendingOrphanEntry, type DeadLetterEntry, type Position, type RawEvent, type Reservation } from '../types/trading';

const STORAGE_KEY = 'switchyard-mobile-offline-v2';
const ACCOUNT_ID = 'demo-001';
const DAY_MS = 24 * 60 * 60 * 1000;
const HOUR_MS = 60 * 60 * 1000;
const MINUTE_MS = 60 * 1000;

type MobileDeliveryMode = 'ON_DEVICE';

interface OfflineCard {
  id: string;
  title: string;
  category: string;
  difficulty: string;
  question: string;
  shortAnswer: string;
  longAnswer: string;
  routes: string[];
  codeReferences: string[];
  keywords: string[];
}

interface OfflineDrill {
  id: string;
  title: string;
  category: string;
  prompt: string;
  routes: string[];
  keywords: string[];
}

interface OfflineOrderBundle {
  order: Order;
  finalOut: OrderFinalOut;
  opsOverview: OpsOverview;
}

interface OfflineState {
  version: number;
  accountId: string;
  updatedAt: number;
  anchor: MobileHome['progress']['anchor'];
  cardProgress: Record<string, MobileProgressCard>;
  drillProgress: Record<string, MobileDrillProgress>;
  orders: Order[];
  finalOuts: Record<string, OrderFinalOut>;
  opsOverview: Record<string, OpsOverview>;
  accountOverview: AccountOverview;
  positions: Position[];
}

const SYMBOLS = {
  '7203': { name: 'トヨタ自動車', price: 2525, phase: 0.2 },
  '6758': { name: 'ソニーグループ', price: 13680, phase: 1.1 },
  '8306': { name: '三菱UFJフィナンシャル・グループ', price: 1205, phase: 2.4 },
  '9984': { name: 'ソフトバンクグループ', price: 6280, phase: 3.6 },
  '6861': { name: 'キーエンス', price: 52350, phase: 0.7 },
} as const;

const CARD_DEFINITIONS: OfflineCard[] = [
  {
    id: 'oms-vs-backoffice',
    title: 'OMS と BackOffice の境界',
    category: '設計',
    difficulty: 'medium',
    question: '同じ注文を見ているのに、なぜ OMS と BackOffice を分けるのか。',
    shortAnswer: 'OMS は注文状態の収束、BackOffice は cash / position / P&L / ledger の正本責務。',
    longAnswer:
      'OMS は accepted、partial、filled、canceled、expired の注文ライフサイクルを管理する。BackOffice は fills 起点で cash、reservation release、position、realized P&L、ledger を確定させる。両者を分けることで、注文状態の競合と会計整合の論点を独立に扱える。',
    routes: ['/mobile/orders', '/mobile/ledger'],
    codeReferences: [
      '/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/http/OrderApiHandler.java',
      '/Users/fujii/Desktop/dev/event-switchyard/backoffice-java/src/main/java/backofficejava/http/LedgerHttpHandler.java',
    ],
    keywords: ['注文状態管理', '台帳正本', '責務分離'],
  },
  {
    id: 'reservation-vs-margin',
    title: 'reservation と margin の違い',
    category: 'リスク',
    difficulty: 'medium',
    question: 'reservation をそのまま margin と呼ばない理由は何か。',
    shortAnswer: 'reservation は注文拘束。margin はポートフォリオ全体のリスク前提を使う別物。',
    longAnswer:
      'reservation は OMS が注文受理時に余力を拘束する operational control。margin はボラ、相関、保有期間などの前提を使う risk model。両者を混同すると、業務制御とリスク計算の境界が崩れる。',
    routes: ['/mobile/ledger', '/mobile/risk'],
    codeReferences: [
      '/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/demo/ReplayScenarioService.java',
      '/Users/fujii/Desktop/dev/event-switchyard/frontend/src/components/mobile/MobileRiskView.tsx',
    ],
    keywords: ['reservation', 'margin', 'risk model'],
  },
  {
    id: 'outbox-audit-bus',
    title: 'outbox / audit / bus を分ける理由',
    category: '設計',
    difficulty: 'hard',
    question: 'なぜ single stream ではなく、outbox、audit、bus を分けるのか。',
    shortAnswer: '再送、観測、下流連携の責務が違うから。',
    longAnswer:
      'outbox は gateway 内の durable emission 境界、audit は復元と説明責務、bus は下流 consumer への配信責務を持つ。1 本にまとめると fast path の責務と downstream reliability の責務が絡み、再送戦略や運用説明が曖昧になる。',
    routes: ['/mobile/architecture'],
    codeReferences: [
      '/Users/fujii/Desktop/dev/event-switchyard/gateway-rust/src/outbox',
      '/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/http/OpsApiHandler.java',
    ],
    keywords: ['outbox', 'audit', 'bus'],
  },
  {
    id: 'aggregate-seq-gap',
    title: 'aggregateSeq gap を保留する理由',
    category: '運用',
    difficulty: 'hard',
    question: 'aggregateSeq を見ずに届いた順に apply すると何が壊れるか。',
    shortAnswer: 'cancel / fill / reservation release の順序が崩れ、projection が壊れる。',
    longAnswer:
      'Java 側 projection は aggregateSeq を使って gap を pending orphan として保留する。これが無いと accepted 前に fill を会計反映する、partial fill より前に cancel complete を適用する、といった順序破綻が起きる。',
    routes: ['/mobile/architecture'],
    codeReferences: [
      '/Users/fujii/Desktop/dev/event-switchyard/oms-java/src/main/java/oms/audit/GatewayAuditIntakeService.java',
      '/Users/fujii/Desktop/dev/event-switchyard/backoffice-java/src/main/java/backofficejava/audit/GatewayAuditIntakeService.java',
    ],
    keywords: ['aggregateSeq', 'pending orphan', 'projection'],
  },
  {
    id: 'final-out-reading',
    title: 'final-out の読み方',
    category: '注文',
    difficulty: 'easy',
    question: 'final-out を見たとき、何から読むべきか。',
    shortAnswer: 'status と timeline、その次に reservation / fills / balance effect。',
    longAnswer:
      'まず order status と timeline で注文の物語を掴む。その後 reservation と fills を見て、最後に balance delta と positions で業務結果を確認する。raw events は説明の裏取りに使う。',
    routes: ['/mobile/orders', '/mobile/ledger'],
    codeReferences: [
      '/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/http/OrderApiHandler.java',
    ],
    keywords: ['final-out', 'timeline', 'balance effect'],
  },
  {
    id: 'pending-vs-dlq',
    title: 'pending orphan と DLQ の違い',
    category: '運用',
    difficulty: 'medium',
    question: 'pending orphan と DLQ はどう違うか。',
    shortAnswer: 'pending orphan は待てば解ける順序問題、DLQ は手当が必要な失敗イベント。',
    longAnswer:
      'aggregateSeq gap や accepted 未着の fill は pending orphan で保留し、前提イベント到着後に replay する。一方、解釈不能 payload や不整合は DLQ に落として operator が再投入や調査を行う。',
    routes: ['/mobile/architecture'],
    codeReferences: [
      '/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/http/OpsApiHandler.java',
    ],
    keywords: ['pending orphan', 'DLQ', 'requeue'],
  },
  {
    id: 'offset-vs-progress',
    title: 'offset と aggregate progress の違い',
    category: '運用',
    difficulty: 'medium',
    question: 'consumer offset が進んでいれば安全と言えない理由は何か。',
    shortAnswer: 'offset は読み取り位置、aggregate progress は適用済み sequence の位置。',
    longAnswer:
      'offset だけでは out-of-order や pending orphan が解けたかは分からない。aggregate progress とセットで見て初めて projection recovery を説明できる。',
    routes: ['/mobile/architecture'],
    codeReferences: [
      '/Users/fujii/Desktop/dev/event-switchyard/oms-java/src/main/resources/db/migration/V3__oms_aggregate_progress.sql',
      '/Users/fujii/Desktop/dev/event-switchyard/backoffice-java/src/main/resources/db/migration/V3__backoffice_aggregate_progress.sql',
    ],
    keywords: ['offset', 'checkpoint', 'aggregate progress'],
  },
  {
    id: 'ledger-truth',
    title: 'ledger が truth になる瞬間',
    category: '台帳',
    difficulty: 'medium',
    question: 'どのタイミングで cash / position / pnl を確定させるべきか。',
    shortAnswer: '注文 status ではなく fill 起点で ledger を起こす。',
    longAnswer:
      'accepted だけでは会計を確定できない。fill を受けて ledger entry を起こし、reservation release と合わせて cash / position / realized P&L を確定する。',
    routes: ['/mobile/ledger'],
    codeReferences: [
      '/Users/fujii/Desktop/dev/event-switchyard/backoffice-java/src/main/java/backofficejava/ledger',
    ],
    keywords: ['ledger', 'fill', 'cash delta'],
  },
  {
    id: 'realized-vs-unrealized',
    title: 'realized と unrealized の違い',
    category: '台帳',
    difficulty: 'easy',
    question: 'realized PnL と unrealized PnL をどう分けて説明するか。',
    shortAnswer: 'realized は約定で確定、unrealized は保有ポジションの評価差。',
    longAnswer:
      'BackOffice の realized PnL は fills と平均取得価格から確定する。一方 unrealized は current price を使った評価差であり、mark-to-market 前提に依存する。',
    routes: ['/mobile/ledger', '/mobile/risk'],
    codeReferences: [
      '/Users/fujii/Desktop/dev/event-switchyard/frontend/src/components/mobile/MobileOrderStudyView.tsx',
    ],
    keywords: ['realized', 'unrealized', 'mark-to-market'],
  },
  {
    id: 'historical-var-reading',
    title: 'historical VaR をどう読むか',
    category: 'リスク',
    difficulty: 'medium',
    question: 'historical VaR を見たとき、何を前提として確認すべきか。',
    shortAnswer: '窓、信頼水準、保有期間、データ品質、モデルの単純化。',
    longAnswer:
      '教育用 historical VaR でも、何本の履歴を使ったか、何% tail か、価格系列がどの頻度かを先に確認する。数字の大きさだけでなく前提の薄さを話せることが重要。',
    routes: ['/mobile/risk'],
    codeReferences: [
      '/Users/fujii/Desktop/dev/event-switchyard/frontend/src/components/mobile/MobileRiskView.tsx',
    ],
    keywords: ['historical VaR', 'confidence level', 'assumptions'],
  },
  {
    id: 'option-payoff-shape',
    title: 'option payoff の形を読む',
    category: 'デリバティブ',
    difficulty: 'medium',
    question: 'call / put の payoff curve を見たとき、最初に何を確認するべきか。',
    shortAnswer: '損益の非線形性、premium、break-even、水準ごとの傾き。',
    longAnswer:
      'option payoff は現物と違って損益が非線形に変わる。strike と premium の位置関係、break-even、spot が動いたときの傾きがどこで変わるかを押さえると、hedge intuition を作りやすい。',
    routes: ['/mobile/risk'],
    codeReferences: [
      '/Users/fujii/Desktop/dev/event-switchyard/frontend/src/components/mobile/MobileRiskView.tsx',
    ],
    keywords: ['option payoff', 'break-even', 'non-linearity'],
  },
  {
    id: 'cross-asset-boundary',
    title: 'asset class ごとに境界をずらす理由',
    category: 'クロスアセット',
    difficulty: 'hard',
    question: 'equities / FX / rates / credit で同じ OMS / risk / backoffice をそのまま使えない理由は何か。',
    shortAnswer: '価格モデル、約定単位、会計単位、運用イベントが違うから。',
    longAnswer:
      'asset class が変わると order lifecycle 自体は似ていても、risk driver、valuation、settlement、operator が見る障害が変わる。共通化する部分と専用化する部分を分けることが設計の本質になる。',
    routes: ['/mobile/cards', '/mobile/architecture'],
    codeReferences: [
      '/Users/fujii/Desktop/dev/event-switchyard/frontend/src/components/mobile/MobileArchitectureView.tsx',
      '/Users/fujii/Desktop/dev/event-switchyard/frontend/src/components/mobile/MobileCardsView.tsx',
    ],
    keywords: ['cross-asset', 'boundary', 'valuation'],
  },
];

const DRILL_DEFINITIONS: OfflineDrill[] = [
  {
    id: 'drill-order-flow',
    title: '注文から台帳までを 90 秒で説明する',
    category: '業務',
    prompt: 'UI から注文が入ってから final-out に至るまでを、Gateway / OMS / BackOffice / ledger の順で説明する。',
    routes: ['/mobile/orders', '/mobile/ledger', '/mobile/architecture'],
    keywords: ['注文受付', 'reservation', 'fill', 'ledger'],
  },
  {
    id: 'drill-sequence-gap',
    title: 'aggregateSeq gap を説明する',
    category: '運用',
    prompt: 'out-of-order なイベントが来たとき、pending orphan と DLQ をどう使い分けるか説明する。',
    routes: ['/mobile/architecture'],
    keywords: ['aggregateSeq', 'pending orphan', 'DLQ'],
  },
  {
    id: 'drill-ledger-narrative',
    title: 'ledger と P&L を一続きで説明する',
    category: '台帳',
    prompt: 'fill、reservation release、cash delta、position 更新、realized / unrealized P&L のつながりを順に説明する。',
    routes: ['/mobile/ledger'],
    keywords: ['ledger', 'P&L', 'reservation release'],
  },
  {
    id: 'drill-risk-assumptions',
    title: 'risk 数字の前提を説明する',
    category: 'リスク',
    prompt: 'stress / historical VaR / hedge comparison の数字に対して、何が前提で何が省略されているか説明する。',
    routes: ['/mobile/risk'],
    keywords: ['historical VaR', 'hedge comparison', 'assumption'],
  },
  {
    id: 'drill-options-greeks',
    title: 'option price と Greeks を説明する',
    category: 'デリバティブ',
    prompt: 'Black-Scholes の入力、price、delta、gamma、vega、theta が何を意味するかを説明する。',
    routes: ['/mobile/risk'],
    keywords: ['options', 'Greeks', 'Black-Scholes'],
  },
  {
    id: 'drill-cross-asset',
    title: 'asset class ごとの差分を説明する',
    category: 'クロスアセット',
    prompt: 'equities / FX / rates / credit の主要 risk driver と system boundary の違いを説明する。',
    routes: ['/mobile/cards'],
    keywords: ['equities', 'FX', 'rates', 'credit'],
  },
];

const RISK_SCENARIOS: MobileRiskScenario[] = [
  {
    id: 'market-down-3',
    title: 'Market down 3%',
    description: '全ポジションに -3% shock を適用',
    targetSymbol: null,
    shockPercent: -3,
    scope: 'portfolio',
    assumptions: ['教育用の単純 market shock', '相関と流動性は未考慮', 'reservation は margin ではない'],
  },
  {
    id: 'market-down-7',
    title: 'Market down 7%',
    description: '全ポジションに -7% shock を適用',
    targetSymbol: null,
    shockPercent: -7,
    scope: 'portfolio',
    assumptions: ['教育用の単純 market shock', '保有期間と信頼水準は未定義', 'realized / unrealized を分けて見る'],
  },
  {
    id: 'toyota-gap-down-12',
    title: 'Toyota gap down 12%',
    description: '7203 に -12% shock を適用',
    targetSymbol: '7203',
    shockPercent: -12,
    scope: 'single-name',
    assumptions: ['single-name shock', '他銘柄は不変', 'ギャップダウン時の集中リスク確認'],
  },
  {
    id: 'financials-down-9',
    title: 'MUFG gap down 9%',
    description: '8306 に -9% shock を適用',
    targetSymbol: '8306',
    shockPercent: -9,
    scope: 'single-name',
    assumptions: ['single-name shock', 'イベントドリブンの急変想定', '相関波及は省略'],
  },
];

export function getOfflineMobileHome(): MobileHome {
  const state = loadState();
  const now = Date.now();
  const cards = summarizeCards(state, now);
  const drills = summarizeDrills(state, now);
  const recentOrders = [...state.orders]
    .sort((left, right) => right.submittedAt - left.submittedAt)
    .slice(0, 6)
    .map((order) => ({
      orderId: order.id,
      symbol: order.symbol,
      symbolName: symbolName(order.symbol),
      status: order.status,
      submittedAt: order.submittedAt,
      filledQuantity: order.filledQuantity ?? 0,
      remainingQuantity: order.remainingQuantity ?? order.quantity,
      learningFocus: learningFocusForStatus(order.status),
    }));
  const anchor = state.anchor;
  const dueCards = cards.filter((card) => card.due).slice(0, 4);
  const bookmarks = cards.filter((card) => card.bookmarked).slice(0, 4);
  const continueRoute = anchor?.route
    ? anchor.route
    : recentOrders[0]
      ? `/mobile/orders/${recentOrders[0].orderId}`
      : '/mobile/cards';

  return {
    accountId: state.accountId,
    generatedAt: now,
    deliveryMode: 'ON_DEVICE',
    continueLearning: {
      route: continueRoute,
      orderId: anchor?.orderId ?? recentOrders[0]?.orderId ?? null,
      cardId: anchor?.cardId ?? dueCards[0]?.id ?? null,
      title: recentOrders[0] ? '端末内の学習セッションを再開' : '設計カードから再開',
      detail: recentOrders[0]
        ? 'backend なしで timeline と final-out を反復できる状態。'
        : '注文が無くても cards / drills / risk を端末内で続行できる。',
    },
    todaySuggestion: {
      label: '今日の 5 分テーマ',
      title: recentOrders[0] ? `${recentOrders[0].symbolName} の final-out を追う` : '設計カードの due を片付ける',
      detail: recentOrders[0]
        ? `${recentOrders[0].status} の order を見て、timeline と ledger rail を言葉にする。`
        : 'review queue を 5 枚回して、境界と判断理由を短く言う。',
      route: recentOrders[0] ? `/mobile/orders/${recentOrders[0].orderId}` : '/mobile/cards',
    },
    mainlineStatus: {
      healthy: true,
      summary: 'on-device pack / server 不要',
      omsState: 'ON_DEVICE',
      backOfficeState: 'ON_DEVICE',
      sequenceGapCount: 0,
      pendingOrphanCount: 0,
      deadLetterCount: 0,
      focusNotes: [
        '学習用データは端末内 local storage に保存',
        'network が無くても cards / drills / risk / replay scenario が使える',
        'live stack へ戻ると mobile API を優先し、失敗時のみ on-device pack に fallback',
      ],
    },
    recentOrders,
    dueCards,
    bookmarks,
    quickActions: [
      { label: '注文フローを見る', route: recentOrders[0] ? `/mobile/orders/${recentOrders[0].orderId}` : '/mobile/orders', tone: '注文' },
      { label: '台帳フローを見る', route: '/mobile/ledger', tone: '台帳' },
      { label: '構成を見る', route: '/mobile/architecture', tone: '運用' },
      { label: '設計カード', route: '/mobile/cards', tone: '設計' },
      { label: '説明ドリル', route: '/mobile/drills', tone: '反復' },
      { label: 'risk sandbox', route: '/mobile/risk', tone: 'リスク' },
    ],
    progress: {
      anchor,
      dueCount: cards.filter((card) => card.due).length,
      dueDrillCount: drills.filter((drill) => drill.due).length,
      bookmarkedCount: cards.filter((card) => card.bookmarked).length,
      completedCount: cards.filter((card) => card.progress.completed).length,
    },
  };
}

export function getOfflineMobileCards(): MobileCardSummary[] {
  return summarizeCards(loadState(), Date.now());
}

export function getOfflineMobileCard(cardId: string): MobileCardDetail {
  const card = CARD_DEFINITIONS.find((candidate) => candidate.id === cardId);
  if (!card) {
    throw new Error(`card_not_found:${cardId}`);
  }
  return {
    card,
    progress: getCardProgress(loadState(), cardId),
  };
}

export function getOfflineMobileProgress(): MobileProgressResponse {
  const state = loadState();
  const cards = summarizeCards(state, Date.now());
  const drills = summarizeDrills(state, Date.now());
  return {
    accountId: state.accountId,
    updatedAt: state.updatedAt,
    anchor: state.anchor,
    dueCount: cards.filter((card) => card.due).length,
    dueDrillCount: drills.filter((drill) => drill.due).length,
    bookmarkedCount: cards.filter((card) => card.bookmarked).length,
    completedCount: cards.filter((card) => card.progress.completed).length,
    cards,
  };
}

export function applyOfflineMobileProgress(request: MobileProgressUpdateRequest): MobileProgressResponse {
  const state = loadState();
  const now = Date.now();
  switch (request.type) {
    case 'anchor':
      state.anchor = {
        route: request.route ?? '/mobile',
        orderId: request.orderId ?? null,
        cardId: request.cardId ?? null,
        updatedAt: now,
      };
      break;
    case 'bookmark': {
      if (!request.cardId) {
        throw new Error('card_id_required');
      }
      const progress = getCardProgress(state, request.cardId);
      state.cardProgress[request.cardId] = {
        ...progress,
        bookmarked: Boolean(request.bookmarked),
      };
      break;
    }
    case 'review': {
      if (!request.cardId) {
        throw new Error('card_id_required');
      }
      const progress = getCardProgress(state, request.cardId);
      const correct = Boolean(request.correct);
      const nextMastery = clamp(progress.masteryLevel + (correct ? 1 : -1), 0, 5);
      state.cardProgress[request.cardId] = {
        ...progress,
        masteryLevel: nextMastery,
        correctCount: progress.correctCount + (correct ? 1 : 0),
        incorrectCount: progress.incorrectCount + (correct ? 0 : 1),
        completed: correct ? nextMastery >= 2 : false,
        lastReviewedAt: now,
        nextReviewAt: now + (correct ? nextCardReviewDelay(nextMastery) : 20 * MINUTE_MS),
      };
      break;
    }
    default:
      throw new Error(`unsupported_progress_type:${request.type}`);
  }
  state.updatedAt = now;
  saveState(state);
  return getOfflineMobileProgress();
}

export function getOfflineMobileDrills(): MobileDrillSummary[] {
  return summarizeDrills(loadState(), Date.now());
}

export function getOfflineMobileDrill(drillId: string): MobileDrillDetail {
  const drill = DRILL_DEFINITIONS.find((candidate) => candidate.id === drillId);
  if (!drill) {
    throw new Error(`drill_not_found:${drillId}`);
  }
  return {
    drill,
    progress: getDrillProgress(loadState(), drillId),
  };
}

export function applyOfflineMobileDrillAttempt(request: MobileDrillAttemptRequest): MobileDrillProgressResponse {
  if (!request.drillId) {
    throw new Error('drill_id_required');
  }
  const state = loadState();
  const now = Date.now();
  const progress = getDrillProgress(state, request.drillId);
  const clarity = clamp(request.clarityScore ?? 1, 0, 2);
  state.drillProgress[request.drillId] = {
    ...progress,
    attemptCount: progress.attemptCount + 1,
    lastAttemptAt: now,
    lastClarityScore: clarity,
    nextReviewAt: now + nextDrillReviewDelay(clarity, progress.attemptCount + 1),
    lastNote: request.note?.trim() ? request.note.trim() : null,
    audioDataUrl: request.audioDataUrl ?? null,
  };
  state.updatedAt = now;
  saveState(state);
  const drills = summarizeDrills(state, now);
  return {
    accountId: state.accountId,
    updatedAt: state.updatedAt,
    dueCount: drills.filter((drill) => drill.due).length,
    drills,
  };
}

export function getOfflineMobileRiskScenarios(): MobileRiskScenario[] {
  return [...RISK_SCENARIOS];
}

export function evaluateOfflineMobileRisk(request: MobileRiskEvaluateRequest): MobileRiskEvaluation {
  const state = loadState();
  const scenario = resolveRiskScenario(request);
  const positions = currentPortfolioPositions(state);
  const impacts = calculateRiskImpacts(positions, scenario, null, 0);
  const currentMarketValue = round2(impacts.reduce((total, item) => total + item.currentValue, 0));
  const shockedMarketValue = round2(impacts.reduce((total, item) => total + item.shockedValue, 0));
  const pnlDelta = round2(impacts.reduce((total, item) => total + item.pnlDelta, 0));
  const historicalVar = buildHistoricalVar(positions);
  const hedgeComparison = buildHedgeComparison(positions, scenario, pnlDelta);

  return {
    accountId: state.accountId,
    scenarioId: scenario.id,
    title: scenario.title,
    description: scenario.description,
    evaluatedAt: Date.now(),
    portfolio: {
      currentMarketValue,
      shockedMarketValue,
      pnlDelta,
      cashBalance: state.accountOverview.cashBalance,
      realizedPnl: state.accountOverview.realizedPnl,
    },
    positions: impacts,
    historicalVar,
    hedgeComparison,
    assumptions: [...scenario.assumptions, 'on-device pack の価格系列を使う教育用評価', '相関、流動性、skew、greeks cross-term は未考慮'],
  };
}

export function evaluateOfflineMobileOption(request: MobileOptionEvaluateRequest): MobileOptionEvaluation {
  const symbol = request.symbol?.trim() || '7203';
  const spot = request.spotPrice ?? currentPrice(symbol);
  const strike = request.strikePrice ?? round2(spot);
  const volatility = Math.max(0.01, (request.volatilityPercent ?? 24) / 100);
  const rate = (request.ratePercent ?? 0.5) / 100;
  const maturityDays = Math.max(1, request.maturityDays ?? 30);
  const contracts = Math.max(1, request.contracts ?? 1);
  const optionType = request.optionType === 'PUT' ? 'PUT' : 'CALL';
  const call = optionType === 'CALL';
  const t = maturityDays / 365;
  const sqrtT = Math.sqrt(t);
  const d1 = (Math.log(spot / strike) + (rate + 0.5 * volatility * volatility) * t) / (volatility * sqrtT);
  const d2 = d1 - volatility * sqrtT;
  const discountedStrike = strike * Math.exp(-rate * t);
  const optionPrice = call
    ? spot * cumulativeNormal(d1) - discountedStrike * cumulativeNormal(d2)
    : discountedStrike * cumulativeNormal(-d2) - spot * cumulativeNormal(-d1);
  const delta = call ? cumulativeNormal(d1) : cumulativeNormal(d1) - 1;
  const gamma = normalDensity(d1) / (spot * volatility * sqrtT);
  const vega = (spot * normalDensity(d1) * sqrtT) / 100;
  const theta =
    ((-spot * normalDensity(d1) * volatility) / (2 * sqrtT) -
      (call ? 1 : -1) * rate * discountedStrike * (call ? cumulativeNormal(d2) : cumulativeNormal(-d2))) /
    365;

  return {
    symbol,
    symbolName: symbolName(symbol),
    optionType,
    spotPrice: round2(spot),
    strikePrice: round2(strike),
    volatilityPercent: round2(volatility * 100),
    ratePercent: round2(rate * 100),
    maturityDays,
    contracts,
    optionPrice: round2(optionPrice),
    totalPremium: round2(optionPrice * contracts),
    greeks: {
      delta: round4(delta),
      gamma: round4(gamma),
      vega: round4(vega),
      theta: round4(theta),
    },
    payoffCurve: buildOptionPayoffCurve(call, strike, optionPrice, spot, contracts),
    assumptions: [
      'Black-Scholes の連続時間前提',
      'lognormal、一定ボラ、一定金利の簡易モデル',
      '配当、スキュー、流動性、早期行使は未考慮',
    ],
  };
}

export function getOfflineOrders(): Order[] {
  return [...loadState().orders].sort((left, right) => right.submittedAt - left.submittedAt);
}

export function getOfflineOrderFinalOut(orderId: string): OrderFinalOut {
  const state = loadState();
  const finalOut = state.finalOuts[orderId];
  if (!finalOut) {
    throw new Error(`order_not_found:${orderId}`);
  }
  return finalOut;
}

export function getOfflineOpsOverview(orderId: string | null): OpsOverview {
  const state = loadState();
  if (orderId && state.opsOverview[orderId]) {
    return state.opsOverview[orderId];
  }
  const mostRecentOrder = state.orders.slice().sort((left, right) => right.submittedAt - left.submittedAt)[0];
  if (mostRecentOrder && state.opsOverview[mostRecentOrder.id]) {
    return state.opsOverview[mostRecentOrder.id];
  }
  return createOpsOverview(null, state.accountOverview, state.positions, []);
}

export function runOfflineReplayScenario(input: { scenario: string; request: OrderRequest }): Order {
  const state = loadState();
  const bundle = buildReplayBundle(state, input.scenario, input.request);
  state.orders = [bundle.order, ...state.orders].sort((left, right) => right.submittedAt - left.submittedAt).slice(0, 24);
  state.finalOuts[bundle.order.id] = bundle.finalOut;
  state.opsOverview[bundle.order.id] = bundle.opsOverview;
  state.accountOverview = bundle.finalOut.accountOverview;
  state.positions = bundle.finalOut.positions;
  state.updatedAt = Date.now();
  state.anchor = {
    route: `/mobile/orders/${bundle.order.id}`,
    orderId: bundle.order.id,
    cardId: state.anchor?.cardId ?? null,
    updatedAt: state.updatedAt,
  };
  saveState(state);
  return bundle.order;
}

export function mobileOfflineMode(): MobileDeliveryMode {
  return 'ON_DEVICE';
}

function loadState(): OfflineState {
  const stored = readStorage();
  if (stored) {
    return stored;
  }
  const seed = createSeedState();
  saveState(seed);
  return seed;
}

function readStorage(): OfflineState | null {
  const storage = getBrowserStorage();
  if (!storage) {
    return null;
  }
  const raw = storage.getItem(STORAGE_KEY);
  if (!raw) {
    return null;
  }
  try {
    const parsed = JSON.parse(raw) as OfflineState;
    if (parsed.version !== 2) {
      return null;
    }
    return parsed;
  } catch {
    return null;
  }
}

function saveState(state: OfflineState) {
  const storage = getBrowserStorage();
  if (!storage) {
    return;
  }
  storage.setItem(STORAGE_KEY, JSON.stringify(state));
}

function getBrowserStorage(): Storage | null {
  if (typeof window === 'undefined') {
    return null;
  }
  try {
    return window.localStorage;
  } catch {
    return null;
  }
}

function createSeedState(): OfflineState {
  const now = Date.now();
  const baseAccount: AccountOverview = {
    accountId: ACCOUNT_ID,
    cashBalance: 11_428_500,
    availableBuyingPower: 11_428_500,
    reservedBuyingPower: 0,
    positionCount: 2,
    realizedPnl: 18_500,
    updatedAt: new Date(now).toISOString(),
  };
  const basePositions: Position[] = [
    toPosition('7203', 220, 2480),
    toPosition('8306', 600, 1180),
  ];
  const scenarios: Array<{ scenario: string; submittedAt: number; request: OrderRequest }> = [
    {
      scenario: 'filled',
      submittedAt: now - 12 * HOUR_MS,
      request: {
        symbol: '7203',
        side: OrderSide.BUY,
        type: OrderType.MARKET,
        quantity: 100,
        price: null,
        timeInForce: TimeInForce.GTC,
        expireAt: null,
      },
    },
    {
      scenario: 'partial-fill',
      submittedAt: now - 7 * HOUR_MS,
      request: {
        symbol: '8306',
        side: OrderSide.BUY,
        type: OrderType.LIMIT,
        quantity: 300,
        price: 1200,
        timeInForce: TimeInForce.GTC,
        expireAt: null,
      },
    },
    {
      scenario: 'canceled',
      submittedAt: now - 3 * HOUR_MS,
      request: {
        symbol: '6758',
        side: OrderSide.SELL,
        type: OrderType.LIMIT,
        quantity: 50,
        price: 13750,
        timeInForce: TimeInForce.GTC,
        expireAt: null,
      },
    },
    {
      scenario: 'rejected',
      submittedAt: now - HOUR_MS,
      request: {
        symbol: '9984',
        side: OrderSide.BUY,
        type: OrderType.MARKET,
        quantity: 200,
        price: null,
        timeInForce: TimeInForce.GTC,
        expireAt: null,
      },
    },
  ];

  const state: OfflineState = {
    version: 2,
    accountId: ACCOUNT_ID,
    updatedAt: now,
    anchor: null,
    cardProgress: {
      'oms-vs-backoffice': seededCardProgress('oms-vs-backoffice', now - DAY_MS, 2, true, true, 3, 1),
      'final-out-reading': seededCardProgress('final-out-reading', now - 2 * HOUR_MS, 1, false, false, 1, 0),
      'historical-var-reading': seededCardProgress('historical-var-reading', now - 6 * HOUR_MS, 0, false, false, 0, 1),
    },
    drillProgress: {
      'drill-order-flow': seededDrillProgress('drill-order-flow', now - DAY_MS, 2, 3, 'timeline -> reservation -> fill -> ledger の順で説明', null),
      'drill-sequence-gap': seededDrillProgress('drill-sequence-gap', now - 90 * MINUTE_MS, 0, 1, 'pending orphan と DLQ をまず分ける', null),
    },
    orders: [],
    finalOuts: {},
    opsOverview: {},
    accountOverview: baseAccount,
    positions: basePositions,
  };

  scenarios.forEach((entry, index) => {
    const bundle = buildReplayBundle(state, entry.scenario, entry.request, {
      submittedAt: entry.submittedAt,
      orderId: `seed-${index + 1}-${entry.scenario}`,
      mutateState: false,
    });
    state.orders.push(bundle.order);
    state.finalOuts[bundle.order.id] = bundle.finalOut;
    state.opsOverview[bundle.order.id] = bundle.opsOverview;
  });

  state.orders.sort((left, right) => right.submittedAt - left.submittedAt);
  state.anchor = {
    route: `/mobile/orders/${state.orders[0]?.id ?? ''}`,
    orderId: state.orders[0]?.id ?? null,
    cardId: 'oms-vs-backoffice',
    updatedAt: now,
  };
  return state;
}

function seededCardProgress(
  cardId: string,
  lastReviewedAt: number,
  masteryLevel: number,
  completed: boolean,
  bookmarked: boolean,
  correctCount: number,
  incorrectCount: number
): MobileProgressCard {
  return {
    cardId,
    bookmarked,
    completed,
    masteryLevel,
    correctCount,
    incorrectCount,
    lastReviewedAt,
    nextReviewAt: lastReviewedAt + (completed ? DAY_MS : HOUR_MS),
  };
}

function seededDrillProgress(
  drillId: string,
  lastAttemptAt: number,
  lastClarityScore: number,
  attemptCount: number,
  lastNote: string | null,
  audioDataUrl: string | null
): MobileDrillProgress {
  return {
    drillId,
    attemptCount,
    lastAttemptAt,
    lastClarityScore,
    nextReviewAt: lastAttemptAt + (lastClarityScore >= 2 ? DAY_MS : 2 * HOUR_MS),
    lastNote,
    audioDataUrl,
  };
}

function summarizeCards(state: OfflineState, now: number): MobileCardSummary[] {
  return CARD_DEFINITIONS.map((card) => {
    const progress = getCardProgress(state, card.id);
    return {
      id: card.id,
      title: card.title,
      category: card.category,
      difficulty: card.difficulty,
      bookmarked: progress.bookmarked,
      due: !progress.completed || progress.nextReviewAt <= now,
      progress,
      route: `/mobile/cards/${card.id}`,
    };
  }).sort((left, right) => Number(right.due) - Number(left.due) || right.progress.lastReviewedAt - left.progress.lastReviewedAt);
}

function summarizeDrills(state: OfflineState, now: number): MobileDrillSummary[] {
  return DRILL_DEFINITIONS.map((drill) => {
    const progress = getDrillProgress(state, drill.id);
    return {
      id: drill.id,
      title: drill.title,
      category: drill.category,
      due: progress.nextReviewAt <= now,
      progress,
    };
  }).sort((left, right) => Number(right.due) - Number(left.due) || right.progress.lastAttemptAt - left.progress.lastAttemptAt);
}

function getCardProgress(state: OfflineState, cardId: string): MobileProgressCard {
  return state.cardProgress[cardId] ?? {
    cardId,
    bookmarked: false,
    completed: false,
    masteryLevel: 0,
    correctCount: 0,
    incorrectCount: 0,
    lastReviewedAt: 0,
    nextReviewAt: 0,
  };
}

function getDrillProgress(state: OfflineState, drillId: string): MobileDrillProgress {
  return state.drillProgress[drillId] ?? {
    drillId,
    attemptCount: 0,
    lastAttemptAt: 0,
    lastClarityScore: -1,
    nextReviewAt: 0,
    lastNote: null,
    audioDataUrl: null,
  };
}

function nextCardReviewDelay(masteryLevel: number): number {
  switch (masteryLevel) {
    case 0:
    case 1:
      return 6 * HOUR_MS;
    case 2:
      return DAY_MS;
    case 3:
      return 3 * DAY_MS;
    case 4:
      return 7 * DAY_MS;
    default:
      return 14 * DAY_MS;
  }
}

function nextDrillReviewDelay(clarityScore: number, attemptCount: number): number {
  if (clarityScore <= 0) {
    return 30 * MINUTE_MS;
  }
  if (clarityScore === 1) {
    return Math.min(DAY_MS, Math.max(HOUR_MS, attemptCount * 2 * HOUR_MS));
  }
  return Math.min(3 * DAY_MS, Math.max(4 * HOUR_MS, attemptCount * 6 * HOUR_MS));
}

function currentPortfolioPositions(state: OfflineState): Position[] {
  return state.positions.map((position) => toPosition(position.symbol, position.quantity, position.avgPrice));
}

function calculateRiskImpacts(
  positions: Position[],
  scenario: MobileRiskScenario,
  hedgeSymbol: string | null,
  hedgeRatio: number
) {
  return positions
    .filter((position) => position.quantity !== 0)
    .map((position) => {
      const quantityFactor = hedgeSymbol && hedgeSymbol === position.symbol ? Math.max(0, 1 - hedgeRatio) : 1;
      const netQty = Math.round(position.quantity * quantityFactor);
      const current = currentPrice(position.symbol);
      const shockPercent = scenario.scope === 'single-name'
        ? position.symbol === scenario.targetSymbol
          ? scenario.shockPercent
          : 0
        : scenario.shockPercent;
      const shocked = round2(current * (1 + shockPercent / 100));
      const currentValue = round2(current * netQty);
      const shockedValue = round2(shocked * netQty);
      return {
        symbol: position.symbol,
        symbolName: symbolName(position.symbol),
        netQty,
        avgPrice: round2(position.avgPrice),
        currentPrice: round2(current),
        shockedPrice: shocked,
        currentValue,
        shockedValue,
        pnlDelta: round2(shockedValue - currentValue),
        shockPercent,
      };
    })
    .sort((left, right) => Math.abs(right.pnlDelta) - Math.abs(left.pnlDelta));
}

function buildHistoricalVar(positions: Position[]) {
  const active = positions.filter((position) => position.quantity !== 0);
  if (active.length === 0) {
    return {
      confidenceLevel: 95,
      observationCount: 0,
      varLoss: 0,
      expectedShortfall: 0,
      holdingPeriod: '1 tick',
      methodology: 'insufficient_history',
    };
  }
  const samples: number[] = [];
  const histories = active.map((position) => buildPriceHistory(position.symbol, 120));
  const observationCount = Math.min(...histories.map((history) => history.length - 1));
  for (let offset = 0; offset < observationCount; offset += 1) {
    let pnl = 0;
    active.forEach((position, index) => {
      const history = histories[index];
      const previous = history[offset];
      const current = history[offset + 1];
      pnl += (current - previous) * position.quantity;
    });
    samples.push(round2(pnl));
  }
  samples.sort((left, right) => left - right);
  const tailSize = Math.max(1, Math.ceil(samples.length * 0.05));
  const tail = samples.slice(0, tailSize);
  return {
    confidenceLevel: 95,
    observationCount,
    varLoss: round2(Math.abs(Math.min(0, tail[tail.length - 1] ?? 0))),
    expectedShortfall: round2(tail.reduce((total, value) => total + Math.abs(Math.min(0, value)), 0) / tailSize),
    holdingPeriod: '1 tick',
    methodology: `過去 ${observationCount} 本の return`,
  };
}

function buildHedgeComparison(positions: Position[], scenario: MobileRiskScenario, unhedgedPnlDelta: number) {
  if (positions.length === 0) {
    return {
      hedgeSymbol: null,
      hedgeRatio: 0,
      unhedgedPnlDelta: round2(unhedgedPnlDelta),
      hedgedPnlDelta: round2(unhedgedPnlDelta),
      protectionAmount: 0,
      note: 'ポジションなし',
    };
  }
  const hedgeSymbol =
    scenario.targetSymbol ??
    positions
      .slice()
      .sort((left, right) => Math.abs(right.quantity * currentPrice(right.symbol)) - Math.abs(left.quantity * currentPrice(left.symbol)))[0]
      ?.symbol ??
    null;
  if (!hedgeSymbol) {
    return {
      hedgeSymbol: null,
      hedgeRatio: 0,
      unhedgedPnlDelta: round2(unhedgedPnlDelta),
      hedgedPnlDelta: round2(unhedgedPnlDelta),
      protectionAmount: 0,
      note: 'hedge symbol を決められませんでした',
    };
  }
  const hedgeRatio = 0.5;
  const hedged = calculateRiskImpacts(positions, scenario, hedgeSymbol, hedgeRatio);
  const hedgedPnlDelta = round2(hedged.reduce((total, item) => total + item.pnlDelta, 0));
  return {
    hedgeSymbol,
    hedgeRatio,
    unhedgedPnlDelta: round2(unhedgedPnlDelta),
    hedgedPnlDelta,
    protectionAmount: round2(Math.abs(unhedgedPnlDelta) - Math.abs(hedgedPnlDelta)),
    note: `${hedgeSymbol} のエクスポージャーを 50% 落とした教育用比較`,
  };
}

function resolveRiskScenario(request: MobileRiskEvaluateRequest): MobileRiskScenario {
  if (request.customShockPercent !== null && request.customShockPercent !== undefined) {
    const targetSymbol = request.targetSymbol?.trim() || null;
    return {
      id: 'custom',
      title: targetSymbol ? 'Custom single-name shock' : 'Custom market shock',
      description: targetSymbol
        ? `${targetSymbol} に ${request.customShockPercent}% shock を適用`
        : `全ポジションに ${request.customShockPercent}% shock を適用`,
      targetSymbol,
      shockPercent: request.customShockPercent,
      scope: targetSymbol ? 'single-name' : 'portfolio',
      assumptions: ['教育用の単純ショック', 'ボラ、相関、流動性、greeks は未考慮', 'reservation は margin ではない'],
    };
  }
  return RISK_SCENARIOS.find((scenario) => scenario.id === request.scenarioId) ?? RISK_SCENARIOS[0];
}

function buildReplayBundle(
  state: OfflineState,
  scenarioName: string,
  request: OrderRequest,
  options?: { submittedAt?: number; orderId?: string; mutateState?: boolean }
): OfflineOrderBundle {
  const now = options?.submittedAt ?? Date.now();
  const mutateState = options?.mutateState ?? true;
  const orderId = options?.orderId ?? `${scenarioName}-${typeof crypto !== 'undefined' && crypto.randomUUID ? crypto.randomUUID() : `${now}`}`;
  const reservePrice = request.price ?? currentPrice(request.symbol);
  const filledPrice = request.price ?? currentPrice(request.symbol);
  const fillQuantity = scenarioName === 'partial-fill' ? Math.max(1, Math.round(request.quantity * 0.4)) : request.quantity;
  const remainingQuantity = Math.max(0, request.quantity - (scenarioName === 'filled' ? request.quantity : scenarioName === 'partial-fill' ? fillQuantity : 0));
  const reserveAmount = round2(reservePrice * request.quantity);
  const remainingReserve = scenarioName === 'accepted'
    ? reserveAmount
    : scenarioName === 'partial-fill'
      ? round2(reservePrice * remainingQuantity)
      : 0;
  const releasedAmount = round2(Math.max(0, reserveAmount - remainingReserve));
  const existingAccount = state.accountOverview;
  const existingPositions = currentPortfolioPositions(state);

  let status: Order['status'] = OrderStatus.ACCEPTED;
  switch (scenarioName) {
    case 'partial-fill':
      status = OrderStatus.PARTIALLY_FILLED;
      break;
    case 'filled':
      status = OrderStatus.FILLED;
      break;
    case 'canceled':
      status = OrderStatus.CANCELED;
      break;
    case 'expired':
      status = OrderStatus.EXPIRED;
      break;
    case 'rejected':
      status = OrderStatus.REJECTED;
      break;
    default:
      status = OrderStatus.ACCEPTED;
      break;
  }

  const executionQuantity = status === OrderStatus.FILLED ? request.quantity : status === OrderStatus.PARTIALLY_FILLED ? fillQuantity : 0;
  const cashDelta = executionQuantity === 0
    ? 0
    : request.side === OrderSide.BUY
      ? -round2(filledPrice * executionQuantity)
      : round2(filledPrice * executionQuantity);
  const realizedPnlDelta = request.side === OrderSide.SELL && executionQuantity > 0
    ? round2((filledPrice - averagePriceFor(existingPositions, request.symbol)) * executionQuantity)
    : 0;
  const accountAfterDraft = applyAccount(existingAccount, {
    cashDelta,
    availableBuyingPowerDelta:
      status === OrderStatus.ACCEPTED
        ? -remainingReserve
        : status === OrderStatus.PARTIALLY_FILLED
          ? round2(cashDelta - remainingReserve)
          : status === OrderStatus.FILLED
            ? cashDelta
            : 0,
    reservedBuyingPowerDelta:
      status === OrderStatus.ACCEPTED
        ? remainingReserve
        : status === OrderStatus.PARTIALLY_FILLED
          ? remainingReserve
          : 0,
    realizedPnlDelta,
  });
  const positionsAfter = applyPositionChange(existingPositions, request.symbol, request.side, executionQuantity, filledPrice);
  const accountAfter: AccountOverview = {
    ...accountAfterDraft,
    positionCount: positionsAfter.length,
  };
  const order: Order = {
    id: orderId,
    accountId: state.accountId,
    symbol: request.symbol,
    side: request.side,
    type: request.type,
    quantity: request.quantity,
    price: request.price,
    status,
    timeInForce: request.timeInForce,
    expireAt: request.expireAt,
    submittedAt: now,
    filledAt: executionQuantity > 0 ? now + 2_000 : null,
    executionTimeMs: executionQuantity > 0 ? 18 : status === OrderStatus.REJECTED ? 5 : 11,
    statusReason: rejectionReason(status, scenarioName),
    filledQuantity: executionQuantity,
    remainingQuantity,
  };
  const balanceEffect: BalanceEffect = {
    cashDelta,
    availableBuyingPowerDelta:
      status === OrderStatus.ACCEPTED
        ? -remainingReserve
        : status === OrderStatus.PARTIALLY_FILLED
          ? round2(cashDelta - remainingReserve)
          : status === OrderStatus.FILLED
            ? cashDelta
            : 0,
    reservedBuyingPowerDelta:
      status === OrderStatus.ACCEPTED
        ? remainingReserve
        : status === OrderStatus.PARTIALLY_FILLED
          ? remainingReserve
          : 0,
    realizedPnlDelta,
  };
  const reservations = createReservations({
    status,
    orderId,
    symbol: request.symbol,
    side: request.side,
    quantity: request.quantity,
    reserveAmount,
    remainingReserve,
    releasedAmount,
    eventAt: now,
  });
  const fills = createFills({
    status,
    orderId,
    symbol: request.symbol,
    side: request.side,
    quantity: executionQuantity,
    price: filledPrice,
    eventAt: now + 2_000,
  });
  const timeline = createTimeline(order, status, executionQuantity, filledPrice, remainingQuantity, reserveAmount);
  const rawEvents = createRawEvents(orderId, timeline);
  const ledgerEntries = createLedgerEntries(order, balanceEffect, fills, reservations, rawEvents);
  const finalOut: OrderFinalOut = {
    order,
    accountOverview: mutateState ? accountAfter : snapshotAccountForScenario(status, existingAccount, accountAfter),
    balanceEffect,
    reservations,
    fills,
    positions: mutateState ? positionsAfter : snapshotPositionsForScenario(status, existingPositions, positionsAfter),
    timeline,
    rawEvents,
  };
  const opsOverview = createOpsOverview(orderId, finalOut.accountOverview, finalOut.positions, ledgerEntries);
  return { order, finalOut, opsOverview };
}

function snapshotAccountForScenario(
  status: OrderStatus,
  before: AccountOverview,
  after: AccountOverview
): AccountOverview {
  if (status === OrderStatus.ACCEPTED || status === OrderStatus.PARTIALLY_FILLED || status === OrderStatus.FILLED) {
    return after;
  }
  return {
    ...before,
    updatedAt: after.updatedAt,
  };
}

function snapshotPositionsForScenario(status: OrderStatus, before: Position[], after: Position[]): Position[] {
  if (status === OrderStatus.ACCEPTED || status === OrderStatus.PARTIALLY_FILLED || status === OrderStatus.FILLED) {
    return after;
  }
  return before;
}

function createReservations(input: {
  status: OrderStatus;
  orderId: string;
  symbol: string;
  side: OrderSide;
  quantity: number;
  reserveAmount: number;
  remainingReserve: number;
  releasedAmount: number;
  eventAt: number;
}): Reservation[] {
  if (input.status === OrderStatus.REJECTED) {
    return [];
  }
  return [
    {
      reservationId: `reservation-${input.orderId}`,
      accountId: ACCOUNT_ID,
      orderId: input.orderId,
      symbol: input.symbol,
      side: input.side,
      reservedQuantity: input.status === OrderStatus.PARTIALLY_FILLED ? input.quantity - Math.round(input.releasedAmount / Math.max(1, input.reserveAmount / input.quantity)) : input.quantity,
      reservedAmount: input.reserveAmount,
      releasedAmount: input.releasedAmount,
      status:
        input.status === OrderStatus.ACCEPTED
          ? 'OPEN'
          : input.status === OrderStatus.PARTIALLY_FILLED
            ? 'PARTIAL_RELEASED'
            : 'RELEASED',
      openedAt: input.eventAt,
      updatedAt: input.eventAt + 2_000,
    },
  ];
}

function createFills(input: {
  status: OrderStatus;
  orderId: string;
  symbol: string;
  side: OrderSide;
  quantity: number;
  price: number;
  eventAt: number;
}): Fill[] {
  if (input.status !== OrderStatus.PARTIALLY_FILLED && input.status !== OrderStatus.FILLED) {
    return [];
  }
  return [
    {
      fillId: `fill-${input.orderId}`,
      orderId: input.orderId,
      accountId: ACCOUNT_ID,
      symbol: input.symbol,
      side: input.side,
      quantity: input.quantity,
      price: input.price,
      notional: round2(input.quantity * input.price),
      liquidity: 'TAKER',
      filledAt: input.eventAt,
    },
  ];
}

function createTimeline(
  order: Order,
  status: OrderStatus,
  executionQuantity: number,
  filledPrice: number,
  remainingQuantity: number,
  reserveAmount: number
): OrderTimelineEntry[] {
  const entries: OrderTimelineEntry[] = [
    {
      eventType: 'OrderSubmitted',
      eventAt: order.submittedAt,
      label: '注文送信',
      detail: `${order.side} ${order.symbol} ${order.quantity} 株を送信。`,
    },
  ];
  if (status === OrderStatus.REJECTED) {
    entries.push({
      eventType: 'OrderRejected',
      eventAt: order.submittedAt + 400,
      label: 'Reject',
      detail: order.statusReason ?? '受理前に reject。',
    });
    return entries;
  }
  entries.push({
    eventType: 'OrderAccepted',
    eventAt: order.submittedAt + 800,
    label: 'Accepted',
    detail: `reservation ${reserveAmount.toLocaleString()} を確保。`,
  });
  if (status === OrderStatus.PARTIALLY_FILLED) {
    entries.push({
      eventType: 'ExecutionReport',
      eventAt: order.submittedAt + 2_000,
      label: 'Partial Fill',
      detail: `${executionQuantity} 株 @ ${filledPrice.toLocaleString()} / remaining ${remainingQuantity} 株。`,
    });
  } else if (status === OrderStatus.FILLED) {
    entries.push({
      eventType: 'ExecutionReport',
      eventAt: order.submittedAt + 2_000,
      label: 'Filled',
      detail: `${executionQuantity} 株 @ ${filledPrice.toLocaleString()} で全量約定。`,
    });
  } else if (status === OrderStatus.CANCELED) {
    entries.push({
      eventType: 'CancelRequested',
      eventAt: order.submittedAt + 1_500,
      label: 'Cancel Request',
      detail: 'working order を cancel。venue へ control を送信。',
    });
    entries.push({
      eventType: 'CancelConfirmed',
      eventAt: order.submittedAt + 2_700,
      label: 'Canceled',
      detail: '残拘束を release して terminal へ遷移。',
    });
  } else if (status === OrderStatus.EXPIRED) {
    entries.push({
      eventType: 'OrderExpired',
      eventAt: order.submittedAt + 2_500,
      label: 'Expired',
      detail: '期限切れで working order を終了。',
    });
  }
  return entries;
}

function createRawEvents(orderId: string, timeline: OrderTimelineEntry[]): RawEvent[] {
  return timeline.map((entry, index) => ({
    orderId,
    eventType: entry.eventType,
    eventAt: entry.eventAt,
    label: entry.label,
    detail: entry.detail,
    source: index === 0 ? 'frontend' : entry.eventType === 'ExecutionReport' ? 'venue' : 'oms',
    eventRef: `${orderId}-event-${index + 1}`,
  }));
}

function createLedgerEntries(
  order: Order,
  balanceEffect: BalanceEffect,
  fills: Fill[],
  reservations: Reservation[],
  rawEvents: RawEvent[]
): LedgerEntry[] {
  const entries: LedgerEntry[] = [];
  if (reservations[0]) {
    const reservation = reservations[0];
    entries.push({
      entryId: `ledger-${order.id}-reservation`,
      eventRef: rawEvents[1]?.eventRef ?? rawEvents[0].eventRef,
      accountId: ACCOUNT_ID,
      orderId: order.id,
      eventType: 'ReservationApplied',
      symbol: order.symbol,
      side: order.side,
      quantityDelta: 0,
      cashDelta: 0,
      reservedBuyingPowerDelta: round2(reservation.reservedAmount - reservation.releasedAmount),
      realizedPnlDelta: 0,
      detail: `reservation ${reservation.status}`,
      eventAt: rawEvents[1]?.eventAt ?? order.submittedAt,
      source: 'oms-java',
    });
  }
  fills.forEach((fill, index) => {
    entries.push({
      entryId: `ledger-${order.id}-fill-${index + 1}`,
      eventRef: rawEvents.find((event) => event.eventType === 'ExecutionReport')?.eventRef ?? rawEvents[0].eventRef,
      accountId: ACCOUNT_ID,
      orderId: order.id,
      eventType: 'FillApplied',
      symbol: order.symbol,
      side: order.side,
      quantityDelta: order.side === OrderSide.BUY ? fill.quantity : -fill.quantity,
      cashDelta: order.side === OrderSide.BUY ? -fill.notional : fill.notional,
      reservedBuyingPowerDelta: round2(balanceEffect.reservedBuyingPowerDelta),
      realizedPnlDelta: round2(balanceEffect.realizedPnlDelta),
      detail: `${fill.quantity} 株 @ ${fill.price}`,
      eventAt: fill.filledAt,
      source: 'backoffice-java',
    });
  });
  return entries;
}

function createOpsOverview(
  orderId: string | null,
  accountOverview: AccountOverview,
  positions: Position[],
  ledgerEntries: LedgerEntry[]
): OpsOverview {
  const nowIso = new Date().toISOString();
  const omsStats: OmsStats = {
    enabled: true,
    state: 'ON_DEVICE',
    auditPath: 'on-device-pack',
    offsetPath: 'local-storage',
    startMode: 'offline-seed',
    startedAt: nowIso,
    processed: 0,
    skipped: 0,
    duplicates: 0,
    orphans: 0,
    sequenceGaps: 0,
    replays: 0,
    lastEventAt: Date.now(),
    currentOffset: 0,
    currentAuditSize: 0,
    deadLetterCount: 0,
    pendingOrphanCount: 0,
    aggregateProgressCount: 1,
  };
  const backOfficeStats: BackOfficeStats = {
    enabled: true,
    state: 'ON_DEVICE',
    auditPath: 'on-device-pack',
    offsetPath: 'local-storage',
    startMode: 'offline-seed',
    startedAt: nowIso,
    processed: 0,
    skipped: 0,
    duplicates: 0,
    orphans: 0,
    sequenceGaps: 0,
    replays: 0,
    lastEventAt: Date.now(),
    currentOffset: 0,
    currentAuditSize: 0,
    ledgerEntryCount: ledgerEntries.length,
    deadLetterCount: 0,
    pendingOrphanCount: 0,
    aggregateProgressCount: 1,
  };
  const busStats: BusStats = {
    enabled: true,
    kafkaEnabled: false,
    state: 'ON_DEVICE',
    topic: 'on-device-pack',
    groupId: 'mobile-learning',
    startedAt: nowIso,
    received: 0,
    applied: 0,
    duplicates: 0,
    pending: 0,
    deadLetters: 0,
    errors: 0,
    lastEventAt: Date.now(),
  };
  const omsReconcile: OmsReconcile = {
    accountId: ACCOUNT_ID,
    totalOrders: orderId ? 1 : 0,
    openOrders: 0,
    expectedReservedAmount: accountOverview.reservedBuyingPower,
    actualReservedAmount: accountOverview.reservedBuyingPower,
    reservedGapAmount: 0,
    issues: [],
  };
  const backOfficeReconcile: BackOfficeReconcile = {
    accountId: ACCOUNT_ID,
    cashBalance: accountOverview.cashBalance,
    availableBuyingPower: accountOverview.availableBuyingPower,
    reservedBuyingPower: accountOverview.reservedBuyingPower,
    realizedPnl: accountOverview.realizedPnl,
    expectedCashBalance: accountOverview.cashBalance,
    expectedReservedBuyingPower: accountOverview.reservedBuyingPower,
    expectedRealizedPnl: accountOverview.realizedPnl,
    positions: positions.map((position) => ({
      accountId: ACCOUNT_ID,
      symbol: position.symbol,
      netQty: position.quantity,
      avgPrice: position.avgPrice,
    })),
    issues: [],
  };
  return {
    accountId: ACCOUNT_ID,
    orderId,
    omsStats,
    omsBusStats: busStats,
    omsReconcile,
    omsOrphans: [] as DeadLetterEntry[],
    omsPendingOrphans: [] as PendingOrphanEntry[],
    backOfficeStats,
    backOfficeBusStats: busStats,
    backOfficeReconcile,
    ledgerEntries,
    backOfficeOrphans: [] as DeadLetterEntry[],
    backOfficePendingOrphans: [] as PendingOrphanEntry[],
  };
}

function applyAccount(account: AccountOverview, effect: BalanceEffect): AccountOverview {
  return {
    accountId: account.accountId,
    cashBalance: round2(account.cashBalance + effect.cashDelta),
    availableBuyingPower: round2(Math.max(0, account.availableBuyingPower + effect.availableBuyingPowerDelta)),
    reservedBuyingPower: round2(Math.max(0, account.reservedBuyingPower + effect.reservedBuyingPowerDelta)),
    positionCount: account.positionCount,
    realizedPnl: round2(account.realizedPnl + effect.realizedPnlDelta),
    updatedAt: new Date().toISOString(),
  };
}

function applyPositionChange(
  positions: Position[],
  symbol: string,
  side: OrderSide,
  quantity: number,
  price: number
): Position[] {
  if (quantity <= 0) {
    return positions.map((position) => toPosition(position.symbol, position.quantity, position.avgPrice));
  }
  const next = new Map(positions.map((position) => [position.symbol, { quantity: position.quantity, avgPrice: position.avgPrice }]));
  const current = next.get(symbol) ?? { quantity: 0, avgPrice: price };
  if (side === OrderSide.BUY) {
    const newQuantity = current.quantity + quantity;
    const avgPrice = newQuantity === 0
      ? price
      : round2(((current.quantity * current.avgPrice) + (quantity * price)) / newQuantity);
    next.set(symbol, { quantity: newQuantity, avgPrice });
  } else {
    const newQuantity = current.quantity - quantity;
    if (newQuantity <= 0) {
      next.delete(symbol);
    } else {
      next.set(symbol, { quantity: newQuantity, avgPrice: current.avgPrice });
    }
  }
  return [...next.entries()]
    .map(([nextSymbol, value]) => toPosition(nextSymbol, value.quantity, value.avgPrice))
    .sort((left, right) => left.symbol.localeCompare(right.symbol));
}

function toPosition(symbol: string, quantity: number, avgPrice: number): Position {
  const market = currentPrice(symbol);
  const unrealizedPnL = round2((market - avgPrice) * quantity);
  const denominator = avgPrice * Math.max(1, quantity);
  return {
    symbol,
    quantity,
    avgPrice: round2(avgPrice),
    currentPrice: round2(market),
    unrealizedPnL,
    unrealizedPnLPercent: denominator === 0 ? 0 : round2((unrealizedPnL / denominator) * 100),
  };
}

function currentPrice(symbol: string): number {
  return SYMBOLS[symbol as keyof typeof SYMBOLS]?.price ?? 1000;
}

function symbolName(symbol: string): string {
  return SYMBOLS[symbol as keyof typeof SYMBOLS]?.name ?? symbol;
}

function averagePriceFor(positions: Position[], symbol: string): number {
  return positions.find((position) => position.symbol === symbol)?.avgPrice ?? currentPrice(symbol);
}

function buildPriceHistory(symbol: string, count: number): number[] {
  const spec = SYMBOLS[symbol as keyof typeof SYMBOLS] ?? { name: symbol, price: 1000, phase: 0 };
  return Array.from({ length: count }, (_, index) => {
    const wave = Math.sin(index * 0.14 + spec.phase) * 0.018;
    const drift = Math.cos(index * 0.06 + spec.phase) * 0.007;
    return round2(spec.price * (1 + wave + drift));
  });
}

function buildOptionPayoffCurve(call: boolean, strike: number, premium: number, spot: number, contracts: number) {
  return Array.from({ length: 9 }, (_, index) => index - 4).map((step) => {
    const underlyingPrice = round2(spot * (1 + step * 0.08));
    const intrinsic = call ? Math.max(0, underlyingPrice - strike) : Math.max(0, strike - underlyingPrice);
    return {
      underlyingPrice,
      payoff: round2((intrinsic - premium) * contracts),
    };
  });
}

function learningFocusForStatus(status: string): string {
  switch (status) {
    case OrderStatus.FILLED:
      return 'timeline、fills、balance effect を一気に読む。';
    case OrderStatus.PARTIALLY_FILLED:
      return 'working quantity と reservation 縮小の関係を見る。';
    case OrderStatus.CANCELED:
      return 'cancel request から release までの遷移を見る。';
    case OrderStatus.REJECTED:
      return 'hot path reject と会計影響なしの境界を見る。';
    default:
      return 'accepted から terminal までの責務分離を見る。';
  }
}

function rejectionReason(status: OrderStatus, scenarioName: string): string | null {
  if (status !== OrderStatus.REJECTED) {
    return null;
  }
  if (scenarioName === 'rejected') {
    return 'RISK_LIMIT_EXCEEDED';
  }
  return 'SCENARIO_REJECTED';
}

function cumulativeNormal(value: number): number {
  return 0.5 * (1 + erf(value / Math.sqrt(2)));
}

function normalDensity(value: number): number {
  return Math.exp(-0.5 * value * value) / Math.sqrt(2 * Math.PI);
}

function erf(value: number): number {
  const sign = Math.sign(value);
  const abs = Math.abs(value);
  const t = 1 / (1 + 0.3275911 * abs);
  const polynomial =
    (((((1.061405429 * t - 1.453152027) * t + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t);
  return sign * (1 - polynomial * Math.exp(-abs * abs));
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function round2(value: number): number {
  return Math.round(value * 100) / 100;
}

function round4(value: number): number {
  return Math.round(value * 10_000) / 10_000;
}
