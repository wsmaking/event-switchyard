export const OrderSide = {
  BUY: 'BUY',
  SELL: 'SELL',
} as const;

export type OrderSide = (typeof OrderSide)[keyof typeof OrderSide];

export const OrderType = {
  MARKET: 'MARKET',
  LIMIT: 'LIMIT',
} as const;

export type OrderType = (typeof OrderType)[keyof typeof OrderType];

export const OrderStatus = {
  PENDING_ACCEPT: 'PENDING_ACCEPT',
  ACCEPTED: 'ACCEPTED',
  PARTIALLY_FILLED: 'PARTIALLY_FILLED',
  FILLED: 'FILLED',
  CANCEL_PENDING: 'CANCEL_PENDING',
  CANCELED: 'CANCELED',
  EXPIRED: 'EXPIRED',
  REJECTED: 'REJECTED',
  AMEND_PENDING: 'AMEND_PENDING',
} as const;

export type OrderStatus = (typeof OrderStatus)[keyof typeof OrderStatus];

export const TimeInForce = {
  GTC: 'GTC',
  GTD: 'GTD',
} as const;

export type TimeInForce = (typeof TimeInForce)[keyof typeof TimeInForce];

export interface Order {
  id: string;
  accountId?: string;
  symbol: string;
  side: OrderSide;
  type: OrderType;
  quantity: number;
  price: number | null;
  status: OrderStatus;
  timeInForce: TimeInForce;
  expireAt: number | null;
  submittedAt: number;
  filledAt: number | null;
  executionTimeMs: number | null;
  statusReason?: string | null;
  filledQuantity?: number;
  remainingQuantity?: number;
}

export interface OrderRequest {
  symbol: string;
  side: OrderSide;
  type: OrderType;
  quantity: number;
  price: number | null;
  timeInForce: TimeInForce;
  expireAt: number | null;
}

export interface StockInfo {
  symbol: string;
  name: string;
  currentPrice: number;
  change: number;
  changePercent: number;
  high: number;
  low: number;
  volume: number;
}

export interface PricePoint {
  timestamp: number;
  price: number;
}

export interface Position {
  symbol: string;
  quantity: number;
  avgPrice: number;
  currentPrice: number;
  unrealizedPnL: number;
  unrealizedPnLPercent: number;
}

export interface AccountOverview {
  accountId: string;
  cashBalance: number;
  availableBuyingPower: number;
  reservedBuyingPower: number;
  positionCount: number;
  realizedPnl: number;
  updatedAt: string | null;
}

export interface OrderFinalOut {
  order: Order;
  accountOverview: AccountOverview;
  balanceEffect: BalanceEffect;
  reservations: Reservation[];
  fills: Fill[];
  positions: Position[];
  timeline: OrderTimelineEntry[];
  rawEvents: RawEvent[];
}

export interface OrderTimelineEntry {
  eventType: string;
  eventAt: number;
  label: string;
  detail: string;
}

export interface BalanceEffect {
  cashDelta: number;
  availableBuyingPowerDelta: number;
  reservedBuyingPowerDelta: number;
  realizedPnlDelta: number;
}

export interface Reservation {
  reservationId: string;
  accountId: string;
  orderId: string;
  symbol: string;
  side: OrderSide;
  reservedQuantity: number;
  reservedAmount: number;
  releasedAmount: number;
  status: string;
  openedAt: number;
  updatedAt: number;
}

export interface Fill {
  fillId: string;
  orderId: string;
  accountId: string;
  symbol: string;
  side: OrderSide;
  quantity: number;
  price: number;
  notional: number;
  liquidity: string;
  filledAt: number;
}

export interface RawEvent {
  orderId: string;
  eventType: string;
  eventAt: number;
  label: string;
  detail: string;
  source: string;
  eventRef: string;
}

export interface OmsStats {
  enabled: boolean;
  state: string;
  auditPath: string;
  offsetPath: string;
  startMode: string;
  startedAt: string;
  processed: number;
  skipped: number;
  duplicates: number;
  orphans: number;
  sequenceGaps: number;
  replays: number;
  lastEventAt: number | null;
  currentOffset: number;
  currentAuditSize: number;
  deadLetterCount: number;
  pendingOrphanCount: number;
  aggregateProgressCount: number;
}

export interface OmsReconcile {
  accountId: string | null;
  totalOrders: number;
  openOrders: number;
  expectedReservedAmount: number;
  actualReservedAmount: number;
  reservedGapAmount: number;
  issues: string[];
}

export interface BackOfficeStats {
  enabled: boolean;
  state: string;
  auditPath: string;
  offsetPath: string;
  startMode: string;
  startedAt: string;
  processed: number;
  skipped: number;
  duplicates: number;
  orphans: number;
  sequenceGaps: number;
  replays: number;
  lastEventAt: number | null;
  currentOffset: number;
  currentAuditSize: number;
  ledgerEntryCount: number;
  deadLetterCount: number;
  pendingOrphanCount: number;
  aggregateProgressCount: number;
}

export interface LedgerEntry {
  entryId: string;
  eventRef: string;
  accountId: string;
  orderId: string;
  eventType: string;
  symbol: string;
  side: string;
  quantityDelta: number;
  cashDelta: number;
  reservedBuyingPowerDelta: number;
  realizedPnlDelta: number;
  detail: string;
  eventAt: number;
  source: string;
}

export interface BackOfficeReconcile {
  accountId: string;
  cashBalance: number;
  availableBuyingPower: number;
  reservedBuyingPower: number;
  realizedPnl: number;
  expectedCashBalance: number;
  expectedReservedBuyingPower: number;
  expectedRealizedPnl: number;
  positions: Array<{
    accountId: string;
    symbol: string;
    netQty: number;
    avgPrice: number;
  }>;
  issues: string[];
}

export interface BusStats {
  enabled: boolean;
  kafkaEnabled: boolean;
  state: string;
  topic: string;
  groupId: string;
  startedAt: string;
  received: number;
  applied: number;
  duplicates: number;
  pending: number;
  deadLetters: number;
  errors: number;
  lastEventAt: number | null;
}

export interface OpsOverview {
  accountId: string;
  orderId: string | null;
  omsStats: OmsStats | null;
  omsBusStats: BusStats | null;
  omsReconcile: OmsReconcile | null;
  omsOrphans: DeadLetterEntry[];
  omsPendingOrphans: PendingOrphanEntry[];
  backOfficeStats: BackOfficeStats | null;
  backOfficeBusStats: BusStats | null;
  backOfficeReconcile: BackOfficeReconcile | null;
  ledgerEntries: LedgerEntry[];
  backOfficeOrphans: DeadLetterEntry[];
  backOfficePendingOrphans: PendingOrphanEntry[];
}

export interface DeadLetterEntry {
  entryId: string;
  eventRef: string;
  accountId: string | null;
  orderId: string | null;
  eventType: string | null;
  reason: string;
  detail: string;
  rawLine: string;
  eventAt: number;
  recordedAt: number;
  source: string;
}

export interface PendingOrphanEntry {
  entryId: string;
  eventRef: string;
  accountId: string | null;
  orderId: string | null;
  eventType: string | null;
  reason: string;
  rawLine: string;
  eventAt: number;
  recordedAt: number;
  source: string;
}

export interface OrphanRequeueResult {
  oms: {
    status: string;
    orderId: string | null;
    reprocessed: number;
    pendingRemaining: number;
  } | null;
  backOffice: {
    status: string;
    orderId: string | null;
    reprocessed: number;
    pendingRemaining: number;
  } | null;
}

export interface DeadLetterRequeueResult {
  oms: {
    status: string;
    eventRef: string | null;
    outcome: string;
    pendingRemaining: number;
    deadLetterRemaining: number;
  } | null;
  backOffice: {
    status: string;
    eventRef: string | null;
    outcome: string;
    pendingRemaining: number;
    deadLetterRemaining: number;
  } | null;
}

export interface AuditReplayResult {
  oms: {
    status: string;
    offset: number;
    processed: number;
    skipped: number;
    duplicates: number;
    orphans: number;
  } | null;
  backOffice: {
    status: string;
    offset: number;
    processed: number;
    skipped: number;
    duplicates: number;
    orphans: number;
  } | null;
}

export interface StrategyConfig {
  enabled: boolean;
  symbols: string[];
  tickMs: number;
  maxOrdersPerMin: number;
  cooldownMs: number;
  updatedAtMs: number;
  storage: string;
  storageHealthy: boolean;
  storageMessage: string | null;
  storageErrorAtMs: number | null;
}

export interface StrategyConfigUpdate {
  enabled: boolean;
  symbols: string[];
  tickMs: number;
  maxOrdersPerMin: number;
  cooldownMs: number;
}
