import { OrderStatus } from '../../types/trading';

export function formatCurrency(value: number | null | undefined) {
  const safeValue = value ?? 0;
  return new Intl.NumberFormat('ja-JP', {
    style: 'currency',
    currency: 'JPY',
    maximumFractionDigits: 0,
  }).format(safeValue);
}

export function formatSignedCurrency(value: number | null | undefined) {
  const safeValue = value ?? 0;
  const prefix = safeValue > 0 ? '+' : '';
  return `${prefix}${formatCurrency(safeValue)}`;
}

export function formatNumber(value: number | null | undefined) {
  return new Intl.NumberFormat('ja-JP').format(value ?? 0);
}

export function formatDateTime(value: number | null | undefined) {
  if (!value) return '未記録';
  return new Date(value).toLocaleString('ja-JP', {
    month: 'numeric',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

export function statusLabel(status: string) {
  switch (status as OrderStatus) {
    case OrderStatus.PENDING_ACCEPT:
      return '受付中';
    case OrderStatus.ACCEPTED:
      return '受付済';
    case OrderStatus.PARTIALLY_FILLED:
      return '一部約定';
    case OrderStatus.FILLED:
      return '約定済';
    case OrderStatus.CANCEL_PENDING:
      return '取消中';
    case OrderStatus.CANCELED:
      return '取消済';
    case OrderStatus.EXPIRED:
      return '失効';
    case OrderStatus.REJECTED:
      return '拒否';
    case OrderStatus.AMEND_PENDING:
      return '訂正中';
    default:
      return status;
  }
}

export function statusTone(status: string) {
  switch (status as OrderStatus) {
    case OrderStatus.FILLED:
      return 'border-emerald-500/40 bg-emerald-500/15 text-emerald-100';
    case OrderStatus.PARTIALLY_FILLED:
      return 'border-sky-500/40 bg-sky-500/15 text-sky-100';
    case OrderStatus.REJECTED:
    case OrderStatus.CANCELED:
    case OrderStatus.EXPIRED:
      return 'border-rose-500/40 bg-rose-500/15 text-rose-100';
    case OrderStatus.CANCEL_PENDING:
    case OrderStatus.AMEND_PENDING:
      return 'border-cyan-500/40 bg-cyan-500/15 text-cyan-100';
    default:
      return 'border-amber-500/40 bg-amber-500/15 text-amber-100';
  }
}

export function difficultyTone(difficulty: string) {
  switch (difficulty) {
    case 'hard':
      return 'border-rose-500/30 bg-rose-500/15 text-rose-100';
    case 'medium':
      return 'border-amber-500/30 bg-amber-500/15 text-amber-100';
    default:
      return 'border-emerald-500/30 bg-emerald-500/15 text-emerald-100';
  }
}

export function masteryTone(masteryLevel: number) {
  if (masteryLevel >= 3) return 'text-emerald-300';
  if (masteryLevel >= 2) return 'text-sky-300';
  if (masteryLevel >= 1) return 'text-amber-300';
  return 'text-rose-300';
}
