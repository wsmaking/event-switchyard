import type { MobileHome } from '../../types/mobile';
import { difficultyTone, formatDateTime, statusLabel, statusTone } from './mobileUtils';

interface MobileHomeViewProps {
  home: MobileHome | undefined;
  isLoading: boolean;
  isError: boolean;
  errorMessage: string | null;
  onNavigate: (path: string) => void;
}

export function MobileHomeView({
  home,
  isLoading,
  isError,
  errorMessage,
  onNavigate,
}: MobileHomeViewProps) {
  if (isLoading) {
    return <div className="px-4 py-6 text-sm text-[color:var(--mobile-muted)]">読み込み中...</div>;
  }

  if (isError || !home) {
    return (
      <div className="px-4 py-6 text-sm text-rose-200">
        mobile home を取得できませんでした
        <div className="mt-2 text-xs text-rose-200/80">{errorMessage ?? 'unknown_error'}</div>
      </div>
    );
  }

  return (
    <div className="space-y-4 px-4 py-5">
      <section className="rounded-[28px] border border-white/10 bg-[linear-gradient(135deg,rgba(20,83,45,0.85),rgba(15,23,42,0.92))] p-5 shadow-[0_22px_60px_rgba(0,0,0,0.38)]">
        <div className="flex items-start justify-between gap-4">
          <div>
            <div className="text-[11px] uppercase tracking-[0.24em] text-emerald-100/70">Mobile Learning</div>
            <h1 className="mt-2 text-2xl font-semibold text-white">業務理解の 5 分導線</h1>
            <p className="mt-2 text-sm leading-6 text-emerald-50/80">
              注文、台帳、運用、設計判断をスマホで短く反復するための入口。
            </p>
          </div>
          <div className={`rounded-full px-3 py-1 text-[11px] font-medium ${home.mainlineStatus.healthy ? 'bg-emerald-400/15 text-emerald-100' : 'bg-amber-400/15 text-amber-100'}`}>
            {home.mainlineStatus.healthy ? 'mainline stable' : '要確認'}
          </div>
        </div>

        <div className="mt-5 grid grid-cols-2 gap-3">
          <MetricCard label="要復習" value={String(home.progress.dueCount)} />
          <MetricCard label="反復" value={String(home.progress.dueDrillCount)} />
          <MetricCard label="Bookmark" value={String(home.progress.bookmarkedCount)} />
          <MetricCard label="完了" value={String(home.progress.completedCount)} />
        </div>
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4 backdrop-blur">
        <div className="text-xs uppercase tracking-[0.2em] text-[color:var(--mobile-muted)]">Continue</div>
        <div className="mt-2 text-lg font-semibold text-white">{home.continueLearning.title}</div>
        <div className="mt-2 text-sm leading-6 text-slate-300">{home.continueLearning.detail}</div>
        <button
          onClick={() => onNavigate(home.continueLearning.route)}
          className="mt-4 w-full rounded-2xl bg-white px-4 py-3 text-sm font-semibold text-slate-950 transition hover:bg-emerald-50"
        >
          続きから開く
        </button>
      </section>

      <section className="rounded-[24px] border border-white/10 bg-[linear-gradient(135deg,rgba(30,41,59,0.88),rgba(13,20,36,0.96))] p-4">
        <div className="text-xs uppercase tracking-[0.2em] text-[color:var(--mobile-muted)]">{home.todaySuggestion.label}</div>
        <div className="mt-2 text-lg font-semibold text-white">{home.todaySuggestion.title}</div>
        <div className="mt-2 text-sm leading-6 text-slate-300">{home.todaySuggestion.detail}</div>
        <button
          onClick={() => onNavigate(home.todaySuggestion.route)}
          className="mt-4 rounded-full border border-sky-400/40 bg-sky-400/10 px-4 py-2 text-xs font-medium text-sky-100"
        >
          このテーマに入る
        </button>
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="flex items-center justify-between">
          <div>
            <div className="text-xs uppercase tracking-[0.2em] text-[color:var(--mobile-muted)]">Mainline</div>
            <div className="mt-1 text-base font-semibold text-white">{home.mainlineStatus.summary}</div>
          </div>
          <div className="rounded-full bg-slate-900/80 px-3 py-1 text-[11px] text-slate-300">
            OMS {home.mainlineStatus.omsState}
          </div>
        </div>
        <div className="mt-4 grid grid-cols-3 gap-2 text-center">
          <StatPill label="Gap" value={home.mainlineStatus.sequenceGapCount} />
          <StatPill label="Pending" value={home.mainlineStatus.pendingOrphanCount} />
          <StatPill label="DLQ" value={home.mainlineStatus.deadLetterCount} />
        </div>
        <div className="mt-4 space-y-2">
          {home.mainlineStatus.focusNotes.map((note) => (
            <div key={note} className="rounded-2xl border border-white/6 bg-slate-950/50 px-3 py-2 text-xs leading-5 text-slate-300">
              {note}
            </div>
          ))}
        </div>
      </section>

      <section className="grid grid-cols-2 gap-3">
        {home.quickActions.map((action) => (
          <button
            key={action.label}
            onClick={() => onNavigate(action.route)}
            className="rounded-[22px] border border-white/10 bg-white/5 px-4 py-4 text-left"
          >
            <div className="text-[11px] uppercase tracking-[0.18em] text-[color:var(--mobile-muted)]">{action.tone}</div>
            <div className="mt-2 text-sm font-semibold text-white">{action.label}</div>
          </button>
        ))}
      </section>

      <section className="space-y-3">
        <SectionTitle title="最近の注文" actionLabel="一覧" onAction={() => onNavigate('/mobile/orders')} />
        {home.recentOrders.length === 0 ? (
          <EmptyCard text="まだ注文がありません。replay scenario で seed できます。" />
        ) : (
          home.recentOrders.map((order) => (
            <button
              key={order.orderId}
              onClick={() => onNavigate(`/mobile/orders/${order.orderId}`)}
              className="w-full rounded-[24px] border border-white/10 bg-white/5 p-4 text-left"
            >
              <div className="flex items-center justify-between gap-3">
                <div>
                  <div className="text-xs text-[color:var(--mobile-muted)]">{order.symbol}</div>
                  <div className="mt-1 text-base font-semibold text-white">{order.symbolName}</div>
                </div>
                <div className={`rounded-full border px-3 py-1 text-xs font-medium ${statusTone(order.status)}`}>
                  {statusLabel(order.status)}
                </div>
              </div>
              <div className="mt-3 text-sm text-slate-300">{order.learningFocus}</div>
              <div className="mt-3 text-xs text-slate-400">{formatDateTime(order.submittedAt)}</div>
            </button>
          ))
        )}
      </section>

      <section className="space-y-3">
        <SectionTitle title="要復習カード" actionLabel="全件" onAction={() => onNavigate('/mobile/cards')} />
        {home.dueCards.length === 0 ? (
          <EmptyCard text="復習待ちカードはありません。" />
        ) : (
          home.dueCards.map((card) => (
            <button
              key={card.id}
              onClick={() => onNavigate(`/mobile/cards/${card.id}`)}
              className="w-full rounded-[22px] border border-white/10 bg-white/5 p-4 text-left"
            >
              <div className="flex items-center justify-between gap-3">
                <div className="text-sm font-semibold text-white">{card.title}</div>
                <div className={`rounded-full border px-3 py-1 text-[11px] ${difficultyTone(card.difficulty)}`}>
                  {card.difficulty}
                </div>
              </div>
              <div className="mt-2 text-xs text-slate-300">{card.category}</div>
            </button>
          ))
        )}
      </section>

      {home.bookmarks.length > 0 && (
        <section className="space-y-3 pb-20">
          <SectionTitle title="Bookmark" actionLabel="カード" onAction={() => onNavigate('/mobile/cards')} />
          {home.bookmarks.map((card) => (
            <button
              key={card.id}
              onClick={() => onNavigate(`/mobile/cards/${card.id}`)}
              className="w-full rounded-[22px] border border-white/10 bg-white/5 p-4 text-left"
            >
              <div className="text-sm font-semibold text-white">{card.title}</div>
              <div className="mt-2 text-xs text-slate-400">
                最終更新 {formatDateTime(card.progress.lastReviewedAt || home.generatedAt)}
              </div>
            </button>
          ))}
        </section>
      )}
    </div>
  );
}

function MetricCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-2xl border border-white/10 bg-black/20 px-3 py-3">
      <div className="text-[11px] uppercase tracking-[0.18em] text-emerald-50/60">{label}</div>
      <div className="mt-2 text-xl font-semibold text-white">{value}</div>
    </div>
  );
}

function StatPill({ label, value }: { label: string; value: number }) {
  return (
    <div className="rounded-2xl border border-white/8 bg-slate-950/60 px-3 py-3">
      <div className="text-[11px] uppercase tracking-[0.18em] text-slate-500">{label}</div>
      <div className="mt-1 text-lg font-semibold text-white">{value}</div>
    </div>
  );
}

function SectionTitle({
  title,
  actionLabel,
  onAction,
}: {
  title: string;
  actionLabel: string;
  onAction: () => void;
}) {
  return (
    <div className="flex items-center justify-between">
      <div className="text-base font-semibold text-white">{title}</div>
      <button onClick={onAction} className="text-xs font-medium text-emerald-200">
        {actionLabel}
      </button>
    </div>
  );
}

function EmptyCard({ text }: { text: string }) {
  return <div className="rounded-[22px] border border-dashed border-white/10 bg-white/5 px-4 py-5 text-sm text-slate-400">{text}</div>;
}
