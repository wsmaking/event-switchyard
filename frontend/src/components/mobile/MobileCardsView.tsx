import { useMobileCard, useMobileCards, useMobileProgress, useUpdateMobileProgress } from '../../hooks/useMobileLearning';
import { difficultyTone, formatDateTime, masteryTone } from './mobileUtils';

interface MobileCardsViewProps {
  cardId: string | null;
  onNavigate: (path: string) => void;
}

export function MobileCardsView({ cardId, onNavigate }: MobileCardsViewProps) {
  const { data: progress } = useMobileProgress();
  const { data: cards, isLoading, isError, error } = useMobileCards();
  const { data: detail, isLoading: detailLoading } = useMobileCard(cardId);
  const updateProgress = useUpdateMobileProgress();

  if (!cardId) {
    if (isLoading) {
      return <div className="px-4 py-6 text-sm text-[color:var(--mobile-muted)]">カードを読み込み中...</div>;
    }
    if (isError || !cards) {
      return (
        <div className="px-4 py-6 text-sm text-rose-200">
          カード一覧を取得できませんでした
          <div className="mt-2 text-xs text-rose-200/80">{error instanceof Error ? error.message : 'unknown_error'}</div>
        </div>
      );
    }

    const dueCards = cards.filter((card) => card.due);
    const bookmarkedCards = cards.filter((card) => card.bookmarked);

    return (
      <div className="space-y-4 px-4 py-5 pb-24">
        <section className="rounded-[28px] border border-white/10 bg-[linear-gradient(135deg,rgba(76,29,149,0.55),rgba(15,23,42,0.95))] p-5">
          <div className="text-[11px] uppercase tracking-[0.22em] text-violet-100/70">Design Cards</div>
          <h1 className="mt-2 text-2xl font-semibold text-white">設計判断を言語化する</h1>
          <p className="mt-2 text-sm leading-6 text-slate-300">
            面接で問われるのは知識よりも境界と判断理由。短文で即答できる状態まで反復する。
          </p>
          <div className="mt-4 grid grid-cols-3 gap-3">
            <CountPill label="全件" value={cards.length} />
            <CountPill label="要復習" value={progress?.dueCount ?? dueCards.length} />
            <CountPill label="Bookmark" value={progress?.bookmarkedCount ?? bookmarkedCards.length} />
          </div>
        </section>

        {dueCards.length > 0 && (
          <Section title="要復習">
            {dueCards.map((card) => (
              <CardListItem key={card.id} card={card} onNavigate={onNavigate} />
            ))}
          </Section>
        )}

        {bookmarkedCards.length > 0 && (
          <Section title="Bookmark">
            {bookmarkedCards.map((card) => (
              <CardListItem key={card.id} card={card} onNavigate={onNavigate} />
            ))}
          </Section>
        )}

        <Section title="全カード">
          {cards.map((card) => (
            <CardListItem key={card.id} card={card} onNavigate={onNavigate} />
          ))}
        </Section>
      </div>
    );
  }

  if (detailLoading || !detail) {
    return <div className="px-4 py-6 text-sm text-[color:var(--mobile-muted)]">カード詳細を読み込み中...</div>;
  }

  const bookmarked = detail.progress.bookmarked;

  return (
    <div className="space-y-4 px-4 py-5 pb-24">
      <button onClick={() => onNavigate('/mobile/cards')} className="text-sm font-medium text-emerald-200">
        ← カード一覧へ
      </button>

      <section className="rounded-[28px] border border-white/10 bg-[linear-gradient(135deg,rgba(88,28,135,0.62),rgba(15,23,42,0.95))] p-5">
        <div className="flex items-start justify-between gap-3">
          <div>
            <div className="text-[11px] uppercase tracking-[0.22em] text-violet-100/70">{detail.card.category}</div>
            <h1 className="mt-2 text-2xl font-semibold text-white">{detail.card.title}</h1>
          </div>
          <div className={`rounded-full border px-3 py-1 text-[11px] ${difficultyTone(detail.card.difficulty)}`}>
            {detail.card.difficulty}
          </div>
        </div>
        <div className="mt-4 rounded-[22px] border border-white/10 bg-black/20 px-4 py-4 text-sm leading-6 text-slate-100">
          {detail.card.question}
        </div>
        <div className="mt-4 grid grid-cols-3 gap-3 text-center">
          <CountPill label="Mastery" value={detail.progress.masteryLevel} tone={masteryTone(detail.progress.masteryLevel)} />
          <CountPill label="Correct" value={detail.progress.correctCount} />
          <CountPill label="Wrong" value={detail.progress.incorrectCount} />
        </div>
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-xs uppercase tracking-[0.2em] text-[color:var(--mobile-muted)]">Short Answer</div>
        <div className="mt-3 text-base font-semibold leading-7 text-white">{detail.card.shortAnswer}</div>
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-xs uppercase tracking-[0.2em] text-[color:var(--mobile-muted)]">Long Answer</div>
        <div className="mt-3 text-sm leading-7 text-slate-300">{detail.card.longAnswer}</div>
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-xs uppercase tracking-[0.2em] text-[color:var(--mobile-muted)]">Keywords</div>
        <div className="mt-3 flex flex-wrap gap-2">
          {detail.card.keywords.map((keyword) => (
            <div key={keyword} className="rounded-full border border-white/10 bg-slate-950/60 px-3 py-1 text-xs text-slate-200">
              {keyword}
            </div>
          ))}
        </div>
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-xs uppercase tracking-[0.2em] text-[color:var(--mobile-muted)]">Related Routes</div>
        <div className="mt-3 flex flex-wrap gap-2">
          {detail.card.routes.map((route) => (
            <button
              key={route}
              onClick={() => onNavigate(route)}
              className="rounded-full border border-emerald-300/30 bg-emerald-500/10 px-3 py-2 text-xs font-medium text-emerald-100"
            >
              {route}
            </button>
          ))}
        </div>
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-xs uppercase tracking-[0.2em] text-[color:var(--mobile-muted)]">Code References</div>
        <div className="mt-3 space-y-3">
          {detail.card.codeReferences.map((reference) => (
            <div key={reference} className="rounded-[18px] border border-white/8 bg-slate-950/55 px-3 py-3 text-xs leading-6 text-slate-300">
              {reference}
            </div>
          ))}
        </div>
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-xs uppercase tracking-[0.2em] text-[color:var(--mobile-muted)]">Review Actions</div>
        <div className="mt-4 grid grid-cols-3 gap-3">
          <button
            onClick={() => updateProgress.mutate({ type: 'bookmark', cardId, bookmarked: !bookmarked })}
            className="rounded-[20px] border border-white/10 bg-slate-950/55 px-3 py-4 text-xs font-medium text-white"
          >
            {bookmarked ? 'Bookmark解除' : 'Bookmark'}
          </button>
          <button
            onClick={() => updateProgress.mutate({ type: 'review', cardId, correct: false })}
            className="rounded-[20px] border border-amber-300/30 bg-amber-500/10 px-3 py-4 text-xs font-medium text-amber-100"
          >
            要復習
          </button>
          <button
            onClick={() => updateProgress.mutate({ type: 'review', cardId, correct: true })}
            className="rounded-[20px] border border-emerald-300/30 bg-emerald-500/10 px-3 py-4 text-xs font-medium text-emerald-100"
          >
            理解した
          </button>
        </div>
        <div className="mt-4 text-xs text-slate-500">
          最終レビュー {formatDateTime(detail.progress.lastReviewedAt)}
        </div>
      </section>
    </div>
  );
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section className="space-y-3">
      <div className="text-base font-semibold text-white">{title}</div>
      {children}
    </section>
  );
}

function CardListItem({
  card,
  onNavigate,
}: {
  card: MobileCardsViewProps['cardId'] extends never ? never : {
    id: string;
    title: string;
    category: string;
    difficulty: string;
    bookmarked: boolean;
    due: boolean;
    progress: {
      masteryLevel: number;
    };
  };
  onNavigate: (path: string) => void;
}) {
  return (
    <button
      onClick={() => onNavigate(`/mobile/cards/${card.id}`)}
      className="w-full rounded-[22px] border border-white/10 bg-white/5 p-4 text-left"
    >
      <div className="flex items-center justify-between gap-3">
        <div>
          <div className="text-sm font-semibold text-white">{card.title}</div>
          <div className="mt-1 text-xs text-slate-400">{card.category}</div>
        </div>
        <div className="flex flex-col items-end gap-2">
          <div className={`rounded-full border px-3 py-1 text-[11px] ${difficultyTone(card.difficulty)}`}>
            {card.difficulty}
          </div>
          <div className={`text-[11px] ${masteryTone(card.progress.masteryLevel)}`}>
            mastery {card.progress.masteryLevel}
          </div>
        </div>
      </div>
      <div className="mt-3 flex items-center gap-2 text-[11px] text-slate-400">
        {card.bookmarked && <span className="rounded-full bg-white/10 px-2 py-1 text-white">bookmark</span>}
        {card.due && <span className="rounded-full bg-amber-500/10 px-2 py-1 text-amber-100">due</span>}
      </div>
    </button>
  );
}

function CountPill({
  label,
  value,
  tone,
}: {
  label: string;
  value: number;
  tone?: string;
}) {
  return (
    <div className="rounded-2xl border border-white/10 bg-black/20 px-3 py-3 text-center">
      <div className="text-[11px] uppercase tracking-[0.18em] text-violet-100/60">{label}</div>
      <div className={`mt-2 text-lg font-semibold ${tone ?? 'text-white'}`}>{value}</div>
    </div>
  );
}
