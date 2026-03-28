import { useState, type ReactNode, type TouchEvent } from 'react';
import { useMobileCard, useMobileCards, useMobileProgress, useUpdateMobileProgress } from '../../hooks/useMobileLearning';
import type { MobileCardSummary } from '../../types/mobile';
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
  const [touchStartX, setTouchStartX] = useState<number | null>(null);

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
    const reviewQueue = dueCards.slice(0, 5);

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

        <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
          <div className="flex items-center justify-between gap-3">
            <div>
              <div className="text-xs uppercase tracking-[0.2em] text-[color:var(--mobile-muted)]">5 Minute Queue</div>
              <div className="mt-2 text-lg font-semibold text-white">今日の review deck</div>
              <div className="mt-2 text-sm leading-6 text-slate-300">
                due card を 5 枚までまとめて回す。間違えたカードは queue に残り続ける。
              </div>
            </div>
            <div className="rounded-full border border-violet-300/30 bg-violet-500/10 px-3 py-1 text-xs text-violet-100">
              {reviewQueue.length} cards
            </div>
          </div>
          {reviewQueue.length > 0 ? (
            <button
              onClick={() => onNavigate(`/mobile/cards/${reviewQueue[0].id}`)}
              className="mt-4 w-full rounded-2xl bg-white px-4 py-3 text-sm font-semibold text-slate-950"
            >
              review を始める
            </button>
          ) : (
            <div className="mt-4 rounded-[20px] border border-dashed border-white/10 px-4 py-4 text-sm text-slate-400">
              due card はありません。bookmark や新規カードで学習を続けられます。
            </div>
          )}
        </section>

        {reviewQueue.length > 0 && (
          <Section title="Review Queue">
            {reviewQueue.map((card, index) => (
              <CardListItem key={card.id} card={card} onNavigate={onNavigate} badge={`Q${index + 1}`} />
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

  if (detailLoading || !detail || !cards) {
    return <div className="px-4 py-6 text-sm text-[color:var(--mobile-muted)]">カード詳細を読み込み中...</div>;
  }

  const bookmarked = detail.progress.bookmarked;
  const reviewQueue = cards.filter((card) => card.due && card.id !== cardId);

  const handleReview = (correct: boolean) => {
    updateProgress.mutate(
      { type: 'review', cardId, correct },
      {
        onSuccess: () => {
          if (reviewQueue.length > 0) {
            onNavigate(`/mobile/cards/${reviewQueue[0].id}`);
            return;
          }
          onNavigate('/mobile/cards');
        },
      }
    );
  };

  const handleTouchStart = (event: TouchEvent<HTMLDivElement>) => {
    setTouchStartX(event.changedTouches[0]?.clientX ?? null);
  };

  const handleTouchEnd = (event: TouchEvent<HTMLDivElement>) => {
    if (touchStartX === null) {
      return;
    }
    const delta = (event.changedTouches[0]?.clientX ?? 0) - touchStartX;
    setTouchStartX(null);
    if (delta >= 64) {
      handleReview(true);
    } else if (delta <= -64) {
      handleReview(false);
    }
  };

  return (
    <div className="space-y-4 px-4 py-5 pb-24">
      <button onClick={() => onNavigate('/mobile/cards')} className="text-sm font-medium text-emerald-200">
        ← カード一覧へ
      </button>

      <section
        className="rounded-[28px] border border-white/10 bg-[linear-gradient(135deg,rgba(88,28,135,0.62),rgba(15,23,42,0.95))] p-5"
        onTouchStart={handleTouchStart}
        onTouchEnd={handleTouchEnd}
      >
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
        <div className="mt-4 rounded-[18px] border border-white/10 bg-black/20 px-4 py-3 text-xs leading-6 text-violet-100/90">
          右スワイプで「理解した」、左スワイプで「要復習」。次の due card へ自動で進む。
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
            onClick={() => handleReview(false)}
            className="rounded-[20px] border border-amber-300/30 bg-amber-500/10 px-3 py-4 text-xs font-medium text-amber-100"
          >
            要復習
          </button>
          <button
            onClick={() => handleReview(true)}
            className="rounded-[20px] border border-emerald-300/30 bg-emerald-500/10 px-3 py-4 text-xs font-medium text-emerald-100"
          >
            理解した
          </button>
        </div>
        <div className="mt-4 grid gap-2 text-xs text-slate-500">
          <div>最終レビュー {formatDateTime(detail.progress.lastReviewedAt)}</div>
          <div>次回レビュー {formatDateTime(detail.progress.nextReviewAt)}</div>
          {reviewQueue.length > 0 && <div>次の due card {reviewQueue[0].title}</div>}
        </div>
      </section>
    </div>
  );
}

function Section({ title, children }: { title: string; children: ReactNode }) {
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
  badge,
}: {
  card: MobileCardSummary;
  onNavigate: (path: string) => void;
  badge?: string;
}) {
  return (
    <button
      onClick={() => onNavigate(`/mobile/cards/${card.id}`)}
      className="w-full rounded-[22px] border border-white/10 bg-white/5 p-4 text-left"
    >
      <div className="flex items-center justify-between gap-3">
        <div>
          <div className="flex items-center gap-2">
            <div className="text-sm font-semibold text-white">{card.title}</div>
            {badge && <span className="rounded-full bg-violet-500/15 px-2 py-1 text-[10px] text-violet-100">{badge}</span>}
          </div>
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
      <div className="mt-3 flex flex-wrap items-center gap-2 text-[11px] text-slate-400">
        {card.bookmarked && <span className="rounded-full bg-white/10 px-2 py-1 text-white">bookmark</span>}
        {card.due && <span className="rounded-full bg-amber-500/10 px-2 py-1 text-amber-100">due</span>}
        {card.progress.nextReviewAt > 0 && <span>next {formatDateTime(card.progress.nextReviewAt)}</span>}
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
