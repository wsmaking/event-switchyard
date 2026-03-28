import { useEffect, useRef, useState, type MutableRefObject } from 'react';
import {
  useMobileDrill,
  useMobileDrills,
  useSubmitMobileDrillAttempt,
} from '../../hooks/useMobileLearning';
import { formatDateTime } from './mobileUtils';

interface MobileDrillsViewProps {
  drillId: string | null;
  onNavigate: (path: string) => void;
}

export function MobileDrillsView({ drillId, onNavigate }: MobileDrillsViewProps) {
  const { data: drills, isLoading, isError, error } = useMobileDrills();
  const { data: detail, isLoading: detailLoading } = useMobileDrill(drillId);
  const submitAttempt = useSubmitMobileDrillAttempt();
  const [note, setNote] = useState('');
  const [audioDataUrl, setAudioDataUrl] = useState<string | null>(null);
  const [recordingState, setRecordingState] = useState<'idle' | 'recording' | 'processing'>('idle');
  const [recordingError, setRecordingError] = useState<string | null>(null);
  const mediaRecorderRef = useRef<MediaRecorder | null>(null);
  const streamRef = useRef<MediaStream | null>(null);
  const chunksRef = useRef<Blob[]>([]);

  useEffect(() => {
    return () => {
      cleanupRecorder(streamRef, mediaRecorderRef, chunksRef);
    };
  }, []);

  useEffect(() => {
    if (!detail) {
      return;
    }
    setNote(detail.progress.lastNote ?? '');
    setAudioDataUrl(detail.progress.audioDataUrl ?? null);
    setRecordingError(null);
    setRecordingState('idle');
  }, [detail?.drill.id]);

  if (!drillId) {
    if (isLoading) {
      return <div className="px-4 py-6 text-sm text-[color:var(--mobile-muted)]">ドリルを読み込み中...</div>;
    }
    if (isError || !drills) {
      return (
        <div className="px-4 py-6 text-sm text-rose-200">
          ドリル一覧を取得できませんでした
          <div className="mt-2 text-xs text-rose-200/80">{error instanceof Error ? error.message : 'unknown_error'}</div>
        </div>
      );
    }

    const dueDrills = drills.filter((drill) => drill.due);
    const recentDrills = drills.filter((drill) => drill.progress.attemptCount > 0);

    return (
      <div className="space-y-4 px-4 py-5 pb-24">
        <section className="rounded-[28px] border border-white/10 bg-[linear-gradient(135deg,rgba(8,47,73,0.82),rgba(17,24,39,0.96))] p-5">
          <div className="text-[11px] uppercase tracking-[0.22em] text-cyan-100/70">Explanation Drills</div>
          <h1 className="mt-2 text-2xl font-semibold text-white">説明を短く反復する</h1>
          <p className="mt-2 text-sm leading-6 text-slate-300">
            注文、台帳、運用、リスクを 60-90 秒で説明できる状態まで反復する。音声メモも残せる。
          </p>
          <div className="mt-4 grid grid-cols-3 gap-3">
            <CountPill label="全件" value={drills.length} />
            <CountPill label="要反復" value={dueDrills.length} />
            <CountPill label="記録済" value={recentDrills.length} />
          </div>
        </section>

        <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
          <div className="flex items-center justify-between gap-3">
            <div>
              <div className="text-xs uppercase tracking-[0.2em] text-[color:var(--mobile-muted)]">Today Queue</div>
              <div className="mt-2 text-lg font-semibold text-white">今日の説明ドリル</div>
              <div className="mt-2 text-sm leading-6 text-slate-300">
                あいまいだったテーマはすぐ再出題し、言い切れたテーマは間隔を空ける。
              </div>
            </div>
            <div className="rounded-full border border-cyan-300/30 bg-cyan-500/10 px-3 py-1 text-xs text-cyan-100">
              {dueDrills.length} drills
            </div>
          </div>
          {dueDrills.length > 0 ? (
            <button
              onClick={() => onNavigate(`/mobile/drills/${dueDrills[0].id}`)}
              className="mt-4 w-full rounded-2xl bg-white px-4 py-3 text-sm font-semibold text-slate-950"
            >
              先頭ドリルを開く
            </button>
          ) : (
            <div className="mt-4 rounded-[20px] border border-dashed border-white/10 px-4 py-4 text-sm text-slate-400">
              直近で詰まった説明はありません。別テーマで反復を続けられます。
            </div>
          )}
        </section>

        {dueDrills.length > 0 && (
          <section className="space-y-3">
            <div className="text-base font-semibold text-white">要反復</div>
            {dueDrills.map((drill, index) => (
              <DrillListItem
                key={drill.id}
                drill={drill}
                badge={`D${index + 1}`}
                onNavigate={onNavigate}
              />
            ))}
          </section>
        )}

        <section className="space-y-3">
          <div className="text-base font-semibold text-white">全ドリル</div>
          {drills.map((drill) => (
            <DrillListItem key={drill.id} drill={drill} onNavigate={onNavigate} />
          ))}
        </section>
      </div>
    );
  }

  if (detailLoading || !detail || !drills) {
    return <div className="px-4 py-6 text-sm text-[color:var(--mobile-muted)]">ドリル詳細を読み込み中...</div>;
  }

  const nextDue = drills.find((drill) => drill.due && drill.id !== drillId) ?? null;

  const submit = (clarityScore: 0 | 1 | 2) => {
    submitAttempt.mutate(
      {
        drillId,
        clarityScore,
        note,
        audioDataUrl,
      },
      {
        onSuccess: () => {
          if (clarityScore === 0) {
            return;
          }
          if (nextDue) {
            onNavigate(`/mobile/drills/${nextDue.id}`);
            return;
          }
          onNavigate('/mobile/drills');
        },
      }
    );
  };

  return (
    <div className="space-y-4 px-4 py-5 pb-24">
      <button onClick={() => onNavigate('/mobile/drills')} className="text-sm font-medium text-emerald-200">
        ← ドリル一覧へ
      </button>

      <section className="rounded-[28px] border border-white/10 bg-[linear-gradient(135deg,rgba(12,74,110,0.78),rgba(15,23,42,0.96))] p-5">
        <div className="flex items-start justify-between gap-3">
          <div>
            <div className="text-[11px] uppercase tracking-[0.22em] text-cyan-100/70">{detail.drill.category}</div>
            <h1 className="mt-2 text-2xl font-semibold text-white">{detail.drill.title}</h1>
          </div>
          <div className={`rounded-full border px-3 py-1 text-[11px] ${dueTone(detail.progress.nextReviewAt)}`}>
            {detail.progress.attemptCount === 0 ? '未着手' : detail.progress.nextReviewAt <= Date.now() ? '要反復' : '保留中'}
          </div>
        </div>
        <div className="mt-4 rounded-[22px] border border-white/10 bg-black/20 px-4 py-4 text-sm leading-6 text-slate-100">
          {detail.drill.prompt}
        </div>
        <div className="mt-4 grid grid-cols-3 gap-3">
          <CountPill label="試行" value={detail.progress.attemptCount} />
          <CountPill label="前回" value={clarityLabel(detail.progress.lastClarityScore)} />
          <CountPill label="次回" value={shortNextReview(detail.progress.nextReviewAt)} />
        </div>
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-xs uppercase tracking-[0.2em] text-[color:var(--mobile-muted)]">Routes</div>
        <div className="mt-3 flex flex-wrap gap-2">
          {detail.drill.routes.map((route) => (
            <button
              key={route}
              onClick={() => onNavigate(route)}
              className="rounded-full border border-cyan-300/30 bg-cyan-500/10 px-3 py-2 text-xs font-medium text-cyan-100"
            >
              {route}
            </button>
          ))}
        </div>
        <div className="mt-4 text-xs uppercase tracking-[0.2em] text-[color:var(--mobile-muted)]">Keywords</div>
        <div className="mt-3 flex flex-wrap gap-2">
          {detail.drill.keywords.map((keyword) => (
            <div key={keyword} className="rounded-full border border-white/10 bg-slate-950/60 px-3 py-1 text-xs text-slate-200">
              {keyword}
            </div>
          ))}
        </div>
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-base font-semibold text-white">メモ</div>
        <textarea
          value={note}
          onChange={(event) => setNote(event.target.value)}
          placeholder="自分の言葉で要点を書く"
          className="mt-4 h-32 w-full rounded-[20px] border border-white/10 bg-slate-950/60 px-4 py-3 text-sm text-white outline-none placeholder:text-slate-500"
        />
        <div className="mt-3 text-xs text-slate-400">
          最終更新 {formatDateTime(detail.progress.lastAttemptAt)}
        </div>
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="flex items-center justify-between gap-3">
          <div>
            <div className="text-base font-semibold text-white">音声メモ</div>
            <div className="mt-2 text-sm leading-6 text-slate-300">短く話して録音し、次回の比較材料として残す。</div>
          </div>
          <div className="rounded-full border border-white/10 bg-slate-950/60 px-3 py-1 text-[11px] text-slate-300">
            60秒以内推奨
          </div>
        </div>
        <div className="mt-4 grid grid-cols-2 gap-3">
          <button
            onClick={() => startRecording(setRecordingState, setRecordingError, setAudioDataUrl, mediaRecorderRef, streamRef, chunksRef)}
            disabled={recordingState !== 'idle'}
            className="rounded-[20px] border border-amber-300/30 bg-amber-500/10 px-3 py-4 text-xs font-medium text-amber-100 disabled:opacity-50"
          >
            録音開始
          </button>
          <button
            onClick={() => stopRecording(setRecordingState, mediaRecorderRef, streamRef)}
            disabled={recordingState !== 'recording'}
            className="rounded-[20px] border border-rose-300/30 bg-rose-500/10 px-3 py-4 text-xs font-medium text-rose-100 disabled:opacity-50"
          >
            録音停止
          </button>
        </div>
        <div className="mt-3 text-xs text-slate-400">
          状態 {recordingState}
        </div>
        {recordingError && <div className="mt-3 text-xs text-rose-200">{recordingError}</div>}
        {audioDataUrl && (
          <div className="mt-4 space-y-3">
            <audio controls src={audioDataUrl} className="w-full" />
            <button
              onClick={() => setAudioDataUrl(null)}
              className="rounded-full border border-white/10 bg-slate-950/60 px-3 py-2 text-xs text-slate-300"
            >
              音声を消す
            </button>
          </div>
        )}
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-base font-semibold text-white">反復結果</div>
        <div className="mt-4 grid grid-cols-3 gap-3">
          <button
            onClick={() => submit(0)}
            disabled={submitAttempt.isPending}
            className="rounded-[20px] border border-rose-300/30 bg-rose-500/10 px-3 py-4 text-xs font-medium text-rose-100 disabled:opacity-50"
          >
            曖昧
          </button>
          <button
            onClick={() => submit(1)}
            disabled={submitAttempt.isPending}
            className="rounded-[20px] border border-amber-300/30 bg-amber-500/10 px-3 py-4 text-xs font-medium text-amber-100 disabled:opacity-50"
          >
            説明できた
          </button>
          <button
            onClick={() => submit(2)}
            disabled={submitAttempt.isPending}
            className="rounded-[20px] border border-emerald-300/30 bg-emerald-500/10 px-3 py-4 text-xs font-medium text-emerald-100 disabled:opacity-50"
          >
            短く言えた
          </button>
        </div>
        {nextDue && (
          <div className="mt-4 text-xs text-slate-400">
            次の要反復 {nextDue.title}
          </div>
        )}
      </section>
    </div>
  );
}

function DrillListItem({
  drill,
  onNavigate,
  badge,
}: {
  drill: {
    id: string;
    title: string;
    category: string;
    due: boolean;
    progress: {
      attemptCount: number;
      lastClarityScore: number;
      nextReviewAt: number;
    };
  };
  onNavigate: (path: string) => void;
  badge?: string;
}) {
  return (
    <button
      onClick={() => onNavigate(`/mobile/drills/${drill.id}`)}
      className="w-full rounded-[22px] border border-white/10 bg-white/5 px-4 py-4 text-left"
    >
      <div className="flex items-start justify-between gap-3">
        <div>
          <div className="text-sm font-semibold text-white">{drill.title}</div>
          <div className="mt-2 text-xs text-slate-400">
            {drill.category} / 試行 {drill.progress.attemptCount} 回 / 前回 {clarityLabel(drill.progress.lastClarityScore)}
          </div>
        </div>
        <div className={`rounded-full border px-3 py-1 text-[11px] ${drill.due ? 'border-cyan-300/30 bg-cyan-500/10 text-cyan-100' : 'border-white/10 bg-slate-950/60 text-slate-300'}`}>
          {badge ?? (drill.due ? 'due' : 'queue')}
        </div>
      </div>
      <div className="mt-3 text-xs text-slate-500">
        次回 {formatDateTime(drill.progress.nextReviewAt)}
      </div>
    </button>
  );
}

function CountPill({ label, value, tone }: { label: string; value: number | string; tone?: string }) {
  return (
    <div className="rounded-2xl border border-white/10 bg-black/20 px-3 py-3 text-center">
      <div className="text-[11px] uppercase tracking-[0.18em] text-cyan-50/60">{label}</div>
      <div className={`mt-2 text-lg font-semibold ${tone ?? 'text-white'}`}>{value}</div>
    </div>
  );
}

function clarityLabel(score: number) {
  switch (score) {
    case 2:
      return '短く言えた';
    case 1:
      return '説明できた';
    case 0:
      return '曖昧';
    default:
      return '未記録';
  }
}

function shortNextReview(nextReviewAt: number) {
  if (!nextReviewAt) {
    return '今すぐ';
  }
  if (nextReviewAt <= Date.now()) {
    return '要反復';
  }
  return new Date(nextReviewAt).toLocaleDateString('ja-JP', { month: 'numeric', day: 'numeric' });
}

function dueTone(nextReviewAt: number) {
  if (!nextReviewAt) {
    return 'border-white/10 bg-slate-950/60 text-slate-200';
  }
  if (nextReviewAt <= Date.now()) {
    return 'border-cyan-300/30 bg-cyan-500/10 text-cyan-100';
  }
  return 'border-white/10 bg-slate-950/60 text-slate-300';
}

async function startRecording(
  setRecordingState: (state: 'idle' | 'recording' | 'processing') => void,
  setRecordingError: (message: string | null) => void,
  setAudioDataUrl: (value: string | null) => void,
  mediaRecorderRef: MutableRefObject<MediaRecorder | null>,
  streamRef: MutableRefObject<MediaStream | null>,
  chunksRef: MutableRefObject<Blob[]>
) {
  setRecordingError(null);
  if (!navigator.mediaDevices?.getUserMedia || typeof MediaRecorder === 'undefined') {
    setRecordingError('このブラウザでは録音に対応していません');
    return;
  }
  try {
    const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
    const recorder = new MediaRecorder(stream);
    chunksRef.current = [];
    recorder.ondataavailable = (event) => {
      if (event.data.size > 0) {
        chunksRef.current.push(event.data);
      }
    };
    recorder.onstop = () => {
      setRecordingState('processing');
      const blob = new Blob(chunksRef.current, { type: recorder.mimeType || 'audio/webm' });
      const reader = new FileReader();
      reader.onloadend = () => {
        setAudioDataUrl(typeof reader.result === 'string' ? reader.result : null);
        cleanupRecorder(streamRef, mediaRecorderRef, chunksRef);
        setRecordingState('idle');
      };
      reader.onerror = () => {
        cleanupRecorder(streamRef, mediaRecorderRef, chunksRef);
        setRecordingError('録音データを読み込めませんでした');
        setRecordingState('idle');
      };
      reader.readAsDataURL(blob);
    };
    mediaRecorderRef.current = recorder;
    streamRef.current = stream;
    recorder.start();
    setRecordingState('recording');
  } catch (error) {
    setRecordingError(error instanceof Error ? error.message : '録音を開始できませんでした');
    cleanupRecorder(streamRef, mediaRecorderRef, chunksRef);
    setRecordingState('idle');
  }
}

function stopRecording(
  setRecordingState: (state: 'idle' | 'recording' | 'processing') => void,
  mediaRecorderRef: MutableRefObject<MediaRecorder | null>,
  streamRef: MutableRefObject<MediaStream | null>
) {
  if (!mediaRecorderRef.current || mediaRecorderRef.current.state !== 'recording') {
    return;
  }
  setRecordingState('processing');
  mediaRecorderRef.current.stop();
  streamRef.current?.getTracks().forEach((track) => track.stop());
}

function cleanupRecorder(
  streamRef: MutableRefObject<MediaStream | null>,
  mediaRecorderRef: MutableRefObject<MediaRecorder | null>,
  chunksRef: MutableRefObject<Blob[]>
) {
  streamRef.current?.getTracks().forEach((track) => track.stop());
  streamRef.current = null;
  mediaRecorderRef.current = null;
  chunksRef.current = [];
}
