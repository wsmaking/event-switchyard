import type { ReactNode } from 'react';
import { useMobileOperationsGuide } from '../../hooks/useMobileLearning';

export function MobileOperationsView() {
  const { data, isLoading, isError, error } = useMobileOperationsGuide();

  if (isLoading) {
    return <div className="px-4 py-6 text-sm text-[color:var(--mobile-muted)]">運用工学を読み込み中...</div>;
  }

  if (isError || !data) {
    return (
      <div className="px-4 py-6 text-sm text-rose-200">
        運用工学を取得できませんでした
        <div className="mt-2 text-xs text-rose-200/80">{error instanceof Error ? error.message : 'unknown_error'}</div>
      </div>
    );
  }

  return (
    <div className="space-y-4 px-4 py-5 pb-24">
      <section className="rounded-[28px] border border-white/10 bg-[linear-gradient(135deg,rgba(22,101,52,0.32),rgba(15,23,42,0.96))] p-5">
        <div className="text-[11px] uppercase tracking-[0.22em] text-emerald-100/70">Production Engineering</div>
        <h1 className="mt-2 text-2xl font-semibold text-white">運用工学と事故時の読み方</h1>
        <p className="mt-2 text-sm leading-6 text-slate-300">
          sequence gap、pending orphan、DLQ、schema rollout、market data stale を別問題として扱い、operator の最初の問いを固定する。
        </p>
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-sm font-semibold text-white">現在の live state</div>
        <div className="mt-4 grid grid-cols-2 gap-3">
          <Metric label="Gateway" value={data.liveState.gatewayState} />
          <Metric label="OMS" value={data.liveState.omsState} />
          <Metric label="BackOffice" value={data.liveState.backOfficeState} />
          <Metric label="Feed" value={data.liveState.marketDataState} />
          <Metric label="Schema" value={data.liveState.schemaState} />
          <Metric label="Capacity" value={data.liveState.capacityState} />
          <Metric label="gap" value={`${data.liveState.sequenceGapCount}`} />
          <Metric label="pending" value={`${data.liveState.pendingOrphanCount}`} />
          <Metric label="DLQ" value={`${data.liveState.deadLetterCount}`} />
          <Metric label="incident" value={`${data.liveState.activeIncidentCount}`} />
        </div>
        <div className="mt-4 space-y-2">
          {data.liveState.activeIncidents.map((incident) => (
            <div key={incident} className="rounded-2xl border border-amber-200/20 bg-amber-500/10 px-3 py-3 text-sm leading-6 text-amber-100">
              {incident}
            </div>
          ))}
        </div>
        <div className="mt-4 space-y-2">
          {data.liveState.reconcileNotes.map((note) => (
            <div key={note} className="rounded-2xl border border-white/8 bg-slate-950/55 px-3 py-3 text-sm leading-6 text-slate-300">
              {note}
            </div>
          ))}
        </div>
      </section>

      <Section title="Session Monitors">
        {data.sessionMonitors.map((monitor) => (
          <div key={monitor.name} className="rounded-[22px] border border-white/10 bg-white/5 p-4">
            <div className="flex items-center justify-between gap-3">
              <div className="text-base font-semibold text-white">{monitor.name}</div>
              <div className="rounded-full border border-white/10 bg-black/20 px-3 py-1 text-[11px] text-slate-300">{monitor.state}</div>
            </div>
            <div className="mt-3 text-sm leading-6 text-slate-300">{monitor.whyItMatters}</div>
            <div className="mt-3 rounded-2xl border border-cyan-300/20 bg-cyan-400/10 px-3 py-3 text-sm leading-6 text-cyan-100">
              current: {monitor.currentValue}
            </div>
            <List items={monitor.checkpoints} />
            <SubList title="Operator Actions" items={monitor.operatorActions} />
          </div>
        ))}
      </Section>

      <Section title="Incident Drills">
        {data.incidentDrills.map((drill) => (
          <div key={drill.name} className="rounded-[22px] border border-white/10 bg-white/5 p-4">
            <div className="flex items-center justify-between gap-3">
              <div className="text-base font-semibold text-white">{drill.name}</div>
              <div className={`rounded-full border px-3 py-1 text-[11px] ${drill.active ? 'border-amber-200/30 bg-amber-500/15 text-amber-100' : 'border-white/10 bg-black/20 text-slate-300'}`}>
                {drill.severity}
              </div>
            </div>
            <div className="mt-2 text-sm text-amber-100">Trigger: {drill.trigger}</div>
            <SubList title="最初の問い" items={drill.firstQuestions} />
            <SubList title="Actions" items={drill.actions} />
            <div className="mt-3 rounded-2xl border border-white/8 bg-slate-950/55 px-3 py-3 text-sm leading-6 text-slate-300">
              Recovery signal: {drill.recoverySignal}
            </div>
          </div>
        ))}
      </Section>

      <Section title="Schema Controls">
        {data.schemaControls.map((item) => (
          <div key={item.title} className="rounded-[22px] border border-white/10 bg-white/5 p-4">
            <div className="flex items-center justify-between gap-3">
              <div className="text-sm font-semibold text-white">{item.title}</div>
              <div className="rounded-full border border-white/10 bg-black/20 px-3 py-1 text-[11px] text-slate-300">{item.currentState}</div>
            </div>
            <div className="mt-2 text-sm leading-6 text-slate-300">{item.rule}</div>
            <div className="mt-3 text-xs leading-5 text-rose-200/80">failure mode: {item.failureMode}</div>
            <SubList title="Operator Checks" items={item.operatorChecks} />
          </div>
        ))}
      </Section>

      <Section title="Capacity Controls">
        {data.capacityControls.map((item) => (
          <div key={item.title} className="rounded-[22px] border border-white/10 bg-white/5 p-4">
            <div className="flex items-center justify-between gap-3">
              <div className="text-sm font-semibold text-white">{item.title}</div>
              <div className="rounded-full border border-white/10 bg-black/20 px-3 py-1 text-[11px] text-slate-300">{item.status}</div>
            </div>
            <div className="mt-2 text-xs uppercase tracking-[0.14em] text-slate-500">{item.metric}</div>
            <div className="mt-2 text-sm text-emerald-100">{item.threshold}</div>
            <div className="mt-3 rounded-2xl border border-emerald-200/20 bg-emerald-500/10 px-3 py-3 text-sm leading-6 text-emerald-100">
              current: {item.currentValue}
            </div>
            <div className="mt-3 text-sm leading-6 text-slate-300">{item.whyItMatters}</div>
            <SubList title="Operator Actions" items={item.operatorActions} />
          </div>
        ))}
      </Section>

      <Section title="Operator Sequence">
        <List items={data.operatorSequence} />
      </Section>

      <Section title="実装アンカー">
        {data.implementationAnchors.map((anchor) => (
          <div key={`${anchor.path}-${anchor.title}`} className="rounded-[20px] border border-white/8 bg-slate-950/55 px-4 py-4">
            <div className="text-sm font-semibold text-white">{anchor.title}</div>
            <div className="mt-2 text-xs leading-5 text-slate-400">{anchor.focus}</div>
            <div className="mt-3 break-all text-xs text-cyan-200">{anchor.path}</div>
          </div>
        ))}
      </Section>
    </div>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-2xl border border-white/8 bg-black/20 px-3 py-3">
      <div className="text-[11px] uppercase tracking-[0.14em] text-slate-500">{label}</div>
      <div className="mt-2 text-sm font-medium text-white">{value}</div>
    </div>
  );
}

function Section({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section className="space-y-3">
      <div className="text-sm font-semibold text-white">{title}</div>
      {children}
    </section>
  );
}

function List({ items }: { items: string[] }) {
  return (
    <div className="mt-3 space-y-2">
      {items.map((item) => (
        <div key={item} className="rounded-2xl border border-white/8 bg-slate-950/55 px-3 py-3 text-sm leading-6 text-slate-300">
          {item}
        </div>
      ))}
    </div>
  );
}

function SubList({ title, items }: { title: string; items: string[] }) {
  return (
    <div className="mt-3">
      <div className="text-xs uppercase tracking-[0.14em] text-slate-500">{title}</div>
      <List items={items} />
    </div>
  );
}
