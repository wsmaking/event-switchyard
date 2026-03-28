import { useOpsOverview } from '../../hooks/useOrders';
import type { MobileHome } from '../../types/mobile';

interface MobileArchitectureViewProps {
  home: MobileHome | undefined;
  orderId: string | null;
}

export function MobileArchitectureView({ home, orderId }: MobileArchitectureViewProps) {
  const { data: opsOverview, isLoading, isError, error } = useOpsOverview(orderId);

  if (isLoading) {
    return <div className="px-4 py-6 text-sm text-[color:var(--mobile-muted)]">ops overview を読み込み中...</div>;
  }

  if (isError || !opsOverview) {
    return (
      <div className="px-4 py-6 text-sm text-rose-200">
        ops overview を取得できませんでした
        <div className="mt-2 text-xs text-rose-200/80">{error instanceof Error ? error.message : 'unknown_error'}</div>
      </div>
    );
  }

  const sequenceGaps = (opsOverview.omsStats?.sequenceGaps ?? 0) + (opsOverview.backOfficeStats?.sequenceGaps ?? 0);
  const pending = (opsOverview.omsStats?.pendingOrphanCount ?? 0) + (opsOverview.backOfficeStats?.pendingOrphanCount ?? 0);
  const dlq = (opsOverview.omsStats?.deadLetterCount ?? 0) + (opsOverview.backOfficeStats?.deadLetterCount ?? 0);

  return (
    <div className="space-y-4 px-4 py-5 pb-24">
      <section className="rounded-[28px] border border-white/10 bg-[linear-gradient(135deg,rgba(30,41,59,0.94),rgba(2,6,23,0.96))] p-5">
        <div className="text-[11px] uppercase tracking-[0.22em] text-cyan-100/70">Architecture Map</div>
        <h1 className="mt-2 text-2xl font-semibold text-white">注文から ledger までの責務境界</h1>
        <p className="mt-2 text-sm leading-6 text-slate-300">
          hot path を壊さず、projection と ops を Java 側へ寄せる現在の構成を一枚で確認する。
        </p>
      </section>

      <div className="space-y-2">
        <NodeCard title="Frontend / Mobile UI" subtitle="学習導線 / final-out / cards / risk sandbox" status="READ MODEL" />
        <Connector />
        <NodeCard title="app-java" subtitle="BFF / aggregation / mobile progress / educational risk" status="RUNNING" />
        <Connector />
        <NodeCard title="gateway-rust" subtitle="accept hot path / venue control / outbox" status="HOT PATH" />
        <Connector />
        <NodeCard
          title="Kafka Bus"
          subtitle={`oms ${opsOverview.omsBusStats?.topic ?? '-'} / bo ${opsOverview.backOfficeBusStats?.topic ?? '-'} / pending ${(opsOverview.omsBusStats?.pending ?? 0) + (opsOverview.backOfficeBusStats?.pending ?? 0)}`}
          status={opsOverview.omsBusStats?.state ?? 'UNKNOWN'}
        />
        <Connector />
        <NodeCard title="oms-java" subtitle={`orders / reservations / aggregateSeq / pending ${opsOverview.omsStats?.pendingOrphanCount ?? 0}`} status={opsOverview.omsStats?.state ?? 'UNKNOWN'} />
        <Connector />
        <NodeCard title="backoffice-java" subtitle={`ledger / positions / cash / dlq ${opsOverview.backOfficeStats?.deadLetterCount ?? 0}`} status={opsOverview.backOfficeStats?.state ?? 'UNKNOWN'} />
        <Connector />
        <NodeCard title="Postgres" subtitle="projection store / aggregate progress / replay recovery" status="PERSISTENCE" />
      </div>

      <section className="grid grid-cols-3 gap-3">
        <OpsMetric label="Gap" value={sequenceGaps} />
        <OpsMetric label="Pending" value={pending} />
        <OpsMetric label="DLQ" value={dlq} />
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-base font-semibold text-white">Reconcile</div>
        <div className="mt-4 grid gap-3">
          <IssueCard
            title="OMS"
            body={opsOverview.omsReconcile?.issues?.length ? opsOverview.omsReconcile.issues.join(' / ') : 'issue なし'}
          />
          <IssueCard
            title="BackOffice"
            body={opsOverview.backOfficeReconcile?.issues?.length ? opsOverview.backOfficeReconcile.issues.join(' / ') : 'issue なし'}
          />
        </div>
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-base font-semibold text-white">面接で話すポイント</div>
        <div className="mt-4 space-y-3">
          <Bullet body="gateway-rust には学習 UI や aggregation を載せず、hot path を守る。" />
          <Bullet body="OMS は注文状態、BackOffice は ledger / cash / positions の正本責務。" />
          <Bullet body="aggregateSeq gap は pending orphan として保留し、DLQ は operator action に回す。" />
          <Bullet body={home?.mainlineStatus.summary ?? 'mainline status を確認して運用説明へ繋ぐ。'} />
        </div>
      </section>
    </div>
  );
}

function NodeCard({ title, subtitle, status }: { title: string; subtitle: string; status: string }) {
  return (
    <div className="rounded-[24px] border border-white/10 bg-white/5 p-4">
      <div className="flex items-start justify-between gap-3">
        <div>
          <div className="text-sm font-semibold text-white">{title}</div>
          <div className="mt-2 text-sm leading-6 text-slate-300">{subtitle}</div>
        </div>
        <div className="rounded-full border border-cyan-400/30 bg-cyan-400/10 px-3 py-1 text-[11px] font-medium text-cyan-100">
          {status}
        </div>
      </div>
    </div>
  );
}

function Connector() {
  return <div className="mx-auto h-6 w-px bg-gradient-to-b from-white/20 to-transparent" />;
}

function OpsMetric({ label, value }: { label: string; value: number }) {
  return (
    <div className="rounded-[22px] border border-white/10 bg-slate-950/55 px-3 py-4 text-center">
      <div className="text-[11px] uppercase tracking-[0.18em] text-slate-500">{label}</div>
      <div className="mt-2 text-xl font-semibold text-white">{value}</div>
    </div>
  );
}

function IssueCard({ title, body }: { title: string; body: string }) {
  return (
    <div className="rounded-[20px] border border-white/8 bg-slate-950/55 px-4 py-4">
      <div className="text-sm font-semibold text-white">{title}</div>
      <div className="mt-2 text-sm leading-6 text-slate-300">{body}</div>
    </div>
  );
}

function Bullet({ body }: { body: string }) {
  return (
    <div className="rounded-[20px] border border-white/8 bg-slate-950/55 px-4 py-4 text-sm leading-6 text-slate-300">
      {body}
    </div>
  );
}
