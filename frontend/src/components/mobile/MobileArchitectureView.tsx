import { useMobileOpsOverview, useMobileOrderStream } from '../../hooks/useMobileStudy';
import type { MobileHome } from '../../types/mobile';

interface MobileArchitectureViewProps {
  home: MobileHome | undefined;
  orderId: string | null;
}

interface ArchitectureAnchor {
  title: string;
  path: string;
  focus: string;
}

export function MobileArchitectureView({ home, orderId }: MobileArchitectureViewProps) {
  const { data: opsOverview, isLoading, isError, error } = useMobileOpsOverview(orderId);
  const streamState = useMobileOrderStream(orderId);

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
  const pulseActive = streamState === 'open' || pending > 0 || dlq > 0;
  const decisions = buildArchitectureDecisions(home?.deliveryMode === 'ON_DEVICE');
  const rejected = buildRejectedChoices();
  const runbook = buildRunbook(sequenceGaps, pending, dlq, home);
  const anchors = buildArchitectureAnchors();

  return (
    <div className="space-y-4 px-4 py-5 pb-24">
      <section className="rounded-[28px] border border-white/10 bg-[linear-gradient(135deg,rgba(30,41,59,0.94),rgba(2,6,23,0.96))] p-5">
        <div className="text-[11px] uppercase tracking-[0.22em] text-cyan-100/70">構成マップ</div>
        <h1 className="mt-2 text-2xl font-semibold text-white">注文から台帳までの責務境界</h1>
        <p className="mt-2 text-sm leading-6 text-slate-300">
          速く受ける責務、注文状態を収束させる責務、台帳を確定する責務、利用者に説明する責務を分けている。ここを見ると、
          なぜ `gateway-rust / oms-java / backoffice-java / app-java` に割っているのかが分かる。
        </p>
        <div className="mt-4 flex flex-wrap gap-2">
          <StatusPill label={`event flow ${streamState}`} active={pulseActive} />
          <StatusPill label={`gap ${sequenceGaps}`} active={sequenceGaps > 0} />
          <StatusPill label={`pending ${pending}`} active={pending > 0} />
          <StatusPill label={`DLQ ${dlq}`} active={dlq > 0} />
        </div>
      </section>

      <section className="space-y-2">
        <NodeCard title="Frontend / Mobile UI" subtitle="利用者への最終表示、学習導線、final-out の読み方" status="READ MODEL" />
        <Connector active={pulseActive} />
        <NodeCard title="app-java" subtitle="BFF、final-out 集約、ops overview、mobile progress と学習 API" status="集約" />
        <Connector active={pulseActive} />
        <NodeCard title="gateway-rust" subtitle="受理、hot path、venue control、outbox / audit" status="HOT PATH" />
        <Connector active={pulseActive} />
        <NodeCard
          title="Kafka Bus"
          subtitle={`oms ${opsOverview.omsBusStats?.topic ?? '-'} / bo ${opsOverview.backOfficeBusStats?.topic ?? '-'} / pending ${(opsOverview.omsBusStats?.pending ?? 0) + (opsOverview.backOfficeBusStats?.pending ?? 0)}`}
          status={opsOverview.omsBusStats?.state ?? 'UNKNOWN'}
        />
        <Connector active={pulseActive} />
        <NodeCard title="oms-java" subtitle={`注文状態、reservation、aggregateSeq、pending ${opsOverview.omsStats?.pendingOrphanCount ?? 0}`} status={opsOverview.omsStats?.state ?? 'UNKNOWN'} />
        <Connector active={pulseActive} />
        <NodeCard title="backoffice-java" subtitle={`ledger、cash、positions、realized PnL、DLQ ${opsOverview.backOfficeStats?.deadLetterCount ?? 0}`} status={opsOverview.backOfficeStats?.state ?? 'UNKNOWN'} />
        <Connector active={pulseActive} />
        <NodeCard title="Postgres" subtitle="projection store、aggregate progress、replay recovery の永続層" status="PERSISTENCE" />
      </section>

      <section className="grid grid-cols-3 gap-3">
        <OpsMetric label="Gap" value={sequenceGaps} />
        <OpsMetric label="Pending" value={pending} />
        <OpsMetric label="DLQ" value={dlq} />
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-base font-semibold text-white">この構成で取ったもの</div>
        <BulletStack items={decisions} tone="emerald" />
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-base font-semibold text-white">意図的に取らなかったもの</div>
        <BulletStack items={rejected} tone="amber" />
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-base font-semibold text-white">運用時にまず見る順番</div>
        <StepRail steps={runbook} />
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-base font-semibold text-white">いまの数字から読む</div>
        <BulletStack
          items={[
            `OMS reconcile: ${opsOverview.omsReconcile?.issues?.length ? opsOverview.omsReconcile.issues.join(' / ') : 'issue なし'}`,
            `BackOffice reconcile: ${opsOverview.backOfficeReconcile?.issues?.length ? opsOverview.backOfficeReconcile.issues.join(' / ') : 'issue なし'}`,
            `OMS processed ${opsOverview.omsStats?.processed ?? 0} / duplicates ${opsOverview.omsStats?.duplicates ?? 0} / replays ${opsOverview.omsStats?.replays ?? 0}`,
            `BackOffice ledger ${opsOverview.backOfficeStats?.ledgerEntryCount ?? 0} / duplicates ${opsOverview.backOfficeStats?.duplicates ?? 0} / replays ${opsOverview.backOfficeStats?.replays ?? 0}`,
            '市場構造と執行品質は app-java の market / final-out 集約で見せ、hot path に押し込まない。',
            home?.mainlineStatus.summary ?? 'mainline status は未取得',
          ]}
        />
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-base font-semibold text-white">実装アンカー</div>
        <AnchorList anchors={anchors} />
      </section>
    </div>
  );
}

function buildArchitectureDecisions(onDevice: boolean | undefined) {
  const decisions = [
    'gateway-rust には受理と venue control を寄せ、学習 UI や重い集約を載せない。hot path を守るため。',
    'OMS は注文状態と reservation の正本、BackOffice は ledger / cash / positions の正本とし、fill を境に責務を切る。',
    'outbox / audit / bus を分け、durable emission、説明責務、consumer 配信を別故障モードとして扱う。',
    'aggregateSeq gap は pending orphan へ保留し、届いた順 apply を避ける。',
    '利用者には final-out で 1 枚に見せるが、内部の truth は分離したままにする。',
  ];
  if (onDevice) {
    decisions.push('server が無くても学習を止めないため、mobile には端末内 pack を持たせる。');
  }
  return decisions;
}

function buildRejectedChoices() {
  return [
    '1 サービスで受理、OMS、台帳、ops を全部持つ設計は取らない。障害責務が濁るため。',
    'accepted 時点で ledger を動かす設計は取らない。filled 前に会計を確定すると壊れるため。',
    'offset だけ見て「追いついた」と言う設計は取らない。aggregate progress、pending、DLQ を別に見る。',
    'reservation を margin と同じ言葉で説明する設計は取らない。受理制御と risk model を分けるため。',
    '不確定状態を勝手に成功/失敗へ丸める設計は取らない。operator judgement を残すため。',
  ];
}

function buildRunbook(
  sequenceGaps: number,
  pending: number,
  dlq: number,
  home: MobileHome | undefined
) {
  return [
    `最初に gap ${sequenceGaps}、pending ${pending}、DLQ ${dlq} を見る。順序問題か手当待ちかを切り分ける。`,
    '次に OMS reconcile と BackOffice reconcile を見て、注文状態の問題か台帳の問題かを切り分ける。',
    'その後に final-out と raw event を見て、利用者表示が何を根拠にしているか確認する。',
    home?.mainlineStatus.summary ?? '最後に mainline status を見て、どこまでが自動復旧でどこからが人判断かを確認する。',
  ];
}

function buildArchitectureAnchors(): ArchitectureAnchor[] {
  return [
    {
      title: '利用者向け最終表示',
      path: '/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/http/OrderApiHandler.java',
      focus: 'final-out を返す app-java の入口。注文状態、台帳結果、執行品質をどう束ねるかを見る。',
    },
    {
      title: '市場構造 API',
      path: '/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/http/MarketApiHandler.java',
      focus: 'bid / ask / spread / venue state を mobile へ返す入口。注文説明と市場説明を分ける理由が見える。',
    },
    {
      title: 'arrival benchmark 保存',
      path: '/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/order/ExecutionBenchmarkStore.java',
      focus: '送信時点の到着時基準を orderId 単位で残し、後から現在価格で説明を塗り替えない。',
    },
    {
      title: 'OMS の順序制御',
      path: '/Users/fujii/Desktop/dev/event-switchyard/oms-java/src/main/java/oms/audit/GatewayAuditIntakeService.java',
      focus: 'aggregateSeq、pending orphan、DLQ、replay の中心。届いた順 apply を避ける理由が見える。',
    },
    {
      title: 'BackOffice の台帳反映',
      path: '/Users/fujii/Desktop/dev/event-switchyard/backoffice-java/src/main/java/backofficejava/audit/GatewayAuditIntakeService.java',
      focus: 'fill 起点で ledger、cash、positions、realized PnL をどう確定するかを見る。',
    },
    {
      title: 'hot path と venue control',
      path: '/Users/fujii/Desktop/dev/event-switchyard/gateway-rust/src/server/http/orders/classic.rs',
      focus: '受理、cancel、replace、amend をどこまで gateway-rust で持つかを見る。',
    },
    {
      title: 'durable emission 境界',
      path: '/Users/fujii/Desktop/dev/event-switchyard/gateway-rust/src/outbox',
      focus: 'outbox / audit / bus を分ける理由の実装側の入口。',
    },
    {
      title: 'event contract',
      path: '/Users/fujii/Desktop/dev/event-switchyard/contracts/bus_event_v2.schema.json',
      focus: 'order identity、eventId、aggregateSeq をどう contract に載せるかを見る。',
    },
  ];
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

function Connector({ active }: { active: boolean }) {
  return (
    <div className="flex justify-center">
      <div className={`h-6 w-px bg-gradient-to-b ${active ? 'from-cyan-300 via-cyan-500/60 to-transparent animate-pulse' : 'from-white/20 to-transparent'}`} />
    </div>
  );
}

function StatusPill({ label, active }: { label: string; active: boolean }) {
  return (
    <div className={`inline-flex items-center gap-2 rounded-full border px-3 py-2 text-xs ${active ? 'border-cyan-300/25 bg-cyan-500/10 text-cyan-100' : 'border-white/10 bg-white/5 text-slate-300'}`}>
      <span className={`h-2 w-2 rounded-full ${active ? 'bg-cyan-300' : 'bg-slate-500'}`} />
      {label}
    </div>
  );
}

function OpsMetric({ label, value }: { label: string; value: number }) {
  return (
    <div className="rounded-[22px] border border-white/10 bg-slate-950/55 px-3 py-4 text-center">
      <div className="text-[11px] uppercase tracking-[0.18em] text-slate-500">{label}</div>
      <div className="mt-2 text-xl font-semibold text-white">{value}</div>
    </div>
  );
}

function StepRail({ steps }: { steps: string[] }) {
  return (
    <div className="mt-4 space-y-3">
      {steps.map((step, index) => (
        <div key={`${index}-${step}`} className="flex gap-3">
          <div className="flex flex-col items-center">
            <div className="mt-1 flex h-5 w-5 items-center justify-center rounded-full bg-cyan-400/15 text-[11px] font-semibold text-cyan-100">
              {index + 1}
            </div>
            {index < steps.length - 1 && <div className="mt-2 h-full w-px bg-white/10" />}
          </div>
          <div className="flex-1 rounded-[20px] border border-white/8 bg-slate-950/55 px-4 py-3 text-sm leading-6 text-slate-300">
            {step}
          </div>
        </div>
      ))}
    </div>
  );
}

function BulletStack({
  items,
  tone = 'slate',
}: {
  items: string[];
  tone?: 'slate' | 'emerald' | 'amber';
}) {
  const toneClass =
    tone === 'emerald'
      ? 'border-emerald-300/15 bg-emerald-500/10 text-emerald-50/90'
      : tone === 'amber'
        ? 'border-amber-300/15 bg-amber-500/10 text-amber-50/90'
        : 'border-white/8 bg-slate-950/55 text-slate-300';

  return (
    <div className="mt-4 space-y-3">
      {items.map((item, index) => (
        <div key={`${index}-${item}`} className={`rounded-[20px] border px-4 py-4 text-sm leading-6 ${toneClass}`}>
          {item}
        </div>
      ))}
    </div>
  );
}

function AnchorList({ anchors }: { anchors: ArchitectureAnchor[] }) {
  return (
    <div className="mt-4 space-y-3">
      {anchors.map((item) => (
        <div key={`${item.title}-${item.path}`} className="rounded-[20px] border border-white/8 bg-slate-950/55 px-4 py-4">
          <div className="text-sm font-semibold text-white">{item.title}</div>
          <div className="mt-2 text-sm leading-6 text-slate-300">{item.focus}</div>
          <div className="mt-3 break-all text-[11px] leading-5 text-cyan-200/80">{item.path}</div>
        </div>
      ))}
    </div>
  );
}
