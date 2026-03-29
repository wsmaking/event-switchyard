import { useMobileInstitutionalFlow } from '../../hooks/useMobileLearning';
import type { MobileInstitutionalFlow } from '../../types/mobile';
import { formatCurrency, formatNumber, formatPercent } from './mobileUtils';

interface MobileInstitutionalFlowViewProps {
  onNavigate: (path: string) => void;
}

export function MobileInstitutionalFlowView({ onNavigate }: MobileInstitutionalFlowViewProps) {
  const { data, isLoading, isError, error } = useMobileInstitutionalFlow();

  if (isLoading) {
    return <div className="px-4 py-6 text-sm text-[color:var(--mobile-muted)]">執行判断を読み込み中...</div>;
  }

  if (isError || !data) {
    return (
      <div className="px-4 py-6 text-sm text-rose-200">
        執行判断を取得できませんでした
        <div className="mt-2 text-xs text-rose-200/80">{error instanceof Error ? error.message : 'unknown_error'}</div>
      </div>
    );
  }

  return (
    <div className="space-y-4 px-4 py-5 pb-24">
      <section className="rounded-[28px] border border-white/10 bg-[linear-gradient(135deg,rgba(29,78,216,0.28),rgba(15,23,42,0.96))] p-5">
        <div className="text-[11px] uppercase tracking-[0.22em] text-sky-100/70">機関投資家フロー</div>
        <h1 className="mt-2 text-2xl font-semibold text-white">親注文から子注文、配賦まで</h1>
        <p className="mt-2 text-sm leading-6 text-slate-300">{data.clientIntent}</p>
        <div className="mt-4 flex flex-wrap gap-2 text-xs text-slate-300">
          <Badge label={`銘柄 ${data.symbolName}`} />
          <Badge label={`スタイル ${data.parentOrderPlan.chosenStyle}`} />
          <Badge label={`参加率 ${formatPercent(data.parentOrderPlan.targetParticipationPercent, 0)}`} />
        </div>
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="flex items-center justify-between gap-3">
          <div>
            <div className="text-sm font-semibold text-white">親注文の設計</div>
            <div className="mt-2 text-sm text-slate-300">
              arrival mid {formatCurrency(data.parentOrderPlan.arrivalMidPrice)} / total {formatNumber(data.parentOrderPlan.totalQuantity)} 株 / 窓 {data.parentOrderPlan.scheduleWindowMinutes} 分
            </div>
          </div>
          {data.anchorOrderId && (
            <button
              onClick={() => onNavigate(`/mobile/orders/${data.anchorOrderId}`)}
              className="rounded-full border border-sky-300/30 bg-sky-500/10 px-3 py-2 text-[11px] font-medium text-sky-100"
            >
              注文へ
            </button>
          )}
        </div>
        <div className="mt-4 space-y-2">
          {data.parentOrderPlan.whyNotOtherChoices.map((item) => (
            <div key={item} className="rounded-2xl border border-white/8 bg-black/20 px-3 py-3 text-sm leading-6 text-slate-300">
              {item}
            </div>
          ))}
        </div>
      </section>

      <section className="space-y-3">
        <div className="text-sm font-semibold text-white">執行スタイル比較</div>
        {data.executionStyles.map((style) => (
          <div key={style.name} className="rounded-[22px] border border-white/10 bg-white/5 p-4">
            <div className="flex items-center justify-between gap-3">
              <div className="text-base font-semibold text-white">{style.name}</div>
              <div className="rounded-full border border-white/10 bg-black/20 px-3 py-1 text-[11px] text-slate-300">{style.useCase}</div>
            </div>
            <div className="mt-3 text-sm leading-6 text-slate-300">{style.businessRule}</div>
            <div className="mt-3 rounded-2xl border border-white/8 bg-slate-950/55 px-3 py-3 text-sm leading-6 text-slate-300">
              {style.systemImplication}
            </div>
            <div className="mt-3 space-y-2">
              {style.tradeoffs.map((tradeoff) => (
                <div key={tradeoff} className="text-xs leading-5 text-slate-400">
                  ・{tradeoff}
                </div>
              ))}
            </div>
          </div>
        ))}
      </section>

      <section className="space-y-3">
        <div className="text-sm font-semibold text-white">子注文の刻み方</div>
        {data.childOrders.map((slice) => (
          <div key={slice.id} className="rounded-[22px] border border-white/10 bg-white/5 p-4">
            <div className="flex items-center justify-between gap-3">
              <div>
                <div className="text-sm font-semibold text-white">{slice.timeBucketLabel}</div>
                <div className="mt-1 text-xs text-slate-400">{slice.venueIntent}</div>
              </div>
              <div className="rounded-full border border-emerald-300/20 bg-emerald-500/10 px-3 py-1 text-[11px] text-emerald-100">
                {formatNumber(slice.plannedQuantity)} 株
              </div>
            </div>
            <div className="mt-3 grid grid-cols-2 gap-3 text-sm">
              <MetricCard label="benchmark" value={formatCurrency(slice.benchmarkPrice)} />
              <MetricCard label="想定約定" value={formatCurrency(slice.expectedFillPrice)} />
              <MetricCard label="slippage" value={formatPercent(slice.expectedSlippageBps / 100, 2)} />
              <MetricCard label="id" value={slice.id} />
            </div>
            <div className="mt-3 rounded-2xl border border-white/8 bg-black/20 px-3 py-3 text-sm leading-6 text-slate-300">
              {slice.learningPoint}
            </div>
          </div>
        ))}
      </section>

      {data.parentExecutionState && (
        <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
          <div className="text-sm font-semibold text-white">親注文の進捗</div>
          <div className="mt-4 grid grid-cols-2 gap-3 text-sm">
            <MetricCard label="status" value={data.parentExecutionState.parentStatus} />
            <MetricCard label="style" value={data.parentExecutionState.executionStyle} />
            <MetricCard label="target" value={`${formatNumber(data.parentExecutionState.targetQuantity)} 株`} />
            <MetricCard label="executed" value={`${formatNumber(data.parentExecutionState.executedQuantity)} 株`} />
            <MetricCard label="remaining" value={`${formatNumber(data.parentExecutionState.remainingQuantity)} 株`} />
            <MetricCard label="window" value={data.parentExecutionState.scheduleWindowLabel} />
            <MetricCard label="target participation" value={formatPercent(data.parentExecutionState.participationTargetPercent, 0)} />
            <MetricCard label="actual participation" value={formatPercent(data.parentExecutionState.participationActualPercent, 0)} />
          </div>
          <div className="mt-4 space-y-2">
            {data.parentExecutionState.operatorAlerts.map((alert) => (
              <div key={alert} className="rounded-2xl border border-amber-300/15 bg-amber-500/10 px-3 py-3 text-sm leading-6 text-amber-50/90">
                {alert}
              </div>
            ))}
          </div>
          <div className="mt-4 space-y-3">
            {data.parentExecutionState.childStates.map((child) => (
              <div key={child.childId} className="rounded-[20px] border border-white/8 bg-slate-950/55 px-4 py-4">
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <div className="text-sm font-semibold text-white">{child.childId}</div>
                    <div className="mt-1 text-xs text-slate-400">{child.venueIntent}</div>
                  </div>
                  <div className="rounded-full border border-white/10 bg-black/20 px-3 py-1 text-[11px] text-slate-300">{child.state}</div>
                </div>
                <div className="mt-3 grid grid-cols-2 gap-3">
                  <MetricCard label="planned" value={`${formatNumber(child.plannedQuantity)} 株`} />
                  <MetricCard label="executed" value={`${formatNumber(child.executedQuantity)} 株`} />
                  <MetricCard label="remaining" value={`${formatNumber(child.remainingQuantity)} 株`} />
                  <MetricCard label="avg fill" value={child.executedQuantity > 0 ? formatCurrency(child.averageFillPrice) : '-'} />
                </div>
                <div className="mt-3 rounded-2xl border border-white/8 bg-black/20 px-3 py-3 text-xs leading-5 text-slate-300">
                  {child.nextAction}
                </div>
              </div>
            ))}
          </div>
        </section>
      )}

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-sm font-semibold text-white">配賦とブロック説明</div>
        <div className="mt-2 text-sm leading-6 text-slate-300">{data.allocationPlan.settlementNote}</div>
        <div className="mt-4 grid grid-cols-2 gap-3">
          <MetricCard label="book" value={data.allocationPlan.blockBook} />
          <MetricCard label="平均単価" value={formatCurrency(data.allocationPlan.averagePrice)} />
        </div>
        <div className="mt-4 space-y-3">
          {data.allocationPlan.allocations.map((allocation) => (
            <div key={allocation.targetBook} className="rounded-2xl border border-white/8 bg-slate-950/55 px-3 py-3">
              <div className="flex items-center justify-between gap-3">
                <div className="text-sm font-semibold text-white">{allocation.targetBook}</div>
                <div className="text-xs text-slate-300">{formatPercent(allocation.ratioPercent, 0)}</div>
              </div>
              <div className="mt-2 text-sm text-slate-300">{formatNumber(allocation.quantity)} 株</div>
              <div className="mt-2 text-xs leading-5 text-slate-400">{allocation.note}</div>
            </div>
          ))}
        </div>
      </section>

      {data.allocationState && (
        <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
          <div className="text-sm font-semibold text-white">配賦の正本状態</div>
          <div className="mt-4 grid grid-cols-2 gap-3">
            <MetricCard label="status" value={data.allocationState.allocationStatus} />
            <MetricCard label="avg price" value={formatCurrency(data.allocationState.allocationAveragePrice)} />
            <MetricCard label="allocated" value={`${formatNumber(data.allocationState.allocatedQuantity)} 株`} />
            <MetricCard label="pending" value={`${formatNumber(data.allocationState.pendingQuantity)} 株`} />
          </div>
          <div className="mt-4 space-y-3">
            {data.allocationState.books.map((book) => (
              <div key={book.book} className="rounded-[20px] border border-white/8 bg-slate-950/55 px-4 py-4">
                <div className="flex items-center justify-between gap-3">
                  <div className="text-sm font-semibold text-white">{book.book}</div>
                  <div className="rounded-full border border-white/10 bg-black/20 px-3 py-1 text-[11px] text-slate-300">{book.status}</div>
                </div>
                <div className="mt-3 grid grid-cols-2 gap-3">
                  <MetricCard label="target" value={`${formatNumber(book.targetQuantity)} 株`} />
                  <MetricCard label="allocated" value={`${formatNumber(book.allocatedQuantity)} 株`} />
                </div>
                <div className="mt-3 text-xs leading-5 text-slate-400">{book.rationale}</div>
              </div>
            ))}
          </div>
          <div className="mt-4 space-y-2">
            {data.allocationState.controls.map((control) => (
              <div key={control} className="rounded-2xl border border-white/8 bg-black/20 px-3 py-3 text-sm leading-6 text-slate-300">
                {control}
              </div>
            ))}
          </div>
        </section>
      )}

      {data.accountHierarchy && (
        <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
          <div className="text-sm font-semibold text-white">Account / Book Hierarchy</div>
          <div className="mt-4 grid grid-cols-2 gap-3">
            <MetricCard label="client" value={data.accountHierarchy.clientName} />
            <MetricCard label="entity" value={data.accountHierarchy.legalEntity} />
            <MetricCard label="desk" value={data.accountHierarchy.desk} />
            <MetricCard label="strategy" value={data.accountHierarchy.strategy} />
            <MetricCard label="clearing" value={data.accountHierarchy.clearingBroker} />
            <MetricCard label="custodian" value={data.accountHierarchy.custodian} />
          </div>
          <div className="mt-4 space-y-3">
            {data.accountHierarchy.books.map((book) => (
              <div key={`${book.book}-${book.subAccount}`} className="rounded-[20px] border border-white/8 bg-slate-950/55 px-4 py-4">
                <div className="text-sm font-semibold text-white">{book.book}</div>
                <div className="mt-2 text-xs leading-5 text-slate-400">
                  fund {book.fund} / sub-account {book.subAccount} / trader {book.trader}
                </div>
                <div className="mt-2 text-xs leading-5 text-slate-400">
                  settlement {book.settlementLocation} / mandate {book.mandate}
                </div>
              </div>
            ))}
          </div>
          <div className="mt-4 space-y-3">
            {data.accountHierarchy.permissions.map((permission) => (
              <div key={`${permission.role}-${permission.scope}`} className="rounded-[20px] border border-white/8 bg-black/20 px-4 py-4">
                <div className="flex items-center justify-between gap-3">
                  <div className="text-sm font-semibold text-white">{permission.role}</div>
                  <div className="rounded-full border border-white/10 bg-black/20 px-3 py-1 text-[11px] text-slate-300">
                    {permission.scope}
                  </div>
                </div>
                <div className="mt-3 text-xs leading-5 text-slate-400">{permission.note}</div>
                <div className="mt-3 text-xs leading-5 text-slate-300">
                  actions: {permission.actions.join(' / ')}
                </div>
                <div className="mt-1 text-xs leading-5 text-slate-400">
                  approval: {permission.approvalRequired ? 'required' : 'not required'}
                </div>
              </div>
            ))}
          </div>
          <ChecklistSection title="Hierarchy Reporting Lines" items={data.accountHierarchy.reportingLines} />
          <ChecklistSection title="Hierarchy Control Checks" items={data.accountHierarchy.controlChecks} />
        </section>
      )}

      {data.operatorControlState && (
        <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
          <div className="text-sm font-semibold text-white">Operator Control State</div>
          <div className="mt-4 grid grid-cols-2 gap-3">
            <MetricCard label="workflow" value={data.operatorControlState.workflowState} />
            <MetricCard label="escalation" value={data.operatorControlState.escalationLevel} />
          </div>
          <div className="mt-4 space-y-3">
            {data.operatorControlState.requiredApprovals.map((approval) => (
              <div key={`${approval.role}-${approval.name}`} className="rounded-[20px] border border-amber-300/15 bg-amber-500/10 px-4 py-4">
                <div className="flex items-center justify-between gap-3">
                  <div className="text-sm font-semibold text-amber-50">{approval.name}</div>
                  <div className="rounded-full border border-amber-200/20 bg-black/20 px-3 py-1 text-[11px] text-amber-100">
                    {approval.role} / {approval.state}
                  </div>
                </div>
                <div className="mt-3 text-sm leading-6 text-amber-50/90">{approval.reason}</div>
                <div className="mt-2 text-xs leading-5 text-amber-100/80">next: {approval.nextAction}</div>
              </div>
            ))}
            {data.operatorControlState.acknowledgements.map((ack) => (
              <div key={`${ack.actor}-${ack.action}-${ack.atLabel}`} className="rounded-[20px] border border-white/8 bg-slate-950/55 px-4 py-4">
                <div className="flex items-center justify-between gap-3">
                  <div className="text-sm font-semibold text-white">{ack.actor}</div>
                  <div className="text-xs text-slate-400">{ack.atLabel}</div>
                </div>
                <div className="mt-2 text-sm leading-6 text-slate-300">{ack.action}</div>
                <div className="mt-2 text-xs leading-5 text-slate-400">{ack.note}</div>
              </div>
            ))}
          </div>
          <ChecklistSection title="Permitted Actions" items={data.operatorControlState.permittedActions} />
          <ChecklistSection title="Blocked Actions" items={data.operatorControlState.blockedActions} />
          <ChecklistSection title="Audit Trail" items={data.operatorControlState.auditTrail} />
        </section>
      )}

      <ChecklistSection title="運用で確認すること" items={data.operatorChecks} />
      <AnchorsSection anchors={data.implementationAnchors} />
    </div>
  );
}

function Badge({ label }: { label: string }) {
  return <div className="rounded-full border border-white/10 bg-black/20 px-3 py-1 text-[11px] text-slate-200">{label}</div>;
}

function MetricCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-2xl border border-white/8 bg-black/20 px-3 py-3">
      <div className="text-[11px] uppercase tracking-[0.14em] text-slate-500">{label}</div>
      <div className="mt-2 text-sm font-medium text-white">{value}</div>
    </div>
  );
}

function ChecklistSection({ title, items }: { title: string; items: string[] }) {
  return (
    <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
      <div className="text-sm font-semibold text-white">{title}</div>
      <div className="mt-4 space-y-2">
        {items.map((item) => (
          <div key={item} className="rounded-2xl border border-white/8 bg-slate-950/55 px-3 py-3 text-sm leading-6 text-slate-300">
            {item}
          </div>
        ))}
      </div>
    </section>
  );
}

function AnchorsSection({ anchors }: { anchors: MobileInstitutionalFlow['implementationAnchors'] }) {
  return (
    <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
      <div className="text-sm font-semibold text-white">実装アンカー</div>
      <div className="mt-4 space-y-3">
        {anchors.map((anchor) => (
          <div key={`${anchor.path}-${anchor.title}`} className="rounded-[20px] border border-white/8 bg-slate-950/55 px-4 py-4">
            <div className="text-sm font-semibold text-white">{anchor.title}</div>
            <div className="mt-2 text-xs leading-5 text-slate-400">{anchor.focus}</div>
            <div className="mt-3 text-xs text-cyan-200 break-all">{anchor.path}</div>
          </div>
        ))}
      </div>
    </section>
  );
}
