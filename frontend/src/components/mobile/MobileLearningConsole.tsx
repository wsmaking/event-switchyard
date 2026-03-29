import { useEffect, useRef } from 'react';
import { useMobileHome, useUpdateMobileProgress } from '../../hooks/useMobileLearning';
import { useMobileOrders } from '../../hooks/useMobileStudy';
import type { MobileRouteState } from '../../types/mobile';
import { MobileArchitectureView } from './MobileArchitectureView';
import { MobileAssetClassView } from './MobileAssetClassView';
import { MobileCardsView } from './MobileCardsView';
import { MobileDrillsView } from './MobileDrillsView';
import { MobileHomeView } from './MobileHomeView';
import { MobileInstitutionalFlowView } from './MobileInstitutionalFlowView';
import { MobileMarketStructureView } from './MobileMarketStructureView';
import { MobileOperationsView } from './MobileOperationsView';
import { MobileOrderStudyView } from './MobileOrderStudyView';
import { MobilePostTradeView } from './MobilePostTradeView';
import { MobileRiskView } from './MobileRiskView';

interface MobileLearningConsoleProps {
  path: string;
  onNavigate: (path: string) => void;
  onExit: () => void;
}

export function MobileLearningConsole({ path, onNavigate, onExit }: MobileLearningConsoleProps) {
  const route = parseMobileRoute(path);
  const { data: home, isLoading, isError, error } = useMobileHome();
  const { data: orders } = useMobileOrders();
  const updateProgress = useUpdateMobileProgress();
  const lastAnchorRef = useRef<string | null>(null);

  const activeOrderId = route.orderId ?? home?.continueLearning.orderId ?? orders?.[0]?.id ?? null;
  const activeSymbol = route.symbol ?? orders?.find((order) => order.id === activeOrderId)?.symbol ?? orders?.[0]?.symbol ?? '7203';

  useEffect(() => {
    if (route.section === 'home') {
      return;
    }
    const anchorOrderId =
      route.section === 'orders' ||
      route.section === 'ledger' ||
      route.section === 'architecture' ||
      route.section === 'market' ||
      route.section === 'institutional' ||
      route.section === 'posttrade'
        ? activeOrderId
        : null;
    const signature = `${path}|${anchorOrderId ?? ''}|${route.cardId ?? ''}`;
    if (lastAnchorRef.current === signature) {
      return;
    }
    lastAnchorRef.current = signature;
    updateProgress.mutate({
      type: 'anchor',
      route: path,
      orderId: anchorOrderId,
      cardId: route.cardId,
    });
  }, [path, route.section, route.cardId, activeOrderId]);

  return (
    <div
      className="min-h-screen bg-[radial-gradient(circle_at_top,rgba(20,83,45,0.28),transparent_28%),linear-gradient(180deg,#08111d_0%,#0f172a_52%,#070b12_100%)] text-slate-100"
      style={
        {
          '--mobile-muted': 'rgba(226,232,240,0.58)',
        } as React.CSSProperties
      }
    >
      <header className="sticky top-0 z-30 border-b border-white/10 bg-slate-950/75 backdrop-blur">
        <div className="mx-auto flex max-w-md items-center justify-between px-4 py-4">
            <div>
              <div className="text-[11px] uppercase tracking-[0.24em] text-emerald-200/70">Event Switchyard</div>
            <div className="mt-1 text-base font-semibold text-white">学習コンソール</div>
            </div>
          <div className="flex items-center gap-2">
            {home?.deliveryMode === 'ON_DEVICE' && (
              <div className="rounded-full border border-emerald-300/20 bg-emerald-400/10 px-3 py-2 text-[11px] font-medium text-emerald-100">
                端末内
              </div>
            )}
            <button
              onClick={onExit}
              className="rounded-full border border-white/10 bg-white/5 px-3 py-2 text-xs font-medium text-slate-200"
            >
              Desktop
            </button>
          </div>
        </div>
      </header>

      <main className="mx-auto max-w-md">
        {route.section === 'home' && (
          <MobileHomeView
            home={home}
            isLoading={isLoading}
            isError={isError}
            errorMessage={error instanceof Error ? error.message : null}
            onNavigate={onNavigate}
          />
        )}
        {route.section === 'orders' && (
          <MobileOrderStudyView focus="lifecycle" orderId={activeOrderId} onNavigate={onNavigate} />
        )}
        {route.section === 'market' && (
          <MobileMarketStructureView symbol={activeSymbol} orderId={activeOrderId} onNavigate={onNavigate} />
        )}
        {route.section === 'institutional' && <MobileInstitutionalFlowView onNavigate={onNavigate} />}
        {route.section === 'posttrade' && <MobilePostTradeView onNavigate={onNavigate} />}
        {route.section === 'ledger' && (
          <MobileOrderStudyView focus="ledger" orderId={activeOrderId} onNavigate={onNavigate} />
        )}
        {route.section === 'architecture' && <MobileArchitectureView home={home} orderId={activeOrderId} />}
        {route.section === 'assets' && <MobileAssetClassView />}
        {route.section === 'operations' && <MobileOperationsView />}
        {route.section === 'cards' && <MobileCardsView cardId={route.cardId} onNavigate={onNavigate} />}
        {route.section === 'drills' && <MobileDrillsView drillId={route.drillId} onNavigate={onNavigate} />}
        {route.section === 'risk' && <MobileRiskView />}
      </main>

      <nav className="fixed inset-x-0 bottom-0 z-30 border-t border-white/10 bg-slate-950/92 backdrop-blur">
        <div className="mx-auto flex max-w-md gap-1 overflow-x-auto px-2 py-2">
          <NavButton label="ホーム" active={route.section === 'home'} onClick={() => onNavigate('/mobile')} />
          <NavButton label="注文" active={route.section === 'orders'} onClick={() => onNavigate(activeOrderId ? `/mobile/orders/${activeOrderId}` : '/mobile/orders')} />
          <NavButton label="市場" active={route.section === 'market'} onClick={() => onNavigate(`/mobile/market/${activeSymbol}`)} />
          <NavButton label="執行" active={route.section === 'institutional'} onClick={() => onNavigate('/mobile/institutional')} />
          <NavButton label="決済" active={route.section === 'posttrade'} onClick={() => onNavigate('/mobile/posttrade')} />
          <NavButton label="台帳" active={route.section === 'ledger'} onClick={() => onNavigate('/mobile/ledger')} />
          <NavButton label="構成" active={route.section === 'architecture'} onClick={() => onNavigate('/mobile/architecture')} />
          <NavButton label="資産" active={route.section === 'assets'} onClick={() => onNavigate('/mobile/assets')} />
          <NavButton label="運用" active={route.section === 'operations'} onClick={() => onNavigate('/mobile/operations')} />
          <NavButton label="判断" active={route.section === 'cards'} onClick={() => onNavigate('/mobile/cards')} />
          <NavButton label="反復" active={route.section === 'drills'} onClick={() => onNavigate('/mobile/drills')} />
          <NavButton label="リスク" active={route.section === 'risk'} onClick={() => onNavigate('/mobile/risk')} />
        </div>
      </nav>
    </div>
  );
}

function NavButton({
  label,
  active,
  onClick,
}: {
  label: string;
  active: boolean;
  onClick: () => void;
}) {
  return (
    <button
      onClick={onClick}
      className={`shrink-0 rounded-2xl px-3 py-3 text-[11px] font-medium transition ${active ? 'bg-emerald-500/15 text-emerald-100' : 'text-slate-400 hover:text-slate-100'}`}
    >
      {label}
    </button>
  );
}

export function parseMobileRoute(path: string): MobileRouteState {
  const normalized = path.split('?')[0].replace(/\/+$/, '') || '/';
  const segments = normalized.split('/').filter(Boolean);
  if (segments[0] !== 'mobile' || segments.length === 0) {
    return { section: 'home', orderId: null, symbol: null, cardId: null, drillId: null };
  }
  if (segments.length === 1) {
    return { section: 'home', orderId: null, symbol: null, cardId: null, drillId: null };
  }
  switch (segments[1]) {
    case 'orders':
      return { section: 'orders', orderId: segments[2] ?? null, symbol: null, cardId: null, drillId: null };
    case 'market':
      return { section: 'market', orderId: null, symbol: segments[2] ?? null, cardId: null, drillId: null };
    case 'institutional':
      return { section: 'institutional', orderId: null, symbol: null, cardId: null, drillId: null };
    case 'posttrade':
      return { section: 'posttrade', orderId: null, symbol: null, cardId: null, drillId: null };
    case 'ledger':
      return { section: 'ledger', orderId: null, symbol: null, cardId: null, drillId: null };
    case 'architecture':
      return { section: 'architecture', orderId: null, symbol: null, cardId: null, drillId: null };
    case 'assets':
      return { section: 'assets', orderId: null, symbol: null, cardId: null, drillId: null };
    case 'operations':
      return { section: 'operations', orderId: null, symbol: null, cardId: null, drillId: null };
    case 'cards':
      return { section: 'cards', orderId: null, symbol: null, cardId: segments[2] ?? null, drillId: null };
    case 'drills':
      return { section: 'drills', orderId: null, symbol: null, cardId: null, drillId: segments[2] ?? null };
    case 'risk':
      return { section: 'risk', orderId: null, symbol: null, cardId: null, drillId: null };
    default:
      return { section: 'home', orderId: null, symbol: null, cardId: null, drillId: null };
  }
}
