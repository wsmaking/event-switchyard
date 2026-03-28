import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { lazy, Suspense, useEffect, useState } from 'react';

const Dashboard = lazy(() => import('./components/Dashboard').then((module) => ({ default: module.Dashboard })));
const TradingView = lazy(() => import('./components/TradingView').then((module) => ({ default: module.TradingView })));
const StrategySettings = lazy(() => import('./components/StrategySettings').then((module) => ({ default: module.StrategySettings })));
const MobileLearningConsole = lazy(() =>
  import('./components/mobile/MobileLearningConsole').then((module) => ({ default: module.MobileLearningConsole }))
);

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
    },
  },
});

function App() {
  const [activeTab, setActiveTab] = useState<'dashboard' | 'trading' | 'strategy'>('dashboard');
  const [path, setPath] = useState(() => window.location.pathname || '/');

  useEffect(() => {
    const handlePopState = () => {
      setPath(window.location.pathname || '/');
    };
    window.addEventListener('popstate', handlePopState);
    return () => window.removeEventListener('popstate', handlePopState);
  }, []);

  const navigate = (nextPath: string) => {
    if ((window.location.pathname || '/') !== nextPath) {
      window.history.pushState({}, '', nextPath);
    }
    setPath(nextPath);
  };

  if (path.startsWith('/mobile')) {
    return (
      <QueryClientProvider client={queryClient}>
        <Suspense fallback={<LoadingScreen message="Mobile Learning を読み込み中..." />}>
          <MobileLearningConsole path={path} onNavigate={navigate} onExit={() => navigate('/')} />
        </Suspense>
      </QueryClientProvider>
    );
  }

  return (
    <QueryClientProvider client={queryClient}>
      <div className="min-h-screen text-slate-100">
        {/* Tab Navigation */}
        <div className="sticky top-0 z-30 border-b border-slate-800/70 bg-slate-950/70 backdrop-blur">
          <div className="mx-auto flex max-w-7xl items-center justify-between px-4 sm:px-6 lg:px-8">
            <div className="flex items-center gap-3 py-4">
              <div className="h-9 w-9 rounded-xl border border-slate-700/60 bg-slate-900/80 text-blue-200 grid place-items-center text-sm font-semibold">
                ES
              </div>
              <div>
                <div className="text-sm font-semibold tracking-wide text-slate-100">Event Switchyard</div>
                <div className="text-xs text-slate-500">Execution Gateway + SOR Console</div>
              </div>
            </div>
            <div className="flex items-center gap-6 text-sm font-medium">
              <button
                onClick={() => navigate('/mobile')}
                className="rounded-full border border-emerald-400/30 bg-emerald-500/10 px-3 py-2 text-xs font-semibold uppercase tracking-[0.18em] text-emerald-100 transition hover:bg-emerald-500/20"
              >
                Mobile Learning
              </button>
              <button
                onClick={() => setActiveTab('dashboard')}
                className={`pb-4 pt-4 border-b-2 transition ${
                  activeTab === 'dashboard'
                    ? 'border-blue-400 text-blue-200'
                    : 'border-transparent text-slate-400 hover:text-slate-200 hover:border-slate-600'
                }`}
              >
                Operations Dashboard
              </button>
              <button
                onClick={() => setActiveTab('trading')}
                className={`pb-4 pt-4 border-b-2 transition ${
                  activeTab === 'trading'
                    ? 'border-blue-400 text-blue-200'
                    : 'border-transparent text-slate-400 hover:text-slate-200 hover:border-slate-600'
                }`}
              >
                Trading
              </button>
              <button
                onClick={() => setActiveTab('strategy')}
                className={`pb-4 pt-4 border-b-2 transition ${
                  activeTab === 'strategy'
                    ? 'border-blue-400 text-blue-200'
                    : 'border-transparent text-slate-400 hover:text-slate-200 hover:border-slate-600'
                }`}
              >
                Strategy
              </button>
            </div>
          </div>
        </div>

        {/* Tab Content */}
        <Suspense fallback={<LoadingScreen message="画面を読み込み中..." />}>
          {activeTab === 'dashboard' && <Dashboard />}
          {activeTab === 'trading' && <TradingView />}
          {activeTab === 'strategy' && <StrategySettings />}
        </Suspense>
      </div>
    </QueryClientProvider>
  );
}

function LoadingScreen({ message }: { message: string }) {
  return (
    <div className="flex min-h-screen items-center justify-center text-sm text-slate-300">
      {message}
    </div>
  );
}

export default App;
