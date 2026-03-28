import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { useEffect, useState } from 'react';
import { Dashboard } from './components/Dashboard';
import { MobileLearningConsole } from './components/mobile/MobileLearningConsole';
import { TradingView } from './components/TradingView';
import { StrategySettings } from './components/StrategySettings';

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
        <MobileLearningConsole path={path} onNavigate={navigate} onExit={() => navigate('/')} />
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
        {activeTab === 'dashboard' && <Dashboard />}
        {activeTab === 'trading' && <TradingView />}
        {activeTab === 'strategy' && <StrategySettings />}
      </div>
    </QueryClientProvider>
  );
}

export default App;
