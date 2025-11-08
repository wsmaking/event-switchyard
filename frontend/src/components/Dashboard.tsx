import { useStats } from '../hooks/useStats';
import { useTimeSeriesData } from '../hooks/useTimeSeriesData';
import { LatencyChart } from './LatencyChart';
import { ThroughputChart } from './ThroughputChart';

export function Dashboard() {
  const { metrics, isLoading, isError, error } = useStats(1000);
  const timeSeriesData = useTimeSeriesData(metrics);

  if (isLoading) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <div className="text-xl">Loading metrics...</div>
      </div>
    );
  }

  if (isError) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <div className="text-xl text-red-500">
          Error: {error instanceof Error ? error.message : 'Unknown error'}
          <div className="text-sm mt-2">
            Make sure the backend is running on http://localhost:8080
          </div>
        </div>
      </div>
    );
  }

  if (!metrics) {
    return <div>No data</div>;
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-900 via-gray-800 to-gray-900 text-white p-6">
      {/* Header */}
      <div className="mb-8">
        <h1 className="text-5xl font-bold bg-gradient-to-r from-blue-400 to-purple-500 bg-clip-text text-transparent mb-2">
          HFT Fast Path Dashboard
        </h1>
        <p className="text-gray-400 text-sm">
          Real-time monitoring • Update interval: 1s • Data retention: 60s
        </p>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
        {/* Latency Summary Card */}
        <div className="bg-gradient-to-br from-blue-900/40 to-blue-800/20 rounded-xl p-6 shadow-2xl border border-blue-500/20">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-sm font-semibold text-blue-300 uppercase tracking-wide">
              Latency
            </h2>
            <div className="text-2xl">⚡</div>
          </div>
          <div className="space-y-3">
            <div>
              <div className="text-gray-400 text-xs mb-1">p50</div>
              <div className="text-2xl font-bold text-blue-400">
                {metrics.latency.p50.toFixed(1)}<span className="text-sm text-gray-400 ml-1">μs</span>
              </div>
            </div>
            <div className="flex justify-between text-sm">
              <div>
                <div className="text-gray-500 text-xs">p99</div>
                <div className="text-yellow-400 font-semibold">{metrics.latency.p99.toFixed(1)}μs</div>
              </div>
              <div>
                <div className="text-gray-500 text-xs">p999</div>
                <div className="text-red-400 font-semibold">{metrics.latency.p999.toFixed(1)}μs</div>
              </div>
            </div>
          </div>
        </div>

        {/* Throughput Card */}
        <div className="bg-gradient-to-br from-green-900/40 to-green-800/20 rounded-xl p-6 shadow-2xl border border-green-500/20">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-sm font-semibold text-green-300 uppercase tracking-wide">
              Throughput
            </h2>
            <div className="text-2xl">🚀</div>
          </div>
          <div className="space-y-3">
            <div>
              <div className="text-gray-400 text-xs mb-1">Total Events</div>
              <div className="text-2xl font-bold text-green-400">
                {metrics.throughput.total.toLocaleString()}
              </div>
            </div>
            <div className="flex justify-between text-sm">
              <div>
                <div className="text-gray-500 text-xs">Fast Path</div>
                <div className="text-green-300 font-semibold">{metrics.throughput.fastPath.toLocaleString()}</div>
              </div>
              <div>
                <div className="text-gray-500 text-xs">Slow Path</div>
                <div className="text-orange-300 font-semibold">{metrics.throughput.slowPath.toLocaleString()}</div>
              </div>
            </div>
          </div>
        </div>

        {/* Fast Path Ratio Card */}
        <div className="bg-gradient-to-br from-purple-900/40 to-purple-800/20 rounded-xl p-6 shadow-2xl border border-purple-500/20">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-sm font-semibold text-purple-300 uppercase tracking-wide">
              Fast Path
            </h2>
            <div className="text-2xl">📈</div>
          </div>
          <div className="space-y-3">
            <div>
              <div className="text-gray-400 text-xs mb-1">Usage Ratio</div>
              <div className="text-4xl font-bold text-purple-400">
                {metrics.throughput.ratio.toFixed(1)}<span className="text-xl text-gray-400 ml-1">%</span>
              </div>
            </div>
            <div className="w-full bg-gray-700 rounded-full h-2">
              <div
                className="bg-gradient-to-r from-purple-500 to-pink-500 h-2 rounded-full transition-all duration-300"
                style={{ width: `${metrics.throughput.ratio}%` }}
              />
            </div>
          </div>
        </div>

        {/* SLO Status Card */}
        <div className="bg-gradient-to-br from-indigo-900/40 to-indigo-800/20 rounded-xl p-6 shadow-2xl border border-indigo-500/20">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-sm font-semibold text-indigo-300 uppercase tracking-wide">
              SLO Status
            </h2>
            <div className="text-2xl">🎯</div>
          </div>
          <div className="space-y-3">
            <div className="flex justify-between items-center">
              <span className="text-gray-400 text-xs">p99 &lt; 100μs</span>
              <span className={`text-xs font-bold px-2 py-1 rounded ${
                metrics.latency.p99 < 100
                  ? 'bg-green-500/20 text-green-400'
                  : 'bg-red-500/20 text-red-400'
              }`}>
                {metrics.latency.p99 < 100 ? 'PASS' : 'FAIL'}
              </span>
            </div>
            <div className="flex justify-between items-center">
              <span className="text-gray-400 text-xs">Tail Ratio &lt; 12</span>
              <span className={`text-xs font-bold px-2 py-1 rounded ${
                metrics.tailRatio < 12
                  ? 'bg-green-500/20 text-green-400'
                  : 'bg-red-500/20 text-red-400'
              }`}>
                {metrics.tailRatio < 12 ? 'PASS' : 'FAIL'}
              </span>
            </div>
            <div className="flex justify-between items-center pt-2 border-t border-gray-700">
              <span className="text-gray-400 text-xs">Tail Ratio</span>
              <span className="text-indigo-400 font-bold">{metrics.tailRatio.toFixed(2)}</span>
            </div>
            <div className="flex justify-between items-center">
              <span className="text-gray-400 text-xs">Drops</span>
              <span className={`font-bold ${
                metrics.errors.dropCount === 0 ? 'text-green-400' : 'text-red-400'
              }`}>
                {metrics.errors.dropCount}
              </span>
            </div>
          </div>
        </div>
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
        <LatencyChart data={timeSeriesData} />
        <ThroughputChart data={timeSeriesData} />
      </div>
    </div>
  );
}
