import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import type { TimeSeriesPoint } from '../hooks/useTimeSeriesData';

interface LatencyChartProps {
  data: TimeSeriesPoint[];
}

export function LatencyChart({ data }: LatencyChartProps) {
  // Format timestamp for X-axis
  const formattedData = data.map((point) => ({
    ...point,
    time: new Date(point.timestamp).toLocaleTimeString(),
  }));

  return (
    <div className="bg-gray-800 rounded-lg p-6 shadow-lg">
      <h2 className="text-xl font-semibold mb-4 text-blue-400">
        Latency Trends (μs)
      </h2>
      <ResponsiveContainer width="100%" height={300}>
        <LineChart data={formattedData}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis
            dataKey="time"
            stroke="#9CA3AF"
            tick={{ fill: '#9CA3AF' }}
          />
          <YAxis
            stroke="#9CA3AF"
            tick={{ fill: '#9CA3AF' }}
            label={{ value: 'μs', angle: -90, position: 'insideLeft', fill: '#9CA3AF' }}
          />
          <Tooltip
            contentStyle={{
              backgroundColor: '#1F2937',
              border: '1px solid #374151',
              borderRadius: '0.5rem',
              color: '#F3F4F6',
            }}
          />
          <Legend wrapperStyle={{ color: '#9CA3AF' }} />
          <Line
            type="monotone"
            dataKey="p50"
            stroke="#3B82F6"
            strokeWidth={2}
            dot={false}
            name="p50"
          />
          <Line
            type="monotone"
            dataKey="p99"
            stroke="#F59E0B"
            strokeWidth={2}
            dot={false}
            name="p99"
          />
          <Line
            type="monotone"
            dataKey="p999"
            stroke="#EF4444"
            strokeWidth={2}
            dot={false}
            name="p999"
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
