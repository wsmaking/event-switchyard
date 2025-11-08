import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import type { TimeSeriesPoint } from '../hooks/useTimeSeriesData';

interface ThroughputChartProps {
  data: TimeSeriesPoint[];
}

export function ThroughputChart({ data }: ThroughputChartProps) {
  const formattedData = data.map((point) => ({
    time: new Date(point.timestamp).toLocaleTimeString(),
    throughput: point.throughput,
    fastPathRatio: point.fastPathRatio,
  }));

  return (
    <div className="bg-gray-800 rounded-lg p-6 shadow-lg">
      <h2 className="text-xl font-semibold mb-4 text-green-400">
        Throughput & Fast Path Ratio
      </h2>
      <ResponsiveContainer width="100%" height={300}>
        <AreaChart data={formattedData}>
          <defs>
            <linearGradient id="colorThroughput" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="#10B981" stopOpacity={0.8}/>
              <stop offset="95%" stopColor="#10B981" stopOpacity={0.1}/>
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis
            dataKey="time"
            stroke="#9CA3AF"
            tick={{ fill: '#9CA3AF' }}
          />
          <YAxis
            yAxisId="left"
            stroke="#9CA3AF"
            tick={{ fill: '#9CA3AF' }}
            label={{ value: 'Events', angle: -90, position: 'insideLeft', fill: '#9CA3AF' }}
          />
          <YAxis
            yAxisId="right"
            orientation="right"
            stroke="#9CA3AF"
            tick={{ fill: '#9CA3AF' }}
            label={{ value: 'Ratio (%)', angle: 90, position: 'insideRight', fill: '#9CA3AF' }}
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
          <Area
            yAxisId="left"
            type="monotone"
            dataKey="throughput"
            stroke="#10B981"
            fillOpacity={1}
            fill="url(#colorThroughput)"
            name="Throughput"
          />
          <Area
            yAxisId="right"
            type="monotone"
            dataKey="fastPathRatio"
            stroke="#8B5CF6"
            fillOpacity={0.3}
            fill="#8B5CF6"
            name="Fast Path %"
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}
