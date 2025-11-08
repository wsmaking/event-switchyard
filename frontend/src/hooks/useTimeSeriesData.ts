import { useState, useEffect, useRef } from 'react';
import type { DashboardMetrics } from '../types/stats';

export interface TimeSeriesPoint {
  timestamp: number;
  p50: number;
  p99: number;
  p999: number;
  throughput: number;
  fastPathRatio: number;
}

const MAX_POINTS = 60; // Keep last 60 data points (1 minute at 1Hz)
const SAMPLE_INTERVAL_MS = 1000; // Sample every 1 second

export function useTimeSeriesData(metrics: DashboardMetrics | undefined) {
  const [data, setData] = useState<TimeSeriesPoint[]>([]);
  const metricsRef = useRef(metrics);

  // Keep ref updated
  useEffect(() => {
    metricsRef.current = metrics;
  }, [metrics]);

  // Sample data on interval
  useEffect(() => {
    const interval = setInterval(() => {
      if (!metricsRef.current) return;

      const newPoint: TimeSeriesPoint = {
        timestamp: Date.now(),
        p50: metricsRef.current.latency.p50,
        p99: metricsRef.current.latency.p99,
        p999: metricsRef.current.latency.p999,
        throughput: metricsRef.current.throughput.total,
        fastPathRatio: metricsRef.current.throughput.ratio,
      };

      setData((prev) => {
        const updated = [...prev, newPoint];
        return updated.slice(-MAX_POINTS);
      });
    }, SAMPLE_INTERVAL_MS);

    return () => clearInterval(interval);
  }, []);

  return data;
}
