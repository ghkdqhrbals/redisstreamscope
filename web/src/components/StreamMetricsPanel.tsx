import { ReactNode, useCallback, useEffect, useMemo, useState } from "react";
import { Radio, RefreshCw } from "lucide-react";
import { api } from "../api";
import { useI18n } from "../i18n";
import type { StreamMetricPoint, StreamMetricSeries } from "../types";
import { MetricTimeSeriesChart, type MetricChartSeries } from "./MetricTimeSeriesChart";

type StreamMetricsPanelProps = {
  connectionId: string;
  streamKey: string;
  monitored: boolean;
  leadingControls?: ReactNode;
};

export function StreamMetricsPanel({ connectionId, streamKey, monitored, leadingControls }: StreamMetricsPanelProps) {
  const { locale, t } = useI18n();
  const [range, setRange] = useState<StreamMetricSeries["range"]>("5m");
  const [metrics, setMetrics] = useState<StreamMetricSeries | null>(null);
  const [live, setLive] = useState(true);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const load = useCallback(async () => {
    if (!connectionId || !streamKey || !monitored) {
      setMetrics(null);
      return;
    }
    setLoading(true);
    try {
      setMetrics(await api.metrics(connectionId, range, streamKey));
      setError("");
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Unable to load stream metrics."));
    } finally {
      setLoading(false);
    }
  }, [connectionId, monitored, range, streamKey, t]);

  useEffect(() => {
    setMetrics(null);
    setError("");
  }, [connectionId, streamKey]);

  useEffect(() => {
    void load();
    if (!live || !connectionId || !streamKey || !monitored) return;
    const timer = window.setInterval(() => void load(), 1000);
    return () => window.clearInterval(timer);
  }, [connectionId, live, load, monitored, streamKey]);

  const pressureSeries = useMemo<MetricChartSeries<StreamMetricPoint>[]>(() => [
    {
      id: "lag",
      label: t("Total lag"),
      description: t("Messages not yet delivered across the stream's consumer groups."),
      className: "metric-line-primary",
      value: (point) => point.totalLag,
      format: (value) => Math.round(value).toLocaleString(locale),
    },
    {
      id: "pending",
      label: t("Pending"),
      description: t("Delivered messages still waiting to be acknowledged."),
      className: "metric-line-secondary",
      value: (point) => point.pending,
      format: (value) => Math.round(value).toLocaleString(locale),
    },
    {
      id: "consumers",
      label: t("Consumers"),
      description: t("Consumers reported across all consumer groups."),
      className: "metric-line-tertiary",
      value: (point) => point.consumerCount,
      format: (value) => Math.round(value).toLocaleString(locale),
    },
  ], [locale, t]);

  const rateSeries = useMemo<MetricChartSeries<StreamMetricPoint>[]>(() => [
    {
      id: "published",
      label: t("Published / s"),
      description: t("Change in stream entries per second. Trimming can make this lower than producer throughput."),
      className: "metric-line-primary",
      value: (point) => point.publishRate,
      format: formatRate,
    },
    {
      id: "delivered",
      label: t("Group deliveries / s"),
      description: t("Sum of delivery progress across consumer groups per second; one entry may be counted once per group."),
      className: "metric-line-secondary",
      value: (point) => point.consumeRate,
      format: formatRate,
    },
    {
      id: "lag-change",
      label: t("Lag change / s"),
      description: t("Change in total consumer-group lag per second."),
      className: "metric-line-tertiary",
      value: (point) => point.lagDelta,
      format: formatSignedRate,
    },
  ], [t]);

  const collectorSeries = useMemo<MetricChartSeries<StreamMetricPoint>[]>(() => [
    {
      id: "ping",
      label: t("Redis PING"),
      description: t("Round-trip time for the collector's Redis PING."),
      className: "metric-line-primary",
      value: (point) => point.redisLatencyMs,
      format: formatMilliseconds,
    },
  ], [t]);

  const emptyMessage = !streamKey
    ? t("Select a monitored stream to view history.")
    : !monitored
      ? t("Add this stream to monitoring to collect history.")
      : !metrics?.items.length
        ? t("Waiting for time-series samples…")
        : "";

  return <section className="metric-history-panel stream-metrics-panel">
    <header className="metric-history-header">
      <div><h2>{t("Stream performance")}</h2><span>{metrics ? t("{seconds}s samples", { seconds: metrics.intervalSeconds }) : t("Time series")}</span></div>
      <div className="metric-history-controls">
        {leadingControls}
        <select value={range} onChange={(event) => setRange(event.target.value as StreamMetricSeries["range"])} aria-label={t("Time range")}>
          <option value="1m">{t("Last minute")}</option>
          <option value="5m">{t("Last 5 minutes")}</option>
          <option value="15m">{t("Last 15 minutes")}</option>
          <option value="1h">{t("Last hour")}</option>
          <option value="6h">{t("Last 6 hours")}</option>
          <option value="24h">{t("Last 24 hours")}</option>
          <option value="7d">{t("Last 7 days")}</option>
        </select>
        <button type="button" className={live ? "metric-live active" : "metric-live"} aria-pressed={live} onClick={() => setLive((current) => !current)}><Radio size={14} />{t("Live")}</button>
        <button type="button" aria-label={t("Refresh metrics")} title={t("Refresh metrics")} disabled={loading || !connectionId || !streamKey || !monitored} onClick={() => void load()}><RefreshCw className={loading ? "spin" : ""} size={14} /></button>
      </div>
    </header>
    {error ? <div className="metric-history-error">{error}</div> : null}
    {emptyMessage ? <div className="consumer-group-metrics-empty">{emptyMessage}</div> : <div className="metric-chart-grid stream-metric-chart-grid">
      <MetricTimeSeriesChart title={t("Consumer pressure")} points={metrics?.items ?? []} series={pressureSeries} />
      <MetricTimeSeriesChart title={t("Message rates")} points={metrics?.items ?? []} series={rateSeries} />
      <MetricTimeSeriesChart title={t("Collector Redis latency")} points={metrics?.items ?? []} series={collectorSeries} />
    </div>}
  </section>;
}

function formatMilliseconds(value: number) {
  if (value < 1) return `${value.toFixed(2)} ms`;
  if (value < 1000) return `${value.toFixed(value < 10 ? 1 : 0)} ms`;
  return `${(value / 1000).toFixed(1)} s`;
}

function formatRate(value: number) {
  return `${value.toFixed(Math.abs(value) < 10 ? 2 : 1)} /s`;
}

function formatSignedRate(value: number) {
  const formatted = value.toFixed(Math.abs(value) < 10 ? 2 : 1);
  return `${value > 0 ? "+" : ""}${formatted}/s`;
}
