import { ReactNode, useCallback, useEffect, useMemo, useState } from "react";
import { Radio, RefreshCw } from "lucide-react";
import { api } from "../api";
import { useI18n } from "../i18n";
import type { ConsumerGroupMetricPoint, ConsumerGroupMetricSeries, StreamMetricSeries } from "../types";
import { MetricTimeSeriesChart, type MetricChartSeries } from "./MetricTimeSeriesChart";

type ConsumerGroupMetricsPanelProps = {
  connectionId: string;
  streamKey: string;
  monitored: boolean;
  leadingControls?: ReactNode;
};

const groupLineClasses = [
  "metric-line-group-0",
  "metric-line-group-1",
  "metric-line-group-2",
  "metric-line-group-3",
  "metric-line-group-4",
  "metric-line-group-5",
];

export function ConsumerGroupMetricsPanel({ connectionId, streamKey, monitored, leadingControls }: ConsumerGroupMetricsPanelProps) {
  const { locale, t } = useI18n();
  const [range, setRange] = useState<StreamMetricSeries["range"]>("5m");
  const [metrics, setMetrics] = useState<ConsumerGroupMetricSeries | null>(null);
  const [selectedGroup, setSelectedGroup] = useState("");
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
      const response = await api.consumerGroupMetrics(connectionId, streamKey, range);
      setMetrics(response);
      setError("");
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Unable to load consumer group metrics."));
    } finally {
      setLoading(false);
    }
  }, [connectionId, monitored, range, streamKey, t]);

  useEffect(() => {
    setSelectedGroup("");
    setMetrics(null);
  }, [connectionId, streamKey]);

  useEffect(() => {
    void load();
    if (!live || !connectionId || !streamKey || !monitored) return;
    const timer = window.setInterval(() => void load(), 1000);
    return () => window.clearInterval(timer);
  }, [connectionId, live, load, monitored, streamKey]);

  useEffect(() => {
    if (selectedGroup && !metrics?.groups.includes(selectedGroup)) setSelectedGroup("");
  }, [metrics?.groups, selectedGroup]);

  const visibleGroups = useMemo(
    () => selectedGroup ? [selectedGroup] : metrics?.groups ?? [],
    [metrics?.groups, selectedGroup],
  );
  const consumeSeries = useGroupSeries(visibleGroups, t("Messages consumed by this group per second."), (point, group) => point.values[group]?.consumeRate ?? null, formatRate);
  const lagSeries = useGroupSeries(visibleGroups, t("Messages waiting for this group to consume."), (point, group) => point.values[group]?.lag ?? null, (value) => Math.round(value).toLocaleString(locale));
  const delaySeries = useGroupSeries(visibleGroups, t("Observed delivery time minus the publish time encoded in the Stream ID."), (point, group) => point.values[group]?.consumeDelayMs ?? null, formatMilliseconds);

  const emptyMessage = !streamKey
    ? t("Select a stream to view consumer group history.")
    : !monitored
      ? t("Add this stream to monitoring to collect consumer group history.")
      : !metrics?.groups.length
        ? t("Waiting for consumer group samples…")
        : "";

  return <section className="metric-history-panel consumer-group-metrics-panel">
    <header className="metric-history-header">
      <div><h2>{t("Consumer group performance")}</h2><span>{metrics ? t("{seconds}s samples", { seconds: metrics.intervalSeconds }) : t("Time series")}</span></div>
      <div className="metric-history-controls">
        {leadingControls}
        {metrics && metrics.groups.length > 1 ? <select value={selectedGroup} onChange={(event) => setSelectedGroup(event.target.value)} aria-label={t("Metric consumer group")}>
          <option value="">{t("All consumer groups")}</option>
          {metrics.groups.map((group) => <option key={group} value={group}>{group}</option>)}
        </select> : null}
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
    {emptyMessage ? <div className="consumer-group-metrics-empty">{emptyMessage}</div> : <div className="metric-chart-grid">
      <MetricTimeSeriesChart title={t("Consumption rate")} points={metrics?.items ?? []} series={consumeSeries} />
      <MetricTimeSeriesChart title={t("Lag")} points={metrics?.items ?? []} series={lagSeries} />
      <MetricTimeSeriesChart title={t("Consume delay")} points={metrics?.items ?? []} series={delaySeries} />
    </div>}
  </section>;
}

function useGroupSeries(
  groups: string[],
  description: string,
  value: (point: ConsumerGroupMetricPoint, group: string) => number | null,
  format: (value: number) => string,
) {
  return useMemo<MetricChartSeries<ConsumerGroupMetricPoint>[]>(() => groups.map((group, index) => ({
    id: group,
    label: group,
    description,
    className: groupLineClasses[index % groupLineClasses.length],
    value: (point) => value(point, group),
    format,
  })), [description, format, groups, value]);
}

function formatMilliseconds(value: number) {
  if (value < 1) return `${value.toFixed(2)} ms`;
  if (value < 1000) return `${value.toFixed(value < 10 ? 1 : 0)} ms`;
  if (value < 60000) return `${(value / 1000).toFixed(1)} s`;
  return `${(value / 60000).toFixed(1)} min`;
}

function formatRate(value: number) {
  return `${value.toFixed(value < 10 ? 2 : 1)} /s`;
}
