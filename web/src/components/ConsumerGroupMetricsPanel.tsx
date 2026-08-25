import { ReactNode, useCallback, useEffect, useMemo, useState } from "react";
import { ChevronRight, Radio, RefreshCw, UsersRound } from "lucide-react";
import { api } from "../api";
import { useI18n } from "../i18n";
import type { ConsumerGroup, ConsumerGroupMetricPoint, ConsumerGroupMetricSeries, ConsumerGroupMetricValue, StreamMetricSeries } from "../types";
import { MetricTimeSeriesChart, type MetricChartSeries } from "./MetricTimeSeriesChart";
import { ResizableGrid, type ResizableGridColumn } from "./ResizableGrid";

type ConsumerGroupMetricsPanelProps = {
  connectionId: string;
  streamKey: string;
  monitored: boolean;
  leadingControls?: ReactNode;
  groups?: ConsumerGroup[];
  selectedGroupName?: string;
  onOpenGroup?: (groupName: string) => void;
};

type GroupPerformance = {
  latest: ConsumerGroupMetricValue | null;
  lagHistory: number[];
};

const groupLineClasses = [
  "metric-line-group-0",
  "metric-line-group-1",
  "metric-line-group-2",
  "metric-line-group-3",
  "metric-line-group-4",
  "metric-line-group-5",
];

export function ConsumerGroupMetricsPanel({
  connectionId,
  streamKey,
  monitored,
  leadingControls,
  groups,
  selectedGroupName = "",
  onOpenGroup,
}: ConsumerGroupMetricsPanelProps) {
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

  useEffect(() => {
    if (selectedGroupName && metrics?.groups.includes(selectedGroupName)) setSelectedGroup(selectedGroupName);
  }, [metrics?.groups, selectedGroupName]);

  const visibleGroups = useMemo(
    () => selectedGroup ? [selectedGroup] : metrics?.groups ?? [],
    [metrics?.groups, selectedGroup],
  );
  const consumeSeries = useGroupSeries(visibleGroups, t("Messages consumed by this group per second."), (point, group) => point.values[group]?.consumeRate ?? null, formatRate);
  const lagSeries = useGroupSeries(visibleGroups, t("Messages waiting for this group to consume."), (point, group) => point.values[group]?.lag ?? null, (value) => Math.round(value).toLocaleString(locale));
  const delaySeries = useGroupSeries(visibleGroups, t("Observed delivery time minus the publish time encoded in the Stream ID."), (point, group) => point.values[group]?.observedDeliveryAgeMs ?? null, formatMilliseconds);
  const groupColumns = useMemo<ResizableGridColumn[]>(() => [
    { id: "name", label: t("Name"), defaultWidth: 200, minWidth: 150, grow: true },
    { id: "consumers", label: t("Consumers"), defaultWidth: 90, minWidth: 75 },
    { id: "pending", label: t("Pending"), defaultWidth: 85, minWidth: 70 },
    { id: "lag", label: t("Lag / change"), defaultWidth: 120, minWidth: 95 },
    { id: "rate", label: t("Rate"), defaultWidth: 100, minWidth: 82 },
    { id: "delay", label: t("Observed age"), defaultWidth: 120, minWidth: 95 },
    { id: "last-delivered", label: t("Last delivered ID"), defaultWidth: 160, minWidth: 130 },
    { id: "trend", label: t("Lag trend"), defaultWidth: 104, minWidth: 84 },
    { id: "actions", label: null, ariaLabel: t("Actions"), defaultWidth: 38, minWidth: 28 },
  ], [t]);
  const performanceByGroup = useMemo(() => buildGroupPerformance(metrics), [metrics]);
  const sortedGroups = useMemo(() => [...(groups ?? [])].sort((left, right) => {
    const leftLatest = performanceByGroup.get(left.name)?.latest;
    const rightLatest = performanceByGroup.get(right.name)?.latest;
    return right.lag - left.lag
      || right.pending - left.pending
      || (rightLatest?.observedDeliveryAgeMs ?? -1) - (leftLatest?.observedDeliveryAgeMs ?? -1)
      || left.name.localeCompare(right.name);
  }), [groups, performanceByGroup]);

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
    {groups ? <ResizableGrid className="simple-table" storageKey="stream-consumer-group-performance" columns={groupColumns} headerClassName="simple-head">
      {sortedGroups.map((group) => {
        const performance = performanceByGroup.get(group.name);
        const latest = performance?.latest;
        const openGroup = () => {
          setSelectedGroup(group.name);
          onOpenGroup?.(group.name);
        };
        return <button
          type="button"
          className={`simple-row group-row ${selectedGroupName === group.name ? "selected" : ""}`}
          key={group.name}
          onClick={openGroup}
        >
          <span><UsersRound size={15} />{group.name}</span>
          <span>{group.consumers.toLocaleString(locale)}</span>
          <span>{group.pending.toLocaleString(locale)}</span>
          <span title={t("Current lag and change per second")}>
            {group.lag.toLocaleString(locale)} <small>{formatSignedRate(latest?.lagDelta ?? null)}</small>
          </span>
          <span>{latest?.consumeRate == null ? "—" : formatRate(latest.consumeRate)}</span>
          <span>{latest?.observedDeliveryAgeMs == null ? "—" : formatMilliseconds(latest.observedDeliveryAgeMs)}</span>
          <span className="mono stream-id-cell" title={group.lastDeliveredId}>{group.lastDeliveredId}</span>
          <LagSparkline groupName={group.name} values={performance?.lagHistory ?? []} />
          <ChevronRight size={16} />
        </button>;
      })}
      {!groups.length ? <div className="panel-empty">{t("No consumer groups.")}</div> : null}
    </ResizableGrid> : null}
    {emptyMessage ? <div className="consumer-group-metrics-empty">{emptyMessage}</div> : <div className="metric-chart-grid">
      <MetricTimeSeriesChart title={t("Consumption rate")} points={metrics?.items ?? []} series={consumeSeries} />
      <MetricTimeSeriesChart title={t("Lag")} points={metrics?.items ?? []} series={lagSeries} />
      <MetricTimeSeriesChart title={t("Observed delivery age")} points={metrics?.items ?? []} series={delaySeries} />
    </div>}
  </section>;
}

function buildGroupPerformance(metrics: ConsumerGroupMetricSeries | null) {
  const result = new Map<string, GroupPerformance>();
  for (const point of metrics?.items ?? []) {
    for (const [groupName, value] of Object.entries(point.values)) {
      const current = result.get(groupName) ?? { latest: null, lagHistory: [] };
      current.latest = value;
      if (value.lag !== null) current.lagHistory.push(value.lag);
      result.set(groupName, current);
    }
  }
  return result;
}

function LagSparkline({ groupName, values }: { groupName: string; values: number[] }) {
  const { t } = useI18n();
  const visible = values.slice(-32);
  if (visible.length < 2) return <span aria-label={t("No lag trend available")}>—</span>;

  const width = 96;
  const height = 24;
  const minimum = Math.min(...visible);
  const maximum = Math.max(...visible);
  const range = Math.max(1, maximum - minimum);
  const points = visible.map((value, index) => {
    const x = (index / (visible.length - 1)) * width;
    const y = height - 3 - ((value - minimum) / range) * (height - 6);
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" ");

  return <svg
    viewBox={`0 0 ${width} ${height}`}
    role="img"
    aria-label={t("Lag trend for {group}", { group: groupName })}
    preserveAspectRatio="none"
    style={{ width: "84px", height: "24px", color: "#555960" }}
  >
    <polyline points={points} fill="none" stroke="currentColor" strokeWidth="1.6" vectorEffect="non-scaling-stroke" />
  </svg>;
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

function formatSignedRate(value: number | null) {
  if (value === null) return "—";
  const formatted = value.toFixed(Math.abs(value) < 10 ? 2 : 1);
  return `${value > 0 ? "+" : ""}${formatted}/s`;
}
