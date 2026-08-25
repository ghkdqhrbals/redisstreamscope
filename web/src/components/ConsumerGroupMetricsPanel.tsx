import { ReactNode, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ChevronRight, Radio, RefreshCw, UsersRound } from "lucide-react";
import { api } from "../api";
import { useI18n } from "../i18n";
import type { ConsumerGroup, ConsumerGroupMetricPoint, ConsumerGroupMetricSeries, ConsumerGroupMetricValue, OperationalSnapshot, StreamMetricSeries, StreamOperationalHealth } from "../types";
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

type OperationalSummary = {
  backlog: number | null;
  consumeRate: number | null;
  oldestPendingIdAgeMs: number | null;
  oldestPendingIdleMs: number | null;
  maxDeliveryCount: number | null;
  poisonMessagesSampled: number | null;
  netDrainRate: number | null;
  drainEtaSeconds: number | null;
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
  const [operationalSnapshot, setOperationalSnapshot] = useState<OperationalSnapshot | null>(null);
  const [selectedGroup, setSelectedGroup] = useState("");
  const [live, setLive] = useState(true);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const requestSequence = useRef(0);
  const inFlightRequest = useRef<{ sequence: number; signature: string } | null>(null);
  const operationalSnapshotRef = useRef<OperationalSnapshot | null>(null);
  const operationalRequestedAt = useRef(0);
  const operationalRequest = useRef<{ connectionId: string; promise: Promise<OperationalSnapshot> } | null>(null);
  const operationalError = useRef("");

  const load = useCallback(async (forceOperational = false) => {
    if (!connectionId || !streamKey || !monitored) {
      requestSequence.current += 1;
      inFlightRequest.current = null;
      setMetrics(null);
      setOperationalSnapshot(null);
      setLoading(false);
      return;
    }
    const signature = `${connectionId}\u0000${streamKey}\u0000${range}`;
    if (inFlightRequest.current?.signature === signature) return;
    const sequence = ++requestSequence.current;
    inFlightRequest.current = { sequence, signature };
    setLoading(true);
    try {
      const pendingOperational = operationalRequest.current?.connectionId === connectionId
        ? operationalRequest.current
        : null;
      const refreshOperational = Boolean(pendingOperational)
        || forceOperational
        || Date.now() - operationalRequestedAt.current >= 5_000;
      let snapshotPromise: Promise<OperationalSnapshot | null>;
      if (pendingOperational) {
        snapshotPromise = pendingOperational.promise;
      } else if (refreshOperational) {
        operationalRequestedAt.current = Date.now();
        const request = { connectionId, promise: api.operationalSnapshot(connectionId) };
        operationalRequest.current = request;
        request.promise.then(
          () => { if (operationalRequest.current === request) operationalRequest.current = null; },
          () => { if (operationalRequest.current === request) operationalRequest.current = null; },
        );
        snapshotPromise = request.promise;
      } else {
        snapshotPromise = Promise.resolve(operationalSnapshotRef.current);
      }
      const [metricResult, snapshotResult] = await Promise.allSettled([
        api.consumerGroupMetrics(connectionId, streamKey, range),
        snapshotPromise,
      ]);
      if (sequence !== requestSequence.current) return;
      if (snapshotResult.status === "fulfilled" && snapshotResult.value) {
        const normalizedSnapshot = refreshOperational
          ? normalizeOperationalSnapshot(snapshotResult.value)
          : snapshotResult.value;
        operationalSnapshotRef.current = normalizedSnapshot;
        setOperationalSnapshot((current) => current === normalizedSnapshot ? current : normalizedSnapshot);
        if (refreshOperational) operationalError.current = "";
      } else if (snapshotResult.status === "rejected") {
        operationalError.current = t("Operational consumer health is temporarily unavailable.");
      }
      if (metricResult.status === "rejected") throw metricResult.reason;
      setMetrics(metricResult.value);
      setError(operationalError.current);
    } catch (cause) {
      if (sequence !== requestSequence.current) return;
      setError(cause instanceof Error ? t(cause.message) : t("Unable to load consumer group metrics."));
    } finally {
      if (inFlightRequest.current?.sequence === sequence) inFlightRequest.current = null;
      if (sequence === requestSequence.current) setLoading(false);
    }
  }, [connectionId, monitored, range, streamKey, t]);

  useEffect(() => {
    requestSequence.current += 1;
    inFlightRequest.current = null;
    setSelectedGroup("");
    setMetrics(null);
    setOperationalSnapshot(null);
    operationalSnapshotRef.current = null;
    operationalRequestedAt.current = 0;
    operationalRequest.current = null;
    operationalError.current = "";
    setError("");
  }, [connectionId, streamKey]);

  useEffect(() => () => {
    requestSequence.current += 1;
    inFlightRequest.current = null;
    operationalRequest.current = null;
  }, []);

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
  const formatLag = useCallback((value: number) => Math.round(value).toLocaleString(locale), [locale]);
  const consumeSeries = useGroupSeries(visibleGroups, t("Consumer-group entries-read progress per second. This measures delivery progress, not ACK completion."), groupConsumeRate, formatRate);
  const lagSeries = useGroupSeries(visibleGroups, t("Entries that have not yet been delivered to this consumer group."), groupLag, formatLag);
  const delaySeries = useGroupSeries(visibleGroups, t("Age of the group's most recently delivered Stream ID at collection time. This is not processing latency."), groupObservedDeliveryAge, formatMilliseconds);
  const groupColumns = useMemo<ResizableGridColumn[]>(() => [
    { id: "name", label: t("Name"), defaultWidth: 200, minWidth: 150, grow: true },
    { id: "consumers", label: t("Consumers"), defaultWidth: 90, minWidth: 75 },
    { id: "backlog", label: t("Backlog"), defaultWidth: 128, minWidth: 104 },
    { id: "rate", label: t("Consume rate"), defaultWidth: 108, minWidth: 90 },
    { id: "oldest", label: t("Oldest pending"), defaultWidth: 154, minWidth: 128 },
    { id: "delivery", label: t("Delivery attempts"), defaultWidth: 148, minWidth: 120 },
    { id: "drain", label: t("Net drain / s"), defaultWidth: 112, minWidth: 94 },
    { id: "eta", label: t("Drain ETA"), defaultWidth: 108, minWidth: 88 },
    { id: "trend", label: t("Lag trend"), defaultWidth: 104, minWidth: 84 },
    { id: "actions", label: null, ariaLabel: t("Actions"), defaultWidth: 38, minWidth: 28 },
  ], [t]);
  const performanceByGroup = useMemo(() => buildGroupPerformance(metrics), [metrics]);
  const operationalStream = useMemo(
    () => operationalSnapshot?.streams.find((stream) => stream.key === streamKey) ?? null,
    [operationalSnapshot?.streams, streamKey],
  );
  const operationalByGroup = useMemo(
    () => new Map((operationalStream?.groups ?? []).map((group) => [group.name, group])),
    [operationalStream?.groups],
  );
  const selectedScopeGroup = selectedGroup;
  const operationalSummary = useMemo(
    () => buildOperationalSummary(operationalStream, selectedScopeGroup, performanceByGroup),
    [operationalStream, performanceByGroup, selectedScopeGroup],
  );
  const sortedGroups = useMemo(() => [...(groups ?? [])].sort((left, right) => {
    const leftOperational = operationalByGroup.get(left.name);
    const rightOperational = operationalByGroup.get(right.name);
    const leftBacklog = currentBacklog(left, leftOperational);
    const rightBacklog = currentBacklog(right, rightOperational);
    return (rightBacklog ?? -1) - (leftBacklog ?? -1)
      || right.pending - left.pending
      || (rightOperational?.pendingSample.oldestPendingIdAgeMs ?? -1) - (leftOperational?.pendingSample.oldestPendingIdAgeMs ?? -1)
      || left.name.localeCompare(right.name);
  }), [groups, operationalByGroup]);

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
        <button type="button" aria-label={t("Refresh metrics")} title={t("Refresh metrics")} disabled={loading || !connectionId || !streamKey || !monitored} onClick={() => void load(true)}><RefreshCw className={loading ? "spin" : ""} size={14} /></button>
      </div>
    </header>
    {error ? <div className="metric-history-error">{error}</div> : null}
    {monitored && streamKey ? <OperationalKpis summary={operationalSummary} /> : null}
    {groups ? <ResizableGrid className="simple-table" storageKey="stream-consumer-group-performance" columns={groupColumns} headerClassName="simple-head">
      {sortedGroups.map((group) => {
        const performance = performanceByGroup.get(group.name);
        const latest = performance?.latest;
        const operational = operationalByGroup.get(group.name);
        const backlog = currentBacklog(group, operational);
        const openGroup = () => {
          setSelectedGroup(group.name);
          onOpenGroup?.(group.name);
        };
        return <button
          type="button"
          className={`simple-row group-row ${selectedGroupName === group.name ? "selected" : ""}`}
          key={group.name}
          aria-label={`${group.name}; ${t("Consumers")}: ${group.consumers.toLocaleString(locale)}; ${t("Backlog")}: ${formatCount(backlog ?? null, locale)}; ${t("Pending")}: ${group.pending.toLocaleString(locale)}; ${t("Drain ETA")}: ${formatDuration(operational?.drainEtaSeconds == null ? undefined : operational.drainEtaSeconds * 1000)}`}
          onClick={openGroup}
        >
          <span className="group-metric-name"><UsersRound size={15} /><strong title={group.name}>{group.name}</strong></span>
          <span className="group-metric-value" data-label={t("Consumers")}>{group.consumers.toLocaleString(locale)}</span>
          <span className="group-metric-value" data-label={t("Backlog")} title={t("Backlog is undelivered lag plus delivered entries still awaiting ACK.")}>
            {formatCount(backlog ?? null, locale)} <small>{t("{lag} lag · {pending} pending", { lag: group.lag < 0 ? "—" : group.lag.toLocaleString(locale), pending: group.pending.toLocaleString(locale) })}</small>
          </span>
          <span className="group-metric-value" data-label={t("Consume rate")} title={t("Consumer-group entries-read progress per second. This measures delivery progress, not ACK completion.")}>{latest?.consumeRate == null ? "—" : formatRate(latest.consumeRate)}</span>
          <span className="group-metric-value" data-label={t("Oldest pending")} title={t("Oldest pending entry age and time since its last delivery.")}>
            {formatDuration(operational?.pendingSample.oldestPendingIdAgeMs)} <small>{t("{idle} idle", { idle: formatDuration(operational?.pendingSample.oldestPendingIdleMs) })}</small>
          </span>
          <span className="group-metric-value" data-label={t("Delivery attempts")} title={t("Maximum sampled delivery count and sampled entries over the poison threshold.")}>
            {operational ? `${operational.pendingSample.maxDeliveryCount.toLocaleString(locale)}×` : "—"} <small>{t("{count} poison sampled", { count: operational?.pendingSample.poisonMessagesSampled.toLocaleString(locale) ?? "—" })}</small>
          </span>
          <span className="group-metric-value" data-label={t("Net drain / s")} title={t("Positive means backlog is draining; negative means it is growing.")}>{formatNetDrain(operational?.netDrainRate)}</span>
          <span className="group-metric-value" data-label={t("Drain ETA")} title={t("Estimated time to clear the current backlog at the observed net drain rate.")}>{formatDuration(operational?.drainEtaSeconds == null ? undefined : operational.drainEtaSeconds * 1000)}</span>
          <span className="group-metric-trend" data-label={t("Lag trend")}><LagSparkline groupName={group.name} values={performance?.lagHistory ?? []} /></span>
          <ChevronRight className="group-metric-open" size={16} />
        </button>;
      })}
      {!groups.length ? <div className="panel-empty">{t("No consumer groups.")}</div> : null}
    </ResizableGrid> : null}
    {emptyMessage ? <div className="consumer-group-metrics-empty">{emptyMessage}</div> : <div className="metric-chart-grid">
      <MetricTimeSeriesChart title={t("Consumer delivery rate")} points={metrics?.items ?? []} series={consumeSeries} />
      <MetricTimeSeriesChart title={t("Undelivered lag")} points={metrics?.items ?? []} series={lagSeries} />
      <MetricTimeSeriesChart title={t("Last-delivered entry age")} points={metrics?.items ?? []} series={delaySeries} />
    </div>}
  </section>;
}

function OperationalKpis({ summary }: { summary: OperationalSummary }) {
  const { locale, t } = useI18n();
  return <div className="consumer-operational-kpis" role="group" aria-label={t("Consumer group operational summary")}>
    <div title={t("Undelivered lag plus delivered entries still awaiting ACK.")}><span>{t("Backlog")}</span><strong>{formatCount(summary.backlog, locale)}</strong></div>
    <div title={t("Consumer-group entries-read progress per second. This measures delivery progress, not ACK completion.")}><span>{t("Consume rate")}</span><strong>{summary.consumeRate == null ? "—" : formatRate(summary.consumeRate)}</strong></div>
    <div title={t("Age of the oldest sampled pending entry and time since its last delivery.")}><span>{t("Oldest pending")}</span><strong>{formatDuration(summary.oldestPendingIdAgeMs ?? undefined)}</strong><small>{t("{idle} idle", { idle: formatDuration(summary.oldestPendingIdleMs ?? undefined) })}</small></div>
    <div title={t("Maximum sampled delivery count and sampled entries over the poison threshold.")}><span>{t("Delivery attempts")}</span><strong>{summary.maxDeliveryCount == null ? "—" : `${summary.maxDeliveryCount.toLocaleString(locale)}×`}</strong><small>{t("{count} poison sampled", { count: formatCount(summary.poisonMessagesSampled, locale) })}</small></div>
    <div title={t("Positive means backlog is draining; negative means it is growing.")}><span>{t("Net drain / s")}</span><strong>{formatNetDrain(summary.netDrainRate ?? undefined)}</strong></div>
    <div title={t("Estimated time to clear the current backlog at the observed net drain rate.")}><span>{t("Drain ETA")}</span><strong>{formatDuration(summary.drainEtaSeconds == null ? undefined : summary.drainEtaSeconds * 1000)}</strong></div>
  </div>;
}

function normalizeOperationalSnapshot(snapshot: OperationalSnapshot): OperationalSnapshot {
  return {
    ...snapshot,
    streams: (snapshot.streams ?? []).map((stream) => ({
      ...stream,
      risks: stream.risks ?? [],
      groups: stream.groups ?? [],
    })),
  };
}

function buildOperationalSummary(
  stream: StreamOperationalHealth | null,
  selectedGroup: string,
  performanceByGroup: Map<string, GroupPerformance>,
): OperationalSummary {
  const groups = selectedGroup
    ? stream?.groups.filter((group) => group.name === selectedGroup) ?? []
    : stream?.groups ?? [];
  const metricGroups = selectedGroup
    ? [selectedGroup]
    : Array.from(new Set([...(stream?.groups.map((group) => group.name) ?? []), ...performanceByGroup.keys()]));
  return {
    backlog: sumKnown(groups.map((group) => group.backlog)),
    consumeRate: sumKnown(metricGroups.map((group) => performanceByGroup.get(group)?.latest?.consumeRate)),
    oldestPendingIdAgeMs: maximumKnown(groups.map((group) => group.pendingSample.oldestPendingIdAgeMs)),
    oldestPendingIdleMs: maximumKnown(groups.map((group) => group.pendingSample.oldestPendingIdleMs)),
    maxDeliveryCount: maximumKnown(groups.map((group) => group.pendingSample.maxDeliveryCount)),
    poisonMessagesSampled: sumKnown(groups.map((group) => group.pendingSample.poisonMessagesSampled)),
    netDrainRate: sumKnown(groups.map((group) => group.netDrainRate)),
    drainEtaSeconds: selectedGroup
      ? groups[0]?.drainEtaSeconds ?? null
      : stream?.drainEtaSeconds ?? maximumKnown(groups.map((group) => group.drainEtaSeconds)),
  };
}

function sumKnown(values: Array<number | null | undefined>) {
  const known = values.filter((value): value is number => value != null && Number.isFinite(value));
  return known.length ? known.reduce((sum, value) => sum + value, 0) : null;
}

function maximumKnown(values: Array<number | null | undefined>) {
  const known = values.filter((value): value is number => value != null && Number.isFinite(value));
  return known.length ? Math.max(...known) : null;
}

function currentBacklog(group: ConsumerGroup, operational?: StreamOperationalHealth["groups"][number]) {
  if (operational && Number.isFinite(operational.backlog)) return operational.backlog;
  return group.lag >= 0 ? group.lag + group.pending : null;
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

function formatNetDrain(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) return "—";
  const formatted = value.toFixed(Math.abs(value) < 10 ? 2 : 1);
  return `${value > 0 ? "+" : ""}${formatted}/s`;
}

function formatDuration(milliseconds: number | null | undefined) {
  if (milliseconds == null || !Number.isFinite(milliseconds)) return "—";
  if (milliseconds < 1000) return `${Math.round(milliseconds)} ms`;
  if (milliseconds < 60_000) return `${(milliseconds / 1000).toFixed(milliseconds < 10_000 ? 1 : 0)} s`;
  if (milliseconds < 3_600_000) return `${(milliseconds / 60_000).toFixed(1)} min`;
  if (milliseconds < 86_400_000) return `${(milliseconds / 3_600_000).toFixed(1)} h`;
  return `${(milliseconds / 86_400_000).toFixed(1)} d`;
}

function formatCount(value: number | null, locale: string) {
  return value == null ? "—" : value.toLocaleString(locale);
}

function groupConsumeRate(point: ConsumerGroupMetricPoint, group: string) {
  return point.values[group]?.consumeRate ?? null;
}

function groupLag(point: ConsumerGroupMetricPoint, group: string) {
  return point.values[group]?.lag ?? null;
}

function groupObservedDeliveryAge(point: ConsumerGroupMetricPoint, group: string) {
  return point.values[group]?.observedDeliveryAgeMs ?? null;
}
