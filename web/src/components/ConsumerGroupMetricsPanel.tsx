import { ReactNode, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ChevronRight, Radio, RefreshCw, UsersRound } from "lucide-react";
import { api } from "../api";
import { useI18n } from "../i18n";
import type { ConsumerGroup, ConsumerGroupMetricPoint, ConsumerGroupMetricSeries, ConsumerGroupMetricValue, OperationalSnapshot, StreamMetricSeries, StreamOperationalHealth } from "../types";
import { MetricTimeSeriesChart, type MetricChartSeries } from "./MetricTimeSeriesChart";
import { ResizableGrid, type ResizableGridColumn } from "./ResizableGrid";
import { Select } from "./Select";

type ConsumerGroupMetricsPanelProps = {
  connectionId: string;
  streamKey: string;
  monitored: boolean;
  mode?: "stream" | "group";
  leadingControls?: ReactNode;
  groups?: ConsumerGroup[];
  selectedGroupName?: string;
  onOpenGroup?: (groupName: string) => void;
};

type GroupPerformance = {
  latest: ConsumerGroupMetricValue | null;
};

type OperationalSummary = {
  backlog: number | null;
  pending: number | null;
  deliveryRate: number | null;
  activeConsumers: number | null;
  oldestPendingIdAgeMs: number | null;
  oldestPendingIdleMs: number | null;
};

const groupLineClasses = [
  "metric-line-group-0",
  "metric-line-group-1",
  "metric-line-group-2",
  "metric-line-group-3",
  "metric-line-group-4",
  "metric-line-group-5",
  "metric-line-group-6",
  "metric-line-group-7",
];
const maximumVisibleGroups = 8;
const initialVisibleGroups = 6;

export function ConsumerGroupMetricsPanel({
  connectionId,
  streamKey,
  monitored,
  mode = "stream",
  leadingControls,
  groups,
  selectedGroupName = "",
  onOpenGroup,
}: ConsumerGroupMetricsPanelProps) {
  const { locale, t } = useI18n();
  const [range, setRange] = useState<StreamMetricSeries["range"]>("5m");
  const [metrics, setMetrics] = useState<ConsumerGroupMetricSeries | null>(null);
  const [operationalSnapshot, setOperationalSnapshot] = useState<OperationalSnapshot | null>(null);
  const [live, setLive] = useState(true);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [visibleGroupNames, setVisibleGroupNames] = useState<string[]>([]);
  const requestSequence = useRef(0);
  const inFlightRequest = useRef<{ sequence: number; signature: string } | null>(null);
  const operationalSnapshotRef = useRef<OperationalSnapshot | null>(null);
  const operationalRequestedAt = useRef(0);
  const operationalRequest = useRef<{ connectionId: string; promise: Promise<OperationalSnapshot> } | null>(null);
  const operationalError = useRef("");
  const rangeOptions = useMemo(() => [
    { value: "1m", label: t("Last minute") },
    { value: "5m", label: t("Last 5 minutes") },
    { value: "15m", label: t("Last 15 minutes") },
    { value: "1h", label: t("Last hour") },
    { value: "6h", label: t("Last 6 hours") },
    { value: "24h", label: t("Last 24 hours") },
    { value: "7d", label: t("Last 7 days") },
  ], [t]);

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

  const formatLag = useCallback((value: number) => Math.round(value).toLocaleString(locale), [locale]);
  const metricGroupIdentity = [...(metrics?.groups ?? [])].sort((left, right) => left.localeCompare(right)).join("\u0000");
  const availableMetricGroups = useMemo(() => {
    const currentByName = new Map((groups ?? []).map((group) => [group.name, group]));
    return metricGroupIdentity ? metricGroupIdentity.split("\u0000").sort((left, right) => {
      const leftGroup = currentByName.get(left);
      const rightGroup = currentByName.get(right);
      const leftBacklog = leftGroup ? Math.max(0, leftGroup.lag) + leftGroup.pending : -1;
      const rightBacklog = rightGroup ? Math.max(0, rightGroup.lag) + rightGroup.pending : -1;
      return rightBacklog - leftBacklog || left.localeCompare(right);
    }) : [];
  }, [groups, metricGroupIdentity]);
  useEffect(() => {
    setVisibleGroupNames((current) => {
      const available = new Set(availableMetricGroups);
      const retained = current.filter((name) => available.has(name)).slice(0, maximumVisibleGroups);
      return retained.length ? retained : availableMetricGroups.slice(0, initialVisibleGroups);
    });
  }, [connectionId, metricGroupIdentity, streamKey]);
  const stableMetricGroupOrder = useMemo(() => metricGroupIdentity ? metricGroupIdentity.split("\u0000") : [], [metricGroupIdentity]);
  const streamLagSeries = useGroupSeries(visibleGroupNames, stableMetricGroupOrder, t("Entries that have not yet been delivered to this consumer group."), groupLag, formatLag);
  const streamPendingSeries = useGroupSeries(visibleGroupNames, stableMetricGroupOrder, t("Delivered messages that remain unacknowledged in the PEL."), groupPending, formatLag);
  const streamDeliverySeries = useGroupSeries(visibleGroupNames, stableMetricGroupOrder, t("Consumer-group entries-read progress per second. This measures delivery progress, not ACK completion."), groupConsumeRate, formatRate);
  const streamLagChangeSeries = useGroupSeries(visibleGroupNames, stableMetricGroupOrder, t("Positive values mean backlog is growing; negative values mean it is draining."), groupLagDelta, formatSignedRate);
  const streamDelaySeries = useGroupSeries(visibleGroupNames, stableMetricGroupOrder, t("Age of the group's most recently delivered Stream ID at collection time. This is not processing latency."), groupObservedDeliveryAge, formatMilliseconds);
  const groupBacklogSeries = useMemo<MetricChartSeries<ConsumerGroupMetricPoint>[]>(() => [{
    id: "group-undelivered",
    label: t("Undelivered lag"),
    description: t("Entries that have not yet been delivered to this consumer group."),
    className: "metric-line-group-0",
    value: (point) => selectedGroupName ? groupLag(point, selectedGroupName) : null,
    format: formatLag,
  }, {
    id: "group-pending",
    label: t("Pending"),
    description: t("Delivered messages that remain unacknowledged in the PEL."),
    className: "metric-line-group-1",
    value: (point) => selectedGroupName ? groupPending(point, selectedGroupName) : null,
    format: formatLag,
  }], [formatLag, selectedGroupName, t]);
  const selectedMetricGroups = useMemo(() => selectedGroupName ? [selectedGroupName] : [], [selectedGroupName]);
  const groupDeliverySeries = useMemo<MetricChartSeries<ConsumerGroupMetricPoint>[]>(() => [{
    id: `${selectedGroupName}:delivery`,
    label: t("Consumer delivery rate"),
    description: t("Consumer-group entries-read progress per second. This measures delivery progress, not ACK completion."),
    className: "metric-line-group-0",
    value: (point) => selectedGroupName ? groupConsumeRate(point, selectedGroupName) : null,
    format: formatRate,
  }, {
    id: `${selectedGroupName}:lag-change`,
    label: t("Lag change / s"),
    description: t("Positive values mean backlog is growing; negative values mean it is draining."),
    className: "metric-line-group-1",
    value: (point) => selectedGroupName ? groupLagDelta(point, selectedGroupName) : null,
    format: formatSignedRate,
  }], [selectedGroupName, t]);
  const groupDelaySeries = useGroupSeries(selectedMetricGroups, selectedMetricGroups, t("Age of the group's most recently delivered Stream ID at collection time. This is not processing latency."), groupObservedDeliveryAge, formatMilliseconds);
  const groupColumns = useMemo<ResizableGridColumn[]>(() => [
    { id: "name", label: t("Consumer groups"), defaultWidth: 240, minWidth: 170, grow: true },
    { id: "backlog", label: t("Backlog"), defaultWidth: 128, minWidth: 104 },
    { id: "pending", label: t("Pending"), defaultWidth: 108, minWidth: 88 },
    { id: "rate", label: t("Consumer delivery rate"), defaultWidth: 146, minWidth: 118 },
    { id: "oldest", label: t("Oldest pending"), defaultWidth: 154, minWidth: 128 },
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
  const selectedScopeGroup = mode === "group" ? selectedGroupName : "";
  const operationalSummary = useMemo(
    () => buildOperationalSummary(operationalStream, selectedScopeGroup, performanceByGroup, groups ?? []),
    [groups, operationalStream, performanceByGroup, selectedScopeGroup],
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
      : mode === "group" && !selectedGroupName
        ? t("Select a consumer group to view its history.")
        : !metrics?.groups.length || (mode === "group" && !metrics.groups.includes(selectedGroupName))
        ? t("Waiting for consumer group samples…")
        : "";

  return <section className={`metric-history-panel consumer-group-metrics-panel consumer-group-metrics-panel--${mode}`}>
    <header className="metric-history-header">
      <div><h2>{t("Consumer group performance")}</h2><span>{metrics ? t("{seconds}s samples", { seconds: metrics.intervalSeconds }) : t("Time series")}</span></div>
      <div className="metric-history-controls">
        {leadingControls}
        <Select value={range} options={rangeOptions} onChange={(next) => setRange(next as StreamMetricSeries["range"])} ariaLabel={t("Time range")} prefix={t("Range")} size="compact" />
        <button type="button" className={live ? "metric-live active" : "metric-live"} aria-pressed={live} onClick={() => setLive((current) => !current)}><Radio size={14} />{t("Live")}</button>
        <button type="button" aria-label={t("Refresh metrics")} title={t("Refresh metrics")} disabled={loading || !connectionId || !streamKey || !monitored} onClick={() => void load(true)}><RefreshCw className={loading ? "spin" : ""} size={14} /></button>
      </div>
    </header>
    {error ? <div className="metric-history-error">{error}</div> : null}
    {monitored && streamKey && (mode === "stream" || selectedGroupName) ? <OperationalKpis summary={operationalSummary} mode={mode} /> : null}
    {mode === "stream" && availableMetricGroups.length ? <div className="metric-series-picker metric-group-series-picker" role="group" aria-label={t("Consumer groups shown on charts")}>
      <span>{t("Compare")}</span>
      <div>{availableMetricGroups.map((groupName) => {
        const active = visibleGroupNames.includes(groupName);
        const colorIndex = Math.max(0, stableMetricGroupOrder.indexOf(groupName)) % groupLineClasses.length;
        return <button
          type="button"
          key={groupName}
          className={active ? "active" : ""}
          aria-pressed={active}
          disabled={!active && visibleGroupNames.length >= maximumVisibleGroups}
          onClick={() => setVisibleGroupNames((current) => current.includes(groupName)
            ? current.filter((name) => name !== groupName)
            : current.length < maximumVisibleGroups ? [...current, groupName] : current)}
          title={groupName}
        ><i className={groupLineClasses[colorIndex]} /><span className="mono">{groupName}</span></button>;
      })}</div>
      <em>{t("{count} of 8 consumer groups", { count: visibleGroupNames.length })}</em>
    </div> : null}
    {emptyMessage || (mode === "stream" && !visibleGroupNames.length) ? <div className="consumer-group-metrics-empty">{emptyMessage || t("Select consumer groups to compare.")}</div> : <div className={`metric-chart-grid consumer-group-chart-grid consumer-group-chart-grid--${mode}`}>
      {mode === "stream" ? <>
        <MetricTimeSeriesChart title={t("Lag by consumer group")} points={metrics?.items ?? []} series={streamLagSeries} valueKind="count" expectedIntervalSeconds={metrics?.intervalSeconds} />
        <MetricTimeSeriesChart title={t("Pending by consumer group")} points={metrics?.items ?? []} series={streamPendingSeries} valueKind="count" expectedIntervalSeconds={metrics?.intervalSeconds} />
        <MetricTimeSeriesChart title={t("Delivery rate by consumer group")} points={metrics?.items ?? []} series={streamDeliverySeries} valueKind="rate" expectedIntervalSeconds={metrics?.intervalSeconds} />
        <MetricTimeSeriesChart title={t("Lag change by consumer group")} points={metrics?.items ?? []} series={streamLagChangeSeries} valueKind="rate" expectedIntervalSeconds={metrics?.intervalSeconds} />
        <MetricTimeSeriesChart title={t("Observed delivery age by consumer group")} points={metrics?.items ?? []} series={streamDelaySeries} valueKind="duration" expectedIntervalSeconds={metrics?.intervalSeconds} />
      </> : <>
        <MetricTimeSeriesChart title={`${t("Backlog")} · ${t("Pending")}`} points={metrics?.items ?? []} series={groupBacklogSeries} valueKind="count" expectedIntervalSeconds={metrics?.intervalSeconds} />
        <MetricTimeSeriesChart title={`${t("Consumer delivery rate")} · ${t("Lag change / s")}`} points={metrics?.items ?? []} series={groupDeliverySeries} valueKind="rate" expectedIntervalSeconds={metrics?.intervalSeconds} />
        <MetricTimeSeriesChart title={t("Last-delivered entry age")} points={metrics?.items ?? []} series={groupDelaySeries} valueKind="duration" expectedIntervalSeconds={metrics?.intervalSeconds} />
      </>}
    </div>}
    {mode === "stream" && groups ? <ResizableGrid className="simple-table" storageKey="stream-consumer-group-performance" columns={groupColumns} headerClassName="simple-head">
      {sortedGroups.map((group) => {
        const performance = performanceByGroup.get(group.name);
        const latest = performance?.latest;
        const operational = operationalByGroup.get(group.name);
        const backlog = currentBacklog(group, operational);
        return <button
          type="button"
          className={`simple-row group-row ${selectedGroupName === group.name ? "selected" : ""}`}
          key={group.name}
          aria-label={`${group.name}; ${t("Backlog")}: ${formatCount(backlog ?? null, locale)}; ${t("Pending")}: ${group.pending.toLocaleString(locale)}; ${t("Consumer delivery rate")}: ${latest?.consumeRate == null ? "—" : formatRate(latest.consumeRate)}; ${t("Oldest pending")}: ${formatDuration(operational?.pendingSample.oldestPendingIdAgeMs)}`}
          onClick={() => onOpenGroup?.(group.name)}
        >
          <span className="group-metric-name"><UsersRound size={15} /><strong title={group.name}>{group.name}</strong></span>
          <span className="group-metric-value" data-label={t("Backlog")} title={t("Backlog is undelivered lag plus delivered entries still awaiting ACK.")}>
            {formatCount(backlog ?? null, locale)} <small>{t("{lag} lag · {pending} pending", { lag: group.lag < 0 ? "—" : group.lag.toLocaleString(locale), pending: group.pending.toLocaleString(locale) })}</small>
          </span>
          <span className="group-metric-value" data-label={t("Pending")}>{group.pending.toLocaleString(locale)}</span>
          <span className="group-metric-value" data-label={t("Consumer delivery rate")} title={t("Consumer-group entries-read progress per second. This measures delivery progress, not ACK completion.")}>{latest?.consumeRate == null ? "—" : formatRate(latest.consumeRate)}</span>
          <span className="group-metric-value" data-label={t("Oldest pending")} title={t("Oldest pending entry age and time since its last delivery.")}>
            {formatDuration(operational?.pendingSample.oldestPendingIdAgeMs)} <small>{t("{idle} idle", { idle: formatDuration(operational?.pendingSample.oldestPendingIdleMs) })}</small>
          </span>
          <ChevronRight className="group-metric-open" size={16} />
        </button>;
      })}
      {!groups.length ? <div className="panel-empty">{t("No consumer groups.")}</div> : null}
    </ResizableGrid> : null}
  </section>;
}

function OperationalKpis({ summary, mode }: { summary: OperationalSummary; mode: "stream" | "group" }) {
  const { locale, t } = useI18n();
  return <div className={`consumer-operational-kpis consumer-operational-kpis--${mode}`} role="group" aria-label={t("Consumer group operational summary")}>
    <div title={t("Undelivered lag plus delivered entries still awaiting ACK.")}><span>{t("Backlog")}</span><strong>{formatCount(summary.backlog, locale)}</strong></div>
    <div title={t("Delivered messages that remain unacknowledged in the PEL.")}><span>{t("Pending")}</span><strong>{formatCount(summary.pending, locale)}</strong></div>
    <div title={t("Consumer-group entries-read progress per second. This measures delivery progress, not ACK completion.")}><span>{t("Consumer delivery rate")}</span><strong>{summary.deliveryRate == null ? "—" : formatRate(summary.deliveryRate)}</strong></div>
    <div title={t("Consumers currently registered across the selected consumer groups.")}><span>{t("Registered consumers")}</span><strong>{formatCount(summary.activeConsumers, locale)}</strong></div>
    {mode === "group" ? <div title={t("Age of the oldest sampled pending entry and time since its last delivery.")}><span>{t("Oldest pending")}</span><strong>{formatDuration(summary.oldestPendingIdAgeMs ?? undefined)}</strong><small>{t("{idle} idle", { idle: formatDuration(summary.oldestPendingIdleMs ?? undefined) })}</small></div> : null}
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
  currentGroups: ConsumerGroup[],
): OperationalSummary {
  const operationalByName = new Map((stream?.groups ?? []).map((group) => [group.name, group]));
  const currentByName = new Map(currentGroups.map((group) => [group.name, group]));
  const names = selectedGroup
    ? [selectedGroup]
    : currentGroups.length
      ? currentGroups.map((group) => group.name)
      : stream?.groups.map((group) => group.name) ?? [];
  const backlogValues = names.map((name) => {
    const current = currentByName.get(name);
    const operational = operationalByName.get(name);
    if (current) return currentBacklog(current, operational);
    return operational?.backlog ?? null;
  });
  const pendingValues = names.map((name) => currentByName.get(name)?.pending ?? operationalByName.get(name)?.pending);
  const consumerValues = names.map((name) => currentByName.get(name)?.consumers ?? operationalByName.get(name)?.consumers);
  const operationalGroups = names.map((name) => operationalByName.get(name)).filter((group): group is StreamOperationalHealth["groups"][number] => Boolean(group));
  return {
    backlog: sumComplete(backlogValues),
    pending: sumComplete(pendingValues),
    deliveryRate: sumComplete(names.map((name) => performanceByGroup.get(name)?.latest?.consumeRate)),
    activeConsumers: sumComplete(consumerValues),
    oldestPendingIdAgeMs: maximumKnown(operationalGroups.map((group) => group.pendingSample.oldestPendingIdAgeMs)),
    oldestPendingIdleMs: maximumKnown(operationalGroups.map((group) => group.pendingSample.oldestPendingIdleMs)),
  };
}

function sumComplete(values: Array<number | null | undefined>) {
  if (!values.length || values.some((value) => value == null || !Number.isFinite(value))) return null;
  return (values as number[]).reduce((sum, value) => sum + value, 0);
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
      const current = result.get(groupName) ?? { latest: null };
      current.latest = value;
      result.set(groupName, current);
    }
  }
  return result;
}

function useGroupSeries(
  groups: string[],
  stableGroupOrder: string[],
  description: string,
  value: (point: ConsumerGroupMetricPoint, group: string) => number | null,
  format: (value: number) => string,
) {
  return useMemo<MetricChartSeries<ConsumerGroupMetricPoint>[]>(() => groups.map((group) => ({
    id: group,
    label: group,
    description,
    className: groupLineClasses[Math.max(0, stableGroupOrder.indexOf(group)) % groupLineClasses.length],
    value: (point) => value(point, group),
    format,
  })), [description, format, groups, stableGroupOrder, value]);
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

function formatSignedRate(value: number) {
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

function groupPending(point: ConsumerGroupMetricPoint, group: string) {
  return point.values[group]?.pending ?? null;
}

function groupLagDelta(point: ConsumerGroupMetricPoint, group: string) {
  return point.values[group]?.lagDelta ?? null;
}

function groupObservedDeliveryAge(point: ConsumerGroupMetricPoint, group: string) {
  return point.values[group]?.observedDeliveryAgeMs ?? null;
}
