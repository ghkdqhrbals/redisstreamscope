import { useCallback, useEffect, useMemo, useState } from "react";
import { Activity, Clock3, Database, Gauge, ListChecks, Radio, RefreshCw, UsersRound } from "lucide-react";
import { api } from "../api";
import { MetricTimeSeriesChart, type MetricChartSeries } from "../components/MetricTimeSeriesChart";
import { ResizableGrid, type ResizableGridColumn } from "../components/ResizableGrid";
import { useI18n } from "../i18n";
import type { OverviewStreamItem, RedisConnection, StreamMetricSeries } from "../types";

type OverviewStream = OverviewStreamItem & {
  connectionId: string;
  connectionName: string;
};

type OverviewViewProps = {
  onOpenGroups: (target: { connectionId: string; key: string }) => void;
};

export function OverviewView({ onOpenGroups }: OverviewViewProps) {
  const { locale, t } = useI18n();
  const [connections, setConnections] = useState<RedisConnection[]>([]);
  const [streams, setStreams] = useState<OverviewStream[]>([]);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);
  const [metricConnectionId, setMetricConnectionId] = useState("");
  const [metricStreamKey, setMetricStreamKey] = useState("");
  const [metricRange, setMetricRange] = useState<StreamMetricSeries["range"]>("5m");
  const [metricSeries, setMetricSeries] = useState<StreamMetricSeries | null>(null);
  const [metricError, setMetricError] = useState("");
  const [metricLoading, setMetricLoading] = useState(false);
  const [liveMetrics, setLiveMetrics] = useState(true);

  const load = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const connectionResponse = await api.connections();
      setConnections(connectionResponse.items);
      const results = await Promise.allSettled(connectionResponse.items.map(async (connection) => {
        const response = await api.overview(connection.id);
        return response.items.map((stream) => ({
          ...stream,
          connectionId: connection.id,
          connectionName: connection.name,
        }));
      }));
      setStreams(results.flatMap((result) => result.status === "fulfilled" ? result.value : []));
      if (results.some((result) => result.status === "rejected")) {
        setError(t("Some Redis connections could not be summarized."));
      }
    } catch (cause) {
      setConnections([]);
      setStreams([]);
      setError(cause instanceof Error ? t(cause.message) : t("Unable to load Redis status."));
    } finally {
      setLoading(false);
    }
  }, [t]);

  useEffect(() => { void load(); }, [load]);
  useEffect(() => {
    if (!connections.length) {
      setMetricConnectionId("");
      return;
    }
    if (!connections.some((connection) => connection.id === metricConnectionId)) {
      setMetricConnectionId(connections[0].id);
    }
  }, [connections, metricConnectionId]);

  const monitoredStreams = useMemo(
    () => streams.filter((stream) => stream.connectionId === metricConnectionId && stream.monitored && stream.available),
    [metricConnectionId, streams],
  );

  useEffect(() => {
    if (metricStreamKey && !monitoredStreams.some((stream) => stream.key === metricStreamKey)) {
      setMetricStreamKey("");
    }
  }, [metricStreamKey, monitoredStreams]);

  const loadMetrics = useCallback(async () => {
    if (!metricConnectionId) return;
    setMetricLoading(true);
    try {
      const response = await api.metrics(metricConnectionId, metricRange, metricStreamKey);
      setMetricSeries(response);
      setMetricError("");
    } catch (cause) {
      setMetricError(cause instanceof Error ? t(cause.message) : t("Unable to load stream metrics."));
    } finally {
      setMetricLoading(false);
    }
  }, [metricConnectionId, metricRange, metricStreamKey, t]);

  useEffect(() => {
    if (!metricConnectionId) return;
    void loadMetrics();
    if (!liveMetrics) return;
    const timer = window.setInterval(() => void loadMetrics(), 1000);
    return () => window.clearInterval(timer);
  }, [liveMetrics, loadMetrics, metricConnectionId]);

  const totals = useMemo(() => {
    let availableStreams = 0;
    let entries = 0;
    let consumerGroups = 0;
    let totalLag = 0;
    let pending = 0;
    let lagKnown = true;
    let lastConsumed = "";
    for (const stream of streams) {
      if (stream.available) availableStreams += 1;
      entries += stream.length;
      consumerGroups += stream.consumerGroups;
      totalLag += stream.totalLag;
      pending += stream.pending;
      if (!stream.lagKnown) lagKnown = false;
      if (compareStreamIds(stream.lastConsumed, lastConsumed) > 0) lastConsumed = stream.lastConsumed;
    }
    return { availableStreams, entries, consumerGroups, totalLag, pending, lagKnown, lastConsumed };
  }, [streams]);
  const streamColumns = useMemo<ResizableGridColumn[]>(() => [
    { id: "key", label: t("Key"), defaultWidth: 260, minWidth: 170, grow: true },
    { id: "entries", label: t("Entries"), defaultWidth: 110, minWidth: 85 },
    { id: "groups", label: t("Consumer groups"), defaultWidth: 150, minWidth: 120 },
    { id: "lag", label: t("Total lag"), defaultWidth: 115, minWidth: 90 },
    { id: "pending", label: t("Pending"), defaultWidth: 110, minWidth: 85 },
    { id: "last-consumed", label: t("Last consumed"), defaultWidth: 200, minWidth: 150 },
    { id: "connection", label: t("Connection"), defaultWidth: 170, minWidth: 120 },
  ], [t]);
  const pressureSeries = useMemo<MetricChartSeries[]>(() => [
    {
      id: "lag",
      label: t("Total lag"),
      description: t("Messages still waiting to be delivered across consumer groups."),
      className: "metric-line-primary",
      value: (point) => point.totalLag,
      format: (value) => Math.round(value).toLocaleString(locale),
    },
    {
      id: "pending",
      label: t("Pending"),
      description: t("Delivered messages that remain unacknowledged in the PEL."),
      className: "metric-line-secondary",
      value: (point) => point.pending,
      format: (value) => Math.round(value).toLocaleString(locale),
    },
    {
      id: "consumers",
      label: t("Consumers"),
      description: t("Consumers currently registered across the selected consumer groups."),
      className: "metric-line-tertiary",
      value: (point) => point.consumerCount,
      format: (value) => Math.round(value).toLocaleString(locale),
    },
  ], [locale, t]);
  const latencySeries = useMemo<MetricChartSeries[]>(() => [
    {
      id: "consume-delay",
      label: t("Consume delay"),
      description: t("Observed delivery time minus the publish time encoded in the Redis Stream ID."),
      className: "metric-line-primary",
      value: (point) => point.consumeDelayMs,
      format: formatMilliseconds,
    },
    {
      id: "redis-latency",
      label: t("Redis response"),
      description: t("Round-trip time for the collector's Redis PING."),
      className: "metric-line-secondary",
      value: (point) => point.redisLatencyMs,
      format: formatMilliseconds,
    },
  ], [t]);
  const rateSeries = useMemo<MetricChartSeries[]>(() => [
    {
      id: "publish-rate",
      label: t("Published / s"),
      description: t("Entries added per second. Uses entries-added when Redis exposes it."),
      className: "metric-line-primary",
      value: (point) => point.publishRate,
      format: formatRate,
    },
    {
      id: "consume-rate",
      label: t("Consumed / s"),
      description: t("Consumer group progress per second from entries-read."),
      className: "metric-line-secondary",
      value: (point) => point.consumeRate,
      format: formatRate,
    },
    {
      id: "lag-delta",
      label: t("Lag change / s"),
      description: t("Positive values mean backlog is growing; negative values mean it is draining."),
      className: "metric-line-tertiary",
      value: (point) => point.lagDelta,
      format: formatSignedRate,
    },
  ], [t]);

  return (
    <div className="overview-page">
      <div className="page-header overview-header">
        <div><div className="breadcrumbs">{t("Overview")}</div><h1>{t("Redis Streams overview")}</h1></div>
        <div className="header-actions"><button onClick={() => void load()} disabled={loading}><RefreshCw size={14} />{loading ? t("Loading…") : t("Refresh")}</button></div>
      </div>
      {error ? <div className="page-error">{error}</div> : null}
      <div className="overview-metrics">
        <div><span><Database size={15} />{t("Streams")}</span><strong>{totals.availableStreams.toLocaleString(locale)}</strong></div>
        <div><span><Activity size={15} />{t("Entries")}</span><strong>{totals.entries.toLocaleString(locale)}</strong></div>
        <div><span><UsersRound size={15} />{t("Consumer groups")}</span><strong>{totals.consumerGroups.toLocaleString(locale)}</strong></div>
        <div><span><Gauge size={15} />{t("Total lag")}</span><strong>{totals.lagKnown ? totals.totalLag.toLocaleString(locale) : "—"}</strong></div>
        <div><span><ListChecks size={15} />{t("Pending")}</span><strong>{totals.pending.toLocaleString(locale)}</strong></div>
        <div><span><Clock3 size={15} />{t("Last consumed")}</span><strong className="mono overview-last-consumed">{totals.lastConsumed || "—"}</strong></div>
      </div>
      <section className="metric-history-panel">
        <header className="metric-history-header">
          <div><h2>{t("Stream performance")}</h2><span>{metricSeries ? t("{seconds}s samples", { seconds: metricSeries.intervalSeconds }) : t("Time series")}</span></div>
          <div className="metric-history-controls">
            {connections.length > 1 ? <select value={metricConnectionId} onChange={(event) => setMetricConnectionId(event.target.value)} aria-label={t("Metric connection")}>
              {connections.map((connection) => <option key={connection.id} value={connection.id}>{connection.name}</option>)}
            </select> : null}
            <select value={metricStreamKey} onChange={(event) => setMetricStreamKey(event.target.value)} aria-label={t("Metric stream")}>
              <option value="">{t("All monitored streams")}</option>
              {monitoredStreams.map((stream) => <option key={stream.key} value={stream.key}>{stream.key}</option>)}
            </select>
            <select value={metricRange} onChange={(event) => setMetricRange(event.target.value as StreamMetricSeries["range"])} aria-label={t("Time range")}>
              <option value="1m">{t("Last minute")}</option>
              <option value="5m">{t("Last 5 minutes")}</option>
              <option value="15m">{t("Last 15 minutes")}</option>
              <option value="1h">{t("Last hour")}</option>
              <option value="6h">{t("Last 6 hours")}</option>
              <option value="24h">{t("Last 24 hours")}</option>
              <option value="7d">{t("Last 7 days")}</option>
            </select>
            <button type="button" className={liveMetrics ? "metric-live active" : "metric-live"} aria-pressed={liveMetrics} onClick={() => setLiveMetrics((current) => !current)}><Radio size={14} />{t("Live")}</button>
            <button type="button" aria-label={t("Refresh metrics")} title={t("Refresh metrics")} disabled={metricLoading || !metricConnectionId} onClick={() => void loadMetrics()}><RefreshCw className={metricLoading ? "spin" : ""} size={14} /></button>
          </div>
        </header>
        {metricError ? <div className="metric-history-error">{metricError}</div> : null}
        <div className="metric-chart-grid">
          <MetricTimeSeriesChart title={t("Consumer pressure")} points={metricSeries?.items ?? []} series={pressureSeries} />
          <MetricTimeSeriesChart title={t("Latency")} points={metricSeries?.items ?? []} series={latencySeries} />
          <MetricTimeSeriesChart title={t("Message rates")} points={metricSeries?.items ?? []} series={rateSeries} />
        </div>
      </section>
      <div className="overview-grid overview-grid--live">
        <section className="lag-panel">
          <div className="section-title"><h2>{t("Connection health")}</h2></div>
          {connections.map((connection) => (
            <div className="lag-row" key={connection.id}>
              <i className={connection.healthy ? "" : "red"}><Activity size={14} /></i>
              <div><strong>{connection.name}</strong><span>{connection.mode}{connection.username ? ` · ACL ${connection.username}` : ""}</span></div>
              <b>{connection.latencyMs.toFixed(1)} ms</b>
              <em className={connection.healthy ? "healthy" : "high"}>{connection.healthy ? t("Healthy") : t("Down")}</em>
            </div>
          ))}
          {!connections.length && !loading ? <div className="panel-empty">{t("No connections are configured.")}</div> : null}
        </section>
        <section className="streams-panel">
          <div className="section-title"><h2>{t("Streams")}</h2></div>
          <ResizableGrid className="overview-stream-table" storageKey="overview-streams" columns={streamColumns} headerClassName="overview-stream-head">
            {streams.map((stream) => (
              <div className="overview-stream-row" key={`${stream.connectionId}:${stream.key}`}>
                <strong className="mono overview-stream-key">{stream.key}{!stream.available ? <em>{t("Waiting")}</em> : null}</strong>
                <span>{stream.length.toLocaleString(locale)}</span>
                <span>
                  <button
                    type="button"
                    className="overview-group-link"
                    disabled={stream.consumerGroups === 0}
                    onClick={() => onOpenGroups({ connectionId: stream.connectionId, key: stream.key })}
                  >
                    {stream.consumerGroups.toLocaleString(locale)}
                  </button>
                </span>
                <span>{stream.lagKnown ? stream.totalLag.toLocaleString(locale) : "—"}</span>
                <span>{stream.pending.toLocaleString(locale)}</span>
                <span className="mono">{stream.lastConsumed || "—"}</span>
                <span>{stream.connectionName}</span>
              </div>
            ))}
            {!streams.length && !loading ? <div className="panel-empty">{t("No streams match the current pattern.")}</div> : null}
          </ResizableGrid>
        </section>
      </div>
    </div>
  );
}

function compareStreamIds(left: string, right: string) {
  if (left === right) return 0;
  if (!left) return -1;
  if (!right) return 1;
  const [leftTime = "0", leftSequence = "0"] = left.split("-");
  const [rightTime = "0", rightSequence = "0"] = right.split("-");
  try {
    const timeDifference = BigInt(leftTime) - BigInt(rightTime);
    if (timeDifference !== 0n) return timeDifference < 0n ? -1 : 1;
    const sequenceDifference = BigInt(leftSequence) - BigInt(rightSequence);
    return sequenceDifference === 0n ? 0 : sequenceDifference < 0n ? -1 : 1;
  } catch {
    return left.localeCompare(right, undefined, { numeric: true });
  }
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
  const prefix = value > 0 ? "+" : "";
  return `${prefix}${value.toFixed(Math.abs(value) < 10 ? 2 : 1)} /s`;
}
