import { useCallback, useEffect, useMemo, useState } from "react";
import { Activity, AlertTriangle, CheckCircle2, Clock3, Database, Gauge, ListChecks, RefreshCw, UsersRound } from "lucide-react";
import { api } from "../api";
import { ConsumerGroupMetricsPanel } from "../components/ConsumerGroupMetricsPanel";
import { OperationalHealthPanel } from "../components/OperationalHealthPanel";
import { OperationalIntelligencePanel } from "../components/OperationalIntelligencePanel";
import { RequestLifecyclePanel } from "../components/RequestLifecyclePanel";
import { SavedDashboardsPanel } from "../components/SavedDashboardsPanel";
import { StreamMetricsPanel } from "../components/StreamMetricsPanel";
import { ResizableGrid, type ResizableGridColumn } from "../components/ResizableGrid";
import { useI18n } from "../i18n";
import type { ConsumerGroupMetricSnapshot, OverviewStreamItem, RedisConnection, ToastState } from "../types";

type OverviewStream = OverviewStreamItem & {
  connectionId: string;
  connectionName: string;
};

type OverviewViewProps = {
  onOpenGroups: (target: { connectionId: string; key: string; groupName?: string }) => void;
  currentUserId: string;
  role: "viewer" | "operator" | "admin";
  canWrite: boolean;
  onToast: (toast: ToastState) => void;
};

type OverviewConsumerGroupSnapshot = ConsumerGroupMetricSnapshot & {
  connectionId: string;
  connectionName: string;
};

type AttentionItem = {
  id: string;
  connectionId: string;
  connectionName: string;
  streamKey: string;
  groupName: string;
  consumerCount: number | null;
  pending: number;
  lag: number | null;
  observedDeliveryAgeMs: number | null;
  lastActivityAt: string | null;
  unavailable: boolean;
};

export function OverviewView({ onOpenGroups, currentUserId, role, canWrite, onToast }: OverviewViewProps) {
  const { locale, t } = useI18n();
  const [connections, setConnections] = useState<RedisConnection[]>([]);
  const [streams, setStreams] = useState<OverviewStream[]>([]);
  const [groupSnapshots, setGroupSnapshots] = useState<OverviewConsumerGroupSnapshot[]>([]);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);
  const [metricConnectionId, setMetricConnectionId] = useState("");
  const [metricStreamKey, setMetricStreamKey] = useState("");

  const load = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const connectionResponse = await api.connections();
      setConnections(connectionResponse.items);
      const [results, snapshotResults] = await Promise.all([
        Promise.allSettled(connectionResponse.items.map(async (connection) => {
          const response = await api.overview(connection.id);
          return response.items.map((stream) => ({
            ...stream,
            connectionId: connection.id,
            connectionName: connection.name,
          }));
        })),
        loadLatestGroupSnapshots(connectionResponse.items),
      ]);
      setStreams(results.flatMap((result) => result.status === "fulfilled" ? result.value : []));
      setGroupSnapshots(snapshotResults.items);
      if (results.some((result) => result.status === "rejected") || snapshotResults.failed) {
        setError(t("Some Redis connections could not be summarized."));
      }
    } catch (cause) {
      setConnections([]);
      setStreams([]);
      setGroupSnapshots([]);
      setError(cause instanceof Error ? t(cause.message) : t("Unable to load Redis status."));
    } finally {
      setLoading(false);
    }
  }, [t]);

  useEffect(() => { void load(); }, [load]);
  useEffect(() => {
    if (!connections.length) return;
    const timer = window.setInterval(() => {
      void loadLatestGroupSnapshots(connections).then((result) => {
        if (!result.failed) setGroupSnapshots(result.items);
      });
    }, 5000);
    return () => window.clearInterval(timer);
  }, [connections]);
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
    if (!monitoredStreams.some((stream) => stream.key === metricStreamKey)) setMetricStreamKey(monitoredStreams[0]?.key ?? "");
  }, [metricStreamKey, monitoredStreams]);

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
  const attentionColumns = useMemo<ResizableGridColumn[]>(() => [
    { id: "stream", label: t("Stream key"), defaultWidth: 190, minWidth: 150, grow: true },
    { id: "group", label: t("Consumer group"), defaultWidth: 170, minWidth: 130, grow: true },
    { id: "lag", label: t("Lag"), defaultWidth: 72, minWidth: 64 },
    { id: "pending", label: t("Pending"), defaultWidth: 78, minWidth: 68 },
    { id: "delay", label: t("Observed delivery age"), defaultWidth: 145, minWidth: 115 },
    { id: "activity", label: t("Last activity"), defaultWidth: 145, minWidth: 120 },
    { id: "action", label: null, ariaLabel: t("Actions"), defaultWidth: 70, minWidth: 64 },
  ], [t]);
  const attentionItems = useMemo(() => buildAttentionItems(streams, groupSnapshots), [groupSnapshots, streams]);
  const visibleAttentionItems = attentionItems.slice(0, 8);

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
      <section className="metric-history-panel overview-attention-panel">
        <header className="metric-history-header">
          <div>
            <h2><AlertTriangle size={16} />{t("Needs attention")}</h2>
            {attentionItems.length ? <span>{t("{count} affected consumer groups or streams", { count: attentionItems.length })}</span> : null}
          </div>
        </header>
        {visibleAttentionItems.length ? <ResizableGrid className="overview-stream-table overview-attention-table" storageKey="overview-attention-v2" columns={attentionColumns} headerClassName="overview-stream-head">
          {visibleAttentionItems.map((item) => <div className={`overview-stream-row overview-attention-row attention-${attentionKind(item)}`} key={item.id}>
            <strong className="mono overview-stream-key" title={`${item.connectionName} / ${item.streamKey}`}>{item.streamKey}{connections.length > 1 ? <em>{item.connectionName}</em> : null}</strong>
            <span className="mono" title={item.unavailable ? t("Unavailable") : item.groupName || t("All consumer groups")}>{item.unavailable ? t("Unavailable") : item.groupName || t("All consumer groups")}</span>
            <span>{item.lag === null ? "—" : item.lag.toLocaleString(locale)}</span>
            <span>{item.pending.toLocaleString(locale)}</span>
            <span>{formatMilliseconds(item.observedDeliveryAgeMs)}</span>
            <span>{formatActivity(item.lastActivityAt, locale)}</span>
            <span><button type="button" className="overview-group-link" onClick={() => onOpenGroups({ connectionId: item.connectionId, key: item.streamKey, groupName: item.groupName || undefined })}>{t("Open")}</button></span>
          </div>)}
        </ResizableGrid> : <div className="consumer-group-metrics-empty"><CheckCircle2 size={19} /><span>{t("No active lag or pending work")}</span></div>}
      </section>
      <OperationalHealthPanel connectionId={metricConnectionId} />
      <OperationalIntelligencePanel connectionId={metricConnectionId} streamKey={metricStreamKey} canWrite={canWrite} onToast={onToast} />
      <SavedDashboardsPanel currentUserId={currentUserId} role={role} targets={streams.filter((stream) => stream.monitored && stream.available).map((stream) => ({ connectionId: stream.connectionId, streamKey: stream.key, connectionName: stream.connectionName }))} onToast={onToast} />
      <StreamMetricsPanel
        key={`${metricConnectionId}:${metricStreamKey}`}
        connectionId={metricConnectionId}
        streamKey={metricStreamKey}
        monitored={monitoredStreams.some((stream) => stream.key === metricStreamKey)}
        leadingControls={<>
          {connections.length > 1 ? <select value={metricConnectionId} onChange={(event) => setMetricConnectionId(event.target.value)} aria-label={t("Metric connection")}>
            {connections.map((connection) => <option key={connection.id} value={connection.id}>{connection.name}</option>)}
          </select> : null}
          <select value={metricStreamKey} onChange={(event) => setMetricStreamKey(event.target.value)} aria-label={t("Metric stream")} disabled={!monitoredStreams.length}>
            {!monitoredStreams.length ? <option value="">{t("No monitored streams")}</option> : null}
            {monitoredStreams.map((stream) => <option key={stream.key} value={stream.key}>{stream.key}</option>)}
          </select>
        </>}
      />
      <RequestLifecyclePanel connectionId={metricConnectionId} streamKey={metricStreamKey} />
      <ConsumerGroupMetricsPanel
        key={`groups:${metricConnectionId}:${metricStreamKey}`}
        connectionId={metricConnectionId}
        streamKey={metricStreamKey}
        monitored={monitoredStreams.some((stream) => stream.key === metricStreamKey)}
      />
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

async function loadLatestGroupSnapshots(connections: RedisConnection[]) {
  const results = await Promise.allSettled(connections.map(async (connection) => {
    const response = await api.latestConsumerGroupMetrics(connection.id);
    return response.items.map((item) => ({
      ...item,
      connectionId: connection.id,
      connectionName: connection.name,
    }));
  }));
  return {
    items: results.flatMap((result) => result.status === "fulfilled" ? result.value : []),
    failed: results.some((result) => result.status === "rejected"),
  };
}

function buildAttentionItems(streams: OverviewStream[], snapshots: OverviewConsumerGroupSnapshot[]) {
  const streamByID = new Map(streams.map((stream) => [`${stream.connectionId}\u0000${stream.key}`, stream]));
  const coveredStreams = new Set<string>();
  const items: AttentionItem[] = [];
  for (const snapshot of snapshots) {
    const streamID = `${snapshot.connectionId}\u0000${snapshot.streamKey}`;
    const stream = streamByID.get(streamID);
    if (!stream?.available || !((snapshot.lag ?? 0) > 0 || snapshot.pending > 0)) continue;
    coveredStreams.add(streamID);
    items.push({
      id: `${streamID}\u0000${snapshot.groupName}`,
      connectionId: snapshot.connectionId,
      connectionName: snapshot.connectionName,
      streamKey: snapshot.streamKey,
      groupName: snapshot.groupName,
      consumerCount: snapshot.consumerCount,
      pending: snapshot.pending,
      lag: snapshot.lag,
      observedDeliveryAgeMs: snapshot.observedDeliveryAgeMs,
      lastActivityAt: snapshot.lastActivityAt,
      unavailable: false,
    });
  }
  for (const stream of streams) {
    const streamID = `${stream.connectionId}\u0000${stream.key}`;
    const unavailable = stream.monitored && !stream.available;
    const hasBacklog = stream.pending > 0 || (stream.lagKnown && stream.totalLag > 0);
    if ((!unavailable && !hasBacklog) || coveredStreams.has(streamID)) continue;
    items.push({
      id: `${streamID}\u0000*`,
      connectionId: stream.connectionId,
      connectionName: stream.connectionName,
      streamKey: stream.key,
      groupName: "",
      consumerCount: null,
      pending: stream.pending,
      lag: stream.lagKnown ? stream.totalLag : null,
      observedDeliveryAgeMs: null,
      lastActivityAt: null,
      unavailable,
    });
  }
  return items.sort((left, right) => {
    const severityDifference = attentionSeverity(right) - attentionSeverity(left);
    if (severityDifference) return severityDifference;
    if (left.pending !== right.pending) return right.pending - left.pending;
    if ((left.lag ?? -1) !== (right.lag ?? -1)) return (right.lag ?? -1) - (left.lag ?? -1);
    if ((left.observedDeliveryAgeMs ?? -1) !== (right.observedDeliveryAgeMs ?? -1)) return (right.observedDeliveryAgeMs ?? -1) - (left.observedDeliveryAgeMs ?? -1);
    return left.streamKey.localeCompare(right.streamKey) || left.groupName.localeCompare(right.groupName);
  });
}

function attentionSeverity(item: AttentionItem) {
  if (item.unavailable) return 4;
  if (item.consumerCount === 0 && (item.lag ?? 0) > 0) return 3;
  if (item.pending > 0) return 2;
  if ((item.lag ?? 0) > 0) return 1;
  return 0;
}

function attentionKind(item: AttentionItem) {
  if (item.unavailable) return "unavailable";
  if (item.consumerCount === 0 && (item.lag ?? 0) > 0) return "blocked";
  if (item.pending > 0) return "pending";
  return "lagging";
}

function formatMilliseconds(value: number | null) {
  if (value === null) return "—";
  if (value < 1) return `${value.toFixed(2)} ms`;
  if (value < 1000) return `${value.toFixed(value < 10 ? 1 : 0)} ms`;
  if (value < 60000) return `${(value / 1000).toFixed(1)} s`;
  return `${(value / 60000).toFixed(1)} min`;
}

function formatActivity(value: string | null, locale: string) {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "—";
  return date.toLocaleString(locale, { dateStyle: "short", timeStyle: "medium", hour12: false });
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
