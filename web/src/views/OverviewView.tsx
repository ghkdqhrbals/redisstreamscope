import { useCallback, useEffect, useMemo, useState } from "react";
import { Activity, Clock3, Database, Gauge, HardDrive, ListChecks, RefreshCw, UsersRound } from "lucide-react";
import { api } from "../api";
import { ConsumerGroupMetricsPanel } from "../components/ConsumerGroupMetricsPanel";
import { OperationalHealthPanel } from "../components/OperationalHealthPanel";
import { OperationalIntelligencePanel } from "../components/OperationalIntelligencePanel";
import { RequestLifecyclePanel } from "../components/RequestLifecyclePanel";
import { SavedDashboardsPanel } from "../components/SavedDashboardsPanel";
import { Select } from "../components/Select";
import { StreamMetricsPanel } from "../components/StreamMetricsPanel";
import { ResizableGrid, type ResizableGridColumn } from "../components/ResizableGrid";
import { useI18n } from "../i18n";
import type { OverviewStreamItem, RedisConnection, ToastState } from "../types";

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

export function OverviewView({ onOpenGroups, currentUserId, role, canWrite, onToast }: OverviewViewProps) {
  const { locale, t } = useI18n();
  const [connections, setConnections] = useState<RedisConnection[]>([]);
  const [streams, setStreams] = useState<OverviewStream[]>([]);
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
  const metricConnectionOptions = useMemo(() => connections.map((connection) => ({
    value: connection.id,
    label: connection.name,
    description: connection.id,
    meta: connection.mode,
    keywords: `${connection.id} ${connection.mode} ${connection.username}`,
    tone: connection.healthy ? "success" as const : "danger" as const,
  })), [connections]);
  const metricStreamOptions = useMemo(() => monitoredStreams.map((stream) => ({
    value: stream.key,
    label: stream.key,
    description: stream.connectionName,
    meta: t("{count} entries", { count: stream.length.toLocaleString(locale) }),
    keywords: `${stream.key} ${stream.connectionName}`,
  })), [locale, monitoredStreams, t]);

  useEffect(() => {
    if (!monitoredStreams.some((stream) => stream.key === metricStreamKey)) setMetricStreamKey(monitoredStreams[0]?.key ?? "");
  }, [metricStreamKey, monitoredStreams]);

  const totals = useMemo(() => {
    let availableStreams = 0;
    let entries = 0;
    let memoryBytes = 0;
    let memorySampleCount = 0;
    let memoryKnown = true;
    let consumerGroups = 0;
    let totalLag = 0;
    let pending = 0;
    let lagKnown = true;
    let lastConsumed = "";
    for (const stream of streams) {
      if (stream.available) {
        availableStreams += 1;
        if (stream.memoryBytes === null) memoryKnown = false;
        else {
          memoryBytes += stream.memoryBytes;
          memorySampleCount += 1;
        }
      }
      entries += stream.length;
      consumerGroups += stream.consumerGroups;
      totalLag += stream.totalLag;
      pending += stream.pending;
      if (!stream.lagKnown) lagKnown = false;
      if (compareStreamIds(stream.lastConsumed, lastConsumed) > 0) lastConsumed = stream.lastConsumed;
    }
    return { availableStreams, entries, memoryBytes, memoryKnown: memoryKnown && memorySampleCount > 0, consumerGroups, totalLag, pending, lagKnown, lastConsumed };
  }, [streams]);
  const streamColumns = useMemo<ResizableGridColumn[]>(() => [
    { id: "key", label: t("Key"), defaultWidth: 260, minWidth: 170, grow: true },
    { id: "entries", label: t("Entries"), defaultWidth: 110, minWidth: 85 },
    { id: "memory", label: t("Memory"), defaultWidth: 120, minWidth: 90 },
    { id: "groups", label: t("Consumer groups"), defaultWidth: 150, minWidth: 120 },
    { id: "lag", label: t("Total lag"), defaultWidth: 115, minWidth: 90 },
    { id: "pending", label: t("Pending"), defaultWidth: 110, minWidth: 85 },
    { id: "last-consumed", label: t("Last consumed"), defaultWidth: 200, minWidth: 150 },
    { id: "connection", label: t("Connection"), defaultWidth: 170, minWidth: 120 },
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
        <div title={t("Approximate RAM used by available stream keys.")}><span><HardDrive size={15} />{t("Stream memory")}</span><strong>{totals.memoryKnown ? formatBytes(totals.memoryBytes, locale) : "—"}</strong></div>
        <div><span><UsersRound size={15} />{t("Consumer groups")}</span><strong>{totals.consumerGroups.toLocaleString(locale)}</strong></div>
        <div><span><Gauge size={15} />{t("Total lag")}</span><strong>{totals.lagKnown ? totals.totalLag.toLocaleString(locale) : "—"}</strong></div>
        <div><span><ListChecks size={15} />{t("Pending")}</span><strong>{totals.pending.toLocaleString(locale)}</strong></div>
        <div className="overview-last-consumed-card"><span><Clock3 size={15} />{t("Last consumed")}</span><strong className="mono overview-last-consumed">{totals.lastConsumed || "—"}</strong></div>
      </div>
      <OperationalHealthPanel connectionId={metricConnectionId} />
      <OperationalIntelligencePanel connectionId={metricConnectionId} streamKey={metricStreamKey} canWrite={canWrite} onToast={onToast} />
      <SavedDashboardsPanel currentUserId={currentUserId} role={role} targets={streams.filter((stream) => stream.monitored && stream.available).map((stream) => ({ connectionId: stream.connectionId, streamKey: stream.key, connectionName: stream.connectionName }))} onToast={onToast} />
      <StreamMetricsPanel
        key={`${metricConnectionId}:${metricStreamKey}`}
        connectionId={metricConnectionId}
        streamKey={metricStreamKey}
        monitored={monitoredStreams.some((stream) => stream.key === metricStreamKey)}
        leadingControls={<>
          {connections.length > 1 ? <Select
            value={metricConnectionId}
            options={metricConnectionOptions}
            onChange={setMetricConnectionId}
            ariaLabel={t("Metric connection")}
            prefix={t("Connection")}
            searchable
            size="compact"
          /> : null}
          <Select
            value={metricStreamKey}
            options={metricStreamOptions}
            onChange={setMetricStreamKey}
            ariaLabel={t("Metric stream")}
            prefix={t("Stream")}
            placeholder={t("No monitored streams")}
            disabled={!monitoredStreams.length}
            searchable
            size="compact"
          />
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
                <strong className="mono overview-stream-key" data-label={t("Key")}>{stream.key}{!stream.available ? <em>{t("Waiting")}</em> : null}</strong>
                <span data-label={t("Entries")}>{stream.length.toLocaleString(locale)}</span>
                <span data-label={t("Memory")} title={t("Approximate RAM used by this stream key.")}>{formatBytes(stream.memoryBytes, locale)}</span>
                <span data-label={t("Consumer groups")}>
                  <button
                    type="button"
                    className="overview-group-link"
                    disabled={stream.consumerGroups === 0}
                    onClick={() => onOpenGroups({ connectionId: stream.connectionId, key: stream.key })}
                  >
                    {stream.consumerGroups.toLocaleString(locale)}
                  </button>
                </span>
                <span data-label={t("Total lag")}>{stream.lagKnown ? stream.totalLag.toLocaleString(locale) : "—"}</span>
                <span data-label={t("Pending")}>{stream.pending.toLocaleString(locale)}</span>
                <span className="mono overview-stream-last-consumed" data-label={t("Last consumed")}>{stream.lastConsumed || "—"}</span>
                <span data-label={t("Connection")}>{stream.connectionName}</span>
              </div>
            ))}
            {!streams.length && !loading ? <div className="panel-empty">{t("No streams match the current pattern.")}</div> : null}
          </ResizableGrid>
        </section>
      </div>
    </div>
  );
}


const BYTE_UNITS = ["B", "KB", "MB", "GB", "TB", "PB"] as const;

function formatBytes(value: number | null | undefined, locale: string) {
  if (value === null || value === undefined || !Number.isFinite(value) || value < 0) return "—";
  if (value === 0) return "0 B";
  const unitIndex = Math.min(Math.floor(Math.log(value) / Math.log(1024)), BYTE_UNITS.length - 1);
  const amount = value / (1024 ** unitIndex);
  const maximumFractionDigits = unitIndex === 0 || amount >= 100 ? 0 : amount >= 10 ? 1 : 2;
  return `${new Intl.NumberFormat(locale, { maximumFractionDigits }).format(amount)} ${BYTE_UNITS[unitIndex]}`;
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
