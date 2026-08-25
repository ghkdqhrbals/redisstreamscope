import { type ReactNode, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Activity, AlertTriangle, ChevronDown, Database, Gauge, HardDrive, RefreshCw, Server } from "lucide-react";
import { api } from "../api";
import { useI18n } from "../i18n";
import type { OperationalSnapshot } from "../types";
import { ResizableGrid, type ResizableGridColumn } from "./ResizableGrid";

export function OperationalHealthPanel({ connectionId }: { connectionId: string }) {
  const { locale, t } = useI18n();
  const [snapshot, setSnapshot] = useState<OperationalSnapshot | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const requestSequence = useRef(0);

  const load = useCallback(async () => {
    if (!connectionId) {
      setSnapshot(null);
      return;
    }
    const sequence = ++requestSequence.current;
    setLoading(true);
    try {
      const response = await api.operationalSnapshot(connectionId);
      if (sequence !== requestSequence.current) return;
      setSnapshot({
        ...response,
        nodes: response.nodes ?? [],
        streams: (response.streams ?? []).map((stream) => ({
          ...stream,
          risks: stream.risks ?? [],
          groups: stream.groups ?? [],
        })),
      });
      setError("");
    } catch (cause) {
      if (sequence !== requestSequence.current) return;
      setError(cause instanceof Error ? t(cause.message) : t("Unable to load operational health."));
    } finally {
      if (sequence === requestSequence.current) setLoading(false);
    }
  }, [connectionId, t]);

  useEffect(() => {
    requestSequence.current += 1;
    setSnapshot(null);
    setError("");
    void load();
    if (!connectionId) return;
    const timer = window.setInterval(() => void load(), 5000);
    return () => window.clearInterval(timer);
  }, [connectionId, load]);

  const riskCount = useMemo(
    () => snapshot?.streams.filter((stream) => stream.risks.length > 0 || stream.poisonMessagesSampled > 0).length ?? 0,
    [snapshot],
  );
  const streamColumns = useMemo<ResizableGridColumn[]>(() => [
    { id: "stream", label: t("Stream"), defaultWidth: 280, minWidth: 180, grow: true },
    { id: "memory", label: t("Memory"), defaultWidth: 110, minWidth: 84 },
    { id: "retention", label: t("Retention window"), defaultWidth: 135, minWidth: 105 },
    { id: "oldest", label: t("Oldest pending"), defaultWidth: 130, minWidth: 100 },
    { id: "poison", label: t("Poison"), defaultWidth: 82, minWidth: 68 },
    { id: "eta", label: t("Drain ETA"), defaultWidth: 110, minWidth: 86 },
    { id: "risk", label: t("Risk"), defaultWidth: 190, minWidth: 130, grow: true },
  ], [t]);
  const groupColumns = useMemo<ResizableGridColumn[]>(() => [
    { id: "group", label: t("Consumer group"), defaultWidth: 220, minWidth: 150, grow: true },
    { id: "consumers", label: t("Consumers"), defaultWidth: 92, minWidth: 76 },
    { id: "lag", label: t("Lag"), defaultWidth: 82, minWidth: 68 },
    { id: "pending", label: t("Pending"), defaultWidth: 88, minWidth: 70 },
    { id: "backlog", label: t("Backlog"), defaultWidth: 92, minWidth: 74 },
    { id: "oldest", label: t("Oldest sampled"), defaultWidth: 130, minWidth: 102 },
    { id: "p95", label: t("P95 pending idle"), defaultWidth: 135, minWidth: 105 },
    { id: "delivery", label: t("Max delivery"), defaultWidth: 110, minWidth: 88 },
    { id: "poison", label: t("Poison sampled"), defaultWidth: 115, minWidth: 92 },
    { id: "drain", label: t("Net drain / s"), defaultWidth: 112, minWidth: 90 },
    { id: "eta", label: t("Drain ETA"), defaultWidth: 105, minWidth: 84 },
    { id: "sample", label: t("Sample"), defaultWidth: 105, minWidth: 84 },
  ], [t]);

  return <section className="metric-history-panel operational-health-panel">
    <header className="metric-history-header">
      <div><h2>{t("Operational health")}</h2><span>{snapshot ? formatTimestamp(snapshot.collectedAt, locale) : t("Collector and Redis state")}</span></div>
      <div className="metric-history-controls"><button type="button" onClick={() => void load()} disabled={loading || !connectionId} aria-label={t("Refresh operational health")}><RefreshCw className={loading ? "spin" : ""} size={14} />{t("Refresh")}</button></div>
    </header>
    {error ? <div className="metric-history-error">{error}</div> : null}
    {snapshot ? <>
      <div className="operational-summary">
        <OperationalStat icon={<Activity size={15} />} label={t("Collector")} value={t(statusLabel(snapshot.collector.status))} detail={snapshot.collector.lastSuccessAt ? t("{age} since last complete sample", { age: formatAge(snapshot.collector.ageMs) }) : t("No complete sample yet")} state={collectorState(snapshot.collector.status)} />
        <OperationalStat icon={<Database size={15} />} label={t("Redis")} value={snapshot.up ? t("Available") : t("Unavailable")} detail={snapshot.pingLatencyMs == null ? "—" : `PING ${formatMilliseconds(snapshot.pingLatencyMs)}`} state={snapshot.up ? "healthy" : "critical"} />
        <OperationalStat icon={<HardDrive size={15} />} label={t("Memory")} value={formatBytes(snapshot.memory.usedBytes)} detail={snapshot.memory.maxBytes > 0 ? `${snapshot.memory.pressurePct.toFixed(1)}% · ${formatBytes(snapshot.memory.maxBytes)}` : t("No maxmemory limit")} state={snapshot.memory.critical ? "critical" : snapshot.memory.high ? "warning" : "healthy"} />
        <OperationalStat icon={<Server size={15} />} label={snapshot.cluster.enabled ? t("Cluster") : t("Nodes")} value={snapshot.cluster.enabled ? (snapshot.cluster.state || t("Unknown")) : snapshot.nodes.length.toLocaleString(locale)} detail={snapshot.cluster.enabled ? t("{ok} OK · {failed} failed slots", { ok: snapshot.cluster.slotsOk.toLocaleString(locale), failed: (snapshot.cluster.slotsPfail + snapshot.cluster.slotsFail).toLocaleString(locale) }) : t("{count} blocked clients", { count: snapshot.nodes.reduce((sum, node) => sum + node.blockedClients, 0).toLocaleString(locale) })} state={snapshot.nodes.some((node) => !node.up) || (snapshot.cluster.enabled && (snapshot.cluster.state !== "ok" || snapshot.cluster.slotsPfail + snapshot.cluster.slotsFail > 0)) ? "critical" : "healthy"} />
        <OperationalStat icon={<AlertTriangle size={15} />} label={t("Stream risks")} value={riskCount.toLocaleString(locale)} detail={t("{count} monitored streams", { count: snapshot.streams.length.toLocaleString(locale) })} state={riskCount > 0 ? "warning" : "healthy"} />
      </div>
      <div className="operational-collector-detail">
        <span><strong>{t("Last attempt")}</strong>{snapshot.collector.lastAttemptAt ? formatTimestamp(snapshot.collector.lastAttemptAt, locale) : "—"}</span>
        <span><strong>{t("Collection duration")}</strong>{formatMilliseconds(snapshot.collector.durationMs)}</span>
        <span><strong>{t("Successful streams")}</strong>{snapshot.collector.succeededStreams.toLocaleString(locale)}</span>
        <span><strong>{t("Failed streams")}</strong>{snapshot.collector.failedStreams.toLocaleString(locale)}</span>
        <span><strong>{t("Consecutive failures")}</strong>{snapshot.collector.consecutiveFailures.toLocaleString(locale)}</span>
        {snapshot.collector.error ? <span className="critical" title={snapshot.collector.error}><strong>{t("Error")}</strong>{snapshot.collector.error}</span> : null}
      </div>
      {snapshot.cluster.enabled ? <div className="operational-cluster-detail">
        <span><strong>{t("Cluster size")}</strong>{snapshot.cluster.clusterSize.toLocaleString(locale)}</span>
        <span><strong>{t("Known nodes")}</strong>{snapshot.cluster.knownNodes.toLocaleString(locale)}</span>
        <span><strong>{t("Slots assigned")}</strong>{snapshot.cluster.slotsAssigned.toLocaleString(locale)}</span>
        <span><strong>{t("Slots OK")}</strong>{snapshot.cluster.slotsOk.toLocaleString(locale)}</span>
        <span><strong>{t("Slots PFAIL")}</strong>{snapshot.cluster.slotsPfail.toLocaleString(locale)}</span>
        <span><strong>{t("Slots FAIL")}</strong>{snapshot.cluster.slotsFail.toLocaleString(locale)}</span>
      </div> : null}
      <ResizableGrid className="operational-streams operational-stream-grid" storageKey="operational-streams" columns={streamColumns} headerClassName="operational-stream-head">
        {snapshot.streams.map((stream) => <details key={stream.key} className={stream.risks.length || stream.poisonMessagesSampled ? "operational-stream-row has-risk" : "operational-stream-row"}>
          <summary>
            <span className="mono" title={stream.key}><ChevronDown size={14} />{stream.key}</span>
            <span>{stream.memoryBytes == null ? "—" : formatBytes(stream.memoryBytes)}</span>
            <span>{stream.retentionWindowMs == null ? "—" : formatDuration(stream.retentionWindowMs)}</span>
            <span>{stream.oldestPendingIdleMs == null ? "—" : formatDuration(stream.oldestPendingIdleMs)}</span>
            <span>{stream.poisonMessagesSampled.toLocaleString(locale)}</span>
            <span>{stream.drainEtaSeconds == null ? "—" : formatDuration(stream.drainEtaSeconds * 1000)}</span>
            <span>{stream.risks.length ? stream.risks.map((risk) => t(riskLabel(risk))).join(", ") : t("None")}</span>
          </summary>
          <div className="operational-stream-expanded">
            <dl className="operational-stream-facts">
              <div><dt>{t("Entries added")}</dt><dd>{stream.entriesAdded.toLocaleString(locale)}</dd></div>
              <div><dt>{t("Removed estimate")}</dt><dd>{stream.removedEntriesEstimate.toLocaleString(locale)}</dd></div>
              <div><dt>{t("Length growth / s")}</dt><dd>{formatRate(stream.lengthGrowthPerSecond, locale)}</dd></div>
              <div><dt>{t("Memory growth / s")}</dt><dd>{stream.memoryGrowthBytesPerSecond == null ? "—" : `${formatBytes(Math.abs(stream.memoryGrowthBytesPerSecond))}/s${stream.memoryGrowthBytesPerSecond < 0 ? " ↓" : ""}`}</dd></div>
              <div><dt>{t("Memory share")}</dt><dd>{stream.memorySharePct == null ? "—" : `${stream.memorySharePct.toFixed(2)}%`}</dd></div>
              <div><dt>{t("Retention status")}</dt><dd>{t(retentionLabel(stream.retentionStatus))}</dd></div>
            </dl>
            {stream.error ? <div className="metric-history-error">{stream.error}</div> : null}
            <ResizableGrid className="operational-group-table" storageKey="operational-consumer-groups" columns={groupColumns} headerClassName="operational-group-head">
            {stream.groups.map((group) => <div className="operational-group-row" key={group.name}><span className="mono" title={group.name}>{group.name}</span><span>{group.consumers.toLocaleString(locale)}</span><span>{group.lag == null ? "—" : group.lag.toLocaleString(locale)}</span><span>{group.pending.toLocaleString(locale)}</span><span>{group.backlog == null ? "—" : group.backlog.toLocaleString(locale)}</span><span>{group.pendingSample.oldestPendingIdleMs == null ? "—" : formatDuration(group.pendingSample.oldestPendingIdleMs)}</span><span>{group.pendingSample.p95PendingIdleMs == null ? "—" : formatDuration(group.pendingSample.p95PendingIdleMs)}</span><span>{group.pendingSample.maxDeliveryCount.toLocaleString(locale)}</span><span>{group.pendingSample.poisonMessagesSampled.toLocaleString(locale)}</span><span>{formatRate(group.netDrainRate, locale)}</span><span>{group.drainEtaSeconds == null ? "—" : formatDuration(group.drainEtaSeconds * 1000)}</span><span>{group.pendingSample.sampleTruncated ? t("Truncated") : `${group.pendingSample.sampled.toLocaleString(locale)}/${group.pending.toLocaleString(locale)}`}</span></div>)}
            {!stream.groups.length ? <div className="panel-empty">{t("No consumer groups.")}</div> : null}
            </ResizableGrid>
          </div>
        </details>)}
        {!snapshot.streams.length ? <div className="panel-empty">{t("No monitored streams")}</div> : null}
      </ResizableGrid>
      <details className="operational-nodes">
        <summary><Gauge size={14} />{t("Redis nodes")}<span>{snapshot.nodes.length.toLocaleString(locale)}</span><ChevronDown size={14} /></summary>
        <div>{snapshot.nodes.map((node) => <article key={node.id}><header><strong className="mono">{node.address || node.id}</strong><span className={node.up ? "healthy" : "critical"}>{node.up ? t("Up") : t("Down")}</span></header>{node.error ? <div className="metric-history-error">{node.error}</div> : null}<dl><div><dt>{t("Role")}</dt><dd>{node.role || "—"}</dd></div><div><dt>{t("Redis version")}</dt><dd>{node.version || "—"}</dd></div><div><dt>{t("Uptime")}</dt><dd>{formatDuration(node.uptimeSeconds * 1000)}</dd></div><div><dt>{t("Used memory")}</dt><dd>{formatBytes(node.usedMemoryBytes)}</dd></div><div><dt>{t("Memory pressure")}</dt><dd>{node.maxMemoryBytes > 0 ? `${node.memoryPressurePct.toFixed(1)}%` : t("No maxmemory limit")}</dd></div><div><dt>{t("Fragmentation")}</dt><dd>{node.memoryFragmentationRatio ? node.memoryFragmentationRatio.toFixed(2) : "—"}</dd></div><div><dt>{t("Ops / s")}</dt><dd>{node.operationsPerSecond.toLocaleString(locale)}</dd></div><div><dt>{t("Clients")}</dt><dd>{node.connectedClients.toLocaleString(locale)}</dd></div><div><dt>{t("Blocked")}</dt><dd>{node.blockedClients.toLocaleString(locale)}</dd></div><div><dt>{t("Evicted keys")}</dt><dd>{node.evictedKeys.toLocaleString(locale)}</dd></div><div><dt>{t("Rejected connections")}</dt><dd>{node.rejectedConnections.toLocaleString(locale)}</dd></div><div><dt>{t("Loading")}</dt><dd>{node.loading ? t("Yes") : t("No")}</dd></div><div><dt>{t("RDB last save")}</dt><dd>{node.rdbLastSaveStatus || "—"}</dd></div><div><dt>{t("AOF last rewrite")}</dt><dd>{node.aofLastRewriteStatus || "—"}</dd></div><div><dt>{t("Master link")}</dt><dd>{node.masterLinkStatus || "—"}</dd></div><div><dt>{t("Replicas")}</dt><dd>{node.connectedReplicas.toLocaleString(locale)}</dd></div><div><dt>{t("Replication offset")}</dt><dd>{node.replicationOffset.toLocaleString(locale)}</dd></div><div><dt>{t("Full resyncs")}</dt><dd>{node.fullResyncs.toLocaleString(locale)}</dd></div><div><dt>{t("Partial resync errors")}</dt><dd>{node.partialResyncErrors.toLocaleString(locale)}</dd></div></dl></article>)}</div>
      </details>
    </> : <div className="consumer-group-metrics-empty">{connectionId ? t("Waiting for operational samples…") : t("No connections are configured.")}</div>}
  </section>;
}

function OperationalStat({ icon, label, value, detail, state }: { icon: ReactNode; label: string; value: string; detail: string; state: "healthy" | "warning" | "critical" }) {
  return <div className={`operational-stat operational-stat--${state}`}><span>{icon}{label}</span><strong>{value}</strong><em>{detail}</em></div>;
}

function statusLabel(status: OperationalSnapshot["collector"]["status"]) {
  return ({ never_run: "Never run", collecting: "Collecting", healthy: "Healthy", partial: "Partial", failed: "Failed", stale: "Stale" } as const)[status];
}

function collectorState(status: OperationalSnapshot["collector"]["status"]): "healthy" | "warning" | "critical" {
  if (status === "failed" || status === "stale") return "critical";
  if (status === "partial" || status === "collecting" || status === "never_run") return "warning";
  return "healthy";
}

function riskLabel(risk: string) {
  return ({
    growing_without_removal: "Growing without observed removal",
    poison_messages_sampled: "Poison messages sampled",
    connection_memory_high: "Connection memory high",
  } as Record<string, string>)[risk] ?? risk;
}

function retentionLabel(status: string) {
  return ({ stable: "Stable", growing: "Growing", growing_without_removal: "Growing without observed removal", unknown: "Unknown" } as Record<string, string>)[status] ?? status;
}

function formatRate(value: number | undefined, locale: string) {
  if (value == null || !Number.isFinite(value)) return "—";
  return `${value.toLocaleString(locale, { maximumFractionDigits: 2 })}/s`;
}

function formatBytes(value: number) {
  if (!Number.isFinite(value) || value <= 0) return value === 0 ? "0 B" : "—";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  const index = Math.min(units.length - 1, Math.floor(Math.log(value) / Math.log(1024)));
  return `${(value / 1024 ** index).toFixed(index === 0 ? 0 : 1)} ${units[index]}`;
}

function formatMilliseconds(value: number) {
  return value < 1 ? `${value.toFixed(2)} ms` : `${value.toFixed(value < 10 ? 1 : 0)} ms`;
}

function formatDuration(milliseconds: number) {
  if (milliseconds < 1000) return `${Math.round(milliseconds)} ms`;
  if (milliseconds < 60_000) return `${(milliseconds / 1000).toFixed(milliseconds < 10_000 ? 1 : 0)} s`;
  if (milliseconds < 3_600_000) return `${(milliseconds / 60_000).toFixed(1)} min`;
  if (milliseconds < 86_400_000) return `${(milliseconds / 3_600_000).toFixed(1)} h`;
  return `${(milliseconds / 86_400_000).toFixed(1)} d`;
}

function formatAge(ageMs?: number) {
  return ageMs == null ? "—" : formatDuration(ageMs);
}

function formatTimestamp(value: string, locale: string) {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString(locale, { hour12: false });
}
