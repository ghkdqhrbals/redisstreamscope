import { useCallback, useEffect, useMemo, useState } from "react";
import { Activity, Clock3, Database, Gauge, ListChecks, RefreshCw, UsersRound } from "lucide-react";
import { api } from "../api";
import { ResizableGrid, type ResizableGridColumn } from "../components/ResizableGrid";
import { useI18n } from "../i18n";
import type { OverviewStreamItem, RedisConnection } from "../types";

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

  const totals = useMemo(() => {
    let entries = 0;
    let consumerGroups = 0;
    let totalLag = 0;
    let pending = 0;
    let lagKnown = true;
    let lastConsumed = "";
    for (const stream of streams) {
      entries += stream.length;
      consumerGroups += stream.consumerGroups;
      totalLag += stream.totalLag;
      pending += stream.pending;
      if (!stream.lagKnown) lagKnown = false;
      if (compareStreamIds(stream.lastConsumed, lastConsumed) > 0) lastConsumed = stream.lastConsumed;
    }
    return { entries, consumerGroups, totalLag, pending, lagKnown, lastConsumed };
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

  return (
    <div className="overview-page">
      <div className="page-header overview-header">
        <div><div className="breadcrumbs">{t("Overview")}</div><h1>{t("Redis Streams overview")}</h1></div>
        <div className="header-actions"><button onClick={() => void load()} disabled={loading}><RefreshCw size={14} />{loading ? t("Loading…") : t("Refresh")}</button></div>
      </div>
      {error ? <div className="page-error">{error}</div> : null}
      <div className="overview-metrics">
        <div><span><Database size={15} />{t("Streams")}</span><strong>{streams.length.toLocaleString(locale)}</strong></div>
        <div><span><Activity size={15} />{t("Entries")}</span><strong>{totals.entries.toLocaleString(locale)}</strong></div>
        <div><span><UsersRound size={15} />{t("Consumer groups")}</span><strong>{totals.consumerGroups.toLocaleString(locale)}</strong></div>
        <div><span><Gauge size={15} />{t("Total lag")}</span><strong>{totals.lagKnown ? totals.totalLag.toLocaleString(locale) : "—"}</strong></div>
        <div><span><ListChecks size={15} />{t("Pending")}</span><strong>{totals.pending.toLocaleString(locale)}</strong></div>
        <div><span><Clock3 size={15} />{t("Last consumed")}</span><strong className="mono overview-last-consumed">{totals.lastConsumed || "—"}</strong></div>
      </div>
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
                <strong className="mono">{stream.key}</strong>
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
