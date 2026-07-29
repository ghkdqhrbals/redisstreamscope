import { useCallback, useEffect, useMemo, useState } from "react";
import { Activity, Database, RefreshCw, Server, ShieldCheck } from "lucide-react";
import { api } from "../api";
import { useI18n } from "../i18n";
import type { RedisConnection, StreamItem } from "../types";

type OverviewStream = StreamItem & {
  connectionId: string;
  connectionName: string;
};

export function OverviewView() {
  const { t } = useI18n();
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
        const response = await api.streams(connection.id);
        return response.items.map((stream) => ({
          ...stream,
          connectionId: connection.id,
          connectionName: connection.name,
        }));
      }));
      setStreams(results.flatMap((result) => result.status === "fulfilled" ? result.value : []));
    } catch (cause) {
      setConnections([]);
      setStreams([]);
      setError(cause instanceof Error ? t(cause.message) : t("Unable to load Redis status."));
    } finally {
      setLoading(false);
    }
  }, [t]);

  useEffect(() => { void load(); }, [load]);

  const entryCount = useMemo(() => streams.reduce((sum, stream) => sum + stream.length, 0), [streams]);
  return (
    <div className="overview-page">
      <div className="page-header overview-header">
        <div><div className="breadcrumbs">{t("Overview")}</div><h1>{t("Redis Streams overview")}</h1></div>
        <div className="header-actions"><button onClick={() => void load()} disabled={loading}><RefreshCw size={14} />{loading ? t("Loading…") : t("Refresh")}</button></div>
      </div>
      {error ? <div className="page-error">{error}</div> : null}
      <div className="overview-metrics">
        <div><span><Server size={15} />{t("Connections")}</span><strong>{connections.length}</strong></div>
        <div><span><Database size={15} />{t("Streams")}</span><strong>{streams.length}</strong></div>
        <div><span><Activity size={15} />{t("Entries")}</span><strong>{entryCount.toLocaleString()}</strong></div>
        <div><span><ShieldCheck size={15} />TLS</span><strong>{connections.filter((connection) => connection.tls).length}</strong></div>
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
          <div className="overview-stream-head"><span>{t("Key")}</span><span>{t("Entries")}</span><span>{t("Connection")}</span></div>
          {streams.map((stream) => (
            <div className="overview-stream-row" key={`${stream.connectionId}:${stream.key}`}><strong>{stream.key}</strong><span>{stream.length.toLocaleString()}</span><span>{stream.connectionName}</span></div>
          ))}
          {!streams.length && !loading ? <div className="panel-empty">{t("No streams match the current pattern.")}</div> : null}
        </section>
      </div>
    </div>
  );
}
