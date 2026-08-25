import { useCallback, useEffect, useMemo, useState } from "react";
import { Activity, Cpu, DatabaseZap, RefreshCw } from "lucide-react";
import { api } from "../api";
import { useI18n } from "../i18n";
import type { CapacityForecast, ConsumerHistoryResponse, MonitoringEvent, ToastState, TopologyModel } from "../types";

type Props = {
  connectionId: string;
  streamKey: string;
  canWrite: boolean;
  onToast: (toast: ToastState) => void;
};

export function OperationalIntelligencePanel({ connectionId, streamKey, canWrite, onToast }: Props) {
  const { t } = useI18n();
  const [tab, setTab] = useState<"consumers" | "topology" | "capacity">("consumers");
  const [history, setHistory] = useState<ConsumerHistoryResponse | null>(null);
  const [topology, setTopology] = useState<TopologyModel | null>(null);
  const [topologyEvents, setTopologyEvents] = useState<MonitoringEvent[]>([]);
  const [capacity, setCapacity] = useState<CapacityForecast | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const load = useCallback(async () => {
    if (!connectionId) return;
    setLoading(true); setError("");
    const to = new Date(); const from = new Date(to.getTime() - 24 * 60 * 60 * 1000);
    try {
      if (tab === "consumers") setHistory(await api.consumerHistory({ connectionId, streamKey, from: from.toISOString(), to: to.toISOString() }));
      if (tab === "topology") {
        const [current, events] = await Promise.all([api.topology(connectionId), api.topologyEvents(connectionId, from.toISOString(), to.toISOString())]);
        setTopology(current); setTopologyEvents(events.items);
      }
      if (tab === "capacity") setCapacity(await api.capacity(connectionId, "24h"));
    } catch (cause) { setError(cause instanceof Error ? t(cause.message) : t("Unable to load operational intelligence.")); }
    finally { setLoading(false); }
  }, [connectionId, streamKey, t, tab]);

  useEffect(() => { void load(); }, [load]);

  return <section className="metric-history-panel operational-intelligence-panel">
    <header className="metric-history-header"><div><h2>{t("Operational intelligence")}</h2><span>{t("Consumer history, topology and capacity")}</span></div><button type="button" onClick={() => void load()} disabled={loading} aria-label={t("Refresh")}><RefreshCw size={14} /></button></header>
    <div className="subtabs intelligence-tabs">
      <button type="button" className={tab === "consumers" ? "active" : ""} onClick={() => setTab("consumers")}><Activity size={14} />{t("Consumer history")}</button>
      <button type="button" className={tab === "topology" ? "active" : ""} onClick={() => setTab("topology")}><Cpu size={14} />{t("Topology & failover")}</button>
      <button type="button" className={tab === "capacity" ? "active" : ""} onClick={() => setTab("capacity")}><DatabaseZap size={14} />{t("Capacity & retention")}</button>
    </div>
    {error ? <div className="metric-history-error" role="alert">{error}</div> : null}
    {tab === "consumers" ? <ConsumerHistoryView history={history} /> : null}
    {tab === "topology" ? <TopologyView model={topology} events={topologyEvents} /> : null}
    {tab === "capacity" ? <CapacityView forecast={capacity} streamKey={streamKey} canWrite={canWrite} onReload={load} onToast={onToast} /> : null}
  </section>;
}

function ConsumerHistoryView({ history }: { history: ConsumerHistoryResponse | null }) {
  const { locale, t } = useI18n();
  if (!history) return <div className="panel-empty">{t("Waiting for consumer history…")}</div>;
  const lagPoints = history.samples.map((item) => item.lag ?? 0);
  return <div className="intelligence-content">
    <div className="consumer-history-summary">
      <div><span>{t("Groups")}</span><strong>{history.current.length.toLocaleString(locale)}</strong></div>
      <div><span>{t("Consumers")}</span><strong>{history.current.reduce((sum, item) => sum + item.consumerCount, 0).toLocaleString(locale)}</strong></div>
      <div><span>{t("Stalled")}</span><strong>{history.current.reduce((sum, item) => sum + item.stalledConsumers, 0).toLocaleString(locale)}</strong></div>
      <MiniTrend values={lagPoints} />
    </div>
    <div className="consumer-state-grid">{history.current.map((group) => <div key={`${group.streamKey}:${group.groupName}`}>
      <header><strong className="mono">{group.groupName}</strong><span>{group.consumerCount} {t("consumers")}</span></header>
      <p><span>{t("Lag")}</span><strong>{group.lag ?? "—"}</strong><span>{t("Pending")}</span><strong>{group.pending}</strong><span>{t("Stalled")}</span><strong>{group.stalledConsumers}</strong></p>
      <div>{group.consumers.map((consumer) => <span className={consumer.stalled ? "consumer-chip stalled" : "consumer-chip"} key={consumer.name}>{consumer.name}</span>)}</div>
    </div>)}</div>
    <EventTimeline events={history.events} empty={t("No consumer transitions in this range.")} />
  </div>;
}

function TopologyView({ model, events }: { model: TopologyModel | null; events: MonitoringEvent[] }) {
  const { t } = useI18n();
  if (!model) return <div className="panel-empty">{t("Waiting for topology samples…")}</div>;
  return <div className="intelligence-content">
    <div className="topology-map">{model.nodes.map((node) => <div className={node.up ? "topology-node up" : "topology-node down"} key={node.id || node.address}>
      <i /><div><strong>{node.address || node.id}</strong><span>{node.role || t("Unknown")} · Redis {node.version || "—"}</span></div><em>{node.masterLinkStatus || (node.up ? t("Up") : t("Down"))}</em>
    </div>)}</div>
    <EventTimeline events={events} empty={t("No topology, replication or persistence changes in this range.")} />
  </div>;
}

function CapacityView({ forecast, streamKey, canWrite, onReload, onToast }: { forecast: CapacityForecast | null; streamKey: string; canWrite: boolean; onReload: () => Promise<void>; onToast: (toast: ToastState) => void }) {
  const { locale, t } = useI18n();
  const selected = forecast?.streams.find((item) => item.streamKey === streamKey) ?? null;
  const [maxLength, setMaxLength] = useState("");
  const [maxAge, setMaxAge] = useState("");
  const [requireTrimming, setRequireTrimming] = useState(false);
  const [busy, setBusy] = useState(false);
  useEffect(() => {
    setMaxLength(selected?.policy?.maxLength?.toString() ?? "");
    setMaxAge(selected?.policy?.maxAgeSeconds?.toString() ?? "");
    setRequireTrimming(selected?.policy?.requireTrimming ?? false);
  }, [selected?.policy?.maxAgeSeconds, selected?.policy?.maxLength, selected?.policy?.requireTrimming, streamKey]);
  if (!forecast) return <div className="panel-empty">{t("Collecting capacity samples…")}</div>;
  const save = async () => {
    setBusy(true);
    try {
      await api.saveRetentionPolicy({ connectionId: forecast.connectionId, streamKey, maxLength: maxLength ? Number(maxLength) : null, maxAgeSeconds: maxAge ? Number(maxAge) : null, requireTrimming });
      onToast({ kind: "success", title: t("Retention expectation saved"), message: t("Capacity checks now use this policy.") }); await onReload();
    } catch (cause) { onToast({ kind: "error", title: t("Unable to save retention expectation"), message: cause instanceof Error ? t(cause.message) : t("Request failed") }); }
    finally { setBusy(false); }
  };
  return <div className="intelligence-content capacity-content">
    <div className="capacity-cards">
      <ProjectionCard title={t("Connection memory")} projection={forecast.connection.usedMemoryBytes} suffix="B" locale={locale} />
      {selected ? <><ProjectionCard title={t("Stream length")} projection={selected.length} suffix="" locale={locale} /><ProjectionCard title={t("Stream memory")} projection={selected.memoryBytes ?? null} suffix="B" locale={locale} /></> : null}
    </div>
    {selected ? <div className="retention-policy-editor">
      <header><div><strong>{t("Retention expectation")}</strong><span className={`compliance-${selected.compliance.status}`}>{t(selected.compliance.status)}</span></div><span>{selected.compliance.reasons.map((reason) => t(reason)).join(" · ") || t("No violations")}</span></header>
      <div><label>{t("Maximum length")}<input type="number" min={1} value={maxLength} onChange={(event) => setMaxLength(event.target.value)} placeholder={t("Not set")} /></label><label>{t("Maximum age (seconds)")}<input type="number" min={1} value={maxAge} onChange={(event) => setMaxAge(event.target.value)} placeholder={t("Not set")} /></label><label className="checkbox-field"><input type="checkbox" checked={requireTrimming} onChange={(event) => setRequireTrimming(event.target.checked)} /><span>{t("Require observed trimming")}</span></label>{canWrite ? <button type="button" className="primary-button" onClick={() => void save()} disabled={busy}>{busy ? t("Saving…") : t("Save expectation")}</button> : null}</div>
    </div> : <div className="panel-empty">{t("Choose a monitored stream to view its capacity forecast.")}</div>}
    <p className="capacity-disclaimer">{forecast.disclaimer}</p>
  </div>;
}

function ProjectionCard({ title, projection, suffix, locale }: { title: string; projection: CapacityForecast["connection"]["usedMemoryBytes"] | null; suffix: string; locale: string }) {
  const { t } = useI18n();
  if (!projection) return <div className="capacity-card"><span>{title}</span><strong>—</strong><em>{t("No samples")}</em></div>;
  return <div className="capacity-card"><span>{title}</span><strong>{projection.current.toLocaleString(locale)}{suffix}</strong><em>{projection.growthPerSecond == null ? t("Trend unavailable") : `${projection.growthPerSecond.toLocaleString(locale, { maximumFractionDigits: 2 })}${suffix}/s`} · {Math.round(projection.confidence * 100)}% {t("confidence")} · {projection.sampleCount} {t("samples")}</em></div>;
}

function EventTimeline({ events, empty }: { events: MonitoringEvent[]; empty: string }) {
  const { locale, t } = useI18n();
  return <div className="event-timeline">{events.slice(0, 30).map((event) => <div key={event.id}><i /><time>{new Date(event.occurredAt).toLocaleString(locale, { dateStyle: "short", timeStyle: "medium", hour12: false })}</time><strong>{t(event.type)}</strong><span className="mono">{[event.streamKey, event.groupName, event.consumerName, event.nodeId].filter(Boolean).join(" / ")}</span>{event.from || event.to ? <em>{event.from || "—"} → {event.to || "—"}</em> : null}</div>)}{!events.length ? <div className="panel-empty">{empty}</div> : null}</div>;
}

function MiniTrend({ values }: { values: number[] }) {
  if (values.length < 2) return <div className="mini-trend" />;
  const width = 180; const height = 42; const max = Math.max(1, ...values); const min = Math.min(...values);
  const points = values.map((value, index) => `${(index / (values.length - 1)) * width},${height - ((value - min) / Math.max(1, max - min)) * (height - 4) - 2}`).join(" ");
  return <svg className="mini-trend" viewBox={`0 0 ${width} ${height}`} role="img"><polyline points={points} fill="none" stroke="currentColor" strokeWidth="1.5" /></svg>;
}
