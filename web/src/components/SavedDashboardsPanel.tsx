import { FormEvent, useCallback, useEffect, useMemo, useState } from "react";
import { BookmarkPlus, LayoutDashboard, Trash2 } from "lucide-react";
import { api } from "../api";
import { useI18n } from "../i18n";
import type { DashboardTarget, SavedDashboard, StreamMetricSeries, ToastState } from "../types";
import { Select } from "./Select";

type AvailableTarget = DashboardTarget & { connectionName: string };

export function SavedDashboardsPanel({ targets, currentUserId, role, onToast }: { targets: AvailableTarget[]; currentUserId: string; role: "viewer" | "operator" | "admin"; onToast: (toast: ToastState) => void }) {
  const { locale, t } = useI18n();
  const [items, setItems] = useState<SavedDashboard[]>([]);
  const [selectedID, setSelectedID] = useState("");
  const [series, setSeries] = useState<Record<string, StreamMetricSeries>>({});
  const [showCreate, setShowCreate] = useState(false);
  const [name, setName] = useState("");
  const [range, setRange] = useState<StreamMetricSeries["range"]>("15m");
  const [selectedTargets, setSelectedTargets] = useState<string[]>([]);
  const [shared, setShared] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const rangeOptions = useMemo(() => [
    { value: "5m", label: t("Last 5 minutes") },
    { value: "15m", label: t("Last 15 minutes") },
    { value: "1h", label: t("Last hour") },
    { value: "6h", label: t("Last 6 hours") },
    { value: "24h", label: t("Last 24 hours") },
  ], [t]);
  const selected = items.find((item) => item.id === selectedID) ?? null;
  const canDeleteSelected = Boolean(selected && (role === "admin" || selected.ownerId === currentUserId));

  const load = useCallback(async () => {
    try {
      const response = await api.dashboards();
      setItems(response.items);
      setSelectedID((current) => response.items.some((item) => item.id === current) ? current : response.items[0]?.id ?? "");
    } catch (cause) { setError(cause instanceof Error ? t(cause.message) : t("Unable to load dashboards.")); }
  }, [t]);
  useEffect(() => { void load(); }, [load]);
  useEffect(() => {
    if (!selected) { setSeries({}); return; }
    let active = true;
    void Promise.allSettled(selected.definition.targets.map(async (target) => ({ key: targetID(target), value: await api.metrics(target.connectionId, selected.definition.timeRange, target.streamKey) }))).then((results) => {
      if (!active) return;
      const next: Record<string, StreamMetricSeries> = {};
      for (const result of results) if (result.status === "fulfilled") next[result.value.key] = result.value.value;
      setSeries(next);
    });
    return () => { active = false; };
  }, [selectedID, selected?.updatedAt]);

  const create = async (event: FormEvent) => {
    event.preventDefault(); setBusy(true); setError("");
    try {
      const definition = { timeRange: range, targets: selectedTargets.map((id) => parseTargetID(id)), widgets: ["lag", "pending", "rates", "latency"] };
      const item = await api.createDashboard({ name, shared, definition });
      setItems((current) => [item, ...current]); setSelectedID(item.id); setShowCreate(false); setName(""); setSelectedTargets([]);
      onToast({ kind: "success", title: t("Dashboard saved"), message: t("The comparison is ready to reuse.") });
    } catch (cause) { setError(cause instanceof Error ? t(cause.message) : t("Unable to save dashboard.")); }
    finally { setBusy(false); }
  };
  const remove = async () => {
    if (!selected) return;
    setBusy(true);
    try { await api.deleteDashboard(selected.id); await load(); onToast({ kind: "success", title: t("Dashboard deleted"), message: selected.name }); }
    catch (cause) { setError(cause instanceof Error ? t(cause.message) : t("Unable to delete dashboard.")); }
    finally { setBusy(false); }
  };

  const targetNames = useMemo(() => new Map(targets.map((target) => [targetID(target), target])), [targets]);
  return <section className="metric-history-panel saved-dashboards-panel">
    <header className="metric-history-header"><div><h2><LayoutDashboard size={16} />{t("Saved dashboards")}</h2><span>{t("Reusable multi-stream comparisons")}</span></div><button type="button" onClick={() => setShowCreate((current) => !current)}><BookmarkPlus size={14} />{t("Save dashboard")}</button></header>
    {error ? <div className="metric-history-error" role="alert">{error}</div> : null}
    {showCreate ? <form className="dashboard-builder" onSubmit={create}>
      <div><label>{t("Dashboard name")}<input value={name} onChange={(event) => setName(event.target.value)} required maxLength={120} /></label><label>{t("Time range")}<Select value={range} options={rangeOptions} onChange={(next) => setRange(next as StreamMetricSeries["range"])} ariaLabel={t("Time range")} className="select-control--block" size="compact" /></label><label className="checkbox-field"><input type="checkbox" checked={shared} onChange={(event) => setShared(event.target.checked)} /><span>{t("Share with other users")}</span></label></div>
      <div className="dashboard-target-picker">{targets.map((target) => { const id = targetID(target); return <label key={id}><input type="checkbox" checked={selectedTargets.includes(id)} disabled={!selectedTargets.includes(id) && selectedTargets.length >= 8} onChange={(event) => setSelectedTargets((current) => event.target.checked ? [...current, id] : current.filter((item) => item !== id))} /><span className="mono">{target.streamKey}</span><em>{target.connectionName}</em></label>; })}</div>
      <footer><span>{t("{count} of 8 streams", { count: selectedTargets.length })}</span><button type="submit" className="primary-button" disabled={busy || !name.trim() || !selectedTargets.length}>{busy ? t("Saving…") : t("Save dashboard")}</button></footer>
    </form> : null}
    <div className="dashboard-selector">{items.map((item) => <button type="button" key={item.id} className={item.id === selectedID ? "active" : ""} onClick={() => setSelectedID(item.id)}>{item.name}{item.shared ? <span>{t("Shared")}</span> : null}</button>)}{canDeleteSelected ? <button type="button" className="dashboard-delete" onClick={() => void remove()} disabled={busy} aria-label={t("Delete dashboard")}><Trash2 size={14} /></button> : null}</div>
    {selected ? <div className="dashboard-comparison">{selected.definition.targets.map((target) => {
      const metric = series[targetID(target)]; const latest = metric?.items.at(-1); const label = targetNames.get(targetID(target));
      return <div className="dashboard-stream-card" key={targetID(target)}><header><strong className="mono">{target.streamKey}</strong><span>{label?.connectionName ?? target.connectionId}</span></header><div><span>{t("Lag")}<strong>{latest?.totalLag?.toLocaleString(locale) ?? "—"}</strong></span><span>{t("Pending")}<strong>{latest?.pending.toLocaleString(locale) ?? "—"}</strong></span><span>{t("Consumed / s")}<strong>{latest?.consumeRate?.toLocaleString(locale, { maximumFractionDigits: 2 }) ?? "—"}</strong></span><span>{t("Latency")}<strong>{latest ? `${latest.redisLatencyMs.toLocaleString(locale, { maximumFractionDigits: 1 })} ms` : "—"}</strong></span></div><DashboardSparkline values={metric?.items.map((point) => point.totalLag ?? 0) ?? []} /></div>;
    })}</div> : <div className="panel-empty">{items.length ? t("Choose a saved dashboard.") : t("Save a dashboard to compare up to eight streams.")}</div>}
  </section>;
}

function targetID(target: DashboardTarget) { return `${target.connectionId}\u0000${target.streamKey}`; }
function parseTargetID(id: string): DashboardTarget { const [connectionId, streamKey] = id.split("\u0000"); return { connectionId, streamKey }; }
function DashboardSparkline({ values }: { values: number[] }) {
  if (values.length < 2) return <div className="dashboard-sparkline" />;
  const width = 240; const height = 48; const max = Math.max(1, ...values); const min = Math.min(...values);
  const points = values.map((value, index) => `${(index / (values.length - 1)) * width},${height - ((value - min) / Math.max(1, max - min)) * (height - 6) - 3}`).join(" ");
  return <svg className="dashboard-sparkline" viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none"><polyline points={points} fill="none" stroke="currentColor" strokeWidth="1.5" /></svg>;
}
