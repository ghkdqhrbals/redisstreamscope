import { FormEvent, type ReactNode, useCallback, useEffect, useState } from "react";
import { CalendarClock, Pencil, Plus, Route, Siren, Trash2, VolumeX, X } from "lucide-react";
import { api } from "../api";
import { useI18n } from "../i18n";
import type { AlertEscalationPolicy, AlertEscalationPolicyInput, AlertMetricDefinition, AlertSelector, AlertSuppression, AlertSuppressionInput, AlertWebhookRoute, AlertWebhookRouteInput, ToastState } from "../types";

type OperationTab = "silences" | "maintenance" | "routes" | "escalations";
type Editing = { kind: OperationTab; item?: AlertSuppression | AlertWebhookRoute | AlertEscalationPolicy } | null;

export function AlertOperationsPanel({ canWrite, metrics, onToast }: { canWrite: boolean; metrics: AlertMetricDefinition[]; onToast: (toast: ToastState) => void }) {
  const { locale, t } = useI18n();
  const [tab, setTab] = useState<OperationTab>("silences");
  const [silences, setSilences] = useState<AlertSuppression[]>([]);
  const [maintenance, setMaintenance] = useState<AlertSuppression[]>([]);
  const [routes, setRoutes] = useState<AlertWebhookRoute[]>([]);
  const [policies, setPolicies] = useState<AlertEscalationPolicy[]>([]);
  const [editing, setEditing] = useState<Editing>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const load = useCallback(async () => {
    setLoading(true); setError("");
    try {
      const [silenceResult, maintenanceResult, routeResult, policyResult] = await Promise.all([api.alertSilences(), api.alertMaintenanceWindows(), api.alertWebhookRoutes(), api.alertEscalationPolicies()]);
      setSilences(silenceResult.items); setMaintenance(maintenanceResult.items); setRoutes(routeResult.items); setPolicies(policyResult.items);
    } catch (cause) { setError(cause instanceof Error ? t(cause.message) : t("Unable to load alert operations.")); }
    finally { setLoading(false); }
  }, [t]);
  useEffect(() => { void load(); }, [load]);

  const remove = async (kind: OperationTab, id: string, name: string) => {
    if (!window.confirm(t("Delete {name}?", { name }))) return;
    try {
      if (kind === "silences") await api.deleteAlertSilence(id);
      if (kind === "maintenance") await api.deleteAlertMaintenanceWindow(id);
      if (kind === "routes") await api.deleteAlertWebhookRoute(id);
      if (kind === "escalations") await api.deleteAlertEscalationPolicy(id);
      await load(); onToast({ kind: "success", title: t("Alert operation deleted"), message: name });
    } catch (cause) { onToast({ kind: "error", title: t("Unable to delete alert operation"), message: cause instanceof Error ? t(cause.message) : "" }); }
  };
  const suppressions = tab === "silences" ? silences : maintenance;

  return <section className="alert-table-panel alert-operations-panel">
    <div className="subtabs alert-operation-tabs">
      <button className={tab === "silences" ? "active" : ""} onClick={() => setTab("silences")}><VolumeX size={14} />{t("Silences")}</button>
      <button className={tab === "maintenance" ? "active" : ""} onClick={() => setTab("maintenance")}><CalendarClock size={14} />{t("Maintenance")}</button>
      <button className={tab === "routes" ? "active" : ""} onClick={() => setTab("routes")}><Route size={14} />{t("Routes")}</button>
      <button className={tab === "escalations" ? "active" : ""} onClick={() => setTab("escalations")}><Siren size={14} />{t("Escalations")}</button>
      {canWrite ? <button className="subtab-create primary-button" onClick={() => setEditing({ kind: tab })}><Plus size={14} />{t("Add")}</button> : null}
    </div>
    {error ? <div className="metric-history-error" role="alert">{error}</div> : null}
    {(tab === "silences" || tab === "maintenance") ? <div className="alert-operation-list">{suppressions.map((item) => <div key={item.id}><OperationState enabled={item.enabled} startsAt={item.startsAt} endsAt={item.endsAt} /><span data-label={t("Name")}><strong>{item.name}</strong><em>{item.reason}</em></span><code data-label={t("Scope")}>{formatOperationScope(item)}</code><span data-label={t("Time range")}>{new Date(item.startsAt).toLocaleString(locale, { hour12: false })}<em>→ {new Date(item.endsAt).toLocaleString(locale, { hour12: false })}</em></span><OperationActions canWrite={canWrite} onEdit={() => setEditing({ kind: tab, item })} onDelete={() => void remove(tab, item.id, item.name)} /></div>)}{!suppressions.length && !loading ? <div className="panel-empty">{t(tab === "silences" ? "No silences." : "No maintenance windows.")}</div> : null}</div> : null}
    {tab === "routes" ? <div className="alert-operation-list">{routes.map((item) => {
      const references = item.referencedByPolicies ?? [];
      const deleteReason = references.length ? t("Used by enabled escalation policies: {names}", { names: references.map((reference) => reference.name).join(", ") }) : "";
      return <div key={item.id}><OperationState enabled={item.enabled} /><span data-label={t("Name")}><strong>{item.name}</strong><em>{item.destinations.length} {t("destinations")}{deleteReason ? ` · ${deleteReason}` : ""}</em></span><code data-label={t("Scope")}>{formatOperationScope(item.selector)}</code><span data-label={t("Destinations")}>{item.destinations.map((destination) => destination.name).join(" · ") || "—"}</span><OperationActions canWrite={canWrite} deleteDisabled={item.deleteBlocked || references.length > 0} deleteReason={deleteReason} onEdit={() => setEditing({ kind: tab, item })} onDelete={() => void remove(tab, item.id, item.name)} /></div>;
    })}{!routes.length && !loading ? <div className="panel-empty">{t("No webhook routes.")}</div> : null}</div> : null}
    {tab === "escalations" ? <div className="alert-operation-list">{policies.map((item) => <div key={item.id}><OperationState enabled={item.enabled} /><span data-label={t("Name")}><strong>{item.name}</strong><em>{item.steps.length} {t("steps")}</em></span><code data-label={t("Scope")}>{formatOperationScope(item.selector)}</code><span data-label={t("Escalation steps")}>{item.steps.map((step) => `${formatDelay(step.afterSeconds)} → ${step.targetSeverity}`).join(" · ")}</span><OperationActions canWrite={canWrite} onEdit={() => setEditing({ kind: tab, item })} onDelete={() => void remove(tab, item.id, item.name)} /></div>)}{!policies.length && !loading ? <div className="panel-empty">{t("No escalation policies.")}</div> : null}</div> : null}
    {editing && (editing.kind === "silences" || editing.kind === "maintenance") ? <SuppressionEditor kind={editing.kind} item={editing.item as AlertSuppression | undefined} metrics={metrics} onClose={() => setEditing(null)} onSaved={load} onToast={onToast} /> : null}
    {editing?.kind === "routes" ? <RouteEditor item={editing.item as AlertWebhookRoute | undefined} metrics={metrics} onClose={() => setEditing(null)} onSaved={load} onToast={onToast} /> : null}
    {editing?.kind === "escalations" ? <EscalationEditor item={editing.item as AlertEscalationPolicy | undefined} metrics={metrics} routes={routes} onClose={() => setEditing(null)} onSaved={load} onToast={onToast} /> : null}
  </section>;
}

function OperationState({ enabled, startsAt, endsAt }: { enabled: boolean; startsAt?: string; endsAt?: string }) {
  const { t } = useI18n(); const now = Date.now(); const active = enabled && (!startsAt || new Date(startsAt).getTime() <= now) && (!endsAt || new Date(endsAt).getTime() > now);
  return <span className={active ? "operation-state active" : "operation-state"} data-label={t("State")}><i />{t(active ? "Active" : enabled ? "Scheduled" : "Disabled")}</span>;
}
function OperationActions({ canWrite, onEdit, onDelete, deleteDisabled = false, deleteReason = "" }: { canWrite: boolean; onEdit: () => void; onDelete: () => void; deleteDisabled?: boolean; deleteReason?: string }) {
  const { t } = useI18n(); if (!canWrite) return <span data-label={t("Actions")}>—</span>;
  return <span className="alert-row-actions" data-label={t("Actions")}><button aria-label={t("Edit")} onClick={onEdit}><Pencil size={14} /></button><button aria-label={deleteReason || t("Delete")} title={deleteReason || undefined} disabled={deleteDisabled} onClick={onDelete}><Trash2 size={14} /></button></span>;
}

function SuppressionEditor({ kind, item, metrics, onClose, onSaved, onToast }: { kind: "silences" | "maintenance"; item?: AlertSuppression; metrics: AlertMetricDefinition[]; onClose: () => void; onSaved: () => Promise<void>; onToast: (toast: ToastState) => void }) {
  const { t } = useI18n(); const now = new Date(); const later = new Date(now.getTime() + 60 * 60 * 1000);
  const [draft, setDraft] = useState<AlertSuppressionInput>(() => item ? { name: item.name, metric: item.metric, severity: item.severity, connectionId: item.connectionId, streamKey: item.streamKey, groupName: item.groupName, labels: item.labels, startsAt: item.startsAt, endsAt: item.endsAt, reason: item.reason, enabled: item.enabled } : { name: "", startsAt: now.toISOString(), endsAt: later.toISOString(), reason: "", enabled: true });
  const [busy, setBusy] = useState(false); const [error, setError] = useState("");
  const submit = async (event: FormEvent) => { event.preventDefault(); setBusy(true); setError(""); try {
    if (kind === "silences") item ? await api.updateAlertSilence(item.id, draft) : await api.createAlertSilence(draft);
    else item ? await api.updateAlertMaintenanceWindow(item.id, draft) : await api.createAlertMaintenanceWindow(draft);
    await onSaved(); onClose(); onToast({ kind: "success", title: t(kind === "silences" ? "Silence saved" : "Maintenance window saved"), message: draft.name });
  } catch (cause) { setError(cause instanceof Error ? t(cause.message) : t("Unable to save alert suppression.")); } finally { setBusy(false); } };
  return <OperationModal title={t(kind === "silences" ? "Silence" : "Maintenance window")} onClose={onClose}><form onSubmit={submit} className="alert-operation-form">
    <label>{t("Name")}<input value={draft.name} onChange={(event) => setDraft({ ...draft, name: event.target.value })} required /></label><label>{t("Reason")}<input value={draft.reason} onChange={(event) => setDraft({ ...draft, reason: event.target.value })} required /></label>
    <label>{t("Starts at")}<input type="datetime-local" value={toLocalDateTime(draft.startsAt)} onChange={(event) => setDraft({ ...draft, startsAt: fromLocalDateTime(event.target.value) })} required /></label><label>{t("Ends at")}<input type="datetime-local" value={toLocalDateTime(draft.endsAt)} onChange={(event) => setDraft({ ...draft, endsAt: fromLocalDateTime(event.target.value) })} required /></label>
    <SelectorFields selector={draft} metrics={metrics} onChange={(next) => setDraft({ ...draft, ...next })} />
    <label className="checkbox-field"><input type="checkbox" checked={draft.enabled} onChange={(event) => setDraft({ ...draft, enabled: event.target.checked })} /><span>{t("Enabled")}</span></label>
    {error ? <div className="login-error operation-wide">{error}</div> : null}<footer className="operation-wide"><button type="button" onClick={onClose}>{t("Cancel")}</button><button className="primary-button" disabled={busy}>{busy ? t("Saving…") : t("Save")}</button></footer>
  </form></OperationModal>;
}

function RouteEditor({ item, metrics, onClose, onSaved, onToast }: { item?: AlertWebhookRoute; metrics: AlertMetricDefinition[]; onClose: () => void; onSaved: () => Promise<void>; onToast: (toast: ToastState) => void }) {
  const { t } = useI18n();
  const [draft, setDraft] = useState<AlertWebhookRouteInput>(() => item ? { name: item.name, selector: item.selector, destinations: item.destinations.map((destination) => ({ id: destination.id, name: destination.name, webhookUrl: "", enabled: destination.enabled })), enabled: item.enabled } : { name: "", selector: {}, destinations: [{ name: "Primary", webhookUrl: "", enabled: true }], enabled: true });
  const [busy, setBusy] = useState(false); const [error, setError] = useState("");
  const submit = async (event: FormEvent) => { event.preventDefault(); setBusy(true); try { item ? await api.updateAlertWebhookRoute(item.id, draft) : await api.createAlertWebhookRoute(draft); await onSaved(); onClose(); onToast({ kind: "success", title: t("Webhook route saved"), message: draft.name }); } catch (cause) { setError(cause instanceof Error ? t(cause.message) : t("Unable to save webhook route.")); } finally { setBusy(false); } };
  return <OperationModal title={t("Webhook route")} onClose={onClose}><form onSubmit={submit} className="alert-operation-form"><label>{t("Name")}<input value={draft.name} onChange={(event) => setDraft({ ...draft, name: event.target.value })} required /></label><span />
    <SelectorFields multiple selector={draft.selector} metrics={metrics} onChange={(selector) => setDraft({ ...draft, selector })} />
    <div className="operation-wide destination-editor"><header><strong>{t("Destinations")}</strong><button type="button" onClick={() => setDraft({ ...draft, destinations: [...draft.destinations, { name: "", webhookUrl: "", enabled: true }] })}><Plus size={13} />{t("Add destination")}</button></header>{draft.destinations.map((destination, index) => <div key={destination.id ?? index}><input value={destination.name} onChange={(event) => setDraft({ ...draft, destinations: draft.destinations.map((current, currentIndex) => currentIndex === index ? { ...current, name: event.target.value } : current) })} placeholder={t("Destination name")} required /><input type="url" value={destination.webhookUrl} onChange={(event) => setDraft({ ...draft, destinations: draft.destinations.map((current, currentIndex) => currentIndex === index ? { ...current, webhookUrl: event.target.value } : current) })} placeholder={destination.id ? t("Leave blank to keep current URL") : "https://…"} required={!destination.id} /><button type="button" onClick={() => setDraft({ ...draft, destinations: draft.destinations.filter((_, currentIndex) => currentIndex !== index) })}><Trash2 size={14} /></button></div>)}</div>
    <label className="checkbox-field"><input type="checkbox" checked={draft.enabled} onChange={(event) => setDraft({ ...draft, enabled: event.target.checked })} /><span>{t("Enabled")}</span></label>{error ? <div className="login-error operation-wide">{error}</div> : null}<footer className="operation-wide"><button type="button" onClick={onClose}>{t("Cancel")}</button><button className="primary-button" disabled={busy}>{busy ? t("Saving…") : t("Save")}</button></footer></form></OperationModal>;
}

function EscalationEditor({ item, metrics, routes, onClose, onSaved, onToast }: { item?: AlertEscalationPolicy; metrics: AlertMetricDefinition[]; routes: AlertWebhookRoute[]; onClose: () => void; onSaved: () => Promise<void>; onToast: (toast: ToastState) => void }) {
  const { t } = useI18n();
  const [draft, setDraft] = useState<AlertEscalationPolicyInput>(() => item ? { name: item.name, selector: item.selector, steps: item.steps.map((step) => ({ id: step.id, afterSeconds: step.afterSeconds, routeId: step.routeId, targetSeverity: step.targetSeverity })), enabled: item.enabled } : { name: "", selector: {}, steps: [{ afterSeconds: 300, routeId: routes[0]?.id ?? "", targetSeverity: "critical" }], enabled: true });
  const [busy, setBusy] = useState(false); const [error, setError] = useState("");
  const submit = async (event: FormEvent) => { event.preventDefault(); setBusy(true); try { item ? await api.updateAlertEscalationPolicy(item.id, draft) : await api.createAlertEscalationPolicy(draft); await onSaved(); onClose(); onToast({ kind: "success", title: t("Escalation policy saved"), message: draft.name }); } catch (cause) { setError(cause instanceof Error ? t(cause.message) : t("Unable to save escalation policy.")); } finally { setBusy(false); } };
  return <OperationModal title={t("Escalation policy")} onClose={onClose}><form onSubmit={submit} className="alert-operation-form"><label>{t("Name")}<input value={draft.name} onChange={(event) => setDraft({ ...draft, name: event.target.value })} required /></label><span />
    <SelectorFields multiple selector={draft.selector} metrics={metrics} onChange={(selector) => setDraft({ ...draft, selector })} />
    <div className="operation-wide escalation-editor"><header><strong>{t("Escalation steps")}</strong><button type="button" onClick={() => setDraft({ ...draft, steps: [...draft.steps, { afterSeconds: (draft.steps.at(-1)?.afterSeconds ?? 0) + 300, routeId: routes[0]?.id ?? "", targetSeverity: "critical" }] })}><Plus size={13} />{t("Add step")}</button></header>{draft.steps.map((step, index) => <div key={step.id ?? index}><label>{t("After seconds")}<input type="number" min={0} value={step.afterSeconds} onChange={(event) => setDraft({ ...draft, steps: draft.steps.map((current, currentIndex) => currentIndex === index ? { ...current, afterSeconds: Number(event.target.value) } : current) })} /></label><label>{t("Route")}<select value={step.routeId} onChange={(event) => setDraft({ ...draft, steps: draft.steps.map((current, currentIndex) => currentIndex === index ? { ...current, routeId: event.target.value } : current) })}>{routes.map((route) => <option value={route.id} key={route.id}>{route.name}</option>)}</select></label><label>{t("Severity")}<select value={step.targetSeverity} onChange={(event) => setDraft({ ...draft, steps: draft.steps.map((current, currentIndex) => currentIndex === index ? { ...current, targetSeverity: event.target.value } : current) })}><option value="info">{t("Info")}</option><option value="warning">{t("Warning")}</option><option value="critical">{t("Critical")}</option></select></label><button type="button" onClick={() => setDraft({ ...draft, steps: draft.steps.filter((_, currentIndex) => currentIndex !== index) })}><Trash2 size={14} /></button></div>)}</div>
    <label className="checkbox-field"><input type="checkbox" checked={draft.enabled} onChange={(event) => setDraft({ ...draft, enabled: event.target.checked })} /><span>{t("Enabled")}</span></label>{error ? <div className="login-error operation-wide">{error}</div> : null}<footer className="operation-wide"><button type="button" onClick={onClose}>{t("Cancel")}</button><button className="primary-button" disabled={busy || !routes.length}>{busy ? t("Saving…") : t("Save")}</button></footer></form></OperationModal>;
}

type SelectorFieldsValue = AlertSelector & { severity?: string; metric?: string };

function SelectorFields({ selector, metrics, onChange, multiple = false }: { selector: SelectorFieldsValue; metrics: AlertMetricDefinition[]; onChange: (next: SelectorFieldsValue) => void; multiple?: boolean }) {
  const { t } = useI18n();
  const patch = (values: Partial<SelectorFieldsValue>) => onChange({ ...selector, ...values });
  const metricOptions = Array.from(new Set(metrics.map((item) => item.name))).map((value) => ({ value, label: value }));
  return <>
    {multiple ? <MultiValueSelector
      label={t("Severity")}
      emptyLabel={t("All severities")}
      values={selector.severities ?? []}
      options={[{ value: "info", label: t("Info") }, { value: "warning", label: t("Warning") }, { value: "critical", label: t("Critical") }]}
      onChange={(severities) => patch({ severities })}
    /> : <label>{t("Severity")}<select value={selector.severity ?? ""} onChange={(event) => patch({ severity: event.target.value })}><option value="">{t("All severities")}</option><option value="info">{t("Info")}</option><option value="warning">{t("Warning")}</option><option value="critical">{t("Critical")}</option></select></label>}
    {multiple ? <MultiValueSelector label={t("Metric")} emptyLabel={t("All metrics")} values={selector.metrics ?? []} options={metricOptions} onChange={(nextMetrics) => patch({ metrics: nextMetrics })} /> : <label>{t("Metric")}<select value={selector.metric ?? ""} onChange={(event) => patch({ metric: event.target.value })}><option value="">{t("All metrics")}</option>{metricOptions.map((item) => <option key={item.value} value={item.value}>{item.label}</option>)}</select></label>}
    <label>{t("Connection ID")}<input value={selector.connectionId ?? ""} onChange={(event) => patch({ connectionId: event.target.value })} placeholder="*" /></label><label>{t("Stream key")}<input value={selector.streamKey ?? ""} onChange={(event) => patch({ streamKey: event.target.value })} placeholder="*" /></label><label>{t("Consumer group")}<input value={selector.groupName ?? ""} onChange={(event) => patch({ groupName: event.target.value })} placeholder="*" /></label>
  </>;
}

function MultiValueSelector({ label, emptyLabel, values, options, onChange }: { label: string; emptyLabel: string; values: string[]; options: Array<{ value: string; label: string }>; onChange: (values: string[]) => void }) {
  const selected = new Set(values);
  const toggle = (value: string) => onChange(selected.has(value) ? values.filter((item) => item !== value) : [...values, value]);
  return <fieldset className="selector-multi-field">
    <legend>{label}</legend>
    <div className="selector-chip-list">
      {options.map((option) => <label className={selected.has(option.value) ? "selector-chip selected" : "selector-chip"} key={option.value}>
        <input type="checkbox" checked={selected.has(option.value)} onChange={() => toggle(option.value)} />
        <span>{option.label}</span>
      </label>)}
    </div>
    {!values.length ? <small>{emptyLabel}</small> : null}
  </fieldset>;
}

function OperationModal({ title, onClose, children }: { title: string; onClose: () => void; children: ReactNode }) { const { t } = useI18n(); return <div className="modal-backdrop" onMouseDown={onClose}><div className="modal alert-operation-modal" onMouseDown={(event) => event.stopPropagation()}><header><h2>{title}</h2><button onClick={onClose} aria-label={t("Close")}><X size={18} /></button></header>{children}</div></div>; }
function formatOperationScope(item: { severity?: string; severities?: string[]; metric?: string; metrics?: string[]; connectionId?: string; streamKey?: string; groupName?: string }) { return [item.severity || item.severities?.join(","), item.metric || item.metrics?.join(","), item.connectionId, item.streamKey, item.groupName].filter(Boolean).join(" / ") || "*"; }
function fromLocalDateTime(value: string) {
  if (!value) return "";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "" : date.toISOString();
}
function toLocalDateTime(value: string) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  const offset = date.getTimezoneOffset() * 60000;
  return new Date(date.getTime() - offset).toISOString().slice(0, 16);
}
function formatDelay(seconds: number) { return seconds < 60 ? `${seconds}s` : seconds < 3600 ? `${Math.round(seconds / 60)}m` : `${Math.round(seconds / 3600)}h`; }
