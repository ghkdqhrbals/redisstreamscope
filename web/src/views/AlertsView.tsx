import { FormEvent, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { AlertCircle, BellRing, Check, CheckCircle2, Pencil, Plus, RefreshCw, Send, Trash2, Webhook, X } from "lucide-react";
import { api } from "../api";
import { AlertOperationsPanel } from "../components/AlertOperationsPanel";
import { ResizableGrid, type ResizableGridColumn } from "../components/ResizableGrid";
import { useI18n } from "../i18n";
import type { AlertIncident, AlertMetricDefinition, AlertRule, AlertRuleInput, AlertSummary, AlertWebhookDelivery, ToastState } from "../types";

type AlertsViewProps = {
  canWrite: boolean;
  onToast: (toast: ToastState) => void;
};

type AlertTab = "incidents" | "rules" | "deliveries" | "operations";

export function AlertsView({ canWrite, onToast }: AlertsViewProps) {
  const { locale, t } = useI18n();
  const [tab, setTab] = useState<AlertTab>("incidents");
  const [status, setStatus] = useState("");
  const [metrics, setMetrics] = useState<AlertMetricDefinition[]>([]);
  const [rules, setRules] = useState<AlertRule[]>([]);
  const [incidents, setIncidents] = useState<AlertIncident[]>([]);
  const [summary, setSummary] = useState<AlertSummary>({ firing: 0, acknowledged: 0, resolved: 0, enabledRules: 0, webhookFailures: 0 });
  const [deliveries, setDeliveries] = useState<AlertWebhookDelivery[]>([]);
  const [editing, setEditing] = useState<AlertRule | "new" | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const loadRequestId = useRef(0);
  const load = useCallback(async () => {
    const requestId = ++loadRequestId.current;
    setLoading(true);
    try {
      const [metricResult, ruleResult, incidentResult, deliveryResult] = await Promise.all([
        api.alertMetrics(), api.alertRules(), api.alertIncidents(status), api.alertWebhookDeliveries(),
      ]);
      if (requestId !== loadRequestId.current) return;
      setMetrics(metricResult.items);
      setRules(ruleResult.items);
      setIncidents(incidentResult.items);
      setSummary(incidentResult.summary);
      setDeliveries(deliveryResult.items);
      setError("");
    } catch (cause) {
      if (requestId !== loadRequestId.current) return;
      setError(cause instanceof Error ? t(cause.message) : t("Unable to load alerts."));
    } finally {
      if (requestId === loadRequestId.current) setLoading(false);
    }
  }, [status, t]);

  useEffect(() => { void load(); }, [load]);
  useEffect(() => () => { loadRequestId.current += 1; }, []);
  useEffect(() => {
    const timer = window.setInterval(() => void load(), 5000);
    return () => window.clearInterval(timer);
  }, [load]);

  const incidentColumns = useMemo<ResizableGridColumn[]>(() => [
    { id: "status", label: t("Status"), defaultWidth: 116, minWidth: 94 },
    { id: "rule", label: t("Rule"), defaultWidth: 190, minWidth: 140 },
    { id: "scope", label: t("Scope"), defaultWidth: 250, minWidth: 160, grow: true },
    { id: "value", label: t("Value"), defaultWidth: 110, minWidth: 84 },
    { id: "started", label: t("Started"), defaultWidth: 178, minWidth: 145 },
    { id: "summary", label: t("Summary"), defaultWidth: 320, minWidth: 200, grow: true },
    { id: "action", label: null, ariaLabel: t("Actions"), defaultWidth: 110, minWidth: 90 },
  ], [t]);
  const ruleColumns = useMemo<ResizableGridColumn[]>(() => [
    { id: "state", label: t("State"), defaultWidth: 110, minWidth: 90 },
    { id: "name", label: t("Rule"), defaultWidth: 200, minWidth: 150, grow: true },
    { id: "metric", label: t("Metric"), defaultWidth: 220, minWidth: 150 },
    { id: "condition", label: t("Condition"), defaultWidth: 145, minWidth: 115 },
    { id: "scope", label: t("Scope"), defaultWidth: 235, minWidth: 160, grow: true },
    { id: "observed", label: t("Last observed"), defaultWidth: 155, minWidth: 120 },
    { id: "webhook", label: t("Webhook"), defaultWidth: 105, minWidth: 82 },
    { id: "enabled", label: t("Enabled"), defaultWidth: 100, minWidth: 82 },
    { id: "actions", label: null, ariaLabel: t("Actions"), defaultWidth: 94, minWidth: 80 },
  ], [t]);
  const deliveryColumns = useMemo<ResizableGridColumn[]>(() => [
    { id: "time", label: t("Time"), defaultWidth: 180, minWidth: 145 },
    { id: "event", label: t("Event"), defaultWidth: 110, minWidth: 85 },
    { id: "incident", label: t("Incident"), defaultWidth: 190, minWidth: 140, grow: true },
    { id: "attempt", label: t("Attempt"), defaultWidth: 90, minWidth: 72 },
    { id: "status", label: "HTTP", defaultWidth: 86, minWidth: 68 },
    { id: "outcome", label: t("Outcome"), defaultWidth: 120, minWidth: 94 },
    { id: "error", label: t("Error"), defaultWidth: 320, minWidth: 180, grow: true },
  ], [t]);

  const acknowledge = async (incident: AlertIncident) => {
    try {
      await api.acknowledgeAlertIncident(incident.id);
      await load();
      onToast({ kind: "success", title: t("Incident acknowledged"), message: incident.ruleName || incident.id });
    } catch (cause) {
      onToast({ kind: "error", title: t("Unable to acknowledge incident"), message: cause instanceof Error ? t(cause.message) : "" });
    }
  };

  const toggleRule = async (rule: AlertRule) => {
    try {
      await api.updateAlertRule(rule.id, { enabled: !rule.enabled });
      await load();
    } catch (cause) {
      onToast({ kind: "error", title: t("Unable to update rule"), message: cause instanceof Error ? t(cause.message) : "" });
    }
  };

  const deleteRule = async (rule: AlertRule) => {
    if (!window.confirm(t("Delete alert rule {name}?", { name: rule.name }))) return;
    try {
      await api.deleteAlertRule(rule.id);
      await load();
      onToast({ kind: "success", title: t("Alert rule deleted"), message: rule.name });
    } catch (cause) {
      onToast({ kind: "error", title: t("Unable to delete rule"), message: cause instanceof Error ? t(cause.message) : "" });
    }
  };

  return <div className="alerts-page">
    <div className="page-header"><div><div className="breadcrumbs">{t("Monitoring")} / {t("Alerts")}</div><h1>{t("Alerts")}</h1></div><div className="header-actions"><button onClick={() => void load()} disabled={loading}><RefreshCw className={loading ? "spin" : ""} size={14} />{t("Refresh")}</button>{canWrite ? <button className="primary-button" onClick={() => setEditing("new")}><Plus size={14} />{t("New rule")}</button> : null}</div></div>
    {error ? <div className="page-error">{error}</div> : null}
    <div className="alert-summary">
      <button onClick={() => { setStatus("firing"); setTab("incidents"); }}><BellRing size={17} /><span>{t("Firing")}</span><strong>{summary.firing}</strong></button>
      <button onClick={() => { setStatus("acknowledged"); setTab("incidents"); }}><CheckCircle2 size={17} /><span>{t("Acknowledged")}</span><strong>{summary.acknowledged}</strong></button>
      <button onClick={() => setTab("rules")}><AlertCircle size={17} /><span>{t("Enabled rules")}</span><strong>{summary.enabledRules}</strong></button>
      <button onClick={() => setTab("deliveries")}><Webhook size={17} /><span>{t("Webhook failures")}</span><strong>{summary.webhookFailures}</strong></button>
    </div>
    <div className="alert-tabs"><button className={tab === "incidents" ? "active" : ""} onClick={() => setTab("incidents")}>{t("Incidents")}</button><button className={tab === "rules" ? "active" : ""} onClick={() => setTab("rules")}>{t("Rules")}</button><button className={tab === "deliveries" ? "active" : ""} onClick={() => setTab("deliveries")}>{t("Webhook deliveries")}</button><button className={tab === "operations" ? "active" : ""} onClick={() => setTab("operations")}>{t("Routing & suppression")}</button></div>

    {tab === "incidents" ? <section className="alert-table-panel">
      <div className="alert-toolbar"><select value={status} onChange={(event) => setStatus(event.target.value)}><option value="">{t("All incidents")}</option><option value="firing">{t("Firing")}</option><option value="acknowledged">{t("Acknowledged")}</option><option value="resolved">{t("Resolved")}</option></select></div>
      <ResizableGrid className="alert-grid" storageKey="alerts-incidents" columns={incidentColumns} headerClassName="alert-grid-head">
        {incidents.map((incident) => <div className="alert-grid-row" key={incident.id}><span><i className={`alert-state alert-state--${incident.status}`} />{t(titleCase(incident.status))}</span><strong>{incident.ruleName || incident.ruleId}</strong><code title={formatScope(incident)}>{formatScope(incident)}</code><span className="mono">{formatAlertValue(incident.lastValue)}</span><span>{formatDate(incident.startedAt, locale)}</span><span title={incident.summary}>{incident.summary}</span><span>{canWrite && incident.status === "firing" ? <button onClick={() => void acknowledge(incident)}><Check size={13} />{t("Acknowledge")}</button> : "—"}</span></div>)}
        {!incidents.length && !loading ? <div className="panel-empty">{t("No incidents.")}</div> : null}
      </ResizableGrid>
    </section> : null}

    {tab === "rules" ? <section className="alert-table-panel"><ResizableGrid className="alert-grid" storageKey="alerts-rules" columns={ruleColumns} headerClassName="alert-grid-head">
      {rules.map((rule) => <div className="alert-grid-row" key={rule.id}><span><i className={`alert-state alert-state--${rule.state}`} />{t(titleCase(rule.state))}</span><strong title={rule.name}>{rule.name}<em>{t(titleCase(rule.severity))}</em></strong><code title={metricName(rule.metric)}>{metricName(rule.metric)}</code><span className="mono">{rule.operator} {formatAlertValue(rule.threshold)} · {formatSeconds(rule.forSeconds)}</span><code title={formatScope(rule)}>{formatScope(rule)}</code><span>{rule.lastValue == null ? "—" : `${formatAlertValue(rule.lastValue)} · ${formatDate(rule.lastObservedAt, locale)}`}</span><span>{rule.webhookConfigured ? t("Configured") : "—"}</span><span>{canWrite ? <button className={rule.enabled ? "status-toggle active" : "status-toggle"} onClick={() => void toggleRule(rule)}>{rule.enabled ? t("On") : t("Off")}</button> : rule.enabled ? t("On") : t("Off")}</span><span className="alert-row-actions">{canWrite ? <><button aria-label={t("Edit rule")} onClick={() => setEditing(rule)}><Pencil size={14} /></button><button aria-label={t("Delete rule")} onClick={() => void deleteRule(rule)}><Trash2 size={14} /></button></> : "—"}</span></div>)}
      {!rules.length && !loading ? <div className="panel-empty">{t("No alert rules.")}</div> : null}
    </ResizableGrid></section> : null}

    {tab === "deliveries" ? <section className="alert-table-panel"><ResizableGrid className="alert-grid" storageKey="alerts-webhooks" columns={deliveryColumns} headerClassName="alert-grid-head">
      {deliveries.map((delivery) => <div className="alert-grid-row" key={delivery.id}><span>{formatDate(delivery.completedAt, locale)}</span><span>{t(titleCase(delivery.event))}</span><code>{delivery.incidentId}</code><span>{delivery.attempt}</span><span>{delivery.statusCode || "—"}</span><span>{t(titleCase(delivery.outcome))}</span><span title={delivery.error}>{delivery.error || "—"}</span></div>)}
      {!deliveries.length && !loading ? <div className="panel-empty">{t("No webhook deliveries.")}</div> : null}
    </ResizableGrid></section> : null}

    {tab === "operations" ? <AlertOperationsPanel canWrite={canWrite} metrics={metrics} onToast={onToast} /> : null}

    {editing ? <RuleEditor rule={editing === "new" ? null : editing} metrics={metrics} onClose={() => setEditing(null)} onSaved={async () => { setEditing(null); await load(); }} onToast={onToast} /> : null}
  </div>;
}

function RuleEditor({ rule, metrics, onClose, onSaved, onToast }: { rule: AlertRule | null; metrics: AlertMetricDefinition[]; onClose: () => void; onSaved: () => Promise<void>; onToast: (toast: ToastState) => void }) {
  const { t } = useI18n();
  const firstMetric = metrics[0];
  const [draft, setDraft] = useState<AlertRuleInput>(() => rule ? {
    name: rule.name, metric: rule.metric, operator: rule.operator, threshold: rule.threshold,
    connectionId: rule.connectionId || "", streamKey: rule.streamKey || "", groupName: rule.groupName || "",
    forSeconds: rule.forSeconds, cooldownSeconds: rule.cooldownSeconds, severity: rule.severity,
    enabled: rule.enabled,
  } : {
    name: "", metric: firstMetric?.name || "consumer_group_lag", operator: firstMetric?.suggestedOperator || ">",
    threshold: firstMetric?.suggestedValue ?? 100, connectionId: "", streamKey: "", groupName: "",
    forSeconds: 30, cooldownSeconds: 300, severity: "warning", enabled: true, webhookUrl: "",
  });
  const [webhookUrl, setWebhookUrl] = useState("");
  const [clearWebhook, setClearWebhook] = useState(false);
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);
  const [error, setError] = useState("");
  const definition = metrics.find((item) => item.name === draft.metric);

  const selectMetric = (metric: string) => {
    const selected = metrics.find((item) => item.name === metric);
    setDraft((current) => ({ ...current, metric, operator: selected?.suggestedOperator || current.operator, threshold: selected?.suggestedValue ?? current.threshold }));
  };
  const submit = async (event: FormEvent) => {
    event.preventDefault();
    setSaving(true);
    setError("");
    try {
      const input: AlertRuleInput = { ...draft };
      if (webhookUrl.trim()) input.webhookUrl = webhookUrl.trim();
      if (clearWebhook) input.webhookUrl = "";
      if (rule) await api.updateAlertRule(rule.id, input);
      else await api.createAlertRule(input);
      await onSaved();
      onToast({ kind: "success", title: rule ? t("Alert rule updated") : t("Alert rule created"), message: draft.name });
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Unable to save alert rule."));
    } finally {
      setSaving(false);
    }
  };
  const testWebhook = async () => {
    if (!webhookUrl.trim()) return;
    setTesting(true);
    try {
      const result = await api.testAlertWebhook(webhookUrl.trim());
      onToast({ kind: "success", title: t("Webhook delivered"), message: `HTTP ${result.statusCode}` });
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Webhook test failed."));
    } finally {
      setTesting(false);
    }
  };

  return <div className="modal-backdrop" onMouseDown={onClose}><form className="modal alert-rule-modal" role="dialog" aria-modal="true" onSubmit={submit} onMouseDown={(event) => event.stopPropagation()}>
    <header><div><h2>{rule ? t("Edit alert rule") : t("New alert rule")}</h2><p>{definition?.description || t("Evaluate monitored Redis Stream and request lifecycle metrics.")}</p></div><button type="button" onClick={onClose} aria-label={t("Close alert rule")}><X size={18} /></button></header>
    <div className="alert-rule-form">
      <label className="wide"><span>{t("Rule name")}</span><input value={draft.name} onChange={(event) => setDraft({ ...draft, name: event.target.value })} maxLength={120} required /></label>
      <label className="wide"><span>{t("Metric")}</span><select value={draft.metric} onChange={(event) => selectMetric(event.target.value)}>{metrics.map((metric) => <option key={metric.name} value={metric.name}>{metricName(metric.name)} · {metric.unit}</option>)}</select></label>
      <label><span>{t("Operator")}</span><select value={draft.operator} onChange={(event) => setDraft({ ...draft, operator: event.target.value })}>{[">", ">=", "<", "<=", "==", "!="].map((item) => <option key={item}>{item}</option>)}</select></label>
      <label><span>{t("Threshold")}</span><input type="number" step="any" value={draft.threshold} onChange={(event) => setDraft({ ...draft, threshold: Number(event.target.value) })} required /></label>
      <label><span>{t("For")}</span><input type="number" min={0} value={draft.forSeconds} onChange={(event) => setDraft({ ...draft, forSeconds: Number(event.target.value) })} /><em>{t("seconds")}</em></label>
      <label><span>{t("Cooldown")}</span><input type="number" min={0} value={draft.cooldownSeconds} onChange={(event) => setDraft({ ...draft, cooldownSeconds: Number(event.target.value) })} /><em>{t("seconds")}</em></label>
      <label><span>{t("Severity")}</span><select value={draft.severity} onChange={(event) => setDraft({ ...draft, severity: event.target.value as AlertRuleInput["severity"] })}><option value="info">{t("Info")}</option><option value="warning">{t("Warning")}</option><option value="critical">{t("Critical")}</option></select></label>
      <label className="checkbox-label"><input type="checkbox" checked={draft.enabled} onChange={(event) => setDraft({ ...draft, enabled: event.target.checked })} /><span>{t("Enabled")}</span></label>
      <fieldset className="wide"><legend>{t("Scope")}</legend><div><label><span>{t("Connection ID")}</span><input value={draft.connectionId} onChange={(event) => setDraft({ ...draft, connectionId: event.target.value })} placeholder={t("All connections")} /></label><label><span>{t("Stream key")}</span><input value={draft.streamKey} onChange={(event) => setDraft({ ...draft, streamKey: event.target.value })} placeholder={t("All streams")} /></label><label><span>{t("Consumer group")}</span><input value={draft.groupName} onChange={(event) => setDraft({ ...draft, groupName: event.target.value })} placeholder={t("All groups")} /></label></div></fieldset>
      <fieldset className="wide"><legend>{t("Webhook")}</legend><div className="webhook-editor"><label><span>{rule?.webhookConfigured ? t("New webhook URL (leave blank to keep current)") : t("Webhook URL")}</span><input type="url" value={webhookUrl} disabled={clearWebhook} onChange={(event) => setWebhookUrl(event.target.value)} placeholder="https://alerts.example.com/hooks/redis" /></label><button type="button" disabled={!webhookUrl.trim() || testing || clearWebhook} onClick={() => void testWebhook()}><Send size={14} />{testing ? t("Testing…") : t("Test")}</button></div>{rule?.webhookConfigured ? <label className="checkbox-label"><input type="checkbox" checked={clearWebhook} onChange={(event) => setClearWebhook(event.target.checked)} /><span>{t("Remove current webhook")}</span></label> : null}</fieldset>
      {error ? <div className="login-error wide" role="alert">{error}</div> : null}
    </div>
    <footer><button type="button" onClick={onClose}>{t("Cancel")}</button><button className="primary-button" disabled={saving}>{saving ? t("Saving…") : t("Save rule")}</button></footer>
  </form></div>;
}

function formatScope(item: { connectionId?: string; streamKey?: string; groupName?: string }) {
  return [item.connectionId || "*", item.streamKey, item.groupName].filter(Boolean).join(" / ");
}

function metricName(metric: string) {
  return metric.split("_").map(titleCase).join(" ");
}

function titleCase(value: string) {
  return value ? value.charAt(0).toUpperCase() + value.slice(1).replaceAll("_", " ") : value;
}

function formatAlertValue(value: number) {
  if (!Number.isFinite(value)) return "—";
  return Math.abs(value) >= 1000 ? value.toLocaleString(undefined, { maximumFractionDigits: 2 }) : value.toLocaleString(undefined, { maximumFractionDigits: 3 });
}

function formatDate(value: string | undefined, locale: string) {
  if (!value) return "—";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "—" : date.toLocaleString(locale, { hour12: false });
}

function formatSeconds(value: number) {
  if (value === 0) return "immediate";
  if (value < 60) return `${value}s`;
  if (value < 3600) return `${Math.round(value / 60)}m`;
  return `${Math.round(value / 3600)}h`;
}
