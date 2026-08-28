import { type FormEvent, type RefObject, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { AlertCircle, BellRing, Check, CheckCircle2, Pencil, Plus, RefreshCw, Send, Trash2, Webhook, X } from "lucide-react";
import { api } from "../api";
import { AlertOperationsPanel } from "../components/AlertOperationsPanel";
import { ResizableGrid, type ResizableGridColumn } from "../components/ResizableGrid";
import { Select, type SelectOption } from "../components/Select";
import { SlackAlertPreview } from "../components/SlackAlertPreview";
import { useI18n } from "../i18n";
import type { AlertIncident, AlertMetricDefinition, AlertRule, AlertRuleInput, AlertSummary, AlertWebhookDelivery, ToastState } from "../types";

type AlertsViewProps = {
  canWrite: boolean;
  focusedIncidentId: string;
  onCloseFocusedIncident: () => void;
  onToast: (toast: ToastState) => void;
};

type AlertTab = "incidents" | "rules" | "deliveries" | "operations";

export function AlertsView({ canWrite, focusedIncidentId, onCloseFocusedIncident, onToast }: AlertsViewProps) {
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
  const [linkedIncident, setLinkedIncident] = useState<AlertIncident | null>(null);
  const [linkedIncidentLoading, setLinkedIncidentLoading] = useState(false);
  const [linkedIncidentUnavailable, setLinkedIncidentUnavailable] = useState(false);
  const loadRequestId = useRef(0);
  const linkedIncidentRequestId = useRef(0);
  const evidenceRef = useRef<HTMLElement>(null);
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
  useEffect(() => {
    if (!focusedIncidentId) return;
    setTab("incidents");
    setStatus("");
    const frame = window.requestAnimationFrame(() => evidenceRef.current?.focus());
    return () => window.cancelAnimationFrame(frame);
  }, [focusedIncidentId]);
  useEffect(() => {
    const requestId = ++linkedIncidentRequestId.current;
    if (!focusedIncidentId) {
      setLinkedIncident(null);
      setLinkedIncidentLoading(false);
      setLinkedIncidentUnavailable(false);
      return;
    }
    setLinkedIncident(null);
    setLinkedIncidentLoading(true);
    setLinkedIncidentUnavailable(false);
    api.alertIncident(focusedIncidentId)
      .then((response) => {
        if (requestId !== linkedIncidentRequestId.current) return;
        setLinkedIncident(response.item);
      })
      .catch(() => {
        if (requestId !== linkedIncidentRequestId.current) return;
        setLinkedIncidentUnavailable(true);
      })
      .finally(() => {
        if (requestId === linkedIncidentRequestId.current) setLinkedIncidentLoading(false);
      });
    return () => { linkedIncidentRequestId.current += 1; };
  }, [focusedIncidentId]);

  const listedFocusedIncident = focusedIncidentId ? incidents.find((incident) => incident.id === focusedIncidentId) : undefined;
  const focusedIncident = linkedIncident?.id === focusedIncidentId ? linkedIncident : listedFocusedIncident;
  const focusedRule = focusedIncident ? rules.find((rule) => rule.id === focusedIncident.ruleId) : undefined;
  const displayedIncidents = focusedIncident && !incidents.some((incident) => incident.id === focusedIncident.id) ? [focusedIncident, ...incidents] : incidents;

  const incidentColumns = useMemo<ResizableGridColumn[]>(() => [
    { id: "status", label: t("Status"), defaultWidth: 100, minWidth: 90 },
    { id: "rule", label: t("Rule"), defaultWidth: 155, minWidth: 120 },
    { id: "scope", label: t("Scope"), defaultWidth: 180, minWidth: 140, grow: true },
    { id: "value", label: t("Value"), defaultWidth: 90, minWidth: 80 },
    { id: "started", label: t("Started"), defaultWidth: 145, minWidth: 125 },
    { id: "summary", label: t("Summary"), defaultWidth: 225, minWidth: 170, grow: true },
    { id: "action", label: null, ariaLabel: t("Actions"), defaultWidth: 96, minWidth: 86 },
  ], [t]);
  const ruleColumns = useMemo<ResizableGridColumn[]>(() => [
    { id: "state", label: t("State"), defaultWidth: 110, minWidth: 90 },
    { id: "name", label: t("Rule"), defaultWidth: 200, minWidth: 150, grow: true },
    { id: "metric", label: t("Metric"), defaultWidth: 220, minWidth: 150 },
    { id: "condition", label: t("Condition"), defaultWidth: 145, minWidth: 115 },
    { id: "scope", label: t("Scope"), defaultWidth: 235, minWidth: 160, grow: true },
    { id: "observed", label: t("Last observed"), defaultWidth: 155, minWidth: 120 },
    { id: "webhook", label: t("Delivery"), defaultWidth: 105, minWidth: 82 },
    { id: "enabled", label: t("Enabled"), defaultWidth: 100, minWidth: 82 },
    { id: "actions", label: null, ariaLabel: t("Actions"), defaultWidth: 94, minWidth: 80 },
  ], [t]);
  const deliveryColumns = useMemo<ResizableGridColumn[]>(() => [
    { id: "time", label: t("Time"), defaultWidth: 155, minWidth: 135 },
    { id: "event", label: t("Event"), defaultWidth: 90, minWidth: 80 },
    { id: "incident", label: t("Incident"), defaultWidth: 155, minWidth: 130, grow: true },
    { id: "attempt", label: t("Attempt"), defaultWidth: 78, minWidth: 70 },
    { id: "status", label: "HTTP", defaultWidth: 74, minWidth: 68 },
    { id: "outcome", label: t("Outcome"), defaultWidth: 105, minWidth: 90 },
    { id: "error", label: t("Error"), defaultWidth: 230, minWidth: 170, grow: true },
  ], [t]);
  const incidentStatusOptions = useMemo<SelectOption[]>(() => [
    {
      value: "",
      label: t("All incidents"),
      description: t("Show incidents in every state."),
      meta: (summary.firing + summary.acknowledged + summary.resolved).toLocaleString(locale),
      tone: "neutral",
    },
    {
      value: "firing",
      label: t("Firing"),
      description: t("Active incidents that have not resolved."),
      meta: summary.firing.toLocaleString(locale),
      tone: "danger",
    },
    {
      value: "acknowledged",
      label: t("Acknowledged"),
      description: t("Reviewed incidents that remain open."),
      meta: summary.acknowledged.toLocaleString(locale),
      tone: "warning",
    },
    {
      value: "resolved",
      label: t("Resolved"),
      description: t("Incidents that returned to a healthy state."),
      meta: summary.resolved.toLocaleString(locale),
      tone: "success",
    },
  ], [locale, summary.acknowledged, summary.firing, summary.resolved, t]);

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
      <button className={tab === "incidents" && status === "firing" ? "active" : ""} aria-pressed={tab === "incidents" && status === "firing"} onClick={() => { setStatus("firing"); setTab("incidents"); }}><BellRing size={17} /><span>{t("Firing")}</span><strong>{summary.firing}</strong></button>
      <button className={tab === "incidents" && status === "acknowledged" ? "active" : ""} aria-pressed={tab === "incidents" && status === "acknowledged"} onClick={() => { setStatus("acknowledged"); setTab("incidents"); }}><CheckCircle2 size={17} /><span>{t("Acknowledged")}</span><strong>{summary.acknowledged}</strong></button>
      <button className={tab === "rules" ? "active" : ""} aria-pressed={tab === "rules"} onClick={() => setTab("rules")}><AlertCircle size={17} /><span>{t("Enabled rules")}</span><strong>{summary.enabledRules}</strong></button>
      <button className={tab === "deliveries" ? "active" : ""} aria-pressed={tab === "deliveries"} onClick={() => setTab("deliveries")}><Webhook size={17} /><span>{t("Delivery failures")}</span><strong>{summary.webhookFailures}</strong></button>
    </div>
    <div className="alert-tabs" role="tablist" aria-label={t("Alerts")}><button type="button" role="tab" aria-selected={tab === "incidents"} className={tab === "incidents" ? "active" : ""} onClick={() => setTab("incidents")}>{t("Incidents")}</button><button type="button" role="tab" aria-selected={tab === "rules"} className={tab === "rules" ? "active" : ""} onClick={() => setTab("rules")}>{t("Rules")}</button><button type="button" role="tab" aria-selected={tab === "deliveries"} className={tab === "deliveries" ? "active" : ""} onClick={() => setTab("deliveries")}>{t("Delivery history")}</button><button type="button" role="tab" aria-selected={tab === "operations"} className={tab === "operations" ? "active" : ""} onClick={() => setTab("operations")}>{t("Routing & suppression")}</button></div>

    {focusedIncidentId ? <AlertEvidencePanel focusRef={evidenceRef} incidentId={focusedIncidentId} incident={focusedIncident} rule={focusedRule} loading={linkedIncidentLoading || (loading && !focusedIncident)} unavailable={linkedIncidentUnavailable && !focusedIncident} locale={locale} onClose={onCloseFocusedIncident} /> : null}

    {tab === "incidents" ? <section className="alert-table-panel">
      <div className="alert-toolbar"><Select value={status} options={incidentStatusOptions} onChange={setStatus} ariaLabel={`${t("Incidents")} · ${t("Status")}`} prefix={t("Status")} size="compact" /></div>
      <ResizableGrid className="alert-grid alert-grid--incidents" storageKey="alerts-incidents-v2" columns={incidentColumns} headerClassName="alert-grid-head">
        {displayedIncidents.map((incident) => <div className={`alert-grid-row${incident.id === focusedIncidentId ? " alert-grid-row--focused" : ""}`} aria-current={incident.id === focusedIncidentId ? "true" : undefined} key={incident.id}><span data-label={t("Status")}><i className={`alert-state alert-state--${incident.status}`} />{t(titleCase(incident.status))}</span><strong data-label={t("Rule")}>{incident.ruleName || incident.ruleId}</strong><code data-label={t("Scope")} title={formatScope(incident)}>{formatScope(incident)}</code><span className="mono" data-label={t("Value")}>{formatAlertValue(incident.lastValue)}</span><span data-label={t("Started")}>{formatDate(incident.startedAt, locale)}</span><span data-label={t("Summary")} title={incident.summary}>{incident.summary}</span><span data-label={t("Actions")}>{canWrite && incident.status === "firing" ? <button onClick={() => void acknowledge(incident)}><Check size={13} />{t("Acknowledge")}</button> : "—"}</span></div>)}
        {!displayedIncidents.length && !loading ? <div className="panel-empty">{t("No incidents.")}</div> : null}
      </ResizableGrid>
    </section> : null}

    {tab === "rules" ? <section className="alert-table-panel"><ResizableGrid className="alert-grid alert-grid--rules" storageKey="alerts-rules" columns={ruleColumns} headerClassName="alert-grid-head">
      {rules.map((rule) => <div className="alert-grid-row" key={rule.id}><span data-label={t("State")}><i className={`alert-state alert-state--${rule.state}`} />{t(titleCase(rule.state))}</span><strong data-label={t("Rule")} title={rule.name}>{rule.name}<em>{t(titleCase(rule.severity))}</em></strong><code data-label={t("Metric")} title={metricName(rule.metric)}>{metricName(rule.metric)}</code><span className="mono" data-label={t("Condition")}>{rule.operator} {formatAlertValue(rule.threshold)} · {formatSeconds(rule.forSeconds)}</span><code data-label={t("Scope")} title={formatScope(rule)}>{formatScope(rule)}</code><span data-label={t("Last observed")}>{rule.lastValue == null ? "—" : `${formatAlertValue(rule.lastValue)} · ${formatDate(rule.lastObservedAt, locale)}`}</span><span data-label={t("Delivery")}>{rule.webhookConfigured ? t(rule.webhookFormat === "slack" ? "Slack" : "JSON webhook") : "—"}</span><span data-label={t("Enabled")}>{canWrite ? <button className={rule.enabled ? "status-toggle active" : "status-toggle"} onClick={() => void toggleRule(rule)}>{rule.enabled ? t("On") : t("Off")}</button> : rule.enabled ? t("On") : t("Off")}</span><span className="alert-row-actions" data-label={t("Actions")}>{canWrite ? <><button aria-label={t("Edit rule")} onClick={() => setEditing(rule)}><Pencil size={14} /></button><button aria-label={t("Delete rule")} onClick={() => void deleteRule(rule)}><Trash2 size={14} /></button></> : "—"}</span></div>)}
      {!rules.length && !loading ? <div className="panel-empty">{t("No alert rules.")}</div> : null}
    </ResizableGrid></section> : null}

    {tab === "deliveries" ? <section className="alert-table-panel"><ResizableGrid className="alert-grid alert-grid--deliveries" storageKey="alerts-webhooks-v2" columns={deliveryColumns} headerClassName="alert-grid-head">
      {deliveries.map((delivery) => <div className="alert-grid-row" key={delivery.id}><span data-label={t("Time")}>{formatDate(delivery.completedAt, locale)}</span><span data-label={t("Event")}>{t(titleCase(delivery.event))}</span><code data-label={t("Incident")}>{delivery.incidentId}</code><span data-label={t("Attempt")}>{delivery.attempt}</span><span data-label="HTTP">{delivery.statusCode || "—"}</span><span data-label={t("Outcome")}>{t(titleCase(delivery.outcome))}</span><span data-label={t("Error")} title={delivery.error}>{delivery.error || "—"}</span></div>)}
      {!deliveries.length && !loading ? <div className="panel-empty">{t("No deliveries.")}</div> : null}
    </ResizableGrid></section> : null}

    {tab === "operations" ? <AlertOperationsPanel canWrite={canWrite} metrics={metrics} onToast={onToast} /> : null}

    {editing ? <RuleEditor rule={editing === "new" ? null : editing} metrics={metrics} onClose={() => setEditing(null)} onSaved={async () => { setEditing(null); await load(); }} onToast={onToast} /> : null}
  </div>;
}

function AlertEvidencePanel({ focusRef, incidentId, incident, rule, loading, unavailable, locale, onClose }: { focusRef: RefObject<HTMLElement | null>; incidentId: string; incident?: AlertIncident; rule?: AlertRule; loading: boolean; unavailable: boolean; locale: string; onClose: () => void }) {
  const { t } = useI18n();
  const titleId = `alert-evidence-${incidentId.replaceAll(/[^a-zA-Z0-9_-]/g, "-")}`;

  return <section className="alert-evidence" aria-labelledby={titleId} ref={focusRef} tabIndex={-1}>
    <header>
      <div><span>{t("Alert evidence")}</span><h2 id={titleId}>{incident?.id || incidentId}</h2></div>
      <button type="button" onClick={onClose} aria-label={t("Close alert evidence")}><X size={17} /></button>
    </header>
    {incident ? <>
      <p className="alert-evidence-summary"><strong>{t("Recorded trigger")}</strong><span>{incident.summary}</span></p>
      <dl className="alert-evidence-grid">
        <EvidenceField label={t("Incident status")} value={titleCase(incident.status)} tone={incident.status} />
        <EvidenceField label={t("Current rule")} value={rule?.name || incident.ruleName || incident.ruleId} />
        <EvidenceField label={t("Current rule severity")} value={titleCase(incident.severity || rule?.severity || "unknown")} tone={incident.severity || rule?.severity} />
        <EvidenceField label={t("Current rule metric")} value={metricName(incident.metric || rule?.metric || "unknown")} code />
        <EvidenceField label={t("Current rule definition")} value={rule ? `${rule.operator} ${formatAlertValue(rule.threshold)} · ${formatSeconds(rule.forSeconds)}` : t("Rule definition unavailable")} code={Boolean(rule)} />
        <EvidenceField label={t("Trigger value")} value={formatAlertValue(incident.triggerValue)} code />
        <EvidenceField label={t("Current value")} value={formatAlertValue(incident.lastValue)} code />
        <EvidenceField label={t("Scope")} value={formatScope(incident)} code wide />
        <EvidenceField label={t("Started")} value={formatDate(incident.startedAt, locale)} />
        <EvidenceField label={t("Last updated")} value={formatDate(incident.updatedAt, locale)} />
        {incident.acknowledgedAt ? <EvidenceField label={t("Acknowledged")} value={`${formatDate(incident.acknowledgedAt, locale)}${incident.acknowledgedBy ? ` · ${incident.acknowledgedBy}` : ""}`} /> : null}
        {incident.resolvedAt ? <EvidenceField label={t("Resolved")} value={formatDate(incident.resolvedAt, locale)} /> : null}
        <EvidenceField label={t("Incident ID")} value={incident.id} code wide />
      </dl>
    </> : <div className="alert-evidence-empty" role="status">
      <strong>{loading ? t("Loading alert evidence…") : t("Alert evidence unavailable")}</strong>
      <span>{loading ? t("Retrieving the incident and matching rule definition.") : unavailable ? t("This incident was not found or is outside your access scope.") : t("No evidence is available for this incident.")}</span>
      <code>{incidentId}</code>
    </div>}
  </section>;
}

function EvidenceField({ label, value, code = false, tone, wide = false }: { label: string; value: string; code?: boolean; tone?: string; wide?: boolean }) {
  return <div className={wide ? "alert-evidence-field alert-evidence-field--wide" : "alert-evidence-field"}>
    <dt>{label}</dt>
    <dd>{tone ? <span className={`alert-evidence-state alert-evidence-state--${tone}`}>{value}</span> : code ? <code>{value}</code> : value}</dd>
  </div>;
}

function RuleEditor({ rule, metrics, onClose, onSaved, onToast }: { rule: AlertRule | null; metrics: AlertMetricDefinition[]; onClose: () => void; onSaved: () => Promise<void>; onToast: (toast: ToastState) => void }) {
  const { t } = useI18n();
  const firstMetric = metrics[0];
  const [draft, setDraft] = useState<AlertRuleInput>(() => rule ? {
    name: rule.name, metric: rule.metric, operator: rule.operator, threshold: rule.threshold,
    connectionId: rule.connectionId || "", streamKey: rule.streamKey || "", groupName: rule.groupName || "",
    forSeconds: rule.forSeconds, cooldownSeconds: rule.cooldownSeconds, severity: rule.severity,
    enabled: rule.enabled, webhookFormat: rule.webhookFormat || "webhook",
  } : {
    name: "", metric: firstMetric?.name || "consumer_group_lag", operator: firstMetric?.suggestedOperator || ">",
    threshold: firstMetric?.suggestedValue ?? 100, connectionId: "", streamKey: "", groupName: "",
    forSeconds: 30, cooldownSeconds: 300, severity: "warning", enabled: true, webhookUrl: "", webhookFormat: "webhook",
  });
  const [webhookUrl, setWebhookUrl] = useState("");
  const [clearWebhook, setClearWebhook] = useState(false);
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);
  const [error, setError] = useState("");
  const definition = metrics.find((item) => item.name === draft.metric);
  const metricOptions = useMemo<SelectOption[]>(() => metrics.map((metric) => ({
    value: metric.name,
    label: metricName(metric.name),
    description: metric.description,
    meta: metric.unit,
    keywords: `${metric.name} ${metric.scope.join(" ")}`,
  })), [metrics]);
  const operatorOptions = useMemo<SelectOption[]>(() => [
    { value: ">", label: t("Above (>)"), description: t("Trigger when the value is greater than the threshold.") },
    { value: ">=", label: t("At least (>=)"), description: t("Trigger when the value reaches or exceeds the threshold.") },
    { value: "<", label: t("Below (<)"), description: t("Trigger when the value is lower than the threshold.") },
    { value: "<=", label: t("At most (<=)"), description: t("Trigger when the value reaches or falls below the threshold.") },
    { value: "==", label: t("Equal to (==)"), description: t("Trigger when the value exactly matches the threshold.") },
    { value: "!=", label: t("Not equal to (!=)"), description: t("Trigger when the value differs from the threshold.") },
  ], [t]);
  const severityOptions = useMemo<SelectOption[]>(() => [
    { value: "info", label: t("Info"), tone: "info" },
    { value: "warning", label: t("Warning"), tone: "warning" },
    { value: "critical", label: t("Critical"), tone: "danger" },
  ], [t]);
  const webhookFormat = draft.webhookFormat ?? "webhook";
  const webhookFormatChanged = Boolean(rule?.webhookConfigured && webhookFormat !== (rule.webhookFormat || "webhook"));

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
      const result = await api.testAlertWebhook(webhookUrl.trim(), webhookFormat);
      onToast({ kind: "success", title: t(webhookFormat === "slack" ? "Slack test delivered" : "Webhook delivered"), message: `HTTP ${result.statusCode}` });
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
      <label className="wide"><span>{t("Metric")}</span><Select className="select-control--block" value={draft.metric} options={metricOptions} onChange={selectMetric} ariaLabel={t("Metric")} searchable searchPlaceholder={t("Search metrics…")} /></label>
      <label><span>{t("Operator")}</span><Select className="select-control--block" value={draft.operator} options={operatorOptions} onChange={(operator) => setDraft({ ...draft, operator })} ariaLabel={t("Operator")} /></label>
      <label><span>{t("Threshold")}</span><input type="number" step="any" value={draft.threshold} onChange={(event) => setDraft({ ...draft, threshold: Number(event.target.value) })} required /></label>
      <label><span>{t("For")}</span><input type="number" min={0} value={draft.forSeconds} onChange={(event) => setDraft({ ...draft, forSeconds: Number(event.target.value) })} /><em>{t("seconds")}</em></label>
      <label><span>{t("Cooldown")}</span><input type="number" min={0} value={draft.cooldownSeconds} onChange={(event) => setDraft({ ...draft, cooldownSeconds: Number(event.target.value) })} /><em>{t("seconds")}</em></label>
      <label><span>{t("Severity")}</span><Select className="select-control--block" value={draft.severity} options={severityOptions} onChange={(severity) => setDraft({ ...draft, severity: severity as AlertRuleInput["severity"] })} ariaLabel={t("Severity")} /></label>
      <label className="checkbox-label"><input type="checkbox" checked={draft.enabled} onChange={(event) => setDraft({ ...draft, enabled: event.target.checked })} /><span>{t("Enabled")}</span></label>
      <fieldset className="wide"><legend>{t("Scope")}</legend><div><label><span>{t("Connection ID")}</span><input value={draft.connectionId} onChange={(event) => setDraft({ ...draft, connectionId: event.target.value })} placeholder={t("All connections")} /></label><label><span>{t("Stream key")}</span><input value={draft.streamKey} onChange={(event) => setDraft({ ...draft, streamKey: event.target.value })} placeholder={t("All streams")} /></label><label><span>{t("Consumer group")}</span><input value={draft.groupName} onChange={(event) => setDraft({ ...draft, groupName: event.target.value })} placeholder={t("All groups")} /></label></div></fieldset>
      <fieldset className="wide delivery-fieldset"><legend>{t("Notification delivery")}</legend><div className="delivery-editor">
        <div className="delivery-format-picker" role="radiogroup" aria-label={t("Delivery format")}>
          <button type="button" role="radio" aria-checked={webhookFormat === "webhook"} className={webhookFormat === "webhook" ? "active" : ""} onClick={() => setDraft({ ...draft, webhookFormat: "webhook" })}><Webhook size={17} /><span><strong>{t("JSON webhook")}</strong><small>{t("HTTP POST with the complete alert event.")}</small></span></button>
          <button type="button" role="radio" aria-checked={webhookFormat === "slack"} className={webhookFormat === "slack" ? "active" : ""} onClick={() => setDraft({ ...draft, webhookFormat: "slack" })}><BellRing size={17} /><span><strong>{t("Slack")}</strong><small>{t("A channel-ready Incoming Webhook message.")}</small></span></button>
        </div>
        <div className="delivery-contract" role="note">
          <strong>{t(webhookFormat === "slack" ? "How Slack receives it" : "How the webhook receives it")}</strong>
          {webhookFormat === "slack" ? <p>{t("Create an Incoming Webhook for the target Slack channel, paste its URL, then send a test. Messages include the event, severity, rule, metric value, scope, and incident ID. When a public URL is configured, they also link to the incident evidence.")}</p> : <p>{t("We POST JSON for firing, repeat, resolved, and escalation events. The body includes event, sentAt, rule, incident, observation, and routing metadata.")}</p>}
          <code>{webhookFormat === "slack" ? "POST · application/json · text + blocks" : "POST · application/json"}</code>
          <small>{t("Public HTTP(S) URLs only. Failed deliveries are retried up to 3 times. Destination URLs are hidden after save.")}</small>
        </div>
        {webhookFormat === "slack" ? <SlackAlertPreview /> : null}
        <div className="webhook-editor"><label><span>{rule?.webhookConfigured && !webhookFormatChanged ? t("New destination URL (leave blank to keep current)") : t(webhookFormat === "slack" ? "Slack incoming webhook URL (optional)" : "Webhook URL (optional)")}</span><input type="url" value={webhookUrl} disabled={clearWebhook} onChange={(event) => setWebhookUrl(event.target.value)} placeholder={webhookFormat === "slack" ? "https://hooks.slack.com/services/…" : "https://alerts.example.com/hooks/redis"} required={!clearWebhook && webhookFormatChanged} /></label><button type="button" disabled={!webhookUrl.trim() || testing || clearWebhook} onClick={() => void testWebhook()}><Send size={14} />{testing ? t("Testing…") : t("Send test")}</button></div>
        {webhookFormatChanged && !clearWebhook ? <p className="delivery-format-warning">{t("Enter a new destination URL when changing the delivery format.")}</p> : null}
        {rule?.webhookConfigured ? <label className="checkbox-label"><input type="checkbox" checked={clearWebhook} onChange={(event) => setClearWebhook(event.target.checked)} /><span>{t("Remove current destination")}</span></label> : null}
      </div></fieldset>
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
