import { useCallback, useEffect, useState } from "react";
import { Braces, GitBranch, RefreshCw, Search } from "lucide-react";
import { api } from "../api";
import { useI18n } from "../i18n";
import type { StreamSchemaAnalysis, TraceSpan, TraceSummary } from "../types";

export function StreamInsightsPanel({ connectionId, streamKey }: { connectionId: string; streamKey: string }) {
  const { locale, t } = useI18n();
  const [tab, setTab] = useState<"schema" | "traces">("schema");
  const [schema, setSchema] = useState<StreamSchemaAnalysis | null>(null);
  const [traces, setTraces] = useState<TraceSummary[]>([]);
  const [selectedTrace, setSelectedTrace] = useState("");
  const [spans, setSpans] = useState<TraceSpan[]>([]);
  const [search, setSearch] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const loadSchema = useCallback(async () => {
    setLoading(true); setError("");
    try { setSchema(await api.schemaAnalysis(connectionId, streamKey)); }
    catch (cause) { setError(cause instanceof Error ? t(cause.message) : t("Unable to analyze the stream schema.")); }
    finally { setLoading(false); }
  }, [connectionId, streamKey, t]);
  const loadTraces = useCallback(async () => {
    setLoading(true); setError("");
    try {
      const result = await api.traces({ connectionId, streamKey, search, limit: 50 });
      setTraces(result.items);
      if (!result.items.some((item) => item.traceId === selectedTrace)) setSelectedTrace(result.items[0]?.traceId ?? "");
    } catch (cause) { setError(cause instanceof Error ? t(cause.message) : t("Unable to load traces.")); }
    finally { setLoading(false); }
  }, [connectionId, search, selectedTrace, streamKey, t]);

  useEffect(() => {
    setSchema(null); setTraces([]); setSelectedTrace(""); setSpans([]); setError("");
    if (tab === "schema") void loadSchema(); else void loadTraces();
  }, [connectionId, streamKey, tab]);
  useEffect(() => {
    if (!selectedTrace) { setSpans([]); return; }
    void api.trace(selectedTrace).then((result) => setSpans(result.spans)).catch((cause) => setError(cause instanceof Error ? t(cause.message) : t("Unable to load trace details.")));
  }, [selectedTrace, t]);

  return <div className="stream-insights">
    <div className="subtabs insight-tabs">
      <button type="button" className={tab === "schema" ? "active" : ""} onClick={() => setTab("schema")}><Braces size={14} />{t("Schema drift")}</button>
      <button type="button" className={tab === "traces" ? "active" : ""} onClick={() => setTab("traces")}><GitBranch size={14} />{t("Cross-stream traces")}</button>
      <button type="button" className="subtab-refresh" aria-label={t("Refresh")} onClick={() => void (tab === "schema" ? loadSchema() : loadTraces())} disabled={loading}><RefreshCw size={14} /></button>
    </div>
    {error ? <div className="metric-history-error" role="alert">{error}</div> : null}
    {tab === "schema" ? <div className="schema-workspace">
      {schema ? <>
        <div className="insight-kpis">
          <div><span>{t("Fingerprint")}</span><strong className="mono">{schema.fingerprint}</strong></div>
          <div><span>{t("Sampled entries")}</span><strong>{schema.sampleCount.toLocaleString(locale)}</strong></div>
          <div><span>{t("P95 payload size")}</span><strong>{schema.p95SizeBytes.toLocaleString(locale)} B</strong></div>
          <div><span>{t("Schema state")}</span><strong className={schema.drift.detected ? "text-warning" : "text-healthy"}>{schema.drift.detected ? t("Drift detected") : t("Stable")}</strong></div>
        </div>
        {schema.drift.detected ? <div className="schema-drift-summary">
          {schema.drift.added.map((path) => <span className="drift-added mono" key={`a:${path}`}>+ {path}</span>)}
          {schema.drift.removed.map((path) => <span className="drift-removed mono" key={`r:${path}`}>− {path}</span>)}
          {schema.drift.typeChanged.map((path) => <span className="drift-changed mono" key={`t:${path}`}>± {path}</span>)}
        </div> : null}
        <div className="schema-field-table"><div className="schema-field-head"><span>{t("Field path")}</span><span>{t("Types")}</span><span>{t("Presence")}</span></div>
          {schema.fields.map((field) => <div key={field.path}><strong className="mono">{field.path}</strong><span>{field.types.join(" · ")}</span><span>{field.presencePct.toLocaleString(locale)}%</span></div>)}
        </div>
      </> : !loading ? <div className="panel-empty">{t("No schema sample is available.")}</div> : null}
    </div> : null}
    {tab === "traces" ? <div className="trace-workspace">
      <label className="toolbar-search trace-search"><Search size={14} /><input value={search} onChange={(event) => setSearch(event.target.value)} onKeyDown={(event) => { if (event.key === "Enter") void loadTraces(); }} placeholder={t("Search trace, service or entry ID…")} /></label>
      <div className="trace-master-detail">
        <div className="trace-list">{traces.map((trace) => <button type="button" key={trace.traceId} className={selectedTrace === trace.traceId ? "active" : ""} onClick={() => setSelectedTrace(trace.traceId)}>
          <span><strong className="mono">{trace.traceId}</strong><em>{trace.spanCount} {t("spans")} · {trace.streamCount} {t("streams")}</em></span>
          <span><i className={`trace-state trace-${trace.status}`} />{trace.durationMs == null ? "—" : `${trace.durationMs.toLocaleString(locale, { maximumFractionDigits: 1 })} ms`}</span>
        </button>)}{!traces.length && !loading ? <div className="panel-empty">{t("No traces for this stream.")}</div> : null}</div>
        <div className="trace-detail">{spans.map((span, index) => <div className="trace-span" key={span.spanId}>
          <i /><span className="trace-order">{index + 1}</span><div><strong>{span.service || span.streamKey}{span.operation ? ` · ${span.operation}` : ""}</strong><span className="mono">{span.streamKey}{span.entryId ? ` / ${span.entryId}` : ""}</span></div><em className={`trace-status-${span.error || span.outcome === "failed" ? "failed" : span.acknowledgedAt || span.processedAt ? "success" : "pending"}`}>{span.error || span.outcome || t("In flight")}</em>
        </div>)}{selectedTrace && !spans.length ? <div className="panel-empty">{t("Loading trace…")}</div> : null}</div>
      </div>
    </div> : null}
  </div>;
}
