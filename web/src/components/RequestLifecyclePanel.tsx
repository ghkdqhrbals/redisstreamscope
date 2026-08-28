import { useCallback, useDeferredValue, useEffect, useMemo, useRef, useState } from "react";
import { Activity, CheckCircle2, ChevronLeft, ChevronRight, Clock3, Radio, RefreshCw, Search, Timer, XCircle } from "lucide-react";
import { api } from "../api";
import { useI18n } from "../i18n";
import type { LifecycleMetrics, LifecycleRequest, LifecycleSeriesPoint, StreamMetricSeries } from "../types";
import { MetricTimeSeriesChart, type MetricChartSeries } from "./MetricTimeSeriesChart";
import { ResizableGrid, type ResizableGridColumn } from "./ResizableGrid";
import { Select } from "./Select";

type RequestLifecyclePanelProps = {
  connectionId: string;
  streamKey: string;
  groupName?: string;
};

type LifecycleChartPoint = LifecycleSeriesPoint & { timestamp: string };

const ranges: Record<StreamMetricSeries["range"], { durationMs: number; bucket: string }> = {
  "1m": { durationMs: 60_000, bucket: "1s" },
  "5m": { durationMs: 5 * 60_000, bucket: "1s" },
  "15m": { durationMs: 15 * 60_000, bucket: "5s" },
  "1h": { durationMs: 60 * 60_000, bucket: "1m" },
  "6h": { durationMs: 6 * 60 * 60_000, bucket: "5m" },
  "24h": { durationMs: 24 * 60 * 60_000, bucket: "15m" },
  "7d": { durationMs: 7 * 24 * 60 * 60_000, bucket: "1h" },
};

export function RequestLifecyclePanel({ connectionId, streamKey, groupName = "" }: RequestLifecyclePanelProps) {
  const { locale, t } = useI18n();
  const [range, setRange] = useState<StreamMetricSeries["range"]>("5m");
  const [metrics, setMetrics] = useState<LifecycleMetrics | null>(null);
  const [live, setLive] = useState(true);
  const [loading, setLoading] = useState(false);
  const [metricsError, setMetricsError] = useState("");
  const [requestsError, setRequestsError] = useState("");
  const [requestSearch, setRequestSearch] = useState("");
  const rangeOptions = useMemo(() => [
    { value: "1m", label: t("Last minute") },
    { value: "5m", label: t("Last 5 minutes") },
    { value: "15m", label: t("Last 15 minutes") },
    { value: "1h", label: t("Last hour") },
    { value: "6h", label: t("Last 6 hours") },
    { value: "24h", label: t("Last 24 hours") },
    { value: "7d", label: t("Last 7 days") },
  ], [t]);
  const deferredRequestSearch = useDeferredValue(requestSearch);
  const [requestCursor, setRequestCursor] = useState("");
  const [requestHistory, setRequestHistory] = useState<string[]>([]);
  const [requestPage, setRequestPage] = useState<{ items: LifecycleRequest[]; nextCursor: string | null; hasMore: boolean }>({ items: [], nextCursor: null, hasMore: false });
  const [requestLoading, setRequestLoading] = useState(false);
  const metricsRequestSequence = useRef(0);
  const requestsRequestSequence = useRef(0);

  const load = useCallback(async () => {
    if (!connectionId || !streamKey) {
      setMetrics(null);
      return;
    }
    const requestSequence = ++metricsRequestSequence.current;
    const to = new Date();
    const selected = ranges[range];
    setLoading(true);
    try {
      const response = await api.lifecycleMetrics({
        connectionId,
        streamKey,
        groupName,
        from: new Date(to.getTime() - selected.durationMs).toISOString(),
        to: to.toISOString(),
        bucket: selected.bucket,
      });
      if (requestSequence !== metricsRequestSequence.current) return;
      setMetrics(response);
      setMetricsError("");
    } catch (cause) {
      if (requestSequence !== metricsRequestSequence.current) return;
      setMetricsError(cause instanceof Error ? t(cause.message) : t("Unable to load request lifecycle metrics."));
    } finally {
      if (requestSequence === metricsRequestSequence.current) setLoading(false);
    }
  }, [connectionId, groupName, range, streamKey, t]);

  const loadRequests = useCallback(async () => {
    if (!connectionId || !streamKey) {
      setRequestPage({ items: [], nextCursor: null, hasMore: false });
      return;
    }
    const requestSequence = ++requestsRequestSequence.current;
    setRequestLoading(true);
    try {
      const response = await api.lifecycleRequests({ connectionId, streamKey, groupName, search: deferredRequestSearch, cursor: requestCursor, limit: 50 });
      if (requestSequence !== requestsRequestSequence.current) return;
      setRequestPage(response);
      setRequestsError("");
    } catch (cause) {
      if (requestSequence !== requestsRequestSequence.current) return;
      setRequestsError(cause instanceof Error ? t(cause.message) : t("Unable to load request lifecycles."));
    } finally {
      if (requestSequence === requestsRequestSequence.current) setRequestLoading(false);
    }
  }, [connectionId, deferredRequestSearch, groupName, requestCursor, streamKey, t]);

  useEffect(() => {
    setMetrics(null);
    setMetricsError("");
    setRequestsError("");
    setRequestCursor("");
    setRequestHistory([]);
  }, [connectionId, groupName, streamKey]);

  useEffect(() => {
    setRequestCursor("");
    setRequestHistory([]);
  }, [deferredRequestSearch]);

  useEffect(() => {
    void load();
    if (!live || !connectionId || !streamKey) return;
    const timer = window.setInterval(() => void load(), 2000);
    return () => window.clearInterval(timer);
  }, [connectionId, live, load, streamKey]);

  useEffect(() => {
    void loadRequests();
    if (!live || !connectionId || !streamKey) return;
    const timer = window.setInterval(() => void loadRequests(), 2000);
    return () => window.clearInterval(timer);
  }, [connectionId, live, loadRequests, streamKey]);

  const points = useMemo<LifecycleChartPoint[]>(
    () => (metrics?.points ?? []).map((point) => ({ ...point, timestamp: point.at })),
    [metrics?.points],
  );
  const latencySeries = useMemo<MetricChartSeries<LifecycleChartPoint>[]>(() => [
    { id: "queue", label: t("Queue p95"), description: t("Registered to processing started, p95."), className: "metric-line-primary", value: (point) => point.queueDelay.p95Ms, format: formatMilliseconds },
    { id: "processing", label: t("Processing p95"), description: t("Processing started to processed, p95."), className: "metric-line-secondary", value: (point) => point.processing.p95Ms, format: formatMilliseconds },
    { id: "completion", label: t("Completion p95"), description: t("Registered to processed, p95."), className: "metric-line-group-2", value: (point) => point.completion.p95Ms, format: formatMilliseconds },
    { id: "ack", label: t("ACK p95"), description: t("Processed to acknowledged, p95."), className: "metric-line-tertiary", value: (point) => point.ackDelay.p95Ms, format: formatMilliseconds },
    { id: "end-to-end", label: t("End-to-end p95"), description: t("Registered to acknowledged, p95."), className: "metric-line-group-3", value: (point) => point.endToEnd.p95Ms, format: formatMilliseconds },
  ], [t]);
  const volumeSeries = useMemo<MetricChartSeries<LifecycleChartPoint>[]>(() => [
    { id: "registered", label: t("Registered"), description: t("Requests registered in this time bucket."), className: "metric-line-primary", value: (point) => point.registered, format: formatCount },
    { id: "processed", label: t("Processed"), description: t("Requests processed in this time bucket."), className: "metric-line-secondary", value: (point) => point.processed, format: formatCount },
    { id: "acknowledged", label: t("Acknowledged"), description: t("Requests acknowledged in this time bucket."), className: "metric-line-tertiary", value: (point) => point.acknowledged, format: formatCount },
    { id: "failed", label: t("Failed"), description: t("Failed requests in this time bucket."), className: "metric-line-group-3", value: (point) => point.failed, format: formatCount },
  ], [t]);
  const requestColumns = useMemo<ResizableGridColumn[]>(() => [
    { id: "trace", label: t("Trace ID"), defaultWidth: 190, minWidth: 140 },
    { id: "group", label: t("Consumer group"), defaultWidth: 170, minWidth: 125 },
    { id: "consumer", label: t("Consumer"), defaultWidth: 160, minWidth: 120 },
    { id: "registered", label: t("Registered"), defaultWidth: 185, minWidth: 155 },
    { id: "started", label: t("Processing started"), defaultWidth: 185, minWidth: 155 },
    { id: "processed", label: t("Processed"), defaultWidth: 185, minWidth: 155 },
    { id: "acknowledged", label: t("Acknowledged"), defaultWidth: 185, minWidth: 155 },
    { id: "queue", label: t("Queue"), defaultWidth: 105, minWidth: 82 },
    { id: "processing", label: t("Processing"), defaultWidth: 110, minWidth: 86 },
    { id: "completion", label: t("Completion"), defaultWidth: 110, minWidth: 86 },
    { id: "ack", label: "ACK", defaultWidth: 100, minWidth: 78 },
    { id: "total", label: t("End-to-end"), defaultWidth: 115, minWidth: 90 },
    { id: "outcome", label: t("Outcome"), defaultWidth: 120, minWidth: 92 },
  ], [t]);

  const summary = metrics?.summary;
  return <section className="metric-history-panel lifecycle-panel">
    <header className="metric-history-header">
      <div><h2>{t("Request lifecycle")}</h2><span>{groupName || t("Application-instrumented latency")}</span></div>
      <div className="metric-history-controls">
        <Select value={range} options={rangeOptions} onChange={(next) => setRange(next as StreamMetricSeries["range"])} ariaLabel={t("Time range")} prefix={t("Range")} size="compact" />
        <button type="button" className={live ? "metric-live active" : "metric-live"} aria-pressed={live} onClick={() => setLive((current) => !current)}><Radio size={14} />{t("Live")}</button>
        <button type="button" aria-label={t("Refresh metrics")} disabled={loading || requestLoading || !connectionId || !streamKey} onClick={() => { void load(); void loadRequests(); }}><RefreshCw className={loading || requestLoading ? "spin" : ""} size={14} /></button>
      </div>
    </header>
    {metricsError ? <div className="metric-history-error">{metricsError}</div> : null}
    {summary ? <div className="lifecycle-summary">
      <div><Activity size={15} /><span>{t("Requests")}</span><strong>{summary.requests.toLocaleString(locale)}</strong></div>
      <div><Timer size={15} /><span>{t("Completion p95")}</span><strong>{formatNullableMilliseconds(summary.completion.p95Ms)}</strong></div>
      <div><Clock3 size={15} /><span>{t("In flight")}</span><strong>{summary.inFlight.toLocaleString(locale)}</strong></div>
      <div><CheckCircle2 size={15} /><span>{t("Succeeded")}</span><strong>{summary.succeeded.toLocaleString(locale)}</strong></div>
      <div><XCircle size={15} /><span>{t("Failed")}</span><strong>{summary.failed.toLocaleString(locale)}</strong></div>
    </div> : null}
    {!metrics && loading ? <div className="lifecycle-empty"><Activity size={20} /><strong>{t("Loading…")}</strong></div> : !metrics?.instrumented ? <div className="lifecycle-empty"><Activity size={20} /><strong>{t("No lifecycle telemetry in this range")}</strong><span>{t("Create a telemetry token in Settings and send application lifecycle timestamps.")}</span></div> : <>
      {metrics.truncated ? <div className="metric-history-error">{t("The lifecycle query reached its safety limit; narrow the time range.")}</div> : null}
      <div className="metric-chart-grid lifecycle-chart-grid">
        <MetricTimeSeriesChart title={t("Latency percentiles")} points={points} series={latencySeries} />
        <MetricTimeSeriesChart title={t("Request flow")} points={points} series={volumeSeries} />
      </div>
      <section className="lifecycle-requests">
        <header><div><strong>{t("Request timings")}</strong><span>{t("{count} requests", { count: requestPage.items.length })}</span></div><label><Search size={14} /><input value={requestSearch} onChange={(event) => setRequestSearch(event.target.value)} placeholder={t("Search trace, entry, consumer, or outcome…")} /></label></header>
        {requestsError ? <div className="metric-history-error">{requestsError}</div> : null}
        <ResizableGrid className="lifecycle-request-grid" storageKey="request-lifecycle-timings" columns={requestColumns} headerClassName="lifecycle-request-head">
          {requestPage.items.map((item) => <div className="lifecycle-request-row" key={item.traceId}><code title={item.traceId}>{item.traceId}</code><code title={item.groupName}>{item.groupName || "—"}</code><code title={item.consumer}>{item.consumer || "—"}</code><span>{formatTimestamp(item.registeredAt, locale)}</span><span>{formatTimestamp(item.processingStartedAt, locale)}</span><span>{formatTimestamp(item.processedAt, locale)}</span><span>{formatTimestamp(item.acknowledgedAt, locale)}</span><span>{formatOptionalMilliseconds(item.queueDelayMs)}</span><span>{formatOptionalMilliseconds(item.processingMs)}</span><span>{formatOptionalMilliseconds(item.completionMs)}</span><span>{formatOptionalMilliseconds(item.ackDelayMs)}</span><span>{formatOptionalMilliseconds(item.endToEndMs)}</span><span className={`lifecycle-outcome lifecycle-outcome--${item.outcome || "in-flight"}`} title={item.error}>{item.outcome || t("In flight")}</span></div>)}
          {!requestPage.items.length && !requestLoading ? <div className="panel-empty">{t("No request timings match this view.")}</div> : null}
        </ResizableGrid>
        <footer><span>{requestLoading ? t("Loading…") : t("Newest first")}</span><div><button disabled={!requestHistory.length || requestLoading} onClick={() => { const history = requestHistory.slice(0, -1); setRequestCursor(requestHistory.at(-1) ?? ""); setRequestHistory(history); }}><ChevronLeft size={14} />{t("Previous")}</button><strong>{t("Page {page}", { page: requestHistory.length + 1 })}</strong><button disabled={!requestPage.hasMore || !requestPage.nextCursor || requestLoading} onClick={() => { if (!requestPage.nextCursor) return; setRequestHistory((current) => [...current, requestCursor]); setRequestCursor(requestPage.nextCursor); }}>{t("Next")}<ChevronRight size={14} /></button></div></footer>
      </section>
    </>}
  </section>;
}

function formatMilliseconds(value: number) {
  if (value < 1) return `${value.toFixed(2)} ms`;
  if (value < 1000) return `${value.toFixed(value < 10 ? 1 : 0)} ms`;
  if (value < 60_000) return `${(value / 1000).toFixed(1)} s`;
  return `${(value / 60_000).toFixed(1)} min`;
}

function formatNullableMilliseconds(value: number | null) {
  return value == null ? "—" : formatMilliseconds(value);
}

function formatOptionalMilliseconds(value: number | undefined) {
  return value == null ? "—" : formatMilliseconds(value);
}

function formatTimestamp(value: string | undefined, locale: string) {
  if (!value) return "—";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "—" : date.toLocaleString(locale, {
    year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit",
    fractionalSecondDigits: 3, timeZoneName: "short", hour12: false,
  });
}

function formatCount(value: number) {
  return Math.round(value).toLocaleString();
}
