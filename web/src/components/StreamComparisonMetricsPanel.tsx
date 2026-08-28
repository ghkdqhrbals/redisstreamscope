import { ReactNode, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Radio, RefreshCw } from "lucide-react";
import { api } from "../api";
import { useI18n } from "../i18n";
import type { StreamComparisonMetricPoint, StreamComparisonMetricSeries, StreamMetricSeries } from "../types";
import { MetricTimeSeriesChart, type MetricChartSeries } from "./MetricTimeSeriesChart";
import { Select } from "./Select";

type ComparisonStream = {
  key: string;
  length: number;
  pending: number;
  totalLag: number;
};

type StreamComparisonMetricsPanelProps = {
  connectionId: string;
  streams: ComparisonStream[];
  leadingControls?: ReactNode;
};

const maximumVisibleStreams = 8;
const initialVisibleStreams = 6;
const lineClassCount = 8;

export function StreamComparisonMetricsPanel({ connectionId, streams, leadingControls }: StreamComparisonMetricsPanelProps) {
  const { locale, t } = useI18n();
  const [range, setRange] = useState<StreamMetricSeries["range"]>("5m");
  const [selectedKeys, setSelectedKeys] = useState<string[]>([]);
  const [metrics, setMetrics] = useState<StreamComparisonMetricSeries | null>(null);
  const [live, setLive] = useState(true);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const requestSequence = useRef(0);
  const inFlightSignature = useRef("");
  const rangeOptions = useMemo(() => [
    { value: "1m", label: t("Last minute") },
    { value: "5m", label: t("Last 5 minutes") },
    { value: "15m", label: t("Last 15 minutes") },
    { value: "1h", label: t("Last hour") },
    { value: "6h", label: t("Last 6 hours") },
    { value: "24h", label: t("Last 24 hours") },
    { value: "7d", label: t("Last 7 days") },
  ], [t]);

  const rankedKeys = useMemo(() => [...streams]
    .sort((left, right) => right.totalLag - left.totalLag
      || right.pending - left.pending
      || right.length - left.length
      || left.key.localeCompare(right.key))
    .map((stream) => stream.key), [streams]);
  const rankedKeySignature = rankedKeys.join("\u0000");

  useEffect(() => {
    setSelectedKeys((current) => {
      const available = new Set(rankedKeys);
      const retained = current.filter((key) => available.has(key)).slice(0, maximumVisibleStreams);
      return retained.length ? retained : rankedKeys.slice(0, initialVisibleStreams);
    });
  }, [connectionId, rankedKeySignature]);

  const selectedSignature = selectedKeys.join("\u0000");
  const load = useCallback(async () => {
    if (!connectionId || !selectedKeys.length) {
      requestSequence.current += 1;
      inFlightSignature.current = "";
      setMetrics(null);
      setLoading(false);
      return;
    }
    const signature = `${connectionId}\u0000${range}\u0000${selectedSignature}`;
    if (inFlightSignature.current === signature) return;
    const sequence = ++requestSequence.current;
    inFlightSignature.current = signature;
    setLoading(true);
    try {
      const result = await api.streamComparisonMetrics(connectionId, range, selectedKeys);
      if (sequence !== requestSequence.current) return;
      setMetrics(result);
      setError("");
    } catch (cause) {
      if (sequence !== requestSequence.current) return;
      setError(cause instanceof Error ? t(cause.message) : t("Unable to load stream comparison metrics."));
    } finally {
      if (inFlightSignature.current === signature) inFlightSignature.current = "";
      if (sequence === requestSequence.current) setLoading(false);
    }
  }, [connectionId, range, selectedKeys, selectedSignature, t]);

  useEffect(() => {
    requestSequence.current += 1;
    inFlightSignature.current = "";
    setMetrics(null);
    setError("");
  }, [connectionId, range, selectedSignature]);

  useEffect(() => () => {
    requestSequence.current += 1;
    inFlightSignature.current = "";
  }, []);

  useEffect(() => {
    void load();
    if (!live || !connectionId || !selectedKeys.length) return;
    const timer = window.setInterval(() => void load(), 1000);
    return () => window.clearInterval(timer);
  }, [connectionId, live, load, selectedKeys.length]);

  const stableClassByKey = useMemo(() => new Map(
    [...rankedKeys].sort((left, right) => left.localeCompare(right)).map((key, index) => [key, `metric-line-group-${index % lineClassCount}`]),
  ), [rankedKeySignature]);
  const makeSeries = useCallback((
    description: string,
    value: (point: StreamComparisonMetricPoint, streamKey: string) => number | null,
    format: (value: number) => string,
  ): MetricChartSeries<StreamComparisonMetricPoint>[] => selectedKeys.map((streamKey) => ({
    id: streamKey,
    label: streamKey,
    description,
    className: stableClassByKey.get(streamKey) ?? "metric-line-group-0",
    value: (point) => value(point, streamKey),
    format,
  })), [selectedKeys, stableClassByKey]);

  const lagSeries = useMemo(() => makeSeries(
    t("Messages not yet delivered across the stream's consumer groups."),
    (point, streamKey) => point.values[streamKey]?.totalLag ?? null,
    (value) => Math.round(value).toLocaleString(locale),
  ), [locale, makeSeries, t]);
  const pendingSeries = useMemo(() => makeSeries(
    t("Delivered messages that remain unacknowledged in the PEL."),
    (point, streamKey) => point.values[streamKey]?.pending ?? null,
    (value) => Math.round(value).toLocaleString(locale),
  ), [locale, makeSeries, t]);
  const publishSeries = useMemo(() => makeSeries(
    t("Entries added per second. Uses entries-added when Redis exposes it."),
    (point, streamKey) => point.values[streamKey]?.publishRate ?? null,
    formatRate,
  ), [makeSeries, t]);
  const deliverySeries = useMemo(() => makeSeries(
    t("Consumer group progress per second from entries-read."),
    (point, streamKey) => point.values[streamKey]?.consumeRate ?? null,
    formatRate,
  ), [makeSeries, t]);
  const lagChangeSeries = useMemo(() => makeSeries(
    t("Positive values mean backlog is growing; negative values mean it is draining."),
    (point, streamKey) => point.values[streamKey]?.lagDelta ?? null,
    formatSignedRate,
  ), [makeSeries, t]);
  const deliveryAgeSeries = useMemo(() => makeSeries(
    t("Age of the group's most recently delivered Stream ID at collection time. This is not processing latency."),
    (point, streamKey) => point.values[streamKey]?.observedDeliveryAgeMs ?? null,
    formatMilliseconds,
  ), [makeSeries, t]);

  const toggleStream = (streamKey: string) => {
    setSelectedKeys((current) => current.includes(streamKey)
      ? current.filter((key) => key !== streamKey)
      : current.length < maximumVisibleStreams ? [...current, streamKey] : current);
  };
  const hasSeries = Boolean(selectedKeys.length && metrics?.items.length);
  const emptyMessage = !rankedKeys.length
    ? t("No monitored streams")
    : !selectedKeys.length
      ? t("Select streams to compare.")
      : t("Waiting for time-series samples…");

  return <section className="metric-history-panel stream-comparison-panel">
    <header className="metric-history-header">
      <div><h2>{t("Stream comparison")}</h2><span>{metrics ? t("{seconds}s samples", { seconds: metrics.intervalSeconds }) : t("Time series")}</span></div>
      <div className="metric-history-controls">
        {leadingControls}
        <Select value={range} options={rangeOptions} onChange={(next) => setRange(next as StreamMetricSeries["range"])} ariaLabel={t("Time range")} prefix={t("Range")} size="compact" />
        <button type="button" className={live ? "metric-live active" : "metric-live"} aria-pressed={live} onClick={() => setLive((current) => !current)}><Radio size={14} />{t("Live")}</button>
        <button type="button" aria-label={t("Refresh metrics")} title={t("Refresh metrics")} disabled={loading || !connectionId || !selectedKeys.length} onClick={() => void load()}><RefreshCw className={loading ? "spin" : ""} size={14} /></button>
      </div>
    </header>
    {rankedKeys.length ? <div className="metric-series-picker" role="group" aria-label={t("Streams shown on charts")}>
      <span>{t("Compare")}</span>
      <div>{rankedKeys.map((streamKey) => {
        const active = selectedKeys.includes(streamKey);
        return <button
          type="button"
          key={streamKey}
          className={active ? "active" : ""}
          aria-pressed={active}
          disabled={!active && selectedKeys.length >= maximumVisibleStreams}
          onClick={() => toggleStream(streamKey)}
          title={streamKey}
        ><i className={stableClassByKey.get(streamKey)} /><span className="mono">{streamKey}</span></button>;
      })}</div>
      <em>{t("{count} of 8 streams", { count: selectedKeys.length })}</em>
    </div> : null}
    {error ? <div className="metric-history-error" role="alert">{error}</div> : null}
    {!hasSeries ? <div className="consumer-group-metrics-empty">{emptyMessage}</div> : <div className="metric-chart-grid stream-comparison-chart-grid">
      <MetricTimeSeriesChart title={t("Lag by stream")} points={metrics?.items ?? []} series={lagSeries} valueKind="count" expectedIntervalSeconds={metrics?.intervalSeconds} />
      <MetricTimeSeriesChart title={t("Pending by stream")} points={metrics?.items ?? []} series={pendingSeries} valueKind="count" expectedIntervalSeconds={metrics?.intervalSeconds} />
      <MetricTimeSeriesChart title={t("Published rate by stream")} points={metrics?.items ?? []} series={publishSeries} valueKind="rate" expectedIntervalSeconds={metrics?.intervalSeconds} />
      <MetricTimeSeriesChart title={t("Delivery rate by stream")} points={metrics?.items ?? []} series={deliverySeries} valueKind="rate" expectedIntervalSeconds={metrics?.intervalSeconds} />
      <MetricTimeSeriesChart title={t("Lag change by stream")} points={metrics?.items ?? []} series={lagChangeSeries} valueKind="rate" expectedIntervalSeconds={metrics?.intervalSeconds} />
      <MetricTimeSeriesChart title={t("Observed delivery age by stream")} points={metrics?.items ?? []} series={deliveryAgeSeries} valueKind="duration" expectedIntervalSeconds={metrics?.intervalSeconds} />
    </div>}
  </section>;
}

function formatRate(value: number) {
  return `${value.toFixed(Math.abs(value) < 10 ? 2 : 1)} /s`;
}

function formatSignedRate(value: number) {
  const formatted = value.toFixed(Math.abs(value) < 10 ? 2 : 1);
  return `${value > 0 ? "+" : ""}${formatted}/s`;
}

function formatMilliseconds(value: number) {
  if (value < 1) return `${value.toFixed(2)} ms`;
  if (value < 1000) return `${value.toFixed(value < 10 ? 1 : 0)} ms`;
  if (value < 60_000) return `${(value / 1000).toFixed(1)} s`;
  if (value < 3_600_000) return `${(value / 60_000).toFixed(1)} min`;
  return `${(value / 3_600_000).toFixed(1)} h`;
}
