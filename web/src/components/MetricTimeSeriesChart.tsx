import { useId, useLayoutEffect, useMemo, useRef, useState } from "react";
import { useI18n } from "../i18n";
import type { StreamMetricPoint } from "../types";

type TimestampedMetricPoint = { timestamp: string };

export type MetricChartSeries<TPoint extends TimestampedMetricPoint = StreamMetricPoint> = {
  id: string;
  label: string;
  description: string;
  className: string;
  value: (point: TPoint) => number | null;
  format: (value: number) => string;
};

type MetricTimeSeriesChartProps<TPoint extends TimestampedMetricPoint> = {
  title: string;
  points: TPoint[];
  series: MetricChartSeries<TPoint>[];
  valueKind?: "count" | "rate" | "duration";
  emptyLabel?: string;
  expectedIntervalSeconds?: number;
};

const defaultWidth = 720;
const defaultHeight = 246;
const plot = { left: 54, right: 18, top: 20, bottom: 38 };

export function MetricTimeSeriesChart<TPoint extends TimestampedMetricPoint>({
  title,
  points,
  series,
  valueKind = "count",
  emptyLabel,
  expectedIntervalSeconds,
}: MetricTimeSeriesChartProps<TPoint>) {
  const { locale, t } = useI18n();
  const canvasRef = useRef<HTMLDivElement>(null);
  const svgRef = useRef<SVGSVGElement>(null);
  const touchStartRef = useRef<{ pointerId: number; clientX: number; clientY: number } | null>(null);
  const accessibleStatusId = useId();
  const [hoveredIndex, setHoveredIndex] = useState<number | null>(null);
  const [selectedIndex, setSelectedIndex] = useState<number | null>(null);
  const [dimensions, setDimensions] = useState({ width: defaultWidth, height: defaultHeight });
  const chartWidth = dimensions.width;
  const chartHeight = dimensions.height;
  const plotWidth = chartWidth - plot.left - plot.right;
  const plotHeight = chartHeight - plot.top - plot.bottom;

  useLayoutEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas || typeof ResizeObserver === "undefined") return;
    const updateDimensions = () => {
      const bounds = canvas.getBoundingClientRect();
      if (!bounds.width || !bounds.height) return;
      const next = {
        width: Math.max(280, Math.round(bounds.width)),
        height: Math.max(200, Math.round(bounds.height)),
      };
      setDimensions((current) => current.width === next.width && current.height === next.height ? current : next);
    };
    updateDimensions();
    const observer = new ResizeObserver(updateDimensions);
    observer.observe(canvas);
    return () => observer.disconnect();
  }, []);
  const values = useMemo(
    () => points.flatMap((point) => series.map((item) => item.value(point))).filter((value): value is number => value !== null && Number.isFinite(value)),
    [points, series],
  );
  const [minimum, maximum] = useMemo(() => metricDomain(values, valueKind), [valueKind, values]);
  const hasRenderableData = Boolean(points.length && values.length && series.length);
  const valueRange = Math.max(1e-9, maximum - minimum);
  const pointTimes = useMemo(() => points.map((point) => {
    const value = new Date(point.timestamp).getTime();
    return Number.isFinite(value) ? value : null;
  }), [points]);
  const xPositions = useMemo(
    () => metricXPositions(pointTimes, plot.left, plotWidth),
    [plotWidth, pointTimes],
  );
  const gapThresholdMs = useMemo(
    () => metricGapThreshold(pointTimes, expectedIntervalSeconds),
    [expectedIntervalSeconds, pointTimes],
  );
  const xAt = (index: number) => xPositions[index] ?? plot.left + plotWidth / 2;
  const yAt = (value: number) => plot.top + plotHeight - ((value - minimum) / valueRange) * plotHeight;
  const activeHoveredIndex = hoveredIndex !== null && hoveredIndex < points.length ? hoveredIndex : null;
  const activeSelectedIndex = selectedIndex !== null && selectedIndex < points.length ? selectedIndex : null;
  const activeIndex = activeHoveredIndex ?? activeSelectedIndex;
  const activePoint = activeIndex === null ? null : points[activeIndex];
  const selectedPoint = activeSelectedIndex === null ? null : points[activeSelectedIndex];
  const tooltipLeft = activeIndex === null ? 50 : (xAt(activeIndex) / chartWidth) * 100;
  const accessibleIndex = activeSelectedIndex ?? Math.max(0, points.length - 1);
  const accessiblePoint = points[accessibleIndex];
  const accessibleValueText = accessiblePoint ? describeMetricPoint(accessiblePoint, series, locale) : title;
  const selectedValueText = selectedPoint ? describeMetricPoint(selectedPoint, series, locale) : "";

  const nearestPointIndex = (clientX: number) => {
    if (!points.length || !svgRef.current) return null;
    const bounds = svgRef.current.getBoundingClientRect();
    if (!bounds.width) return null;
    const viewX = ((clientX - bounds.left) / bounds.width) * chartWidth;
    return nearestXIndex(xPositions, viewX);
  };

  const onPointerMove = (event: React.PointerEvent<SVGSVGElement>) => {
    if (event.pointerType === "touch") return;
    const index = nearestPointIndex(event.clientX);
    if (index !== null) setHoveredIndex(index);
  };

  const onPointerDown = (event: React.PointerEvent<SVGSVGElement>) => {
    if (event.pointerType === "mouse") return;
    touchStartRef.current = { pointerId: event.pointerId, clientX: event.clientX, clientY: event.clientY };
  };

  const onPointerUp = (event: React.PointerEvent<SVGSVGElement>) => {
    const start = touchStartRef.current;
    touchStartRef.current = null;
    if (!start || start.pointerId !== event.pointerId) return;
    if (Math.hypot(event.clientX - start.clientX, event.clientY - start.clientY) > 10) return;
    const index = nearestPointIndex(event.clientX);
    if (index === null) return;
    setHoveredIndex(null);
    setSelectedIndex(index);
  };

  const onKeyDown = (event: React.KeyboardEvent<SVGSVGElement>) => {
    if (!points.length) return;
    if (event.key === "Escape") {
      event.preventDefault();
      setHoveredIndex(null);
      setSelectedIndex(null);
      return;
    }
    const currentIndex = activeSelectedIndex ?? points.length - 1;
    let nextIndex: number | null = null;
    if (event.key === "ArrowLeft") nextIndex = Math.max(0, currentIndex - 1);
    if (event.key === "ArrowRight") nextIndex = Math.min(points.length - 1, currentIndex + 1);
    if (event.key === "Home") nextIndex = 0;
    if (event.key === "End") nextIndex = points.length - 1;
    if (nextIndex === null) return;
    event.preventDefault();
    setHoveredIndex(null);
    setSelectedIndex(nextIndex);
  };

  return (
    <article className={`metric-chart metric-chart--${valueKind}${hasRenderableData ? "" : " metric-chart--empty"}`}>
      <header>
        <h3>{title}</h3>
        <div className="metric-chart-legend">
          {series.map((item) => <span key={item.id} title={item.description}><i className={item.className} />{item.label}</span>)}
        </div>
      </header>
      <div ref={canvasRef} className="metric-chart-canvas" style={{ height: hasRenderableData ? "clamp(220px, 28vw, 270px)" : "132px", minHeight: 0 }}>
        {hasRenderableData ? <>
          <svg
            ref={svgRef}
            viewBox={`0 0 ${chartWidth} ${chartHeight}`}
            role="slider"
            aria-label={title}
            aria-describedby={accessibleStatusId}
            aria-keyshortcuts="ArrowLeft ArrowRight Home End"
            aria-orientation="horizontal"
            aria-valuemin={1}
            aria-valuemax={points.length}
            aria-valuenow={accessibleIndex + 1}
            aria-valuetext={accessibleValueText}
            tabIndex={0}
            onFocus={() => {
              if (activeSelectedIndex === null) setSelectedIndex(points.length - 1);
            }}
            onBlur={() => {
              setHoveredIndex(null);
              setSelectedIndex(null);
            }}
            onKeyDown={onKeyDown}
            onPointerDown={onPointerDown}
            onPointerMove={onPointerMove}
            onPointerUp={onPointerUp}
            onPointerCancel={() => { touchStartRef.current = null; }}
            onPointerLeave={() => setHoveredIndex(null)}
            style={{ width: "100%", height: "100%" }}
          >
            {[0, 0.25, 0.5, 0.75, 1].map((ratio) => {
              const y = plot.top + plotHeight - ratio * plotHeight;
              const axisValue = minimum + valueRange * ratio;
              return <g key={ratio}><line className={`metric-grid-line ${Math.abs(axisValue) < valueRange / 1000 ? "zero" : ""}`} x1={plot.left} x2={chartWidth - plot.right} y1={y} y2={y} /><text className="metric-axis-label" x={plot.left - 9} y={y + 4} textAnchor="end">{formatAxisValue(axisValue, valueKind, locale)}</text></g>;
            })}
            {series.map((item) => {
              const segments = lineSegments(points, (point) => item.value(point), xAt, yAt, pointTimes, gapThresholdMs);
              return <g key={item.id} className={`metric-series ${item.className}`}>
                {segments.map((path, index) => <path key={index} d={path} />)}
                {points.length === 1 && item.value(points[0]) !== null ? <circle cx={xAt(0)} cy={yAt(item.value(points[0]) as number)} r="3.5" /> : null}
              </g>;
            })}
            {activeIndex !== null ? <>
              <line className="metric-hover-line" x1={xAt(activeIndex)} x2={xAt(activeIndex)} y1={plot.top} y2={plot.top + plotHeight} />
              {series.map((item) => {
                const value = item.value(points[activeIndex]);
                return value === null ? null : <circle key={item.id} className={`metric-hover-point ${item.className}`} cx={xAt(activeIndex)} cy={yAt(value)} r="4" />;
              })}
            </> : null}
            <rect className="metric-chart-hit-area" x={plot.left} y={plot.top} width={plotWidth} height={plotHeight} />
            {metricAxisPointIndexes(xPositions).map((pointIndex, index, axisPoints) => {
              const point = points[pointIndex];
              const x = xAt(pointIndex);
              const textAnchor = index === 0 ? "start" : index === axisPoints.length - 1 ? "end" : "middle";
              return <text key={`${point.timestamp}:${index}`} className="metric-axis-label metric-x-axis-label" x={x} y={chartHeight - 9} textAnchor={textAnchor}>{formatChartTime(point.timestamp, locale)}</text>;
            })}
          </svg>
          <span id={accessibleStatusId} aria-live="polite" aria-atomic="true" style={visuallyHiddenStyle}>{selectedValueText}</span>
          {activePoint ? <div className={`metric-chart-tooltip ${tooltipLeft > 70 ? "align-right" : tooltipLeft < 30 ? "align-left" : ""}`} style={{ left: `${tooltipLeft}%` }}>
            <strong>{new Date(activePoint.timestamp).toLocaleString(locale, { hour12: false })}</strong>
            {series.map((item) => {
              const value = item.value(activePoint);
              return <div key={item.id}><span><i className={item.className} />{item.label}</span><b>{value === null ? "—" : item.format(value)}</b></div>;
            })}
          </div> : null}
        </> : <div className="metric-chart-empty metric-chart-empty--unknown">{emptyLabel ?? t(points.length ? "Unavailable" : "Waiting for time-series samples…")}</div>}
      </div>
    </article>
  );
}

const visuallyHiddenStyle = {
  position: "absolute",
  width: 1,
  height: 1,
  padding: 0,
  margin: -1,
  overflow: "hidden",
  clip: "rect(0, 0, 0, 0)",
  whiteSpace: "nowrap",
  border: 0,
} as const;

function describeMetricPoint<TPoint extends TimestampedMetricPoint>(
  point: TPoint,
  series: MetricChartSeries<TPoint>[],
  locale: string,
) {
  const date = new Date(point.timestamp);
  const timestamp = Number.isNaN(date.getTime()) ? point.timestamp : date.toLocaleString(locale, { hour12: false });
  const values = series.map((item) => {
    const value = item.value(point);
    return `${item.label}: ${value === null ? "—" : item.format(value)}`;
  });
  return [timestamp, ...values].join(", ");
}

function lineSegments<TPoint extends TimestampedMetricPoint>(
  points: TPoint[],
  value: (point: TPoint) => number | null,
  xAt: (index: number) => number,
  yAt: (value: number) => number,
  pointTimes: Array<number | null>,
  gapThresholdMs: number | null,
) {
  const segments: string[] = [];
  let current = "";
  let previousTime: number | null = null;
  points.forEach((point, index) => {
    const item = value(point);
    if (item === null || !Number.isFinite(item)) {
      if (current) segments.push(current);
      current = "";
      previousTime = pointTimes[index] ?? null;
      return;
    }
    const pointTime = pointTimes[index] ?? null;
    if (current && gapThresholdMs !== null && previousTime !== null && pointTime !== null && pointTime - previousTime > gapThresholdMs) {
      segments.push(current);
      current = "";
    }
    current += `${current ? " L" : "M"} ${xAt(index).toFixed(2)} ${yAt(item).toFixed(2)}`;
    previousTime = pointTime;
  });
  if (current) segments.push(current);
  return segments;
}

function metricXPositions(pointTimes: Array<number | null>, left: number, width: number) {
  if (pointTimes.length <= 1) return pointTimes.map(() => left + width / 2);
  const validTimes = pointTimes.filter((value): value is number => value !== null);
  const minimum = validTimes.length === pointTimes.length ? Math.min(...validTimes) : NaN;
  const maximum = validTimes.length === pointTimes.length ? Math.max(...validTimes) : NaN;
  if (!Number.isFinite(minimum) || !Number.isFinite(maximum) || maximum <= minimum) {
    return pointTimes.map((_, index) => left + (index / (pointTimes.length - 1)) * width);
  }
  return pointTimes.map((value) => left + (((value as number) - minimum) / (maximum - minimum)) * width);
}

function nearestXIndex(xPositions: number[], target: number) {
  if (!xPositions.length) return null;
  let low = 0;
  let high = xPositions.length - 1;
  while (low < high) {
    const middle = Math.floor((low + high) / 2);
    if (xPositions[middle] < target) low = middle + 1;
    else high = middle;
  }
  if (low === 0) return 0;
  const previous = low - 1;
  return Math.abs(xPositions[low] - target) < Math.abs(xPositions[previous] - target) ? low : previous;
}

function metricGapThreshold(pointTimes: Array<number | null>, expectedIntervalSeconds?: number) {
  if (expectedIntervalSeconds && expectedIntervalSeconds > 0) return expectedIntervalSeconds * 2500;
  const deltas: number[] = [];
  for (let index = 1; index < pointTimes.length; index += 1) {
    const previous = pointTimes[index - 1];
    const current = pointTimes[index];
    if (previous !== null && current !== null && current > previous) deltas.push(current - previous);
  }
  if (!deltas.length) return null;
  deltas.sort((left, right) => left - right);
  return deltas[Math.floor(deltas.length / 2)] * 2.5;
}

function metricAxisPointIndexes(xPositions: number[]) {
  if (!xPositions.length) return [];
  if (xPositions.length === 1) return [0];
  const middleX = (xPositions[0] + xPositions[xPositions.length - 1]) / 2;
  const middle = nearestXIndex(xPositions, middleX) ?? Math.floor((xPositions.length - 1) / 2);
  return [...new Set([0, middle, xPositions.length - 1])];
}

function metricDomain(values: number[], valueKind: "count" | "rate" | "duration"): [number, number] {
  if (!values.length) return [0, 1];
  const rawMinimum = Math.min(...values);
  const rawMaximum = Math.max(...values);
  if (valueKind === "duration" && rawMinimum >= 0 && rawMaximum > 0) {
    if (rawMinimum === rawMaximum) {
      const padding = Math.max(1, rawMaximum * 0.08);
      return [Math.max(0, rawMinimum - padding), rawMaximum + padding];
    }
    const padding = Math.max(1, (rawMaximum - rawMinimum) * 0.1);
    return [Math.max(0, rawMinimum - padding), rawMaximum + padding];
  }
  if (rawMinimum >= 0) {
    if (rawMaximum === 0) return [0, valueKind === "rate" ? 0.1 : 1];
    return [0, valueKind === "count" && rawMaximum <= 1 ? 1 : rawMaximum * 1.08];
  }
  const padding = Math.max(0.001, (rawMaximum - rawMinimum) * 0.08);
  return [Math.min(0, rawMinimum - padding), Math.max(0, rawMaximum + padding)];
}

function formatAxisValue(value: number, valueKind: "count" | "rate" | "duration", locale: string) {
  if (valueKind === "duration") return formatAxisDuration(value, locale);
  if (valueKind === "rate") {
    return `${new Intl.NumberFormat(locale, { maximumFractionDigits: Math.abs(value) < 1 ? 2 : 1 }).format(value)}/s`;
  }
  return compactNumber(value, locale);
}

function formatAxisDuration(value: number, locale: string) {
  const absolute = Math.abs(value);
  const format = (item: number, maximumFractionDigits: number) => new Intl.NumberFormat(locale, { maximumFractionDigits }).format(item);
  if (absolute < 1_000) return `${format(value, absolute < 10 ? 1 : 0)}ms`;
  if (absolute < 60_000) return `${format(value / 1_000, 1)}s`;
  if (absolute < 3_600_000) return `${format(value / 60_000, 1)}m`;
  if (absolute < 86_400_000) return `${format(value / 3_600_000, 1)}h`;
  return `${format(value / 86_400_000, 1)}d`;
}

function compactNumber(value: number, locale: string) {
  return new Intl.NumberFormat(locale, { notation: "compact", maximumFractionDigits: 1 }).format(value);
}

function formatChartTime(value: string, locale: string) {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleTimeString(locale, { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false });
}
