import { useMemo, useRef, useState } from "react";
import { useI18n } from "../i18n";
import type { StreamMetricPoint } from "../types";

export type MetricChartSeries = {
  id: string;
  label: string;
  description: string;
  className: string;
  value: (point: StreamMetricPoint) => number | null;
  format: (value: number) => string;
};

type MetricTimeSeriesChartProps = {
  title: string;
  points: StreamMetricPoint[];
  series: MetricChartSeries[];
};

const width = 720;
const height = 246;
const plot = { left: 54, right: 18, top: 20, bottom: 38 };

export function MetricTimeSeriesChart({ title, points, series }: MetricTimeSeriesChartProps) {
  const { locale, t } = useI18n();
  const svgRef = useRef<SVGSVGElement>(null);
  const [hoveredIndex, setHoveredIndex] = useState<number | null>(null);
  const plotWidth = width - plot.left - plot.right;
  const plotHeight = height - plot.top - plot.bottom;
  const values = useMemo(
    () => points.flatMap((point) => series.map((item) => item.value(point))).filter((value): value is number => value !== null && Number.isFinite(value)),
    [points, series],
  );
  const minimum = Math.min(0, ...values);
  const maximum = Math.max(1, ...values);
  const valueRange = Math.max(1, maximum - minimum);
  const xAt = (index: number) => plot.left + (points.length <= 1 ? plotWidth / 2 : (index / (points.length - 1)) * plotWidth);
  const yAt = (value: number) => plot.top + plotHeight - ((value - minimum) / valueRange) * plotHeight;
  const hoveredPoint = hoveredIndex === null ? null : points[hoveredIndex];
  const tooltipLeft = hoveredIndex === null ? 50 : (xAt(hoveredIndex) / width) * 100;

  const onPointerMove = (event: React.PointerEvent<SVGSVGElement>) => {
    if (!points.length || !svgRef.current) return;
    const bounds = svgRef.current.getBoundingClientRect();
    const viewX = ((event.clientX - bounds.left) / bounds.width) * width;
    const ratio = Math.min(1, Math.max(0, (viewX - plot.left) / plotWidth));
    setHoveredIndex(points.length <= 1 ? 0 : Math.round(ratio * (points.length - 1)));
  };

  return (
    <article className="metric-chart">
      <header>
        <h3>{title}</h3>
        <div className="metric-chart-legend">
          {series.map((item) => <span key={item.id} title={item.description}><i className={item.className} />{item.label}</span>)}
        </div>
      </header>
      <div className="metric-chart-canvas">
        {points.length ? <>
          <svg
            ref={svgRef}
            viewBox={`0 0 ${width} ${height}`}
            role="img"
            aria-label={title}
            onPointerMove={onPointerMove}
            onPointerLeave={() => setHoveredIndex(null)}
          >
            {[0, 0.25, 0.5, 0.75, 1].map((ratio) => {
              const y = plot.top + plotHeight - ratio * plotHeight;
              const axisValue = minimum + valueRange * ratio;
              return <g key={ratio}><line className={`metric-grid-line ${Math.abs(axisValue) < valueRange / 1000 ? "zero" : ""}`} x1={plot.left} x2={width - plot.right} y1={y} y2={y} /><text className="metric-axis-label" x={plot.left - 9} y={y + 4} textAnchor="end">{compactNumber(axisValue, locale)}</text></g>;
            })}
            {series.map((item) => {
              const segments = lineSegments(points, (point) => item.value(point), xAt, yAt);
              return <g key={item.id} className={`metric-series ${item.className}`}>
                {segments.map((path, index) => <path key={index} d={path} />)}
                {points.length === 1 && item.value(points[0]) !== null ? <circle cx={xAt(0)} cy={yAt(item.value(points[0]) as number)} r="3.5" /> : null}
              </g>;
            })}
            {hoveredIndex !== null ? <>
              <line className="metric-hover-line" x1={xAt(hoveredIndex)} x2={xAt(hoveredIndex)} y1={plot.top} y2={plot.top + plotHeight} />
              {series.map((item) => {
                const value = item.value(points[hoveredIndex]);
                return value === null ? null : <circle key={item.id} className={`metric-hover-point ${item.className}`} cx={xAt(hoveredIndex)} cy={yAt(value)} r="4" />;
              })}
            </> : null}
            <rect className="metric-chart-hit-area" x={plot.left} y={plot.top} width={plotWidth} height={plotHeight} />
          </svg>
          <div className="metric-x-axis">
            {[points[0], points[Math.floor((points.length - 1) / 2)], points[points.length - 1]].map((point, index) =>
              <span key={`${point.timestamp}:${index}`}>{formatChartTime(point.timestamp, locale)}</span>)}
          </div>
          {hoveredPoint ? <div className={`metric-chart-tooltip ${tooltipLeft > 70 ? "align-right" : tooltipLeft < 30 ? "align-left" : ""}`} style={{ left: `${tooltipLeft}%` }}>
            <strong>{new Date(hoveredPoint.timestamp).toLocaleString(locale, { hour12: false })}</strong>
            {series.map((item) => {
              const value = item.value(hoveredPoint);
              return <div key={item.id}><span><i className={item.className} />{item.label}</span><b>{value === null ? "—" : item.format(value)}</b></div>;
            })}
          </div> : null}
        </> : <div className="metric-chart-empty">{t("Waiting for time-series samples…")}</div>}
      </div>
    </article>
  );
}

function lineSegments(
  points: StreamMetricPoint[],
  value: (point: StreamMetricPoint) => number | null,
  xAt: (index: number) => number,
  yAt: (value: number) => number,
) {
  const segments: string[] = [];
  let current = "";
  points.forEach((point, index) => {
    const item = value(point);
    if (item === null || !Number.isFinite(item)) {
      if (current) segments.push(current);
      current = "";
      return;
    }
    current += `${current ? " L" : "M"} ${xAt(index).toFixed(2)} ${yAt(item).toFixed(2)}`;
  });
  if (current) segments.push(current);
  return segments;
}

function compactNumber(value: number, locale: string) {
  return new Intl.NumberFormat(locale, { notation: "compact", maximumFractionDigits: 1 }).format(value);
}

function formatChartTime(value: string, locale: string) {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleTimeString(locale, { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false });
}
