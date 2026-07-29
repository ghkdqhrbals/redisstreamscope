import {
  type CSSProperties,
  type KeyboardEvent,
  type PointerEvent as ReactPointerEvent,
  type ReactNode,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { useI18n } from "../i18n";

export type ResizableGridColumn = {
  id: string;
  label: ReactNode;
  ariaLabel?: string;
  defaultWidth: number;
  minWidth: number;
  grow?: boolean;
};

type ResizableGridProps = {
  className: string;
  storageKey: string;
  columns: ResizableGridColumn[];
  headerClassName: string;
  fixedLayout?: boolean;
  renderHeader?: (column: ResizableGridColumn) => ReactNode;
  children: ReactNode;
};

const STORAGE_PREFIX = "streamscope:grid-columns:v1:";

export function ResizableGrid({
  className,
  storageKey,
  columns,
  headerClassName,
  fixedLayout = false,
  renderHeader,
  children,
}: ResizableGridProps) {
  const { t } = useI18n();
  const rootRef = useRef<HTMLDivElement>(null);
  const [widths, setWidths] = useState<Record<string, number>>(() => loadWidths(storageKey, columns));
  const [fixedLayoutReady, setFixedLayoutReady] = useState(!fixedLayout);

  useLayoutEffect(() => {
    if (!fixedLayout || fixedLayoutReady) return;
    const cells = rootRef.current?.querySelectorAll<HTMLElement>(":scope > .resizable-grid-head > .resizable-grid-header-cell");
    if (!cells || cells.length !== columns.length) return;
    setWidths(Object.fromEntries(columns.map((column, index) => [
      column.id,
      Math.max(column.minWidth, Math.round(cells[index].getBoundingClientRect().width)),
    ])));
    setFixedLayoutReady(true);
  }, [columns, fixedLayout, fixedLayoutReady]);

  useEffect(() => {
    setWidths((current) => {
      const next = { ...current };
      let changed = false;
      for (const column of columns) {
        const validWidth = Math.max(column.minWidth, Number(next[column.id]) || column.defaultWidth);
        if (next[column.id] !== validWidth) {
          next[column.id] = validWidth;
          changed = true;
        }
      }
      return changed ? next : current;
    });
  }, [columns]);

  useEffect(() => {
    try {
      window.localStorage.setItem(`${STORAGE_PREFIX}${storageKey}`, JSON.stringify(widths));
    } catch {
      // Column resizing still works when browser storage is unavailable.
    }
  }, [storageKey, widths]);

  useEffect(() => () => document.body.classList.remove("column-resizing"), []);

  const gridStyle = useMemo(() => {
    const tracks = columns.map((column) => {
      const width = widths[column.id] ?? column.defaultWidth;
      return column.grow && !fixedLayoutReady ? `minmax(${width}px, 1fr)` : `${width}px`;
    });
    const minimumWidth = columns.reduce((total, column) => total + (widths[column.id] ?? column.defaultWidth), 0);
    return {
      "--resizable-grid-columns": tracks.join(" "),
      "--resizable-grid-min-width": `${minimumWidth}px`,
    } as CSSProperties;
  }, [columns, fixedLayoutReady, widths]);

  const beginResize = (event: ReactPointerEvent<HTMLSpanElement>, column: ResizableGridColumn) => {
    event.preventDefault();
    event.stopPropagation();
    const startX = event.clientX;
    const startWidth = widths[column.id] ?? column.defaultWidth;
    document.body.classList.add("column-resizing");

    const resize = (moveEvent: PointerEvent) => {
      const width = Math.max(column.minWidth, startWidth + moveEvent.clientX - startX);
      setWidths((current) => current[column.id] === width ? current : { ...current, [column.id]: width });
    };
    const finish = () => {
      document.body.classList.remove("column-resizing");
      window.removeEventListener("pointermove", resize);
      window.removeEventListener("pointerup", finish);
      window.removeEventListener("pointercancel", finish);
    };

    window.addEventListener("pointermove", resize);
    window.addEventListener("pointerup", finish, { once: true });
    window.addEventListener("pointercancel", finish, { once: true });
  };

  const resizeWithKeyboard = (event: KeyboardEvent<HTMLSpanElement>, column: ResizableGridColumn) => {
    if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") return;
    event.preventDefault();
    const change = event.key === "ArrowLeft" ? -16 : 16;
    setWidths((current) => ({
      ...current,
      [column.id]: Math.max(column.minWidth, (current[column.id] ?? column.defaultWidth) + change),
    }));
  };

  return (
    <div className={`resizable-grid ${className}`} style={gridStyle} ref={rootRef}>
      <div className={`${headerClassName} resizable-grid-head`}>
        {columns.map((column) => (
          <div className="resizable-grid-header-cell" key={column.id}>
            <div className="resizable-grid-header-content">
              {renderHeader ? renderHeader(column) : column.label}
            </div>
            <span
              className="column-resize-handle"
              role="separator"
              aria-orientation="vertical"
              aria-label={t("Resize {column} column", { column: column.ariaLabel ?? String(column.label) })}
              aria-valuemin={column.minWidth}
              aria-valuenow={widths[column.id] ?? column.defaultWidth}
              tabIndex={0}
              onPointerDown={(event) => beginResize(event, column)}
              onKeyDown={(event) => resizeWithKeyboard(event, column)}
              onDoubleClick={() => setWidths((current) => ({ ...current, [column.id]: column.defaultWidth }))}
            />
          </div>
        ))}
      </div>
      {children}
    </div>
  );
}

function loadWidths(storageKey: string, columns: ResizableGridColumn[]) {
  try {
    const saved = JSON.parse(window.localStorage.getItem(`${STORAGE_PREFIX}${storageKey}`) ?? "{}") as Record<string, number>;
    return Object.fromEntries(columns.map((column) => [
      column.id,
      Math.max(column.minWidth, Number(saved[column.id]) || column.defaultWidth),
    ]));
  } catch {
    return Object.fromEntries(columns.map((column) => [column.id, column.defaultWidth]));
  }
}
