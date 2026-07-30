import { GripVertical } from "lucide-react";
import { useEffect, useRef } from "react";
import { useI18n } from "../i18n";
import { readMigratedStorage } from "../storage";

const STORAGE_KEY = "redisstreamscope:inspector-width:v1";
const LEGACY_STORAGE_KEY = "streamscope:inspector-width:v1";
const MIN_WIDTH = 320;
const MAX_WIDTH = 720;

function clampWidth(width: number) {
  const availableWidth = window.innerWidth > 900 ? window.innerWidth - 360 : window.innerWidth - 32;
  const maximumWidth = Math.max(MIN_WIDTH, Math.min(MAX_WIDTH, availableWidth));
  return Math.min(Math.max(width, MIN_WIDTH), maximumWidth);
}

export function InspectorResizeHandle() {
  const { t } = useI18n();
  const handleRef = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    const panel = handleRef.current?.parentElement;
    if (!panel) return;
    const stored = Number(readMigratedStorage(window.localStorage, STORAGE_KEY, LEGACY_STORAGE_KEY));
    if (Number.isFinite(stored) && stored >= MIN_WIDTH) panel.style.width = `${clampWidth(stored)}px`;
  }, []);

  const applyWidth = (width: number) => {
    const panel = handleRef.current?.parentElement;
    if (!panel) return;
    const nextWidth = clampWidth(width);
    panel.style.width = `${nextWidth}px`;
    window.localStorage.setItem(STORAGE_KEY, String(nextWidth));
  };

  const startResize = (event: React.PointerEvent<HTMLButtonElement>) => {
    event.preventDefault();
    const panel = event.currentTarget.parentElement;
    if (!panel) return;
    const startX = event.clientX;
    const startWidth = panel.getBoundingClientRect().width;
    document.body.classList.add("inspector-resizing");

    const move = (moveEvent: PointerEvent) => applyWidth(startWidth + startX - moveEvent.clientX);
    const stop = () => {
      document.body.classList.remove("inspector-resizing");
      window.removeEventListener("pointermove", move);
      window.removeEventListener("pointerup", stop);
    };
    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", stop, { once: true });
  };

  const resizeWithKeyboard = (event: React.KeyboardEvent<HTMLButtonElement>) => {
    if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") return;
    event.preventDefault();
    const panel = event.currentTarget.parentElement;
    if (!panel) return;
    applyWidth(panel.getBoundingClientRect().width + (event.key === "ArrowLeft" ? 24 : -24));
  };

  return <button ref={handleRef} type="button" className="inspector-resizer" role="separator" aria-label={t("Resize details panel")} aria-orientation="vertical" onPointerDown={startResize} onKeyDown={resizeWithKeyboard}><GripVertical size={14} /></button>;
}
