import { Check, ChevronDown, Search } from "lucide-react";
import { forwardRef, useCallback, useEffect, useId, useLayoutEffect, useMemo, useRef, useState, type KeyboardEvent, type Ref } from "react";
import { createPortal } from "react-dom";
import { useI18n } from "../i18n";

export type SelectOption = {
  value: string;
  label: string;
  description?: string;
  meta?: string;
  keywords?: string;
  tone?: "neutral" | "info" | "success" | "warning" | "danger";
  disabled?: boolean;
};

type SelectProps = {
  value: string;
  options: readonly SelectOption[];
  onChange: (value: string) => void;
  ariaLabel: string;
  placeholder?: string;
  disabled?: boolean;
  searchable?: boolean;
  searchPlaceholder?: string;
  emptyLabel?: string;
  prefix?: string;
  className?: string;
  size?: "default" | "compact";
};

type PopoverPosition = {
  left: number;
  width: number;
  maxHeight: number;
  top?: number;
  bottom?: number;
};

export const Select = forwardRef<HTMLButtonElement, SelectProps>(function Select({
  value,
  options,
  onChange,
  ariaLabel,
  placeholder,
  disabled = false,
  searchable = false,
  searchPlaceholder,
  emptyLabel,
  prefix,
  className = "",
  size = "default",
}, forwardedRef) {
  const { t } = useI18n();
  const listboxId = useId();
  const triggerRef = useRef<HTMLButtonElement | null>(null);
  const popoverRef = useRef<HTMLDivElement | null>(null);
  const searchRef = useRef<HTMLInputElement | null>(null);
  const optionRefs = useRef(new Map<string, HTMLButtonElement>());
  const typeahead = useRef("");
  const typeaheadTimer = useRef<number | null>(null);
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [activeValue, setActiveValue] = useState("");
  const [position, setPosition] = useState<PopoverPosition | null>(null);

  const selected = options.find((option) => option.value === value);
  const visibleOptions = useMemo(() => {
    const needle = query.trim().toLocaleLowerCase();
    if (!needle) return options;
    return options.filter((option) => `${option.label} ${option.description ?? ""} ${option.meta ?? ""} ${option.keywords ?? ""}`.toLocaleLowerCase().includes(needle));
  }, [options, query]);
  const enabledOptions = useMemo(() => visibleOptions.filter((option) => !option.disabled), [visibleOptions]);

  const assignTriggerRef = useCallback((node: HTMLButtonElement | null) => {
    triggerRef.current = node;
    assignRef(forwardedRef, node);
  }, [forwardedRef]);

  const updatePosition = useCallback(() => {
    const trigger = triggerRef.current;
    if (!trigger) return;
    const rect = trigger.getBoundingClientRect();
    const viewportPadding = 8;
    const gap = 6;
    const availableBelow = window.innerHeight - rect.bottom - gap - viewportPadding;
    const availableAbove = rect.top - gap - viewportPadding;
    const openAbove = availableBelow < 230 && availableAbove > availableBelow;
    const availableHeight = Math.max(150, openAbove ? availableAbove : availableBelow);
    const width = Math.min(Math.max(rect.width, searchable ? 300 : 248), window.innerWidth - viewportPadding * 2);
    const left = Math.min(Math.max(viewportPadding, rect.left), window.innerWidth - width - viewportPadding);
    setPosition({
      left,
      width,
      maxHeight: Math.min(390, availableHeight),
      ...(openAbove ? { bottom: window.innerHeight - rect.top + gap } : { top: rect.bottom + gap }),
    });
  }, [searchable]);

  const close = useCallback((returnFocus = false) => {
    setOpen(false);
    setQuery("");
    setPosition(null);
    if (returnFocus) window.requestAnimationFrame(() => triggerRef.current?.focus());
  }, []);

  const openMenu = useCallback((edge?: "first" | "last") => {
    if (disabled || !options.length) return;
    const enabled = options.filter((option) => !option.disabled);
    const next = edge === "last" ? enabled.at(-1) : edge === "first" ? enabled[0] : enabled.find((option) => option.value === value) ?? enabled[0];
    setActiveValue(next?.value ?? "");
    setQuery("");
    setOpen(true);
  }, [disabled, options, value]);

  useLayoutEffect(() => {
    if (!open) return;
    updatePosition();
    const listener = () => updatePosition();
    window.addEventListener("resize", listener);
    window.addEventListener("scroll", listener, true);
    return () => {
      window.removeEventListener("resize", listener);
      window.removeEventListener("scroll", listener, true);
    };
  }, [open, updatePosition]);

  useEffect(() => {
    if (!open) return;
    const onPointerDown = (event: PointerEvent) => {
      const target = event.target as Node;
      if (triggerRef.current?.contains(target) || popoverRef.current?.contains(target)) return;
      close(false);
    };
    document.addEventListener("pointerdown", onPointerDown);
    return () => document.removeEventListener("pointerdown", onPointerDown);
  }, [close, open]);

  useEffect(() => {
    if (!open) return;
    if (!enabledOptions.some((option) => option.value === activeValue)) setActiveValue(enabledOptions[0]?.value ?? "");
  }, [activeValue, enabledOptions, open]);

  useEffect(() => {
    if (!open) return;
    const frame = window.requestAnimationFrame(() => {
      if (searchable) searchRef.current?.focus();
      else optionRefs.current.get(activeValue)?.focus();
    });
    return () => window.cancelAnimationFrame(frame);
  }, [open, searchable]);

  useEffect(() => () => {
    if (typeaheadTimer.current !== null) window.clearTimeout(typeaheadTimer.current);
  }, []);

  const moveActive = (offset: number) => {
    if (!enabledOptions.length) return;
    const index = enabledOptions.findIndex((option) => option.value === activeValue);
    const nextIndex = index < 0 ? 0 : (index + offset + enabledOptions.length) % enabledOptions.length;
    const next = enabledOptions[nextIndex];
    setActiveValue(next.value);
    window.requestAnimationFrame(() => optionRefs.current.get(next.value)?.focus());
  };

  const choose = (option: SelectOption) => {
    if (option.disabled) return;
    if (option.value !== value) onChange(option.value);
    close(true);
  };

  const handleMenuKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    if (event.key === "Escape") {
      event.preventDefault();
      event.stopPropagation();
      close(true);
      return;
    }
    if (event.key === "Tab") {
      close(false);
      return;
    }
    if (event.key === "ArrowDown" || event.key === "ArrowUp") {
      event.preventDefault();
      moveActive(event.key === "ArrowDown" ? 1 : -1);
      return;
    }
    if (event.key === "Home" || event.key === "End") {
      event.preventDefault();
      const next = event.key === "Home" ? enabledOptions[0] : enabledOptions.at(-1);
      if (next) {
        setActiveValue(next.value);
        window.requestAnimationFrame(() => optionRefs.current.get(next.value)?.focus());
      }
      return;
    }
    if (event.key === "Enter" && event.target === searchRef.current) {
      event.preventDefault();
      const next = enabledOptions.find((option) => option.value === activeValue) ?? enabledOptions[0];
      if (next) choose(next);
      return;
    }
    if (!searchable && event.key.length === 1 && !event.metaKey && !event.ctrlKey && !event.altKey) {
      typeahead.current += event.key.toLocaleLowerCase();
      if (typeaheadTimer.current !== null) window.clearTimeout(typeaheadTimer.current);
      typeaheadTimer.current = window.setTimeout(() => { typeahead.current = ""; }, 500);
      const next = enabledOptions.find((option) => option.label.toLocaleLowerCase().startsWith(typeahead.current));
      if (next) {
        setActiveValue(next.value);
        window.requestAnimationFrame(() => optionRefs.current.get(next.value)?.focus());
      }
    }
  };

  const triggerLabel = selected?.label ?? placeholder ?? t("Select an option");
  const popover = open && position ? <>
    <button type="button" tabIndex={-1} className="select-backdrop" aria-label={t("Close options")} onClick={() => close(true)} />
    <div
      ref={popoverRef}
      className="select-popover"
      style={{ left: position.left, width: position.width, maxHeight: position.maxHeight, top: position.top, bottom: position.bottom }}
      onKeyDown={handleMenuKeyDown}
    >
      <div className="select-popover-heading"><strong>{ariaLabel}</strong><span>{triggerLabel}</span></div>
      {searchable ? <label className="select-search">
        <Search size={15} aria-hidden="true" />
        <input
          ref={searchRef}
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder={searchPlaceholder ?? t("Search options…")}
          aria-label={searchPlaceholder ?? t("Search options…")}
          aria-controls={listboxId}
        />
      </label> : null}
      <span className="select-live-region" aria-live="polite">{t("{count} options", { count: visibleOptions.length })}</span>
      <div id={listboxId} className="select-options" role="listbox" aria-label={ariaLabel}>
        {visibleOptions.map((option, index) => <button
        type="button"
        id={`${listboxId}-option-${index}`}
        key={option.value}
        ref={(node) => {
          if (node) optionRefs.current.set(option.value, node);
          else optionRefs.current.delete(option.value);
        }}
        role="option"
        aria-selected={option.value === value}
        aria-disabled={option.disabled || undefined}
        className={`select-option${option.value === value ? " selected" : ""}${option.value === activeValue ? " active" : ""}`}
        tabIndex={option.value === activeValue ? 0 : -1}
        disabled={option.disabled}
        onMouseEnter={() => { if (!option.disabled) setActiveValue(option.value); }}
        onKeyDown={(event) => {
          if (event.key !== "Enter" && event.key !== " ") return;
          event.preventDefault();
          event.stopPropagation();
          choose(option);
        }}
        onClick={() => choose(option)}
      >
        {option.tone ? <i className={`select-tone select-tone--${option.tone}`} aria-hidden="true" /> : null}
        <span className="select-option-copy"><strong>{option.label}</strong>{option.description ? <small>{option.description}</small> : null}</span>
        {option.meta ? <em>{option.meta}</em> : null}
        <Check className="select-check" size={16} aria-hidden="true" />
        </button>)}
        {!visibleOptions.length ? <div className="select-empty">{emptyLabel ?? t("No matching options")}</div> : null}
      </div>
    </div>
  </> : null;

  return <span className={`select-control select-control--${size}${open ? " open" : ""}${className ? ` ${className}` : ""}`}>
    <button
      ref={assignTriggerRef}
      type="button"
      className="select-trigger"
      role="combobox"
      aria-label={ariaLabel}
      aria-haspopup="listbox"
      aria-expanded={open}
      aria-controls={open ? listboxId : undefined}
      title={selected?.label ?? triggerLabel}
      disabled={disabled || !options.length}
      onClick={() => open ? close(false) : openMenu()}
      onKeyDown={(event) => {
        if (event.key === "ArrowDown" || event.key === "ArrowUp") {
          event.preventDefault();
          openMenu(event.key === "ArrowUp" ? "last" : undefined);
        } else if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          open ? close(false) : openMenu();
        } else if (event.key === "Escape" && open) {
          event.preventDefault();
          event.stopPropagation();
          close(true);
        } else if (event.key === "Tab" && open) close(false);
      }}
    >
      {selected?.tone ? <i className={`select-tone select-tone--${selected.tone}`} aria-hidden="true" /> : null}
      <span className="select-trigger-copy">{prefix ? <span className="select-trigger-prefix">{prefix}</span> : null}<strong>{triggerLabel}</strong>{selected?.meta ? <em>{selected.meta}</em> : null}</span>
      <ChevronDown className="select-chevron" size={16} aria-hidden="true" />
    </button>
    {typeof document !== "undefined" ? createPortal(popover, document.body) : null}
  </span>;
});

function assignRef<T>(ref: Ref<T> | undefined, value: T | null) {
  if (typeof ref === "function") ref(value);
  else if (ref) ref.current = value;
}
