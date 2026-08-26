import {
  Activity,
  ArrowRight,
  BellRing,
  ChevronDown,
  CircleUserRound,
  Command,
  Database,
  Gauge,
  Layers3,
  LogOut,
  Menu,
  PanelLeftClose,
  PanelLeftOpen,
  Search,
  Settings,
  ShieldCheck,
  UserRoundCog,
  X,
} from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { api } from "../api";
import type { Page, RedisConnection, StreamItem } from "../types";
import { LanguageSelect, useI18n } from "../i18n";
import { readMigratedStorage } from "../storage";

const SIDEBAR_STORAGE_KEY = "redisstreamscope:sidebar-collapsed:v1";
const LEGACY_SIDEBAR_STORAGE_KEY = "streamscope:sidebar-collapsed:v1";
const MOBILE_NAV_QUERY = "(max-width: 900px)";
const OPEN_STREAM_CATALOG_EVENT = "redisstreamscope:open-stream-catalog";

type AppShellProps = {
  page: Page;
  username: string;
  role: "viewer" | "operator" | "admin";
  permissions: string[];
  mobileNav: boolean;
  selectedStreamConnectionId: string;
  onNavigate: (page: Page) => void;
  onSelectStream: (target: { connectionId: string; key: string }) => void;
  onToggleNav: () => void;
  onLogout: () => void;
  children: React.ReactNode;
};

type NavigationStream = StreamItem & {
  connectionId: string;
  connectionName: string;
};

const navigation = [
  { id: "overview" as const, label: "Overview", icon: Gauge },
  { id: "streams" as const, label: "Streams", icon: Database },
  { id: "alerts" as const, label: "Alerts", icon: BellRing },
  { id: "connections" as const, label: "Connections", icon: Activity },
  { id: "access" as const, label: "Access Control", icon: ShieldCheck },
  { id: "settings" as const, label: "Settings", icon: Settings },
];

export function AppShell({
  page,
  username,
  role,
  permissions,
  mobileNav,
  selectedStreamConnectionId,
  onNavigate,
  onSelectStream,
  onToggleNav,
  onLogout,
  children,
}: AppShellProps) {
  const { locale, t } = useI18n();
  const [connections, setConnections] = useState<RedisConnection[]>([]);
  const [streams, setStreams] = useState<NavigationStream[]>([]);
  const [commandOpen, setCommandOpen] = useState(false);
  const [commandQuery, setCommandQuery] = useState("");
  const [profileOpen, setProfileOpen] = useState(false);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(() =>
    readMigratedStorage(window.localStorage, SIDEBAR_STORAGE_KEY, LEGACY_SIDEBAR_STORAGE_KEY) === "true",
  );
  const [isMobileViewport, setIsMobileViewport] = useState(() => window.matchMedia(MOBILE_NAV_QUERY).matches);
  const commandInputRef = useRef<HTMLInputElement>(null);
  const mobileMenuTriggerRef = useRef<HTMLButtonElement>(null);
  const mobileMenuCloseRef = useRef<HTMLButtonElement>(null);
  const previousMobileNavRef = useRef(mobileNav);
  const skipMobileTriggerFocusRef = useRef(false);
  const connectionLoadSequence = useRef(0);
  const normalizedCommandQuery = commandQuery.trim().toLowerCase();
  const can = (action: string) => permissions.includes("*") || permissions.includes(action);
  const canOpenAccessControl = role === "admin";
  const canOpenPage = (id: Page) => {
    if (id === "overview" || id === "streams") return can("streams:read");
    if (id === "alerts") return can("alerts:read");
    if (id === "connections") return can("connections:read");
    if (id === "access") return canOpenAccessControl;
    return true;
  };
  const connection = connections.find((item) => item.id === selectedStreamConnectionId) ?? connections[0] ?? null;
  const visibleNavigation = navigation.filter((item) =>
    canOpenPage(item.id)
    && (!normalizedCommandQuery || `${t(item.label)} ${item.label} ${item.id}`.toLowerCase().includes(normalizedCommandQuery)),
  );
  const commandStreams = streams
    .filter((stream) => !normalizedCommandQuery || `${stream.key} ${stream.connectionName} ${stream.connectionId}`.toLowerCase().includes(normalizedCommandQuery))
    .slice(0, 8);

  useEffect(() => {
    const loadConnections = () => {
      const sequence = ++connectionLoadSequence.current;
      api.connections()
        .then(async ({ items }) => {
          const streamResults = await Promise.allSettled(items.map(async (item) => ({
            connection: item,
            streams: (await api.streams(item.id)).items,
          })));
          if (sequence !== connectionLoadSequence.current) return;
          setConnections(items);
          setStreams(streamResults.flatMap((result) => result.status === "fulfilled"
            ? result.value.streams.map((stream) => ({
              ...stream,
              connectionId: result.value.connection.id,
              connectionName: result.value.connection.name,
            }))
            : []));
        })
        .catch(() => {
          if (sequence !== connectionLoadSequence.current) return;
          setConnections([]);
          setStreams([]);
        });
    };
    loadConnections();
    window.addEventListener("redisstreamscope:connections-changed", loadConnections);
    window.addEventListener("redisstreamscope:streams-changed", loadConnections);
    return () => {
      window.removeEventListener("redisstreamscope:connections-changed", loadConnections);
      window.removeEventListener("redisstreamscope:streams-changed", loadConnections);
    };
  }, []);

  useEffect(() => {
    const handleShortcut = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        setCommandOpen((current) => !current);
        setProfileOpen(false);
      }
      if (event.key === "Escape") {
        setCommandOpen(false);
        setProfileOpen(false);
        if (mobileNav) {
          event.preventDefault();
          mobileMenuTriggerRef.current?.focus();
          onToggleNav();
        }
      }
    };
    window.addEventListener("keydown", handleShortcut);
    return () => window.removeEventListener("keydown", handleShortcut);
  }, [mobileNav, onToggleNav]);

  useEffect(() => {
    if (!commandOpen) return;
    setCommandQuery("");
    window.setTimeout(() => commandInputRef.current?.focus(), 0);
  }, [commandOpen]);

  useEffect(() => {
    window.localStorage.setItem(SIDEBAR_STORAGE_KEY, String(sidebarCollapsed));
  }, [sidebarCollapsed]);

  useEffect(() => {
    const mediaQuery = window.matchMedia(MOBILE_NAV_QUERY);
    const handleViewportChange = (event: MediaQueryListEvent) => setIsMobileViewport(event.matches);
    setIsMobileViewport(mediaQuery.matches);
    mediaQuery.addEventListener("change", handleViewportChange);
    return () => mediaQuery.removeEventListener("change", handleViewportChange);
  }, []);

  useEffect(() => {
    const wasOpen = previousMobileNavRef.current;
    previousMobileNavRef.current = mobileNav;
    if (!isMobileViewport || wasOpen === mobileNav) return;

    const focusFrame = window.requestAnimationFrame(() => {
      if (mobileNav) {
        mobileMenuCloseRef.current?.focus();
        return;
      }
      if (skipMobileTriggerFocusRef.current) {
        skipMobileTriggerFocusRef.current = false;
        return;
      }
      mobileMenuTriggerRef.current?.focus();
    });
    return () => window.cancelAnimationFrame(focusFrame);
  }, [isMobileViewport, mobileNav]);

  const openCommandFromMobileNav = () => {
    setCommandOpen(true);
    setProfileOpen(false);
    if (mobileNav) {
      skipMobileTriggerFocusRef.current = true;
      onToggleNav();
    }
  };

  const closeMobileNav = () => {
    mobileMenuTriggerRef.current?.focus();
    onToggleNav();
  };

  const navigate = (nextPage: Page) => {
    if (nextPage === "streams") {
      onSelectStream({ connectionId: selectedStreamConnectionId || connection?.id || "", key: "" });
      window.dispatchEvent(new Event(OPEN_STREAM_CATALOG_EVENT));
    }
    onNavigate(nextPage);
    setCommandOpen(false);
    setProfileOpen(false);
    if (mobileNav) closeMobileNav();
  };

  const selectStream = (stream: NavigationStream) => {
    onSelectStream({ connectionId: stream.connectionId, key: stream.key });
    onNavigate("streams");
    setCommandOpen(false);
    setProfileOpen(false);
    if (mobileNav) closeMobileNav();
  };

  const runFirstCommand = () => {
    const firstPage = visibleNavigation[0];
    if (firstPage) {
      navigate(firstPage.id);
      return;
    }
    const firstStream = commandStreams[0];
    if (firstStream) selectStream(firstStream);
  };

  return (
    <div className={`app-shell ${sidebarCollapsed ? "app-shell--sidebar-collapsed" : ""}`}>
      <header className="topbar">
        <button
          ref={mobileMenuTriggerRef}
          className="mobile-menu"
          onClick={() => { setSidebarCollapsed(false); onToggleNav(); }}
          aria-label={mobileNav ? t("Close menu") : t("Open menu")}
          aria-controls="app-navigation"
          aria-expanded={mobileNav}
        >
          <Menu size={20} />
        </button>
        <button className="sidebar-toggle" onClick={() => setSidebarCollapsed((current) => !current)} aria-label={sidebarCollapsed ? t("Open navigation") : t("Close navigation")} title={sidebarCollapsed ? t("Open navigation") : t("Close navigation")}>
          {sidebarCollapsed ? <PanelLeftOpen size={18} /> : <PanelLeftClose size={18} />}
        </button>
        <button className="wordmark" onClick={() => onNavigate("overview")} aria-label={t("Open overview")}>
          <span className="brand-mark"><Layers3 size={18} /></span>
          <strong>RedisStreamScope</strong>
        </button>
        <div className="connection-select">
          <Database size={14} />
          <strong>{connection?.name ?? "Redis"}</strong>
          <span className={connection?.healthy ? "connected" : "connected disconnected"}><i />{connection?.healthy ? t("Connected") : t("Unavailable")}</span>
        </div>
        <div className="environment">{t("Mode")}: <strong>{connection?.mode ?? "—"}</strong></div>
        <button className="command-search" onClick={() => { setCommandOpen(true); setProfileOpen(false); }} aria-label={t("Open search and command")}>
          <Search size={15} />
          <span>{t("Search / Command")}</span>
          <kbd><Command size={11} /> K</kbd>
        </button>
        <div className="user-menu">
          <button className="profile-trigger" onClick={() => { setProfileOpen((current) => !current); setCommandOpen(false); }} aria-label={t("Open profile menu")} aria-expanded={profileOpen} aria-haspopup="menu">
            <CircleUserRound size={20} />
            <span>{username}</span>
            <ChevronDown size={13} />
          </button>
          {profileOpen ? <>
            <button className="profile-scrim" onClick={() => setProfileOpen(false)} aria-label={t("Close profile menu")} />
            <div className="profile-menu" role="menu">
              <div className="profile-summary"><span>{username.slice(0, 2).toUpperCase()}</span><div><strong>{username}</strong><em>{role}</em></div></div>
              <LanguageSelect className="profile-language" />
              <button role="menuitem" onClick={() => navigate("settings")}><UserRoundCog size={16} /><span><strong>{t("Account settings")}</strong><em>{t("Username and password")}</em></span></button>
              {canOpenAccessControl ? <button role="menuitem" onClick={() => navigate("access")}><ShieldCheck size={16} /><span><strong>{t("Access control")}</strong><em>{t("Users, roles and audit logs")}</em></span></button> : null}
              <button role="menuitem" className="profile-logout" onClick={() => { setProfileOpen(false); onLogout(); }}><LogOut size={16} /><span><strong>{t("Sign out")}</strong><em>{t("End this session")}</em></span></button>
            </div>
          </> : null}
        </div>
      </header>

      <aside
        id="app-navigation"
        className={`sidebar ${mobileNav ? "sidebar--open" : ""}`}
        aria-hidden={sidebarCollapsed || (isMobileViewport && !mobileNav)}
        inert={sidebarCollapsed || (isMobileViewport && !mobileNav)}
      >
        <div className="mobile-sidebar-head">
          <span>{t("Navigation")}</span>
          <button ref={mobileMenuCloseRef} onClick={closeMobileNav} aria-label={t("Close menu")}><X size={18} /></button>
        </div>
        <nav aria-label={t("Main navigation")}>
          {isMobileViewport ? <button onClick={openCommandFromMobileNav}>
            <Search size={16} />
            <span>{t("Search / Command")}</span>
          </button> : null}
          {navigation.filter((item) => canOpenPage(item.id)).map(({ id, label, icon: Icon }) => (
            <button
              key={id}
              className={page === id ? "active" : ""}
              onClick={() => navigate(id)}
            >
              <Icon size={16} />
              <span>{t(label)}</span>
            </button>
          ))}
        </nav>
      </aside>

      {mobileNav ? <button className="nav-scrim" onClick={closeMobileNav} aria-label={t("Close menu")} /> : null}
      <main className="workspace">{children}</main>
      {commandOpen ? <div className="command-backdrop" onMouseDown={() => setCommandOpen(false)}>
        <section className="command-palette" role="dialog" aria-modal="true" aria-label={t("Search and command")} onMouseDown={(event) => event.stopPropagation()}>
          <label className="command-input"><Search size={18} /><input ref={commandInputRef} value={commandQuery} onChange={(event) => setCommandQuery(event.target.value)} onKeyDown={(event) => { if (event.key === "Enter") runFirstCommand(); }} placeholder={t("Search pages or streams…")} /><kbd>ESC</kbd></label>
          <div className="command-results">
            {visibleNavigation.length ? <div className="command-group"><span>{t("Navigation")}</span>{visibleNavigation.map(({ id, label, icon: Icon }) => <button key={id} onClick={() => navigate(id)}><Icon size={17} /><span><strong>{t(label)}</strong><em>{t("Open page")}</em></span><ArrowRight size={15} /></button>)}</div> : null}
            {commandStreams.length ? <div className="command-group"><span>{t("Streams")}</span>{commandStreams.map((stream) => <button key={`${stream.connectionId}:${stream.key}`} onClick={() => selectStream(stream)}><Database size={17} /><span><strong className="mono">{stream.key}</strong><em>{connections.length > 1 ? `${stream.connectionName} · ` : ""}{t("{count} entries", { count: stream.length.toLocaleString(locale) })}</em></span><ArrowRight size={15} /></button>)}</div> : null}
            {!visibleNavigation.length && !commandStreams.length ? <div className="command-empty">{t("No matching pages or streams.")}</div> : null}
          </div>
          <footer><span><kbd>↵</kbd> {t("select")}</span><span><kbd>ESC</kbd> {t("close")}</span></footer>
        </section>
      </div> : null}
    </div>
  );
}
