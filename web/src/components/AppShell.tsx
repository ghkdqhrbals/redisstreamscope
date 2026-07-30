import {
  Activity,
  ArrowRight,
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
import { useDeferredValue, useEffect, useRef, useState } from "react";
import { api } from "../api";
import type { Page, RedisConnection, StreamItem } from "../types";
import { LanguageSelect, useI18n } from "../i18n";
import { readMigratedStorage } from "../storage";

const SIDEBAR_STORAGE_KEY = "redisstreamscope:sidebar-collapsed:v1";
const LEGACY_SIDEBAR_STORAGE_KEY = "streamscope:sidebar-collapsed:v1";

type AppShellProps = {
  page: Page;
  username: string;
  role: "viewer" | "operator" | "admin";
  mobileNav: boolean;
  selectedStreamKey: string;
  onNavigate: (page: Page) => void;
  onSelectStream: (key: string) => void;
  onToggleNav: () => void;
  onLogout: () => void;
  children: React.ReactNode;
};

const navigation = [
  { id: "overview" as const, label: "Overview", icon: Gauge },
  { id: "streams" as const, label: "Streams", icon: Database },
  { id: "connections" as const, label: "Connections", icon: Activity },
  { id: "access" as const, label: "Access Control", icon: ShieldCheck },
  { id: "settings" as const, label: "Settings", icon: Settings },
];

export function AppShell({
  page,
  username,
  role,
  mobileNav,
  selectedStreamKey,
  onNavigate,
  onSelectStream,
  onToggleNav,
  onLogout,
  children,
}: AppShellProps) {
  const { locale, t } = useI18n();
  const [connection, setConnection] = useState<RedisConnection | null>(null);
  const [streams, setStreams] = useState<StreamItem[]>([]);
  const [streamFilter, setStreamFilter] = useState("");
  const [commandOpen, setCommandOpen] = useState(false);
  const [commandQuery, setCommandQuery] = useState("");
  const [profileOpen, setProfileOpen] = useState(false);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(() =>
    readMigratedStorage(window.localStorage, SIDEBAR_STORAGE_KEY, LEGACY_SIDEBAR_STORAGE_KEY) === "true",
  );
  const commandInputRef = useRef<HTMLInputElement>(null);
  const deferredStreamFilter = useDeferredValue(streamFilter.trim().toLowerCase());
  const normalizedCommandQuery = commandQuery.trim().toLowerCase();
  const visibleStreams = deferredStreamFilter
    ? streams.filter((stream) => stream.key.toLowerCase().includes(deferredStreamFilter))
    : streams;
  const visibleNavigation = navigation.filter((item) =>
    (item.id !== "access" || role === "admin")
    && (!normalizedCommandQuery || `${t(item.label)} ${item.label} ${item.id}`.toLowerCase().includes(normalizedCommandQuery)),
  );
  const commandStreams = streams
    .filter((stream) => !normalizedCommandQuery || stream.key.toLowerCase().includes(normalizedCommandQuery))
    .slice(0, 8);

  useEffect(() => {
    const loadConnections = () => {
    api.connections()
      .then(async ({ items }) => {
        const first = items[0] ?? null;
        setConnection(first);
        if (first) setStreams((await api.streams(first.id)).items);
      })
      .catch(() => {
        setConnection(null);
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
      }
    };
    window.addEventListener("keydown", handleShortcut);
    return () => window.removeEventListener("keydown", handleShortcut);
  }, []);

  useEffect(() => {
    if (!commandOpen) return;
    setCommandQuery("");
    window.setTimeout(() => commandInputRef.current?.focus(), 0);
  }, [commandOpen]);

  useEffect(() => {
    window.localStorage.setItem(SIDEBAR_STORAGE_KEY, String(sidebarCollapsed));
  }, [sidebarCollapsed]);

  const navigate = (nextPage: Page) => {
    onNavigate(nextPage);
    setCommandOpen(false);
    setProfileOpen(false);
    if (mobileNav) onToggleNav();
  };

  const selectStream = (key: string) => {
    onSelectStream(key);
    navigate("streams");
  };

  const runFirstCommand = () => {
    const firstPage = visibleNavigation[0];
    if (firstPage) {
      navigate(firstPage.id);
      return;
    }
    const firstStream = commandStreams[0];
    if (firstStream) selectStream(firstStream.key);
  };

  return (
    <div className={`app-shell ${sidebarCollapsed ? "app-shell--sidebar-collapsed" : ""}`}>
      <header className="topbar">
        <button className="mobile-menu" onClick={() => { setSidebarCollapsed(false); onToggleNav(); }} aria-label={t("Open menu")}>
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
              {role === "admin" ? <button role="menuitem" onClick={() => navigate("access")}><ShieldCheck size={16} /><span><strong>{t("Access control")}</strong><em>{t("Users, roles and audit logs")}</em></span></button> : null}
              <button role="menuitem" className="profile-logout" onClick={() => { setProfileOpen(false); onLogout(); }}><LogOut size={16} /><span><strong>{t("Sign out")}</strong><em>{t("End this session")}</em></span></button>
            </div>
          </> : null}
        </div>
      </header>

      <aside className={`sidebar ${mobileNav ? "sidebar--open" : ""}`} aria-hidden={sidebarCollapsed} inert={sidebarCollapsed}>
        <div className="mobile-sidebar-head">
          <span>{t("Navigation")}</span>
          <button onClick={onToggleNav} aria-label={t("Close menu")}><X size={18} /></button>
        </div>
        <nav aria-label={t("Main navigation")}>
          {navigation.filter((item) => item.id !== "access" || role === "admin").map(({ id, label, icon: Icon }) => (
            <button
              key={id}
              className={page === id ? "active" : ""}
              onClick={() => {
                onNavigate(id);
                if (mobileNav) onToggleNav();
              }}
            >
              <Icon size={16} />
              <span>{t(label)}</span>
            </button>
          ))}
        </nav>
        <div className="stream-nav">
          <div className="nav-section-title"><span>STREAMS</span></div>
          <label className="nav-filter"><Search size={13} /><input value={streamFilter} onChange={(event) => setStreamFilter(event.target.value)} placeholder={t("Filter streams…")} aria-label={t("Filter streams")} />{streamFilter ? <button type="button" onClick={() => setStreamFilter("")} aria-label={t("Clear filter")}><X size={13} /></button> : null}</label>
          {visibleStreams.map((stream) => (
            <button
              key={stream.key}
              className={page === "streams" && stream.key === selectedStreamKey ? "stream-link selected" : "stream-link"}
              onClick={() => {
                selectStream(stream.key);
              }}
            >
              <span>{stream.key}</span>
              <em>{stream.length.toLocaleString(locale)}</em>
            </button>
          ))}
          {!visibleStreams.length ? <div className="nav-empty">{streams.length ? t("No streams match this filter.") : t("No streams to display.")}</div> : null}
        </div>
        <div className="connection-health">
          <div><span><i className={connection?.healthy ? "health-dot" : "health-dot health-dot--down"} />{connection?.healthy ? t("Connection healthy") : t("Connection unavailable")}</span><Activity size={15} /></div>
          <p>{connection?.mode ?? "Redis"} <b>·</b> {connection ? `${connection.latencyMs.toFixed(1)} ms` : "—"}</p>
          <p>TLS <b>{connection?.tls ? t("enabled") : t("disabled")}</b></p>
        </div>
      </aside>

      {mobileNav ? <button className="nav-scrim" onClick={onToggleNav} aria-label={t("Close menu")} /> : null}
      <main className="workspace">{children}</main>
      {commandOpen ? <div className="command-backdrop" onMouseDown={() => setCommandOpen(false)}>
        <section className="command-palette" role="dialog" aria-modal="true" aria-label={t("Search and command")} onMouseDown={(event) => event.stopPropagation()}>
          <label className="command-input"><Search size={18} /><input ref={commandInputRef} value={commandQuery} onChange={(event) => setCommandQuery(event.target.value)} onKeyDown={(event) => { if (event.key === "Enter") runFirstCommand(); }} placeholder={t("Search pages or streams…")} /><kbd>ESC</kbd></label>
          <div className="command-results">
            {visibleNavigation.length ? <div className="command-group"><span>{t("Navigation")}</span>{visibleNavigation.map(({ id, label, icon: Icon }) => <button key={id} onClick={() => navigate(id)}><Icon size={17} /><span><strong>{t(label)}</strong><em>{t("Open page")}</em></span><ArrowRight size={15} /></button>)}</div> : null}
            {commandStreams.length ? <div className="command-group"><span>{t("Streams")}</span>{commandStreams.map((stream) => <button key={stream.key} onClick={() => selectStream(stream.key)}><Database size={17} /><span><strong className="mono">{stream.key}</strong><em>{t("{count} entries", { count: stream.length.toLocaleString(locale) })}</em></span><ArrowRight size={15} /></button>)}</div> : null}
            {!visibleNavigation.length && !commandStreams.length ? <div className="command-empty">{t("No matching pages or streams.")}</div> : null}
          </div>
          <footer><span><kbd>↵</kbd> {t("select")}</span><span><kbd>ESC</kbd> {t("close")}</span></footer>
        </section>
      </div> : null}
    </div>
  );
}
