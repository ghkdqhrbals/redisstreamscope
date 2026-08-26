import { useEffect, useState } from "react";
import { AlertTriangle, CheckCircle2, X, XCircle } from "lucide-react";
import { api } from "./api";
import { AppShell } from "./components/AppShell";
import { LoginView } from "./components/LoginView";
import { PasswordChangeView } from "./components/PasswordChangeView";
import { SetupWizard } from "./components/SetupWizard";
import type { Page, RedisConnectionConfig, ToastState } from "./types";
import { OverviewView } from "./views/OverviewView";
import { StreamsView } from "./views/StreamsView";
import { ConnectionsView, SettingsView } from "./views/SystemViews";
import { AccessControlView } from "./views/AccessControlView";
import { AlertsView } from "./views/AlertsView";
import { useI18n } from "./i18n";
import { readMigratedStorage } from "./storage";

const DEV_SESSION_KEY = "redisstreamscope:dev-session";
const LEGACY_DEV_SESSION_KEY = "streamscope:dev-session";

export function App() {
  const { t } = useI18n();
  const [setupRequired, setSetupRequired] = useState<boolean | null>(null);
  const [configPath, setConfigPath] = useState("/data/config.properties");
  const [initialConnection, setInitialConnection] = useState<RedisConnectionConfig | undefined>();
  const [authenticated, setAuthenticated] = useState<boolean | null>(null);
  const [userId, setUserId] = useState("");
  const [username, setUsername] = useState("admin");
  const [role, setRole] = useState<"viewer" | "operator" | "admin">("admin");
  const [permissions, setPermissions] = useState<string[]>([]);
  const [passwordChangeRequired, setPasswordChangeRequired] = useState(false);
  const [loginBusy, setLoginBusy] = useState(false);
  const [loginError, setLoginError] = useState("");
  const [page, setPage] = useState<Page>("streams");
  const [selectedStreamKey, setSelectedStreamKey] = useState("");
  const [selectedStreamConnectionId, setSelectedStreamConnectionId] = useState("");
  const [streamFocus, setStreamFocus] = useState<"groups" | null>(null);
  const [streamFocusGroup, setStreamFocusGroup] = useState("");
  const [mobileNav, setMobileNav] = useState(false);
  const [toast, setToast] = useState<ToastState | null>(null);

  useEffect(() => {
    let active = true;
    api.setupStatus()
      .then(async (status) => {
        if (!active) return;
        setSetupRequired(status.setupRequired);
        setConfigPath(status.configPath);
        setInitialConnection(status.connections?.[0]);
        if (status.setupRequired) {
          setAuthenticated(false);
          return;
        }
        const session = await api.session();
        if (!active) return;
        setAuthenticated(session.authenticated);
        setUserId(session.userId ?? "");
        if (session.username) setUsername(session.username);
        if (session.role) setRole(session.role);
        setPermissions(session.permissions ?? permissionsForRole(session.role));
        setPasswordChangeRequired(Boolean(session.passwordChangeRequired));
      })
      .catch(() => {
        if (!active) return;
        setSetupRequired(false);
        const developmentSession = import.meta.env.DEV && readMigratedStorage(sessionStorage, DEV_SESSION_KEY, LEGACY_DEV_SESSION_KEY) === "true";
        setAuthenticated(developmentSession);
        if (developmentSession) setPermissions(["*"]);
      });
    return () => { active = false; };
  }, []);

  useEffect(() => {
    if (!toast) return;
    const timeout = window.setTimeout(() => setToast(null), 4000);
    return () => window.clearTimeout(timeout);
  }, [toast]);

  const login = async (nextUsername: string, password: string) => {
    setLoginBusy(true);
    setLoginError("");
    try {
      const session = await api.login(nextUsername, password);
      setUserId(session.userId ?? "");
      setUsername(session.username ?? nextUsername);
      setRole(session.role ?? "admin");
      setPermissions(session.permissions ?? permissionsForRole(session.role));
      setPasswordChangeRequired(Boolean(session.passwordChangeRequired));
      setAuthenticated(true);
    } catch (error) {
      if (import.meta.env.DEV && nextUsername && password) {
        sessionStorage.setItem(DEV_SESSION_KEY, "true");
        setUsername(nextUsername);
        setPermissions(["*"]);
        setAuthenticated(true);
      } else {
        setLoginError(error instanceof Error ? t(error.message) : t("Sign-in failed."));
      }
    } finally {
      setLoginBusy(false);
    }
  };

  const logout = async () => {
    await api.logout().catch(() => undefined);
    sessionStorage.removeItem(DEV_SESSION_KEY);
    sessionStorage.removeItem(LEGACY_DEV_SESSION_KEY);
    setPermissions([]);
    setUserId("");
    setAuthenticated(false);
  };

  const setupComplete = (session: Awaited<ReturnType<typeof api.setup>>) => {
    setSetupRequired(false);
    setAuthenticated(true);
    setUserId(session.userId ?? "");
    if (session.username) setUsername(session.username);
    if (session.role) setRole(session.role);
    setPermissions(session.permissions ?? permissionsForRole(session.role));
    setPasswordChangeRequired(false);
  };

  if (setupRequired === null || authenticated === null) {
    return <div className="app-loading"><span className="brand-loader" />{t("Preparing RedisStreamScope…")}</div>;
  }

  if (setupRequired) {
    return <SetupWizard configPath={configPath} initialConnection={initialConnection} onComplete={setupComplete} />;
  }

  if (!authenticated) {
    return <LoginView busy={loginBusy} error={loginError} onLogin={login} />;
  }

  if (passwordChangeRequired) {
    return <PasswordChangeView username={username} onChanged={() => {
      setPasswordChangeRequired(false);
      setPage("settings");
    }} />;
  }

  return (
    <>
      <AppShell
        page={page}
        username={username}
        role={role}
        permissions={permissions}
        mobileNav={mobileNav}
        selectedStreamConnectionId={selectedStreamConnectionId}
        onNavigate={(nextPage) => {
          if (nextPage !== "streams") {
            setStreamFocus(null);
            setStreamFocusGroup("");
          }
          setPage(nextPage);
        }}
        onSelectStream={(target) => {
          setSelectedStreamConnectionId(target.connectionId);
          setStreamFocus(null);
          setStreamFocusGroup("");
          setSelectedStreamKey(target.key);
        }}
        onToggleNav={() => setMobileNav((value) => !value)}
        onLogout={logout}
      >
        {page === "overview" ? <OverviewView currentUserId={userId} role={role} canWrite={hasPermission(permissions, "streams:write")} onToast={setToast} onOpenGroups={(target) => {
          setSelectedStreamConnectionId(target.connectionId);
          setSelectedStreamKey(target.key);
          setStreamFocus("groups");
          setStreamFocusGroup(target.groupName ?? "");
          setPage("streams");
        }} /> : null}
        {page === "streams" ? <StreamsView
          key={selectedStreamConnectionId || "default"}
          selectedConnectionId={selectedStreamConnectionId}
          selectedStreamKey={selectedStreamKey}
          focusSection={streamFocus}
          focusGroup={streamFocusGroup}
          canWrite={hasPermission(permissions, "streams:write")}
          canManageGroups={hasPermission(permissions, "groups:manage")}
          onSelectedStreamChange={(target) => {
            setSelectedStreamConnectionId(target.connectionId);
            setSelectedStreamKey(target.key);
          }}
          onToast={setToast}
        /> : null}
        {page === "alerts" ? <AlertsView canWrite={hasPermission(permissions, "alerts:write")} onToast={setToast} /> : null}
        {page === "connections" ? <ConnectionsView canReadSettings={hasPermission(permissions, "settings:read")} canWriteSettings={hasPermission(permissions, "settings:write")} onToast={setToast} /> : null}
        {page === "access" && role === "admin" ? <AccessControlView onToast={setToast} /> : null}
        {page === "settings" ? <SettingsView username={username} canReadSettings={hasPermission(permissions, "settings:read")} canWriteSettings={hasPermission(permissions, "settings:write")} onUsernameChanged={setUsername} onToast={setToast} /> : null}
      </AppShell>
      {toast ? (
        <div className={`toast toast--${toast.kind}`} role="status">
          {toast.kind === "success" ? <CheckCircle2 size={18} /> : null}
          {toast.kind === "warning" ? <AlertTriangle size={18} /> : null}
          {toast.kind === "error" ? <XCircle size={18} /> : null}
          <div><strong>{toast.title}</strong><span>{toast.message}</span></div>
          <button onClick={() => setToast(null)} aria-label={t("Dismiss notification")}><X size={15} /></button>
        </div>
      ) : null}
    </>
  );
}

function hasPermission(permissions: string[], action: string) {
  return permissions.includes("*") || permissions.includes(action);
}

function permissionsForRole(role: "viewer" | "operator" | "admin" | undefined) {
  if (role === "admin") return ["*"];
  const shared = ["profile:write", "connections:read", "streams:read", "groups:read", "alerts:read"];
  return role === "operator" ? [...shared, "streams:write", "groups:manage"] : shared;
}
