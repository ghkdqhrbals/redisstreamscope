import { useEffect, useState } from "react";
import { AlertTriangle, CheckCircle2, X, XCircle } from "lucide-react";
import { api } from "./api";
import { AppShell } from "./components/AppShell";
import { LoginView } from "./components/LoginView";
import { PasswordChangeView } from "./components/PasswordChangeView";
import { SetupWizard } from "./components/SetupWizard";
import type { Page, RedisConnectionConfig, ToastState } from "./types";
import { GroupsView } from "./views/GroupsView";
import { OverviewView } from "./views/OverviewView";
import { StreamsView } from "./views/StreamsView";
import { ConnectionsView, SettingsView } from "./views/SystemViews";
import { AccessControlView } from "./views/AccessControlView";
import { useI18n } from "./i18n";

const DEV_SESSION_KEY = "streamscope:dev-session";

export function App() {
  const { t } = useI18n();
  const [setupRequired, setSetupRequired] = useState<boolean | null>(null);
  const [configPath, setConfigPath] = useState("/data/config.properties");
  const [initialConnection, setInitialConnection] = useState<RedisConnectionConfig | undefined>();
  const [authenticated, setAuthenticated] = useState<boolean | null>(null);
  const [username, setUsername] = useState("admin");
  const [role, setRole] = useState<"viewer" | "operator" | "admin">("admin");
  const [passwordChangeRequired, setPasswordChangeRequired] = useState(false);
  const [loginBusy, setLoginBusy] = useState(false);
  const [loginError, setLoginError] = useState("");
  const [page, setPage] = useState<Page>("streams");
  const [selectedStreamKey, setSelectedStreamKey] = useState("");
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
        if (session.username) setUsername(session.username);
        if (session.role) setRole(session.role);
        setPasswordChangeRequired(Boolean(session.passwordChangeRequired));
      })
      .catch(() => {
        if (!active) return;
        setSetupRequired(false);
        setAuthenticated(import.meta.env.DEV && sessionStorage.getItem(DEV_SESSION_KEY) === "true");
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
      setUsername(session.username ?? nextUsername);
      setRole(session.role ?? "admin");
      setPasswordChangeRequired(Boolean(session.passwordChangeRequired));
      setAuthenticated(true);
    } catch (error) {
      if (import.meta.env.DEV && nextUsername && password) {
        sessionStorage.setItem(DEV_SESSION_KEY, "true");
        setUsername(nextUsername);
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
    setAuthenticated(false);
  };

  const setupComplete = (session: Awaited<ReturnType<typeof api.setup>>) => {
    setSetupRequired(false);
    setAuthenticated(true);
    if (session.username) setUsername(session.username);
    if (session.role) setRole(session.role);
    setPasswordChangeRequired(false);
  };

  if (setupRequired === null || authenticated === null) {
    return <div className="app-loading"><span className="brand-loader" />{t("Preparing StreamScope…")}</div>;
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
        mobileNav={mobileNav}
        selectedStreamKey={selectedStreamKey}
        onNavigate={setPage}
        onSelectStream={setSelectedStreamKey}
        onToggleNav={() => setMobileNav((value) => !value)}
        onLogout={logout}
      >
        {page === "overview" ? <OverviewView /> : null}
        {page === "streams" ? <StreamsView selectedStreamKey={selectedStreamKey} onSelectedStreamChange={setSelectedStreamKey} onToast={setToast} /> : null}
        {page === "groups" ? <GroupsView onToast={setToast} /> : null}
        {page === "connections" ? <ConnectionsView /> : null}
        {page === "access" && role === "admin" ? <AccessControlView onToast={setToast} /> : null}
        {page === "settings" ? <SettingsView username={username} role={role} onUsernameChanged={setUsername} onToast={setToast} /> : null}
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
