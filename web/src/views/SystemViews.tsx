import { FormEvent, useCallback, useEffect, useState } from "react";
import { AlertTriangle, Database, KeyRound, LockKeyhole, Pencil, Plus, RefreshCw, Save, Server, Settings2, ShieldCheck, Trash2, UserRound, X } from "lucide-react";
import { api } from "../api";
import { PasswordForm } from "../components/PasswordForm";
import { emptyRedisConnection, RedisConnectionEditor } from "../components/RedisConnectionEditor";
import { LanguageSelect, useI18n } from "../i18n";
import type { RedisConnection, RedisConnectionConfig, ToastState } from "../types";

type ConnectionsViewProps = {
  role: "viewer" | "operator" | "admin";
  onToast: (toast: ToastState) => void;
};

export function ConnectionsView({ role, onToast }: ConnectionsViewProps) {
  const { t } = useI18n();
  const [connections, setConnections] = useState<RedisConnection[]>([]);
  const [configs, setConfigs] = useState<RedisConnectionConfig[]>([]);
  const [editing, setEditing] = useState<RedisConnectionConfig | null>(null);
  const [deleting, setDeleting] = useState<RedisConnectionConfig | null>(null);
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const load = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const [health, settings] = await Promise.all([
        api.connections(),
        role === "admin" ? api.settings() : Promise.resolve({ connections: [] }),
      ]);
      setConnections(health.items);
      setConfigs(settings.connections);
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Unable to load connection health."));
    } finally {
      setLoading(false);
    }
  }, [role, t]);
  useEffect(() => { void load(); }, [load]);

  const saveConnection = async () => {
    if (!editing) return;
    setSaving(true);
    setError("");
    try {
      await api.updateSettings(configs.map((connection) => connection.id === editing.id ? editing : connection));
      setEditing(null);
      window.dispatchEvent(new Event("streamscope:connections-changed"));
      await load();
      onToast({ kind: "success", title: t("Redis connection updated"), message: t("The connection was saved and reloaded immediately.") });
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Unable to save Redis settings."));
    } finally {
      setSaving(false);
    }
  };

  const testConnection = async () => {
    if (!editing) return;
    setTesting(true);
    try {
      const result = await api.testRedis(editing);
      onToast({ kind: "success", title: t("Redis connection verified"), message: `${editing.name || editing.id} · ${result.latencyMs.toFixed(1)} ms` });
    } catch (cause) {
      onToast({ kind: "error", title: t("Connection failed"), message: cause instanceof Error ? t(cause.message) : t("Redis connection failed.") });
    } finally {
      setTesting(false);
    }
  };

  const deleteConnection = async () => {
    if (!deleting) return;
    setSaving(true);
    setError("");
    try {
      await api.updateSettings(configs.filter((connection) => connection.id !== deleting.id));
      const deletedName = deleting.name || deleting.id;
      setDeleting(null);
      window.dispatchEvent(new Event("streamscope:connections-changed"));
      await load();
      onToast({ kind: "success", title: t("Redis connection deleted"), message: t("Deleted {name} and reloaded the connection list.", { name: deletedName }) });
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Unable to delete the Redis connection."));
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="system-page">
      <div className="page-header"><div><div className="breadcrumbs">{t("Connections")}</div><h1>{t("Redis connections")}</h1><p>{t("Credentials remain in server configuration and are never sent to the browser.")}</p></div><div className="header-actions"><button onClick={() => void load()} disabled={loading}><RefreshCw size={14} />{loading ? t("Checking…") : t("Check health")}</button></div></div>
      {error ? <div className="page-error">{error}</div> : null}
      {connections.map((connection) => <section className="connection-card" key={connection.id}>
        <header>
          <div className="connection-icon"><Database size={20} /></div>
          <div><h2>{connection.name}</h2><p>{connection.mode} · {connection.id}</p></div>
          <span className={connection.healthy ? "health-badge" : "health-badge unhealthy"}><i />{connection.healthy ? t("Healthy") : t("Unavailable")}</span>
          {role === "admin" ? <div className="connection-card-actions">
            <button type="button" onClick={() => setEditing(configs.find((item) => item.id === connection.id) ?? null)}><Pencil size={14} />{t("Reconfigure")}</button>
            <button type="button" className="danger" onClick={() => setDeleting(configs.find((item) => item.id === connection.id) ?? null)}><Trash2 size={14} />{t("Delete")}</button>
          </div> : null}
        </header>
        <div className="connection-stats"><div><span>{t("Latency")}</span><strong>{connection.latencyMs.toFixed(1)} ms</strong></div><div><span>{t("Mode")}</span><strong>{connection.mode}</strong></div><div><span>{t("ACL user")}</span><strong>{connection.username || "default"}</strong></div><div><span>TLS</span><strong>{connection.tls ? t("Enabled") : t("Disabled")}</strong></div></div>
        <div className="connection-detail">
          <div><Server size={15} /><span>{t("Connection ID")}</span><strong>{connection.id}</strong></div>
          <div><KeyRound size={15} /><span>{t("Credentials")}</span><strong>{t("Server-side secret")}</strong></div>
          <div><LockKeyhole size={15} /><span>TLS</span><strong className={connection.tls ? "green" : ""}>{connection.tls ? t("Enabled") : t("Disabled")}</strong></div>
          <div><ShieldCheck size={15} /><span>{t("Health check")}</span><strong>{connection.healthy ? t("PING succeeded") : t("PING failed")}</strong></div>
        </div>
      </section>)}
      {!connections.length && !loading ? <div className="panel-empty">{t("No Redis connections are configured.")}</div> : null}
      {editing ? <div className="modal-backdrop" onMouseDown={() => setEditing(null)}>
        <section className="modal connection-config-modal" role="dialog" aria-modal="true" aria-labelledby="connection-config-title" onMouseDown={(event) => event.stopPropagation()}>
          <header><div><h2 id="connection-config-title">{t("Reconfigure Redis connection")}</h2><p>{t("Changes are saved to CONFIG_PATH and applied immediately.")}</p></div><button type="button" onClick={() => setEditing(null)} aria-label={t("Close connection dialog")}><X size={18} /></button></header>
          <div className="connection-config-modal-body">
            <RedisConnectionEditor value={editing} onChange={setEditing} compact idReadOnly />
          </div>
          <footer>
            <button type="button" onClick={() => void testConnection()} disabled={testing || saving}><RefreshCw size={14} />{testing ? t("Testing…") : t("Test connection")}</button>
            <button type="button" onClick={() => setEditing(null)}>{t("Cancel")}</button>
            <button type="button" className="primary-button" onClick={() => void saveConnection()} disabled={saving || testing}><Save size={14} />{saving ? t("Saving…") : t("Save changes")}</button>
          </footer>
        </section>
      </div> : null}
      {deleting ? <div className="modal-backdrop" onMouseDown={() => setDeleting(null)}>
        <section className="modal delete-connection-modal" role="alertdialog" aria-modal="true" aria-labelledby="delete-connection-title" onMouseDown={(event) => event.stopPropagation()}>
          <header><div><h2 id="delete-connection-title">{t("Delete Redis connection")}</h2><p>{deleting.name || deleting.id}</p></div><button type="button" onClick={() => setDeleting(null)} aria-label={t("Close connection dialog")}><X size={18} /></button></header>
          <div className="delete-connection-warning"><AlertTriangle size={20} /><p>{t("This removes the connection from CONFIG_PATH and immediately stops its active Redis sessions.")}</p></div>
          <footer><button type="button" onClick={() => setDeleting(null)}>{t("Cancel")}</button><button type="button" className="danger-button" onClick={() => void deleteConnection()} disabled={saving}><Trash2 size={14} />{saving ? t("Deleting…") : t("Delete connection")}</button></footer>
        </section>
      </div> : null}
    </div>
  );
}

type SettingsProps = {
  username: string;
  role: "viewer" | "operator" | "admin";
  onUsernameChanged: (username: string) => void;
  onToast: (toast: ToastState) => void;
};

export function SettingsView({ username, role, onUsernameChanged, onToast }: SettingsProps) {
  const { t } = useI18n();
  const [section, setSection] = useState<"connections" | "general" | "account">(role === "admin" ? "connections" : "account");
  return (
    <div className="system-page">
      <div className="page-header"><div><div className="breadcrumbs">{t("Settings")}</div><h1>{t("Console settings")}</h1></div></div>
      <div className="settings-layout">
        <nav>
          {role === "admin" ? <button className={section === "connections" ? "active" : ""} onClick={() => setSection("connections")}><Database size={15} />{t("Redis connections")}</button> : null}
          <button className={section === "account" ? "active" : ""} onClick={() => setSection("account")}><KeyRound size={15} />{t("Account")}</button>
          <button className={section === "general" ? "active" : ""} onClick={() => setSection("general")}><Settings2 size={15} />{t("General")}</button>
        </nav>
        {section === "connections" && role === "admin" ? <ConnectionSettings onToast={onToast} /> : null}
        {section === "general" ? <section className="settings-panel">
          <h2>{t("General")}</h2>
          <div className="setting-row"><div><strong>{t("Language")}</strong><span>{t("Changes apply immediately and are saved in this browser.")}</span></div><LanguageSelect className="settings-language" /></div>
        </section> : null}
        {section === "account" ? <section className="settings-panel account-settings">
          <h2>{t("Administrator username")}</h2>
          <UsernameForm username={username} onChanged={onUsernameChanged} onToast={onToast} />
          <div className="settings-divider" />
          <h2>{t("Change password")}</h2>
          <PasswordForm onChanged={() => onToast({ kind: "success", title: t("Password changed"), message: t("The password was changed and other sign-in sessions were ended.") })} />
        </section> : null}
      </div>
    </div>
  );
}

function ConnectionSettings({ onToast }: { onToast: (toast: ToastState) => void }) {
  const { t } = useI18n();
  const [connections, setConnections] = useState<RedisConnectionConfig[]>([]);
  const [configPath, setConfigPath] = useState("");
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState("");
  const [error, setError] = useState("");

  const load = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const result = await api.settings();
      setConnections(result.connections);
      setConfigPath(result.configPath);
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Unable to load settings."));
    } finally {
      setLoading(false);
    }
  }, [t]);

  useEffect(() => { void load(); }, [load]);

  const update = (index: number, connection: RedisConnectionConfig) => {
    setConnections((current) => current.map((item, itemIndex) => itemIndex === index ? connection : item));
  };

  const test = async (connection: RedisConnectionConfig) => {
    setTesting(connection.id);
    try {
      const result = await api.testRedis(connection);
      onToast({ kind: "success", title: t("Redis connection verified"), message: `${connection.name || connection.id} · ${result.latencyMs.toFixed(1)} ms` });
    } catch (cause) {
      onToast({ kind: "error", title: t("Connection failed"), message: cause instanceof Error ? t(cause.message) : t("Redis connection failed.") });
    } finally {
      setTesting("");
    }
  };

  const save = async () => {
    setSaving(true);
    setError("");
    try {
      await api.updateSettings(connections);
      await load();
      window.dispatchEvent(new Event("streamscope:connections-changed"));
      onToast({ kind: "success", title: t("Configuration saved"), message: t("Saved to CONFIG_PATH and reloaded Redis connections immediately.") });
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Unable to save Redis settings."));
    } finally {
      setSaving(false);
    }
  };

  return (
    <section className="settings-panel connection-settings">
      <div className="settings-panel-heading">
        <h2>{t("Redis connections")}</h2>
        <button type="button" onClick={() => setConnections((current) => [...current, emptyRedisConnection(current.length + 1)])}><Plus size={14} />{t("Add connection")}</button>
      </div>
      {configPath ? <div className="config-path-row"><span>CONFIG_PATH</span><code>{configPath}</code></div> : null}
      {loading ? <div className="panel-empty">{t("Loading Redis settings…")}</div> : null}
      {!loading && connections.map((connection, index) => (
        <div className="connection-editor-wrap" key={`${connection.id}-${index}`}>
          <RedisConnectionEditor
            value={connection}
            onChange={(next) => update(index, next)}
            onRemove={() => setConnections((current) => current.filter((_, itemIndex) => itemIndex !== index))}
            compact
          />
          <button className="connection-test-button" type="button" disabled={testing === connection.id} onClick={() => void test(connection)}>
            <RefreshCw size={13} />{testing === connection.id ? t("Testing…") : t("Test connection")}
          </button>
        </div>
      ))}
      {!loading && connections.length === 0 ? <div className="panel-empty">{t("There are no Redis connections. StreamScope will run, but readiness will be degraded.")}</div> : null}
      {error ? <div className="login-error" role="alert">{error}</div> : null}
      <div className="connection-settings-footer">
        <span>{t("Saving atomically replaces the properties file without exposing existing passwords to the browser.")}</span>
        <button className="primary-button" type="button" disabled={loading || saving} onClick={() => void save()}><Save size={14} />{saving ? t("Saving…") : t("Save configuration")}</button>
      </div>
    </section>
  );
}

function UsernameForm({ username, onChanged, onToast }: {
  username: string;
  onChanged: (username: string) => void;
  onToast: (toast: ToastState) => void;
}) {
  const { t } = useI18n();
  const [nextUsername, setNextUsername] = useState(username);
  const [currentPassword, setCurrentPassword] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    setBusy(true);
    setError("");
    try {
      const result = await api.changeUsername(currentPassword, nextUsername.trim());
      onChanged(result.username);
      setCurrentPassword("");
      onToast({ kind: "success", title: t("Username changed"), message: t("The sign-in username was changed to {username}.", { username: result.username }) });
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Unable to change the username."));
    } finally {
      setBusy(false);
    }
  };

  return (
    <form className="username-form" onSubmit={submit}>
      <label><span><UserRound size={13} />{t("New username")}</span><input value={nextUsername} minLength={3} onChange={(event) => setNextUsername(event.target.value)} autoComplete="username" required /></label>
      <label><span><LockKeyhole size={13} />{t("Current password")}</span><input type="password" value={currentPassword} onChange={(event) => setCurrentPassword(event.target.value)} autoComplete="current-password" required /></label>
      {error ? <div className="login-error" role="alert">{error}</div> : null}
      <button className="primary-button" disabled={busy || nextUsername.trim() === username}>{busy ? t("Changing…") : t("Change username")}</button>
    </form>
  );
}
