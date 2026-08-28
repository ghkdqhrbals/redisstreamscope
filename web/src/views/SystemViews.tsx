import { FormEvent, useCallback, useEffect, useMemo, useState } from "react";
import { Activity, AlertTriangle, Check, ChevronDown, Copy, Database, KeyRound, LockKeyhole, MoreHorizontal, Pencil, Plus, RefreshCw, Save, Server, ShieldCheck, Trash2, UserRound, X } from "lucide-react";
import { api } from "../api";
import { PasswordForm } from "../components/PasswordForm";
import { emptyRedisConnection, RedisConnectionEditor } from "../components/RedisConnectionEditor";
import { ResizableGrid, type ResizableGridColumn } from "../components/ResizableGrid";
import { useI18n } from "../i18n";
import type { RedisConnection, RedisConnectionConfig, TelemetryToken, ToastState } from "../types";

type ConnectionsViewProps = {
  canReadSettings: boolean;
  canWriteSettings: boolean;
  onToast: (toast: ToastState) => void;
};

export function ConnectionsView({ canReadSettings, canWriteSettings, onToast }: ConnectionsViewProps) {
  const { t } = useI18n();
  const [connections, setConnections] = useState<RedisConnection[]>([]);
  const [configs, setConfigs] = useState<RedisConnectionConfig[]>([]);
  const [editing, setEditing] = useState<RedisConnectionConfig | null>(null);
  const [adding, setAdding] = useState(false);
  const [deleting, setDeleting] = useState<RedisConnectionConfig | null>(null);
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const canConfigure = canReadSettings && canWriteSettings;
  const load = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const [health, settings] = await Promise.all([
        api.connections(),
        canReadSettings ? api.settings() : Promise.resolve({ connections: [] }),
      ]);
      setConnections(health.items);
      setConfigs(settings.connections);
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Unable to load connection health."));
    } finally {
      setLoading(false);
    }
  }, [canReadSettings, t]);
  useEffect(() => { void load(); }, [load]);

  const closeEditor = () => {
    setEditing(null);
    setAdding(false);
  };

  const addConnection = () => {
    let index = 1;
    let draft = emptyRedisConnection(index);
    while (configs.some((connection) => connection.id === draft.id)) {
      index += 1;
      draft = emptyRedisConnection(index);
    }
    setAdding(true);
    setEditing(draft);
  };

  const reconfigureConnection = (connection: RedisConnectionConfig) => {
    setAdding(false);
    setEditing(connection);
  };

  const saveConnection = async () => {
    if (!editing) return;
    setSaving(true);
    setError("");
    try {
      const nextConnections = adding
        ? [...configs, editing]
        : configs.map((connection) => connection.id === editing.id ? editing : connection);
      await api.updateSettings(nextConnections);
      const added = adding;
      closeEditor();
      window.dispatchEvent(new Event("redisstreamscope:connections-changed"));
      await load();
      onToast({
        kind: "success",
        title: added ? t("Redis connection added") : t("Redis connection updated"),
        message: t("The connection was saved and reloaded immediately."),
      });
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Unable to save Redis settings."));
    } finally {
      setSaving(false);
    }
  };

  const testConnection = async (connection: RedisConnectionConfig) => {
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

  const deleteConnection = async () => {
    if (!deleting) return;
    setSaving(true);
    setError("");
    try {
      await api.updateSettings(configs.filter((connection) => connection.id !== deleting.id));
      const deletedName = deleting.name || deleting.id;
      setDeleting(null);
      window.dispatchEvent(new Event("redisstreamscope:connections-changed"));
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
      <div className="page-header"><div><div className="breadcrumbs">{t("Connections")}</div><h1>{t("Redis connections")}</h1><p>{t("Credentials remain in server configuration and are never sent to the browser.")}</p></div><div className="header-actions">{canConfigure ? <button className="accent-button" type="button" onClick={addConnection}><Plus size={14} />{t("Add connection")}</button> : null}<button onClick={() => void load()} disabled={loading}><RefreshCw size={14} />{loading ? t("Checking…") : t("Check health")}</button></div></div>
      {error ? <div className="page-error">{error}</div> : null}
      {connections.map((connection) => {
        const config = configs.find((item) => item.id === connection.id);
        return <section className="connection-card" key={connection.id}>
          <header>
            <div className="connection-icon"><Database size={20} /></div>
            <div><h2>{connection.name}</h2><p>{connection.id}</p></div>
            <span className={connection.healthy ? "health-badge" : "health-badge unhealthy"}><i />{connection.healthy ? t("Healthy") : t("Unavailable")}</span>
            {canConfigure ? <div className="connection-card-actions">
              <button type="button" disabled={!config || testing === connection.id} onClick={() => config && void testConnection(config)}><RefreshCw size={14} />{testing === connection.id ? t("Testing…") : t("Test")}</button>
              <button type="button" disabled={!config} onClick={() => config && reconfigureConnection(config)}><Pencil size={14} />{t("Reconfigure")}</button>
              <details className="connection-overflow">
                <summary aria-label={t("Connection actions")}><MoreHorizontal size={16} /></summary>
                <div role="menu">
                  <button type="button" role="menuitem" className="danger" disabled={!config} onClick={(event) => {
                    event.currentTarget.closest("details")?.removeAttribute("open");
                    if (config) setDeleting(config);
                  }}><Trash2 size={14} />{t("Delete")}</button>
                </div>
              </details>
            </div> : null}
          </header>
          <div className="connection-stats"><div><span>{t("PING latency")}</span><strong>{connection.latencyMs.toFixed(1)} ms</strong></div><div><span>{t("Mode")}</span><strong>{connection.mode}</strong></div><div><span>{t("ACL user")}</span><strong>{connection.username || "default"}</strong></div><div><span>TLS</span><strong>{connection.tls ? t("Enabled") : t("Disabled")}</strong></div></div>
          <details className="connection-advanced">
            <summary><ChevronDown size={15} />{t("Advanced details")}</summary>
            <div className="connection-detail">
              <div><Server size={15} /><span>{t("Connection ID")}</span><strong>{connection.id}</strong></div>
              <div><KeyRound size={15} /><span>{t("Credentials")}</span><strong>{t("Server-side secret")}</strong></div>
              <div><ShieldCheck size={15} /><span>{t("Health check")}</span><strong>{connection.healthy ? t("PING succeeded") : t("PING failed")}</strong></div>
            </div>
          </details>
        </section>;
      })}
      {!connections.length && !loading ? <div className="panel-empty">{t("No Redis connections are configured.")}</div> : null}
      {editing ? <div className="modal-backdrop" onMouseDown={closeEditor}>
        <section className="modal connection-config-modal" role="dialog" aria-modal="true" aria-labelledby="connection-config-title" onMouseDown={(event) => event.stopPropagation()}>
          <header><div><h2 id="connection-config-title">{adding ? t("Add Redis connection") : t("Reconfigure Redis connection")}</h2><p>{t("Changes are saved to CONFIG_PATH and applied immediately.")}</p></div><button type="button" onClick={closeEditor} aria-label={t("Close connection dialog")}><X size={18} /></button></header>
          <div className="connection-config-modal-body">
            <RedisConnectionEditor value={editing} onChange={setEditing} compact idReadOnly={!adding} />
          </div>
          <footer>
            <button type="button" onClick={() => void testConnection(editing)} disabled={Boolean(testing) || saving}><RefreshCw size={14} />{testing ? t("Testing…") : t("Test connection")}</button>
            <button type="button" onClick={closeEditor}>{t("Cancel")}</button>
            <button type="button" className="primary-button" onClick={() => void saveConnection()} disabled={saving || Boolean(testing)}><Save size={14} />{saving ? t("Saving…") : adding ? t("Add connection") : t("Save changes")}</button>
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
  canReadSettings: boolean;
  canWriteSettings: boolean;
  onUsernameChanged: (username: string) => void;
  onToast: (toast: ToastState) => void;
};

export function SettingsView({ username, canReadSettings, canWriteSettings, onUsernameChanged, onToast }: SettingsProps) {
  const { t } = useI18n();
  const [section, setSection] = useState<"connections" | "telemetry" | "account">(canReadSettings ? "connections" : "account");
  return (
    <div className="system-page">
      <div className="page-header"><div><div className="breadcrumbs">{t("Settings")}</div><h1>{t("Console settings")}</h1></div></div>
      <div className="settings-layout">
        <nav aria-label={t("Settings")}>
          {canReadSettings ? <button aria-current={section === "connections" ? "page" : undefined} className={section === "connections" ? "active" : ""} onClick={() => setSection("connections")}><Database size={15} />{t("Redis connections")}</button> : null}
          {canReadSettings ? <button aria-current={section === "telemetry" ? "page" : undefined} className={section === "telemetry" ? "active" : ""} onClick={() => setSection("telemetry")}><Activity size={15} />{t("Request telemetry")}</button> : null}
          <button aria-current={section === "account" ? "page" : undefined} className={section === "account" ? "active" : ""} onClick={() => setSection("account")}><KeyRound size={15} />{t("Account")}</button>
        </nav>
        {section === "connections" && canReadSettings ? <ConnectionSettings canWrite={canWriteSettings} onToast={onToast} /> : null}
        {section === "telemetry" && canReadSettings ? <TelemetrySettings canWrite={canWriteSettings} onToast={onToast} /> : null}
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

function TelemetrySettings({ canWrite, onToast }: { canWrite: boolean; onToast: (toast: ToastState) => void }) {
  const { locale, t } = useI18n();
  const [tokens, setTokens] = useState<TelemetryToken[]>([]);
  const [name, setName] = useState("");
  const [createdToken, setCreatedToken] = useState("");
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState("");
  const [error, setError] = useState("");
  const tokenColumns = useMemo<ResizableGridColumn[]>(() => [
    { id: "name", label: t("Name"), defaultWidth: 210, minWidth: 140, grow: true },
    { id: "prefix", label: t("Token prefix"), defaultWidth: 180, minWidth: 125 },
    { id: "last-used", label: t("Last used"), defaultWidth: 210, minWidth: 145, grow: true },
    { id: "status", label: t("Status"), defaultWidth: 105, minWidth: 88 },
    { id: "actions", label: null, ariaLabel: t("Actions"), defaultWidth: 46, minWidth: 40 },
  ], [t]);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      setTokens((await api.telemetryTokens()).items);
      setError("");
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Unable to load telemetry tokens."));
    } finally {
      setLoading(false);
    }
  }, [t]);

  useEffect(() => { void load(); }, [load]);

  const create = async (event: FormEvent) => {
    event.preventDefault();
    if (!name.trim()) return;
    setBusy("create");
    try {
      const created = await api.createTelemetryToken(name.trim());
      setCreatedToken(created.token);
      setName("");
      await load();
      onToast({ kind: "success", title: t("Telemetry token created"), message: t("Copy the token now; its secret is shown only once.") });
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Unable to create telemetry token."));
    } finally {
      setBusy("");
    }
  };

  const update = async (token: TelemetryToken, enabled: boolean) => {
    setBusy(token.id);
    try {
      await api.updateTelemetryToken(token.id, { name: token.name, enabled });
      await load();
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Unable to update telemetry token."));
    } finally {
      setBusy("");
    }
  };

  const remove = async (token: TelemetryToken) => {
    setBusy(token.id);
    try {
      await api.deleteTelemetryToken(token.id);
      await load();
      onToast({ kind: "success", title: t("Telemetry token deleted"), message: token.name });
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Unable to delete telemetry token."));
    } finally {
      setBusy("");
    }
  };

  const copyToken = async () => {
    if (!createdToken) return;
    await navigator.clipboard.writeText(createdToken);
    onToast({ kind: "success", title: t("Copied"), message: t("Telemetry token copied to the clipboard.") });
  };

  const sampleToken = createdToken || "<TELEMETRY_TOKEN>";
  return <section className="settings-panel telemetry-settings">
    <div className="settings-panel-heading"><div><h2>{t("Request lifecycle telemetry")}</h2><p>{t("Measure application registration, processing, completion, and ACK timestamps without inferring latency from Redis IDs.")}</p></div></div>
    {canWrite ? <form className="telemetry-create" onSubmit={create}>
      <label><span>{t("Token name")}</span><input value={name} onChange={(event) => setName(event.target.value)} maxLength={80} placeholder={t("Production consumers")} required /></label>
      <button className="primary-button" disabled={busy === "create"}><Plus size={14} />{t("Create token")}</button>
    </form> : null}
    {createdToken ? <div className="telemetry-secret" role="status"><div><Check size={17} /><span><strong>{t("Copy this token now")}</strong><em>{t("The secret cannot be shown again.")}</em></span></div><code>{createdToken}</code><button type="button" onClick={() => void copyToken()}><Copy size={14} />{t("Copy")}</button></div> : null}
    {error ? <div className="login-error" role="alert">{error}</div> : null}
    <ResizableGrid className="telemetry-token-list" storageKey="telemetry-tokens" columns={tokenColumns} headerClassName="telemetry-token-head">
      {tokens.map((token) => <div className="telemetry-token-row" key={token.id}>
        <strong data-label={t("Name")}>{token.name}</strong><code data-label={t("Token prefix")}>{token.prefix}…</code><span data-label={t("Last used")}>{token.lastUsedAt ? new Date(token.lastUsedAt).toLocaleString(locale, { hour12: false }) : t("Never")}</span>{canWrite ? <button type="button" className={token.enabled ? "status-pill active" : "status-pill"} data-label={t("Status")} disabled={busy === token.id} onClick={() => void update(token, !token.enabled)}>{token.enabled ? t("Enabled") : t("Disabled")}</button> : <span data-label={t("Status")}>{token.enabled ? t("Enabled") : t("Disabled")}</span>}{canWrite ? <button type="button" className="icon-danger" aria-label={t("Delete telemetry token")} disabled={busy === token.id} onClick={() => void remove(token)}><Trash2 size={14} /></button> : <span>—</span>}
      </div>)}
      {!loading && !tokens.length ? <div className="panel-empty">{t("No telemetry tokens.")}</div> : null}
      {loading ? <div className="panel-empty">{t("Loading…")}</div> : null}
    </ResizableGrid>
    <div className="telemetry-example"><header><strong>POST /api/telemetry/lifecycle</strong><span>Authorization: Bearer</span></header><pre>{`curl -X POST "${window.location.origin}/api/telemetry/lifecycle" \\\n  -H "Authorization: Bearer ${sampleToken}" \\\n  -H "Content-Type: application/json" \\\n  -d '{"events":[{\n    "traceId":"request-123",\n    "connectionId":"default",\n    "streamKey":"orders",\n    "groupName":"workers",\n    "registeredAt":"2026-08-25T01:00:00Z",\n    "processingStartedAt":"2026-08-25T01:00:00.120Z",\n    "processedAt":"2026-08-25T01:00:00.480Z",\n    "acknowledgedAt":"2026-08-25T01:00:00.500Z",\n    "outcome":"success"\n  }]}'`}</pre></div>
  </section>;
}

function ConnectionSettings({ canWrite, onToast }: { canWrite: boolean; onToast: (toast: ToastState) => void }) {
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
      window.dispatchEvent(new Event("redisstreamscope:connections-changed"));
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
        {canWrite ? <button type="button" onClick={() => setConnections((current) => [...current, emptyRedisConnection(current.length + 1)])}><Plus size={14} />{t("Add connection")}</button> : null}
      </div>
      {configPath ? <div className="config-path-row"><span>CONFIG_PATH</span><code>{configPath}</code></div> : null}
      {loading ? <div className="panel-empty">{t("Loading Redis settings…")}</div> : null}
      {!loading && connections.map((connection, index) => (
        <fieldset className="connection-editor-permission-boundary" disabled={!canWrite} key={`${connection.id}-${index}`}><div className="connection-editor-wrap">
          <RedisConnectionEditor
            value={connection}
            onChange={(next) => update(index, next)}
            onRemove={canWrite ? () => setConnections((current) => current.filter((_, itemIndex) => itemIndex !== index)) : undefined}
            compact
          />
          {canWrite ? <button className="connection-test-button" type="button" disabled={testing === connection.id} onClick={() => void test(connection)}>
            <RefreshCw size={13} />{testing === connection.id ? t("Testing…") : t("Test connection")}
          </button> : null}
        </div></fieldset>
      ))}
      {!loading && connections.length === 0 ? <div className="panel-empty">{t("There are no Redis connections. RedisStreamScope will run, but readiness will be degraded.")}</div> : null}
      {error ? <div className="login-error" role="alert">{error}</div> : null}
      {canWrite ? <div className="connection-settings-footer">
        <span>{t("Saving atomically replaces the properties file without exposing existing passwords to the browser.")}</span>
        <button className="primary-button" type="button" disabled={loading || saving} onClick={() => void save()}><Save size={14} />{saving ? t("Saving…") : t("Save configuration")}</button>
      </div> : null}
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
