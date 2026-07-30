import { FormEvent, useCallback, useEffect, useMemo, useState } from "react";
import {
  Activity,
  Check,
  Download,
  KeyRound,
  LockKeyhole,
  MoreHorizontal,
  Pencil,
  Plus,
  RefreshCw,
  Search,
  ShieldCheck,
  UserCheck,
  UserRoundCog,
  UsersRound,
  X,
} from "lucide-react";
import { api } from "../api";
import { ResizableGrid, type ResizableGridColumn } from "../components/ResizableGrid";
import { useI18n } from "../i18n";
import type { ToastState } from "../types";

type AccessUser = {
  id: string;
  username: string;
  displayName: string;
  role: string;
  enabled: boolean;
  lastLoginAt?: string;
};

type Grant = { id: number; userId: string; action: string; scope: string; effect: "allow" | "deny" };

export function AccessControlView({ onToast }: { onToast: (toast: ToastState) => void }) {
  const { locale, t } = useI18n();
  const [tab, setTab] = useState<"users" | "roles" | "logs">("users");
  const [users, setUsers] = useState<AccessUser[]>([]);
  const [logs, setLogs] = useState<Array<Record<string, string | number>>>([]);
  const [grants, setGrants] = useState<Grant[]>([]);
  const [search, setSearch] = useState("");
  const [roleFilter, setRoleFilter] = useState("all");
  const [statusFilter, setStatusFilter] = useState("all");
  const [showCreate, setShowCreate] = useState(false);
  const [editingUser, setEditingUser] = useState<AccessUser | null>(null);

  const load = useCallback(() => {
    Promise.all([api.users(), api.accessLogs(), api.grants()])
      .then(([userResponse, logResponse, grantResponse]) => {
        setUsers(userResponse.items);
        setLogs(logResponse.items);
        setGrants(grantResponse.items);
      })
      .catch((error) => onToast({ kind: "error", title: t("Access data unavailable"), message: error instanceof Error ? t(error.message) : t("Unable to load administration data.") }));
  }, [onToast, t]);

  useEffect(() => { load(); }, [load]);

  const filteredUsers = useMemo(
    () => users.filter((user) =>
      `${user.username} ${user.displayName} ${user.role}`.toLowerCase().includes(search.toLowerCase())
      && (roleFilter === "all" || user.role === roleFilter)
      && (statusFilter === "all" || (statusFilter === "active" ? user.enabled : !user.enabled))),
    [roleFilter, search, statusFilter, users],
  );

  const updateRole = async (user: AccessUser, role: string) => {
    setUsers((current) => current.map((item) => item.id === user.id ? { ...item, role } : item));
    try {
      const updated = await api.updateUser(user.id, { username: user.username, displayName: user.displayName, role, enabled: user.enabled });
      setUsers((current) => current.map((item) => item.id === user.id ? updated : item));
      onToast({ kind: "success", title: t("Role updated"), message: t("Changed {username}'s base role to {role}.", { username: user.username, role }) });
    } catch (error) {
      setUsers((current) => current.map((item) => item.id === user.id ? user : item));
      onToast({ kind: "error", title: t("Role update failed"), message: error instanceof Error ? t(error.message) : t("Unable to change the role.") });
    }
  };

  const allowedRequests = logs.filter((log) => Number(log.status) < 400).length;
  const allowedPercent = logs.length ? ((allowedRequests / logs.length) * 100).toFixed(1) : "0.0";
  const userColumns = useMemo<ResizableGridColumn[]>(() => [
    { id: "user", label: t("User"), defaultWidth: 230, minWidth: 170, grow: true },
    { id: "role", label: t("Role"), defaultWidth: 125, minWidth: 105 },
    { id: "access", label: t("Resource access"), defaultWidth: 220, minWidth: 155 },
    { id: "last-login", label: t("Last login"), defaultWidth: 145, minWidth: 115 },
    { id: "status", label: t("Status"), defaultWidth: 110, minWidth: 90 },
    { id: "actions", label: null, ariaLabel: t("Actions"), defaultWidth: 40, minWidth: 32 },
  ], [t]);

  return (
    <div className="access-page">
      <div className="page-header">
        <div><div className="breadcrumbs">{t("Administration")} <span>/</span> {t("Access Control")}</div><h1>{t("Users & access")}</h1></div>
        <div className="header-actions"><button onClick={() => exportPermissionReport(users, grants)}><Download size={14} />{t("Permission report")}</button><button className="accent-button" onClick={() => setShowCreate(true)}><Plus size={14} />{t("Add user")}</button></div>
      </div>
      <div className="access-summary">
        <div><UsersRound size={17} /><span>{t("Users")}</span><strong>{users.length}</strong><em>{t("{count} active", { count: users.filter((user) => user.enabled).length })}</em></div>
        <div><UserRoundCog size={17} /><span>{t("Administrators")}</span><strong>{users.filter((user) => user.role === "admin").length}</strong><em>{t("Full access")}</em></div>
        <div><KeyRound size={17} /><span>{t("Custom grants")}</span><strong>{grants.length}</strong><em>{t("{count} explicit deny", { count: grants.filter((grant) => grant.effect === "deny").length })}</em></div>
        <div><Activity size={17} /><span>{t("Recent requests")}</span><strong>{logs.length}</strong><em className="green">{t("{percent}% allowed", { percent: allowedPercent })}</em></div>
      </div>
      <div className="content-tabs access-tabs">
        <button className={tab === "users" ? "active" : ""} onClick={() => setTab("users")}>{t("Users")}</button>
        <button className={tab === "roles" ? "active" : ""} onClick={() => setTab("roles")}>{t("Roles & permissions")}</button>
        <button className={tab === "logs" ? "active" : ""} onClick={() => setTab("logs")}>{t("Access logs")}</button>
      </div>

      {tab === "users" ? (
        <section className="access-surface">
          <div className="access-toolbar"><label><Search size={14} /><input value={search} onChange={(event) => setSearch(event.target.value)} placeholder={t("Search users…")} /></label><select value={roleFilter} onChange={(event) => setRoleFilter(event.target.value)}><option value="all">{t("All roles")}</option><option value="admin">{t("Admin")}</option><option value="operator">{t("Operator")}</option><option value="viewer">{t("Viewer")}</option></select><select value={statusFilter} onChange={(event) => setStatusFilter(event.target.value)}><option value="all">{t("All status")}</option><option value="active">{t("Active")}</option><option value="disabled">{t("Disabled")}</option></select></div>
          <ResizableGrid className="users-table" storageKey="access-users" columns={userColumns} headerClassName="users-head">
            {filteredUsers.map((user) => (
              <div className="user-row" key={user.id}>
                <span className="user-identity"><i>{user.displayName.slice(0, 2).toUpperCase()}</i><span><strong>{user.displayName}</strong><em>{user.username}</em></span></span>
                <span><select value={user.role} aria-label={`${t("Role")} · ${user.username}`} onChange={(event) => updateRole(user, event.target.value)}><option value="viewer">{t("Viewer")}</option><option value="operator">{t("Operator")}</option><option value="admin">{t("Admin")}</option></select></span>
                <span className="scope-list">{user.role === "admin" ? <b>{t("All resources")}</b> : user.role === "operator" ? <b>{t("Configured Redis resources")}</b> : <b>{t("Read only")}</b>}</span>
                <span>{user.lastLoginAt ? formatTime(user.lastLoginAt, locale) : t("Never")}</span>
                <span className={user.enabled ? "user-status active" : "user-status"}><i />{user.enabled ? t("Active") : t("Disabled")}</span>
                <button aria-label={t("Manage {username}", { username: user.username })} onClick={() => setEditingUser(user)}><MoreHorizontal size={16} /></button>
              </div>
            ))}
            {!filteredUsers.length ? <div className="grant-empty">{t("No users match the filters.")}</div> : null}
          </ResizableGrid>
        </section>
      ) : null}

      {tab === "roles" ? <RolesPanel users={users} grants={grants} onGrantsChange={setGrants} onToast={onToast} /> : null}
      {tab === "logs" ? <LogsPanel logs={logs} onRefresh={load} /> : null}
      {showCreate ? <CreateUserModal onClose={() => setShowCreate(false)} onCreated={(user) => { setUsers((current) => [...current, user]); setShowCreate(false); onToast({ kind: "success", title: t("User created"), message: t("Created account {username}.", { username: user.username }) }); }} /> : null}
      {editingUser ? <EditUserModal user={editingUser} onClose={() => setEditingUser(null)} onSaved={(next) => { setUsers((current) => current.map((item) => item.id === next.id ? next : item)); setEditingUser(null); onToast({ kind: "success", title: t("User updated"), message: t("Updated account and sessions for {username}.", { username: next.username }) }); }} /> : null}
    </div>
  );
}

function EditUserModal({ user, onClose, onSaved }: { user: AccessUser; onClose: () => void; onSaved: (user: AccessUser) => void }) {
  const { t } = useI18n();
  const [username, setUsername] = useState(user.username);
  const [displayName, setDisplayName] = useState(user.displayName);
  const [role, setRole] = useState(user.role);
  const [enabled, setEnabled] = useState(user.enabled);
  const [password, setPassword] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const submit = async (event: FormEvent) => {
    event.preventDefault();
    setBusy(true);
    setError("");
    try {
      const updated = await api.updateUser(user.id, { username, displayName, role, enabled, password: password || undefined });
      onSaved(updated);
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Unable to update the user."));
    } finally {
      setBusy(false);
    }
  };
  return (
    <div className="modal-backdrop" onMouseDown={onClose}>
      <form className="modal user-modal" onSubmit={submit} onMouseDown={(event) => event.stopPropagation()}>
        <header><h2>{t("Manage user")}</h2><button type="button" onClick={onClose} aria-label={t("Close user dialog")}><X size={18} /></button></header>
        <div className="field-pair"><label>{t("Username")}<input value={username} onChange={(event) => setUsername(event.target.value)} minLength={3} required /></label><label>{t("Display name")}<input value={displayName} onChange={(event) => setDisplayName(event.target.value)} required /></label></div>
        <div className="field-pair"><label>{t("Basic role")}<select value={role} onChange={(event) => setRole(event.target.value)}><option value="viewer">{t("Viewer")}</option><option value="operator">{t("Operator")}</option><option value="admin">{t("Admin")}</option></select></label><label>{t("Status")}<select value={enabled ? "enabled" : "disabled"} onChange={(event) => setEnabled(event.target.value === "enabled")}><option value="enabled">{t("Active")}</option><option value="disabled">{t("Disabled")}</option></select></label></div>
        <label>{t("Reset password")}<input type="password" value={password} onChange={(event) => setPassword(event.target.value)} minLength={12} placeholder={t("Leave blank to keep current password")} /></label>
        <div className="role-explainer"><LockKeyhole size={15} /><span>{t("Disabling a user or resetting the password revokes all sessions. The last active administrator cannot be disabled or demoted.")}</span></div>
        {error ? <div className="login-error" role="alert">{error}</div> : null}
        <footer><button type="button" onClick={onClose}>{t("Cancel")}</button><button className="primary-button" disabled={busy}>{busy ? t("Saving…") : t("Save changes")}</button></footer>
      </form>
    </div>
  );
}

function RolesPanel({ users, grants, onGrantsChange, onToast }: { users: AccessUser[]; grants: Grant[]; onGrantsChange: (grants: Grant[]) => void; onToast: (toast: ToastState) => void }) {
  const { t } = useI18n();
  const [showGrant, setShowGrant] = useState(false);
  const [editingGrant, setEditingGrant] = useState<Grant | null>(null);
  const deleteGrant = async (grant: Grant) => {
    try {
      await api.deleteGrant(grant.id);
      onGrantsChange(grants.filter((item) => item.id !== grant.id));
      onToast({ kind: "success", title: t("Permission removed"), message: t("Removed the per-user permission override.") });
    } catch (cause) {
      onToast({ kind: "error", title: t("Permission removal failed"), message: cause instanceof Error ? t(cause.message) : t("Unable to remove the permission.") });
    }
  };
  const roles = [
    { id: "admin", name: "Admin", icon: ShieldCheck, userCount: users.filter((user) => user.role === "admin").length, description: "All features including users, permissions and connections", permissions: ["All actions", "All connections", "Manage users"] },
    { id: "operator", name: "Operator", icon: UserCheck, userCount: users.filter((user) => user.role === "operator").length, description: "Stream operations and consumer group management", permissions: ["Read streams", "Write messages", "Manage groups"] },
    { id: "viewer", name: "Viewer", icon: LockKeyhole, userCount: users.filter((user) => user.role === "viewer").length, description: "Read-only access to permitted Redis resources", permissions: ["Read streams", "Read groups", "No mutations"] },
  ];
  const grantColumns = useMemo<ResizableGridColumn[]>(() => [
    { id: "principal", label: t("Principal"), defaultWidth: 180, minWidth: 130 },
    { id: "action", label: t("Action"), defaultWidth: 170, minWidth: 120 },
    { id: "scope", label: t("Scope"), defaultWidth: 300, minWidth: 180, grow: true },
    { id: "effect", label: t("Effect"), defaultWidth: 105, minWidth: 85 },
    { id: "actions", label: null, ariaLabel: t("Actions"), defaultWidth: 62, minWidth: 50 },
  ], [t]);
  return (
    <section className="roles-grid">
      {roles.map(({ id, name, icon: Icon, userCount, description, permissions }) => (
        <article key={id}>
          <header><span><Icon size={17} /></span><div><h2>{t(name)}</h2><p>{t("{count} users", { count: userCount })}</p></div><button disabled>{t("Built-in")}</button></header>
          <p>{t(description)}</p>
          <div>{permissions.map((permission) => <span key={permission}><Check size={12} />{t(permission)}</span>)}</div>
        </article>
      ))}
      <section className="grant-matrix">
        <header><h2>{t("Resource overrides")}</h2><button onClick={() => setShowGrant(true)}><Plus size={13} />{t("Add permission")}</button></header>
        <ResizableGrid className="grant-table" storageKey="access-grants" columns={grantColumns} headerClassName="grant-head">
          {grants.length ? grants.map((grant) => {
            const user = users.find((item) => item.id === grant.userId);
            return <div className="grant-row" key={grant.id}><span>{user?.username ?? grant.userId}</span><span className="mono">{grant.action}</span><span className="mono">{grant.scope}</span><span className={grant.effect}>{grant.effect === "allow" ? t("Allow") : t("Deny")}</span><span className="grant-actions"><button aria-label={t("Edit permission")} onClick={() => setEditingGrant(grant)}><Pencil size={13} /></button><button aria-label={t("Delete permission")} onClick={() => void deleteGrant(grant)}><X size={14} /></button></span></div>;
          }) : <div className="grant-empty">{t("There are no per-user permission overrides yet.")}</div>}
        </ResizableGrid>
      </section>
      {showGrant ? <GrantModal users={users} onClose={() => setShowGrant(false)} onSaved={(grant) => { onGrantsChange([...grants.filter((item) => !(item.userId === grant.userId && item.action === grant.action && item.scope === grant.scope)), grant]); setShowGrant(false); onToast({ kind: "success", title: t("Permission saved"), message: t("Saved permission {action} / {scope}.", { action: grant.action, scope: grant.scope }) }); }} /> : null}
      {editingGrant ? <GrantModal users={users} initial={editingGrant} onClose={() => setEditingGrant(null)} onSaved={(grant) => { onGrantsChange([...grants.filter((item) => item.id !== editingGrant.id && !(item.userId === grant.userId && item.action === grant.action && item.scope === grant.scope)), grant]); setEditingGrant(null); onToast({ kind: "success", title: t("Permission updated"), message: t("Updated permission {action} / {scope}.", { action: grant.action, scope: grant.scope }) }); }} /> : null}
    </section>
  );
}

function GrantModal({ users, initial, onClose, onSaved }: { users: AccessUser[]; initial?: Grant; onClose: () => void; onSaved: (grant: Grant) => void }) {
  const { t } = useI18n();
  const [userId, setUserId] = useState(initial?.userId ?? users.find((user) => user.role !== "admin")?.id ?? users[0]?.id ?? "");
  const [action, setAction] = useState(initial?.action ?? "streams:read");
  const [scope, setScope] = useState(initial?.scope ?? "stream:redis:*");
  const [effect, setEffect] = useState<"allow" | "deny">(initial?.effect ?? "allow");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const submit = async (event: FormEvent) => {
    event.preventDefault();
    setBusy(true);
    setError("");
    try {
      const input = { userId, action, scope, effect };
      const saved = initial ? await api.updateGrant(initial.id, input) : await api.saveGrant(input);
      onSaved(saved);
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Unable to save the permission."));
    } finally {
      setBusy(false);
    }
  };
  return (
    <div className="modal-backdrop" onMouseDown={onClose}>
      <form className="modal" onSubmit={submit} onMouseDown={(event) => event.stopPropagation()}>
        <header><h2>{initial ? t("Edit resource permission") : t("Add resource permission")}</h2><button type="button" onClick={onClose} aria-label={t("Close permission dialog")}><X size={18} /></button></header>
        <div className="field-pair"><label>{t("User")}<select value={userId} onChange={(event) => setUserId(event.target.value)}>{users.filter((user) => user.role !== "admin").map((user) => <option key={user.id} value={user.id}>{user.username}</option>)}</select></label><label>{t("Effect")}<select value={effect} onChange={(event) => setEffect(event.target.value as "allow" | "deny")}><option value="allow">{t("Allow")}</option><option value="deny">{t("Deny")}</option></select></label></div>
        <label>{t("Action")}<select value={action} onChange={(event) => setAction(event.target.value)}><option>streams:read</option><option>streams:write</option><option>groups:read</option><option>groups:manage</option><option>connections:read</option></select></label>
        <label>{t("Scope")}<input className="mono" value={scope} onChange={(event) => setScope(event.target.value)} placeholder="stream:redis:orders.*" required /></label>
        <div className="role-explainer"><ShieldCheck size={15} /><span>{t("An explicit Deny overrides the base role and Allow. Wildcards are supported only as a trailing *.")}</span></div>
        {error ? <div className="login-error" role="alert">{error}</div> : null}
        <footer><button type="button" onClick={onClose}>{t("Cancel")}</button><button className="primary-button" disabled={busy || !userId}>{busy ? t("Saving…") : t("Save permission")}</button></footer>
      </form>
    </div>
  );
}

function LogsPanel({ logs, onRefresh }: { logs: Array<Record<string, string | number>>; onRefresh: () => void }) {
  const { locale, t } = useI18n();
  const [search, setSearch] = useState("");
  const [result, setResult] = useState("all");
  const filtered = logs.filter((log) => {
    const matchesText = Object.values(log).join(" ").toLowerCase().includes(search.toLowerCase());
    const allowed = Number(log.status) < 400;
    return matchesText && (result === "all" || (result === "allowed" ? allowed : !allowed));
  });
  const logColumns = useMemo<ResizableGridColumn[]>(() => [
    { id: "time", label: t("Time"), defaultWidth: 170, minWidth: 130 },
    { id: "user", label: t("User"), defaultWidth: 130, minWidth: 100 },
    { id: "action", label: t("Action"), defaultWidth: 165, minWidth: 115 },
    { id: "scope", label: t("Scope"), defaultWidth: 300, minWidth: 180, grow: true },
    { id: "result", label: t("Result"), defaultWidth: 135, minWidth: 105 },
    { id: "ip", label: t("Source IP"), defaultWidth: 145, minWidth: 115 },
  ], [t]);
  return (
    <section className="access-surface">
      <div className="access-toolbar"><button onClick={onRefresh}><RefreshCw size={13} />{t("Refresh")}</button><label><Search size={14} /><input value={search} onChange={(event) => setSearch(event.target.value)} placeholder={t("User, action, scope, IP…")} /></label><select value={result} onChange={(event) => setResult(event.target.value)}><option value="all">{t("All results")}</option><option value="allowed">{t("Allowed")}</option><option value="denied">{t("Denied")}</option></select><button onClick={() => exportAuditLogs(filtered)}><Download size={13} />{t("Export CSV")}</button></div>
      <ResizableGrid className="logs-table" storageKey="access-logs" columns={logColumns} headerClassName="logs-head">
        {filtered.map((log, index) => (
          <div className="log-row" key={`${log.requestId ?? index}-${index}`}>
            <span className="mono">{formatTime(String(log.createdAt), locale)}</span><strong>{String(log.username)}</strong><span className="mono">{String(log.action)}</span><span className="mono log-scope">{String(log.scope)}</span><span className={Number(log.status) < 400 ? "log-result allowed" : "log-result denied"}>{Number(log.status) < 400 ? t("Allowed") : t("Denied")} · {log.status}</span><span className="mono">{String(log.ip)}</span>
          </div>
        ))}
        {!filtered.length ? <div className="grant-empty">{t("No audit logs match the filters.")}</div> : null}
      </ResizableGrid>
    </section>
  );
}

function CreateUserModal({ onClose, onCreated }: { onClose: () => void; onCreated: (user: AccessUser) => void }) {
  const { t } = useI18n();
  const [username, setUsername] = useState("");
  const [displayName, setDisplayName] = useState("");
  const [password, setPassword] = useState("");
  const [role, setRole] = useState("viewer");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const submit = async (event: FormEvent) => {
    event.preventDefault();
    setBusy(true);
    setError("");
    try {
      const result = await api.createUser({ username, displayName, password, role }) as AccessUser;
      onCreated(result);
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Unable to create the user."));
    } finally {
      setBusy(false);
    }
  };
  return (
    <div className="modal-backdrop" onMouseDown={onClose}>
      <form className="modal user-modal" onSubmit={submit} onMouseDown={(event) => event.stopPropagation()}>
        <header><h2>{t("Add user")}</h2><button type="button" onClick={onClose} aria-label={t("Close user dialog")}><X size={18} /></button></header>
        <div className="field-pair"><label>{t("Username")}<input value={username} onChange={(event) => setUsername(event.target.value)} minLength={3} required /></label><label>{t("Display name")}<input value={displayName} onChange={(event) => setDisplayName(event.target.value)} required /></label></div>
        <label>{t("Initial password")}<input type="password" value={password} onChange={(event) => setPassword(event.target.value)} minLength={12} placeholder={t("12 characters minimum")} required /></label>
        <label>{t("Basic role")}<select value={role} onChange={(event) => setRole(event.target.value)}><option value="viewer">{t("Viewer — read only")}</option><option value="operator">{t("Operator — stream operations")}</option><option value="admin">{t("Admin — full access")}</option></select></label>
        <div className="role-explainer"><ShieldCheck size={15} /><span>{t("Detailed connection and stream permissions can be assigned under Resource overrides after account creation.")}</span></div>
        {error ? <div className="login-error" role="alert">{error}</div> : null}
        <footer><button type="button" onClick={onClose}>{t("Cancel")}</button><button className="primary-button" disabled={busy}>{busy ? t("Creating…") : t("Create user")}</button></footer>
      </form>
    </div>
  );
}

function formatTime(value: string, locale: string) {
  if (!value.includes("T")) return value;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString(locale, { hour12: false });
}

function exportAuditLogs(logs: Array<Record<string, string | number>>) {
  downloadCSV("redisstreamscope-audit-logs.csv", ["createdAt", "username", "method", "path", "action", "scope", "status", "durationMs", "ip", "requestId"], logs);
}

function exportPermissionReport(users: AccessUser[], grants: Grant[]) {
  const rows = users.flatMap((user) => {
    const userGrants = grants.filter((grant) => grant.userId === user.id);
    return [
      { username: user.username, displayName: user.displayName, role: user.role, enabled: user.enabled, action: "(base role)", scope: "*", effect: "allow" },
      ...userGrants.map((grant) => ({ username: user.username, displayName: user.displayName, role: user.role, enabled: user.enabled, action: grant.action, scope: grant.scope, effect: grant.effect })),
    ];
  });
  downloadCSV("redisstreamscope-permissions.csv", ["username", "displayName", "role", "enabled", "action", "scope", "effect"], rows);
}

function downloadCSV(filename: string, columns: string[], rows: Array<Record<string, unknown>>) {
  const escape = (value: unknown) => `"${String(value ?? "").replaceAll("\"", "\"\"")}"`;
  const content = [columns.map(escape).join(","), ...rows.map((row) => columns.map((column) => escape(row[column])).join(","))].join("\r\n");
  const href = URL.createObjectURL(new Blob(["\uFEFF", content], { type: "text/csv;charset=utf-8" }));
  const anchor = document.createElement("a");
  anchor.href = href;
  anchor.download = filename;
  anchor.click();
  URL.revokeObjectURL(href);
}
