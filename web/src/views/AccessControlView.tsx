import { FormEvent, KeyboardEvent, RefObject, useCallback, useEffect, useId, useMemo, useRef, useState } from "react";
import {
  Activity,
  Check,
  Download,
  KeyRound,
  LockKeyhole,
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
import { Select } from "../components/Select";
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
type LogResult = "all" | "allowed" | "denied";
type AccessLogPage = {
  items: Array<Record<string, string | number>>;
  nextCursor: number | null;
  hasMore: boolean;
  summary: { total: number; allowed: number; denied: number };
};

type AccessTab = "users" | "roles" | "logs";

const emptyLogPage: AccessLogPage = { items: [], nextCursor: null, hasMore: false, summary: { total: 0, allowed: 0, denied: 0 } };

const rolePermissions: Record<string, string[]> = {
  viewer: ["connections:read", "streams:read", "groups:read", "alerts:read"],
  operator: ["connections:read", "streams:read", "streams:write", "groups:read", "groups:manage", "alerts:read"],
  admin: ["*"],
};

const grantActionDefinitions = [
  { value: "streams:read", category: "Streams" },
  { value: "streams:write", category: "Streams" },
  { value: "groups:read", category: "Consumer groups" },
  { value: "groups:manage", category: "Consumer groups" },
  { value: "connections:read", category: "Connections" },
  { value: "alerts:read", category: "Alerts" },
  { value: "alerts:write", category: "Alerts" },
  { value: "settings:read", category: "Settings" },
  { value: "settings:write", category: "Settings" },
  { value: "access-logs:read", category: "Access logs" },
  { value: "users:read", category: "Users" },
  { value: "users:write", category: "Users" },
  { value: "roles:read", category: "Roles & permissions" },
  { value: "roles:write", category: "Roles & permissions" },
] as const;

function roleName(role: string) {
  return role.charAt(0).toUpperCase() + role.slice(1);
}

function permissionDiff(fromRole: string, toRole: string) {
  if (fromRole === toRole) return { added: [] as string[], removed: [] as string[] };
  if (toRole === "admin") return { added: ["Full administrative access (*)"], removed: [] as string[] };
  if (fromRole === "admin") return { added: [] as string[], removed: ["Full administrative access (*)"] };
  const from = new Set(rolePermissions[fromRole] ?? []);
  const to = new Set(rolePermissions[toRole] ?? []);
  return {
    added: [...to].filter((permission) => !from.has(permission)),
    removed: [...from].filter((permission) => !to.has(permission)),
  };
}

export function AccessControlView({ onToast }: { onToast: (toast: ToastState) => void }) {
  const { locale, t } = useI18n();
  const [tab, setTab] = useState<AccessTab>("users");
  const [users, setUsers] = useState<AccessUser[]>([]);
  const [logPage, setLogPage] = useState<AccessLogPage>(emptyLogPage);
  const [logCursor, setLogCursor] = useState<number | null>(null);
  const [logCursorHistory, setLogCursorHistory] = useState<Array<number | null>>([]);
  const [logSearch, setLogSearch] = useState("");
  const [logLoading, setLogLoading] = useState(false);
  const [grants, setGrants] = useState<Grant[]>([]);
  const [search, setSearch] = useState("");
  const [roleFilter, setRoleFilter] = useState("all");
  const [statusFilter, setStatusFilter] = useState("all");
  const [logResult, setLogResult] = useState<LogResult>("all");
  const [showCreate, setShowCreate] = useState(false);
  const [editingUser, setEditingUser] = useState<AccessUser | null>(null);

  const loadAdministration = useCallback(() => {
    Promise.all([api.users(), api.grants()])
      .then(([userResponse, grantResponse]) => {
        setUsers(userResponse.items);
        setGrants(grantResponse.items);
      })
      .catch((error) => onToast({ kind: "error", title: t("Access data unavailable"), message: error instanceof Error ? t(error.message) : t("Unable to load administration data.") }));
  }, [onToast, t]);

  const loadLogs = useCallback(async () => {
    setLogLoading(true);
    try {
      setLogPage(await api.accessLogs({ limit: 100, cursor: logCursor, search: logSearch, result: logResult }));
    } catch (error) {
      onToast({ kind: "error", title: t("Access data unavailable"), message: error instanceof Error ? t(error.message) : t("Unable to load administration data.") });
    } finally {
      setLogLoading(false);
    }
  }, [logCursor, logResult, logSearch, onToast, t]);

  useEffect(() => { loadAdministration(); }, [loadAdministration]);
  useEffect(() => {
    const timer = window.setTimeout(() => { void loadLogs(); }, 250);
    return () => window.clearTimeout(timer);
  }, [loadLogs]);

  const filteredUsers = useMemo(
    () => users.filter((user) =>
      `${user.username} ${user.displayName} ${user.role}`.toLowerCase().includes(search.toLowerCase())
      && (roleFilter === "all" || user.role === roleFilter)
      && (statusFilter === "all" || (statusFilter === "active" ? user.enabled : !user.enabled))),
    [roleFilter, search, statusFilter, users],
  );

  const deniedRequests = logPage.summary.denied;
  const userColumns = useMemo<ResizableGridColumn[]>(() => [
    { id: "user", label: t("User"), defaultWidth: 230, minWidth: 170, grow: true },
    { id: "role", label: t("Role"), defaultWidth: 125, minWidth: 105 },
    { id: "access", label: t("Resource access"), defaultWidth: 220, minWidth: 155 },
    { id: "last-login", label: t("Last login"), defaultWidth: 145, minWidth: 115 },
    { id: "status", label: t("Status"), defaultWidth: 110, minWidth: 90 },
    { id: "actions", label: null, ariaLabel: t("Actions"), defaultWidth: 40, minWidth: 32 },
  ], [t]);
  const handleTabKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    const tabs: AccessTab[] = ["users", "roles", "logs"];
    const currentIndex = tabs.indexOf(tab);
    let nextIndex = currentIndex;
    if (event.key === "ArrowRight" || event.key === "ArrowDown") nextIndex = (currentIndex + 1) % tabs.length;
    else if (event.key === "ArrowLeft" || event.key === "ArrowUp") nextIndex = (currentIndex - 1 + tabs.length) % tabs.length;
    else if (event.key === "Home") nextIndex = 0;
    else if (event.key === "End") nextIndex = tabs.length - 1;
    else return;
    event.preventDefault();
    const nextTab = tabs[nextIndex];
    setTab(nextTab);
    window.requestAnimationFrame(() => document.getElementById(`access-tab-${nextTab}`)?.focus());
  };

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
        <div className={tab === "logs" && logResult === "denied" ? "access-summary-link active" : "access-summary-link"}><button type="button" aria-label={t("Open denied access logs")} aria-pressed={tab === "logs" && logResult === "denied"} onClick={() => { setLogCursor(null); setLogCursorHistory([]); setLogResult("denied"); setTab("logs"); }}><Activity size={17} /><span>{t("Denied requests")}</span><strong>{deniedRequests}</strong><em>{t("of {count} matching requests", { count: logPage.summary.allowed + logPage.summary.denied })}</em></button></div>
      </div>
      <div className="content-tabs access-tabs" role="tablist" aria-label={t("Access Control")} onKeyDown={handleTabKeyDown}>
        <button id="access-tab-users" role="tab" aria-selected={tab === "users"} aria-controls="access-panel-users" tabIndex={tab === "users" ? 0 : -1} className={tab === "users" ? "active" : ""} onClick={() => setTab("users")}>{t("Users")}</button>
        <button id="access-tab-roles" role="tab" aria-selected={tab === "roles"} aria-controls="access-panel-roles" tabIndex={tab === "roles" ? 0 : -1} className={tab === "roles" ? "active" : ""} onClick={() => setTab("roles")}>{t("Roles & permissions")}</button>
        <button id="access-tab-logs" role="tab" aria-selected={tab === "logs"} aria-controls="access-panel-logs" tabIndex={tab === "logs" ? 0 : -1} className={tab === "logs" ? "active" : ""} onClick={() => setTab("logs")}>{t("Access logs")}</button>
      </div>

      {tab === "users" ? (
        <section id="access-panel-users" role="tabpanel" aria-labelledby="access-tab-users" className="access-surface">
          <div className="access-toolbar access-users-toolbar">
            <label><Search size={14} /><input value={search} onChange={(event) => setSearch(event.target.value)} placeholder={t("Search users…")} /></label>
            <Select
              value={roleFilter}
              onChange={setRoleFilter}
              ariaLabel={t("Role")}
              prefix={t("Role")}
              size="compact"
              options={[
                { value: "all", label: t("All roles") },
                { value: "admin", label: t("Admin"), tone: "warning" },
                { value: "operator", label: t("Operator"), tone: "info" },
                { value: "viewer", label: t("Viewer"), tone: "neutral" },
              ]}
            />
            <Select
              value={statusFilter}
              onChange={setStatusFilter}
              ariaLabel={t("Status")}
              prefix={t("Status")}
              size="compact"
              options={[
                { value: "all", label: t("All status") },
                { value: "active", label: t("Active"), tone: "success" },
                { value: "disabled", label: t("Disabled"), tone: "danger" },
              ]}
            />
          </div>
          <ResizableGrid className="users-table" storageKey="access-users" columns={userColumns} headerClassName="users-head">
            {filteredUsers.map((user) => (
              <div className="user-row" key={user.id}>
                <span className="user-identity" data-label={t("User")}><i>{user.displayName.slice(0, 2).toUpperCase()}</i><span><strong>{user.displayName}</strong><em>{user.username}</em></span></span>
                <span className="user-role" data-label={t("Role")}>{t(roleName(user.role))}</span>
                <span className="scope-list" data-label={t("Resource access")}>{user.role === "admin" ? <b>{t("All resources")}</b> : user.role === "operator" ? <b>{t("Configured Redis resources")}</b> : <b>{t("Read only")}</b>}</span>
                <span data-label={t("Last login")}>{user.lastLoginAt ? formatTime(user.lastLoginAt, locale) : t("Never")}</span>
                <span className={user.enabled ? "user-status active" : "user-status"} data-label={t("Status")}><i />{user.enabled ? t("Active") : t("Disabled")}</span>
                <button aria-label={t("Edit {username}", { username: user.username })} onClick={() => setEditingUser(user)}><Pencil size={15} /></button>
              </div>
            ))}
            {!filteredUsers.length ? <div className="grant-empty">{t("No users match the filters.")}</div> : null}
          </ResizableGrid>
        </section>
      ) : null}

      {tab === "roles" ? <div id="access-panel-roles" role="tabpanel" aria-labelledby="access-tab-roles"><RolesPanel users={users} grants={grants} onGrantsChange={setGrants} onToast={onToast} /></div> : null}
      {tab === "logs" ? <div id="access-panel-logs" role="tabpanel" aria-labelledby="access-tab-logs"><LogsPanel
        page={logPage}
        loading={logLoading}
        search={logSearch}
        onSearchChange={(value) => { setLogSearch(value); setLogCursor(null); setLogCursorHistory([]); }}
        onRefresh={() => void loadLogs()}
        result={logResult}
        onResultChange={(value) => { setLogResult(value); setLogCursor(null); setLogCursorHistory([]); }}
        pageNumber={logCursorHistory.length + 1}
        onPrevious={() => {
          const previous = logCursorHistory.at(-1) ?? null;
          setLogCursorHistory((current) => current.slice(0, -1));
          setLogCursor(previous);
        }}
        onNext={() => {
          if (!logPage.nextCursor) return;
          setLogCursorHistory((current) => [...current, logCursor]);
          setLogCursor(logPage.nextCursor);
        }}
      /></div> : null}
      {showCreate ? <CreateUserModal onClose={() => setShowCreate(false)} onCreated={(user) => { setUsers((current) => [...current, user]); setShowCreate(false); onToast({ kind: "success", title: t("User created"), message: t("Created account {username}.", { username: user.username }) }); }} /> : null}
      {editingUser ? <EditUserModal user={editingUser} onClose={() => setEditingUser(null)} onSaved={(next) => { setUsers((current) => current.map((item) => item.id === next.id ? next : item)); setEditingUser(null); onToast({ kind: "success", title: t("User updated"), message: t("Updated account and sessions for {username}.", { username: next.username }) }); }} /> : null}
    </div>
  );
}

function EditUserModal({ user, onClose, onSaved }: { user: AccessUser; onClose: () => void; onSaved: (user: AccessUser) => void }) {
  const { t } = useI18n();
  const titleId = useId();
  const usernameRef = useDialogFocus<HTMLInputElement>(onClose);
  const [username, setUsername] = useState(user.username);
  const [displayName, setDisplayName] = useState(user.displayName);
  const [role, setRole] = useState(user.role);
  const [enabled, setEnabled] = useState(user.enabled);
  const [password, setPassword] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const diff = permissionDiff(user.role, role);
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
      <form className="modal user-modal" role="dialog" aria-modal="true" aria-labelledby={titleId} onSubmit={submit} onMouseDown={(event) => event.stopPropagation()}>
        <header><h2 id={titleId}>{t("Manage user")}</h2><button type="button" onClick={onClose} aria-label={t("Close user dialog")}><X size={18} /></button></header>
        <div className="field-pair"><label>{t("Username")}<input ref={usernameRef} value={username} onChange={(event) => setUsername(event.target.value)} minLength={3} required /></label><label>{t("Display name")}<input value={displayName} onChange={(event) => setDisplayName(event.target.value)} required /></label></div>
        <div className="field-pair">
          <label>{t("Basic role")}<Select
            className="select-control--block"
            value={role}
            onChange={setRole}
            ariaLabel={t("Basic role")}
            options={[
              { value: "viewer", label: t("Viewer"), description: t("Read-only access to permitted Redis resources"), meta: t("Read only"), tone: "neutral" },
              { value: "operator", label: t("Operator"), description: t("Stream operations and consumer group management"), meta: t("Configured Redis resources"), tone: "info" },
              { value: "admin", label: t("Admin"), description: t("All features including users, permissions and connections"), meta: t("Full access"), tone: "warning" },
            ]}
          /></label>
          <label>{t("Status")}<Select
            className="select-control--block"
            value={enabled ? "enabled" : "disabled"}
            onChange={(value) => setEnabled(value === "enabled")}
            ariaLabel={t("Status")}
            options={[
              { value: "enabled", label: t("Active"), tone: "success" },
              { value: "disabled", label: t("Disabled"), tone: "danger" },
            ]}
          /></label>
        </div>
        {role !== user.role ? <section className="role-change-diff" aria-live="polite">
          <header><strong>{t("Role change")}</strong><span>{t(roleName(user.role))} → {t(roleName(role))}</span></header>
          {diff.added.length ? <div><span>{t("Added permissions")}</span><ul>{diff.added.map((permission) => <li className="mono" key={permission}>+ {t(permission)}</li>)}</ul></div> : null}
          {diff.removed.length ? <div><span>{t("Removed permissions")}</span><ul>{diff.removed.map((permission) => <li className="mono" key={permission}>− {t(permission)}</li>)}</ul></div> : null}
          <p>{t("Resulting base permissions")}: <span className="mono">{rolePermissions[role]?.map((permission) => t(permission)).join(", ")}</span></p>
        </section> : null}
        <label>{t("Reset password")}<input type="password" autoComplete="new-password" value={password} onChange={(event) => setPassword(event.target.value)} placeholder={t("Leave blank to keep current password")} /></label>
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
            return <div className="grant-row" key={grant.id}><span data-label={t("Principal")}>{user?.username ?? grant.userId}</span><span className="mono" data-label={t("Action")}>{grant.action}</span><span className="mono" data-label={t("Scope")}>{grant.scope}</span><span className={grant.effect} data-label={t("Effect")}>{grant.effect === "allow" ? t("Allow") : t("Deny")}</span><span className="grant-actions"><button aria-label={t("Edit permission")} onClick={() => setEditingGrant(grant)}><Pencil size={13} /></button><button aria-label={t("Delete permission")} onClick={() => void deleteGrant(grant)}><X size={14} /></button></span></div>;
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
  const titleId = useId();
  const userRef = useDialogFocus<HTMLButtonElement>(onClose);
  const [userId, setUserId] = useState(initial?.userId ?? users.find((user) => user.role !== "admin")?.id ?? users[0]?.id ?? "");
  const [action, setAction] = useState(initial?.action ?? "streams:read");
  const [scope, setScope] = useState(initial?.scope ?? "stream:redis:*");
  const [effect, setEffect] = useState<"allow" | "deny">(initial?.effect ?? "allow");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const userOptions = users.filter((user) => user.role !== "admin").map((user) => ({
    value: user.id,
    label: user.username,
    description: user.displayName,
    meta: t(roleName(user.role)),
    keywords: `${user.username} ${user.displayName} ${user.id} ${user.role}`,
  }));
  const actionOptions = grantActionDefinitions.map(({ value, category }) => ({
    value,
    label: t(value),
    description: t(category),
    meta: value,
    keywords: `${value} ${t(value)} ${t(category)}`,
  }));
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
      <form className="modal" role="dialog" aria-modal="true" aria-labelledby={titleId} onSubmit={submit} onMouseDown={(event) => event.stopPropagation()}>
        <header><h2 id={titleId}>{initial ? t("Edit resource permission") : t("Add resource permission")}</h2><button type="button" onClick={onClose} aria-label={t("Close permission dialog")}><X size={18} /></button></header>
        <div className="field-pair">
          <label>{t("User")}<Select
            ref={userRef}
            className="select-control--block"
            value={userId}
            onChange={setUserId}
            options={userOptions}
            ariaLabel={t("User")}
            searchable
            searchPlaceholder={t("Search users…")}
          /></label>
          <label>{t("Effect")}<Select
            className="select-control--block"
            value={effect}
            onChange={(value) => setEffect(value as "allow" | "deny")}
            ariaLabel={t("Effect")}
            options={[
              { value: "allow", label: t("Allow"), description: t("Apply this action to the matching scope."), meta: t("Allowed"), tone: "success" },
              { value: "deny", label: t("Deny"), description: t("Overrides the base role and any explicit Allow."), meta: t("Denied"), tone: "danger" },
            ]}
          /></label>
        </div>
        <label>{t("Action")}<Select
          className="select-control--block"
          value={action}
          onChange={setAction}
          options={actionOptions}
          ariaLabel={t("Action")}
          searchable
        /></label>
        <label>{t("Scope")}<input className="mono" value={scope} onChange={(event) => setScope(event.target.value)} placeholder="stream:redis:orders.*" required /></label>
        <div className="role-explainer"><ShieldCheck size={15} /><span>{t("An explicit Deny overrides the base role and Allow. Wildcards are supported only as a trailing *.")}</span></div>
        {error ? <div className="login-error" role="alert">{error}</div> : null}
        <footer><button type="button" onClick={onClose}>{t("Cancel")}</button><button className="primary-button" disabled={busy || !userId}>{busy ? t("Saving…") : t("Save permission")}</button></footer>
      </form>
    </div>
  );
}

function LogsPanel({ page, loading, search, onSearchChange, onRefresh, result, onResultChange, pageNumber, onPrevious, onNext }: {
  page: AccessLogPage;
  loading: boolean;
  search: string;
  onSearchChange: (value: string) => void;
  onRefresh: () => void;
  result: LogResult;
  onResultChange: (result: LogResult) => void;
  pageNumber: number;
  onPrevious: () => void;
  onNext: () => void;
}) {
  const { locale, t } = useI18n();
  const logColumns = useMemo<ResizableGridColumn[]>(() => [
    { id: "time", label: t("Time"), defaultWidth: 145, minWidth: 130 },
    { id: "user", label: t("User"), defaultWidth: 115, minWidth: 100 },
    { id: "action", label: t("Action"), defaultWidth: 145, minWidth: 115 },
    { id: "scope", label: t("Scope"), defaultWidth: 260, minWidth: 180, grow: true },
    { id: "result", label: t("Result"), defaultWidth: 125, minWidth: 105 },
    { id: "ip", label: t("Source IP"), defaultWidth: 130, minWidth: 115 },
  ], [t]);
  return (
    <section className="access-surface">
      <div className="access-toolbar access-logs-toolbar">
        <button onClick={onRefresh} disabled={loading}><RefreshCw size={13} />{loading ? t("Loading…") : t("Refresh")}</button>
        <label><Search size={14} /><input value={search} onChange={(event) => onSearchChange(event.target.value)} placeholder={t("User, action, scope, IP…")} /></label>
        <Select
          value={result}
          onChange={(value) => onResultChange(value as LogResult)}
          ariaLabel={t("Result")}
          prefix={t("Result")}
          size="compact"
          options={[
            { value: "all", label: t("All results") },
            { value: "allowed", label: t("Allowed"), tone: "success" },
            { value: "denied", label: t("Denied"), tone: "danger" },
          ]}
        />
        <button onClick={() => exportAuditLogs(page.items)}><Download size={13} />{t("Export page")}</button>
      </div>
      <ResizableGrid className="logs-table" storageKey="access-logs-v2" columns={logColumns} headerClassName="logs-head">
        {page.items.map((log, index) => (
          <div className="log-row" key={`${log.requestId ?? index}-${index}`}>
            <span className="mono" data-label={t("Time")}>{formatTime(String(log.createdAt), locale)}</span><strong data-label={t("User")}>{String(log.username)}</strong><span className="mono" data-label={t("Action")}>{String(log.action)}</span><span className="mono log-scope" data-label={t("Scope")}>{String(log.scope)}</span><span className={Number(log.status) < 400 ? "log-result allowed" : "log-result denied"} data-label={t("Result")}>{Number(log.status) < 400 ? t("Allowed") : t("Denied")} · {log.status}</span><span className="mono" data-label={t("Source IP")}>{String(log.ip)}</span>
          </div>
        ))}
        {!page.items.length && !loading ? <div className="grant-empty">{t("No audit logs match the filters.")}</div> : null}
      </ResizableGrid>
      <footer className="table-footer access-log-pagination">
        <span>{t("{count} matching requests", { count: page.summary.total.toLocaleString(locale) })}</span>
        <div><button type="button" onClick={onPrevious} disabled={pageNumber <= 1 || loading}>{t("Previous")}</button><strong>{t("Page {page}", { page: pageNumber })}</strong><button type="button" onClick={onNext} disabled={!page.hasMore || loading}>{t("Next")}</button></div>
      </footer>
    </section>
  );
}

function CreateUserModal({ onClose, onCreated }: { onClose: () => void; onCreated: (user: AccessUser) => void }) {
  const { t } = useI18n();
  const titleId = useId();
  const usernameRef = useDialogFocus<HTMLInputElement>(onClose);
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
      <form className="modal user-modal" role="dialog" aria-modal="true" aria-labelledby={titleId} onSubmit={submit} onMouseDown={(event) => event.stopPropagation()}>
        <header><h2 id={titleId}>{t("Add user")}</h2><button type="button" onClick={onClose} aria-label={t("Close user dialog")}><X size={18} /></button></header>
        <div className="field-pair"><label>{t("Username")}<input ref={usernameRef} value={username} onChange={(event) => setUsername(event.target.value)} minLength={3} required /></label><label>{t("Display name")}<input value={displayName} onChange={(event) => setDisplayName(event.target.value)} required /></label></div>
        <label>{t("Initial password")}<input type="password" autoComplete="new-password" value={password} onChange={(event) => setPassword(event.target.value)} required /></label>
        <label>{t("Basic role")}<Select
          className="select-control--block"
          value={role}
          onChange={setRole}
          ariaLabel={t("Basic role")}
          options={[
            { value: "viewer", label: t("Viewer"), description: t("Read-only access to permitted Redis resources"), meta: t("Read only"), tone: "neutral" },
            { value: "operator", label: t("Operator"), description: t("Stream operations and consumer group management"), meta: t("Configured Redis resources"), tone: "info" },
            { value: "admin", label: t("Admin"), description: t("All features including users, permissions and connections"), meta: t("Full access"), tone: "warning" },
          ]}
        /></label>
        <div className="role-explainer"><ShieldCheck size={15} /><span>{t("Detailed connection and stream permissions can be assigned under Resource overrides after account creation.")}</span></div>
        {error ? <div className="login-error" role="alert">{error}</div> : null}
        <footer><button type="button" onClick={onClose}>{t("Cancel")}</button><button className="primary-button" disabled={busy}>{busy ? t("Creating…") : t("Create user")}</button></footer>
      </form>
    </div>
  );
}

function useDialogFocus<T extends HTMLElement>(onClose: () => void): RefObject<T | null> {
  const initialFocusRef = useRef<T>(null);
  const onCloseRef = useRef(onClose);

  useEffect(() => {
    onCloseRef.current = onClose;
  }, [onClose]);

  useEffect(() => {
    const returnFocusElement = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    initialFocusRef.current?.focus();
    const closeOnEscape = (event: globalThis.KeyboardEvent) => {
      if (event.key !== "Escape") return;
      event.preventDefault();
      onCloseRef.current();
    };
    document.addEventListener("keydown", closeOnEscape);
    return () => {
      document.removeEventListener("keydown", closeOnEscape);
      if (returnFocusElement?.isConnected) returnFocusElement.focus({ preventScroll: true });
    };
  }, []);

  return initialFocusRef;
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
