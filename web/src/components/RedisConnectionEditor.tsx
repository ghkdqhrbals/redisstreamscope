import { Database, LockKeyhole, ShieldCheck, Trash2 } from "lucide-react";
import { useI18n } from "../i18n";
import type { RedisConnectionConfig } from "../types";

export const emptyRedisConnection = (index = 1): RedisConnectionConfig => ({
  id: index === 1 ? "redis" : `redis-${index}`,
  name: index === 1 ? "Redis" : `Redis ${index}`,
  mode: "standalone",
  addrs: [""],
  masterName: "",
  username: "",
  password: "",
  passwordConfigured: false,
  clearPassword: false,
  db: 0,
  keyPattern: "*",
  tls: false,
  tlsServerName: "",
  tlsCAFile: "",
  tlsCertFile: "",
  tlsKeyFile: "",
});

type Props = {
  value: RedisConnectionConfig;
  onChange: (value: RedisConnectionConfig) => void;
  onRemove?: () => void;
  compact?: boolean;
  idReadOnly?: boolean;
};

export function RedisConnectionEditor({ value, onChange, onRemove, compact = false, idReadOnly = false }: Props) {
  const { t } = useI18n();
  const update = <K extends keyof RedisConnectionConfig>(key: K, next: RedisConnectionConfig[K]) => onChange({ ...value, [key]: next });
  return (
    <section className={`redis-editor ${compact ? "redis-editor--compact" : ""}`}>
      <header>
        <span className="connection-icon"><Database size={18} /></span>
        <div><strong>{value.name || value.id || t("New Redis connection")}</strong><small>{value.mode}</small></div>
        {onRemove ? <button type="button" className="icon-danger" onClick={onRemove} aria-label={t("Remove connection")}><Trash2 size={15} /></button> : null}
      </header>

      <div className="editor-grid">
        <label>{t("Connection ID")}<input value={value.id} onChange={(event) => update("id", event.target.value)} placeholder="production" required readOnly={idReadOnly} /></label>
        <label>{t("Display name")}<input value={value.name} onChange={(event) => update("name", event.target.value)} placeholder="Production Redis" required /></label>
        <label>{t("Mode")}<select value={value.mode} onChange={(event) => {
          const mode = event.target.value as RedisConnectionConfig["mode"];
          onChange({ ...value, mode, db: mode === "cluster" ? 0 : value.db });
        }}><option value="standalone">Standalone</option><option value="sentinel">Sentinel</option><option value="cluster">Cluster</option></select></label>
        <label>Database<input type="number" min={0} value={value.db} disabled={value.mode === "cluster"} onChange={(event) => update("db", Number(event.target.value))} /></label>
        <label className="editor-span">{t("Redis URL / node addresses")}<span>{t("Separate multiple addresses with commas. To reach host Redis from Docker, use host.docker.internal:6379.")}</span><input className="mono" value={value.addrs.join(", ")} onChange={(event) => update("addrs", event.target.value.split(",").map((item) => item.trim()))} placeholder={value.mode === "sentinel" ? "sentinel-1:26379, sentinel-2:26379" : "host.docker.internal:6379"} required /></label>
        {value.mode === "sentinel" ? <label className="editor-span">{t("Sentinel master name")}<input value={value.masterName} onChange={(event) => update("masterName", event.target.value)} placeholder="mymaster" required /></label> : null}
        <label>{t("ACL username")}<span>{t("Leave empty when unused.")}</span><input value={value.username} onChange={(event) => update("username", event.target.value)} autoComplete="off" /></label>
        <label>{t("Password")}<span>{value.passwordConfigured && !value.clearPassword ? t("Leave empty to keep the existing value.") : t("Leave empty when Redis has no password.")}</span><input type="password" value={value.password ?? ""} onChange={(event) => onChange({ ...value, password: event.target.value, clearPassword: false })} autoComplete="new-password" /></label>
        {value.passwordConfigured ? <label className="checkbox-field editor-span"><input type="checkbox" checked={Boolean(value.clearPassword)} onChange={(event) => onChange({ ...value, clearPassword: event.target.checked, password: "" })} />{t("Remove saved Redis password")}</label> : null}
        <label className="editor-span">{t("Stream key pattern")}<input className="mono" value={value.keyPattern} onChange={(event) => update("keyPattern", event.target.value)} placeholder="*" /></label>
      </div>

      <div className="tls-toggle"><span><ShieldCheck size={15} /><span><strong>TLS</strong><small>{t("rediss or a TLS-enabled Redis connection")}</small></span></span><button type="button" className={`toggle ${value.tls ? "on" : ""}`} aria-label={t("Toggle TLS")} aria-pressed={value.tls} onClick={() => update("tls", !value.tls)}><i /></button></div>
      {value.tls ? <div className="editor-grid tls-fields">
        <label className="editor-span">{t("TLS server name")}<input value={value.tlsServerName} onChange={(event) => update("tlsServerName", event.target.value)} placeholder="redis.internal" /></label>
        <label>{t("CA file path")}<input className="mono" value={value.tlsCAFile} onChange={(event) => update("tlsCAFile", event.target.value)} placeholder="/data/certs/redis-ca.pem" /></label>
        <label>{t("Client certificate")}<input className="mono" value={value.tlsCertFile} onChange={(event) => update("tlsCertFile", event.target.value)} placeholder="/data/certs/client.pem" /></label>
        <label className="editor-span">{t("Client private key")}<input className="mono" value={value.tlsKeyFile} onChange={(event) => update("tlsKeyFile", event.target.value)} placeholder="/data/certs/client-key.pem" /></label>
      </div> : null}
      <div className="editor-security"><LockKeyhole size={14} /><span>{t("Passwords are never returned to the browser and are stored in the CONFIG_PATH properties file.")}</span></div>
    </section>
  );
}
