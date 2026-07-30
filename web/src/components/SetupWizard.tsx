import { FormEvent, useState } from "react";
import { ArrowLeft, ArrowRight, Check, CheckCircle2, Database, Layers3, LockKeyhole, ServerCog, UserRoundCog } from "lucide-react";
import { api } from "../api";
import type { ApiSession, RedisConnectionConfig } from "../types";
import { LanguageSelect, useI18n } from "../i18n";
import { emptyRedisConnection, RedisConnectionEditor } from "./RedisConnectionEditor";

const steps = [
  { label: "Welcome", icon: Layers3 },
  { label: "Administrator", icon: UserRoundCog },
  { label: "Redis connection", icon: Database },
  { label: "Review", icon: ServerCog },
];

export function SetupWizard({ configPath, initialConnection, onComplete }: {
  configPath: string;
  initialConnection?: RedisConnectionConfig;
  onComplete: (session: ApiSession) => void;
}) {
  const { t } = useI18n();
  const [step, setStep] = useState(0);
  const [username, setUsername] = useState("admin");
  const [displayName, setDisplayName] = useState("System Administrator");
  const [password, setPassword] = useState("");
  const [confirmation, setConfirmation] = useState("");
  const [connection, setConnection] = useState<RedisConnectionConfig>(initialConnection ?? emptyRedisConnection());
  const [testState, setTestState] = useState<"idle" | "testing" | "success" | "error">("idle");
  const [testMessage, setTestMessage] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  const next = () => {
    setError("");
    if (step === 1) {
      if (username.trim().length < 3) return setError(t("Administrator username must be at least 3 characters."));
      if (password.length < 12) return setError(t("Administrator password must be at least 12 characters."));
      if (password !== confirmation) return setError(t("Password confirmation does not match."));
    }
    if (step === 2 && (!connection.id.trim() || !connection.addrs.some(Boolean))) return setError(t("Enter a Redis ID and address."));
    setStep((current) => Math.min(steps.length - 1, current + 1));
  };

  const testConnection = async () => {
    setTestState("testing");
    setTestMessage("");
    try {
      const result = await api.setupTestRedis(connection);
      setTestState("success");
      setTestMessage(t("Connection succeeded · {latency} ms", { latency: result.latencyMs.toFixed(1) }));
    } catch (cause) {
      setTestState("error");
      setTestMessage(cause instanceof Error ? t(cause.message) : t("Redis connection failed."));
    }
  };

  const finish = async (event: FormEvent) => {
    event.preventDefault();
    setBusy(true);
    setError("");
    try {
      onComplete(await api.setup({ admin: { username: username.trim(), displayName: displayName.trim(), password }, connections: [connection] }));
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Unable to finish initial setup."));
    } finally {
      setBusy(false);
    }
  };

  return (
    <main className="setup-page">
      <aside className="setup-rail">
        <div className="setup-wordmark"><span className="brand-mark"><Layers3 size={19} /></span><strong>RedisStreamScope</strong></div>
        <LanguageSelect className="setup-language" />
        <div className="setup-progress">
          {steps.map(({ label, icon: Icon }, index) => <div className={index === step ? "active" : index < step ? "complete" : ""} key={label}><span>{index < step ? <Check size={14} /> : <Icon size={15} />}</span><div><strong>{t(label)}</strong><small>{index < step ? t("Complete") : index === step ? t("In progress") : t("Pending")}</small></div></div>)}
        </div>
        <div className="setup-path"><ServerCog size={14} /><div><span>CONFIG_PATH</span><code>{configPath}</code></div></div>
      </aside>

      <section className="setup-workspace">
        <div className="setup-content">
          {step === 0 ? <div className="setup-intro">
            <div className="setup-icon"><Layers3 size={24} /></div>
            <h1>{t("Set up RedisStreamScope")}</h1>
            <p>{t("The container is ready. Configure the administrator account and Redis connection in your browser.")}</p>
            <div className="setup-facts"><div><LockKeyhole size={17} /><span><strong>{t("Administrator account")}</strong><small>{t("Credentials stored securely")}</small></span></div><div><Database size={17} /><span><strong>{t("Redis connection")}</strong><small>{t("Standalone, Sentinel and Cluster support")}</small></span></div><div><ServerCog size={17} /><span><strong>{t("Persistent properties")}</strong><small>{t("Settings are atomically stored at CONFIG_PATH")}</small></span></div></div>
            <div className="setup-notice">{t("Until setup is complete, expose the service only on 127.0.0.1.")}</div>
          </div> : null}

          {step === 1 ? <div className="setup-form-page">
            <div className="setup-icon"><UserRoundCog size={23} /></div><h1>{t("Administrator account")}</h1><p>{t("This is the final administrator password. No temporary password or pre-generated hash is required.")}</p>
            <div className="setup-form-grid"><label>{t("Username")}<input autoComplete="username" value={username} onChange={(event) => setUsername(event.target.value)} minLength={3} required /></label><label>{t("Display name")}<input value={displayName} onChange={(event) => setDisplayName(event.target.value)} required /></label><label>{t("Password")}<input type="password" autoComplete="new-password" value={password} onChange={(event) => setPassword(event.target.value)} minLength={12} required /></label><label>{t("Confirm password")}<input type="password" autoComplete="new-password" value={confirmation} onChange={(event) => setConfirmation(event.target.value)} minLength={12} required /></label></div>
          </div> : null}

          {step === 2 ? <div className="setup-form-page setup-redis-page">
            <div className="setup-icon"><Database size={23} /></div><h1>{t("Redis connection")}</h1><p>{t("Leave password empty when Redis has no password. For Cluster, enter node addresses separated by commas.")}</p>
            <RedisConnectionEditor value={connection} onChange={(next) => { setConnection(next); setTestState("idle"); }} />
            <div className={`connection-test connection-test--${testState}`}><button type="button" onClick={() => void testConnection()} disabled={testState === "testing"}>{testState === "testing" ? t("Testing…") : t("Test connection")}</button>{testMessage ? <span>{testState === "success" ? <CheckCircle2 size={14} /> : null}{testMessage}</span> : <small>{t("Verify credentials and network access with PING before saving.")}</small>}</div>
          </div> : null}

          {step === 3 ? <form className="setup-form-page" onSubmit={finish}>
            <div className="setup-icon"><CheckCircle2 size={23} /></div><h1>{t("Review configuration")}</h1><p>{t("Finishing setup saves the administrator account and config.properties, then signs you in.")}</p>
            <div className="setup-review"><div><span>{t("Administrator")}</span><strong>{username}</strong><small>{displayName}</small></div><div><span>Redis</span><strong>{connection.name} · {connection.mode}</strong><small>{connection.addrs.filter(Boolean).join(", ")}</small></div><div><span>{t("Authentication")}</span><strong>{connection.username || t("Default Redis user")}</strong><small>{connection.password || connection.passwordConfigured ? t("Password configured") : t("No password")}</small></div><div><span>{t("Config file")}</span><strong className="mono">{configPath}</strong><small>{t("Persisted in the Docker volume")}</small></div></div>
            {error ? <div className="login-error" role="alert">{error}</div> : null}
            <button className="primary-button setup-finish" disabled={busy}>{busy ? t("Saving configuration…") : t("Finish setup")}<ArrowRight size={15} /></button>
          </form> : null}
          {step !== 3 && error ? <div className="login-error setup-error" role="alert">{error}</div> : null}
        </div>
        <footer className="setup-footer">
          <button type="button" onClick={() => setStep((current) => Math.max(0, current - 1))} disabled={step === 0}><ArrowLeft size={15} />{t("Back")}</button>
          {step < 3 ? <button type="button" className="primary-button" onClick={next}>{step === 0 ? t("Start configuration") : t("Continue")}<ArrowRight size={15} /></button> : null}
        </footer>
      </section>
    </main>
  );
}
