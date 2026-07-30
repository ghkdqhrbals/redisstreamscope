import { FormEvent, useState } from "react";
import { ArrowRight, Eye, EyeOff, Layers3, LockKeyhole, Server } from "lucide-react";
import { LanguageSelect, useI18n } from "../i18n";

type LoginViewProps = {
  busy: boolean;
  error: string;
  onLogin: (username: string, password: string) => Promise<void>;
};

export function LoginView({ busy, error, onLogin }: LoginViewProps) {
  const { t } = useI18n();
  const [username, setUsername] = useState("admin");
  const [password, setPassword] = useState("");
  const [showPassword, setShowPassword] = useState(false);

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    await onLogin(username.trim(), password);
  };

  return (
    <main className="login-page">
      <div className="login-grid" aria-hidden="true" />
      <section className="login-brand">
        <div className="brand-mark brand-mark--large"><Layers3 size={25} /></div>
        <div>
          <div className="login-wordmark">RedisStreamScope</div>
          <p>{t("Redis Streams, clearly operational.")}</p>
        </div>
        <div className="login-signal">
          <div className="signal-line">
            <span>Redis Streams</span><strong>{t("Live operations")}</strong>
          </div>
          <div className="signal-bars">
            {[14, 22, 18, 31, 28, 42, 35, 53, 47, 62, 58, 72, 64, 81].map((height, index) => (
              <i key={index} style={{ height: `${height}%` }} />
            ))}
          </div>
          <div className="signal-meta">
            <span><i className="health-dot" /> {t("Your Redis connection")}</span>
            <span>RBAC · audit logs</span>
          </div>
        </div>
      </section>

      <section className="login-panel">
        <LanguageSelect className="login-language" />
        <div className="login-card">
          <div className="mobile-login-brand">
            <span className="brand-mark"><Layers3 size={18} /></span>
            <strong>RedisStreamScope</strong>
          </div>
          <div className="login-icon"><LockKeyhole size={22} /></div>
          <h1>{t("Administrator sign in")}</h1>
          <p>{t("Enter your administrator credentials to access the Redis operations console.")}</p>
          <form onSubmit={submit}>
            <label htmlFor="username">{t("Username")}</label>
            <div className="input-shell">
              <Server size={16} />
              <input
                id="username"
                autoComplete="username"
                value={username}
                onChange={(event) => setUsername(event.target.value)}
                required
              />
            </div>
            <label htmlFor="password">{t("Password")}</label>
            <div className="input-shell">
              <LockKeyhole size={16} />
              <input
                id="password"
                type={showPassword ? "text" : "password"}
                autoComplete="current-password"
                value={password}
                onChange={(event) => setPassword(event.target.value)}
                placeholder="••••••••••••"
                required
              />
              <button
                type="button"
                className="input-action"
                onClick={() => setShowPassword((current) => !current)}
                aria-label={showPassword ? t("Hide password") : t("Show password")}
              >
                {showPassword ? <EyeOff size={16} /> : <Eye size={16} />}
              </button>
            </div>
            {error ? <div className="login-error" role="alert">{error}</div> : null}
            <button className="primary-button login-button" disabled={busy}>
              {busy ? t("Checking…") : t("Sign in")}
              {!busy ? <ArrowRight size={16} /> : null}
            </button>
          </form>
          <div className="login-security">
            <LockKeyhole size={13} />
            {t("Your session is maintained using an encrypted secure cookie.")}
          </div>
        </div>
      </section>
    </main>
  );
}
