import { KeyRound, Layers3, ShieldCheck } from "lucide-react";
import { useI18n } from "../i18n";
import { PasswordForm } from "./PasswordForm";

export function PasswordChangeView({ username, onChanged }: { username: string; onChanged: () => void }) {
  const { t } = useI18n();
  return (
    <main className="password-change-page">
      <section className="password-change-card">
        <div className="password-change-brand"><span className="brand-mark"><Layers3 size={18} /></span><strong>RedisStreamScope</strong></div>
        <div className="login-icon"><KeyRound size={22} /></div>
        <h1>{t("Password update required")}</h1>
        <p><strong>{username}</strong> · {t("Change the initial password before continuing.")}</p>
        <PasswordForm onChanged={onChanged} />
        <div className="login-security"><ShieldCheck size={13} />{t("Your initial password is used only for account creation.")}</div>
      </section>
    </main>
  );
}
