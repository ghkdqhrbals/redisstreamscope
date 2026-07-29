import { FormEvent, useState } from "react";
import { ArrowRight, KeyRound, LockKeyhole } from "lucide-react";
import { api } from "../api";
import { useI18n } from "../i18n";

type PasswordFormProps = {
  submitLabel?: string;
  onChanged: () => void;
};

export function PasswordForm({ submitLabel, onChanged }: PasswordFormProps) {
  const { t } = useI18n();
  const [currentPassword, setCurrentPassword] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [confirmation, setConfirmation] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    setError("");
    if (newPassword !== confirmation) {
      setError(t("New password confirmation does not match."));
      return;
    }
    if (newPassword.length < 12) {
      setError(t("New password must be at least 12 characters."));
      return;
    }
    setBusy(true);
    try {
      await api.changePassword(currentPassword, newPassword);
      setCurrentPassword("");
      setNewPassword("");
      setConfirmation("");
      onChanged();
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Unable to change the password."));
    } finally {
      setBusy(false);
    }
  };

  return (
    <form className="password-form" onSubmit={submit}>
      <label>
        {t("Current password")}
        <span className="password-input">
          <LockKeyhole size={15} />
          <input type="password" autoComplete="current-password" value={currentPassword} onChange={(event) => setCurrentPassword(event.target.value)} required />
        </span>
      </label>
      <label>
        {t("New password")}
        <span className="password-input">
          <KeyRound size={15} />
          <input type="password" autoComplete="new-password" minLength={12} value={newPassword} onChange={(event) => setNewPassword(event.target.value)} required />
        </span>
      </label>
      <label>
        {t("Confirm new password")}
        <span className="password-input">
          <KeyRound size={15} />
          <input type="password" autoComplete="new-password" minLength={12} value={confirmation} onChange={(event) => setConfirmation(event.target.value)} required />
        </span>
      </label>
      <p className="password-hint">{t("Use at least 12 characters. Changing it signs out sessions in other browsers.")}</p>
      {error ? <div className="login-error" role="alert">{error}</div> : null}
      <button className="primary-button password-submit" disabled={busy}>
        {busy ? t("Changing…") : submitLabel ?? t("Change password")}
        {!busy ? <ArrowRight size={15} /> : null}
      </button>
    </form>
  );
}
