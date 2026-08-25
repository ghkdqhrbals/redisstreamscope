import { useCallback, useEffect, useMemo, useState } from "react";
import { Archive, CheckCircle2, RotateCcw, ShieldCheck } from "lucide-react";
import { api } from "../api";
import { useI18n } from "../i18n";
import type { ConsumerGroup, QuarantineInput, QuarantinePlan, QuarantineRecord, RecoveryPlan, RecoveryPlanInput, ToastState } from "../types";

type Props = {
  connectionId: string;
  streamKey: string;
  groups: ConsumerGroup[];
  canManageGroups: boolean;
  canWriteStreams: boolean;
  onChanged: () => Promise<void>;
  onToast: (toast: ToastState) => void;
};

const splitIDs = (value: string) => Array.from(new Set(value.split(/[\s,]+/).map((item) => item.trim()).filter(Boolean)));
const operationID = () => `ui-${Date.now()}-${globalThis.crypto?.randomUUID?.() ?? Math.random().toString(36).slice(2)}`;

export function RecoveryCenter({ connectionId, streamKey, groups, canManageGroups, canWriteStreams, onChanged, onToast }: Props) {
  const { t } = useI18n();
  const [tab, setTab] = useState<"recovery" | "quarantine">("recovery");
  return <div className="recovery-center">
    <div className="subtabs recovery-tabs">
      <button type="button" className={tab === "recovery" ? "active" : ""} onClick={() => setTab("recovery")}><RotateCcw size={14} />{t("Recovery")}</button>
      <button type="button" className={tab === "quarantine" ? "active" : ""} onClick={() => setTab("quarantine")}><Archive size={14} />{t("Quarantine / DLQ")}</button>
    </div>
    {tab === "recovery" ? <RecoveryWorkspace connectionId={connectionId} streamKey={streamKey} groups={groups} canExecute={canManageGroups} onChanged={onChanged} onToast={onToast} /> : null}
    {tab === "quarantine" ? <QuarantineWorkspace connectionId={connectionId} streamKey={streamKey} groups={groups} canManageGroups={canManageGroups} canWriteStreams={canWriteStreams} onChanged={onChanged} onToast={onToast} /> : null}
  </div>;
}

function RecoveryWorkspace({ connectionId, streamKey, groups, canExecute, onChanged, onToast }: Omit<Props, "canWriteStreams" | "canManageGroups"> & { canExecute: boolean }) {
  const { locale, t } = useI18n();
  const [action, setAction] = useState<RecoveryPlanInput["action"]>("xautoclaim");
  const [group, setGroup] = useState(groups[0]?.name ?? "");
  const [consumer, setConsumer] = useState("redisstreamscope-recovery");
  const [ids, setIDs] = useState("");
  const [minIdleMs, setMinIdleMs] = useState(60000);
  const [count, setCount] = useState(100);
  const [start, setStart] = useState("0-0");
  const [targetID, setTargetID] = useState("0-0");
  const [prepared, setPrepared] = useState<RecoveryPlanInput | null>(null);
  const [plan, setPlan] = useState<RecoveryPlan | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    if (!groups.some((item) => item.name === group)) setGroup(groups[0]?.name ?? "");
  }, [group, groups]);
  useEffect(() => { setPlan(null); setPrepared(null); }, [action, connectionId, consumer, count, group, ids, minIdleMs, start, streamKey, targetID]);

  const input = useMemo<RecoveryPlanInput>(() => {
    const common = { action, connectionId, streamKey, group } as RecoveryPlanInput;
    if (action === "xack") return { ...common, ids: splitIDs(ids) };
    if (action === "xclaim") return { ...common, ids: splitIDs(ids), consumer, minIdleMs };
    if (action === "xautoclaim") return { ...common, consumer, minIdleMs, count, start };
    return { ...common, targetId: targetID };
  }, [action, connectionId, consumer, count, group, ids, minIdleMs, start, streamKey, targetID]);

  const preview = async () => {
    setBusy(true); setError("");
    try {
      const next = await api.recoveryPlan(input);
      setPrepared(input); setPlan(next);
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Unable to prepare the recovery plan."));
    } finally { setBusy(false); }
  };
  const execute = async () => {
    if (!plan || !prepared || !canExecute) return;
    setBusy(true); setError("");
    try {
      const result = await api.executeRecovery({
        ...prepared,
        expectedCurrentGroupId: plan.expectedCurrentGroupId,
        confirmation: plan.confirmation,
        idempotencyKey: operationID(),
      });
      onToast({ kind: "success", title: t("Recovery completed"), message: t("{count} entries were affected.", { count: result.affected }) });
      setPlan(null); setPrepared(null);
      await onChanged();
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Unable to execute recovery."));
    } finally { setBusy(false); }
  };

  return <div className="recovery-workspace">
    <div className="recovery-form-grid">
      <label>{t("Consumer group")}<select value={group} onChange={(event) => setGroup(event.target.value)} disabled={!groups.length}>{groups.map((item) => <option key={item.name} value={item.name}>{item.name}</option>)}</select></label>
      <label>{t("Action")}<select value={action} onChange={(event) => setAction(event.target.value as RecoveryPlanInput["action"])}>
        <option value="xautoclaim">XAUTOCLAIM</option><option value="xclaim">XCLAIM</option><option value="xack">XACK</option><option value="xgroup-setid">XGROUP SETID</option>
      </select></label>
      {action === "xclaim" || action === "xautoclaim" ? <label>{t("Target consumer")}<input value={consumer} onChange={(event) => setConsumer(event.target.value)} /></label> : null}
      {action === "xclaim" || action === "xautoclaim" ? <label>{t("Minimum idle (ms)")}<input type="number" min={0} value={minIdleMs} onChange={(event) => setMinIdleMs(Number(event.target.value))} /></label> : null}
      {action === "xautoclaim" ? <><label>{t("Start ID")}<input className="mono" value={start} onChange={(event) => setStart(event.target.value)} /></label><label>{t("Maximum entries")}<input type="number" min={1} max={500} value={count} onChange={(event) => setCount(Number(event.target.value))} /></label></> : null}
      {action === "xack" || action === "xclaim" ? <label className="recovery-wide-field">{t("Entry IDs")}<textarea className="mono" rows={4} value={ids} onChange={(event) => setIDs(event.target.value)} placeholder={t("Whitespace or comma separated")} /></label> : null}
      {action === "xgroup-setid" ? <label>{t("Target group ID")}<input className="mono" value={targetID} onChange={(event) => setTargetID(event.target.value)} /></label> : null}
    </div>
    <div className="recovery-actions"><button type="button" onClick={() => void preview()} disabled={busy || !group}>{busy ? t("Working…") : t("Preview plan")}</button></div>
    {error ? <div className="metric-history-error" role="alert">{error}</div> : null}
    {plan ? <div className="operation-plan">
      <header><div><ShieldCheck size={17} /><strong>{t("Execution plan")}</strong></div><span>{t("{count} eligible", { count: plan.candidateCount.toLocaleString(locale) })}</span></header>
      <div className="operation-plan-summary"><span>{plan.action.toUpperCase()}</span><span className="mono">{plan.group}</span><span>{plan.requiredPermission}</span></div>
      {plan.pending.length ? <div className="operation-candidates">{plan.pending.slice(0, 8).map((item) => <div key={item.id}><span className="mono">{item.id}</span><span className="mono">{item.consumer}</span><span>{Math.round(item.idleMs / 1000)}s</span><span>{item.deliveryCount}×</span></div>)}</div> : null}
      {[...plan.warnings, ...plan.guarantees].map((message) => <p key={message}>{message}</p>)}
      <footer><button className="primary-button" type="button" onClick={() => void execute()} disabled={busy || !canExecute || plan.candidateCount === 0}><CheckCircle2 size={14} />{canExecute ? t("Execute plan") : t("Read-only access")}</button></footer>
    </div> : null}
  </div>;
}

function QuarantineWorkspace({ connectionId, streamKey, groups, canManageGroups, canWriteStreams, onChanged, onToast }: Props) {
  const { locale, t } = useI18n();
  const [ids, setIDs] = useState("");
  const [dlqStream, setDLQStream] = useState(`${streamKey}.dlq`);
  const [group, setGroup] = useState(groups[0]?.name ?? "");
  const [acknowledgeSource, setAcknowledgeSource] = useState(false);
  const [prepared, setPrepared] = useState<QuarantineInput | null>(null);
  const [plan, setPlan] = useState<QuarantinePlan | null>(null);
  const [records, setRecords] = useState<QuarantineRecord[]>([]);
  const [selected, setSelected] = useState<number[]>([]);
  const [actionPlan, setActionPlan] = useState<{ action: "replay" | "skip"; confirmation: string; eligibleCount: number; targetStream?: string } | null>(null);
  const [replayTarget, setReplayTarget] = useState(streamKey);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  const refreshRecords = useCallback(async () => {
    try { setRecords((await api.quarantineRecords(connectionId, streamKey)).items); } catch { setRecords([]); }
  }, [connectionId, streamKey]);
  useEffect(() => { setDLQStream(`${streamKey}.dlq`); setReplayTarget(streamKey); setPlan(null); setPrepared(null); setSelected([]); void refreshRecords(); }, [refreshRecords, streamKey]);
  useEffect(() => { if (!groups.some((item) => item.name === group)) setGroup(groups[0]?.name ?? ""); }, [group, groups]);
  useEffect(() => { if (!canManageGroups || !canWriteStreams) setAcknowledgeSource(false); }, [canManageGroups, canWriteStreams]);
  useEffect(() => { setPlan(null); setPrepared(null); }, [acknowledgeSource, connectionId, dlqStream, group, ids, streamKey]);

  const input = useMemo<QuarantineInput>(() => ({ connectionId, sourceStream: streamKey, sourceGroup: acknowledgeSource ? group : undefined, dlqStream, ids: splitIDs(ids), acknowledgeSource }), [acknowledgeSource, connectionId, dlqStream, group, ids, streamKey]);
  const canExecuteQuarantine = canWriteStreams && (!acknowledgeSource || canManageGroups);
  const preview = async () => {
    setBusy(true); setError("");
    try { const next = await api.quarantinePlan(input); setPrepared(input); setPlan(next); }
    catch (cause) { setError(cause instanceof Error ? t(cause.message) : t("Unable to prepare quarantine.")); }
    finally { setBusy(false); }
  };
  const execute = async () => {
    if (!plan || !prepared || !canExecuteQuarantine) return;
    setBusy(true); setError("");
    try {
      const result = await api.executeQuarantine({ ...prepared, confirmation: plan.confirmation, idempotencyKey: operationID() });
      onToast({ kind: "success", title: t("Quarantine completed"), message: t("{count} entries were copied to the DLQ.", { count: result.items.length }) });
      setPlan(null); setPrepared(null); setIDs(""); await refreshRecords(); await onChanged();
    } catch (cause) { setError(cause instanceof Error ? t(cause.message) : t("Unable to quarantine entries.")); }
    finally { setBusy(false); }
  };
  const prepareRecordAction = async (action: "replay" | "skip") => {
    if (!selected.length) return;
    setBusy(true); setError("");
    try {
      const targetStream = action === "replay" ? replayTarget : undefined;
      const next = await api.quarantineActionPlan({ action, connectionId, recordIds: selected, targetStream });
      setActionPlan({ action, confirmation: next.confirmation, eligibleCount: next.eligibleCount, targetStream });
    } catch (cause) { setError(cause instanceof Error ? t(cause.message) : t("Unable to prepare the DLQ action.")); }
    finally { setBusy(false); }
  };
  const executeRecordAction = async () => {
    if (!actionPlan || !canWriteStreams) return;
    setBusy(true); setError("");
    try {
      await api.executeQuarantineAction({ action: actionPlan.action, connectionId, recordIds: selected, targetStream: actionPlan.targetStream, confirmation: actionPlan.confirmation, idempotencyKey: operationID() });
      onToast({ kind: "success", title: t("DLQ updated"), message: t("The selected records were updated.") });
      setActionPlan(null); setSelected([]); await refreshRecords(); await onChanged();
    } catch (cause) { setError(cause instanceof Error ? t(cause.message) : t("Unable to update the DLQ records.")); }
    finally { setBusy(false); }
  };

  return <div className="recovery-workspace">
    <div className="recovery-form-grid">
      <label>{t("DLQ stream")}<input className="mono" value={dlqStream} onChange={(event) => setDLQStream(event.target.value)} /></label>
      <label className="checkbox-field"><input type="checkbox" checked={acknowledgeSource} disabled={!canWriteStreams || !canManageGroups || !groups.length} onChange={(event) => setAcknowledgeSource(event.target.checked)} /><span>{t("Acknowledge source after DLQ write")}</span></label>
      {acknowledgeSource ? <label>{t("Source consumer group")}<select value={group} disabled={!groups.length} onChange={(event) => setGroup(event.target.value)}>{groups.map((item) => <option key={item.name} value={item.name}>{item.name}</option>)}</select></label> : null}
      <label className="recovery-wide-field">{t("Entry IDs")}<textarea className="mono" rows={4} value={ids} onChange={(event) => setIDs(event.target.value)} placeholder={t("Whitespace or comma separated")} /></label>
    </div>
    <div className="recovery-actions"><button type="button" onClick={() => void preview()} disabled={busy || !input.ids.length}>{busy ? t("Working…") : t("Preview quarantine")}</button></div>
    {error ? <div className="metric-history-error" role="alert">{error}</div> : null}
    {plan ? <div className="operation-plan"><header><div><ShieldCheck size={17} /><strong>{t("Quarantine plan")}</strong></div><span>{t("{count} entries · {size}", { count: plan.entryCount, size: `${plan.payloadBytes.toLocaleString(locale)} B` })}</span></header>
      {plan.missingIds.length ? <p className="operation-warning">{t("Missing IDs: {ids}", { ids: plan.missingIds.join(", ") })}</p> : null}
      {plan.guarantees.map((message) => <p key={message}>{message}</p>)}
      <footer><button className="primary-button" type="button" onClick={() => void execute()} disabled={busy || !canExecuteQuarantine || plan.entryCount === 0 || plan.missingIds.length > 0}><Archive size={14} />{canExecuteQuarantine ? t("Execute quarantine") : t("Read-only access")}</button></footer>
    </div> : null}
    <div className="quarantine-records">
      <header><strong>{t("Quarantined entries")}</strong><button type="button" onClick={() => void refreshRecords()}>{t("Refresh")}</button></header>
      {records.map((record) => <label className="quarantine-record" key={record.id}>
        <input type="checkbox" disabled={record.status !== "quarantined"} checked={selected.includes(record.id)} onChange={(event) => setSelected((current) => event.target.checked ? [...current, record.id] : current.filter((id) => id !== record.id))} />
        <span className="mono">{record.sourceId}</span><span className={`status-dot status-${record.status}`}>{record.status}</span><span className="mono">{record.dlqStream}</span>
      </label>)}
      {!records.length ? <div className="panel-empty">{t("No quarantine records.")}</div> : null}
      {selected.length ? <div className="quarantine-actions"><input className="mono" value={replayTarget} onChange={(event) => { setReplayTarget(event.target.value); setActionPlan(null); }} aria-label={t("Replay target stream")} /><button type="button" onClick={() => void prepareRecordAction("replay")} disabled={busy}>{t("Preview replay")}</button><button type="button" onClick={() => void prepareRecordAction("skip")} disabled={busy}>{t("Preview skip")}</button></div> : null}
      {actionPlan ? <div className="operation-plan compact"><p>{t("{count} records are eligible for {action}.", { count: actionPlan.eligibleCount, action: actionPlan.action })}</p><footer><button className="primary-button" type="button" onClick={() => void executeRecordAction()} disabled={busy || !canWriteStreams}>{t("Execute {action}", { action: actionPlan.action })}</button></footer></div> : null}
    </div>
  </div>;
}
