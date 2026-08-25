import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Archive, CheckCircle2, ChevronDown, ExternalLink, RefreshCw, RotateCcw, ShieldCheck } from "lucide-react";
import { api } from "../api";
import { useI18n } from "../i18n";
import type { ConsumerGroup, PendingEntry, QuarantineRecord, RecoveryPlan, RecoveryPlanInput, ToastState } from "../types";

type Props = {
  connectionId: string;
  streamKey: string;
  groups: ConsumerGroup[];
  refreshRevision: number;
  canManageGroups: boolean;
  canWriteStreams: boolean;
  onChanged: () => Promise<void>;
  onInspectEntry: (entryId: string) => void;
  onToast: (toast: ToastState) => void;
};

const operationID = () => `ui-${Date.now()}-${globalThis.crypto?.randomUUID?.() ?? Math.random().toString(36).slice(2)}`;

export function RecoveryCenter({ connectionId, streamKey, groups, refreshRevision, canManageGroups, canWriteStreams, onChanged, onInspectEntry, onToast }: Props) {
  const { t } = useI18n();
  const [tab, setTab] = useState<"pending" | "dlq">("pending");
  return <div className="recovery-center">
    <div className="subtabs recovery-tabs">
      <button type="button" className={tab === "pending" ? "active" : ""} onClick={() => setTab("pending")}><RotateCcw size={16} />{t("Pending recovery")}</button>
      <button type="button" className={tab === "dlq" ? "active" : ""} onClick={() => setTab("dlq")}><Archive size={16} />{t("Dead letter queue")}</button>
    </div>
    {tab === "pending" ? <PendingRecoveryWorkspace connectionId={connectionId} streamKey={streamKey} groups={groups} refreshRevision={refreshRevision} canExecute={canManageGroups} onChanged={onChanged} onInspectEntry={onInspectEntry} onToast={onToast} /> : null}
    {tab === "dlq" ? <DeadLetterWorkspace connectionId={connectionId} streamKey={streamKey} refreshRevision={refreshRevision} canWriteStreams={canWriteStreams} onChanged={onChanged} onToast={onToast} /> : null}
  </div>;
}

function PendingRecoveryWorkspace({ connectionId, streamKey, groups, refreshRevision, canExecute, onChanged, onInspectEntry, onToast }: {
  connectionId: string;
  streamKey: string;
  groups: ConsumerGroup[];
  refreshRevision: number;
  canExecute: boolean;
  onChanged: () => Promise<void>;
  onInspectEntry: (entryId: string) => void;
  onToast: (toast: ToastState) => void;
}) {
  const { locale, t } = useI18n();
  const [groupName, setGroupName] = useState("");
  const [pending, setPending] = useState<PendingEntry[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const requestSequence = useRef(0);

  useEffect(() => {
    setGroupName((current) => groups.some((group) => group.name === current)
      ? current
      : groups.find((group) => group.pending > 0)?.name ?? groups[0]?.name ?? "");
  }, [groups]);

  const loadPending = useCallback(async () => {
    const requestID = ++requestSequence.current;
    if (!groupName) {
      setPending([]);
      setLoading(false);
      setError("");
      return;
    }
    setLoading(true);
    setError("");
    try {
      const response = await api.pending(connectionId, streamKey, groupName);
      if (requestID !== requestSequence.current) return;
      setPending(response.items);
    } catch (cause) {
      if (requestID !== requestSequence.current) return;
      setPending([]);
      setError(cause instanceof Error ? t(cause.message) : t("Unable to load pending messages."));
    } finally {
      if (requestID === requestSequence.current) setLoading(false);
    }
  }, [connectionId, groupName, streamKey, t]);

  useEffect(() => {
    void loadPending();
    return () => { requestSequence.current += 1; };
  }, [loadPending, refreshRevision]);

  const selectedGroup = groups.find((group) => group.name === groupName);
  return <div className="pending-recovery-workspace">
    <header className="pending-recovery-header">
      <label><span>{t("Consumer group")}</span><select value={groupName} disabled={!groups.length} onChange={(event) => setGroupName(event.target.value)}>{groups.map((group) => <option key={group.name} value={group.name}>{group.name} · {group.pending.toLocaleString(locale)} {t("pending")}</option>)}</select></label>
      <div><span>{t("Pending")}</span><strong>{selectedGroup?.pending.toLocaleString(locale) ?? "0"}</strong></div>
      <div><span>{t("Lag")}</span><strong>{selectedGroup ? (selectedGroup.lag < 0 ? "—" : selectedGroup.lag.toLocaleString(locale)) : "0"}</strong></div>
      <button type="button" onClick={() => void loadPending()} disabled={loading || !groupName}><RefreshCw size={16} />{t("Refresh")}</button>
    </header>

    {error ? <div className="delivery-inline-error" role="alert">{error}</div> : null}
    <div className="pending-recovery-list">
      <div className="pending-recovery-list-head"><span>{t("Entry")}</span><span>{t("Owner")}</span><span>{t("Idle")}</span><span>{t("Deliveries")}</span><span /></div>
      {pending.map((entry) => <div className="pending-recovery-row" key={entry.id}>
        <strong className="mono" title={entry.id}>{entry.id}</strong>
        <span className="mono" title={entry.consumer}>{entry.consumer}</span>
        <span>{formatDuration(entry.idleMs)}</span>
        <span>{entry.retryCount.toLocaleString(locale)}</span>
        <button type="button" onClick={() => onInspectEntry(entry.id)}><ExternalLink size={15} />{t("Inspect")}</button>
      </div>)}
      {!pending.length && !loading ? <div className="panel-empty">{t("No pending messages in this group.")}</div> : null}
      {loading ? <div className="panel-empty">{t("Loading pending messages…")}</div> : null}
    </div>

    <details className="advanced-recovery-tools">
      <summary><span>{t("Advanced tools")}</span><ChevronDown size={16} /></summary>
      <AdvancedRecoveryTools connectionId={connectionId} streamKey={streamKey} groups={groups} canExecute={canExecute} onChanged={onChanged} onToast={onToast} />
    </details>
  </div>;
}

function AdvancedRecoveryTools({ connectionId, streamKey, groups, canExecute, onChanged, onToast }: {
  connectionId: string;
  streamKey: string;
  groups: ConsumerGroup[];
  canExecute: boolean;
  onChanged: () => Promise<void>;
  onToast: (toast: ToastState) => void;
}) {
  const { locale, t } = useI18n();
  const [action, setAction] = useState<"xautoclaim" | "xgroup-setid">("xautoclaim");
  const [group, setGroup] = useState(groups[0]?.name ?? "");
  const [consumer, setConsumer] = useState("");
  const [availableConsumers, setAvailableConsumers] = useState<string[]>([]);
  const [consumerLoading, setConsumerLoading] = useState(false);
  const [consumerError, setConsumerError] = useState("");
  const [minIdleMs, setMinIdleMs] = useState(60000);
  const [count, setCount] = useState(100);
  const [start, setStart] = useState("0-0");
  const [targetID, setTargetID] = useState("0-0");
  const [prepared, setPrepared] = useState<RecoveryPlanInput | null>(null);
  const [plan, setPlan] = useState<RecoveryPlan | null>(null);
  const [idempotencyKey, setIdempotencyKey] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [lastClaimed, setLastClaimed] = useState<{ consumer: string; ids: string[] } | null>(null);
  const consumerRequestSequence = useRef(0);
  const previewSequence = useRef(0);

  useEffect(() => {
    if (!groups.some((item) => item.name === group)) setGroup(groups[0]?.name ?? "");
  }, [group, groups]);
  useEffect(() => {
    const requestID = ++consumerRequestSequence.current;
    setConsumer("");
    setAvailableConsumers([]);
    setConsumerError("");
    setLastClaimed(null);
    if (action !== "xautoclaim" || !group) {
      setConsumerLoading(false);
      return;
    }
    setConsumerLoading(true);
    void api.consumers(connectionId, streamKey, group).then((response) => {
      if (requestID !== consumerRequestSequence.current) return;
      const names = Array.from(new Set(response.items.map((item) => item.name).filter(Boolean)));
      setAvailableConsumers(names);
      setConsumer("");
    }).catch((cause) => {
      if (requestID !== consumerRequestSequence.current) return;
      setConsumerError(cause instanceof Error ? t(cause.message) : t("Unable to load consumers."));
    }).finally(() => {
      if (requestID === consumerRequestSequence.current) setConsumerLoading(false);
    });
    return () => { consumerRequestSequence.current += 1; };
  }, [action, connectionId, group, streamKey, t]);
  useEffect(() => {
    previewSequence.current += 1;
    setPlan(null);
    setPrepared(null);
    setIdempotencyKey("");
    setBusy(false);
  }, [action, connectionId, consumer, count, group, minIdleMs, start, streamKey, targetID]);

  const invalidatePreview = () => {
    previewSequence.current += 1;
    setPlan(null);
    setPrepared(null);
    setIdempotencyKey("");
  };

  const input = useMemo<RecoveryPlanInput>(() => action === "xautoclaim"
    ? { action, connectionId, streamKey, group, consumer, minIdleMs, count, start }
    : { action, connectionId, streamKey, group, targetId: targetID }, [action, connectionId, consumer, count, group, minIdleMs, start, streamKey, targetID]);

  const prepare = async () => {
    if (action === "xautoclaim" && !consumer) return;
    const requestID = ++previewSequence.current;
    setBusy(true); setError(""); setPlan(null); setPrepared(null); setIdempotencyKey("");
    try {
      const response = await api.recoveryPlan(input);
      if (requestID !== previewSequence.current) return;
      setPrepared(input);
      setPlan(response);
      setIdempotencyKey(operationID());
    } catch (cause) {
      if (requestID !== previewSequence.current) return;
      setError(cause instanceof Error ? t(cause.message) : t("Unable to prepare the recovery plan."));
    } finally {
      if (requestID === previewSequence.current) setBusy(false);
    }
  };

  const execute = async () => {
    if (!plan || !prepared || !canExecute) return;
    setBusy(true); setError("");
    try {
      const result = await api.executeRecovery({ ...prepared, expectedCurrentGroupId: plan.expectedCurrentGroupId, confirmation: plan.confirmation, idempotencyKey });
      if (result.affected === 0) {
        onToast({ kind: "warning", title: t("No pending state changed"), message: t("The pending state changed before execution. Refresh and preview the action again.") });
      } else if (prepared.action === "xautoclaim") {
        const ids = result.messageIds ?? [];
        setLastClaimed({ consumer: prepared.consumer ?? "", ids });
        onToast({ kind: "success", title: t("PEL ownership reassigned"), message: t("{count} entries changed ownership. The target consumer must explicitly read its pending entries.", { count: result.affected }) });
      } else {
        onToast({ kind: "success", title: t("Recovery completed"), message: t("{count} entries were affected.", { count: result.affected }) });
      }
      setPlan(null); setPrepared(null); await onChanged();
    } catch (cause) { setError(cause instanceof Error ? t(cause.message) : t("Unable to execute recovery.")); }
    finally { setBusy(false); }
  };

  return <div className="recovery-workspace advanced-recovery-workspace">
    <div className="recovery-form-grid">
      <label>{t("Consumer group")}<select value={group} onChange={(event) => { invalidatePreview(); setGroup(event.target.value); }} disabled={busy || !groups.length}>{groups.map((item) => <option key={item.name} value={item.name}>{item.name}</option>)}</select></label>
      <label>{t("Action")}<select value={action} onChange={(event) => { invalidatePreview(); setAction(event.target.value as typeof action); }} disabled={busy}><option value="xautoclaim">XAUTOCLAIM</option><option value="xgroup-setid">XGROUP SETID</option></select></label>
      {action === "xautoclaim" ? <><label>{t("Target consumer")}<select value={consumer} onChange={(event) => { invalidatePreview(); setConsumer(event.target.value); }} disabled={busy || consumerLoading || !availableConsumers.length}><option value="">{consumerLoading ? t("Loading…") : availableConsumers.length ? t("Select a consumer") : t("No active consumers")}</option>{availableConsumers.map((name) => <option key={name} value={name}>{name}</option>)}</select></label><label>{t("Minimum idle (ms)")}<input type="number" min={0} value={minIdleMs} disabled={busy} onChange={(event) => { invalidatePreview(); setMinIdleMs(Number(event.target.value)); }} /></label><label>{t("Start ID")}<input className="mono" value={start} disabled={busy} onChange={(event) => { invalidatePreview(); setStart(event.target.value); }} /></label><label>{t("Maximum entries")}<input type="number" min={1} max={500} value={count} disabled={busy} onChange={(event) => { invalidatePreview(); setCount(Number(event.target.value)); }} /></label></> : null}
      {action === "xgroup-setid" ? <label>{t("Target group ID")}<input className="mono" value={targetID} disabled={busy} onChange={(event) => { invalidatePreview(); setTargetID(event.target.value); }} /></label> : null}
    </div>
    {action === "xautoclaim" ? <p className="advanced-recovery-caveat">{t("XAUTOCLAIM only reassigns PEL ownership. The target consumer must explicitly read its pending entries.")}</p> : null}
    {consumerError ? <div className="delivery-inline-error" role="alert">{consumerError}</div> : null}
    <div className="recovery-actions"><button type="button" onClick={() => void prepare()} disabled={busy || !group || (action === "xautoclaim" && !consumer)}>{busy ? t("Working…") : t("Preview plan")}</button></div>
    {error ? <div className="delivery-inline-error" role="alert">{error}</div> : null}
    {plan ? <div className="operation-plan">
      <header><div><ShieldCheck size={17} /><strong>{t("Execution plan")}</strong></div><span>{t("{count} eligible", { count: plan.candidateCount.toLocaleString(locale) })}</span></header>
      <div className="operation-plan-summary"><span>{plan.action.toUpperCase()}</span><span className="mono">{plan.group}</span>{prepared?.action === "xautoclaim" ? <span className="mono">{prepared.consumer}</span> : null}<span>{plan.requiredPermission}</span></div>
      {plan.pending.length ? <div className="operation-candidates">{plan.pending.slice(0, 8).map((item) => <div key={item.id}><span className="mono">{item.id}</span><span className="mono">{item.consumer}</span><span>{Math.round(item.idleMs / 1000)}s</span><span>{item.deliveryCount}×</span></div>)}</div> : null}
      {[...plan.warnings, ...plan.guarantees].map((message) => <p key={message}>{message}</p>)}
      {prepared?.action === "xautoclaim" ? <p>{t("XAUTOCLAIM only reassigns PEL ownership. The target consumer must explicitly read its pending entries.")}</p> : null}
      <footer><button className="primary-button" type="button" onClick={() => void execute()} disabled={busy || !canExecute || (action === "xautoclaim" && plan.candidateCount === 0)}><CheckCircle2 size={15} />{canExecute ? t("Execute plan") : t("Read-only access")}</button></footer>
    </div> : null}
    {lastClaimed ? <div className="advanced-recovery-result">
      <strong>{t("PEL ownership reassigned")}</strong>
      <span>{t("Target consumer")}: <b className="mono">{lastClaimed.consumer}</b></span>
      {lastClaimed.ids.length ? <div>{lastClaimed.ids.slice(0, 20).map((id) => <span className="mono" key={id}>{id}</span>)}</div> : null}
      <p>{t("The target consumer must explicitly read its pending entries.")}</p>
    </div> : null}
  </div>;
}

function DeadLetterWorkspace({ connectionId, streamKey, refreshRevision, canWriteStreams, onChanged, onToast }: {
  connectionId: string;
  streamKey: string;
  refreshRevision: number;
  canWriteStreams: boolean;
  onChanged: () => Promise<void>;
  onToast: (toast: ToastState) => void;
}) {
  const { t } = useI18n();
  const [records, setRecords] = useState<QuarantineRecord[]>([]);
  const [selected, setSelected] = useState<number[]>([]);
  const [actionPlan, setActionPlan] = useState<{ action: "replay" | "skip"; confirmation: string; eligibleCount: number; targetStream?: string; idempotencyKey: string } | null>(null);
  const [replayTarget, setReplayTarget] = useState(streamKey);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  const refreshRecords = useCallback(async () => {
    setBusy(true); setError("");
    try { setRecords((await api.quarantineRecords(connectionId, streamKey)).items); }
    catch (cause) { setRecords([]); setError(cause instanceof Error ? t(cause.message) : t("Unable to load DLQ records.")); }
    finally { setBusy(false); }
  }, [connectionId, streamKey, t]);
  useEffect(() => { setReplayTarget(streamKey); setActionPlan(null); setSelected([]); void refreshRecords(); }, [refreshRecords, refreshRevision, streamKey]);

  const prepareRecordAction = async (action: "replay" | "skip") => {
    if (!selected.length) return;
    setBusy(true); setError(""); setActionPlan(null);
    try {
      const targetStream = action === "replay" ? replayTarget : undefined;
      const next = await api.quarantineActionPlan({ action, connectionId, recordIds: selected, targetStream });
      setActionPlan({ action, confirmation: next.confirmation, eligibleCount: next.eligibleCount, targetStream, idempotencyKey: operationID() });
    } catch (cause) { setError(cause instanceof Error ? t(cause.message) : t("Unable to prepare the DLQ action.")); }
    finally { setBusy(false); }
  };

  const executeRecordAction = async () => {
    if (!actionPlan || !canWriteStreams) return;
    setBusy(true); setError("");
    try {
      await api.executeQuarantineAction({ action: actionPlan.action, connectionId, recordIds: selected, targetStream: actionPlan.targetStream, confirmation: actionPlan.confirmation, idempotencyKey: actionPlan.idempotencyKey });
      onToast({ kind: "success", title: t("DLQ updated"), message: t("The selected records were updated.") });
      setActionPlan(null); setSelected([]); await Promise.all([refreshRecords(), onChanged()]);
    } catch (cause) { setError(cause instanceof Error ? t(cause.message) : t("Unable to update the DLQ records.")); }
    finally { setBusy(false); }
  };

  return <div className="dead-letter-workspace">
    <header><strong>{t("Dead letter records")}</strong><button type="button" onClick={() => void refreshRecords()} disabled={busy}><RefreshCw size={16} />{t("Refresh")}</button></header>
    {error ? <div className="delivery-inline-error" role="alert">{error}</div> : null}
    <div className="dead-letter-list">
      {records.map((record) => <label className="dead-letter-record" key={record.id}>
        <input type="checkbox" disabled={record.status !== "quarantined"} checked={selected.includes(record.id)} onChange={(event) => { setSelected((current) => event.target.checked ? [...current, record.id] : current.filter((id) => id !== record.id)); setActionPlan(null); }} />
        <span><small>{t("Source entry")}</small><strong className="mono" title={record.sourceId}>{record.sourceId}</strong></span>
        <span><small>{t("DLQ entry")}</small><strong className="mono" title={record.dlqEntryId}>{record.dlqEntryId}</strong></span>
        <span><small>{t("Status")}</small><strong className={`status-dot status-${record.status}`}>{t(record.status)}</strong></span>
      </label>)}
      {!records.length && !busy ? <div className="panel-empty">{t("No dead letter records.")}</div> : null}
    </div>
    {selected.length ? <div className="dead-letter-actions"><label><span>{t("Replay target stream")}</span><input className="mono" value={replayTarget} onChange={(event) => { setReplayTarget(event.target.value); setActionPlan(null); }} /></label><button type="button" onClick={() => void prepareRecordAction("replay")} disabled={busy}>{t("Preview replay")}</button><button type="button" onClick={() => void prepareRecordAction("skip")} disabled={busy}>{t("Preview skip")}</button></div> : null}
    {actionPlan ? <div className="operation-plan compact"><p>{t("{count} records are eligible for {action}.", { count: actionPlan.eligibleCount, action: actionPlan.action })}</p><footer><button className="primary-button" type="button" onClick={() => void executeRecordAction()} disabled={busy || !canWriteStreams}>{busy ? t("Working…") : t("Execute {action}", { action: actionPlan.action })}</button></footer></div> : null}
  </div>;
}

function formatDuration(milliseconds: number) {
  const duration = Math.max(0, milliseconds);
  if (duration < 1000) return `${duration} ms`;
  if (duration < 60000) return `${Math.round(duration / 1000)} s`;
  if (duration < 3600000) return `${Math.round(duration / 60000)} min`;
  return `${Math.round(duration / 3600000)} h`;
}
