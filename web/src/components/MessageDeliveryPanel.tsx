import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Archive, Check, RefreshCw, RotateCcw, ShieldCheck, X } from "lucide-react";
import { api } from "../api";
import { useI18n } from "../i18n";
import type {
  MessageDelivery,
  MessageDeliveryGroup,
  QuarantineInput,
  QuarantinePlan,
  RecoveryPlan,
  RecoveryPlanInput,
  ToastState,
} from "../types";
import { Select } from "./Select";

type Props = {
  connectionId: string;
  streamKey: string;
  entryId: string;
  initialGroupName?: string;
  sourceAvailable: boolean;
  canManageGroups: boolean;
  canWriteStreams: boolean;
  onChanged: () => Promise<void>;
  onToast: (toast: ToastState) => void;
};

type PendingActionPreview = {
  kind: "ack" | "retry";
  input: RecoveryPlanInput;
  plan: RecoveryPlan;
  idempotencyKey: string;
};

type QuarantinePreview = {
  kind: "dlq";
  input: QuarantineInput;
  plan: QuarantinePlan;
  idempotencyKey: string;
};

type ActionPreview = PendingActionPreview | QuarantinePreview;

const operationID = () => `ui-${Date.now()}-${globalThis.crypto?.randomUUID?.() ?? Math.random().toString(36).slice(2)}`;

export function MessageDeliveryPanel({ connectionId, streamKey, entryId, initialGroupName = "", sourceAvailable, canManageGroups, canWriteStreams, onChanged, onToast }: Props) {
  const { locale, t } = useI18n();
  const [delivery, setDelivery] = useState<MessageDelivery | null>(null);
  const [selectedGroupName, setSelectedGroupName] = useState("");
  const [retryConsumers, setRetryConsumers] = useState<Record<string, string>>({});
  const [dlqStream, setDLQStream] = useState(`${streamKey}.dlq`);
  const [acknowledgeSource, setAcknowledgeSource] = useState(false);
  const [preview, setPreview] = useState<ActionPreview | null>(null);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const requestSequence = useRef(0);
  const actionSequence = useRef(0);
  const currentEntryID = useRef(entryId);

  const load = useCallback(async () => {
    const requestID = ++requestSequence.current;
    setLoading(true);
    setError("");
    try {
      const response = await api.messageDelivery(connectionId, streamKey, entryId);
      if (requestID !== requestSequence.current) return;
      setDelivery(response);
      setSelectedGroupName((current) => {
        if (initialGroupName) {
          return response.groups.some((group) => group.group === initialGroupName) ? initialGroupName : "";
        }
        return response.groups.some((group) => group.group === current)
          ? current
          : response.groups.find((group) => group.pending)?.group ?? response.groups[0]?.group ?? "";
      });
      setRetryConsumers(Object.fromEntries(response.groups.map((group) => [
        group.group,
        group.availableConsumers.find((consumer) => consumer !== group.consumer) ?? group.availableConsumers[0] ?? "",
      ])));
    } catch (cause) {
      if (requestID !== requestSequence.current) return;
      setDelivery(null);
      setError(cause instanceof Error ? t(cause.message) : t("Unable to load delivery state."));
    } finally {
      if (requestID === requestSequence.current) setLoading(false);
    }
  }, [connectionId, entryId, initialGroupName, streamKey, t]);

  useEffect(() => {
    currentEntryID.current = entryId;
    actionSequence.current += 1;
    setDLQStream(`${streamKey}.dlq`);
    setPreview(null);
    setBusy(false);
    void load();
    return () => { requestSequence.current += 1; actionSequence.current += 1; };
  }, [entryId, load, streamKey]);

  const selectedGroup = delivery?.groups.find((group) => group.group === selectedGroupName) ?? null;
  useEffect(() => {
    actionSequence.current += 1;
    setPreview(null);
    setBusy(false);
    setAcknowledgeSource(Boolean(selectedGroup?.pending && canManageGroups));
  }, [canManageGroups, selectedGroupName, selectedGroup?.pending]);

  const targetConsumers = useMemo(() => {
    if (!selectedGroup) return [];
    return selectedGroup.availableConsumers;
  }, [selectedGroup]);

  const preparePendingAction = async (kind: "ack" | "retry", group: MessageDeliveryGroup) => {
    const actionID = ++actionSequence.current;
    const input: RecoveryPlanInput = kind === "ack"
      ? { action: "xack", connectionId, streamKey, group: group.group, ids: [entryId] }
      : {
        action: "xclaim",
        connectionId,
        streamKey,
        group: group.group,
        ids: [entryId],
        consumer: retryConsumers[group.group],
        minIdleMs: 0,
      };
    setBusy(true);
    setError("");
    setPreview(null);
    try {
      const plan = await api.recoveryPlan(input);
      if (actionID !== actionSequence.current) return;
      setPreview({ kind, input, plan, idempotencyKey: operationID() });
    } catch (cause) {
      if (actionID !== actionSequence.current) return;
      setError(cause instanceof Error ? t(cause.message) : t("Unable to prepare the message action."));
    } finally {
      if (actionID === actionSequence.current) setBusy(false);
    }
  };

  const prepareDLQ = async () => {
    if (!sourceAvailable) return;
    const actionID = ++actionSequence.current;
    const input: QuarantineInput = {
      connectionId,
      sourceStream: streamKey,
      sourceGroup: selectedGroup?.group,
      dlqStream,
      ids: [entryId],
      acknowledgeSource,
    };
    setBusy(true);
    setError("");
    setPreview(null);
    try {
      const plan = await api.quarantinePlan(input);
      if (actionID !== actionSequence.current) return;
      setPreview({ kind: "dlq", input, plan, idempotencyKey: operationID() });
    } catch (cause) {
      if (actionID !== actionSequence.current) return;
      setError(cause instanceof Error ? t(cause.message) : t("Unable to prepare the DLQ action."));
    } finally {
      if (actionID === actionSequence.current) setBusy(false);
    }
  };

  const execute = async () => {
    if (!preview) return;
    const executingEntryID = entryId;
    setBusy(true);
    setError("");
    try {
      if (preview.kind === "dlq") {
        const result = await api.executeQuarantine({
          ...preview.input,
          confirmation: preview.plan.confirmation,
          idempotencyKey: preview.idempotencyKey,
        });
        const copied = result.items.length;
        const sourceAckIncomplete = preview.input.acknowledgeSource && result.acknowledged < copied;
        onToast(sourceAckIncomplete
          ? {
            kind: "warning",
            title: t("Copied to DLQ; source ACK incomplete"),
            message: t("{copied} copied · {acknowledged} source ACKs", { copied, acknowledged: result.acknowledged }),
          }
          : {
            kind: "success",
            title: t("Sent to dead letter queue"),
            message: preview.input.acknowledgeSource
              ? t("{copied} copied · {acknowledged} source ACKs", { copied, acknowledged: result.acknowledged })
              : t("{count} entries were copied to the DLQ.", { count: copied }),
          });
      } else {
        const result = await api.executeRecovery({
          ...preview.input,
          expectedCurrentGroupId: preview.plan.expectedCurrentGroupId,
          confirmation: preview.plan.confirmation,
          idempotencyKey: preview.idempotencyKey,
        });
        onToast(result.affected === 0
          ? { kind: "warning", title: t("No pending state changed"), message: t("The entry may no longer be pending for this group. Delivery state was refreshed.") }
          : {
            kind: "success",
            title: preview.kind === "ack" ? t("Message acknowledged") : t("PEL ownership reassigned"),
            message: preview.kind === "retry"
              ? t("The target consumer must explicitly read its pending entries.")
              : t("{count} entries were affected.", { count: result.affected }),
          });
      }
      if (currentEntryID.current === executingEntryID) {
        setPreview(null);
        await Promise.all([load(), onChanged()]);
      } else {
        await onChanged();
      }
    } catch (cause) {
      if (currentEntryID.current === executingEntryID) setError(cause instanceof Error ? t(cause.message) : t("Unable to execute the message action."));
    } finally {
      if (currentEntryID.current === executingEntryID) setBusy(false);
    }
  };

  if (loading) return <div className="delivery-state-loading"><span className="brand-loader" />{t("Loading delivery state…")}</div>;
  if (error && !delivery) return <div className="delivery-state-error" role="alert"><span>{error}</span><button type="button" onClick={() => void load()}><RefreshCw size={15} />{t("Retry")}</button></div>;

  return <div className="message-delivery-panel">
    {!sourceAvailable ? <div className="delivery-source-missing">{t("The source payload is unavailable. PEL actions remain available, but this entry cannot be copied to a DLQ.")}</div> : null}
    <div className="delivery-summary">
      <span>{t("Consumer groups")}</span>
      <strong>{(delivery?.totalGroups ?? delivery?.groups.length ?? 0).toLocaleString(locale)}</strong>
      <span>{t("Pending groups")}</span>
      <strong>{delivery ? `${delivery.groups.filter((group) => group.pending).length.toLocaleString(locale)}${delivery.groupsTruncated ? "+" : ""}` : "0"}</strong>
      <button type="button" onClick={() => void load()} disabled={busy} aria-label={t("Refresh delivery state")}><RefreshCw size={15} /></button>
    </div>
    {delivery?.groupsTruncated ? <p className="delivery-truncation-note">{t("Showing {count} of {total} consumer groups.", { count: delivery.groups.length, total: delivery.totalGroups ?? delivery.groups.length })}</p> : null}

    {delivery?.groups.length ? <div className="delivery-group-list" aria-label={t("Delivery state by consumer group")}>
      {delivery.groups.map((group) => <button
        type="button"
        key={group.group}
        className={group.group === selectedGroupName ? "selected" : ""}
        onClick={() => { actionSequence.current += 1; setPreview(null); setBusy(false); setSelectedGroupName(group.group); }}
        aria-pressed={group.group === selectedGroupName}
      >
        <span className={`delivery-state-label ${group.pending ? "pending" : "not-pending"}`}><i />{t(group.pending ? "Pending" : "Not pending")}</span>
        <strong className="mono" title={group.group}>{group.group}</strong>
        <span><small>{t("Owner")}</small><b className="mono">{group.pending ? group.consumer || "—" : "—"}</b></span>
        <span><small>{t("Idle")}</small><b>{group.pending && group.idleMs !== undefined ? formatDuration(group.idleMs) : "—"}</b></span>
        <span><small>{t("Deliveries")}</small><b>{group.pending && group.deliveryCount !== undefined ? group.deliveryCount.toLocaleString(locale) : "—"}</b></span>
        <span><small>{t("Lag")}</small><b>{group.lag < 0 ? "—" : group.lag.toLocaleString(locale)}</b></span>
      </button>)}
    </div> : <div className="panel-empty">{t("No consumer groups are attached to this stream.")}</div>}
    {delivery?.notPendingMeaning && delivery.groups.some((group) => !group.pending) ? <p className="delivery-state-meaning">{t(delivery.notPendingMeaning)}</p> : null}

    {selectedGroup ? <section className="delivery-action-panel">
      <header>
        <strong className="mono">{selectedGroup.group}</strong>
        <span>{t(selectedGroup.pending ? "Pending message" : "Not pending")}</span>
      </header>
      {selectedGroup.pending ? <div className="delivery-pending-actions">
        <button type="button" onClick={() => void preparePendingAction("ack", selectedGroup)} disabled={busy || !canManageGroups}><Check size={16} />{t("Preview acknowledge")}</button>
        <label>
          <span>{t("Retry consumer")}</span>
          <Select
            className="select-control--block"
            value={retryConsumers[selectedGroup.group] ?? ""}
            onChange={(consumer) => {
              actionSequence.current += 1;
              setRetryConsumers((current) => ({ ...current, [selectedGroup.group]: consumer }));
              setPreview(null);
              setBusy(false);
            }}
            options={targetConsumers.map((consumer) => ({
              value: consumer,
              label: consumer,
              description: t("Target consumer"),
              meta: consumer === selectedGroup.consumer ? t("Owner") : undefined,
              keywords: `${consumer} ${consumer === selectedGroup.consumer ? t("Owner") : ""}`,
            }))}
            ariaLabel={t("Retry consumer")}
            placeholder={t("No active consumers")}
            searchable
            searchPlaceholder={t("Search consumer…")}
          />
          {selectedGroup.consumersTruncated ? <small>{t("Showing {count} of {total} consumers.", { count: selectedGroup.availableConsumers.length, total: selectedGroup.consumerCount ?? selectedGroup.availableConsumers.length })}</small> : null}
        </label>
        <button type="button" onClick={() => void preparePendingAction("retry", selectedGroup)} disabled={busy || !canManageGroups || !retryConsumers[selectedGroup.group]}><RotateCcw size={16} />{t("Preview NACK / retry")}</button>
        <small className="retry-mechanism">{t("XCLAIM reassigns PEL ownership. The target consumer must explicitly read pending entries; Redis Streams has no native NACK.")}</small>
      </div> : null}

      {canWriteStreams && sourceAvailable ? <div className="delivery-dlq-editor">
        <label><span>{t("Dead letter stream")}</span><input className="mono" value={dlqStream} onChange={(event) => { actionSequence.current += 1; setDLQStream(event.target.value); setPreview(null); setBusy(false); }} /></label>
        <label className="delivery-ack-source"><input type="checkbox" checked={acknowledgeSource} disabled={!selectedGroup.pending || !canManageGroups} onChange={(event) => { actionSequence.current += 1; setAcknowledgeSource(event.target.checked); setPreview(null); setBusy(false); }} /><span>{t("Acknowledge source after DLQ write")}</span></label>
        <button type="button" onClick={() => void prepareDLQ()} disabled={busy || !dlqStream.trim()}><Archive size={16} />{t("Preview send to DLQ")}</button>
      </div> : null}
    </section> : canWriteStreams && sourceAvailable && !initialGroupName ? <section className="delivery-action-panel delivery-action-panel--no-group">
      <div className="delivery-dlq-editor">
        <label><span>{t("Dead letter stream")}</span><input className="mono" value={dlqStream} onChange={(event) => { actionSequence.current += 1; setDLQStream(event.target.value); setPreview(null); setBusy(false); }} /></label>
        <button type="button" onClick={() => void prepareDLQ()} disabled={busy || !dlqStream.trim()}><Archive size={16} />{t("Preview send to DLQ")}</button>
      </div>
    </section> : null}

    {error ? <div className="delivery-inline-error" role="alert">{error}</div> : null}
    {preview ? <ActionPreviewCard preview={preview} busy={busy} canExecute={preview.kind === "dlq" ? canWriteStreams && (!preview.input.acknowledgeSource || canManageGroups) : canManageGroups} onCancel={() => setPreview(null)} onExecute={() => void execute()} /> : null}
  </div>;
}

function ActionPreviewCard({ preview, busy, canExecute, onCancel, onExecute }: { preview: ActionPreview; busy: boolean; canExecute: boolean; onCancel: () => void; onExecute: () => void }) {
  const { locale, t } = useI18n();
  const isDLQ = preview.kind === "dlq";
  const count = isDLQ ? preview.plan.entryCount : preview.plan.candidateCount;
  const warnings = isDLQ
    ? (preview.plan.missingIds.length ? [t("Missing IDs: {ids}", { ids: preview.plan.missingIds.join(", ") })] : [])
    : preview.plan.warnings;
  return <section className="message-action-preview" aria-live="polite">
    <header><span><ShieldCheck size={17} /><strong>{t("Action preview")}</strong></span><button type="button" onClick={onCancel} aria-label={t("Cancel preview")}><X size={16} /></button></header>
    <dl>
      <div><dt>{t("Action")}</dt><dd>{t(preview.kind === "ack" ? "Acknowledge" : preview.kind === "retry" ? "NACK / retry" : "Send to DLQ")}</dd></div>
      <div><dt>{t("Entry")}</dt><dd className="mono">{preview.input.ids?.[0] ?? "—"}</dd></div>
      <div><dt>{t("Eligible")}</dt><dd>{count.toLocaleString(locale)}</dd></div>
      {preview.kind !== "dlq" ? <div><dt>{t("Consumer group")}</dt><dd className="mono">{preview.input.group}</dd></div> : null}
      {preview.kind === "retry" ? <div><dt>{t("Target consumer")}</dt><dd className="mono">{preview.input.consumer}</dd></div> : null}
      {preview.kind === "dlq" ? <><div><dt>{t("Source consumer group")}</dt><dd className="mono">{preview.input.sourceGroup ?? "—"}</dd></div><div><dt>{t("Acknowledge source")}</dt><dd>{t(preview.input.acknowledgeSource ? "Yes" : "No")}</dd></div><div><dt>{t("Destination")}</dt><dd className="mono">{preview.input.dlqStream}</dd></div></> : null}
    </dl>
    {warnings.map((warning) => <p key={warning}>{warning}</p>)}
    {preview.kind === "retry" ? <p>{t("XCLAIM only reassigns PEL ownership. The target consumer must explicitly read its pending entries.")}{preview.input.consumer === preview.plan.pending[0]?.consumer ? ` ${t("The selected target is already the current owner.")}` : ""}</p> : null}
    <footer><button type="button" onClick={onCancel}>{t("Cancel")}</button><button className="primary-button" type="button" onClick={onExecute} disabled={busy || !canExecute || count === 0}>{busy ? t("Working…") : t("Execute action")}</button></footer>
  </section>;
}

function formatDuration(milliseconds: number) {
  const duration = Math.max(0, milliseconds);
  if (duration < 1000) return `${duration} ms`;
  if (duration < 60000) return `${Math.round(duration / 1000)} s`;
  if (duration < 3600000) return `${Math.round(duration / 60000)} min`;
  return `${Math.round(duration / 3600000)} h`;
}
