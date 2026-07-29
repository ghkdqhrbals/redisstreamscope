import { useCallback, useEffect, useMemo, useState } from "react";
import { AlertTriangle, Check, ChevronRight, CircleUserRound, RefreshCw, Search, Trash2, X } from "lucide-react";
import { api } from "../api";
import { InspectorResizeHandle } from "../components/InspectorResizeHandle";
import { useI18n } from "../i18n";
import type { ConsumerGroup, ConsumerInfo, PendingEntry, RedisConnection, StreamItem, ToastState } from "../types";

type GroupsViewProps = {
  initialConnectionId?: string;
  initialStreamKey?: string;
  onToast: (toast: ToastState) => void;
};

export function GroupsView({ initialConnectionId = "", initialStreamKey = "", onToast }: GroupsViewProps) {
  const { t } = useI18n();
  const [connections, setConnections] = useState<RedisConnection[]>([]);
  const [connectionId, setConnectionId] = useState(initialConnectionId);
  const [streams, setStreams] = useState<StreamItem[]>([]);
  const [key, setKey] = useState(initialStreamKey);
  const [groups, setGroups] = useState<ConsumerGroup[]>([]);
  const [groupName, setGroupName] = useState("");
  const [consumers, setConsumers] = useState<ConsumerInfo[]>([]);
  const [pending, setPending] = useState<PendingEntry[]>([]);
  const [selectedId, setSelectedId] = useState("");
  const [selectedConsumerName, setSelectedConsumerName] = useState("");
  const [detailTab, setDetailTab] = useState<"consumers" | "pending">("consumers");
  const [query, setQuery] = useState("");
  const [confirmDestroy, setConfirmDestroy] = useState(false);
  const [confirmText, setConfirmText] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const loadGroup = useCallback(async (nextConnectionId: string, nextKey: string, preferredGroup = "") => {
    if (!nextConnectionId || !nextKey) {
      setGroups([]);
      setGroupName("");
      setConsumers([]);
      setPending([]);
      return;
    }
    const groupItems = (await api.groups(nextConnectionId, nextKey)).items;
    setGroups(groupItems);
    const nextGroup = groupItems.some((item) => item.name === preferredGroup) ? preferredGroup : groupItems[0]?.name ?? "";
    setGroupName(nextGroup);
    const [consumerItems, pendingItems] = nextGroup
      ? await Promise.all([
        api.consumers(nextConnectionId, nextKey, nextGroup).then((response) => response.items),
        api.pending(nextConnectionId, nextKey, nextGroup).then((response) => response.items),
      ])
      : [[], []];
    setConsumers(consumerItems);
    setPending(pendingItems);
    setSelectedConsumerName(consumerItems[0]?.name ?? "");
    setSelectedId(pendingItems[0]?.id ?? "");
  }, []);

  const load = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const connectionItems = (await api.connections()).items;
      setConnections(connectionItems);
      const nextConnectionId = connectionId || connectionItems[0]?.id || "";
      setConnectionId(nextConnectionId);
      if (!nextConnectionId) return;
      const streamItems = (await api.streams(nextConnectionId)).items;
      setStreams(streamItems);
      const nextKey = streamItems.some((stream) => stream.key === key) ? key : streamItems[0]?.key || "";
      setKey(nextKey);
      await loadGroup(nextConnectionId, nextKey, groupName);
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Unable to load consumer group data."));
    } finally {
      setLoading(false);
    }
  }, [connectionId, groupName, key, loadGroup, t]);

  useEffect(() => { void load(); }, []); // Initial discovery only.

  const changeConnection = async (nextConnectionId: string) => {
    setConnectionId(nextConnectionId);
    setDetailTab("consumers");
    setQuery("");
    setLoading(true);
    setError("");
    try {
      const streamItems = (await api.streams(nextConnectionId)).items;
      setStreams(streamItems);
      const nextKey = streamItems[0]?.key ?? "";
      setKey(nextKey);
      await loadGroup(nextConnectionId, nextKey);
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Unable to change the connection."));
    } finally {
      setLoading(false);
    }
  };

  const changeStream = async (nextKey: string) => {
    setKey(nextKey);
    setDetailTab("consumers");
    setQuery("");
    setLoading(true);
    setError("");
    try {
      await loadGroup(connectionId, nextKey);
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Unable to change the stream."));
    } finally {
      setLoading(false);
    }
  };

  const changeGroup = async (nextGroup: string) => {
    setGroupName(nextGroup);
    setDetailTab("consumers");
    setQuery("");
    setLoading(true);
    setError("");
    try {
      const [consumerItems, pendingItems] = await Promise.all([
        api.consumers(connectionId, key, nextGroup).then((response) => response.items),
        api.pending(connectionId, key, nextGroup).then((response) => response.items),
      ]);
      setConsumers(consumerItems);
      setPending(pendingItems);
      setSelectedConsumerName(consumerItems[0]?.name ?? "");
      setSelectedId(pendingItems[0]?.id ?? "");
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Unable to load pending entries."));
    } finally {
      setLoading(false);
    }
  };

  const filteredPending = useMemo(() => pending.filter((entry) => `${entry.id} ${entry.consumer}`.toLowerCase().includes(query.toLowerCase())), [pending, query]);
  const filteredConsumers = useMemo(() => consumers.filter((consumer) => consumer.name.toLowerCase().includes(query.toLowerCase())), [consumers, query]);
  const selected = pending.find((entry) => entry.id === selectedId) ?? null;
  const selectedConsumer = consumers.find((consumer) => consumer.name === selectedConsumerName) ?? null;
  const selectedConsumerPending = pending.filter((entry) => entry.consumer === selectedConsumerName);
  const group = groups.find((item) => item.name === groupName);
  const connection = connections.find((item) => item.id === connectionId);
  const hasInspector = detailTab === "consumers" ? Boolean(selectedConsumer) : Boolean(selected);

  const acknowledge = async (entry: PendingEntry) => {
    try {
      await api.action("xack", { connectionId, key, group: groupName, ids: [entry.id] });
      setPending((current) => current.filter((item) => item.id !== entry.id));
      setSelectedId("");
      onToast({ kind: "success", title: t("Acknowledged"), message: t("Removed {id} from the PEL.", { id: entry.id }) });
    } catch (cause) {
      onToast({ kind: "error", title: t("Acknowledge failed"), message: cause instanceof Error ? t(cause.message) : t("XACK failed.") });
    }
  };

  const destroyGroup = async () => {
    try {
      await api.action("xgroup-destroy", { connectionId, key, group: groupName, confirm: `${key}/${groupName}` });
      setConfirmDestroy(false);
      setConfirmText("");
      const deletedName = groupName;
      await loadGroup(connectionId, key);
      onToast({ kind: "success", title: t("Group destroyed"), message: t("Destroyed consumer group {name}.", { name: deletedName }) });
    } catch (cause) {
      onToast({ kind: "error", title: t("Destroy failed"), message: cause instanceof Error ? t(cause.message) : t("Unable to destroy the consumer group.") });
    }
  };

  return (
    <div className={`groups-page ${hasInspector ? "" : "groups-page--wide"}`}>
      <section className="groups-main">
        <div className="page-header">
          <div><div className="breadcrumbs">{t("Consumer Groups")} <span>/</span> <strong>{groupName || t("No group")}</strong></div><h1>{groupName || t("Consumer groups")}</h1><p>{key || t("No stream selected")} <span>·</span><i className={connection?.healthy ? "health-dot" : "health-dot health-dot--down"} />{connection?.name ?? "Redis"}</p></div>
          <div className="header-actions stream-selectors">
            <select value={connectionId} onChange={(event) => void changeConnection(event.target.value)}>{connections.map((item) => <option key={item.id} value={item.id}>{item.name}</option>)}</select>
            <select value={key} onChange={(event) => void changeStream(event.target.value)}>{streams.map((stream) => <option key={stream.key} value={stream.key}>{stream.key}</option>)}</select>
            <select value={groupName} onChange={(event) => void changeGroup(event.target.value)}>{groups.map((item) => <option key={item.name} value={item.name}>{item.name}</option>)}</select>
            <button onClick={() => void load()} disabled={loading}><RefreshCw size={15} />{loading ? t("Loading…") : t("Refresh")}</button>
            <button disabled={!groupName} onClick={() => setConfirmDestroy(true)}><Trash2 size={14} />{t("Destroy")}</button>
          </div>
        </div>
        {error ? <div className="page-error">{error}</div> : null}
        <div className="kpi-strip group-kpis group-kpis-real">
          <div><span>{t("Pending")}</span><strong>{group?.pending ?? 0}</strong></div>
          <div><span>{t("Lag")}</span><strong>{group?.lag ?? 0}</strong></div>
          <div><span>{t("Consumers")}</span><strong>{group?.consumers ?? 0}</strong></div>
          <div><span>{t("Last delivered ID")}</span><strong className="mono small-value">{group?.lastDeliveredId ?? "—"}</strong></div>
        </div>
        <div className="group-picker"><span>{t("Groups")}</span><div>{groups.map((item) => <button key={item.name} className={item.name === groupName ? "active" : ""} onClick={() => void changeGroup(item.name)}><span className="mono">{item.name}</span><em>{t("{consumers} consumers · {pending} pending", { consumers: item.consumers, pending: item.pending })}</em><ChevronRight size={15} /></button>)}</div></div>
        <div className="content-tabs"><button className={detailTab === "consumers" ? "active" : ""} onClick={() => setDetailTab("consumers")}>{t("Consumers")}</button><button className={detailTab === "pending" ? "active" : ""} onClick={() => setDetailTab("pending")}>{t("Pending entries")}</button></div>
        <div className="table-toolbar groups-toolbar"><label className="toolbar-search"><Search size={14} /><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder={detailTab === "consumers" ? t("Search consumer…") : t("Search ID or consumer…")} /></label><span className="selection-count">{detailTab === "consumers" ? t("{count} consumers", { count: filteredConsumers.length }) : t("{count} pending entries", { count: filteredPending.length })}</span></div>
        {detailTab === "consumers" ? <div className="consumer-activity-table">
          <div className="consumer-activity-head"><span>{t("Consumer")}</span><span>{t("Status")}</span><span>{t("Pending")}</span><span>{t("Idle")}</span><span>{t("Inactive")}</span><span>{t("Assigned messages")}</span><span /></div>
          {filteredConsumers.map((consumer) => {
            const assigned = pending.filter((entry) => entry.consumer === consumer.name);
            return <button key={consumer.name} className={`consumer-activity-row ${selectedConsumerName === consumer.name ? "selected" : ""}`} onClick={() => setSelectedConsumerName(consumer.name)}>
              <span className="mono">{consumer.name}</span><span className={consumer.idleMs < 60000 ? "consumer-state active" : "consumer-state"}><i />{consumer.idleMs < 60000 ? t("Active") : t("Idle")}</span><span>{consumer.pending}</span><span>{formatDuration(consumer.idleMs)}</span><span>{formatDuration(consumer.inactiveMs)}</span><span className="mono">{assigned.slice(0, 3).map((entry) => entry.id).join(", ") || "—"}</span><ChevronRight size={15} />
            </button>;
          })}
          {!filteredConsumers.length && !loading ? <div className="empty-table">{groupName ? t("No consumers are registered.") : t("No consumer groups.")}</div> : null}
        </div> : <div className="pending-table">
          <div className="pending-head"><span /><span>{t("Message ID")}</span><span>{t("Owner consumer")}</span><span>{t("Idle time")}</span><span>{t("Deliveries")}</span><span>{t("Group")}</span><span>{t("Status")}</span></div>
          {filteredPending.map((entry) => <button className={`pending-row ${selectedId === entry.id ? "selected" : ""}`} key={entry.id} onClick={() => setSelectedId(entry.id)}>
            <span className="fake-check">{selectedId === entry.id ? <Check size={12} /> : null}</span><span className="mono">{entry.id}</span><span className="mono">{entry.consumer}</span><span className={entry.idleMs >= 30000 ? "amber" : ""}>{formatDuration(entry.idleMs)}</span><span>{entry.retryCount}</span><span className="mono payload-preview">{groupName}</span><span className="pending-status"><i />{t("Pending")}</span>
          </button>)}
          {!filteredPending.length && !loading ? <div className="empty-table">{groupName ? t("No pending entries.") : t("No consumer groups.")}</div> : null}
        </div>}
        <div className="table-footer"><span>{detailTab === "consumers" ? t("Showing {count} consumers", { count: filteredConsumers.length }) : t("Showing {count} pending entries", { count: filteredPending.length })}</span></div>
      </section>

      {detailTab === "consumers" && selectedConsumer ? <ConsumerActivityInspector consumer={selectedConsumer} pending={selectedConsumerPending} stream={key} group={groupName} onClose={() => setSelectedConsumerName("")} /> : null}
      {detailTab === "pending" && selected ? <aside className="pending-inspector">
        <InspectorResizeHandle />
        <header><div><strong>{t("Pending entry")}</strong><span className="mono">{selected.id}</span></div><div><button onClick={() => setSelectedId("")} aria-label={t("Close pending entry details")}><X size={16} /></button></div></header>
        <div className="detail-list pending-summary"><div><span>{t("Owner consumer")}</span><strong className="mono">{selected.consumer}</strong></div><div><span>{t("Idle time")}</span><strong className={selected.idleMs >= 30000 ? "amber" : ""}>{formatDuration(selected.idleMs)}</strong></div><div><span>{t("Deliveries")}</span><strong>{selected.retryCount}</strong></div><div><span>Stream</span><strong className="mono">{key}</strong></div><div><span>{t("Group")}</span><strong className="mono">{groupName}</strong></div></div>
        <div className="pending-actions"><div className="inline-warning"><AlertTriangle size={16} /><span>{t("Acknowledge removes this entry from the consumer group PEL.")}</span></div><div className="action-grid"><button disabled><CircleUserRound size={14} />{t("Claim requires a consumer")}</button><button className="confirm-action" onClick={() => void acknowledge(selected)}><Check size={14} />{t("Acknowledge")}</button></div></div>
      </aside> : null}

      {confirmDestroy ? <section className="destroy-drawer" role="dialog" aria-modal="true" aria-labelledby="destroy-group-title">
        <AlertTriangle size={42} /><div><h2 id="destroy-group-title">{t("Destroy consumer group")}</h2><p>{t("This action cannot be undone. Enter {value} to confirm.", { value: `${key}/${groupName}` })}</p></div>
        <label>{t("Confirmation")}<input value={confirmText} onChange={(event) => setConfirmText(event.target.value)} /></label>
        <button onClick={() => setConfirmDestroy(false)}>{t("Cancel")}</button>
        <button className="danger-button" disabled={confirmText !== `${key}/${groupName}`} onClick={() => void destroyGroup()}><Trash2 size={15} />{t("Destroy group")}</button>
      </section> : null}
    </div>
  );
}

function ConsumerActivityInspector({
  consumer,
  pending,
  stream,
  group,
  onClose,
}: {
  consumer: ConsumerInfo;
  pending: PendingEntry[];
  stream: string;
  group: string;
  onClose: () => void;
}) {
  const { t } = useI18n();
  return <aside className="pending-inspector consumer-activity-inspector">
    <InspectorResizeHandle />
    <header><div><strong>{t("Consumer activity")}</strong><span className="mono">{consumer.name}</span></div><div><button onClick={onClose} aria-label={t("Close consumer details")}><X size={16} /></button></div></header>
    <div className="consumer-activity-status">
      <span className={consumer.idleMs < 60000 ? "consumer-state active" : "consumer-state"}><i />{consumer.idleMs < 60000 ? t("Active") : t("Idle")}</span>
      <p>{consumer.pending ? t("Processing {count} pending messages.", { count: consumer.pending }) : t("No pending messages are currently being processed.")}</p>
    </div>
    <div className="detail-list pending-summary">
      <div><span>Stream</span><strong className="mono">{stream}</strong></div>
      <div><span>{t("Group")}</span><strong className="mono">{group}</strong></div>
      <div><span>{t("Pending")}</span><strong>{consumer.pending}</strong></div>
      <div><span>{t("Idle")}</span><strong>{formatDuration(consumer.idleMs)}</strong></div>
      <div><span>{t("Inactive")}</span><strong>{formatDuration(consumer.inactiveMs)}</strong></div>
    </div>
    <section className="consumer-assigned">
      <header><h2>{t("Assigned messages")}</h2><span>{pending.length}</span></header>
      <div className="consumer-pending-head"><span>{t("Message ID")}</span><span>{t("Deliveries")}</span><span>{t("Idle")}</span></div>
      <div className="consumer-pending-list">
        {pending.map((entry) => <div key={entry.id}><strong className="mono">{entry.id}</strong><span>{entry.retryCount}</span><span>{formatDuration(entry.idleMs)}</span></div>)}
        {!pending.length ? <div className="panel-empty">{t("No messages are assigned in the PEL.")}</div> : null}
      </div>
    </section>
  </aside>;
}

function formatDuration(milliseconds: number) {
  const duration = Math.max(0, milliseconds);
  if (duration < 1000) return `${duration} ms`;
  if (duration < 60000) return `${(duration / 1000).toFixed(1)} s`;
  return `${Math.floor(duration / 60000)}m ${Math.floor((duration % 60000) / 1000)}s`;
}
