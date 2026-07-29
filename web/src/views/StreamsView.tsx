import { FormEvent, useCallback, useDeferredValue, useEffect, useMemo, useState } from "react";
import {
  ArrowDownUp,
  ChevronLeft,
  ChevronRight,
  Clipboard,
  Pause,
  Play,
  Plus,
  Radio,
  RefreshCw,
  Search,
  Send,
  UsersRound,
  X,
} from "lucide-react";
import { api } from "../api";
import { InspectorResizeHandle } from "../components/InspectorResizeHandle";
import { useI18n } from "../i18n";
import type { ConsumerGroup, ConsumerInfo, PendingEntry, RedisConnection, RedisEntry, StreamItem, ToastState } from "../types";

type StreamsViewProps = {
  selectedStreamKey: string;
  onSelectedStreamChange: (key: string) => void;
  onToast: (toast: ToastState) => void;
};

type MessageSortKey = "id" | "timestamp" | "size" | "fields";

export function StreamsView({ selectedStreamKey, onSelectedStreamChange, onToast }: StreamsViewProps) {
  const { locale, t } = useI18n();
  const [connections, setConnections] = useState<RedisConnection[]>([]);
  const [connectionId, setConnectionId] = useState("");
  const [streams, setStreams] = useState<StreamItem[]>([]);
  const [streamCursor, setStreamCursor] = useState(0);
  const [hasMoreStreams, setHasMoreStreams] = useState(false);
  const [key, setKey] = useState("");
  const [entries, setEntries] = useState<RedisEntry[]>([]);
  const [entryCursor, setEntryCursor] = useState("");
  const [hasMoreEntries, setHasMoreEntries] = useState(false);
  const [groups, setGroups] = useState<ConsumerGroup[]>([]);
  const [selectedId, setSelectedId] = useState("");
  const [streamQuery, setStreamQuery] = useState("");
  const [search, setSearch] = useState("");
  const [messageSort, setMessageSort] = useState<{ key: MessageSortKey; direction: "asc" | "desc" }>({ key: "id", direction: "desc" });
  const [live, setLive] = useState(false);
  const [liveStatus, setLiveStatus] = useState<"idle" | "connecting" | "live" | "reconnecting">("idle");
  const [paused, setPaused] = useState(false);
  const [tab, setTab] = useState<"messages" | "groups" | "info">("info");
  const [showAdd, setShowAdd] = useState(false);
  const [selectedGroupName, setSelectedGroupName] = useState("");
  const [groupConsumers, setGroupConsumers] = useState<ConsumerInfo[]>([]);
  const [groupPending, setGroupPending] = useState<PendingEntry[]>([]);
  const [groupDetailLoading, setGroupDetailLoading] = useState(false);
  const [loadingMoreStreams, setLoadingMoreStreams] = useState(false);
  const [loadingMoreEntries, setLoadingMoreEntries] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const deferredStreamQuery = useDeferredValue(streamQuery.trim().toLowerCase());
  const deferredSearch = useDeferredValue(search.trim().toLowerCase());

  const loadEntriesAndGroups = useCallback(async (nextConnectionId: string, nextKey: string) => {
    if (!nextConnectionId || !nextKey) {
      setEntries([]);
      setGroups([]);
      return;
    }
    const [entryResponse, groupResponse] = await Promise.all([
      api.entries(nextConnectionId, nextKey),
      api.groups(nextConnectionId, nextKey).catch(() => ({ items: [] })),
    ]);
    setEntries(entryResponse.items);
    setEntryCursor(entryResponse.nextCursor);
    setHasMoreEntries(entryResponse.hasMore);
    setGroups(groupResponse.items);
    setSelectedId(entryResponse.items[0]?.id ?? "");
  }, []);

  const load = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const connectionResponse = await api.connections();
      setConnections(connectionResponse.items);
      const nextConnectionId = connectionId || connectionResponse.items[0]?.id || "";
      setConnectionId(nextConnectionId);
      if (!nextConnectionId) {
        setStreams([]);
        setEntries([]);
        return;
      }
      const streamResponse = await api.streams(nextConnectionId);
      setStreams(streamResponse.items);
      setStreamCursor(streamResponse.nextCursor);
      setHasMoreStreams(streamResponse.hasMore);
      const preferredKey = selectedStreamKey || key;
      const nextKey = streamResponse.items.some((stream) => stream.key === preferredKey) ? preferredKey : streamResponse.items[0]?.key || "";
      setKey(nextKey);
      onSelectedStreamChange(nextKey);
      await loadEntriesAndGroups(nextConnectionId, nextKey);
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Unable to load stream data."));
    } finally {
      setLoading(false);
    }
  }, [connectionId, key, loadEntriesAndGroups, onSelectedStreamChange, selectedStreamKey, t]);

  useEffect(() => { void load(); }, []); // Initial discovery only.

  useEffect(() => {
    if (!live || paused || !connectionId || !key) {
      setLiveStatus("idle");
      return;
    }
    setLiveStatus("connecting");
    const source = new EventSource(`/api/tail?connectionId=${encodeURIComponent(connectionId)}&key=${encodeURIComponent(key)}&lastId=$`);
    const receive = (event: MessageEvent) => {
      try {
        const payload = JSON.parse(event.data) as { id: string; fields: Record<string, string | number> };
        const timestamp = new Date(Number(payload.id.split("-")[0])).toISOString();
        setEntries((current) => [{ id: payload.id, fields: payload.fields, timestamp }, ...current.filter((entry) => entry.id !== payload.id)].slice(0, 100));
      } catch {
        // Ignore malformed live events and keep the current table stable.
      }
    };
    source.addEventListener("entry", receive as EventListener);
    source.onopen = () => setLiveStatus("live");
    source.onerror = () => setLiveStatus(source.readyState === EventSource.CONNECTING ? "reconnecting" : "idle");
    return () => {
      source.close();
      setLiveStatus("idle");
    };
  }, [connectionId, key, live, paused]);

  const changeConnection = async (nextConnectionId: string) => {
    setConnectionId(nextConnectionId);
    setLoading(true);
    try {
      const response = await api.streams(nextConnectionId);
      setStreams(response.items);
      setStreamCursor(response.nextCursor);
      setHasMoreStreams(response.hasMore);
      const nextKey = response.items[0]?.key ?? "";
      setKey(nextKey);
      onSelectedStreamChange(nextKey);
      setSelectedGroupName("");
      await loadEntriesAndGroups(nextConnectionId, nextKey);
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Unable to change the connection."));
    } finally {
      setLoading(false);
    }
  };

  const changeStream = async (nextKey: string) => {
    setKey(nextKey);
    onSelectedStreamChange(nextKey);
    setSelectedGroupName("");
    setTab("info");
    setLoading(true);
    try {
      await loadEntriesAndGroups(connectionId, nextKey);
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Unable to load the stream."));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (!selectedStreamKey || selectedStreamKey === key || !connectionId || !streams.some((stream) => stream.key === selectedStreamKey)) return;
    void changeStream(selectedStreamKey);
  }, [selectedStreamKey]);

  const filteredEntries = useMemo(() => {
    if (!deferredSearch) return entries;
    return entries.filter((entry) => `${entry.id} ${Object.entries(entry.fields).flat().join(" ")}`.toLowerCase().includes(deferredSearch));
  }, [deferredSearch, entries]);
  const filteredStreams = useMemo(() => {
    if (!deferredStreamQuery) return streams;
    return streams.filter((stream) => stream.key.toLowerCase().includes(deferredStreamQuery));
  }, [deferredStreamQuery, streams]);
  const displayedEntries = useMemo(() => [...filteredEntries].sort((left, right) => {
    let comparison = 0;
    if (messageSort.key === "id") comparison = compareStreamIds(left.id, right.id);
    if (messageSort.key === "timestamp") comparison = new Date(left.timestamp).getTime() - new Date(right.timestamp).getTime();
    if (messageSort.key === "size") comparison = entrySize(left) - entrySize(right);
    if (messageSort.key === "fields") comparison = Object.keys(left.fields).length - Object.keys(right.fields).length;
    return messageSort.direction === "asc" ? comparison : -comparison;
  }), [filteredEntries, messageSort]);
  const selectedIndex = displayedEntries.findIndex((entry) => entry.id === selectedId);
  const selectedEntry = selectedIndex >= 0 ? displayedEntries[selectedIndex] : null;
  const selectedStream = streams.find((stream) => stream.key === key);
  const selectedGroup = groups.find((group) => group.name === selectedGroupName) ?? null;
  const showEntryInspector = tab === "messages" && selectedEntry;
  const showGroupInspector = tab === "groups" && selectedGroup;
  const liveLabel = paused ? t("Paused") : liveStatus === "connecting" ? t("Connecting…") : liveStatus === "reconnecting" ? t("Reconnecting…") : liveStatus === "live" ? t("Listening") : t("Stopped");

  const moveSelection = (step: number) => {
    if (!displayedEntries.length) return;
    const next = Math.min(displayedEntries.length - 1, Math.max(0, selectedIndex + step));
    setSelectedId(displayedEntries[next].id);
  };

  const toggleMessageSort = (nextKey: MessageSortKey) => {
    setMessageSort((current) => current.key === nextKey
      ? { key: nextKey, direction: current.direction === "asc" ? "desc" : "asc" }
      : { key: nextKey, direction: "desc" });
  };

  const loadMoreStreams = async () => {
    if (!connectionId || !hasMoreStreams || loadingMoreStreams) return;
    setLoadingMoreStreams(true);
    setError("");
    try {
      const response = await api.streams(connectionId, streamCursor);
      setStreams((current) => {
        const existing = new Set(current.map((stream) => stream.key));
        return [...current, ...response.items.filter((stream) => !existing.has(stream.key))];
      });
      setStreamCursor(response.nextCursor);
      setHasMoreStreams(response.hasMore);
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Unable to load more streams."));
    } finally {
      setLoadingMoreStreams(false);
    }
  };

  const loadMoreEntries = async () => {
    if (!connectionId || !key || !entryCursor || !hasMoreEntries || loadingMoreEntries) return;
    setLoadingMoreEntries(true);
    setError("");
    try {
      const response = await api.entries(connectionId, key, 100, entryCursor);
      setEntries((current) => {
        const existing = new Set(current.map((entry) => entry.id));
        return [...current, ...response.items.filter((entry) => !existing.has(entry.id))];
      });
      setEntryCursor(response.nextCursor);
      setHasMoreEntries(response.hasMore);
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Unable to load more entries."));
    } finally {
      setLoadingMoreEntries(false);
    }
  };

  const openGroup = async (groupName: string) => {
    setTab("groups");
    setSelectedGroupName(groupName);
    setGroupConsumers([]);
    setGroupPending([]);
    setGroupDetailLoading(true);
    setError("");
    try {
      const [consumerResponse, pendingResponse] = await Promise.all([
        api.consumers(connectionId, key, groupName),
        api.pending(connectionId, key, groupName),
      ]);
      setGroupConsumers(consumerResponse.items);
      setGroupPending(pendingResponse.items);
    } catch (cause) {
      setGroupConsumers([]);
      setGroupPending([]);
      setError(cause instanceof Error ? t(cause.message) : t("Unable to load consumer details."));
    } finally {
      setGroupDetailLoading(false);
    }
  };

  return (
    <div className={`stream-layout ${showEntryInspector || showGroupInspector ? "" : "stream-layout--wide"}`}>
      <section className="stream-main">
        <div className="page-header">
          <div>
            <div className="breadcrumbs">{t("Streams")} <span>/</span> <strong>{key || t("No stream")}</strong></div>
            <h1>{key || t("Redis Streams")} {key ? <button title={t("Copy key")} aria-label={t("Copy key")} onClick={() => void navigator.clipboard?.writeText(key)}><Clipboard size={15} /></button> : null}</h1>
            <p><i className={connections.find((connection) => connection.id === connectionId)?.healthy ? "health-dot" : "health-dot health-dot--down"} />{connections.find((connection) => connection.id === connectionId)?.name ?? "Redis"}</p>
          </div>
          <div className="header-actions stream-selectors">
            <select value={connectionId} onChange={(event) => void changeConnection(event.target.value)}>{connections.map((connection) => <option value={connection.id} key={connection.id}>{connection.name}</option>)}</select>
            <select value={key} onChange={(event) => void changeStream(event.target.value)}>{streams.map((stream) => <option value={stream.key} key={stream.key}>{stream.key}</option>)}</select>
            <button onClick={() => void load()} disabled={loading}><RefreshCw size={15} />{loading ? t("Loading…") : t("Refresh")}</button>
            <button className="accent-button" disabled={!key} onClick={() => setShowAdd(true)}><Plus size={15} />{t("Add message")}</button>
          </div>
        </div>
        {error ? <div className="page-error">{error}</div> : null}

        <section className="stream-catalog">
          <header><h2>{t("Streams")}</h2><label><Search size={15} /><input value={streamQuery} onChange={(event) => setStreamQuery(event.target.value)} placeholder={t("Filter stream keys…")} /></label></header>
          <div className="stream-catalog-table">
            <div className="stream-catalog-head"><span>{t("Stream key")}</span><span>{t("Length")}</span><span>{t("Status")}</span><span /></div>
            {filteredStreams.map((stream) => <button key={stream.key} className={stream.key === key ? "selected" : ""} onClick={() => void changeStream(stream.key)}>
              <span className="mono">{stream.key}</span><span>{stream.length.toLocaleString(locale)}</span><span>{stream.key === key ? t("Opened") : t("Ready")}</span><ChevronRight size={16} />
            </button>)}
            {!filteredStreams.length && !loading ? <div className="panel-empty">{streams.length ? t("No streams match this filter.") : t("No streams match the current pattern.")}</div> : null}
            {hasMoreStreams && !streamQuery ? <div className="table-load-more"><button type="button" onClick={() => void loadMoreStreams()} disabled={loadingMoreStreams}>{loadingMoreStreams ? t("Loading more…") : t("Load more")}</button></div> : null}
          </div>
        </section>

        {key ? <div className="stream-detail">
          <div className="stream-detail-heading"><div><span>{t("Selected stream")}</span><h2 className="mono">{key}</h2></div><button onClick={() => void navigator.clipboard?.writeText(key)}><Clipboard size={14} />{t("Copy key")}</button></div>
          <div className="kpi-strip stream-kpis-real">
            <div><span>{t("Length")}</span><strong>{selectedStream?.length.toLocaleString(locale) ?? "0"}</strong></div>
            <div><span>{t("Consumer groups")}</span><strong>{groups.length}</strong></div>
            <div><span>{t("Loaded entries")}</span><strong>{entries.length}</strong></div>
            <div><span>{t("Last entry")}</span><strong className="mono small-value">{entries[0]?.id ?? "—"}</strong></div>
          </div>

          <section className="stream-data-section">
            <div className="surface-heading"><h2>{t("Messages")}</h2></div>
          <div className="table-toolbar">
            <label className="toolbar-search"><Search size={14} /><input value={search} onChange={(event) => setSearch(event.target.value)} placeholder={t("Search ID, field or value…")} /></label>
            <button className={`live-button ${live ? "active" : ""}`} aria-pressed={live} onClick={() => setLive((value) => !value)}><span /><span>{t("Live tail")}</span></button>
            <button className={paused ? "pause-button active" : "pause-button"} aria-label={paused ? t("Resume live tail") : t("Pause live tail")} onClick={() => setPaused((value) => !value)} disabled={!live}>{paused ? <Play size={14} /> : <Pause size={14} />}</button>
            {live ? <span className={`live-state live-state--${liveStatus}`}><Radio size={12} />{liveLabel}</span> : null}
          </div>
          <div className="message-table-scroll">
            <div className="message-table">
              <div className="message-table-head">
                <button className={messageSort.key === "id" ? "message-sort active" : "message-sort"} onClick={() => toggleMessageSort("id")}>ID <ArrowDownUp size={13} /></button>
                <button className={messageSort.key === "timestamp" ? "message-sort active" : "message-sort"} onClick={() => toggleMessageSort("timestamp")}>{t("Timestamp")} <ArrowDownUp size={13} /></button>
                <span>{t("Payload preview")}</span>
                <button className={messageSort.key === "size" ? "message-sort active" : "message-sort"} onClick={() => toggleMessageSort("size")}>{t("Size")} <ArrowDownUp size={13} /></button>
                <button className={messageSort.key === "fields" ? "message-sort active" : "message-sort"} onClick={() => toggleMessageSort("fields")}>{t("Fields")} <ArrowDownUp size={13} /></button>
              </div>
              <div className="message-table-body">
              {displayedEntries.map((entry) => <button className={`message-row ${entry.id === selectedId && tab === "messages" ? "selected" : ""}`} key={entry.id} onClick={() => { setSelectedId(entry.id); setTab("messages"); }}>
                <span className="mono id-cell">{entry.id}</span>
                <span className="mono timestamp-cell">{formatTimestamp(entry.timestamp, locale)}</span>
                <span className="mono fields-cell">{Object.entries(entry.fields).slice(0, 4).map(([field, value]) => `${field}=${String(value)}`).join("   ")}</span>
                <span className="mono size-cell">{entrySize(entry)} B</span>
                <span className="delivery-cell">{Object.keys(entry.fields).length}</span>
              </button>)}
              {!displayedEntries.length && !loading ? <div className="empty-table">{key ? t("No entries to display.") : t("No streams match the current pattern.")}</div> : null}
              </div>
            </div>
          </div>
          <div className="table-footer"><span>{t("Showing {visible} of {total} entries", { visible: displayedEntries.length, total: selectedStream?.length.toLocaleString(locale) ?? 0 })}</span><div><span>{t("Sorted by {key} · {direction}", { key: t(messageSort.key === "id" ? "ID" : messageSort.key === "timestamp" ? "Timestamp" : messageSort.key === "size" ? "Size" : "Fields"), direction: t(messageSort.direction === "asc" ? "ascending" : "descending") })}</span>{hasMoreEntries ? <button type="button" onClick={() => void loadMoreEntries()} disabled={loadingMoreEntries}>{loadingMoreEntries ? t("Loading more…") : t("Load more")}</button> : null}</div></div>
          </section>

          <section className="stream-data-section stream-groups-section">
          <div className="surface-heading"><h2>{t("Consumer groups")}</h2></div>
          <div className="simple-table">
            <div className="simple-head"><span>{t("Name")}</span><span>{t("Consumers")}</span><span>{t("Pending")}</span><span>{t("Lag")}</span><span>{t("Last delivered ID")}</span><span /></div>
            {groups.map((group) => <button className={`simple-row group-row ${selectedGroupName === group.name ? "selected" : ""}`} key={group.name} onClick={() => void openGroup(group.name)}><span><UsersRound size={15} />{group.name}</span><span>{group.consumers}</span><span>{group.pending}</span><span>{group.lag}</span><span className="mono">{group.lastDeliveredId}</span><ChevronRight size={16} /></button>)}
            {!groups.length ? <div className="panel-empty">{t("No consumer groups.")}</div> : null}
          </div>
          </section>

          <div className="stream-data-section info-grid">
          <section><h2>{t("Stream metadata")}</h2><div className="detail-list"><div><span>{t("Key")}</span><strong className="mono">{key || "—"}</strong></div><div><span>{t("Length")}</span><strong>{selectedStream?.length.toLocaleString(locale) ?? 0}</strong></div><div><span>{t("Groups")}</span><strong>{groups.length}</strong></div><div><span>{t("Latest ID")}</span><strong className="mono">{entries[0]?.id ?? "—"}</strong></div></div></section>
          </div>
        </div> : null}
      </section>

      {showEntryInspector ? <EntryInspector entry={showEntryInspector} stream={key} onClose={() => setSelectedId("")} onMove={moveSelection} onToast={onToast} /> : null}
      {showGroupInspector ? <ConsumerGroupInspector group={showGroupInspector} stream={key} consumers={groupConsumers} pending={groupPending} loading={groupDetailLoading} onClose={() => setSelectedGroupName("")} /> : null}
      {showAdd ? <AddMessageModal connectionId={connectionId} stream={key} onClose={() => setShowAdd(false)} onAdded={async (id) => {
        setShowAdd(false);
        await loadEntriesAndGroups(connectionId, key);
        onToast({ kind: "success", title: t("Message added"), message: t("Added entry {id}.", { id }) });
      }} /> : null}
    </div>
  );
}

function ConsumerGroupInspector({
  group,
  stream,
  consumers,
  pending,
  loading,
  onClose,
}: {
  group: ConsumerGroup;
  stream: string;
  consumers: ConsumerInfo[];
  pending: PendingEntry[];
  loading: boolean;
  onClose: () => void;
}) {
  const { t } = useI18n();
  const [selectedConsumer, setSelectedConsumer] = useState("");

  useEffect(() => {
    setSelectedConsumer((current) => consumers.some((consumer) => consumer.name === current) ? current : consumers[0]?.name ?? "");
  }, [consumers]);

  const consumer = consumers.find((item) => item.name === selectedConsumer) ?? null;
  const consumerPending = pending.filter((entry) => entry.consumer === selectedConsumer);

  return <aside className="inspector group-inspector">
    <InspectorResizeHandle />
    <header><div><strong>{t("Consumer group")}</strong><span className="mono">{group.name}</span></div><div className="inspector-actions"><button onClick={onClose} aria-label={t("Close consumer group details")}><X size={17} /></button></div></header>
    <div className="group-inspector-summary">
      <div><span>Stream</span><strong className="mono">{stream}</strong></div>
      <div><span>{t("Consumers")}</span><strong>{group.consumers}</strong></div>
      <div><span>{t("Pending")}</span><strong>{group.pending}</strong></div>
      <div><span>{t("Lag")}</span><strong>{group.lag}</strong></div>
      <div><span>{t("Last delivered")}</span><strong className="mono">{group.lastDeliveredId}</strong></div>
    </div>
    <section className="consumer-section">
      <header><h2>{t("Consumers")}</h2><span>{consumers.length}</span></header>
      {loading ? <div className="panel-empty">{t("Loading consumer activity…")}</div> : null}
      {!loading ? <div className="consumer-list">
        {consumers.map((item) => <button key={item.name} className={selectedConsumer === item.name ? "active" : ""} onClick={() => setSelectedConsumer(item.name)}>
          <span className="consumer-avatar">{item.name.slice(0, 2).toUpperCase()}</span>
          <span><strong className="mono">{item.name}</strong><em>{item.pending ? t("{count} pending", { count: item.pending }) : t("No pending messages")}</em></span>
          <span className={item.idleMs < 60000 ? "consumer-state active" : "consumer-state"}><i />{item.idleMs < 60000 ? t("Active") : t("Idle")}</span>
        </button>)}
        {!consumers.length ? <div className="panel-empty">{t("No consumers are registered in this group.")}</div> : null}
      </div> : null}
    </section>
    {consumer ? <section className="consumer-detail">
      <header><div><h2 className="mono">{consumer.name}</h2><p>{consumer.pending ? t("Processing pending messages.") : t("No pending messages are currently assigned.")}</p></div></header>
      <div className="detail-list">
        <div><span>{t("Status")}</span><strong>{consumer.idleMs < 60000 ? t("Active") : t("Idle")}</strong></div>
        <div><span>{t("Pending messages")}</span><strong>{consumer.pending}</strong></div>
        <div><span>{t("Idle")}</span><strong>{formatDuration(consumer.idleMs)}</strong></div>
        <div><span>{t("Inactive")}</span><strong>{formatDuration(consumer.inactiveMs)}</strong></div>
      </div>
      <div className="consumer-pending-head"><span>{t("Assigned message")}</span><span>{t("Deliveries")}</span><span>{t("Idle")}</span></div>
      <div className="consumer-pending-list">
        {consumerPending.map((entry) => <div key={entry.id}><strong className="mono">{entry.id}</strong><span>{entry.retryCount}</span><span>{formatDuration(entry.idleMs)}</span></div>)}
        {!consumerPending.length ? <div className="panel-empty">{t("No messages are assigned in the PEL.")}</div> : null}
      </div>
    </section> : null}
  </aside>;
}

function EntryInspector({ entry, stream, onClose, onMove, onToast }: { entry: RedisEntry; stream: string; onClose: () => void; onMove: (step: number) => void; onToast: (toast: ToastState) => void }) {
  const { locale, t } = useI18n();
  const copyPayload = async () => {
    await navigator.clipboard?.writeText(JSON.stringify(entry.fields, null, 2));
    onToast({ kind: "success", title: t("Payload copied"), message: t("The JSON payload was copied to the clipboard.") });
  };
  return <aside className="inspector">
    <InspectorResizeHandle />
    <header><div><strong>{t("Entry details")}</strong><span className="mono">{entry.id}</span></div><div className="inspector-actions"><button onClick={() => onMove(-1)} aria-label={t("Previous entry")}><ChevronLeft size={16} /></button><button onClick={() => onMove(1)} aria-label={t("Next entry")}><ChevronRight size={16} /></button><button onClick={onClose} aria-label={t("Close entry details")}><X size={17} /></button></div></header>
    <div className="inspector-tabs"><button className="active">{t("Payload")}</button></div>
    <div className="code-toolbar"><span>{t("Redis hash fields")}</span><button onClick={copyPayload}><Clipboard size={13} />{t("Copy JSON")}</button></div>
    <div className="code-view"><span className="line-no">1</span><code>{"{"}</code>{Object.entries(entry.fields).map(([field, value], index) => <div className="code-line" key={field}><span className="line-no">{index + 2}</span><code><i>"{field}"</i>: <b>"{String(value)}"</b>{index < Object.keys(entry.fields).length - 1 ? "," : ""}</code></div>)}<span className="line-no">{Object.keys(entry.fields).length + 2}</span><code>{"}"}</code></div>
    <div className="inspector-section"><h4>{t("Metadata")}</h4><div className="detail-list"><div><span>Stream</span><strong className="mono">{stream}</strong></div><div><span>{t("Timestamp")}</span><strong className="mono">{formatTimestamp(entry.timestamp, locale)}</strong></div><div><span>{t("Size")}</span><strong>{new Blob([JSON.stringify(entry.fields)]).size} B</strong></div><div><span>{t("Fields")}</span><strong>{Object.keys(entry.fields).length}</strong></div></div></div>
  </aside>;
}

function AddMessageModal({ connectionId, stream, onClose, onAdded }: { connectionId: string; stream: string; onClose: () => void; onAdded: (id: string) => void }) {
  const { t } = useI18n();
  const [fields, setFields] = useState([{ name: "", value: "" }]);
  const [maxLen, setMaxLen] = useState("");
  const [approximateMaxLen, setApproximateMaxLen] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const submit = async (event: FormEvent) => {
    event.preventDefault();
    const values = Object.fromEntries(fields.filter((field) => field.name).map((field) => [field.name, field.value]));
    if (!Object.keys(values).length) {
      setError(t("At least one field is required."));
      return;
    }
    const parsedMaxLen = maxLen ? Number(maxLen) : 0;
    if (maxLen && (!Number.isSafeInteger(parsedMaxLen) || parsedMaxLen < 1)) {
      setError(t("MAXLEN must be an integer greater than zero."));
      return;
    }
    setBusy(true);
    setError("");
    try {
      const response = await api.action("xadd", {
        connectionId,
        key: stream,
        id: "*",
        fields: values,
        ...(parsedMaxLen ? { maxLen: parsedMaxLen, exact: !approximateMaxLen } : {}),
      }) as { result?: string };
      onAdded(response.result ?? "created");
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Unable to add the message."));
    } finally {
      setBusy(false);
    }
  };
  return <div className="modal-backdrop" onMouseDown={onClose}><form className="modal" onSubmit={submit} onMouseDown={(event) => event.stopPropagation()}>
    <header><h2>{t("Add stream message")}</h2><button type="button" onClick={onClose} aria-label={t("Close add message dialog")}><X size={18} /></button></header>
    {fields.map((field, index) => <div className="field-row" key={index}><div className="field-pair"><label>{t("Field")}<input value={field.name} onChange={(event) => setFields((current) => current.map((item, itemIndex) => itemIndex === index ? { ...item, name: event.target.value } : item))} required /></label><label>{t("Value")}<input value={field.value} onChange={(event) => setFields((current) => current.map((item, itemIndex) => itemIndex === index ? { ...item, value: event.target.value } : item))} /></label></div><button type="button" className="remove-field" disabled={fields.length === 1} onClick={() => setFields((current) => current.filter((_, itemIndex) => itemIndex !== index))} aria-label={t("Remove field")}><X size={15} /></button></div>)}
    <button type="button" className="add-field" onClick={() => setFields((current) => [...current, { name: "", value: "" }])}><Plus size={14} />{t("Add field")}</button>
    <section className="maxlen-options">
      <label>{t("MAXLEN (optional)")}<input type="number" min="1" step="1" value={maxLen} onChange={(event) => setMaxLen(event.target.value)} placeholder="e.g. 100000" /></label>
      <label className="maxlen-checkbox"><input type="checkbox" checked={approximateMaxLen} onChange={(event) => setApproximateMaxLen(event.target.checked)} disabled={!maxLen} /><span>{t("Approximate trimming (`~`)")}</span></label>
      <p>{t("When set, MAXLEN is applied to this XADD. It is not stored as a persistent Redis setting.")}</p>
    </section>
    {error ? <div className="login-error">{error}</div> : null}
    <footer><button type="button" onClick={onClose}>{t("Cancel")}</button><button className="primary-button" disabled={busy}><Send size={14} />{busy ? t("Adding…") : t("Add message")}</button></footer>
  </form></div>;
}

function formatTimestamp(value: string, locale: string) {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString(locale, { hour12: false });
}

function entrySize(entry: RedisEntry) {
  return new Blob([JSON.stringify(entry.fields)]).size;
}

function compareStreamIds(left: string, right: string) {
  const [leftTime = "0", leftSequence = "0"] = left.split("-");
  const [rightTime = "0", rightSequence = "0"] = right.split("-");
  try {
    const timeDifference = BigInt(leftTime) - BigInt(rightTime);
    if (timeDifference !== 0n) return timeDifference < 0n ? -1 : 1;
    const sequenceDifference = BigInt(leftSequence) - BigInt(rightSequence);
    return sequenceDifference === 0n ? 0 : sequenceDifference < 0n ? -1 : 1;
  } catch {
    return left.localeCompare(right, undefined, { numeric: true });
  }
}

function formatDuration(milliseconds: number) {
  const duration = Math.max(0, milliseconds);
  if (duration < 1000) return `${duration} ms`;
  if (duration < 60000) return `${Math.round(duration / 1000)} s`;
  if (duration < 3600000) return `${Math.round(duration / 60000)} min`;
  return `${Math.round(duration / 3600000)} h`;
}
