import {
  FormEvent,
  useCallback,
  useDeferredValue,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import {
  ArrowDownUp,
  ChevronDown,
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
  Trash2,
  UsersRound,
  WrapText,
  X,
} from "lucide-react";
import { api } from "../api";
import { InspectorResizeHandle } from "../components/InspectorResizeHandle";
import { ResizableGrid, type ResizableGridColumn } from "../components/ResizableGrid";
import { useI18n } from "../i18n";
import type { ConsumerGroup, ConsumerInfo, OverviewStreamItem, PendingEntry, RedisConnection, RedisEntry, StreamItem, ToastState } from "../types";

type StreamsViewProps = {
  selectedConnectionId?: string;
  selectedStreamKey: string;
  focusSection?: "groups" | null;
  onSelectedStreamChange: (key: string) => void;
  onToast: (toast: ToastState) => void;
};

type MessageSortKey = "id" | "timestamp" | "size" | "fields";
const messagePageSize = 100;

export function StreamsView({ selectedConnectionId = "", selectedStreamKey, focusSection = null, onSelectedStreamChange, onToast }: StreamsViewProps) {
  const { locale, t } = useI18n();
  const [connections, setConnections] = useState<RedisConnection[]>([]);
  const [connectionId, setConnectionId] = useState("");
  const [streams, setStreams] = useState<StreamItem[]>([]);
  const [overviewStreams, setOverviewStreams] = useState<OverviewStreamItem[]>([]);
  const [streamCursor, setStreamCursor] = useState(0);
  const [hasMoreStreams, setHasMoreStreams] = useState(false);
  const [key, setKey] = useState("");
  const [entries, setEntries] = useState<RedisEntry[]>([]);
  const [entryCursor, setEntryCursor] = useState("");
  const [entryPageCursor, setEntryPageCursor] = useState("+");
  const [entryCursorHistory, setEntryCursorHistory] = useState<string[]>([]);
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
  const [showMonitor, setShowMonitor] = useState(false);
  const [streamsSectionOpen, setStreamsSectionOpen] = useState(true);
  const [messagesSectionOpen, setMessagesSectionOpen] = useState(true);
  const [groupsSectionOpen, setGroupsSectionOpen] = useState(true);
  const [streamMutation, setStreamMutation] = useState("");
  const [selectedGroupName, setSelectedGroupName] = useState("");
  const [groupConsumers, setGroupConsumers] = useState<ConsumerInfo[]>([]);
  const [groupPending, setGroupPending] = useState<PendingEntry[]>([]);
  const [groupDetailLoading, setGroupDetailLoading] = useState(false);
  const [loadingMoreStreams, setLoadingMoreStreams] = useState(false);
  const [entryPageLoading, setEntryPageLoading] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const groupsSectionRef = useRef<HTMLElement>(null);
  const deferredStreamQuery = useDeferredValue(streamQuery.trim().toLowerCase());
  const deferredSearch = useDeferredValue(search.trim().toLowerCase());
  const streamColumns = useMemo<ResizableGridColumn[]>(() => [
    { id: "key", label: t("Stream key"), defaultWidth: 260, minWidth: 180, grow: true },
    { id: "entries", label: t("Entries"), defaultWidth: 100, minWidth: 80 },
    { id: "groups", label: t("Consumer groups"), defaultWidth: 145, minWidth: 115 },
    { id: "lag", label: t("Total lag"), defaultWidth: 105, minWidth: 85 },
    { id: "pending", label: t("Pending"), defaultWidth: 100, minWidth: 80 },
    { id: "last-consumed", label: t("Last consumed"), defaultWidth: 190, minWidth: 145 },
    { id: "actions", label: null, ariaLabel: t("Actions"), defaultWidth: 62, minWidth: 56 },
  ], [t]);
  const messageColumns = useMemo<ResizableGridColumn[]>(() => [
    { id: "id", label: "ID", defaultWidth: 180, minWidth: 130 },
    { id: "timestamp", label: t("Timestamp"), defaultWidth: 205, minWidth: 160 },
    { id: "payload", label: t("Payload preview"), defaultWidth: 420, minWidth: 220, grow: true },
    { id: "size", label: t("Size"), defaultWidth: 95, minWidth: 80 },
    { id: "fields", label: t("Fields"), defaultWidth: 90, minWidth: 80 },
  ], [t]);
  const groupColumns = useMemo<ResizableGridColumn[]>(() => [
    { id: "name", label: t("Name"), defaultWidth: 220, minWidth: 140, grow: true },
    { id: "consumers", label: t("Consumers"), defaultWidth: 105, minWidth: 85 },
    { id: "pending", label: t("Pending"), defaultWidth: 95, minWidth: 75 },
    { id: "lag", label: t("Lag"), defaultWidth: 90, minWidth: 70 },
    { id: "last-delivered", label: t("Last delivered ID"), defaultWidth: 220, minWidth: 150 },
    { id: "actions", label: null, ariaLabel: t("Actions"), defaultWidth: 38, minWidth: 28 },
  ], [t]);

  const loadEntriesAndGroups = useCallback(async (nextConnectionId: string, nextKey: string) => {
    if (!nextConnectionId || !nextKey) {
      setEntries([]);
      setEntryCursor("");
      setEntryPageCursor("+");
      setEntryCursorHistory([]);
      setHasMoreEntries(false);
      setGroups([]);
      return;
    }
    const [entryResponse, groupResponse] = await Promise.all([
      api.entries(nextConnectionId, nextKey, messagePageSize),
      api.groups(nextConnectionId, nextKey).catch(() => ({ items: [] })),
    ]);
    setEntries(entryResponse.items);
    setEntryCursor(entryResponse.nextCursor);
    setEntryPageCursor("+");
    setEntryCursorHistory([]);
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
      const nextConnectionId = selectedConnectionId || connectionId || connectionResponse.items[0]?.id || "";
      setConnectionId(nextConnectionId);
      if (!nextConnectionId) {
        setStreams([]);
        setOverviewStreams([]);
        setEntries([]);
        return;
      }
      const [streamResponse, overviewResponse] = await Promise.all([
        api.streams(nextConnectionId),
        api.overview(nextConnectionId),
      ]);
      setStreams(streamResponse.items);
      setOverviewStreams(overviewResponse.items);
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
  }, [connectionId, key, loadEntriesAndGroups, onSelectedStreamChange, selectedConnectionId, selectedStreamKey, t]);

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

  useEffect(() => {
    if (focusSection !== "groups" || loading) return;
    setMessagesSectionOpen(false);
    setGroupsSectionOpen(true);
    const frame = window.requestAnimationFrame(() => groupsSectionRef.current?.scrollIntoView({ behavior: "smooth", block: "start" }));
    return () => window.cancelAnimationFrame(frame);
  }, [focusSection, key, loading]);

  const filteredEntries = useMemo(() => {
    if (!deferredSearch) return entries;
    return entries.filter((entry) => `${entry.id} ${Object.entries(entry.fields).flat().join(" ")}`.toLowerCase().includes(deferredSearch));
  }, [deferredSearch, entries]);
  const filteredStreams = useMemo(() => {
    if (!deferredStreamQuery) return streams;
    return streams.filter((stream) => stream.key.toLowerCase().includes(deferredStreamQuery));
  }, [deferredStreamQuery, streams]);
  const overviewByKey = useMemo(
    () => new Map(overviewStreams.map((stream) => [stream.key, stream])),
    [overviewStreams],
  );
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
      const [response, overviewResponse] = await Promise.all([
        api.streams(connectionId, streamCursor),
        api.overview(connectionId),
      ]);
      setStreams((current) => {
        const existing = new Set(current.map((stream) => stream.key));
        return [...current, ...response.items.filter((stream) => !existing.has(stream.key))];
      });
      setOverviewStreams(overviewResponse.items);
      setStreamCursor(response.nextCursor);
      setHasMoreStreams(response.hasMore);
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Unable to load more streams."));
    } finally {
      setLoadingMoreStreams(false);
    }
  };

  const loadEntryPage = async (cursor: string, history: string[]) => {
    if (!connectionId || !key || entryPageLoading) return;
    setEntryPageLoading(true);
    setError("");
    setLive(false);
    setPaused(false);
    try {
      const response = await api.entries(connectionId, key, messagePageSize, cursor);
      setEntries(response.items);
      setEntryCursor(response.nextCursor);
      setEntryPageCursor(cursor);
      setEntryCursorHistory(history);
      setHasMoreEntries(response.hasMore);
      setSelectedId("");
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Unable to load the message page."));
    } finally {
      setEntryPageLoading(false);
    }
  };

  const loadNextEntryPage = async () => {
    if (!entryCursor || !hasMoreEntries) return;
    await loadEntryPage(entryCursor, [...entryCursorHistory, entryPageCursor]);
  };

  const loadPreviousEntryPage = async () => {
    if (!entryCursorHistory.length) return;
    const previousCursor = entryCursorHistory[entryCursorHistory.length - 1];
    await loadEntryPage(previousCursor, entryCursorHistory.slice(0, -1));
  };

  const monitorStreams = async (keys: string[]) => {
    let available = 0;
    let waiting = 0;
    for (const nextKey of keys) {
      const response = await api.monitorStream(connectionId, nextKey);
      if (response.available) available += 1;
      else waiting += 1;
    }
    const [streamResponse, overviewResponse] = await Promise.all([
      api.streams(connectionId),
      api.overview(connectionId),
    ]);
    setStreams(streamResponse.items);
    setOverviewStreams(overviewResponse.items);
    setStreamCursor(streamResponse.nextCursor);
    setHasMoreStreams(streamResponse.hasMore);
    setShowMonitor(false);
    window.dispatchEvent(new Event("redisstreamscope:streams-changed"));
    await changeStream(keys[0]);
    onToast({
      kind: "success",
      title: t("Stream keys added to monitoring"),
      message: t("Monitoring {available} · Waiting {waiting}", { available, waiting }),
    });
  };

  const unmonitorStream = async (streamKey: string) => {
    setStreamMutation(streamKey);
    setError("");
    try {
      await api.unmonitorStream(connectionId, streamKey);
      const [response, overviewResponse] = await Promise.all([
        api.streams(connectionId),
        api.overview(connectionId),
      ]);
      setStreams(response.items);
      setOverviewStreams(overviewResponse.items);
      setStreamCursor(response.nextCursor);
      setHasMoreStreams(response.hasMore);
      if (streamKey === key && !response.items.some((stream) => stream.key === streamKey)) {
        const nextKey = response.items.find((stream) => stream.available)?.key ?? "";
        await changeStream(nextKey);
      }
      window.dispatchEvent(new Event("redisstreamscope:streams-changed"));
      onToast({
        kind: "success",
        title: t("Removed from monitoring"),
        message: t("Stopped monitoring {key}.", { key: streamKey }),
      });
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Unable to remove the stream key."));
    } finally {
      setStreamMutation("");
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
          <div className="header-actions">
            <button onClick={() => void load()} disabled={loading}><RefreshCw size={15} />{loading ? t("Loading…") : t("Refresh")}</button>
          </div>
        </div>
        {error ? <div className="page-error">{error}</div> : null}

        <section className={`stream-catalog ${key ? "stream-catalog--connected" : ""}`}>
          <header>
            <button type="button" className="stream-section-toggle" aria-expanded={streamsSectionOpen} onClick={() => setStreamsSectionOpen((current) => !current)}>
              {streamsSectionOpen ? <ChevronDown size={17} /> : <ChevronRight size={17} />}
              <h2>{t("Streams")}</h2>
            </button>
            {streamsSectionOpen ? <div className="stream-catalog-actions">
              <label><Search size={15} /><input value={streamQuery} onChange={(event) => setStreamQuery(event.target.value)} placeholder={t("Filter stream keys…")} /></label>
              <button type="button" className="stream-monitor-button" disabled={!connectionId} onClick={() => setShowMonitor(true)}><Plus size={15} />{t("Add stream keys for monitoring")}</button>
            </div> : <strong className="stream-section-count">{streams.length.toLocaleString(locale)}</strong>}
          </header>
          {streamsSectionOpen ? <ResizableGrid className="stream-catalog-table" storageKey="streams-catalog-metrics" columns={streamColumns} headerClassName="stream-catalog-head" fixedLayout>
            {filteredStreams.map((stream) => {
              const metrics = overviewByKey.get(stream.key);
              return <div
                key={stream.key}
                className={`stream-catalog-row ${stream.key === key ? "selected" : ""}`}
              >
              <button
                type="button"
                className="stream-catalog-open"
                aria-label={`${stream.key} ${stream.length} ${metrics?.consumerGroups ?? 0} ${metrics?.totalLag ?? 0} ${metrics?.pending ?? 0} ${metrics?.lastConsumed || "—"}`}
                aria-expanded={stream.key === key}
                onClick={() => void changeStream(stream.key)}
              />
              <span className="stream-key-cell mono">
                <span>{stream.key}</span>
                {!stream.available ? <em>{t("Waiting")}</em> : null}
              </span>
              <span>{(metrics?.length ?? stream.length).toLocaleString(locale)}</span>
              <span>{(metrics?.consumerGroups ?? 0).toLocaleString(locale)}</span>
              <span>{metrics ? (metrics.lagKnown ? metrics.totalLag.toLocaleString(locale) : "—") : "—"}</span>
              <span>{(metrics?.pending ?? 0).toLocaleString(locale)}</span>
              <span className="mono stream-catalog-last-consumed" title={metrics?.lastConsumed || "—"}>{metrics?.lastConsumed || "—"}</span>
              <div className="stream-row-actions">
                {stream.key === key ? <ChevronDown size={17} /> : <ChevronRight size={16} />}
                {stream.monitored ? <button
                  type="button"
                  className="stream-unmonitor-button"
                  disabled={streamMutation === stream.key}
                  aria-label={t("Remove from monitoring")}
                  title={t("Remove from monitoring")}
                  onClick={() => void unmonitorStream(stream.key)}
                ><Trash2 size={15} /></button>
                  : null}
              </div>
              </div>;
            })}
            {!filteredStreams.length && !loading ? <div className="panel-empty">{streams.length ? t("No streams match this filter.") : t("No streams match the current pattern.")}</div> : null}
            {hasMoreStreams && !streamQuery ? <div className="table-load-more"><button type="button" onClick={() => void loadMoreStreams()} disabled={loadingMoreStreams}>{loadingMoreStreams ? t("Loading more…") : t("Load more")}</button></div> : null}
          </ResizableGrid> : null}
        </section>

        {key ? <div className="stream-detail">
          <div className="stream-detail-heading">
            <div className="stream-detail-identity"><h2 className="mono">{key}</h2></div>
            <button onClick={() => void navigator.clipboard?.writeText(key)}><Clipboard size={14} />{t("Copy key")}</button>
          </div>
          <div className="stream-detail-tree">
          <div className="kpi-strip stream-kpis-real">
            <div><span>{t("Length")}</span><strong>{selectedStream?.length.toLocaleString(locale) ?? "0"}</strong></div>
            <div><span>{t("Consumer groups")}</span><strong>{groups.length}</strong></div>
            <div><span>{t("Loaded entries")}</span><strong>{entries.length}</strong></div>
            <div><span>{t("Last entry")}</span><strong className="mono small-value">{entries[0]?.id ?? "—"}</strong></div>
          </div>

          <section className="stream-data-section">
            <div className="surface-heading stream-section-heading">
              <button type="button" className="stream-section-toggle" aria-expanded={messagesSectionOpen} onClick={() => setMessagesSectionOpen((current) => !current)}>
                {messagesSectionOpen ? <ChevronDown size={17} /> : <ChevronRight size={17} />}
                <span><h2>{t("Messages")}</h2></span>
              </button>
              <button type="button" className="stream-section-action" disabled={!key} onClick={() => setShowAdd(true)}><Plus size={15} />{t("Add message")}</button>
            </div>
          {messagesSectionOpen ? <>
          <div className="table-toolbar">
            <label className="toolbar-search"><Search size={14} /><input value={search} onChange={(event) => setSearch(event.target.value)} placeholder={t("Search ID, field or value…")} /></label>
            <button className={`live-button ${live ? "active" : ""}`} aria-pressed={live} onClick={() => setLive((value) => !value)}><span /><span>{t("Live tail")}</span></button>
            <button className={paused ? "pause-button active" : "pause-button"} aria-label={paused ? t("Resume live tail") : t("Pause live tail")} onClick={() => setPaused((value) => !value)} disabled={!live}>{paused ? <Play size={14} /> : <Pause size={14} />}</button>
            {live ? <span className={`live-state live-state--${liveStatus}`}><Radio size={12} />{liveLabel}</span> : null}
          </div>
          <div className="message-table-scroll">
            <ResizableGrid
              className="message-table"
              storageKey="stream-messages"
              columns={messageColumns}
              headerClassName="message-table-head"
              renderHeader={(column) => {
                if (column.id === "payload") return column.label;
                const sortKey = column.id as MessageSortKey;
                return <button className={messageSort.key === sortKey ? "message-sort active" : "message-sort"} onClick={() => toggleMessageSort(sortKey)}>{column.label} <ArrowDownUp size={13} /></button>;
              }}
            >
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
            </ResizableGrid>
          </div>
          <div className="table-footer">
            <span>{t("Showing {visible} of {total} entries", { visible: displayedEntries.length, total: selectedStream?.length.toLocaleString(locale) ?? 0 })}</span>
            <div>
              <span>{t("Sorted by {key} · {direction}", { key: t(messageSort.key === "id" ? "ID" : messageSort.key === "timestamp" ? "Timestamp" : messageSort.key === "size" ? "Size" : "Fields"), direction: t(messageSort.direction === "asc" ? "ascending" : "descending") })}</span>
              <div className="message-pagination">
                <button type="button" onClick={() => void loadPreviousEntryPage()} disabled={entryPageLoading || !entryCursorHistory.length}><ChevronLeft size={14} />{t("Previous")}</button>
                <span>{t("Page {page}", { page: entryCursorHistory.length + 1 })}</span>
                <button type="button" onClick={() => void loadNextEntryPage()} disabled={entryPageLoading || !hasMoreEntries}>{t("Next")}<ChevronRight size={14} /></button>
              </div>
            </div>
          </div>
          </> : null}
          </section>

          <section className="stream-data-section stream-groups-section" ref={groupsSectionRef}>
          <div className="surface-heading stream-section-heading">
            <button type="button" className="stream-section-toggle" aria-expanded={groupsSectionOpen} onClick={() => setGroupsSectionOpen((current) => !current)}>
              {groupsSectionOpen ? <ChevronDown size={17} /> : <ChevronRight size={17} />}
              <span><h2>{t("Consumer groups")}</h2></span>
            </button>
            <strong className="stream-section-count">{groups.length}</strong>
          </div>
          {groupsSectionOpen ? <ResizableGrid className="simple-table" storageKey="stream-consumer-groups" columns={groupColumns} headerClassName="simple-head">
            {groups.map((group) => <button className={`simple-row group-row ${selectedGroupName === group.name ? "selected" : ""}`} key={group.name} onClick={() => void openGroup(group.name)}><span><UsersRound size={15} />{group.name}</span><span>{group.consumers}</span><span>{group.pending}</span><span>{group.lag}</span><span className="mono stream-id-cell" title={group.lastDeliveredId}>{group.lastDeliveredId}</span><ChevronRight size={16} /></button>)}
            {!groups.length ? <div className="panel-empty">{t("No consumer groups.")}</div> : null}
          </ResizableGrid> : null}
          </section>
          </div>
        </div> : null}
      </section>

      {showEntryInspector ? <EntryInspector entry={showEntryInspector} stream={key} onClose={() => setSelectedId("")} onMove={moveSelection} /> : null}
      {showGroupInspector ? <ConsumerGroupInspector group={showGroupInspector} stream={key} consumers={groupConsumers} pending={groupPending} loading={groupDetailLoading} onClose={() => setSelectedGroupName("")} /> : null}
      {showAdd ? <AddMessageModal connectionId={connectionId} stream={key} onClose={() => setShowAdd(false)} onAdded={async (id) => {
        setShowAdd(false);
        await loadEntriesAndGroups(connectionId, key);
        onToast({ kind: "success", title: t("Message added"), message: t("Added entry {id}.", { id }) });
      }} /> : null}
      {showMonitor ? <MonitorStreamModal connectionId={connectionId} onClose={() => setShowMonitor(false)} onSubmit={monitorStreams} /> : null}
    </div>
  );
}

function ConsumerPendingTable({
  pending,
  storageKey,
  firstColumnLabel,
}: {
  pending: PendingEntry[];
  storageKey: string;
  firstColumnLabel: string;
}) {
  const { t } = useI18n();
  const columns = useMemo<ResizableGridColumn[]>(() => [
    { id: "message", label: firstColumnLabel, defaultWidth: 210, minWidth: 130, grow: true },
    { id: "deliveries", label: t("Deliveries"), defaultWidth: 82, minWidth: 68 },
    { id: "idle", label: t("Idle"), defaultWidth: 82, minWidth: 68 },
  ], [firstColumnLabel, t]);
  return (
    <ResizableGrid className="consumer-pending-table" storageKey={storageKey} columns={columns} headerClassName="consumer-pending-head">
      <div className="consumer-pending-list">
        {pending.map((entry) => <div key={entry.id}><strong className="mono">{entry.id}</strong><span>{entry.retryCount}</span><span>{formatDuration(entry.idleMs)}</span></div>)}
        {!pending.length ? <div className="panel-empty">{t("No messages are assigned in the PEL.")}</div> : null}
      </div>
    </ResizableGrid>
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
      <div><span>{t("Last delivered")}</span><strong className="mono stream-id-cell" title={group.lastDeliveredId}>{group.lastDeliveredId}</strong></div>
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
      <ConsumerPendingTable pending={consumerPending} storageKey="stream-group-inspector-pending" firstColumnLabel={t("Assigned message")} />
    </section> : null}
  </aside>;
}

function EntryInspector({ entry, stream, onClose, onMove }: { entry: RedisEntry; stream: string; onClose: () => void; onMove: (step: number) => void }) {
  const { locale, t } = useI18n();
  const [wrapLines, setWrapLines] = useState(true);
  const payload = useMemo(() => presentEntryPayload(entry.fields), [entry.fields]);
  return <aside className="inspector">
    <InspectorResizeHandle />
    <header><div><strong>{t("Entry details")}</strong><span className="mono">{entry.id}</span></div><div className="inspector-actions"><button onClick={() => onMove(-1)} aria-label={t("Previous entry")}><ChevronLeft size={16} /></button><button onClick={() => onMove(1)} aria-label={t("Next entry")}><ChevronRight size={16} /></button><button onClick={onClose} aria-label={t("Close entry details")}><X size={17} /></button></div></header>
    <div className="inspector-tabs"><button className="active">{t("Payload")}</button></div>
    <div className="code-toolbar"><span>{t(payload.format === "json" ? "JSON payload" : payload.format === "text" ? "Text payload" : "Redis fields")}</span><button className={wrapLines ? "active" : ""} aria-pressed={wrapLines} aria-label={wrapLines ? t("Disable line wrapping") : t("Wrap lines")} title={wrapLines ? t("Disable line wrapping") : t("Wrap lines")} onClick={() => setWrapLines((current) => !current)}><WrapText size={15} /></button></div>
    <pre className={`payload-code-view ${wrapLines ? "wrap" : ""}`}>{payload.content}</pre>
    <div className="inspector-section"><h4>{t("Metadata")}</h4><div className="detail-list"><div><span>Stream</span><strong className="mono">{stream}</strong></div><div><span>{t("Timestamp")}</span><strong className="mono">{formatTimestamp(entry.timestamp, locale)}</strong></div><div><span>{t("Size")}</span><strong>{new Blob([JSON.stringify(entry.fields)]).size} B</strong></div><div><span>{t("Fields")}</span><strong>{Object.keys(entry.fields).length}</strong></div></div></div>
  </aside>;
}

function MonitorStreamModal({ connectionId, onClose, onSubmit }: { connectionId: string; onClose: () => void; onSubmit: (keys: string[]) => Promise<void> }) {
  const { t } = useI18n();
  const [value, setValue] = useState("");
  const [missingKeys, setMissingKeys] = useState<string[]>([]);
  const [checkedSignature, setCheckedSignature] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const keys = useMemo(
    () => Array.from(new Set(value.split(/\r?\n/).map((item) => item.trim()).filter(Boolean))),
    [value],
  );
  const submit = async (event: FormEvent) => {
    event.preventDefault();
    if (keys.length > 100) {
      setError(t("You can add up to 100 stream keys at once."));
      return;
    }
    const signature = keys.join("\n");
    setBusy(true);
    setError("");
    try {
      if (missingKeys.length && checkedSignature === signature) {
        await onSubmit(keys);
        return;
      }
      const statuses: Array<{ key: string; available: boolean; exists: boolean; redisType: string }> = [];
      for (const key of keys) {
        statuses.push(await api.streamStatus(connectionId, key));
      }
      const invalid = statuses.filter((status) => status.exists && !status.available);
      if (invalid.length) {
        setError(t("These keys are not Redis Streams: {keys}", {
          keys: invalid.slice(0, 5).map((status) => `${status.key} (${status.redisType})`).join(", "),
        }));
        return;
      }
      const missing = statuses.filter((status) => !status.exists).map((status) => status.key);
      if (missing.length) {
        setMissingKeys(missing);
        setCheckedSignature(signature);
        return;
      }
      await onSubmit(statuses.map((status) => status.key));
    } catch (cause) {
      setError(cause instanceof Error ? t(cause.message) : t("Unable to add the stream keys to monitoring."));
    } finally {
      setBusy(false);
    }
  };
  return <div className="modal-backdrop" onMouseDown={onClose}><form className="modal stream-monitor-modal" onSubmit={submit} onMouseDown={(event) => event.stopPropagation()}>
    <header><h2>{t("Add stream keys for monitoring")}</h2><button type="button" onClick={onClose} aria-label={t("Close add stream keys for monitoring dialog")}><X size={18} /></button></header>
    <label>{t("Stream keys")}<textarea autoFocus value={value} onChange={(event) => { setValue(event.target.value); setMissingKeys([]); setCheckedSignature(""); setError(""); }} placeholder={t("One stream key per line · up to 100")} rows={7} required /></label>
    {missingKeys.length ? <div className="stream-key-warning" role="alert">
      <strong>{t("Stream keys not found ({count})", { count: missingKeys.length })}</strong>
      <ul>{missingKeys.slice(0, 6).map((key) => <li className="mono" key={key}>{key}</li>)}</ul>
      {missingKeys.length > 6 ? <p>{t("and {count} more", { count: missingKeys.length - 6 })}</p> : null}
      <p>{t("Add them to monitoring anyway and keep them in Waiting until they are created?")}</p>
    </div> : null}
    {error ? <div className="login-error">{error}</div> : null}
    <footer><button type="button" onClick={onClose}>{t("Cancel")}</button><button className="primary-button" disabled={busy || !keys.length}><Plus size={14} />{busy ? t(missingKeys.length ? "Adding…" : "Checking…") : missingKeys.length ? t("Monitor anyway") : keys.length ? t("Add {count} to monitoring", { count: keys.length }) : t("Add to monitoring")}</button></footer>
  </form></div>;
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

function presentEntryPayload(fields: Record<string, string | number>): { content: string; format: "json" | "text" | "fields" } {
  const keys = Object.keys(fields);
  const payloadKey = keys.find((key) => key.toLowerCase() === "payload");
  const value = payloadKey ? fields[payloadKey] : keys.length === 1 ? fields[keys[0]] : fields;
  if (typeof value !== "string") {
    return { content: JSON.stringify(value, null, 2), format: value === fields ? "fields" : "json" };
  }
  try {
    return { content: JSON.stringify(JSON.parse(value), null, 2), format: "json" };
  } catch {
    return { content: value, format: "text" };
  }
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
