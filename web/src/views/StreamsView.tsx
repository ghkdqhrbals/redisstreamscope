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
  ArrowDown,
  ArrowDownUp,
  ArrowUp,
  ChevronDown,
  ChevronLeft,
  ChevronRight,
  Pause,
  Play,
  Plus,
  Radio,
  RefreshCw,
  Search,
  Send,
  Trash2,
  WrapText,
  X,
} from "lucide-react";
import { api } from "../api";
import { ConsumerGroupMetricsPanel } from "../components/ConsumerGroupMetricsPanel";
import { InspectorResizeHandle } from "../components/InspectorResizeHandle";
import { MessageDeliveryPanel } from "../components/MessageDeliveryPanel";
import { ResizableGrid, type ResizableGridColumn } from "../components/ResizableGrid";
import { Select } from "../components/Select";
import { useI18n } from "../i18n";
import type { ConsumerGroup, ConsumerInfo, OverviewStreamItem, PendingEntry, RedisConnection, RedisEntry, StreamItem, ToastState } from "../types";

type StreamsViewProps = {
  selectedConnectionId?: string;
  selectedStreamKey: string;
  focusSection?: "groups" | null;
  focusGroup?: string;
  canWrite: boolean;
  canManageGroups: boolean;
  onSelectedStreamChange: (target: { connectionId: string; key: string }) => void;
  onToast: (toast: ToastState) => void;
};

type MessageSortKey = "id" | "timestamp" | "size" | "fields";
type StreamDepth = "catalog" | "stream" | "group";
type MessageOrigin = { kind: "messages" } | { kind: "group"; groupName: string };
const messagePageSize = 100;
const streamMetricsModeProps = { mode: "stream" as const };
const groupMetricsModeProps = { mode: "group" as const };
const OPEN_STREAM_CATALOG_EVENT = "redisstreamscope:open-stream-catalog";

export function StreamsView({ selectedConnectionId = "", selectedStreamKey, focusSection = null, focusGroup = "", canWrite, canManageGroups, onSelectedStreamChange, onToast }: StreamsViewProps) {
  const { locale, t } = useI18n();
  const [connections, setConnections] = useState<RedisConnection[]>([]);
  const [connectionId, setConnectionId] = useState("");
  const [depth, setDepth] = useState<StreamDepth>(selectedStreamKey ? "stream" : "catalog");
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
  const [groupError, setGroupError] = useState("");
  const [selectedId, setSelectedId] = useState("");
  const [inspectedEntry, setInspectedEntry] = useState<RedisEntry | null>(null);
  const [inspectedEntrySourceMissing, setInspectedEntrySourceMissing] = useState(false);
  const [entryInspectorPreferredTab, setEntryInspectorPreferredTab] = useState<"payload" | "delivery">("payload");
  const [messageOrigin, setMessageOrigin] = useState<MessageOrigin>({ kind: "messages" });
  const [streamQuery, setStreamQuery] = useState("");
  const [search, setSearch] = useState("");
  const [messageSort, setMessageSort] = useState<{ key: MessageSortKey; direction: "asc" | "desc" }>({ key: "id", direction: "desc" });
  const [live, setLive] = useState(false);
  const [liveStatus, setLiveStatus] = useState<"idle" | "connecting" | "live" | "reconnecting">("idle");
  const [paused, setPaused] = useState(false);
  const [showAdd, setShowAdd] = useState(false);
  const [showMonitor, setShowMonitor] = useState(false);
  const [streamsSectionOpen, setStreamsSectionOpen] = useState(true);
  const [messagesSectionOpen, setMessagesSectionOpen] = useState(true);
  const [groupsSectionOpen, setGroupsSectionOpen] = useState(true);
  const [inspectorOverlay, setInspectorOverlay] = useState(false);
  const [streamMutation, setStreamMutation] = useState("");
  const [selectedGroupName, setSelectedGroupName] = useState("");
  const [groupConsumers, setGroupConsumers] = useState<ConsumerInfo[]>([]);
  const [groupPending, setGroupPending] = useState<PendingEntry[]>([]);
  const [groupDetailLoading, setGroupDetailLoading] = useState(false);
  const [loadingMoreStreams, setLoadingMoreStreams] = useState(false);
  const [entryPageLoading, setEntryPageLoading] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const messagesSectionRef = useRef<HTMLElement>(null);
  const groupsSectionRef = useRef<HTMLElement>(null);
  const streamMainRef = useRef<HTMLElement>(null);
  const selectedStreamKeyRef = useRef(selectedStreamKey);
  const discoveryLoadSequence = useRef(0);
  const streamContentLoadSequence = useRef(0);
  const streamSelectionSequence = useRef(0);
  const entryPageLoadSequence = useRef(0);
  const entryInspectSequence = useRef(0);
  const groupDetailLoadSequence = useRef(0);
  const deferredStreamQuery = useDeferredValue(streamQuery.trim().toLowerCase());
  const deferredSearch = useDeferredValue(search.trim().toLowerCase());
  selectedStreamKeyRef.current = selectedStreamKey;
  const streamColumns = useMemo<ResizableGridColumn[]>(() => [
    { id: "key", label: t("Stream key"), defaultWidth: 310, minWidth: 240, grow: true },
    { id: "entries", label: t("Entries"), defaultWidth: 78, minWidth: 72 },
    { id: "memory", label: t("Memory"), defaultWidth: 90, minWidth: 80 },
    { id: "groups", label: t("Consumer groups"), defaultWidth: 96, minWidth: 90 },
    { id: "lag", label: t("Total lag"), defaultWidth: 80, minWidth: 72 },
    { id: "pending", label: t("Pending"), defaultWidth: 80, minWidth: 72 },
    { id: "last-consumed", label: t("Last consumed"), defaultWidth: 160, minWidth: 145 },
    { id: "actions", label: null, ariaLabel: t("Actions"), defaultWidth: 50, minWidth: 48 },
  ], [t]);
  const messageColumns = useMemo<ResizableGridColumn[]>(() => [
    { id: "id", label: "ID", defaultWidth: 165, minWidth: 130 },
    { id: "timestamp", label: t("Timestamp"), defaultWidth: 190, minWidth: 160 },
    { id: "payload", label: t("Payload preview"), defaultWidth: 400, minWidth: 220, grow: true },
    { id: "size", label: t("Size"), defaultWidth: 90, minWidth: 80 },
    { id: "fields", label: t("Fields"), defaultWidth: 80, minWidth: 74 },
  ], [t]);
  const messageSortOptions = useMemo(() => (['id', 'timestamp', 'size', 'fields'] as MessageSortKey[]).flatMap((sortKey) =>
    (["desc", "asc"] as const).map((direction) => ({
      value: `${sortKey}:${direction}`,
      label: `${t(sortKey === "id" ? "ID" : sortKey === "timestamp" ? "Timestamp" : sortKey === "size" ? "Size" : "Fields")} · ${t(direction === "asc" ? "ascending" : "descending")}`,
    })),
  ), [t]);
  const loadEntriesAndGroups = useCallback(async (nextConnectionId: string, nextKey: string) => {
    const requestID = ++streamContentLoadSequence.current;
    entryPageLoadSequence.current += 1;
    entryInspectSequence.current += 1;
    groupDetailLoadSequence.current += 1;
    setEntries([]);
    setEntryCursor("");
    setEntryPageCursor("+");
    setEntryCursorHistory([]);
    setHasMoreEntries(false);
    setGroups([]);
    setGroupError("");
    setGroupConsumers([]);
    setGroupPending([]);
    setGroupDetailLoading(false);
    setEntryPageLoading(false);
    setSelectedId("");
    setInspectedEntry(null);
    setInspectedEntrySourceMissing(false);
    if (!nextConnectionId || !nextKey) {
      return;
    }
    const [entryResult, groupResult] = await Promise.allSettled([
      api.entries(nextConnectionId, nextKey, messagePageSize),
      api.groups(nextConnectionId, nextKey),
    ]);
    if (requestID !== streamContentLoadSequence.current) return;
    if (groupResult.status === "fulfilled") {
      setGroups(groupResult.value.items);
    } else {
      setGroups([]);
      setGroupError(groupResult.reason instanceof Error ? t(groupResult.reason.message) : t("Unable to load consumer details."));
    }
    if (entryResult.status === "rejected") throw entryResult.reason;
    const entryResponse = entryResult.value;
    setEntries(entryResponse.items);
    setEntryCursor(entryResponse.nextCursor);
    setEntryPageCursor("+");
    setEntryCursorHistory([]);
    setHasMoreEntries(entryResponse.hasMore);
    setInspectedEntry(null);
    setInspectedEntrySourceMissing(false);
    setSelectedId(entryResponse.items[0]?.id ?? "");
  }, [t]);

  const refreshDeliveryContext = useCallback(async () => {
    if (!connectionId || !key) return;
    const contentRequestID = streamContentLoadSequence.current;
    const detailGroupName = selectedGroupName;
    const detailRequestID = detailGroupName ? ++groupDetailLoadSequence.current : 0;
    const [groupResult, overviewResult, consumerResult, pendingResult] = await Promise.allSettled([
      api.groups(connectionId, key),
      api.overview(connectionId),
      detailGroupName ? api.consumers(connectionId, key, detailGroupName) : Promise.resolve({ items: [] }),
      detailGroupName ? api.pending(connectionId, key, detailGroupName) : Promise.resolve({ items: [] }),
    ]);
    if (contentRequestID !== streamContentLoadSequence.current) return;
    if (groupResult.status === "fulfilled") {
      setGroups(groupResult.value.items);
      setGroupError("");
    } else {
      setGroupError(groupResult.reason instanceof Error ? t(groupResult.reason.message) : t("Unable to load consumer details."));
    }
    if (overviewResult.status === "fulfilled") {
      setOverviewStreams(overviewResult.value.items);
      const current = overviewResult.value.items.find((stream) => stream.key === key);
      if (current) setStreams((items) => items.map((stream) => stream.key === key ? {
        ...stream,
        length: current.length,
        monitored: current.monitored,
        available: current.available,
        redisType: current.redisType,
      } : stream));
    }
    if (detailGroupName && detailRequestID === groupDetailLoadSequence.current) {
      setGroupConsumers(consumerResult.status === "fulfilled" ? consumerResult.value.items : []);
      setGroupPending(pendingResult.status === "fulfilled" ? pendingResult.value.items : []);
    }
  }, [connectionId, key, selectedGroupName, t]);

  const load = useCallback(async () => {
    const requestID = ++discoveryLoadSequence.current;
    const selectionID = ++streamSelectionSequence.current;
    setLoading(true);
    setError("");
    try {
      const connectionResponse = await api.connections();
      if (requestID !== discoveryLoadSequence.current || selectionID !== streamSelectionSequence.current) return;
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
      if (requestID !== discoveryLoadSequence.current || selectionID !== streamSelectionSequence.current) return;
      setStreams(streamResponse.items);
      setOverviewStreams(overviewResponse.items);
      setStreamCursor(streamResponse.nextCursor);
      setHasMoreStreams(streamResponse.hasMore);
      const preferredKey = selectedStreamKeyRef.current || key;
      const nextKey = preferredKey && streamResponse.items.some((stream) => stream.key === preferredKey) ? preferredKey : "";
      setKey(nextKey);
      setDepth(nextKey ? "stream" : "catalog");
      onSelectedStreamChange({ connectionId: nextConnectionId, key: nextKey });
      if (nextKey) await loadEntriesAndGroups(nextConnectionId, nextKey);
      else await loadEntriesAndGroups(nextConnectionId, "");
    } catch (cause) {
      if (requestID === discoveryLoadSequence.current && selectionID === streamSelectionSequence.current) {
        setError(cause instanceof Error ? t(cause.message) : t("Unable to load stream data."));
      }
    } finally {
      if (requestID === discoveryLoadSequence.current && selectionID === streamSelectionSequence.current) setLoading(false);
    }
  }, [connectionId, key, loadEntriesAndGroups, onSelectedStreamChange, selectedConnectionId, selectedStreamKey, t]);

  useEffect(() => { void load(); }, []); // Initial discovery only.

  const isFirstEntryPage = entryPageCursor === "+" && entryCursorHistory.length === 0;

  useEffect(() => {
    if (!live || paused || !isFirstEntryPage || !connectionId || !key) {
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
  }, [connectionId, isFirstEntryPage, key, live, paused]);

  const changeStream = async (nextKey: string) => {
    discoveryLoadSequence.current += 1;
    const selectionID = ++streamSelectionSequence.current;
    setKey(nextKey);
    setDepth(nextKey ? "stream" : "catalog");
    onSelectedStreamChange({ connectionId, key: nextKey });
    setSelectedGroupName("");
    setMessageOrigin({ kind: "messages" });
    setInspectedEntry(null);
    setInspectedEntrySourceMissing(false);
    setLoading(true);
    setError("");
    try {
      await loadEntriesAndGroups(connectionId, nextKey);
    } catch (cause) {
      if (selectionID === streamSelectionSequence.current) setError(cause instanceof Error ? t(cause.message) : t("Unable to load the stream."));
    } finally {
      if (selectionID === streamSelectionSequence.current) setLoading(false);
    }
  };

  const openCatalog = useCallback(() => {
    discoveryLoadSequence.current += 1;
    streamSelectionSequence.current += 1;
    streamContentLoadSequence.current += 1;
    entryPageLoadSequence.current += 1;
    entryInspectSequence.current += 1;
    groupDetailLoadSequence.current += 1;
    setDepth("catalog");
    setKey("");
    setEntries([]);
    setEntryCursor("");
    setEntryPageCursor("+");
    setEntryCursorHistory([]);
    setHasMoreEntries(false);
    setGroups([]);
    setGroupError("");
    setGroupConsumers([]);
    setGroupPending([]);
    setGroupDetailLoading(false);
    setEntryPageLoading(false);
    setSelectedGroupName("");
    setSelectedId("");
    setInspectedEntry(null);
    setInspectedEntrySourceMissing(false);
    setMessageOrigin({ kind: "messages" });
    setError("");
    setLoading(false);
    onSelectedStreamChange({ connectionId, key: "" });
  }, [connectionId, onSelectedStreamChange]);

  useEffect(() => {
    window.addEventListener(OPEN_STREAM_CATALOG_EVENT, openCatalog);
    return () => window.removeEventListener(OPEN_STREAM_CATALOG_EVENT, openCatalog);
  }, [openCatalog]);

  const openStreamDepth = (section?: "messages" | "groups") => {
    if (!key) {
      openCatalog();
      return;
    }
    setDepth("stream");
    groupDetailLoadSequence.current += 1;
    setSelectedGroupName("");
    setInspectedEntry(null);
    setInspectedEntrySourceMissing(false);
    if (section === "messages") setMessagesSectionOpen(true);
    if (section === "groups") setGroupsSectionOpen(true);
    if (section) {
      window.requestAnimationFrame(() => {
        const target = section === "messages" ? messagesSectionRef.current : groupsSectionRef.current;
        target?.scrollIntoView({ behavior: "smooth", block: "start" });
      });
    }
  };

  useEffect(() => {
    if (!connectionId) return;
    if (!selectedStreamKey) {
      if (key) openCatalog();
      else setDepth("catalog");
      return;
    }
    if (selectedStreamKey === key || !streams.some((stream) => stream.key === selectedStreamKey)) return;
    void changeStream(selectedStreamKey);
  }, [selectedStreamKey]);

  useEffect(() => {
    const frame = window.requestAnimationFrame(() => {
      streamMainRef.current?.scrollTo({ top: 0, left: 0 });
      streamMainRef.current?.querySelector<HTMLElement>("[data-depth-heading]")?.focus({ preventScroll: true });
    });
    return () => window.cancelAnimationFrame(frame);
  }, [depth]);

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
  const showEntryInspector = inspectedEntry?.id === selectedId ? inspectedEntry : null;
  const selectedStream = streams.find((stream) => stream.key === key);
  const selectedGroup = groups.find((group) => group.name === selectedGroupName) ?? null;
  const liveLabel = paused ? t("Paused") : liveStatus === "connecting" ? t("Connecting…") : liveStatus === "reconnecting" ? t("Reconnecting…") : liveStatus === "live" ? t("Listening") : t("Stopped");

  useEffect(() => {
    const query = window.matchMedia("(max-width: 900px)");
    const sync = () => setInspectorOverlay(query.matches);
    sync();
    query.addEventListener("change", sync);
    return () => query.removeEventListener("change", sync);
  }, []);

  useEffect(() => {
    if (depth !== "group" || groupDetailLoading || !selectedGroupName || selectedGroup) return;
    openStreamDepth("groups");
  }, [depth, groupDetailLoading, selectedGroup, selectedGroupName]);

  const moveSelection = (step: number) => {
    if (!displayedEntries.length || selectedIndex < 0) return;
    const next = Math.min(displayedEntries.length - 1, Math.max(0, selectedIndex + step));
    entryInspectSequence.current += 1;
    setInspectedEntry(displayedEntries[next]);
    setInspectedEntrySourceMissing(false);
    setSelectedId(displayedEntries[next].id);
  };

  const closeEntryInspector = useCallback(() => {
    entryInspectSequence.current += 1;
    setSelectedId("");
    setInspectedEntry(null);
    setInspectedEntrySourceMissing(false);
  }, []);

  const inspectEntry = async (entryId: string, origin: MessageOrigin, preferredTab: "payload" | "delivery") => {
    const requestID = ++entryInspectSequence.current;
    const loaded = entries.find((entry) => entry.id === entryId);
    if (loaded) {
      setInspectedEntry(loaded);
      setInspectedEntrySourceMissing(false);
      setEntryInspectorPreferredTab(preferredTab);
      setMessageOrigin(origin);
      setSelectedId(entryId);
      return;
    }
    setError("");
    try {
      const response = await api.entries(connectionId, key, 1, entryId);
      if (requestID !== entryInspectSequence.current) return;
      const entry = response.items.find((item) => item.id === entryId);
      setInspectedEntry(entry ?? { id: entryId, timestamp: streamIDTimestamp(entryId), fields: {} });
      setInspectedEntrySourceMissing(!entry);
      setEntryInspectorPreferredTab(preferredTab);
      setMessageOrigin(origin);
      setSelectedId(entryId);
    } catch (cause) {
      if (requestID === entryInspectSequence.current) onToast({ kind: "error", title: t("Unable to open message"), message: cause instanceof Error ? t(cause.message) : t("Unable to load the message.") });
    }
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
    if (!connectionId || !key || entryPageLoading) return false;
    const requestID = ++entryPageLoadSequence.current;
    setEntryPageLoading(true);
    setError("");
    setLive(false);
    setPaused(false);
    try {
      const response = await api.entries(connectionId, key, messagePageSize, cursor);
      if (requestID !== entryPageLoadSequence.current) return false;
      setEntries(response.items);
      setEntryCursor(response.nextCursor);
      setEntryPageCursor(cursor);
      setEntryCursorHistory(history);
      setHasMoreEntries(response.hasMore);
      setSelectedId("");
      setInspectedEntry(null);
      setInspectedEntrySourceMissing(false);
      return true;
    } catch (cause) {
      if (requestID === entryPageLoadSequence.current) setError(cause instanceof Error ? t(cause.message) : t("Unable to load the message page."));
      return false;
    } finally {
      if (requestID === entryPageLoadSequence.current) setEntryPageLoading(false);
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

  const toggleLiveTail = async () => {
    if (live) {
      setLive(false);
      setPaused(false);
      return;
    }
    if (!isFirstEntryPage && !await loadEntryPage("+", [])) return;
    setPaused(false);
    setLive(true);
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
        openCatalog();
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
    const requestID = ++groupDetailLoadSequence.current;
    setDepth("group");
    setSelectedGroupName(groupName);
    setInspectedEntry(null);
    setInspectedEntrySourceMissing(false);
    setGroupConsumers([]);
    setGroupPending([]);
    setGroupDetailLoading(true);
    setError("");
    try {
      const [consumerResponse, pendingResponse] = await Promise.all([
        api.consumers(connectionId, key, groupName),
        api.pending(connectionId, key, groupName),
      ]);
      if (requestID !== groupDetailLoadSequence.current) return;
      setGroupConsumers(consumerResponse.items);
      setGroupPending(pendingResponse.items);
    } catch (cause) {
      if (requestID !== groupDetailLoadSequence.current) return;
      setGroupConsumers([]);
      setGroupPending([]);
      setError(cause instanceof Error ? t(cause.message) : t("Unable to load consumer details."));
    } finally {
      if (requestID === groupDetailLoadSequence.current) setGroupDetailLoading(false);
    }
  };

  useEffect(() => {
    if (focusSection !== "groups" || loading || !key) return;
    if (focusGroup && groups.some((group) => group.name === focusGroup)) {
      void openGroup(focusGroup);
      return;
    }
    setMessagesSectionOpen(false);
    openStreamDepth("groups");
  }, [focusGroup, focusSection, groups, key, loading]);

  const activeConnection = connections.find((connection) => connection.id === connectionId);

  return (
    <div className={`stream-layout stream-depth-page ${showEntryInspector ? "" : "stream-layout--wide"}`}>
      <section className="stream-main" ref={streamMainRef} inert={showEntryInspector && inspectorOverlay ? true : undefined}>
        {depth === "catalog" ? <>
          <div className="page-header stream-depth-header">
            <div>
              <StreamDepthBreadcrumbs current={t("Streams")} />
              <h1 data-depth-heading tabIndex={-1}>{t("Redis Streams")}</h1>
              <p><i className={activeConnection?.healthy ? "health-dot" : "health-dot health-dot--down"} />{activeConnection?.name ?? "Redis"}</p>
            </div>
            <div className="header-actions"><button onClick={() => void load()} disabled={loading}><RefreshCw size={15} />{loading ? t("Loading…") : t("Refresh")}</button></div>
          </div>
          {error ? <div className="page-error">{error}</div> : null}
          <section className="stream-catalog">
            <header>
              <button type="button" className="stream-section-toggle" aria-expanded={streamsSectionOpen} onClick={() => setStreamsSectionOpen((current) => !current)}>
                {streamsSectionOpen ? <ChevronDown size={17} /> : <ChevronRight size={17} />}
                <h2>{t("Streams")}</h2>
              </button>
              {streamsSectionOpen ? <div className="stream-catalog-actions">
                <label><Search size={15} /><input value={streamQuery} onChange={(event) => setStreamQuery(event.target.value)} placeholder={t("Filter stream keys…")} /></label>
                {canWrite ? <button type="button" className="stream-monitor-button" disabled={!connectionId} onClick={() => setShowMonitor(true)}><Plus size={15} />{t("Add stream keys for monitoring")}</button> : null}
              </div> : <strong className="stream-section-count">{streams.length.toLocaleString(locale)}</strong>}
            </header>
            {streamsSectionOpen ? <ResizableGrid className="stream-catalog-table" storageKey="streams-catalog-metrics-v6" columns={streamColumns} headerClassName="stream-catalog-head" fixedLayout>
              {filteredStreams.map((stream) => {
                const metrics = overviewByKey.get(stream.key);
                const memory = metrics?.memoryBytes ?? stream.memoryBytes;
                return <div key={stream.key} className="stream-catalog-row">
                  <button
                    type="button"
                    className="stream-catalog-open"
                    aria-label={`${t("Stream")}: ${stream.key}; ${t("Entries")}: ${stream.length.toLocaleString(locale)}; ${t("Memory")}: ${formatBytes(memory, locale)}; ${t("Consumer groups")}: ${(metrics?.consumerGroups ?? 0).toLocaleString(locale)}; ${t("Total lag")}: ${metrics?.lagKnown ? metrics.totalLag.toLocaleString(locale) : "—"}; ${t("Pending")}: ${(metrics?.pending ?? 0).toLocaleString(locale)}; ${t("Last consumed")}: ${metrics?.lastConsumed || "—"}`}
                    onClick={() => void changeStream(stream.key)}
                  />
                  <span className="stream-key-cell mono" data-label={t("Stream key")}><span>{stream.key}</span>{!stream.available ? <em>{t("Waiting")}</em> : null}</span>
                  <span data-label={t("Entries")}>{(metrics?.length ?? stream.length).toLocaleString(locale)}</span>
                  <span data-label={t("Memory")} title={t("Approximate RAM used by this stream key.")}>{formatBytes(memory, locale)}</span>
                  <span data-label={t("Consumer groups")}>{(metrics?.consumerGroups ?? 0).toLocaleString(locale)}</span>
                  <span data-label={t("Total lag")}>{metrics ? (metrics.lagKnown ? metrics.totalLag.toLocaleString(locale) : "—") : "—"}</span>
                  <span data-label={t("Pending")}>{(metrics?.pending ?? 0).toLocaleString(locale)}</span>
                  <span className="mono stream-catalog-last-consumed" data-label={t("Last consumed")} title={metrics?.lastConsumed || "—"}>{metrics?.lastConsumed || "—"}</span>
                  <div className="stream-row-actions">
                    <ChevronRight size={16} />
                    {canWrite && stream.monitored ? <button type="button" className="stream-unmonitor-button" disabled={streamMutation === stream.key} aria-label={t("Remove from monitoring")} title={t("Remove from monitoring")} onClick={() => void unmonitorStream(stream.key)}><Trash2 size={15} /></button> : null}
                  </div>
                </div>;
              })}
              {!filteredStreams.length && !loading ? <div className="panel-empty">{streams.length ? t("No streams match this filter.") : t("No streams match the current pattern.")}</div> : null}
              {hasMoreStreams && !streamQuery ? <div className="table-load-more"><button type="button" onClick={() => void loadMoreStreams()} disabled={loadingMoreStreams}>{loadingMoreStreams ? t("Loading more…") : t("Load more")}</button></div> : null}
            </ResizableGrid> : null}
          </section>
        </> : null}

        {depth === "stream" && key ? <>
          <div className="page-header stream-depth-header">
            <div>
              <StreamDepthBreadcrumbs items={[{ label: t("Streams"), onClick: openCatalog }]} current={key} />
              <h1 className="mono" data-depth-heading tabIndex={-1}>{key}</h1>
              <p><i className={activeConnection?.healthy ? "health-dot" : "health-dot health-dot--down"} />{activeConnection?.name ?? "Redis"}</p>
            </div>
            <div className="header-actions">
              <button className="stream-depth-back" type="button" onClick={openCatalog}><ChevronLeft size={15} />{t("Back")}</button>
              <button onClick={() => void load()} disabled={loading}><RefreshCw size={15} />{loading ? t("Loading…") : t("Refresh")}</button>
            </div>
          </div>
          {error ? <div className="page-error">{error}</div> : null}
          <div className="stream-depth-content">
            <div className="stream-detail">
              <div className="stream-detail-tree">
              <div className="kpi-strip stream-kpis-real">
                <div><span>{t("Entries")}</span><strong>{selectedStream?.length.toLocaleString(locale) ?? "0"}</strong></div>
                <div title={t("Approximate RAM used by this stream key.")}><span>{t("Memory")}</span><strong>{formatBytes(selectedStream?.memoryBytes, locale)}</strong></div>
                <div><span>{t("Consumer groups")}</span><strong>{groups.length}</strong></div>
                <div><span>{t("Last entry")}</span><strong className="mono small-value">{entries[0]?.id ?? "—"}</strong></div>
              </div>

              <section className="stream-data-section" ref={messagesSectionRef}>
                <div className="surface-heading stream-section-heading">
                  <button type="button" className="stream-section-toggle" aria-expanded={messagesSectionOpen} onClick={() => setMessagesSectionOpen((current) => !current)}>
                    {messagesSectionOpen ? <ChevronDown size={17} /> : <ChevronRight size={17} />}<span><h2>{t("Messages")}</h2></span>
                  </button>
                  {canWrite ? <button type="button" className="stream-section-action" onClick={() => setShowAdd(true)}><Plus size={15} />{t("Add message")}</button> : null}
                </div>
                {messagesSectionOpen ? <>
                  <div className="table-toolbar">
                    <label className="toolbar-search"><Search size={14} /><input value={search} onChange={(event) => setSearch(event.target.value)} placeholder={t("Search ID, field or value…")} /></label>
                    <div className="mobile-message-sort">
                      <ArrowDownUp size={14} />
                      <Select
                        ariaLabel={t("Sorted by {key} · {direction}", { key: t(messageSort.key === "id" ? "ID" : messageSort.key === "timestamp" ? "Timestamp" : messageSort.key === "size" ? "Size" : "Fields"), direction: t(messageSort.direction === "asc" ? "ascending" : "descending") })}
                        value={`${messageSort.key}:${messageSort.direction}`}
                        options={messageSortOptions}
                        onChange={(next) => {
                          const [sortKey, direction] = next.split(":") as [MessageSortKey, "asc" | "desc"];
                          setMessageSort({ key: sortKey, direction });
                        }}
                        className="select-control--block"
                        size="compact"
                      />
                    </div>
                    <button className={`live-button ${live ? "active" : ""}`} aria-pressed={live} disabled={entryPageLoading} onClick={() => void toggleLiveTail()}><span /><span>{t("Live tail")}</span></button>
                    <button className={paused ? "pause-button active" : "pause-button"} aria-label={paused ? t("Resume live tail") : t("Pause live tail")} onClick={() => setPaused((value) => !value)} disabled={!live}>{paused ? <Play size={14} /> : <Pause size={14} />}</button>
                    {live ? <span className={`live-state live-state--${liveStatus}`}><Radio size={12} />{liveLabel}</span> : null}
                  </div>
                  <div className="message-table-scroll">
                    <ResizableGrid className="message-table" storageKey="stream-messages-v4" columns={messageColumns} headerClassName="message-table-head" renderHeader={(column) => {
                      if (column.id === "payload") return column.label;
                      const sortKey = column.id as MessageSortKey;
                      const active = messageSort.key === sortKey;
                      const SortIcon = active ? (messageSort.direction === "asc" ? ArrowUp : ArrowDown) : ArrowDownUp;
                      return <button className={active ? "message-sort active" : "message-sort"} aria-label={active ? t("Sorted by {key} · {direction}", { key: String(column.label), direction: t(messageSort.direction === "asc" ? "ascending" : "descending") }) : t("Sort by {key}", { key: String(column.label) })} onClick={() => toggleMessageSort(sortKey)}>{column.label} <SortIcon size={13} /></button>;
                    }}>
                      <div className="message-table-body">
                        {displayedEntries.map((entry) => <button className={showEntryInspector?.id === entry.id ? "message-row selected" : "message-row"} aria-pressed={showEntryInspector?.id === entry.id} key={entry.id} onClick={() => void inspectEntry(entry.id, { kind: "messages" }, "payload")}>
                          <span className="mono id-cell" data-label={t("ID")}>{entry.id}</span>
                          <span className="mono timestamp-cell" data-label={t("Timestamp")}>{formatTimestamp(entry.timestamp, locale)}</span>
                          <span className="mono fields-cell" data-label={t("Payload preview")}>{Object.entries(entry.fields).slice(0, 4).map(([field, value]) => `${field}=${String(value)}`).join("   ")}</span>
                          <span className="mono size-cell" data-label={t("Size")}>{entrySize(entry)} B</span>
                          <span className="delivery-cell" data-label={t("Fields")}>{Object.keys(entry.fields).length}</span>
                        </button>)}
                        {!displayedEntries.length && !loading ? <div className="empty-table">{t("No entries to display.")}</div> : null}
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
                    {groupsSectionOpen ? <ChevronDown size={17} /> : <ChevronRight size={17} />}<span><h2>{t("Consumer groups")}</h2></span>
                  </button>
                  <strong className="stream-section-count">{groupError ? "—" : groups.length}</strong>
                </div>
                {groupsSectionOpen ? loading ? <div className="panel-empty">{t("Loading…")}</div> : groupError ? <div className="metric-history-error" role="alert">{groupError}</div> : <ConsumerGroupMetricsPanel
                  {...streamMetricsModeProps}
                  key={`${connectionId}:${key}`}
                  connectionId={connectionId}
                  streamKey={key}
                  monitored={selectedStream?.monitored ?? false}
                  groups={groups}
                  selectedGroupName=""
                  onOpenGroup={(groupName) => { void openGroup(groupName); }}
                /> : null}
              </section>

              </div>
            </div>
          </div>
        </> : null}

        {depth === "group" && key && selectedGroup ? <>
          <div className="page-header stream-depth-header">
            <div>
              <StreamDepthBreadcrumbs items={[
                { label: t("Streams"), onClick: openCatalog },
                { label: key, onClick: () => openStreamDepth() },
                { label: t("Consumer groups"), onClick: () => openStreamDepth("groups") },
              ]} current={selectedGroup.name} />
              <h1 className="mono" data-depth-heading tabIndex={-1}>{selectedGroup.name}</h1>
            </div>
            <div className="header-actions">
              <button className="stream-depth-back" type="button" onClick={() => openStreamDepth("groups")}><ChevronLeft size={15} />{t("Back")}</button>
              <button type="button" onClick={() => void openGroup(selectedGroup.name)} disabled={groupDetailLoading}><RefreshCw size={15} />{groupDetailLoading ? t("Loading…") : t("Refresh")}</button>
            </div>
          </div>
          {error ? <div className="page-error">{error}</div> : null}
          <div className="stream-depth-content">
            <div className="consumer-group-depth">
              <ConsumerGroupMetricsPanel
                {...groupMetricsModeProps}
                key={`${connectionId}:${key}:${selectedGroup.name}`}
                connectionId={connectionId}
                streamKey={key}
                monitored={selectedStream?.monitored ?? false}
                groups={groups}
                selectedGroupName={selectedGroup.name}
              />
              <ConsumerGroupDepthContent
                group={selectedGroup}
                stream={key}
                consumers={groupConsumers}
                pending={groupPending}
                loading={groupDetailLoading}
                onOpenEntry={(entryId) => void inspectEntry(entryId, { kind: "group", groupName: selectedGroup.name }, "delivery")}
              />
            </div>
          </div>
        </> : null}
      </section>

      {showEntryInspector && key ? <EntryInspector
        connectionId={connectionId}
        entry={showEntryInspector}
        stream={key}
        preferredTab={entryInspectorPreferredTab}
        initialGroupName={messageOrigin.kind === "group" ? messageOrigin.groupName : ""}
        sourceMissing={inspectedEntrySourceMissing}
        canManageGroups={canManageGroups}
        canWriteStreams={canWrite}
        canMovePrevious={selectedIndex > 0}
        canMoveNext={selectedIndex >= 0 && selectedIndex < displayedEntries.length - 1}
        modal={inspectorOverlay}
        onChanged={refreshDeliveryContext}
        onToast={onToast}
        onMove={moveSelection}
        onClose={closeEntryInspector}
      /> : null}

      {canWrite && showAdd ? <AddMessageModal connectionId={connectionId} stream={key} onClose={() => setShowAdd(false)} onAdded={async (id) => {
        setShowAdd(false);
        await loadEntriesAndGroups(connectionId, key);
        onToast({ kind: "success", title: t("Message added"), message: t("Added entry {id}.", { id }) });
      }} /> : null}
      {canWrite && showMonitor ? <MonitorStreamModal connectionId={connectionId} onClose={() => setShowMonitor(false)} onSubmit={monitorStreams} /> : null}
    </div>
  );
}

function ConsumerPendingTable({
  pending,
  storageKey,
  firstColumnLabel,
  onOpenEntry,
}: {
  pending: PendingEntry[];
  storageKey: string;
  firstColumnLabel: string;
  onOpenEntry: (entryId: string) => void;
}) {
  const { t } = useI18n();
  const columns = useMemo<ResizableGridColumn[]>(() => [
    { id: "message", label: firstColumnLabel, defaultWidth: 210, minWidth: 130, grow: true },
    { id: "deliveries", label: t("Deliveries"), defaultWidth: 82, minWidth: 68 },
    { id: "idle", label: t("Idle"), defaultWidth: 82, minWidth: 68 },
    { id: "open", label: null, ariaLabel: t("Actions"), defaultWidth: 44, minWidth: 44 },
  ], [firstColumnLabel, t]);
  return (
    <ResizableGrid className="consumer-pending-table" storageKey={storageKey} columns={columns} headerClassName="consumer-pending-head">
      <div className="consumer-pending-list">
        {pending.map((entry) => <button type="button" className="consumer-pending-row" key={entry.id} onClick={() => onOpenEntry(entry.id)}><strong className="mono">{entry.id}</strong><span data-label={t("Deliveries")}>{entry.retryCount}</span><span data-label={t("Idle")}>{formatDuration(entry.idleMs)}</span><ChevronRight size={15} /></button>)}
        {!pending.length ? <div className="panel-empty">{t("No messages are assigned in the PEL.")}</div> : null}
      </div>
    </ResizableGrid>
  );
}

function ConsumerGroupDepthContent({
  group,
  stream,
  consumers,
  pending,
  loading,
  onOpenEntry,
}: {
  group: ConsumerGroup;
  stream: string;
  consumers: ConsumerInfo[];
  pending: PendingEntry[];
  loading: boolean;
  onOpenEntry: (entryId: string) => void;
}) {
  const { t } = useI18n();
  const [selectedConsumer, setSelectedConsumer] = useState("");

  useEffect(() => {
    setSelectedConsumer((current) => consumers.some((consumer) => consumer.name === current) ? current : consumers[0]?.name ?? "");
  }, [consumers]);

  const consumer = consumers.find((item) => item.name === selectedConsumer) ?? null;
  const consumerPending = pending.filter((entry) => entry.consumer === selectedConsumer);

  return <div className="consumer-group-depth-content">
    <div className="group-inspector-summary">
      <div><span>Stream</span><strong className="mono">{stream}</strong></div>
      <div><span>{t("Consumers")}</span><strong>{group.consumers}</strong></div>
      <div><span>{t("Pending")}</span><strong>{group.pending}</strong></div>
      <div><span>{t("Lag")}</span><strong>{group.lag}</strong></div>
      <div><span>{t("Last delivered")}</span><strong className="mono stream-id-cell" title={group.lastDeliveredId}>{group.lastDeliveredId}</strong></div>
    </div>

    <section className="consumer-group-depth-section consumer-group-pending-section">
      <header><h2>{t("Pending messages")}</h2><span>{pending.length}</span></header>
      {loading ? <div className="panel-empty">{t("Loading pending messages…")}</div> : <ConsumerPendingTable pending={pending} storageKey="stream-group-depth-pending" firstColumnLabel={t("Pending message")} onOpenEntry={onOpenEntry} />}
    </section>

    <section className="consumer-section consumer-group-depth-section">
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
      <ConsumerPendingTable pending={consumerPending} storageKey="stream-group-consumer-pending" firstColumnLabel={t("Assigned message")} onOpenEntry={onOpenEntry} />
    </section> : null}
  </div>;
}

function EntryInspector({ connectionId, entry, stream, preferredTab, initialGroupName, sourceMissing, canManageGroups, canWriteStreams, canMovePrevious, canMoveNext, modal, onChanged, onToast, onMove, onClose }: {
  connectionId: string;
  entry: RedisEntry;
  stream: string;
  preferredTab: "payload" | "delivery";
  initialGroupName: string;
  sourceMissing: boolean;
  canManageGroups: boolean;
  canWriteStreams: boolean;
  canMovePrevious: boolean;
  canMoveNext: boolean;
  modal: boolean;
  onChanged: () => Promise<void>;
  onToast: (toast: ToastState) => void;
  onMove: (step: number) => void;
  onClose: () => void;
}) {
  const { t } = useI18n();
  const panelRef = useRef<HTMLElement>(null);
  const closeButtonRef = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    const returnFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        onClose();
        return;
      }
      if (!modal || event.key !== "Tab" || !panelRef.current) return;
      const focusable = Array.from(panelRef.current.querySelectorAll<HTMLElement>(
        'button:not([disabled]), a[href], input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])',
      )).filter((element) => element.offsetParent !== null);
      if (!focusable.length) return;
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    };
    closeButtonRef.current?.focus({ preventScroll: true });
    document.addEventListener("keydown", closeOnEscape);
    return () => {
      document.removeEventListener("keydown", closeOnEscape);
      if (returnFocus?.isConnected) returnFocus.focus({ preventScroll: true });
    };
  }, [modal, onClose]);

  return <aside ref={panelRef} className="inspector entry-inspector" role={modal ? "dialog" : undefined} aria-modal={modal || undefined} aria-labelledby="entry-inspector-title">
    <InspectorResizeHandle />
    <header>
      <div><strong id="entry-inspector-title">{t("Entry details")}</strong><span className="mono" title={entry.id}>{entry.id}</span></div>
      <div className="inspector-actions">
        <button type="button" onClick={() => onMove(-1)} disabled={!canMovePrevious} aria-label={t("Previous entry")}><ChevronLeft size={16} /></button>
        <button type="button" onClick={() => onMove(1)} disabled={!canMoveNext} aria-label={t("Next entry")}><ChevronRight size={16} /></button>
        <button ref={closeButtonRef} type="button" onClick={onClose} aria-label={t("Close entry details")}><X size={17} /></button>
      </div>
    </header>
    <MessageDepthContent
      connectionId={connectionId}
      entry={entry}
      stream={stream}
      preferredTab={preferredTab}
      initialGroupName={initialGroupName}
      sourceMissing={sourceMissing}
      canManageGroups={canManageGroups}
      canWriteStreams={canWriteStreams}
      onChanged={onChanged}
      onToast={onToast}
      embedded
    />
  </aside>;
}

function MessageDepthContent({ connectionId, entry, stream, preferredTab, initialGroupName, sourceMissing, canManageGroups, canWriteStreams, onChanged, onToast, embedded = false }: {
  connectionId: string;
  entry: RedisEntry;
  stream: string;
  preferredTab: "payload" | "delivery";
  initialGroupName: string;
  sourceMissing: boolean;
  canManageGroups: boolean;
  canWriteStreams: boolean;
  onChanged: () => Promise<void>;
  onToast: (toast: ToastState) => void;
  embedded?: boolean;
}) {
  const { locale, t } = useI18n();
  const [inspectorTab, setInspectorTab] = useState<"payload" | "delivery">(sourceMissing ? "delivery" : "payload");
  const [wrapLines, setWrapLines] = useState(true);
  const payload = useMemo(() => presentEntryPayload(entry.fields), [entry.fields]);
  useEffect(() => { setInspectorTab(sourceMissing ? "delivery" : preferredTab); }, [entry.id, preferredTab, sourceMissing]);
  const moveTab = (direction: -1 | 1) => {
    const tabs: Array<"payload" | "delivery"> = sourceMissing ? ["delivery"] : ["payload", "delivery"];
    const currentIndex = Math.max(0, tabs.indexOf(inspectorTab));
    const nextTab = tabs[(currentIndex + direction + tabs.length) % tabs.length];
    setInspectorTab(nextTab);
    window.requestAnimationFrame(() => document.getElementById(`message-depth-${nextTab}-tab`)?.focus());
  };
  return <div className={embedded ? undefined : "message-depth-surface"}>
    {sourceMissing ? <div className="message-depth-source-status"><em className="source-entry-missing">{t("Source entry unavailable")}</em></div> : null}
    <div className="inspector-tabs message-depth-tabs" role="tablist" aria-label={t("Entry details")} onKeyDown={(event) => {
      if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") return;
      event.preventDefault();
      moveTab(event.key === "ArrowLeft" ? -1 : 1);
    }}>{!sourceMissing ? <button id="message-depth-payload-tab" type="button" role="tab" aria-controls="message-depth-payload-panel" aria-selected={inspectorTab === "payload"} tabIndex={inspectorTab === "payload" ? 0 : -1} className={inspectorTab === "payload" ? "active" : ""} onClick={() => setInspectorTab("payload")}>{t("Payload")}</button> : null}<button id="message-depth-delivery-tab" type="button" role="tab" aria-controls="message-depth-delivery-panel" aria-selected={inspectorTab === "delivery"} tabIndex={inspectorTab === "delivery" ? 0 : -1} className={inspectorTab === "delivery" ? "active" : ""} onClick={() => setInspectorTab("delivery")}>{t("Delivery")}</button></div>
    {inspectorTab === "payload" ? <div id="message-depth-payload-panel" role="tabpanel" aria-labelledby="message-depth-payload-tab">
      <div className="code-toolbar"><span>{t(payload.format === "json" ? "JSON payload" : payload.format === "text" ? "Text payload" : "Redis fields")}</span><button className={wrapLines ? "active" : ""} aria-pressed={wrapLines} aria-label={wrapLines ? t("Disable line wrapping") : t("Wrap lines")} title={wrapLines ? t("Disable line wrapping") : t("Wrap lines")} onClick={() => setWrapLines((current) => !current)}><WrapText size={15} /></button></div>
      <pre className={`payload-code-view ${wrapLines ? "wrap" : ""}`}>{payload.content}</pre>
      <div className="inspector-section"><h4>{t("Metadata")}</h4><div className="detail-list"><div><span>Stream</span><strong className="mono">{stream}</strong></div><div><span>{t("Timestamp")}</span><strong className="mono">{formatTimestamp(entry.timestamp, locale)}</strong></div><div><span>{t("Size")}</span><strong>{new Blob([JSON.stringify(entry.fields)]).size} B</strong></div><div><span>{t("Fields")}</span><strong>{Object.keys(entry.fields).length}</strong></div></div></div>
    </div> : <div id="message-depth-delivery-panel" role="tabpanel" aria-labelledby="message-depth-delivery-tab"><MessageDeliveryPanel connectionId={connectionId} streamKey={stream} entryId={entry.id} initialGroupName={initialGroupName} sourceAvailable={!sourceMissing} canManageGroups={canManageGroups} canWriteStreams={canWriteStreams} onChanged={onChanged} onToast={onToast} /></div>}
  </div>;
}

type StreamDepthBreadcrumbItem = { label: string; onClick: () => void };

function StreamDepthBreadcrumbs({ items = [], current }: { items?: StreamDepthBreadcrumbItem[]; current: string }) {
  const { t } = useI18n();
  return <nav className="stream-depth-breadcrumbs breadcrumbs" aria-label={t("Breadcrumb")}>
    {items.map((item, index) => <span key={`${item.label}:${index}`}><button type="button" onClick={item.onClick} title={item.label}>{item.label}</button><i aria-hidden="true">/</i></span>)}
    <strong aria-current="page" title={current}>{current}</strong>
  </nav>;
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

const BYTE_UNITS = ["B", "KB", "MB", "GB", "TB", "PB"] as const;

function formatBytes(value: number | null | undefined, locale: string) {
  if (value === null || value === undefined || !Number.isFinite(value) || value < 0) return "—";
  if (value === 0) return "0 B";
  const unitIndex = Math.min(Math.floor(Math.log(value) / Math.log(1024)), BYTE_UNITS.length - 1);
  const amount = value / (1024 ** unitIndex);
  const maximumFractionDigits = unitIndex === 0 || amount >= 100 ? 0 : amount >= 10 ? 1 : 2;
  return `${new Intl.NumberFormat(locale, { maximumFractionDigits }).format(amount)} ${BYTE_UNITS[unitIndex]}`;
}

function streamIDTimestamp(id: string) {
  const milliseconds = Number(id.split("-")[0]);
  return Number.isFinite(milliseconds) ? new Date(milliseconds).toISOString() : id;
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
