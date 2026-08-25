import type { AlertEscalationPolicy, AlertEscalationPolicyInput, AlertIncident, AlertMetricDefinition, AlertRule, AlertRuleInput, AlertSummary, AlertSuppression, AlertSuppressionInput, AlertWebhookDelivery, AlertWebhookRoute, AlertWebhookRouteInput, ApiSession, CapacityForecast, ConsumerGroup, ConsumerGroupMetricSeries, ConsumerGroupMetricSnapshot, ConsumerHistoryResponse, ConsumerInfo, DashboardDefinition, LifecycleMetrics, LifecycleRequest, MonitoringEvent, OperationalSnapshot, OverviewStreamItem, PendingEntry, QuarantineInput, QuarantinePlan, QuarantineRecord, RecoveryPlan, RecoveryPlanInput, RedisConnection, RedisConnectionConfig, RedisEntry, RetentionPolicy, SavedDashboard, StreamItem, StreamMetricSeries, StreamSchemaAnalysis, TelemetryToken, TopologyModel, TraceSpan, TraceSummary } from "./types";

const request = async <T>(path: string, init?: RequestInit): Promise<T> => {
  const response = await fetch(path, {
    ...init,
    credentials: "same-origin",
    headers: {
      "Content-Type": "application/json",
      ...init?.headers,
    },
  });
  const payload = (await response.json().catch(() => null)) as T | { error?: string } | null;
  if (!response.ok) {
    const message = payload && typeof payload === "object" && "error" in payload ? payload.error : undefined;
    throw new Error(message ?? `Request failed (${response.status})`);
  }
  return payload as T;
};

export const api = {
  setupStatus: () => request<{ setupRequired: boolean; configPath: string; connections: RedisConnectionConfig[] }>("/api/setup/status"),
  setup: (input: { admin: { username: string; displayName: string; password: string }; connections: RedisConnectionConfig[] }) =>
    request<ApiSession>("/api/setup", { method: "POST", body: JSON.stringify(input) }),
  setupTestRedis: (connection: RedisConnectionConfig) =>
    request<{ ok: boolean; latencyMs: number }>("/api/setup/test-redis", { method: "POST", body: JSON.stringify(connection) }),
  session: () => request<ApiSession>("/api/session"),
  login: (username: string, password: string) =>
    request<ApiSession>("/api/login", {
      method: "POST",
      body: JSON.stringify({ username, password }),
    }),
  logout: () => request<{ ok: boolean }>("/api/logout", { method: "POST", body: "{}" }),
  changePassword: (currentPassword: string, newPassword: string) =>
    request<ApiSession>("/api/me/password", {
      method: "POST",
      body: JSON.stringify({ currentPassword, newPassword }),
    }),
  changeUsername: (currentPassword: string, username: string) =>
    request<{ ok: boolean; username: string }>("/api/me/username", {
      method: "POST",
      body: JSON.stringify({ currentPassword, username }),
    }),
  settings: () => request<{ configPath: string; connections: RedisConnectionConfig[] }>("/api/settings"),
  updateSettings: (connections: RedisConnectionConfig[]) =>
    request<{ ok: boolean; connections: number }>("/api/settings", { method: "PUT", body: JSON.stringify({ connections }) }),
  testRedis: (connection: RedisConnectionConfig) =>
    request<{ ok: boolean; latencyMs: number }>("/api/settings/test-redis", { method: "POST", body: JSON.stringify(connection) }),
  connections: () => request<{ items: RedisConnection[] }>("/api/connections"),
  overview: (connectionId: string) =>
    request<{ connectionId: string; healthy: boolean; items: OverviewStreamItem[]; generatedAt: string }>(
      `/api/overview?connectionId=${encodeURIComponent(connectionId)}`,
    ),
  metrics: (connectionId: string, range: StreamMetricSeries["range"], streamKey = "") =>
    request<StreamMetricSeries>(
      `/api/metrics/timeseries?connectionId=${encodeURIComponent(connectionId)}&range=${encodeURIComponent(range)}${streamKey ? `&streamKey=${encodeURIComponent(streamKey)}` : ""}`,
    ),
  consumerGroupMetrics: (connectionId: string, streamKey: string, range: StreamMetricSeries["range"]) =>
    request<ConsumerGroupMetricSeries>(
      `/api/metrics/consumer-groups?connectionId=${encodeURIComponent(connectionId)}&streamKey=${encodeURIComponent(streamKey)}&range=${encodeURIComponent(range)}`,
    ),
  latestConsumerGroupMetrics: (connectionId: string) =>
    request<{ connectionId: string; generatedAt: string; items: ConsumerGroupMetricSnapshot[] }>(
      `/api/metrics/consumer-groups/latest?connectionId=${encodeURIComponent(connectionId)}`,
    ),
  operationalSnapshot: (connectionId: string) =>
    request<OperationalSnapshot>(`/api/operations/snapshot?connectionId=${encodeURIComponent(connectionId)}`),
  consumerHistory: (input: { connectionId: string; streamKey?: string; groupName?: string; from: string; to: string; cursor?: number; limit?: number }) => {
    const query = new URLSearchParams({ connectionId: input.connectionId, from: input.from, to: input.to, limit: String(input.limit ?? 100) });
    if (input.streamKey) query.set("streamKey", input.streamKey);
    if (input.groupName) query.set("group", input.groupName);
    if (input.cursor) query.set("cursor", String(input.cursor));
    return request<ConsumerHistoryResponse>(`/api/operations/consumer-history?${query.toString()}`);
  },
  topology: (connectionId: string) => request<TopologyModel>(`/api/operations/topology?connectionId=${encodeURIComponent(connectionId)}`),
  topologyEvents: (connectionId: string, from: string, to: string) => {
    const query = new URLSearchParams({ connectionId, from, to, limit: "100" });
    return request<{ connectionId: string; from: string; to: string; items: MonitoringEvent[]; nextCursor?: number }>(`/api/operations/topology/events?${query.toString()}`);
  },
  capacity: (connectionId: string, window = "1h") => request<CapacityForecast>(`/api/operations/capacity?connectionId=${encodeURIComponent(connectionId)}&window=${encodeURIComponent(window)}`),
  retentionPolicies: (connectionId: string) => request<{ items: RetentionPolicy[] }>(`/api/operations/retention-policies?connectionId=${encodeURIComponent(connectionId)}`),
  saveRetentionPolicy: (input: { connectionId: string; streamKey: string; maxLength: number | null; maxAgeSeconds: number | null; requireTrimming: boolean }) =>
    request<RetentionPolicy>(`/api/operations/retention-policies?connectionId=${encodeURIComponent(input.connectionId)}&streamKey=${encodeURIComponent(input.streamKey)}`, { method: "PUT", body: JSON.stringify({ maxLength: input.maxLength, maxAgeSeconds: input.maxAgeSeconds, requireTrimming: input.requireTrimming }) }),
  deleteRetentionPolicy: (connectionId: string, streamKey: string) => request<{ deleted: boolean }>(`/api/operations/retention-policies?connectionId=${encodeURIComponent(connectionId)}&streamKey=${encodeURIComponent(streamKey)}`, { method: "DELETE", body: "{}" }),
  lifecycleMetrics: (input: { connectionId: string; streamKey?: string; groupName?: string; from: string; to: string; bucket: string }) => {
    const query = new URLSearchParams({
      connectionId: input.connectionId,
      from: input.from,
      to: input.to,
      bucket: input.bucket,
    });
    if (input.streamKey) query.set("streamKey", input.streamKey);
    if (input.groupName) query.set("groupName", input.groupName);
    return request<LifecycleMetrics>(`/api/telemetry/lifecycle/metrics?${query.toString()}`);
  },
  lifecycleRequests: (input: { connectionId: string; streamKey?: string; groupName?: string; search?: string; cursor?: string; limit?: number }) => {
    const query = new URLSearchParams({ connectionId: input.connectionId, limit: String(input.limit ?? 50) });
    if (input.streamKey) query.set("streamKey", input.streamKey);
    if (input.groupName) query.set("groupName", input.groupName);
    if (input.search?.trim()) query.set("search", input.search.trim());
    if (input.cursor) query.set("cursor", input.cursor);
    return request<{ items: LifecycleRequest[]; nextCursor: string | null; hasMore: boolean }>(`/api/telemetry/lifecycle/requests?${query.toString()}`);
  },
  schemaAnalysis: (connectionId: string, streamKey: string, sample = 200) =>
    request<StreamSchemaAnalysis>(`/api/analysis/schema?connectionId=${encodeURIComponent(connectionId)}&streamKey=${encodeURIComponent(streamKey)}&sample=${sample}`),
  traces: (input: { connectionId: string; streamKey: string; search?: string; cursor?: string; limit?: number }) => {
    const query = new URLSearchParams({ connectionId: input.connectionId, streamKey: input.streamKey, limit: String(input.limit ?? 25) });
    if (input.search?.trim()) query.set("search", input.search.trim());
    if (input.cursor) query.set("cursor", input.cursor);
    return request<{ items: TraceSummary[]; nextCursor?: string; hasMore: boolean }>(`/api/telemetry/traces?${query.toString()}`);
  },
  trace: (traceId: string) => request<{ traceId: string; spans: TraceSpan[] }>(`/api/telemetry/traces/${encodeURIComponent(traceId)}`),
  dashboards: () => request<{ items: SavedDashboard[] }>("/api/dashboards"),
  createDashboard: (input: { name: string; shared: boolean; definition: DashboardDefinition }) =>
    request<SavedDashboard>("/api/dashboards", { method: "POST", body: JSON.stringify(input) }),
  updateDashboard: (id: string, input: { name: string; shared: boolean; definition: DashboardDefinition }) =>
    request<SavedDashboard>(`/api/dashboards/${encodeURIComponent(id)}`, { method: "PUT", body: JSON.stringify(input) }),
  deleteDashboard: (id: string) => request<{ ok: boolean }>(`/api/dashboards/${encodeURIComponent(id)}`, { method: "DELETE", body: "{}" }),
  recoveryPlan: (input: RecoveryPlanInput) => request<RecoveryPlan>("/api/recovery/plans", { method: "POST", body: JSON.stringify(input) }),
  executeRecovery: (input: RecoveryPlanInput & { confirmation: string; idempotencyKey: string }) =>
    request<{ ok: boolean; action: string; affected: number; messageIds?: string[]; nextStart?: string; targetGroupId?: string; idempotentReplay?: boolean }>("/api/recovery/executions", { method: "POST", body: JSON.stringify(input) }),
  quarantinePlan: (input: QuarantineInput) => request<QuarantinePlan>("/api/quarantine/plans", { method: "POST", body: JSON.stringify(input) }),
  executeQuarantine: (input: QuarantineInput & { confirmation: string; idempotencyKey: string }) =>
    request<{ ok: boolean; items: QuarantineRecord[]; acknowledged: number; idempotentReplay?: boolean }>("/api/quarantine/executions", { method: "POST", body: JSON.stringify(input) }),
  quarantineRecords: (connectionId: string, streamKey: string, status = "all", cursor?: number) => {
    const query = new URLSearchParams({ connectionId, key: streamKey, status, limit: "25" });
    if (cursor) query.set("cursor", String(cursor));
    return request<{ items: QuarantineRecord[]; nextCursor?: number; hasMore: boolean }>(`/api/quarantine/records?${query.toString()}`);
  },
  quarantineActionPlan: (input: { action: "replay" | "skip"; connectionId: string; recordIds: number[]; targetStream?: string }) =>
    request<{ action: string; eligibleCount: number; confirmation: string; records: QuarantineRecord[]; guarantees: string[] }>("/api/quarantine/actions/plans", { method: "POST", body: JSON.stringify(input) }),
  executeQuarantineAction: (input: { action: "replay" | "skip"; connectionId: string; recordIds: number[]; targetStream?: string; confirmation: string; idempotencyKey: string }) =>
    request<{ ok: boolean; action: string; items: QuarantineRecord[]; idempotentReplay?: boolean }>("/api/quarantine/actions", { method: "POST", body: JSON.stringify(input) }),
  telemetryTokens: () => request<{ items: TelemetryToken[] }>("/api/telemetry/tokens"),
  createTelemetryToken: (name: string) => request<TelemetryToken & { token: string }>("/api/telemetry/tokens", { method: "POST", body: JSON.stringify({ name }) }),
  updateTelemetryToken: (id: string, input: { name: string; enabled: boolean }) => request<TelemetryToken>(`/api/telemetry/tokens/${encodeURIComponent(id)}`, { method: "PATCH", body: JSON.stringify(input) }),
  deleteTelemetryToken: (id: string) => request<{ ok: boolean }>(`/api/telemetry/tokens/${encodeURIComponent(id)}`, { method: "DELETE", body: "{}" }),
  alertMetrics: () => request<{ items: AlertMetricDefinition[] }>("/api/alert-metrics"),
  alertRules: () => request<{ items: AlertRule[] }>("/api/alert-rules"),
  createAlertRule: (input: AlertRuleInput) => request<{ item: AlertRule }>("/api/alert-rules", { method: "POST", body: JSON.stringify(input) }),
  updateAlertRule: (id: string, input: Partial<AlertRuleInput>) => request<{ item: AlertRule }>(`/api/alert-rules/${encodeURIComponent(id)}`, { method: "PATCH", body: JSON.stringify(input) }),
  deleteAlertRule: (id: string) => request<void>(`/api/alert-rules/${encodeURIComponent(id)}`, { method: "DELETE" }),
  alertIncidents: (status = "", limit = 100) => request<{ items: AlertIncident[]; summary: AlertSummary }>(`/api/alert-incidents?limit=${limit}${status ? `&status=${encodeURIComponent(status)}` : ""}`),
  acknowledgeAlertIncident: (id: string) => request<{ item: AlertIncident }>(`/api/alert-incidents/${encodeURIComponent(id)}/ack`, { method: "POST", body: "{}" }),
  testAlertWebhook: (url: string) => request<{ status: string; statusCode: number }>("/api/alert-webhooks/test", { method: "POST", body: JSON.stringify({ url }) }),
  alertWebhookDeliveries: (incidentId = "", limit = 100) => request<{ items: AlertWebhookDelivery[] }>(`/api/alert-webhook-deliveries?limit=${limit}${incidentId ? `&incidentId=${encodeURIComponent(incidentId)}` : ""}`),
  alertSilences: () => request<{ items: AlertSuppression[] }>("/api/alert-silences"),
  createAlertSilence: (input: AlertSuppressionInput) => request<{ item: AlertSuppression }>("/api/alert-silences", { method: "POST", body: JSON.stringify(input) }),
  updateAlertSilence: (id: string, input: AlertSuppressionInput) => request<{ item: AlertSuppression }>(`/api/alert-silences/${encodeURIComponent(id)}`, { method: "PUT", body: JSON.stringify(input) }),
  deleteAlertSilence: (id: string) => request<void>(`/api/alert-silences/${encodeURIComponent(id)}`, { method: "DELETE", body: "{}" }),
  alertMaintenanceWindows: () => request<{ items: AlertSuppression[] }>("/api/alert-maintenance-windows"),
  createAlertMaintenanceWindow: (input: AlertSuppressionInput) => request<{ item: AlertSuppression }>("/api/alert-maintenance-windows", { method: "POST", body: JSON.stringify(input) }),
  updateAlertMaintenanceWindow: (id: string, input: AlertSuppressionInput) => request<{ item: AlertSuppression }>(`/api/alert-maintenance-windows/${encodeURIComponent(id)}`, { method: "PUT", body: JSON.stringify(input) }),
  deleteAlertMaintenanceWindow: (id: string) => request<void>(`/api/alert-maintenance-windows/${encodeURIComponent(id)}`, { method: "DELETE", body: "{}" }),
  alertWebhookRoutes: () => request<{ items: AlertWebhookRoute[] }>("/api/alert-webhook-routes"),
  createAlertWebhookRoute: (input: AlertWebhookRouteInput) => request<{ item: AlertWebhookRoute }>("/api/alert-webhook-routes", { method: "POST", body: JSON.stringify(input) }),
  updateAlertWebhookRoute: (id: string, input: AlertWebhookRouteInput) => request<{ item: AlertWebhookRoute }>(`/api/alert-webhook-routes/${encodeURIComponent(id)}`, { method: "PUT", body: JSON.stringify(input) }),
  deleteAlertWebhookRoute: (id: string) => request<void>(`/api/alert-webhook-routes/${encodeURIComponent(id)}`, { method: "DELETE", body: "{}" }),
  alertEscalationPolicies: () => request<{ items: AlertEscalationPolicy[] }>("/api/alert-escalation-policies"),
  createAlertEscalationPolicy: (input: AlertEscalationPolicyInput) => request<{ item: AlertEscalationPolicy }>("/api/alert-escalation-policies", { method: "POST", body: JSON.stringify(input) }),
  updateAlertEscalationPolicy: (id: string, input: AlertEscalationPolicyInput) => request<{ item: AlertEscalationPolicy }>(`/api/alert-escalation-policies/${encodeURIComponent(id)}`, { method: "PUT", body: JSON.stringify(input) }),
  deleteAlertEscalationPolicy: (id: string) => request<void>(`/api/alert-escalation-policies/${encodeURIComponent(id)}`, { method: "DELETE", body: "{}" }),
  streams: (connectionId: string, cursor = 0, pattern?: string) =>
    request<{ items: StreamItem[]; nextCursor: number; hasMore: boolean }>(
      `/api/streams?connectionId=${encodeURIComponent(connectionId)}&cursor=${cursor}&limit=500${pattern ? `&pattern=${encodeURIComponent(pattern)}` : ""}`,
    ),
  streamStatus: (connectionId: string, key: string) =>
    request<{ key: string; available: boolean; exists: boolean; redisType: string }>(
      `/api/monitored-streams/status?connectionId=${encodeURIComponent(connectionId)}&key=${encodeURIComponent(key)}`,
    ),
  monitorStream: (connectionId: string, key: string) =>
    request<{
      item: { connectionId: string; key: string; createdBy?: string; createdAt: string };
      created: boolean;
      available: boolean;
      redisType: string;
    }>(
      `/api/monitored-streams?connectionId=${encodeURIComponent(connectionId)}&key=${encodeURIComponent(key)}`,
      { method: "POST", body: "{}" },
    ),
  unmonitorStream: (connectionId: string, key: string) =>
    request<{ ok: boolean }>(
      `/api/monitored-streams?connectionId=${encodeURIComponent(connectionId)}&key=${encodeURIComponent(key)}`,
      { method: "DELETE", body: "{}" },
    ),
  entries: (connectionId: string, key: string, limit = 100, start = "+") =>
    request<{ items: RedisEntry[]; nextCursor: string; hasMore: boolean }>(
      `/api/entries?connectionId=${encodeURIComponent(connectionId)}&key=${encodeURIComponent(key)}&start=${encodeURIComponent(start)}&limit=${limit}`,
    ),
  groups: (connectionId: string, key: string) =>
    request<{ items: ConsumerGroup[] }>(
      `/api/groups?connectionId=${encodeURIComponent(connectionId)}&key=${encodeURIComponent(key)}`,
    ),
  consumers: (connectionId: string, key: string, group: string) =>
    request<{ items: ConsumerInfo[] }>(
      `/api/consumers?connectionId=${encodeURIComponent(connectionId)}&key=${encodeURIComponent(key)}&group=${encodeURIComponent(group)}`,
    ),
  pending: (connectionId: string, key: string, group: string) =>
    request<{ items: PendingEntry[] }>(
      `/api/pending?connectionId=${encodeURIComponent(connectionId)}&key=${encodeURIComponent(key)}&group=${encodeURIComponent(group)}&limit=500`,
    ),
  action: (action: string, input: Record<string, unknown>) =>
    request<{ ok: boolean; affected?: number; message?: string }>("/api/actions", {
      method: "POST",
      body: JSON.stringify({ action, ...input }),
    }),
  users: () => request<{ items: Array<{ id: string; username: string; displayName: string; role: string; enabled: boolean; lastLoginAt?: string }> }>("/api/users"),
  createUser: (input: { username: string; displayName: string; password: string; role: string }) =>
    request("/api/users", { method: "POST", body: JSON.stringify(input) }),
  updateUser: (id: string, input: { username: string; displayName: string; role: string; enabled: boolean; password?: string }) =>
    request<{ id: string; username: string; displayName: string; role: string; enabled: boolean; lastLoginAt?: string }>(
      `/api/users/${encodeURIComponent(id)}`,
      { method: "PATCH", body: JSON.stringify(input) },
    ),
  accessLogs: (input: { limit?: number; cursor?: number | null; search?: string; result?: "all" | "allowed" | "denied" } = {}) => {
    const query = new URLSearchParams({ limit: String(input.limit ?? 100) });
    if (input.cursor) query.set("cursor", String(input.cursor));
    if (input.search?.trim()) query.set("search", input.search.trim());
    if (input.result && input.result !== "all") query.set("result", input.result);
    return request<{
      items: Array<Record<string, string | number>>;
      nextCursor: number | null;
      hasMore: boolean;
      summary: { total: number; allowed: number; denied: number };
    }>(`/api/access-logs?${query.toString()}`);
  },
  grants: () => request<{ items: Array<{ id: number; userId: string; action: string; scope: string; effect: "allow" | "deny" }> }>("/api/grants"),
  saveGrant: (input: { userId: string; action: string; scope: string; effect: "allow" | "deny" }) =>
    request<{ id: number; userId: string; action: string; scope: string; effect: "allow" | "deny" }>("/api/grants", { method: "PUT", body: JSON.stringify(input) }),
  updateGrant: (id: number, input: { userId: string; action: string; scope: string; effect: "allow" | "deny" }) =>
    request<{ id: number; userId: string; action: string; scope: string; effect: "allow" | "deny" }>(`/api/grants/${id}`, { method: "PATCH", body: JSON.stringify(input) }),
  deleteGrant: (id: number) => request(`/api/grants/${id}`, { method: "DELETE", body: "{}" }),
};
