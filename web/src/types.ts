export type Page = "overview" | "streams" | "alerts" | "connections" | "access" | "settings";

export type StreamItem = {
  key: string;
  length: number;
  monitored: boolean;
  available: boolean;
  redisType: string;
};

export type OverviewStreamItem = StreamItem & {
  consumerGroups: number;
  totalLag: number;
  lagKnown: boolean;
  pending: number;
  lastConsumed: string;
};

export type StreamMetricPoint = {
  timestamp: string;
  entries: number;
  consumerGroups: number;
  consumerCount: number;
  totalLag: number | null;
  pending: number;
  observedDeliveryAgeMs: number | null;
  redisLatencyMs: number;
  publishRate: number | null;
  consumeRate: number | null;
  lagDelta: number | null;
};

export type StreamMetricSeries = {
  connectionId: string;
  streamKey: string;
  range: "1m" | "5m" | "15m" | "1h" | "6h" | "24h" | "7d";
  intervalSeconds: number;
  generatedAt: string;
  items: StreamMetricPoint[];
};

export type ConsumerGroupMetricValue = {
  consumerCount: number;
  pending: number;
  lag: number | null;
  observedDeliveryAgeMs: number | null;
  consumeRate: number | null;
  lagDelta: number | null;
};

export type ConsumerGroupMetricPoint = {
  timestamp: string;
  values: Record<string, ConsumerGroupMetricValue>;
};

export type ConsumerGroupMetricSeries = {
  connectionId: string;
  streamKey: string;
  range: StreamMetricSeries["range"];
  intervalSeconds: number;
  generatedAt: string;
  groups: string[];
  items: ConsumerGroupMetricPoint[];
};

export type ConsumerGroupMetricSnapshot = {
  streamKey: string;
  groupName: string;
  consumerCount: number;
  pending: number;
  lag: number | null;
  observedDeliveryAgeMs: number | null;
  consumeRate: number | null;
  lagDelta: number | null;
  lastDeliveredId: string;
  sampledAt: string;
  lastActivityAt: string | null;
};

export type CollectorFreshness = {
  connectionId: string;
  status: "never_run" | "collecting" | "healthy" | "partial" | "failed" | "stale";
  fresh: boolean;
  inProgress: boolean;
  lastAttemptAt?: string;
  lastSuccessAt?: string;
  ageMs?: number;
  durationMs: number;
  consecutiveFailures: number;
  monitoredStreams: number;
  succeededStreams: number;
  failedStreams: number;
  error?: string;
};

export type PendingSampleHealth = {
  sampled: number;
  sampleTruncated: boolean;
  oldestPendingId?: string;
  oldestPendingIdAgeMs?: number;
  oldestPendingIdleMs?: number;
  p95PendingIdAgeMs?: number;
  p95PendingIdleMs?: number;
  maxDeliveryCount: number;
  poisonMessagesSampled: number;
  poisonDeliveryThreshold: number;
};

export type ConsumerGroupOperationalHealth = {
  name: string;
  consumers: number;
  pending: number;
  lag?: number;
  backlog?: number;
  lastDeliveredId: string;
  pendingSample: PendingSampleHealth;
  netDrainRate?: number;
  drainEtaSeconds?: number;
  sampleStatus: string;
};

export type StreamOperationalHealth = {
  key: string;
  available: boolean;
  error?: string;
  length: number;
  entriesAdded: number;
  removedEntriesEstimate: number;
  firstEntryId?: string;
  lastEntryId?: string;
  maxDeletedEntryId?: string;
  retentionWindowMs?: number;
  retentionStatus: string;
  potentiallyUnbounded: boolean;
  lengthGrowthPerSecond?: number;
  memoryBytes?: number;
  memoryGrowthBytesPerSecond?: number;
  memorySharePct?: number;
  oldestPendingIdAgeMs?: number;
  oldestPendingIdleMs?: number;
  maxDeliveryCount: number;
  poisonMessagesSampled: number;
  drainEtaSeconds?: number;
  risks: string[];
  groups: ConsumerGroupOperationalHealth[];
};

export type RedisNodeHealth = {
  id: string;
  address?: string;
  up: boolean;
  error?: string;
  version?: string;
  role?: string;
  uptimeSeconds: number;
  usedMemoryBytes: number;
  maxMemoryBytes: number;
  memoryPressurePct: number;
  memoryFragmentationRatio: number;
  connectedClients: number;
  blockedClients: number;
  operationsPerSecond: number;
  evictedKeys: number;
  rejectedConnections: number;
  loading: boolean;
  rdbLastSaveStatus?: string;
  aofLastRewriteStatus?: string;
  masterLinkStatus?: string;
  connectedReplicas: number;
  replicationOffset: number;
  fullResyncs: number;
  partialResyncErrors: number;
  totalSystemMemoryBytes: number;
};

export type OperationalSnapshot = {
  connectionId: string;
  mode: string;
  collectedAt: string;
  nodeSampledAt?: string;
  streamSampledAt?: string;
  memorySampledAt?: string;
  up: boolean;
  pingLatencyMs?: number;
  error?: string;
  collector: CollectorFreshness;
  nodes: RedisNodeHealth[];
  cluster: {
    enabled: boolean;
    state?: string;
    slotsAssigned: number;
    slotsOk: number;
    slotsPfail: number;
    slotsFail: number;
    knownNodes: number;
    clusterSize: number;
    message?: string;
  };
  memory: {
    usedBytes: number;
    maxBytes: number;
    pressurePct: number;
    fragmentationRatio: number;
    high: boolean;
    critical: boolean;
  };
  streams: StreamOperationalHealth[];
  streamsTruncated: number;
  groupsTruncated: number;
};

export type TelemetryToken = {
  id: string;
  name: string;
  prefix: string;
  enabled: boolean;
  createdBy?: string;
  createdAt: string;
  lastUsedAt?: string;
};

export type LifecycleDurationStats = {
  count: number;
  avgMs: number | null;
  p50Ms: number | null;
  p95Ms: number | null;
  p99Ms: number | null;
  maxMs: number | null;
};

export type LifecycleSummary = {
  requests: number;
  registered: number;
  processingStarted: number;
  processed: number;
  acknowledged: number;
  succeeded: number;
  failed: number;
  inFlight: number;
  queueDelay: LifecycleDurationStats;
  processing: LifecycleDurationStats;
  completion: LifecycleDurationStats;
  ackDelay: LifecycleDurationStats;
  endToEnd: LifecycleDurationStats;
};

export type LifecycleSeriesPoint = {
  at: string;
  registered: number;
  processingStarted: number;
  processed: number;
  acknowledged: number;
  succeeded: number;
  failed: number;
  queueDelay: LifecycleDurationStats;
  processing: LifecycleDurationStats;
  completion: LifecycleDurationStats;
  ackDelay: LifecycleDurationStats;
  endToEnd: LifecycleDurationStats;
};

export type LifecycleMetrics = {
  from: string;
  to: string;
  bucket: string;
  summary: LifecycleSummary;
  points: LifecycleSeriesPoint[];
  truncated: boolean;
  instrumented: boolean;
};

export type LifecycleRequest = {
  traceId: string;
  connectionId: string;
  streamKey: string;
  groupName?: string;
  entryId?: string;
  consumer?: string;
  registeredAt?: string;
  processingStartedAt?: string;
  processedAt?: string;
  acknowledgedAt?: string;
  outcome?: string;
  error?: string;
  attempt: number;
  createdAt: string;
  updatedAt: string;
  queueDelayMs?: number;
  processingMs?: number;
  completionMs?: number;
  ackDelayMs?: number;
  endToEndMs?: number;
};

export type SchemaFieldInsight = {
  path: string;
  types: string[];
  present: number;
  presencePct: number;
};

export type StreamSchemaSnapshot = {
  id: number;
  connectionId: string;
  streamKey: string;
  fingerprint: string;
  sampleCount: number;
  jsonPayloads: number;
  textPayloads: number;
  p50SizeBytes: number;
  p95SizeBytes: number;
  maxSizeBytes: number;
  fields: SchemaFieldInsight[];
  capturedAt: string;
};

export type StreamSchemaAnalysis = StreamSchemaSnapshot & {
  drift: {
    detected: boolean;
    comparedAt?: string;
    added: string[];
    removed: string[];
    typeChanged: string[];
  };
  history: StreamSchemaSnapshot[];
};

export type TraceSummary = {
  traceId: string;
  spanCount: number;
  streamCount: number;
  startedAt?: string;
  completedAt?: string;
  durationMs?: number;
  status: "success" | "failed" | "in_flight";
  updatedAt: string;
};

export type TraceSpan = {
  traceId: string;
  spanId: string;
  parentSpanId?: string;
  connectionId: string;
  streamKey: string;
  groupName?: string;
  entryId?: string;
  consumer?: string;
  service?: string;
  operation?: string;
  registeredAt?: string;
  processingStartedAt?: string;
  processedAt?: string;
  acknowledgedAt?: string;
  outcome?: string;
  error?: string;
  attempt: number;
  attributes?: Record<string, string>;
  createdAt: string;
  updatedAt: string;
};

export type DashboardTarget = { connectionId: string; streamKey: string };
export type DashboardDefinition = {
  timeRange: StreamMetricSeries["range"];
  targets: DashboardTarget[];
  widgets: string[];
};
export type SavedDashboard = {
  id: string;
  name: string;
  ownerId: string;
  shared: boolean;
  definition: DashboardDefinition;
  createdAt: string;
  updatedAt: string;
};

export type RecoveryPlanInput = {
  action: "xack" | "xclaim" | "xautoclaim" | "xgroup-setid";
  connectionId: string;
  streamKey: string;
  group: string;
  consumer?: string;
  ids?: string[];
  minIdleMs?: number;
  start?: string;
  count?: number;
  targetId?: string;
  expectedCurrentGroupId?: string;
};

export type RecoveryPlan = {
  action: RecoveryPlanInput["action"];
  connectionId: string;
  streamKey: string;
  group: string;
  requiredPermission: string;
  scope: string;
  requestedCount: number;
  candidateCount: number;
  pending: Array<{ id: string; consumer: string; idleMs: number; deliveryCount: number; eligible: boolean }>;
  currentGroupId?: string;
  expectedCurrentGroupId?: string;
  targetGroupId?: string;
  confirmation: string;
  warnings: string[];
  guarantees: string[];
};

export type QuarantineInput = {
  connectionId: string;
  sourceStream: string;
  sourceGroup?: string;
  dlqStream: string;
  ids: string[];
  acknowledgeSource: boolean;
};

export type QuarantinePlan = {
  connectionId: string;
  sourceStream: string;
  sourceGroup?: string;
  dlqStream: string;
  requestedCount: number;
  entryCount: number;
  payloadBytes: number;
  missingIds: string[];
  acknowledgeSource: boolean;
  requiredPermissions: string[];
  confirmation: string;
  guarantees: string[];
};

export type QuarantineRecord = {
  id: number;
  connectionId: string;
  sourceStream: string;
  sourceGroup?: string;
  sourceId: string;
  dlqStream: string;
  dlqEntryId: string;
  fields: Record<string, unknown>;
  metadata: Record<string, unknown>;
  status: "quarantined" | "replayed" | "skipped";
  sourceAcknowledged: boolean;
  replayTarget?: string;
  replayEntryId?: string;
  createdBy?: string;
  createdAt: string;
  updatedAt: string;
};

export type MonitoringEvent = {
  id: number;
  occurredAt: string;
  category: "consumer" | "topology" | "replication" | "persistence";
  connectionId: string;
  streamKey?: string;
  groupName?: string;
  consumerName?: string;
  nodeId?: string;
  type: string;
  from?: string;
  to?: string;
  details: Record<string, unknown>;
};

export type ConsumerHistoryResponse = {
  connectionId: string;
  from: string;
  to: string;
  stalledAfterMs: number;
  current: Array<{
    streamKey: string;
    groupName: string;
    consumerCount: number;
    pending: number;
    lag: number | null;
    lastDeliveredId: string;
    stalledConsumers: number;
    consumerSampleStatus: string;
    observedAt: string;
    consumers: Array<{ name: string; pending: number; idleMs: number; inactiveMs: number; stalled: boolean; observedAt: string }>;
  }>;
  samples: Array<{ id: number; recordedAt: string; streamKey: string; groupName: string; consumerCount: number; pending: number; lag: number | null; stalledConsumers: number }>;
  events: MonitoringEvent[];
  nextCursor?: number;
};

export type TopologyModel = {
  connectionId: string;
  recordedAt: string;
  mode: string;
  up: boolean;
  cluster: OperationalSnapshot["cluster"];
  nodes: RedisNodeHealth[];
};

export type RetentionPolicy = {
  connectionId: string;
  streamKey: string;
  maxLength?: number;
  maxAgeSeconds?: number;
  requireTrimming: boolean;
  updatedBy?: string;
  createdAt: string;
  updatedAt: string;
};

export type CapacityProjection = {
  current: number;
  growthPerSecond?: number;
  projectedIn1Hour?: number;
  projectedIn24Hours?: number;
  confidence: number;
  sampleCount: number;
  historySpanSeconds: number;
};

export type CapacityForecast = {
  connectionId: string;
  sampledAt: string;
  windowSeconds: number;
  connection: { usedMemoryBytes: CapacityProjection; maxMemoryBytes: number; timeToMaxMemorySeconds?: number };
  streams: Array<{
    streamKey: string;
    length: CapacityProjection;
    memoryBytes?: CapacityProjection;
    policy?: RetentionPolicy;
    compliance: { status: "unconfigured" | "compliant" | "at_risk" | "violating"; reasons: string[]; trimObserved: boolean; observedRetentionSeconds?: number; policySource: string };
  }>;
  disclaimer: string;
};

export type AlertMetricDefinition = {
  name: string;
  unit: string;
  scope: string[];
  description: string;
  suggestedOperator: string;
  suggestedValue: number;
};

export type AlertRule = {
  id: string;
  name: string;
  metric: string;
  operator: string;
  threshold: number;
  connectionId?: string;
  streamKey?: string;
  groupName?: string;
  forSeconds: number;
  cooldownSeconds: number;
  severity: "info" | "warning" | "critical";
  enabled: boolean;
  webhookConfigured: boolean;
  createdBy?: string;
  createdAt: string;
  updatedAt: string;
  state: "normal" | "pending" | "firing" | "acknowledged";
  conditionSince?: string;
  lastEvaluatedAt?: string;
  lastObservedAt?: string;
  lastValue?: number;
  lastTransitionAt?: string;
  lastNotificationAt?: string;
};

export type AlertRuleInput = {
  name: string;
  metric: string;
  operator: string;
  threshold: number;
  connectionId?: string;
  streamKey?: string;
  groupName?: string;
  forSeconds: number;
  cooldownSeconds: number;
  severity: "info" | "warning" | "critical";
  enabled: boolean;
  webhookUrl?: string;
};

export type AlertIncident = {
  id: string;
  ruleId: string;
  ruleName?: string;
  metric?: string;
  severity?: string;
  status: "firing" | "acknowledged" | "resolved";
  connectionId?: string;
  streamKey?: string;
  groupName?: string;
  startedAt: string;
  updatedAt: string;
  resolvedAt?: string;
  acknowledgedAt?: string;
  acknowledgedBy?: string;
  triggerValue: number;
  lastValue: number;
  summary: string;
};

export type AlertSummary = {
  firing: number;
  acknowledged: number;
  resolved: number;
  enabledRules: number;
  webhookFailures: number;
};

export type AlertWebhookDelivery = {
  id: number;
  ruleId: string;
  incidentId: string;
  event: string;
  attempt: number;
  statusCode: number;
  outcome: string;
  error?: string;
  createdAt: string;
  completedAt: string;
};

export type AlertSelector = {
  severities?: string[];
  metrics?: string[];
  connectionId?: string;
  streamKey?: string;
  groupName?: string;
  labels?: Record<string, string>;
};

export type AlertSuppression = {
  id: string;
  name: string;
  metric?: string;
  severity?: string;
  connectionId?: string;
  streamKey?: string;
  groupName?: string;
  labels?: Record<string, string>;
  startsAt: string;
  endsAt: string;
  reason: string;
  enabled: boolean;
  createdBy?: string;
  createdAt: string;
  updatedAt: string;
};

export type AlertSuppressionInput = Omit<AlertSuppression, "id" | "createdBy" | "createdAt" | "updatedAt">;

export type AlertRouteDestination = {
  id: string;
  name: string;
  webhookConfigured: boolean;
  enabled: boolean;
  createdAt: string;
  updatedAt: string;
};

export type AlertWebhookRoute = {
  id: string;
  name: string;
  selector: AlertSelector;
  destinations: AlertRouteDestination[];
  enabled: boolean;
  referencedByPolicies: Array<{ id: string; name: string }>;
  deleteBlocked: boolean;
  createdBy?: string;
  createdAt: string;
  updatedAt: string;
};

export type AlertWebhookRouteInput = {
  name: string;
  selector: AlertSelector;
  destinations: Array<{ id?: string; name: string; webhookUrl?: string; enabled: boolean }>;
  enabled: boolean;
};

export type AlertEscalationPolicy = {
  id: string;
  name: string;
  selector: AlertSelector;
  steps: Array<{ id: string; afterSeconds: number; routeId: string; targetSeverity: string; createdAt: string; updatedAt: string }>;
  enabled: boolean;
  createdBy?: string;
  createdAt: string;
  updatedAt: string;
};

export type AlertEscalationPolicyInput = {
  name: string;
  selector: AlertSelector;
  steps: Array<{ id?: string; afterSeconds: number; routeId: string; targetSeverity: string }>;
  enabled: boolean;
};

export type RedisConnection = {
  id: string;
  name: string;
  mode: "standalone" | "sentinel" | "cluster";
  healthy: boolean;
  latencyMs: number;
  tls: boolean;
  username: string;
};

export type RedisEntry = {
  id: string;
  timestamp: string;
  fields: Record<string, string | number>;
};

export type ConsumerGroup = {
  name: string;
  consumers: number;
  pending: number;
  lastDeliveredId: string;
  entriesRead: number;
  lag: number;
};

export type ConsumerInfo = {
  name: string;
  pending: number;
  idleMs: number;
  inactiveMs: number;
};

export type PendingEntry = {
  id: string;
  consumer: string;
  idleMs: number;
  retryCount: number;
};

export type ApiSession = {
  authenticated: boolean;
  userId?: string;
  username?: string;
  displayName?: string;
  role?: "viewer" | "operator" | "admin";
  permissions?: string[];
  expiresAt?: string;
  passwordChangeRequired?: boolean;
};

export type RedisConnectionConfig = {
  id: string;
  name: string;
  mode: "standalone" | "sentinel" | "cluster";
  addrs: string[];
  masterName: string;
  username: string;
  password?: string;
  passwordConfigured?: boolean;
  clearPassword?: boolean;
  db: number;
  keyPattern: string;
  tls: boolean;
  tlsServerName: string;
  tlsCAFile: string;
  tlsCertFile: string;
  tlsKeyFile: string;
};

export type ToastState = {
  kind: "success" | "warning" | "error";
  title: string;
  message: string;
};
