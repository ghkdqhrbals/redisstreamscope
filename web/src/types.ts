export type Page = "overview" | "streams" | "connections" | "access" | "settings";

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
  consumeDelayMs: number | null;
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
  username?: string;
  displayName?: string;
  role?: "viewer" | "operator" | "admin";
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
