import type { ApiSession, ConsumerGroup, ConsumerInfo, PendingEntry, RedisConnection, RedisConnectionConfig, RedisEntry, StreamItem } from "./types";

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
  streams: (connectionId: string, cursor = 0, pattern?: string) =>
    request<{ items: StreamItem[]; nextCursor: number; hasMore: boolean }>(
      `/api/streams?connectionId=${encodeURIComponent(connectionId)}&cursor=${cursor}&limit=500${pattern ? `&pattern=${encodeURIComponent(pattern)}` : ""}`,
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
  accessLogs: () => request<{ items: Array<Record<string, string | number>> }>("/api/access-logs?limit=500"),
  grants: () => request<{ items: Array<{ id: number; userId: string; action: string; scope: string; effect: "allow" | "deny" }> }>("/api/grants"),
  saveGrant: (input: { userId: string; action: string; scope: string; effect: "allow" | "deny" }) =>
    request<{ id: number; userId: string; action: string; scope: string; effect: "allow" | "deny" }>("/api/grants", { method: "PUT", body: JSON.stringify(input) }),
  updateGrant: (id: number, input: { userId: string; action: string; scope: string; effect: "allow" | "deny" }) =>
    request<{ id: number; userId: string; action: string; scope: string; effect: "allow" | "deny" }>(`/api/grants/${id}`, { method: "PATCH", body: JSON.stringify(input) }),
  deleteGrant: (id: number) => request(`/api/grants/${id}`, { method: "DELETE", body: "{}" }),
};
