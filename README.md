# StreamScope – A UI for Redis Streams

StreamScope is a lightweight, self-hosted web application for managing and debugging Redis Streams. It gives you one clean interface for exploring messages, monitoring consumer groups, managing Redis connections, and controlling access.

[Documentation](https://ghkdqhrbals.github.io/streamscope/) · [Container image](https://github.com/ghkdqhrbals/streamscope/pkgs/container/streamscope)

## Preview

[![StreamScope preview](./design/streamscope-overview.png)](./design/preview.mp4)

[Watch preview.mp4](./design/preview.mp4)

## Features

- **Stream explorer:** Browse streams, inspect messages and payloads, search and sort entries, follow live traffic, add messages, and configure `MAXLEN`.
- **Consumer groups:** Inspect groups and consumers, lag, pending entries, idle and inactive time, delivery state, and assigned messages.
- **Redis connections:** Add, test, reconfigure, and remove standalone, Sentinel, and Cluster connections with password, ACL, TLS, or mTLS support.
- **Access control:** Manage users, roles, detailed permissions, and audit logs from the application.
- **Operations overview:** See stream and entry counts, consumer groups, total lag, pending messages, last consumption activity, and connection health.
- **Bilingual interface:** Use StreamScope in English or Korean.

## Getting started

### Prerequisites

- Docker
- Redis Open Source 6.2 or newer

### Quick start

Run StreamScope:

```sh
docker run -d \
  --name streamscope \
  -p 8080:8080 \
  -v streamscope-data:/data \
  ghcr.io/ghkdqhrbals/streamscope:latest
```

Open [http://localhost:8080](http://localhost:8080) and sign in with:

```text
Username: admin
Password: password
```

Go to **Settings → Redis connections**, add your Redis server, test the connection, and save it. No Redis configuration needs to be passed to Docker.

If Redis runs directly on your macOS or Windows host, connect to `host.docker.internal:6379`.

## Redis compatibility

Every pull request runs the Redis Streams integration suite against the latest patch release in each supported Redis Open Source version line.

| Redis version | Support |
| --- | --- |
| `6.2` | Core Streams, groups, pending entries, `XCLAIM`, and `XAUTOCLAIM` |
| `7.0` | Consumer-group lag and entries-read metrics |
| `7.2`, `7.4` | Separate consumer inactive-time reporting |
| `8.0`, `8.2`, `8.4`, `8.6`, `8.8` | All current StreamScope features and metrics |
