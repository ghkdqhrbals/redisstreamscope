# StreamScope

StreamScope is a lightweight, self-hosted UI for Redis Streams. Browse streams and messages, follow live traffic, inspect consumer groups and pending entries, manage Redis connections, and control user access from one clean interface.

[Documentation](https://ghkdqhrbals.github.io/streamscope/) · [Container image](https://github.com/ghkdqhrbals/streamscope/pkgs/container/streamscope)

## Preview

### Overview

![StreamScope Overview](./design/streamscope-overview.png)

### Streams

![StreamScope Streams](./design/streamscope-streams.png)

### Consumer groups

![StreamScope Consumer Groups](./design/streamscope-consumer-groups.png)

### Redis connections

![StreamScope Redis Connections](./design/streamscope-connections.png)

## Quick start

One command is all you need:

```sh
docker run -d \
  --name streamscope \
  -p 8080:8080 \
  -v streamscope-data:/data \
  ghcr.io/ghkdqhrbals/streamscope:latest
```

Then:

1. Open [http://localhost:8080](http://localhost:8080).
2. Sign in with `admin` / `password`.
3. Open **Settings → Redis connections** and add your Redis server.
4. Click **Test connection**, then **Save configuration**.

That is it. You do not need to prepare a config file or pass Redis settings to Docker. StreamScope saves everything you configure in the browser to the `streamscope-data` volume.

> Running Redis directly on your macOS or Windows machine? Use `host.docker.internal:6379` instead of `localhost:6379`.

### Use a different host port

Keep the container port at `8080` and change only the number on the left:

```sh
docker run -d \
  --name streamscope \
  -p 9090:8080 \
  -v streamscope-data:/data \
  ghcr.io/ghkdqhrbals/streamscope:latest
```

Now open [http://localhost:9090](http://localhost:9090).

## What you can connect

Redis connections are managed inside StreamScope. You can add, test, reconfigure, and remove:

- Standalone Redis
- Redis Sentinel
- Redis Cluster
- Redis with or without a password
- Redis ACL users
- TLS and mTLS connections

## Redis compatibility

Every pull request runs the Redis Streams integration suite against the latest patch release in each supported Redis Open Source version line.

| Redis version | Support |
| --- | --- |
| `6.2` | Core Streams, groups, pending entries, `XCLAIM`, and `XAUTOCLAIM` |
| `7.0` | Adds consumer-group lag and entries-read metrics |
| `7.2`, `7.4` | Adds separate consumer inactive-time reporting |
| `8.0`, `8.2`, `8.4`, `8.6`, `8.8` | All current StreamScope features and metrics |

Redis 6.2 is the minimum supported version.

## Build locally

```sh
docker build -t streamscope:local .
docker run -d \
  --name streamscope \
  -p 8080:8080 \
  -v streamscope-data:/data \
  streamscope:local
```

## Releases

Run the [Release container image](https://github.com/ghkdqhrbals/streamscope/actions/workflows/release.yml) workflow on `main` and choose `patch`, `minor`, or `major`. The workflow updates the version, runs the test matrix, publishes `linux/amd64` and `linux/arm64` images to GHCR, and creates the matching GitHub Release.
