# RedisStreamScope Helm chart

This chart installs one RedisStreamScope instance and a persistent `/data`
volume. Redis is not bundled; connect an existing standalone, Sentinel, or
Cluster deployment through the setup screen or Helm values.

## Install from GHCR

```sh
helm upgrade --install redisstreamscope \
  oci://ghcr.io/ghkdqhrbals/charts/redisstreamscope \
  --namespace redisstreamscope \
  --create-namespace
```

For a source checkout, replace the OCI URL with
`./charts/redisstreamscope`.

Without Ingress, open the application with:

```sh
kubectl --namespace redisstreamscope \
  port-forward service/redisstreamscope 8080:80
```

Retrieve the generated initial password from the release notes:

```sh
helm get notes redisstreamscope --namespace redisstreamscope
```

Then visit <http://localhost:8080> and sign in as `admin`. The application
requires a new password immediately after the first sign-in.

To supply your own initial password instead, create a Secret and set:

```yaml
initialAdmin:
  existingSecret: redisstreamscope-initial-admin
  passwordKey: password
```

The Secret is only read when the database has no users; it does not replace an
existing administrator password during upgrades.

## Configure an Ingress

```yaml
ingress:
  enabled: true
  className: nginx
  hosts:
    - host: streams.example.com
      paths:
        - path: /
          pathType: Prefix
  tls:
    - secretName: streams-example-tls
      hosts:
        - streams.example.com

config:
  publicURL: https://streams.example.com
  secureCookies: true
```

`config.publicURL` supplies the evidence links included in Slack alerts. Use
the externally reachable root URL and enable secure cookies when HTTPS is in
use.

## Bootstrap a Redis connection

The default installation leaves Redis unconfigured so the browser setup flow
remains available. To provide a connection at install time, first create a
Secret:

```sh
kubectl --namespace redisstreamscope create secret generic redisstreamscope-redis \
  --from-literal=password='replace-me'
```

Then install or upgrade with a values file:

```yaml
redis:
  host: redis.default.svc.cluster.local
  port: 6379
  username: app
  auth:
    existingSecret: redisstreamscope-redis
    passwordKey: password

probes:
  readiness:
    path: /health/ready
```

The password Secret is mounted as a file and is not copied into Helm values.
Set exactly one of `redis.host`, `redis.url`, or `redis.nodes`. Avoid putting
credentials in `redis.url`, because the application persists startup connection
configuration to its managed file on the data volume.

## Storage and upgrades

The chart intentionally uses one replica and the `Recreate` deployment
strategy. RedisStreamScope stores application state in an embedded SQLite WAL
database, so multiple Pods must not share the data volume.

Persistence is enabled by default with a 1 GiB `ReadWriteOnce` claim. The chart
marks a generated claim for retention after uninstall. To reuse a claim:

```yaml
persistence:
  existingClaim: redisstreamscope-data
```

Back up the claim before storage migrations or destructive cluster operations.
If persistence is disabled, all accounts, sessions, alert settings, and managed
Redis connection settings are lost when the Pod is replaced.
