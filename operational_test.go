package main

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestCollectorFreshnessTracksCompletePartialFailedAndStaleRuns(t *testing.T) {
	clock := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	monitor := newOperationalMonitor()
	monitor.now = func() time.Time { return clock }

	attempt := monitor.BeginCollectorAttempt("redis", 0)
	attempt.SetMonitored(2)
	clock = clock.Add(125 * time.Millisecond)
	attempt.Finish(2, 0, nil)
	attempt.Finish(0, 2, errors.New("must be ignored because Finish is idempotent"))

	fresh := monitor.CollectorFreshness("redis")
	if fresh.Status != "healthy" || !fresh.Fresh {
		t.Fatalf("expected a healthy fresh collector, got %#v", fresh)
	}
	if fresh.DurationMs != 125 || fresh.MonitoredStreams != 2 || fresh.SucceededStreams != 2 {
		t.Fatalf("unexpected collector measurements: %#v", fresh)
	}

	clock = clock.Add(6 * time.Second)
	stale := monitor.CollectorFreshness("redis")
	if stale.Status != "stale" || stale.Fresh || stale.AgeMs == nil || *stale.AgeMs != 6000 {
		t.Fatalf("expected a stale collector, got %#v", stale)
	}

	partialAttempt := monitor.BeginCollectorAttempt("redis", 2)
	clock = clock.Add(20 * time.Millisecond)
	partialAttempt.Finish(1, 1, nil)
	partial := monitor.CollectorFreshness("redis")
	if partial.Status != "partial" || partial.ConsecutiveFailures != 1 || partial.LastSuccessAt == nil {
		t.Fatalf("expected partial status while retaining last full success: %#v", partial)
	}

	failedAttempt := monitor.BeginCollectorAttempt("redis", 2)
	clock = clock.Add(10 * time.Millisecond)
	failedAttempt.Finish(0, 2, errors.New("PING failed"))
	failed := monitor.CollectorFreshness("redis")
	if failed.Status != "failed" || failed.ConsecutiveFailures != 2 || failed.Error != "PING failed" {
		t.Fatalf("expected failed collector state, got %#v", failed)
	}
}

func TestRedisInfoParsersProduceNodeAndClusterHealth(t *testing.T) {
	raw := "# Server\r\n" +
		"redis_version:7.4.2\r\nrun_id:node-a\r\nuptime_in_seconds:99\r\n" +
		"# Memory\r\nused_memory:800\r\nmaxmemory:1000\r\nmem_fragmentation_ratio:1.25\r\n" +
		"# Clients\r\nconnected_clients:8\r\nblocked_clients:2\r\n" +
		"# Stats\r\ninstantaneous_ops_per_sec:250\r\nevicted_keys:3\r\nrejected_connections:4\r\n" +
		"# Replication\r\nrole:master\r\nconnected_slaves:2\r\nmaster_repl_offset:4455\r\n" +
		"master_link_status:up:with-colon\r\n"
	info := parseRedisInfo(raw)
	if info["master_link_status"] != "up:with-colon" {
		t.Fatalf("INFO values containing a colon must be preserved: %q", info["master_link_status"])
	}
	node := redisNodeHealthFromInfo(info, "fallback")
	if node.ID != "node-a" || node.Version != "7.4.2" || node.Role != "master" {
		t.Fatalf("unexpected node identity: %#v", node)
	}
	if node.MemoryPressurePct != 80 || node.ConnectedClients != 8 || node.ReplicationOffset != 4455 {
		t.Fatalf("unexpected parsed node health: %#v", node)
	}

	cluster := redisClusterHealthFromInfo(parseRedisInfo(
		"cluster_state:ok\ncluster_slots_assigned:16384\ncluster_slots_ok:16384\n" +
			"cluster_slots_pfail:0\ncluster_slots_fail:0\ncluster_known_nodes:6\ncluster_size:3\n",
	))
	if !cluster.Enabled || cluster.State != "ok" || cluster.SlotsOK != 16384 || cluster.KnownNodes != 6 {
		t.Fatalf("unexpected cluster health: %#v", cluster)
	}
}

func TestSummarizePendingSampleUsesPublishAgeIdleAndRetryThreshold(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000).UTC()
	pending := []redis.XPendingExt{
		{ID: "1799999990000-0", Idle: 2 * time.Second, RetryCount: 1},
		{ID: "1799999995000-0", Idle: 9 * time.Second, RetryCount: 5},
		{ID: "1799999999000-0", Idle: 1 * time.Second, RetryCount: 8},
	}
	result := summarizePendingSample(now, 10, pending, 5)
	if result.Sampled != 3 || !result.SampleTruncated {
		t.Fatalf("unexpected sample bounds: %#v", result)
	}
	if result.OldestPendingID != pending[0].ID || result.OldestPendingIDAgeMs == nil || *result.OldestPendingIDAgeMs != 10_000 {
		t.Fatalf("unexpected oldest pending entry: %#v", result)
	}
	if result.OldestPendingIdleMs == nil || *result.OldestPendingIdleMs != 9_000 {
		t.Fatalf("unexpected oldest idle value: %#v", result)
	}
	if result.MaxDeliveryCount != 8 || result.PoisonMessagesSampled != 2 {
		t.Fatalf("unexpected poison message summary: %#v", result)
	}
}

func TestOperationalCollectionIsBoundedAndComputesNetDrainETA(t *testing.T) {
	clock := time.UnixMilli(1_800_000_000_000).UTC()
	client := newFakeOperationalRedis(clock)
	monitor := newOperationalMonitor()
	monitor.now = func() time.Time { return clock }
	monitor.nodeInterval = 0
	monitor.streamInterval = 0
	monitor.memoryInterval = 0
	monitored := []monitoredStreamRecord{{ConnectionID: "redis", Key: "orders"}}

	first := monitor.CollectConnection(context.Background(), "redis", "standalone", client, monitored)
	if !first.Up || len(first.Streams) != 1 || len(first.Streams[0].Groups) != 1 {
		t.Fatalf("unexpected first operational snapshot: %#v", first)
	}
	stream := first.Streams[0]
	group := stream.Groups[0]
	if stream.MemoryBytes == nil || *stream.MemoryBytes != 4096 {
		t.Fatalf("expected bounded MEMORY USAGE sample: %#v", stream)
	}
	if group.DrainETASeconds != nil {
		t.Fatalf("an ETA requires at least two backlog observations: %#v", group)
	}
	if group.PendingSample.PoisonMessagesSampled != 1 || group.PendingSample.OldestPendingIDAgeMs == nil {
		t.Fatalf("expected PEL health from the bounded sample: %#v", group.PendingSample)
	}
	if group.ConsumerSampleStatus != "sampled" || len(group.ConsumerStates) != 2 || group.ConsumerStates[0].Name != "worker-1" {
		t.Fatalf("expected bounded XINFO CONSUMERS state: %#v", group)
	}

	client.mu.Lock()
	client.groups["orders"] = []redis.XInfoGroup{{
		Name: "workers", Consumers: 2, Pending: 1, Lag: 5, LastDeliveredID: "1799999999000-0",
	}}
	client.pending["orders\x00workers"] = []redis.XPendingExt{{
		ID: "1799999995000-0", Consumer: "worker-1", Idle: 3 * time.Second, RetryCount: 2,
	}}
	client.mu.Unlock()
	clock = clock.Add(15 * time.Second)
	second := monitor.CollectConnection(context.Background(), "redis", "standalone", client, monitored)
	group = second.Streams[0].Groups[0]
	if group.NetDrainRate == nil || !almostEqual(*group.NetDrainRate, 0.4) {
		t.Fatalf("expected net backlog drain of (12-6)/15 = 0.4/s, got %#v", group.NetDrainRate)
	}
	if group.DrainETASeconds == nil || !almostEqual(*group.DrainETASeconds, 15) {
		t.Fatalf("expected 15 second drain ETA, got %#v", group.DrainETASeconds)
	}

	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.pendingArgs) != 2 {
		t.Fatalf("expected one XPENDING call per due snapshot, got %d", len(client.pendingArgs))
	}
	for _, arguments := range client.pendingArgs {
		if arguments.Count != operationalPendingSampleLimit || arguments.Start != "-" || arguments.End != "+" {
			t.Fatalf("XPENDING must remain bounded: %#v", arguments)
		}
	}
	if len(client.memorySamples) != 2 || client.memorySamples[0] != 5 || client.memorySamples[1] != 5 {
		t.Fatalf("MEMORY USAGE must use bounded sampling: %#v", client.memorySamples)
	}
}

func TestDrainETAIsClearedWhenBacklogStopsDraining(t *testing.T) {
	clock := time.Unix(1_800_000_000, 0).UTC()
	monitor := newOperationalMonitor()
	firstBacklog := int64(10)
	first := consumerGroupOperationalHealth{Name: "workers", Backlog: &firstBacklog}
	monitor.applyGroupTrend("redis", "orders", &first, clock)

	secondBacklog := int64(5)
	second := consumerGroupOperationalHealth{Name: "workers", Backlog: &secondBacklog}
	monitor.applyGroupTrend("redis", "orders", &second, clock.Add(10*time.Second))
	if second.NetDrainRate == nil || *second.NetDrainRate != 0.5 || second.DrainETASeconds == nil || *second.DrainETASeconds != 10 {
		t.Fatalf("expected a finite drain estimate: %+v", second)
	}

	stalledBacklog := int64(5)
	stalled := consumerGroupOperationalHealth{Name: "workers", Backlog: &stalledBacklog}
	monitor.applyGroupTrend("redis", "orders", &stalled, clock.Add(20*time.Second))
	if stalled.NetDrainRate == nil || *stalled.NetDrainRate != 0 || stalled.DrainETASeconds != nil {
		t.Fatalf("stalled backlog must not retain a finite ETA: %+v", stalled)
	}
}

func TestOperationalCollectionCapsGroupsAndPendingCallsPerStream(t *testing.T) {
	clock := time.UnixMilli(1_800_000_000_000).UTC()
	client := newFakeOperationalRedis(clock)
	groups := make([]redis.XInfoGroup, 70)
	client.pending = make(map[string][]redis.XPendingExt, len(groups))
	for index := range groups {
		groups[index] = redis.XInfoGroup{Name: "group-" + strconv.Itoa(index), Pending: 1, Lag: 1}
		client.pending["orders\x00"+groups[index].Name] = []redis.XPendingExt{{
			ID: strconv.FormatInt(clock.Add(-time.Second).UnixMilli(), 10) + "-0", RetryCount: 1,
		}}
	}
	client.groups["orders"] = groups
	monitor := newOperationalMonitor()
	monitor.now = func() time.Time { return clock }
	monitor.nodeInterval = 0
	monitor.streamInterval = 0
	monitor.memoryInterval = 0

	snapshot := monitor.CollectConnection(context.Background(), "redis", "standalone", client, []monitoredStreamRecord{{Key: "orders"}})
	if len(snapshot.Streams) != 1 || len(snapshot.Streams[0].Groups) != operationalMaxGroupsPerStream {
		t.Fatalf("consumer group cardinality was not bounded: %#v", snapshot)
	}
	if snapshot.GroupsTruncated != 6 {
		t.Fatalf("expected six truncated groups, got %d", snapshot.GroupsTruncated)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.pendingArgs) != operationalMaxGroupsPerStream {
		t.Fatalf("expected at most %d pending calls, got %d", operationalMaxGroupsPerStream, len(client.pendingArgs))
	}
}

func TestRetentionRiskRequiresRepeatedGrowthWithoutObservedRemoval(t *testing.T) {
	clock := time.Unix(100, 0).UTC()
	monitor := newOperationalMonitor()
	monitor.now = func() time.Time { return clock }
	stream := streamOperationalHealth{Key: "events", Length: 100, EntriesAdded: 100, Risks: []string{}}
	monitor.applyStreamTrend("redis", &stream, false, clock)
	if stream.PotentiallyUnbounded {
		t.Fatal("one observation cannot prove a retention risk")
	}

	clock = clock.Add(15 * time.Second)
	stream = streamOperationalHealth{Key: "events", Length: 110, EntriesAdded: 110, Risks: []string{}}
	monitor.applyStreamTrend("redis", &stream, false, clock)
	if stream.PotentiallyUnbounded {
		t.Fatal("one growth interval is still insufficient")
	}

	clock = clock.Add(15 * time.Second)
	stream = streamOperationalHealth{Key: "events", Length: 120, EntriesAdded: 120, Risks: []string{}}
	monitor.applyStreamTrend("redis", &stream, false, clock)
	if !stream.PotentiallyUnbounded || stream.RetentionStatus != "growing_without_removal" {
		t.Fatalf("expected repeated unbounded growth risk, got %#v", stream)
	}
}

func TestCloneOperationalSnapshotPreservesEmptyCollections(t *testing.T) {
	cloned := cloneOperationalSnapshot(operationalSnapshot{
		Nodes: []redisNodeHealth{},
		Streams: []streamOperationalHealth{{
			Key: "orders", Risks: []string{}, Groups: []consumerGroupOperationalHealth{},
		}},
	})
	if cloned.Nodes == nil || cloned.Streams == nil {
		t.Fatalf("top-level collections must remain non-nil: %#v", cloned)
	}
	if cloned.Streams[0].Risks == nil || cloned.Streams[0].Groups == nil {
		t.Fatalf("stream collections must remain non-nil: %#v", cloned.Streams[0])
	}
}

func TestPrometheusRendererUsesAccurateNamesAndEscapesLabels(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	monitor := newOperationalMonitor()
	monitor.now = func() time.Time { return now }
	lastSuccess := now.Add(-time.Second)
	streamSampledAt := now.Add(-time.Second)
	ping := 2.5
	memory := int64(2048)
	retention := int64(60_000)
	lag := int64(4)
	oldest := int64(9_000)
	monitor.mu.Lock()
	monitor.freshness[`prod"east`] = collectorFreshnessState{
		collectorFreshness: collectorFreshness{
			ConnectionID: `prod"east`, LastSuccessAt: &lastSuccess, DurationMs: 12,
		},
		lastResult: "healthy",
	}
	monitor.snapshots[`prod"east`] = operationalSnapshot{
		ConnectionID: `prod"east`, Up: true, PingLatencyMs: &ping, StreamSampledAt: &streamSampledAt,
		Memory: redisMemoryHealth{UsedBytes: 1000, MaxBytes: 2000},
		Cluster: redisClusterHealth{
			Enabled: true, State: "ok", SlotsAssigned: 16_384, SlotsOK: 16_380,
			SlotsPFail: 3, SlotsFail: 1, KnownNodes: 6, ClusterSize: 3,
		},
		Nodes: []redisNodeHealth{{
			ID: "node-a", Role: "replica", Up: true, UptimeSeconds: 99,
			UsedMemoryBytes: 800, MaxMemoryBytes: 1000, TotalSystemMemoryBytes: 4096,
			MemoryFragmentation: 1.25, ConnectedClients: 8, BlockedClients: 2,
			OperationsPerSecond: 250, EvictedKeys: 3, RejectedConnections: 4,
			Loading: false, RDBLastSaveStatus: "ok", AOFLastRewriteStatus: "err",
			MasterLinkStatus: "up", ReplicationOffset: 4455, FullResyncs: 2,
			PartialResyncErrors: 1,
		}},
		Streams: []streamOperationalHealth{{
			Key: "orders\nv1", Available: true, Length: 7, EntriesAdded: 10, MemoryBytes: &memory,
			RetentionWindowMs: &retention,
			Groups: []consumerGroupOperationalHealth{{
				Name: "workers", Pending: 2, Lag: &lag,
				PendingSample: pendingSampleHealth{OldestPendingIDAgeMs: &oldest, OldestPendingIdleMs: &oldest, MaxDeliveryCount: 6, PoisonMessagesSampled: 1},
			}},
		}},
	}
	monitor.mu.Unlock()

	output := monitor.RenderPrometheus()
	for _, expected := range []string{
		`redisstreamscope_redis_ping_duration_seconds{connection="prod\"east"} 0.002500`,
		`redisstreamscope_redis_node_up{connection="prod\"east",node="node-a",role="replica"} 1`,
		`redisstreamscope_redis_node_replication_link_up{connection="prod\"east",node="node-a",role="replica"} 1`,
		`redisstreamscope_redis_node_rdb_last_bgsave_success{connection="prod\"east",node="node-a",role="replica"} 1`,
		`redisstreamscope_redis_node_aof_last_bgrewrite_success{connection="prod\"east",node="node-a",role="replica"} 0`,
		`redisstreamscope_redis_cluster_slots_fail{connection="prod\"east"} 1`,
		`redisstreamscope_stream_metrics_fresh{connection="prod\"east"} 1`,
		`redisstreamscope_stream_length{connection="prod\"east",stream="orders\nv1"} 7`,
		`redisstreamscope_consumer_group_lag{connection="prod\"east",group="workers",stream="orders\nv1"} 4`,
		`Redis PING round-trip duration. This is not application request latency.`,
	} {
		if !strings.Contains(output, expected) {
			t.Fatalf("expected Prometheus output to contain %q\n%s", expected, output)
		}
	}
	if strings.Contains(output, "request_latency") {
		t.Fatalf("PING must not be exported as request latency:\n%s", output)
	}
}

func TestPrometheusRendererDoesNotExportStaleOrUnavailableStreamSnapshots(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	monitor := newOperationalMonitor()
	monitor.now = func() time.Time { return now }
	monitor.streamInterval = 15 * time.Second
	oldSample := now.Add(-time.Minute)
	monitor.mu.Lock()
	monitor.snapshots["redis-down"] = operationalSnapshot{
		ConnectionID: "redis-down", Up: false, StreamSampledAt: &oldSample,
		Streams: []streamOperationalHealth{{
			Key: "stale-orders", Available: true, Length: 99,
			Groups: []consumerGroupOperationalHealth{{Name: "workers", Pending: 9}},
		}},
	}
	monitor.snapshots["redis-stale"] = operationalSnapshot{
		ConnectionID: "redis-stale", Up: true, StreamSampledAt: &oldSample,
		Streams: []streamOperationalHealth{{
			Key: "stale-while-up", Available: true, Length: 77,
		}},
	}
	freshSample := now.Add(-time.Second)
	lastSuccess := now.Add(-time.Second)
	monitor.freshness["redis-up"] = collectorFreshnessState{
		collectorFreshness: collectorFreshness{ConnectionID: "redis-up", LastSuccessAt: &lastSuccess},
		lastResult:         "healthy",
	}
	monitor.snapshots["redis-up"] = operationalSnapshot{
		ConnectionID: "redis-up", Up: true, StreamSampledAt: &freshSample,
		Streams: []streamOperationalHealth{
			{Key: "missing-orders", Available: false, Length: 88},
			{Key: "current-orders", Available: true, Length: 7},
		},
	}
	monitor.mu.Unlock()

	output := monitor.RenderPrometheus()
	for _, unexpected := range []string{"stale-orders", "stale-while-up", "missing-orders"} {
		if strings.Contains(output, unexpected) {
			t.Fatalf("stale or unavailable stream %q was exported:\n%s", unexpected, output)
		}
	}
	for _, expected := range []string{
		`redisstreamscope_stream_metrics_fresh{connection="redis-down"} 0`,
		`redisstreamscope_stream_metrics_fresh{connection="redis-stale"} 0`,
		`redisstreamscope_stream_metrics_fresh{connection="redis-up"} 1`,
		`redisstreamscope_stream_length{connection="redis-up",stream="current-orders"} 7`,
	} {
		if !strings.Contains(output, expected) {
			t.Fatalf("expected Prometheus output to contain %q\n%s", expected, output)
		}
	}
}

type fakeOperationalRedis struct {
	mu sync.Mutex

	info          string
	clusterInfo   string
	pingErr       error
	streams       map[string]*redis.XInfoStream
	groups        map[string][]redis.XInfoGroup
	pending       map[string][]redis.XPendingExt
	consumers     map[string][]redis.XInfoConsumer
	memory        map[string]int64
	pendingArgs   []*redis.XPendingExtArgs
	memorySamples []int
}

func newFakeOperationalRedis(now time.Time) *fakeOperationalRedis {
	return &fakeOperationalRedis{
		info: "redis_version:7.4.2\nrun_id:node-a\nrole:master\nused_memory:10000\nmaxmemory:20000\nmem_fragmentation_ratio:1.1\n",
		streams: map[string]*redis.XInfoStream{
			"orders": {
				Length: 20, EntriesAdded: 25, MaxDeletedEntryID: "1799999900000-0",
				FirstEntry: redis.XMessage{ID: "1799999900000-0"},
				LastEntry:  redis.XMessage{ID: "1799999999000-0"},
			},
		},
		groups: map[string][]redis.XInfoGroup{
			"orders": {{Name: "workers", Consumers: 2, Pending: 2, Lag: 10, LastDeliveredID: "1799999999000-0"}},
		},
		pending: map[string][]redis.XPendingExt{
			"orders\x00workers": {
				{ID: strconv.FormatInt(now.Add(-10*time.Second).UnixMilli(), 10) + "-0", Consumer: "worker-1", Idle: 10 * time.Second, RetryCount: 6},
				{ID: strconv.FormatInt(now.Add(-2*time.Second).UnixMilli(), 10) + "-0", Consumer: "worker-2", Idle: 2 * time.Second, RetryCount: 1},
			},
		},
		consumers: map[string][]redis.XInfoConsumer{
			"orders\x00workers": {
				{Name: "worker-1", Pending: 1, Idle: 10 * time.Second, Inactive: 10 * time.Second},
				{Name: "worker-2", Pending: 1, Idle: 2 * time.Second, Inactive: 2 * time.Second},
			},
		},
		memory: map[string]int64{"orders": 4096},
	}
}

func (client *fakeOperationalRedis) Ping(context.Context) *redis.StatusCmd {
	client.mu.Lock()
	defer client.mu.Unlock()
	return redis.NewStatusResult("PONG", client.pingErr)
}

func (client *fakeOperationalRedis) Info(context.Context, ...string) *redis.StringCmd {
	client.mu.Lock()
	defer client.mu.Unlock()
	return redis.NewStringResult(client.info, nil)
}

func (client *fakeOperationalRedis) ClusterInfo(context.Context) *redis.StringCmd {
	client.mu.Lock()
	defer client.mu.Unlock()
	return redis.NewStringResult(client.clusterInfo, nil)
}

func (client *fakeOperationalRedis) XInfoStream(_ context.Context, key string) *redis.XInfoStreamCmd {
	client.mu.Lock()
	defer client.mu.Unlock()
	command := redis.NewXInfoStreamCmd(context.Background(), key)
	value := client.streams[key]
	if value == nil {
		command.SetErr(redis.Nil)
	} else {
		copy := *value
		command.SetVal(&copy)
	}
	return command
}

func (client *fakeOperationalRedis) XInfoGroups(_ context.Context, key string) *redis.XInfoGroupsCmd {
	client.mu.Lock()
	defer client.mu.Unlock()
	command := redis.NewXInfoGroupsCmd(context.Background(), key)
	command.SetVal(append([]redis.XInfoGroup(nil), client.groups[key]...))
	return command
}

func (client *fakeOperationalRedis) XPendingExt(_ context.Context, arguments *redis.XPendingExtArgs) *redis.XPendingExtCmd {
	client.mu.Lock()
	defer client.mu.Unlock()
	copyArguments := *arguments
	client.pendingArgs = append(client.pendingArgs, &copyArguments)
	command := redis.NewXPendingExtCmd(context.Background())
	command.SetVal(append([]redis.XPendingExt(nil), client.pending[arguments.Stream+"\x00"+arguments.Group]...))
	return command
}

func (client *fakeOperationalRedis) XInfoConsumers(_ context.Context, key, group string) *redis.XInfoConsumersCmd {
	client.mu.Lock()
	defer client.mu.Unlock()
	command := redis.NewXInfoConsumersCmd(context.Background(), key, group)
	command.SetVal(append([]redis.XInfoConsumer(nil), client.consumers[key+"\x00"+group]...))
	return command
}

func (client *fakeOperationalRedis) MemoryUsage(_ context.Context, key string, samples ...int) *redis.IntCmd {
	client.mu.Lock()
	defer client.mu.Unlock()
	if len(samples) > 0 {
		client.memorySamples = append(client.memorySamples, samples[0])
	}
	return redis.NewIntResult(client.memory[key], nil)
}

func almostEqual(left, right float64) bool {
	difference := left - right
	if difference < 0 {
		difference = -difference
	}
	return difference < 0.000001
}
