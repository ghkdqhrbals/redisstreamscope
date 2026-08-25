package main

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Operational collection deliberately runs slower than the one-second time-series
// collector.  The limits below keep the amount of work bounded even when a Redis
// deployment has a large number of consumer groups.
const (
	operationalCollectionInterval   = 5 * time.Second
	operationalNodeInterval         = 15 * time.Second
	operationalStreamInterval       = 15 * time.Second
	operationalMemoryInterval       = 30 * time.Second
	operationalCollectorStaleAfter  = 5 * time.Second
	operationalPendingSampleLimit   = int64(100)
	operationalMaxStreams           = 500
	operationalMaxGroupsPerStream   = 64
	operationalMaxGroups            = 2048
	operationalMaxPendingCalls      = 256
	operationalMaxConsumerCalls     = 128
	operationalMaxConsumers         = 512
	operationalMaxConsumersPerGroup = 64
	operationalMaxClusterNodes      = 64
	operationalPoisonRetryThreshold = int64(5)
)

type collectorFreshness struct {
	ConnectionID        string     `json:"connectionId"`
	Status              string     `json:"status"`
	Fresh               bool       `json:"fresh"`
	InProgress          bool       `json:"inProgress"`
	LastAttemptAt       *time.Time `json:"lastAttemptAt,omitempty"`
	LastSuccessAt       *time.Time `json:"lastSuccessAt,omitempty"`
	AgeMs               *int64     `json:"ageMs,omitempty"`
	DurationMs          float64    `json:"durationMs"`
	ConsecutiveFailures int        `json:"consecutiveFailures"`
	MonitoredStreams    int        `json:"monitoredStreams"`
	SucceededStreams    int        `json:"succeededStreams"`
	FailedStreams       int        `json:"failedStreams"`
	Error               string     `json:"error,omitempty"`
}

type collectorFreshnessState struct {
	collectorFreshness
	lastResult string
}

// collectorAttempt is the integration point for the existing one-second
// collector. BeginCollectorAttempt should be called before PING and Finish must
// be deferred/called on every return path.
type collectorAttempt struct {
	monitor      *operationalMonitor
	connectionID string
	startedAt    time.Time
	monitored    int
	once         sync.Once
}

func (attempt *collectorAttempt) SetMonitored(monitored int) {
	if attempt == nil || attempt.monitor == nil || monitored < 0 {
		return
	}
	attempt.monitored = monitored
	attempt.monitor.mu.Lock()
	state := attempt.monitor.freshness[attempt.connectionID]
	state.MonitoredStreams = monitored
	attempt.monitor.freshness[attempt.connectionID] = state
	attempt.monitor.mu.Unlock()
}

func (attempt *collectorAttempt) Finish(succeeded, failed int, err error) {
	if attempt == nil || attempt.monitor == nil {
		return
	}
	attempt.once.Do(func() {
		attempt.monitor.finishCollectorAttempt(
			attempt.connectionID,
			attempt.startedAt,
			attempt.monitored,
			succeeded,
			failed,
			err,
		)
	})
}

type redisNodeHealth struct {
	ID                     string  `json:"id"`
	Address                string  `json:"address,omitempty"`
	Up                     bool    `json:"up"`
	Error                  string  `json:"error,omitempty"`
	Version                string  `json:"version,omitempty"`
	Role                   string  `json:"role,omitempty"`
	UptimeSeconds          int64   `json:"uptimeSeconds"`
	UsedMemoryBytes        int64   `json:"usedMemoryBytes"`
	MaxMemoryBytes         int64   `json:"maxMemoryBytes"`
	MemoryPressurePct      float64 `json:"memoryPressurePct"`
	MemoryFragmentation    float64 `json:"memoryFragmentationRatio"`
	ConnectedClients       int64   `json:"connectedClients"`
	BlockedClients         int64   `json:"blockedClients"`
	OperationsPerSecond    int64   `json:"operationsPerSecond"`
	EvictedKeys            int64   `json:"evictedKeys"`
	RejectedConnections    int64   `json:"rejectedConnections"`
	Loading                bool    `json:"loading"`
	RDBLastSaveStatus      string  `json:"rdbLastSaveStatus,omitempty"`
	AOFLastRewriteStatus   string  `json:"aofLastRewriteStatus,omitempty"`
	MasterLinkStatus       string  `json:"masterLinkStatus,omitempty"`
	ConnectedReplicas      int64   `json:"connectedReplicas"`
	ReplicationOffset      int64   `json:"replicationOffset"`
	FullResyncs            int64   `json:"fullResyncs"`
	PartialResyncErrors    int64   `json:"partialResyncErrors"`
	TotalSystemMemoryBytes int64   `json:"totalSystemMemoryBytes"`
}

type redisClusterHealth struct {
	Enabled       bool   `json:"enabled"`
	State         string `json:"state,omitempty"`
	SlotsAssigned int64  `json:"slotsAssigned"`
	SlotsOK       int64  `json:"slotsOk"`
	SlotsPFail    int64  `json:"slotsPfail"`
	SlotsFail     int64  `json:"slotsFail"`
	KnownNodes    int64  `json:"knownNodes"`
	ClusterSize   int64  `json:"clusterSize"`
	Message       string `json:"message,omitempty"`
}

type redisMemoryHealth struct {
	UsedBytes          int64   `json:"usedBytes"`
	MaxBytes           int64   `json:"maxBytes"`
	PressurePct        float64 `json:"pressurePct"`
	FragmentationRatio float64 `json:"fragmentationRatio"`
	High               bool    `json:"high"`
	Critical           bool    `json:"critical"`
}

type pendingSampleHealth struct {
	Sampled                 int    `json:"sampled"`
	SampleTruncated         bool   `json:"sampleTruncated"`
	OldestPendingID         string `json:"oldestPendingId,omitempty"`
	OldestPendingIDAgeMs    *int64 `json:"oldestPendingIdAgeMs,omitempty"`
	OldestPendingIdleMs     *int64 `json:"oldestPendingIdleMs,omitempty"`
	P95PendingIDAgeMs       *int64 `json:"p95PendingIdAgeMs,omitempty"`
	P95PendingIdleMs        *int64 `json:"p95PendingIdleMs,omitempty"`
	MaxDeliveryCount        int64  `json:"maxDeliveryCount"`
	PoisonMessagesSampled   int64  `json:"poisonMessagesSampled"`
	PoisonDeliveryThreshold int64  `json:"poisonDeliveryThreshold"`
}

type consumerGroupOperationalHealth struct {
	Name                 string                      `json:"name"`
	Consumers            int64                       `json:"consumers"`
	Pending              int64                       `json:"pending"`
	Lag                  *int64                      `json:"lag,omitempty"`
	Backlog              *int64                      `json:"backlog,omitempty"`
	LastDeliveredID      string                      `json:"lastDeliveredId"`
	PendingSample        pendingSampleHealth         `json:"pendingSample"`
	NetDrainRate         *float64                    `json:"netDrainRate,omitempty"`
	DrainETASeconds      *float64                    `json:"drainEtaSeconds,omitempty"`
	SampleStatus         string                      `json:"sampleStatus"`
	ConsumerSampleStatus string                      `json:"consumerSampleStatus"`
	ConsumerStates       []consumerOperationalHealth `json:"consumerStates"`
}

type consumerOperationalHealth struct {
	Name       string `json:"name"`
	Pending    int64  `json:"pending"`
	IdleMs     int64  `json:"idleMs"`
	InactiveMs int64  `json:"inactiveMs"`
}

type streamOperationalHealth struct {
	Key                    string                           `json:"key"`
	Available              bool                             `json:"available"`
	Error                  string                           `json:"error,omitempty"`
	Length                 int64                            `json:"length"`
	EntriesAdded           int64                            `json:"entriesAdded"`
	RemovedEntriesEstimate int64                            `json:"removedEntriesEstimate"`
	FirstEntryID           string                           `json:"firstEntryId,omitempty"`
	LastEntryID            string                           `json:"lastEntryId,omitempty"`
	MaxDeletedEntryID      string                           `json:"maxDeletedEntryId,omitempty"`
	RetentionWindowMs      *int64                           `json:"retentionWindowMs,omitempty"`
	RetentionStatus        string                           `json:"retentionStatus"`
	PotentiallyUnbounded   bool                             `json:"potentiallyUnbounded"`
	LengthGrowthPerSecond  *float64                         `json:"lengthGrowthPerSecond,omitempty"`
	MemoryBytes            *int64                           `json:"memoryBytes,omitempty"`
	MemoryGrowthPerSecond  *float64                         `json:"memoryGrowthBytesPerSecond,omitempty"`
	MemorySharePct         *float64                         `json:"memorySharePct,omitempty"`
	OldestPendingIDAgeMs   *int64                           `json:"oldestPendingIdAgeMs,omitempty"`
	OldestPendingIdleMs    *int64                           `json:"oldestPendingIdleMs,omitempty"`
	MaxDeliveryCount       int64                            `json:"maxDeliveryCount"`
	PoisonMessagesSampled  int64                            `json:"poisonMessagesSampled"`
	DrainETASeconds        *float64                         `json:"drainEtaSeconds,omitempty"`
	Risks                  []string                         `json:"risks"`
	Groups                 []consumerGroupOperationalHealth `json:"groups"`
}

type operationalSnapshot struct {
	ConnectionID     string                    `json:"connectionId"`
	Mode             string                    `json:"mode"`
	CollectedAt      time.Time                 `json:"collectedAt"`
	NodeSampledAt    *time.Time                `json:"nodeSampledAt,omitempty"`
	StreamSampledAt  *time.Time                `json:"streamSampledAt,omitempty"`
	MemorySampledAt  *time.Time                `json:"memorySampledAt,omitempty"`
	Up               bool                      `json:"up"`
	PingLatencyMs    *float64                  `json:"pingLatencyMs,omitempty"`
	Error            string                    `json:"error,omitempty"`
	Collector        collectorFreshness        `json:"collector"`
	Nodes            []redisNodeHealth         `json:"nodes"`
	Cluster          redisClusterHealth        `json:"cluster"`
	Memory           redisMemoryHealth         `json:"memory"`
	Streams          []streamOperationalHealth `json:"streams"`
	StreamsTruncated int                       `json:"streamsTruncated"`
	GroupsTruncated  int                       `json:"groupsTruncated"`
}

type groupOperationalTrend struct {
	RecordedAt time.Time
	Backlog    int64
	DrainEMA   float64
}

type streamOperationalTrend struct {
	RecordedAt          time.Time
	Length              int64
	Removed             int64
	NoRemovalGrowthRuns int
	MemoryRecordedAt    time.Time
	MemoryBytes         int64
}

type operationalMonitor struct {
	mu sync.RWMutex

	now                 func() time.Time
	collectorStaleAfter time.Duration
	nodeInterval        time.Duration
	streamInterval      time.Duration
	memoryInterval      time.Duration

	freshness   map[string]collectorFreshnessState
	snapshots   map[string]operationalSnapshot
	collecting  map[string]bool
	groupTrends map[string]groupOperationalTrend
	streamTrend map[string]streamOperationalTrend
}

func newOperationalMonitor() *operationalMonitor {
	return &operationalMonitor{
		now:                 time.Now,
		collectorStaleAfter: operationalCollectorStaleAfter,
		nodeInterval:        operationalNodeInterval,
		streamInterval:      operationalStreamInterval,
		memoryInterval:      operationalMemoryInterval,
		freshness:           make(map[string]collectorFreshnessState),
		snapshots:           make(map[string]operationalSnapshot),
		collecting:          make(map[string]bool),
		groupTrends:         make(map[string]groupOperationalTrend),
		streamTrend:         make(map[string]streamOperationalTrend),
	}
}

var operationalMonitors sync.Map // map[*apiServer]*operationalMonitor

func (s *apiServer) operationalMonitor() *operationalMonitor {
	if existing, ok := operationalMonitors.Load(s); ok {
		return existing.(*operationalMonitor)
	}
	created := newOperationalMonitor()
	actual, _ := operationalMonitors.LoadOrStore(s, created)
	return actual.(*operationalMonitor)
}

func (m *operationalMonitor) BeginCollectorAttempt(connectionID string, monitored int) *collectorAttempt {
	startedAt := m.now().UTC()
	m.mu.Lock()
	state := m.freshness[connectionID]
	state.ConnectionID = connectionID
	state.InProgress = true
	state.LastAttemptAt = timePointer(startedAt)
	state.MonitoredStreams = monitored
	state.SucceededStreams = 0
	state.FailedStreams = 0
	state.Error = ""
	m.freshness[connectionID] = state
	m.mu.Unlock()
	return &collectorAttempt{monitor: m, connectionID: connectionID, startedAt: startedAt, monitored: monitored}
}

func (m *operationalMonitor) finishCollectorAttempt(
	connectionID string,
	startedAt time.Time,
	monitored int,
	succeeded int,
	failed int,
	err error,
) {
	finishedAt := m.now().UTC()
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.freshness[connectionID]
	state.ConnectionID = connectionID
	state.InProgress = false
	state.LastAttemptAt = timePointer(startedAt)
	state.DurationMs = float64(finishedAt.Sub(startedAt).Microseconds()) / 1000
	state.MonitoredStreams = monitored
	state.SucceededStreams = succeeded
	state.FailedStreams = failed
	state.Error = ""
	complete := err == nil && failed == 0 && (monitored == 0 || succeeded == monitored)
	switch {
	case complete:
		state.LastSuccessAt = timePointer(finishedAt)
		state.ConsecutiveFailures = 0
		state.lastResult = "healthy"
	case err != nil:
		state.ConsecutiveFailures++
		state.Error = err.Error()
		state.lastResult = "failed"
	case succeeded > 0:
		state.ConsecutiveFailures++
		state.Error = fmt.Sprintf("%d monitored streams failed", failed)
		state.lastResult = "partial"
	default:
		state.ConsecutiveFailures++
		state.Error = "no monitored stream was collected"
		state.lastResult = "failed"
	}
	m.freshness[connectionID] = state
}

func (m *operationalMonitor) CollectorFreshness(connectionID string) collectorFreshness {
	return m.collectorFreshnessAt(connectionID, m.now().UTC())
}

func (m *operationalMonitor) collectorFreshnessAt(connectionID string, now time.Time) collectorFreshness {
	m.mu.RLock()
	state, exists := m.freshness[connectionID]
	m.mu.RUnlock()
	if !exists {
		return collectorFreshness{ConnectionID: connectionID, Status: "never_run"}
	}
	result := state.collectorFreshness
	result.Fresh = false
	result.AgeMs = nil
	if state.LastSuccessAt != nil {
		age := now.Sub(*state.LastSuccessAt)
		if age < 0 {
			age = 0
		}
		ageMs := age.Milliseconds()
		result.AgeMs = &ageMs
		result.Fresh = age <= m.collectorStaleAfter
	}
	switch {
	case state.InProgress:
		result.Status = "collecting"
	case state.lastResult == "failed":
		result.Status = "failed"
	case state.lastResult == "partial":
		result.Status = "partial"
	case !result.Fresh:
		result.Status = "stale"
	default:
		result.Status = "healthy"
	}
	return result
}

type operationalRedisClient interface {
	Ping(context.Context) *redis.StatusCmd
	Info(context.Context, ...string) *redis.StringCmd
	ClusterInfo(context.Context) *redis.StringCmd
	XInfoStream(context.Context, string) *redis.XInfoStreamCmd
	XInfoGroups(context.Context, string) *redis.XInfoGroupsCmd
	XInfoConsumers(context.Context, string, string) *redis.XInfoConsumersCmd
	XPendingExt(context.Context, *redis.XPendingExtArgs) *redis.XPendingExtCmd
	MemoryUsage(context.Context, string, ...int) *redis.IntCmd
}

type operationalClusterClient interface {
	ForEachShard(context.Context, func(context.Context, *redis.Client) error) error
}

// CollectConnection updates only the portions whose low-overhead schedule is due.
// Calls for the same connection are coalesced, so a slow Redis node cannot create
// an accumulating goroutine backlog.
func (m *operationalMonitor) CollectConnection(
	ctx context.Context,
	connectionID string,
	mode string,
	client operationalRedisClient,
	monitored []monitoredStreamRecord,
) operationalSnapshot {
	now := m.now().UTC()
	m.mu.Lock()
	previous := cloneOperationalSnapshot(m.snapshots[connectionID])
	if m.collecting[connectionID] {
		previous.Collector = m.collectorFreshnessAtLocked(connectionID, now)
		m.mu.Unlock()
		return previous
	}
	m.collecting[connectionID] = true
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		delete(m.collecting, connectionID)
		m.mu.Unlock()
	}()

	snapshot := previous
	snapshot.ConnectionID = connectionID
	snapshot.Mode = mode
	snapshot.Collector = m.collectorFreshnessAt(connectionID, now)
	nodeDue := snapshot.NodeSampledAt == nil || now.Sub(*snapshot.NodeSampledAt) >= m.nodeInterval
	streamDue := snapshot.StreamSampledAt == nil || now.Sub(*snapshot.StreamSampledAt) >= m.streamInterval
	memoryDue := snapshot.MemorySampledAt == nil || now.Sub(*snapshot.MemorySampledAt) >= m.memoryInterval

	if nodeDue {
		snapshot.CollectedAt = now
		nodes, cluster, pingLatency, err := collectRedisNodeHealth(ctx, mode, client)
		snapshot.NodeSampledAt = timePointer(now)
		snapshot.Nodes = nodes
		snapshot.Cluster = cluster
		snapshot.PingLatencyMs = pingLatency
		snapshot.Up = err == nil
		if err != nil {
			snapshot.Error = err.Error()
		} else {
			snapshot.Error = ""
		}
		snapshot.Memory = aggregateNodeMemory(nodes)
	}

	if streamDue && snapshot.Up {
		snapshot.CollectedAt = now
		streams, streamsTruncated, groupsTruncated := m.collectStreamHealth(
			ctx,
			connectionID,
			client,
			monitored,
			memoryDue,
			snapshot.Streams,
			snapshot.Memory,
			now,
		)
		snapshot.Streams = streams
		snapshot.StreamsTruncated = streamsTruncated
		snapshot.GroupsTruncated = groupsTruncated
		snapshot.StreamSampledAt = timePointer(now)
		if memoryDue {
			snapshot.MemorySampledAt = timePointer(now)
		}
	}

	m.mu.Lock()
	m.snapshots[connectionID] = cloneOperationalSnapshot(snapshot)
	m.mu.Unlock()
	return cloneOperationalSnapshot(snapshot)
}

func (m *operationalMonitor) collectorFreshnessAtLocked(connectionID string, now time.Time) collectorFreshness {
	state, exists := m.freshness[connectionID]
	if !exists {
		return collectorFreshness{ConnectionID: connectionID, Status: "never_run"}
	}
	result := state.collectorFreshness
	if state.LastSuccessAt != nil {
		age := now.Sub(*state.LastSuccessAt)
		if age < 0 {
			age = 0
		}
		ageMs := age.Milliseconds()
		result.AgeMs = &ageMs
		result.Fresh = age <= m.collectorStaleAfter
	}
	switch {
	case state.InProgress:
		result.Status = "collecting"
	case state.lastResult == "failed":
		result.Status = "failed"
	case state.lastResult == "partial":
		result.Status = "partial"
	case !result.Fresh:
		result.Status = "stale"
	default:
		result.Status = "healthy"
	}
	return result
}

func collectRedisNodeHealth(
	ctx context.Context,
	mode string,
	client operationalRedisClient,
) ([]redisNodeHealth, redisClusterHealth, *float64, error) {
	startedAt := time.Now()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, redisClusterHealth{Enabled: mode == "cluster"}, nil, err
	}
	latency := float64(time.Since(startedAt).Microseconds()) / 1000
	pingLatency := &latency

	nodes := make([]redisNodeHealth, 0, 1)
	if clusterClient, ok := client.(operationalClusterClient); ok && mode == "cluster" {
		var mu sync.Mutex
		seen := 0
		_ = clusterClient.ForEachShard(ctx, func(nodeContext context.Context, node *redis.Client) error {
			mu.Lock()
			seen++
			index := seen
			mu.Unlock()
			if index > operationalMaxClusterNodes {
				return nil
			}
			info, err := node.Info(nodeContext).Result()
			health := redisNodeHealth{Address: node.Options().Addr, Up: err == nil}
			if err != nil {
				health.ID = node.Options().Addr
				health.Error = err.Error()
			} else {
				health = redisNodeHealthFromInfo(parseRedisInfo(info), node.Options().Addr)
			}
			mu.Lock()
			nodes = append(nodes, health)
			mu.Unlock()
			return nil
		})
	}
	if len(nodes) == 0 {
		info, err := client.Info(ctx).Result()
		if err != nil {
			return nil, redisClusterHealth{Enabled: mode == "cluster"}, pingLatency, err
		}
		nodes = append(nodes, redisNodeHealthFromInfo(parseRedisInfo(info), "default"))
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})

	cluster := redisClusterHealth{Enabled: mode == "cluster"}
	if mode == "cluster" {
		clusterInfo, err := client.ClusterInfo(ctx).Result()
		if err != nil {
			cluster.Message = err.Error()
		} else {
			cluster = redisClusterHealthFromInfo(parseRedisInfo(clusterInfo))
		}
	}
	return nodes, cluster, pingLatency, nil
}

func redisNodeHealthFromInfo(info map[string]string, fallbackID string) redisNodeHealth {
	health := redisNodeHealth{
		ID:                     firstNonEmpty(info["run_id"], fallbackID),
		Address:                fallbackID,
		Up:                     true,
		Version:                info["redis_version"],
		Role:                   info["role"],
		UptimeSeconds:          infoInt64(info, "uptime_in_seconds"),
		UsedMemoryBytes:        infoInt64(info, "used_memory"),
		MaxMemoryBytes:         infoInt64(info, "maxmemory"),
		MemoryFragmentation:    infoFloat64(info, "mem_fragmentation_ratio"),
		ConnectedClients:       infoInt64(info, "connected_clients"),
		BlockedClients:         infoInt64(info, "blocked_clients"),
		OperationsPerSecond:    infoInt64(info, "instantaneous_ops_per_sec"),
		EvictedKeys:            infoInt64(info, "evicted_keys"),
		RejectedConnections:    infoInt64(info, "rejected_connections"),
		Loading:                info["loading"] == "1",
		RDBLastSaveStatus:      info["rdb_last_bgsave_status"],
		AOFLastRewriteStatus:   info["aof_last_bgrewrite_status"],
		MasterLinkStatus:       info["master_link_status"],
		ConnectedReplicas:      infoInt64(info, "connected_slaves"),
		FullResyncs:            infoInt64(info, "sync_full"),
		PartialResyncErrors:    infoInt64(info, "sync_partial_err"),
		TotalSystemMemoryBytes: infoInt64(info, "total_system_memory"),
	}
	if health.Role == "slave" || health.Role == "replica" {
		health.ReplicationOffset = infoInt64(info, "slave_repl_offset")
	} else {
		health.ReplicationOffset = infoInt64(info, "master_repl_offset")
	}
	if health.MaxMemoryBytes > 0 {
		health.MemoryPressurePct = float64(health.UsedMemoryBytes) / float64(health.MaxMemoryBytes) * 100
	}
	return health
}

func redisClusterHealthFromInfo(info map[string]string) redisClusterHealth {
	return redisClusterHealth{
		Enabled:       true,
		State:         info["cluster_state"],
		SlotsAssigned: infoInt64(info, "cluster_slots_assigned"),
		SlotsOK:       infoInt64(info, "cluster_slots_ok"),
		SlotsPFail:    infoInt64(info, "cluster_slots_pfail"),
		SlotsFail:     infoInt64(info, "cluster_slots_fail"),
		KnownNodes:    infoInt64(info, "cluster_known_nodes"),
		ClusterSize:   infoInt64(info, "cluster_size"),
	}
}

func aggregateNodeMemory(nodes []redisNodeHealth) redisMemoryHealth {
	result := redisMemoryHealth{}
	var fragmentationTotal float64
	var fragmentationCount int
	allNodesBounded := true
	upNodes := 0
	for _, node := range nodes {
		if !node.Up {
			continue
		}
		upNodes++
		result.UsedBytes += node.UsedMemoryBytes
		if node.MaxMemoryBytes <= 0 {
			allNodesBounded = false
		} else {
			result.MaxBytes += node.MaxMemoryBytes
		}
		if node.MemoryFragmentation > 0 {
			fragmentationTotal += node.MemoryFragmentation
			fragmentationCount++
		}
	}
	if upNodes == 0 || !allNodesBounded {
		// A mixed bounded/unbounded cluster has no meaningful aggregate
		// percentage. Retain used bytes but report max=0 (unlimited/unknown).
		result.MaxBytes = 0
	}
	if result.MaxBytes > 0 {
		result.PressurePct = float64(result.UsedBytes) / float64(result.MaxBytes) * 100
		result.High = result.PressurePct >= 80
		result.Critical = result.PressurePct >= 90
	}
	if fragmentationCount > 0 {
		result.FragmentationRatio = fragmentationTotal / float64(fragmentationCount)
	}
	return result
}

func (m *operationalMonitor) collectStreamHealth(
	ctx context.Context,
	connectionID string,
	client operationalRedisClient,
	monitored []monitoredStreamRecord,
	memoryDue bool,
	previousStreams []streamOperationalHealth,
	memory redisMemoryHealth,
	now time.Time,
) ([]streamOperationalHealth, int, int) {
	streamsTruncated := 0
	if len(monitored) > operationalMaxStreams {
		streamsTruncated = len(monitored) - operationalMaxStreams
		monitored = monitored[:operationalMaxStreams]
	}
	previousByKey := make(map[string]streamOperationalHealth, len(previousStreams))
	for _, stream := range previousStreams {
		previousByKey[stream.Key] = stream
	}

	type streamCommands struct {
		info   *redis.XInfoStreamCmd
		groups *redis.XInfoGroupsCmd
		memory *redis.IntCmd
	}
	commands := make([]streamCommands, len(monitored))
	if pipeline, ok := client.(overviewPipelineClient); ok {
		_, _ = pipeline.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			for index, item := range monitored {
				commands[index].info = pipe.XInfoStream(ctx, item.Key)
				commands[index].groups = pipe.XInfoGroups(ctx, item.Key)
				if memoryDue {
					commands[index].memory = pipe.MemoryUsage(ctx, item.Key, 5)
				}
			}
			return nil
		})
	} else {
		for index, item := range monitored {
			commands[index].info = client.XInfoStream(ctx, item.Key)
			commands[index].groups = client.XInfoGroups(ctx, item.Key)
			if memoryDue {
				commands[index].memory = client.MemoryUsage(ctx, item.Key, 5)
			}
		}
	}

	result := make([]streamOperationalHealth, 0, len(monitored))
	groupBudget := operationalMaxGroups
	pendingCallBudget := operationalMaxPendingCalls
	consumerCallBudget := operationalMaxConsumerCalls
	consumerBudget := operationalMaxConsumers
	groupsTruncated := 0
	for index, item := range monitored {
		stream := streamOperationalHealth{Key: item.Key, Risks: []string{}, Groups: []consumerGroupOperationalHealth{}}
		info, err := commands[index].info.Result()
		if err != nil {
			stream.Error = err.Error()
			stream.Available = false
			result = append(result, stream)
			continue
		}
		stream.Available = true
		stream.Length = info.Length
		stream.EntriesAdded = maxInt64(info.EntriesAdded, info.Length)
		stream.RemovedEntriesEstimate = maxInt64(0, stream.EntriesAdded-info.Length)
		stream.FirstEntryID = info.FirstEntry.ID
		stream.LastEntryID = info.LastEntry.ID
		stream.MaxDeletedEntryID = info.MaxDeletedEntryID
		stream.RetentionWindowMs = streamIDSpan(info.FirstEntry.ID, info.LastEntry.ID)

		previous := previousByKey[item.Key]
		if !memoryDue && previous.MemoryBytes != nil {
			value := *previous.MemoryBytes
			stream.MemoryBytes = &value
		}
		if commands[index].memory != nil {
			value, memoryErr := commands[index].memory.Result()
			if memoryErr == nil {
				stream.MemoryBytes = &value
			}
		}
		m.applyStreamTrend(connectionID, &stream, memoryDue, now)
		if stream.MemoryBytes != nil && memory.UsedBytes > 0 {
			share := float64(*stream.MemoryBytes) / float64(memory.UsedBytes) * 100
			stream.MemorySharePct = &share
		}
		if memory.High {
			stream.Risks = append(stream.Risks, "connection_memory_high")
		}

		groups, groupErr := commands[index].groups.Result()
		if groupErr != nil && !errors.Is(groupErr, redis.Nil) {
			stream.Error = groupErr.Error()
			result = append(result, stream)
			continue
		}
		allowedGroups := len(groups)
		if allowedGroups > operationalMaxGroupsPerStream {
			groupsTruncated += allowedGroups - operationalMaxGroupsPerStream
			allowedGroups = operationalMaxGroupsPerStream
		}
		if allowedGroups > groupBudget {
			groupsTruncated += allowedGroups - groupBudget
			allowedGroups = groupBudget
		}
		groupBudget -= allowedGroups
		for _, group := range groups[:allowedGroups] {
			health := consumerGroupOperationalHealth{
				Name: group.Name, Consumers: group.Consumers, Pending: group.Pending,
				LastDeliveredID: group.LastDeliveredID, SampleStatus: "not_required",
				ConsumerSampleStatus: "not_sampled_budget", ConsumerStates: []consumerOperationalHealth{},
				PendingSample: pendingSampleHealth{PoisonDeliveryThreshold: operationalPoisonRetryThreshold},
			}
			if consumerCallBudget > 0 && consumerBudget > 0 {
				consumerCallBudget--
				consumers, consumerErr := client.XInfoConsumers(ctx, item.Key, group.Name).Result()
				if consumerErr != nil {
					health.ConsumerSampleStatus = "failed"
				} else {
					limit := len(consumers)
					if limit > operationalMaxConsumersPerGroup {
						limit = operationalMaxConsumersPerGroup
						health.ConsumerSampleStatus = "truncated"
					} else {
						health.ConsumerSampleStatus = "sampled"
					}
					if limit > consumerBudget {
						limit = consumerBudget
						health.ConsumerSampleStatus = "truncated"
					}
					consumerBudget -= limit
					for _, consumer := range consumers[:limit] {
						health.ConsumerStates = append(health.ConsumerStates, consumerOperationalHealth{
							Name: consumer.Name, Pending: consumer.Pending,
							IdleMs:     maxInt64(0, consumer.Idle.Milliseconds()),
							InactiveMs: maxInt64(0, consumer.Inactive.Milliseconds()),
						})
					}
				}
			}
			if group.Lag >= 0 {
				lag := group.Lag
				backlog := lag + group.Pending
				health.Lag = &lag
				health.Backlog = &backlog
				m.applyGroupTrend(connectionID, item.Key, &health, now)
			}
			if group.Pending > 0 {
				if pendingCallBudget <= 0 {
					health.SampleStatus = "not_sampled_budget"
				} else {
					pendingCallBudget--
					pending, pendingErr := client.XPendingExt(ctx, &redis.XPendingExtArgs{
						Stream: item.Key,
						Group:  group.Name,
						Start:  "-",
						End:    "+",
						Count:  operationalPendingSampleLimit,
					}).Result()
					if pendingErr != nil {
						health.SampleStatus = "failed"
					} else {
						health.SampleStatus = "sampled"
						health.PendingSample = summarizePendingSample(now, group.Pending, pending, operationalPoisonRetryThreshold)
					}
				}
			}
			mergeStreamGroupHealth(&stream, health)
			stream.Groups = append(stream.Groups, health)
		}
		recomputeStreamDrainETA(&stream)
		result = append(result, stream)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result, streamsTruncated, groupsTruncated
}

func (m *operationalMonitor) applyStreamTrend(connectionID string, stream *streamOperationalHealth, memoryDue bool, now time.Time) {
	key := connectionID + "\x00" + stream.Key
	m.mu.Lock()
	defer m.mu.Unlock()
	previous, exists := m.streamTrend[key]
	next := previous
	next.RecordedAt = now
	next.Length = stream.Length
	next.Removed = stream.RemovedEntriesEstimate
	stream.RetentionStatus = "insufficient_data"
	if stream.RemovedEntriesEstimate > 0 {
		stream.RetentionStatus = "removal_observed"
	}
	if exists {
		elapsed := now.Sub(previous.RecordedAt).Seconds()
		if elapsed > 0 {
			growth := float64(stream.Length-previous.Length) / elapsed
			stream.LengthGrowthPerSecond = &growth
			if growth > 0 && stream.RemovedEntriesEstimate <= previous.Removed {
				next.NoRemovalGrowthRuns = previous.NoRemovalGrowthRuns + 1
			} else if stream.RemovedEntriesEstimate > previous.Removed || growth <= 0 {
				next.NoRemovalGrowthRuns = 0
			}
		}
	}
	if memoryDue && stream.MemoryBytes != nil {
		if !previous.MemoryRecordedAt.IsZero() {
			elapsed := now.Sub(previous.MemoryRecordedAt).Seconds()
			if elapsed > 0 {
				growth := float64(*stream.MemoryBytes-previous.MemoryBytes) / elapsed
				stream.MemoryGrowthPerSecond = &growth
			}
		}
		next.MemoryRecordedAt = now
		next.MemoryBytes = *stream.MemoryBytes
	}
	if next.NoRemovalGrowthRuns >= 2 {
		stream.RetentionStatus = "growing_without_removal"
		stream.PotentiallyUnbounded = true
		stream.Risks = append(stream.Risks, "growing_without_removal")
	}
	m.streamTrend[key] = next
}

func (m *operationalMonitor) applyGroupTrend(
	connectionID string,
	streamKey string,
	group *consumerGroupOperationalHealth,
	now time.Time,
) {
	if group.Backlog == nil {
		return
	}
	key := connectionID + "\x00" + streamKey + "\x00" + group.Name
	m.mu.Lock()
	defer m.mu.Unlock()
	previous, exists := m.groupTrends[key]
	next := groupOperationalTrend{RecordedAt: now, Backlog: *group.Backlog, DrainEMA: previous.DrainEMA}
	if *group.Backlog == 0 {
		zero := float64(0)
		group.NetDrainRate = &zero
		group.DrainETASeconds = &zero
		next.DrainEMA = 0
	} else if exists {
		elapsed := now.Sub(previous.RecordedAt).Seconds()
		if elapsed > 0 {
			drained := previous.Backlog - *group.Backlog
			if drained <= 0 {
				// A stalled or growing backlog invalidates a previous ETA. Keeping a
				// decaying positive EMA would claim the queue is draining when the
				// latest operational evidence says it is not.
				next.DrainEMA = 0
				zero := float64(0)
				group.NetDrainRate = &zero
			} else {
				observed := float64(drained) / elapsed
				if previous.DrainEMA == 0 {
					next.DrainEMA = observed
				} else {
					next.DrainEMA = previous.DrainEMA*0.7 + observed*0.3
				}
				rate := next.DrainEMA
				group.NetDrainRate = &rate
				eta := float64(*group.Backlog) / rate
				group.DrainETASeconds = &eta
			}
		}
	}
	m.groupTrends[key] = next
}

func summarizePendingSample(now time.Time, total int64, pending []redis.XPendingExt, poisonThreshold int64) pendingSampleHealth {
	result := pendingSampleHealth{
		Sampled:                 len(pending),
		SampleTruncated:         total > int64(len(pending)),
		PoisonDeliveryThreshold: poisonThreshold,
	}
	if len(pending) == 0 {
		return result
	}
	ages := make([]int64, 0, len(pending))
	idles := make([]int64, 0, len(pending))
	oldestAge := int64(-1)
	oldestIdle := int64(-1)
	for _, entry := range pending {
		age := streamIDAge(now, entry.ID)
		if age != nil {
			ages = append(ages, *age)
			if *age > oldestAge {
				oldestAge = *age
				result.OldestPendingID = entry.ID
			}
		}
		idle := maxInt64(0, entry.Idle.Milliseconds())
		idles = append(idles, idle)
		if idle > oldestIdle {
			oldestIdle = idle
		}
		if entry.RetryCount > result.MaxDeliveryCount {
			result.MaxDeliveryCount = entry.RetryCount
		}
		if entry.RetryCount >= poisonThreshold {
			result.PoisonMessagesSampled++
		}
	}
	if oldestAge >= 0 {
		result.OldestPendingIDAgeMs = int64Pointer(oldestAge)
		result.P95PendingIDAgeMs = int64Pointer(percentile95(ages))
	}
	if oldestIdle >= 0 {
		result.OldestPendingIdleMs = int64Pointer(oldestIdle)
		result.P95PendingIdleMs = int64Pointer(percentile95(idles))
	}
	return result
}

func mergeStreamGroupHealth(stream *streamOperationalHealth, group consumerGroupOperationalHealth) {
	if group.PendingSample.OldestPendingIDAgeMs != nil &&
		(stream.OldestPendingIDAgeMs == nil || *group.PendingSample.OldestPendingIDAgeMs > *stream.OldestPendingIDAgeMs) {
		stream.OldestPendingIDAgeMs = int64Pointer(*group.PendingSample.OldestPendingIDAgeMs)
	}
	if group.PendingSample.OldestPendingIdleMs != nil &&
		(stream.OldestPendingIdleMs == nil || *group.PendingSample.OldestPendingIdleMs > *stream.OldestPendingIdleMs) {
		stream.OldestPendingIdleMs = int64Pointer(*group.PendingSample.OldestPendingIdleMs)
	}
	stream.MaxDeliveryCount = maxInt64(stream.MaxDeliveryCount, group.PendingSample.MaxDeliveryCount)
	stream.PoisonMessagesSampled += group.PendingSample.PoisonMessagesSampled
	if group.PendingSample.PoisonMessagesSampled > 0 {
		stream.Risks = appendUnique(stream.Risks, "poison_messages_sampled")
	}
}

func recomputeStreamDrainETA(stream *streamOperationalHealth) {
	var maximum *float64
	for _, group := range stream.Groups {
		if group.Backlog == nil || *group.Backlog == 0 {
			continue
		}
		if group.DrainETASeconds == nil {
			// One non-empty group with no observed net drain makes a stream-wide
			// ETA unknowable; do not hide it behind a faster group.
			stream.DrainETASeconds = nil
			return
		}
		if maximum == nil || *group.DrainETASeconds > *maximum {
			maximum = float64Pointer(*group.DrainETASeconds)
		}
	}
	stream.DrainETASeconds = maximum
}

func parseRedisInfo(raw string) map[string]string {
	result := make(map[string]string)
	for _, line := range strings.Split(strings.ReplaceAll(raw, "\r\n", "\n"), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, found := strings.Cut(line, ":")
		if !found || key == "" {
			continue
		}
		result[key] = value
	}
	return result
}

func infoInt64(info map[string]string, key string) int64 {
	value, _ := strconv.ParseInt(strings.TrimSpace(info[key]), 10, 64)
	return value
}

func infoFloat64(info map[string]string, key string) float64 {
	value, _ := strconv.ParseFloat(strings.TrimSpace(info[key]), 64)
	return value
}

func streamIDMillis(id string) (int64, bool) {
	raw, _, found := strings.Cut(id, "-")
	if !found || raw == "" {
		return 0, false
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	return value, err == nil
}

func streamIDAge(now time.Time, id string) *int64 {
	millis, ok := streamIDMillis(id)
	if !ok {
		return nil
	}
	age := now.UnixMilli() - millis
	if age < 0 {
		age = 0
	}
	return &age
}

func streamIDSpan(first, last string) *int64 {
	firstMillis, firstOK := streamIDMillis(first)
	lastMillis, lastOK := streamIDMillis(last)
	if !firstOK || !lastOK {
		return nil
	}
	span := maxInt64(0, lastMillis-firstMillis)
	return &span
}

func percentile95(values []int64) int64 {
	if len(values) == 0 {
		return 0
	}
	ordered := append([]int64(nil), values...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	index := int(math.Ceil(float64(len(ordered))*0.95)) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(ordered) {
		index = len(ordered) - 1
	}
	return ordered[index]
}

func (m *operationalMonitor) Snapshot(connectionID string) (operationalSnapshot, bool) {
	m.mu.RLock()
	snapshot, exists := m.snapshots[connectionID]
	m.mu.RUnlock()
	if !exists {
		return operationalSnapshot{}, false
	}
	result := cloneOperationalSnapshot(snapshot)
	result.Collector = m.CollectorFreshness(connectionID)
	return result, true
}

func (m *operationalMonitor) Snapshots() []operationalSnapshot {
	m.mu.RLock()
	ids := make([]string, 0, len(m.snapshots))
	for connectionID := range m.snapshots {
		ids = append(ids, connectionID)
	}
	m.mu.RUnlock()
	sort.Strings(ids)
	result := make([]operationalSnapshot, 0, len(ids))
	for _, connectionID := range ids {
		if snapshot, exists := m.Snapshot(connectionID); exists {
			result = append(result, snapshot)
		}
	}
	return result
}

func cloneOperationalSnapshot(snapshot operationalSnapshot) operationalSnapshot {
	result := snapshot
	// Keep collection fields as JSON arrays even when they are empty. Appending
	// to a nil slice turns an intentionally empty slice back into nil, which is
	// encoded as JSON null and violates the snapshot API contract used by the UI.
	result.Nodes = append([]redisNodeHealth{}, snapshot.Nodes...)
	result.Streams = make([]streamOperationalHealth, len(snapshot.Streams))
	for index, stream := range snapshot.Streams {
		result.Streams[index] = stream
		result.Streams[index].Risks = append([]string{}, stream.Risks...)
		result.Streams[index].Groups = append([]consumerGroupOperationalHealth{}, stream.Groups...)
		for groupIndex := range result.Streams[index].Groups {
			result.Streams[index].Groups[groupIndex].ConsumerStates = append(
				[]consumerOperationalHealth{}, stream.Groups[groupIndex].ConsumerStates...,
			)
		}
	}
	return result
}

// startOperationalCollection performs bounded, low-frequency work independently
// of the one-second time-series collector.
func (s *apiServer) startOperationalCollection(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.collectOperationalSnapshots(ctx)
		ticker := time.NewTicker(operationalCollectionInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.collectOperationalSnapshots(ctx)
			}
		}
	}()
	return done
}

func (s *apiServer) collectOperationalSnapshots(parent context.Context) {
	monitor := s.operationalMonitor()
	for _, connectionID := range s.redis.ids() {
		if parent.Err() != nil {
			return
		}
		connection, err := s.redis.get(connectionID)
		if err != nil {
			continue
		}
		ctx, cancel := context.WithTimeout(parent, 8*time.Second)
		monitored, listErr := s.store.listMonitoredStreams(ctx, connectionID)
		if listErr != nil {
			cancel()
			log.Printf(`{"level":"warn","message":"Unable to load monitored streams for operational metrics","connection":%q,"error":%q}`, connectionID, listErr)
			continue
		}
		snapshot := monitor.CollectConnection(ctx, connectionID, connection.config.Mode, connection.client, monitored)
		if historyErr := s.store.recordOperationalSnapshot(ctx, snapshot); historyErr != nil && !errors.Is(historyErr, context.Canceled) {
			log.Printf(`{"level":"warn","message":"Unable to store operational history","connection":%q,"error":%q}`, connectionID, historyErr)
		}
		cancel()
	}
}

func (s *apiServer) operationsSnapshot(writer http.ResponseWriter, request *http.Request) {
	connection, err := s.redis.get(request.URL.Query().Get("connectionId"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "unknown_connection", err.Error())
		return
	}
	checker, err := s.streamPermissionChecker(request, "streams:read")
	if err != nil {
		writePermissionCheckError(writer)
		return
	}
	monitor := s.operationalMonitor()
	snapshot, exists := monitor.Snapshot(connection.config.ID)
	if !exists {
		ctx, cancel := context.WithTimeout(request.Context(), 8*time.Second)
		defer cancel()
		monitored, listErr := s.store.listMonitoredStreams(ctx, connection.config.ID)
		if listErr != nil {
			writeError(writer, http.StatusInternalServerError, "operational_snapshot_failed", listErr.Error())
			return
		}
		snapshot = monitor.CollectConnection(ctx, connection.config.ID, connection.config.Mode, connection.client, monitored)
		if historyErr := s.store.recordOperationalSnapshot(ctx, snapshot); historyErr != nil && !errors.Is(historyErr, context.Canceled) {
			log.Printf(`{"level":"warn","message":"Unable to store operational history","connection":%q,"error":%q}`, connection.config.ID, historyErr)
		}
	}
	visibleStreams := make([]streamOperationalHealth, 0, len(snapshot.Streams))
	failedStreams := 0
	for _, stream := range snapshot.Streams {
		if !checker.allows("streams:read", redisStreamScope(connection.config.ID, stream.Key)) {
			continue
		}
		visibleStreams = append(visibleStreams, stream)
		if !stream.Available {
			failedStreams++
		}
	}
	snapshot.Streams = visibleStreams
	if !checker.admin {
		snapshot.StreamsTruncated = 0
		snapshot.GroupsTruncated = 0
		snapshot.Collector.MonitoredStreams = len(visibleStreams)
		snapshot.Collector.FailedStreams = failedStreams
		snapshot.Collector.SucceededStreams = len(visibleStreams) - failedStreams
	}
	writeJSON(writer, http.StatusOK, snapshot)
}

// RenderPrometheus exports only explicitly monitored streams and their bounded
// consumer-group samples. It never exports stream entry IDs, consumer names, or
// payload-derived labels.
func (m *operationalMonitor) RenderPrometheus() string {
	var builder strings.Builder
	writePrometheusHeader(&builder, "redisstreamscope_collector_fresh", "Whether the one-second collector completed successfully within the freshness window.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_collector_last_success_timestamp_seconds", "Unix timestamp of the last complete collector success.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_collector_duration_seconds", "Duration of the most recent one-second collector attempt.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_collector_consecutive_failures", "Consecutive incomplete or failed collector attempts.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_redis_up", "Whether the most recent Redis operational PING succeeded.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_redis_ping_duration_seconds", "Redis PING round-trip duration. This is not application request latency.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_redis_memory_used_bytes", "Redis memory used across sampled nodes.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_redis_memory_max_bytes", "Configured Redis maxmemory across sampled nodes.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_redis_node_up", "Whether the sampled Redis node responded to INFO.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_redis_node_uptime_seconds", "Redis node uptime in seconds.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_redis_node_memory_used_bytes", "Memory used by the Redis node.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_redis_node_memory_max_bytes", "Configured maxmemory for the Redis node; zero means unlimited or unknown.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_redis_node_total_system_memory_bytes", "Total system memory reported by the Redis node.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_redis_node_memory_fragmentation_ratio", "Redis node memory fragmentation ratio.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_redis_node_connected_clients", "Connected clients reported by the Redis node.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_redis_node_blocked_clients", "Blocked clients reported by the Redis node.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_redis_node_operations_per_second", "Instantaneous operations per second reported by the Redis node.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_redis_node_evicted_keys_total", "Cumulative keys evicted by the Redis node.", "counter")
	writePrometheusHeader(&builder, "redisstreamscope_redis_node_rejected_connections_total", "Cumulative connections rejected by the Redis node.", "counter")
	writePrometheusHeader(&builder, "redisstreamscope_redis_node_loading", "Whether the Redis node is loading a persistence file.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_redis_node_rdb_last_bgsave_success", "Whether the last Redis RDB background save succeeded; omitted when unavailable.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_redis_node_aof_last_bgrewrite_success", "Whether the last Redis AOF background rewrite succeeded; omitted when unavailable.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_redis_node_connected_replicas", "Replicas connected to the Redis primary node.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_redis_node_replication_offset", "Current Redis replication offset for the node.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_redis_node_replication_link_up", "Whether the replica link to its primary is up; omitted for nodes without a reported link state.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_redis_node_full_resyncs_total", "Cumulative full replication resynchronizations reported by the Redis node.", "counter")
	writePrometheusHeader(&builder, "redisstreamscope_redis_node_partial_resync_errors_total", "Cumulative failed partial replication resynchronizations reported by the Redis node.", "counter")
	writePrometheusHeader(&builder, "redisstreamscope_redis_cluster_enabled", "Whether the Redis connection is configured in cluster mode.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_redis_cluster_state_ok", "Whether Redis reports cluster_state=ok; omitted for non-cluster connections.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_redis_cluster_slots_assigned", "Hash slots assigned in the Redis cluster.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_redis_cluster_slots_ok", "Hash slots in an OK state in the Redis cluster.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_redis_cluster_slots_pfail", "Hash slots in a probable-failure state in the Redis cluster.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_redis_cluster_slots_fail", "Hash slots in a failed state in the Redis cluster.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_redis_cluster_known_nodes", "Nodes known to the Redis cluster.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_redis_cluster_size", "Primary node count reported by the Redis cluster.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_stream_metrics_fresh", "Whether the stream and consumer-group snapshot is current and Redis was available when exported.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_stream_length", "Current Redis Stream length.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_stream_memory_bytes", "MEMORY USAGE for an explicitly monitored stream.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_stream_retention_window_seconds", "Time span between the first and last retained stream IDs.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_stream_entries_added_total", "Entries ever added as reported by XINFO STREAM.", "counter")
	writePrometheusHeader(&builder, "redisstreamscope_stream_removed_entries_estimate", "Entries added minus current length; includes trims and explicit deletions.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_stream_potentially_unbounded", "Whether repeated growth without observed removal suggests retention risk.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_consumer_group_lag", "Undelivered Redis Stream entries for a monitored consumer group.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_consumer_group_pending", "Pending entries in the consumer group PEL.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_consumer_group_oldest_pending_idle_seconds", "Longest time since delivery in the bounded pending-entry sample.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_consumer_group_oldest_pending_id_age_seconds", "ID-derived publish age in the bounded pending-entry sample; explicit custom IDs may not represent wall-clock time.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_consumer_group_max_delivery_count_sampled", "Maximum delivery count in the bounded pending-entry sample.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_consumer_group_poison_messages_sampled", "Pending entries at or above the poison delivery threshold in the bounded sample.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_consumer_group_net_drain_rate", "Observed net decrease of lag plus pending entries per second.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_consumer_group_drain_eta_seconds", "Estimated time to drain lag plus pending at the observed net drain rate.", "gauge")

	now := m.now().UTC()
	for _, snapshot := range m.Snapshots() {
		connectionLabels := prometheusLabels(map[string]string{"connection": snapshot.ConnectionID})
		fresh := 0
		if snapshot.Collector.Fresh {
			fresh = 1
		}
		fmt.Fprintf(&builder, "redisstreamscope_collector_fresh%s %d\n", connectionLabels, fresh)
		if snapshot.Collector.LastSuccessAt != nil {
			fmt.Fprintf(&builder, "redisstreamscope_collector_last_success_timestamp_seconds%s %.3f\n", connectionLabels, float64(snapshot.Collector.LastSuccessAt.UnixMilli())/1000)
		}
		fmt.Fprintf(&builder, "redisstreamscope_collector_duration_seconds%s %.6f\n", connectionLabels, snapshot.Collector.DurationMs/1000)
		fmt.Fprintf(&builder, "redisstreamscope_collector_consecutive_failures%s %d\n", connectionLabels, snapshot.Collector.ConsecutiveFailures)
		up := 0
		if snapshot.Up {
			up = 1
		}
		fmt.Fprintf(&builder, "redisstreamscope_redis_up%s %d\n", connectionLabels, up)
		if snapshot.Up {
			if snapshot.PingLatencyMs != nil {
				fmt.Fprintf(&builder, "redisstreamscope_redis_ping_duration_seconds%s %.6f\n", connectionLabels, *snapshot.PingLatencyMs/1000)
			}
			fmt.Fprintf(&builder, "redisstreamscope_redis_memory_used_bytes%s %d\n", connectionLabels, snapshot.Memory.UsedBytes)
			fmt.Fprintf(&builder, "redisstreamscope_redis_memory_max_bytes%s %d\n", connectionLabels, snapshot.Memory.MaxBytes)
		}
		clusterEnabled := 0
		if snapshot.Cluster.Enabled && snapshot.Up {
			clusterEnabled = 1
		}
		fmt.Fprintf(&builder, "redisstreamscope_redis_cluster_enabled%s %d\n", connectionLabels, clusterEnabled)
		if snapshot.Cluster.Enabled {
			clusterOK := 0
			if strings.EqualFold(snapshot.Cluster.State, "ok") {
				clusterOK = 1
			}
			fmt.Fprintf(&builder, "redisstreamscope_redis_cluster_state_ok%s %d\n", connectionLabels, clusterOK)
			fmt.Fprintf(&builder, "redisstreamscope_redis_cluster_slots_assigned%s %d\n", connectionLabels, snapshot.Cluster.SlotsAssigned)
			fmt.Fprintf(&builder, "redisstreamscope_redis_cluster_slots_ok%s %d\n", connectionLabels, snapshot.Cluster.SlotsOK)
			fmt.Fprintf(&builder, "redisstreamscope_redis_cluster_slots_pfail%s %d\n", connectionLabels, snapshot.Cluster.SlotsPFail)
			fmt.Fprintf(&builder, "redisstreamscope_redis_cluster_slots_fail%s %d\n", connectionLabels, snapshot.Cluster.SlotsFail)
			fmt.Fprintf(&builder, "redisstreamscope_redis_cluster_known_nodes%s %d\n", connectionLabels, snapshot.Cluster.KnownNodes)
			fmt.Fprintf(&builder, "redisstreamscope_redis_cluster_size%s %d\n", connectionLabels, snapshot.Cluster.ClusterSize)
		}
		for _, node := range snapshot.Nodes {
			nodeID := firstNonEmpty(node.ID, node.Address)
			nodeLabels := prometheusLabels(map[string]string{
				"connection": snapshot.ConnectionID,
				"node":       nodeID,
				"role":       node.Role,
			})
			nodeUp := 0
			if node.Up {
				nodeUp = 1
			}
			fmt.Fprintf(&builder, "redisstreamscope_redis_node_up%s %d\n", nodeLabels, nodeUp)
			if !node.Up {
				continue
			}
			fmt.Fprintf(&builder, "redisstreamscope_redis_node_uptime_seconds%s %d\n", nodeLabels, node.UptimeSeconds)
			fmt.Fprintf(&builder, "redisstreamscope_redis_node_memory_used_bytes%s %d\n", nodeLabels, node.UsedMemoryBytes)
			fmt.Fprintf(&builder, "redisstreamscope_redis_node_memory_max_bytes%s %d\n", nodeLabels, node.MaxMemoryBytes)
			fmt.Fprintf(&builder, "redisstreamscope_redis_node_total_system_memory_bytes%s %d\n", nodeLabels, node.TotalSystemMemoryBytes)
			fmt.Fprintf(&builder, "redisstreamscope_redis_node_memory_fragmentation_ratio%s %.6f\n", nodeLabels, node.MemoryFragmentation)
			fmt.Fprintf(&builder, "redisstreamscope_redis_node_connected_clients%s %d\n", nodeLabels, node.ConnectedClients)
			fmt.Fprintf(&builder, "redisstreamscope_redis_node_blocked_clients%s %d\n", nodeLabels, node.BlockedClients)
			fmt.Fprintf(&builder, "redisstreamscope_redis_node_operations_per_second%s %d\n", nodeLabels, node.OperationsPerSecond)
			fmt.Fprintf(&builder, "redisstreamscope_redis_node_evicted_keys_total%s %d\n", nodeLabels, node.EvictedKeys)
			fmt.Fprintf(&builder, "redisstreamscope_redis_node_rejected_connections_total%s %d\n", nodeLabels, node.RejectedConnections)
			fmt.Fprintf(&builder, "redisstreamscope_redis_node_loading%s %d\n", nodeLabels, boolInt(node.Loading))
			if node.RDBLastSaveStatus != "" {
				fmt.Fprintf(&builder, "redisstreamscope_redis_node_rdb_last_bgsave_success%s %d\n", nodeLabels, prometheusStatusOK(node.RDBLastSaveStatus))
			}
			if node.AOFLastRewriteStatus != "" {
				fmt.Fprintf(&builder, "redisstreamscope_redis_node_aof_last_bgrewrite_success%s %d\n", nodeLabels, prometheusStatusOK(node.AOFLastRewriteStatus))
			}
			fmt.Fprintf(&builder, "redisstreamscope_redis_node_connected_replicas%s %d\n", nodeLabels, node.ConnectedReplicas)
			fmt.Fprintf(&builder, "redisstreamscope_redis_node_replication_offset%s %d\n", nodeLabels, node.ReplicationOffset)
			if node.MasterLinkStatus != "" {
				fmt.Fprintf(&builder, "redisstreamscope_redis_node_replication_link_up%s %d\n", nodeLabels, prometheusStatusOK(node.MasterLinkStatus))
			}
			fmt.Fprintf(&builder, "redisstreamscope_redis_node_full_resyncs_total%s %d\n", nodeLabels, node.FullResyncs)
			fmt.Fprintf(&builder, "redisstreamscope_redis_node_partial_resync_errors_total%s %d\n", nodeLabels, node.PartialResyncErrors)
		}
		streamMetricsFresh := m.streamMetricsFresh(snapshot, now)
		fmt.Fprintf(&builder, "redisstreamscope_stream_metrics_fresh%s %d\n", connectionLabels, boolInt(streamMetricsFresh))
		if !streamMetricsFresh {
			continue
		}
		for _, stream := range snapshot.Streams {
			if !stream.Available {
				continue
			}
			labels := prometheusLabels(map[string]string{"connection": snapshot.ConnectionID, "stream": stream.Key})
			fmt.Fprintf(&builder, "redisstreamscope_stream_length%s %d\n", labels, stream.Length)
			fmt.Fprintf(&builder, "redisstreamscope_stream_entries_added_total%s %d\n", labels, stream.EntriesAdded)
			fmt.Fprintf(&builder, "redisstreamscope_stream_removed_entries_estimate%s %d\n", labels, stream.RemovedEntriesEstimate)
			risk := 0
			if stream.PotentiallyUnbounded {
				risk = 1
			}
			fmt.Fprintf(&builder, "redisstreamscope_stream_potentially_unbounded%s %d\n", labels, risk)
			if stream.MemoryBytes != nil {
				fmt.Fprintf(&builder, "redisstreamscope_stream_memory_bytes%s %d\n", labels, *stream.MemoryBytes)
			}
			if stream.RetentionWindowMs != nil {
				fmt.Fprintf(&builder, "redisstreamscope_stream_retention_window_seconds%s %.3f\n", labels, float64(*stream.RetentionWindowMs)/1000)
			}
			for _, group := range stream.Groups {
				groupLabels := prometheusLabels(map[string]string{"connection": snapshot.ConnectionID, "stream": stream.Key, "group": group.Name})
				if group.Lag != nil {
					fmt.Fprintf(&builder, "redisstreamscope_consumer_group_lag%s %d\n", groupLabels, *group.Lag)
				}
				fmt.Fprintf(&builder, "redisstreamscope_consumer_group_pending%s %d\n", groupLabels, group.Pending)
				if group.PendingSample.OldestPendingIdleMs != nil {
					fmt.Fprintf(&builder, "redisstreamscope_consumer_group_oldest_pending_idle_seconds%s %.3f\n", groupLabels, float64(*group.PendingSample.OldestPendingIdleMs)/1000)
				}
				if group.PendingSample.OldestPendingIDAgeMs != nil {
					fmt.Fprintf(&builder, "redisstreamscope_consumer_group_oldest_pending_id_age_seconds%s %.3f\n", groupLabels, float64(*group.PendingSample.OldestPendingIDAgeMs)/1000)
				}
				fmt.Fprintf(&builder, "redisstreamscope_consumer_group_max_delivery_count_sampled%s %d\n", groupLabels, group.PendingSample.MaxDeliveryCount)
				fmt.Fprintf(&builder, "redisstreamscope_consumer_group_poison_messages_sampled%s %d\n", groupLabels, group.PendingSample.PoisonMessagesSampled)
				if group.NetDrainRate != nil {
					fmt.Fprintf(&builder, "redisstreamscope_consumer_group_net_drain_rate%s %.6f\n", groupLabels, *group.NetDrainRate)
				}
				if group.DrainETASeconds != nil {
					fmt.Fprintf(&builder, "redisstreamscope_consumer_group_drain_eta_seconds%s %.6f\n", groupLabels, *group.DrainETASeconds)
				}
			}
		}
	}
	return builder.String()
}

func (m *operationalMonitor) streamMetricsFresh(snapshot operationalSnapshot, now time.Time) bool {
	if !snapshot.Up || !snapshot.Collector.Fresh || snapshot.StreamSampledAt == nil {
		return false
	}
	age := now.Sub(snapshot.StreamSampledAt.UTC())
	if age < 0 {
		return false
	}
	interval := m.streamInterval
	if interval <= 0 {
		interval = operationalStreamInterval
	}
	return age <= 2*interval
}

func (s *apiServer) prometheusMetrics(writer http.ResponseWriter, request *http.Request) {
	token := strings.TrimSpace(s.config.MetricsToken)
	if token == "" {
		http.NotFound(writer, request)
		return
	}
	provided := strings.TrimSpace(strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer "))
	if len(provided) != len(token) || subtle.ConstantTimeCompare([]byte(provided), []byte(token)) != 1 {
		writer.Header().Set("WWW-Authenticate", `Bearer realm="metrics"`)
		writeError(writer, http.StatusUnauthorized, "metrics_authentication_required", "A valid metrics bearer token is required.")
		return
	}
	output := s.operationalMonitor().RenderPrometheus()
	if s.store != nil {
		stored, err := s.store.renderPrometheusStoredMetrics(request.Context(), time.Now().UTC())
		if err == nil {
			output += stored
		} else {
			var status strings.Builder
			writePrometheusHeader(&status, "redisstreamscope_stored_metrics_collection_success", "Whether lifecycle and alert metrics were read successfully from the embedded database.", "gauge")
			status.WriteString("redisstreamscope_stored_metrics_collection_success 0\n")
			output += status.String()
		}
	}
	writer.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write([]byte(output))
}

func prometheusStatusOK(value string) int {
	if strings.EqualFold(strings.TrimSpace(value), "ok") || strings.EqualFold(strings.TrimSpace(value), "up") {
		return 1
	}
	return 0
}

func writePrometheusHeader(builder *strings.Builder, name, help, metricType string) {
	fmt.Fprintf(builder, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, metricType)
}

func prometheusLabels(values map[string]string) string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		value := strings.NewReplacer("\\", "\\\\", "\n", "\\n", "\"", "\\\"").Replace(values[key])
		parts = append(parts, key+"=\""+value+"\"")
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return "unknown"
}

func maxInt64(left, right int64) int64 {
	if left > right {
		return left
	}
	return right
}

func appendUnique(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func timePointer(value time.Time) *time.Time { return &value }
func int64Pointer(value int64) *int64        { return &value }
func float64Pointer(value float64) *float64  { return &value }
