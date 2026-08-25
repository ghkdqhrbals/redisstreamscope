package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func openMonitoringHistoryTestStore(t *testing.T) *store {
	t.Helper()
	value, err := openStore(appConfig{DataPath: filepath.Join(t.TempDir(), "redisstreamscope.db"), SessionTTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		monitoringHistoryStates.Delete(value)
		capacitySchemaStates.Delete(value)
		_ = value.close()
	})
	return value
}

func historySnapshot(at time.Time, consumers []consumerOperationalHealth) operationalSnapshot {
	lag := int64(3)
	return operationalSnapshot{
		ConnectionID: "redis", Mode: "standalone", CollectedAt: at, NodeSampledAt: timePointer(at),
		StreamSampledAt: timePointer(at), MemorySampledAt: timePointer(at), Up: true,
		Cluster: redisClusterHealth{}, Memory: redisMemoryHealth{UsedBytes: 1000, MaxBytes: 10_000},
		Nodes: []redisNodeHealth{{ID: "run-a", Address: "redis:6379", Up: true, Role: "master", RDBLastSaveStatus: "ok"}},
		Streams: []streamOperationalHealth{{
			Key: "orders", Available: true, Length: 10, EntriesAdded: 10,
			Groups: []consumerGroupOperationalHealth{{
				Name: "workers", Consumers: int64(len(consumers)), Pending: 1, Lag: &lag,
				LastDeliveredID: "1-0", ConsumerSampleStatus: "sampled", ConsumerStates: consumers,
			}},
		}},
	}
}

func TestConsumerHistoryDetectsJoinStallRecoveryAndLeave(t *testing.T) {
	dataStore := openMonitoringHistoryTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 8, 25, 10, 0, 0, 0, time.UTC)

	if err := dataStore.recordConsumerHistory(ctx, historySnapshot(base, []consumerOperationalHealth{{Name: "worker-1"}})); err != nil {
		t.Fatal(err)
	}
	if err := dataStore.recordConsumerHistory(ctx, historySnapshot(base.Add(20*time.Second), []consumerOperationalHealth{{
		Name: "worker-1", Pending: 1, IdleMs: 61_000, InactiveMs: 61_000,
	}})); err != nil {
		t.Fatal(err)
	}
	if err := dataStore.recordConsumerHistory(ctx, historySnapshot(base.Add(40*time.Second), []consumerOperationalHealth{{Name: "worker-1"}})); err != nil {
		t.Fatal(err)
	}
	if err := dataStore.recordConsumerHistory(ctx, historySnapshot(base.Add(5*time.Minute+time.Second), []consumerOperationalHealth{})); err != nil {
		t.Fatal(err)
	}

	result, err := dataStore.queryConsumerHistory(ctx, "redis", "orders", "workers",
		base.Add(-time.Second), base.Add(6*time.Minute), 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Current) != 1 || len(result.Current[0].Consumers) != 0 {
		t.Fatalf("unexpected current consumer state: %#v", result.Current)
	}
	if len(result.Samples) != 2 {
		t.Fatalf("expected minute-bounded group history samples, got %d: %#v", len(result.Samples), result.Samples)
	}
	wanted := map[string]bool{
		"group_joined": false, "consumer_joined": false, "consumer_stalled": false,
		"consumer_recovered": false, "consumer_left": false, "group_stalled": false, "group_recovered": false,
	}
	for _, event := range result.Events {
		if _, exists := wanted[event.Type]; exists {
			wanted[event.Type] = true
		}
	}
	for eventType, found := range wanted {
		if !found {
			t.Errorf("missing %s event in %#v", eventType, result.Events)
		}
	}
	firstPage, cursor, err := dataStore.queryMonitoringEvents(ctx, "redis", []string{"consumer"}, "orders", "workers",
		base.Add(-time.Second), base.Add(6*time.Minute), 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(firstPage) != 2 || cursor == 0 {
		t.Fatalf("expected a bounded cursor page: items=%#v cursor=%d", firstPage, cursor)
	}
	secondPage, _, err := dataStore.queryMonitoringEvents(ctx, "redis", []string{"consumer"}, "orders", "workers",
		base.Add(-time.Second), base.Add(6*time.Minute), cursor, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(secondPage) == 0 || firstPage[len(firstPage)-1].ID == secondPage[0].ID {
		t.Fatalf("cursor pages overlap or lost the remaining events: first=%#v second=%#v", firstPage, secondPage)
	}
}

func TestConsumerHistoryDoesNotInferLeavesFromTruncatedSamples(t *testing.T) {
	dataStore := openMonitoringHistoryTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 8, 25, 11, 0, 0, 0, time.UTC)
	if err := dataStore.recordConsumerHistory(ctx, historySnapshot(base, []consumerOperationalHealth{{Name: "worker-1"}, {Name: "worker-2"}})); err != nil {
		t.Fatal(err)
	}
	next := historySnapshot(base.Add(time.Minute), []consumerOperationalHealth{{Name: "worker-1"}})
	next.Streams[0].Groups[0].ConsumerSampleStatus = "truncated"
	if err := dataStore.recordConsumerHistory(ctx, next); err != nil {
		t.Fatal(err)
	}
	result, err := dataStore.queryConsumerHistory(ctx, "redis", "orders", "workers", base.Add(-time.Second), base.Add(2*time.Minute), 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Current) != 1 || len(result.Current[0].Consumers) != 2 {
		t.Fatalf("truncated sample removed an unseen consumer: %#v", result.Current)
	}
	for _, event := range result.Events {
		if event.Type == "consumer_left" {
			t.Fatalf("truncated sample emitted a false leave event: %#v", event)
		}
	}
}

func TestTopologyHistoryDetectsFailoverAndPersistenceTransitions(t *testing.T) {
	dataStore := openMonitoringHistoryTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	first := historySnapshot(base, nil)
	first.Mode = "cluster"
	first.Cluster = redisClusterHealth{Enabled: true, State: "ok", SlotsAssigned: 16384, SlotsOK: 16384}
	first.Nodes = []redisNodeHealth{{
		ID: "run-a", Address: "10.0.0.1:6379", Up: true, Role: "replica",
		MasterLinkStatus: "up", RDBLastSaveStatus: "ok", AOFLastRewriteStatus: "ok",
	}}
	if err := dataStore.recordTopologyHistory(ctx, first); err != nil {
		t.Fatal(err)
	}
	second := first
	second.NodeSampledAt = timePointer(base.Add(30 * time.Second))
	second.CollectedAt = base.Add(30 * time.Second)
	second.Nodes = []redisNodeHealth{{
		ID: "run-a", Address: "10.0.0.1:6379", Up: true, Role: "master",
		RDBLastSaveStatus: "err", AOFLastRewriteStatus: "ok",
	}}
	if err := dataStore.recordTopologyHistory(ctx, second); err != nil {
		t.Fatal(err)
	}
	third := second
	third.NodeSampledAt = timePointer(base.Add(61 * time.Second))
	third.CollectedAt = base.Add(61 * time.Second)
	if err := dataStore.recordTopologyHistory(ctx, third); err != nil {
		t.Fatal(err)
	}
	down := third
	down.NodeSampledAt = timePointer(base.Add(90 * time.Second))
	down.CollectedAt = base.Add(90 * time.Second)
	down.Up = false
	down.Error = "dial timeout"
	down.Nodes = nil
	if err := dataStore.recordTopologyHistory(ctx, down); err != nil {
		t.Fatal(err)
	}

	model, exists, err := loadCurrentTopology(ctx, dataStore.db, "redis")
	if err != nil || !exists || len(model.Nodes) != 1 || model.Nodes[0].Role != "master" {
		t.Fatalf("unexpected current topology: model=%#v exists=%v err=%v", model, exists, err)
	}
	events, _, err := dataStore.queryMonitoringEvents(ctx, "redis", []string{"topology", "replication", "persistence"}, "", "", base.Add(-time.Second), base.Add(2*time.Minute), 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	wanted := map[string]bool{"failover_promoted": false, "rdb_status_changed": false, "master_link_changed": false, "node_down": false}
	for _, event := range events {
		if event.Type == "node_left" {
			t.Fatalf("connection outage was misreported as node leave: %#v", events)
		}
		if _, exists := wanted[event.Type]; exists {
			wanted[event.Type] = true
		}
	}
	for eventType, found := range wanted {
		if !found {
			t.Errorf("missing %s in %#v", eventType, events)
		}
	}
	var topologySamples int
	if err := dataStore.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM redis_topology_history WHERE connection_id='redis'`).Scan(&topologySamples); err != nil {
		t.Fatal(err)
	}
	if topologySamples != 2 {
		t.Fatalf("expected minute-bounded topology samples, got %d", topologySamples)
	}
}

func TestMonitoringHistoryRetentionRemovesOldRows(t *testing.T) {
	dataStore := openMonitoringHistoryTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 8, 25, 13, 0, 0, 0, time.UTC)
	old := historySnapshot(now.Add(-40*24*time.Hour), []consumerOperationalHealth{{Name: "old"}})
	if err := dataStore.recordOperationalSnapshot(ctx, old); err != nil {
		t.Fatal(err)
	}
	recent := historySnapshot(now, []consumerOperationalHealth{{Name: "new"}})
	if err := dataStore.recordOperationalSnapshot(ctx, recent); err != nil {
		t.Fatal(err)
	}
	if err := dataStore.maintainMonitoringHistory(ctx, now); err != nil {
		t.Fatal(err)
	}
	for _, check := range []struct {
		table string
		want  int
	}{
		{"consumer_group_history_samples", 1},
		{"consumer_history_samples", 1},
		{"redis_topology_history", 1},
		{"stream_capacity_samples", 1},
		{"connection_capacity_samples", 1},
	} {
		var count int
		if err := dataStore.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+check.table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != check.want {
			t.Errorf("%s count=%d, want %d", check.table, count, check.want)
		}
	}
}
