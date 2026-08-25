package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestObservedConsumeDelayOnlyChangesWhenDeliveryAdvances(t *testing.T) {
	now := time.UnixMilli(1_725_000_010_000).UTC()
	if value := observedConsumeDelay(now, "1725000000000-0", streamMetricState{}); value != nil {
		t.Fatalf("the first observation must not invent a consume timestamp: %v", *value)
	}
	previousDelay := int64(250)
	previous := streamMetricState{LastDeliveredID: "1725000000000-0", ConsumeDelayMs: &previousDelay}
	if value := observedConsumeDelay(now, "1725000000000-0", previous); value == nil || *value != previousDelay {
		t.Fatalf("unchanged delivery must keep the last measurement, got %v", value)
	}
	value := observedConsumeDelay(now, "1725000009000-0", previous)
	if value == nil || *value != 1000 {
		t.Fatalf("advanced delivery delay=%v, want 1000ms", value)
	}
}

func TestConsumerGroupMetricRates(t *testing.T) {
	previous := consumerGroupMetricState{
		RecordedAt:    time.Unix(100, 0),
		ConsumedTotal: 20,
		Lag:           8,
		LagKnown:      true,
	}
	consumeRate, lagDelta := consumerGroupMetricRates(time.Unix(102, 0), previous, 26, 4, true, true)
	if consumeRate == nil || *consumeRate != 3 {
		t.Fatalf("consume rate=%v, want 3", consumeRate)
	}
	if lagDelta == nil || *lagDelta != -2 {
		t.Fatalf("lag delta=%v, want -2", lagDelta)
	}
}

func TestConsumerGroupMetricSamplesAggregateByGroup(t *testing.T) {
	config := appConfig{DataPath: filepath.Join(t.TempDir(), "redisstreamscope.db"), SessionTTL: time.Hour}
	store, err := openStore(config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	ctx := context.Background()
	first := time.Date(2026, 7, 31, 2, 0, 0, 0, time.UTC)
	second := first.Add(time.Second)
	delayA := int64(100)
	delayB := int64(300)
	rateA := 2.0
	rateB := 1.0
	samples := []consumerGroupMetricSample{
		{RecordedAt: first, ConnectionID: "redis", StreamKey: "orders", GroupName: "workers-a", ConsumerCount: 2, Pending: 1, Lag: 5, LagKnown: true, LastDeliveredID: "1-0", ConsumeDelayMs: &delayA, ConsumedTotal: 10, ConsumeRate: &rateA},
		{RecordedAt: first, ConnectionID: "redis", StreamKey: "orders", GroupName: "workers-b", ConsumerCount: 1, Pending: 0, Lag: 7, LagKnown: true, LastDeliveredID: "1-0", ConsumeDelayMs: &delayB, ConsumedTotal: 8, ConsumeRate: &rateB},
		{RecordedAt: second, ConnectionID: "redis", StreamKey: "orders", GroupName: "workers-a", ConsumerCount: 2, Pending: 0, Lag: 3, LagKnown: true, LastDeliveredID: "2-0", ConsumeDelayMs: &delayA, ConsumedTotal: 12, ConsumeRate: &rateA},
	}
	if err := store.writeConsumerGroupMetricSamples(ctx, samples); err != nil {
		t.Fatal(err)
	}
	groups, points, err := store.listConsumerGroupMetricSeries(ctx, "redis", "orders", first.Add(-time.Second), second.Add(time.Second), 600)
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 2 || groups[0] != "workers-a" || groups[1] != "workers-b" {
		t.Fatalf("groups=%v", groups)
	}
	if len(points) != 2 {
		t.Fatalf("points=%d, want 2", len(points))
	}
	firstA := points[0].Values["workers-a"]
	firstB := points[0].Values["workers-b"]
	if firstA.Lag == nil || *firstA.Lag != 5 || firstB.ConsumeDelayMs == nil || *firstB.ConsumeDelayMs != 300 {
		t.Fatalf("unexpected first point: %+v", points[0])
	}
}

func TestLatestConsumerGroupMetricSnapshotsUseLatestFreshSample(t *testing.T) {
	config := appConfig{DataPath: filepath.Join(t.TempDir(), "redisstreamscope.db"), SessionTTL: time.Hour}
	store, err := openStore(config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()

	ctx := context.Background()
	first := time.Date(2026, 8, 9, 2, 0, 0, 0, time.UTC)
	second := first.Add(time.Second)
	delay := int64(425)
	activeRate := 3.0
	idleRate := 0.0
	lagDelta := -2.0
	samples := []consumerGroupMetricSample{
		{RecordedAt: first, ConnectionID: "redis", StreamKey: "orders", GroupName: "workers-a", ConsumerCount: 2, Pending: 4, Lag: 9, LagKnown: true, LastDeliveredID: "1000-0", ConsumeDelayMs: &delay, ConsumedTotal: 10, ConsumeRate: &activeRate},
		{RecordedAt: second, ConnectionID: "redis", StreamKey: "orders", GroupName: "workers-a", ConsumerCount: 3, Pending: 1, Lag: 5, LagKnown: true, LastDeliveredID: "2000-0", ConsumeDelayMs: &delay, ConsumedTotal: 13, ConsumeRate: &idleRate, LagDelta: &lagDelta},
		{RecordedAt: second, ConnectionID: "redis", StreamKey: "orders", GroupName: "workers-unknown", ConsumerCount: 1, Pending: 2, Lag: 0, LagKnown: false, LastDeliveredID: "2000-0", ConsumedTotal: 2, ConsumeRate: &idleRate},
		{RecordedAt: second, ConnectionID: "other", StreamKey: "orders", GroupName: "workers-a", ConsumerCount: 9, Pending: 9, Lag: 9, LagKnown: true, LastDeliveredID: "2000-0", ConsumedTotal: 9, ConsumeRate: &activeRate},
	}
	if err := store.writeConsumerGroupMetricSamples(ctx, samples); err != nil {
		t.Fatal(err)
	}

	items, err := store.listLatestConsumerGroupMetricSnapshots(ctx, "redis", first.Add(-time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("items=%d, want 2: %+v", len(items), items)
	}
	workers := items[0]
	if workers.GroupName != "workers-a" || workers.ConsumerCount != 3 || workers.Pending != 1 || workers.Lag == nil || *workers.Lag != 5 {
		t.Fatalf("latest workers snapshot=%+v", workers)
	}
	if workers.ConsumeDelayMs == nil || *workers.ConsumeDelayMs != delay || workers.LagDelta == nil || *workers.LagDelta != lagDelta {
		t.Fatalf("latest workers metrics=%+v", workers)
	}
	if workers.LastActivityAt == nil || !workers.LastActivityAt.Equal(first) {
		t.Fatalf("last activity=%v, want %v", workers.LastActivityAt, first)
	}
	if items[1].Lag != nil {
		t.Fatalf("unknown lag must remain null: %+v", items[1])
	}

	stale, err := store.listLatestConsumerGroupMetricSnapshots(ctx, "redis", second.Add(time.Second))
	if err != nil || len(stale) != 0 {
		t.Fatalf("stale snapshots=%+v err=%v", stale, err)
	}
}

func TestLatestConsumerGroupMetricSnapshotsKeepRollupActivity(t *testing.T) {
	config := appConfig{DataPath: filepath.Join(t.TempDir(), "redisstreamscope.db"), SessionTTL: time.Hour}
	store, err := openStore(config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()

	ctx := context.Background()
	first := time.Date(2026, 8, 9, 2, 0, 0, 0, time.UTC)
	activeRate := 1.0
	idleRate := 0.0
	if err := store.writeConsumerGroupMetricSamples(ctx, []consumerGroupMetricSample{{
		RecordedAt: first, ConnectionID: "redis", StreamKey: "orders", GroupName: "workers",
		ConsumerCount: 1, Lag: 1, LagKnown: true, LastDeliveredID: "1000-0", ConsumedTotal: 1, ConsumeRate: &activeRate,
	}}); err != nil {
		t.Fatal(err)
	}
	if err := store.maintainMetricSamples(ctx, first.Add(61*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := store.maintainMetricSamples(ctx, first.Add(64*time.Minute)); err != nil {
		t.Fatal(err)
	}
	current := first.Add(65 * time.Minute)
	if err := store.writeConsumerGroupMetricSamples(ctx, []consumerGroupMetricSample{{
		RecordedAt: current, ConnectionID: "redis", StreamKey: "orders", GroupName: "workers",
		ConsumerCount: 1, Lag: 0, LagKnown: true, LastDeliveredID: "2000-0", ConsumedTotal: 1, ConsumeRate: &idleRate,
	}}); err != nil {
		t.Fatal(err)
	}

	items, err := store.listLatestConsumerGroupMetricSnapshots(ctx, "redis", current.Add(-time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].LastActivityAt == nil || !items[0].LastActivityAt.Equal(first) {
		t.Fatalf("rollup activity snapshot=%+v", items)
	}
}

func TestMetricSamplesAggregateAndFilterByStream(t *testing.T) {
	config := appConfig{DataPath: filepath.Join(t.TempDir(), "redisstreamscope.db"), SessionTTL: time.Hour}
	store, err := openStore(config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	ctx := context.Background()
	first := time.Date(2026, 7, 30, 2, 0, 0, 0, time.UTC)
	second := first.Add(30 * time.Second)
	delayA := int64(100)
	delayB := int64(300)
	samples := []streamMetricSample{
		{RecordedAt: first, ConnectionID: "redis", StreamKey: "orders", Entries: 10, ConsumerGroups: 1, TotalLag: 3, LagKnown: true, Pending: 1, LastDeliveredID: "1-0", ConsumeDelayMs: &delayA, RedisLatencyMs: 1.2},
		{RecordedAt: first, ConnectionID: "redis", StreamKey: "billing", Entries: 20, ConsumerGroups: 2, TotalLag: 5, LagKnown: true, Pending: 2, LastDeliveredID: "2-0", ConsumeDelayMs: &delayB, RedisLatencyMs: 1.2},
		{RecordedAt: second, ConnectionID: "redis", StreamKey: "orders", Entries: 12, ConsumerGroups: 1, TotalLag: 1, LagKnown: true, Pending: 0, LastDeliveredID: "3-0", ConsumeDelayMs: &delayA, RedisLatencyMs: 0.8},
	}
	if err := store.writeMetricSamples(ctx, samples); err != nil {
		t.Fatal(err)
	}
	items, err := store.listMetricSeries(ctx, "redis", "", first.Add(-time.Second), second.Add(time.Second), 600)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || items[0].Entries != 30 || items[0].ConsumerGroups != 3 || items[0].TotalLag == nil || *items[0].TotalLag != 8 || items[0].Pending != 3 {
		t.Fatalf("unexpected aggregate metrics: %+v", items)
	}
	if items[0].ConsumeDelayMs == nil || *items[0].ConsumeDelayMs != 200 {
		t.Fatalf("consume delay must average monitored streams: %+v", items[0].ConsumeDelayMs)
	}
	orders, err := store.listMetricSeries(ctx, "redis", "orders", first.Add(-time.Second), second.Add(time.Second), 600)
	if err != nil || len(orders) != 2 || orders[0].Entries != 10 {
		t.Fatalf("stream filter: items=%+v err=%v", orders, err)
	}

	if err := store.maintainMetricSamples(ctx, first.Add(61*time.Minute)); err != nil {
		t.Fatal(err)
	}
	rollups, err := store.listMetricSeries(ctx, "redis", "", first.Add(-time.Minute), first.Add(61*time.Minute), 600)
	if err != nil || len(rollups) != 1 || rollups[0].Entries != 31 {
		t.Fatalf("minute rollup: items=%+v err=%v", rollups, err)
	}
}

func TestMetricRatesUseOneSecondDeltas(t *testing.T) {
	recordedAt := time.Date(2026, 7, 30, 2, 0, 1, 0, time.UTC)
	previous := streamMetricState{
		RecordedAt: recordedAt.Add(-time.Second), PublishedTotal: 100,
		ConsumedTotal: 90, TotalLag: 10, LagKnown: true,
	}
	published, consumed, lagDelta := metricRates(recordedAt, previous, 104, 93, 11, true, true)
	if published == nil || *published != 4 || consumed == nil || *consumed != 3 || lagDelta == nil || *lagDelta != 1 {
		t.Fatalf("unexpected one-second rates: published=%v consumed=%v lagDelta=%v", published, consumed, lagDelta)
	}
}
