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
