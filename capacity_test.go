package main

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestRobustTrendIgnoresAnIsolatedOutlier(t *testing.T) {
	base := time.Date(2026, 8, 25, 10, 0, 0, 0, time.UTC)
	points := make([]trendPoint, 0, 13)
	for index := 0; index < 13; index++ {
		value := float64(100 + index*60) // one item per second
		if index == 6 {
			value += 100_000
		}
		points = append(points, trendPoint{At: base.Add(time.Duration(index) * time.Minute), Value: value})
	}
	trend := robustTrendFor(points)
	if trend.Slope == nil || math.Abs(*trend.Slope-1) > 0.001 {
		t.Fatalf("unexpected robust slope: %#v", trend)
	}
	if trend.Confidence <= 0.5 {
		t.Fatalf("expected useful confidence despite one outlier: %#v", trend)
	}
}

func TestRobustTrendRequiresEnoughHistory(t *testing.T) {
	base := time.Now().UTC()
	trend := robustTrendFor([]trendPoint{
		{At: base, Value: 1},
		{At: base.Add(time.Minute), Value: 2},
		{At: base.Add(2 * time.Minute), Value: 3},
	})
	if trend.Slope != nil || trend.Confidence != 0 {
		t.Fatalf("short history produced a forecast: %#v", trend)
	}
}

func TestCapacityForecastAndRetentionPolicyCompliance(t *testing.T) {
	dataStore := openMonitoringHistoryTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 8, 25, 10, 0, 0, 0, time.UTC)
	maxLength := int64(700)
	maxAge := int64(120)
	policy, err := dataStore.upsertRetentionPolicy(ctx, retentionPolicy{
		ConnectionID: "redis", StreamKey: "orders", MaxLength: &maxLength,
		MaxAgeSeconds: &maxAge, RequireTrimming: true, UpdatedBy: "admin",
	})
	if err != nil {
		t.Fatal(err)
	}
	if policy.MaxLength == nil || *policy.MaxLength != maxLength {
		t.Fatalf("policy was not persisted: %#v", policy)
	}
	for index := 0; index < 13; index++ {
		at := base.Add(time.Duration(index) * time.Minute)
		length := int64(100 + index*50)
		memory := int64(1000 + index*600)
		retention := int64(180_000)
		snapshot := historySnapshot(at, nil)
		snapshot.Memory = redisMemoryHealth{UsedBytes: 10_000 + int64(index)*600, MaxBytes: 100_000}
		snapshot.Streams[0].Length = length
		snapshot.Streams[0].MemoryBytes = &memory
		snapshot.Streams[0].EntriesAdded = int64(100 + index*50)
		snapshot.Streams[0].RemovedEntriesEstimate = 0
		snapshot.Streams[0].RetentionWindowMs = &retention
		if err := dataStore.recordCapacitySample(ctx, snapshot); err != nil {
			t.Fatal(err)
		}
	}
	result, err := dataStore.capacityForecast(ctx, "redis", base.Add(-time.Second), base.Add(13*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if result.Connection.TimeToMaxMemorySeconds == nil || *result.Connection.TimeToMaxMemorySeconds <= 0 {
		t.Fatalf("expected a meaningful time-to-maxmemory: %#v", result.Connection)
	}
	if len(result.Streams) != 1 {
		t.Fatalf("unexpected stream forecasts: %#v", result.Streams)
	}
	stream := result.Streams[0]
	if stream.Length.GrowthPerSecond == nil || *stream.Length.GrowthPerSecond <= 0 {
		t.Fatalf("missing length growth: %#v", stream.Length)
	}
	if stream.Compliance.Status != "violating" {
		t.Fatalf("expected policy violation, got %#v", stream.Compliance)
	}
	wantedReasons := map[string]bool{
		"observed_age_exceeds_expected_max":   false,
		"no_trim_observed_while_entries_grew": false,
	}
	for _, reason := range stream.Compliance.Reasons {
		if _, exists := wantedReasons[reason]; exists {
			wantedReasons[reason] = true
		}
	}
	for reason, found := range wantedReasons {
		if !found {
			t.Errorf("missing compliance reason %q in %#v", reason, stream.Compliance)
		}
	}
}

func TestCapacityDoesNotEstimateTimeToUnlimitedOrShrinkingMemory(t *testing.T) {
	for _, test := range []struct {
		name      string
		maxMemory int64
		delta     int64
	}{
		{name: "unlimited", maxMemory: 0, delta: 100},
		{name: "shrinking", maxMemory: 100_000, delta: -100},
	} {
		t.Run(test.name, func(t *testing.T) {
			dataStore := openMonitoringHistoryTestStore(t)
			ctx := context.Background()
			base := time.Date(2026, 8, 25, 11, 0, 0, 0, time.UTC)
			for index := 0; index < 13; index++ {
				at := base.Add(time.Duration(index) * time.Minute)
				snapshot := historySnapshot(at, nil)
				snapshot.Memory = redisMemoryHealth{UsedBytes: 10_000 + int64(index)*test.delta, MaxBytes: test.maxMemory}
				if err := dataStore.recordCapacitySample(ctx, snapshot); err != nil {
					t.Fatal(err)
				}
			}
			result, err := dataStore.capacityForecast(ctx, "redis", base.Add(-time.Second), base.Add(13*time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			if result.Connection.TimeToMaxMemorySeconds != nil {
				t.Fatalf("unsafe time-to-maxmemory was reported: %#v", result.Connection)
			}
		})
	}
}

func TestRetentionPolicyValidationAndDeletion(t *testing.T) {
	dataStore := openMonitoringHistoryTestStore(t)
	ctx := context.Background()
	if _, err := dataStore.upsertRetentionPolicy(ctx, retentionPolicy{ConnectionID: "redis", StreamKey: "orders"}); err == nil {
		t.Fatal("empty expectation was accepted")
	}
	maxLength := int64(100)
	if _, err := dataStore.upsertRetentionPolicy(ctx, retentionPolicy{ConnectionID: "redis", StreamKey: "orders", MaxLength: &maxLength}); err != nil {
		t.Fatal(err)
	}
	deleted, err := dataStore.deleteRetentionPolicy(ctx, "redis", "orders")
	if err != nil || !deleted {
		t.Fatalf("delete policy: deleted=%v err=%v", deleted, err)
	}
	if _, err := dataStore.retentionPolicy(ctx, "redis", "orders"); err == nil {
		t.Fatal("deleted policy still exists")
	}
}
