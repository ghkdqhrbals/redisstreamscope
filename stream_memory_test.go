package main

import (
	"context"
	"errors"
	"testing"

	"github.com/redis/go-redis/v9"
)

type fakeStreamMetadataClient struct {
	types         map[string]string
	lengths       map[string]int64
	groups        map[string][]redis.XInfoGroup
	memory        map[string]int64
	memoryErrors  map[string]error
	memorySamples map[string][]int
}

func (client *fakeStreamMetadataClient) Type(_ context.Context, key string) *redis.StatusCmd {
	value, exists := client.types[key]
	if !exists {
		value = "none"
	}
	return redis.NewStatusResult(value, nil)
}

func (client *fakeStreamMetadataClient) XLen(_ context.Context, key string) *redis.IntCmd {
	value, exists := client.lengths[key]
	if !exists {
		return redis.NewIntResult(0, redis.Nil)
	}
	return redis.NewIntResult(value, nil)
}

func (client *fakeStreamMetadataClient) XInfoGroups(_ context.Context, key string) *redis.XInfoGroupsCmd {
	command := redis.NewXInfoGroupsCmd(context.Background(), key)
	command.SetVal(append([]redis.XInfoGroup(nil), client.groups[key]...))
	return command
}

func (client *fakeStreamMetadataClient) MemoryUsage(_ context.Context, key string, samples ...int) *redis.IntCmd {
	client.memorySamples[key] = append(client.memorySamples[key], samples...)
	if err := client.memoryErrors[key]; err != nil {
		return redis.NewIntResult(0, err)
	}
	value, exists := client.memory[key]
	if !exists {
		return redis.NewIntResult(0, redis.Nil)
	}
	return redis.NewIntResult(value, nil)
}

func newFakeStreamMetadataClient() *fakeStreamMetadataClient {
	return &fakeStreamMetadataClient{
		types:         map[string]string{},
		lengths:       map[string]int64{},
		groups:        map[string][]redis.XInfoGroup{},
		memory:        map[string]int64{},
		memoryErrors:  map[string]error{},
		memorySamples: map[string][]int{},
	}
}

func TestCollectOverviewBatchIncludesBoundedMemoryUsage(t *testing.T) {
	client := newFakeStreamMetadataClient()
	client.lengths["orders"] = 12
	client.lengths["restricted"] = 3
	client.groups["orders"] = []redis.XInfoGroup{{
		Name: "workers", Consumers: 2, Pending: 1, Lag: 4, LastDeliveredID: "1800000000000-0",
	}}
	client.memory["orders"] = 4096
	client.memoryErrors["restricted"] = errors.New("NOPERM this user has no permissions to run the 'memory|usage' command")

	items, err := collectOverviewBatch(context.Background(), client, nil, false, []string{"orders", "restricted"})
	if err != nil {
		t.Fatalf("collect overview batch: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected two streams, got %#v", items)
	}
	if items[0].MemoryBytes == nil || *items[0].MemoryBytes != 4096 {
		t.Fatalf("expected Redis MEMORY USAGE in overview: %#v", items[0])
	}
	if items[0].ConsumerGroups != 1 || items[0].TotalLag != 4 || items[0].Pending != 1 {
		t.Fatalf("memory collection changed overview group metrics: %#v", items[0])
	}
	if items[1].MemoryBytes != nil {
		t.Fatalf("MEMORY ACL errors must be represented as null: %#v", items[1])
	}
	for _, key := range []string{"orders", "restricted"} {
		if samples := client.memorySamples[key]; len(samples) != 1 || samples[0] != 5 {
			t.Fatalf("MEMORY USAGE for %q must use a bounded sample: %#v", key, samples)
		}
	}
}

func TestCollectStreamListItemsIncludesMemoryAndPreservesUnavailableKeys(t *testing.T) {
	client := newFakeStreamMetadataClient()
	client.types["orders"] = "stream"
	client.types["missing"] = "none"
	client.lengths["orders"] = 8
	client.lengths["audit"] = 2
	client.memory["orders"] = 2048
	client.memoryErrors["audit"] = errors.New("ERR unknown command 'MEMORY'")
	monitored := map[string]struct{}{"orders": {}, "missing": {}}

	items, err := collectStreamListItems(
		context.Background(), client, nil, false,
		[]string{"orders", "audit", "missing"}, monitored,
	)
	if err != nil {
		t.Fatalf("collect stream list: %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("expected all available and monitored streams, got %#v", items)
	}
	if items[0].MemoryBytes == nil || *items[0].MemoryBytes != 2048 {
		t.Fatalf("expected MEMORY USAGE on the stream item: %#v", items[0])
	}
	if items[1].MemoryBytes != nil || !items[1].Available {
		t.Fatalf("unsupported MEMORY must not hide an available stream: %#v", items[1])
	}
	if items[2].Available || !items[2].Monitored || items[2].RedisType != "none" || items[2].MemoryBytes != nil {
		t.Fatalf("unavailable monitored stream metadata changed unexpectedly: %#v", items[2])
	}
	for _, key := range []string{"orders", "audit", "missing"} {
		if samples := client.memorySamples[key]; len(samples) != 1 || samples[0] != 5 {
			t.Fatalf("MEMORY USAGE for %q must use a bounded sample: %#v", key, samples)
		}
	}
}

func TestOptionalMemoryUsageRejectsInvalidResults(t *testing.T) {
	if optionalMemoryUsage(nil) != nil {
		t.Fatal("nil command must return nil memory")
	}
	if value := optionalMemoryUsage(redis.NewIntResult(-1, nil)); value != nil {
		t.Fatalf("negative memory usage must not be exposed: %v", *value)
	}
	if value := optionalMemoryUsage(redis.NewIntResult(0, errors.New("NOPERM"))); value != nil {
		t.Fatalf("failed memory usage must not be exposed: %v", *value)
	}
}
