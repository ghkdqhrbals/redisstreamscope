package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedisVersionCompatibility(t *testing.T) {
	address := strings.TrimSpace(os.Getenv("REDIS_TEST_ADDR"))
	if address == "" {
		t.Skip("REDIS_TEST_ADDR is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	manager, err := newRedisManager(appConfig{Connections: []connectionConfig{{
		ID: "compatibility", Name: "Compatibility test", Mode: "standalone",
		Addrs: []string{address}, KeyPattern: "*",
	}}})
	if err != nil {
		t.Fatalf("create Redis manager: %v", err)
	}
	defer manager.close()

	connection, err := manager.get("compatibility")
	if err != nil {
		t.Fatalf("get Redis connection: %v", err)
	}
	client := connection.client

	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping Redis: %v", err)
	}
	actualVersion := redisServerVersion(t, ctx, client)
	expectedSeries := strings.TrimSpace(os.Getenv("REDIS_TEST_VERSION"))
	if expectedSeries != "" && actualVersion != expectedSeries && !strings.HasPrefix(actualVersion, expectedSeries+".") {
		t.Fatalf("Redis version %q does not match expected series %q", actualVersion, expectedSeries)
	}
	t.Logf("running StreamScope compatibility suite against Redis %s", actualVersion)

	prefix := fmt.Sprintf("streamscope:compat:%d", time.Now().UnixNano())
	streamKey := prefix + ":events"
	groupStreamKey := prefix + ":group-events"
	nonStreamKey := prefix + ":not-a-stream"
	groupName := "workers"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_ = client.Del(cleanupCtx, streamKey, groupStreamKey, nonStreamKey).Err()
	})

	if err := client.Set(ctx, nonStreamKey, "value", 0).Err(); err != nil {
		t.Fatalf("create non-stream control key: %v", err)
	}

	var entryIDs []string
	for index := 0; index < 4; index++ {
		id, addErr := client.XAdd(ctx, newXAddArgs(
			streamKey,
			"*",
			map[string]string{"index": fmt.Sprint(index), "payload": "compatibility"},
			3,
			true,
		)).Result()
		if addErr != nil {
			t.Fatalf("XADD with exact MAXLEN: %v", addErr)
		}
		entryIDs = append(entryIDs, id)
	}

	length, err := client.XLen(ctx, streamKey).Result()
	if err != nil {
		t.Fatalf("XLEN: %v", err)
	}
	if length != 3 {
		t.Fatalf("XADD MAXLEN retained %d entries, want 3", length)
	}

	scanResult, err := client.Do(ctx, "SCAN", 0, "MATCH", prefix+":*", "COUNT", 100, "TYPE", "stream").Result()
	if err != nil {
		t.Fatalf("SCAN TYPE stream: %v", err)
	}
	_, streamKeys, err := parseScan(scanResult)
	if err != nil {
		t.Fatalf("parse SCAN result: %v", err)
	}
	if !containsString(streamKeys, streamKey) {
		t.Fatalf("SCAN TYPE did not return stream key %q: %v", streamKey, streamKeys)
	}
	if containsString(streamKeys, nonStreamKey) {
		t.Fatalf("SCAN TYPE returned non-stream key %q", nonStreamKey)
	}

	reverseEntries, err := client.XRevRangeN(ctx, streamKey, "+", "-", 2).Result()
	if err != nil {
		t.Fatalf("XREVRANGE: %v", err)
	}
	if len(reverseEntries) != 2 {
		t.Fatalf("XREVRANGE returned %d entries, want 2", len(reverseEntries))
	}
	nextPage, err := client.XRevRangeN(ctx, streamKey, "("+reverseEntries[1].ID, "-", 2).Result()
	if err != nil {
		t.Fatalf("XREVRANGE with exclusive pagination cursor: %v", err)
	}
	if len(nextPage) != 1 {
		t.Fatalf("paginated XREVRANGE returned %d entries, want 1", len(nextPage))
	}

	readResult, err := client.XRead(ctx, &redis.XReadArgs{
		Streams: []string{streamKey, "0-0"},
		Count:   1,
		Block:   20 * time.Millisecond,
	}).Result()
	if err != nil {
		t.Fatalf("XREAD used by live tail: %v", err)
	}
	if len(readResult) != 1 || len(readResult[0].Messages) != 1 {
		t.Fatalf("XREAD returned an unexpected result: %#v", readResult)
	}

	if err := client.XGroupCreateMkStream(ctx, groupStreamKey, groupName, "0").Err(); err != nil {
		t.Fatalf("XGROUP CREATE MKSTREAM: %v", err)
	}
	if err := client.XGroupCreateConsumer(ctx, groupStreamKey, groupName, "consumer-a").Err(); err != nil {
		t.Fatalf("XGROUP CREATECONSUMER: %v", err)
	}

	var groupEntryIDs []string
	for index := 0; index < 2; index++ {
		id, addErr := client.XAdd(ctx, &redis.XAddArgs{
			Stream: groupStreamKey,
			ID:     "*",
			Values: map[string]string{"index": fmt.Sprint(index)},
		}).Result()
		if addErr != nil {
			t.Fatalf("XADD group entry: %v", addErr)
		}
		groupEntryIDs = append(groupEntryIDs, id)
	}

	groupRead, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: "consumer-a",
		Streams:  []string{groupStreamKey, ">"},
		Count:    2,
	}).Result()
	if err != nil {
		t.Fatalf("XREADGROUP: %v", err)
	}
	if len(groupRead) != 1 || len(groupRead[0].Messages) != 2 {
		t.Fatalf("XREADGROUP returned an unexpected result: %#v", groupRead)
	}

	pendingEntries, err := client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: groupStreamKey,
		Group:  groupName,
		Start:  "-",
		End:    "+",
		Count:  10,
	}).Result()
	if err != nil {
		t.Fatalf("XPENDING: %v", err)
	}
	if len(pendingEntries) != 2 {
		t.Fatalf("XPENDING returned %d entries, want 2", len(pendingEntries))
	}

	groups, err := client.XInfoGroups(ctx, groupStreamKey).Result()
	if err != nil {
		t.Fatalf("XINFO GROUPS: %v", err)
	}
	if len(groups) != 1 || groups[0].Name != groupName || groups[0].Pending != 2 {
		t.Fatalf("XINFO GROUPS returned an unexpected result: %#v", groups)
	}

	consumers, err := client.XInfoConsumers(ctx, groupStreamKey, groupName).Result()
	if err != nil {
		t.Fatalf("XINFO CONSUMERS: %v", err)
	}
	if len(consumers) != 1 || consumers[0].Name != "consumer-a" || consumers[0].Pending != 2 {
		t.Fatalf("XINFO CONSUMERS returned an unexpected result: %#v", consumers)
	}

	claimed, err := client.XClaim(ctx, &redis.XClaimArgs{
		Stream:   groupStreamKey,
		Group:    groupName,
		Consumer: "consumer-b",
		MinIdle:  0,
		Messages: []string{groupEntryIDs[0]},
	}).Result()
	if err != nil {
		t.Fatalf("XCLAIM: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != groupEntryIDs[0] {
		t.Fatalf("XCLAIM returned an unexpected result: %#v", claimed)
	}

	autoClaimed, _, err := client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   groupStreamKey,
		Group:    groupName,
		Consumer: "consumer-c",
		MinIdle:  0,
		Start:    "0-0",
		Count:    10,
	}).Result()
	if err != nil {
		t.Fatalf("XAUTOCLAIM: %v", err)
	}
	if len(autoClaimed) == 0 {
		t.Fatal("XAUTOCLAIM returned no pending entries")
	}

	acknowledged, err := client.XAck(ctx, groupStreamKey, groupName, groupEntryIDs...).Result()
	if err != nil {
		t.Fatalf("XACK: %v", err)
	}
	if acknowledged != int64(len(groupEntryIDs)) {
		t.Fatalf("XACK acknowledged %d entries, want %d", acknowledged, len(groupEntryIDs))
	}
	if err := client.XGroupSetID(ctx, groupStreamKey, groupName, "$").Err(); err != nil {
		t.Fatalf("XGROUP SETID: %v", err)
	}
	for _, consumer := range []string{"consumer-a", "consumer-b", "consumer-c"} {
		if err := client.XGroupDelConsumer(ctx, groupStreamKey, groupName, consumer).Err(); err != nil {
			t.Fatalf("XGROUP DELCONSUMER %s: %v", consumer, err)
		}
	}
	if destroyed, destroyErr := client.XGroupDestroy(ctx, groupStreamKey, groupName).Result(); destroyErr != nil || destroyed != 1 {
		t.Fatalf("XGROUP DESTROY returned destroyed=%v err=%v", destroyed, destroyErr)
	}

	if _, err := client.XTrimMaxLen(ctx, streamKey, 2).Result(); err != nil {
		t.Fatalf("XTRIM exact: %v", err)
	}
	if _, err := client.XTrimMaxLenApprox(ctx, streamKey, 1, 0).Result(); err != nil {
		t.Fatalf("XTRIM approximate: %v", err)
	}
	if _, err := client.XDel(ctx, streamKey, entryIDs[len(entryIDs)-1]).Result(); err != nil {
		t.Fatalf("XDEL: %v", err)
	}
}

func redisServerVersion(t *testing.T, ctx context.Context, client redis.UniversalClient) string {
	t.Helper()
	info, err := client.Info(ctx, "server").Result()
	if err != nil {
		t.Fatalf("read Redis server version: %v", err)
	}
	for _, line := range strings.Split(info, "\n") {
		if version, found := strings.CutPrefix(strings.TrimSpace(line), "redis_version:"); found {
			return strings.TrimSpace(version)
		}
	}
	t.Fatalf("redis_version was missing from INFO server")
	return ""
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
