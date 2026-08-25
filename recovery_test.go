package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func newRecoveryTestStore(t *testing.T) *store {
	t.Helper()
	dataStore, err := openStore(appConfig{DataPath: filepath.Join(t.TempDir(), "recovery.db"), SessionTTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dataStore.close() })
	if err := initRecoverySchema(context.Background(), dataStore); err != nil {
		t.Fatal(err)
	}
	return dataStore
}

func mustEncodedRedisFields(t *testing.T, fields map[string]any) string {
	t.Helper()
	value, err := encodeRedisFields(fields)
	if err != nil {
		t.Fatal(err)
	}
	return string(value)
}

func TestNormalizeRecoveryRequestBoundsAndConfirmation(t *testing.T) {
	input, err := normalizeRecoveryRequest(recoveryPlanRequest{
		Action: " XCLAIM ", ConnectionID: " redis ", StreamKey: " orders ", Group: " workers ",
		Consumer: " recovery-1 ", IDs: []string{"1-0", "1-0", "2-0"}, MinIdleMs: 5000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if input.Action != "xclaim" || input.ConnectionID != "redis" || len(input.IDs) != 2 {
		t.Fatalf("unexpected normalized request: %#v", input)
	}
	confirmation := recoveryConfirmation(input)
	if !strings.HasPrefix(confirmation, "confirm:xclaim:") {
		t.Fatalf("unexpected confirmation: %s", confirmation)
	}
	changed := input
	changed.MinIdleMs++
	if confirmation == recoveryConfirmation(changed) {
		t.Fatal("confirmation must cover every mutating argument")
	}

	tooMany := make([]string, recoveryMaxCount+1)
	for index := range tooMany {
		tooMany[index] = strconv.Itoa(index+1) + "-0"
	}
	_, err = normalizeRecoveryRequest(recoveryPlanRequest{
		Action: "xack", ConnectionID: "redis", StreamKey: "orders", Group: "workers", IDs: tooMany,
	})
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("expected bounded ID validation, got %v", err)
	}
	_, err = normalizeRecoveryRequest(recoveryPlanRequest{
		Action: "xgroup-setid", ConnectionID: "redis", StreamKey: "orders", Group: "workers", TargetID: "not-an-id",
	})
	if err == nil {
		t.Fatal("expected invalid stream ID to be rejected")
	}
}

func TestRecoveryPlanIsReadOnlyAndFiltersEligibility(t *testing.T) {
	client := &fakeRecoveryClient{
		pending: []redis.XPendingExt{
			{ID: "1-0", Consumer: "dead", Idle: 20 * time.Second, RetryCount: 3},
			{ID: "2-0", Consumer: "live", Idle: time.Second, RetryCount: 1},
		},
	}
	input, err := normalizeRecoveryRequest(recoveryPlanRequest{
		Action: "xclaim", ConnectionID: "redis", StreamKey: "orders", Group: "workers",
		Consumer: "rescue", IDs: []string{"1-0", "2-0", "3-0"}, MinIdleMs: 5000,
	})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := buildRecoveryPlan(context.Background(), client, input)
	if err != nil {
		t.Fatal(err)
	}
	if plan.RequestedCount != 3 || len(plan.Pending) != 2 || plan.CandidateCount != 1 {
		t.Fatalf("unexpected recovery plan: %#v", plan)
	}
	if client.mutationCalls != 0 {
		t.Fatalf("dry-run plan mutated Redis %d times", client.mutationCalls)
	}
	if plan.Pending[0].ID != "1-0" || !plan.Pending[0].Eligible || plan.Pending[1].Eligible {
		t.Fatalf("eligibility does not respect min idle: %#v", plan.Pending)
	}
}

func TestAutoClaimPlanIsBoundedAndDocumentsSnapshotNature(t *testing.T) {
	pending := make([]redis.XPendingExt, 40)
	for index := range pending {
		pending[index] = redis.XPendingExt{
			ID: strconv.Itoa(index+1) + "-0", Consumer: "old", Idle: 10 * time.Second, RetryCount: int64(index + 1),
		}
	}
	client := &fakeRecoveryClient{pending: pending}
	input, err := normalizeRecoveryRequest(recoveryPlanRequest{
		Action: "xautoclaim", ConnectionID: "redis", StreamKey: "orders", Group: "workers",
		Consumer: "rescue", Start: "0-0", Count: 5, MinIdleMs: 5000,
	})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := buildRecoveryPlan(context.Background(), client, input)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Pending) != 5 || plan.CandidateCount != 5 {
		t.Fatalf("XAUTOCLAIM preview exceeded requested count: %#v", plan)
	}
	if len(plan.Warnings) == 0 || !strings.Contains(plan.Warnings[0], "execution time") {
		t.Fatalf("missing moving-snapshot warning: %#v", plan.Warnings)
	}
}

func TestGroupSetIDPlanReportsCurrentAndTargetOffsets(t *testing.T) {
	client := &fakeRecoveryClient{groups: []redis.XInfoGroup{{Name: "workers", LastDeliveredID: "8-0"}}}
	input, err := normalizeRecoveryRequest(recoveryPlanRequest{
		Action: "xgroup-setid", ConnectionID: "redis", StreamKey: "orders", Group: "workers", TargetID: "2-0",
	})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := buildRecoveryPlan(context.Background(), client, input)
	if err != nil {
		t.Fatal(err)
	}
	if plan.CurrentGroupID != "8-0" || plan.TargetGroupID != "2-0" || plan.CandidateCount != 1 {
		t.Fatalf("unexpected group offset plan: %#v", plan)
	}
	if plan.ExpectedCurrentGroupID != "8-0" {
		t.Fatalf("group revision was not captured in the plan: %#v", plan)
	}
	signed := input
	signed.ExpectedCurrentGroupID = plan.ExpectedCurrentGroupID
	if plan.Confirmation != recoveryConfirmation(signed) {
		t.Fatal("confirmation does not cover the expected current group revision")
	}
	client.groups[0].LastDeliveredID = "9-0"
	changedPlan, err := buildRecoveryPlan(context.Background(), client, input)
	if err != nil {
		t.Fatal(err)
	}
	if changedPlan.Confirmation == plan.Confirmation {
		t.Fatal("confirmation must change when the current group offset changes")
	}
	if client.mutationCalls != 0 {
		t.Fatal("set-id planning must not mutate Redis")
	}
}

func TestGroupSetIDExecutionRejectsStalePlanBeforeMutation(t *testing.T) {
	client := &fakeRecoveryClient{groups: []redis.XInfoGroup{{Name: "workers", LastDeliveredID: "9-0"}}}
	input := recoveryPlanRequest{
		Action: "xgroup-setid", StreamKey: "orders", Group: "workers", TargetID: "2-0", ExpectedCurrentGroupID: "8-0",
	}
	result, err := executeRecoveryCommand(context.Background(), client, input)
	if !errors.Is(err, errGroupRevisionChanged) {
		t.Fatalf("expected stale recovery plan error, result=%#v err=%v", result, err)
	}
	if client.mutationCalls != 0 {
		t.Fatalf("stale plan mutated Redis %d times", client.mutationCalls)
	}
	if client.groupQueries != 0 || len(client.evalKeys) != 1 || len(client.evalKeys[0]) != 1 || client.evalKeys[0][0] != "orders" {
		t.Fatalf("SETID did not use one cluster-routable Redis-side comparison: queries=%d keys=%v", client.groupQueries, client.evalKeys)
	}
	if !strings.Contains(client.evalScripts[0], "KEYS[1]") || !strings.Contains(client.evalScripts[0], "XGROUP', 'SETID") {
		t.Fatalf("unexpected atomic SETID script: %s", client.evalScripts[0])
	}

	input.ExpectedCurrentGroupID = "9-0"
	result, err = executeRecoveryCommand(context.Background(), client, input)
	if err != nil || result.Affected != 1 || client.mutationCalls != 1 {
		t.Fatalf("current plan was not executed: result=%#v mutations=%d err=%v", result, client.mutationCalls, err)
	}
}

func TestRecoveryExecutionFailureMapsStalePlanToConflict(t *testing.T) {
	status, code := recoveryExecutionFailure(fmt.Errorf("wrapped: %w", errGroupRevisionChanged))
	if status != http.StatusConflict || code != "recovery_plan_stale" {
		t.Fatalf("stale recovery mapping=%d %s", status, code)
	}
	status, code = recoveryExecutionFailure(errors.New("redis unavailable"))
	if status != http.StatusBadGateway || code != "redis_error" {
		t.Fatalf("Redis error mapping=%d %s", status, code)
	}
}

func TestExecuteRecoveryCommands(t *testing.T) {
	client := &fakeRecoveryClient{
		groups:        []redis.XInfoGroup{{Name: "workers", LastDeliveredID: "8-0"}},
		claimMessages: []redis.XMessage{{ID: "1-0"}, {ID: "2-0"}},
		autoMessages:  []redis.XMessage{{ID: "3-0"}},
		autoNext:      "4-0",
	}
	tests := []struct {
		name  string
		input recoveryPlanRequest
		check func(t *testing.T, result recoveryExecutionResponse)
	}{
		{"ack", recoveryPlanRequest{Action: "xack", StreamKey: "orders", Group: "workers", IDs: []string{"1-0", "2-0"}}, func(t *testing.T, result recoveryExecutionResponse) {
			if result.Affected != 2 {
				t.Fatalf("unexpected ack result: %#v", result)
			}
		}},
		{"claim", recoveryPlanRequest{Action: "xclaim", StreamKey: "orders", Group: "workers", Consumer: "rescue", IDs: []string{"1-0", "2-0"}}, func(t *testing.T, result recoveryExecutionResponse) {
			if result.Affected != 2 || len(result.MessageIDs) != 2 {
				t.Fatalf("unexpected claim result: %#v", result)
			}
		}},
		{"autoclaim", recoveryPlanRequest{Action: "xautoclaim", StreamKey: "orders", Group: "workers", Consumer: "rescue", Start: "0-0", Count: 5}, func(t *testing.T, result recoveryExecutionResponse) {
			if result.Affected != 1 || result.NextStart != "4-0" {
				t.Fatalf("unexpected auto-claim result: %#v", result)
			}
		}},
		{"setid", recoveryPlanRequest{Action: "xgroup-setid", StreamKey: "orders", Group: "workers", TargetID: "10-0", ExpectedCurrentGroupID: "8-0"}, func(t *testing.T, result recoveryExecutionResponse) {
			if result.Affected != 1 || result.TargetGroupID != "10-0" {
				t.Fatalf("unexpected set-id result: %#v", result)
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := executeRecoveryCommand(context.Background(), client, test.input)
			if err != nil {
				t.Fatal(err)
			}
			test.check(t, result)
		})
	}
	if client.mutationCalls != len(tests) {
		t.Fatalf("expected %d mutation calls, got %d", len(tests), client.mutationCalls)
	}
}

func TestRecoveryExecutionIdempotencyPersistence(t *testing.T) {
	dataStore := newRecoveryTestStore(t)
	ctx := context.Background()
	session := sessionRecord{UserID: "user-1", Username: "operator"}
	stored, acquired, err := dataStore.beginRecoveryExecution(ctx, "request-123", "hash-a", session, "xack", "redis", "orders", "workers")
	if err != nil || !acquired || stored.Status != "running" {
		t.Fatalf("unable to acquire execution: acquired=%v stored=%#v err=%v", acquired, stored, err)
	}
	response := []byte(`{"ok":true,"action":"xack","affected":2}`)
	if err := dataStore.completeRecoveryExecution(ctx, "request-123", http.StatusOK, response); err != nil {
		t.Fatal(err)
	}
	stored, acquired, err = dataStore.beginRecoveryExecution(ctx, "request-123", "hash-a", session, "xack", "redis", "orders", "workers")
	if err != nil || acquired || stored.Status != "succeeded" || string(stored.ResponseJSON) != string(response) {
		t.Fatalf("completed result was not replayed: acquired=%v stored=%#v err=%v", acquired, stored, err)
	}
	conflict, acquired, err := dataStore.beginRecoveryExecution(ctx, "request-123", "hash-b", session, "xack", "redis", "orders", "workers")
	if err != nil || acquired || conflict.RequestHash != "hash-a" {
		t.Fatalf("idempotency conflict was not retained: acquired=%v stored=%#v err=%v", acquired, conflict, err)
	}
}

func TestExecuteRecoveryRequiresPreciseACLAndAuditsDenial(t *testing.T) {
	dataStore := newRecoveryTestStore(t)
	server := &apiServer{store: dataStore}
	plan, err := normalizeRecoveryRequest(recoveryPlanRequest{
		Action: "xack", ConnectionID: "redis", StreamKey: "orders", Group: "workers", IDs: []string{"1-0"},
	})
	if err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(recoveryExecutionRequest{
		recoveryPlanRequest: plan, Confirmation: recoveryConfirmation(plan), IdempotencyKey: "request-123",
	})
	request := httptest.NewRequest(http.MethodPost, "/api/recovery/executions", bytes.NewReader(body))
	request = request.WithContext(context.WithValue(request.Context(), sessionContextKey, sessionRecord{
		UserID: "viewer-1", Username: "viewer", Role: "viewer",
	}))
	response := httptest.NewRecorder()
	server.executeRecovery(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("expected precise ACL denial, got %d: %s", response.Code, response.Body.String())
	}
	logs, err := dataStore.listAccessLogs(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 1 || logs[0]["action"] != "recovery:xack" || logs[0]["scope"] != "stream:redis:orders" {
		t.Fatalf("missing precise recovery audit event: %#v", logs)
	}
}

func TestQuarantinePlanBoundsPayloadAndNeverMutates(t *testing.T) {
	client := &fakeQuarantineClient{entries: map[string]redis.XMessage{
		"1-0": {ID: "1-0", Values: map[string]any{"kind": "invoice", "value": "42"}},
	}}
	entries, missing, payloadBytes, err := loadQuarantineSourceEntries(context.Background(), client, "orders", []string{"1-0", "2-0"})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || len(missing) != 1 || missing[0] != "2-0" || payloadBytes == 0 {
		t.Fatalf("unexpected quarantine preview: entries=%#v missing=%#v bytes=%d", entries, missing, payloadBytes)
	}
	if client.evalCalls != 0 || client.ackCalls != 0 {
		t.Fatal("quarantine preview mutated Redis")
	}

	oversized := &fakeQuarantineClient{entries: map[string]redis.XMessage{
		"1-0": {ID: "1-0", Values: map[string]any{"payload": strings.Repeat("x", recoveryMaxEntryPayloadBytes)}},
	}}
	if _, _, _, err := loadQuarantineSourceEntries(context.Background(), oversized, "orders", []string{"1-0"}); err == nil {
		t.Fatal("expected oversized payload to be rejected before mutation")
	}
}

func TestQuarantineDefaultsToNoSourceAcknowledgement(t *testing.T) {
	input, err := normalizeQuarantineRequest(quarantineRequest{
		ConnectionID: "redis", SourceStream: "orders", DLQStream: "orders.dlq", IDs: []string{"1-0"},
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	if input.Acknowledge {
		t.Fatal("quarantine must not acknowledge the source by default")
	}
	confirmation := quarantineConfirmation(input)
	input.Acknowledge = true
	input.SourceGroup = "workers"
	if confirmation == quarantineConfirmation(input) {
		t.Fatal("confirmation must distinguish optional source acknowledgement")
	}
}

func TestRedisFieldEncodingPreservesNonJSONBytes(t *testing.T) {
	fields := map[string]any{string([]byte{'k', 0xff}): []byte{0x00, 0xfe, 0x7f}}
	encoded, err := encodeRedisFields(fields)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeRedisFields(string(encoded))
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range fields {
		if got := []byte(decoded[name].(string)); !bytes.Equal(got, value.([]byte)) {
			t.Fatalf("binary Redis field changed: got=%v want=%v", got, value)
		}
	}
}

func TestRedisXAddOnceAndClusterSidecar(t *testing.T) {
	client := &fakeQuarantineClient{entries: map[string]redis.XMessage{}, markers: map[string]string{}}
	for _, stream := range []string{"orders", "{tenant-1}:orders", "orders{}archive", "orders{broken"} {
		marker := redisSidecarKey(stream, "replay", "record-1")
		if redisClusterSlot(stream) != redisClusterSlot(marker) {
			t.Fatalf("stream %q and marker %q are in different slots", stream, marker)
		}
	}
	marker := redisSidecarKey("orders", "replay", "record-1")
	firstID, created, err := redisXAddOnce(context.Background(), client, "orders", marker, map[string]any{"kind": "invoice"})
	if err != nil || !created {
		t.Fatalf("first idempotent XADD failed: id=%s created=%v err=%v", firstID, created, err)
	}
	secondID, created, err := redisXAddOnce(context.Background(), client, "orders", marker, map[string]any{"kind": "invoice"})
	if err != nil || created || secondID != firstID {
		t.Fatalf("duplicate XADD was not deduplicated: first=%s second=%s created=%v err=%v", firstID, secondID, created, err)
	}
	if client.added != 1 {
		t.Fatalf("expected one stream append, got %d", client.added)
	}
}

func TestQuarantineStoreLifecyclePaginationAndRetention(t *testing.T) {
	dataStore := newRecoveryTestStore(t)
	ctx := context.Background()
	first, err := dataStore.saveQuarantineRecord(ctx, quarantineRecord{
		ConnectionID: "redis", SourceStream: "orders", SourceGroup: "workers", SourceID: "1-0",
		DLQStream: "orders.dlq", DLQEntryID: "10-0", Fields: map[string]any{"kind": "one"}, Metadata: map[string]any{"reason": "poison"},
		CreatedBy: "operator", fieldsJSON: mustEncodedRedisFields(t, map[string]any{"kind": "one"}), metadataJSON: `{"reason":"poison"}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := dataStore.saveQuarantineRecord(ctx, quarantineRecord{
		ConnectionID: "redis", SourceStream: "orders", SourceGroup: "workers", SourceID: "1-0",
		DLQStream: "orders.dlq", DLQEntryID: "different", CreatedBy: "operator",
		fieldsJSON: mustEncodedRedisFields(t, map[string]any{"kind": "different"}), metadataJSON: `{}`,
	})
	if err != nil || duplicate.ID != first.ID || duplicate.DLQEntryID != "10-0" {
		t.Fatalf("origin uniqueness was not idempotent: first=%#v duplicate=%#v err=%v", first, duplicate, err)
	}
	second, err := dataStore.saveQuarantineRecord(ctx, quarantineRecord{
		ConnectionID: "redis", SourceStream: "orders", SourceGroup: "workers", SourceID: "2-0",
		DLQStream: "orders.dlq", DLQEntryID: "11-0", CreatedBy: "operator", fieldsJSON: mustEncodedRedisFields(t, map[string]any{"kind": "two"}), metadataJSON: `{}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	page, err := dataStore.listQuarantineRecords(ctx, "redis", "orders", "all", 0, 1)
	if err != nil || len(page.Items) != 1 || !page.HasMore || page.NextCursor != second.ID {
		t.Fatalf("unexpected first page: %#v err=%v", page, err)
	}
	next, err := dataStore.listQuarantineRecords(ctx, "redis", "orders", "all", page.NextCursor, 1)
	if err != nil || len(next.Items) != 1 || next.Items[0].ID != first.ID {
		t.Fatalf("unexpected next page: %#v err=%v", next, err)
	}
	if _, err := dataStore.markQuarantineReplayed(ctx, first.ID, "orders.replay", "20-0"); err != nil {
		t.Fatal(err)
	}
	if _, err := dataStore.markQuarantineSkipped(ctx, second.ID); err != nil {
		t.Fatal(err)
	}
	old := time.Now().UTC().Add(-31 * 24 * time.Hour).Format(time.RFC3339Nano)
	if _, err := dataStore.db.ExecContext(ctx, `UPDATE quarantine_records SET terminal_at=?,updated_at=? WHERE id=?`, old, old, first.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := dataStore.db.ExecContext(ctx, `UPDATE quarantine_records SET terminal_at=NULL,status='quarantined',updated_at=? WHERE id=?`, old, second.ID); err != nil {
		t.Fatal(err)
	}
	if err := dataStore.pruneRecoveryHistory(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := dataStore.loadQuarantineRecordByID(ctx, first.ID); err == nil {
		t.Fatal("old terminal quarantine record was not pruned")
	}
	if _, err := dataStore.loadQuarantineRecordByID(ctx, second.ID); err != nil {
		t.Fatalf("active quarantine record must not be pruned: %v", err)
	}
}

func TestRecoveryRedisIntegration(t *testing.T) {
	address := strings.TrimSpace(os.Getenv("REDIS_TEST_ADDR"))
	if address == "" {
		t.Skip("set REDIS_TEST_ADDR to run Redis recovery integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client := redis.NewClient(&redis.Options{Addr: address})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis test server is unavailable: %v", err)
	}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	stream := "redisstreamscope:test:recovery:" + suffix
	dlq := stream + ":dlq"
	group := "workers"
	defer client.Del(context.Background(), stream, dlq)
	entryID, err := client.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: map[string]any{"kind": "invoice", "value": "42"}}).Result()
	if err != nil {
		t.Fatal(err)
	}
	if err := client.XGroupCreate(ctx, stream, group, "0-0").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{Group: group, Consumer: "dead", Streams: []string{stream, ">"}, Count: 1}).Result(); err != nil {
		t.Fatal(err)
	}
	input, err := normalizeRecoveryRequest(recoveryPlanRequest{
		Action: "xclaim", ConnectionID: "redis", StreamKey: stream, Group: group, Consumer: "rescue", IDs: []string{entryID},
	})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := buildRecoveryPlan(ctx, client, input)
	if err != nil || plan.CandidateCount != 1 {
		t.Fatalf("unexpected integration recovery plan: %#v err=%v", plan, err)
	}
	pending, err := client.XPendingExt(ctx, &redis.XPendingExtArgs{Stream: stream, Group: group, Start: entryID, End: entryID, Count: 1}).Result()
	if err != nil || len(pending) != 1 || pending[0].Consumer != "dead" {
		t.Fatalf("planning mutated pending ownership: %#v err=%v", pending, err)
	}
	result, err := executeRecoveryCommand(ctx, client, input)
	if err != nil || result.Affected != 1 {
		t.Fatalf("claim execution failed: %#v err=%v", result, err)
	}
	setIDInput, err := normalizeRecoveryRequest(recoveryPlanRequest{
		Action: "xgroup-setid", ConnectionID: "redis", StreamKey: stream, Group: group, TargetID: "0-0",
	})
	if err != nil {
		t.Fatal(err)
	}
	setIDPlan, err := buildRecoveryPlan(ctx, client, setIDInput)
	if err != nil {
		t.Fatalf("atomic SETID planning failed: %v", err)
	}
	setIDInput.ExpectedCurrentGroupID = setIDPlan.ExpectedCurrentGroupID
	setIDResult, err := executeRecoveryCommand(ctx, client, setIDInput)
	if err != nil || setIDResult.Affected != 1 {
		t.Fatalf("atomic SETID execution failed: result=%#v err=%v", setIDResult, err)
	}
	groups, err := client.XInfoGroups(ctx, stream).Result()
	if err != nil || len(groups) != 1 || groups[0].LastDeliveredID != "0-0" {
		t.Fatalf("atomic SETID result was not applied: groups=%#v err=%v", groups, err)
	}
	entries, missing, _, err := loadQuarantineSourceEntries(ctx, client, stream, []string{entryID})
	if err != nil || len(entries) != 1 || len(missing) != 0 {
		t.Fatalf("source load failed: entries=%#v missing=%#v err=%v", entries, missing, err)
	}
	marker := redisSidecarKey(dlq, "quarantine", stream+"\x00"+entryID)
	first, created, err := redisXAddOnce(ctx, client, dlq, marker, map[string]any{"fields_json": entries[0].FieldsJSON})
	if err != nil || !created {
		t.Fatalf("DLQ append failed: id=%s created=%v err=%v", first, created, err)
	}
	second, created, err := redisXAddOnce(ctx, client, dlq, marker, map[string]any{"fields_json": entries[0].FieldsJSON})
	if err != nil || created || first != second {
		t.Fatalf("DLQ idempotency failed: first=%s second=%s created=%v err=%v", first, second, created, err)
	}
}

type fakeRecoveryClient struct {
	pending        []redis.XPendingExt
	groups         []redis.XInfoGroup
	claimMessages  []redis.XMessage
	autoMessages   []redis.XMessage
	autoNext       string
	mutationCalls  int
	pendingQueries int
	groupQueries   int
	evalScripts    []string
	evalKeys       [][]string
}

func (client *fakeRecoveryClient) XPendingExt(ctx context.Context, arguments *redis.XPendingExtArgs) *redis.XPendingExtCmd {
	client.pendingQueries++
	command := redis.NewXPendingExtCmd(ctx)
	items := make([]redis.XPendingExt, 0)
	for _, item := range client.pending {
		if arguments.Start == arguments.End && arguments.Start != item.ID {
			continue
		}
		items = append(items, item)
		if int64(len(items)) >= arguments.Count {
			break
		}
	}
	command.SetVal(items)
	return command
}

func (client *fakeRecoveryClient) XInfoGroups(ctx context.Context, stream string) *redis.XInfoGroupsCmd {
	client.groupQueries++
	command := redis.NewXInfoGroupsCmd(ctx, stream)
	command.SetVal(client.groups)
	return command
}

func (client *fakeRecoveryClient) XAck(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd {
	client.mutationCalls++
	command := redis.NewIntCmd(ctx)
	command.SetVal(int64(len(ids)))
	return command
}

func (client *fakeRecoveryClient) XClaim(ctx context.Context, arguments *redis.XClaimArgs) *redis.XMessageSliceCmd {
	client.mutationCalls++
	command := redis.NewXMessageSliceCmd(ctx)
	command.SetVal(client.claimMessages)
	return command
}

func (client *fakeRecoveryClient) XAutoClaim(ctx context.Context, arguments *redis.XAutoClaimArgs) *redis.XAutoClaimCmd {
	client.mutationCalls++
	command := redis.NewXAutoClaimCmd(ctx)
	command.SetVal(client.autoMessages, client.autoNext)
	return command
}

func (client *fakeRecoveryClient) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	client.evalScripts = append(client.evalScripts, script)
	client.evalKeys = append(client.evalKeys, append([]string(nil), keys...))
	command := redis.NewCmd(ctx)
	if len(keys) != 1 || len(args) != 3 {
		command.SetErr(errors.New("unexpected script invocation"))
		return command
	}
	group, expected, target := fmt.Sprint(args[0]), fmt.Sprint(args[1]), fmt.Sprint(args[2])
	for index := range client.groups {
		if client.groups[index].Name != group {
			continue
		}
		current := client.groups[index].LastDeliveredID
		if current != expected {
			command.SetVal([]interface{}{int64(0), current})
			return command
		}
		client.groups[index].LastDeliveredID = target
		client.mutationCalls++
		command.SetVal([]interface{}{int64(1), target})
		return command
	}
	command.SetVal([]interface{}{int64(-1), ""})
	return command
}

type fakeQuarantineClient struct {
	entries   map[string]redis.XMessage
	markers   map[string]string
	evalCalls int
	ackCalls  int
	added     int
}

func (client *fakeQuarantineClient) XRangeN(ctx context.Context, stream, start, end string, count int64) *redis.XMessageSliceCmd {
	command := redis.NewXMessageSliceCmd(ctx)
	if item, exists := client.entries[start]; exists {
		command.SetVal([]redis.XMessage{item})
	} else {
		command.SetVal([]redis.XMessage{})
	}
	return command
}

func (client *fakeQuarantineClient) XAck(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd {
	client.ackCalls++
	command := redis.NewIntCmd(ctx)
	command.SetVal(int64(len(ids)))
	return command
}

func (client *fakeQuarantineClient) Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	client.evalCalls++
	if client.markers == nil {
		client.markers = make(map[string]string)
	}
	command := redis.NewCmd(ctx)
	if existing := client.markers[keys[1]]; existing != "" {
		command.SetVal([]any{existing, int64(0)})
		return command
	}
	client.added++
	id := strconv.Itoa(client.added) + "-0"
	client.markers[keys[1]] = id
	command.SetVal([]any{id, int64(1)})
	return command
}
