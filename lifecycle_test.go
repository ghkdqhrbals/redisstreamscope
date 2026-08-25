package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func openLifecycleTestStore(t *testing.T) *store {
	t.Helper()
	value, err := openStore(appConfig{
		DataPath:   filepath.Join(t.TempDir(), "redisstreamscope.db"),
		SessionTTL: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := value.migrateLifecycleTelemetry(context.Background()); err != nil {
		value.close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = value.close() })
	return value
}

func lifecycleTime(value time.Time) *time.Time {
	value = value.UTC()
	return &value
}

func TestTelemetryTokenIsReturnedOnceAndComparedSecurely(t *testing.T) {
	value := openLifecycleTestStore(t)
	ctx := context.Background()
	created, err := value.createTelemetryToken(ctx, "checkout workers", "admin-1")
	if err != nil {
		t.Fatal(err)
	}
	if created.Token == "" || !strings.HasPrefix(created.Token, telemetryTokenPrefix) {
		t.Fatalf("unexpected raw token %q", created.Token)
	}
	items, err := value.listTelemetryTokens(ctx)
	if err != nil || len(items) != 1 {
		t.Fatalf("items=%+v err=%v", items, err)
	}
	serialized, err := json.Marshal(items)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(serialized, []byte(created.Token)) {
		t.Fatal("the list API must never return the raw token")
	}

	var storedHash []byte
	if err := value.db.QueryRowContext(ctx, `SELECT token_hash FROM telemetry_tokens WHERE id=?`, created.ID).Scan(&storedHash); err != nil {
		t.Fatal(err)
	}
	if len(storedHash) != sha256.Size || bytes.Contains(storedHash, []byte(created.Token)) {
		t.Fatalf("expected a SHA-256 hash, got %d bytes", len(storedHash))
	}
	authenticated, err := value.authenticateTelemetryToken(ctx, created.Token)
	if err != nil || authenticated.ID != created.ID || authenticated.LastUsedAt == nil {
		t.Fatalf("authenticated=%+v err=%v", authenticated, err)
	}
	wrong := created.Token[:len(created.Token)-1] + "A"
	if wrong == created.Token {
		wrong = created.Token[:len(created.Token)-1] + "B"
	}
	if _, err := value.authenticateTelemetryToken(ctx, wrong); !errors.Is(err, errInvalidTelemetryToken) {
		t.Fatalf("wrong secret error=%v", err)
	}
	if _, err := value.authenticateTelemetryToken(ctx, "not-a-token"); !errors.Is(err, errInvalidTelemetryToken) {
		t.Fatalf("malformed token error=%v", err)
	}
	updated, err := value.updateTelemetryToken(ctx, created.ID, "renamed", false)
	if err != nil || updated.Enabled || updated.Name != "renamed" {
		t.Fatalf("updated=%+v err=%v", updated, err)
	}
	if _, err := value.authenticateTelemetryToken(ctx, created.Token); !errors.Is(err, errInvalidTelemetryToken) {
		t.Fatalf("disabled token error=%v", err)
	}
	deleted, err := value.deleteTelemetryToken(ctx, created.ID)
	if err != nil || !deleted {
		t.Fatalf("deleted=%v err=%v", deleted, err)
	}
}

func TestLifecycleBatchIsIdempotentAndMeasuresActualDurations(t *testing.T) {
	value := openLifecycleTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 8, 25, 5, 0, 0, 125_000_000, time.UTC)

	registered := lifecycleEvent{
		TraceID: "trace-123", ConnectionID: "primary", StreamKey: "orders",
		GroupName: "checkout", EntryID: "1780000000000-0", RegisteredAt: lifecycleTime(base),
	}
	started := lifecycleEvent{
		RequestID: "trace-123", ConnectionID: "primary", StreamKey: "orders",
		GroupName: "checkout", EntryID: "1780000000000-0", Consumer: "worker-a",
		ProcessingStartedAt: lifecycleTime(base.Add(2 * time.Second)), Attempt: 1,
	}
	completed := lifecycleEvent{
		TraceID: "trace-123", ConnectionID: "primary", StreamKey: "orders",
		GroupName: "checkout", EntryID: "1780000000000-0", Consumer: "worker-a",
		ProcessedAt:    lifecycleTime(base.Add(5 * time.Second)),
		AcknowledgedAt: lifecycleTime(base.Add(6 * time.Second)),
		Outcome:        "success", Attempt: 1,
	}
	if err := value.ingestLifecycleBatch(ctx, []lifecycleEvent{registered}); err != nil {
		t.Fatal(err)
	}
	if err := value.ingestLifecycleBatch(ctx, []lifecycleEvent{started, completed}); err != nil {
		t.Fatal(err)
	}
	// A retry of the same payload must neither create a second request nor
	// distort any timestamp.
	if err := value.ingestLifecycleBatch(ctx, []lifecycleEvent{registered, started, completed}); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := value.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM request_lifecycles`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("stored rows=%d, want 1", count)
	}
	record, err := value.getRequestLifecycle(ctx, "trace-123")
	if err != nil {
		t.Fatal(err)
	}
	if record.RegisteredAt == nil || !record.RegisteredAt.Equal(base) || record.Attempt != 1 || record.Outcome != "success" {
		t.Fatalf("record=%+v", record)
	}

	metrics, err := value.queryLifecycleMetrics(ctx, lifecycleQuery{
		ConnectionID: "primary", StreamKey: "orders", GroupName: "checkout",
		From: base.Add(-time.Second), To: base.Add(10 * time.Second), Bucket: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !metrics.Instrumented || metrics.Summary.Requests != 1 || metrics.Summary.Succeeded != 1 || metrics.Summary.Acknowledged != 1 {
		t.Fatalf("summary=%+v", metrics.Summary)
	}
	assertLifecycleMilliseconds(t, "queue", metrics.Summary.QueueDelay, 2_000)
	assertLifecycleMilliseconds(t, "processing", metrics.Summary.Processing, 3_000)
	assertLifecycleMilliseconds(t, "completion", metrics.Summary.Completion, 5_000)
	assertLifecycleMilliseconds(t, "ack", metrics.Summary.AckDelay, 1_000)
	assertLifecycleMilliseconds(t, "end-to-end", metrics.Summary.EndToEnd, 6_000)
}

func assertLifecycleMilliseconds(t *testing.T, name string, stats lifecycleDurationStats, expected float64) {
	t.Helper()
	if stats.Count != 1 || stats.AvgMs == nil || stats.P50Ms == nil || stats.P95Ms == nil || stats.P99Ms == nil || stats.MaxMs == nil {
		t.Fatalf("%s stats are incomplete: %+v", name, stats)
	}
	for label, actual := range map[string]float64{
		"avg": *stats.AvgMs, "p50": *stats.P50Ms, "p95": *stats.P95Ms, "p99": *stats.P99Ms, "max": *stats.MaxMs,
	} {
		if actual != expected {
			t.Fatalf("%s %s=%v, want %v", name, label, actual, expected)
		}
	}
}

func TestLifecycleUpsertIsMonotonicAcrossRetries(t *testing.T) {
	value := openLifecycleTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 8, 25, 6, 0, 0, 0, time.UTC)
	latest := lifecycleEvent{
		TraceID: "retry-request", ConnectionID: "primary", StreamKey: "billing", GroupName: "workers",
		Consumer: "worker-2", RegisteredAt: lifecycleTime(base),
		ProcessingStartedAt: lifecycleTime(base.Add(2 * time.Second)),
		ProcessedAt:         lifecycleTime(base.Add(8 * time.Second)),
		AcknowledgedAt:      lifecycleTime(base.Add(9 * time.Second)), Outcome: "success", Attempt: 2,
	}
	if err := value.ingestLifecycleBatch(ctx, []lifecycleEvent{latest}); err != nil {
		t.Fatal(err)
	}
	// A delayed event from the first attempt may fill an earlier start, but it
	// must not reduce the attempt, replace the final consumer, or move completion
	// backwards.
	older := lifecycleEvent{
		TraceID: "retry-request", ConnectionID: "primary", StreamKey: "billing", GroupName: "workers",
		Consumer: "worker-1", ProcessingStartedAt: lifecycleTime(base.Add(time.Second)),
		ProcessedAt: lifecycleTime(base.Add(4 * time.Second)), Outcome: "failed", Error: "temporary", Attempt: 1,
	}
	if err := value.ingestLifecycleBatch(ctx, []lifecycleEvent{older}); err != nil {
		t.Fatal(err)
	}
	record, err := value.getRequestLifecycle(ctx, "retry-request")
	if err != nil {
		t.Fatal(err)
	}
	if record.Attempt != 2 || record.Consumer != "worker-2" || record.Outcome != "success" || record.Error != "" {
		t.Fatalf("final retry metadata regressed: %+v", record)
	}
	if record.ProcessingStartedAt == nil || !record.ProcessingStartedAt.Equal(base.Add(time.Second)) ||
		record.ProcessedAt == nil || !record.ProcessedAt.Equal(base.Add(8*time.Second)) {
		t.Fatalf("monotonic timestamps=%+v", record)
	}
}

func TestLifecycleValidationRollsBackWholeBatch(t *testing.T) {
	value := openLifecycleTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 8, 25, 7, 0, 0, 0, time.UTC)
	valid := lifecycleEvent{
		TraceID: "valid", ConnectionID: "primary", StreamKey: "orders", RegisteredAt: lifecycleTime(base),
	}
	invalid := lifecycleEvent{
		TraceID: "invalid", ConnectionID: "primary", StreamKey: "orders",
		RegisteredAt: lifecycleTime(base), ProcessingStartedAt: lifecycleTime(base.Add(-time.Second)),
	}
	if err := value.ingestLifecycleBatch(ctx, []lifecycleEvent{valid, invalid}); err == nil || !strings.Contains(err.Error(), "processingStartedAt") {
		t.Fatalf("validation error=%v", err)
	}
	if _, err := value.getRequestLifecycle(ctx, "valid"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("valid row from rejected batch must not be stored: %v", err)
	}
	tooMany := make([]lifecycleEvent, lifecycleBatchLimit+1)
	if err := value.ingestLifecycleBatch(ctx, tooMany); err == nil {
		t.Fatal("oversized batch was accepted")
	}
}

func TestLifecyclePercentilesUseObservedDurations(t *testing.T) {
	values := lifecycleStats([]float64{10, 20, 30, 40, 50})
	if values.P50Ms == nil || *values.P50Ms != 30 {
		t.Fatalf("p50=%v", values.P50Ms)
	}
	if values.P95Ms == nil || *values.P95Ms != 48 {
		t.Fatalf("p95=%v", values.P95Ms)
	}
	if values.P99Ms == nil || *values.P99Ms != 49.6 {
		t.Fatalf("p99=%v", values.P99Ms)
	}
}

func TestLifecycleIngestHandlerRequiresBearerToken(t *testing.T) {
	value := openLifecycleTestStore(t)
	created, err := value.createTelemetryToken(context.Background(), "handler", "admin")
	if err != nil {
		t.Fatal(err)
	}
	server := &apiServer{store: value}
	base := time.Date(2026, 8, 25, 8, 0, 0, 0, time.UTC)
	body, _ := json.Marshal(map[string]any{"events": []lifecycleEvent{{
		TraceID: "handler-request", ConnectionID: "primary", StreamKey: "orders", RegisteredAt: &base,
	}}})

	unauthorizedRequest := httptest.NewRequest(http.MethodPost, "/api/telemetry/lifecycle", bytes.NewReader(body))
	unauthorizedResponse := httptest.NewRecorder()
	server.ingestLifecycleEvents(unauthorizedResponse, unauthorizedRequest)
	if unauthorizedResponse.Code != http.StatusUnauthorized {
		t.Fatalf("without token status=%d", unauthorizedResponse.Code)
	}

	request := httptest.NewRequest(http.MethodPost, "/api/telemetry/lifecycle", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+created.Token)
	response := httptest.NewRecorder()
	server.ingestLifecycleEvents(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if _, err := value.getRequestLifecycle(context.Background(), "handler-request"); err != nil {
		t.Fatal(err)
	}
}

func TestLifecycleRequestCursorHasNoDuplicatesOrGaps(t *testing.T) {
	value := openLifecycleTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 8, 25, 9, 0, 0, 0, time.UTC)
	events := make([]lifecycleEvent, 0, 7)
	for _, traceID := range []string{"trace-a", "trace-b", "trace-c", "trace-d", "trace-e", "trace-f", "trace-g"} {
		events = append(events, lifecycleEvent{
			TraceID: traceID, ConnectionID: "primary", StreamKey: "orders", GroupName: "workers",
			RegisteredAt: lifecycleTime(base),
		})
	}
	// All rows in a batch deliberately share updated_at. The trace-id tie
	// breaker is therefore essential to avoid a skipped or repeated request.
	if err := value.ingestLifecycleBatch(ctx, events); err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]bool)
	cursor := ""
	for pageNumber := 0; ; pageNumber++ {
		if pageNumber > 10 {
			t.Fatal("cursor pagination did not terminate")
		}
		page, err := value.listLifecycleRequests(ctx, lifecycleRequestQuery{
			ConnectionID: "primary", StreamKey: "orders", Cursor: cursor, Limit: 2,
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range page.Items {
			if seen[item.TraceID] {
				t.Fatalf("duplicate trace %q", item.TraceID)
			}
			seen[item.TraceID] = true
		}
		if !page.HasMore {
			if page.NextCursor != "" {
				t.Fatalf("last page exposed a next cursor: %q", page.NextCursor)
			}
			break
		}
		if page.NextCursor == "" {
			t.Fatal("a non-final page did not expose its cursor")
		}
		cursor = page.NextCursor
	}
	if len(seen) != len(events) {
		t.Fatalf("visited %d requests, want %d: %v", len(seen), len(events), seen)
	}
	if _, err := value.listLifecycleRequests(ctx, lifecycleRequestQuery{Cursor: "not-a-cursor", Limit: 2}); err == nil {
		t.Fatal("invalid cursor was accepted")
	}
}

func TestLifecycleRequestFiltersAndDurations(t *testing.T) {
	value := openLifecycleTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 8, 25, 10, 0, 0, 250_000_000, time.UTC)
	events := []lifecycleEvent{
		{
			TraceID: "checkout-success", ConnectionID: "primary", StreamKey: "orders", GroupName: "checkout",
			EntryID: "100-0", Consumer: "checkout-worker-7", RegisteredAt: lifecycleTime(base),
			ProcessingStartedAt: lifecycleTime(base.Add(1500 * time.Millisecond)),
			ProcessedAt:         lifecycleTime(base.Add(3750 * time.Millisecond)),
			AcknowledgedAt:      lifecycleTime(base.Add(4 * time.Second)), Outcome: "success", Attempt: 1,
		},
		{
			TraceID: "checkout-inflight", ConnectionID: "primary", StreamKey: "orders", GroupName: "checkout",
			EntryID: "101-0", Consumer: "checkout-worker-8", RegisteredAt: lifecycleTime(base.Add(time.Second)),
		},
		{
			TraceID: "billing-failure", ConnectionID: "secondary", StreamKey: "billing", GroupName: "billing-workers",
			EntryID: "200-0", Consumer: "billing-worker", RegisteredAt: lifecycleTime(base),
			ProcessingStartedAt: lifecycleTime(base.Add(time.Second)), ProcessedAt: lifecycleTime(base.Add(2 * time.Second)),
			Outcome: "failed", Error: "payment gateway unavailable", Attempt: 2,
		},
	}
	if err := value.ingestLifecycleBatch(ctx, events); err != nil {
		t.Fatal(err)
	}
	page, err := value.listLifecycleRequests(ctx, lifecycleRequestQuery{
		ConnectionID: "primary", StreamKey: "orders", GroupName: "checkout", Search: "worker-7", Limit: 200,
	})
	if err != nil {
		t.Fatal(err)
	}
	if page.HasMore || len(page.Items) != 1 || page.Items[0].TraceID != "checkout-success" {
		t.Fatalf("filtered page=%+v", page)
	}
	item := page.Items[0]
	for name, value := range map[string]*float64{
		"queue": item.QueueDelayMs, "processing": item.ProcessingMs, "completion": item.CompletionMs, "ack": item.AckDelayMs, "end-to-end": item.EndToEndMs,
	} {
		if value == nil {
			t.Fatalf("%s duration is null", name)
		}
	}
	if *item.QueueDelayMs != 1500 || *item.ProcessingMs != 2250 || *item.CompletionMs != 3750 || *item.AckDelayMs != 250 || *item.EndToEndMs != 4000 {
		t.Fatalf("durations=%+v", item)
	}

	inflight, err := value.listLifecycleRequests(ctx, lifecycleRequestQuery{Search: "CHECKOUT-INFLIGHT", Limit: 10})
	if err != nil || len(inflight.Items) != 1 {
		t.Fatalf("case-insensitive search page=%+v err=%v", inflight, err)
	}
	if inflight.Items[0].QueueDelayMs != nil || inflight.Items[0].ProcessingMs != nil ||
		inflight.Items[0].CompletionMs != nil || inflight.Items[0].AckDelayMs != nil || inflight.Items[0].EndToEndMs != nil {
		t.Fatalf("incomplete lifecycle must expose null durations: %+v", inflight.Items[0])
	}

	errorSearch, err := value.listLifecycleRequests(ctx, lifecycleRequestQuery{Search: "gateway unavailable", Limit: 10})
	if err != nil || len(errorSearch.Items) != 1 || errorSearch.Items[0].TraceID != "billing-failure" {
		t.Fatalf("error search page=%+v err=%v", errorSearch, err)
	}
}

func TestLifecycleCompletionDoesNotRequireAcknowledgement(t *testing.T) {
	value := openLifecycleTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 8, 25, 10, 30, 0, 0, time.UTC)
	if err := value.ingestLifecycleBatch(ctx, []lifecycleEvent{{
		TraceID: "processed-without-ack", ConnectionID: "primary", StreamKey: "orders", GroupName: "workers",
		RegisteredAt: lifecycleTime(base), ProcessingStartedAt: lifecycleTime(base.Add(100 * time.Millisecond)),
		ProcessedAt: lifecycleTime(base.Add(750 * time.Millisecond)), Outcome: "success",
	}}); err != nil {
		t.Fatal(err)
	}
	metrics, err := value.queryLifecycleMetrics(ctx, lifecycleQuery{ConnectionID: "primary", StreamKey: "orders", From: base.Add(-time.Second), To: base.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	assertLifecycleMilliseconds(t, "completion", metrics.Summary.Completion, 750)
	if metrics.Summary.EndToEnd.Count != 0 || metrics.Summary.EndToEnd.P95Ms != nil {
		t.Fatalf("acknowledgement latency must remain absent: %+v", metrics.Summary.EndToEnd)
	}
	page, err := value.listLifecycleRequests(ctx, lifecycleRequestQuery{ConnectionID: "primary", StreamKey: "orders", Limit: 10})
	if err != nil || len(page.Items) != 1 {
		t.Fatalf("page=%+v err=%v", page, err)
	}
	if page.Items[0].CompletionMs == nil || *page.Items[0].CompletionMs != 750 || page.Items[0].EndToEndMs != nil {
		t.Fatalf("durations=%+v", page.Items[0])
	}
}

func TestLifecycleMetricsBucketDurationsAtTheirEndEvents(t *testing.T) {
	base := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	registered := base.Add(100 * time.Millisecond)
	started := base.Add(1100 * time.Millisecond)
	processed := base.Add(2100 * time.Millisecond)
	acknowledged := base.Add(3100 * time.Millisecond)
	metrics := aggregateLifecycleMetrics([]requestLifecycle{{
		TraceID: "bucketed-request", ConnectionID: "primary", StreamKey: "orders",
		RegisteredAt: &registered, ProcessingStartedAt: &started, ProcessedAt: &processed,
		AcknowledgedAt: &acknowledged, Outcome: "success",
	}}, base, base.Add(4*time.Second), time.Second, false)

	points := make(map[time.Time]lifecycleSeriesPoint, len(metrics.Points))
	for _, point := range metrics.Points {
		points[point.At] = point
	}
	if point := points[base.Add(time.Second)]; point.QueueDelay.Count != 1 || point.Processing.Count != 0 || point.Completion.Count != 0 {
		t.Fatalf("processing-start bucket=%+v", point)
	}
	if point := points[base.Add(2*time.Second)]; point.Processing.Count != 1 || point.Completion.Count != 1 || point.Succeeded != 1 || point.AckDelay.Count != 0 {
		t.Fatalf("processed bucket=%+v", point)
	}
	if point := points[base.Add(3*time.Second)]; point.AckDelay.Count != 1 || point.EndToEnd.Count != 1 || point.Succeeded != 0 {
		t.Fatalf("acknowledged bucket=%+v", point)
	}
}

func TestLifecycleCompletionAndOutcomeRemainVisibleWhenAckIsOutsideWindow(t *testing.T) {
	value := openLifecycleTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second).Add(-time.Minute)
	registered := base
	started := base.Add(time.Second)
	processed := base.Add(2 * time.Second)
	acknowledged := base.Add(10 * time.Second)
	if err := value.ingestLifecycleBatch(ctx, []lifecycleEvent{{
		TraceID: "ack-outside-window", ConnectionID: "primary", StreamKey: "orders",
		RegisteredAt: &registered, ProcessingStartedAt: &started, ProcessedAt: &processed,
		AcknowledgedAt: &acknowledged, Outcome: "success",
	}}); err != nil {
		t.Fatal(err)
	}

	metrics, err := value.queryLifecycleMetrics(ctx, lifecycleQuery{
		ConnectionID: "primary", StreamKey: "orders", From: base, To: base.Add(3 * time.Second), Bucket: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if metrics.Summary.Processed != 1 || metrics.Summary.Acknowledged != 0 || metrics.Summary.Succeeded != 1 {
		t.Fatalf("summary=%+v", metrics.Summary)
	}
	assertLifecycleMilliseconds(t, "completion", metrics.Summary.Completion, 2_000)
	if metrics.Summary.AckDelay.Count != 0 || metrics.Summary.EndToEnd.Count != 0 {
		t.Fatalf("acknowledgement metrics must stay outside the window: %+v", metrics.Summary)
	}
}

func TestLifecycleErrorOnlyTerminalRecordIsNotInFlight(t *testing.T) {
	base := time.Date(2026, 8, 25, 12, 30, 0, 0, time.UTC)
	processed := base.Add(time.Second)
	metrics := aggregateLifecycleMetrics([]requestLifecycle{{
		TraceID: "error-only", ConnectionID: "primary", StreamKey: "orders",
		ProcessedAt: &processed, Error: "worker crashed",
	}}, base, base.Add(2*time.Second), time.Second, false)
	if metrics.Summary.Failed != 1 || metrics.Summary.InFlight != 0 {
		t.Fatalf("summary=%+v", metrics.Summary)
	}
}

func TestLifecycleMetricsLimitKeepsNewestRecordsAndReturnsChronologicalPoints(t *testing.T) {
	value := openLifecycleTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second).Add(-10 * time.Second)
	events := make([]lifecycleEvent, 0, 4)
	for index := 1; index <= 4; index++ {
		registered := base.Add(time.Duration(index) * time.Second)
		events = append(events, lifecycleEvent{
			TraceID: fmt.Sprintf("limited-%d", index), ConnectionID: "primary", StreamKey: "orders",
			RegisteredAt: &registered,
		})
	}
	registered := base.Add(500 * time.Millisecond)
	processed := base.Add(1500 * time.Millisecond)
	acknowledged := base.Add(time.Hour)
	events = append(events, lifecycleEvent{
		TraceID: "old-completion-late-ack", ConnectionID: "primary", StreamKey: "orders",
		RegisteredAt: &registered, ProcessedAt: &processed, AcknowledgedAt: &acknowledged, Outcome: "success",
	})
	if err := value.ingestLifecycleBatch(ctx, events); err != nil {
		t.Fatal(err)
	}
	metrics, err := value.queryLifecycleMetrics(ctx, lifecycleQuery{
		ConnectionID: "primary", StreamKey: "orders", From: base, To: base.Add(5 * time.Second),
		Bucket: time.Second, Limit: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !metrics.Truncated || metrics.Summary.Requests != 2 || len(metrics.Points) != 2 {
		t.Fatalf("metrics=%+v", metrics)
	}
	if !metrics.Points[0].At.Equal(base.Add(3*time.Second)) || !metrics.Points[1].At.Equal(base.Add(4*time.Second)) {
		t.Fatalf("points=%+v", metrics.Points)
	}
}

func TestLifecycleRequestsHandlerReturnsExpandedItems(t *testing.T) {
	value := openLifecycleTestStore(t)
	base := time.Date(2026, 8, 25, 11, 0, 0, 0, time.UTC)
	if err := value.ingestLifecycleBatch(context.Background(), []lifecycleEvent{{
		TraceID: "visible-request", ConnectionID: "primary", StreamKey: "orders",
		RegisteredAt: lifecycleTime(base), ProcessingStartedAt: lifecycleTime(base.Add(time.Second)),
	}}); err != nil {
		t.Fatal(err)
	}
	server := &apiServer{store: value}
	request := httptest.NewRequest(http.MethodGet, "/api/telemetry/lifecycle/requests?connectionId=primary&limit=10", nil)
	request = request.WithContext(context.WithValue(request.Context(), sessionContextKey, sessionRecord{Role: "admin"}))
	response := httptest.NewRecorder()
	server.lifecycleRequests(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	items, ok := payload["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("payload=%s", response.Body.String())
	}
	item, _ := items[0].(map[string]any)
	if item["traceId"] != "visible-request" || item["queueDelayMs"] != float64(1000) {
		t.Fatalf("expanded item=%+v", item)
	}
	if item["processingMs"] != nil || item["ackDelayMs"] != nil || item["endToEndMs"] != nil {
		t.Fatalf("missing durations must be JSON null: %+v", item)
	}
}
