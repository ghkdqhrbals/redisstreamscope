package main

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestAnalyzeStreamSchemaFindsNestedFieldsSizesAndDrift(t *testing.T) {
	now := time.Date(2026, 8, 25, 11, 0, 0, 0, time.UTC)
	first := analyzeStreamSchema("redis", "orders", []redis.XMessage{
		{ID: "1-0", Values: map[string]any{"payload": `{"orderId":1,"customer":{"tier":"gold"}}`, "kind": "created"}},
		{ID: "2-0", Values: map[string]any{"payload": `{"orderId":2,"customer":{"tier":"silver"}}`, "kind": "created"}},
	}, now)
	if first.SampleCount != 2 || first.JSONPayloads != 2 || first.TextPayloads != 0 || first.P50SizeBytes <= 0 || first.P95SizeBytes < first.P50SizeBytes {
		t.Fatalf("unexpected first schema summary: %+v", first)
	}
	assertSchemaField(t, first.Fields, "payload.orderId", "integer", 2)
	assertSchemaField(t, first.Fields, "payload.customer.tier", "string", 2)

	second := analyzeStreamSchema("redis", "orders", []redis.XMessage{
		{ID: "3-0", Values: map[string]any{"payload": `{"orderId":"3","currency":"KRW"}`, "kind": "created"}},
	}, now.Add(time.Minute))
	drift := compareSchemaSnapshots(first, second)
	if !drift.Detected || !dashboardContainsString(drift.Added, "payload.currency") || !dashboardContainsString(drift.Removed, "payload.customer") || !dashboardContainsString(drift.TypeChanged, "payload.orderId") {
		t.Fatalf("unexpected schema drift: %+v", drift)
	}
	plain := analyzeStreamSchema("redis", "orders", []redis.XMessage{
		{ID: "4-0", Values: map[string]any{"payload": "not-json", "kind": "created"}},
	}, now.Add(2*time.Minute))
	if plain.JSONPayloads != 0 || plain.TextPayloads != 1 {
		t.Fatalf("payload classification must count entries rather than fields: %+v", plain)
	}
}

func assertSchemaField(t *testing.T, fields []schemaFieldInsight, path, kind string, present int) {
	t.Helper()
	for _, field := range fields {
		if field.Path == path {
			if !dashboardContainsString(field.Types, kind) || field.Present != present {
				t.Fatalf("schema field %s=%+v", path, field)
			}
			return
		}
	}
	t.Fatalf("schema field %s not found: %+v", path, fields)
}

func TestCrossStreamTraceSpansAreStoredWithoutCollapsing(t *testing.T) {
	store := openInsightTestStore(t)
	defer store.close()
	ctx := context.Background()
	base := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	registered, firstDone := base, base.Add(200*time.Millisecond)
	secondStart, secondDone := base.Add(210*time.Millisecond), base.Add(600*time.Millisecond)
	events := []lifecycleEvent{
		{TraceID: "trace-cross-stream", SpanID: "publish", ConnectionID: "redis", StreamKey: "orders", Service: "checkout", Operation: "publish", RegisteredAt: &registered, ProcessedAt: &firstDone, Outcome: "success", Attributes: map[string]string{"region": "kr"}},
		{TraceID: "trace-cross-stream", SpanID: "consume", ParentSpanID: "publish", ConnectionID: "redis", StreamKey: "billing", GroupName: "workers", Service: "billing", Operation: "consume", RegisteredAt: &secondStart, ProcessedAt: &secondDone, Outcome: "success"},
	}
	if err := store.ingestLifecycleBatch(ctx, events); err != nil {
		t.Fatal(err)
	}
	spans, err := store.listTraceSpans(ctx, "trace-cross-stream")
	if err != nil || len(spans) != 2 {
		t.Fatalf("spans=%+v err=%v", spans, err)
	}
	if spans[1].ParentSpanID != "publish" || spans[0].StreamKey == spans[1].StreamKey {
		t.Fatalf("trace lineage or streams were collapsed: %+v", spans)
	}
	page, err := store.listTraceSummaries(ctx, "redis", "", "trace-cross", "", 10, scopedPermissionChecker{admin: true})
	if err != nil || len(page.Items) != 1 || page.Items[0].SpanCount != 2 || page.Items[0].StreamCount != 2 || page.Items[0].Status != "success" {
		t.Fatalf("trace summaries=%+v err=%v", page, err)
	}
}

func TestTraceSummaryFilterSelectsTraceButAggregatesEveryVisibleSpan(t *testing.T) {
	store := openInsightTestStore(t)
	defer store.close()
	ctx := context.Background()
	base := time.Date(2026, 8, 25, 12, 30, 0, 0, time.UTC)
	events := []lifecycleEvent{
		{TraceID: "trace-filtered", SpanID: "orders", ConnectionID: "redis", StreamKey: "orders", RegisteredAt: lifecycleTime(base.Add(100 * time.Millisecond)), ProcessedAt: lifecycleTime(base.Add(200 * time.Millisecond)), Outcome: "success"},
		{TraceID: "trace-filtered", SpanID: "billing", ParentSpanID: "orders", ConnectionID: "redis", StreamKey: "billing", RegisteredAt: lifecycleTime(base.Add(50 * time.Millisecond)), AcknowledgedAt: lifecycleTime(base.Add(500 * time.Millisecond)), Outcome: "success"},
		// This span deliberately has the earliest start, latest completion, and a
		// failure. None of those values may leak when the caller cannot read it.
		{TraceID: "trace-filtered", SpanID: "secret", ParentSpanID: "orders", ConnectionID: "redis", StreamKey: "secret", RegisteredAt: lifecycleTime(base.Add(-time.Second)), AcknowledgedAt: lifecycleTime(base.Add(2 * time.Second)), Outcome: "failed", Error: "private failure"},
	}
	if err := store.ingestLifecycleBatch(ctx, events); err != nil {
		t.Fatal(err)
	}
	checker := scopedPermissionChecker{role: "viewer", grants: []scopedPermissionGrant{{
		action: "streams:read", scope: redisStreamScope("redis", "secret"), effect: "deny",
	}}}
	page, err := store.listTraceSummaries(ctx, "redis", "orders", "", "", 10, checker)
	if err != nil || len(page.Items) != 1 {
		t.Fatalf("trace summaries=%+v err=%v", page, err)
	}
	item := page.Items[0]
	if item.SpanCount != 2 || item.StreamCount != 2 || item.Status != "success" {
		t.Fatalf("denied span leaked or visible cross-stream span was omitted: %+v", item)
	}
	if item.StartedAt == nil || !item.StartedAt.Equal(base.Add(50*time.Millisecond)) ||
		item.CompletedAt == nil || !item.CompletedAt.Equal(base.Add(500*time.Millisecond)) ||
		item.DurationMs == nil || *item.DurationMs != 450 {
		t.Fatalf("summary timing did not use all and only visible spans: %+v", item)
	}
}

func TestExplicitTraceSpanMergeRejectsInvalidChronology(t *testing.T) {
	store := openInsightTestStore(t)
	defer store.close()
	ctx := context.Background()
	base := time.Date(2026, 8, 25, 13, 0, 0, 0, time.UTC)
	first := lifecycleEvent{
		TraceID: "trace-invalid-merge", SpanID: "consume", ConnectionID: "redis", StreamKey: "orders",
		ProcessedAt: lifecycleTime(base.Add(time.Second)), Outcome: "success",
	}
	if err := store.ingestLifecycleBatch(ctx, []lifecycleEvent{first}); err != nil {
		t.Fatal(err)
	}
	lateStart := lifecycleEvent{
		TraceID: "trace-invalid-merge", SpanID: "consume", ConnectionID: "redis", StreamKey: "orders",
		ProcessingStartedAt: lifecycleTime(base.Add(2 * time.Second)),
	}
	if err := store.ingestLifecycleBatch(ctx, []lifecycleEvent{lateStart}); err == nil || !strings.Contains(err.Error(), "processedAt must not be before processingStartedAt") {
		t.Fatalf("invalid merged chronology was accepted: %v", err)
	}
	spans, err := store.listTraceSpans(ctx, first.TraceID)
	if err != nil || len(spans) != 1 {
		t.Fatalf("stored spans=%+v err=%v", spans, err)
	}
	if spans[0].ProcessingStartedAt != nil || spans[0].ProcessedAt == nil || !spans[0].ProcessedAt.Equal(base.Add(time.Second)) {
		t.Fatalf("rejected merge changed the stored span: %+v", spans[0])
	}
}

func TestExplicitTraceSpanSuccessfulRetryClearsEarlierError(t *testing.T) {
	store := openInsightTestStore(t)
	defer store.close()
	ctx := context.Background()
	base := time.Date(2026, 8, 25, 13, 15, 0, 0, time.UTC)
	for _, test := range []struct {
		name           string
		successAttempt int
	}{{name: "equal attempt", successAttempt: 1}, {name: "higher attempt", successAttempt: 2}} {
		t.Run(test.name, func(t *testing.T) {
			traceID := "trace-retry-" + strings.ReplaceAll(test.name, " ", "-")
			failed := lifecycleEvent{
				TraceID: traceID, SpanID: "consume", ConnectionID: "redis", StreamKey: "orders",
				ProcessedAt: lifecycleTime(base.Add(time.Second)), Outcome: "failed", Error: "temporary", Attempt: 1,
			}
			succeeded := lifecycleEvent{
				TraceID: traceID, SpanID: "consume", ConnectionID: "redis", StreamKey: "orders",
				AcknowledgedAt: lifecycleTime(base.Add(2 * time.Second)), Outcome: "success", Attempt: test.successAttempt,
			}
			if err := store.ingestLifecycleBatch(ctx, []lifecycleEvent{failed}); err != nil {
				t.Fatal(err)
			}
			if err := store.ingestLifecycleBatch(ctx, []lifecycleEvent{succeeded}); err != nil {
				t.Fatal(err)
			}
			spans, err := store.listTraceSpans(ctx, traceID)
			if err != nil || len(spans) != 1 {
				t.Fatalf("spans=%+v err=%v", spans, err)
			}
			if spans[0].Outcome != "success" || spans[0].Error != "" || spans[0].Attempt != test.successAttempt {
				t.Fatalf("successful retry retained stale failure metadata: %+v", spans[0])
			}
		})
	}
	page, err := store.listTraceSummaries(ctx, "redis", "orders", "trace-retry", "", 10, scopedPermissionChecker{admin: true})
	if err != nil || len(page.Items) != 2 {
		t.Fatalf("retry summaries=%+v err=%v", page, err)
	}
	for _, item := range page.Items {
		if item.Status != "success" {
			t.Fatalf("successful retry summary retained failed status: %+v", item)
		}
	}
}

func TestTraceSummaryCursorBoundsLargeCrossStreamCandidateSet(t *testing.T) {
	store := openInsightTestStore(t)
	defer store.close()
	ctx := context.Background()
	base := time.Date(2026, 8, 25, 14, 0, 0, 0, time.UTC)
	const traceCount = 150
	events := make([]lifecycleEvent, 0, traceCount*3)
	for index := 0; index < traceCount; index++ {
		traceID := fmt.Sprintf("trace-load-%03d", index)
		started := base.Add(time.Duration(index) * time.Millisecond)
		events = append(events,
			lifecycleEvent{TraceID: traceID, SpanID: "public", ConnectionID: "redis", StreamKey: "public", RegisteredAt: lifecycleTime(started), ProcessedAt: lifecycleTime(started.Add(time.Millisecond)), Outcome: "success"},
			lifecycleEvent{TraceID: traceID, SpanID: "billing", ParentSpanID: "public", ConnectionID: "redis", StreamKey: "billing", RegisteredAt: lifecycleTime(started.Add(time.Millisecond)), AcknowledgedAt: lifecycleTime(started.Add(2 * time.Millisecond)), Outcome: "success"},
			lifecycleEvent{TraceID: traceID, SpanID: "secret", ParentSpanID: "public", ConnectionID: "redis", StreamKey: "secret", RegisteredAt: lifecycleTime(started), ProcessedAt: lifecycleTime(started.Add(3 * time.Millisecond)), Outcome: "failed", Error: "private"},
		)
	}
	if err := store.ingestLifecycleBatch(ctx, events); err != nil {
		t.Fatal(err)
	}
	checker := scopedPermissionChecker{role: "viewer", grants: []scopedPermissionGrant{{
		action: "streams:read", scope: redisStreamScope("redis", "secret"), effect: "deny",
	}}}
	seen := map[string]bool{}
	cursor := ""
	for pageNumber := 0; ; pageNumber++ {
		if pageNumber > traceCount {
			t.Fatal("trace cursor did not converge")
		}
		page, err := store.listTraceSummaries(ctx, "redis", "public", "trace-load", cursor, 17, checker)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Items) == 0 || len(page.Items) > 17 {
			t.Fatalf("candidate page was not bounded: page=%d items=%d", pageNumber, len(page.Items))
		}
		for _, item := range page.Items {
			if seen[item.TraceID] {
				t.Fatalf("trace %s was repeated across cursor pages", item.TraceID)
			}
			seen[item.TraceID] = true
			if item.SpanCount != 2 || item.StreamCount != 2 || item.Status != "success" {
				t.Fatalf("cross-stream ACL aggregate=%+v", item)
			}
		}
		if !page.HasMore {
			break
		}
		if page.NextCursor == "" || page.NextCursor == cursor {
			t.Fatalf("invalid next cursor on page %d: %q", pageNumber, page.NextCursor)
		}
		cursor = page.NextCursor
	}
	if len(seen) != traceCount {
		t.Fatalf("cursor returned %d unique traces, want %d", len(seen), traceCount)
	}
}

func TestTraceSummaryCursorUsesFilteredMatchRecency(t *testing.T) {
	store := openInsightTestStore(t)
	defer store.close()
	ctx := context.Background()
	base := time.Date(2026, 8, 25, 14, 30, 0, 0, time.UTC)
	events := []lifecycleEvent{
		{TraceID: "trace-filter-older", SpanID: "public", ConnectionID: "redis", StreamKey: "public", RegisteredAt: lifecycleTime(base), ProcessedAt: lifecycleTime(base.Add(time.Millisecond)), Outcome: "success"},
		{TraceID: "trace-filter-older", SpanID: "billing", ParentSpanID: "public", ConnectionID: "redis", StreamKey: "billing", RegisteredAt: lifecycleTime(base), ProcessedAt: lifecycleTime(base.Add(time.Millisecond)), Outcome: "success"},
		{TraceID: "trace-filter-newer", SpanID: "public", ConnectionID: "redis", StreamKey: "public", RegisteredAt: lifecycleTime(base), ProcessedAt: lifecycleTime(base.Add(time.Millisecond)), Outcome: "success"},
		{TraceID: "trace-filter-newer", SpanID: "billing", ParentSpanID: "public", ConnectionID: "redis", StreamKey: "billing", RegisteredAt: lifecycleTime(base), ProcessedAt: lifecycleTime(base.Add(time.Millisecond)), Outcome: "success"},
	}
	if err := store.ingestLifecycleBatch(ctx, events); err != nil {
		t.Fatal(err)
	}
	updates := []struct {
		traceID string
		spanID  string
		offset  time.Duration
	}{
		{"trace-filter-older", "public", time.Second},
		// The unfiltered billing span makes this summary newest, but must not
		// change ordering or the cursor for a public-stream search.
		{"trace-filter-older", "billing", 10 * time.Second},
		{"trace-filter-newer", "public", 2 * time.Second},
		{"trace-filter-newer", "billing", 2 * time.Second},
	}
	for _, update := range updates {
		if _, err := store.db.ExecContext(ctx, `UPDATE trace_spans SET updated_at=? WHERE trace_id=? AND span_id=?`,
			base.Add(update.offset).UnixNano(), update.traceID, update.spanID); err != nil {
			t.Fatal(err)
		}
	}
	checker := scopedPermissionChecker{admin: true}
	first, err := store.listTraceSummaries(ctx, "redis", "public", "trace-filter", "", 1, checker)
	if err != nil || len(first.Items) != 1 || first.Items[0].TraceID != "trace-filter-newer" || !first.HasMore {
		t.Fatalf("first filtered page=%+v err=%v", first, err)
	}
	second, err := store.listTraceSummaries(ctx, "redis", "public", "trace-filter", first.NextCursor, 1, checker)
	if err != nil || len(second.Items) != 1 || second.Items[0].TraceID != "trace-filter-older" || second.HasMore {
		t.Fatalf("second filtered page=%+v err=%v", second, err)
	}
	if !second.Items[0].UpdatedAt.Equal(base.Add(10 * time.Second)) {
		t.Fatalf("cross-stream summary recency was lost: %+v", second.Items[0])
	}
}

func TestSavedDashboardsEnforceOwnerAndBoundedDefinition(t *testing.T) {
	store := openInsightTestStore(t)
	defer store.close()
	ctx := context.Background()
	definition := dashboardDefinition{
		TimeRange: "15m",
		Targets: []dashboardTarget{
			{ConnectionID: "redis", StreamKey: "orders"},
			{ConnectionID: "redis", StreamKey: "orders"},
			{ConnectionID: "analytics", StreamKey: "events"},
		},
		Widgets: []string{"lag", "rates", "lag"},
	}
	created, err := store.createDashboard(ctx, "Operations", "owner", true, definition)
	if err != nil {
		t.Fatal(err)
	}
	if len(created.Definition.Targets) != 2 || len(created.Definition.Widgets) != 2 {
		t.Fatalf("dashboard definition was not normalized: %+v", created)
	}
	shared, err := store.listDashboards(ctx, sessionRecord{UserID: "viewer", Role: "viewer"})
	if err != nil || len(shared) != 1 {
		t.Fatalf("shared dashboards=%+v err=%v", shared, err)
	}
	if _, err := store.updateDashboard(ctx, created.ID, "viewer", "viewer", "Hijacked", false, definition); !errors.Is(err, errDashboardForbidden) {
		t.Fatalf("non-owner update error=%v", err)
	}
	if err := store.deleteDashboard(ctx, created.ID, "admin", "admin"); err != nil {
		t.Fatal(err)
	}
}

func TestDashboardDefinitionRejectsUnboundedOrUnknownContent(t *testing.T) {
	targets := make([]dashboardTarget, dashboardMaximumTargets+1)
	for index := range targets {
		targets[index] = dashboardTarget{ConnectionID: "redis", StreamKey: "stream-" + string(rune('a'+index))}
	}
	if _, err := normalizeDashboardDefinition(dashboardDefinition{TimeRange: "5m", Targets: targets}); err == nil {
		t.Fatal("expected too many targets to be rejected")
	}
	if _, err := normalizeDashboardDefinition(dashboardDefinition{TimeRange: "5m", Targets: targets[:1], Widgets: []string{"arbitrary-html"}}); err == nil {
		t.Fatal("expected unknown widget to be rejected")
	}
}

func TestSavedDashboardQuotasAndListBound(t *testing.T) {
	definition := dashboardDefinition{TimeRange: "5m", Targets: []dashboardTarget{{ConnectionID: "redis", StreamKey: "orders"}}, Widgets: []string{"lag"}}

	t.Run("per owner", func(t *testing.T) {
		store := openInsightTestStore(t)
		defer store.close()
		seedDashboardRows(t, store, "owner", dashboardMaximumPerOwner, false)
		if _, err := store.createDashboard(context.Background(), "one too many", "owner", false, definition); !errors.Is(err, errDashboardQuotaExceeded) {
			t.Fatalf("create error=%v", err)
		}
	})

	t.Run("global", func(t *testing.T) {
		store := openInsightTestStore(t)
		defer store.close()
		for index := 0; index < dashboardMaximumTotal; index++ {
			seedDashboardRows(t, store, fmt.Sprintf("owner-%d", index), 1, false)
		}
		if _, err := store.createDashboard(context.Background(), "one too many", "another-owner", false, definition); !errors.Is(err, errDashboardQuotaExceeded) {
			t.Fatalf("create error=%v", err)
		}
	})

	t.Run("shared and list", func(t *testing.T) {
		store := openInsightTestStore(t)
		defer store.close()
		for index := 0; index < dashboardMaximumShared; index++ {
			seedDashboardRows(t, store, fmt.Sprintf("shared-owner-%d", index), 1, true)
		}
		private, err := store.createDashboard(context.Background(), "private", "private-owner", false, definition)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.updateDashboard(context.Background(), private.ID, "private-owner", "viewer", private.Name, true, definition); !errors.Is(err, errDashboardQuotaExceeded) {
			t.Fatalf("share update error=%v", err)
		}
		items, err := store.listDashboards(context.Background(), sessionRecord{UserID: "viewer", Role: "viewer"})
		if err != nil {
			t.Fatal(err)
		}
		if len(items) != dashboardListMaximum {
			t.Fatalf("list length=%d, want %d", len(items), dashboardListMaximum)
		}
	})
}

func seedDashboardRows(t *testing.T, store *store, owner string, count int, shared bool) {
	t.Helper()
	definition := `{"timeRange":"5m","targets":[{"connectionId":"redis","streamKey":"orders"}],"widgets":["lag"]}`
	now := time.Now().UTC().UnixNano()
	for index := 0; index < count; index++ {
		id := fmt.Sprintf("seed-%s-%d", owner, index)
		if _, err := store.db.Exec(`INSERT INTO saved_dashboards(id,name,owner_id,shared,definition_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?)`, id, id, owner, boolToInt(shared), definition, now+int64(index), now+int64(index)); err != nil {
			t.Fatal(err)
		}
	}
}

func openInsightTestStore(t *testing.T) *store {
	t.Helper()
	result, err := openStore(appConfig{DataPath: filepath.Join(t.TempDir(), "redisstreamscope.db"), SessionTTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	return result
}
