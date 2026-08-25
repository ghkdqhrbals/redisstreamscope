package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestEvaluateAlertTransitionForDurationCooldownAndRecovery(t *testing.T) {
	start := time.Date(2026, 8, 25, 1, 2, 3, 0, time.UTC)
	rule := alertRule{
		ID: "rule-1", Enabled: true, Operator: ">", Threshold: 10,
		ForSeconds: 5, CooldownSeconds: 30,
	}
	observation := alertObservation{Available: true, Value: 11, ObservedAt: start}

	decision := evaluateAlertTransition(rule, alertEvaluationState{RuleID: rule.ID, Status: alertStateNormal}, observation, start)
	if decision.State.Status != alertStatePending || decision.OpenIncident || decision.NotifyEvent != "" {
		t.Fatalf("first breach must be pending: %+v", decision)
	}
	decision = evaluateAlertTransition(rule, decision.State, observation, start.Add(4*time.Second))
	if decision.State.Status != alertStatePending || decision.OpenIncident {
		t.Fatalf("for-duration was not satisfied: %+v", decision)
	}
	decision = evaluateAlertTransition(rule, decision.State, observation, start.Add(5*time.Second))
	if decision.State.Status != alertStateFiring || !decision.OpenIncident || decision.NotifyEvent != "firing" {
		t.Fatalf("threshold should start an incident: %+v", decision)
	}
	firing := decision.State
	decision = evaluateAlertTransition(rule, firing, observation, start.Add(34*time.Second))
	if decision.NotifyEvent != "" {
		t.Fatalf("cooldown should suppress early repeat: %+v", decision)
	}
	decision = evaluateAlertTransition(rule, decision.State, observation, start.Add(35*time.Second))
	if decision.NotifyEvent != "repeat" || decision.OpenIncident {
		t.Fatalf("cooldown should send a repeat without opening another incident: %+v", decision)
	}
	recovered := alertObservation{Available: true, Value: 9, ObservedAt: start.Add(36 * time.Second)}
	decision = evaluateAlertTransition(rule, decision.State, recovered, start.Add(36*time.Second))
	if decision.State.Status != alertStateNormal || !decision.ResolveIncident || decision.NotifyEvent != "resolved" {
		t.Fatalf("recovery should resolve the incident: %+v", decision)
	}
}

func TestEvaluateAlertTransitionMissingDataAndAcknowledgement(t *testing.T) {
	now := time.Date(2026, 8, 25, 1, 0, 0, 0, time.UTC)
	lastNotification := now.Add(-time.Hour)
	value := 99.0
	rule := alertRule{ID: "rule-1", Enabled: true, Operator: ">", Threshold: 10, CooldownSeconds: 5}
	acknowledged := alertEvaluationState{
		RuleID: rule.ID, Status: alertStateAcknowledged,
		LastNotificationAt: &lastNotification, LastValue: &value,
	}

	decision := evaluateAlertTransition(rule, acknowledged, alertObservation{}, now)
	if decision.State.Status != alertStateAcknowledged || decision.ResolveIncident || decision.NotifyEvent != "" {
		t.Fatalf("missing data must not look like recovery: %+v", decision)
	}
	decision = evaluateAlertTransition(rule, decision.State, alertObservation{Available: true, Value: 99, ObservedAt: now}, now)
	if decision.State.Status != alertStateAcknowledged || decision.NotifyEvent != "" {
		t.Fatalf("acknowledged incident must stay silent: %+v", decision)
	}
	rule.Enabled = false
	decision = evaluateAlertTransition(rule, decision.State, alertObservation{}, now)
	if decision.State.Status != alertStateNormal || !decision.ResolveIncident || decision.NotifyEvent != "resolved" {
		t.Fatalf("disabling a rule must resolve active state: %+v", decision)
	}
}

func TestSelectAlertObservationUsesWorstMatchingScope(t *testing.T) {
	rule := alertRule{Metric: alertMetricConsumerGroupLag, Operator: ">", Threshold: 10, ConnectionID: "redis", StreamKey: "orders"}
	observations := []alertObservation{
		{Metric: rule.Metric, ConnectionID: "other", StreamKey: "orders", GroupName: "a", Value: 1000, Available: true},
		{Metric: rule.Metric, ConnectionID: "redis", StreamKey: "orders", GroupName: "a", Value: 12, Available: true},
		{Metric: rule.Metric, ConnectionID: "redis", StreamKey: "orders", GroupName: "b", Value: 27, Available: true},
	}
	selected := selectAlertObservation(rule, observations)
	if !selected.Available || selected.GroupName != "b" || selected.Value != 27 {
		t.Fatalf("selected observation=%+v", selected)
	}
}

func TestWebhookURLSafetyBlocksLocalPrivateLinkLocalAndMetadata(t *testing.T) {
	lookup := func(_ context.Context, _ string, host string) ([]netip.Addr, error) {
		switch host {
		case "public.example":
			return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
		case "rebind.example":
			return []netip.Addr{netip.MustParseAddr("8.8.8.8"), netip.MustParseAddr("127.0.0.1")}, nil
		default:
			return nil, errors.New("not found")
		}
	}
	policy := webhookURLPolicy{LookupNetIP: lookup}
	blocked := []string{
		"file:///etc/passwd",
		"https://user:secret@public.example/hook",
		"https://127.0.0.1/hook",
		"https://[::1]/hook",
		"http://10.0.0.1/hook",
		"http://100.64.0.1/hook",
		"http://169.254.169.254/latest/meta-data",
		"http://100.100.100.200/latest/meta-data",
		"http://metadata.google.internal/computeMetadata/v1",
		"https://rebind.example/hook",
	}
	for _, raw := range blocked {
		if _, err := validateWebhookURL(context.Background(), raw, policy); err == nil {
			t.Errorf("unsafe URL was accepted: %s", raw)
		}
	}
	if _, err := validateWebhookURL(context.Background(), "https://public.example/hook", policy); err != nil {
		t.Fatalf("public webhook was rejected: %v", err)
	}
}

func TestAlertRuleJSONDoesNotExposeWebhookSecret(t *testing.T) {
	payload, err := json.Marshal(alertRule{ID: "rule", WebhookURL: "https://hooks.example/path?token=secret", WebhookConfigured: true})
	if err != nil {
		t.Fatal(err)
	}
	text := string(payload)
	if strings.Contains(text, "secret") || strings.Contains(text, "webhookUrl") {
		t.Fatalf("webhook secret was exposed: %s", text)
	}
	if !strings.Contains(text, `"webhookConfigured":true`) {
		t.Fatalf("webhook configuration state is missing: %s", text)
	}
}

func TestSafeWebhookClientDoesNotFollowRedirectsAndLimitsResponse(t *testing.T) {
	var redirected atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/redirect", func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, "/target", http.StatusFound)
	})
	mux.HandleFunc("/target", func(writer http.ResponseWriter, _ *http.Request) {
		redirected.Add(1)
		writer.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/large", func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(strings.Repeat("x", (64<<10)+1)))
	})
	mux.HandleFunc("/slow", func(writer http.ResponseWriter, _ *http.Request) {
		time.Sleep(150 * time.Millisecond)
		writer.WriteHeader(http.StatusNoContent)
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	client := newSafeWebhookClient(time.Second, webhookURLPolicy{AllowLoopback: true})

	status, err := client.Send(context.Background(), server.URL+"/redirect", []byte(`{"event":"test"}`))
	if err == nil || status != http.StatusFound || redirected.Load() != 0 {
		t.Fatalf("redirect status=%d err=%v followed=%d", status, err, redirected.Load())
	}
	status, err = client.Send(context.Background(), server.URL+"/large", []byte(`{"event":"test"}`))
	if err == nil || status != http.StatusOK || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("large response status=%d err=%v", status, err)
	}
	slowClient := newSafeWebhookClient(30*time.Millisecond, webhookURLPolicy{AllowLoopback: true})
	started := time.Now()
	status, err = slowClient.Send(context.Background(), server.URL+"/slow", []byte(`{"event":"test"}`))
	if err == nil || status != 0 || time.Since(started) > 120*time.Millisecond {
		t.Fatalf("timeout was not enforced: status=%d elapsed=%s err=%v", status, time.Since(started), err)
	}
}

func TestWebhookDispatcherEnqueueIsBoundedAndNonBlocking(t *testing.T) {
	dispatcher := newAlertWebhookDispatcher(nil, nil, 1, 1)
	notification := alertNotification{Rule: alertRule{WebhookURL: "https://8.8.8.8/hook"}}
	if !dispatcher.Enqueue(notification) {
		t.Fatal("first notification should fit in the queue")
	}
	started := time.Now()
	if dispatcher.Enqueue(notification) {
		t.Fatal("full queue should reject, not block")
	}
	if elapsed := time.Since(started); elapsed > 20*time.Millisecond {
		t.Fatalf("full queue blocked for %s", elapsed)
	}
}

type capturedAlertNotifications struct {
	mu    sync.Mutex
	items []alertNotification
}

func (sink *capturedAlertNotifications) Enqueue(notification alertNotification) bool {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	sink.items = append(sink.items, notification)
	return true
}

func (sink *capturedAlertNotifications) snapshot() []alertNotification {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	return append([]alertNotification(nil), sink.items...)
}

func TestAlertServicePersistsIncidentAcknowledgementAndResolution(t *testing.T) {
	config := appConfig{DataPath: filepath.Join(t.TempDir(), "redisstreamscope.db"), SessionTTL: time.Hour}
	store, err := openStore(config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	ctx := context.Background()
	enabled := true
	rule, err := store.createAlertRule(ctx, alertRuleInput{
		Name: "orders lag", Metric: alertMetricConsumerGroupLag, Operator: ">", Threshold: 10,
		ConnectionID: "redis", StreamKey: "orders", GroupName: "workers",
		ForSeconds: 2, CooldownSeconds: 30, Severity: "critical", Enabled: &enabled,
		WebhookURL: "https://8.8.8.8/hook",
	}, "admin")
	if err != nil {
		t.Fatal(err)
	}
	sink := &capturedAlertNotifications{}
	service, err := newAlertServiceWithNotifier(store, sink)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 8, 25, 3, 0, 0, 0, time.UTC)
	bad := alertObservation{
		Metric: alertMetricConsumerGroupLag, ConnectionID: "redis", StreamKey: "orders",
		GroupName: "workers", Value: 25, Available: true, ObservedAt: start,
	}
	if err := service.EvaluateAt(ctx, start, []alertObservation{bad}); err != nil {
		t.Fatal(err)
	}
	if incidents, err := store.listAlertIncidents(ctx, "", 10); err != nil || len(incidents) != 0 {
		t.Fatalf("pending incidents=%+v err=%v", incidents, err)
	}
	if err := service.EvaluateAt(ctx, start.Add(2*time.Second), []alertObservation{bad}); err != nil {
		t.Fatal(err)
	}
	incidents, err := store.listAlertIncidents(ctx, alertIncidentFiring, 10)
	if err != nil || len(incidents) != 1 || incidents[0].RuleID != rule.ID {
		t.Fatalf("firing incidents=%+v err=%v", incidents, err)
	}
	acknowledged, err := store.acknowledgeAlertIncident(ctx, incidents[0].ID, "operator", start.Add(3*time.Second))
	if err != nil || acknowledged.Status != alertIncidentAcknowledged {
		t.Fatalf("acknowledged=%+v err=%v", acknowledged, err)
	}
	if err := service.EvaluateAt(ctx, start.Add(time.Minute), []alertObservation{bad}); err != nil {
		t.Fatal(err)
	}
	good := bad
	good.Value, good.ObservedAt = 1, start.Add(61*time.Second)
	if err := service.EvaluateAt(ctx, start.Add(61*time.Second), []alertObservation{good}); err != nil {
		t.Fatal(err)
	}
	resolved, err := store.listAlertIncidents(ctx, alertIncidentResolved, 10)
	if err != nil || len(resolved) != 1 || resolved[0].AcknowledgedBy != "operator" {
		t.Fatalf("resolved incidents=%+v err=%v", resolved, err)
	}
	summary, err := store.alertDashboardSummary(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Firing != 0 || summary.Acknowledged != 0 || summary.Resolved != 1 || summary.EnabledRules != 1 || summary.WebhookFailures != 0 {
		t.Fatalf("alert dashboard summary=%+v", summary)
	}
	notifications := sink.snapshot()
	if len(notifications) != 2 || notifications[0].Event != "firing" || notifications[1].Event != "resolved" {
		t.Fatalf("notifications=%+v", notifications)
	}
	views, err := store.listAlertRuleViews(ctx)
	if err != nil || len(views) != 1 || views[0].State != alertStateNormal || views[0].LastValue == nil || *views[0].LastValue != 1 {
		t.Fatalf("rule views=%+v err=%v", views, err)
	}
}

func TestRuntimeAlertObservationProviderMapsOperationalAndLifecycleMetrics(t *testing.T) {
	now := time.Date(2026, 8, 25, 6, 0, 0, 0, time.UTC)
	config := appConfig{DataPath: filepath.Join(t.TempDir(), "redisstreamscope.db"), SessionTTL: time.Hour}
	store, err := openStore(config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	if err := store.migrateLifecycleTelemetry(context.Background()); err != nil {
		t.Fatal(err)
	}
	for index, duration := range []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 300 * time.Millisecond} {
		registered := now.Add(-time.Minute).Add(time.Duration(index) * time.Second)
		processed := registered.Add(duration)
		outcome, message := "success", ""
		if index == 1 {
			outcome, message = "failed", "application error"
		}
		_, err := store.db.Exec(`
			INSERT INTO request_lifecycles(
				trace_id,connection_id,stream_key,group_name,registered_at,processed_at,
				outcome,error_message,created_at,updated_at
			) VALUES(?,?,?,?,?,?,?,?,?,?)`,
			"trace-"+string(rune('a'+index)), "redis", "orders", "workers",
			registered.UnixNano(), processed.UnixNano(), outcome, message,
			registered.UnixNano(), processed.UnixNano())
		if err != nil {
			t.Fatal(err)
		}
	}
	oldestIdle := int64(90_000)
	lag := int64(25)
	backlog := int64(27)
	ping := 12.5
	nodeSampledAt := now.Add(-time.Second)
	streamSampledAt := now.Add(-time.Second)
	monitor := newOperationalMonitor()
	monitor.now = func() time.Time { return now }
	monitor.snapshots["redis"] = operationalSnapshot{
		ConnectionID: "redis", CollectedAt: now.Add(-time.Second), NodeSampledAt: &nodeSampledAt,
		StreamSampledAt: &streamSampledAt, Up: true, PingLatencyMs: &ping,
		Memory: redisMemoryHealth{UsedBytes: 90, MaxBytes: 100, PressurePct: 90},
		Streams: []streamOperationalHealth{{
			Key: "orders", Available: true, Groups: []consumerGroupOperationalHealth{{
				Name: "workers", Pending: 2, Lag: &lag, Backlog: &backlog,
				PendingSample: pendingSampleHealth{OldestPendingIdleMs: &oldestIdle, PoisonMessagesSampled: 1},
			}},
		}},
	}
	lastSuccess := now.Add(-time.Second)
	monitor.freshness["redis"] = collectorFreshnessState{
		collectorFreshness: collectorFreshness{ConnectionID: "redis", LastSuccessAt: &lastSuccess},
		lastResult:         "healthy",
	}
	provider := newRuntimeAlertObservationProvider(monitor, store)
	provider.now = func() time.Time { return now }
	provider.startedAt = now.Add(-time.Minute)
	items, err := provider.AlertObservations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assertObservationValue(t, items, alertMetricCollectorStaleSeconds, 1)
	assertObservationValue(t, items, alertMetricConnectionUp, 1)
	assertObservationValue(t, items, alertMetricMemoryUsedPercent, 90)
	assertObservationValue(t, items, alertMetricConsumerGroupLag, 25)
	assertObservationValue(t, items, alertMetricConsumerGroupPending, 2)
	assertObservationValue(t, items, alertMetricOldestPendingMs, 90_000)
	assertObservationValue(t, items, alertMetricPoisonMessages, 1)
	assertObservationValue(t, items, alertMetricDrainETASeconds, float64(10*365*24*60*60))
	assertObservationValue(t, items, alertMetricLifecycleP95Ms, 290)
	assertObservationValue(t, items, alertMetricLifecycleErrorRate, 100.0/3.0)
}

func TestRuntimeAlertObservationProviderDoesNotReuseStaleOperationalValues(t *testing.T) {
	now := time.Date(2026, 8, 25, 6, 30, 0, 0, time.UTC)
	lag := int64(99)
	ping := 42.0
	streamSampledAt := now.Add(-time.Minute)
	nodeSampledAt := now.Add(-time.Second)
	lastSuccess := now.Add(-20 * time.Second)
	monitor := newOperationalMonitor()
	monitor.now = func() time.Time { return now }
	monitor.snapshots["redis"] = operationalSnapshot{
		ConnectionID: "redis", Up: false, NodeSampledAt: &nodeSampledAt, StreamSampledAt: &streamSampledAt,
		PingLatencyMs: &ping, Memory: redisMemoryHealth{MaxBytes: 100, PressurePct: 95},
		Streams: []streamOperationalHealth{{Key: "orders", Available: true, Groups: []consumerGroupOperationalHealth{{Name: "workers", Lag: &lag, Pending: 5}}}},
	}
	monitor.freshness["redis"] = collectorFreshnessState{
		collectorFreshness: collectorFreshness{ConnectionID: "redis", LastSuccessAt: &lastSuccess},
		lastResult:         "failed",
	}
	provider := newRuntimeAlertObservationProvider(monitor, nil)
	provider.now = func() time.Time { return now }
	items, err := provider.AlertObservations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assertObservationValue(t, items, alertMetricCollectorStaleSeconds, 20)
	assertObservationValue(t, items, alertMetricConnectionUp, 0)
	for _, item := range items {
		switch item.Metric {
		case alertMetricRedisPingLatencyMs, alertMetricMemoryUsedPercent, alertMetricConsumerGroupLag, alertMetricConsumerGroupPending:
			t.Fatalf("stale operational metric was emitted: %+v", item)
		}
	}
}

func assertObservationValue(t *testing.T, items []alertObservation, metric string, expected float64) {
	t.Helper()
	for _, item := range items {
		if item.Metric == metric {
			if difference := item.Value - expected; difference < -0.0001 || difference > 0.0001 {
				t.Fatalf("%s value=%f, want %f", metric, item.Value, expected)
			}
			return
		}
	}
	t.Fatalf("metric %s was not emitted: %+v", metric, items)
}
