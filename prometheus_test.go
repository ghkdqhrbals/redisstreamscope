package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPrometheusStoredMetricsExportsLifecycleRulesAndIncidents(t *testing.T) {
	dataStore, err := openStore(appConfig{
		DataPath:   filepath.Join(t.TempDir(), "redisstreamscope.db"),
		SessionTTL: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dataStore.close() })

	ctx := context.Background()
	now := time.Now().UTC()
	registered := now.Add(-time.Second)
	started := registered.Add(100 * time.Millisecond)
	processed := registered.Add(300 * time.Millisecond)
	acknowledged := registered.Add(400 * time.Millisecond)
	if err := dataStore.ingestLifecycleBatch(ctx, []lifecycleEvent{{
		TraceID: "request-1", ConnectionID: "primary", StreamKey: "orders", GroupName: "workers",
		RegisteredAt: &registered, ProcessingStartedAt: &started, ProcessedAt: &processed,
		AcknowledgedAt: &acknowledged, Outcome: "success", Attempt: 1,
	}}); err != nil {
		t.Fatal(err)
	}

	enabled := true
	rule, err := dataStore.createAlertRule(ctx, alertRuleInput{
		Name: "orders lag", Metric: alertMetricConsumerGroupLag, Operator: ">", Threshold: 10,
		ConnectionID: "primary", StreamKey: "orders", GroupName: "workers",
		Severity: "critical", Enabled: &enabled,
	}, "admin")
	if err != nil {
		t.Fatal(err)
	}
	service, err := newAlertServiceWithNotifier(dataStore, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.EvaluateAt(ctx, now, []alertObservation{{
		Metric: alertMetricConsumerGroupLag, ConnectionID: "primary", StreamKey: "orders",
		GroupName: "workers", Value: 25, Available: true, ObservedAt: now,
	}}); err != nil {
		t.Fatal(err)
	}

	server := &apiServer{config: appConfig{MetricsToken: "metrics-secret"}, store: dataStore}
	t.Cleanup(func() { operationalMonitors.Delete(server) })
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	request.Header.Set("Authorization", "Bearer metrics-secret")
	response := httptest.NewRecorder()
	server.prometheusMetrics(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	output := response.Body.String()
	for _, expected := range []string{
		`redisstreamscope_stored_metrics_collection_success 1`,
		`redisstreamscope_lifecycle_window_requests{connection="primary",group="workers",stream="orders"} 1`,
		`redisstreamscope_lifecycle_window_events{connection="primary",event="acknowledged",group="workers",stream="orders"} 1`,
		`redisstreamscope_lifecycle_window_duration_seconds{connection="primary",group="workers",phase="completion",statistic="p95",stream="orders"} 0.300000`,
		`redisstreamscope_lifecycle_window_duration_seconds{connection="primary",group="workers",phase="end_to_end",statistic="p95",stream="orders"} 0.400000`,
		`redisstreamscope_lifecycle_window_error_ratio{connection="primary",group="workers",stream="orders"} 0.000000`,
		`redisstreamscope_alert_rules{enabled="true",severity="critical",state="firing"} 1`,
		fmt.Sprintf(`redisstreamscope_alert_rule_state{connection="primary",group="workers",metric="consumer_group_lag",rule_id="%s",severity="critical",state="firing",stream="orders"} 1`, rule.ID),
		fmt.Sprintf(`redisstreamscope_alert_rule_last_value{connection="primary",group="workers",metric="consumer_group_lag",rule_id="%s",severity="critical",stream="orders"} 25.000000`, rule.ID),
		`redisstreamscope_alert_incidents{severity="critical",status="firing"} 1`,
	} {
		if !strings.Contains(output, expected) {
			t.Fatalf("expected Prometheus output to contain %q\n%s", expected, output)
		}
	}
	if strings.Contains(output, rule.Name) {
		t.Fatalf("rule names should not become Prometheus labels:\n%s", output)
	}
}
