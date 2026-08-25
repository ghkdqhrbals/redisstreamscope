package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestMaintenancePrunesBoundedLifecycleAndAlertHistory(t *testing.T) {
	dataStore, err := openStore(appConfig{DataPath: filepath.Join(t.TempDir(), "redisstreamscope.db"), SessionTTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dataStore.close() })
	ctx := context.Background()
	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	oldLifecycle := now.Add(-lifecycleHistoryRetention - time.Hour)
	currentLifecycle := now.Add(-lifecycleHistoryRetention + time.Hour)
	for _, item := range []struct {
		traceID string
		at      time.Time
	}{{"expired", oldLifecycle}, {"retained", currentLifecycle}} {
		if _, err := dataStore.db.ExecContext(ctx, `
			INSERT INTO request_lifecycles(trace_id,connection_id,stream_key,created_at,updated_at)
			VALUES(?,?,?,?,?)`, item.traceID, "redis", "orders", item.at.UnixNano(), item.at.UnixNano()); err != nil {
			t.Fatal(err)
		}
	}

	ruleID := "retention-rule"
	if _, err := dataStore.db.ExecContext(ctx, `
		INSERT INTO alert_rules(id,name,metric,operator,threshold,severity,enabled,created_at,updated_at)
		VALUES(?,?,?,?,?,?,?,?,?)`, ruleID, "Retention", alertMetricConsumerGroupLag, ">", 1, "warning", 1, formatAlertTime(now), formatAlertTime(now)); err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		id string
		at time.Time
	}{{"expired-incident", now.Add(-alertHistoryRetention - time.Hour)}, {"retained-incident", now.Add(-alertHistoryRetention + time.Hour)}} {
		if _, err := dataStore.db.ExecContext(ctx, `
			INSERT INTO alert_incidents(id,rule_id,status,started_at,updated_at,resolved_at,trigger_value,last_value,summary)
			VALUES(?,?,?,?,?,?,?,?,?)`, item.id, ruleID, alertIncidentResolved, formatAlertTime(item.at), formatAlertTime(item.at), formatAlertTime(item.at), 2, 0, "resolved"); err != nil {
			t.Fatal(err)
		}
		if _, err := dataStore.db.ExecContext(ctx, `
			INSERT INTO alert_webhook_deliveries(rule_id,incident_id,event,attempt,status_code,outcome,created_at,completed_at)
			VALUES(?,?,?,?,?,?,?,?)`, ruleID, item.id, "resolved", 1, 200, "delivered", formatAlertTime(item.at), formatAlertTime(item.at)); err != nil {
			t.Fatal(err)
		}
	}

	if err := dataStore.maintainOperationalHistory(ctx, now); err != nil {
		t.Fatal(err)
	}
	assertRetentionCount(t, dataStore, `SELECT COUNT(*) FROM request_lifecycles WHERE trace_id='expired'`, 0)
	assertRetentionCount(t, dataStore, `SELECT COUNT(*) FROM request_lifecycles WHERE trace_id='retained'`, 1)
	assertRetentionCount(t, dataStore, `SELECT COUNT(*) FROM alert_incidents WHERE id='expired-incident'`, 0)
	assertRetentionCount(t, dataStore, `SELECT COUNT(*) FROM alert_incidents WHERE id='retained-incident'`, 1)
	assertRetentionCount(t, dataStore, `SELECT COUNT(*) FROM alert_webhook_deliveries WHERE incident_id='expired-incident'`, 0)
	assertRetentionCount(t, dataStore, `SELECT COUNT(*) FROM alert_webhook_deliveries WHERE incident_id='retained-incident'`, 1)
}

func assertRetentionCount(t *testing.T, dataStore *store, query string, expected int) {
	t.Helper()
	var count int
	if err := dataStore.db.QueryRow(query).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != expected {
		t.Fatalf("count=%d, want %d for %s", count, expected, query)
	}
}
