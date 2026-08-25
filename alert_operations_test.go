package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type failingDurableAlertSink struct {
	err error
}

func (sink *failingDurableAlertSink) Enqueue(alertNotification) bool { return false }

func (sink *failingDurableAlertSink) EnqueueDurable(context.Context, alertNotification) (bool, error) {
	return false, sink.err
}

func openAlertOperationsTestStore(t *testing.T) *store {
	t.Helper()
	dataStore, err := openStore(appConfig{DataPath: filepath.Join(t.TempDir(), "redisstreamscope.db"), SessionTTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dataStore.close() })
	return dataStore
}

func TestAlertSelectorAndSuppressionMatching(t *testing.T) {
	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	notification := alertNotification{
		Rule:        alertRule{Metric: alertMetricConsumerGroupLag, Severity: "critical", ConnectionID: "redis", StreamKey: "orders", GroupName: "workers"},
		Observation: alertObservation{Labels: map[string]string{"environment": "production", "region": "ap-northeast-2"}},
	}
	selector := alertSelector{
		Severities: []string{"critical"}, Metrics: []string{alertMetricConsumerGroupLag},
		ConnectionID: "redis", StreamKey: "orders", GroupName: "workers",
		Labels: map[string]string{"environment": "production"},
	}
	if !alertSelectorMatches(selector, notification, "") {
		t.Fatal("matching selector was rejected")
	}
	notification.Rule.ConnectionID, notification.Rule.StreamKey, notification.Rule.GroupName = "", "", ""
	notification.Observation.ConnectionID, notification.Observation.StreamKey, notification.Observation.GroupName = "redis", "orders", "workers"
	if !alertSelectorMatches(selector, notification, "") {
		t.Fatal("selector did not use the actual observation scope for a broad rule")
	}
	selector.Labels["environment"] = "staging"
	if alertSelectorMatches(selector, notification, "") {
		t.Fatal("mismatching label selector was accepted")
	}
	suppression := alertSuppression{
		Metric: alertMetricConsumerGroupLag, Severity: "critical", ConnectionID: "redis",
		StreamKey: "orders", Labels: map[string]string{"region": "ap-northeast-2"},
		StartsAt: now.Add(-time.Minute), EndsAt: now.Add(time.Minute), Enabled: true,
	}
	if !alertSuppressionMatches(suppression, notification, "", now) {
		t.Fatal("active matching suppression was rejected")
	}
	if alertSuppressionMatches(suppression, notification, "", suppression.EndsAt) {
		t.Fatal("suppression end must be exclusive")
	}
	suppression.StartsAt = now.Add(time.Second)
	if alertSuppressionMatches(suppression, notification, "", now) {
		t.Fatal("future suppression was active")
	}
}

func TestSilenceAndMaintenanceWindowsPersistAndMatchOnlyWhileActive(t *testing.T) {
	dataStore := openAlertOperationsTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	input := alertSuppressionInput{
		Name: "orders deploy", Metric: alertMetricConsumerGroupLag, Severity: "warning",
		ConnectionID: "redis", StreamKey: "orders", Labels: map[string]string{"environment": "production"},
		StartsAt: now.Add(-time.Minute), EndsAt: now.Add(time.Minute), Reason: "deployment",
	}
	for _, kind := range []string{"silence", "maintenance"} {
		item, err := dataStore.createAlertSuppression(ctx, kind, input, "admin")
		if err != nil {
			t.Fatalf("create %s: %v", kind, err)
		}
		if item.CreatedBy != "admin" || item.Reason != "deployment" {
			t.Fatalf("persisted %s=%+v", kind, item)
		}
		active, err := dataStore.listAlertSuppressions(ctx, kind, &now, 10)
		if err != nil || len(active) != 1 || active[0].ID != item.ID {
			t.Fatalf("active %s=%+v err=%v", kind, active, err)
		}
		future := now.Add(2 * time.Minute)
		active, err = dataStore.listAlertSuppressions(ctx, kind, &future, 10)
		if err != nil || len(active) != 0 {
			t.Fatalf("expired %s=%+v err=%v", kind, active, err)
		}
	}
	notification := alertNotification{
		SentAt: now, Rule: alertRule{Metric: alertMetricConsumerGroupLag, Severity: "warning", ConnectionID: "redis", StreamKey: "orders"},
		Observation: alertObservation{Labels: map[string]string{"environment": "production"}},
	}
	suppressed, err := dataStore.alertNotificationSuppressed(ctx, notification, "", now)
	if err != nil || !suppressed {
		t.Fatalf("notification suppressed=%v err=%v", suppressed, err)
	}
}

func TestAlertWebhookRouteSecretsAreRedactedAndDestinationCanBePreservedOnUpdate(t *testing.T) {
	dataStore := openAlertOperationsTestStore(t)
	ctx := context.Background()
	enabled := true
	route, err := dataStore.createAlertWebhookRoute(ctx, alertWebhookRouteInput{
		Name:         "primary on-call",
		Selector:     alertSelector{Severities: []string{"critical"}, Metrics: []string{alertMetricConsumerGroupLag}},
		Destinations: []alertRouteDestinationInput{{Name: "pager", WebhookURL: "https://8.8.8.8/hooks?token=super-secret", Enabled: &enabled}},
	}, "admin")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(route)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "super-secret") || strings.Contains(string(encoded), "webhookUrl") {
		t.Fatalf("route leaked a webhook secret: %s", encoded)
	}
	if !strings.Contains(string(encoded), `"webhookConfigured":true`) {
		t.Fatalf("redacted route omitted configuration state: %s", encoded)
	}
	listed, err := dataStore.listAlertWebhookRoutes(ctx, false)
	if err != nil || len(listed) != 1 || listed[0].Destinations[0].WebhookURL != "" {
		t.Fatalf("redacted routes=%+v err=%v", listed, err)
	}
	updated, err := dataStore.updateAlertWebhookRoute(ctx, route.ID, alertWebhookRouteInput{
		Name: route.Name, Selector: route.Selector,
		Destinations: []alertRouteDestinationInput{{ID: route.Destinations[0].ID, Name: "pager renamed", Enabled: &enabled}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated.Destinations[0].WebhookURL == "" {
		t.Fatal("omitted webhook URL did not preserve the existing secret")
	}
	stored, err := dataStore.getAlertWebhookRoute(ctx, route.ID, true)
	if err != nil || !strings.Contains(stored.Destinations[0].WebhookURL, "super-secret") {
		t.Fatalf("stored route=%+v err=%v", stored, err)
	}
}

func TestAlertWebhookRoutingMatchesScopeAndFansOutDestinations(t *testing.T) {
	dataStore := openAlertOperationsTestStore(t)
	ctx := context.Background()
	_, err := dataStore.createAlertWebhookRoute(ctx, alertWebhookRouteInput{
		Name: "production lag",
		Selector: alertSelector{
			Severities: []string{"critical"}, Metrics: []string{alertMetricConsumerGroupLag},
			ConnectionID: "redis", StreamKey: "orders", Labels: map[string]string{"environment": "production"},
		},
		Destinations: []alertRouteDestinationInput{
			{Name: "on-call", WebhookURL: "https://8.8.8.8/on-call"},
			{Name: "audit", WebhookURL: "https://8.8.4.4/audit"},
		},
	}, "admin")
	if err != nil {
		t.Fatal(err)
	}
	notification := alertNotification{
		Rule:        alertRule{Metric: alertMetricConsumerGroupLag, Severity: "critical", ConnectionID: "redis", StreamKey: "orders"},
		Observation: alertObservation{Labels: map[string]string{"environment": "production"}},
	}
	targets, err := dataStore.matchingAlertDeliveryTargets(ctx, notification, "")
	if err != nil || len(targets) != 2 {
		t.Fatalf("targets=%+v err=%v", targets, err)
	}
	targets, err = dataStore.matchingAlertDeliveryTargets(ctx, notification, "warning")
	if err != nil || len(targets) != 0 {
		t.Fatalf("severity mismatch targets=%+v err=%v", targets, err)
	}
}

func TestAlertNotificationJobsDeduplicateClaimAndBoundRetries(t *testing.T) {
	dataStore := openAlertOperationsTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 8, 25, 13, 0, 0, 0, time.UTC)
	notification := alertNotification{
		Event: "firing", SentAt: now,
		Rule:           alertRule{ID: "rule-1", Name: "lag", Metric: alertMetricConsumerGroupLag, Severity: "critical"},
		Incident:       alertIncident{ID: "incident-1", RuleID: "rule-1", Status: alertIncidentFiring, StartedAt: now},
		DestinationURL: "https://8.8.8.8/hook", DeliveryGroupID: "group-1", DedupKey: "dedup-1",
	}
	reserved, inserted, err := dataStore.reserveAlertNotificationJob(ctx, notification, now)
	if err != nil || !inserted {
		t.Fatalf("first reserve inserted=%v err=%v", inserted, err)
	}
	_, inserted, err = dataStore.reserveAlertNotificationJob(ctx, notification, now)
	if err != nil || inserted {
		t.Fatalf("duplicate reserve inserted=%v err=%v", inserted, err)
	}
	job, claimed, err := dataStore.claimAlertNotificationJob(ctx, reserved.JobID, now)
	if err != nil || !claimed || job.Attempts != 1 || job.State != alertJobClaimed {
		t.Fatalf("first claim=%+v claimed=%v err=%v", job, claimed, err)
	}
	if _, claimed, err := dataStore.claimAlertNotificationJob(ctx, reserved.JobID, now); err != nil || claimed {
		t.Fatalf("concurrent claim claimed=%v err=%v", claimed, err)
	}
	state, err := dataStore.completeAlertNotificationJob(ctx, job, now, http.StatusInternalServerError, errors.New("temporary"))
	if err != nil || state != alertJobRetry {
		t.Fatalf("retry state=%s err=%v", state, err)
	}
	if _, claimed, err := dataStore.claimAlertNotificationJob(ctx, reserved.JobID, now.Add(500*time.Millisecond)); err != nil || claimed {
		t.Fatalf("early retry claimed=%v err=%v", claimed, err)
	}
	job, claimed, err = dataStore.claimAlertNotificationJob(ctx, reserved.JobID, now.Add(time.Second))
	if err != nil || !claimed || job.Attempts != 2 {
		t.Fatalf("second claim=%+v claimed=%v err=%v", job, claimed, err)
	}
	state, err = dataStore.completeAlertNotificationJob(ctx, job, now.Add(time.Second), http.StatusInternalServerError, errors.New("temporary again"))
	if err != nil || state != alertJobRetry {
		t.Fatalf("second retry state=%s err=%v", state, err)
	}
	job, claimed, err = dataStore.claimAlertNotificationJob(ctx, reserved.JobID, now.Add(5*time.Second))
	if err != nil || !claimed || job.Attempts != 3 {
		t.Fatalf("third claim=%+v claimed=%v err=%v", job, claimed, err)
	}
	state, err = dataStore.completeAlertNotificationJob(ctx, job, now.Add(5*time.Second), http.StatusInternalServerError, errors.New("still failing"))
	if err != nil || state != alertJobFailed {
		t.Fatalf("bounded terminal state=%s err=%v", state, err)
	}
	if _, claimed, err := dataStore.claimAlertNotificationJob(ctx, reserved.JobID, now.Add(time.Hour)); err != nil || claimed {
		t.Fatalf("terminal job was reclaimed: claimed=%v err=%v", claimed, err)
	}
}

func TestSuppressedFiringIsRetriedAfterSilenceEndsEvenWithZeroCooldown(t *testing.T) {
	dataStore := openAlertOperationsTestStore(t)
	ctx := context.Background()
	enabled := true
	rule, err := dataStore.createAlertRule(ctx, alertRuleInput{
		Name: "orders lag", Metric: alertMetricConsumerGroupLag, Operator: ">", Threshold: 10,
		ConnectionID: "redis", StreamKey: "orders", GroupName: "workers", Severity: "critical",
		CooldownSeconds: 0, Enabled: &enabled, WebhookURL: "https://8.8.8.8/hook",
	}, "admin")
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 8, 25, 13, 30, 0, 0, time.UTC)
	_, err = dataStore.createAlertSuppression(ctx, "silence", alertSuppressionInput{
		Name: "deploy", Metric: alertMetricConsumerGroupLag, ConnectionID: "redis", StreamKey: "orders",
		StartsAt: start.Add(-time.Minute), EndsAt: start.Add(time.Minute), Reason: "deploy",
	}, "admin")
	if err != nil {
		t.Fatal(err)
	}
	sink := &capturedAlertNotifications{}
	service, err := newAlertServiceWithNotifier(dataStore, sink)
	if err != nil {
		t.Fatal(err)
	}
	bad := alertObservation{Metric: rule.Metric, ConnectionID: "redis", StreamKey: "orders", GroupName: "workers", Value: 100, Available: true, ObservedAt: start}
	if err := service.EvaluateAt(ctx, start, []alertObservation{bad}); err != nil {
		t.Fatal(err)
	}
	if got := sink.snapshot(); len(got) != 0 {
		t.Fatalf("silenced notifications=%+v", got)
	}
	bad.ObservedAt = start.Add(2 * time.Minute)
	if err := service.EvaluateAt(ctx, bad.ObservedAt, []alertObservation{bad}); err != nil {
		t.Fatal(err)
	}
	got := sink.snapshot()
	if len(got) != 1 || got[0].Event != "repeat" {
		t.Fatalf("post-silence notifications=%+v", got)
	}
}

func TestDurableReservationFailureClearsNotificationTimestampForRetry(t *testing.T) {
	dataStore := openAlertOperationsTestStore(t)
	ctx := context.Background()
	enabled := true
	rule, err := dataStore.createAlertRule(ctx, alertRuleInput{
		Name: "orders lag", Metric: alertMetricConsumerGroupLag, Operator: ">", Threshold: 10,
		Severity: "critical", Enabled: &enabled, WebhookURL: "https://8.8.8.8/hook",
	}, "admin")
	if err != nil {
		t.Fatal(err)
	}
	failing := &failingDurableAlertSink{err: errors.New("database unavailable")}
	service, err := newAlertServiceWithNotifier(dataStore, failing)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 25, 13, 45, 0, 0, time.UTC)
	observation := alertObservation{Metric: rule.Metric, ConnectionID: "redis", StreamKey: "orders", GroupName: "workers", Value: 99, Available: true, ObservedAt: now}
	if err := service.EvaluateAt(ctx, now, []alertObservation{observation}); err == nil || !strings.Contains(err.Error(), "database unavailable") {
		t.Fatalf("reservation failure err=%v", err)
	}
	var lastNotificationAt *string
	if err := dataStore.db.QueryRowContext(ctx, `SELECT last_notification_at FROM alert_rule_states WHERE rule_id=?`, rule.ID).Scan(&lastNotificationAt); err != nil {
		t.Fatal(err)
	}
	if lastNotificationAt != nil {
		t.Fatalf("failed reservation left notification timestamp=%v", *lastNotificationAt)
	}
	sink := &capturedAlertNotifications{}
	service.notifier = sink
	observation.ObservedAt = now.Add(time.Second)
	if err := service.EvaluateAt(ctx, observation.ObservedAt, []alertObservation{observation}); err != nil {
		t.Fatal(err)
	}
	items := sink.snapshot()
	if len(items) != 1 || items[0].Event != "repeat" {
		t.Fatalf("retried notifications=%+v", items)
	}
}

func TestEnabledEscalationPolicyBlocksRouteDeletionAndExposesReferences(t *testing.T) {
	dataStore := openAlertOperationsTestStore(t)
	ctx := context.Background()
	enabled := true
	route, err := dataStore.createAlertWebhookRoute(ctx, alertWebhookRouteInput{
		Name: "on-call", Destinations: []alertRouteDestinationInput{{Name: "primary", WebhookURL: "https://8.8.8.8/hook"}}, Enabled: &enabled,
	}, "admin")
	if err != nil {
		t.Fatal(err)
	}
	policyInput := alertEscalationPolicyInput{
		Name: "critical escalation", Steps: []alertEscalationStepInput{{AfterSeconds: 60, RouteID: route.ID, TargetSeverity: "critical"}}, Enabled: &enabled,
	}
	policy, err := dataStore.createAlertEscalationPolicy(ctx, policyInput, "admin")
	if err != nil {
		t.Fatal(err)
	}
	view, err := dataStore.getAlertWebhookRoute(ctx, route.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	if !view.DeleteBlocked || len(view.ReferencedByPolicies) != 1 || view.ReferencedByPolicies[0].ID != policy.ID {
		t.Fatalf("route references=%+v blocked=%v", view.ReferencedByPolicies, view.DeleteBlocked)
	}
	if deleted, err := dataStore.deleteAlertWebhookRoute(ctx, route.ID); deleted || !errors.Is(err, errAlertRouteReferenced) {
		t.Fatalf("referenced route deleted=%v err=%v", deleted, err)
	}
	recorder := httptest.NewRecorder()
	writeAlertHandlerError(recorder, errAlertRouteReferenced)
	if recorder.Code != http.StatusConflict || !strings.Contains(recorder.Body.String(), "route_in_use") {
		t.Fatalf("conflict response=%d %s", recorder.Code, recorder.Body.String())
	}
	disabled := false
	policyInput.Enabled = &disabled
	policyInput.Steps[0].ID = policy.Steps[0].ID
	if _, err := dataStore.updateAlertEscalationPolicy(ctx, policy.ID, policyInput); err != nil {
		t.Fatal(err)
	}
	if deleted, err := dataStore.deleteAlertWebhookRoute(ctx, route.ID); err != nil || !deleted {
		t.Fatalf("unreferenced route deleted=%v err=%v", deleted, err)
	}
}

func TestWildcardRuleIncidentUsesActualWorstObservationScope(t *testing.T) {
	dataStore := openAlertOperationsTestStore(t)
	ctx := context.Background()
	enabled := true
	rule, err := dataStore.createAlertRule(ctx, alertRuleInput{
		Name: "all lag", Metric: alertMetricConsumerGroupLag, Operator: ">", Threshold: 10,
		Severity: "warning", Enabled: &enabled,
	}, "admin")
	if err != nil {
		t.Fatal(err)
	}
	service, err := newAlertServiceWithNotifier(dataStore, nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 25, 14, 30, 0, 0, time.UTC)
	observations := []alertObservation{
		{Metric: rule.Metric, ConnectionID: "redis-a", StreamKey: "orders", GroupName: "workers-a", Value: 20, Available: true, ObservedAt: now},
		{Metric: rule.Metric, ConnectionID: "redis-b", StreamKey: "payments", GroupName: "workers-b", Value: 80, Available: true, ObservedAt: now},
	}
	if err := service.EvaluateAt(ctx, now, observations); err != nil {
		t.Fatal(err)
	}
	incidents, err := dataStore.listAlertIncidents(ctx, alertIncidentFiring, 10)
	if err != nil || len(incidents) != 1 {
		t.Fatalf("incidents=%+v err=%v", incidents, err)
	}
	incident := incidents[0]
	if incident.ConnectionID != "redis-b" || incident.StreamKey != "payments" || incident.GroupName != "workers-b" {
		t.Fatalf("actual scope=%+v", incident)
	}
	if !strings.Contains(incident.Summary, "redis-b/payments/workers-b") {
		t.Fatalf("summary does not contain actual scope: %s", incident.Summary)
	}
	loaded, err := dataStore.getAlertIncident(ctx, incident.ID)
	if err != nil || loaded.ConnectionID != "redis-b" || loaded.StreamKey != "payments" || loaded.GroupName != "workers-b" {
		t.Fatalf("loaded incident=%+v err=%v", loaded, err)
	}
}

func TestWebhookFailureSummaryCountsLatestUnresolvedDeliveryNotAttempts(t *testing.T) {
	dataStore := openAlertOperationsTestStore(t)
	ctx := context.Background()
	now := formatAlertTime(time.Now().UTC())
	_, err := dataStore.db.ExecContext(ctx, `INSERT INTO alert_notification_jobs(
		id,dedup_key,delivery_group_id,rule_id,incident_id,event,severity,destination_url,
		notification_json,state,attempts,max_attempts,next_attempt_at,last_error,created_at,updated_at,completed_at
	) VALUES('job','dedup','group','rule','incident','firing','critical','https://8.8.8.8','{}','failed',3,3,?,'failed',?,?,?)`, now, now, now, now)
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= 3; attempt++ {
		_, err = dataStore.db.ExecContext(ctx, `INSERT INTO alert_webhook_deliveries(
			rule_id,incident_id,event,attempt,status_code,outcome,error,created_at,completed_at,
			delivery_group_id,dedup_key
		) VALUES('rule','incident','firing',?,500,'failed','failed',?,?,'group','dedup')`, attempt, now, now)
		if err != nil {
			t.Fatal(err)
		}
	}
	summary, err := dataStore.alertDashboardSummary(ctx)
	if err != nil || summary.WebhookFailures != 1 {
		t.Fatalf("failed summary=%+v err=%v", summary, err)
	}
	_, err = dataStore.db.ExecContext(ctx, `UPDATE alert_notification_jobs SET state='delivered',last_error='',completed_at=? WHERE id='job'`, now)
	if err != nil {
		t.Fatal(err)
	}
	summary, err = dataStore.alertDashboardSummary(ctx)
	if err != nil || summary.WebhookFailures != 0 {
		t.Fatalf("recovered summary=%+v err=%v", summary, err)
	}
	_, err = dataStore.db.ExecContext(ctx, `INSERT INTO alert_webhook_deliveries(
		rule_id,incident_id,event,attempt,status_code,outcome,error,created_at,completed_at
	) VALUES
	('legacy','legacy-incident','firing',1,500,'failed','failed',?,?),
	('legacy','legacy-incident','firing',2,204,'delivered','',?,?),
	('legacy','legacy-other','firing',1,500,'failed','failed',?,?)`, now, now, now, now, now, now)
	if err != nil {
		t.Fatal(err)
	}
	summary, err = dataStore.alertDashboardSummary(ctx)
	if err != nil || summary.WebhookFailures != 1 {
		t.Fatalf("legacy latest summary=%+v err=%v", summary, err)
	}
}

func TestEscalationSchedulingIsPersistedAndDeduplicated(t *testing.T) {
	dataStore := openAlertOperationsTestStore(t)
	ctx := context.Background()
	enabled := true
	route, err := dataStore.createAlertWebhookRoute(ctx, alertWebhookRouteInput{
		Name: "escalation", Destinations: []alertRouteDestinationInput{{Name: "primary", WebhookURL: "https://8.8.8.8/escalate"}},
	}, "admin")
	if err != nil {
		t.Fatal(err)
	}
	policy, err := dataStore.createAlertEscalationPolicy(ctx, alertEscalationPolicyInput{
		Name: "critical escalation", Selector: alertSelector{Severities: []string{"critical"}, Labels: map[string]string{"environment": "production"}},
		Steps: []alertEscalationStepInput{{AfterSeconds: 60, RouteID: route.ID, TargetSeverity: "critical"}}, Enabled: &enabled,
	}, "admin")
	if err != nil {
		t.Fatal(err)
	}
	rule, err := dataStore.createAlertRule(ctx, alertRuleInput{
		Name: "orders lag", Metric: alertMetricConsumerGroupLag, Operator: ">", Threshold: 10,
		ConnectionID: "redis", StreamKey: "orders", GroupName: "workers", Severity: "critical", Enabled: &enabled,
	}, "admin")
	if err != nil {
		t.Fatal(err)
	}
	started := time.Date(2026, 8, 25, 14, 0, 0, 0, time.UTC)
	_, err = dataStore.db.ExecContext(ctx, `INSERT INTO alert_incidents(
		id,rule_id,status,started_at,updated_at,trigger_value,last_value,summary
	) VALUES(?,?,?,?,?,?,?,?)`, "incident-escalation", rule.ID, alertIncidentFiring,
		formatAlertTime(started), formatAlertTime(started), 50, 50, "lag is high")
	if err != nil {
		t.Fatal(err)
	}
	_, err = dataStore.db.ExecContext(ctx, `INSERT INTO alert_incident_labels(incident_id,labels_json,updated_at) VALUES(?,?,?)`,
		"incident-escalation", `{"environment":"production"}`, formatAlertTime(started))
	if err != nil {
		t.Fatal(err)
	}
	dispatcher := newAlertWebhookDispatcher(dataStore, nil, 8, 1)
	service, err := newAlertServiceWithNotifier(dataStore, &failingDurableAlertSink{err: errors.New("outbox unavailable")})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.ScheduleEscalationsAt(ctx, started.Add(59*time.Second)); err != nil {
		t.Fatal(err)
	}
	assertAlertJobCount(t, dataStore, "escalation", 0)
	if err := service.ScheduleEscalationsAt(ctx, started.Add(time.Minute)); err == nil || !strings.Contains(err.Error(), "outbox unavailable") {
		t.Fatalf("escalation reservation failure=%v", err)
	}
	assertAlertJobCount(t, dataStore, "escalation", 0)
	service.notifier = dispatcher
	if err := service.ScheduleEscalationsAt(ctx, started.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := service.ScheduleEscalationsAt(ctx, started.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	assertAlertJobCount(t, dataStore, "escalation", 1)
	var policyID, stepID, groupID string
	if err := dataStore.db.QueryRowContext(ctx, `SELECT escalation_policy_id,escalation_step_id,delivery_group_id
		FROM alert_notification_jobs WHERE event='escalation'`).Scan(&policyID, &stepID, &groupID); err != nil {
		t.Fatal(err)
	}
	if policyID != policy.ID || stepID != policy.Steps[0].ID || groupID == "" {
		t.Fatalf("policy=%s step=%s group=%s", policyID, stepID, groupID)
	}
}

func assertAlertJobCount(t *testing.T, dataStore *store, event string, expected int) {
	t.Helper()
	var count int
	if err := dataStore.db.QueryRow(`SELECT COUNT(*) FROM alert_notification_jobs WHERE event=?`, event).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != expected {
		t.Fatalf("%s jobs=%d, want %d", event, count, expected)
	}
}

func TestAdvancedAlertRetentionRemovesOnlyExpiredTerminalAndDeletedRecords(t *testing.T) {
	dataStore := openAlertOperationsTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 8, 25, 15, 0, 0, 0, time.UTC)
	old := formatAlertTime(now.Add(-alertHistoryRetention - time.Hour))
	recent := formatAlertTime(now.Add(-time.Hour))
	_, err := dataStore.db.ExecContext(ctx, `INSERT INTO alert_notification_jobs(
		id,dedup_key,delivery_group_id,rule_id,incident_id,event,severity,destination_url,
		notification_json,state,attempts,max_attempts,next_attempt_at,created_at,updated_at,completed_at
	) VALUES
		('old','old','g','r','i','firing','warning','https://8.8.8.8','{}','delivered',1,3,?,?,?,?),
		('recent','recent','g','r','i','firing','warning','https://8.8.8.8','{}','delivered',1,3,?,?,?,?),
		('pending-old','pending-old','g','r','i','firing','warning','https://8.8.8.8','{}','pending',0,3,?,?,?,NULL)`,
		old, old, old, old, recent, recent, recent, recent, old, old, old)
	if err != nil {
		t.Fatal(err)
	}
	if err := dataStore.maintainAdvancedAlertHistory(ctx, now); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := dataStore.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM alert_notification_jobs`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("retained jobs=%d, want recent terminal plus old pending", count)
	}
}
