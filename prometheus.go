package main

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	prometheusLifecycleWindow      = 5 * time.Minute
	prometheusLifecycleRecordLimit = 10_000
	prometheusLifecycleScopeLimit  = 2_048
	prometheusAlertRuleLimit       = 2_048
	prometheusStoredCacheTTL       = 5 * time.Second
)

type prometheusLifecycleScope struct {
	ConnectionID string
	StreamKey    string
	GroupName    string
	Records      []requestLifecycle
}

type prometheusAlertRule struct {
	ID           string
	Metric       string
	Severity     string
	ConnectionID string
	StreamKey    string
	GroupName    string
	Enabled      bool
	State        string
	LastValue    *float64
}

type prometheusAlertCount struct {
	State    string
	Severity string
	Enabled  bool
	Count    int64
}

type prometheusIncidentCount struct {
	Status   string
	Severity string
	Count    int64
}

// renderPrometheusStoredMetrics deliberately uses bounded lifecycle and rule
// detail queries. Aggregate alert counts remain exact, while high-cardinality
// per-rule and per-request dimensions have explicit truncation indicators.
func (s *store) renderPrometheusStoredMetrics(ctx context.Context, now time.Time) (string, error) {
	now = now.UTC()
	s.prometheusMu.Lock()
	defer s.prometheusMu.Unlock()
	if age := now.Sub(s.prometheusCachedAt); !s.prometheusCachedAt.IsZero() && age >= 0 && age < prometheusStoredCacheTTL {
		return s.prometheusCachedValue, nil
	}
	value, err := s.renderPrometheusStoredMetricsUncached(ctx, now)
	if err != nil {
		return "", err
	}
	s.prometheusCachedAt = now
	s.prometheusCachedValue = value
	return value, nil
}

func (s *store) renderPrometheusStoredMetricsUncached(ctx context.Context, now time.Time) (string, error) {
	lifecycleScopes, lifecycleTruncated, err := s.prometheusLifecycleScopes(ctx, now.UTC())
	if err != nil {
		return "", err
	}
	alertRules, alertRulesTruncated, alertCounts, incidentCounts, err := s.prometheusAlertMetrics(ctx)
	if err != nil {
		return "", err
	}

	var builder strings.Builder
	writePrometheusHeader(&builder, "redisstreamscope_stored_metrics_collection_success", "Whether lifecycle and alert metrics were read successfully from the embedded database.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_lifecycle_window_seconds", "Lookback window used for exported request lifecycle metrics.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_lifecycle_export_truncated", "Whether request lifecycle records or scopes exceeded the bounded Prometheus export limits.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_lifecycle_window_requests", "Request lifecycle records updated within the export window.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_lifecycle_window_events", "Request lifecycle events observed within the export window by event type.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_lifecycle_window_duration_observations", "Observed request lifecycle durations within the export window by phase.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_lifecycle_window_duration_seconds", "Request lifecycle duration statistic within the export window by phase and statistic.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_lifecycle_window_error_ratio", "Failed terminal requests divided by succeeded plus failed terminal requests in the export window.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_alert_rule_export_truncated", "Whether per-rule Prometheus details exceeded the bounded export limit.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_alert_rules", "Configured alert rule count by state, severity, and enabled status.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_alert_rule_state", "One for each bounded alert rule and its current state.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_alert_rule_enabled", "Whether the bounded alert rule is enabled.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_alert_rule_last_value", "Most recent observed value for the bounded alert rule; omitted until a value is observed.", "gauge")
	writePrometheusHeader(&builder, "redisstreamscope_alert_incidents", "Persisted alert incident count by status and severity.", "gauge")

	builder.WriteString("redisstreamscope_stored_metrics_collection_success 1\n")
	fmt.Fprintf(&builder, "redisstreamscope_lifecycle_window_seconds %.0f\n", prometheusLifecycleWindow.Seconds())
	fmt.Fprintf(&builder, "redisstreamscope_lifecycle_export_truncated %d\n", boolInt(lifecycleTruncated))
	for _, scope := range lifecycleScopes {
		metrics := aggregateLifecycleMetrics(
			scope.Records,
			now.Add(-prometheusLifecycleWindow),
			now,
			prometheusLifecycleWindow,
			false,
		)
		labels := map[string]string{
			"connection": scope.ConnectionID,
			"stream":     scope.StreamKey,
			"group":      scope.GroupName,
		}
		fmt.Fprintf(&builder, "redisstreamscope_lifecycle_window_requests%s %d\n", prometheusLabels(labels), metrics.Summary.Requests)
		for _, event := range []struct {
			name  string
			value int
		}{
			{name: "registered", value: metrics.Summary.Registered},
			{name: "processing_started", value: metrics.Summary.ProcessingStarted},
			{name: "processed", value: metrics.Summary.Processed},
			{name: "acknowledged", value: metrics.Summary.Acknowledged},
			{name: "succeeded", value: metrics.Summary.Succeeded},
			{name: "failed", value: metrics.Summary.Failed},
			{name: "in_flight", value: metrics.Summary.InFlight},
		} {
			eventLabels := clonePrometheusLabels(labels)
			eventLabels["event"] = event.name
			fmt.Fprintf(&builder, "redisstreamscope_lifecycle_window_events%s %d\n", prometheusLabels(eventLabels), event.value)
		}
		writePrometheusLifecycleDuration(&builder, labels, "queue", metrics.Summary.QueueDelay)
		writePrometheusLifecycleDuration(&builder, labels, "processing", metrics.Summary.Processing)
		writePrometheusLifecycleDuration(&builder, labels, "completion", metrics.Summary.Completion)
		writePrometheusLifecycleDuration(&builder, labels, "ack", metrics.Summary.AckDelay)
		writePrometheusLifecycleDuration(&builder, labels, "end_to_end", metrics.Summary.EndToEnd)
		terminal := metrics.Summary.Succeeded + metrics.Summary.Failed
		if terminal > 0 {
			fmt.Fprintf(&builder, "redisstreamscope_lifecycle_window_error_ratio%s %.6f\n", prometheusLabels(labels), float64(metrics.Summary.Failed)/float64(terminal))
		}
	}

	fmt.Fprintf(&builder, "redisstreamscope_alert_rule_export_truncated %d\n", boolInt(alertRulesTruncated))
	for _, count := range alertCounts {
		labels := prometheusLabels(map[string]string{
			"enabled":  fmt.Sprint(count.Enabled),
			"severity": count.Severity,
			"state":    count.State,
		})
		fmt.Fprintf(&builder, "redisstreamscope_alert_rules%s %d\n", labels, count.Count)
	}
	for _, rule := range alertRules {
		labels := map[string]string{
			"connection": rule.ConnectionID,
			"group":      rule.GroupName,
			"metric":     rule.Metric,
			"rule_id":    rule.ID,
			"severity":   rule.Severity,
			"state":      rule.State,
			"stream":     rule.StreamKey,
		}
		fmt.Fprintf(&builder, "redisstreamscope_alert_rule_state%s 1\n", prometheusLabels(labels))
		enabledLabels := clonePrometheusLabels(labels)
		delete(enabledLabels, "state")
		fmt.Fprintf(&builder, "redisstreamscope_alert_rule_enabled%s %d\n", prometheusLabels(enabledLabels), boolInt(rule.Enabled))
		if rule.LastValue != nil {
			fmt.Fprintf(&builder, "redisstreamscope_alert_rule_last_value%s %.6f\n", prometheusLabels(enabledLabels), *rule.LastValue)
		}
	}
	for _, count := range incidentCounts {
		labels := prometheusLabels(map[string]string{"severity": count.Severity, "status": count.Status})
		fmt.Fprintf(&builder, "redisstreamscope_alert_incidents%s %d\n", labels, count.Count)
	}
	return builder.String(), nil
}

func (s *store) prometheusLifecycleScopes(ctx context.Context, now time.Time) ([]prometheusLifecycleScope, bool, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT trace_id,connection_id,stream_key,group_name,entry_id,consumer,
			registered_at,processing_started_at,processed_at,acknowledged_at,
			outcome,error_message,attempt,created_at,updated_at
		FROM request_lifecycles
		WHERE updated_at>=?
		ORDER BY updated_at DESC,trace_id
		LIMIT ?`, now.Add(-prometheusLifecycleWindow).UnixNano(), prometheusLifecycleRecordLimit+1)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()

	scopes := make(map[string]*prometheusLifecycleScope)
	truncated := false
	rowCount := 0
	for rows.Next() {
		rowCount++
		if rowCount > prometheusLifecycleRecordLimit {
			truncated = true
			break
		}
		var record requestLifecycle
		var registered, started, processed, acknowledged sql.NullInt64
		var createdAt, updatedAt int64
		if err := rows.Scan(
			&record.TraceID, &record.ConnectionID, &record.StreamKey, &record.GroupName,
			&record.EntryID, &record.Consumer, &registered, &started, &processed,
			&acknowledged, &record.Outcome, &record.Error, &record.Attempt,
			&createdAt, &updatedAt,
		); err != nil {
			return nil, false, err
		}
		record.RegisteredAt = lifecycleTimeFromNull(registered)
		record.ProcessingStartedAt = lifecycleTimeFromNull(started)
		record.ProcessedAt = lifecycleTimeFromNull(processed)
		record.AcknowledgedAt = lifecycleTimeFromNull(acknowledged)
		record.CreatedAt = time.Unix(0, createdAt).UTC()
		record.UpdatedAt = time.Unix(0, updatedAt).UTC()
		key := record.ConnectionID + "\x00" + record.StreamKey + "\x00" + record.GroupName
		scope := scopes[key]
		if scope == nil {
			if len(scopes) >= prometheusLifecycleScopeLimit {
				truncated = true
				continue
			}
			scope = &prometheusLifecycleScope{
				ConnectionID: record.ConnectionID,
				StreamKey:    record.StreamKey,
				GroupName:    record.GroupName,
			}
			scopes[key] = scope
		}
		scope.Records = append(scope.Records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	keys := make([]string, 0, len(scopes))
	for key := range scopes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]prometheusLifecycleScope, 0, len(keys))
	for _, key := range keys {
		result = append(result, *scopes[key])
	}
	return result, truncated, nil
}

func (s *store) prometheusAlertMetrics(ctx context.Context) (
	[]prometheusAlertRule,
	bool,
	[]prometheusAlertCount,
	[]prometheusIncidentCount,
	error,
) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT r.id,r.metric,r.severity,r.connection_id,r.stream_key,r.group_name,
			r.enabled,COALESCE(st.status,'normal'),st.last_value
		FROM alert_rules r
		LEFT JOIN alert_rule_states st ON st.rule_id=r.id
		WHERE r.deleted_at IS NULL
		ORDER BY r.id
		LIMIT ?`, prometheusAlertRuleLimit+1)
	if err != nil {
		return nil, false, nil, nil, err
	}
	rules := make([]prometheusAlertRule, 0, min(prometheusAlertRuleLimit, 64))
	truncated := false
	for rows.Next() {
		if len(rules) >= prometheusAlertRuleLimit {
			truncated = true
			break
		}
		var rule prometheusAlertRule
		var enabled int
		var lastValue sql.NullFloat64
		if err := rows.Scan(
			&rule.ID, &rule.Metric, &rule.Severity, &rule.ConnectionID,
			&rule.StreamKey, &rule.GroupName, &enabled, &rule.State, &lastValue,
		); err != nil {
			rows.Close()
			return nil, false, nil, nil, err
		}
		rule.Enabled = enabled == 1
		if lastValue.Valid {
			value := lastValue.Float64
			rule.LastValue = &value
		}
		rules = append(rules, rule)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, false, nil, nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, false, nil, nil, err
	}

	alertCounts, err := s.prometheusAlertRuleCounts(ctx)
	if err != nil {
		return nil, false, nil, nil, err
	}
	incidentCounts, err := s.prometheusIncidentCounts(ctx)
	if err != nil {
		return nil, false, nil, nil, err
	}
	return rules, truncated, alertCounts, incidentCounts, nil
}

func (s *store) prometheusAlertRuleCounts(ctx context.Context) ([]prometheusAlertCount, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT COALESCE(st.status,'normal'),r.severity,r.enabled,COUNT(*)
		FROM alert_rules r
		LEFT JOIN alert_rule_states st ON st.rule_id=r.id
		WHERE r.deleted_at IS NULL
		GROUP BY COALESCE(st.status,'normal'),r.severity,r.enabled
		ORDER BY COALESCE(st.status,'normal'),r.severity,r.enabled`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]prometheusAlertCount, 0)
	for rows.Next() {
		var item prometheusAlertCount
		var enabled int
		if err := rows.Scan(&item.State, &item.Severity, &enabled, &item.Count); err != nil {
			return nil, err
		}
		item.Enabled = enabled == 1
		result = append(result, item)
	}
	return result, rows.Err()
}

func (s *store) prometheusIncidentCounts(ctx context.Context) ([]prometheusIncidentCount, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT i.status,r.severity,COUNT(*)
		FROM alert_incidents i
		JOIN alert_rules r ON r.id=i.rule_id
		GROUP BY i.status,r.severity
		ORDER BY i.status,r.severity`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]prometheusIncidentCount, 0)
	for rows.Next() {
		var item prometheusIncidentCount
		if err := rows.Scan(&item.Status, &item.Severity, &item.Count); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func writePrometheusLifecycleDuration(
	builder *strings.Builder,
	baseLabels map[string]string,
	phase string,
	stats lifecycleDurationStats,
) {
	labels := clonePrometheusLabels(baseLabels)
	labels["phase"] = phase
	fmt.Fprintf(builder, "redisstreamscope_lifecycle_window_duration_observations%s %d\n", prometheusLabels(labels), stats.Count)
	for _, statistic := range []struct {
		name         string
		milliseconds *float64
	}{
		{name: "avg", milliseconds: stats.AvgMs},
		{name: "p50", milliseconds: stats.P50Ms},
		{name: "p95", milliseconds: stats.P95Ms},
		{name: "p99", milliseconds: stats.P99Ms},
		{name: "max", milliseconds: stats.MaxMs},
	} {
		if statistic.milliseconds == nil {
			continue
		}
		statLabels := clonePrometheusLabels(labels)
		statLabels["statistic"] = statistic.name
		fmt.Fprintf(builder, "redisstreamscope_lifecycle_window_duration_seconds%s %.6f\n", prometheusLabels(statLabels), *statistic.milliseconds/1000)
	}
}

func clonePrometheusLabels(values map[string]string) map[string]string {
	result := make(map[string]string, len(values)+1)
	for key, value := range values {
		result[key] = value
	}
	return result
}
