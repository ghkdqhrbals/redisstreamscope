package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Alert metric names are deliberately stable because they are persisted in
// SQLite and form part of the public API.
const (
	alertMetricCollectorStaleSeconds = "collector_stale_seconds"
	alertMetricConnectionUp          = "connection_up"
	alertMetricConsumerGroupLag      = "consumer_group_lag"
	alertMetricConsumerGroupPending  = "consumer_group_pending"
	alertMetricOldestPendingMs       = "oldest_pending_ms"
	alertMetricPoisonMessages        = "poison_messages"
	alertMetricDrainETASeconds       = "drain_eta_seconds"
	alertMetricMemoryUsedPercent     = "memory_used_percent"
	alertMetricRedisPingLatencyMs    = "redis_ping_latency_ms"
	alertMetricLifecycleP95Ms        = "lifecycle_p95_ms"
	alertMetricLifecycleErrorRate    = "lifecycle_error_rate_percent"
)

const (
	alertStateNormal       = "normal"
	alertStatePending      = "pending"
	alertStateFiring       = "firing"
	alertStateAcknowledged = "acknowledged"

	alertIncidentFiring       = "firing"
	alertIncidentAcknowledged = "acknowledged"
	alertIncidentResolved     = "resolved"
)

var supportedAlertMetrics = map[string]struct{}{
	alertMetricCollectorStaleSeconds: {},
	alertMetricConnectionUp:          {},
	alertMetricConsumerGroupLag:      {},
	alertMetricConsumerGroupPending:  {},
	alertMetricOldestPendingMs:       {},
	alertMetricPoisonMessages:        {},
	alertMetricDrainETASeconds:       {},
	alertMetricMemoryUsedPercent:     {},
	alertMetricRedisPingLatencyMs:    {},
	alertMetricLifecycleP95Ms:        {},
	alertMetricLifecycleErrorRate:    {},
}

func alertACLPattern(metric, connectionID, streamKey string) string {
	connectionID = strings.TrimSpace(connectionID)
	streamKey = strings.TrimSpace(streamKey)
	if streamKey != "" {
		if connectionID == "" {
			// A stream selector without a connection applies across connections.
			// Requiring complete stream coverage is deliberately conservative.
			return "stream:*"
		}
		return redisStreamScope(connectionID, streamKey)
	}
	switch metric {
	case alertMetricCollectorStaleSeconds, alertMetricConnectionUp,
		alertMetricMemoryUsedPercent, alertMetricRedisPingLatencyMs:
		if connectionID == "" {
			return "connection:*"
		}
		return "connection:" + connectionID
	default:
		if connectionID == "" {
			return "stream:*"
		}
		return redisStreamScope(connectionID, "*")
	}
}

func alertResourceVisible(checker scopedPermissionChecker, action, metric, connectionID, streamKey string) bool {
	return checker.allowsScopeCoverage(action, alertACLPattern(metric, connectionID, streamKey))
}

type alertRule struct {
	ID                string    `json:"id"`
	Name              string    `json:"name"`
	Metric            string    `json:"metric"`
	Operator          string    `json:"operator"`
	Threshold         float64   `json:"threshold"`
	ConnectionID      string    `json:"connectionId,omitempty"`
	StreamKey         string    `json:"streamKey,omitempty"`
	GroupName         string    `json:"groupName,omitempty"`
	ForSeconds        int64     `json:"forSeconds"`
	CooldownSeconds   int64     `json:"cooldownSeconds"`
	Severity          string    `json:"severity"`
	Enabled           bool      `json:"enabled"`
	WebhookURL        string    `json:"-"`
	WebhookConfigured bool      `json:"webhookConfigured"`
	CreatedBy         string    `json:"createdBy,omitempty"`
	CreatedAt         time.Time `json:"createdAt"`
	UpdatedAt         time.Time `json:"updatedAt"`
}

type alertRuleInput struct {
	Name            string  `json:"name"`
	Metric          string  `json:"metric"`
	Operator        string  `json:"operator"`
	Threshold       float64 `json:"threshold"`
	ConnectionID    string  `json:"connectionId,omitempty"`
	StreamKey       string  `json:"streamKey,omitempty"`
	GroupName       string  `json:"groupName,omitempty"`
	ForSeconds      int64   `json:"forSeconds"`
	CooldownSeconds int64   `json:"cooldownSeconds"`
	Severity        string  `json:"severity"`
	Enabled         *bool   `json:"enabled,omitempty"`
	WebhookURL      string  `json:"webhookUrl,omitempty"`
}

type alertIncident struct {
	ID             string     `json:"id"`
	RuleID         string     `json:"ruleId"`
	RuleName       string     `json:"ruleName,omitempty"`
	Metric         string     `json:"metric,omitempty"`
	Severity       string     `json:"severity,omitempty"`
	Status         string     `json:"status"`
	ConnectionID   string     `json:"connectionId,omitempty"`
	StreamKey      string     `json:"streamKey,omitempty"`
	GroupName      string     `json:"groupName,omitempty"`
	StartedAt      time.Time  `json:"startedAt"`
	UpdatedAt      time.Time  `json:"updatedAt"`
	ResolvedAt     *time.Time `json:"resolvedAt,omitempty"`
	AcknowledgedAt *time.Time `json:"acknowledgedAt,omitempty"`
	AcknowledgedBy string     `json:"acknowledgedBy,omitempty"`
	TriggerValue   float64    `json:"triggerValue"`
	LastValue      float64    `json:"lastValue"`
	Summary        string     `json:"summary"`
}

type alertDashboardSummary struct {
	Firing          int64 `json:"firing"`
	Acknowledged    int64 `json:"acknowledged"`
	Resolved        int64 `json:"resolved"`
	EnabledRules    int64 `json:"enabledRules"`
	WebhookFailures int64 `json:"webhookFailures"`
}

type alertEvaluationState struct {
	RuleID             string
	Status             string
	ConditionSince     *time.Time
	LastEvaluatedAt    *time.Time
	LastObservedAt     *time.Time
	LastValue          *float64
	LastTransitionAt   *time.Time
	LastNotificationAt *time.Time
}

// alertObservation is the narrow integration contract between collectors and
// the alert evaluator. Available=false means "unknown" and never resolves an
// existing incident. This prevents dependency outages from looking healthy.
type alertObservation struct {
	Metric       string            `json:"metric"`
	ConnectionID string            `json:"connectionId,omitempty"`
	StreamKey    string            `json:"streamKey,omitempty"`
	GroupName    string            `json:"groupName,omitempty"`
	Value        float64           `json:"value"`
	Available    bool              `json:"available"`
	ObservedAt   time.Time         `json:"observedAt"`
	Labels       map[string]string `json:"labels,omitempty"`
}

type alertObservationProvider interface {
	AlertObservations(context.Context) ([]alertObservation, error)
}

// runtimeAlertObservationProvider turns the bounded operational snapshots and
// explicit lifecycle telemetry into alert observations. It performs no Redis
// calls and therefore cannot add work to either collector.
type runtimeAlertObservationProvider struct {
	monitor         *operationalMonitor
	store           *store
	lifecycleWindow time.Duration
	startedAt       time.Time
	now             func() time.Time
}

func newRuntimeAlertObservationProvider(monitor *operationalMonitor, s *store) *runtimeAlertObservationProvider {
	return &runtimeAlertObservationProvider{
		monitor: monitor, store: s, lifecycleWindow: 5 * time.Minute,
		startedAt: time.Now().UTC(), now: time.Now,
	}
}

func (provider *runtimeAlertObservationProvider) AlertObservations(ctx context.Context) ([]alertObservation, error) {
	if provider == nil || provider.monitor == nil {
		return nil, errors.New("operational alert provider is not configured")
	}
	now := provider.now().UTC()
	observations := make([]alertObservation, 0, 128)
	for _, snapshot := range provider.monitor.Snapshots() {
		nodeObservedAt := snapshot.CollectedAt
		if snapshot.NodeSampledAt != nil {
			nodeObservedAt = *snapshot.NodeSampledAt
		}
		if nodeObservedAt.IsZero() {
			nodeObservedAt = now
		}
		staleSeconds := now.Sub(provider.startedAt).Seconds()
		if snapshot.Collector.AgeMs != nil {
			staleSeconds = float64(*snapshot.Collector.AgeMs) / 1000
		}
		if staleSeconds < 0 {
			staleSeconds = 0
		}
		observations = append(observations,
			alertObservation{Metric: alertMetricCollectorStaleSeconds, ConnectionID: snapshot.ConnectionID, Value: staleSeconds, Available: true, ObservedAt: now},
			alertObservation{Metric: alertMetricConnectionUp, ConnectionID: snapshot.ConnectionID, Value: boolAlertValue(snapshot.Up), Available: true, ObservedAt: nodeObservedAt},
		)
		if snapshot.Up && snapshot.PingLatencyMs != nil {
			observations = append(observations, alertObservation{Metric: alertMetricRedisPingLatencyMs, ConnectionID: snapshot.ConnectionID, Value: *snapshot.PingLatencyMs, Available: true, ObservedAt: nodeObservedAt})
		}
		if snapshot.Up && snapshot.Memory.MaxBytes > 0 {
			observations = append(observations, alertObservation{Metric: alertMetricMemoryUsedPercent, ConnectionID: snapshot.ConnectionID, Value: snapshot.Memory.PressurePct, Available: true, ObservedAt: nodeObservedAt})
		}
		streamObservedAt, streamFresh := operationalStreamObservationTime(snapshot, now)
		if !streamFresh {
			continue
		}
		for _, stream := range snapshot.Streams {
			if !stream.Available {
				continue
			}
			for _, group := range stream.Groups {
				base := alertObservation{ConnectionID: snapshot.ConnectionID, StreamKey: stream.Key, GroupName: group.Name, Available: true, ObservedAt: streamObservedAt}
				if group.Lag != nil {
					item := base
					item.Metric, item.Value = alertMetricConsumerGroupLag, float64(*group.Lag)
					observations = append(observations, item)
				}
				pending := base
				pending.Metric, pending.Value = alertMetricConsumerGroupPending, float64(group.Pending)
				observations = append(observations, pending)
				if group.PendingSample.OldestPendingIdleMs != nil {
					oldest := base
					oldest.Metric, oldest.Value = alertMetricOldestPendingMs, float64(*group.PendingSample.OldestPendingIdleMs)
					if group.PendingSample.OldestPendingIDAgeMs != nil {
						oldest.Labels = map[string]string{"idAgeMs": strconv.FormatInt(*group.PendingSample.OldestPendingIDAgeMs, 10)}
					}
					observations = append(observations, oldest)
				}
				poison := base
				poison.Metric, poison.Value = alertMetricPoisonMessages, float64(group.PendingSample.PoisonMessagesSampled)
				observations = append(observations, poison)
				if group.DrainETASeconds != nil {
					drain := base
					drain.Metric, drain.Value = alertMetricDrainETASeconds, *group.DrainETASeconds
					observations = append(observations, drain)
				} else if group.Backlog != nil && *group.Backlog > 0 {
					// A positive backlog with no observed drain rate is operationally
					// worse than a long finite ETA. A finite sentinel keeps JSON and
					// SQLite interoperable while the label preserves its meaning.
					drain := base
					drain.Metric, drain.Value = alertMetricDrainETASeconds, float64(10*365*24*60*60)
					drain.Labels = map[string]string{"estimate": "not_draining"}
					observations = append(observations, drain)
				}
			}
		}
	}
	if provider.store != nil {
		// Lifecycle instrumentation is optional. A missing/pre-upgrade lifecycle
		// table must not suppress Redis health alerts.
		if lifecycle, err := provider.lifecycleObservations(ctx, now); err == nil {
			observations = append(observations, lifecycle...)
		}
	}
	return observations, nil
}

func operationalStreamObservationTime(snapshot operationalSnapshot, now time.Time) (time.Time, bool) {
	if !snapshot.Up || !snapshot.Collector.Fresh || snapshot.StreamSampledAt == nil {
		return time.Time{}, false
	}
	observedAt := snapshot.StreamSampledAt.UTC()
	age := now.Sub(observedAt)
	if age < 0 {
		age = 0
	}
	// The operational collector samples stream metadata every 15 seconds. Two
	// missed intervals make the value unknown rather than a current observation.
	return observedAt, age <= 2*operationalStreamInterval
}

func boolAlertValue(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

type lifecycleAlertAccumulator struct {
	ConnectionID string
	StreamKey    string
	GroupName    string
	Durations    []float64
	Succeeded    int
	Failed       int
}

func (provider *runtimeAlertObservationProvider) lifecycleObservations(ctx context.Context, now time.Time) ([]alertObservation, error) {
	window := provider.lifecycleWindow
	if window <= 0 {
		window = 5 * time.Minute
	}
	rows, err := provider.store.db.QueryContext(ctx, `
		SELECT connection_id,stream_key,group_name,registered_at,processed_at,outcome,error_message
		FROM request_lifecycles
		WHERE updated_at>=?
		ORDER BY updated_at DESC LIMIT 100000`, now.Add(-window).UnixNano())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	accumulators := make(map[string]*lifecycleAlertAccumulator)
	for rows.Next() {
		var connectionID, streamKey, groupName, outcome, errorMessage string
		var registeredAt, processedAt sql.NullInt64
		if err := rows.Scan(&connectionID, &streamKey, &groupName, &registeredAt, &processedAt, &outcome, &errorMessage); err != nil {
			return nil, err
		}
		key := connectionID + "\x00" + streamKey + "\x00" + groupName
		accumulator := accumulators[key]
		if accumulator == nil {
			accumulator = &lifecycleAlertAccumulator{ConnectionID: connectionID, StreamKey: streamKey, GroupName: groupName}
			accumulators[key] = accumulator
		}
		if registeredAt.Valid && processedAt.Valid && processedAt.Int64 >= registeredAt.Int64 {
			accumulator.Durations = append(accumulator.Durations, float64(processedAt.Int64-registeredAt.Int64)/float64(time.Millisecond))
		}
		if lifecycleOutcomeSucceeded(outcome) {
			accumulator.Succeeded++
		} else if lifecycleOutcomeFailed(outcome, errorMessage) {
			accumulator.Failed++
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	observations := make([]alertObservation, 0, len(accumulators)*2)
	for _, accumulator := range accumulators {
		base := alertObservation{
			ConnectionID: accumulator.ConnectionID, StreamKey: accumulator.StreamKey,
			GroupName: accumulator.GroupName, Available: true, ObservedAt: now,
		}
		if stats := lifecycleStats(accumulator.Durations); stats.P95Ms != nil {
			latency := base
			latency.Metric, latency.Value = alertMetricLifecycleP95Ms, *stats.P95Ms
			observations = append(observations, latency)
		}
		terminal := accumulator.Succeeded + accumulator.Failed
		if terminal > 0 {
			errorRate := base
			errorRate.Metric = alertMetricLifecycleErrorRate
			errorRate.Value = float64(accumulator.Failed) / float64(terminal) * 100
			observations = append(observations, errorRate)
		}
	}
	return observations, nil
}

var (
	alertSchemaMu    sync.Mutex
	alertSchemaReady = make(map[*sql.DB]struct{})
)

// initAlertSchema is safe to call during startup and from lazy integration
// points. Keeping it idempotent makes upgrades from older database files safe.
func initAlertSchema(ctx context.Context, s *store) error {
	if s == nil || s.db == nil {
		return errors.New("alert store is not configured")
	}
	alertSchemaMu.Lock()
	defer alertSchemaMu.Unlock()
	if _, ready := alertSchemaReady[s.db]; ready {
		return nil
	}
	statements := []string{
		`CREATE TABLE IF NOT EXISTS alert_rules (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			metric TEXT NOT NULL,
			operator TEXT NOT NULL,
			threshold REAL NOT NULL,
			connection_id TEXT NOT NULL DEFAULT '',
			stream_key TEXT NOT NULL DEFAULT '',
			group_name TEXT NOT NULL DEFAULT '',
			for_seconds INTEGER NOT NULL DEFAULT 0,
			cooldown_seconds INTEGER NOT NULL DEFAULT 300,
			severity TEXT NOT NULL DEFAULT 'warning',
			enabled INTEGER NOT NULL DEFAULT 1,
			webhook_url TEXT NOT NULL DEFAULT '',
			created_by TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			deleted_at TEXT
		)`,
		`CREATE INDEX IF NOT EXISTS alert_rules_enabled_idx
			ON alert_rules(enabled, metric, connection_id, stream_key, group_name)`,
		`CREATE TABLE IF NOT EXISTS alert_rule_states (
			rule_id TEXT PRIMARY KEY REFERENCES alert_rules(id) ON DELETE CASCADE,
			status TEXT NOT NULL DEFAULT 'normal',
			condition_since TEXT,
			last_evaluated_at TEXT,
			last_observed_at TEXT,
			last_value REAL,
			last_transition_at TEXT,
			last_notification_at TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS alert_incidents (
			id TEXT PRIMARY KEY,
			rule_id TEXT NOT NULL REFERENCES alert_rules(id) ON DELETE CASCADE,
			status TEXT NOT NULL,
			started_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			resolved_at TEXT,
			acknowledged_at TEXT,
			acknowledged_by TEXT NOT NULL DEFAULT '',
			trigger_value REAL NOT NULL,
			last_value REAL NOT NULL,
			summary TEXT NOT NULL
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS alert_incidents_active_rule_idx
			ON alert_incidents(rule_id) WHERE status IN ('firing','acknowledged')`,
		`CREATE INDEX IF NOT EXISTS alert_incidents_started_idx
			ON alert_incidents(started_at DESC)`,
		`CREATE TABLE IF NOT EXISTS alert_webhook_deliveries (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			rule_id TEXT NOT NULL,
			incident_id TEXT NOT NULL,
			event TEXT NOT NULL,
			attempt INTEGER NOT NULL,
			status_code INTEGER NOT NULL DEFAULT 0,
			outcome TEXT NOT NULL,
			error TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			completed_at TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS alert_webhook_deliveries_incident_idx
			ON alert_webhook_deliveries(incident_id, id DESC)`,
	}
	statements = append(statements, alertOperationSchemaStatements()...)
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("alert schema migration: %w", err)
		}
	}
	rows, err := s.db.QueryContext(ctx, `SELECT deleted_at FROM alert_rules LIMIT 0`)
	if err == nil {
		_ = rows.Close()
	} else {
		if _, alterErr := s.db.ExecContext(ctx, `ALTER TABLE alert_rules ADD COLUMN deleted_at TEXT`); alterErr != nil {
			return fmt.Errorf("alert schema migration: %w", alterErr)
		}
	}
	if err := migrateAlertOperationSchema(ctx, s); err != nil {
		return err
	}
	alertSchemaReady[s.db] = struct{}{}
	return nil
}

func normalizeAlertRule(input alertRuleInput) alertRule {
	enabled := true
	if input.Enabled != nil {
		enabled = *input.Enabled
	}
	cooldown := input.CooldownSeconds
	if cooldown == 0 {
		cooldown = 300
	}
	severity := strings.TrimSpace(input.Severity)
	if severity == "" {
		severity = "warning"
	}
	return alertRule{
		Name:              strings.TrimSpace(input.Name),
		Metric:            strings.TrimSpace(input.Metric),
		Operator:          strings.TrimSpace(input.Operator),
		Threshold:         input.Threshold,
		ConnectionID:      strings.TrimSpace(input.ConnectionID),
		StreamKey:         strings.TrimSpace(input.StreamKey),
		GroupName:         strings.TrimSpace(input.GroupName),
		ForSeconds:        input.ForSeconds,
		CooldownSeconds:   cooldown,
		Severity:          severity,
		Enabled:           enabled,
		WebhookURL:        strings.TrimSpace(input.WebhookURL),
		WebhookConfigured: strings.TrimSpace(input.WebhookURL) != "",
	}
}

func validateAlertRule(rule alertRule) error {
	if rule.Name == "" || len(rule.Name) > 120 {
		return errors.New("alert name must contain 1 to 120 characters")
	}
	if _, ok := supportedAlertMetrics[rule.Metric]; !ok {
		return fmt.Errorf("unsupported alert metric %q", rule.Metric)
	}
	switch rule.Operator {
	case ">", ">=", "<", "<=", "==", "!=":
	default:
		return errors.New("operator must be one of >, >=, <, <=, ==, !=")
	}
	if math.IsNaN(rule.Threshold) || math.IsInf(rule.Threshold, 0) {
		return errors.New("threshold must be a finite number")
	}
	if rule.ForSeconds < 0 || rule.ForSeconds > 7*24*60*60 {
		return errors.New("forSeconds must be between 0 and 604800")
	}
	if rule.CooldownSeconds < 0 || rule.CooldownSeconds > 30*24*60*60 {
		return errors.New("cooldownSeconds must be between 0 and 2592000")
	}
	switch rule.Severity {
	case "info", "warning", "critical":
	default:
		return errors.New("severity must be info, warning, or critical")
	}
	if len(rule.ConnectionID) > 128 || len(rule.StreamKey) > 512 || len(rule.GroupName) > 256 {
		return errors.New("alert scope is too long")
	}
	if rule.GroupName != "" && rule.StreamKey == "" {
		return errors.New("streamKey is required when groupName is set")
	}
	if len(rule.WebhookURL) > 2048 {
		return errors.New("webhookUrl is too long")
	}
	return nil
}

func (s *store) createAlertRule(ctx context.Context, input alertRuleInput, createdBy string) (alertRule, error) {
	if err := initAlertSchema(ctx, s); err != nil {
		return alertRule{}, err
	}
	rule := normalizeAlertRule(input)
	if err := validateAlertRule(rule); err != nil {
		return alertRule{}, err
	}
	if rule.WebhookURL != "" {
		if err := validateAlertWebhookURL(ctx, rule.WebhookURL); err != nil {
			return alertRule{}, fmt.Errorf("invalid webhook URL: %w", err)
		}
	}
	id, err := randomID(16)
	if err != nil {
		return alertRule{}, err
	}
	now := time.Now().UTC()
	rule.ID, rule.CreatedBy, rule.CreatedAt, rule.UpdatedAt = id, createdBy, now, now
	transaction, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return alertRule{}, err
	}
	defer transaction.Rollback()
	_, err = transaction.ExecContext(ctx, `
		INSERT INTO alert_rules(
			id,name,metric,operator,threshold,connection_id,stream_key,group_name,
			for_seconds,cooldown_seconds,severity,enabled,webhook_url,created_by,created_at,updated_at
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		rule.ID, rule.Name, rule.Metric, rule.Operator, rule.Threshold,
		rule.ConnectionID, rule.StreamKey, rule.GroupName, rule.ForSeconds,
		rule.CooldownSeconds, rule.Severity, boolInt(rule.Enabled), rule.WebhookURL,
		rule.CreatedBy, formatAlertTime(rule.CreatedAt), formatAlertTime(rule.UpdatedAt))
	if err != nil {
		return alertRule{}, err
	}
	if _, err = transaction.ExecContext(ctx, `INSERT INTO alert_rule_states(rule_id,status) VALUES(?,?)`, rule.ID, alertStateNormal); err != nil {
		return alertRule{}, err
	}
	if err := transaction.Commit(); err != nil {
		return alertRule{}, err
	}
	return rule, nil
}

func (s *store) listAlertRules(ctx context.Context) ([]alertRule, error) {
	if err := initAlertSchema(ctx, s); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id,name,metric,operator,threshold,connection_id,stream_key,group_name,
			for_seconds,cooldown_seconds,severity,enabled,webhook_url,created_by,created_at,updated_at
		FROM alert_rules WHERE deleted_at IS NULL
		ORDER BY CASE severity WHEN 'critical' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END,
			name COLLATE NOCASE,id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]alertRule, 0)
	for rows.Next() {
		rule, err := scanAlertRule(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, rule)
	}
	return items, rows.Err()
}

func (s *store) getAlertRule(ctx context.Context, id string) (alertRule, error) {
	if err := initAlertSchema(ctx, s); err != nil {
		return alertRule{}, err
	}
	return scanAlertRule(s.db.QueryRowContext(ctx, `
		SELECT id,name,metric,operator,threshold,connection_id,stream_key,group_name,
			for_seconds,cooldown_seconds,severity,enabled,webhook_url,created_by,created_at,updated_at
		FROM alert_rules WHERE id=? AND deleted_at IS NULL`, id))
}

type alertRuleScanner interface {
	Scan(...any) error
}

func scanAlertRule(scanner alertRuleScanner) (alertRule, error) {
	var rule alertRule
	var enabled int
	var createdAt, updatedAt string
	err := scanner.Scan(
		&rule.ID, &rule.Name, &rule.Metric, &rule.Operator, &rule.Threshold,
		&rule.ConnectionID, &rule.StreamKey, &rule.GroupName, &rule.ForSeconds,
		&rule.CooldownSeconds, &rule.Severity, &enabled, &rule.WebhookURL,
		&rule.CreatedBy, &createdAt, &updatedAt,
	)
	if err != nil {
		return alertRule{}, err
	}
	rule.Enabled = enabled == 1
	rule.WebhookConfigured = rule.WebhookURL != ""
	rule.CreatedAt = parseAlertTime(createdAt)
	rule.UpdatedAt = parseAlertTime(updatedAt)
	return rule, nil
}

func (s *store) updateAlertRule(ctx context.Context, id string, input alertRuleInput) (alertRule, error) {
	if err := initAlertSchema(ctx, s); err != nil {
		return alertRule{}, err
	}
	current, err := s.getAlertRule(ctx, id)
	if err != nil {
		return alertRule{}, err
	}
	rule := normalizeAlertRule(input)
	rule.ID, rule.CreatedBy, rule.CreatedAt = current.ID, current.CreatedBy, current.CreatedAt
	rule.UpdatedAt = time.Now().UTC()
	if err := validateAlertRule(rule); err != nil {
		return alertRule{}, err
	}
	if rule.WebhookURL != "" {
		if err := validateAlertWebhookURL(ctx, rule.WebhookURL); err != nil {
			return alertRule{}, fmt.Errorf("invalid webhook URL: %w", err)
		}
	}
	transaction, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return alertRule{}, err
	}
	defer transaction.Rollback()
	result, err := transaction.ExecContext(ctx, `
		UPDATE alert_rules SET name=?,metric=?,operator=?,threshold=?,connection_id=?,stream_key=?,group_name=?,
			for_seconds=?,cooldown_seconds=?,severity=?,enabled=?,webhook_url=?,updated_at=?
		WHERE id=?`,
		rule.Name, rule.Metric, rule.Operator, rule.Threshold, rule.ConnectionID,
		rule.StreamKey, rule.GroupName, rule.ForSeconds, rule.CooldownSeconds,
		rule.Severity, boolInt(rule.Enabled), rule.WebhookURL,
		formatAlertTime(rule.UpdatedAt), rule.ID)
	if err != nil {
		return alertRule{}, err
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		return alertRule{}, sql.ErrNoRows
	}
	// A changed expression starts a fresh for-duration window. It must not
	// carry an incident from a different metric or scope into the new rule.
	if current.Metric != rule.Metric || current.Operator != rule.Operator || current.Threshold != rule.Threshold ||
		current.ConnectionID != rule.ConnectionID || current.StreamKey != rule.StreamKey || current.GroupName != rule.GroupName {
		now := formatAlertTime(rule.UpdatedAt)
		_, err = transaction.ExecContext(ctx, `
			UPDATE alert_incidents SET status='resolved',updated_at=?,resolved_at=?
			WHERE rule_id=? AND status IN ('firing','acknowledged')`, now, now, rule.ID)
		if err != nil {
			return alertRule{}, err
		}
		_, err = transaction.ExecContext(ctx, `
			UPDATE alert_rule_states SET condition_since=NULL,status='normal',last_transition_at=?
			WHERE rule_id=?`, now, rule.ID)
		if err != nil {
			return alertRule{}, err
		}
	}
	if err := transaction.Commit(); err != nil {
		return alertRule{}, err
	}
	return rule, nil
}

func (s *store) deleteAlertRule(ctx context.Context, id string) (bool, error) {
	if err := initAlertSchema(ctx, s); err != nil {
		return false, err
	}
	now := time.Now().UTC()
	transaction, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer transaction.Rollback()
	result, err := transaction.ExecContext(ctx, `
		UPDATE alert_rules SET enabled=0,deleted_at=?,updated_at=? WHERE id=? AND deleted_at IS NULL`,
		formatAlertTime(now), formatAlertTime(now), id)
	if err != nil {
		return false, err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return false, nil
	}
	_, err = transaction.ExecContext(ctx, `
		UPDATE alert_incidents SET status='resolved',updated_at=?,resolved_at=?
		WHERE rule_id=? AND status IN ('firing','acknowledged')`, formatAlertTime(now), formatAlertTime(now), id)
	if err != nil {
		return false, err
	}
	_, err = transaction.ExecContext(ctx, `
		UPDATE alert_rule_states SET status='normal',condition_since=NULL,last_transition_at=? WHERE rule_id=?`,
		formatAlertTime(now), id)
	if err != nil {
		return false, err
	}
	if err := transaction.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func formatAlertTime(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

func parseAlertTime(value string) time.Time {
	parsed, _ := time.Parse(time.RFC3339Nano, value)
	return parsed
}

func scanNullableAlertTime(value sql.NullString) *time.Time {
	if !value.Valid || value.String == "" {
		return nil
	}
	parsed := parseAlertTime(value.String)
	return &parsed
}

func (s *store) listAlertIncidents(ctx context.Context, status string, limit int) ([]alertIncident, error) {
	return s.listVisibleAlertIncidents(ctx, status, limit, scopedPermissionChecker{admin: true})
}

func (s *store) listVisibleAlertIncidents(ctx context.Context, status string, limit int, checker scopedPermissionChecker) ([]alertIncident, error) {
	if err := initAlertSchema(ctx, s); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	status = strings.TrimSpace(status)
	if status != "" && status != alertIncidentFiring && status != alertIncidentAcknowledged && status != alertIncidentResolved {
		return nil, errors.New("invalid incident status")
	}
	query := `
		SELECT i.id,i.rule_id,r.name,r.metric,r.severity,i.status,
			COALESCE(NULLIF(l.connection_id,''),r.connection_id),
			COALESCE(NULLIF(l.stream_key,''),r.stream_key),
			COALESCE(NULLIF(l.group_name,''),r.group_name),
			i.started_at,i.updated_at,i.resolved_at,i.acknowledged_at,
			i.acknowledged_by,i.trigger_value,i.last_value,i.summary
		FROM alert_incidents i JOIN alert_rules r ON r.id=i.rule_id
		LEFT JOIN alert_incident_labels l ON l.incident_id=i.id`
	args := make([]any, 0, 2)
	if status != "" {
		query += ` WHERE i.status=?`
		args = append(args, status)
	}
	query += ` ORDER BY i.started_at DESC,i.id DESC`
	if checker.admin {
		query += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]alertIncident, 0, limit)
	for rows.Next() {
		item, err := scanAlertIncident(rows)
		if err != nil {
			return nil, err
		}
		if !alertResourceVisible(checker, "alerts:read", item.Metric, item.ConnectionID, item.StreamKey) {
			continue
		}
		items = append(items, item)
		if len(items) == limit {
			break
		}
	}
	return items, rows.Err()
}

func (s *store) alertDashboardSummary(ctx context.Context) (alertDashboardSummary, error) {
	if err := initAlertSchema(ctx, s); err != nil {
		return alertDashboardSummary{}, err
	}
	var result alertDashboardSummary
	err := s.db.QueryRowContext(ctx, `
		SELECT
			(SELECT COUNT(*) FROM alert_incidents WHERE status='firing'),
			(SELECT COUNT(*) FROM alert_incidents WHERE status='acknowledged'),
			(SELECT COUNT(*) FROM alert_incidents WHERE status='resolved'),
			(SELECT COUNT(*) FROM alert_rules WHERE deleted_at IS NULL AND enabled=1),
			(SELECT COUNT(*) FROM alert_notification_jobs
				WHERE state<>'delivered' AND last_error<>'') +
			(SELECT COUNT(*) FROM alert_webhook_deliveries d
				WHERE d.dedup_key='' AND d.outcome<>'delivered' AND d.id=(
					SELECT MAX(latest.id) FROM alert_webhook_deliveries latest
					WHERE latest.dedup_key='' AND latest.incident_id=d.incident_id AND latest.event=d.event
				))
	`).Scan(&result.Firing, &result.Acknowledged, &result.Resolved, &result.EnabledRules, &result.WebhookFailures)
	return result, err
}

func (s *store) visibleAlertDashboardSummary(ctx context.Context, checker scopedPermissionChecker) (alertDashboardSummary, error) {
	if checker.admin {
		return s.alertDashboardSummary(ctx)
	}
	if err := initAlertSchema(ctx, s); err != nil {
		return alertDashboardSummary{}, err
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT i.status,r.metric,
			COALESCE(NULLIF(l.connection_id,''),r.connection_id),
			COALESCE(NULLIF(l.stream_key,''),r.stream_key)
		FROM alert_incidents i JOIN alert_rules r ON r.id=i.rule_id
		LEFT JOIN alert_incident_labels l ON l.incident_id=i.id
		UNION ALL
		SELECT 'enabled',r.metric,r.connection_id,r.stream_key
		FROM alert_rules r WHERE r.deleted_at IS NULL AND r.enabled=1
		UNION ALL
		SELECT 'webhook',r.metric,
			COALESCE(NULLIF(l.connection_id,''),r.connection_id),
			COALESCE(NULLIF(l.stream_key,''),r.stream_key)
		FROM alert_notification_jobs j
		JOIN alert_incidents i ON i.id=j.incident_id
		JOIN alert_rules r ON r.id=i.rule_id
		LEFT JOIN alert_incident_labels l ON l.incident_id=i.id
		WHERE j.state<>'delivered' AND j.last_error<>''
		UNION ALL
		SELECT 'webhook',r.metric,
			COALESCE(NULLIF(l.connection_id,''),r.connection_id),
			COALESCE(NULLIF(l.stream_key,''),r.stream_key)
		FROM alert_webhook_deliveries d
		JOIN alert_incidents i ON i.id=d.incident_id
		JOIN alert_rules r ON r.id=i.rule_id
		LEFT JOIN alert_incident_labels l ON l.incident_id=i.id
		WHERE d.dedup_key='' AND d.outcome<>'delivered' AND d.id=(
			SELECT MAX(latest.id) FROM alert_webhook_deliveries latest
			WHERE latest.dedup_key='' AND latest.incident_id=d.incident_id AND latest.event=d.event
		)`)
	if err != nil {
		return alertDashboardSummary{}, err
	}
	defer rows.Close()
	var result alertDashboardSummary
	for rows.Next() {
		var kind, metric, connectionID, streamKey string
		if err := rows.Scan(&kind, &metric, &connectionID, &streamKey); err != nil {
			return alertDashboardSummary{}, err
		}
		if !alertResourceVisible(checker, "alerts:read", metric, connectionID, streamKey) {
			continue
		}
		switch kind {
		case alertIncidentFiring:
			result.Firing++
		case alertIncidentAcknowledged:
			result.Acknowledged++
		case alertIncidentResolved:
			result.Resolved++
		case "enabled":
			result.EnabledRules++
		case "webhook":
			result.WebhookFailures++
		}
	}
	return result, rows.Err()
}

type alertIncidentScanner interface {
	Scan(...any) error
}

func scanAlertIncident(scanner alertIncidentScanner) (alertIncident, error) {
	var item alertIncident
	var startedAt, updatedAt string
	var resolvedAt, acknowledgedAt sql.NullString
	err := scanner.Scan(
		&item.ID, &item.RuleID, &item.RuleName, &item.Metric, &item.Severity,
		&item.Status, &item.ConnectionID, &item.StreamKey, &item.GroupName,
		&startedAt, &updatedAt, &resolvedAt, &acknowledgedAt,
		&item.AcknowledgedBy, &item.TriggerValue, &item.LastValue, &item.Summary,
	)
	if err != nil {
		return alertIncident{}, err
	}
	item.StartedAt = parseAlertTime(startedAt)
	item.UpdatedAt = parseAlertTime(updatedAt)
	item.ResolvedAt = scanNullableAlertTime(resolvedAt)
	item.AcknowledgedAt = scanNullableAlertTime(acknowledgedAt)
	return item, nil
}

func (s *store) acknowledgeAlertIncident(ctx context.Context, id, userID string, now time.Time) (alertIncident, error) {
	if err := initAlertSchema(ctx, s); err != nil {
		return alertIncident{}, err
	}
	now = now.UTC()
	transaction, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return alertIncident{}, err
	}
	defer transaction.Rollback()
	var ruleID, status string
	if err := transaction.QueryRowContext(ctx, `SELECT rule_id,status FROM alert_incidents WHERE id=?`, id).Scan(&ruleID, &status); err != nil {
		return alertIncident{}, err
	}
	if status == alertIncidentResolved {
		return alertIncident{}, errors.New("resolved incidents cannot be acknowledged")
	}
	_, err = transaction.ExecContext(ctx, `
		UPDATE alert_incidents SET status=?,updated_at=?,acknowledged_at=?,acknowledged_by=? WHERE id=?`,
		alertIncidentAcknowledged, formatAlertTime(now), formatAlertTime(now), userID, id)
	if err != nil {
		return alertIncident{}, err
	}
	_, err = transaction.ExecContext(ctx, `UPDATE alert_rule_states SET status=?,last_transition_at=? WHERE rule_id=?`,
		alertStateAcknowledged, formatAlertTime(now), ruleID)
	if err != nil {
		return alertIncident{}, err
	}
	if err := transaction.Commit(); err != nil {
		return alertIncident{}, err
	}
	return s.getAlertIncident(ctx, id)
}

func (s *store) getAlertIncident(ctx context.Context, id string) (alertIncident, error) {
	return scanAlertIncident(s.db.QueryRowContext(ctx, `
		SELECT i.id,i.rule_id,r.name,r.metric,r.severity,i.status,
			COALESCE(NULLIF(l.connection_id,''),r.connection_id),
			COALESCE(NULLIF(l.stream_key,''),r.stream_key),
			COALESCE(NULLIF(l.group_name,''),r.group_name),
			i.started_at,i.updated_at,i.resolved_at,i.acknowledged_at,
			i.acknowledged_by,i.trigger_value,i.last_value,i.summary
		FROM alert_incidents i JOIN alert_rules r ON r.id=i.rule_id
		LEFT JOIN alert_incident_labels l ON l.incident_id=i.id WHERE i.id=?`, id))
}

type alertEvaluationDecision struct {
	State           alertEvaluationState
	OpenIncident    bool
	ResolveIncident bool
	NotifyEvent     string
	Matched         bool
}

func compareAlertValue(operator string, value, threshold float64) bool {
	switch operator {
	case ">":
		return value > threshold
	case ">=":
		return value >= threshold
	case "<":
		return value < threshold
	case "<=":
		return value <= threshold
	case "==":
		return value == threshold
	case "!=":
		return value != threshold
	default:
		return false
	}
}

// evaluateAlertTransition is pure: callers persist the returned state and
// side-effect decisions only after their transaction succeeds.
func evaluateAlertTransition(rule alertRule, current alertEvaluationState, observation alertObservation, now time.Time) alertEvaluationDecision {
	now = now.UTC()
	if current.Status == "" {
		current.Status = alertStateNormal
	}
	decision := alertEvaluationDecision{State: current}
	decision.State.RuleID = rule.ID
	decision.State.LastEvaluatedAt = alertTimePointer(now)

	if !rule.Enabled {
		if current.Status == alertStateFiring || current.Status == alertStateAcknowledged {
			decision.ResolveIncident = true
			decision.NotifyEvent = "resolved"
		}
		if current.Status != alertStateNormal {
			decision.State.LastTransitionAt = alertTimePointer(now)
		}
		decision.State.Status = alertStateNormal
		decision.State.ConditionSince = nil
		return decision
	}
	if !observation.Available || math.IsNaN(observation.Value) || math.IsInf(observation.Value, 0) {
		// Missing data is not evidence of recovery. Collector freshness and
		// connection-up rules provide explicit observations for those failures.
		return decision
	}

	decision.Matched = true
	observedAt := observation.ObservedAt.UTC()
	if observedAt.IsZero() {
		observedAt = now
	}
	value := observation.Value
	decision.State.LastObservedAt = alertTimePointer(observedAt)
	decision.State.LastValue = &value
	breached := compareAlertValue(rule.Operator, value, rule.Threshold)
	if !breached {
		if current.Status == alertStateFiring || current.Status == alertStateAcknowledged {
			decision.ResolveIncident = true
			decision.NotifyEvent = "resolved"
		}
		if current.Status != alertStateNormal {
			decision.State.LastTransitionAt = alertTimePointer(now)
		}
		decision.State.Status = alertStateNormal
		decision.State.ConditionSince = nil
		return decision
	}

	switch current.Status {
	case alertStateNormal:
		decision.State.ConditionSince = alertTimePointer(now)
		decision.State.LastTransitionAt = alertTimePointer(now)
		if rule.ForSeconds == 0 {
			decision.State.Status = alertStateFiring
			decision.State.LastNotificationAt = alertTimePointer(now)
			decision.OpenIncident = true
			decision.NotifyEvent = "firing"
		} else {
			decision.State.Status = alertStatePending
		}
	case alertStatePending:
		conditionSince := current.ConditionSince
		if conditionSince == nil {
			conditionSince = alertTimePointer(now)
			decision.State.ConditionSince = conditionSince
		}
		if now.Sub(*conditionSince) >= time.Duration(rule.ForSeconds)*time.Second {
			decision.State.Status = alertStateFiring
			decision.State.LastTransitionAt = alertTimePointer(now)
			decision.State.LastNotificationAt = alertTimePointer(now)
			decision.OpenIncident = true
			decision.NotifyEvent = "firing"
		}
	case alertStateFiring:
		if current.LastNotificationAt == nil || (rule.CooldownSeconds > 0 && now.Sub(*current.LastNotificationAt) >= time.Duration(rule.CooldownSeconds)*time.Second) {
			decision.State.LastNotificationAt = alertTimePointer(now)
			decision.NotifyEvent = "repeat"
		}
	case alertStateAcknowledged:
		// Acknowledgement silences repeats until the condition recovers.
	default:
		decision.State.Status = alertStateNormal
	}
	return decision
}

func alertTimePointer(value time.Time) *time.Time {
	value = value.UTC()
	return &value
}

func alertObservationMatches(rule alertRule, observation alertObservation) bool {
	return rule.Metric == observation.Metric &&
		(rule.ConnectionID == "" || rule.ConnectionID == observation.ConnectionID) &&
		(rule.StreamKey == "" || rule.StreamKey == observation.StreamKey) &&
		(rule.GroupName == "" || rule.GroupName == observation.GroupName)
}

func selectAlertObservation(rule alertRule, observations []alertObservation) alertObservation {
	var selected alertObservation
	found := false
	for _, observation := range observations {
		if !observation.Available || !alertObservationMatches(rule, observation) {
			continue
		}
		if !found || moreSevereAlertObservation(rule, observation.Value, selected.Value) ||
			(observation.Value == selected.Value && observation.ObservedAt.After(selected.ObservedAt)) {
			selected, found = observation, true
		}
	}
	if !found {
		return alertObservation{Metric: rule.Metric}
	}
	return selected
}

func moreSevereAlertObservation(rule alertRule, candidate, current float64) bool {
	switch rule.Operator {
	case ">", ">=":
		return candidate > current
	case "<", "<=":
		return candidate < current
	case "==":
		return math.Abs(candidate-rule.Threshold) < math.Abs(current-rule.Threshold)
	case "!=":
		return math.Abs(candidate-rule.Threshold) > math.Abs(current-rule.Threshold)
	default:
		return false
	}
}

type alertNotification struct {
	Event              string           `json:"event"`
	SentAt             time.Time        `json:"sentAt"`
	Rule               alertRule        `json:"rule"`
	Incident           alertIncident    `json:"incident"`
	Observation        alertObservation `json:"observation"`
	EffectiveSeverity  string           `json:"effectiveSeverity,omitempty"`
	RouteID            string           `json:"routeId,omitempty"`
	DestinationID      string           `json:"destinationId,omitempty"`
	DeliveryGroupID    string           `json:"deliveryGroupId,omitempty"`
	DedupKey           string           `json:"dedupKey,omitempty"`
	EscalationPolicyID string           `json:"escalationPolicyId,omitempty"`
	EscalationStepID   string           `json:"escalationStepId,omitempty"`
	DestinationURL     string           `json:"-"`
	JobID              string           `json:"-"`
}

type alertNotificationSink interface {
	Enqueue(alertNotification) bool
}

// durableAlertNotificationSink distinguishes a duplicate (already durable)
// from a persistence failure. The legacy boolean sink remains supported for
// focused tests and integrations that do not own a durable outbox.
type durableAlertNotificationSink interface {
	EnqueueDurable(context.Context, alertNotification) (bool, error)
}

type alertService struct {
	store               *store
	notifier            alertNotificationSink
	evaluateMu          sync.Mutex
	operationsStartOnce sync.Once
}

type alertServiceOptions struct {
	WebhookQueueSize int
	WebhookWorkers   int
	WebhookTimeout   time.Duration
	WebhookPolicy    webhookURLPolicy
}

func newAlertService(s *store, options alertServiceOptions) (*alertService, *alertWebhookDispatcher, error) {
	if err := initAlertSchema(context.Background(), s); err != nil {
		return nil, nil, err
	}
	client := newSafeWebhookClient(options.WebhookTimeout, options.WebhookPolicy)
	dispatcher := newAlertWebhookDispatcher(s, client, options.WebhookQueueSize, options.WebhookWorkers)
	return &alertService{store: s, notifier: dispatcher}, dispatcher, nil
}

func newAlertServiceWithNotifier(s *store, notifier alertNotificationSink) (*alertService, error) {
	if err := initAlertSchema(context.Background(), s); err != nil {
		return nil, err
	}
	return &alertService{store: s, notifier: notifier}, nil
}

// Run evaluates outside the 1s metrics collector and sends webhooks through a
// bounded queue. A slow destination therefore cannot delay Redis sampling.
func (s *alertService) Run(ctx context.Context, interval time.Duration, provider alertObservationProvider) {
	if dispatcher, ok := s.notifier.(*alertWebhookDispatcher); ok {
		go dispatcher.Run(ctx)
	}
	go s.RunAlertOperations(ctx, interval)
	if interval < time.Second {
		interval = 5 * time.Second
	}
	evaluate := func() {
		if provider == nil {
			return
		}
		evaluationCtx, cancel := context.WithTimeout(ctx, interval)
		observations, err := provider.AlertObservations(evaluationCtx)
		if err == nil {
			_ = s.EvaluateAt(evaluationCtx, time.Now().UTC(), observations)
		}
		cancel()
	}
	evaluate()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			evaluate()
		}
	}
}

func (s *alertService) EvaluateAt(ctx context.Context, now time.Time, observations []alertObservation) error {
	if s == nil || s.store == nil {
		return errors.New("alert service is not configured")
	}
	s.evaluateMu.Lock()
	defer s.evaluateMu.Unlock()
	rules, err := s.store.listAlertRules(ctx)
	if err != nil {
		return err
	}
	for _, rule := range rules {
		observation := selectAlertObservation(rule, observations)
		notification, err := s.evaluateRule(ctx, rule, observation, now.UTC())
		if err != nil {
			return err
		}
		if notification != nil && s.notifier != nil {
			if err := s.dispatchAlertNotification(ctx, *notification); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *alertService) evaluateRule(ctx context.Context, rule alertRule, observation alertObservation, now time.Time) (*alertNotification, error) {
	transaction, err := s.store.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer transaction.Rollback()
	state, err := loadAlertEvaluationState(ctx, transaction, rule.ID)
	if err != nil {
		return nil, err
	}
	decision := evaluateAlertTransition(rule, state, observation, now)
	incident, incidentErr := loadActiveAlertIncident(ctx, transaction, rule.ID)
	if incidentErr != nil && !errors.Is(incidentErr, sql.ErrNoRows) {
		return nil, incidentErr
	}
	var notification *alertNotification
	if decision.OpenIncident {
		if incidentErr == nil {
			// Defensive idempotency for an evaluator restarted between incident
			// insertion and state persistence.
			decision.OpenIncident = false
		} else {
			id, err := randomID(16)
			if err != nil {
				return nil, err
			}
			incident = alertIncident{
				ID: id, RuleID: rule.ID, RuleName: rule.Name, Metric: rule.Metric,
				Severity: rule.Severity, Status: alertIncidentFiring,
				ConnectionID: rule.ConnectionID, StreamKey: rule.StreamKey, GroupName: rule.GroupName,
				StartedAt: now, UpdatedAt: now, TriggerValue: observation.Value,
				LastValue: observation.Value, Summary: alertSummary(rule, observation),
			}
			_, err = transaction.ExecContext(ctx, `
				INSERT INTO alert_incidents(id,rule_id,status,started_at,updated_at,trigger_value,last_value,summary)
				VALUES(?,?,?,?,?,?,?,?)`, incident.ID, incident.RuleID, incident.Status,
				formatAlertTime(now), formatAlertTime(now), incident.TriggerValue, incident.LastValue, incident.Summary)
			if err != nil {
				return nil, err
			}
		}
	}
	if incident.ID != "" && observation.Available {
		if err := persistAlertIncidentObservation(ctx, transaction, incident.ID, observation, now); err != nil {
			return nil, err
		}
		if observation.ConnectionID != "" {
			incident.ConnectionID = observation.ConnectionID
		}
		if observation.StreamKey != "" {
			incident.StreamKey = observation.StreamKey
		}
		if observation.GroupName != "" {
			incident.GroupName = observation.GroupName
		}
	}
	if decision.ResolveIncident && incidentErr == nil {
		value := incident.LastValue
		if observation.Available {
			value = observation.Value
		}
		_, err = transaction.ExecContext(ctx, `
			UPDATE alert_incidents SET status=?,updated_at=?,resolved_at=?,last_value=? WHERE id=?`,
			alertIncidentResolved, formatAlertTime(now), formatAlertTime(now), value, incident.ID)
		if err != nil {
			return nil, err
		}
		incident.Status, incident.UpdatedAt, incident.LastValue = alertIncidentResolved, now, value
		incident.ResolvedAt = alertTimePointer(now)
	}
	if incidentErr == nil && observation.Available && !decision.ResolveIncident {
		_, err = transaction.ExecContext(ctx, `UPDATE alert_incidents SET updated_at=?,last_value=? WHERE id=?`,
			formatAlertTime(now), observation.Value, incident.ID)
		if err != nil {
			return nil, err
		}
		incident.UpdatedAt, incident.LastValue = now, observation.Value
	}
	if err := persistAlertEvaluationState(ctx, transaction, decision.State); err != nil {
		return nil, err
	}
	if err := transaction.Commit(); err != nil {
		return nil, err
	}
	if decision.NotifyEvent != "" && incident.ID != "" {
		notification = &alertNotification{
			Event: decision.NotifyEvent, SentAt: now, Rule: rule,
			Incident: incident, Observation: observation,
		}
	}
	return notification, nil
}

func loadAlertEvaluationState(ctx context.Context, transaction *sql.Tx, ruleID string) (alertEvaluationState, error) {
	var state alertEvaluationState
	var conditionSince, lastEvaluatedAt, lastObservedAt, lastTransitionAt, lastNotificationAt sql.NullString
	var lastValue sql.NullFloat64
	err := transaction.QueryRowContext(ctx, `
		SELECT rule_id,status,condition_since,last_evaluated_at,last_observed_at,last_value,last_transition_at,last_notification_at
		FROM alert_rule_states WHERE rule_id=?`, ruleID).Scan(
		&state.RuleID, &state.Status, &conditionSince, &lastEvaluatedAt, &lastObservedAt,
		&lastValue, &lastTransitionAt, &lastNotificationAt)
	if errors.Is(err, sql.ErrNoRows) {
		state = alertEvaluationState{RuleID: ruleID, Status: alertStateNormal}
		if _, insertErr := transaction.ExecContext(ctx, `INSERT INTO alert_rule_states(rule_id,status) VALUES(?,?)`, ruleID, alertStateNormal); insertErr != nil {
			return state, insertErr
		}
		return state, nil
	}
	if err != nil {
		return state, err
	}
	state.ConditionSince = scanNullableAlertTime(conditionSince)
	state.LastEvaluatedAt = scanNullableAlertTime(lastEvaluatedAt)
	state.LastObservedAt = scanNullableAlertTime(lastObservedAt)
	state.LastTransitionAt = scanNullableAlertTime(lastTransitionAt)
	state.LastNotificationAt = scanNullableAlertTime(lastNotificationAt)
	if lastValue.Valid {
		value := lastValue.Float64
		state.LastValue = &value
	}
	return state, nil
}

func persistAlertEvaluationState(ctx context.Context, transaction *sql.Tx, state alertEvaluationState) error {
	_, err := transaction.ExecContext(ctx, `
		INSERT INTO alert_rule_states(
			rule_id,status,condition_since,last_evaluated_at,last_observed_at,last_value,last_transition_at,last_notification_at
		) VALUES(?,?,?,?,?,?,?,?)
		ON CONFLICT(rule_id) DO UPDATE SET
			status=excluded.status,condition_since=excluded.condition_since,
			last_evaluated_at=excluded.last_evaluated_at,last_observed_at=excluded.last_observed_at,
			last_value=excluded.last_value,last_transition_at=excluded.last_transition_at,
			last_notification_at=excluded.last_notification_at`,
		state.RuleID, state.Status, nullableAlertTime(state.ConditionSince), nullableAlertTime(state.LastEvaluatedAt),
		nullableAlertTime(state.LastObservedAt), nullableAlertFloat(state.LastValue),
		nullableAlertTime(state.LastTransitionAt), nullableAlertTime(state.LastNotificationAt))
	return err
}

func loadActiveAlertIncident(ctx context.Context, transaction *sql.Tx, ruleID string) (alertIncident, error) {
	var item alertIncident
	var startedAt, updatedAt string
	var resolvedAt, acknowledgedAt sql.NullString
	err := transaction.QueryRowContext(ctx, `
		SELECT id,rule_id,status,started_at,updated_at,resolved_at,acknowledged_at,
			acknowledged_by,trigger_value,last_value,summary
		FROM alert_incidents WHERE rule_id=? AND status IN ('firing','acknowledged')
		ORDER BY started_at DESC LIMIT 1`, ruleID).Scan(
		&item.ID, &item.RuleID, &item.Status, &startedAt, &updatedAt,
		&resolvedAt, &acknowledgedAt, &item.AcknowledgedBy,
		&item.TriggerValue, &item.LastValue, &item.Summary)
	if err != nil {
		return item, err
	}
	item.StartedAt, item.UpdatedAt = parseAlertTime(startedAt), parseAlertTime(updatedAt)
	item.ResolvedAt, item.AcknowledgedAt = scanNullableAlertTime(resolvedAt), scanNullableAlertTime(acknowledgedAt)
	return item, nil
}

func nullableAlertTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return formatAlertTime(*value)
}

func nullableAlertFloat(value *float64) any {
	if value == nil {
		return nil
	}
	return *value
}

func alertSummary(rule alertRule, observation alertObservation) string {
	connectionID, streamKey, groupName := observation.ConnectionID, observation.StreamKey, observation.GroupName
	if connectionID == "" {
		connectionID = rule.ConnectionID
	}
	if streamKey == "" {
		streamKey = rule.StreamKey
	}
	if groupName == "" {
		groupName = rule.GroupName
	}
	scope := connectionID
	if streamKey != "" {
		scope += "/" + streamKey
	}
	if groupName != "" {
		scope += "/" + groupName
	}
	if scope == "" {
		scope = "all monitored resources"
	}
	return fmt.Sprintf("%s is %.3f (%s %.3f) for %s", rule.Metric, observation.Value, rule.Operator, rule.Threshold, scope)
}

type webhookURLPolicy struct {
	AllowPrivateNetworks bool
	AllowLoopback        bool
	LookupNetIP          func(context.Context, string, string) ([]netip.Addr, error)
}

func validateAlertWebhookURL(ctx context.Context, raw string) error {
	validationCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, err := validateWebhookURL(validationCtx, raw, defaultWebhookURLPolicy())
	return err
}

func defaultWebhookURLPolicy() webhookURLPolicy {
	return webhookURLPolicy{LookupNetIP: net.DefaultResolver.LookupNetIP}
}

func validateWebhookURL(ctx context.Context, raw string, policy webhookURLPolicy) (*url.URL, error) {
	if len(raw) == 0 || len(raw) > 2048 {
		return nil, errors.New("URL is empty or too long")
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, errors.New("URL cannot be parsed")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, errors.New("only http and https URLs are allowed")
	}
	if parsed.User != nil {
		return nil, errors.New("URL credentials are not allowed")
	}
	if parsed.Fragment != "" {
		return nil, errors.New("URL fragments are not allowed")
	}
	host := strings.TrimSpace(parsed.Hostname())
	if host == "" || strings.Contains(host, "%") {
		return nil, errors.New("URL host is required")
	}
	if parsed.Port() != "" {
		port, err := strconv.Atoi(parsed.Port())
		if err != nil || port < 1 || port > 65535 {
			return nil, errors.New("URL port is invalid")
		}
	}
	if isMetadataHostname(host) {
		return nil, errors.New("metadata service hosts are not allowed")
	}
	if _, err := validatedWebhookAddresses(ctx, host, policy); err != nil {
		return nil, err
	}
	return parsed, nil
}

func isMetadataHostname(host string) bool {
	host = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(host)), ".")
	return host == "metadata" || host == "metadata.google.internal" ||
		host == "instance-data" || host == "instance-data.ec2.internal" ||
		host == "metadata.azure.internal"
}

func validatedWebhookAddresses(ctx context.Context, host string, policy webhookURLPolicy) ([]netip.Addr, error) {
	if parsed, err := netip.ParseAddr(host); err == nil {
		parsed = parsed.Unmap()
		if err := validateWebhookAddress(parsed, policy); err != nil {
			return nil, err
		}
		return []netip.Addr{parsed}, nil
	}
	lookup := policy.LookupNetIP
	if lookup == nil {
		lookup = net.DefaultResolver.LookupNetIP
	}
	addresses, err := lookup(ctx, "ip", host)
	if err != nil || len(addresses) == 0 {
		return nil, errors.New("URL host could not be resolved")
	}
	result := make([]netip.Addr, 0, len(addresses))
	for _, address := range addresses {
		address = address.Unmap()
		if err := validateWebhookAddress(address, policy); err != nil {
			// Reject the entire hostname if any answer is unsafe. Choosing only a
			// safe answer would leave a DNS-rebinding window.
			return nil, err
		}
		result = append(result, address)
	}
	return result, nil
}

func validateWebhookAddress(address netip.Addr, policy webhookURLPolicy) error {
	if !address.IsValid() || address.IsUnspecified() || address.IsMulticast() {
		return errors.New("webhook address is not routable")
	}
	if address.IsLoopback() && !policy.AllowLoopback {
		return errors.New("loopback webhook addresses are not allowed")
	}
	if address.IsLinkLocalUnicast() || address.IsLinkLocalMulticast() || isMetadataAddress(address) {
		return errors.New("link-local and metadata webhook addresses are not allowed")
	}
	if (address.IsPrivate() || isCarrierGradeNAT(address)) && !policy.AllowPrivateNetworks {
		return errors.New("private webhook addresses are not allowed")
	}
	return nil
}

func isMetadataAddress(address netip.Addr) bool {
	metadata := [...]string{
		"169.254.169.254", // AWS, Azure, GCP and OpenStack
		"169.254.170.2",   // ECS task metadata
		"100.100.100.200", // Alibaba Cloud metadata
		"fd00:ec2::254",   // AWS IPv6 metadata
	}
	for _, raw := range metadata {
		if address == netip.MustParseAddr(raw) {
			return true
		}
	}
	return false
}

func isCarrierGradeNAT(address netip.Addr) bool {
	prefix := netip.MustParsePrefix("100.64.0.0/10")
	return prefix.Contains(address)
}

type alertWebhookSender interface {
	Send(context.Context, string, []byte) (int, error)
}

type safeWebhookClient struct {
	client      *http.Client
	policy      webhookURLPolicy
	timeout     time.Duration
	maxRequest  int64
	maxResponse int64
}

func newSafeWebhookClient(timeout time.Duration, policy webhookURLPolicy) *safeWebhookClient {
	if timeout <= 0 || timeout > 15*time.Second {
		timeout = 4 * time.Second
	}
	if policy.LookupNetIP == nil {
		policy.LookupNetIP = net.DefaultResolver.LookupNetIP
	}
	dialer := &net.Dialer{Timeout: minDuration(timeout, 3*time.Second), KeepAlive: -1}
	transport := &http.Transport{
		Proxy:                 nil,
		DisableKeepAlives:     true,
		ForceAttemptHTTP2:     false,
		TLSHandshakeTimeout:   minDuration(timeout, 3*time.Second),
		ResponseHeaderTimeout: minDuration(timeout, 3*time.Second),
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(address)
			if err != nil {
				return nil, errors.New("invalid webhook destination")
			}
			addresses, err := validatedWebhookAddresses(ctx, host, policy)
			if err != nil {
				return nil, err
			}
			var lastErr error
			for _, resolved := range addresses {
				connection, err := dialer.DialContext(ctx, network, net.JoinHostPort(resolved.String(), port))
				if err == nil {
					return connection, nil
				}
				lastErr = err
			}
			if lastErr == nil {
				lastErr = errors.New("webhook destination has no safe address")
			}
			return nil, lastErr
		},
	}
	return &safeWebhookClient{
		client: &http.Client{
			Timeout:   timeout,
			Transport: transport,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		policy: policy, timeout: timeout, maxRequest: 64 << 10, maxResponse: 64 << 10,
	}
}

func minDuration(first, second time.Duration) time.Duration {
	if first < second {
		return first
	}
	return second
}

func (client *safeWebhookClient) Send(ctx context.Context, rawURL string, payload []byte) (int, error) {
	if client == nil || client.client == nil {
		return 0, errors.New("webhook client is not configured")
	}
	if int64(len(payload)) > client.maxRequest {
		return 0, errors.New("webhook payload is too large")
	}
	sendCtx, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()
	parsed, err := validateWebhookURL(sendCtx, rawURL, client.policy)
	if err != nil {
		return 0, err
	}
	request, err := http.NewRequestWithContext(sendCtx, http.MethodPost, parsed.String(), bytes.NewReader(payload))
	if err != nil {
		return 0, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", "RedisStreamScope-alert-webhook/1")
	response, err := client.client.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	read, readErr := io.Copy(io.Discard, io.LimitReader(response.Body, client.maxResponse+1))
	if readErr != nil {
		return response.StatusCode, readErr
	}
	if read > client.maxResponse {
		return response.StatusCode, errors.New("webhook response is too large")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return response.StatusCode, fmt.Errorf("webhook returned HTTP %d", response.StatusCode)
	}
	return response.StatusCode, nil
}

type alertWebhookDispatcher struct {
	store     *store
	sender    alertWebhookSender
	queue     chan alertNotification
	workers   int
	startOnce sync.Once
}

type alertWebhookDelivery struct {
	ID               int64     `json:"id"`
	RuleID           string    `json:"ruleId"`
	IncidentID       string    `json:"incidentId"`
	Event            string    `json:"event"`
	Attempt          int       `json:"attempt"`
	StatusCode       int       `json:"statusCode"`
	Outcome          string    `json:"outcome"`
	Error            string    `json:"error,omitempty"`
	DeliveryGroupID  string    `json:"deliveryGroupId,omitempty"`
	DedupKey         string    `json:"dedupKey,omitempty"`
	RouteID          string    `json:"routeId,omitempty"`
	DestinationID    string    `json:"destinationId,omitempty"`
	EscalationStepID string    `json:"escalationStepId,omitempty"`
	CreatedAt        time.Time `json:"createdAt"`
	CompletedAt      time.Time `json:"completedAt"`
	ACLMetric        string    `json:"-"`
	ACLConnectionID  string    `json:"-"`
	ACLStreamKey     string    `json:"-"`
	ACLScopeKnown    int       `json:"-"`
}

func newAlertWebhookDispatcher(s *store, sender alertWebhookSender, queueSize, workers int) *alertWebhookDispatcher {
	if queueSize <= 0 || queueSize > 4096 {
		queueSize = 256
	}
	if workers <= 0 || workers > 8 {
		workers = 1
	}
	return &alertWebhookDispatcher{store: s, sender: sender, queue: make(chan alertNotification, queueSize), workers: workers}
}

func (dispatcher *alertWebhookDispatcher) Enqueue(notification alertNotification) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	accepted, err := dispatcher.EnqueueDurable(ctx, notification)
	return err == nil && accepted
}

func (dispatcher *alertWebhookDispatcher) EnqueueDurable(ctx context.Context, notification alertNotification) (bool, error) {
	if dispatcher == nil {
		return false, errors.New("alert webhook dispatcher is not configured")
	}
	if notification.DestinationURL == "" {
		notification.DestinationURL = notification.Rule.WebhookURL
	}
	if notification.DestinationURL == "" {
		return false, errors.New("notification destination is required")
	}
	if dispatcher.store != nil {
		reserved, inserted, err := dispatcher.store.reserveAlertNotificationJob(ctx, notification, time.Now().UTC())
		if err != nil {
			return false, err
		}
		if !inserted {
			// The unique dedup key proves an equivalent delivery is already
			// durable. Do not enqueue a second in-memory copy.
			return true, nil
		}
		notification = reserved
	}
	select {
	case dispatcher.queue <- notification:
		return true, nil
	default:
		if dispatcher.store != nil {
			// Queue pressure is recoverable: recoverPending will load the durable
			// pending row. Reporting success prevents a false rollback.
			return true, nil
		}
		return false, errors.New("alert notification queue is full")
	}
}

func (dispatcher *alertWebhookDispatcher) Run(ctx context.Context) {
	if dispatcher == nil {
		return
	}
	dispatcher.startOnce.Do(func() {
		for index := 0; index < dispatcher.workers; index++ {
			go dispatcher.worker(ctx)
		}
		if dispatcher.store != nil {
			go dispatcher.recoverPending(ctx)
		}
	})
	<-ctx.Done()
}

func (dispatcher *alertWebhookDispatcher) recoverPending(ctx context.Context) {
	enqueueDue := func() {
		queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		items, err := dispatcher.store.dueAlertNotificationJobs(queryCtx, time.Now().UTC(), cap(dispatcher.queue))
		cancel()
		if err != nil {
			return
		}
		for _, item := range items {
			select {
			case dispatcher.queue <- item:
			default:
				return
			}
		}
	}
	enqueueDue()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			enqueueDue()
		}
	}
}

func (dispatcher *alertWebhookDispatcher) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case notification := <-dispatcher.queue:
			dispatcher.deliver(ctx, notification)
		}
	}
}

func (dispatcher *alertWebhookDispatcher) deliver(ctx context.Context, notification alertNotification) {
	if dispatcher.sender == nil {
		return
	}
	payload, err := json.Marshal(map[string]any{
		"version": "1", "event": notification.Event, "sentAt": notification.SentAt,
		"rule": notification.Rule, "incident": notification.Incident,
		"observation": notification.Observation,
	})
	if err != nil {
		return
	}
	if dispatcher.store == nil || notification.JobID == "" {
		for attempt := 1; attempt <= 3; attempt++ {
			if ctx.Err() != nil {
				return
			}
			status, sendErr := dispatcher.sender.Send(ctx, notification.DestinationURL, payload)
			if sendErr == nil || !retryableWebhookResult(status) || attempt == 3 {
				return
			}
		}
		return
	}
	claimCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	job, claimed, err := dispatcher.store.claimAlertNotificationJob(claimCtx, notification.JobID, time.Now().UTC())
	cancel()
	if err != nil || !claimed {
		return
	}
	if job.DestinationURL == "" {
		return
	}
	if jobPayload, marshalErr := json.Marshal(map[string]any{
		"version": "1", "event": job.Notification.Event, "sentAt": job.Notification.SentAt,
		"rule": job.Notification.Rule, "incident": job.Notification.Incident,
		"observation": job.Notification.Observation, "effectiveSeverity": job.Notification.EffectiveSeverity,
		"routeId": job.Notification.RouteID, "deliveryGroupId": job.Notification.DeliveryGroupID,
	}); marshalErr == nil {
		payload = jobPayload
	}
	if ctx.Err() != nil {
		return
	}
	startedAt := time.Now().UTC()
	status, sendErr := dispatcher.sender.Send(ctx, job.DestinationURL, payload)
	outcome, message := "delivered", ""
	if sendErr != nil {
		outcome, message = "failed", truncate(sendErr.Error(), 512)
	}
	recordCtx, recordCancel := context.WithTimeout(context.Background(), 2*time.Second)
	_ = dispatcher.store.recordAlertWebhookDelivery(recordCtx, job.Notification, job.Attempts, status, outcome, message, startedAt)
	_, _ = dispatcher.store.completeAlertNotificationJob(recordCtx, job, time.Now().UTC(), status, sendErr)
	recordCancel()
}

func retryableWebhookResult(status int) bool {
	return status == 0 || status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500
}

func (s *store) recordAlertWebhookDelivery(ctx context.Context, notification alertNotification, attempt, status int, outcome, message string, startedAt time.Time) error {
	if s == nil || s.db == nil {
		return errors.New("alert store is not configured")
	}
	completedAt := time.Now().UTC()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO alert_webhook_deliveries(
			rule_id,incident_id,event,attempt,status_code,outcome,error,created_at,completed_at,
			delivery_group_id,dedup_key,route_id,destination_id,escalation_step_id
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		notification.Rule.ID, notification.Incident.ID, notification.Event,
		attempt, status, outcome, message, formatAlertTime(startedAt), formatAlertTime(completedAt),
		notification.DeliveryGroupID, notification.DedupKey, notification.RouteID,
		notification.DestinationID, notification.EscalationStepID)
	return err
}

func (s *store) listAlertWebhookDeliveries(ctx context.Context, incidentID string, limit int) ([]alertWebhookDelivery, error) {
	return s.listVisibleAlertWebhookDeliveries(ctx, incidentID, limit, scopedPermissionChecker{admin: true})
}

func (s *store) listVisibleAlertWebhookDeliveries(ctx context.Context, incidentID string, limit int, checker scopedPermissionChecker) ([]alertWebhookDelivery, error) {
	if err := initAlertSchema(ctx, s); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	query := `SELECT d.id,d.rule_id,d.incident_id,d.event,d.attempt,d.status_code,d.outcome,d.error,d.created_at,d.completed_at,
		d.delivery_group_id,d.dedup_key,d.route_id,d.destination_id,d.escalation_step_id,
		COALESCE(r.metric,''),COALESCE(NULLIF(l.connection_id,''),r.connection_id,''),
		COALESCE(NULLIF(l.stream_key,''),r.stream_key,''),CASE WHEN i.id IS NULL OR r.id IS NULL THEN 0 ELSE 1 END
		FROM alert_webhook_deliveries d
		LEFT JOIN alert_incidents i ON i.id=d.incident_id
		LEFT JOIN alert_rules r ON r.id=i.rule_id
		LEFT JOIN alert_incident_labels l ON l.incident_id=i.id`
	args := make([]any, 0, 2)
	if incidentID = strings.TrimSpace(incidentID); incidentID != "" {
		query += ` WHERE d.incident_id=?`
		args = append(args, incidentID)
	}
	query += ` ORDER BY d.id DESC`
	if checker.admin {
		query += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]alertWebhookDelivery, 0, limit)
	for rows.Next() {
		var item alertWebhookDelivery
		var createdAt, completedAt string
		if err := rows.Scan(&item.ID, &item.RuleID, &item.IncidentID, &item.Event,
			&item.Attempt, &item.StatusCode, &item.Outcome, &item.Error, &createdAt, &completedAt,
			&item.DeliveryGroupID, &item.DedupKey, &item.RouteID, &item.DestinationID,
			&item.EscalationStepID, &item.ACLMetric, &item.ACLConnectionID, &item.ACLStreamKey, &item.ACLScopeKnown); err != nil {
			return nil, err
		}
		item.CreatedAt, item.CompletedAt = parseAlertTime(createdAt), parseAlertTime(completedAt)
		if !checker.admin && (item.ACLScopeKnown != 1 || !alertResourceVisible(checker, "alerts:read", item.ACLMetric, item.ACLConnectionID, item.ACLStreamKey)) {
			continue
		}
		items = append(items, item)
		if len(items) == limit {
			break
		}
	}
	return items, rows.Err()
}

type alertRuleView struct {
	alertRule
	State              string     `json:"state"`
	ConditionSince     *time.Time `json:"conditionSince,omitempty"`
	LastEvaluatedAt    *time.Time `json:"lastEvaluatedAt,omitempty"`
	LastObservedAt     *time.Time `json:"lastObservedAt,omitempty"`
	LastValue          *float64   `json:"lastValue,omitempty"`
	LastTransitionAt   *time.Time `json:"lastTransitionAt,omitempty"`
	LastNotificationAt *time.Time `json:"lastNotificationAt,omitempty"`
}

func (s *store) listAlertRuleViews(ctx context.Context) ([]alertRuleView, error) {
	if err := initAlertSchema(ctx, s); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT r.id,r.name,r.metric,r.operator,r.threshold,r.connection_id,r.stream_key,r.group_name,
			r.for_seconds,r.cooldown_seconds,r.severity,r.enabled,r.webhook_url,r.created_by,r.created_at,r.updated_at,
			COALESCE(st.status,'normal'),st.condition_since,st.last_evaluated_at,st.last_observed_at,
			st.last_value,st.last_transition_at,st.last_notification_at
		FROM alert_rules r LEFT JOIN alert_rule_states st ON st.rule_id=r.id
		WHERE r.deleted_at IS NULL
		ORDER BY CASE r.severity WHEN 'critical' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END,
			r.name COLLATE NOCASE,r.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]alertRuleView, 0)
	for rows.Next() {
		var item alertRuleView
		var enabled int
		var createdAt, updatedAt string
		var conditionSince, lastEvaluatedAt, lastObservedAt, lastTransitionAt, lastNotificationAt sql.NullString
		var lastValue sql.NullFloat64
		if err := rows.Scan(
			&item.ID, &item.Name, &item.Metric, &item.Operator, &item.Threshold,
			&item.ConnectionID, &item.StreamKey, &item.GroupName, &item.ForSeconds,
			&item.CooldownSeconds, &item.Severity, &enabled, &item.WebhookURL,
			&item.CreatedBy, &createdAt, &updatedAt, &item.State, &conditionSince,
			&lastEvaluatedAt, &lastObservedAt, &lastValue, &lastTransitionAt, &lastNotificationAt,
		); err != nil {
			return nil, err
		}
		item.Enabled = enabled == 1
		item.WebhookConfigured = item.WebhookURL != ""
		item.CreatedAt, item.UpdatedAt = parseAlertTime(createdAt), parseAlertTime(updatedAt)
		item.ConditionSince = scanNullableAlertTime(conditionSince)
		item.LastEvaluatedAt = scanNullableAlertTime(lastEvaluatedAt)
		item.LastObservedAt = scanNullableAlertTime(lastObservedAt)
		item.LastTransitionAt = scanNullableAlertTime(lastTransitionAt)
		item.LastNotificationAt = scanNullableAlertTime(lastNotificationAt)
		if lastValue.Valid {
			value := lastValue.Float64
			item.LastValue = &value
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *store) listVisibleAlertRuleViews(ctx context.Context, checker scopedPermissionChecker) ([]alertRuleView, error) {
	items, err := s.listAlertRuleViews(ctx)
	if err != nil {
		return nil, err
	}
	visible := make([]alertRuleView, 0, len(items))
	for _, item := range items {
		if alertResourceVisible(checker, "alerts:read", item.Metric, item.ConnectionID, item.StreamKey) {
			visible = append(visible, item)
		}
	}
	return visible, nil
}

type alertMetricDefinition struct {
	Name              string   `json:"name"`
	Unit              string   `json:"unit"`
	Scope             []string `json:"scope"`
	Description       string   `json:"description"`
	SuggestedOperator string   `json:"suggestedOperator"`
	SuggestedValue    float64  `json:"suggestedValue"`
}

func alertMetricCatalog() []alertMetricDefinition {
	return []alertMetricDefinition{
		{Name: alertMetricCollectorStaleSeconds, Unit: "seconds", Scope: []string{"connection"}, Description: "Seconds since the collector last completed successfully.", SuggestedOperator: ">", SuggestedValue: 15},
		{Name: alertMetricConnectionUp, Unit: "boolean", Scope: []string{"connection"}, Description: "1 when Redis responds, otherwise 0.", SuggestedOperator: "==", SuggestedValue: 0},
		{Name: alertMetricConsumerGroupLag, Unit: "messages", Scope: []string{"connection", "stream", "group"}, Description: "Redis-reported consumer group lag.", SuggestedOperator: ">", SuggestedValue: 1000},
		{Name: alertMetricConsumerGroupPending, Unit: "messages", Scope: []string{"connection", "stream", "group"}, Description: "Entries currently assigned in the pending entries list.", SuggestedOperator: ">", SuggestedValue: 100},
		{Name: alertMetricOldestPendingMs, Unit: "milliseconds", Scope: []string{"connection", "stream", "group"}, Description: "Idle age of the oldest sampled pending entry.", SuggestedOperator: ">", SuggestedValue: 300000},
		{Name: alertMetricPoisonMessages, Unit: "messages", Scope: []string{"connection", "stream", "group"}, Description: "Sampled pending entries at or above the configured delivery-attempt threshold.", SuggestedOperator: ">", SuggestedValue: 0},
		{Name: alertMetricDrainETASeconds, Unit: "seconds", Scope: []string{"connection", "stream", "group"}, Description: "Estimated time to clear current lag at the observed consumption rate.", SuggestedOperator: ">", SuggestedValue: 900},
		{Name: alertMetricMemoryUsedPercent, Unit: "percent", Scope: []string{"connection"}, Description: "Redis used memory as a percentage of maxmemory when configured.", SuggestedOperator: ">", SuggestedValue: 85},
		{Name: alertMetricRedisPingLatencyMs, Unit: "milliseconds", Scope: []string{"connection"}, Description: "Round-trip duration of the collector PING command; not application request latency.", SuggestedOperator: ">", SuggestedValue: 100},
		{Name: alertMetricLifecycleP95Ms, Unit: "milliseconds", Scope: []string{"connection", "stream", "group"}, Description: "P95 end-to-end latency from instrumented registration to processing completion.", SuggestedOperator: ">", SuggestedValue: 1000},
		{Name: alertMetricLifecycleErrorRate, Unit: "percent", Scope: []string{"connection", "stream", "group"}, Description: "Error percentage from explicitly instrumented request lifecycle events.", SuggestedOperator: ">", SuggestedValue: 5},
	}
}

type alertRulePatch struct {
	Name            *string  `json:"name,omitempty"`
	Metric          *string  `json:"metric,omitempty"`
	Operator        *string  `json:"operator,omitempty"`
	Threshold       *float64 `json:"threshold,omitempty"`
	ConnectionID    *string  `json:"connectionId,omitempty"`
	StreamKey       *string  `json:"streamKey,omitempty"`
	GroupName       *string  `json:"groupName,omitempty"`
	ForSeconds      *int64   `json:"forSeconds,omitempty"`
	CooldownSeconds *int64   `json:"cooldownSeconds,omitempty"`
	Severity        *string  `json:"severity,omitempty"`
	Enabled         *bool    `json:"enabled,omitempty"`
	WebhookURL      *string  `json:"webhookUrl,omitempty"`
}

func inputFromAlertRule(rule alertRule) alertRuleInput {
	enabled := rule.Enabled
	return alertRuleInput{
		Name: rule.Name, Metric: rule.Metric, Operator: rule.Operator, Threshold: rule.Threshold,
		ConnectionID: rule.ConnectionID, StreamKey: rule.StreamKey, GroupName: rule.GroupName,
		ForSeconds: rule.ForSeconds, CooldownSeconds: rule.CooldownSeconds,
		Severity: rule.Severity, Enabled: &enabled, WebhookURL: rule.WebhookURL,
	}
}

func applyAlertRulePatch(input *alertRuleInput, patch alertRulePatch) {
	if patch.Name != nil {
		input.Name = *patch.Name
	}
	if patch.Metric != nil {
		input.Metric = *patch.Metric
	}
	if patch.Operator != nil {
		input.Operator = *patch.Operator
	}
	if patch.Threshold != nil {
		input.Threshold = *patch.Threshold
	}
	if patch.ConnectionID != nil {
		input.ConnectionID = *patch.ConnectionID
	}
	if patch.StreamKey != nil {
		input.StreamKey = *patch.StreamKey
	}
	if patch.GroupName != nil {
		input.GroupName = *patch.GroupName
	}
	if patch.ForSeconds != nil {
		input.ForSeconds = *patch.ForSeconds
	}
	if patch.CooldownSeconds != nil {
		input.CooldownSeconds = *patch.CooldownSeconds
	}
	if patch.Severity != nil {
		input.Severity = *patch.Severity
	}
	if patch.Enabled != nil {
		input.Enabled = patch.Enabled
	}
	if patch.WebhookURL != nil {
		input.WebhookURL = *patch.WebhookURL
	}
}

// The following handlers are intentionally not registered here. server.routes
// owns authorization policy; integration should protect them with alerts:read
// or alerts:write as appropriate.
func (s *apiServer) alertMetrics(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{"items": alertMetricCatalog()})
}

func (s *apiServer) alertRules(writer http.ResponseWriter, request *http.Request) {
	switch request.Method {
	case http.MethodGet:
		checker, err := s.store.permissionChecker(request.Context(), requestSession(request), "alerts:read")
		if err != nil {
			writePermissionCheckError(writer)
			return
		}
		items, err := s.store.listVisibleAlertRuleViews(request.Context(), checker)
		if err != nil {
			writeError(writer, http.StatusInternalServerError, "database_error", "Alert rules could not be loaded.")
			return
		}
		writeJSON(writer, http.StatusOK, map[string]any{"items": items})
	case http.MethodPost:
		var input alertRuleInput
		if err := readJSON(request, &input, 32<<10); err != nil {
			writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		session, _ := request.Context().Value(sessionContextKey).(sessionRecord)
		item, err := s.store.createAlertRule(request.Context(), input, session.UserID)
		if err != nil {
			writeAlertHandlerError(writer, err)
			return
		}
		writeJSON(writer, http.StatusCreated, map[string]any{"item": item})
	default:
		writer.Header().Set("Allow", "GET, POST")
		writeError(writer, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed.")
	}
}

func (s *apiServer) alertRuleByID(writer http.ResponseWriter, request *http.Request) {
	id := strings.TrimSpace(request.PathValue("id"))
	if id == "" {
		writeError(writer, http.StatusBadRequest, "invalid_request", "Alert rule ID is required.")
		return
	}
	switch request.Method {
	case http.MethodGet:
		checker, err := s.store.permissionChecker(request.Context(), requestSession(request), "alerts:read")
		if err != nil {
			writePermissionCheckError(writer)
			return
		}
		item, err := s.store.getAlertRule(request.Context(), id)
		if err != nil {
			writeAlertHandlerError(writer, err)
			return
		}
		if !alertResourceVisible(checker, "alerts:read", item.Metric, item.ConnectionID, item.StreamKey) {
			writeError(writer, http.StatusNotFound, "not_found", "Alert resource was not found.")
			return
		}
		writeJSON(writer, http.StatusOK, map[string]any{"item": item})
	case http.MethodPut:
		var input alertRuleInput
		if err := readJSON(request, &input, 32<<10); err != nil {
			writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		item, err := s.store.updateAlertRule(request.Context(), id, input)
		if err != nil {
			writeAlertHandlerError(writer, err)
			return
		}
		writeJSON(writer, http.StatusOK, map[string]any{"item": item})
	case http.MethodPatch:
		current, err := s.store.getAlertRule(request.Context(), id)
		if err != nil {
			writeAlertHandlerError(writer, err)
			return
		}
		var patch alertRulePatch
		if err := readJSON(request, &patch, 32<<10); err != nil {
			writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		input := inputFromAlertRule(current)
		applyAlertRulePatch(&input, patch)
		item, err := s.store.updateAlertRule(request.Context(), id, input)
		if err != nil {
			writeAlertHandlerError(writer, err)
			return
		}
		writeJSON(writer, http.StatusOK, map[string]any{"item": item})
	case http.MethodDelete:
		deleted, err := s.store.deleteAlertRule(request.Context(), id)
		if err != nil {
			writeAlertHandlerError(writer, err)
			return
		}
		if !deleted {
			writeError(writer, http.StatusNotFound, "not_found", "Alert rule was not found.")
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	default:
		writer.Header().Set("Allow", "GET, PUT, PATCH, DELETE")
		writeError(writer, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed.")
	}
}

func (s *apiServer) alertIncidents(writer http.ResponseWriter, request *http.Request) {
	checker, err := s.store.permissionChecker(request.Context(), requestSession(request), "alerts:read")
	if err != nil {
		writePermissionCheckError(writer)
		return
	}
	limit, err := strconv.Atoi(request.URL.Query().Get("limit"))
	if err != nil {
		limit = 100
	}
	items, err := s.store.listVisibleAlertIncidents(request.Context(), request.URL.Query().Get("status"), limit, checker)
	if err != nil {
		writeAlertHandlerError(writer, err)
		return
	}
	summary, err := s.store.visibleAlertDashboardSummary(request.Context(), checker)
	if err != nil {
		writeAlertHandlerError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"items": items, "summary": summary})
}

func (s *apiServer) acknowledgeAlertIncident(writer http.ResponseWriter, request *http.Request) {
	id := strings.TrimSpace(request.PathValue("id"))
	session, _ := request.Context().Value(sessionContextKey).(sessionRecord)
	item, err := s.store.acknowledgeAlertIncident(request.Context(), id, session.UserID, time.Now().UTC())
	if err != nil {
		writeAlertHandlerError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"item": item})
}

func (s *apiServer) testAlertWebhook(writer http.ResponseWriter, request *http.Request) {
	var input struct {
		URL     string         `json:"url"`
		Payload map[string]any `json:"payload,omitempty"`
	}
	if err := readJSON(request, &input, 64<<10); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if input.Payload == nil {
		input.Payload = map[string]any{"event": "test", "sentAt": time.Now().UTC(), "source": "RedisStreamScope"}
	}
	payload, err := json.Marshal(input.Payload)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", "Webhook payload is invalid.")
		return
	}
	client := newSafeWebhookClient(4*time.Second, defaultWebhookURLPolicy())
	status, err := client.Send(request.Context(), strings.TrimSpace(input.URL), payload)
	if err != nil {
		writeError(writer, http.StatusBadGateway, "webhook_failed", err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"status": "delivered", "statusCode": status})
}

func (s *apiServer) alertWebhookDeliveries(writer http.ResponseWriter, request *http.Request) {
	checker, err := s.store.permissionChecker(request.Context(), requestSession(request), "alerts:read")
	if err != nil {
		writePermissionCheckError(writer)
		return
	}
	limit, err := strconv.Atoi(request.URL.Query().Get("limit"))
	if err != nil {
		limit = 100
	}
	items, err := s.store.listVisibleAlertWebhookDeliveries(request.Context(), request.URL.Query().Get("incidentId"), limit, checker)
	if err != nil {
		writeAlertHandlerError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"items": items})
}

func writeAlertHandlerError(writer http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, sql.ErrNoRows):
		writeError(writer, http.StatusNotFound, "not_found", "Alert resource was not found.")
	case errors.Is(err, errAlertRouteReferenced):
		writeError(writer, http.StatusConflict, "route_in_use", "Webhook route is referenced by an enabled escalation policy.")
	case strings.Contains(strings.ToLower(err.Error()), "invalid"),
		strings.Contains(strings.ToLower(err.Error()), "must"),
		strings.Contains(strings.ToLower(err.Error()), "unsupported"),
		strings.Contains(strings.ToLower(err.Error()), "required"),
		strings.Contains(strings.ToLower(err.Error()), "cannot"):
		writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
	default:
		writeError(writer, http.StatusInternalServerError, "database_error", "Alert operation failed.")
	}
}
