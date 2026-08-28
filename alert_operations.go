package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

const (
	alertJobPending   = "pending"
	alertJobClaimed   = "claimed"
	alertJobRetry     = "retry"
	alertJobDelivered = "delivered"
	alertJobFailed    = "failed"

	alertDeliveryMaxAttempts = 3
	alertDeliveryClaimTTL    = 30 * time.Second
	alertOperationMaxItems   = 500
)

var errAlertRouteReferenced = errors.New("webhook route is referenced by an enabled escalation policy")

// alertOperationSchemaStatements is appended to the base alert migration so a
// newly-created database has one atomic, idempotent schema initialization path.
func alertOperationSchemaStatements() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS alert_silences (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			metric TEXT NOT NULL DEFAULT '',
			severity TEXT NOT NULL DEFAULT '',
			connection_id TEXT NOT NULL DEFAULT '',
			stream_key TEXT NOT NULL DEFAULT '',
			group_name TEXT NOT NULL DEFAULT '',
			labels_json TEXT NOT NULL DEFAULT '{}',
			starts_at TEXT NOT NULL,
			ends_at TEXT NOT NULL,
			reason TEXT NOT NULL,
			enabled INTEGER NOT NULL DEFAULT 1,
			created_by TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			deleted_at TEXT
		)`,
		`CREATE INDEX IF NOT EXISTS alert_silences_active_idx
			ON alert_silences(enabled,starts_at,ends_at) WHERE deleted_at IS NULL`,
		`CREATE TABLE IF NOT EXISTS alert_maintenance_windows (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			metric TEXT NOT NULL DEFAULT '',
			severity TEXT NOT NULL DEFAULT '',
			connection_id TEXT NOT NULL DEFAULT '',
			stream_key TEXT NOT NULL DEFAULT '',
			group_name TEXT NOT NULL DEFAULT '',
			labels_json TEXT NOT NULL DEFAULT '{}',
			starts_at TEXT NOT NULL,
			ends_at TEXT NOT NULL,
			reason TEXT NOT NULL,
			enabled INTEGER NOT NULL DEFAULT 1,
			created_by TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			deleted_at TEXT
		)`,
		`CREATE INDEX IF NOT EXISTS alert_maintenance_windows_active_idx
			ON alert_maintenance_windows(enabled,starts_at,ends_at) WHERE deleted_at IS NULL`,
		`CREATE TABLE IF NOT EXISTS alert_webhook_routes (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			severities_json TEXT NOT NULL DEFAULT '[]',
			metrics_json TEXT NOT NULL DEFAULT '[]',
			connection_id TEXT NOT NULL DEFAULT '',
			stream_key TEXT NOT NULL DEFAULT '',
			group_name TEXT NOT NULL DEFAULT '',
			labels_json TEXT NOT NULL DEFAULT '{}',
			enabled INTEGER NOT NULL DEFAULT 1,
			created_by TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			deleted_at TEXT
		)`,
		`CREATE INDEX IF NOT EXISTS alert_webhook_routes_enabled_idx
			ON alert_webhook_routes(enabled,name) WHERE deleted_at IS NULL`,
		`CREATE TABLE IF NOT EXISTS alert_webhook_route_destinations (
			id TEXT PRIMARY KEY,
			route_id TEXT NOT NULL REFERENCES alert_webhook_routes(id) ON DELETE CASCADE,
			name TEXT NOT NULL,
			webhook_url TEXT NOT NULL,
			format TEXT NOT NULL DEFAULT 'webhook',
			enabled INTEGER NOT NULL DEFAULT 1,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			UNIQUE(route_id,name)
		)`,
		`CREATE INDEX IF NOT EXISTS alert_webhook_route_destinations_route_idx
			ON alert_webhook_route_destinations(route_id,enabled)`,
		`CREATE TABLE IF NOT EXISTS alert_escalation_policies (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			severities_json TEXT NOT NULL DEFAULT '[]',
			metrics_json TEXT NOT NULL DEFAULT '[]',
			connection_id TEXT NOT NULL DEFAULT '',
			stream_key TEXT NOT NULL DEFAULT '',
			group_name TEXT NOT NULL DEFAULT '',
			labels_json TEXT NOT NULL DEFAULT '{}',
			enabled INTEGER NOT NULL DEFAULT 1,
			created_by TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			deleted_at TEXT
		)`,
		`CREATE INDEX IF NOT EXISTS alert_escalation_policies_enabled_idx
			ON alert_escalation_policies(enabled,name) WHERE deleted_at IS NULL`,
		`CREATE TABLE IF NOT EXISTS alert_escalation_steps (
			id TEXT PRIMARY KEY,
			policy_id TEXT NOT NULL REFERENCES alert_escalation_policies(id) ON DELETE CASCADE,
			step_order INTEGER NOT NULL,
			after_seconds INTEGER NOT NULL,
			route_id TEXT NOT NULL REFERENCES alert_webhook_routes(id),
			target_severity TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			UNIQUE(policy_id,step_order),
			UNIQUE(policy_id,after_seconds,route_id)
		)`,
		`CREATE TABLE IF NOT EXISTS alert_incident_labels (
			incident_id TEXT PRIMARY KEY REFERENCES alert_incidents(id) ON DELETE CASCADE,
			labels_json TEXT NOT NULL DEFAULT '{}',
			connection_id TEXT NOT NULL DEFAULT '',
			stream_key TEXT NOT NULL DEFAULT '',
			group_name TEXT NOT NULL DEFAULT '',
			observed_at TEXT,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS alert_notification_jobs (
			id TEXT PRIMARY KEY,
			dedup_key TEXT NOT NULL UNIQUE,
			delivery_group_id TEXT NOT NULL,
			rule_id TEXT NOT NULL,
			incident_id TEXT NOT NULL,
			event TEXT NOT NULL,
			severity TEXT NOT NULL,
			route_id TEXT NOT NULL DEFAULT '',
			destination_id TEXT NOT NULL DEFAULT '',
			escalation_policy_id TEXT NOT NULL DEFAULT '',
			escalation_step_id TEXT NOT NULL DEFAULT '',
			destination_url TEXT NOT NULL,
			notification_json TEXT NOT NULL,
			state TEXT NOT NULL,
			attempts INTEGER NOT NULL DEFAULT 0,
			max_attempts INTEGER NOT NULL DEFAULT 3,
			next_attempt_at TEXT NOT NULL,
			claim_token TEXT NOT NULL DEFAULT '',
			claim_until TEXT,
			last_status_code INTEGER NOT NULL DEFAULT 0,
			last_error TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			completed_at TEXT
		)`,
		`CREATE INDEX IF NOT EXISTS alert_notification_jobs_due_idx
			ON alert_notification_jobs(state,next_attempt_at,claim_until)`,
		`CREATE INDEX IF NOT EXISTS alert_notification_jobs_incident_idx
			ON alert_notification_jobs(incident_id,created_at)`,
		`CREATE TRIGGER IF NOT EXISTS alert_webhook_route_delete_guard
			BEFORE UPDATE OF deleted_at ON alert_webhook_routes
			WHEN OLD.deleted_at IS NULL AND NEW.deleted_at IS NOT NULL AND EXISTS(
				SELECT 1 FROM alert_escalation_steps s
				JOIN alert_escalation_policies p ON p.id=s.policy_id
				WHERE s.route_id=OLD.id AND p.enabled=1 AND p.deleted_at IS NULL
			)
			BEGIN
				SELECT RAISE(ABORT,'webhook route is referenced by an enabled escalation policy');
			END`,
	}
}

func migrateAlertOperationSchema(ctx context.Context, s *store) error {
	columns := []string{
		`ALTER TABLE alert_rules ADD COLUMN webhook_format TEXT NOT NULL DEFAULT 'webhook'`,
		`ALTER TABLE alert_webhook_route_destinations ADD COLUMN format TEXT NOT NULL DEFAULT 'webhook'`,
		`ALTER TABLE alert_webhook_deliveries ADD COLUMN delivery_group_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE alert_webhook_deliveries ADD COLUMN dedup_key TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE alert_webhook_deliveries ADD COLUMN route_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE alert_webhook_deliveries ADD COLUMN destination_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE alert_webhook_deliveries ADD COLUMN escalation_step_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE alert_incident_labels ADD COLUMN connection_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE alert_incident_labels ADD COLUMN stream_key TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE alert_incident_labels ADD COLUMN group_name TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE alert_incident_labels ADD COLUMN observed_at TEXT`,
	}
	for _, statement := range columns {
		if _, err := s.db.ExecContext(ctx, statement); err != nil && !strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
			return fmt.Errorf("alert operations schema migration: %w", err)
		}
	}
	return nil
}

type alertSelector struct {
	Severities   []string          `json:"severities,omitempty"`
	Metrics      []string          `json:"metrics,omitempty"`
	ConnectionID string            `json:"connectionId,omitempty"`
	StreamKey    string            `json:"streamKey,omitempty"`
	GroupName    string            `json:"groupName,omitempty"`
	Labels       map[string]string `json:"labels,omitempty"`
}

type alertSuppression struct {
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	Metric       string            `json:"metric,omitempty"`
	Severity     string            `json:"severity,omitempty"`
	ConnectionID string            `json:"connectionId,omitempty"`
	StreamKey    string            `json:"streamKey,omitempty"`
	GroupName    string            `json:"groupName,omitempty"`
	Labels       map[string]string `json:"labels,omitempty"`
	StartsAt     time.Time         `json:"startsAt"`
	EndsAt       time.Time         `json:"endsAt"`
	Reason       string            `json:"reason"`
	Enabled      bool              `json:"enabled"`
	CreatedBy    string            `json:"createdBy,omitempty"`
	CreatedAt    time.Time         `json:"createdAt"`
	UpdatedAt    time.Time         `json:"updatedAt"`
}

type alertSuppressionInput struct {
	Name         string            `json:"name"`
	Metric       string            `json:"metric,omitempty"`
	Severity     string            `json:"severity,omitempty"`
	ConnectionID string            `json:"connectionId,omitempty"`
	StreamKey    string            `json:"streamKey,omitempty"`
	GroupName    string            `json:"groupName,omitempty"`
	Labels       map[string]string `json:"labels,omitempty"`
	StartsAt     time.Time         `json:"startsAt"`
	EndsAt       time.Time         `json:"endsAt"`
	Reason       string            `json:"reason"`
	Enabled      *bool             `json:"enabled,omitempty"`
}

type alertRouteDestination struct {
	ID                string    `json:"id"`
	Name              string    `json:"name"`
	WebhookURL        string    `json:"-"`
	WebhookConfigured bool      `json:"webhookConfigured"`
	Format            string    `json:"format"`
	Enabled           bool      `json:"enabled"`
	CreatedAt         time.Time `json:"createdAt"`
	UpdatedAt         time.Time `json:"updatedAt"`
}

type alertRouteDestinationInput struct {
	ID         string `json:"id,omitempty"`
	Name       string `json:"name"`
	WebhookURL string `json:"webhookUrl,omitempty"`
	Format     string `json:"format,omitempty"`
	Enabled    *bool  `json:"enabled,omitempty"`
}

type alertWebhookRoute struct {
	ID                   string                  `json:"id"`
	Name                 string                  `json:"name"`
	Selector             alertSelector           `json:"selector"`
	Destinations         []alertRouteDestination `json:"destinations"`
	Enabled              bool                    `json:"enabled"`
	ReferencedByPolicies []alertRouteReference   `json:"referencedByPolicies"`
	DeleteBlocked        bool                    `json:"deleteBlocked"`
	CreatedBy            string                  `json:"createdBy,omitempty"`
	CreatedAt            time.Time               `json:"createdAt"`
	UpdatedAt            time.Time               `json:"updatedAt"`
}

type alertRouteReference struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type alertWebhookRouteInput struct {
	Name         string                       `json:"name"`
	Selector     alertSelector                `json:"selector"`
	Destinations []alertRouteDestinationInput `json:"destinations"`
	Enabled      *bool                        `json:"enabled,omitempty"`
}

type alertEscalationStep struct {
	ID             string    `json:"id"`
	AfterSeconds   int64     `json:"afterSeconds"`
	RouteID        string    `json:"routeId"`
	TargetSeverity string    `json:"targetSeverity"`
	CreatedAt      time.Time `json:"createdAt"`
	UpdatedAt      time.Time `json:"updatedAt"`
}

type alertEscalationStepInput struct {
	ID             string `json:"id,omitempty"`
	AfterSeconds   int64  `json:"afterSeconds"`
	RouteID        string `json:"routeId"`
	TargetSeverity string `json:"targetSeverity"`
}

type alertEscalationPolicy struct {
	ID        string                `json:"id"`
	Name      string                `json:"name"`
	Selector  alertSelector         `json:"selector"`
	Steps     []alertEscalationStep `json:"steps"`
	Enabled   bool                  `json:"enabled"`
	CreatedBy string                `json:"createdBy,omitempty"`
	CreatedAt time.Time             `json:"createdAt"`
	UpdatedAt time.Time             `json:"updatedAt"`
}

type alertEscalationPolicyInput struct {
	Name     string                     `json:"name"`
	Selector alertSelector              `json:"selector"`
	Steps    []alertEscalationStepInput `json:"steps"`
	Enabled  *bool                      `json:"enabled,omitempty"`
}

type alertDeliveryTarget struct {
	RouteID       string
	DestinationID string
	URL           string
	Format        string
}

type alertNotificationJob struct {
	ID                 string
	DedupKey           string
	DeliveryGroupID    string
	Notification       alertNotification
	DestinationURL     string
	State              string
	Attempts           int
	MaxAttempts        int
	NextAttemptAt      time.Time
	ClaimToken         string
	ClaimUntil         *time.Time
	LastStatusCode     int
	LastError          string
	EscalationPolicyID string
	EscalationStepID   string
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

func normalizeAlertStringSet(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func normalizeAlertLabels(labels map[string]string) (map[string]string, error) {
	if len(labels) > 32 {
		return nil, errors.New("labels must contain at most 32 entries")
	}
	result := make(map[string]string, len(labels))
	for key, value := range labels {
		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		if key == "" || len(key) > 128 || len(value) > 512 {
			return nil, errors.New("label keys must contain 1 to 128 characters and values at most 512 characters")
		}
		result[key] = value
	}
	return result, nil
}

func normalizeAlertSelector(selector alertSelector) (alertSelector, error) {
	selector.Severities = normalizeAlertStringSet(selector.Severities)
	selector.Metrics = normalizeAlertStringSet(selector.Metrics)
	selector.ConnectionID = strings.TrimSpace(selector.ConnectionID)
	selector.StreamKey = strings.TrimSpace(selector.StreamKey)
	selector.GroupName = strings.TrimSpace(selector.GroupName)
	labels, err := normalizeAlertLabels(selector.Labels)
	if err != nil {
		return selector, err
	}
	selector.Labels = labels
	if len(selector.ConnectionID) > 128 || len(selector.StreamKey) > 512 || len(selector.GroupName) > 256 {
		return selector, errors.New("alert selector scope is too long")
	}
	if selector.GroupName != "" && selector.StreamKey == "" {
		return selector, errors.New("streamKey is required when groupName is set")
	}
	for _, severity := range selector.Severities {
		if !validAlertSeverity(severity) {
			return selector, fmt.Errorf("invalid severity %q", severity)
		}
	}
	for _, metric := range selector.Metrics {
		if _, ok := supportedAlertMetrics[metric]; !ok {
			return selector, fmt.Errorf("unsupported alert metric %q", metric)
		}
	}
	return selector, nil
}

func validAlertSeverity(value string) bool {
	return value == "info" || value == "warning" || value == "critical"
}

func stringInAlertSet(value string, values []string) bool {
	if len(values) == 0 {
		return true
	}
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func alertLabelsMatch(required, actual map[string]string) bool {
	for key, value := range required {
		if actual[key] != value {
			return false
		}
	}
	return true
}

func alertSelectorMatches(selector alertSelector, notification alertNotification, severity string) bool {
	if severity == "" {
		severity = notification.Rule.Severity
	}
	connectionID, streamKey, groupName := alertNotificationScope(notification)
	return stringInAlertSet(severity, selector.Severities) &&
		stringInAlertSet(notification.Rule.Metric, selector.Metrics) &&
		(selector.ConnectionID == "" || selector.ConnectionID == connectionID) &&
		(selector.StreamKey == "" || selector.StreamKey == streamKey) &&
		(selector.GroupName == "" || selector.GroupName == groupName) &&
		alertLabelsMatch(selector.Labels, notification.Observation.Labels)
}

func alertNotificationScope(notification alertNotification) (string, string, string) {
	connectionID, streamKey, groupName := notification.Observation.ConnectionID, notification.Observation.StreamKey, notification.Observation.GroupName
	if connectionID == "" {
		connectionID = notification.Rule.ConnectionID
	}
	if streamKey == "" {
		streamKey = notification.Rule.StreamKey
	}
	if groupName == "" {
		groupName = notification.Rule.GroupName
	}
	return connectionID, streamKey, groupName
}

func marshalAlertJSON(value any) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

func unmarshalAlertJSON(raw string, target any) error {
	if strings.TrimSpace(raw) == "" {
		raw = "{}"
	}
	return json.Unmarshal([]byte(raw), target)
}

func alertDeterministicID(parts ...string) string {
	hash := sha256.New()
	for _, part := range parts {
		_, _ = hash.Write([]byte(part))
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))[:32]
}

func validateAlertSuppression(input alertSuppressionInput) (alertSuppression, error) {
	enabled := true
	if input.Enabled != nil {
		enabled = *input.Enabled
	}
	item := alertSuppression{
		Name: strings.TrimSpace(input.Name), Metric: strings.TrimSpace(input.Metric),
		Severity: strings.TrimSpace(input.Severity), ConnectionID: strings.TrimSpace(input.ConnectionID),
		StreamKey: strings.TrimSpace(input.StreamKey), GroupName: strings.TrimSpace(input.GroupName),
		StartsAt: input.StartsAt.UTC(), EndsAt: input.EndsAt.UTC(), Reason: strings.TrimSpace(input.Reason), Enabled: enabled,
	}
	labels, err := normalizeAlertLabels(input.Labels)
	if err != nil {
		return item, err
	}
	item.Labels = labels
	if item.Name == "" || len(item.Name) > 120 {
		return item, errors.New("suppression name must contain 1 to 120 characters")
	}
	if item.Reason == "" || len(item.Reason) > 1000 {
		return item, errors.New("reason must contain 1 to 1000 characters")
	}
	if item.StartsAt.IsZero() || item.EndsAt.IsZero() || !item.EndsAt.After(item.StartsAt) {
		return item, errors.New("endsAt must be after startsAt")
	}
	if item.EndsAt.Sub(item.StartsAt) > 366*24*time.Hour {
		return item, errors.New("suppression duration must not exceed 366 days")
	}
	if item.Metric != "" {
		if _, exists := supportedAlertMetrics[item.Metric]; !exists {
			return item, fmt.Errorf("unsupported alert metric %q", item.Metric)
		}
	}
	if item.Severity != "" && !validAlertSeverity(item.Severity) {
		return item, errors.New("severity must be empty, info, warning, or critical")
	}
	if len(item.ConnectionID) > 128 || len(item.StreamKey) > 512 || len(item.GroupName) > 256 {
		return item, errors.New("suppression scope is too long")
	}
	if item.GroupName != "" && item.StreamKey == "" {
		return item, errors.New("streamKey is required when groupName is set")
	}
	return item, nil
}

func alertSuppressionTable(kind string) (string, error) {
	switch kind {
	case "silence":
		return "alert_silences", nil
	case "maintenance":
		return "alert_maintenance_windows", nil
	default:
		return "", errors.New("invalid suppression kind")
	}
}

func (s *store) createAlertSuppression(ctx context.Context, kind string, input alertSuppressionInput, createdBy string) (alertSuppression, error) {
	if err := initAlertSchema(ctx, s); err != nil {
		return alertSuppression{}, err
	}
	table, err := alertSuppressionTable(kind)
	if err != nil {
		return alertSuppression{}, err
	}
	item, err := validateAlertSuppression(input)
	if err != nil {
		return item, err
	}
	id, err := randomID(16)
	if err != nil {
		return item, err
	}
	now := time.Now().UTC()
	item.ID, item.CreatedBy, item.CreatedAt, item.UpdatedAt = id, createdBy, now, now
	query := fmt.Sprintf(`INSERT INTO %s(
		id,name,metric,severity,connection_id,stream_key,group_name,labels_json,
		starts_at,ends_at,reason,enabled,created_by,created_at,updated_at
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, table)
	_, err = s.db.ExecContext(ctx, query,
		item.ID, item.Name, item.Metric, item.Severity, item.ConnectionID, item.StreamKey,
		item.GroupName, marshalAlertJSON(item.Labels), formatAlertTime(item.StartsAt),
		formatAlertTime(item.EndsAt), item.Reason, boolInt(item.Enabled), item.CreatedBy,
		formatAlertTime(item.CreatedAt), formatAlertTime(item.UpdatedAt))
	return item, err
}

func (s *store) updateAlertSuppression(ctx context.Context, kind, id string, input alertSuppressionInput) (alertSuppression, error) {
	if err := initAlertSchema(ctx, s); err != nil {
		return alertSuppression{}, err
	}
	table, err := alertSuppressionTable(kind)
	if err != nil {
		return alertSuppression{}, err
	}
	item, err := validateAlertSuppression(input)
	if err != nil {
		return item, err
	}
	current, err := s.getAlertSuppression(ctx, kind, id)
	if err != nil {
		return item, err
	}
	item.ID, item.CreatedBy, item.CreatedAt = current.ID, current.CreatedBy, current.CreatedAt
	item.UpdatedAt = time.Now().UTC()
	query := fmt.Sprintf(`UPDATE %s SET name=?,metric=?,severity=?,connection_id=?,stream_key=?,group_name=?,
		labels_json=?,starts_at=?,ends_at=?,reason=?,enabled=?,updated_at=? WHERE id=? AND deleted_at IS NULL`, table)
	result, err := s.db.ExecContext(ctx, query,
		item.Name, item.Metric, item.Severity, item.ConnectionID, item.StreamKey, item.GroupName,
		marshalAlertJSON(item.Labels), formatAlertTime(item.StartsAt), formatAlertTime(item.EndsAt),
		item.Reason, boolInt(item.Enabled), formatAlertTime(item.UpdatedAt), item.ID)
	if err != nil {
		return item, err
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		return item, sql.ErrNoRows
	}
	return item, nil
}

func scanAlertSuppression(scanner interface{ Scan(...any) error }) (alertSuppression, error) {
	var item alertSuppression
	var labels, startsAt, endsAt, createdAt, updatedAt string
	var enabled int
	err := scanner.Scan(&item.ID, &item.Name, &item.Metric, &item.Severity, &item.ConnectionID,
		&item.StreamKey, &item.GroupName, &labels, &startsAt, &endsAt, &item.Reason,
		&enabled, &item.CreatedBy, &createdAt, &updatedAt)
	if err != nil {
		return item, err
	}
	item.Enabled = enabled == 1
	item.StartsAt, item.EndsAt = parseAlertTime(startsAt), parseAlertTime(endsAt)
	item.CreatedAt, item.UpdatedAt = parseAlertTime(createdAt), parseAlertTime(updatedAt)
	if err := unmarshalAlertJSON(labels, &item.Labels); err != nil {
		return item, err
	}
	return item, nil
}

func (s *store) getAlertSuppression(ctx context.Context, kind, id string) (alertSuppression, error) {
	table, err := alertSuppressionTable(kind)
	if err != nil {
		return alertSuppression{}, err
	}
	query := fmt.Sprintf(`SELECT id,name,metric,severity,connection_id,stream_key,group_name,labels_json,
		starts_at,ends_at,reason,enabled,created_by,created_at,updated_at FROM %s
		WHERE id=? AND deleted_at IS NULL`, table)
	return scanAlertSuppression(s.db.QueryRowContext(ctx, query, id))
}

func (s *store) listAlertSuppressions(ctx context.Context, kind string, activeAt *time.Time, limit int) ([]alertSuppression, error) {
	if err := initAlertSchema(ctx, s); err != nil {
		return nil, err
	}
	table, err := alertSuppressionTable(kind)
	if err != nil {
		return nil, err
	}
	if limit <= 0 || limit > alertOperationMaxItems {
		limit = 100
	}
	query := fmt.Sprintf(`SELECT id,name,metric,severity,connection_id,stream_key,group_name,labels_json,
		starts_at,ends_at,reason,enabled,created_by,created_at,updated_at FROM %s WHERE deleted_at IS NULL`, table)
	args := make([]any, 0, 3)
	if activeAt != nil {
		at := formatAlertTime(activeAt.UTC())
		query += ` AND enabled=1 AND starts_at<=? AND ends_at>?`
		args = append(args, at, at)
	}
	query += ` ORDER BY starts_at DESC,id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]alertSuppression, 0, limit)
	for rows.Next() {
		item, err := scanAlertSuppression(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *store) deleteAlertSuppression(ctx context.Context, kind, id string) (bool, error) {
	if err := initAlertSchema(ctx, s); err != nil {
		return false, err
	}
	table, err := alertSuppressionTable(kind)
	if err != nil {
		return false, err
	}
	now := formatAlertTime(time.Now().UTC())
	query := fmt.Sprintf(`UPDATE %s SET enabled=0,deleted_at=?,updated_at=? WHERE id=? AND deleted_at IS NULL`, table)
	result, err := s.db.ExecContext(ctx, query, now, now, id)
	if err != nil {
		return false, err
	}
	affected, _ := result.RowsAffected()
	return affected > 0, nil
}

func alertSuppressionMatches(item alertSuppression, notification alertNotification, severity string, at time.Time) bool {
	if !item.Enabled || at.Before(item.StartsAt) || !at.Before(item.EndsAt) {
		return false
	}
	if severity == "" {
		severity = notification.Rule.Severity
	}
	connectionID, streamKey, groupName := alertNotificationScope(notification)
	return (item.Metric == "" || item.Metric == notification.Rule.Metric) &&
		(item.Severity == "" || item.Severity == severity) &&
		(item.ConnectionID == "" || item.ConnectionID == connectionID) &&
		(item.StreamKey == "" || item.StreamKey == streamKey) &&
		(item.GroupName == "" || item.GroupName == groupName) &&
		alertLabelsMatch(item.Labels, notification.Observation.Labels)
}

func (s *store) alertNotificationSuppressed(ctx context.Context, notification alertNotification, severity string, at time.Time) (bool, error) {
	for _, kind := range []string{"silence", "maintenance"} {
		items, err := s.listAlertSuppressions(ctx, kind, &at, alertOperationMaxItems)
		if err != nil {
			return false, err
		}
		for _, item := range items {
			if alertSuppressionMatches(item, notification, severity, at) {
				return true, nil
			}
		}
	}
	return false, nil
}

func normalizeAlertWebhookRouteInput(ctx context.Context, input alertWebhookRouteInput, existing map[string]alertRouteDestination) (alertWebhookRoute, error) {
	enabled := true
	if input.Enabled != nil {
		enabled = *input.Enabled
	}
	selector, err := normalizeAlertSelector(input.Selector)
	if err != nil {
		return alertWebhookRoute{}, err
	}
	route := alertWebhookRoute{Name: strings.TrimSpace(input.Name), Selector: selector, Enabled: enabled}
	if route.Name == "" || len(route.Name) > 120 {
		return route, errors.New("route name must contain 1 to 120 characters")
	}
	if len(input.Destinations) == 0 || len(input.Destinations) > 16 {
		return route, errors.New("route must contain 1 to 16 destinations")
	}
	seenNames := make(map[string]struct{}, len(input.Destinations))
	seenIDs := make(map[string]struct{}, len(input.Destinations))
	route.Destinations = make([]alertRouteDestination, 0, len(input.Destinations))
	for _, raw := range input.Destinations {
		enabledDestination := true
		if raw.Enabled != nil {
			enabledDestination = *raw.Enabled
		}
		format := raw.Format
		if strings.TrimSpace(format) == "" && strings.TrimSpace(raw.ID) != "" {
			if current, exists := existing[strings.TrimSpace(raw.ID)]; exists {
				format = current.Format
			}
		}
		item := alertRouteDestination{
			ID: strings.TrimSpace(raw.ID), Name: strings.TrimSpace(raw.Name),
			WebhookURL: strings.TrimSpace(raw.WebhookURL), Format: normalizeAlertWebhookFormat(format), Enabled: enabledDestination,
		}
		if item.Name == "" || len(item.Name) > 120 {
			return route, errors.New("destination name must contain 1 to 120 characters")
		}
		if _, duplicate := seenNames[strings.ToLower(item.Name)]; duplicate {
			return route, errors.New("destination names must be unique within a route")
		}
		seenNames[strings.ToLower(item.Name)] = struct{}{}
		if item.ID != "" {
			if _, duplicate := seenIDs[item.ID]; duplicate {
				return route, errors.New("destination IDs must be unique within a route")
			}
			seenIDs[item.ID] = struct{}{}
		}
		if item.WebhookURL == "" && item.ID != "" {
			if current, exists := existing[item.ID]; exists {
				item.WebhookURL = current.WebhookURL
			}
		}
		if item.WebhookURL == "" {
			return route, errors.New("webhookUrl is required for each new destination")
		}
		if len(item.WebhookURL) > 2048 {
			return route, errors.New("webhookUrl is too long")
		}
		if err := validateAlertWebhookFormat(item.Format); err != nil {
			return route, fmt.Errorf("invalid webhook format for %s: %w", item.Name, err)
		}
		if err := validateAlertWebhookURL(ctx, item.WebhookURL); err != nil {
			return route, fmt.Errorf("invalid webhook URL for %s: %w", item.Name, err)
		}
		item.WebhookConfigured = true
		route.Destinations = append(route.Destinations, item)
	}
	return route, nil
}

func (s *store) createAlertWebhookRoute(ctx context.Context, input alertWebhookRouteInput, createdBy string) (alertWebhookRoute, error) {
	if err := initAlertSchema(ctx, s); err != nil {
		return alertWebhookRoute{}, err
	}
	route, err := normalizeAlertWebhookRouteInput(ctx, input, nil)
	if err != nil {
		return route, err
	}
	route.ID, err = randomID(16)
	if err != nil {
		return route, err
	}
	now := time.Now().UTC()
	route.CreatedBy, route.CreatedAt, route.UpdatedAt = createdBy, now, now
	for index := range route.Destinations {
		route.Destinations[index].ID, err = randomID(16)
		if err != nil {
			return route, err
		}
		route.Destinations[index].CreatedAt = now
		route.Destinations[index].UpdatedAt = now
	}
	if err := s.persistAlertWebhookRoute(ctx, route, false); err != nil {
		return route, err
	}
	return route, nil
}

func (s *store) updateAlertWebhookRoute(ctx context.Context, id string, input alertWebhookRouteInput) (alertWebhookRoute, error) {
	if err := initAlertSchema(ctx, s); err != nil {
		return alertWebhookRoute{}, err
	}
	current, err := s.getAlertWebhookRoute(ctx, id, true)
	if err != nil {
		return alertWebhookRoute{}, err
	}
	existing := make(map[string]alertRouteDestination, len(current.Destinations))
	for _, destination := range current.Destinations {
		existing[destination.ID] = destination
	}
	route, err := normalizeAlertWebhookRouteInput(ctx, input, existing)
	if err != nil {
		return route, err
	}
	now := time.Now().UTC()
	route.ID, route.CreatedBy, route.CreatedAt, route.UpdatedAt = current.ID, current.CreatedBy, current.CreatedAt, now
	for index := range route.Destinations {
		if route.Destinations[index].ID == "" {
			route.Destinations[index].ID, err = randomID(16)
			if err != nil {
				return route, err
			}
			route.Destinations[index].CreatedAt = now
		} else if previous, exists := existing[route.Destinations[index].ID]; exists {
			route.Destinations[index].CreatedAt = previous.CreatedAt
		} else {
			return route, errors.New("destination ID does not belong to this route")
		}
		route.Destinations[index].UpdatedAt = now
	}
	if err := s.persistAlertWebhookRoute(ctx, route, true); err != nil {
		return route, err
	}
	return route, nil
}

func (s *store) persistAlertWebhookRoute(ctx context.Context, route alertWebhookRoute, update bool) error {
	transaction, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer transaction.Rollback()
	severities := marshalAlertJSON(route.Selector.Severities)
	metrics := marshalAlertJSON(route.Selector.Metrics)
	labels := marshalAlertJSON(route.Selector.Labels)
	if update {
		result, err := transaction.ExecContext(ctx, `UPDATE alert_webhook_routes SET
			name=?,severities_json=?,metrics_json=?,connection_id=?,stream_key=?,group_name=?,
			labels_json=?,enabled=?,updated_at=? WHERE id=? AND deleted_at IS NULL`,
			route.Name, severities, metrics, route.Selector.ConnectionID, route.Selector.StreamKey,
			route.Selector.GroupName, labels, boolInt(route.Enabled), formatAlertTime(route.UpdatedAt), route.ID)
		if err != nil {
			return err
		}
		if affected, _ := result.RowsAffected(); affected == 0 {
			return sql.ErrNoRows
		}
		if _, err := transaction.ExecContext(ctx, `DELETE FROM alert_webhook_route_destinations WHERE route_id=?`, route.ID); err != nil {
			return err
		}
	} else {
		_, err := transaction.ExecContext(ctx, `INSERT INTO alert_webhook_routes(
			id,name,severities_json,metrics_json,connection_id,stream_key,group_name,labels_json,
			enabled,created_by,created_at,updated_at
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, route.ID, route.Name, severities, metrics,
			route.Selector.ConnectionID, route.Selector.StreamKey, route.Selector.GroupName, labels,
			boolInt(route.Enabled), route.CreatedBy, formatAlertTime(route.CreatedAt), formatAlertTime(route.UpdatedAt))
		if err != nil {
			return err
		}
	}
	for _, destination := range route.Destinations {
		_, err := transaction.ExecContext(ctx, `INSERT INTO alert_webhook_route_destinations(
			id,route_id,name,webhook_url,format,enabled,created_at,updated_at
		) VALUES(?,?,?,?,?,?,?,?)`, destination.ID, route.ID, destination.Name, destination.WebhookURL, destination.Format,
			boolInt(destination.Enabled), formatAlertTime(destination.CreatedAt), formatAlertTime(destination.UpdatedAt))
		if err != nil {
			return err
		}
	}
	return transaction.Commit()
}

func (s *store) getAlertWebhookRoute(ctx context.Context, id string, includeSecrets bool) (alertWebhookRoute, error) {
	if err := initAlertSchema(ctx, s); err != nil {
		return alertWebhookRoute{}, err
	}
	var route alertWebhookRoute
	var severities, metrics, labels, createdAt, updatedAt string
	var enabled int
	err := s.db.QueryRowContext(ctx, `SELECT id,name,severities_json,metrics_json,connection_id,
		stream_key,group_name,labels_json,enabled,created_by,created_at,updated_at
		FROM alert_webhook_routes WHERE id=? AND deleted_at IS NULL`, id).Scan(
		&route.ID, &route.Name, &severities, &metrics, &route.Selector.ConnectionID,
		&route.Selector.StreamKey, &route.Selector.GroupName, &labels, &enabled,
		&route.CreatedBy, &createdAt, &updatedAt)
	if err != nil {
		return route, err
	}
	route.Enabled = enabled == 1
	route.CreatedAt, route.UpdatedAt = parseAlertTime(createdAt), parseAlertTime(updatedAt)
	if err := unmarshalAlertJSON(severities, &route.Selector.Severities); err != nil {
		return route, err
	}
	if err := unmarshalAlertJSON(metrics, &route.Selector.Metrics); err != nil {
		return route, err
	}
	if err := unmarshalAlertJSON(labels, &route.Selector.Labels); err != nil {
		return route, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,name,webhook_url,format,enabled,created_at,updated_at
		FROM alert_webhook_route_destinations WHERE route_id=? ORDER BY name COLLATE NOCASE,id`, id)
	if err != nil {
		return route, err
	}
	for rows.Next() {
		var destination alertRouteDestination
		var destinationEnabled int
		var destinationCreatedAt, destinationUpdatedAt string
		if err := rows.Scan(&destination.ID, &destination.Name, &destination.WebhookURL, &destination.Format, &destinationEnabled,
			&destinationCreatedAt, &destinationUpdatedAt); err != nil {
			return route, err
		}
		destination.Enabled = destinationEnabled == 1
		destination.WebhookConfigured = destination.WebhookURL != ""
		destination.Format = normalizeAlertWebhookFormat(destination.Format)
		destination.CreatedAt, destination.UpdatedAt = parseAlertTime(destinationCreatedAt), parseAlertTime(destinationUpdatedAt)
		if !includeSecrets {
			destination.WebhookURL = ""
		}
		route.Destinations = append(route.Destinations, destination)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return route, err
	}
	if err := rows.Close(); err != nil {
		return route, err
	}
	route.ReferencedByPolicies, err = s.alertWebhookRouteReferences(ctx, route.ID)
	if err != nil {
		return route, err
	}
	route.DeleteBlocked = len(route.ReferencedByPolicies) > 0
	return route, nil
}

func (s *store) alertWebhookRouteReferences(ctx context.Context, routeID string) ([]alertRouteReference, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT p.id,p.name
		FROM alert_escalation_steps st JOIN alert_escalation_policies p ON p.id=st.policy_id
		WHERE st.route_id=? AND p.enabled=1 AND p.deleted_at IS NULL
		ORDER BY p.name COLLATE NOCASE,p.id`, routeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]alertRouteReference, 0)
	for rows.Next() {
		var item alertRouteReference
		if err := rows.Scan(&item.ID, &item.Name); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *store) listAlertWebhookRoutes(ctx context.Context, includeSecrets bool) ([]alertWebhookRoute, error) {
	if err := initAlertSchema(ctx, s); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM alert_webhook_routes WHERE deleted_at IS NULL ORDER BY name COLLATE NOCASE,id LIMIT ?`, alertOperationMaxItems)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	items := make([]alertWebhookRoute, 0, len(ids))
	for _, id := range ids {
		item, err := s.getAlertWebhookRoute(ctx, id, includeSecrets)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

func (s *store) deleteAlertWebhookRoute(ctx context.Context, id string) (bool, error) {
	if err := initAlertSchema(ctx, s); err != nil {
		return false, err
	}
	references, err := s.alertWebhookRouteReferences(ctx, id)
	if err != nil {
		return false, err
	}
	if len(references) > 0 {
		return false, fmt.Errorf("%w: %s", errAlertRouteReferenced, references[0].Name)
	}
	now := formatAlertTime(time.Now().UTC())
	result, err := s.db.ExecContext(ctx, `UPDATE alert_webhook_routes SET enabled=0,deleted_at=?,updated_at=? WHERE id=? AND deleted_at IS NULL`, now, now, id)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), errAlertRouteReferenced.Error()) {
			return false, errAlertRouteReferenced
		}
		return false, err
	}
	affected, _ := result.RowsAffected()
	return affected > 0, nil
}

func (s *store) matchingAlertDeliveryTargets(ctx context.Context, notification alertNotification, severity string) ([]alertDeliveryTarget, error) {
	routes, err := s.listAlertWebhookRoutes(ctx, true)
	if err != nil {
		return nil, err
	}
	targets := make([]alertDeliveryTarget, 0)
	for _, route := range routes {
		if !route.Enabled || !alertSelectorMatches(route.Selector, notification, severity) {
			continue
		}
		for _, destination := range route.Destinations {
			if destination.Enabled && destination.WebhookURL != "" {
				targets = append(targets, alertDeliveryTarget{RouteID: route.ID, DestinationID: destination.ID, URL: destination.WebhookURL, Format: destination.Format})
			}
		}
	}
	return targets, nil
}

func (s *store) alertRouteTargetsByID(ctx context.Context, routeID string) ([]alertDeliveryTarget, error) {
	route, err := s.getAlertWebhookRoute(ctx, routeID, true)
	if err != nil {
		return nil, err
	}
	if !route.Enabled {
		return nil, nil
	}
	targets := make([]alertDeliveryTarget, 0, len(route.Destinations))
	for _, destination := range route.Destinations {
		if destination.Enabled && destination.WebhookURL != "" {
			targets = append(targets, alertDeliveryTarget{RouteID: route.ID, DestinationID: destination.ID, URL: destination.WebhookURL, Format: destination.Format})
		}
	}
	return targets, nil
}

func (s *store) normalizeAlertEscalationPolicyInput(ctx context.Context, input alertEscalationPolicyInput) (alertEscalationPolicy, error) {
	enabled := true
	if input.Enabled != nil {
		enabled = *input.Enabled
	}
	selector, err := normalizeAlertSelector(input.Selector)
	if err != nil {
		return alertEscalationPolicy{}, err
	}
	policy := alertEscalationPolicy{Name: strings.TrimSpace(input.Name), Selector: selector, Enabled: enabled}
	if policy.Name == "" || len(policy.Name) > 120 {
		return policy, errors.New("escalation policy name must contain 1 to 120 characters")
	}
	if len(input.Steps) == 0 || len(input.Steps) > 16 {
		return policy, errors.New("escalation policy must contain 1 to 16 steps")
	}
	seenDelay := make(map[int64]struct{}, len(input.Steps))
	for _, raw := range input.Steps {
		step := alertEscalationStep{
			ID: strings.TrimSpace(raw.ID), AfterSeconds: raw.AfterSeconds,
			RouteID: strings.TrimSpace(raw.RouteID), TargetSeverity: strings.TrimSpace(raw.TargetSeverity),
		}
		if step.AfterSeconds < 1 || step.AfterSeconds > 30*24*60*60 {
			return policy, errors.New("afterSeconds must be between 1 and 2592000")
		}
		if _, duplicate := seenDelay[step.AfterSeconds]; duplicate {
			return policy, errors.New("escalation step delays must be unique")
		}
		seenDelay[step.AfterSeconds] = struct{}{}
		if step.RouteID == "" {
			return policy, errors.New("routeId is required for every escalation step")
		}
		if _, err := s.getAlertWebhookRoute(ctx, step.RouteID, false); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return policy, fmt.Errorf("routeId %q does not exist", step.RouteID)
			}
			return policy, err
		}
		if !validAlertSeverity(step.TargetSeverity) {
			return policy, errors.New("targetSeverity must be info, warning, or critical")
		}
		policy.Steps = append(policy.Steps, step)
	}
	sort.Slice(policy.Steps, func(first, second int) bool {
		return policy.Steps[first].AfterSeconds < policy.Steps[second].AfterSeconds
	})
	return policy, nil
}

func (s *store) createAlertEscalationPolicy(ctx context.Context, input alertEscalationPolicyInput, createdBy string) (alertEscalationPolicy, error) {
	if err := initAlertSchema(ctx, s); err != nil {
		return alertEscalationPolicy{}, err
	}
	policy, err := s.normalizeAlertEscalationPolicyInput(ctx, input)
	if err != nil {
		return policy, err
	}
	policy.ID, err = randomID(16)
	if err != nil {
		return policy, err
	}
	now := time.Now().UTC()
	policy.CreatedBy, policy.CreatedAt, policy.UpdatedAt = createdBy, now, now
	for index := range policy.Steps {
		policy.Steps[index].ID, err = randomID(16)
		if err != nil {
			return policy, err
		}
		policy.Steps[index].CreatedAt, policy.Steps[index].UpdatedAt = now, now
	}
	if err := s.persistAlertEscalationPolicy(ctx, policy, false); err != nil {
		return policy, err
	}
	return policy, nil
}

func (s *store) updateAlertEscalationPolicy(ctx context.Context, id string, input alertEscalationPolicyInput) (alertEscalationPolicy, error) {
	if err := initAlertSchema(ctx, s); err != nil {
		return alertEscalationPolicy{}, err
	}
	current, err := s.getAlertEscalationPolicy(ctx, id)
	if err != nil {
		return alertEscalationPolicy{}, err
	}
	policy, err := s.normalizeAlertEscalationPolicyInput(ctx, input)
	if err != nil {
		return policy, err
	}
	existing := make(map[string]alertEscalationStep, len(current.Steps))
	for _, step := range current.Steps {
		existing[step.ID] = step
	}
	now := time.Now().UTC()
	policy.ID, policy.CreatedBy, policy.CreatedAt, policy.UpdatedAt = current.ID, current.CreatedBy, current.CreatedAt, now
	for index := range policy.Steps {
		if policy.Steps[index].ID == "" {
			policy.Steps[index].ID, err = randomID(16)
			if err != nil {
				return policy, err
			}
			policy.Steps[index].CreatedAt = now
		} else if previous, exists := existing[policy.Steps[index].ID]; exists {
			policy.Steps[index].CreatedAt = previous.CreatedAt
		} else {
			return policy, errors.New("escalation step ID does not belong to this policy")
		}
		policy.Steps[index].UpdatedAt = now
	}
	if err := s.persistAlertEscalationPolicy(ctx, policy, true); err != nil {
		return policy, err
	}
	return policy, nil
}

func (s *store) persistAlertEscalationPolicy(ctx context.Context, policy alertEscalationPolicy, update bool) error {
	transaction, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer transaction.Rollback()
	severities, metrics, labels := marshalAlertJSON(policy.Selector.Severities), marshalAlertJSON(policy.Selector.Metrics), marshalAlertJSON(policy.Selector.Labels)
	if update {
		result, err := transaction.ExecContext(ctx, `UPDATE alert_escalation_policies SET
			name=?,severities_json=?,metrics_json=?,connection_id=?,stream_key=?,group_name=?,labels_json=?,
			enabled=?,updated_at=? WHERE id=? AND deleted_at IS NULL`, policy.Name, severities, metrics,
			policy.Selector.ConnectionID, policy.Selector.StreamKey, policy.Selector.GroupName, labels,
			boolInt(policy.Enabled), formatAlertTime(policy.UpdatedAt), policy.ID)
		if err != nil {
			return err
		}
		if affected, _ := result.RowsAffected(); affected == 0 {
			return sql.ErrNoRows
		}
		if _, err := transaction.ExecContext(ctx, `DELETE FROM alert_escalation_steps WHERE policy_id=?`, policy.ID); err != nil {
			return err
		}
	} else {
		_, err := transaction.ExecContext(ctx, `INSERT INTO alert_escalation_policies(
			id,name,severities_json,metrics_json,connection_id,stream_key,group_name,labels_json,
			enabled,created_by,created_at,updated_at
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, policy.ID, policy.Name, severities, metrics,
			policy.Selector.ConnectionID, policy.Selector.StreamKey, policy.Selector.GroupName, labels,
			boolInt(policy.Enabled), policy.CreatedBy, formatAlertTime(policy.CreatedAt), formatAlertTime(policy.UpdatedAt))
		if err != nil {
			return err
		}
	}
	for index, step := range policy.Steps {
		_, err := transaction.ExecContext(ctx, `INSERT INTO alert_escalation_steps(
			id,policy_id,step_order,after_seconds,route_id,target_severity,created_at,updated_at
		) VALUES(?,?,?,?,?,?,?,?)`, step.ID, policy.ID, index, step.AfterSeconds, step.RouteID,
			step.TargetSeverity, formatAlertTime(step.CreatedAt), formatAlertTime(step.UpdatedAt))
		if err != nil {
			return err
		}
	}
	return transaction.Commit()
}

func (s *store) getAlertEscalationPolicy(ctx context.Context, id string) (alertEscalationPolicy, error) {
	if err := initAlertSchema(ctx, s); err != nil {
		return alertEscalationPolicy{}, err
	}
	var policy alertEscalationPolicy
	var severities, metrics, labels, createdAt, updatedAt string
	var enabled int
	err := s.db.QueryRowContext(ctx, `SELECT id,name,severities_json,metrics_json,connection_id,
		stream_key,group_name,labels_json,enabled,created_by,created_at,updated_at
		FROM alert_escalation_policies WHERE id=? AND deleted_at IS NULL`, id).Scan(
		&policy.ID, &policy.Name, &severities, &metrics, &policy.Selector.ConnectionID,
		&policy.Selector.StreamKey, &policy.Selector.GroupName, &labels, &enabled,
		&policy.CreatedBy, &createdAt, &updatedAt)
	if err != nil {
		return policy, err
	}
	policy.Enabled = enabled == 1
	policy.CreatedAt, policy.UpdatedAt = parseAlertTime(createdAt), parseAlertTime(updatedAt)
	if err := unmarshalAlertJSON(severities, &policy.Selector.Severities); err != nil {
		return policy, err
	}
	if err := unmarshalAlertJSON(metrics, &policy.Selector.Metrics); err != nil {
		return policy, err
	}
	if err := unmarshalAlertJSON(labels, &policy.Selector.Labels); err != nil {
		return policy, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,after_seconds,route_id,target_severity,created_at,updated_at
		FROM alert_escalation_steps WHERE policy_id=? ORDER BY step_order,id`, id)
	if err != nil {
		return policy, err
	}
	defer rows.Close()
	for rows.Next() {
		var step alertEscalationStep
		var stepCreatedAt, stepUpdatedAt string
		if err := rows.Scan(&step.ID, &step.AfterSeconds, &step.RouteID, &step.TargetSeverity, &stepCreatedAt, &stepUpdatedAt); err != nil {
			return policy, err
		}
		step.CreatedAt, step.UpdatedAt = parseAlertTime(stepCreatedAt), parseAlertTime(stepUpdatedAt)
		policy.Steps = append(policy.Steps, step)
	}
	return policy, rows.Err()
}

func (s *store) listAlertEscalationPolicies(ctx context.Context) ([]alertEscalationPolicy, error) {
	if err := initAlertSchema(ctx, s); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM alert_escalation_policies WHERE deleted_at IS NULL ORDER BY name COLLATE NOCASE,id LIMIT ?`, alertOperationMaxItems)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	items := make([]alertEscalationPolicy, 0, len(ids))
	for _, id := range ids {
		item, err := s.getAlertEscalationPolicy(ctx, id)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

func (s *store) deleteAlertEscalationPolicy(ctx context.Context, id string) (bool, error) {
	if err := initAlertSchema(ctx, s); err != nil {
		return false, err
	}
	now := formatAlertTime(time.Now().UTC())
	result, err := s.db.ExecContext(ctx, `UPDATE alert_escalation_policies SET enabled=0,deleted_at=?,updated_at=? WHERE id=? AND deleted_at IS NULL`, now, now, id)
	if err != nil {
		return false, err
	}
	affected, _ := result.RowsAffected()
	return affected > 0, nil
}

func persistAlertIncidentObservation(ctx context.Context, transaction *sql.Tx, incidentID string, observation alertObservation, now time.Time) error {
	if incidentID == "" {
		return nil
	}
	_, err := transaction.ExecContext(ctx, `INSERT INTO alert_incident_labels(
		incident_id,labels_json,connection_id,stream_key,group_name,observed_at,updated_at
	) VALUES(?,?,?,?,?,?,?) ON CONFLICT(incident_id) DO UPDATE SET
		labels_json=excluded.labels_json,connection_id=excluded.connection_id,stream_key=excluded.stream_key,
		group_name=excluded.group_name,observed_at=excluded.observed_at,updated_at=excluded.updated_at`,
		incidentID, marshalAlertJSON(observation.Labels), observation.ConnectionID, observation.StreamKey,
		observation.GroupName, nullableAlertObservedAt(observation.ObservedAt), formatAlertTime(now))
	return err
}

func nullableAlertObservedAt(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return formatAlertTime(value)
}

func (s *store) loadAlertIncidentLabels(ctx context.Context, incidentID string) (map[string]string, error) {
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT labels_json FROM alert_incident_labels WHERE incident_id=?`, incidentID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return map[string]string{}, nil
	}
	if err != nil {
		return nil, err
	}
	labels := make(map[string]string)
	if err := unmarshalAlertJSON(raw, &labels); err != nil {
		return nil, err
	}
	return labels, nil
}

func (s *alertService) dispatchAlertNotification(ctx context.Context, notification alertNotification) error {
	severity := notification.EffectiveSeverity
	if severity == "" {
		severity = notification.Rule.Severity
	}
	suppressed, err := s.store.alertNotificationSuppressed(ctx, notification, severity, notification.SentAt)
	if err != nil {
		return err
	}
	if suppressed {
		if notification.Event == "firing" || notification.Event == "repeat" {
			return s.store.deferSuppressedAlertNotification(ctx, notification.Rule.ID, notification.SentAt)
		}
		return nil
	}
	targets, err := s.store.matchingAlertDeliveryTargets(ctx, notification, severity)
	if err != nil {
		return err
	}
	if notification.Rule.WebhookURL != "" {
		targets = append([]alertDeliveryTarget{{URL: notification.Rule.WebhookURL, Format: notification.Rule.WebhookFormat}}, targets...)
	}
	if len(targets) == 0 {
		if notification.Event == "firing" || notification.Event == "repeat" {
			return s.store.deferSuppressedAlertNotification(ctx, notification.Rule.ID, notification.SentAt)
		}
		return nil
	}
	groupSeed := formatAlertTime(notification.SentAt)
	if notification.Event != "repeat" {
		groupSeed = notification.Event
	}
	notification.DeliveryGroupID = alertDeterministicID("delivery-group", notification.Incident.ID, notification.Event, groupSeed)
	for _, target := range targets {
		item := notification
		item.EffectiveSeverity = severity
		item.RouteID, item.DestinationID, item.DestinationURL = target.RouteID, target.DestinationID, target.URL
		item.DestinationFormat = normalizeAlertWebhookFormat(target.Format)
		item.DedupKey = alertDeterministicID("notification", item.Incident.ID, item.Event, groupSeed, target.RouteID, target.DestinationID, target.URL)
		// Enqueue is non-blocking. When the production dispatcher is used the
		// job is persisted before the bounded queue is attempted, so queue
		// pressure delays delivery instead of losing it.
		if err := enqueueAlertNotification(ctx, s.notifier, item); err != nil {
			if notification.Event == "firing" || notification.Event == "repeat" {
				if resetErr := s.store.deferSuppressedAlertNotification(ctx, notification.Rule.ID, notification.SentAt); resetErr != nil {
					return errors.Join(fmt.Errorf("persist alert notification: %w", err), fmt.Errorf("reset notification timestamp: %w", resetErr))
				}
			}
			return fmt.Errorf("persist alert notification: %w", err)
		}
	}
	return nil
}

func enqueueAlertNotification(ctx context.Context, sink alertNotificationSink, notification alertNotification) error {
	if sink == nil {
		return errors.New("alert notification sink is not configured")
	}
	if durable, ok := sink.(durableAlertNotificationSink); ok {
		accepted, err := durable.EnqueueDurable(ctx, notification)
		if err != nil {
			return err
		}
		if !accepted {
			return errors.New("alert notification was not durably accepted")
		}
		return nil
	}
	if !sink.Enqueue(notification) {
		return errors.New("alert notification sink rejected delivery")
	}
	return nil
}

func (s *store) deferSuppressedAlertNotification(ctx context.Context, ruleID string, sentAt time.Time) error {
	// Compare-and-clear avoids erasing a newer notification timestamp if an
	// overlapping evaluator completed after this suppressed delivery decision.
	_, err := s.db.ExecContext(ctx, `UPDATE alert_rule_states SET last_notification_at=NULL
		WHERE rule_id=? AND last_notification_at=?`, ruleID, formatAlertTime(sentAt))
	return err
}

func (s *store) reserveAlertNotificationJob(ctx context.Context, notification alertNotification, now time.Time) (alertNotification, bool, error) {
	if err := initAlertSchema(ctx, s); err != nil {
		return notification, false, err
	}
	if notification.DestinationURL == "" {
		notification.DestinationURL = notification.Rule.WebhookURL
	}
	if notification.DestinationURL == "" {
		return notification, false, errors.New("notification destination is required")
	}
	if notification.DestinationFormat == "" {
		notification.DestinationFormat = notification.Rule.WebhookFormat
	}
	notification.DestinationFormat = normalizeAlertWebhookFormat(notification.DestinationFormat)
	if err := validateAlertWebhookFormat(notification.DestinationFormat); err != nil {
		return notification, false, err
	}
	if notification.DeliveryGroupID == "" {
		notification.DeliveryGroupID = alertDeterministicID("delivery-group", notification.Incident.ID, notification.Event, formatAlertTime(notification.SentAt))
	}
	if notification.DedupKey == "" {
		notification.DedupKey = alertDeterministicID("notification", notification.Incident.ID, notification.Event,
			formatAlertTime(notification.SentAt), notification.RouteID, notification.DestinationID, notification.DestinationURL)
	}
	if notification.EffectiveSeverity == "" {
		notification.EffectiveSeverity = notification.Rule.Severity
	}
	notification.JobID = alertDeterministicID("job", notification.DedupKey)
	payload, err := json.Marshal(notification)
	if err != nil {
		return notification, false, err
	}
	now = now.UTC()
	result, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO alert_notification_jobs(
		id,dedup_key,delivery_group_id,rule_id,incident_id,event,severity,route_id,destination_id,
		escalation_policy_id,escalation_step_id,destination_url,notification_json,state,attempts,
		max_attempts,next_attempt_at,created_at,updated_at
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		notification.JobID, notification.DedupKey, notification.DeliveryGroupID,
		notification.Rule.ID, notification.Incident.ID, notification.Event, notification.EffectiveSeverity,
		notification.RouteID, notification.DestinationID, notification.EscalationPolicyID,
		notification.EscalationStepID, notification.DestinationURL, string(payload), alertJobPending,
		0, alertDeliveryMaxAttempts, formatAlertTime(now), formatAlertTime(now), formatAlertTime(now))
	if err != nil {
		return notification, false, err
	}
	inserted, _ := result.RowsAffected()
	return notification, inserted == 1, nil
}

func (s *store) dueAlertNotificationJobs(ctx context.Context, now time.Time, limit int) ([]alertNotification, error) {
	if limit <= 0 || limit > 256 {
		limit = 64
	}
	nowText := formatAlertTime(now.UTC())
	// A worker can terminate after claiming but before sending. Once the final
	// claim lease expires there is no retry budget left, so close the job rather
	// than retaining an unclaimable "claimed" row forever.
	_, _ = s.db.ExecContext(ctx, `UPDATE alert_notification_jobs SET state='failed',
		claim_token='',claim_until=NULL,last_error='delivery claim expired after maximum attempts',
		updated_at=?,completed_at=? WHERE state='claimed' AND attempts>=max_attempts
		AND claim_until IS NOT NULL AND claim_until<=?`, nowText, nowText, nowText)
	rows, err := s.db.QueryContext(ctx, `SELECT id,dedup_key,delivery_group_id,route_id,destination_id,
		escalation_policy_id,escalation_step_id,destination_url,notification_json
		FROM alert_notification_jobs
		WHERE attempts<max_attempts AND next_attempt_at<=? AND (
			state IN ('pending','retry') OR (state='claimed' AND claim_until IS NOT NULL AND claim_until<=?)
		) ORDER BY next_attempt_at,id LIMIT ?`, nowText, nowText, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]alertNotification, 0, limit)
	for rows.Next() {
		var item alertNotification
		var raw string
		if err := rows.Scan(&item.JobID, &item.DedupKey, &item.DeliveryGroupID, &item.RouteID,
			&item.DestinationID, &item.EscalationPolicyID, &item.EscalationStepID,
			&item.DestinationURL, &raw); err != nil {
			return nil, err
		}
		jobID, dedupKey, groupID := item.JobID, item.DedupKey, item.DeliveryGroupID
		routeID, destinationID, destinationURL := item.RouteID, item.DestinationID, item.DestinationURL
		policyID, stepID := item.EscalationPolicyID, item.EscalationStepID
		if err := json.Unmarshal([]byte(raw), &item); err != nil {
			return nil, err
		}
		item.JobID, item.DedupKey, item.DeliveryGroupID = jobID, dedupKey, groupID
		item.RouteID, item.DestinationID, item.DestinationURL = routeID, destinationID, destinationURL
		item.EscalationPolicyID, item.EscalationStepID = policyID, stepID
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *store) claimAlertNotificationJob(ctx context.Context, id string, now time.Time) (alertNotificationJob, bool, error) {
	var job alertNotificationJob
	claimToken, err := randomID(16)
	if err != nil {
		return job, false, err
	}
	now = now.UTC()
	claimUntil := now.Add(alertDeliveryClaimTTL)
	transaction, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return job, false, err
	}
	defer transaction.Rollback()
	result, err := transaction.ExecContext(ctx, `UPDATE alert_notification_jobs SET
		state='claimed',attempts=attempts+1,claim_token=?,claim_until=?,updated_at=?
		WHERE id=? AND attempts<max_attempts AND next_attempt_at<=? AND (
			state IN ('pending','retry') OR (state='claimed' AND claim_until IS NOT NULL AND claim_until<=?)
		)`, claimToken, formatAlertTime(claimUntil), formatAlertTime(now), id, formatAlertTime(now), formatAlertTime(now))
	if err != nil {
		return job, false, err
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return job, false, nil
	}
	var raw, nextAttemptAt, createdAt, updatedAt string
	var claimUntilRaw sql.NullString
	err = transaction.QueryRowContext(ctx, `SELECT id,dedup_key,delivery_group_id,destination_url,state,
		attempts,max_attempts,next_attempt_at,claim_token,claim_until,last_status_code,last_error,
		escalation_policy_id,escalation_step_id,notification_json,created_at,updated_at
		FROM alert_notification_jobs WHERE id=?`, id).Scan(
		&job.ID, &job.DedupKey, &job.DeliveryGroupID, &job.DestinationURL, &job.State,
		&job.Attempts, &job.MaxAttempts, &nextAttemptAt, &job.ClaimToken, &claimUntilRaw,
		&job.LastStatusCode, &job.LastError, &job.EscalationPolicyID, &job.EscalationStepID,
		&raw, &createdAt, &updatedAt)
	if err != nil {
		return job, false, err
	}
	if err := json.Unmarshal([]byte(raw), &job.Notification); err != nil {
		return job, false, err
	}
	job.Notification.JobID, job.Notification.DestinationURL = job.ID, job.DestinationURL
	job.NextAttemptAt, job.CreatedAt, job.UpdatedAt = parseAlertTime(nextAttemptAt), parseAlertTime(createdAt), parseAlertTime(updatedAt)
	job.ClaimUntil = scanNullableAlertTime(claimUntilRaw)
	if err := transaction.Commit(); err != nil {
		return job, false, err
	}
	return job, true, nil
}

func alertDeliveryRetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	return time.Duration(attempt*attempt) * time.Second
}

func (s *store) completeAlertNotificationJob(ctx context.Context, job alertNotificationJob, now time.Time, status int, sendErr error) (string, error) {
	now = now.UTC()
	state, message := alertJobDelivered, ""
	nextAttemptAt := now
	if sendErr != nil {
		message = truncate(sendErr.Error(), 512)
		if retryableWebhookResult(status) && job.Attempts < job.MaxAttempts {
			state = alertJobRetry
			nextAttemptAt = now.Add(alertDeliveryRetryDelay(job.Attempts))
		} else {
			state = alertJobFailed
		}
	}
	completedAt := any(nil)
	if state == alertJobDelivered || state == alertJobFailed {
		completedAt = formatAlertTime(now)
	}
	result, err := s.db.ExecContext(ctx, `UPDATE alert_notification_jobs SET
		state=?,next_attempt_at=?,claim_token='',claim_until=NULL,last_status_code=?,last_error=?,
		updated_at=?,completed_at=? WHERE id=? AND state='claimed' AND claim_token=?`,
		state, formatAlertTime(nextAttemptAt), status, message, formatAlertTime(now), completedAt, job.ID, job.ClaimToken)
	if err != nil {
		return "", err
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return "", errors.New("alert delivery claim is no longer owned")
	}
	return state, nil
}

func (s *store) activeAlertIncidentsForEscalation(ctx context.Context) ([]alertNotification, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT i.id,i.rule_id,i.status,i.started_at,i.updated_at,
		i.trigger_value,i.last_value,i.summary,r.name,r.metric,r.operator,r.threshold,r.severity,r.connection_id,r.stream_key,r.group_name,
		r.for_seconds,r.cooldown_seconds,r.enabled,r.webhook_url,r.webhook_format,r.created_by,r.created_at,r.updated_at,
		COALESCE(l.labels_json,'{}'),COALESCE(l.connection_id,''),COALESCE(l.stream_key,''),
		COALESCE(l.group_name,''),l.observed_at
		FROM alert_incidents i JOIN alert_rules r ON r.id=i.rule_id
		LEFT JOIN alert_incident_labels l ON l.incident_id=i.id
		WHERE i.status='firing' AND r.deleted_at IS NULL ORDER BY i.started_at LIMIT 5000`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]alertNotification, 0)
	for rows.Next() {
		var item alertNotification
		var startedAt, updatedAt, ruleCreatedAt, ruleUpdatedAt, labels string
		var observedAt sql.NullString
		var enabled int
		if err := rows.Scan(&item.Incident.ID, &item.Incident.RuleID, &item.Incident.Status,
			&startedAt, &updatedAt, &item.Incident.TriggerValue, &item.Incident.LastValue, &item.Incident.Summary,
			&item.Rule.Name, &item.Rule.Metric, &item.Rule.Operator, &item.Rule.Threshold, &item.Rule.Severity, &item.Rule.ConnectionID,
			&item.Rule.StreamKey, &item.Rule.GroupName, &item.Rule.ForSeconds, &item.Rule.CooldownSeconds,
			&enabled, &item.Rule.WebhookURL, &item.Rule.WebhookFormat, &item.Rule.CreatedBy, &ruleCreatedAt, &ruleUpdatedAt, &labels,
			&item.Observation.ConnectionID, &item.Observation.StreamKey, &item.Observation.GroupName, &observedAt); err != nil {
			return nil, err
		}
		item.Rule.ID = item.Incident.RuleID
		item.Rule.Enabled, item.Rule.WebhookConfigured = enabled == 1, item.Rule.WebhookURL != ""
		item.Rule.WebhookFormat = normalizeAlertWebhookFormat(item.Rule.WebhookFormat)
		item.Rule.CreatedAt, item.Rule.UpdatedAt = parseAlertTime(ruleCreatedAt), parseAlertTime(ruleUpdatedAt)
		item.Incident.RuleName, item.Incident.Metric, item.Incident.Severity = item.Rule.Name, item.Rule.Metric, item.Rule.Severity
		item.Incident.ConnectionID, item.Incident.StreamKey, item.Incident.GroupName = item.Rule.ConnectionID, item.Rule.StreamKey, item.Rule.GroupName
		item.Incident.StartedAt, item.Incident.UpdatedAt = parseAlertTime(startedAt), parseAlertTime(updatedAt)
		item.Observation.Metric, item.Observation.Value, item.Observation.Available = item.Rule.Metric, item.Incident.LastValue, true
		if item.Observation.ConnectionID == "" {
			item.Observation.ConnectionID = item.Rule.ConnectionID
		}
		if item.Observation.StreamKey == "" {
			item.Observation.StreamKey = item.Rule.StreamKey
		}
		if item.Observation.GroupName == "" {
			item.Observation.GroupName = item.Rule.GroupName
		}
		item.Incident.ConnectionID = item.Observation.ConnectionID
		item.Incident.StreamKey = item.Observation.StreamKey
		item.Incident.GroupName = item.Observation.GroupName
		item.Observation.ObservedAt = item.Incident.UpdatedAt
		if observedAt.Valid {
			item.Observation.ObservedAt = parseAlertTime(observedAt.String)
		}
		if err := unmarshalAlertJSON(labels, &item.Observation.Labels); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// ScheduleEscalationsAt persists one delivery job per incident/policy/step/
// destination. The unique dedup key makes scheduling effectively-once across
// scheduler overlap and process restarts. External HTTP delivery remains
// at-least-once because it cannot share the SQLite transaction.
func (s *alertService) ScheduleEscalationsAt(ctx context.Context, now time.Time) error {
	if s == nil || s.store == nil || s.notifier == nil {
		return nil
	}
	incidents, err := s.store.activeAlertIncidentsForEscalation(ctx)
	if err != nil {
		return err
	}
	policies, err := s.store.listAlertEscalationPolicies(ctx)
	if err != nil {
		return err
	}
	for _, notification := range incidents {
		for _, policy := range policies {
			if !policy.Enabled || !alertSelectorMatches(policy.Selector, notification, notification.Rule.Severity) {
				continue
			}
			for _, step := range policy.Steps {
				if now.Before(notification.Incident.StartedAt.Add(time.Duration(step.AfterSeconds) * time.Second)) {
					continue
				}
				notification.Event, notification.SentAt = "escalation", now.UTC()
				notification.EffectiveSeverity = step.TargetSeverity
				notification.EscalationPolicyID, notification.EscalationStepID = policy.ID, step.ID
				suppressed, err := s.store.alertNotificationSuppressed(ctx, notification, step.TargetSeverity, now)
				if err != nil {
					return err
				}
				if suppressed {
					continue
				}
				targets, err := s.store.alertRouteTargetsByID(ctx, step.RouteID)
				if errors.Is(err, sql.ErrNoRows) {
					continue
				}
				if err != nil {
					return err
				}
				groupID := alertDeterministicID("escalation-group", notification.Incident.ID, policy.ID, step.ID)
				for _, target := range targets {
					item := notification
					item.RouteID, item.DestinationID, item.DestinationURL = target.RouteID, target.DestinationID, target.URL
					item.DestinationFormat = normalizeAlertWebhookFormat(target.Format)
					item.DeliveryGroupID = groupID
					item.DedupKey = alertDeterministicID("escalation", item.Incident.ID, policy.ID, step.ID, target.DestinationID)
					if err := enqueueAlertNotification(ctx, s.notifier, item); err != nil {
						return fmt.Errorf("persist escalation notification: %w", err)
					}
				}
			}
		}
	}
	return nil
}

// RunAlertOperations is safe to call from main in addition to Run; sync.Once
// prevents duplicate schedulers in a single process.
func (s *alertService) RunAlertOperations(ctx context.Context, interval time.Duration) {
	if s == nil {
		return
	}
	s.operationsStartOnce.Do(func() {
		if interval < time.Second {
			interval = 5 * time.Second
		}
		lastRetention := time.Time{}
		evaluate := func() {
			operationCtx, cancel := context.WithTimeout(ctx, interval)
			now := time.Now().UTC()
			_ = s.ScheduleEscalationsAt(operationCtx, now)
			if lastRetention.IsZero() || now.Sub(lastRetention) >= time.Hour {
				_ = s.store.maintainAdvancedAlertHistory(operationCtx, now)
				lastRetention = now
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
	})
}

func (s *store) maintainAdvancedAlertHistory(ctx context.Context, now time.Time) error {
	if s == nil || s.db == nil {
		return errors.New("alert store is not configured")
	}
	cutoff := formatAlertTime(now.UTC().Add(-alertHistoryRetention))
	transaction, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer transaction.Rollback()
	statements := []string{
		`DELETE FROM alert_notification_jobs WHERE completed_at IS NOT NULL AND completed_at<?`,
		`DELETE FROM alert_silences WHERE deleted_at IS NOT NULL AND deleted_at<?`,
		`DELETE FROM alert_maintenance_windows WHERE deleted_at IS NOT NULL AND deleted_at<?`,
		`DELETE FROM alert_escalation_policies WHERE deleted_at IS NOT NULL AND deleted_at<?`,
		`DELETE FROM alert_webhook_routes WHERE deleted_at IS NOT NULL AND deleted_at<?
			AND NOT EXISTS(SELECT 1 FROM alert_escalation_steps s WHERE s.route_id=alert_webhook_routes.id)`,
	}
	for _, statement := range statements {
		if _, err := transaction.ExecContext(ctx, statement, cutoff); err != nil {
			return err
		}
	}
	return transaction.Commit()
}

// These handlers intentionally leave authorization to server.routes, matching
// the existing alert handlers. Collections require alerts:read for GET and
// alerts:write for mutations; item handlers follow the same policy.
func (s *apiServer) alertSilences(writer http.ResponseWriter, request *http.Request) {
	s.alertSuppressions(writer, request, "silence")
}

func (s *apiServer) alertSilenceByID(writer http.ResponseWriter, request *http.Request) {
	s.alertSuppressionByID(writer, request, "silence")
}

func (s *apiServer) alertMaintenanceWindows(writer http.ResponseWriter, request *http.Request) {
	s.alertSuppressions(writer, request, "maintenance")
}

func (s *apiServer) alertMaintenanceWindowByID(writer http.ResponseWriter, request *http.Request) {
	s.alertSuppressionByID(writer, request, "maintenance")
}

func (s *apiServer) alertSuppressions(writer http.ResponseWriter, request *http.Request, kind string) {
	switch request.Method {
	case http.MethodGet:
		var activeAt *time.Time
		if request.URL.Query().Get("active") == "true" {
			now := time.Now().UTC()
			activeAt = &now
		}
		items, err := s.store.listAlertSuppressions(request.Context(), kind, activeAt, 100)
		if err != nil {
			writeAlertHandlerError(writer, err)
			return
		}
		writeJSON(writer, http.StatusOK, map[string]any{"items": items})
	case http.MethodPost:
		var input alertSuppressionInput
		if err := readJSON(request, &input, 32<<10); err != nil {
			writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		session, _ := request.Context().Value(sessionContextKey).(sessionRecord)
		item, err := s.store.createAlertSuppression(request.Context(), kind, input, session.UserID)
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

func (s *apiServer) alertSuppressionByID(writer http.ResponseWriter, request *http.Request, kind string) {
	id := strings.TrimSpace(request.PathValue("id"))
	if id == "" {
		writeError(writer, http.StatusBadRequest, "invalid_request", "Alert suppression ID is required.")
		return
	}
	switch request.Method {
	case http.MethodGet:
		item, err := s.store.getAlertSuppression(request.Context(), kind, id)
		if err != nil {
			writeAlertHandlerError(writer, err)
			return
		}
		writeJSON(writer, http.StatusOK, map[string]any{"item": item})
	case http.MethodPut:
		var input alertSuppressionInput
		if err := readJSON(request, &input, 32<<10); err != nil {
			writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		item, err := s.store.updateAlertSuppression(request.Context(), kind, id, input)
		if err != nil {
			writeAlertHandlerError(writer, err)
			return
		}
		writeJSON(writer, http.StatusOK, map[string]any{"item": item})
	case http.MethodDelete:
		deleted, err := s.store.deleteAlertSuppression(request.Context(), kind, id)
		if err != nil {
			writeAlertHandlerError(writer, err)
			return
		}
		if !deleted {
			writeError(writer, http.StatusNotFound, "not_found", "Alert suppression was not found.")
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	default:
		writer.Header().Set("Allow", "GET, PUT, DELETE")
		writeError(writer, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed.")
	}
}

func (s *apiServer) alertWebhookRoutes(writer http.ResponseWriter, request *http.Request) {
	switch request.Method {
	case http.MethodGet:
		items, err := s.store.listAlertWebhookRoutes(request.Context(), false)
		if err != nil {
			writeAlertHandlerError(writer, err)
			return
		}
		writeJSON(writer, http.StatusOK, map[string]any{"items": items})
	case http.MethodPost:
		var input alertWebhookRouteInput
		if err := readJSON(request, &input, 128<<10); err != nil {
			writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		session, _ := request.Context().Value(sessionContextKey).(sessionRecord)
		item, err := s.store.createAlertWebhookRoute(request.Context(), input, session.UserID)
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

func (s *apiServer) alertWebhookRouteByID(writer http.ResponseWriter, request *http.Request) {
	id := strings.TrimSpace(request.PathValue("id"))
	if id == "" {
		writeError(writer, http.StatusBadRequest, "invalid_request", "Alert route ID is required.")
		return
	}
	switch request.Method {
	case http.MethodGet:
		item, err := s.store.getAlertWebhookRoute(request.Context(), id, false)
		if err != nil {
			writeAlertHandlerError(writer, err)
			return
		}
		writeJSON(writer, http.StatusOK, map[string]any{"item": item})
	case http.MethodPut:
		var input alertWebhookRouteInput
		if err := readJSON(request, &input, 128<<10); err != nil {
			writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		item, err := s.store.updateAlertWebhookRoute(request.Context(), id, input)
		if err != nil {
			writeAlertHandlerError(writer, err)
			return
		}
		writeJSON(writer, http.StatusOK, map[string]any{"item": item})
	case http.MethodDelete:
		deleted, err := s.store.deleteAlertWebhookRoute(request.Context(), id)
		if err != nil {
			writeAlertHandlerError(writer, err)
			return
		}
		if !deleted {
			writeError(writer, http.StatusNotFound, "not_found", "Alert route was not found.")
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	default:
		writer.Header().Set("Allow", "GET, PUT, DELETE")
		writeError(writer, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed.")
	}
}

func (s *apiServer) alertEscalationPolicies(writer http.ResponseWriter, request *http.Request) {
	switch request.Method {
	case http.MethodGet:
		items, err := s.store.listAlertEscalationPolicies(request.Context())
		if err != nil {
			writeAlertHandlerError(writer, err)
			return
		}
		writeJSON(writer, http.StatusOK, map[string]any{"items": items})
	case http.MethodPost:
		var input alertEscalationPolicyInput
		if err := readJSON(request, &input, 64<<10); err != nil {
			writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		session, _ := request.Context().Value(sessionContextKey).(sessionRecord)
		item, err := s.store.createAlertEscalationPolicy(request.Context(), input, session.UserID)
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

func (s *apiServer) alertEscalationPolicyByID(writer http.ResponseWriter, request *http.Request) {
	id := strings.TrimSpace(request.PathValue("id"))
	if id == "" {
		writeError(writer, http.StatusBadRequest, "invalid_request", "Escalation policy ID is required.")
		return
	}
	switch request.Method {
	case http.MethodGet:
		item, err := s.store.getAlertEscalationPolicy(request.Context(), id)
		if err != nil {
			writeAlertHandlerError(writer, err)
			return
		}
		writeJSON(writer, http.StatusOK, map[string]any{"item": item})
	case http.MethodPut:
		var input alertEscalationPolicyInput
		if err := readJSON(request, &input, 64<<10); err != nil {
			writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		item, err := s.store.updateAlertEscalationPolicy(request.Context(), id, input)
		if err != nil {
			writeAlertHandlerError(writer, err)
			return
		}
		writeJSON(writer, http.StatusOK, map[string]any{"item": item})
	case http.MethodDelete:
		deleted, err := s.store.deleteAlertEscalationPolicy(request.Context(), id)
		if err != nil {
			writeAlertHandlerError(writer, err)
			return
		}
		if !deleted {
			writeError(writer, http.StatusNotFound, "not_found", "Escalation policy was not found.")
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	default:
		writer.Header().Set("Allow", "GET, PUT, DELETE")
		writeError(writer, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed.")
	}
}
