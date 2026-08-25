package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	lifecycleBatchLimit        = 500
	lifecycleQueryDefaultLimit = 10_000
	lifecycleQueryMaximumLimit = 100_000
	lifecycleBodyLimit         = 2 << 20
	telemetryTokenPrefix       = "rstl_"
)

var errInvalidTelemetryToken = errors.New("invalid telemetry token")

// telemetryTokenRecord is safe to return through the administration API. The
// token secret is deliberately absent; it is returned only by
// createTelemetryToken.
type telemetryTokenRecord struct {
	ID         string     `json:"id"`
	Name       string     `json:"name"`
	Prefix     string     `json:"prefix"`
	Enabled    bool       `json:"enabled"`
	CreatedBy  string     `json:"createdBy,omitempty"`
	CreatedAt  time.Time  `json:"createdAt"`
	LastUsedAt *time.Time `json:"lastUsedAt,omitempty"`
}

type telemetryTokenCreated struct {
	telemetryTokenRecord
	Token string `json:"token"`
}

// lifecycleEvent is intentionally an explicit application-level signal.
// Redis Stream metadata can approximate publication age, but it cannot reveal
// when application processing started, completed, or was acknowledged.
type lifecycleEvent struct {
	TraceID             string            `json:"traceId,omitempty"`
	RequestID           string            `json:"requestId,omitempty"`
	SpanID              string            `json:"spanId,omitempty"`
	ParentSpanID        string            `json:"parentSpanId,omitempty"`
	ConnectionID        string            `json:"connectionId"`
	StreamKey           string            `json:"streamKey"`
	GroupName           string            `json:"groupName,omitempty"`
	EntryID             string            `json:"entryId,omitempty"`
	Consumer            string            `json:"consumer,omitempty"`
	Service             string            `json:"service,omitempty"`
	Operation           string            `json:"operation,omitempty"`
	Attributes          map[string]string `json:"attributes,omitempty"`
	RegisteredAt        *time.Time        `json:"registeredAt,omitempty"`
	ProcessingStartedAt *time.Time        `json:"processingStartedAt,omitempty"`
	ProcessedAt         *time.Time        `json:"processedAt,omitempty"`
	AcknowledgedAt      *time.Time        `json:"acknowledgedAt,omitempty"`
	Outcome             string            `json:"outcome,omitempty"`
	Error               string            `json:"error,omitempty"`
	Attempt             int               `json:"attempt,omitempty"`
}

type requestLifecycle struct {
	TraceID             string     `json:"traceId"`
	ConnectionID        string     `json:"connectionId"`
	StreamKey           string     `json:"streamKey"`
	GroupName           string     `json:"groupName,omitempty"`
	EntryID             string     `json:"entryId,omitempty"`
	Consumer            string     `json:"consumer,omitempty"`
	RegisteredAt        *time.Time `json:"registeredAt,omitempty"`
	ProcessingStartedAt *time.Time `json:"processingStartedAt,omitempty"`
	ProcessedAt         *time.Time `json:"processedAt,omitempty"`
	AcknowledgedAt      *time.Time `json:"acknowledgedAt,omitempty"`
	Outcome             string     `json:"outcome,omitempty"`
	Error               string     `json:"error,omitempty"`
	Attempt             int        `json:"attempt"`
	CreatedAt           time.Time  `json:"createdAt"`
	UpdatedAt           time.Time  `json:"updatedAt"`
}

type lifecycleQuery struct {
	ConnectionID string
	StreamKey    string
	GroupName    string
	TraceID      string
	From         time.Time
	To           time.Time
	Bucket       time.Duration
	Limit        int
}

type lifecycleDurationStats struct {
	Count int      `json:"count"`
	AvgMs *float64 `json:"avgMs"`
	P50Ms *float64 `json:"p50Ms"`
	P95Ms *float64 `json:"p95Ms"`
	P99Ms *float64 `json:"p99Ms"`
	MaxMs *float64 `json:"maxMs"`
}

type lifecycleSummary struct {
	Requests          int                    `json:"requests"`
	Registered        int                    `json:"registered"`
	ProcessingStarted int                    `json:"processingStarted"`
	Processed         int                    `json:"processed"`
	Acknowledged      int                    `json:"acknowledged"`
	Succeeded         int                    `json:"succeeded"`
	Failed            int                    `json:"failed"`
	InFlight          int                    `json:"inFlight"`
	QueueDelay        lifecycleDurationStats `json:"queueDelay"`
	Processing        lifecycleDurationStats `json:"processing"`
	Completion        lifecycleDurationStats `json:"completion"`
	AckDelay          lifecycleDurationStats `json:"ackDelay"`
	EndToEnd          lifecycleDurationStats `json:"endToEnd"`
}

type lifecycleSeriesPoint struct {
	At                time.Time              `json:"at"`
	Registered        int                    `json:"registered"`
	ProcessingStarted int                    `json:"processingStarted"`
	Processed         int                    `json:"processed"`
	Acknowledged      int                    `json:"acknowledged"`
	Succeeded         int                    `json:"succeeded"`
	Failed            int                    `json:"failed"`
	QueueDelay        lifecycleDurationStats `json:"queueDelay"`
	Processing        lifecycleDurationStats `json:"processing"`
	Completion        lifecycleDurationStats `json:"completion"`
	AckDelay          lifecycleDurationStats `json:"ackDelay"`
	EndToEnd          lifecycleDurationStats `json:"endToEnd"`

	queueValues      []float64
	processingValues []float64
	completionValues []float64
	ackValues        []float64
	endToEndValues   []float64
}

type lifecycleMetricsResponse struct {
	From         time.Time              `json:"from"`
	To           time.Time              `json:"to"`
	Bucket       string                 `json:"bucket"`
	Summary      lifecycleSummary       `json:"summary"`
	Points       []lifecycleSeriesPoint `json:"points"`
	Truncated    bool                   `json:"truncated"`
	Instrumented bool                   `json:"instrumented"`
}

type lifecycleRequestItem struct {
	requestLifecycle
	QueueDelayMs *float64 `json:"queueDelayMs"`
	ProcessingMs *float64 `json:"processingMs"`
	CompletionMs *float64 `json:"completionMs"`
	AckDelayMs   *float64 `json:"ackDelayMs"`
	EndToEndMs   *float64 `json:"endToEndMs"`
}

type lifecycleRequestQuery struct {
	ConnectionID string
	StreamKey    string
	GroupName    string
	Search       string
	Cursor       string
	Limit        int
}

type lifecycleRequestPage struct {
	Items      []lifecycleRequestItem `json:"items"`
	NextCursor string                 `json:"nextCursor"`
	HasMore    bool                   `json:"hasMore"`
}

type lifecycleRequestCursor struct {
	UpdatedAt int64  `json:"u"`
	TraceID   string `json:"t"`
}

func (s *store) migrateLifecycleTelemetry(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS telemetry_tokens (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			token_hash BLOB NOT NULL,
			token_prefix TEXT NOT NULL,
			enabled INTEGER NOT NULL DEFAULT 1,
			created_by TEXT NOT NULL DEFAULT '',
			created_at INTEGER NOT NULL,
			last_used_at INTEGER
		)`,
		`CREATE INDEX IF NOT EXISTS telemetry_tokens_created_idx ON telemetry_tokens(created_at DESC)`,
		`CREATE TABLE IF NOT EXISTS request_lifecycles (
			trace_id TEXT PRIMARY KEY,
			connection_id TEXT NOT NULL,
			stream_key TEXT NOT NULL,
			group_name TEXT NOT NULL DEFAULT '',
			entry_id TEXT NOT NULL DEFAULT '',
			consumer TEXT NOT NULL DEFAULT '',
			registered_at INTEGER,
			processing_started_at INTEGER,
			processed_at INTEGER,
			acknowledged_at INTEGER,
			outcome TEXT NOT NULL DEFAULT '',
			error_message TEXT NOT NULL DEFAULT '',
			attempt INTEGER NOT NULL DEFAULT 0,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS request_lifecycles_scope_time_idx
			ON request_lifecycles(connection_id, stream_key, group_name, updated_at DESC)`,
		`CREATE INDEX IF NOT EXISTS request_lifecycles_registered_idx
			ON request_lifecycles(connection_id, registered_at)`,
		`CREATE INDEX IF NOT EXISTS request_lifecycles_acknowledged_idx
			ON request_lifecycles(connection_id, acknowledged_at)`,
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("lifecycle telemetry migration: %w", err)
		}
	}
	return nil
}

func (s *store) createTelemetryToken(ctx context.Context, name, createdBy string) (telemetryTokenCreated, error) {
	name = strings.TrimSpace(name)
	createdBy = strings.TrimSpace(createdBy)
	if name == "" || len(name) > 80 || len(createdBy) > 128 {
		return telemetryTokenCreated{}, errors.New("telemetry token name is required and must not exceed 80 characters")
	}
	id, err := randomID(12)
	if err != nil {
		return telemetryTokenCreated{}, err
	}
	secretBytes := make([]byte, 32)
	if _, err := rand.Read(secretBytes); err != nil {
		return telemetryTokenCreated{}, err
	}
	secret := base64.RawURLEncoding.EncodeToString(secretBytes)
	rawToken := telemetryTokenPrefix + id + "." + secret
	hash := sha256.Sum256([]byte(secret))
	now := time.Now().UTC()
	prefix := rawToken
	if len(prefix) > 18 {
		prefix = prefix[:18]
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO telemetry_tokens(id, name, token_hash, token_prefix, enabled, created_by, created_at)
		VALUES(?,?,?,?,1,?,?)
	`, id, name, hash[:], prefix, createdBy, now.UnixNano())
	if err != nil {
		return telemetryTokenCreated{}, err
	}
	return telemetryTokenCreated{
		telemetryTokenRecord: telemetryTokenRecord{
			ID: id, Name: name, Prefix: prefix, Enabled: true,
			CreatedBy: createdBy, CreatedAt: now,
		},
		Token: rawToken,
	}, nil
}

func (s *store) listTelemetryTokens(ctx context.Context) ([]telemetryTokenRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, name, token_prefix, enabled, created_by, created_at, last_used_at
		FROM telemetry_tokens ORDER BY created_at DESC, id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]telemetryTokenRecord, 0)
	for rows.Next() {
		var item telemetryTokenRecord
		var enabled int
		var createdAt int64
		var lastUsedAt sql.NullInt64
		if err := rows.Scan(&item.ID, &item.Name, &item.Prefix, &enabled, &item.CreatedBy, &createdAt, &lastUsedAt); err != nil {
			return nil, err
		}
		item.Enabled = enabled == 1
		item.CreatedAt = time.Unix(0, createdAt).UTC()
		item.LastUsedAt = lifecycleTimeFromNull(lastUsedAt)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *store) updateTelemetryToken(ctx context.Context, id, name string, enabled bool) (telemetryTokenRecord, error) {
	id, name = strings.TrimSpace(id), strings.TrimSpace(name)
	if id == "" || name == "" || len(name) > 80 {
		return telemetryTokenRecord{}, errors.New("token id and a name of at most 80 characters are required")
	}
	result, err := s.db.ExecContext(ctx, `UPDATE telemetry_tokens SET name=?, enabled=? WHERE id=?`, name, boolInt(enabled), id)
	if err != nil {
		return telemetryTokenRecord{}, err
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		return telemetryTokenRecord{}, sql.ErrNoRows
	}
	return s.telemetryTokenByID(ctx, id)
}

func (s *store) deleteTelemetryToken(ctx context.Context, id string) (bool, error) {
	result, err := s.db.ExecContext(ctx, `DELETE FROM telemetry_tokens WHERE id=?`, strings.TrimSpace(id))
	if err != nil {
		return false, err
	}
	affected, _ := result.RowsAffected()
	return affected > 0, nil
}

func (s *store) telemetryTokenByID(ctx context.Context, id string) (telemetryTokenRecord, error) {
	var item telemetryTokenRecord
	var enabled int
	var createdAt int64
	var lastUsedAt sql.NullInt64
	err := s.db.QueryRowContext(ctx, `
		SELECT id, name, token_prefix, enabled, created_by, created_at, last_used_at
		FROM telemetry_tokens WHERE id=?
	`, id).Scan(&item.ID, &item.Name, &item.Prefix, &enabled, &item.CreatedBy, &createdAt, &lastUsedAt)
	if err != nil {
		return telemetryTokenRecord{}, err
	}
	item.Enabled = enabled == 1
	item.CreatedAt = time.Unix(0, createdAt).UTC()
	item.LastUsedAt = lifecycleTimeFromNull(lastUsedAt)
	return item, nil
}

func (s *store) authenticateTelemetryToken(ctx context.Context, raw string) (telemetryTokenRecord, error) {
	id, secret, ok := splitTelemetryToken(raw)
	if !ok {
		return telemetryTokenRecord{}, errInvalidTelemetryToken
	}
	var item telemetryTokenRecord
	var expected []byte
	var enabled int
	var createdAt int64
	var lastUsedAt sql.NullInt64
	err := s.db.QueryRowContext(ctx, `
		SELECT id, name, token_prefix, enabled, created_by, created_at, last_used_at, token_hash
		FROM telemetry_tokens WHERE id=?
	`, id).Scan(&item.ID, &item.Name, &item.Prefix, &enabled, &item.CreatedBy, &createdAt, &lastUsedAt, &expected)
	if err != nil {
		return telemetryTokenRecord{}, errInvalidTelemetryToken
	}
	actual := sha256.Sum256([]byte(secret))
	if enabled != 1 || len(expected) != sha256.Size || subtle.ConstantTimeCompare(expected, actual[:]) != 1 {
		return telemetryTokenRecord{}, errInvalidTelemetryToken
	}
	item.Enabled = true
	item.CreatedAt = time.Unix(0, createdAt).UTC()
	item.LastUsedAt = lifecycleTimeFromNull(lastUsedAt)
	now := time.Now().UTC()
	_, _ = s.db.ExecContext(ctx, `UPDATE telemetry_tokens SET last_used_at=? WHERE id=?`, now.UnixNano(), id)
	item.LastUsedAt = &now
	return item, nil
}

func splitTelemetryToken(raw string) (string, string, bool) {
	raw = strings.TrimSpace(raw)
	if len(raw) > 256 || !strings.HasPrefix(raw, telemetryTokenPrefix) {
		return "", "", false
	}
	parts := strings.Split(strings.TrimPrefix(raw, telemetryTokenPrefix), ".")
	if len(parts) != 2 || len(parts[0]) < 8 || len(parts[1]) < 32 {
		return "", "", false
	}
	if _, err := base64.RawURLEncoding.DecodeString(parts[0]); err != nil {
		return "", "", false
	}
	if _, err := base64.RawURLEncoding.DecodeString(parts[1]); err != nil {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func bearerTelemetryToken(request *http.Request) (string, bool) {
	parts := strings.Fields(request.Header.Get("Authorization"))
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return "", false
	}
	return parts[1], true
}

func (event *lifecycleEvent) normalizeAndValidate(now time.Time) error {
	event.TraceID = strings.TrimSpace(event.TraceID)
	event.RequestID = strings.TrimSpace(event.RequestID)
	if event.TraceID == "" {
		event.TraceID = event.RequestID
	}
	if event.TraceID == "" || len(event.TraceID) > 128 {
		return errors.New("traceId or requestId is required and must not exceed 128 characters")
	}
	if event.RequestID != "" && event.RequestID != event.TraceID {
		return errors.New("traceId and requestId must match when both are provided")
	}
	event.RequestID = ""
	event.SpanID = strings.TrimSpace(event.SpanID)
	event.ParentSpanID = strings.TrimSpace(event.ParentSpanID)
	event.ConnectionID = strings.TrimSpace(event.ConnectionID)
	event.StreamKey = strings.TrimSpace(event.StreamKey)
	event.GroupName = strings.TrimSpace(event.GroupName)
	event.EntryID = strings.TrimSpace(event.EntryID)
	event.Consumer = strings.TrimSpace(event.Consumer)
	event.Service = strings.TrimSpace(event.Service)
	event.Operation = strings.TrimSpace(event.Operation)
	event.Outcome = strings.ToLower(strings.TrimSpace(event.Outcome))
	event.Error = strings.TrimSpace(event.Error)
	if event.ConnectionID == "" || len(event.ConnectionID) > 128 {
		return errors.New("connectionId is required and must not exceed 128 characters")
	}
	if event.StreamKey == "" || len(event.StreamKey) > 1024 {
		return errors.New("streamKey is required and must not exceed 1024 characters")
	}
	if len(event.GroupName) > 512 || len(event.EntryID) > 128 || len(event.Consumer) > 512 {
		return errors.New("groupName, entryId, or consumer exceeds its maximum length")
	}
	if len(event.SpanID) > 128 || len(event.ParentSpanID) > 128 || len(event.Service) > 128 || len(event.Operation) > 128 {
		return errors.New("spanId, parentSpanId, service, or operation exceeds its maximum length")
	}
	if event.ParentSpanID != "" && event.SpanID == "" {
		return errors.New("spanId is required when parentSpanId is provided")
	}
	if len(event.Attributes) > 32 {
		return errors.New("attributes must not contain more than 32 items")
	}
	for key, value := range event.Attributes {
		trimmedKey, trimmedValue := strings.TrimSpace(key), strings.TrimSpace(value)
		if trimmedKey == "" || len(trimmedKey) > 128 || len(trimmedValue) > 1024 {
			return errors.New("attribute keys are required and attributes must stay within their size limits")
		}
		if trimmedKey != key {
			delete(event.Attributes, key)
		}
		event.Attributes[trimmedKey] = trimmedValue
	}
	if len(event.Outcome) > 64 || len(event.Error) > 4096 {
		return errors.New("outcome or error exceeds its maximum length")
	}
	if event.Attempt < 0 || event.Attempt > 1_000_000 {
		return errors.New("attempt must be between 0 and 1000000")
	}
	if event.RegisteredAt == nil && event.ProcessingStartedAt == nil && event.ProcessedAt == nil && event.AcknowledgedAt == nil {
		return errors.New("at least one lifecycle timestamp is required")
	}
	for _, value := range []*time.Time{event.RegisteredAt, event.ProcessingStartedAt, event.ProcessedAt, event.AcknowledgedAt} {
		if value == nil {
			continue
		}
		utc := value.UTC()
		*value = utc
		if utc.Before(time.Unix(0, 0)) || utc.After(now.Add(24*time.Hour)) {
			return errors.New("lifecycle timestamp is outside the accepted range")
		}
	}
	return validateLifecycleOrder(event.RegisteredAt, event.ProcessingStartedAt, event.ProcessedAt, event.AcknowledgedAt)
}

func validateLifecycleOrder(registered, started, processed, acknowledged *time.Time) error {
	if registered != nil && started != nil && started.Before(*registered) {
		return errors.New("processingStartedAt must not be before registeredAt")
	}
	if started != nil && processed != nil && processed.Before(*started) {
		return errors.New("processedAt must not be before processingStartedAt")
	}
	if processed != nil && acknowledged != nil && acknowledged.Before(*processed) {
		return errors.New("acknowledgedAt must not be before processedAt")
	}
	if registered != nil && processed != nil && processed.Before(*registered) {
		return errors.New("processedAt must not be before registeredAt")
	}
	if registered != nil && acknowledged != nil && acknowledged.Before(*registered) {
		return errors.New("acknowledgedAt must not be before registeredAt")
	}
	return nil
}

func (s *store) ingestLifecycleBatch(ctx context.Context, events []lifecycleEvent) error {
	if len(events) == 0 {
		return errors.New("events must contain at least one item")
	}
	if len(events) > lifecycleBatchLimit {
		return fmt.Errorf("events must not contain more than %d items", lifecycleBatchLimit)
	}
	now := time.Now().UTC()
	for index := range events {
		if err := events[index].normalizeAndValidate(now); err != nil {
			return fmt.Errorf("events[%d]: %w", index, err)
		}
	}
	transaction, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer transaction.Rollback()
	for index := range events {
		if events[index].SpanID == "" && events[index].ParentSpanID == "" {
			if err := upsertLifecycleEvent(ctx, transaction, events[index], now); err != nil {
				return fmt.Errorf("events[%d]: %w", index, err)
			}
		}
		if err := upsertTraceSpan(ctx, transaction, events[index], now); err != nil {
			return fmt.Errorf("events[%d]: %w", index, err)
		}
	}
	return transaction.Commit()
}

func upsertLifecycleEvent(ctx context.Context, transaction *sql.Tx, event lifecycleEvent, now time.Time) error {
	existing, err := getRequestLifecycleWith(ctx, transaction, event.TraceID)
	if errors.Is(err, sql.ErrNoRows) {
		_, err = transaction.ExecContext(ctx, `
			INSERT INTO request_lifecycles(
				trace_id, connection_id, stream_key, group_name, entry_id, consumer,
				registered_at, processing_started_at, processed_at, acknowledged_at,
				outcome, error_message, attempt, created_at, updated_at
			) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		`, event.TraceID, event.ConnectionID, event.StreamKey, event.GroupName, event.EntryID, event.Consumer,
			lifecycleNullableTime(event.RegisteredAt), lifecycleNullableTime(event.ProcessingStartedAt),
			lifecycleNullableTime(event.ProcessedAt), lifecycleNullableTime(event.AcknowledgedAt),
			event.Outcome, event.Error, event.Attempt, now.UnixNano(), now.UnixNano())
		return err
	}
	if err != nil {
		return err
	}
	merged, err := mergeLifecycle(existing, event, now)
	if err != nil {
		return err
	}
	_, err = transaction.ExecContext(ctx, `
		UPDATE request_lifecycles SET
			connection_id=?, stream_key=?, group_name=?, entry_id=?, consumer=?,
			registered_at=?, processing_started_at=?, processed_at=?, acknowledged_at=?,
			outcome=?, error_message=?, attempt=?, updated_at=?
		WHERE trace_id=?
	`, merged.ConnectionID, merged.StreamKey, merged.GroupName, merged.EntryID, merged.Consumer,
		lifecycleNullableTime(merged.RegisteredAt), lifecycleNullableTime(merged.ProcessingStartedAt),
		lifecycleNullableTime(merged.ProcessedAt), lifecycleNullableTime(merged.AcknowledgedAt),
		merged.Outcome, merged.Error, merged.Attempt, merged.UpdatedAt.UnixNano(), merged.TraceID)
	return err
}

func mergeLifecycle(existing requestLifecycle, event lifecycleEvent, now time.Time) (requestLifecycle, error) {
	if existing.ConnectionID != event.ConnectionID || existing.StreamKey != event.StreamKey {
		return requestLifecycle{}, errors.New("traceId is already associated with another connection or stream")
	}
	var err error
	if existing.GroupName, err = lifecycleMergeIdentity(existing.GroupName, event.GroupName, "groupName"); err != nil {
		return requestLifecycle{}, err
	}
	if existing.EntryID, err = lifecycleMergeIdentity(existing.EntryID, event.EntryID, "entryId"); err != nil {
		return requestLifecycle{}, err
	}
	previousAttempt := existing.Attempt
	if event.Attempt > existing.Attempt {
		existing.Attempt = event.Attempt
	}
	if event.Consumer != "" && (existing.Consumer == "" || event.Attempt >= previousAttempt) {
		existing.Consumer = event.Consumer
	}
	existing.RegisteredAt = lifecycleEarlier(existing.RegisteredAt, event.RegisteredAt)
	existing.ProcessingStartedAt = lifecycleEarlier(existing.ProcessingStartedAt, event.ProcessingStartedAt)
	existing.ProcessedAt = lifecycleLater(existing.ProcessedAt, event.ProcessedAt)
	existing.AcknowledgedAt = lifecycleLater(existing.AcknowledgedAt, event.AcknowledgedAt)
	if event.Outcome != "" && (existing.Outcome == "" || event.Attempt >= previousAttempt) {
		existing.Outcome = event.Outcome
		if lifecycleOutcomeSucceeded(event.Outcome) && event.Error == "" {
			existing.Error = ""
		}
	}
	if event.Error != "" && event.Attempt >= previousAttempt {
		existing.Error = event.Error
	}
	if err := validateLifecycleOrder(existing.RegisteredAt, existing.ProcessingStartedAt, existing.ProcessedAt, existing.AcknowledgedAt); err != nil {
		return requestLifecycle{}, err
	}
	existing.UpdatedAt = now
	return existing, nil
}

func lifecycleMergeIdentity(existing, incoming, field string) (string, error) {
	if incoming == "" {
		return existing, nil
	}
	if existing == "" || existing == incoming {
		return incoming, nil
	}
	return "", fmt.Errorf("traceId is already associated with another %s", field)
}

func lifecycleEarlier(first, second *time.Time) *time.Time {
	if first == nil {
		return lifecycleCloneTime(second)
	}
	if second != nil && second.Before(*first) {
		return lifecycleCloneTime(second)
	}
	return lifecycleCloneTime(first)
}

func lifecycleLater(first, second *time.Time) *time.Time {
	if first == nil {
		return lifecycleCloneTime(second)
	}
	if second != nil && second.After(*first) {
		return lifecycleCloneTime(second)
	}
	return lifecycleCloneTime(first)
}

func lifecycleCloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := value.UTC()
	return &copy
}

func lifecycleNullableTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return value.UTC().UnixNano()
}

type lifecycleQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func getRequestLifecycleWith(ctx context.Context, queryer lifecycleQueryer, traceID string) (requestLifecycle, error) {
	var record requestLifecycle
	var registered, started, processed, acknowledged sql.NullInt64
	var createdAt, updatedAt int64
	err := queryer.QueryRowContext(ctx, `
		SELECT trace_id, connection_id, stream_key, group_name, entry_id, consumer,
			registered_at, processing_started_at, processed_at, acknowledged_at,
			outcome, error_message, attempt, created_at, updated_at
		FROM request_lifecycles WHERE trace_id=?
	`, traceID).Scan(&record.TraceID, &record.ConnectionID, &record.StreamKey, &record.GroupName, &record.EntryID, &record.Consumer,
		&registered, &started, &processed, &acknowledged, &record.Outcome, &record.Error,
		&record.Attempt, &createdAt, &updatedAt)
	if err != nil {
		return requestLifecycle{}, err
	}
	record.RegisteredAt = lifecycleTimeFromNull(registered)
	record.ProcessingStartedAt = lifecycleTimeFromNull(started)
	record.ProcessedAt = lifecycleTimeFromNull(processed)
	record.AcknowledgedAt = lifecycleTimeFromNull(acknowledged)
	record.CreatedAt = time.Unix(0, createdAt).UTC()
	record.UpdatedAt = time.Unix(0, updatedAt).UTC()
	return record, nil
}

func (s *store) getRequestLifecycle(ctx context.Context, traceID string) (requestLifecycle, error) {
	return getRequestLifecycleWith(ctx, s.db, strings.TrimSpace(traceID))
}

func (s *store) listLifecycleRequests(ctx context.Context, query lifecycleRequestQuery) (lifecycleRequestPage, error) {
	query.ConnectionID = strings.TrimSpace(query.ConnectionID)
	query.StreamKey = strings.TrimSpace(query.StreamKey)
	query.GroupName = strings.TrimSpace(query.GroupName)
	query.Search = strings.TrimSpace(query.Search)
	if len(query.ConnectionID) > 128 || len(query.StreamKey) > 1024 || len(query.GroupName) > 512 || len(query.Search) > 256 {
		return lifecycleRequestPage{}, errors.New("a lifecycle request filter exceeds its maximum length")
	}
	if query.Limit <= 0 {
		query.Limit = 50
	}
	if query.Limit > 200 {
		query.Limit = 200
	}
	where := []string{"1=1"}
	args := make([]any, 0, 12)
	addFilter := func(column, value string) {
		if value != "" {
			where = append(where, column+"=?")
			args = append(args, value)
		}
	}
	addFilter("connection_id", query.ConnectionID)
	addFilter("stream_key", query.StreamKey)
	addFilter("group_name", query.GroupName)
	if query.Search != "" {
		pattern := "%" + escapeLifecycleLike(strings.ToLower(query.Search)) + "%"
		where = append(where, `(
			LOWER(trace_id) LIKE ? ESCAPE '\' OR
			LOWER(entry_id) LIKE ? ESCAPE '\' OR
			LOWER(consumer) LIKE ? ESCAPE '\' OR
			LOWER(outcome) LIKE ? ESCAPE '\' OR
			LOWER(error_message) LIKE ? ESCAPE '\'
		)`)
		args = append(args, pattern, pattern, pattern, pattern, pattern)
	}
	if query.Cursor != "" {
		cursor, err := decodeLifecycleRequestCursor(query.Cursor)
		if err != nil {
			return lifecycleRequestPage{}, err
		}
		where = append(where, `(updated_at < ? OR (updated_at = ? AND trace_id < ?))`)
		args = append(args, cursor.UpdatedAt, cursor.UpdatedAt, cursor.TraceID)
	}
	args = append(args, query.Limit+1)
	rows, err := s.db.QueryContext(ctx, `
		SELECT trace_id, connection_id, stream_key, group_name, entry_id, consumer,
			registered_at, processing_started_at, processed_at, acknowledged_at,
			outcome, error_message, attempt, created_at, updated_at
		FROM request_lifecycles
		WHERE `+strings.Join(where, " AND ")+`
		ORDER BY updated_at DESC, trace_id DESC
		LIMIT ?
	`, args...)
	if err != nil {
		return lifecycleRequestPage{}, err
	}
	defer rows.Close()
	items := make([]lifecycleRequestItem, 0, min(query.Limit, 64))
	for rows.Next() {
		var record requestLifecycle
		var registered, started, processed, acknowledged sql.NullInt64
		var createdAt, updatedAt int64
		if err := rows.Scan(&record.TraceID, &record.ConnectionID, &record.StreamKey, &record.GroupName, &record.EntryID, &record.Consumer,
			&registered, &started, &processed, &acknowledged, &record.Outcome, &record.Error,
			&record.Attempt, &createdAt, &updatedAt); err != nil {
			return lifecycleRequestPage{}, err
		}
		record.RegisteredAt = lifecycleTimeFromNull(registered)
		record.ProcessingStartedAt = lifecycleTimeFromNull(started)
		record.ProcessedAt = lifecycleTimeFromNull(processed)
		record.AcknowledgedAt = lifecycleTimeFromNull(acknowledged)
		record.CreatedAt = time.Unix(0, createdAt).UTC()
		record.UpdatedAt = time.Unix(0, updatedAt).UTC()
		items = append(items, lifecycleRequestWithDurations(record))
	}
	if err := rows.Err(); err != nil {
		return lifecycleRequestPage{}, err
	}
	hasMore := len(items) > query.Limit
	if hasMore {
		items = items[:query.Limit]
	}
	page := lifecycleRequestPage{Items: items, HasMore: hasMore}
	if hasMore && len(items) > 0 {
		page.NextCursor, err = encodeLifecycleRequestCursor(lifecycleRequestCursor{
			UpdatedAt: items[len(items)-1].UpdatedAt.UnixNano(),
			TraceID:   items[len(items)-1].TraceID,
		})
		if err != nil {
			return lifecycleRequestPage{}, err
		}
	}
	return page, nil
}

func lifecycleRequestWithDurations(record requestLifecycle) lifecycleRequestItem {
	item := lifecycleRequestItem{requestLifecycle: record}
	item.QueueDelayMs = lifecycleDurationPointer(record.RegisteredAt, record.ProcessingStartedAt)
	item.ProcessingMs = lifecycleDurationPointer(record.ProcessingStartedAt, record.ProcessedAt)
	item.CompletionMs = lifecycleDurationPointer(record.RegisteredAt, record.ProcessedAt)
	item.AckDelayMs = lifecycleDurationPointer(record.ProcessedAt, record.AcknowledgedAt)
	item.EndToEndMs = lifecycleDurationPointer(record.RegisteredAt, record.AcknowledgedAt)
	return item
}

func lifecycleDurationPointer(start, end *time.Time) *float64 {
	value, ok := lifecycleDurationMillis(start, end)
	if !ok {
		return nil
	}
	return &value
}

func escapeLifecycleLike(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `%`, `\%`)
	return strings.ReplaceAll(value, `_`, `\_`)
}

func encodeLifecycleRequestCursor(cursor lifecycleRequestCursor) (string, error) {
	if cursor.UpdatedAt <= 0 || strings.TrimSpace(cursor.TraceID) == "" {
		return "", errors.New("invalid lifecycle request cursor")
	}
	payload, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func decodeLifecycleRequestCursor(raw string) (lifecycleRequestCursor, error) {
	if len(raw) > 512 {
		return lifecycleRequestCursor{}, errors.New("invalid lifecycle request cursor")
	}
	payload, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return lifecycleRequestCursor{}, errors.New("invalid lifecycle request cursor")
	}
	var cursor lifecycleRequestCursor
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cursor); err != nil || cursor.UpdatedAt <= 0 || strings.TrimSpace(cursor.TraceID) == "" || len(cursor.TraceID) > 128 {
		return lifecycleRequestCursor{}, errors.New("invalid lifecycle request cursor")
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return lifecycleRequestCursor{}, errors.New("invalid lifecycle request cursor")
	}
	return cursor, nil
}

func lifecycleTimeFromNull(value sql.NullInt64) *time.Time {
	if !value.Valid {
		return nil
	}
	parsed := time.Unix(0, value.Int64).UTC()
	return &parsed
}

func (s *store) queryLifecycleMetrics(ctx context.Context, query lifecycleQuery) (lifecycleMetricsResponse, error) {
	now := time.Now().UTC()
	if query.To.IsZero() {
		query.To = now
	}
	if query.From.IsZero() {
		query.From = query.To.Add(-time.Hour)
	}
	query.From, query.To = query.From.UTC(), query.To.UTC()
	if !query.From.Before(query.To) {
		return lifecycleMetricsResponse{}, errors.New("from must be before to")
	}
	if query.Bucket <= 0 {
		query.Bucket = lifecycleDefaultBucket(query.To.Sub(query.From))
	}
	if query.Bucket < time.Second {
		query.Bucket = time.Second
	}
	if query.Limit <= 0 {
		query.Limit = lifecycleQueryDefaultLimit
	}
	if query.Limit > lifecycleQueryMaximumLimit {
		query.Limit = lifecycleQueryMaximumLimit
	}
	where := []string{`(
		(registered_at BETWEEN ? AND ?) OR
		(processing_started_at BETWEEN ? AND ?) OR
		(processed_at BETWEEN ? AND ?) OR
		(acknowledged_at BETWEEN ? AND ?)
	)`}
	fromNS, toNS := query.From.UnixNano(), query.To.UnixNano()
	args := []any{fromNS, toNS, fromNS, toNS, fromNS, toNS, fromNS, toNS}
	addFilter := func(column, value string) {
		value = strings.TrimSpace(value)
		if value != "" {
			where = append(where, column+"=?")
			args = append(args, value)
		}
	}
	addFilter("connection_id", query.ConnectionID)
	addFilter("stream_key", query.StreamKey)
	addFilter("group_name", query.GroupName)
	addFilter("trace_id", query.TraceID)
	// Rank by the newest lifecycle event that is actually inside the requested
	// window. A later acknowledgement outside the window must not displace a
	// newer in-window completion when the bounded result is truncated.
	args = append(args,
		fromNS, toNS,
		fromNS, toNS,
		fromNS, toNS,
		fromNS, toNS,
		query.Limit+1,
	)
	rows, err := s.db.QueryContext(ctx, `
		SELECT trace_id, connection_id, stream_key, group_name, entry_id, consumer,
			registered_at, processing_started_at, processed_at, acknowledged_at,
			outcome, error_message, attempt, created_at, updated_at
		FROM request_lifecycles
		WHERE `+strings.Join(where, " AND ")+`
		ORDER BY CASE
			WHEN acknowledged_at BETWEEN ? AND ? THEN acknowledged_at
			WHEN processed_at BETWEEN ? AND ? THEN processed_at
			WHEN processing_started_at BETWEEN ? AND ? THEN processing_started_at
			WHEN registered_at BETWEEN ? AND ? THEN registered_at
			ELSE updated_at
		END DESC, trace_id DESC
		LIMIT ?
	`, args...)
	if err != nil {
		return lifecycleMetricsResponse{}, err
	}
	defer rows.Close()
	records := make([]requestLifecycle, 0, min(query.Limit, 4096))
	for rows.Next() {
		var record requestLifecycle
		var registered, started, processed, acknowledged sql.NullInt64
		var createdAt, updatedAt int64
		if err := rows.Scan(&record.TraceID, &record.ConnectionID, &record.StreamKey, &record.GroupName, &record.EntryID, &record.Consumer,
			&registered, &started, &processed, &acknowledged, &record.Outcome, &record.Error,
			&record.Attempt, &createdAt, &updatedAt); err != nil {
			return lifecycleMetricsResponse{}, err
		}
		record.RegisteredAt = lifecycleTimeFromNull(registered)
		record.ProcessingStartedAt = lifecycleTimeFromNull(started)
		record.ProcessedAt = lifecycleTimeFromNull(processed)
		record.AcknowledgedAt = lifecycleTimeFromNull(acknowledged)
		record.CreatedAt = time.Unix(0, createdAt).UTC()
		record.UpdatedAt = time.Unix(0, updatedAt).UTC()
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return lifecycleMetricsResponse{}, err
	}
	truncated := len(records) > query.Limit
	if truncated {
		records = records[:query.Limit]
	}
	return aggregateLifecycleMetrics(records, query.From, query.To, query.Bucket, truncated), nil
}

func lifecycleDefaultBucket(window time.Duration) time.Duration {
	switch {
	case window <= 10*time.Minute:
		return time.Second
	case window <= 6*time.Hour:
		return time.Minute
	case window <= 7*24*time.Hour:
		return 5 * time.Minute
	default:
		return time.Hour
	}
}

type lifecycleDurationValues struct {
	queue, processing, completion, ack, endToEnd []float64
}

func aggregateLifecycleMetrics(records []requestLifecycle, from, to time.Time, bucket time.Duration, truncated bool) lifecycleMetricsResponse {
	response := lifecycleMetricsResponse{
		From: from, To: to, Bucket: bucket.String(), Points: make([]lifecycleSeriesPoint, 0),
		Truncated: truncated, Instrumented: len(records) > 0,
	}
	pointByTime := make(map[int64]*lifecycleSeriesPoint)
	values := lifecycleDurationValues{}
	pointFor := func(at *time.Time) *lifecycleSeriesPoint {
		if at == nil || at.Before(from) || at.After(to) {
			return nil
		}
		bucketAt := at.Truncate(bucket).UTC()
		key := bucketAt.UnixNano()
		point := pointByTime[key]
		if point == nil {
			point = &lifecycleSeriesPoint{At: bucketAt}
			pointByTime[key] = point
		}
		return point
	}
	for _, record := range records {
		response.Summary.Requests++
		if point := pointFor(record.RegisteredAt); point != nil {
			response.Summary.Registered++
			point.Registered++
		}
		if point := pointFor(record.ProcessingStartedAt); point != nil {
			response.Summary.ProcessingStarted++
			point.ProcessingStarted++
		}
		if point := pointFor(record.ProcessedAt); point != nil {
			response.Summary.Processed++
			point.Processed++
		}
		if point := pointFor(record.AcknowledgedAt); point != nil {
			response.Summary.Acknowledged++
			point.Acknowledged++
		}
		if record.AcknowledgedAt == nil && !lifecycleTerminal(record.Outcome, record.Error) {
			response.Summary.InFlight++
		}
		terminalPoint := pointFor(lifecycleTerminalTimestamp(record))
		if terminalPoint != nil && lifecycleOutcomeSucceeded(record.Outcome) {
			response.Summary.Succeeded++
			terminalPoint.Succeeded++
		} else if terminalPoint != nil && lifecycleOutcomeFailed(record.Outcome, record.Error) {
			response.Summary.Failed++
			terminalPoint.Failed++
		}
		if point := pointFor(record.ProcessingStartedAt); point != nil {
			if value, ok := lifecycleDurationMillis(record.RegisteredAt, record.ProcessingStartedAt); ok {
				values.queue = append(values.queue, value)
				point.queueValues = append(point.queueValues, value)
			}
		}
		if point := pointFor(record.ProcessedAt); point != nil {
			if value, ok := lifecycleDurationMillis(record.ProcessingStartedAt, record.ProcessedAt); ok {
				values.processing = append(values.processing, value)
				point.processingValues = append(point.processingValues, value)
			}
			if value, ok := lifecycleDurationMillis(record.RegisteredAt, record.ProcessedAt); ok {
				values.completion = append(values.completion, value)
				point.completionValues = append(point.completionValues, value)
			}
		}
		if point := pointFor(record.AcknowledgedAt); point != nil {
			if value, ok := lifecycleDurationMillis(record.ProcessedAt, record.AcknowledgedAt); ok {
				values.ack = append(values.ack, value)
				point.ackValues = append(point.ackValues, value)
			}
			if value, ok := lifecycleDurationMillis(record.RegisteredAt, record.AcknowledgedAt); ok {
				values.endToEnd = append(values.endToEnd, value)
				point.endToEndValues = append(point.endToEndValues, value)
			}
		}
	}
	response.Summary.QueueDelay = lifecycleStats(values.queue)
	response.Summary.Processing = lifecycleStats(values.processing)
	response.Summary.Completion = lifecycleStats(values.completion)
	response.Summary.AckDelay = lifecycleStats(values.ack)
	response.Summary.EndToEnd = lifecycleStats(values.endToEnd)
	keys := make([]int64, 0, len(pointByTime))
	for key := range pointByTime {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	for _, key := range keys {
		point := pointByTime[key]
		point.QueueDelay = lifecycleStats(point.queueValues)
		point.Processing = lifecycleStats(point.processingValues)
		point.Completion = lifecycleStats(point.completionValues)
		point.AckDelay = lifecycleStats(point.ackValues)
		point.EndToEnd = lifecycleStats(point.endToEndValues)
		point.queueValues, point.processingValues, point.completionValues, point.ackValues, point.endToEndValues = nil, nil, nil, nil, nil
		response.Points = append(response.Points, *point)
	}
	return response
}

func lifecycleTerminalTimestamp(record requestLifecycle) *time.Time {
	for _, value := range []*time.Time{record.ProcessedAt, record.AcknowledgedAt, record.ProcessingStartedAt, record.RegisteredAt} {
		if value != nil {
			return value
		}
	}
	return &record.UpdatedAt
}

func lifecycleDurationMillis(start, end *time.Time) (float64, bool) {
	if start == nil || end == nil || end.Before(*start) {
		return 0, false
	}
	return float64(end.Sub(*start).Nanoseconds()) / float64(time.Millisecond), true
}

func lifecycleStats(values []float64) lifecycleDurationStats {
	if len(values) == 0 {
		return lifecycleDurationStats{}
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	total := 0.0
	for _, value := range sorted {
		total += value
	}
	avg := total / float64(len(sorted))
	p50 := lifecyclePercentile(sorted, 0.50)
	p95 := lifecyclePercentile(sorted, 0.95)
	p99 := lifecyclePercentile(sorted, 0.99)
	maxValue := sorted[len(sorted)-1]
	return lifecycleDurationStats{
		Count: len(sorted), AvgMs: &avg, P50Ms: &p50, P95Ms: &p95, P99Ms: &p99, MaxMs: &maxValue,
	}
}

func lifecyclePercentile(sorted []float64, percentile float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if len(sorted) == 1 {
		return sorted[0]
	}
	position := percentile * float64(len(sorted)-1)
	lower := int(math.Floor(position))
	upper := int(math.Ceil(position))
	if lower == upper {
		return sorted[lower]
	}
	fraction := position - float64(lower)
	return sorted[lower] + (sorted[upper]-sorted[lower])*fraction
}

func lifecycleOutcomeSucceeded(outcome string) bool {
	switch strings.ToLower(strings.TrimSpace(outcome)) {
	case "success", "succeeded", "ok", "completed":
		return true
	default:
		return false
	}
}

func lifecycleOutcomeFailed(outcome, errorMessage string) bool {
	if strings.TrimSpace(errorMessage) != "" {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(outcome)) {
	case "error", "failed", "failure", "timeout", "cancelled", "canceled", "dead-lettered":
		return true
	default:
		return false
	}
}

func lifecycleTerminal(outcome, errorMessage string) bool {
	return lifecycleOutcomeSucceeded(outcome) || lifecycleOutcomeFailed(outcome, errorMessage)
}

// The following handlers are route-ready integration hooks. Administration
// handlers are expected to be wrapped in apiServer.protect by routes(); the
// ingest handler authenticates a dedicated telemetry bearer token itself.

func (s *apiServer) telemetryTokens(writer http.ResponseWriter, request *http.Request) {
	items, err := s.store.listTelemetryTokens(request.Context())
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "telemetry_tokens_failed", "Telemetry tokens could not be loaded.")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"items": items})
}

func (s *apiServer) createTelemetryToken(writer http.ResponseWriter, request *http.Request) {
	var input struct {
		Name string `json:"name"`
	}
	if err := readJSON(request, &input, 8<<10); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	session, _ := request.Context().Value(sessionContextKey).(sessionRecord)
	created, err := s.store.createTelemetryToken(request.Context(), input.Name, session.UserID)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "telemetry_token_create_failed", err.Error())
		return
	}
	writeJSON(writer, http.StatusCreated, created)
}

func (s *apiServer) updateTelemetryToken(writer http.ResponseWriter, request *http.Request) {
	var input struct {
		Name    string `json:"name"`
		Enabled bool   `json:"enabled"`
	}
	if err := readJSON(request, &input, 8<<10); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	item, err := s.store.updateTelemetryToken(request.Context(), request.PathValue("id"), input.Name, input.Enabled)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(writer, http.StatusNotFound, "telemetry_token_not_found", "Telemetry token was not found.")
		return
	}
	if err != nil {
		writeError(writer, http.StatusBadRequest, "telemetry_token_update_failed", err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, item)
}

func (s *apiServer) deleteTelemetryToken(writer http.ResponseWriter, request *http.Request) {
	deleted, err := s.store.deleteTelemetryToken(request.Context(), request.PathValue("id"))
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "telemetry_token_delete_failed", "Telemetry token could not be deleted.")
		return
	}
	if !deleted {
		writeError(writer, http.StatusNotFound, "telemetry_token_not_found", "Telemetry token was not found.")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"ok": true})
}

func (s *apiServer) ingestLifecycleEvents(writer http.ResponseWriter, request *http.Request) {
	rawToken, ok := bearerTelemetryToken(request)
	if !ok {
		writeError(writer, http.StatusUnauthorized, "telemetry_authentication_required", "A telemetry bearer token is required.")
		return
	}
	if _, err := s.store.authenticateTelemetryToken(request.Context(), rawToken); err != nil {
		writeError(writer, http.StatusUnauthorized, "invalid_telemetry_token", "The telemetry token is invalid or disabled.")
		return
	}
	var input struct {
		Events []lifecycleEvent `json:"events"`
	}
	if err := readJSON(request, &input, lifecycleBodyLimit); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if err := s.store.ingestLifecycleBatch(request.Context(), input.Events); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_lifecycle_events", err.Error())
		return
	}
	writeJSON(writer, http.StatusAccepted, map[string]any{"accepted": len(input.Events)})
}

func (s *apiServer) lifecycleMetrics(writer http.ResponseWriter, request *http.Request) {
	connectionID := strings.TrimSpace(request.URL.Query().Get("connectionId"))
	streamKey := strings.TrimSpace(request.URL.Query().Get("streamKey"))
	if !s.requireNonAdminStreamScope(writer, request, "streams:read", connectionID, streamKey, "lifecycle_scope_required") {
		return
	}
	now := time.Now().UTC()
	query := lifecycleQuery{
		ConnectionID: connectionID,
		StreamKey:    streamKey,
		GroupName:    request.URL.Query().Get("groupName"),
		TraceID:      request.URL.Query().Get("traceId"),
		To:           now,
		From:         now.Add(-time.Hour),
	}
	var err error
	if value := strings.TrimSpace(request.URL.Query().Get("from")); value != "" {
		query.From, err = time.Parse(time.RFC3339Nano, value)
		if err != nil {
			writeError(writer, http.StatusBadRequest, "invalid_time_range", "from must be an RFC3339 timestamp")
			return
		}
	}
	if value := strings.TrimSpace(request.URL.Query().Get("to")); value != "" {
		query.To, err = time.Parse(time.RFC3339Nano, value)
		if err != nil {
			writeError(writer, http.StatusBadRequest, "invalid_time_range", "to must be an RFC3339 timestamp")
			return
		}
	}
	if value := strings.TrimSpace(request.URL.Query().Get("bucket")); value != "" {
		query.Bucket, err = time.ParseDuration(value)
		if err != nil {
			writeError(writer, http.StatusBadRequest, "invalid_bucket", "bucket must be a duration such as 1s or 1m")
			return
		}
	}
	response, err := s.store.queryLifecycleMetrics(request.Context(), query)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "lifecycle_metrics_failed", err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, response)
}

func (s *apiServer) lifecycleRequests(writer http.ResponseWriter, request *http.Request) {
	connectionID := strings.TrimSpace(request.URL.Query().Get("connectionId"))
	streamKey := strings.TrimSpace(request.URL.Query().Get("streamKey"))
	if !s.requireNonAdminStreamScope(writer, request, "streams:read", connectionID, streamKey, "lifecycle_scope_required") {
		return
	}
	limit := 50
	if raw := strings.TrimSpace(request.URL.Query().Get("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 200 {
			writeError(writer, http.StatusBadRequest, "invalid_limit", "limit must be between 1 and 200")
			return
		}
		limit = parsed
	}
	page, err := s.store.listLifecycleRequests(request.Context(), lifecycleRequestQuery{
		ConnectionID: connectionID,
		StreamKey:    streamKey,
		GroupName:    request.URL.Query().Get("groupName"),
		Search:       request.URL.Query().Get("search"),
		Cursor:       request.URL.Query().Get("cursor"),
		Limit:        limit,
	})
	if err != nil {
		writeError(writer, http.StatusBadRequest, "lifecycle_requests_failed", err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, page)
}
