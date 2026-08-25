package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	schemaSampleDefault      = 200
	schemaSampleMaximum      = 500
	schemaSnapshotInterval   = 6 * time.Hour
	schemaCollectionInterval = 5 * time.Minute
	traceListDefaultLimit    = 50
	traceListMaximumLimit    = 200
	dashboardMaximumTargets  = 8
	dashboardMaximumPerOwner = 32
	dashboardMaximumTotal    = 512
	dashboardMaximumShared   = 128
	dashboardListMaximum     = 100
)

type schemaFieldInsight struct {
	Path       string   `json:"path"`
	Types      []string `json:"types"`
	Present    int      `json:"present"`
	PresencePC float64  `json:"presencePct"`
}

type schemaDriftInsight struct {
	Detected    bool       `json:"detected"`
	ComparedAt  *time.Time `json:"comparedAt,omitempty"`
	Added       []string   `json:"added"`
	Removed     []string   `json:"removed"`
	TypeChanged []string   `json:"typeChanged"`
}

type streamSchemaSnapshot struct {
	ID           int64                `json:"id"`
	ConnectionID string               `json:"connectionId"`
	StreamKey    string               `json:"streamKey"`
	Fingerprint  string               `json:"fingerprint"`
	SampleCount  int                  `json:"sampleCount"`
	JSONPayloads int                  `json:"jsonPayloads"`
	TextPayloads int                  `json:"textPayloads"`
	P50SizeBytes int                  `json:"p50SizeBytes"`
	P95SizeBytes int                  `json:"p95SizeBytes"`
	MaxSizeBytes int                  `json:"maxSizeBytes"`
	Fields       []schemaFieldInsight `json:"fields"`
	CapturedAt   time.Time            `json:"capturedAt"`
}

type streamSchemaAnalysis struct {
	streamSchemaSnapshot
	Drift   schemaDriftInsight     `json:"drift"`
	History []streamSchemaSnapshot `json:"history"`
}

type traceSpan struct {
	TraceID             string            `json:"traceId"`
	SpanID              string            `json:"spanId"`
	ParentSpanID        string            `json:"parentSpanId,omitempty"`
	ConnectionID        string            `json:"connectionId"`
	StreamKey           string            `json:"streamKey"`
	GroupName           string            `json:"groupName,omitempty"`
	EntryID             string            `json:"entryId,omitempty"`
	Consumer            string            `json:"consumer,omitempty"`
	Service             string            `json:"service,omitempty"`
	Operation           string            `json:"operation,omitempty"`
	RegisteredAt        *time.Time        `json:"registeredAt,omitempty"`
	ProcessingStartedAt *time.Time        `json:"processingStartedAt,omitempty"`
	ProcessedAt         *time.Time        `json:"processedAt,omitempty"`
	AcknowledgedAt      *time.Time        `json:"acknowledgedAt,omitempty"`
	Outcome             string            `json:"outcome,omitempty"`
	Error               string            `json:"error,omitempty"`
	Attempt             int               `json:"attempt"`
	Attributes          map[string]string `json:"attributes,omitempty"`
	CreatedAt           time.Time         `json:"createdAt"`
	UpdatedAt           time.Time         `json:"updatedAt"`
}

type traceSummary struct {
	TraceID     string     `json:"traceId"`
	SpanCount   int        `json:"spanCount"`
	StreamCount int        `json:"streamCount"`
	StartedAt   *time.Time `json:"startedAt,omitempty"`
	CompletedAt *time.Time `json:"completedAt,omitempty"`
	DurationMs  *float64   `json:"durationMs,omitempty"`
	Status      string     `json:"status"`
	UpdatedAt   time.Time  `json:"updatedAt"`
}

type tracePage struct {
	Items      []traceSummary `json:"items"`
	NextCursor string         `json:"nextCursor,omitempty"`
	HasMore    bool           `json:"hasMore"`
}

type dashboardTarget struct {
	ConnectionID string `json:"connectionId"`
	StreamKey    string `json:"streamKey"`
}

type dashboardDefinition struct {
	TimeRange string            `json:"timeRange"`
	Targets   []dashboardTarget `json:"targets"`
	Widgets   []string          `json:"widgets"`
}

type savedDashboard struct {
	ID         string              `json:"id"`
	Name       string              `json:"name"`
	OwnerID    string              `json:"ownerId"`
	Shared     bool                `json:"shared"`
	Definition dashboardDefinition `json:"definition"`
	CreatedAt  time.Time           `json:"createdAt"`
	UpdatedAt  time.Time           `json:"updatedAt"`
}

func (s *store) migrateInsights(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS stream_schema_snapshots (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			connection_id TEXT NOT NULL,
			stream_key TEXT NOT NULL,
			fingerprint TEXT NOT NULL,
			sample_count INTEGER NOT NULL,
			json_payloads INTEGER NOT NULL,
			text_payloads INTEGER NOT NULL,
			p50_size_bytes INTEGER NOT NULL,
			p95_size_bytes INTEGER NOT NULL,
			max_size_bytes INTEGER NOT NULL,
			fields_json TEXT NOT NULL,
			captured_at INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS stream_schema_snapshots_scope_idx
			ON stream_schema_snapshots(connection_id, stream_key, captured_at DESC)`,
		`CREATE TABLE IF NOT EXISTS trace_spans (
			trace_id TEXT NOT NULL,
			span_id TEXT NOT NULL,
			parent_span_id TEXT NOT NULL DEFAULT '',
			connection_id TEXT NOT NULL,
			stream_key TEXT NOT NULL,
			group_name TEXT NOT NULL DEFAULT '',
			entry_id TEXT NOT NULL DEFAULT '',
			consumer TEXT NOT NULL DEFAULT '',
			service TEXT NOT NULL DEFAULT '',
			operation TEXT NOT NULL DEFAULT '',
			registered_at INTEGER,
			processing_started_at INTEGER,
			processed_at INTEGER,
			acknowledged_at INTEGER,
			outcome TEXT NOT NULL DEFAULT '',
			error_message TEXT NOT NULL DEFAULT '',
			attempt INTEGER NOT NULL DEFAULT 0,
			attributes_json TEXT NOT NULL DEFAULT '{}',
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL,
			PRIMARY KEY(trace_id, span_id)
		)`,
		`CREATE INDEX IF NOT EXISTS trace_spans_scope_time_idx
			ON trace_spans(connection_id, stream_key, updated_at DESC)`,
		`CREATE INDEX IF NOT EXISTS trace_spans_trace_idx
			ON trace_spans(trace_id, registered_at, span_id)`,
		`CREATE TABLE IF NOT EXISTS saved_dashboards (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			owner_id TEXT NOT NULL,
			shared INTEGER NOT NULL DEFAULT 0,
			definition_json TEXT NOT NULL,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS saved_dashboards_owner_idx
			ON saved_dashboards(owner_id, updated_at DESC)`,
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("insights migration: %w", err)
		}
	}
	// Existing lifecycle rows become root spans without changing their current
	// metrics contract. Explicit span instrumentation can then extend the trace.
	_, err := s.db.ExecContext(ctx, `
		INSERT OR IGNORE INTO trace_spans(
			trace_id,span_id,connection_id,stream_key,group_name,entry_id,consumer,
			registered_at,processing_started_at,processed_at,acknowledged_at,
			outcome,error_message,attempt,created_at,updated_at
		)
		SELECT trace_id,'root',connection_id,stream_key,group_name,entry_id,consumer,
			registered_at,processing_started_at,processed_at,acknowledged_at,
			outcome,error_message,attempt,created_at,updated_at
		FROM request_lifecycles`)
	return err
}

func analyzeStreamSchema(connectionID, streamKey string, messages []redis.XMessage, capturedAt time.Time) streamSchemaSnapshot {
	type fieldState struct {
		present int
		types   map[string]struct{}
	}
	states := map[string]*fieldState{}
	sizes := make([]int, 0, len(messages))
	jsonPayloads, textPayloads := 0, 0
	for _, message := range messages {
		entryPaths := map[string]map[string]struct{}{}
		size := len(message.ID)
		messageHasJSON := false
		for name, raw := range message.Values {
			value := fmt.Sprint(raw)
			size += len(name) + len(value)
			kind, decoded, isJSON := classifySchemaValue(value)
			addEntrySchemaPath(entryPaths, name, kind)
			if isJSON {
				messageHasJSON = true
				flattenSchemaValue(entryPaths, name, decoded, 0)
			}
		}
		if messageHasJSON {
			jsonPayloads++
		} else {
			textPayloads++
		}
		for path, kinds := range entryPaths {
			state := states[path]
			if state == nil {
				state = &fieldState{types: map[string]struct{}{}}
				states[path] = state
			}
			state.present++
			for kind := range kinds {
				state.types[kind] = struct{}{}
			}
		}
		sizes = append(sizes, size)
	}
	fields := make([]schemaFieldInsight, 0, len(states))
	for path, state := range states {
		types := make([]string, 0, len(state.types))
		for kind := range state.types {
			types = append(types, kind)
		}
		sort.Strings(types)
		presence := 0.0
		if len(messages) > 0 {
			presence = float64(state.present) * 100 / float64(len(messages))
		}
		fields = append(fields, schemaFieldInsight{Path: path, Types: types, Present: state.present, PresencePC: math.Round(presence*10) / 10})
	}
	sort.Slice(fields, func(i, j int) bool { return fields[i].Path < fields[j].Path })
	fingerprintSource := make([]string, 0, len(fields))
	for _, field := range fields {
		fingerprintSource = append(fingerprintSource, field.Path+":"+strings.Join(field.Types, "|"))
	}
	hash := sha256.Sum256([]byte(strings.Join(fingerprintSource, "\n")))
	sort.Ints(sizes)
	return streamSchemaSnapshot{
		ConnectionID: connectionID, StreamKey: streamKey,
		Fingerprint: hex.EncodeToString(hash[:8]), SampleCount: len(messages),
		JSONPayloads: jsonPayloads, TextPayloads: textPayloads,
		P50SizeBytes: percentileInt(sizes, 0.50), P95SizeBytes: percentileInt(sizes, 0.95), MaxSizeBytes: percentileInt(sizes, 1),
		Fields: fields, CapturedAt: capturedAt.UTC(),
	}
}

func classifySchemaValue(value string) (string, any, bool) {
	trimmed := strings.TrimSpace(value)
	var decoded any
	if trimmed != "" && json.Unmarshal([]byte(trimmed), &decoded) == nil {
		switch decoded.(type) {
		case map[string]any:
			return "object", decoded, true
		case []any:
			return "array", decoded, true
		case bool:
			return "boolean", decoded, true
		case nil:
			return "null", decoded, true
		case float64:
			if !strings.ContainsAny(trimmed, ".eE") {
				return "integer", decoded, true
			}
			return "number", decoded, true
		case string:
			return "string", decoded, true
		}
	}
	return "string", value, false
}

func flattenSchemaValue(paths map[string]map[string]struct{}, prefix string, value any, depth int) {
	if depth >= 4 || len(paths) >= 256 {
		return
	}
	switch typed := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		if len(keys) > 100 {
			keys = keys[:100]
		}
		for _, key := range keys {
			path := prefix + "." + key
			kind := schemaDecodedType(typed[key])
			addEntrySchemaPath(paths, path, kind)
			flattenSchemaValue(paths, path, typed[key], depth+1)
		}
	case []any:
		limit := min(len(typed), 8)
		for index := 0; index < limit; index++ {
			path := prefix + "[]"
			kind := schemaDecodedType(typed[index])
			addEntrySchemaPath(paths, path, kind)
			flattenSchemaValue(paths, path, typed[index], depth+1)
		}
	}
}

func schemaDecodedType(value any) string {
	switch typed := value.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case float64:
		if typed == math.Trunc(typed) {
			return "integer"
		}
		return "number"
	case string:
		return "string"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	default:
		return "string"
	}
}

func addEntrySchemaPath(paths map[string]map[string]struct{}, path, kind string) {
	if len(path) > 1024 || len(paths) >= 256 {
		return
	}
	if paths[path] == nil {
		paths[path] = map[string]struct{}{}
	}
	paths[path][kind] = struct{}{}
}

func percentileInt(sorted []int, percentile float64) int {
	if len(sorted) == 0 {
		return 0
	}
	index := int(math.Ceil(float64(len(sorted))*percentile)) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func compareSchemaSnapshots(previous, current streamSchemaSnapshot) schemaDriftInsight {
	result := schemaDriftInsight{Added: []string{}, Removed: []string{}, TypeChanged: []string{}}
	if previous.ID == 0 && previous.Fingerprint == "" {
		return result
	}
	comparedAt := previous.CapturedAt
	result.ComparedAt = &comparedAt
	previousFields, currentFields := map[string]string{}, map[string]string{}
	for _, field := range previous.Fields {
		previousFields[field.Path] = strings.Join(field.Types, "|")
	}
	for _, field := range current.Fields {
		currentFields[field.Path] = strings.Join(field.Types, "|")
	}
	for path, kinds := range currentFields {
		before, exists := previousFields[path]
		if !exists {
			result.Added = append(result.Added, path)
		} else if before != kinds {
			result.TypeChanged = append(result.TypeChanged, path)
		}
	}
	for path := range previousFields {
		if _, exists := currentFields[path]; !exists {
			result.Removed = append(result.Removed, path)
		}
	}
	sort.Strings(result.Added)
	sort.Strings(result.Removed)
	sort.Strings(result.TypeChanged)
	result.Detected = len(result.Added)+len(result.Removed)+len(result.TypeChanged) > 0
	return result
}

func (s *store) recordSchemaSnapshot(ctx context.Context, snapshot streamSchemaSnapshot) (streamSchemaAnalysis, error) {
	previous, err := s.latestSchemaSnapshot(ctx, snapshot.ConnectionID, snapshot.StreamKey)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return streamSchemaAnalysis{}, err
	}
	drift := compareSchemaSnapshots(previous, snapshot)
	shouldInsert := errors.Is(err, sql.ErrNoRows) || previous.Fingerprint != snapshot.Fingerprint || snapshot.CapturedAt.Sub(previous.CapturedAt) >= schemaSnapshotInterval
	if shouldInsert {
		fieldsJSON, marshalErr := json.Marshal(snapshot.Fields)
		if marshalErr != nil {
			return streamSchemaAnalysis{}, marshalErr
		}
		result, insertErr := s.db.ExecContext(ctx, `
			INSERT INTO stream_schema_snapshots(
				connection_id,stream_key,fingerprint,sample_count,json_payloads,text_payloads,
				p50_size_bytes,p95_size_bytes,max_size_bytes,fields_json,captured_at
			) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
			snapshot.ConnectionID, snapshot.StreamKey, snapshot.Fingerprint, snapshot.SampleCount,
			snapshot.JSONPayloads, snapshot.TextPayloads, snapshot.P50SizeBytes, snapshot.P95SizeBytes,
			snapshot.MaxSizeBytes, string(fieldsJSON), snapshot.CapturedAt.UnixNano())
		if insertErr != nil {
			return streamSchemaAnalysis{}, insertErr
		}
		snapshot.ID, _ = result.LastInsertId()
	} else {
		snapshot.ID = previous.ID
	}
	history, err := s.listSchemaSnapshots(ctx, snapshot.ConnectionID, snapshot.StreamKey, 20)
	if err != nil {
		return streamSchemaAnalysis{}, err
	}
	return streamSchemaAnalysis{streamSchemaSnapshot: snapshot, Drift: drift, History: history}, nil
}

func (s *store) latestSchemaSnapshot(ctx context.Context, connectionID, streamKey string) (streamSchemaSnapshot, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id,connection_id,stream_key,fingerprint,sample_count,json_payloads,text_payloads,
			p50_size_bytes,p95_size_bytes,max_size_bytes,fields_json,captured_at
		FROM stream_schema_snapshots WHERE connection_id=? AND stream_key=?
		ORDER BY captured_at DESC,id DESC LIMIT 1`, connectionID, streamKey)
	return scanSchemaSnapshot(row)
}

func (s *store) listSchemaSnapshots(ctx context.Context, connectionID, streamKey string, limit int) ([]streamSchemaSnapshot, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id,connection_id,stream_key,fingerprint,sample_count,json_payloads,text_payloads,
			p50_size_bytes,p95_size_bytes,max_size_bytes,fields_json,captured_at
		FROM stream_schema_snapshots WHERE connection_id=? AND stream_key=?
		ORDER BY captured_at DESC,id DESC LIMIT ?`, connectionID, streamKey, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]streamSchemaSnapshot, 0, limit)
	for rows.Next() {
		item, scanErr := scanSchemaSnapshot(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

type schemaSnapshotScanner interface{ Scan(...any) error }

func scanSchemaSnapshot(scanner schemaSnapshotScanner) (streamSchemaSnapshot, error) {
	var item streamSchemaSnapshot
	var fieldsJSON string
	var capturedAt int64
	err := scanner.Scan(&item.ID, &item.ConnectionID, &item.StreamKey, &item.Fingerprint, &item.SampleCount,
		&item.JSONPayloads, &item.TextPayloads, &item.P50SizeBytes, &item.P95SizeBytes, &item.MaxSizeBytes,
		&fieldsJSON, &capturedAt)
	if err != nil {
		return streamSchemaSnapshot{}, err
	}
	if err := json.Unmarshal([]byte(fieldsJSON), &item.Fields); err != nil {
		return streamSchemaSnapshot{}, err
	}
	item.CapturedAt = time.Unix(0, capturedAt).UTC()
	return item, nil
}

func (s *apiServer) streamSchemaAnalysis(writer http.ResponseWriter, request *http.Request) {
	connectionID := strings.TrimSpace(request.URL.Query().Get("connectionId"))
	streamKey := strings.TrimSpace(request.URL.Query().Get("streamKey"))
	if connectionID == "" || streamKey == "" || len(streamKey) > 1024 {
		writeError(writer, http.StatusBadRequest, "invalid_scope", "connectionId and streamKey are required")
		return
	}
	connection, err := s.redis.get(connectionID)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "unknown_connection", err.Error())
		return
	}
	limit := schemaSampleDefault
	if raw := strings.TrimSpace(request.URL.Query().Get("sample")); raw != "" {
		parsed, parseErr := strconv.Atoi(raw)
		if parseErr != nil || parsed < 1 || parsed > schemaSampleMaximum {
			writeError(writer, http.StatusBadRequest, "invalid_sample", "sample must be between 1 and 500")
			return
		}
		limit = parsed
	}
	ctx, cancel := context.WithTimeout(request.Context(), 4*time.Second)
	defer cancel()
	messages, err := connection.client.XRevRangeN(ctx, streamKey, "+", "-", int64(limit)).Result()
	if err != nil {
		writeRedisError(writer, err)
		return
	}
	analysis, err := s.store.recordSchemaSnapshot(ctx, analyzeStreamSchema(connectionID, streamKey, messages, time.Now().UTC()))
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "schema_analysis_failed", err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, analysis)
}

// startInsightCollection keeps a low-frequency schema baseline for explicitly
// monitored streams. The work is deliberately isolated from the one-second
// metrics collector so a large payload cannot delay live operational samples.
func (s *apiServer) startInsightCollection(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.collectSchemaSnapshots(ctx)
		ticker := time.NewTicker(schemaCollectionInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.collectSchemaSnapshots(ctx)
			}
		}
	}()
	return done
}

func (s *apiServer) collectSchemaSnapshots(parent context.Context) {
	for _, connectionID := range s.redis.ids() {
		if parent.Err() != nil {
			return
		}
		connection, err := s.redis.get(connectionID)
		if err != nil {
			continue
		}
		listContext, cancelList := context.WithTimeout(parent, 2*time.Second)
		monitored, err := s.store.listMonitoredStreams(listContext, connectionID)
		cancelList()
		if err != nil {
			log.Printf(`{"level":"warn","message":"Unable to load monitored streams for schema analysis","connection":%q,"error":%q}`, connectionID, err)
			continue
		}
		for _, stream := range monitored {
			if parent.Err() != nil {
				return
			}
			ctx, cancel := context.WithTimeout(parent, 3*time.Second)
			messages, readErr := connection.client.XRevRangeN(ctx, stream.Key, "+", "-", schemaSampleDefault).Result()
			if readErr == nil {
				_, readErr = s.store.recordSchemaSnapshot(ctx, analyzeStreamSchema(connectionID, stream.Key, messages, time.Now().UTC()))
			}
			cancel()
			if readErr != nil && !errors.Is(readErr, context.Canceled) && !errors.Is(readErr, context.DeadlineExceeded) {
				log.Printf(`{"level":"warn","message":"Unable to collect stream schema","connection":%q,"stream":%q,"error":%q}`, connectionID, stream.Key, readErr)
			}
		}
	}
}

func upsertTraceSpan(ctx context.Context, transaction *sql.Tx, event lifecycleEvent, now time.Time) error {
	spanID := strings.TrimSpace(event.SpanID)
	if spanID == "" {
		spanID = "root"
	}
	existing, err := getTraceSpanWith(ctx, transaction, event.TraceID, spanID)
	if errors.Is(err, sql.ErrNoRows) {
		attributes, marshalErr := json.Marshal(event.Attributes)
		if marshalErr != nil {
			return marshalErr
		}
		_, err = transaction.ExecContext(ctx, `
			INSERT INTO trace_spans(
				trace_id,span_id,parent_span_id,connection_id,stream_key,group_name,entry_id,consumer,
				service,operation,registered_at,processing_started_at,processed_at,acknowledged_at,
				outcome,error_message,attempt,attributes_json,created_at,updated_at
			) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			event.TraceID, spanID, event.ParentSpanID, event.ConnectionID, event.StreamKey, event.GroupName,
			event.EntryID, event.Consumer, event.Service, event.Operation, lifecycleNullableTime(event.RegisteredAt),
			lifecycleNullableTime(event.ProcessingStartedAt), lifecycleNullableTime(event.ProcessedAt),
			lifecycleNullableTime(event.AcknowledgedAt), event.Outcome, event.Error, event.Attempt,
			string(attributes), now.UnixNano(), now.UnixNano())
		return err
	}
	if err != nil {
		return err
	}
	if existing.ConnectionID != event.ConnectionID || existing.StreamKey != event.StreamKey {
		return errors.New("spanId is already associated with another connection or stream")
	}
	for _, identity := range []struct {
		current  string
		incoming string
		label    string
	}{
		{existing.ParentSpanID, event.ParentSpanID, "parentSpanId"},
		{existing.GroupName, event.GroupName, "groupName"},
		{existing.EntryID, event.EntryID, "entryId"},
	} {
		if identity.current != "" && identity.incoming != "" && identity.current != identity.incoming {
			return fmt.Errorf("spanId is already associated with another %s", identity.label)
		}
	}
	existing.ParentSpanID = firstTraceNonEmpty(existing.ParentSpanID, event.ParentSpanID)
	existing.GroupName = firstTraceNonEmpty(existing.GroupName, event.GroupName)
	existing.EntryID = firstTraceNonEmpty(existing.EntryID, event.EntryID)
	existing.Service = firstTraceNonEmpty(existing.Service, event.Service)
	existing.Operation = firstTraceNonEmpty(existing.Operation, event.Operation)
	if event.Consumer != "" && event.Attempt >= existing.Attempt {
		existing.Consumer = event.Consumer
	}
	existing.RegisteredAt = lifecycleEarlier(existing.RegisteredAt, event.RegisteredAt)
	existing.ProcessingStartedAt = lifecycleEarlier(existing.ProcessingStartedAt, event.ProcessingStartedAt)
	existing.ProcessedAt = lifecycleLater(existing.ProcessedAt, event.ProcessedAt)
	existing.AcknowledgedAt = lifecycleLater(existing.AcknowledgedAt, event.AcknowledgedAt)
	if err := validateLifecycleOrder(existing.RegisteredAt, existing.ProcessingStartedAt, existing.ProcessedAt, existing.AcknowledgedAt); err != nil {
		return err
	}
	if event.Outcome != "" && event.Attempt >= existing.Attempt {
		existing.Outcome = event.Outcome
		if lifecycleOutcomeSucceeded(event.Outcome) && event.Error == "" {
			existing.Error = ""
		}
	}
	if event.Error != "" && event.Attempt >= existing.Attempt {
		existing.Error = event.Error
	}
	if event.Attempt > existing.Attempt {
		existing.Attempt = event.Attempt
	}
	if existing.Attributes == nil {
		existing.Attributes = map[string]string{}
	}
	for key, value := range event.Attributes {
		existing.Attributes[key] = value
	}
	attributes, err := json.Marshal(existing.Attributes)
	if err != nil {
		return err
	}
	_, err = transaction.ExecContext(ctx, `
		UPDATE trace_spans SET parent_span_id=?,group_name=?,entry_id=?,consumer=?,service=?,operation=?,
			registered_at=?,processing_started_at=?,processed_at=?,acknowledged_at=?,outcome=?,error_message=?,
			attempt=?,attributes_json=?,updated_at=? WHERE trace_id=? AND span_id=?`,
		existing.ParentSpanID, existing.GroupName, existing.EntryID, existing.Consumer, existing.Service, existing.Operation,
		lifecycleNullableTime(existing.RegisteredAt), lifecycleNullableTime(existing.ProcessingStartedAt),
		lifecycleNullableTime(existing.ProcessedAt), lifecycleNullableTime(existing.AcknowledgedAt), existing.Outcome,
		existing.Error, existing.Attempt, string(attributes), now.UnixNano(), event.TraceID, spanID)
	return err
}

func firstTraceNonEmpty(current, incoming string) string {
	if current != "" {
		return current
	}
	return incoming
}

type traceSpanScanner interface{ Scan(...any) error }

func getTraceSpanWith(ctx context.Context, queryer lifecycleQueryer, traceID, spanID string) (traceSpan, error) {
	row := queryer.QueryRowContext(ctx, `
		SELECT trace_id,span_id,parent_span_id,connection_id,stream_key,group_name,entry_id,consumer,
			service,operation,registered_at,processing_started_at,processed_at,acknowledged_at,
			outcome,error_message,attempt,attributes_json,created_at,updated_at
		FROM trace_spans WHERE trace_id=? AND span_id=?`, traceID, spanID)
	return scanTraceSpan(row)
}

func scanTraceSpan(scanner traceSpanScanner) (traceSpan, error) {
	var item traceSpan
	var registered, started, processed, acknowledged sql.NullInt64
	var attributes string
	var createdAt, updatedAt int64
	err := scanner.Scan(&item.TraceID, &item.SpanID, &item.ParentSpanID, &item.ConnectionID, &item.StreamKey,
		&item.GroupName, &item.EntryID, &item.Consumer, &item.Service, &item.Operation,
		&registered, &started, &processed, &acknowledged, &item.Outcome, &item.Error, &item.Attempt,
		&attributes, &createdAt, &updatedAt)
	if err != nil {
		return traceSpan{}, err
	}
	item.RegisteredAt = lifecycleTimeFromNull(registered)
	item.ProcessingStartedAt = lifecycleTimeFromNull(started)
	item.ProcessedAt = lifecycleTimeFromNull(processed)
	item.AcknowledgedAt = lifecycleTimeFromNull(acknowledged)
	item.CreatedAt = time.Unix(0, createdAt).UTC()
	item.UpdatedAt = time.Unix(0, updatedAt).UTC()
	if err := json.Unmarshal([]byte(attributes), &item.Attributes); err != nil {
		return traceSpan{}, err
	}
	return item, nil
}

func scanTraceSpanWithMatchUpdated(scanner traceSpanScanner) (traceSpan, int64, error) {
	var item traceSpan
	var registered, started, processed, acknowledged sql.NullInt64
	var attributes string
	var createdAt, updatedAt, matchUpdatedAt int64
	err := scanner.Scan(&item.TraceID, &item.SpanID, &item.ParentSpanID, &item.ConnectionID, &item.StreamKey,
		&item.GroupName, &item.EntryID, &item.Consumer, &item.Service, &item.Operation,
		&registered, &started, &processed, &acknowledged, &item.Outcome, &item.Error, &item.Attempt,
		&attributes, &createdAt, &updatedAt, &matchUpdatedAt)
	if err != nil {
		return traceSpan{}, 0, err
	}
	item.RegisteredAt = lifecycleTimeFromNull(registered)
	item.ProcessingStartedAt = lifecycleTimeFromNull(started)
	item.ProcessedAt = lifecycleTimeFromNull(processed)
	item.AcknowledgedAt = lifecycleTimeFromNull(acknowledged)
	item.CreatedAt = time.Unix(0, createdAt).UTC()
	item.UpdatedAt = time.Unix(0, updatedAt).UTC()
	if err := json.Unmarshal([]byte(attributes), &item.Attributes); err != nil {
		return traceSpan{}, 0, err
	}
	return item, matchUpdatedAt, nil
}

func (s *store) listTraceSpans(ctx context.Context, traceID string) ([]traceSpan, error) {
	traceID = strings.TrimSpace(traceID)
	if traceID == "" || len(traceID) > 256 {
		return nil, errors.New("traceId is required and must not exceed 256 characters")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT trace_id,span_id,parent_span_id,connection_id,stream_key,group_name,entry_id,consumer,
			service,operation,registered_at,processing_started_at,processed_at,acknowledged_at,
			outcome,error_message,attempt,attributes_json,created_at,updated_at
		FROM trace_spans WHERE trace_id=?
		ORDER BY COALESCE(registered_at,created_at),span_id`, traceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []traceSpan{}
	for rows.Next() {
		item, scanErr := scanTraceSpan(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

type traceCursor struct {
	UpdatedAt int64  `json:"u"`
	TraceID   string `json:"t"`
}

func traceVisibilitySQL(checker scopedPermissionChecker, alias string) (string, []any) {
	if checker.admin {
		return "1=1", nil
	}
	scopeExpression := "('stream:' || " + alias + ".connection_id || ':' || " + alias + ".stream_key)"
	condition := func(pattern string) (string, []any) {
		if pattern == "*" {
			return "1=1", nil
		}
		if strings.HasSuffix(pattern, "*") {
			return scopeExpression + ` LIKE ? ESCAPE '\'`, []any{escapeLifecycleLike(strings.TrimSuffix(pattern, "*")) + "%"}
		}
		return scopeExpression + "=?", []any{pattern}
	}
	allow := []string{}
	allowArgs := []any{}
	if roleAllows(checker.role, "streams:read") {
		allow = append(allow, "1=1")
	}
	deny := []string{}
	denyArgs := []any{}
	for _, grant := range checker.grants {
		if !wildcardMatch(grant.action, "streams:read") {
			continue
		}
		clause, args := condition(grant.scope)
		switch grant.effect {
		case "allow":
			allow = append(allow, clause)
			allowArgs = append(allowArgs, args...)
		case "deny":
			deny = append(deny, clause)
			denyArgs = append(denyArgs, args...)
		}
	}
	if len(allow) == 0 {
		return "0=1", nil
	}
	expression := "(" + strings.Join(allow, " OR ") + ")"
	if len(deny) > 0 {
		expression += " AND NOT (" + strings.Join(deny, " OR ") + ")"
	}
	return expression, append(allowArgs, denyArgs...)
}

// listTraceSummaries uses the requested connection/stream/search filters only
// to select trace IDs. Once selected, it aggregates every span the caller is
// allowed to see so a cross-stream trace is not collapsed to the filter span.
// ACL, cursor ordering, and LIMIT are applied in SQLite before span details are
// loaded, bounding memory to at most limit+1 traces while keeping denied spans
// out of counts, status, timing, and cursor order.
func (s *store) listTraceSummaries(ctx context.Context, connectionID, streamKey, search, cursor string, limit int, checker scopedPermissionChecker) (tracePage, error) {
	connectionID, streamKey, search = strings.TrimSpace(connectionID), strings.TrimSpace(streamKey), strings.TrimSpace(search)
	if len(connectionID) > 128 || len(streamKey) > 1024 || len(search) > 256 {
		return tracePage{}, errors.New("a trace filter exceeds its maximum length")
	}
	if limit <= 0 {
		limit = traceListDefaultLimit
	}
	if limit > traceListMaximumLimit {
		limit = traceListMaximumLimit
	}
	matchVisibility, matchVisibilityArgs := traceVisibilitySQL(checker, "matched")
	where, args := []string{matchVisibility}, append([]any(nil), matchVisibilityArgs...)
	if connectionID != "" {
		where = append(where, "matched.connection_id=?")
		args = append(args, connectionID)
	}
	if streamKey != "" {
		where = append(where, "matched.stream_key=?")
		args = append(args, streamKey)
	}
	if search != "" {
		pattern := "%" + escapeLifecycleLike(strings.ToLower(search)) + "%"
		where = append(where, `(LOWER(matched.trace_id) LIKE ? ESCAPE '\' OR LOWER(matched.service) LIKE ? ESCAPE '\' OR LOWER(matched.operation) LIKE ? ESCAPE '\' OR LOWER(matched.entry_id) LIKE ? ESCAPE '\')`)
		args = append(args, pattern, pattern, pattern, pattern)
	}
	having := ""
	if cursor != "" {
		decoded, err := decodeTraceCursor(cursor)
		if err != nil {
			return tracePage{}, err
		}
		having = `HAVING (MAX(matched.updated_at) < ? OR (MAX(matched.updated_at) = ? AND matched.trace_id < ?))`
		args = append(args, decoded.UpdatedAt, decoded.UpdatedAt, decoded.TraceID)
	}
	queryArgs := append([]any(nil), args...)
	queryArgs = append(queryArgs, limit+1)
	resultVisibility, resultVisibilityArgs := traceVisibilitySQL(checker, "result_spans")
	queryArgs = append(queryArgs, resultVisibilityArgs...)
	rows, err := s.db.QueryContext(ctx, `
		WITH candidate_traces AS (
			SELECT matched.trace_id, MAX(matched.updated_at) AS match_updated_at
			FROM trace_spans matched WHERE `+strings.Join(where, " AND ")+`
			GROUP BY matched.trace_id
			`+having+`
			ORDER BY match_updated_at DESC, matched.trace_id DESC
			LIMIT ?
		)
		SELECT result_spans.trace_id,result_spans.span_id,result_spans.parent_span_id,result_spans.connection_id,result_spans.stream_key,
			result_spans.group_name,result_spans.entry_id,result_spans.consumer,result_spans.service,result_spans.operation,
			result_spans.registered_at,result_spans.processing_started_at,result_spans.processed_at,result_spans.acknowledged_at,
			result_spans.outcome,result_spans.error_message,result_spans.attempt,result_spans.attributes_json,result_spans.created_at,result_spans.updated_at,
			candidate.match_updated_at
		FROM trace_spans result_spans INNER JOIN candidate_traces candidate ON candidate.trace_id=result_spans.trace_id
		WHERE `+resultVisibility+`
		ORDER BY candidate.match_updated_at DESC,candidate.trace_id DESC,
			COALESCE(result_spans.registered_at,result_spans.created_at),result_spans.span_id`, queryArgs...)
	if err != nil {
		return tracePage{}, err
	}
	defer rows.Close()
	type traceAggregate struct {
		summary        traceSummary
		streams        map[string]struct{}
		failed         bool
		inFlight       bool
		matchUpdatedAt int64
	}
	aggregates := map[string]*traceAggregate{}
	for rows.Next() {
		span, matchUpdatedAt, scanErr := scanTraceSpanWithMatchUpdated(rows)
		if scanErr != nil {
			return tracePage{}, scanErr
		}
		aggregate := aggregates[span.TraceID]
		if aggregate == nil {
			aggregate = &traceAggregate{
				summary:        traceSummary{TraceID: span.TraceID},
				streams:        map[string]struct{}{},
				matchUpdatedAt: matchUpdatedAt,
			}
			aggregates[span.TraceID] = aggregate
		}
		aggregate.summary.SpanCount++
		aggregate.streams[span.ConnectionID+"\x00"+span.StreamKey] = struct{}{}
		startedAt := span.RegisteredAt
		if startedAt == nil {
			startedAt = &span.CreatedAt
		}
		if aggregate.summary.StartedAt == nil || startedAt.Before(*aggregate.summary.StartedAt) {
			aggregate.summary.StartedAt = lifecycleCloneTime(startedAt)
		}
		completedAt := span.AcknowledgedAt
		if completedAt == nil {
			completedAt = span.ProcessedAt
		}
		if completedAt != nil && (aggregate.summary.CompletedAt == nil || completedAt.After(*aggregate.summary.CompletedAt)) {
			aggregate.summary.CompletedAt = lifecycleCloneTime(completedAt)
		}
		if span.UpdatedAt.After(aggregate.summary.UpdatedAt) {
			aggregate.summary.UpdatedAt = span.UpdatedAt
		}
		outcome := strings.ToLower(span.Outcome)
		if outcome == "failed" || outcome == "failure" || outcome == "error" || span.Error != "" {
			aggregate.failed = true
		}
		if span.AcknowledgedAt == nil && span.ProcessedAt == nil && span.Error == "" {
			aggregate.inFlight = true
		}
	}
	if err := rows.Err(); err != nil {
		return tracePage{}, err
	}
	type orderedTraceSummary struct {
		summary        traceSummary
		matchUpdatedAt int64
	}
	ordered := make([]orderedTraceSummary, 0, len(aggregates))
	for _, aggregate := range aggregates {
		item := aggregate.summary
		item.StreamCount = len(aggregate.streams)
		item.Status = "success"
		if aggregate.failed {
			item.Status = "failed"
		} else if aggregate.inFlight {
			item.Status = "in_flight"
		}
		if item.StartedAt != nil && item.CompletedAt != nil {
			duration := float64(item.CompletedAt.Sub(*item.StartedAt).Microseconds()) / 1000
			item.DurationMs = &duration
		}
		ordered = append(ordered, orderedTraceSummary{summary: item, matchUpdatedAt: aggregate.matchUpdatedAt})
	}
	sort.Slice(ordered, func(first, second int) bool {
		if ordered[first].matchUpdatedAt == ordered[second].matchUpdatedAt {
			return ordered[first].summary.TraceID > ordered[second].summary.TraceID
		}
		return ordered[first].matchUpdatedAt > ordered[second].matchUpdatedAt
	})
	visibleCount := len(ordered)
	if visibleCount > limit {
		visibleCount = limit
	}
	page := tracePage{Items: make([]traceSummary, 0, visibleCount)}
	for _, item := range ordered[:visibleCount] {
		page.Items = append(page.Items, item.summary)
	}
	if len(ordered) > limit {
		page.HasMore = true
		last := ordered[limit-1]
		page.NextCursor = encodeTraceCursor(traceCursor{UpdatedAt: last.matchUpdatedAt, TraceID: last.summary.TraceID})
	}
	return page, nil
}

func encodeTraceCursor(cursor traceCursor) string {
	payload, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(payload)
}

func decodeTraceCursor(raw string) (traceCursor, error) {
	payload, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return traceCursor{}, errors.New("invalid trace cursor")
	}
	var cursor traceCursor
	if json.Unmarshal(payload, &cursor) != nil || cursor.UpdatedAt <= 0 || cursor.TraceID == "" {
		return traceCursor{}, errors.New("invalid trace cursor")
	}
	return cursor, nil
}

func (s *apiServer) traces(writer http.ResponseWriter, request *http.Request) {
	session, _ := request.Context().Value(sessionContextKey).(sessionRecord)
	connectionID := strings.TrimSpace(request.URL.Query().Get("connectionId"))
	streamKey := strings.TrimSpace(request.URL.Query().Get("streamKey"))
	if session.Role != "admin" && (connectionID == "" || streamKey == "") {
		writeError(writer, http.StatusBadRequest, "trace_scope_required", "connectionId and streamKey are required")
		return
	}
	limit := traceListDefaultLimit
	if raw := strings.TrimSpace(request.URL.Query().Get("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > traceListMaximumLimit {
			writeError(writer, http.StatusBadRequest, "invalid_limit", "limit must be between 1 and 200")
			return
		}
		limit = parsed
	}
	checker, err := s.store.permissionChecker(request.Context(), session, "streams:read")
	if err != nil {
		writePermissionCheckError(writer)
		return
	}
	page, err := s.store.listTraceSummaries(request.Context(), connectionID,
		streamKey, request.URL.Query().Get("search"), request.URL.Query().Get("cursor"), limit, checker)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "trace_query_failed", err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, page)
}

func (s *apiServer) traceByID(writer http.ResponseWriter, request *http.Request) {
	session := requestSession(request)
	checker, err := s.store.permissionChecker(request.Context(), session, "streams:read")
	if err != nil {
		writePermissionCheckError(writer)
		return
	}
	items, err := s.store.listTraceSpans(request.Context(), request.PathValue("traceId"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "trace_query_failed", err.Error())
		return
	}
	if len(items) == 0 {
		writeError(writer, http.StatusNotFound, "trace_not_found", "Trace was not found.")
		return
	}
	visible := make([]traceSpan, 0, len(items))
	for _, item := range items {
		if checker.allows("streams:read", redisStreamScope(item.ConnectionID, item.StreamKey)) {
			visible = append(visible, item)
		}
	}
	if len(visible) == 0 {
		writeError(writer, http.StatusNotFound, "trace_not_found", "Trace was not found.")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"traceId": visible[0].TraceID, "spans": visible})
}

func normalizeDashboardDefinition(definition dashboardDefinition) (dashboardDefinition, error) {
	allowedRanges := map[string]bool{"1m": true, "5m": true, "15m": true, "1h": true, "6h": true, "24h": true, "7d": true}
	if definition.TimeRange == "" {
		definition.TimeRange = "5m"
	}
	if !allowedRanges[definition.TimeRange] {
		return dashboardDefinition{}, errors.New("unsupported dashboard time range")
	}
	if len(definition.Targets) == 0 || len(definition.Targets) > dashboardMaximumTargets {
		return dashboardDefinition{}, errors.New("a dashboard must contain between 1 and 8 targets")
	}
	seen := map[string]bool{}
	targets := make([]dashboardTarget, 0, len(definition.Targets))
	for _, target := range definition.Targets {
		target.ConnectionID = strings.TrimSpace(target.ConnectionID)
		target.StreamKey = strings.TrimSpace(target.StreamKey)
		if target.ConnectionID == "" || len(target.ConnectionID) > 128 || target.StreamKey == "" || len(target.StreamKey) > 1024 {
			return dashboardDefinition{}, errors.New("dashboard target connectionId and streamKey are required")
		}
		key := target.ConnectionID + "\x00" + target.StreamKey
		if !seen[key] {
			seen[key] = true
			targets = append(targets, target)
		}
	}
	definition.Targets = targets
	allowedWidgets := map[string]bool{"lag": true, "pending": true, "rates": true, "latency": true, "lifecycle": true, "capacity": true}
	widgets := []string{}
	for _, widget := range definition.Widgets {
		widget = strings.TrimSpace(widget)
		if !allowedWidgets[widget] {
			return dashboardDefinition{}, fmt.Errorf("unsupported dashboard widget: %s", widget)
		}
		if !dashboardContainsString(widgets, widget) {
			widgets = append(widgets, widget)
		}
	}
	if len(widgets) == 0 {
		widgets = []string{"lag", "rates", "latency"}
	}
	definition.Widgets = widgets
	return definition, nil
}

func dashboardContainsString(items []string, value string) bool {
	for _, item := range items {
		if item == value {
			return true
		}
	}
	return false
}

func (s *store) createDashboard(ctx context.Context, name, ownerID string, shared bool, definition dashboardDefinition) (savedDashboard, error) {
	name, ownerID = strings.TrimSpace(name), strings.TrimSpace(ownerID)
	if name == "" || len(name) > 120 || ownerID == "" || len(ownerID) > 128 {
		return savedDashboard{}, errors.New("dashboard name and owner are required")
	}
	normalized, err := normalizeDashboardDefinition(definition)
	if err != nil {
		return savedDashboard{}, err
	}
	id, err := randomID(12)
	if err != nil {
		return savedDashboard{}, err
	}
	encoded, _ := json.Marshal(normalized)
	now := time.Now().UTC()
	result, err := s.db.ExecContext(ctx, `INSERT INTO saved_dashboards(id,name,owner_id,shared,definition_json,created_at,updated_at)
		SELECT ?,?,?,?,?,?,?
		WHERE (SELECT COUNT(*) FROM saved_dashboards WHERE owner_id=?) < ?
		  AND (SELECT COUNT(*) FROM saved_dashboards) < ?
		  AND (?=0 OR (SELECT COUNT(*) FROM saved_dashboards WHERE shared=1) < ?)`,
		id, name, ownerID, boolToInt(shared), string(encoded), now.UnixNano(), now.UnixNano(),
		ownerID, dashboardMaximumPerOwner, dashboardMaximumTotal, boolToInt(shared), dashboardMaximumShared)
	if err != nil {
		return savedDashboard{}, err
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return savedDashboard{}, err
	}
	if inserted == 0 {
		return savedDashboard{}, errDashboardQuotaExceeded
	}
	return savedDashboard{ID: id, Name: name, OwnerID: ownerID, Shared: shared, Definition: normalized, CreatedAt: now, UpdatedAt: now}, nil
}

func (s *store) listDashboards(ctx context.Context, session sessionRecord) ([]savedDashboard, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,name,owner_id,shared,definition_json,created_at,updated_at
		FROM saved_dashboards WHERE owner_id=? OR shared=1 OR ?='admin' ORDER BY updated_at DESC,name LIMIT ?`, session.UserID, session.Role, dashboardListMaximum)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []savedDashboard{}
	for rows.Next() {
		item, scanErr := scanDashboard(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

type dashboardScanner interface{ Scan(...any) error }

func scanDashboard(scanner dashboardScanner) (savedDashboard, error) {
	var item savedDashboard
	var shared int
	var definition string
	var createdAt, updatedAt int64
	if err := scanner.Scan(&item.ID, &item.Name, &item.OwnerID, &shared, &definition, &createdAt, &updatedAt); err != nil {
		return savedDashboard{}, err
	}
	if err := json.Unmarshal([]byte(definition), &item.Definition); err != nil {
		return savedDashboard{}, err
	}
	item.Shared = shared != 0
	item.CreatedAt = time.Unix(0, createdAt).UTC()
	item.UpdatedAt = time.Unix(0, updatedAt).UTC()
	return item, nil
}

func (s *store) updateDashboard(ctx context.Context, id, actorID, actorRole, name string, shared bool, definition dashboardDefinition) (savedDashboard, error) {
	current, err := s.getDashboard(ctx, id)
	if err != nil {
		return savedDashboard{}, err
	}
	if current.OwnerID != actorID && actorRole != "admin" {
		return savedDashboard{}, errDashboardForbidden
	}
	name = strings.TrimSpace(name)
	if name == "" || len(name) > 120 {
		return savedDashboard{}, errors.New("dashboard name is required")
	}
	normalized, err := normalizeDashboardDefinition(definition)
	if err != nil {
		return savedDashboard{}, err
	}
	encoded, _ := json.Marshal(normalized)
	now := time.Now().UTC()
	result, err := s.db.ExecContext(ctx, `UPDATE saved_dashboards SET name=?,shared=?,definition_json=?,updated_at=?
		WHERE id=? AND (?=0 OR shared=1 OR (SELECT COUNT(*) FROM saved_dashboards WHERE shared=1) < ?)`,
		name, boolToInt(shared), string(encoded), now.UnixNano(), id, boolToInt(shared), dashboardMaximumShared)
	if err != nil {
		return savedDashboard{}, err
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return savedDashboard{}, err
	}
	if updated == 0 {
		return savedDashboard{}, errDashboardQuotaExceeded
	}
	return s.getDashboard(ctx, id)
}

var errDashboardForbidden = errors.New("dashboard is owned by another user")
var errDashboardQuotaExceeded = errors.New("saved dashboard quota exceeded")

func (s *store) getDashboard(ctx context.Context, id string) (savedDashboard, error) {
	return scanDashboard(s.db.QueryRowContext(ctx, `SELECT id,name,owner_id,shared,definition_json,created_at,updated_at FROM saved_dashboards WHERE id=?`, strings.TrimSpace(id)))
}

func (s *store) deleteDashboard(ctx context.Context, id, actorID, actorRole string) error {
	current, err := s.getDashboard(ctx, id)
	if err != nil {
		return err
	}
	if current.OwnerID != actorID && actorRole != "admin" {
		return errDashboardForbidden
	}
	_, err = s.db.ExecContext(ctx, `DELETE FROM saved_dashboards WHERE id=?`, id)
	return err
}

func (s *apiServer) dashboards(writer http.ResponseWriter, request *http.Request) {
	session, _ := request.Context().Value(sessionContextKey).(sessionRecord)
	if request.Method == http.MethodGet {
		items, err := s.store.listDashboards(request.Context(), session)
		if err != nil {
			writeError(writer, http.StatusInternalServerError, "dashboards_failed", err.Error())
			return
		}
		visible := make([]savedDashboard, 0, len(items))
		for _, item := range items {
			if s.dashboardTargetsAllowed(request.Context(), session, item.Definition.Targets) {
				visible = append(visible, item)
			}
		}
		writeJSON(writer, http.StatusOK, map[string]any{"items": visible})
		return
	}
	var input struct {
		Name       string              `json:"name"`
		Shared     bool                `json:"shared"`
		Definition dashboardDefinition `json:"definition"`
	}
	if err := readJSON(request, &input, 64<<10); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if !s.dashboardTargetsAllowed(request.Context(), session, input.Definition.Targets) {
		writeError(writer, http.StatusForbidden, "permission_denied", "A dashboard target is outside your stream access.")
		return
	}
	item, err := s.store.createDashboard(request.Context(), input.Name, session.UserID, input.Shared, input.Definition)
	if errors.Is(err, errDashboardQuotaExceeded) {
		writeError(writer, http.StatusConflict, "dashboard_quota_exceeded", err.Error())
		return
	}
	if err != nil {
		writeError(writer, http.StatusBadRequest, "dashboard_create_failed", err.Error())
		return
	}
	writeJSON(writer, http.StatusCreated, item)
}

func (s *apiServer) dashboardByID(writer http.ResponseWriter, request *http.Request) {
	session, _ := request.Context().Value(sessionContextKey).(sessionRecord)
	id := request.PathValue("id")
	if request.Method == http.MethodDelete {
		err := s.store.deleteDashboard(request.Context(), id, session.UserID, session.Role)
		if errors.Is(err, sql.ErrNoRows) {
			writeError(writer, http.StatusNotFound, "dashboard_not_found", "Dashboard was not found.")
			return
		}
		if errors.Is(err, errDashboardForbidden) {
			writeError(writer, http.StatusForbidden, "permission_denied", err.Error())
			return
		}
		if err != nil {
			writeError(writer, http.StatusInternalServerError, "dashboard_delete_failed", err.Error())
			return
		}
		writeJSON(writer, http.StatusOK, map[string]any{"ok": true})
		return
	}
	var input struct {
		Name       string              `json:"name"`
		Shared     bool                `json:"shared"`
		Definition dashboardDefinition `json:"definition"`
	}
	if err := readJSON(request, &input, 64<<10); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if !s.dashboardTargetsAllowed(request.Context(), session, input.Definition.Targets) {
		writeError(writer, http.StatusForbidden, "permission_denied", "A dashboard target is outside your stream access.")
		return
	}
	item, err := s.store.updateDashboard(request.Context(), id, session.UserID, session.Role, input.Name, input.Shared, input.Definition)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(writer, http.StatusNotFound, "dashboard_not_found", "Dashboard was not found.")
		return
	}
	if errors.Is(err, errDashboardForbidden) {
		writeError(writer, http.StatusForbidden, "permission_denied", err.Error())
		return
	}
	if errors.Is(err, errDashboardQuotaExceeded) {
		writeError(writer, http.StatusConflict, "dashboard_quota_exceeded", err.Error())
		return
	}
	if err != nil {
		writeError(writer, http.StatusBadRequest, "dashboard_update_failed", err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, item)
}

func (s *apiServer) dashboardTargetsAllowed(ctx context.Context, session sessionRecord, targets []dashboardTarget) bool {
	for _, target := range targets {
		connectionID := strings.TrimSpace(target.ConnectionID)
		streamKey := strings.TrimSpace(target.StreamKey)
		if connectionID == "" || streamKey == "" || !s.store.allowed(ctx, session, "streams:read", "stream:"+connectionID+":"+streamKey) {
			return false
		}
	}
	return len(targets) > 0
}
